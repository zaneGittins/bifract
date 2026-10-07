package models

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// sanitizeStats replaces non-finite float values (NaN/Inf, e.g. avg() over an
// empty window) with 0 so the result always JSON-encodes. Returns the map for
// convenient inline use.
func sanitizeStats(stats map[string]interface{}) map[string]interface{} {
	for k, v := range stats {
		switch f := v.(type) {
		case float64:
			if math.IsNaN(f) || math.IsInf(f, 0) {
				stats[k] = float64(0)
			}
		case float32:
			if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
				stats[k] = float64(0)
			}
		}
	}
	return stats
}

// previewWindows maps the allowed preview lookback windows to their day count.
// Kept short on purpose: a preview scans raw logs live (no pre-aggregated table
// exists yet), so the window doubles as the cost ceiling.
var previewWindows = map[string]int{
	"1d":  1,
	"7d":  7,
	"30d": 30,
}

// PreviewWindowDays returns the day count for a preview window and whether valid.
func PreviewWindowDays(window string) (int, bool) {
	d, ok := previewWindows[window]
	return d, ok
}

// previewMaxSpan caps an explicit preview range. A preview scans raw logs live,
// so the span is its cost ceiling; 90 days matches the longest backfill.
const previewMaxSpan = 90 * 24 * time.Hour

// PreviewRange is the half-open [Start, End) window a preview scores. Label is
// what the result reports back: the preset name, or "" for an explicit range.
type PreviewRange struct {
	Start, End time.Time
	Label      string
}

// ResolvePreviewRange turns a request's window preset or explicit start/end into
// the range to scan. An explicit range wins over the preset, must be ordered,
// may not exceed previewMaxSpan, and has its end clamped to now.
func ResolvePreviewRange(window string, start, end *time.Time, now time.Time) (PreviewRange, error) {
	now = now.UTC()
	if start != nil || end != nil {
		if start == nil || end == nil {
			return PreviewRange{}, fmt.Errorf("preview range needs both start and end")
		}
		s, e := start.UTC(), end.UTC()
		if e.After(now) {
			e = now
		}
		if !s.Before(e) {
			return PreviewRange{}, fmt.Errorf("preview range start must be before its end")
		}
		if e.Sub(s) > previewMaxSpan {
			return PreviewRange{}, fmt.Errorf("preview range is limited to 90 days")
		}
		return PreviewRange{Start: s, End: e}, nil
	}
	if window == "" {
		window = "7d"
	}
	days, ok := PreviewWindowDays(window)
	if !ok {
		return PreviewRange{}, fmt.Errorf("invalid preview window: %s", window)
	}
	return PreviewRange{Start: now.Add(-time.Duration(days) * 24 * time.Hour), End: now, Label: window}, nil
}

// days is the range's length in whole days, at least 1.
func (r PreviewRange) days() int {
	d := int(math.Ceil(r.End.Sub(r.Start).Hours() / 24))
	if d < 1 {
		return 1
	}
	return d
}

// endsNow reports whether the range ends at the present, so the preview scores
// exactly as the live model does rather than as of a past instant.
func (r PreviewRange) endsNow(now time.Time) bool {
	return now.Sub(r.End) < time.Minute
}

// PreviewResult is the pre-save estimate of a model's output over a recent
// window. It mirrors, as closely as the data allows, what the model would
// produce once backfilled over the same window: the scoring SQL is shared with
// the live data path, and rarity is day-bucketed to match the day-chunked
// backfill. Counts accumulate further once the model streams live, so the
// preview reflects the backfill view of recent history.
type PreviewResult struct {
	ModelType  ModelType                `json:"model_type"`
	Window     string                   `json:"window"`
	Start      time.Time                `json:"start"`
	End        time.Time                `json:"end"`
	Metric     string                   `json:"metric"` // confidence | z_score | event_count
	Histogram  []histBucket             `json:"histogram"`
	Stats      map[string]interface{}   `json:"stats"`
	WouldFlag  uint64                   `json:"would_flag"`
	FlagBasis  string                   `json:"flag_basis"`
	Top        []map[string]interface{} `json:"top"`
	TopColumns []string                 `json:"top_columns"`

	// Distribution lets the editor recount would_flag as thresholds move,
	// without another scan of raw logs. Nil for a type with no threshold.
	Distribution *ScoreDistribution `json:"distribution,omitempty"`
}

// ScoreDistribution is the scored output binned on the alert's threshold axes.
// Each cell is [bin per dim..., count]. A "ceil" dim's bin k holds values in
// ((k-1)*step, k*step], so value > t*step iff k > t; a "floor" dim's bin k holds
// [k*step, (k+1)*step), so value < t*step iff k < t and value >= t*step iff
// k >= t. Counts are exact for thresholds on the step grid.
type ScoreDistribution struct {
	Dims      []string  `json:"dims"`
	Steps     []float64 `json:"steps"`
	Bins      []string  `json:"bins"`
	Cells     [][]int64 `json:"cells"`
	Truncated bool      `json:"truncated,omitempty"`
}

// previewDistCellCap bounds the cells a distribution returns. Past it the
// editor falls back to re-running the preview when a threshold moves.
const previewDistCellCap = 50000

// previewConfig caps the cost of a single preview. A preview is interactive, so
// it cannot throttle across minutes the way a backfill does; instead it bounds
// each query and fails fast with a clear message if the window is too expensive.
type previewConfig struct {
	maxExecutionSec int
	maxThreads      int
	maxGroupByBytes int64
}

func loadPreviewConfig() previewConfig {
	cfg := previewConfig{maxExecutionSec: 25, maxThreads: 4, maxGroupByBytes: 2_000_000_000}
	if v, ok := envInt("BIFRACT_MODEL_PREVIEW_MAX_EXEC"); ok && v >= 1 {
		cfg.maxExecutionSec = v
	}
	if v, ok := envInt("BIFRACT_MODEL_PREVIEW_MAX_THREADS"); ok && v >= 1 {
		cfg.maxThreads = v
	}
	if v, ok := envInt64("BIFRACT_MODEL_PREVIEW_MAX_GROUPBY_BYTES"); ok && v >= 0 {
		cfg.maxGroupByBytes = v
	}
	return cfg
}

func (c previewConfig) settings() string {
	return fmt.Sprintf(" SETTINGS max_execution_time=%d, max_threads=%d, max_bytes_before_external_group_by=%d",
		c.maxExecutionSec, c.maxThreads, c.maxGroupByBytes)
}

// Preview computes a model's estimated output over a recent window WITHOUT
// creating any ClickHouse objects. It builds the same aggregation the backfill
// would write, then runs the same scoring SQL the live data view uses, so the
// numbers match the model once it is backfilled over the same window.
func (m *Manager) Preview(ctx context.Context, fractalID string, mt ModelType, def ModelDefinition, rng PreviewRange) (*PreviewResult, error) {
	if err := validateDefinitionShape(mt, def); err != nil {
		return nil, err
	}
	def, err := m.ResolveSource(ctx, def, fractalID, "")
	if err != nil {
		return nil, err
	}

	// Network models score in Go over a one-off raw-log aggregation (no state table
	// exists yet), reusing the same scoring core as the scorer so preview == warmed.
	if mt.IsScheduled() {
		return m.previewNetwork(ctx, fractalID, mt, def, rng)
	}

	start, end := rng.Start, rng.End
	fidEsc := storage.EscCHStr(fractalID)
	// buildModelSelect scopes the scan to fractalID itself; whereExtra adds only the window.
	whereExtra := fmt.Sprintf("timestamp >= '%s' AND timestamp < '%s'",
		start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))

	agg, err := buildModelSelect(def, mt, m.ch.ReadTable(), whereExtra, fractalID, recordedHistory)
	if err != nil {
		return nil, fmt.Errorf("build preview aggregation: %w", err)
	}
	source := "(" + agg + ")"

	res := &PreviewResult{ModelType: mt, Window: rng.Label, Start: start, End: end}
	switch mt {
	case ModelTypeRarity:
		err = m.previewRarity(ctx, res, source, fidEsc, def)
	case ModelTypeFirstSeen:
		err = m.previewFirstSeen(ctx, res, source, fidEsc, def, end, "entity_key")
	case ModelTypeTLSH:
		err = m.previewFirstSeen(ctx, res, source, fidEsc, def, end, "digest")
	case ModelTypeVolumeBaseline:
		asOf := ""
		if !rng.endsNow(time.Now().UTC()) {
			asOf = fmt.Sprintf("toDateTime('%s', 'UTC')", end.Format("2006-01-02 15:04:05"))
		}
		err = m.previewVolume(ctx, res, source, fidEsc, def, start, asOf)
	default:
		return nil, fmt.Errorf("unknown model type: %s", mt)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

// runPreviewQueries runs the shape query (a histogram or a score distribution),
// the metrics and the top-rows queries concurrently against the same windowed
// source. They are independent reads, so fanning out keeps preview latency near
// a single scan (ClickHouse also shares the warm page cache across them).
func (m *Manager) runPreviewQueries(ctx context.Context, shapeSQL, metricsSQL, topSQL string) (shape []map[string]interface{}, metrics map[string]interface{}, top []map[string]interface{}, err error) {
	cfg := loadPreviewConfig()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	record := func(e error) {
		if e == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		mu.Unlock()
	}

	wg.Add(3)
	go func() {
		defer wg.Done()
		rows, e := m.ch.QuerySchema(ctx, shapeSQL+cfg.settings())
		if e != nil {
			record(fmt.Errorf("preview distribution: %w", e))
			return
		}
		shape = rows
	}()
	go func() {
		defer wg.Done()
		rows, e := m.ch.QuerySchema(ctx, metricsSQL+cfg.settings())
		if e != nil {
			record(fmt.Errorf("preview metrics: %w", e))
			return
		}
		if len(rows) > 0 {
			metrics = rows[0]
		} else {
			metrics = map[string]interface{}{}
		}
	}()
	go func() {
		defer wg.Done()
		rows, e := m.ch.QuerySchema(ctx, topSQL+cfg.settings())
		if e != nil {
			record(fmt.Errorf("preview top rows: %w", e))
			return
		}
		top = rows
	}()
	wg.Wait()
	return shape, metrics, top, firstErr
}

// rarityDistSQL bins scored rarity rows by confidence (ceil, 0.01), percent
// (floor, 0.1) and histogram band, with s = 1 when the row's group has the min history.
func rarityDistSQL(scored string, minSample int) string {
	return fmt.Sprintf(`SELECT toInt64(ceil(round(confidence * 100, 6))) AS c,
    toInt64(floor(round(percent * 10, 6))) AS p,
    toInt64(%s) AS h,
    toInt64(model_total >= %d) AS s,
    toInt64(count()) AS n
FROM (%s)
GROUP BY c, p, h, s
LIMIT %d`, rarityHistBucketExpr, minSample, scored, previewDistCellCap+1)
}

// volumeDistSQL bins scored volume rows by z-score (ceil, 0.1, clamped to
// +/-1000 so the flat-history sentinel lands in the top bin) and histogram band.
func volumeDistSQL(scored string) string {
	return fmt.Sprintf(`SELECT toInt64(ceil(round(greatest(least(z_score, 1000), -1000) * 10, 6))) AS z,
    toInt64(%s) AS h,
    toInt64(count()) AS n
FROM (%s)
GROUP BY z, h
LIMIT %d`, volumeHistBucketExpr, scored, previewDistCellCap+1)
}

// splitDistRows turns distribution rows into the histogram (summed by column h)
// and the cells over dims, keeping only rows where keep is unset or 1.
func splitDistRows(rows []map[string]interface{}, labels []string, dims []string, keep string) ([]histBucket, [][]int64) {
	counts := make([]uint64, len(labels))
	type key [3]int64
	agg := map[key]int64{}
	var order []key
	for _, r := range rows {
		n := toInt64(r["n"])
		if h := toInt64(r["h"]); h >= 0 && int(h) < len(counts) {
			counts[h] += uint64(n)
		}
		if keep != "" && toInt64(r[keep]) != 1 {
			continue
		}
		var k key
		for i, d := range dims {
			k[i] = toInt64(r[d])
		}
		if _, ok := agg[k]; !ok {
			order = append(order, k)
		}
		agg[k] += n
	}
	hist := make([]histBucket, len(labels))
	for i, l := range labels {
		hist[i] = histBucket{Label: l, Count: counts[i]}
	}
	cells := make([][]int64, 0, len(order))
	for _, k := range order {
		cell := append([]int64{}, k[:len(dims)]...)
		cells = append(cells, append(cell, agg[k]))
	}
	return hist, cells
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

func (m *Manager) previewRarity(ctx context.Context, res *PreviewResult, source, fidEsc string, def ModelDefinition) error {
	scored := buildRarityScoredSQL(source, fidEsc)

	// The same rule the data view counts with and the alert fires on, rendered once.
	preds := rarityFlagPredicates(def)
	minSample := def.MinSample
	if minSample < 1 {
		minSample = 1
	}
	flagPred := preds.SQL()
	res.FlagBasis = preds.Text()
	res.Metric = "confidence"

	// ifNotFinite guards avg() over an empty window (NaN), which would otherwise
	// fail JSON encoding; sanitizeStats is a second line of defense.
	metricsSQL := fmt.Sprintf(`SELECT
    toUInt64(count()) AS scored_values,
    toUInt64(uniqExact(partition_val)) AS partitions,
    round(ifNotFinite(avg(confidence), 0), 4) AS avg_confidence,
    round(ifNotFinite(max(confidence), 0), 4) AS max_confidence,
    toUInt64(countIf(%s)) AS would_flag
FROM (%s)`, flagPred, scored)

	topSQL := fmt.Sprintf(`SELECT partition_val, value_val, model_count, percent, confidence
FROM (%s)
WHERE model_total >= %d
ORDER BY confidence DESC, percent ASC, model_count DESC
LIMIT 25`, scored, minSample)

	shape, metrics, top, err := m.runPreviewQueries(ctx, rarityDistSQL(scored, minSample), metricsSQL, topSQL)
	if err != nil {
		return err
	}
	hist, cells := splitDistRows(shape, rarityHistLabels, []string{"c", "p"}, "s")
	res.Histogram = hist
	res.Distribution = &ScoreDistribution{
		Dims: []string{"confidence", "percent"}, Steps: []float64{0.01, 0.1}, Bins: []string{"ceil", "floor"},
		Cells: cells, Truncated: len(shape) > previewDistCellCap,
	}
	res.Top = top
	res.TopColumns = []string{"partition_val", "value_val", "model_count", "percent", "confidence"}
	res.WouldFlag = numToUint64(metrics["would_flag"])
	res.Stats = sanitizeStats(map[string]interface{}{
		"scored_values":  metrics["scored_values"],
		"partitions":     metrics["partitions"],
		"avg_confidence": metrics["avg_confidence"],
		"max_confidence": metrics["max_confidence"],
	})
	return nil
}

func (m *Manager) previewFirstSeen(ctx context.Context, res *PreviewResult, source, fidEsc string, def ModelDefinition, end time.Time, keyCol string) error {
	agg := firstSeenAggSQL(source, fidEsc, "", keyCol, false)

	// The is_new alert fires on entities the live model records for the first time,
	// which nothing in history can replay. Instead we report the new-entity RATE:
	// entities first observed in the last 24h of the window (new entities/day). The
	// FlagBasis makes clear this is a rate estimate, not an instantaneous count.
	recent := end.Add(-24 * time.Hour).UTC().Format("2006-01-02 15:04:05")
	res.Metric = "event_count"

	metricsSQL := fmt.Sprintf(`SELECT
    toUInt64(count()) AS entities,
    toUInt64(countIf(first_seen >= toDateTime64('%s', 3, 'UTC'))) AS new_recent
FROM (%s)`, recent, agg)

	topSQL := fmt.Sprintf(`SELECT %s, first_seen, last_seen, event_count
FROM (%s)
ORDER BY first_seen DESC, event_count DESC
LIMIT 25`, keyCol, agg)

	shape, metrics, top, err := m.runPreviewQueries(ctx,
		histogramQuerySQL(firstSeenCountInner(source, fidEsc, keyCol), firstSeenHistBucketExpr), metricsSQL, topSQL)
	if err != nil {
		return err
	}
	res.Histogram = fillHistogram(shape, firstSeenHistLabels)
	res.Top = top
	res.TopColumns = []string{keyCol, "first_seen", "last_seen", "event_count"}

	alertOnNew := def.Alert != nil && def.Alert.AlertOnNew
	if alertOnNew {
		res.WouldFlag = numToUint64(metrics["new_recent"])
		res.FlagBasis = "~ new entities / day (alert on new entities)"
	} else {
		res.WouldFlag = numToUint64(metrics["entities"])
		res.FlagBasis = "every matching entity"
	}
	res.Stats = sanitizeStats(map[string]interface{}{
		"entities":   metrics["entities"],
		"new_recent": metrics["new_recent"],
	})
	return nil
}

func (m *Manager) previewVolume(ctx context.Context, res *PreviewResult, source, fidEsc string, def ModelDefinition, start time.Time, asOf string) error {
	// History starts at the preview window; the scored bucket is the latest
	// complete one before the window's end (asOf, "" for now), exactly as live
	// scoring does.
	lower := fmt.Sprintf("toDate('%s')", start.Format("2006-01-02"))
	if def.TimeBucket == "hour" {
		lower = fmt.Sprintf("toStartOfHour(toDateTime('%s', 'UTC'))", start.Format("2006-01-02 15:04:05"))
	}
	scored := parser.VolumeScoredSQLAt(source, "fractal_id = '"+fidEsc+"'", def.TimeBucket, def.MinSample, lower, asOf, true)

	z := 3.5
	if def.Alert != nil && def.Alert.ZThreshold > 0 {
		z = def.Alert.ZThreshold
	}
	res.Metric = "z_score"
	res.FlagBasis = fmt.Sprintf("z-score > %g", z)

	metricsSQL := fmt.Sprintf(`SELECT
    toUInt64(count()) AS entities_scored,
    round(ifNotFinite(max(abs(z_score)), 0), 4) AS max_z,
    toUInt64(countIf(z_score > %g)) AS would_flag
FROM (%s)`, z, scored)

	topSQL := fmt.Sprintf(`SELECT entity_val, latest_count, baseline_median, mad, n_buckets, z_score
FROM (%s)
ORDER BY abs(z_score) DESC
LIMIT 25`, scored)

	shape, metrics, top, err := m.runPreviewQueries(ctx, volumeDistSQL(scored), metricsSQL, topSQL)
	if err != nil {
		return err
	}
	hist, cells := splitDistRows(shape, volumeHistLabels, []string{"z"}, "")
	res.Histogram = hist
	res.Distribution = &ScoreDistribution{
		Dims: []string{"z_score"}, Steps: []float64{0.1}, Bins: []string{"ceil"},
		Cells: cells, Truncated: len(shape) > previewDistCellCap,
	}
	res.Top = top
	res.TopColumns = []string{"entity_val", "latest_count", "baseline_median", "mad", "n_buckets", "z_score"}
	res.WouldFlag = numToUint64(metrics["would_flag"])
	res.Stats = sanitizeStats(map[string]interface{}{
		"entities_scored": metrics["entities_scored"],
		"max_z":           metrics["max_z"],
		"min_buckets":     parser.VolumeMinBuckets(def.MinSample),
	})
	return nil
}

// previewNetworkPairCap bounds pairs scored in a preview so an interactive request
// stays cheap. Truncation is reflected in the stats (scanned vs scored).
const previewNetworkPairCap = 20000

// previewNetwork estimates a beacon/long_connection model's output over a recent
// window by aggregating raw logs once and scoring in Go with the SAME functions the
// scorer uses (including the prevalence modifier), so the preview matches the model
// once it warms up.
func (m *Manager) previewNetwork(ctx context.Context, fractalID string, mt ModelType, def ModelDefinition, rng PreviewRange) (*PreviewResult, error) {
	windowSecs := int64(rng.days()) * 86400
	bounds := NetPreviewBounds{Start: rng.Start, End: rng.End, Days: rng.days()}
	source := m.ch.ReadTable()
	cfg := loadPreviewConfig()

	bp := def.Beacon.WithDefaults(windowSecs)
	lc := def.LongConn.WithDefaults()
	mp := def.Modifiers.WithDefaults()
	threshold := networkScoreThreshold(def, mt)

	// Prevalence context over the same window (all sources, not just qualifying).
	var networkSize uint64
	sizeSQL, err := BuildNetPreviewNetworkSize(def, source, fractalID, bounds)
	if err != nil {
		return nil, err
	}
	if err := m.ch.QueryRow(ctx, sizeSQL+cfg.settings()).Scan(&networkSize); err != nil {
		return nil, fmt.Errorf("preview network size: %w", err)
	}
	prevalence := make(map[string]uint64)
	if networkSize > 0 {
		prevSQL, err := BuildNetPreviewPrevalence(def, source, fractalID, bounds)
		if err != nil {
			return nil, err
		}
		if err := m.ch.StreamQuery(ctx, "", prevSQL+cfg.settings(),
			func(row map[string]interface{}) error {
				prevalence[getString(row, "dst")] = getUint64(row, "prev_total")
				return nil
			}, nil); err != nil {
			return nil, fmt.Errorf("preview prevalence: %w", err)
		}
	}
	if networkSize == 0 {
		networkSize = 1 // avoid div-by-zero; no data yields an empty preview below
	}

	aggSQL, err := BuildNetPreviewAgg(def, mt, source, fractalID, bounds)
	if err != nil {
		return nil, err
	}

	cutoff := rng.End.Unix() - windowSecs
	var scored, scanned int
	var maxScore float64
	var flagged uint64
	var counts [10]uint64
	// fine is the final score binned at 0.01 (floor) for client-side recounts.
	var fine [101]int64
	type scoredPair struct {
		row   map[string]interface{}
		final float64
		rs    RegularityScore
		prev  float64
	}
	var top []scoredPair

	streamErr := m.ch.StreamQuery(ctx, "", aggSQL+cfg.settings(), func(row map[string]interface{}) error {
		scanned++
		if scored >= previewNetworkPairCap {
			return nil // keep counting scanned; stop scoring past the cap
		}
		scored++
		p := pairFromRow(row, cutoff)
		var rs RegularityScore
		if mt == ModelTypeLongConnection {
			rs = RegularityScore{Score: ScoreLongConn(p.TotalDuration, lc)}
		} else {
			rs = ScoreBeacon(p, bp, windowSecs)
		}
		prevRatio := float64(prevalence[p.Dst]) / float64(networkSize)
		final, _ := ApplyModifiers(rs.Score, prevRatio, mp)

		band := int(final * 10)
		if band > 9 {
			band = 9
		}
		if band < 0 {
			band = 0
		}
		counts[band]++
		fb := int(math.Floor(final*100 + 1e-9))
		if fb < 0 {
			fb = 0
		}
		if fb > 100 {
			fb = 100
		}
		fine[fb]++
		if final >= threshold {
			flagged++
		}
		if final > maxScore {
			maxScore = final
		}
		top = append(top, scoredPair{row: row, final: final, rs: rs, prev: round3(prevRatio)})
		return nil
	}, nil)
	if streamErr != nil {
		return nil, fmt.Errorf("preview aggregation: %w", streamErr)
	}

	// Top 25 by final score.
	sort.Slice(top, func(i, j int) bool { return top[i].final > top[j].final })
	if len(top) > 25 {
		top = top[:25]
	}
	topRows := make([]map[string]interface{}, 0, len(top))
	for _, sp := range top {
		topRows = append(topRows, map[string]interface{}{
			"src_ip":     getString(sp.row, "src"),
			"dst_ip":     getString(sp.row, "dst"),
			"dst_port":   getString(sp.row, "port"),
			"score":      sp.final,
			"regularity": sp.rs.Score,
			"ts_score":   sp.rs.TsScore,
			"ds_score":   sp.rs.DsScore,
			"dur_score":  sp.rs.DurScore,
			"hist_score": sp.rs.HistScore,
			"prevalence": sp.prev,
			"conn_count": getUint64(sp.row, "cnt"),
		})
	}

	buckets := make([]histBucket, len(rarityHistLabels))
	for i, l := range rarityHistLabels {
		buckets[i] = histBucket{Label: l, Count: counts[i]}
	}

	dist := &ScoreDistribution{Dims: []string{"final_score"}, Steps: []float64{0.01}, Bins: []string{"floor"}, Cells: [][]int64{}}
	for i, n := range fine {
		if n > 0 {
			dist.Cells = append(dist.Cells, []int64{int64(i), n})
		}
	}

	metricLabel := "beacon_score"
	if mt == ModelTypeLongConnection {
		metricLabel = "longconn_score"
	}
	return &PreviewResult{
		ModelType:    mt,
		Window:       rng.Label,
		Start:        rng.Start,
		End:          rng.End,
		Metric:       metricLabel,
		Histogram:    buckets,
		WouldFlag:    flagged,
		Distribution: dist,
		FlagBasis:    fmt.Sprintf("final score >= %g", threshold),
		Top:          topRows,
		TopColumns:   []string{"src_ip", "dst_ip", "dst_port", "score", "regularity", "ts_score", "ds_score", "dur_score", "hist_score", "prevalence", "conn_count"},
		Stats: sanitizeStats(map[string]interface{}{
			"pairs_scanned": uint64(scanned),
			"pairs_scored":  uint64(scored),
			"max_score":     round3(maxScore),
			"network_size":  networkSize,
		}),
	}, nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
