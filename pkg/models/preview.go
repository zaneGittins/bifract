package models

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

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

// PreviewResult is the pre-save estimate of a model's output over a recent
// window. It mirrors, as closely as the data allows, what the model would
// produce once backfilled over the same window: the scoring SQL is shared with
// the live data path, and rarity is day-bucketed to match the day-chunked
// backfill. Counts accumulate further once the model streams live, so the
// preview reflects the backfill view of recent history.
type PreviewResult struct {
	ModelType  ModelType                `json:"model_type"`
	Window     string                   `json:"window"`
	Metric     string                   `json:"metric"` // confidence | z_score | event_count
	Histogram  []histBucket             `json:"histogram"`
	Stats      map[string]interface{}   `json:"stats"`
	WouldFlag  uint64                   `json:"would_flag"`
	FlagBasis  string                   `json:"flag_basis"`
	Top        []map[string]interface{} `json:"top"`
	TopColumns []string                 `json:"top_columns"`
}

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
func (m *Manager) Preview(ctx context.Context, fractalID string, mt ModelType, def ModelDefinition, window string) (*PreviewResult, error) {
	if err := validateDefinitionShape(mt, def); err != nil {
		return nil, err
	}
	days, ok := PreviewWindowDays(window)
	if !ok {
		return nil, fmt.Errorf("invalid preview window: %s", window)
	}

	// Network models score in Go over a one-off raw-log aggregation (no state table
	// exists yet), reusing the same scoring core as the scorer so preview == warmed.
	if mt.IsScheduled() {
		return m.previewNetwork(ctx, fractalID, mt, def, window, days)
	}

	end := time.Now().UTC()
	start := end.Add(-time.Duration(days) * 24 * time.Hour)
	fidEsc := storage.EscCHStr(fractalID)
	whereExtra := fmt.Sprintf("fractal_id = '%s' AND timestamp >= '%s' AND timestamp < '%s'",
		fidEsc, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))

	// rarity counts distinct days, so its windowed aggregation must be split by
	// day to match the day-chunked backfill; first_seen/volume are day-invariant.
	opts := aggOpts{dayBucket: mt == ModelTypeRarity}
	agg, err := buildModelSelect(def, mt, m.ch.ReadTable(), whereExtra, opts)
	if err != nil {
		return nil, fmt.Errorf("build preview aggregation: %w", err)
	}
	source := "(" + agg + ")"

	res := &PreviewResult{ModelType: mt, Window: window}
	switch mt {
	case ModelTypeRarity:
		err = m.previewRarity(ctx, res, source, fidEsc, def)
	case ModelTypeFirstSeen:
		err = m.previewFirstSeen(ctx, res, source, fidEsc, def, end)
	case ModelTypeVolumeBaseline:
		err = m.previewVolume(ctx, res, source, fidEsc, def, start)
	default:
		return nil, fmt.Errorf("unknown model type: %s", mt)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

// runPreviewQueries runs the histogram, metrics, and top-rows queries
// concurrently against the same windowed source. They are independent reads, so
// fanning out keeps preview latency near a single scan (ClickHouse also shares
// the warm page cache across them).
func (m *Manager) runPreviewQueries(ctx context.Context, histInner, histBucketExpr string, histLabels []string,
	metricsSQL, topSQL string) (hist []histBucket, metrics map[string]interface{}, top []map[string]interface{}, err error) {
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
		rows, e := m.ch.Query(ctx, histogramQuerySQL(histInner, histBucketExpr)+cfg.settings())
		if e != nil {
			record(fmt.Errorf("preview histogram: %w", e))
			return
		}
		hist = fillHistogram(rows, histLabels)
	}()
	go func() {
		defer wg.Done()
		rows, e := m.ch.Query(ctx, metricsSQL+cfg.settings())
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
		rows, e := m.ch.Query(ctx, topSQL+cfg.settings())
		if e != nil {
			record(fmt.Errorf("preview top rows: %w", e))
			return
		}
		top = rows
	}()
	wg.Wait()
	return hist, metrics, top, firstErr
}

func (m *Manager) previewRarity(ctx context.Context, res *PreviewResult, source, fidEsc string, def ModelDefinition) error {
	scored := buildRarityScoredSQL(source, fidEsc)

	minSample := def.MinSample
	if minSample < 1 {
		minSample = 1
	}
	// Mirror the linked alert predicate (see GenerateQuery): model_lookup gates on
	// min sample, then confidence/percent thresholds apply when set (>0).
	flagPreds := []string{fmt.Sprintf("model_count >= %d", minSample)}
	basis := []string{fmt.Sprintf("min %d day%s", minSample, plural(minSample))}
	if def.Alert != nil {
		if def.Alert.ConfidenceThreshold > 0 {
			flagPreds = append(flagPreds, fmt.Sprintf("confidence > %g", def.Alert.ConfidenceThreshold))
			basis = append(basis, fmt.Sprintf("confidence > %g", def.Alert.ConfidenceThreshold))
		}
		if def.Alert.PercentThreshold > 0 {
			flagPreds = append(flagPreds, fmt.Sprintf("percent < %g", def.Alert.PercentThreshold))
			basis = append(basis, fmt.Sprintf("percent < %g", def.Alert.PercentThreshold))
		}
	}
	flagPred := strings.Join(flagPreds, " AND ")
	res.FlagBasis = strings.Join(basis, ", ")
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
WHERE model_count >= %d
ORDER BY confidence DESC, percent ASC, model_count DESC
LIMIT 25`, scored, minSample)

	hist, metrics, top, err := m.runPreviewQueries(ctx,
		rarityConfidenceInner(source, fidEsc), rarityHistBucketExpr, rarityHistLabels, metricsSQL, topSQL)
	if err != nil {
		return err
	}
	res.Histogram = hist
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

func (m *Manager) previewFirstSeen(ctx context.Context, res *PreviewResult, source, fidEsc string, def ModelDefinition, end time.Time) error {
	agg := firstSeenAggSQL(source, fidEsc, "")

	// The is_new alert (cmd_model_lookup) fires on entities whose first_seen is
	// within the last hour of each evaluation, so an exact count can't be replayed
	// from a historical backfill. Instead we report the new-entity RATE: entities
	// first observed in the last 24h of the window (i.e. new entities/day). The
	// FlagBasis makes clear this is a rate estimate, not an instantaneous count.
	recent := end.Add(-24 * time.Hour).Format("2006-01-02 15:04:05")
	res.Metric = "event_count"

	metricsSQL := fmt.Sprintf(`SELECT
    toUInt64(count()) AS entities,
    toUInt64(countIf(first_seen >= toDateTime64('%s', 3))) AS new_recent
FROM (%s)`, recent, agg)

	topSQL := fmt.Sprintf(`SELECT entity_key, first_seen, last_seen, event_count
FROM (%s)
ORDER BY first_seen DESC, event_count DESC
LIMIT 25`, agg)

	hist, metrics, top, err := m.runPreviewQueries(ctx,
		firstSeenCountInner(source, fidEsc), firstSeenHistBucketExpr, firstSeenHistLabels, metricsSQL, topSQL)
	if err != nil {
		return err
	}
	res.Histogram = hist
	res.Top = top
	res.TopColumns = []string{"entity_key", "first_seen", "last_seen", "event_count"}

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

func (m *Manager) previewVolume(ctx context.Context, res *PreviewResult, source, fidEsc string, def ModelDefinition, start time.Time) error {
	// Bound the scored buckets to the window, excluding the current incomplete
	// bucket (its count is artificially low), exactly as live scoring does.
	var lower, upper string
	if def.TimeBucket == "hour" {
		lower = fmt.Sprintf("toStartOfHour(toDateTime('%s'))", start.Format("2006-01-02 15:04:05"))
		upper = "toStartOfHour(now())"
	} else {
		lower = fmt.Sprintf("toDate('%s')", start.Format("2006-01-02"))
		upper = "today()"
	}
	scored := buildVolumeBaselineScoringSQL(source, fidEsc, volumeMinBuckets(def), lower, upper)

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

	hist, metrics, top, err := m.runPreviewQueries(ctx,
		scored, volumeHistBucketExpr, volumeHistLabels, metricsSQL, topSQL)
	if err != nil {
		return err
	}
	res.Histogram = hist
	res.Top = top
	res.TopColumns = []string{"entity_val", "latest_count", "baseline_median", "mad", "n_buckets", "z_score"}
	res.WouldFlag = numToUint64(metrics["would_flag"])
	res.Stats = sanitizeStats(map[string]interface{}{
		"entities_scored": metrics["entities_scored"],
		"max_z":           metrics["max_z"],
		"min_buckets":     volumeMinBuckets(def),
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
func (m *Manager) previewNetwork(ctx context.Context, fractalID string, mt ModelType, def ModelDefinition, window string, days int) (*PreviewResult, error) {
	windowSecs := int64(days) * 86400
	source := m.ch.ReadTable()
	cfg := loadPreviewConfig()

	bp := def.Beacon.WithDefaults(windowSecs)
	lc := def.LongConn.WithDefaults()
	mp := def.Modifiers.WithDefaults()
	threshold := networkScoreThreshold(def, mt)

	// Prevalence context over the same window (all sources, not just qualifying).
	var networkSize uint64
	if err := m.ch.QueryRow(ctx, BuildNetPreviewNetworkSize(def, source, fractalID, days)+cfg.settings()).Scan(&networkSize); err != nil {
		return nil, fmt.Errorf("preview network size: %w", err)
	}
	prevalence := make(map[string]uint64)
	if networkSize > 0 {
		if err := m.ch.StreamQuery(ctx, "", BuildNetPreviewPrevalence(def, source, fractalID, days)+cfg.settings(),
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

	aggSQL, err := BuildNetPreviewAgg(def, mt, source, fractalID, days)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().Unix() - windowSecs
	var scored, scanned int
	var maxScore float64
	var flagged uint64
	var counts [10]uint64
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

	metricLabel := "beacon_score"
	if mt == ModelTypeLongConnection {
		metricLabel = "longconn_score"
	}
	return &PreviewResult{
		ModelType:  mt,
		Window:     window,
		Metric:     metricLabel,
		Histogram:  buckets,
		WouldFlag:  flagged,
		FlagBasis:  fmt.Sprintf("final score >= %g", threshold),
		Top:        topRows,
		TopColumns: []string{"src_ip", "dst_ip", "dst_port", "score", "regularity", "ts_score", "ds_score", "dur_score", "hist_score", "prevalence", "conn_count"},
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
