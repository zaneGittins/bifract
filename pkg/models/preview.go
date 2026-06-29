package models

import (
	"context"
	"fmt"
	"math"
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

	// "New" entities are those first observed in the last 24h of the window -- the
	// ones the is_new alert would fire on. For a 1d window the whole window is
	// "recent", which is correct: a fresh backfill has no prior history.
	recent := end.Add(-24 * time.Hour).Format("2006-01-02 15:04:05")
	res.Metric = "event_count"

	metricsSQL := fmt.Sprintf(`SELECT
    toUInt64(count()) AS entities,
    toUInt64(countIf(first_seen >= toDateTime64('%s', 3))) AS new_recent,
    min(first_seen) AS oldest_seen,
    max(last_seen) AS newest_seen
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
		res.FlagBasis = "entities first seen in the last 24h"
	} else {
		res.WouldFlag = numToUint64(metrics["entities"])
		res.FlagBasis = "every matching entity"
	}
	res.Stats = sanitizeStats(map[string]interface{}{
		"entities":    metrics["entities"],
		"new_recent":  metrics["new_recent"],
		"oldest_seen": metrics["oldest_seen"],
		"newest_seen": metrics["newest_seen"],
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

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
