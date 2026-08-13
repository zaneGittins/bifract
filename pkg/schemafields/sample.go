package schemafields

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"bifract/pkg/parser"
)

// Tier B: the per-field distribution, from a bounded sample of recent rows.
//
// The sample is taken per fractal. A global "newest N rows" sample is dominated
// by whichever fractal ingests fastest, so a quiet fractal's fields never appear
// in the tab that decides whether they get a column.

// sampleSize is how many rows per fractal the distribution is measured over.
// Cost is a function of this number alone: the window predicate prunes
// partitions and the LIMIT stops the scan, so a fractal holding a billion rows
// costs the same as one holding a million.
func sampleSize() int {
	if v := os.Getenv("BIFRACT_SCHEMA_INSIGHTS_SAMPLE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 50000
}

// maxSampleWindow caps how far back a fractal's sample may reach. Past this, the
// distribution describes history rather than the schema being ingested now.
const maxSampleWindow = 7 * 24 * time.Hour

// minSampleWindow keeps the predicate from being so tight that ordinary ingest
// jitter empties it.
const minSampleWindow = time.Minute

// windowSafetyFactor oversizes the window derived from the ingest rate, so the
// first attempt usually fills the sample and no widening round is needed.
const windowSafetyFactor = 4

// thinSampleRatio is the fraction of the requested sample below which a window is
// considered too narrow and is widened.
const thinSampleRatio = 4

// maxWidenRounds bounds the widening ladder. Each round costs one cheap count
// probe, not a norm_log read.
const maxWidenRounds = 3

// sampleFractal measures one fractal's field distribution. It returns the
// per-field rows and the exact number of rows sampled, which is the coverage
// denominator.
func (s *Sweeper) sampleFractal(ctx context.Context, fm *fractalMeta) (map[string]*fieldSample, uint64, int, error) {
	anchor := fm.MaxTime
	if anchor.IsZero() || anchor.After(time.Now()) {
		// Parsed timestamps are untrusted user data: a single future-dated row
		// must not push the window past every real one.
		anchor = time.Now()
	}

	want := sampleSize()
	window := derivedWindow(fm.RowsPerSec, want)

	var sampled uint64
	for round := 0; ; round++ {
		n, err := s.probeCount(ctx, fm.ID, anchor, window, want)
		if err != nil {
			return nil, 0, 0, err
		}
		sampled = n
		if n >= uint64(want/thinSampleRatio) || window >= maxSampleWindow || round >= maxWidenRounds {
			break
		}
		window *= 8
		if window > maxSampleWindow {
			window = maxSampleWindow
		}
	}
	if sampled == 0 {
		return nil, 0, 0, nil // no data in range: nothing to describe
	}

	sql := parser.BuildFieldSampleSQL(parser.FieldSampleParams{
		Table:      s.ch.ReadTable(),
		Where:      windowPredicate(fm.ID, anchor, window),
		SampleSize: want,
	})
	rows, err := s.ch.QueryLowPriorityBounded(ctx, sql, sweepMaxMemoryBytes())
	if err != nil {
		return nil, 0, 0, fmt.Errorf("field sample: %w", err)
	}

	// Advisory: a failed capacity probe must not discard the distribution that
	// was just measured, so it degrades to "unknown" rather than to an error.
	maxPaths, err := s.probeDynamicPaths(ctx, fm, anchor, window)
	if err != nil {
		log.Printf("[SchemaSweep] dynamic paths for %s: %v", fm.ID, err)
	}

	out := make(map[string]*fieldSample, len(rows))
	for _, r := range rows {
		name, _ := r["key"].(string)
		if name == "" {
			continue
		}
		out[name] = &fieldSample{
			Present:     asUint64(r["present"]),
			Cardinality: asUint64(r["cardinality"]),
			Top:         parseTopK(r),
		}
	}
	return out, sampled, maxPaths, nil
}

// dynamicPathSample bounds the JSON-structure probe. Dynamic paths are a
// property of the part, not the row, so a few thousand recent rows name the
// current allocation as reliably as a million would; and unlike the flat
// norm_log sample, this one opens the wide JSON column, so it stays small.
const dynamicPathSample = 2000

// probeDynamicPaths reports the most dynamic paths held by any part in the
// window, which is the figure max_dynamic_paths governs.
//
// It cannot be read from part metadata. system.parts_columns lists only
// type-hinted sub-columns; dynamic paths appear in `substreams` but the counts
// there do not agree with the allocation (68 substream entries against 46 actual
// paths on a measured part, since a path contributes several streams).
// JSONDynamicPaths is the documented answer, and type hints are excluded from it
// by definition, which is what makes reserving a field show as freeing a slot.
func (s *Sweeper) probeDynamicPaths(ctx context.Context, fm *fractalMeta, anchor time.Time, window time.Duration) (int, error) {
	sql := fmt.Sprintf(
		"SELECT max(length(JSONDynamicPaths(fields))) AS n FROM (SELECT fields FROM %s WHERE %s LIMIT %d)",
		s.ch.ReadTable(), windowPredicate(fm.ID, anchor, window), dynamicPathSample)
	rows, err := s.ch.QueryLowPriorityBounded(ctx, sql, sweepMaxMemoryBytes())
	if err != nil {
		return 0, fmt.Errorf("dynamic path probe: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return int(asUint64(rows[0]["n"])), nil
}

// fieldSample is one field's measured distribution within one fractal's sample.
type fieldSample struct {
	Present     uint64
	Cardinality uint64
	Top         []TopValue
}

// derivedWindow sizes the sample window from the fractal's measured ingest rate,
// so a firehose reads the last few minutes and a trickle reads the last few days,
// and both land near the requested row count on the first attempt.
func derivedWindow(rowsPerSec float64, want int) time.Duration {
	if rowsPerSec <= 0 {
		return maxSampleWindow
	}
	secs := float64(want) / rowsPerSec * windowSafetyFactor
	w := time.Duration(secs * float64(time.Second))
	if w < minSampleWindow {
		return minSampleWindow
	}
	if w > maxSampleWindow {
		return maxSampleWindow
	}
	return w
}

// windowPredicate scopes a scan to one fractal's trailing window. Both halves
// prune: fractal_id and toDate(timestamp) are the partition key, so only the
// relevant partitions are opened, and the caller's LIMIT stops the read there.
func windowPredicate(fractalID string, anchor time.Time, window time.Duration) string {
	from := anchor.Add(-window).UTC()
	return fmt.Sprintf("fractal_id = %s AND timestamp >= toDateTime64('%s', 3, 'UTC')",
		quoteCH(fractalID), from.Format("2006-01-02 15:04:05.000"))
}

// probeCount reports how many rows the window would contribute, without reading
// norm_log, so widening a too-narrow window costs almost nothing.
func (s *Sweeper) probeCount(ctx context.Context, fractalID string, anchor time.Time, window time.Duration, limit int) (uint64, error) {
	sql := fmt.Sprintf("SELECT count() AS n FROM (SELECT 1 FROM %s WHERE %s LIMIT %d)",
		s.ch.ReadTable(), windowPredicate(fractalID, anchor, window), limit)
	rows, err := s.ch.QueryLowPriorityBounded(ctx, sql, sweepMaxMemoryBytes())
	if err != nil {
		return 0, fmt.Errorf("sample probe: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return asUint64(rows[0]["n"]), nil
}

// parseTopK reads the parallel top-value arrays into the display shape. The
// error term is kept as a flag rather than a number: it only needs to answer
// whether the count can be shown as fact, and it is zero for exactly the
// low-cardinality fields whose value distribution is worth showing.
func parseTopK(row map[string]interface{}) []TopValue {
	vals, _ := row["top_values"].([]string)
	counts, _ := row["top_counts"].([]uint64)
	errs, _ := row["top_errors"].([]uint64)

	out := make([]TopValue, 0, len(vals))
	for i, v := range vals {
		if v == "" {
			continue
		}
		tv := TopValue{Value: v}
		if i < len(counts) {
			tv.Count = counts[i]
		}
		if i < len(errs) && errs[i] > 0 {
			tv.Approx = true
		}
		out = append(out, tv)
	}
	return out
}
