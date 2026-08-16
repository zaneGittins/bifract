package query

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/settings"
)

// fieldStatsSampleSize bounds how many (most-recent) matching rows the field-stats
// aggregation scans. It is a hard ceiling on cost: ClickHouse stops the scan once
// the inner LIMIT is satisfied, so the query stays cheap no matter how many rows
// match. Overridable via BIFRACT_FIELDSTATS_SAMPLE.
func fieldStatsSampleSize() int {
	if v := os.Getenv("BIFRACT_FIELDSTATS_SAMPLE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 50000
}

// fieldStatValue is a single top value with its occurrence count within the sample.
type fieldStatValue struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

// fieldStat is the per-field distribution over the sampled events.
type fieldStat struct {
	Name        string           `json:"name"`
	Present     uint64           `json:"present"`     // non-null occurrences in the sample
	Cardinality uint64           `json:"cardinality"` // distinct values in the sample (exact within sample)
	Top         []fieldStatValue `json:"top"`         // top values by frequency, count-desc
}

type fieldStatsResponse struct {
	Success     bool        `json:"success"`
	SampleSize  uint64      `json:"sample_size"` // rows actually scanned (coverage denominator)
	Approximate bool        `json:"approximate"` // true when the sample cap was hit (more rows match)
	Supported   bool        `json:"supported"`   // false for source-command queries (pgr() etc.)
	Fields      []fieldStat `json:"fields"`
}

// HandleFieldStats computes server-side field statistics for a BQL query's matched
// events: per-field coverage, cardinality, and top values, over a bounded sample.
//
// It reuses prepareQuery for auth, fractal/prism resolution, @variable substitution,
// BQL parsing, and comment() resolution, then builds a separate sampled aggregation
// from the query's WHERE portion only (via parser.BuildFieldStatsSQL). The main
// search is never touched, so opening the Fields rail cannot slow a query. The
// aggregation runs at low ClickHouse priority so it yields to interactive searches
// and ingestion.
func (h *QueryHandler) HandleFieldStats(w http.ResponseWriter, r *http.Request) {
	prep := h.prepareQuery(w, r)
	if prep == nil {
		return // prepareQuery already wrote the response (auth/parse/empty-prism)
	}

	w.Header().Set("Content-Type", "application/json")

	sampleSize := fieldStatsSampleSize()
	statsSQL, err := parser.BuildFieldStatsSQL(prep.pipeline, prep.translationOpts, parser.FieldStatsParams{
		SampleSize: sampleSize,
	})
	if err != nil {
		json.NewEncoder(w).Encode(fieldStatsResponse{Success: false, Supported: true})
		return
	}
	if statsSQL == "" {
		// Source-command composition (pgr() etc.): no norm_log column to aggregate.
		json.NewEncoder(w).Encode(fieldStatsResponse{Success: true, Supported: false, Fields: []fieldStat{}})
		return
	}

	ctx := r.Context()
	if timeoutSec := settings.Get().QueryTimeoutSeconds; timeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
		defer cancel()
	}

	rows, err := h.db.QueryLowPriority(ctx, statsSQL)
	if err != nil {
		json.NewEncoder(w).Encode(fieldStatsResponse{Success: false, Supported: true})
		return
	}

	resp := fieldStatsResponse{Success: true, Supported: true, Fields: make([]fieldStat, 0, len(rows))}
	for _, row := range rows {
		name, _ := row["key"].(string)
		present := asUint64(row["present"])

		if name == "__rows__" {
			// Sentinel row: one entry per sampled row -> exact sample size.
			resp.SampleSize = present
			continue
		}

		fs := fieldStat{
			Name:        name,
			Present:     present,
			Cardinality: asUint64(row["cardinality"]),
		}
		vals, _ := row["top_values"].([]string)
		counts, _ := row["top_counts"].([]uint64)
		for i, v := range vals {
			var c uint64
			if i < len(counts) {
				c = counts[i]
			}
			fs.Top = append(fs.Top, fieldStatValue{Value: v, Count: c})
		}
		// groupArray order is not guaranteed; present top values count-desc.
		sort.SliceStable(fs.Top, func(a, b int) bool { return fs.Top[a].Count > fs.Top[b].Count })
		resp.Fields = append(resp.Fields, fs)
	}

	// The sample cap being reached means more rows match than we scanned, so the
	// distribution is an approximation of the full matched set.
	resp.Approximate = resp.SampleSize >= uint64(sampleSize)

	json.NewEncoder(w).Encode(resp)
}

func asUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case int64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case float64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	}
	return 0
}
