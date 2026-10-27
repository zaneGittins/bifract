package parser

import (
	"fmt"
	"strings"
	"testing"
)

// The schema sweep's cost model depends entirely on the shape of this query, and
// every property below has already been regressed once in this codebase. They are
// asserted here so a later edit that looks harmless fails the build instead of
// quietly making the schema tab scan the retention window again.
func TestBuildFieldSampleSQL_BoundedByTheSample(t *testing.T) {
	sql := BuildFieldSampleSQL(FieldSampleParams{
		Table:      "logs",
		Where:      "fractal_id = 'abc' AND timestamp >= toDateTime64('2026-01-01 00:00:00.000', 3, 'UTC')",
		SampleSize: 1234,
	})

	// The row scan must not sort. On a table partitioned by (fractal_id, date),
	// an ordered LIMIT does not read in order: cost scales with the rows in the
	// window rather than with the limit, which is what made the tab time out.
	// (The outer ORDER BY is over the aggregated field list, a few hundred rows.)
	scan := ""
	for _, line := range strings.Split(sql, "\n") {
		if strings.Contains(line, "SELECT norm_log FROM") {
			scan = line
		}
	}
	if scan == "" {
		t.Fatalf("no row scan found:\n%s", sql)
	}
	if strings.Contains(strings.ToUpper(scan), "ORDER BY") {
		t.Errorf("row scan must not sort; recency comes from the predicate: %s", scan)
	}

	// The predicate must reach the inner scan, or nothing prunes partitions.
	if !strings.Contains(sql, "fractal_id = 'abc'") {
		t.Errorf("predicate missing from the scan:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT 1234") {
		t.Errorf("sample size missing from the scan:\n%s", sql)
	}

	// Empty values are excluded: the typed sub-columns serialize as "" on every
	// row, so counting raw keys would report every field as 100% present.
	if !strings.Contains(sql, "kv.2 != ''") {
		t.Errorf("empty values must be excluded:\n%s", sql)
	}

	// Bounded-state aggregation, not a GROUP BY over every distinct value.
	for _, want := range []string{"uniq(kv.2)", "approx_top_k("} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing bounded aggregate %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "row_number()") {
		t.Errorf("window function reintroduced; top values come from approx_top_k:\n%s", sql)
	}
}

func TestBuildFieldSampleSQL_Defaults(t *testing.T) {
	sql := BuildFieldSampleSQL(FieldSampleParams{Table: "logs_distributed"})
	if !strings.Contains(sql, "FROM logs_distributed") {
		t.Errorf("table missing:\n%s", sql)
	}
	// An empty predicate must still be valid SQL rather than a dangling WHERE.
	if !strings.Contains(sql, "WHERE 1 = 1") {
		t.Errorf("empty predicate must degrade to a true clause:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT 50000") {
		t.Errorf("default sample size missing:\n%s", sql)
	}
}

// The sweep counts a stratified sample's rows with this scan, so each stratum
// must be an unsorted, pruned LIMIT of the per-stratum share.
func TestBuildSampleScanSQL_Strata(t *testing.T) {
	sql := BuildSampleScanSQL("logs", "1", "fractal_id = 'abc'", []string{"a < 1", "a >= 1 AND a < 2", "a >= 2"}, 100)
	scans := strings.Split(sql, "UNION ALL")
	if len(scans) != 3 {
		t.Fatalf("want one scan per stratum:\n%s", sql)
	}
	for i, s := range scans {
		if strings.Contains(strings.ToUpper(s), "ORDER BY") {
			t.Errorf("scan %d must not sort: %s", i, s)
		}
		if !strings.Contains(s, "fractal_id = 'abc'") || !strings.Contains(s, "LIMIT 100") {
			t.Errorf("scan %d lost the predicate or share: %s", i, s)
		}
	}
	if !strings.Contains(scans[1], "AND (a >= 1 AND a < 2)") {
		t.Errorf("stratum predicate must be parenthesised into its scan: %s", scans[1])
	}
	if got := BuildSampleScanSQL("logs", "1", "w = 1", nil, 7); strings.TrimSpace(got) != "SELECT 1 FROM logs WHERE w = 1 LIMIT 7" {
		t.Errorf("no strata must be a single scan: %s", got)
	}
}

// Slices are merged in Go, which needs value hashes to count distinct values
// across them; the hashes must stay capped so a high-cardinality field costs
// FieldSampleDistinctCap values, not one per row.
func TestBuildFieldSampleSQL_CappedDistinctHashes(t *testing.T) {
	sql := BuildFieldSampleSQL(FieldSampleParams{Table: "logs", Where: "w = 1"})
	if !strings.Contains(sql, fmt.Sprintf("groupUniqArray(%d)(cityHash64(kv.2))", FieldSampleDistinctCap)) {
		t.Errorf("missing capped distinct hashes:\n%s", sql)
	}
}
