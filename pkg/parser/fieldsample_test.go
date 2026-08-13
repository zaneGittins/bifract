package parser

import (
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
