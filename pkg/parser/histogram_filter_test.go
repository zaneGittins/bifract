package parser

import (
	"strings"
	"testing"
	"time"
)

// The histogram counts the events the results table shows, so it filters exactly
// as the query does: every operand, wherever the condition is written. A dropped
// operand narrows the bars below the table; a refused one hides them.
func TestHistogramFiltersLikeTheQuery(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "f1",
	}
	cases := []struct {
		query string
		want  []string
	}{
		{`a="x" OR cidr(dst_ip,"10.0.0.0/8")`, []string{"fields.`a`::String = 'x' OR ", "isIPAddressInRange"}},
		{`* | a="x" OR cidr(dst_ip,"10.0.0.0/8")`, []string{"fields.`a`::String = 'x' OR ", "isIPAddressInRange"}},
		{`* | (a="x" OR !cidr(dst_ip,"10.0.0.0/8"))`, []string{"fields.`a`::String = 'x' OR NOT (", "isIPAddressInRange"}},
		{`* | x := lower(a) | x = "b"`, []string{"lower(fields.`a`::String) = 'b'"}},
		{`* | level="a" | lowercase(level)`, []string{"fields.`level`::String = 'a'"}},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			pl, err := ParseQuery(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			sql, err := BuildHistogramSQL(pl, opts, 60)
			if err != nil {
				t.Fatalf("histogram: %v", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(sql, w) {
					t.Errorf("histogram missing %q:\n%s", w, sql)
				}
			}
			pl, _ = ParseQuery(tc.query)
			if HistogramIsScopeOnly(pl, opts) {
				t.Error("a filtered query was answered from the unfiltered rollup")
			}
		})
	}
}

// A binding read more than once is materialised in a WITH clause the filter
// refers to; the histogram and field statistics carry it, or their SQL names a
// table that does not exist.
func TestScanFilterCarriesBindingCTEs(t *testing.T) {
	q := `let &a := * | groupby(image) | table(image); * | in(image, &a) | in(parent_image, &a)`
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "f1",
	}
	pl, err := ParseQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := BuildHistogramSQL(pl, opts, 60)
	if err != nil {
		t.Fatal(err)
	}
	pl, _ = ParseQuery(q)
	stats, err := BuildFieldStatsSQL(pl, opts, FieldStatsParams{})
	if err != nil {
		t.Fatal(err)
	}
	for name, sql := range map[string]string{"histogram": hist, "field stats": stats} {
		if !strings.Contains(sql, "FROM _b_a") || !strings.HasPrefix(sql, "WITH _b_a AS (") {
			t.Errorf("%s reads _b_a without defining it:\n%s", name, sql)
		}
	}
}
