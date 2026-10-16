package parser

import (
	"testing"
	"time"
)

// Translating never changes the parsed query: the handler plans the same
// pipeline for results, histogram and field statistics, and alerts re-plan a
// cached one on every run.
func TestPlanningLeavesThePipelineUnchanged(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   100,
		FractalID: "f1",
	}
	for _, q := range []string{
		`a="1" OR (lower(x)="y" AND b="2")`,
		`cidr(ip, "10.0.0.0/8") OR lower(x)="y"`,
		`* | groupby(user) | (_count > 5 OR lower(user)="root")`,
		`* | case { lower(x)="a" AND b="1" | t := "a"; * | t := "b" } | groupby(t)`,
		`* | frequency(user) | (_percentage > 3 OR lower(_value)="x")`,
		`* | mzscore(duration) | (_modified_z > 3 OR lower(image)="x")`,
		`* | mzscore(duration) | (_modified_z > 3 OR (lower(image)="x" AND len(user) > 2))`,
		`* | frequency(user) | _percentage > 3 | (lower(_value)="x" OR _count > 2)`,
		`* | x := lower(a) | (x="b" OR len(x) > 3)`,
	} {
		t.Run(q, func(t *testing.T) {
			fresh := func() string {
				pl, err := ParseQuery(q)
				if err != nil {
					t.Fatal(err)
				}
				res, err := TranslateToSQLWithOrder(pl, opts)
				if err != nil {
					t.Fatal(err)
				}
				return res.SQL
			}
			want := fresh()
			pl, _ := ParseQuery(q)
			if _, err := BuildHistogramSQL(pl, opts, 60); err != nil {
				t.Fatal(err)
			}
			for i := range 2 {
				res, err := TranslateToSQLWithOrder(pl, opts)
				if err != nil {
					t.Fatal(err)
				}
				if res.SQL != want {
					t.Fatalf("planning %d of a reused pipeline differs:\n%s\nwant:\n%s", i+2, res.SQL, want)
				}
			}
		})
	}
}
