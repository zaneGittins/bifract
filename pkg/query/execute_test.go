package query

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ExecuteBQL is the dashboard and notebook path. These are its refusals, all of
// which must happen before any SQL runs, and the empty-prism answer, which must
// carry non-nil collections so a widget renders "no data" rather than breaking.
func TestExecuteBQLRefusals(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	scope := ExecuteScope{FractalID: "f1", Start: start, End: start.Add(time.Hour)}
	cases := []struct {
		name  string
		query string
		scope ExecuteScope
		want  string
	}{
		{"empty", "  ", scope, "query is empty"},
		{"too long", "a=" + strings.Repeat("b", 10000), scope, "query too long"},
		{"parse error", "level=error | eval(x:=)", scope, "parse error"},
		{"translate error", "level=error | nosuchcommand()", scope, "query error: unsupported command"},
		{"tlsh() arguments", "tlsh(field=hash)", scope, "tlsh(): needs hash="},
		{"tlsh() with no model store", `tlsh(field=hash, hash="T1` + strings.Repeat("A", 70) + `")`, scope, "analytics models are not available"},
		{"pgr() placement", `level=error | pgr(start="g1")`, scope, "must be the first command"},
		{"pgr() without scope", `pgr(start="g1")`, ExecuteScope{Start: start, End: start.Add(time.Hour)}, "fractal scoping is required"},
	}
	h := newTestQueryHandler(1000)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.ExecuteBQL(context.Background(), tc.query, tc.scope)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %+v, %v; want an error containing %q", res, err, tc.want)
			}
		})
	}
}

func TestExecuteBQLEmptyPrism(t *testing.T) {
	res, err := newTestQueryHandler(1000).ExecuteBQL(context.Background(), "level=error | groupby(host)", ExecuteScope{PrismID: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Results == nil || res.FieldOrder == nil || res.ChartConfig == nil || res.Count != 0 {
		t.Errorf("empty prism result = %+v; collections must be empty, not nil", res)
	}
}
