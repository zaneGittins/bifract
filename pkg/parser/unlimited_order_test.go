package parser

import (
	"strings"
	"testing"
	"time"
)

// An unlimited read must not re-sort on the formatter's String timestamp alias,
// which buffers every row; a limited one keeps the mirrored ORDER BY.
func TestFormatterOrderMirroredOnlyWithLimit(t *testing.T) {
	pipeline, err := ParseQuery(`user="bob"`)
	if err != nil {
		t.Fatal(err)
	}
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "f",
		TableName: "logs",
	}
	for _, tc := range []struct {
		maxRows int
		orders  int
	}{{0, 1}, {200, 2}} {
		opts.MaxRows = tc.maxRows
		res, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Count(res.SQL, "ORDER BY"); got != tc.orders {
			t.Errorf("MaxRows=%d: %d ORDER BY clauses, want %d\n%s", tc.maxRows, got, tc.orders, res.SQL)
		}
	}
}

// Literals carry no zone, so a time with an offset must render as UTC.
func TestTimeLiteralIsUTC(t *testing.T) {
	plus2 := time.FixedZone("plus2", 2*3600)
	if got, want := chTimeLiteral(time.Date(2026, 9, 9, 3, 0, 0, 0, plus2)), "2026-09-09 01:00:00.000"; got != want {
		t.Errorf("chTimeLiteral = %q, want %q", got, want)
	}
}
