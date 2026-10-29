package parser

import (
	"strings"
	"testing"
	"time"
)

// A row-count command must not lift the caller's output cap: head(n) used to
// replace MaxRows outright, so * | head(999999999) buffered every row.
func TestRowCountCommandsStayWithinMaxRows(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "f",
		MaxRows:   1000,
	}
	for _, q := range []string{
		"* | head(999999999)",
		"* | tail(999999999)",
		"* | limit(999999999)",
		"* | groupby(user) | head(999999999)",
	} {
		sql := mustTranslate(t, q, opts)
		if strings.Contains(sql, "999999999") {
			t.Errorf("%s: row count escaped MaxRows: %s", q, sql)
		}
		if !strings.Contains(sql, "LIMIT 1000") {
			t.Errorf("%s: expected LIMIT 1000: %s", q, sql)
		}
	}

	// A cap that later commands read through is row selection, not output: it
	// keeps its count, and the output limit lands on the outer query.
	sql := mustTranslate(t, `* | head(5000) | user="x"`, opts)
	if !strings.Contains(sql, "LIMIT 5000)") || !strings.HasSuffix(sql, "LIMIT 1000)") {
		t.Errorf("head(5000) | filter lost its selection or output cap: %s", sql)
	}

	// Below the cap the command's own count stands.
	if sql := mustTranslate(t, "* | head(50)", opts); !strings.Contains(sql, "LIMIT 50") {
		t.Errorf("head(50) lost its count: %s", sql)
	}
	// No cap configured: the count is honoured as written.
	opts.MaxRows = 0
	if sql := mustTranslate(t, "* | head(5000)", opts); !strings.Contains(sql, "LIMIT 5000") {
		t.Errorf("uncapped head(5000) lost its count: %s", sql)
	}
}
