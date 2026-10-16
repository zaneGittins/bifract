package parser

import (
	"strings"
	"testing"
	"time"
)

// Alert-configured field names (throttle field, {{name}} template) are stored
// configuration, not query text, so the lexer never sees them. One carrying a
// backtick closed the projection alias and appended a select expression of the
// alert author's choosing, which the engine then ran on its timer.
func TestAlertExtraFieldInjection(t *testing.T) {
	p, err := ParseQuery(`test_case=citest`)
	if err != nil {
		t.Fatal(err)
	}
	opts := QueryOptions{
		StartTime:          time.Now().Add(-time.Hour),
		EndTime:            time.Now(),
		MaxRows:            10,
		FractalID:          "f1",
		UseIngestTimestamp: true, // the alert path that triggers minimal projection
		AlertExtraFields:   []string{"img` , (SELECT 1) AS pwn, fields.`image"},
	}
	sql, err := TranslateToSQL(p, opts)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	// The payload must not survive as SQL structure: no bare pwn alias, and the
	// hostile name must not be projected at all.
	if strings.Contains(sql, "AS pwn") || strings.Contains(sql, ", pwn") {
		t.Errorf("INJECTED:\n%s", sql)
	}
	t.Logf("SQL: %s", sql)
}

// The alert projection lists its fields in one order every time: the same query
// must translate to the same text, or anything keyed on it (query caches,
// comparisons of a rule's SQL) sees a change that is not one.
func TestAlertProjectionIsDeterministic(t *testing.T) {
	opts := QueryOptions{
		StartTime:          time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		EndTime:            time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:            10,
		FractalID:          "f1",
		UseIngestTimestamp: true,
		AlertExtraFields:   []string{"user", "host"},
	}
	var first string
	for i := 0; i < 30; i++ {
		p, err := ParseQuery(`image=^a AND command_line=~b AND parent=x`)
		if err != nil {
			t.Fatal(err)
		}
		sql, err := TranslateToSQL(p, opts)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = sql
		} else if sql != first {
			t.Fatalf("translation changed between runs:\n%s\n%s", first, sql)
		}
	}
}
