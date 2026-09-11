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
