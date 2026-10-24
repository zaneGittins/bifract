package alerts

import (
	"context"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
)

// Alerts read logs_hot, which has no norm_log n-gram index, so their queries never
// carry norm_log index hints.
func TestAlertQueriesCarryNoNormLogIndexHints(t *testing.T) {
	from := time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC)
	alert := &Alert{ID: "a", FractalID: "f1", QueryString: `computer_name=~"MAvi01"`}
	opts, err := (&Engine{}).buildQueryOpts(context.Background(), alert, from, from.Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	sql, err := parser.TranslateToSQL(mustParse(t, alert.QueryString), opts)
	if err != nil {
		t.Fatal(err)
	}
	if opts.NormLogIndexHints || strings.Contains(sql, "indexHint(") {
		t.Errorf("alert query hinted:\n%s", sql)
	}
}
