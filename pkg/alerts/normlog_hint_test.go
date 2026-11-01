package alerts

import (
	"context"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// logs_hot has no norm_log n-gram index, so only catch-up reads routed to logs
// carry norm_log index hints.
func TestAlertNormLogIndexHintsFollowTableRouting(t *testing.T) {
	now := time.Now()
	cases := map[string]struct {
		from     time.Time
		lookback int
		table    string
		hint     bool
	}{
		"hot":      {now.Add(-time.Minute), 0, "logs_hot", false},
		"catch-up": {now.Add(-3 * time.Hour), 0, "logs", true},
		"lookback": {now.Add(-time.Minute), 3 * 3600, "logs", true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			alert := &Alert{ID: "a", FractalID: "f1", QueryString: `computer_name=~"MAvi01"`, LookbackSeconds: c.lookback}
			e := &Engine{ch: &storage.ClickHouseClient{}}
			opts, err := e.buildQueryOpts(context.Background(), alert, c.from, now, nil)
			if err != nil {
				t.Fatal(err)
			}
			if opts.TableName != c.table {
				t.Fatalf("table %q, want %q", opts.TableName, c.table)
			}
			sql, err := parser.TranslateToSQL(mustParse(t, alert.QueryString), opts)
			if err != nil {
				t.Fatal(err)
			}
			hinted := strings.Contains(sql, "indexHint(")
			if opts.NormLogIndexHints != c.hint || hinted != c.hint {
				t.Errorf("hint=%v hinted=%v, want %v:\n%s", opts.NormLogIndexHints, hinted, c.hint, sql)
			}
		})
	}
}
