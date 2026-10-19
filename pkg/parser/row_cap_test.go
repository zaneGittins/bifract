package parser

import (
	"strings"
	"testing"
)

// A head/tail/limit/dedup caps the rows; whatever follows it reads only those.
func TestWorkAfterARowCapReadsTheCappedRows(t *testing.T) {
	cases := []struct {
		name, query string
		want        []string
		notWant     []string
	}{
		{"filter after head", `a=1 | head(5) | b=2`,
			[]string{"FROM (SELECT * FROM logs WHERE timestamp >= '2026-01-01 00:00:00.000' AND timestamp <= '2026-01-02 00:00:00.000' AND fractal_id = 'f1' AND fields.`a`::String = '1' ORDER BY timestamp ASC LIMIT 5) WHERE fields.`b`::String = '2'"},
			[]string{"AND fields.`b`::String = '2' ORDER BY timestamp ASC LIMIT 5"}},
		{"filter after head keeps head's order", `* | head(5) | b=2`,
			[]string{"WHERE fields.`b`::String = '2' ORDER BY timestamp ASC"}, nil},
		{"filter after tail", `* | tail(5) | b=2`,
			[]string{"ORDER BY timestamp DESC LIMIT 5) WHERE fields.`b`::String = '2'"}, nil},
		{"filter after sort and limit", `* | sort(x) | limit(10) | b=2`,
			[]string{"ORDER BY fields.`x`::String ASC LIMIT 10) WHERE fields.`b`::String = '2'"}, nil},
		{"filter after dedup", `* | dedup(user) | b=2`,
			[]string{"ORDER BY timestamp DESC, log_id DESC LIMIT 1 BY fields.`user`::String) WHERE fields.`b`::String = '2'"}, nil},
		{"filter command after head", `* | head(5) | in(b, values=["2"])`,
			[]string{"LIMIT 5) WHERE fields.`b`::String IN ('2')"}, nil},
		{"aggregation after head", `* | head(5) | groupby(a)`,
			[]string{"FROM (SELECT * FROM logs WHERE timestamp >= '2026-01-01 00:00:00.000' AND timestamp <= '2026-01-02 00:00:00.000' AND fractal_id = 'f1' ORDER BY timestamp ASC LIMIT 5) GROUP BY a"},
			[]string{"GROUP BY a ORDER BY timestamp ASC LIMIT 5"}},
		{"count after limit", `* | limit(3) | count()`,
			[]string{"ORDER BY timestamp DESC, log_id DESC LIMIT 3)"}, nil},
		{"timechart after head charts the first rows", `* | head(5) | timechart(span=1m)`,
			[]string{"ORDER BY timestamp ASC LIMIT 5) GROUP BY time_bucket"}, nil},
		{"sort after head sorts the capped rows", `* | head(5) | sort(x)`,
			[]string{"ORDER BY timestamp ASC LIMIT 5) ORDER BY fields.`x`::String ASC"}, nil},
		{"caps nest", `* | head(10) | b=2 | head(3) | c=1`,
			[]string{"FROM (SELECT * FROM (SELECT * FROM logs WHERE timestamp >= '2026-01-01 00:00:00.000' AND timestamp <= '2026-01-02 00:00:00.000' AND fractal_id = 'f1' ORDER BY timestamp ASC LIMIT 10) WHERE fields.`b`::String = '2' ORDER BY timestamp ASC LIMIT 3) WHERE fields.`c`::String = '1'"}, nil},
		{"transform after head keeps one scan", `* | head(5) | lowercase(a)`,
			[]string{"FROM logs WHERE", "ORDER BY timestamp ASC LIMIT 5"}, []string{"SELECT * FROM"}},
		{"filter before head stays in the scan", `* | b=2 | head(5)`,
			[]string{"AND fields.`b`::String = '2' ORDER BY timestamp ASC LIMIT 5"}, []string{"SELECT * FROM"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := positionSQL(t, tc.query)
			for _, w := range tc.want {
				if !strings.Contains(sql, w) {
					t.Errorf("missing %q:\n%s", w, sql)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(sql, w) {
					t.Errorf("must not contain %q:\n%s", w, sql)
				}
			}
		})
	}
}
