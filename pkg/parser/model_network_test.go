package parser

import (
	"strings"
	"testing"
	"time"
)

// A network model's lookup reads only the pairs its latest completed pass
// scored, so a pair that went quiet stops matching the alert.
func TestModelLookup_NetworkReadsTheCurrentPass(t *testing.T) {
	pass := time.Date(2026, 3, 1, 12, 0, 0, 250_000_000, time.UTC)
	for _, mt := range []string{"beacon", "long_connection"} {
		t.Run(mt, func(t *testing.T) {
			opts := mlookupOpts()
			opts.Models = map[string]AnalyticsModelInfo{
				"net": {ID: "3", TableName: "model_net", ModelType: mt, FractalID: "f1", Keys: []string{"src_ip", "dst_ip", "dst_port"}, ScoredPassAt: &pass},
			}
			pipeline, err := ParseQuery(`* | model_lookup(model="net", key=[src_ip, dst_ip, dst_port])`)
			if err != nil {
				t.Fatal(err)
			}
			res, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Fatal(err)
			}
			const bound = "scored_at >= toDateTime64('2026-03-01 12:00:00.250', 3, 'UTC')"
			if !strings.Contains(res.SQL, "WHERE fractal_id IN ('f1') AND "+bound) {
				t.Errorf("scoring subquery is not bounded to the current pass:\n%s", res.SQL)
			}
			if !strings.Contains(res.SQL, "_mlk_src.fractal_id IN ('f1') AND _mlk_src."+bound) {
				t.Errorf("prefilter is not bounded to the current pass:\n%s", res.SQL)
			}
		})
	}
}

// A model that has never completed a pass has no current pairs.
func TestNetCurrentPredicate(t *testing.T) {
	if got := NetCurrentPredicate(nil, ""); got != "0" {
		t.Errorf("no pass: %q", got)
	}
	pass := time.Date(2026, 3, 1, 12, 0, 0, 0, time.FixedZone("x", 3600))
	if got, want := NetCurrentPredicate(&pass, "x."), "x.scored_at >= toDateTime64('2026-03-01 11:00:00.000', 3, 'UTC')"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// One row per pair, the newest, even when passes landed on different shards.
func TestNetLatestSQL(t *testing.T) {
	got := NetLatestSQL("`t`", "fractal_id = 'f'", []string{"final_score AS beacon_score", "conn_count"})
	want := "SELECT src_ip, dst_ip, dst_port, argMax(_r.final_score, _r.scored_at) AS beacon_score, argMax(_r.conn_count, _r.scored_at) AS conn_count\nFROM (SELECT * FROM `t` WHERE fractal_id = 'f') AS _r\nGROUP BY src_ip, dst_ip, dst_port"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}
