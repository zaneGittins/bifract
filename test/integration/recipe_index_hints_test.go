//go:build integration

// Field and norm_log filters carry a hint that lets ClickHouse skip blocks whose
// full-text index proves the value absent; a wrongly skipped block is the only way
// it could lose a row. Each case value lives in one field of its own rows, so such
// a skip shows as a missing row against both the expected count and an unhinted
// search.
//
//	go test -tags integration ./test/integration/ -run TestIndexHintsKeepEveryMatch -v

package integration

import (
	"reflect"
	"testing"
	"time"
)

func TestIndexHintsKeepEveryMatch(t *testing.T) {
	t.Parallel()
	c := New(t)

	fractalID, scoped := newFractal(t, c, "hints")
	shipper := newShipper(t, scoped, fractalID)

	// The fractal is this test's own, so its scope needs no marker field, which
	// would itself be hinted.
	end := time.Now().UTC().Truncate(time.Millisecond)
	body := func(q string) map[string]any {
		return map[string]any{
			"query": q, "fractal_id": fractalID,
			"start": end.Add(-time.Hour).Format(time.RFC3339Nano), "end": end.Format(time.RFC3339Nano),
		}
	}
	sent := 0
	ship := func(logs []map[string]any) {
		for i := range logs {
			logs[i]["timestamp"] = end.Add(-time.Duration(sent+i+1) * time.Minute).Format(time.RFC3339Nano)
		}
		shipper.DoRaw(t, "POST", "/ingest", logs, nil)
		sent += len(logs)
		Eventually(t, "the ingested logs to become queryable", 120*time.Second, func() bool {
			var res struct {
				Results []struct {
					Count int `json:"_count"`
				} `json:"results"`
			}
			c.DoRaw(t, "POST", "/query", body("* | count()"), &res)
			return len(res.Results) == 1 && res.Results[0].Count == sent
		})
	}
	ship([]map[string]any{
		{"computer_name": "HOST-QaZ1MiX"},
		{"computer_name": "qaz1mix"},
		{"user": "alice.QB2@corp"},
		{"tags": []any{"x-QC3y", "z"}},
		{"proc": map[string]any{"name": "QD4.exe", "pid": 4}},
		{"user": "Zoë-QF6x"},
		{"commandline": `"C:\QG7\cmd.exe" /c "a/b"`},
		{"build": "v1.2-QH8", "port": 8080, "ratio": 1.5},
		{"image": "QI9tool.exe"},
		{"image": "tool.QJ10"},
		{"user": "QK11a"},
		{"user": "QK11b"},
		{"computer_name": "QM13Host"},
		{"user": "QN14", "kind": "neg"},
		{"flag": true, "user": "QO15"},
		{"user": "日本語-QP16"},
		{"commandline": "powershell -nop -w hidden -QR17x"},
		{"image": `C:\Tools\QS18\run.exe`},
		{"note": `say "QT19" now`},
		{"answers": "209.146.77.20"},
		{"image": "QU20-Invoke-Mimikatz.ps1"},
		{"user": "kQV21"},
	})
	// A second insert lands in a block of its own. Its rows match through an OR
	// branch, a negation, or case folding lower() does not reproduce, so a hint
	// pruning there as if its text were required would drop them.
	ship([]map[string]any{
		{"user": "svc_QL12:1"},
		{"user": "other", "kind": "neg"},
		// (?i)k also matches the Kelvin sign and (?i)s the long s.
		{"user": "\u212AQV21"},
		{"user": "\u017FQW22x"},
	})

	for _, tc := range []struct {
		filter string
		want   int
	}{
		{`computer_name=~"qaz1mix"`, 2},
		{`user=~"qb2@corp"`, 1},
		{`tags=~"qc3y"`, 1},
		{`name="QD4.exe"`, 1}, // the JSON normalizer flattens proc.name to name
		{`user=~"qf6x"`, 1},
		{`commandline=~"qg7"`, 1},
		{`build=~"qh8"`, 1},
		{`image=^"qi9tool"`, 1},
		{`image=$"tool.qj10"`, 1},
		{`user="QK11a","QK11b"`, 2},
		{`user="svc_QL12:1"`, 1},
		{`computer_name="QM13Host"`, 1},
		{`computer_name="qm13host"`, 0},
		{`computer_name=~"qaz1mix" OR user="svc_QL12:1"`, 3},
		{`kind="neg" AND !(user="QN14")`, 1},
		{`flag="true" AND user=~"qo15"`, 1},
		// Values with characters JSON escapes are hinted by their other runs.
		{`commandline=~"-nop -w hidden -qr17x"`, 1},
		{`image=~"\\qs18\\run"`, 1},
		{`note=~"\"qt19\" now"`, 1},
		{`answers="209.146.77.20"`, 1},
		// Regexes are hinted by the literals every match contains.
		{`image=/qu20-invoke-mimikatz\.ps1/i`, 1},
		{`image=/QU20-(Invoke)+-Mimikatz/`, 1},
		{`user=/kqv21/i`, 2},
		{`user=/sqw22x/i`, 1},
		// norm_log itself, as Sigma keyword rules search it.
		{`norm_log=~"qaz1mix"`, 2},
		{`norm_log=~"qk11a","qk11b"`, 2},
		{`norm_log=~"\\qg7\\cmd"`, 1},
		{`norm_log=~"/c \"a/b\""`, 1},
		{`norm_log=~"zoë-qf6x"`, 1}, // ë has other cases, so this one goes unhinted
		{`norm_log=~"日本語-qp16"`, 1},
		{`norm_log=~"qaz1mix" OR norm_log=~"svc_ql12"`, 3},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			var hinted, plain struct {
				Success bool             `json:"success"`
				Results []map[string]any `json:"results"`
			}
			c.DoRaw(t, "POST", "/query", body(tc.filter), &hinted)
			// Filters after a row cap get no hint; the cap covers every row.
			c.DoRaw(t, "POST", "/query", body("* | head(1000000) | "+tc.filter), &plain)
			if !hinted.Success || !plain.Success {
				t.Fatalf("search failed: hinted %v, plain %v", hinted.Success, plain.Success)
			}
			// The capped search lists oldest first, so compare by log_id.
			byID := func(rows []map[string]any) map[any]any {
				m := make(map[any]any, len(rows))
				for _, r := range rows {
					m[r["log_id"]] = r["norm_log"]
				}
				return m
			}
			h, p := byID(hinted.Results), byID(plain.Results)
			if len(h) != tc.want || len(hinted.Results) != tc.want {
				t.Errorf("returned %d rows (%d distinct), want %d", len(hinted.Results), len(h), tc.want)
			}
			if !reflect.DeepEqual(h, p) {
				t.Errorf("rows differ from the unhinted search\nhinted: %v\nplain:  %v", h, p)
			}
		})
	}
}
