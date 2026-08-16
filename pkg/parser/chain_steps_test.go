package parser

import (
	"strings"
	"testing"
	"time"
)

// Condition commands must contribute their predicate to the step that names them,
// not to the whole query.
func TestChainStepConditionCommands(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{
			"cidr carries its IP validity guard",
			`* | chain(process_guid) { !cidr(dst_ip,"10.0.0.0/8"); cidr(dst_ip,"10.0.0.0/8") }`,
			[]string{"isIPAddressInRange", "isIPv4String", "isIPv6String"},
		},
		{
			"in expands to an IN list",
			`* | chain(user) { in(event_id,["4624","4625"]); image="cmd.exe" }`,
			[]string{"IN ('4624', '4625')"},
		},
		{
			"commands mix with filters in one step",
			`* | chain(user) { a="x" | cidr(src_ip,"192.168.0.0/16"); c="z" }`,
			[]string{"fields.`a`::String = 'x'", "isIPAddressInRange"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := ParseQuery(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(res.SQL, want) {
					t.Errorf("SQL missing %q:\n%s", want, res.SQL)
				}
			}
		})
	}
}

// A chain step is a row predicate. Anything that reshapes the result, projects a
// column, or aggregates cannot be scoped to one step and must be rejected loudly
// rather than silently applied to the whole query.
func TestChainStepRejectsNonPredicates(t *testing.T) {
	cases := []struct{ name, query, wantErr string }{
		{"groupby", `* | chain(user) { groupby(x); b="y" }`, "only row conditions"},
		{"regex projection", `* | chain(user) { regex("(?P<n>x)", field=image); b="y" }`, "only row conditions"},
		{"sort", `* | chain(user) { sort(x); b="y" }`, "only row conditions"},
		{"nested chain", `* | chain(user) { chain(a) { b="1"; c="2" }; d="y" }`, "chain"},
		{"match enrichment", `* | chain(user) { match(list, field=image, strict=true); b="y" }`, "match()"},
		{"unknown command", `* | chain(user) { nosuchfn(x); b="y" }`, "unsupported command"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := ParseQuery(tc.query)
			if err != nil {
				return // rejected at parse time is also acceptable
			}
			_, err = TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
			if err == nil {
				t.Fatalf("expected rejection for %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// Pipes inside a step stay conjunctive, and OR keeps its own precedence.
func TestChainStepPipeSemanticsPreserved(t *testing.T) {
	pl, err := ParseQuery(`* | chain(user) { event_id="4624" | logon_type="2" OR logon_type="10"; image="cmd.exe" }`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.SQL, "OR") {
		t.Errorf("OR lost from step: %s", res.SQL)
	}
	if !strings.Contains(res.SQL, "fields.`event_id`::String = '4624' AND") {
		t.Errorf("pipe must stay conjunctive: %s", res.SQL)
	}
}

// A condition function is a boolean operand, so OR between two of them must be
// honoured. Silently treating it as AND would match nothing, since the ranges are
// mutually exclusive.
func TestChainStepOrAcrossCommands(t *testing.T) {
	pl, err := ParseQuery(`* | chain(user) { cidr(dst_ip,"10.0.0.0/8") OR cidr(dst_ip,"192.168.0.0/16"); b="y" }`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
	if err != nil {
		t.Fatalf("OR across condition functions must be supported: %v", err)
	}
	if !strings.Contains(res.SQL, "'10.0.0.0/8') AND (isIPv4String(fields.`dst_ip`::String) OR isIPv6String(fields.`dst_ip`::String))) OR (isIPAddressInRange") {
		t.Errorf("the two ranges must be ORed, not ANDed: %s", res.SQL)
	}

	// OR between plain field conditions keeps working alongside a function.
	pl2, err := ParseQuery(`* | chain(user) { a="x" OR b="y" | cidr(src_ip,"10.0.0.0/8"); c="z" }`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TranslateToSQLWithOrder(pl2, QueryOptions{FractalID: "f"}); err != nil {
		t.Errorf("OR between field conditions must remain valid: %v", err)
	}
}

// order=false drops the sequence requirement: steps must all occur for the entity,
// in any order. It compiles to presence counters rather than sequenceMatch.
func TestChainOrderParam(t *testing.T) {
	t.Run("unordered uses presence counters", func(t *testing.T) {
		pl, err := ParseQuery(`* | chain(user, order=false) { a="x"; b="y" }`)
		if err != nil {
			t.Fatal(err)
		}
		res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"countIf(", "least(", "minIf(", "> 0 AND"} {
			if !strings.Contains(res.SQL, want) {
				t.Errorf("unordered SQL missing %q: %s", want, res.SQL)
			}
		}
		if strings.Contains(res.SQL, "sequenceMatch") || strings.Contains(res.SQL, "sequenceCount") {
			t.Errorf("unordered must not use sequence aggregates: %s", res.SQL)
		}
		// Anchors keep the same contract so the event lookup is unchanged.
		if res.Chain == nil || res.Chain.AnchorColumn != "_chain_ts" {
			t.Error("unordered must still expose anchors")
		}
	})

	t.Run("ordered remains the default", func(t *testing.T) {
		pl, _ := ParseQuery(`* | chain(user) { a="x"; b="y" }`)
		res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.SQL, "sequenceMatch") {
			t.Errorf("default must stay ordered: %s", res.SQL)
		}
	})

	t.Run("order=true is explicit ordered", func(t *testing.T) {
		pl, _ := ParseQuery(`* | chain(user, order=true) { a="x"; b="y" }`)
		res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.SQL, "sequenceMatch") {
			t.Errorf("order=true must be ordered: %s", res.SQL)
		}
	})

	// within= needs a sliding span over an unordered set, which no single aggregate
	// expresses; rejecting beats silently approximating.
	t.Run("within with order=false is rejected", func(t *testing.T) {
		pl, _ := ParseQuery(`* | chain(user, within=5m, order=false) { a="x"; b="y" }`)
		if _, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"}); err == nil {
			t.Fatal("expected within= + order=false to be rejected")
		}
	})

	t.Run("invalid order value is rejected", func(t *testing.T) {
		pl, _ := ParseQuery(`* | chain(user, order=maybe) { a="x"; b="y" }`)
		if _, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"}); err == nil {
			t.Fatal("expected order=maybe to be rejected")
		}
	})
}

// The anchors are event timestamps and only prune a table sorted by them. logs_hot
// is sorted by ingest_timestamp, so the fetch must also carry the query's window in
// its own basis or it scans the whole hot table on every alert trigger.
func TestChainEventsFetchBoundsByScopingBasis(t *testing.T) {
	meta := &ChainMeta{
		EntityColumn: "process_guid", EntityExpr: "fields.`process_guid`::String",
		AnchorColumn: "_chain_ts", StepConditions: []string{"c1", "c2"},
	}
	start := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)

	live := BuildChainEventsSQL(meta, []string{"G1"}, [][]int64{{100}},
		QueryOptions{FractalID: "f", StartTime: start, EndTime: end})
	if !strings.Contains(live.SQL, "timestamp >= '2026-08-12 00:00:00") {
		t.Errorf("event-time query must bound on timestamp: %s", live.SQL)
	}

	hot := BuildChainEventsSQL(meta, []string{"G1"}, [][]int64{{100}},
		QueryOptions{FractalID: "f", StartTime: start, EndTime: end, UseIngestTimestamp: true, TableName: "logs_hot"})
	if !strings.Contains(hot.SQL, "ingest_timestamp >= '2026-08-12 00:00:00") {
		t.Errorf("ingest-scoped query must bound on ingest_timestamp: %s", hot.SQL)
	}
	if !strings.Contains(hot.SQL, "FROM logs_hot") {
		t.Errorf("fetch must read the query's table: %s", hot.SQL)
	}
}
