package parser

import (
	"strings"
	"testing"
)

// Alert windows abut, [cursor, end), so every read inside one query must exclude
// the end. A single inclusive bound reads a row stamped on the boundary in two
// windows and fires its alert twice.
func TestEndExclusiveReachesEveryWindowRead(t *testing.T) {
	opts := serverOpts()
	opts.UseIngestTimestamp = true
	opts.EndExclusive = true
	end := "ingest_timestamp < '" + chTimeLiteral(opts.EndTime) + "'"

	reads := map[string]string{}
	for _, q := range []string{
		`action="login"`,
		`action="login" | join(user) { action="logout" | groupby(user) }`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		res, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("translate %q: %v", q, err)
		}
		reads[q] = res.SQL
	}
	meta := &ChainMeta{EntityColumn: "user", EntityExpr: "fields.`user`::String", AnchorColumn: "_chain_ts", StepConditions: []string{"c1"}}
	reads["chain events fetch"] = BuildChainEventsSQL(meta, []string{"alice"}, [][]int64{{100}}, opts).SQL

	for name, sql := range reads {
		if strings.Contains(sql, "ingest_timestamp <= '") {
			t.Errorf("%s: inclusive end bound: %s", name, sql)
		}
		if !strings.Contains(sql, end) {
			t.Errorf("%s: missing %q: %s", name, end, sql)
		}
	}
}
