package parser

import (
	"strings"
	"testing"
)

func translateAfterAggregation(q string) (*TranslationResult, error) {
	pl, err := ParseQuery(q)
	if err != nil {
		return nil, err
	}
	return TranslateToSQLWithOrder(pl, QueryOptions{MaxRows: 100, FractalID: "f1", HasCommentFilter: true, CommentLogIDs: []string{"1"},
		Dictionaries: map[string]map[string]string{"d": {"k": "lookup_d"}}})
}

// A filter command after an aggregation compiles as a comparison there does: a key
// filter removes whole groups, so it may run on the scan, and any other runs on the
// aggregation's output.
func TestFilterCommandAfterAggregationBindsLikeAComparison(t *testing.T) {
	cases := []struct{ name, query, want string }{
		{"negated in on a key", `* | groupby(user) | !in(user, values=["a"])`, "NOT (fields.`user`::String IN ('a')) GROUP BY"},
		{"in on an aggregate", `* | groupby(user) | in(_count, values=["1"])`, "HAVING _count IN ('1')"},
		{"in on a chain aggregate", `* | chain(user) { a=1; b=2 } | in(chain_count, values=["1"])`, "HAVING sequenceMatch"},
		{"in on a chain identity", `* | chain(user) { a=1; b=2 } | in(user, values=["a"])`, "fields.`user`::String IN ('a')"},
		{"field named after values", `* | groupby(user) | in(values=["a"], field=user)`, "IN ('a') GROUP BY"},
		{"in on a computed column", `* | groupby(user) | x := _count * 2 | in(x, values=["6"])`, "GROUP BY user) WHERE x IN ('6')"},
		{"in on a transform output", `* | groupby(user) | len(user) | in(_len, values=["5"])`, "GROUP BY user) WHERE _len IN ('5')"},
		{"in on a named output", `* | groupby(user) | concat([user,user], as=c) | in(c, values=["bb"])`, "GROUP BY user) WHERE c IN ('bb')"},
		{"cidr on a computed column", `* | groupby(user) | concat([user,user], as=c) | cidr(c, "10.0.0.0/8")`, "GROUP BY user) WHERE (isIPAddressInRange(if((isIPv4String(c)"},
		{"key filter past a computed column stays on the scan", `* | groupby(user) | n := _count * 2 | in(user, values=["a"])`, "IN ('a') GROUP BY user"},
		{"key filter past a sprintf stays on the scan", `* | groupby(user) | sprintf("%s!", user, as=hx) | in(user, values=["a"])`, "IN ('a') GROUP BY user"},
		{"after a chain over a groupby", `* | groupby(user) | chain(host) { a=1; b=2 } | user="alice"`, "chain_count"},
		{"selectFirst output", `* | selectfirst(user) | in(_first, values=["a"])`, "HAVING _first IN ('a')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if sql := positionSQL(t, tc.query); !strings.Contains(sql, tc.want) {
				t.Errorf("missing %q:\n%s", tc.want, sql)
			}
		})
	}
}

func TestFilterAfterAggregationRejectsDroppedField(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | groupby(user) | cidr(src_ip, "10.0.0.0/8")`, "src_ip does not exist after groupby()"},
		{`* | groupby(user) | !cidr(src_ip, "10.0.0.0/8")`, "src_ip does not exist after groupby()"},
		{`* | groupby(user) | in(host, values=["a"])`, "host does not exist after groupby()"},
		{`* | groupby(user) | !in(host, values=["a"])`, "host does not exist after groupby()"},
		{`* | groupby(user) | cidr(range="10.0.0.0/8", field=host)`, "host does not exist after groupby()"},
		{`* | skew(bytes) | in(host, values=["a"])`, "host does not exist after skewness()"},
		{`* | chain(user) { a=1; b=2 } | cidr(src_ip, "10.0.0.0/8")`, "src_ip does not exist after chain()"},
		{`* | chain(user) { a=1; b=2 } | in(host, values=["a"])`, "host does not exist after chain()"},
		{`* | chain(user) { a=1; b=2 } | src_ip="10.0.0.1"`, "src_ip does not exist after chain()"},
		{`* | chain(user, host) { a=1; b=2 } | user="a"`, "user does not exist after chain()"},
		{`* | groupby(user) | contains(src_ip, "10.")`, "src_ip does not exist after groupby()"},
		{`* | groupby(user) | lower(src_ip)="10.0.0.1"`, "src_ip does not exist after groupby()"},
		{`* | chain(user) { a=1; b=2 } | contains(src_ip, "10.")`, "src_ip does not exist after chain()"},
		{`* | chain(user) { a=1; b=2 } | src_ip=field(dst_ip)`, "src_ip does not exist after chain()"},
		{`* | groupby(user) | user=field(host)`, "host does not exist after groupby()"},
		{`* | groupby(user) | "foo"`, "a text search reads log rows, so move it before groupby()"},
		{`* | groupby(user) | /foo/`, "a text search reads log rows, so move it before groupby()"},
		{`* | chain(user) { a=1; b=2 } | "foo"`, "a text search reads log rows, so move it before chain()"},
		{`* | y := lower(src_ip) | groupby(user) | y="x"`, "y does not exist after groupby()"},
		{`* | y := lower(src_ip) | chain(user) { a=1; b=2 } | in(y, values=["x"])`, "y does not exist after chain()"},
		{`* | len(src_ip) | chain(user) { a=1; b=2 } | in(_len, values=["8"])`, "_len does not exist after chain()"},
		{`* | groupby(user) | log_id="x"`, "log_id does not exist after groupby()"},
		{`* | groupby(user) | timestamp > "2026-01-01"`, "timestamp does not exist after groupby()"},
		{`* | chain(user) { a=1; b=2 } | normalizer="x"`, "normalizer does not exist after chain()"},
		{`* | groupby(user) | count() | in(user, values=["a"])`, "user does not exist after count()"},
		{`* | groupby(user) | count() | user="a"`, "user does not exist after count()"},
		{`* | chain(user) { a=1; b=2 } | count() | user="a"`, "user does not exist after count()"},
		{`* | groupby(user, host) | groupby(user) | in(host, values=["a"])`, "host does not exist after groupby()"},
		{`* | chain(user) { a=1; b=2 } | groupby(chain_count) | in(user, values=["a"])`, "user does not exist after groupby()"},
		{`* | chain(user) { a=1; b=2 } | groupby(user) | in(chain_count, values=["1"])`, "chain_count does not exist after groupby()"},
		{`* | groupby(user) | in(src_ip, values=["1"]) | src_ip := "z"`, "src_ip does not exist after groupby()"},
		{`* | chain(user) { a=1; b=2 } | in(src_ip, values=["1"]) | src_ip := "z"`, "src_ip does not exist after chain()"},
		{`* | groupby(user) | avg(_count) | user="a"`, "user does not exist after avg()"},
		{`* | groupby(src_ip) | table(user) | user="a"`, "user does not exist after groupby()"},
		{`* | count() | table(user) | in(user, values=["a"])`, "user does not exist after count()"},
	}
	for _, tc := range cases {
		if _, err := translateAfterAggregation(tc.query); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.query, err, tc.want)
		}
	}
}

// Everything an aggregation returns, and what the pipeline computes after it, stays
// filterable.
func TestFilterAfterAggregationKeepsItsOutputs(t *testing.T) {
	for _, q := range []string{
		`* | groupby(user) | lower(user)="a"`,
		`* | groupby(user) | contains(user, "a")`,
		`* | groupby(user) | _count > field(_count)`,
		`* | groupby(user) | sum(bytes) | _sum > 1 | in(user, values=["a"])`,
		`* | sum(a) | avg(b) | _sum > 1`,
		`* | groupby(user) | avg(_count) | _avg > 1`,
		`* | groupby(user) | count() | _count > 1`,
		`* | y := lower(a) | groupby(y) | y="x"`,
		`* | groupby(lower(user)) | lower_user="a"`,
		`* | groupby(user) | x := _count * 2 | x > 1`,
		`* | groupby(user) | x := _count * 2 | contains(user, "a")`,
		`* | groupby(user) | len(user) | _len > 1`,
		`* | groupby(user) | lowercase(user) | user="a"`,
		`* | chain(user) { a=1; b=2 } | chain_count > 1`,
		`* | chain(user, host) { a=1; b=2 } | _entity="a"`,
		`* | groupby(a) | groupby(b) | _count > 1`,
		`* | groupby(user) | head(5) | user="a"`,
		`* | groupby(user) | sort(_count) | _count > 1`,
	} {
		if _, err := translateAfterAggregation(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

func TestDroppedFieldErrorListsOnlyVisibleOutputs(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | chain(user) { a=1; b=2 } | in(host, values=["a"])`, "(chain_count, user)"},
		{`* | chain(user, host) { a=1; b=2 } | in(src_ip, values=["a"])`, "(_entity, chain_count)"},
		{`* | selectfirst(user) | in(host, values=["a"])`, "(_first)"},
		{`* | selectlast(user, as=newest) | in(host, values=["a"])`, "(newest)"},
		{`* | mad(bytes) | in(host, values=["a"])`, "_mad"},
	}
	for _, tc := range cases {
		if _, err := translateAfterAggregation(tc.query); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.query, err, tc.want)
		}
	}
}

// tlsh() and the pgrHas filters read log rows, so they keep their own rejection.
func TestRowOnlyFiltersStayRejectedAfterAggregation(t *testing.T) {
	const tlshAfter = "tlsh() must come before groupby/stats"
	cases := []struct{ query, want string }{
		{`* | groupby(sha) | tlsh(field=sha, hash="T1A")`, tlshAfter},
		{`* | groupby(sha) | !tlsh(field=sha, hash="T1A")`, tlshAfter},
		{`* | groupby(user) | tlsh(hash="T1A", field=sha)`, tlshAfter},
		{`* | chain(sha) { a=1; b=2 } | tlsh(field=sha, hash="T1A")`, tlshAfter},
		{`* | timechart(span=1h) | tlsh(field=sha, hash="T1A")`, tlshAfter},
		{`* | groupby(user) | pgrHasNetwork()`, "put them before groupby()"},
		{`* | chain(user) { a=1; b=2 } | !pgrHasAncestor("cmd.exe")`, "put them before chain()"},
	}
	for _, tc := range cases {
		pl, err := ParseQuery(tc.query)
		if err != nil {
			t.Fatalf("parse %s: %v", tc.query, err)
		}
		opts := QueryOptions{MaxRows: 100, FractalID: "f1", HasTLSHFilter: true, TLSHMatches: []TLSHMatch{{Digest: "d", Distance: 1, Needle: "n"}}}
		err = ValidatePgrFilters(pl)
		if err == nil {
			_, err = TranslateToSQLWithOrder(pl, opts)
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.query, err, tc.want)
		}
	}
}

func TestFilterCommandBeforeAggregationStaysOnTheScan(t *testing.T) {
	sql := positionSQL(t, `* | cidr(src_ip, "10.0.0.0/8") | groupby(user)`)
	if scan, _, _ := strings.Cut(sql, "GROUP BY"); !strings.Contains(scan, "isIPAddressInRange") {
		t.Errorf("filter left the scan:\n%s", sql)
	}
}

// A binding read from a condition belongs to the query, not to the throwaway plan
// its predicate compiles against.
func TestBindingInConditionReachesTheQuery(t *testing.T) {
	const twice = `let &s := * | groupby(user); * | groupby(user) | in(user, values=&s) | in(user, values=&s)`
	res, err := translateAfterAggregation(twice)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(res.SQL, "WITH _b_s AS") {
		t.Errorf("binding read twice lost its WITH:\n%s", res.SQL)
	}
	res, err = translateAfterAggregation(`let &s := * | groupby(user); a=1 OR in(user, values=&s)`)
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimeScopedSubquery || !res.RowCapped {
		t.Errorf("a binding in an OR must mark the query: scoped=%v capped=%v", res.TimeScopedSubquery, res.RowCapped)
	}
}

func TestBindingBudgetIsSharedAcrossConditions(t *testing.T) {
	var q strings.Builder
	q.WriteString("let &s := * | groupby(user); * | groupby(user)")
	for range bindingWorkBudget + 1 {
		q.WriteString(` | in(user, values=&s)`)
	}
	if _, err := translateAfterAggregation(q.String()); err == nil || !strings.Contains(err.Error(), "too many result-set bindings") {
		t.Errorf("got %v", err)
	}
}
