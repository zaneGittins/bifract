package parser

import (
	"slices"
	"strings"
	"testing"
)

// A condition command written straight after a condition is an AND operand of the
// same filter, as any other operand is.
func TestAdjacentConditionCommandJoinsTheFilter(t *testing.T) {
	pl, err := ParseQuery(`a="1" in(x, ["1"])`)
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.Steps) != 1 || pl.Steps[0].Filter == nil {
		t.Fatalf("steps = %+v, want one filter", pl.Steps)
	}
	f := *pl.Steps[0].Filter
	if f.Op != CondAnd || len(f.Children) != 2 || f.Children[1].Command == nil {
		t.Errorf("filter = %+v, want a=1 AND in()", f)
	}
}

// NOT NOT cancels, so it compiles exactly as the condition alone.
func TestDoubleNegationCancels(t *testing.T) {
	for _, pair := range [][2]string{
		{`NOT NOT a="1"`, `a="1"`},
		{`NOT (NOT (a="1" OR b="2"))`, `a="1" OR b="2"`},
		{`* | NOT NOT cidr(ip, "10.0.0.0/8")`, `* | cidr(ip, "10.0.0.0/8")`},
	} {
		if got, want := positionSQL(t, pair[0]), positionSQL(t, pair[1]); got != want {
			t.Errorf("%s:\n%s\nwant the SQL of %s:\n%s", pair[0], got, pair[1], want)
		}
	}
}

// A connector or pipe with nothing after it is refused where it cannot be ignored.
func TestDanglingSyntaxIsRefused(t *testing.T) {
	for _, q := range []string{`(a="1" OR)`, `a="1" |`, `a="1" | | b="2"`, `a="1" | AND b="2"`} {
		if _, err := ParseQuery(q); err == nil {
			t.Errorf("%s: want a parse error", q)
		}
	}
}

// Removing a resolved source command leaves the filters after it reading the
// source's rows directly, as the first stage of what remains.
func TestStripCommandLeavesFirstStageFilters(t *testing.T) {
	pl, err := ParseQuery(`pgr(start="x") | count > 5 | groupby(image)`)
	if err != nil {
		t.Fatal(err)
	}
	first, later := SplitFirstStage(StripCommand(pl, "pgr"))
	if len(first) != 1 || first[0].Field != "count" || len(later) != 0 {
		t.Errorf("first = %+v, later = %+v; want the count filter first", first, later)
	}
}

// A query mode that renders its own SQL still defines the bindings it reads.
func TestQueryModeKeepsBindingCTEs(t *testing.T) {
	pl, err := ParseQuery(`let &s := a="1" | groupby(user); * | in(user, &s) | in(owner, &s) | analyzefields(user)`)
	if err != nil {
		t.Fatal(err)
	}
	sql, err := TranslateToSQL(pl, serverOpts())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sql, "WITH _b_s AS (") || !strings.Contains(sql, "FROM _b_s") {
		t.Errorf("the binding read twice must be defined:\n%s", sql)
	}
}

// analyzeFields() and ptg() each render the whole query, so they cannot combine.
func TestAnalyzeFieldsRefusesPtg(t *testing.T) {
	pl, err := ParseQuery(`* | analyzefields(user) | ptg(start="x")`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TranslateToSQL(pl, serverOpts()); err == nil || !strings.Contains(err.Error(), "cannot be combined with ptg()") {
		t.Errorf("want analyzeFields() with ptg() refused, got %v", err)
	}
}

// Every field a filter reads is projected for an alert and listed for a chain
// step, a condition command's and an expression's included.
func TestConditionFieldsIncludeCommandsAndExpressions(t *testing.T) {
	opts := serverOpts()
	opts.MinimalProjection = true
	pl, err := ParseQuery(`d>5 in(user, values=["a"]) OR lower(image)="x"`)
	if err != nil {
		t.Fatal(err)
	}
	sql, err := TranslateToSQL(pl, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"AS `d`", "AS `user`", "AS `image`"} {
		if !strings.Contains(sql, f) {
			t.Errorf("minimal projection is missing %s:\n%s", f, sql)
		}
	}
	pl, err = ParseQuery(`* | chain(host) { in(user, values=["a"]) a=1; cidr(dst_ip, "10.0.0.0/8") }`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := TranslateToSQLWithOrder(pl, serverOpts())
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Chain.StepFields; !slices.Equal(got, []string{"user", "a", "dst_ip"}) {
		t.Errorf("chain step fields = %v, want [user a dst_ip]", got)
	}
}

// A condition command's fields come from its schema: in(lower(user), ...) reads user.
func TestConditionCommandFieldsFollowTheSchema(t *testing.T) {
	for q, want := range map[string][]string{
		`cidr(dst_ip, "10.0.0.0/8")`:                {"dst_ip"},
		`in(lower(user), values=["a"])`:             {"user"},
		`in(values=["a"], field=host)`:              {"host"},
		`tlsh(hash="T1", field=sha)`:                {"sha"},
		`modelLookup(model="m", key=[user, image])`: {"user", "image"},
	} {
		pl, err := ParseQuery("a=1 OR " + q)
		if err != nil {
			t.Fatal(err)
		}
		cmd := pl.Filters()[0].Children[1].Command
		if got := conditionCommandFields(*cmd); !slices.Equal(got, want) {
			t.Errorf("%s: fields %v, want %v", q, got, want)
		}
	}
}

// tlsh() filters the stored field; redefining the name after it does not change
// what it filters.
func TestTLSHAllowsALaterRedefinition(t *testing.T) {
	matches := []TLSHMatch{{Digest: "T1A", Distance: 1, Needle: "n"}}
	opts := QueryOptions{MaxRows: 10, FractalID: "f1", HasTLSHFilter: true, TLSHMatches: matches}
	for _, q := range []string{`* | tlsh(field=tlsh, dict="d") | tlsh := "z"`, `* | tlsh(field=tlsh, dict="d") | lowercase(tlsh)`} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Fatal(err)
		}
		sql, err := TranslateToSQL(pl, opts)
		if err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		if !strings.Contains(sql, "fields.`tlsh`::String IN ('T1A')") {
			t.Errorf("%s: the stored field must be filtered:\n%s", q, sql)
		}
	}
}
