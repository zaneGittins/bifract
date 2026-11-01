package parser

import (
	"strings"
	"testing"
)

func translateErr(t *testing.T, q string, opts QueryOptions) (*TranslationResult, error) {
	t.Helper()
	pl, err := ParseQuery(q)
	if err != nil {
		t.Fatalf("parse %s: %v", q, err)
	}
	return TranslateToSQLWithOrder(pl, opts)
}

// Only one join() is planned; a second must fail rather than replace the first.
func TestSecondJoinIsRejected(t *testing.T) {
	_, err := translateErr(t, `proto="tcp" | join(dst_ip) { proto="udp" | groupby(dst_ip) } | join(src_ip) { proto="udp" | groupby(src_ip) }`, QueryOptions{MaxRows: 10})
	if err == nil || !strings.Contains(err.Error(), "one join()") {
		t.Errorf("err = %v", err)
	}
}

// ptg() returns its tree as is: after it, only _depth/_path filters and pgraph() apply, so
// anything else fails instead of being dropped.
func TestPtgRejectsWhatItCannotApply(t *testing.T) {
	opts := QueryOptions{MaxRows: 10, ProcessTree: &ProcessTree{}}
	for _, q := range []string{
		`ptg(start="x") | table(image)`,
		`ptg(start="x") | sort(image)`,
		`ptg(start="x") | image="cmd.exe"`,
		`ptg(start="x") | _depth<=2 OR image="cmd.exe"`,
		`ptg(start="x") | limit(5)`,
	} {
		if _, err := translateErr(t, q, opts); err == nil || !strings.Contains(err.Error(), "ptg()") {
			t.Errorf("%s: err = %v", q, err)
		}
	}
	for _, q := range []string{
		`ptg(start="x")`,
		`* | ptg(start="x") | _depth<=2 | pgraph()`,
		`ptg(start="x") | _path=/explorer/ AND _depth>1`,
	} {
		if _, err := translateErr(t, q, opts); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// distinct= counts the distinct values of the last key per group of the others, as it does
// in timechart; grouping by that key as well always counted 1.
func TestGroupByDistinctCountsTheLastKeyPerGroup(t *testing.T) {
	r, err := translateErr(t, `* | groupby(src_ip, dst_ip, distinct=true)`, QueryOptions{MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.SQL, "COUNT(DISTINCT fields.`dst_ip`::String) AS _count") || !strings.Contains(r.SQL, "GROUP BY src_ip ") || strings.Contains(r.SQL, "AS dst_ip") {
		t.Errorf("SQL: %s", r.SQL)
	}
	if strings.Join(r.FieldOrder, ",") != "src_ip,_count" {
		t.Errorf("fields %v", r.FieldOrder)
	}

	r, err = translateErr(t, `* | groupby(user, distinct=true)`, QueryOptions{MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.SQL, "COUNT(DISTINCT fields.`user`::String) AS _count") || strings.Contains(r.SQL, "GROUP BY") || strings.Join(r.FieldOrder, ",") != "_count" {
		t.Errorf("one key counts its distinct values overall: %s %v", r.SQL, r.FieldOrder)
	}
}

// strftime() takes ClickHouse formatDateTime specifiers as is. %R is HH:MM there, so it must
// not be rewritten through %M (the month name); strftime's %A and %B, which ClickHouse lacks,
// mean its %W and %M; %% stays a literal percent.
func TestConvertTimeFormat(t *testing.T) {
	for in, want := range map[string]string{
		"%R":              "%R",
		"%H:%i":           "%H:%i",
		"%M":              "%M",
		"%A %d %B %Y, %R": "%W %d %M %Y, %R",
		"100%% %B":        "100%% %M",
		"%%B is literal":  "%%B is literal",
		"trailing %":      "trailing %",
	} {
		if got := convertTimeFormat(in); got != want {
			t.Errorf("convertTimeFormat(%q) = %q, want %q", in, got, want)
		}
	}
}

// The camelCase spelling places like model_lookup(): the enrichment docs group its output.
func TestModelLookupCamelCaseBeforeGroupBy(t *testing.T) {
	for _, name := range []string{"modelLookup", "model_lookup"} {
		if _, err := translateErr(t, `* | `+name+`(model="fs", key=[user]) | groupby(is_new)`, mlookupOpts()); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A value followed by a space and a bracket ends the value; the bracket opens a
// group joined by implicit AND. Only `name(` with no space is a call there.
func TestValueBeforeSpacedGroupIsNotACall(t *testing.T) {
	for implicit, explicit := range map[string]string{
		`image=~a,b,c (x=1 OR y=2)`:        `image=~a,b,c AND (x=1 OR y=2)`,
		`image=^a (x=1 OR y=2)`:            `image=^a AND (x=1 OR y=2)`,
		`image=a (x=1 OR y=2)`:             `image=a AND (x=1 OR y=2)`,
		`image="a",b (x=1 OR y=2)`:         `image="a",b AND (x=1 OR y=2)`,
		`image!=a (x=1 OR y=2)`:            `image!=a AND (x=1 OR y=2)`,
		`lower(image) =~ a,b (x=1 OR y=2)`: `lower(image) =~ a,b AND (x=1 OR y=2)`,
	} {
		if got, want := translateML(t, implicit), translateML(t, explicit); got != want {
			t.Errorf("%s: want the same SQL as %s\n got: %s\nwant: %s", implicit, explicit, got, want)
		}
	}
	// A command may still be written with a space before its bracket.
	if got, want := translateML(t, `* | groupby (image) | count ()`), translateML(t, `* | groupby(image) | count()`); got != want {
		t.Errorf("spaced command call changed meaning\n got: %s\nwant: %s", got, want)
	}
}

// A comma in a value list must be followed by a value, as it is for = lists, so
// a stray comma cannot swallow or hide what comes after it.
func TestValueListRejectsTrailingComma(t *testing.T) {
	for _, q := range []string{`image=~a,`, `image=^a,b,`, `image!=$a, | count()`, `lower(image) =~ a,`} {
		_, err := ParseQuery(q)
		if err == nil || !strings.Contains(err.Error(), "expected value after ','") {
			t.Errorf("%s: want a missing-value error, got %v", q, err)
		}
	}
}

// Two conditions side by side mean AND, whatever the second one is, so they bind
// as an explicit AND does, inside brackets as well as out.
func TestImplicitAndBindsLikeAnd(t *testing.T) {
	for _, pair := range [][2]string{
		{`a=1 OR b=2 c=3`, `a=1 OR b=2 AND c=3`},
		{`a=1 OR b=2 (c=3 OR d=4)`, `a=1 OR b=2 AND (c=3 OR d=4)`},
		{`a=1 (c=3 OR d=4) OR z=5`, `a=1 AND (c=3 OR d=4) OR z=5`},
		{`image=~a,b (x=1 OR y=2) OR z=3`, `image=~a,b AND (x=1 OR y=2) OR z=3`},
		{`a=1 OR b=2 NOT c=3`, `a=1 OR b=2 AND NOT c=3`},
		{`a=1 OR b=2 !c=3`, `a=1 OR b=2 AND !c=3`},
		{`a=1 OR b=2 NOT NOT c=3`, `a=1 OR b=2 AND c=3`},
		{`(a=1 NOT NOT c=3) OR z=5`, `(a=1 AND c=3) OR z=5`},
		{`a=1 OR b=2 "foo"`, `a=1 OR b=2 AND "foo"`},
		{`a=1 OR b=2 lower(x)="y"`, `a=1 OR b=2 AND lower(x)="y"`},
		{`a=1 lower(x)="y" OR z=5`, `a=1 AND lower(x)="y" OR z=5`},
		{`let &e := image=~cmd; a=1 OR b=2 &e`, `let &e := image=~cmd; a=1 OR b=2 AND &e`},
		{`(a=1 (c=3 OR d=4)) OR z=5`, `(a=1 AND (c=3 OR d=4)) OR z=5`},
		{`(a=1 NOT c=3) OR z=5`, `(a=1 AND NOT c=3) OR z=5`},
		{`let &n := len(cmd); a=1 OR b=2 &n > 5`, `let &n := len(cmd); a=1 OR b=2 AND &n > 5`},
		{`let &n := len(cmd); (a=1 OR b=2) &n > 5`, `let &n := len(cmd); (a=1 OR b=2) AND &n > 5`},
		{`let &n := len(cmd); a=1 OR b=2 NOT &n > 5`, `let &n := len(cmd); a=1 OR b=2 AND NOT &n > 5`},
		{`let &n := len(cmd); a=1 OR &n > 5`, `let &n := len(cmd); a=1 OR len(cmd) > 5`},
		{`let &e := image=~cmd; &e NOT a=1 OR b=2`, `let &e := image=~cmd; &e AND NOT a=1 OR b=2`},
		{`let &e := image=~cmd; &e (a=1) OR b=2`, `let &e := image=~cmd; &e AND (a=1) OR b=2`},
		{`cidr(ip, "10.0.0.0/8") a=1 OR b=2`, `cidr(ip, "10.0.0.0/8") AND a=1 OR b=2`},
		{`NOT in(c, values=["x"]) a=1 OR b=2`, `NOT in(c, values=["x"]) AND a=1 OR b=2`},
		{`a=*foo (x=1 OR y=2) OR z=3`, `a=*foo AND (x=1 OR y=2) OR z=3`},
		{`(a=1 "foo") OR z=5`, `(a=1 AND "foo") OR z=5`},
		{`a=1 OR b=2 cidr(ip, "10.0.0.0/8")`, `a=1 OR b=2 AND cidr(ip, "10.0.0.0/8")`},
		{`a=1 OR b=2 NOT in(c, values=["x"])`, `a=1 OR b=2 AND NOT in(c, values=["x"])`},
		{`a=1 cidr(ip, "10.0.0.0/8") OR z=5`, `a=1 AND cidr(ip, "10.0.0.0/8") OR z=5`},
		{"a=1 OR b=2\n| NOT c=3", "(a=1 OR b=2) AND NOT c=3"},
		{"a=1 OR b=2\n| (c=3 OR d=4)", "(a=1 OR b=2) AND (c=3 OR d=4)"},
		{`(a=1 cidr(ip, "10.0.0.0/8")) OR z=5`, `(a=1 AND cidr(ip, "10.0.0.0/8")) OR z=5`},
		{`* | case { a=1 (c=3 OR d=4) OR z=5 | x:="y"; * | x:="z"; }`, `* | case { a=1 AND (c=3 OR d=4) OR z=5 | x:="y"; * | x:="z"; }`},
	} {
		if got, want := translateML(t, pair[0]), translateML(t, pair[1]); got != want {
			t.Errorf("%s: want the same SQL as %s\n got: %s\nwant: %s", pair[0], pair[1], got, want)
		}
	}
}

// A word written against a bracket is a call, which a list cannot hold; the
// error says how to write the group that was likely meant.
func TestListCallHintsAtSpace(t *testing.T) {
	for _, q := range []string{`image=~a,b(x=1 OR y=2)`, `image="a",b(x=1)`} {
		_, err := ParseQuery(q)
		if err == nil || !strings.Contains(err.Error(), "put a space before '('") {
			t.Errorf("%s: want a hint about the space, got %v", q, err)
		}
	}
}
