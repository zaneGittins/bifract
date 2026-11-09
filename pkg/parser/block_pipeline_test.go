package parser

import (
	"strings"
	"testing"
)

// A block is part of the query, so the query's bindings are in scope inside it.
func TestBindingsResolveInsideBlocks(t *testing.T) {
	for q, want := range map[string]string{
		`let &n := "x"; * | chain(user) { a=&n; b=2 }`:                         "fields.`a`::String = 'x'",
		`let &n := "x"; * | join(user) { a=&n | groupby(user) }`:               "fields.`a`::String = 'x'",
		`let &n := "x"; * | case { a=&n | t:="1"; * | t:="0" }`:                "WHEN fields.`a`::String = 'x' THEN '1'",
		`let &s := a="x" | groupby(user); * | join(user) { &s }`:               "fields.`a`::String = 'x'",
		`let &n := "x"; let &s := a=&n | groupby(user); * | join(user) { &s }`: "fields.`a`::String = 'x'",
	} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Errorf("%s: parse: %v", q, err)
			continue
		}
		sql, err := TranslateToSQL(pl, serverOpts())
		if err != nil {
			t.Errorf("%s: translate: %v", q, err)
			continue
		}
		if !strings.Contains(sql, want) {
			t.Errorf("%s: want %s in:\n%s", q, want, sql)
		}
	}
}

// A mistake inside a block is a parse error pointing into the block, not a
// translation error without a position.
func TestBlockErrorsPointIntoTheBlock(t *testing.T) {
	for q, blockStart := range map[string]string{
		`* | chain(user) { a=1 | ; b=2 }`:                 "{",
		`* | join(user) { a=( | groupby(user) }`:          "{",
		`* | case { a=( | t:="1" }`:                       "{",
		`let &s := a=( | groupby(user); * | in(user, &s)`: ":=",
	} {
		_, err := ParseQuery(q)
		start, _, ok := ErrorPosition(err)
		if err == nil || !ok {
			t.Errorf("%s: want a positioned parse error, got %v", q, err)
			continue
		}
		if start <= strings.Index(q, blockStart) {
			t.Errorf("%s: error at %d is not inside the block: %v", q, start, err)
		}
	}
}

// A chain step may hold a nested block; its braces no longer end the chain.
func TestChainStepKeepsNestedBraces(t *testing.T) {
	pl, err := ParseQuery(`* | chain(user) { a=1 | case { b=1 | t:="x"; * | t:="y" }; c=2 }`)
	if err != nil {
		t.Fatal(err)
	}
	cmds := pl.Commands()
	if len(cmds) != 1 || len(cmds[0].Sequence) != 2 {
		t.Fatalf("want one chain() with two steps, got %+v", cmds)
	}
}

// A block's filters are compiled per use. Compiling one must not write into the
// parsed tree, or a later compile against another registry reuses stale SQL.
func TestBlockFiltersCompileFreshEachTime(t *testing.T) {
	sql := positionSQL(t, `x := lower(a) | case { lower(x)="y" OR b=1 | t:="1"; * | t:="0" }`)
	if !strings.Contains(sql, "lower(lower(fields.`a`::String)) = 'y'") {
		t.Errorf("the branch must read the assigned x, not the raw field:\n%s", sql)
	}
	pl, err := ParseQuery(`* | chain(user) { lower(a)="y" OR b=1; b=2 }`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TranslateToSQL(pl, serverOpts()); err != nil {
		t.Fatal(err)
	}
	opts := serverOpts()
	opts.SourceSubquery, opts.SourceColumns = "SELECT 1 AS a, 2 AS b, 3 AS user", []string{"a", "b", "user"}
	sql, err = TranslateToSQL(pl, opts)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "fields.`a`") {
		t.Errorf("a second translation reused the first one's step SQL:\n%s", sql)
	}
}

// A case result led by a call or NOT is a command, never a literal value.
func TestCaseResultCommandsAreNotLiterals(t *testing.T) {
	for _, result := range []string{`NOT in(user, values=["bob"])`, `chain(user) { b=2; c=3 }`, `join(user) { b=2 | groupby(user) }`} {
		q := `* | case { a=1 | ` + result + ` | t:="1"; * | t:="0" }`
		pl, err := ParseQuery(q)
		if err == nil {
			var sql string
			sql, err = TranslateToSQL(pl, serverOpts())
			if err == nil && strings.Contains(sql, "'"+result) {
				t.Errorf("%s: compiled as a literal:\n%s", q, sql)
			}
		}
		if strings.HasPrefix(result, "NOT") {
			if err != nil {
				t.Errorf("%s: a negated condition is a valid result: %v", q, err)
			}
		} else if err == nil {
			t.Errorf("%s: want a structural command refused", q)
		}
	}
}
