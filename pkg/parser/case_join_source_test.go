package parser

import (
	"strings"
	"testing"
)

// TestCaseFieldNamesRejectSQL verifies a case branch can only assign a plain
// identifier: the name becomes a bare SQL alias and GROUP BY key, so anything
// else could rewrite the query around it, including the fractal filter.
func TestCaseFieldNamesRejectSQL(t *testing.T) {
	for _, q := range []string{
		`* | case { a=1 | fractal_id FROM logs GROUP BY fractal_id -- := 3 ; }`,
		`* | case { a=1 | x y = 3 ; }`,
		`* | case { a=1 | x-y := 3 ; }`,
		`* | case { a=1 | r := "1" ; * | r := "2" } | count()`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, serverOpts())
		if strings.Contains(q, "| count()") {
			if err != nil {
				t.Fatalf("%q: valid case must translate: %v", q, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), "invalid field name") {
			t.Fatalf("%q: want an invalid field name error, got %v", q, err)
		}
	}
}

// TestJoinBlockKeepsSource verifies the join subquery is the text as written,
// not tokens re-joined with spaces, which dropped quotes and regex flags.
func TestJoinBlockKeepsSource(t *testing.T) {
	for _, q := range []string{
		`* | join(user) { message="a b" | groupby(user) }`,
		`* | join(user) { image=/powershell/i | groupby(user) }`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		var block string
		for _, c := range pipeline.Commands {
			if c.Name == "join" {
				block = c.Block
			}
		}
		if !strings.Contains(q, "{ "+block+" }") {
			t.Fatalf("%q: block = %q, want the source between the braces", q, block)
		}
		if _, err := TranslateToSQLWithOrder(pipeline, serverOpts()); err != nil {
			t.Fatalf("translate %q: %v", q, err)
		}
	}
}

// TestJoinRejectsSecondKey verifies a second key is an error: join() matches on
// one key, and an ignored second one silently widens the join.
func TestJoinRejectsSecondKey(t *testing.T) {
	pipeline, err := ParseQuery(`* | join(host, user) { * | groupby(host, user) }`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TranslateToSQLWithOrder(pipeline, serverOpts()); err == nil || !strings.Contains(err.Error(), "fills no remaining parameter") {
		t.Fatalf("want a second join key rejected, got %v", err)
	}
}

// TestJoinBeforeCountNamesTheFilter verifies the error explains that the join runs
// on the aggregated rows and points at in() for filtering before aggregating.
func TestJoinBeforeCountNamesTheFilter(t *testing.T) {
	pipeline, err := ParseQuery(`* | join(host) { * | groupby(host) } | count()`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = TranslateToSQLWithOrder(pipeline, serverOpts())
	if err == nil || !strings.Contains(err.Error(), "applied to the aggregated rows") || !strings.Contains(err.Error(), "in(host, &keys)") {
		t.Fatalf("want the join-after-aggregation explanation, got %v", err)
	}
}
