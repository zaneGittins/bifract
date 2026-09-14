package parser

import (
	"strings"
	"testing"
	"time"
)

// The scope guards (time range, fractal_id) are ordinary entries in the WHERE
// list, joined with AND. AND binds tighter than OR, so a condition carrying a
// top-level OR would leave its right disjunct outside those guards and read
// every fractal at any time.
//
// Every condition producer brackets its own output, but that was an unwritten
// invariant: adding expression filters, a new producer, re-opened the hole.
// andJoin now enforces it at the join, and this test checks the property over
// every shape that can produce an OR rather than trusting each producer.

func isolationOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "f1",
	}
}

func prismOpts() QueryOptions {
	o := isolationOpts()
	o.FractalID = ""
	o.FractalIDs = []string{"f1", "f2", "f3"}
	return o
}

// orShapes is every spelling that can put an OR into a WHERE clause.
var orShapes = []string{
	`* | lower(image) = "a" OR lower(image) = "b"`,
	`* | contains(a,"b") OR contains(c,"d")`,
	`* | !contains(a,"b") OR c="1"`,
	`* | isPrivateIP(a) = false OR b="1"`,
	`* | a="1" OR lower(b)="2"`,
	`cidr(a,"10.0.0.0/8") OR cidr(b,"192.168.0.0/16")`,
	`* | cidr(a,"10.0.0.0/8") OR cidr(b,"192.168.0.0/16")`,
	`a="1" OR b="2"`,
	`* | a="1" OR b="2"`,
	`a="1" OR b="2" OR c="3"`,
	`* | in(a,"1,2") OR in(b,"3,4")`,
	`* | "foo" OR "bar"`,
	`* | /foo/ OR /bar/`,
	`* | a=~x,y OR b=^p,q`,
	`* | (a="1" OR b="2") AND c="3"`,
	`* | x := 1 | a="1" OR b="2"`,
	`NOT a="1" OR b="2"`,
	`* | !cidr(a,"10.0.0.0/8") OR b="1"`,
}

func TestScopeGuardsCannotBeEscapedByOr(t *testing.T) {
	for _, scope := range []struct {
		name  string
		opts  QueryOptions
		guard string
	}{
		{"fractal", isolationOpts(), "fractal_id = 'f1'"},
		{"prism", prismOpts(), "fractal_id IN ("},
	} {
		t.Run(scope.name, func(t *testing.T) {
			for _, q := range orShapes {
				pipeline, err := ParseQuery(q)
				if err != nil {
					t.Errorf("parse %q: %v", q, err)
					continue
				}
				result, err := TranslateToSQLWithOrder(pipeline, scope.opts)
				if err != nil {
					t.Errorf("translate %q: %v", q, err)
					continue
				}
				clause, ok := whereClauseOf(result.SQL)
				if !ok {
					t.Errorf("%q produced no WHERE at all: %s", q, result.SQL)
					continue
				}
				if !strings.Contains(clause, scope.guard) {
					t.Errorf("%q lost its scope guard: %s", q, clause)
					continue
				}
				if hasTopLevelOR(clause) {
					t.Errorf("%q: OR escaped the scope guard, so the right disjunct reads every fractal:\n  %s", q, clause)
				}
			}
		})
	}
}

// whereClauseOf returns the innermost WHERE clause of a generated query.
func whereClauseOf(sql string) (string, bool) {
	i := strings.Index(sql, "WHERE ")
	if i < 0 {
		return "", false
	}
	clause := sql[i+len("WHERE "):]
	for _, stop := range []string{" ORDER BY ", " GROUP BY ", " LIMIT ", " HAVING "} {
		if j := strings.Index(clause, stop); j >= 0 {
			clause = clause[:j]
		}
	}
	return clause, true
}

// andJoin is the enforcement point, so its own behaviour is pinned directly:
// a producer that forgets to bracket must still not escape the guards.
func TestAndJoinBracketsTopLevelOr(t *testing.T) {
	guarded := andJoin([]string{"fractal_id = 'f1'", "a = '1' OR b = '2'"})
	if hasTopLevelOR(guarded) {
		t.Errorf("an unbracketed producer escaped the guard: %s", guarded)
	}
	if !strings.Contains(guarded, "fractal_id = 'f1' AND (a = '1' OR b = '2')") {
		t.Errorf("unexpected shape: %s", guarded)
	}
}

func TestHasTopLevelOr(t *testing.T) {
	cases := []struct {
		clause string
		want   bool
	}{
		{"a = '1' OR b = '2'", true},
		{"(a = '1' OR b = '2')", false},
		{"a = '1' AND (b = '2' OR c = '3')", false},
		{"a = 'OR'", false},                 // inside a string literal
		{"a = 'x' AND b = 'y OR z'", false}, // ditto
		{"f(a, b) OR g(c)", true},           // after a balanced call
		{"multiIf(a OR b, 1, 0)", false},    // wholly inside a call
		{"a = '1'", false},
		{"", false},
		// escapeString doubles backslashes, so a Windows path ends 'C:\\'. Reading
		// that closing quote as escaped loses the string and misses the OR.
		{`a = 'C:\\' OR b = '1'`, true},
		{`a = 'C:\\Windows\\' AND (b = '1' OR c = '2')`, false},
		{`a = 'it\'s' OR b = '1'`, true},
		{`a = 'it\'s OR nothing'`, false},
		// Unterminated: bracket rather than risk letting an OR past the guards.
		{`a = 'unterminated OR b`, true},
	}
	for _, c := range cases {
		if got := hasTopLevelOR(c.clause); got != c.want {
			t.Errorf("hasTopLevelOR(%q) = %v, want %v", c.clause, got, c.want)
		}
	}
}
