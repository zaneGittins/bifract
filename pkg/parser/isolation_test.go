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
// Every predicate records its precedence where it is built, and andOf brackets
// a disjunction. This test checks the property over every shape that can produce
// an OR rather than trusting each producer.

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
	// A filter on a field assigned an OR reads the assignment as one operand.
	`* | x := a = "1" or b = "2" | x = 1`,
	`* | x := a = "1" or b = "2" | x != *`,
	`* | x := a > 1 or b < 2 | x > 0`,
	`* | x := a = "1" or b = "2" | in(x, values=["1"])`,
	`* | x := a = "1" or b = "2" | y := c = "3" or d = "4" | x = y`,
	`* | x := a > 1 or b < 2 | x = 1 | groupby(user)`,
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

// andOf brackets a disjunction, so it cannot re-associate with the guards beside
// it.
func TestAndOfBracketsDisjunctions(t *testing.T) {
	guarded := andOf(atom("fractal_id = 'f1'"), predicate{sql: "a = '1' OR b = '2'", prec: precOr}, orOf(atom("c = '3'"), atom("d = '4'")))
	if want := "fractal_id = 'f1' AND (a = '1' OR b = '2') AND (c = '3' OR d = '4')"; guarded.sql != want {
		t.Errorf("andOf = %s, want %s", guarded.sql, want)
	}
	if got := orOf(andOf(atom("a"), atom("b")), atom("c")).sql; got != "a AND b OR c" {
		t.Errorf("orOf = %s: AND binds tighter, so no brackets are needed", got)
	}
	if got := notOf(orOf(atom("a"), atom("b"))).sql; got != "NOT (a OR b)" {
		t.Errorf("notOf = %s", got)
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
		// "OR(" is a boundary; "ORDER" is not.
		{`a = '1' OR(b = '2')`, true},
		{`a = '1' ORDER BY x`, false},
		{"a = '1'\tOR\tb = '2'", true},
		{"a = '1'\nOR b = '2'", true},
		// A stray paren from a backtick identifier must fail safe, not silently
		// mark the rest of the clause as nested.
		{"fields.`a)b`::String = '1' OR b = '2'", true},
		// A backtick identifier is skipped whole, like a string literal.
		{"fields.`a(b`::String = '1' OR b = '2'", true},
		{"fields.`a'b`::String = 'x' AND (c = 'y' OR d = 'z')", false},
		{"fields.`a'b`::String = 'x' OR fields.`c'd`::String = 'y'", true},
		{"fields.`a OR b`::String = '1'", false},
		{"fields.`a`` OR b`::String = '1'", false},
		{"fields.`a\\\\`::String = '1' OR b = '2'", true},
	}
	for _, c := range cases {
		if got := hasTopLevelOR(c.clause); got != c.want {
			t.Errorf("hasTopLevelOR(%q) = %v, want %v", c.clause, got, c.want)
		}
	}
}

// hasTopLevelOR reports whether the clause contains an OR outside every bracket
// and string literal or backtick identifier. It is the oracle tests hold
// generated predicates to; the translator itself knows each one's precedence.
//
// A quoted run is skipped whole, honouring backslash escapes: escapeString
// doubles backslashes, so a Windows path ends 'C:\\' and a naive "is the
// previous byte a backslash" test reads the closing quote as escaped.
func hasTopLevelOR(clause string) bool {
	space := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
	depth := 0
	for i := 0; i < len(clause); i++ {
		c := clause[i]
		if c == '\'' || c == '`' {
			for i++; i < len(clause) && clause[i] != c; i++ {
				if clause[i] == '\\' {
					i++
				}
			}
			if i >= len(clause) {
				return true // unterminated: not the shape expected
			}
			continue
		}
		switch {
		case c == '(':
			depth++
		case c == ')':
			if depth--; depth < 0 {
				return true // unbalanced: not the shape expected
			}
		case depth == 0 && (c == 'O' || c == 'o') && i > 0 && space(clause[i-1]) &&
			i+2 < len(clause) && (clause[i+1] == 'R' || clause[i+1] == 'r') && (space(clause[i+2]) || clause[i+2] == '('):
			return true
		}
	}
	return false
}
