package parser

import (
	"slices"
	"strings"
	"testing"
)

// scanLogic reads the boolean structure of the scan filter in sql: each atom is
// the SQL text of one condition, and the result evaluates the filter for a truth
// value per atom. Bracketing is the builder's business; this checks what it means.
func scanLogic(t *testing.T, sql string, atoms ...string) func(v ...bool) bool {
	t.Helper()
	start := strings.Index(sql, "fractal_id = 'f1' AND ")
	end := strings.Index(sql, " ORDER BY")
	if start < 0 || end < start {
		t.Fatalf("no scan filter in:\n%s", sql)
	}
	clause := sql[start+len("fractal_id = 'f1' AND ") : end]
	order := slices.Clone(atoms)
	slices.SortFunc(order, func(a, b string) int { return len(b) - len(a) })
	for _, a := range order {
		if !strings.Contains(clause, a) {
			t.Fatalf("atom %q missing from:\n%s", a, clause)
		}
		clause = strings.ReplaceAll(clause, a, " #"+string(rune('a'+slices.Index(atoms, a)))+" ")
	}
	clause = strings.NewReplacer("(", " ( ", ")", " ) ").Replace(clause)
	tokens := strings.Fields(clause)
	for _, tok := range tokens {
		if !slices.Contains([]string{"(", ")", "AND", "OR", "NOT"}, tok) && !strings.HasPrefix(tok, "#") {
			t.Fatalf("unexpected %q in filter:\n%s", tok, clause)
		}
	}
	return func(v ...bool) bool {
		pos := 0
		var or func() bool
		unary := func() bool {
			negate := false
			for tokens[pos] == "NOT" {
				negate, pos = !negate, pos+1
			}
			var val bool
			if tokens[pos] == "(" {
				pos++
				val = or()
				pos++
			} else {
				val = v[tokens[pos][1]-'a']
				pos++
			}
			return val != negate
		}
		and := func() bool {
			val := unary()
			for pos < len(tokens) && tokens[pos] == "AND" {
				pos++
				val = unary() && val
			}
			return val
		}
		or = func() bool {
			val := and()
			for pos < len(tokens) && tokens[pos] == "OR" {
				pos++
				val = and() || val
			}
			return val
		}
		return or()
	}
}

// sameLogic reports the first assignment where got and want disagree.
func sameLogic(t *testing.T, n int, got, want func(v ...bool) bool) {
	t.Helper()
	for mask := range 1 << n {
		v := make([]bool, n)
		for i := range v {
			v[i] = mask&(1<<i) != 0
		}
		if got(v...) != want(v...) {
			t.Errorf("filter is %v for atoms %v, want %v", got(v...), v, want(v...))
			return
		}
	}
}
