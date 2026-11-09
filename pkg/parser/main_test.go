package parser

import (
	"os"
	"strings"
	"testing"
)

// Every plan any test renders is checked for a column whose alias was written
// into its expression: Name() would read the whole text, and the column would be
// referenced by it. Every predicate claimed as one operand is checked for a
// top-level OR, which would re-associate with the conjuncts beside it.
func TestMain(m *testing.M) {
	atomHook = func(sql string) {
		if hasTopLevelOR(sql) {
			panic("predicate with a top-level OR claimed as one operand: " + sql)
		}
	}
	unrecordedHook = func(name string) {
		panic("read before what it reads was recorded: " + name)
	}
	plannedHook = checkRecordedReads
	renderHook = func(p *QueryPlan) {
		layers := []QueryLayer{{Selects: p.format.selects}}
		for _, st := range p.Stages {
			layers = append(layers, st.Layer)
		}
		layers = append(layers, p.window.layers...)
		for _, l := range layers {
			for _, sel := range l.Selects {
				if sel.Alias == "" && topLevelAS(sel.Expr) {
					panic("select with its alias inside Expr: " + sel.Expr)
				}
			}
		}
	}
	os.Exit(m.Run())
}

// topLevelAS reports whether e has an AS outside brackets and quotes.
func topLevelAS(e string) bool {
	depth := 0
	for i := 0; i < len(e); i++ {
		switch c := e[i]; {
		case c == '\'' || c == '`':
			i = skipQuoted(e, i) - 1
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			depth--
		case depth == 0 && i+4 <= len(e) && strings.EqualFold(e[i:i+4], " AS "):
			return true
		}
	}
	return false
}

// skipQuoted returns the index just past the quoted run opening at i, honouring
// backslash escapes and doubled quotes.
func skipQuoted(sql string, i int) int {
	q := sql[i]
	for i++; i < len(sql); i++ {
		switch sql[i] {
		case '\\':
			i++
		case q:
			if i+1 < len(sql) && sql[i+1] == q {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(sql)
}
