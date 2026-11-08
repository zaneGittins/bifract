package parser

import (
	"fmt"
	"strings"
	"testing"
)

// An expression is lexed only as far as it reaches, so text after it can neither
// fail it nor cost anything: a query of n expressions parses in time linear in n.
func TestExpressionLexesOnlyItself(t *testing.T) {
	src := []rune(`lower(a) = "x" | @ never lexed`)
	expr, end, err := ParseExpressionAt(src, 0)
	if err != nil {
		t.Fatalf("text after the expression failed it: %v", err)
	}
	if expr.String() == "" || string(src[:end]) != `lower(a) = "x"` {
		t.Errorf("expression ends at %d: %q", end, string(src[:end]))
	}
}

// A chain of n operands is one node with n children, built without copying the
// chain at each operator.
func TestLongChainIsOneNode(t *testing.T) {
	pl, err := ParseQuery(longOrChain(2000))
	if err != nil {
		t.Fatal(err)
	}
	if f := pl.Filters(); len(f) != 1 || f[0].Op != CondOr || len(f[0].Children) != 2000 {
		t.Fatalf("want one OR of 2000, got %d filters", len(f))
	}
}

func BenchmarkParseLongOrChain(b *testing.B) {
	q := longOrChain(2000)
	for b.Loop() {
		if _, err := ParseQuery(q); err != nil {
			b.Fatal(err)
		}
	}
}

func longOrChain(n int) string {
	var b strings.Builder
	for i := range n {
		if i > 0 {
			b.WriteString(" OR ")
		}
		fmt.Fprintf(&b, `lower(f%d)="v%d"`, i, i)
	}
	return b.String()
}
