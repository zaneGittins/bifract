package query

import (
	"testing"

	"bifract/pkg/dictionaries"
	"bifract/pkg/parser"
)

// A pattern list carries a constant marker attribute so match() can report
// membership without dictHas, which REGEXP_TREE does not support. The dictionary
// package declares it and the parser reads it, and they name it separately
// because the parser cannot import the dictionary package without a cycle. If the
// two ever disagree, membership reads an attribute the dictionary never declared
// and every lookup silently misses.
func TestPatternMatchAttrAgrees(t *testing.T) {
	if parser.PatternMatchAttr != dictionaries.PatternMatchAttr {
		t.Errorf("parser names it %q, dictionaries names it %q",
			parser.PatternMatchAttr, dictionaries.PatternMatchAttr)
	}
}
