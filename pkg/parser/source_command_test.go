package parser

import (
	"strings"
	"testing"
)

// A source command is scoped only by its arguments, so a filter written before it
// is ignored. That is refused however the filter is written, never dropped.
func TestSourceCommandRefusesAFilterBeforeIt(t *testing.T) {
	for _, q := range []string{
		`level=error | pgr(start="g1")`,
		`* | level=error | pgr(start="g1")`,
		`* | (a="x" OR cidr(ip,"10.0.0.0/8")) | pgr(start="g1")`,
	} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Fatalf("parse %s: %v", q, err)
		}
		if err := ValidateSourceCommandPlacement(pl); err == nil || !strings.Contains(err.Error(), "must be the first command") {
			t.Errorf("%s: want the placement error, got %v", q, err)
		}
	}
	pl, err := ParseQuery(`pgr(start="g1") | level=error`)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSourceCommandPlacement(pl); err != nil {
		t.Errorf("a filter after the source command is fine: %v", err)
	}
}
