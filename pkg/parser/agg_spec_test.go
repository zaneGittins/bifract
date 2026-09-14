package parser

import "testing"

// aggSpecNames decides whether a call in an argument is an aggregate or a typo,
// and processAggSpec decides what it renders. A name in one and not the other is
// how skew() and kurt() came to be accepted in one place and rejected in the
// other, so the two are checked against each other here.
func TestEveryAggregateNameRenders(t *testing.T) {
	for name := range aggSpecNames {
		if name == "multi" {
			continue // a wrapper, expanded by applyAggSpecs
		}
		var selects []string
		spec := &AggSpec{
			Name: name,
			Args: []Argument{{Kind: ArgExpr, Expr: &ExprNode{Kind: ExprField, Value: "bytes"}}},
		}
		ok, err := processAggSpec(spec, &selects, map[string]bool{}, NewFieldRegistry(SourceHot, nil))
		if err != nil {
			t.Errorf("%s(): %v", name, err)
			continue
		}
		if !ok || len(selects) == 0 {
			t.Errorf("%s() is listed as an aggregate but renders nothing", name)
		}
	}
}
