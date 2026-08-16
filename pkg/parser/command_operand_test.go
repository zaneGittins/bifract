package parser

import (
	"strings"
	"testing"
)

// A condition function is a boolean operand wherever filters are. Before this,
// OR between two of them was silently dropped and the predicates were ANDed,
// producing an always-false query with no error.
func TestConditionFunctionsAsBooleanOperands(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantJoiner string // the operator that must appear between the two predicates
	}{
		{"or of two cidr", `* | cidr(dst_ip,"10.0.0.0/8") OR cidr(dst_ip,"192.168.0.0/16")`, "OR"},
		{"or of mixed functions", `* | in(user,["a"]) OR cidr(src_ip,"10.0.0.0/8")`, "OR"},
		{"explicit and", `* | cidr(dst_ip,"10.0.0.0/8") AND cidr(src_ip,"192.168.0.0/16")`, "AND"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := ParseQuery(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
			if err != nil {
				t.Fatal(err)
			}
			// Both predicates present, joined by the requested operator.
			if strings.Count(res.SQL, "isIPAddressInRange")+strings.Count(res.SQL, " IN (") < 2 {
				t.Errorf("both predicates must survive: %s", res.SQL)
			}
			if !strings.Contains(res.SQL, ") "+tc.wantJoiner+" (") {
				t.Errorf("predicates must be joined by %s: %s", tc.wantJoiner, res.SQL)
			}
		})
	}
}

// Piped and standalone functions keep their existing conjunctive meaning; only
// explicit boolean operators change anything.
func TestConditionFunctionsWithoutOperatorsUnchanged(t *testing.T) {
	for _, q := range []string{
		`* | cidr(dst_ip,"10.0.0.0/8")`,
		`* | cidr(dst_ip,"10.0.0.0/8") | in(user,["a"])`,
		`status=500 | cidr(dst_ip,"10.0.0.0/8")`,
	} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if !strings.Contains(res.SQL, "isIPAddressInRange") {
			t.Errorf("%s lost its predicate: %s", q, res.SQL)
		}
	}
}

// Negation must bind to the function, not to the whole expression.
func TestNegatedConditionFunctionAsOperand(t *testing.T) {
	pl, err := ParseQuery(`* | !cidr(dst_ip,"10.0.0.0/8") OR cidr(src_ip,"192.168.0.0/16")`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.SQL, "NOT (") {
		t.Errorf("negation must survive: %s", res.SQL)
	}
	if !strings.Contains(res.SQL, " OR (") {
		t.Errorf("OR must survive alongside the negation: %s", res.SQL)
	}
}

// The error a non-predicate function produces must be reported, not swallowed.
func TestNonPredicateFunctionAsOperandRejected(t *testing.T) {
	pl, err := ParseQuery(`* | groupby(user) OR cidr(dst_ip,"10.0.0.0/8")`)
	if err != nil {
		return // rejected at parse time is fine
	}
	if _, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"}); err == nil {
		t.Fatal("a reshaping function cannot be a boolean operand")
	}
}
