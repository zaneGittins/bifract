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

// A condition function is an operand in every position filters appear, not just
// after a pipe or at the head of an expression.
func TestConditionFunctionOperandPositions(t *testing.T) {
	for _, q := range []string{
		`a="x" OR cidr(dst_ip,"10.0.0.0/8")`,           // right of OR, pre-pipe
		`a="x" AND cidr(dst_ip,"10.0.0.0/8")`,          // right of AND, pre-pipe
		`cidr(dst_ip,"10.0.0.0/8") OR a="x"`,           // left of OR, pre-pipe
		`(a="x" OR cidr(dst_ip,"10.0.0.0/8")) | b="y"`, // inside parentheses
		`a="x" OR !cidr(dst_ip,"10.0.0.0/8")`,          // negated operand
		`a="x" OR in(user,["u"])`,                      // a different function
		`a="x" | cidr(dst_ip,"10.0.0.0/8") OR b="y"`,   // post-pipe
	} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Errorf("parse %s: %v", q, err)
			continue
		}
		res, err := TranslateToSQLWithOrder(pl, QueryOptions{FractalID: "f"})
		if err != nil {
			t.Errorf("translate %s: %v", q, err)
			continue
		}
		if !strings.Contains(res.SQL, "isIPAddressInRange") && !strings.Contains(res.SQL, " IN (") {
			t.Errorf("%s: predicate missing from SQL: %s", q, res.SQL)
		}
	}
}

// A command used as an operand is still a command: anything that searches for one
// by name must find it wherever it sits, or a gate silently stops applying.
func TestForEachCommandFindsOperands(t *testing.T) {
	cases := map[string]string{
		"standalone":     `cidr(dst_ip,"10.0.0.0/8")`,
		"piped":          `a="x" | cidr(dst_ip,"10.0.0.0/8")`,
		"operand right":  `a="x" OR cidr(dst_ip,"10.0.0.0/8")`,
		"operand left":   `cidr(dst_ip,"10.0.0.0/8") OR a="x"`,
		"in parentheses": `(a="x" OR cidr(dst_ip,"10.0.0.0/8"))`,
		"post pipe":      `a="x" | b="y" OR cidr(dst_ip,"10.0.0.0/8")`,
	}
	for name, q := range cases {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Errorf("%s: parse: %v", name, err)
			continue
		}
		var found bool
		ForEachCommand(pl, func(cmd CommandNode) {
			if cmd.Name == "cidr" {
				found = true
			}
		})
		if !found {
			t.Errorf("%s: cidr() not discoverable in %q", name, q)
		}
	}
}

// comment() resolves its log ids server-side by being found in the pipeline. As an
// operand it must still be detected, or the pre-fetch silently never happens.
func TestCommentDetectedAsOperand(t *testing.T) {
	for _, q := range []string{
		`comment()`,
		`a="x" | comment()`,
		`a="x" OR comment()`,
		`comment() OR a="x"`,
	} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Fatalf("parse %s: %v", q, err)
		}
		if _, _, found := ExtractCommentParams(pl); !found {
			t.Errorf("comment() not detected in %q", q)
		}
	}
}

// The archive gate must reject an unsupported command used as an operand, or the
// query passes validation and then emits SQL the archive cannot run.
func TestArchiveGateSeesOperands(t *testing.T) {
	pl, err := ParseQuery(`a="x" OR cidr(dst_ip,"10.0.0.0/8")`)
	if err != nil {
		t.Fatal(err)
	}
	if err := icebergSupportedFeatures(pl); err == nil {
		t.Error("cidr() as an operand must be caught by the archive allowlist")
	}
}
