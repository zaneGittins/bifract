package parser

import (
	"strings"
	"testing"
)

// entropy() is one definition in every position: the command, an expression in
// eval(), and a condition operand all render the same SQL.
func TestEntropyRendersOneDefinition(t *testing.T) {
	want := "round(arrayReduce('entropy', ngrams(fields.`query`::String, 1)), 4)"
	for _, q := range []string{
		`* | entropy(query)`,
		`* | entropy(query, as=h) | h > 3.5`,
		`* | eval(h = entropy(query)) | h > 3.5`,
		`entropy(query) > 3.5`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			t.Fatalf("%s: parse: %v", q, err)
		}
		res, err := TranslateToSQLWithOrder(pipeline, serverOpts())
		if err != nil {
			t.Fatalf("%s: translate: %v", q, err)
		}
		if !strings.Contains(res.SQL, want) {
			t.Errorf("%s: want %s in:\n%s", q, want, res.SQL)
		}
	}
}
