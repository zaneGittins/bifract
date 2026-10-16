package parser

import (
	"strings"
	"testing"
)

// A case branch and a chain step are sub-pipelines, and a condition in one means
// what it means in a query, wherever in the sub-pipeline it is written: a
// dropped condition widens the branch or step to every row.
func TestSubPipelineConditionsAreKept(t *testing.T) {
	cases := []struct {
		name, query string
		want        []string
	}{
		{"case branch condition after a negated function",
			`* | case { !in(prog,["sshd"]) AND level="error" | tier:="x"; * | tier:="y" }`,
			[]string{"NOT (fields.`prog`::String IN ('sshd'))", "fields.`level`::String = 'error'"}},
		{"case branch stage condition",
			`* | case { level="error" | in(prog,["sshd"]) | tier:="x"; * | tier:="y" }`,
			[]string{"fields.`level`::String = 'error'", "IN ('sshd')"}},
		{"chain step expression stage",
			`* | chain(host) { a="1" | lower(x)="y"; b="2" }`,
			[]string{"lower(fields.`x`::String) = 'y'"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := positionSQL(t, tc.query)
			if strings.Contains(sql, "WHEN  THEN") {
				t.Fatalf("empty branch condition:\n%s", sql)
			}
			for _, w := range tc.want {
				if !strings.Contains(sql, w) {
					t.Errorf("missing %q:\n%s", w, sql)
				}
			}
		})
	}
}
