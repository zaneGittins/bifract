package parser

import (
	"strings"
	"testing"
)

// A column's name is held apart from its expression, so text inside the
// expression, " AS " in a string literal included, never changes either.
func TestLiteralASStaysInTheExpression(t *testing.T) {
	for q, want := range map[string]string{
		`* | x := concat(a, " AS b") | x="foo"`:            "concat(fields.`a`::String, ' AS b') = 'foo'",
		`* | sprintf("%s AS %s", a, b, as=y) | y="z"`:      "printf('%s AS %s', ifNull(fields.`a`::String, ''), ifNull(fields.`b`::String, '')) = 'z'",
		`* | x := concat(a, " as b") | table(x) | x="foo"`: "' as b') AS x",
	} {
		if sql := positionSQL(t, q); !strings.Contains(sql, want) {
			t.Errorf("%s: want %s in:\n%s", q, want, sql)
		}
	}
}

// Each aggregate scopes itself to a case branch, rather than having its SQL
// rewritten as text: mad() is two nested aggregates, and top(percent=true) and
// iqr() are expressions over aggregates.
func TestCaseBranchAggregatesScopeThemselves(t *testing.T) {
	for q, want := range map[string][]string{
		`* | case { a=1 | multi([mad(bytes)]); * | t:="0" }`: {"groupArrayIf(toFloat64OrNull(fields.`bytes`::String), fields.`a`::String = '1')"},
		`* | case { a=1 | top(user, percent=true); * | t:="0" }`: {
			"approx_top_kIf(10)(fields.`user`::String, fields.`a`::String = '1')",
			"countIf(fields.`a`::String = '1')",
		},
		`* | case { a=1 | iqr(bytes); * | t:="0" }`: {"quantileIf(0.75)(", "- quantileIf(0.25)("},
		`* | case { a=1 | count(); * | t:="0" }`:    {"countIf(fields.`a`::String = '1')"},
	} {
		sql := positionSQL(t, q)
		for _, w := range want {
			if !strings.Contains(sql, w) {
				t.Errorf("%s: want %s in:\n%s", q, w, sql)
			}
		}
		if strings.Contains(sql, "arrayReduceIf") || strings.Contains(sql, "arrayMapIf") {
			t.Errorf("%s: an expression was rewritten as an aggregate:\n%s", q, sql)
		}
	}
}

// selectFirst() and selectLast() over the event time name their column, and
// both forms scope to a case branch.
func TestSelectEdgeColumns(t *testing.T) {
	for q, want := range map[string]string{
		`* | selectfirst(timestamp) | x := _first`:             "SELECT min(timestamp) AS _first FROM logs",
		`* | selectlast(timestamp, as=newest)`:                 "max(timestamp) AS newest",
		`* | case { a=1 | selectfirst(user); * | t:="0" }`:     "argMinIf(fields.`user`::String, timestamp, fields.`a`::String = '1') AS first_1",
		`* | case { a=1 | selectlast(timestamp); * | t:="0" }`: "maxIf(timestamp, fields.`a`::String = '1') AS last_1",
	} {
		if sql := positionSQL(t, q); !strings.Contains(sql, want) {
			t.Errorf("%s: want %s in:\n%s", q, want, sql)
		}
	}
}
