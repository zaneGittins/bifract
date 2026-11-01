package parser

import (
	"fmt"
	"strings"
	"testing"
)

// The scan's scope guards (time range, fractal_id) are bare column references in
// the same SELECT as the query's projections. ClickHouse resolves an identifier in
// WHERE to a SELECT alias before a table column, so an output named after a scope
// column would turn the guard into a comparison against the user's own value:
// `* | eval(fractal_id="<mine>")` read every fractal.

// aliasShapes is every way a query can name an output column. %[1]s is the name.
var aliasShapes = []string{
	`* | eval(%[1]s="f1")`,
	`* | %[1]s := "f1"`,
	`* | groupby([%[1]s])`,
	`* | groupby(%[1]s)`,
	`* | groupby(a, function=count(as=%[1]s))`,
	`* | groupby(a, function=[count(as=%[1]s), sum(b)])`,
	`* | groupby(a, function=selectFirst(b, as=%[1]s))`,
	`* | count(as=%[1]s)`,
	`* | sum(b, as=%[1]s)`,
	`* | max(b, as=%[1]s)`,
	`* | top(field=a, as=%[1]s)`,
	`* | regex("(?<%[1]s>.*)", field=raw_log)`,
	`* | strftime("%%H", as=%[1]s)`,
	`* | sprintf("%%s", a, as=%[1]s)`,
	`* | head(5) | eval(%[1]s="f1")`,
	`* | dedup(a) | eval(%[1]s="f1")`,
	`* | eval(x="f1") | %[1]s := x`,
	`* | lowercase(a, %[1]s)`,
	`* | lowercase(field=a, output=%[1]s)`,
}

func TestScopeColumnsCannotBeShadowedByAnAlias(t *testing.T) {
	for _, opts := range []QueryOptions{isolationOpts(), prismOpts()} {
		for _, col := range scopeColumns {
			for _, shape := range aliasShapes {
				q := fmt.Sprintf(shape, col)
				pipeline, err := ParseQuery(q)
				if err != nil {
					t.Fatalf("%s: parse: %v", q, err)
				}
				sql, err := TranslateToSQL(pipeline, opts)
				if err == nil {
					t.Errorf("%s: translated, want an error naming the reserved column\n%s", q, sql)
					continue
				}
				if !strings.Contains(err.Error(), col) {
					t.Errorf("%s: error %q does not name %s", q, err, col)
				}
			}
		}
	}
}

// Reading a scope column is not shadowing it: the scan may project the column
// itself, and other outputs may be computed from it.
func TestScopeColumnsStayReadable(t *testing.T) {
	for _, q := range []string{
		`*`,
		`* | table([timestamp, log_id, fractal_id, a])`,
		`* | groupby(a)`,
		`* | eval(ts=timestamp)`,
		`* | strftime("%H", as=hour)`,
		`* | head(5)`,
		`* | regex("(?<user>.*)", field=raw_log)`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			t.Fatalf("%s: parse: %v", q, err)
		}
		if _, err := TranslateToSQL(pipeline, isolationOpts()); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}
