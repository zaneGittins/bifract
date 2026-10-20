package parser

import (
	"strings"
	"testing"
	"time"
)

// selectAliases returns the output column names of every SELECT list in sql.
func selectAliases(sql string) [][]string {
	var lists [][]string
	upper := strings.ToUpper(sql)
	for start := 0; ; {
		i := strings.Index(upper[start:], "SELECT ")
		if i < 0 {
			return lists
		}
		pos := start + i + len("SELECT ")
		var cols []string
		depth, quote, from := 0, byte(0), pos
		for j := pos; j < len(sql); j++ {
			c := sql[j]
			switch {
			case quote != 0:
				if c == '\\' {
					j++
				} else if c == quote {
					quote = 0
				}
			case c == '\'' || c == '`' || c == '"':
				quote = c
			case c == '(':
				depth++
			case c == ')':
				depth--
			case depth == 0 && c == ',':
				cols = append(cols, sql[from:j])
				from = j + 1
			case depth == 0 && strings.HasPrefix(upper[j:], " FROM "):
				cols = append(cols, sql[from:j])
				j = len(sql)
			}
		}
		var names []string
		for _, col := range cols {
			col = strings.TrimSpace(col)
			if k := strings.LastIndex(strings.ToUpper(col), " AS "); k >= 0 && !strings.ContainsAny(col[k:], "()'") {
				col = col[k+len(" AS "):]
			}
			names = append(names, strings.Trim(col, "`"))
		}
		lists = append(lists, names)
		start = pos
	}
}

// A name assigned again is one column: the later definition replaces the
// earlier from its position on, and what ran before it keeps the earlier value.
func TestReassignmentProjectsOnce(t *testing.T) {
	cases := []struct {
		name, query string
		want        []string
		notWant     []string
	}{
		{"computed over computed", `* | y := upper(x) | y=5 | y := lower(x)`,
			[]string{"upper(fields.`x`::String) = '5'", "lower(fields.`x`::String) AS y"},
			[]string{"upper(fields.`x`::String) AS y"}},
		{"computed over computed, no filter", `* | a := lower(b) | a := upper(b)`,
			[]string{"upper(fields.`b`::String) AS a"}, []string{"lower(fields.`b`::String) AS a"}},
		{"literal over literal", `* | a := "q" | x="1" | a := "z"`,
			[]string{"'z' AS a"}, []string{"'q' AS a"}},
		{"literal over transform", `* | lowercase(a) | a := "z"`,
			[]string{"'z' AS a"}, []string{"lower(fields.`a`::String) AS a"}},
		{"computed over transform", `* | lowercase(a) | a := lower(b)`,
			[]string{"lower(fields.`b`::String) AS a"}, []string{"lower(fields.`a`::String) AS a"}},
		{"transform over literal", `* | a := "z" | lowercase(a)`,
			[]string{"lower('z') AS a"}, []string{"'z' AS a"}},
		{"literal over an aggregate", `* | groupby(x) | _count > 5 | _count := 1`,
			[]string{"HAVING _count > 5", "1 AS _count"}, []string{"HAVING 1 > 5"}},
		{"computed over an aggregate", `* | groupby(x) | _count := _count * 2`,
			[]string{"toString(_count)) * 2 AS _count"}, []string{"fields.`_count`"}},
		{"literal over literal after aggregation", `* | groupby(x) | y := 1 | y := 2`,
			[]string{"2 AS y"}, nil},
		{"self-reference reads the earlier value", `* | y := upper(x) | y := lower(y)`,
			[]string{"lower((upper(fields.`x`::String))) AS y"}, nil},
		{"self-reference reads a transform", `* | lowercase(x) | x := concat(x, "!")`,
			[]string{"concat((lower(fields.`x`::String)), '!') AS x"}, nil},
		{"self-reference with no earlier value reads the log field", `* | x := x * 100`,
			[]string{"toFloat64OrNull(fields.`x`::String) * 100 AS x"}, nil},
		{"reference before a reassignment keeps the earlier value", `* | lowercase(a) | c := concat(a, "x") | a := "z"`,
			[]string{"concat((lower(fields.`a`::String)), 'x') AS c", "'z' AS a"}, nil},
		{"literal over a grouping key", `* | a := lower(b) | groupby(a) | a := "q"`,
			[]string{"lower(fields.`b`::String) AS a", "GROUP BY a", "'q' AS a"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := ParseQuery(tc.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			res, err := TranslateToSQLWithOrder(pl, QueryOptions{
				StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
				MaxRows:   100,
				FractalID: "f1",
			})
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			for _, names := range append(selectAliases(res.SQL), res.FieldOrder) {
				seen := map[string]bool{}
				for _, n := range names {
					if seen[n] {
						t.Errorf("%q projected twice in %v:\n%s", n, names, res.SQL)
					}
					seen[n] = true
				}
			}
			for _, w := range tc.want {
				if !strings.Contains(res.SQL, w) {
					t.Errorf("missing %q:\n%s", w, res.SQL)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(res.SQL, w) {
					t.Errorf("must not contain %q:\n%s", w, res.SQL)
				}
			}
		})
	}
}
