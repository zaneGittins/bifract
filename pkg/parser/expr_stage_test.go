package parser

import (
	"strings"
	"testing"
)

// A filter leaf compiles the same wherever it sits in a stage, so a stage that
// starts with an expression must not read its other leaves as expressions.
func TestExprFirstStageLeavesCompileAsFilters(t *testing.T) {
	cases := []struct{ name, query, wantExpr, want string }{
		{"field compare after expression", `* | lower(x)="y" AND a=1`,
			"lower(fields.`x`::String) = 'y'", "fields.`a`::String = '1'"},
		{"field compare before expression", `* | a=1 AND lower(x)="y"`,
			"lower(fields.`x`::String) = 'y'", "fields.`a`::String = '1'"},
		{"bare string after expression", `* | lower(x)="y" AND "foo"`,
			"lower(fields.`x`::String) = 'y'", "match(lower(norm_log), 'foo')"},
		{"negated field after expression", `* | lower(x)="y" AND NOT a=1`,
			"lower(fields.`x`::String) = 'y'", "fields.`a`::String != '1'"},
		{"field compare after binding expression", `let &n := len(x); * | &n > 3 AND a=1`,
			"length(fields.`x`::String) > 3", "fields.`a`::String = '1'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := positionSQL(t, tc.query)
			for _, want := range []string{tc.wantExpr, tc.want} {
				if !strings.Contains(sql, want) {
					t.Errorf("want %q in: %s", want, sql)
				}
			}
			if strings.Contains(sql, "toFloat64OrNull(fields.`a`") {
				t.Errorf("a filter leaf compiled as a numeric expression: %s", sql)
			}
		})
	}
}

// Leaves split out of an expression-led stage keep AND binding tighter than OR.
func TestExprFirstStageKeepsPrecedence(t *testing.T) {
	sql := positionSQL(t, `* | lower(x)="y" OR a=1 AND b=2`)
	want := "((lower(fields.`x`::String) = 'y') OR fields.`a`::String = '1' AND fields.`b`::String = '2')"
	if !strings.Contains(sql, want) {
		t.Errorf("want %q in: %s", want, sql)
	}
}

// A bracket followed by a comparison or arithmetic operator is an expression
// leaf, not a condition group, in any position.
func TestBracketedExpressionIsALeaf(t *testing.T) {
	const leaf = "length(fields.`a`::String) + 1 > 2"
	cases := []struct{ query, other string }{
		{`* | lower(x)="y" AND (len(a) + 1) > 2`, "lower(fields.`x`::String) = 'y'"},
		{`* | (len(a) + 1) > 2 AND lower(x)="y"`, "lower(fields.`x`::String) = 'y'"},
		{`* | (len(a) + 1) > 2`, "fractal_id = 'f1' AND (" + leaf + ")"},
		{`(len(a) + 1) > 2 x="y"`, "fields.`x`::String = 'y'"},
		{`* | NOT (len(a) + 1) > 2 OR b=1`, "NOT (" + leaf + ") OR fields.`b`::String = '1'"},
	}
	for _, tc := range cases {
		sql := positionSQL(t, tc.query)
		for _, want := range []string{leaf, tc.other} {
			if !strings.Contains(sql, want) {
				t.Errorf("%s: want %q in: %s", tc.query, want, sql)
			}
		}
	}
	if sql := positionSQL(t, `* | (a=1 OR b=2) c=3`); !strings.Contains(sql, "fields.`a`::String = '1' OR fields.`b`::String = '2'") {
		t.Errorf("a condition group stays a group: %s", sql)
	}
}

// Expression syntax in a filter leaf is refused with a hint on how to write it,
// the same in first position and after an expression.
func TestExpressionSyntaxInFilterLeafHints(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | n + 1 > 2`, "bracket it, for example (bytes_in + bytes_out) > 1000"},
		{`* | lower(x)="a" AND n * 2 > 4`, "bracket it"},
		{`n / 2 > 1`, "bracket it"},
		{`* | b = lower(c)`, "write the expression first, for example lower(...) = b"},
		{`* | lower(x)="a" AND b = lower(c)`, "write the expression first"},
		{`* | a > lower(c)`, "for example lower(...) < a"},
		{`* | a != lower(c)`, "for example lower(...) != a"},
	}
	for _, tc := range cases {
		_, err := ParseQuery(tc.query)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: want error containing %q, got %v", tc.query, tc.want, err)
		}
	}
}

// Errors that are not arithmetic, or calls that are not expression functions,
// keep the plain message rather than a hint that would mislead.
func TestNoMisleadingHints(t *testing.T) {
	cases := []struct{ query, notWant string }{
		{`cmd /c tasklist | findstr`, "arithmetic"},
		{`cmd /c tasklist = 1`, "arithmetic"},
		{`* | cmd /c x = y`, "arithmetic"},
		{`a - b, c = 1`, "arithmetic"},
		{`* | b = foo(c)`, "write the expression first"},
		{`* | b = cidr(c)`, "write the expression first"},
		{`* | (a=1 OR b=2) *`, "in expression"},
	}
	for _, tc := range cases {
		_, err := ParseQuery(tc.query)
		if err == nil || strings.Contains(err.Error(), tc.notWant) {
			t.Errorf("%s: want an error without %q, got %v", tc.query, tc.notWant, err)
		}
	}
}

// A condition compared with text is refused before it reaches ClickHouse, which
// would reject the UInt8 = String comparison at query time.
func TestConditionComparedWithTextIsRefused(t *testing.T) {
	for _, q := range []string{`* | (a=1 OR b=2) = "x"`, `* | lower(x)="y" = "x"`, `* | (a=1) != "x"`} {
		pl, err := ParseQuery(q)
		if err == nil {
			_, err = TranslateToSQLWithOrder(pl, revOpts())
		}
		if err == nil || !strings.Contains(err.Error(), "compare a condition to true or false") {
			t.Errorf("%s: want a type error, got %v", q, err)
		}
	}
	if sql := positionSQL(t, `* | isPrivateIP(a) = false`); !strings.Contains(sql, "= 0") {
		t.Errorf("a condition compared with a boolean still compiles: %s", sql)
	}
}
