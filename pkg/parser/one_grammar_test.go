package parser

import (
	"strings"
	"testing"
)

// BQL has one condition grammar: a filter means the same thing before the first
// pipe as after `* |`, down to the SQL it compiles to.
func TestLeadingAndStageFormsAreOneGrammar(t *testing.T) {
	for _, q := range []string{
		`user="admin"`,
		`user!="admin"`,
		`NOT user="admin"`,
		`a="1" b="2"`,
		`a="1" AND b="2"`,
		`a="1" OR b="2"`,
		`(a="1" OR b="2") AND c="3"`,
		`NOT (a="1" OR b="2")`,
		`a="1" OR (b="2" AND NOT c="3")`,
		`"error"`,
		`/denied/i`,
		`"error" OR /denied/`,
		`image=/powershell/i`,
		`image=~cmd,pwsh`,
		`image=^"C:\\Windows"`,
		`image=$".exe"`,
		`event_id="4624","4625"`,
		`event_id!="4624","4625"`,
		`bytes > 1000`,
		`bytes <= -5`,
		`image=*`,
		`image!=*`,
		`src_port = field(dst_port)`,
		`a="x" OR cidr(dst_ip,"10.0.0.0/8")`,
		`a="x" OR !cidr(dst_ip,"10.0.0.0/8")`,
		`(a="x" OR in(user,["u"])) AND b="y"`,
		`lower(image)="cmd.exe"`,
		`lower(image)="cmd.exe" OR len(commandline) > 500`,
		`lower(image) =~ "cmd","ps"`,
		`lower(image)="cmd.exe" OR user=~admin`,
		`user="a" | groupby(host)`,
		`user="a" | b="c"`,
		`user="a" | cidr(src_ip,"10.0.0.0/8") | b="c"`,
	} {
		t.Run(q, func(t *testing.T) {
			lead, stage := positionSQL(t, q), positionSQL(t, "* | "+q)
			if lead != stage {
				t.Errorf("forms differ:\nleading: %s\nstage:   %s", lead, stage)
			}
		})
	}
}

// Expression leaves end at AND/OR and take a trailing match operator, wherever
// they sit: the filter grammar's operators follow them as they follow a field.
func TestExpressionLeafBeforeFilterSyntax(t *testing.T) {
	cases := map[string][]string{
		`lower(ofn)="a" OR image=~foo`:                    {"lower(fields.`ofn`::String) = 'a'", "multiSearchAnyCaseInsensitive(fields.`image`::String, ['foo'])"},
		`lower(ofn)="a" AND image=/foo/`:                  {"lower(fields.`ofn`::String) = 'a'", "match(fields.`image`::String, 'foo')"},
		`len(cmd) > 5 OR image=^"c:"`:                     {"length(fields.`cmd`::String) > 5", "startsWith(lower(fields.`image`::String), 'c:')"},
		`lower(image) =~ "cmd","ps"`:                      {"multiSearchAnyCaseInsensitive(lower(fields.`image`::String), ['cmd', 'ps'])"},
		`b="1" AND (lower(a)="x" OR image=~y)`:            {"lower(fields.`a`::String) = 'x'", "multiSearchAnyCaseInsensitive(fields.`image`::String, ['y'])"},
		`* | chain(host) { lower(x)="y" OR a=~z; b="2" }`: {"lower(fields.`x`::String) = 'y'", "multiSearchAnyCaseInsensitive(fields.`a`::String, ['z'])"},
		`* | image=*powershell*`:                          {"fields.`image`::String = '*powershell*'"},
	}
	for q, wants := range cases {
		t.Run(q, func(t *testing.T) {
			sql := positionSQL(t, q)
			for _, w := range wants {
				if !strings.Contains(sql, w) {
					t.Errorf("missing %q:\n%s", w, sql)
				}
			}
		})
	}
}

// AND and OR join two operands; one with nothing on its left is a mistake to
// report, not to read as if it were absent.
func TestConnectorNeedsALeftOperand(t *testing.T) {
	for _, q := range []string{`AND`, `OR a=1`, `* | AND a=1`, `a=1 | OR b=2`} {
		if _, err := ParseQuery(q); err == nil {
			t.Errorf("%s: parsed", q)
		}
	}
	for _, q := range []string{`a=1 AND b=2`, `a=1 | b=2 OR c=3`, `cidr(ip,"10.0.0.0/8") OR a=1`} {
		if _, err := ParseQuery(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// A field with no operator, and a NOT with nothing to negate, get an error that
// names what is missing rather than reading the field as a command.
func TestIncompleteConditionErrors(t *testing.T) {
	for q, want := range map[string]string{
		`user`:          "expected comparison operator",
		`a=1 b`:         "expected comparison operator",
		`NOT`:           "NOT needs a condition",
		`a=1 | NOT | b`: "NOT needs a condition",
	} {
		_, err := ParseQuery(q)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want %q, got %v", q, want, err)
		}
	}
}

// A stage is one boolean expression with SQL precedence, however its operands
// were written (fields, condition functions, expressions): AND binds tighter
// than OR across all of them.
func TestStagePrecedenceAcrossOperandKinds(t *testing.T) {
	lowerX, a, b := "lower(fields.`x`::String) = 'y'", "fields.`a`::String = '1'", "fields.`b`::String = '2'"
	andOr := func(v ...bool) bool { return v[0] && v[1] || v[2] }
	cases := []struct {
		query string
		atoms []string
		want  func(v ...bool) bool
	}{
		{`lower(x)="y" a="1" OR b="2"`, []string{lowerX, a, b}, andOr},
		{`in(user,["u"]) AND a="1" OR b="2"`, []string{"fields.`user`::String IN ('u')", a, b}, andOr},
		{`a="1" OR b="2" AND lower(x)="y"`, []string{a, b, lowerX}, func(v ...bool) bool { return v[0] || v[1] && v[2] }},
	}
	for _, tc := range cases {
		for _, form := range []string{tc.query, "* | " + tc.query} {
			t.Run(form, func(t *testing.T) {
				sameLogic(t, len(tc.atoms), scanLogic(t, positionSQL(t, form), tc.atoms...), tc.want)
			})
		}
	}
}

// A connector with nothing after it before a pipe joins nothing; the next
// stage is still AND-ed on, as it always was.
func TestDanglingConnectorDoesNotJoinTheNextStage(t *testing.T) {
	for _, q := range []string{`a="1" OR | b="2"`, `* | a="1" OR | b="2"`} {
		sql := positionSQL(t, q)
		if strings.Contains(sql, "'1' OR ") {
			t.Errorf("%s: the dangling OR reached the next stage:\n%s", q, sql)
		}
	}
	if _, err := ParseQuery(`a="1" OR | groupby(h)`); err != nil {
		t.Errorf("a dangling OR before a command: %v", err)
	}
}

// NOT in front of an expression leaf takes the same trailing match operator the
// leaf takes anywhere else, and NOT NOT cancels.
func TestNegatedExpressionLeaf(t *testing.T) {
	sql := positionSQL(t, `NOT lower(x) =~ "a","b"`)
	if !strings.Contains(sql, "NOT (multiSearchAnyCaseInsensitive(lower(fields.`x`::String), ['a', 'b']))") {
		t.Errorf("negated match on an expression:\n%s", sql)
	}
	if a, b := positionSQL(t, `NOT NOT a="1"`), positionSQL(t, `a="1"`); a != b {
		t.Errorf("NOT NOT must cancel:\n%s\n%s", a, b)
	}
}

// Without a connector, any operand binds like AND, whatever it starts with.
func TestAdjacentOperandBindsLikeAnd(t *testing.T) {
	a, b, c, d := "fields.`a`::String = '1'", "fields.`b`::String = '2'", "fields.`c`::String = '3'", "fields.`d`::String = '4'"
	cNull, cNot := "fields.`c`::String IS NULL", "fields.`c`::String != '3'"
	orAnd := func(v ...bool) bool { return v[0] || v[1] && v[2] }
	orAndOr := func(v ...bool) bool { return v[0] || v[1] && (v[2] || v[3]) }
	cases := []struct {
		query string
		atoms []string
		want  func(v ...bool) bool
	}{
		{`a="1" OR b="2" NOT c="3"`, []string{a, b, cNull, cNot}, orAndOr},
		{`a="1" OR b="2" lower(x)="y"`, []string{a, b, "lower(fields.`x`::String) = 'y'"}, orAnd},
		{`a="1" OR b="2" "x"`, []string{a, b, "match(lower(norm_log), 'x')"}, orAnd},
		{`a="1" OR b="2" (c="3" OR d="4")`, []string{a, b, c, d}, orAndOr},
		{`a="1" OR b="2" c="3"`, []string{a, b, c}, orAnd},
		{`lower(x)="y" a="1" OR b="2"`, []string{"lower(fields.`x`::String) = 'y'", a, b}, func(v ...bool) bool { return v[0] && v[1] || v[2] }},
		{`lower(x)="y" "x" OR b="2"`, []string{"lower(fields.`x`::String) = 'y'", "match(lower(norm_log), 'x')", b}, func(v ...bool) bool { return v[0] && v[1] || v[2] }},
		{`lower(x)="y" (c="3" OR d="4") OR b="2"`, []string{"lower(fields.`x`::String) = 'y'", c, d, b}, func(v ...bool) bool { return v[0] && (v[1] || v[2]) || v[3] }},
		{`a="1" OR b="2" NOT c="3" OR d="4"`, []string{a, b, cNull, cNot, d},
			func(v ...bool) bool { return v[0] || v[1] && (v[2] || v[3]) || v[4] }},
	}
	for _, tc := range cases {
		for _, form := range []string{tc.query, "* | " + tc.query} {
			t.Run(form, func(t *testing.T) {
				sameLogic(t, len(tc.atoms), scanLogic(t, positionSQL(t, form), tc.atoms...), tc.want)
			})
		}
	}
}

// Grouping a stage's operands keeps every one at the position it was written.
func TestGroupedOperandsKeepTheirPosition(t *testing.T) {
	pl, err := ParseQuery(`* | head(1) | a="1" OR b="2" NOT c="3" OR d="4"`)
	if err != nil {
		t.Fatal(err)
	}
	var walk func(c HavingCondition)
	walk = func(c HavingCondition) {
		if c.CmdIndex != 1 {
			t.Errorf("%s at command %d, want 1", describeCondition(c), c.CmdIndex)
		}
		for _, child := range c.Children {
			walk(child)
		}
	}
	for _, c := range pl.HavingConditions {
		walk(c)
	}
}
