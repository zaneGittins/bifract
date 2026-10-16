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
	norm := strings.NewReplacer("(", "", ")", "")
	for q, want := range map[string]string{
		`lower(x)="y" a="1" OR b="2"`:       "lowerfields.`x`::String = 'y' AND fields.`a`::String = '1' OR fields.`b`::String = '2'",
		`in(user,["u"]) AND a="1" OR b="2"`: "fields.`user`::String IN 'u' AND fields.`a`::String = '1' OR fields.`b`::String = '2'",
		`a="1" OR b="2" AND lower(x)="y"`:   "fields.`a`::String = '1' OR fields.`b`::String = '2' AND lowerfields.`x`::String = 'y'",
	} {
		for _, form := range []string{q, "* | " + q} {
			sql := positionSQL(t, form)
			// Bracket-free text, so check precedence by the brackets SQL itself
			// would need: the clause must equal the bracket-free reading.
			// The broken grouping brackets the OR on its own after another operand
			// ('y') or ('u'), or brackets it before one.
			grouped := strings.Contains(sql, "') AND (fields.`a`::String = '1' OR") ||
				strings.Contains(sql, "'2') AND (lower(")
			if !strings.Contains(norm.Replace(sql), want) || grouped {
				t.Errorf("%s: wrong grouping:\n%s", form, sql)
			}
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

// Without a connector, a condition starting with a field binds like AND, while
// any other operand ANDs with everything written before it in the stage.
func TestAdjacentOperandIsANewConjunct(t *testing.T) {
	for q, want := range map[string]string{
		`a="1" OR b="2" NOT c="3"`:                "(fields.`a`::String = '1' OR fields.`b`::String = '2') AND (fields.`c`::String IS NULL",
		`a="1" OR b="2" lower(x)="y"`:             "(fields.`a`::String = '1' OR fields.`b`::String = '2') AND (lower(",
		`a="1" OR b="2" (c="3" OR d="4")`:         "(fields.`a`::String = '1' OR fields.`b`::String = '2') AND (fields.`c`::String = '3' OR",
		`a="1" OR b="2" "x"`:                      "(fields.`a`::String = '1' OR fields.`b`::String = '2') AND match(",
		`a="1" OR b="2" c="3"`:                    "fields.`a`::String = '1' OR fields.`b`::String = '2' AND fields.`c`::String = '3'",
		`lower(x)="y" a="1" OR b="2"`:             "(lower(fields.`x`::String) = 'y') AND fields.`a`::String = '1' OR fields.`b`::String = '2'",
		`a="1" b="2" OR c="3" NOT d="4" OR e="5"`: "(fields.`a`::String = '1' AND fields.`b`::String = '2' OR fields.`c`::String = '3') AND ((fields.`d`::String IS NULL OR fields.`d`::String != '4') OR fields.`e`::String = '5')",
	} {
		for _, form := range []string{q, "* | " + q} {
			if sql := positionSQL(t, form); !strings.Contains(sql, want) {
				t.Errorf("%s: want %s in:\n%s", form, want, sql)
			}
		}
	}
}
