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
