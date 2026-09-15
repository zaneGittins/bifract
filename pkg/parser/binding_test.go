package parser

import (
	"strings"
	"testing"
)

func bindingSQL(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %s: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, serverOpts())
	if err != nil {
		t.Fatalf("translate %s: %v", query, err)
	}
	return result.SQL
}

func TestBindingCompilesAsIfWrittenInline(t *testing.T) {
	cases := []struct{ bound, inline string }{
		{
			`let &lolbin = lower(image) =~ "rundll32.exe","mshta.exe"; * | &lolbin`,
			`* | lower(image) =~ "rundll32.exe","mshta.exe"`,
		},
		{
			`let &cmdlen = len(commandline); * | &cmdlen > 500`,
			`* | len(commandline) > 500`,
		},
		{
			`let &susp = lower(image); * | &susp = "cmd.exe"`,
			`* | lower(image) = "cmd.exe"`,
		},
		{
			`let &susp = lower(image); * | &susp =~ "cmd","ps"`,
			`* | lower(image) =~ "cmd","ps"`,
		},
		{
			`let &n = 500; * | len(commandline) > &n`,
			`* | len(commandline) > 500`,
		},
		{
			`let &cmdlen = len(commandline); * | x := &cmdlen * 2`,
			`* | x := len(commandline) * 2`,
		},
	}
	for _, tc := range cases {
		if got, want := bindingSQL(t, tc.bound), bindingSQL(t, tc.inline); got != want {
			t.Errorf("%s\n got: %s\nwant: %s", tc.bound, got, want)
		}
	}
}

// One binding used twice must compile to two independent copies.
func TestBindingUsedTwiceIsTwoCopies(t *testing.T) {
	sql := bindingSQL(t, `let &cmdlen = len(commandline); * | &cmdlen > 500 AND &cmdlen < 4000`)
	if n := strings.Count(sql, "length(fields.`commandline`::String)"); n != 2 {
		t.Errorf("want two copies of the bound expression, got %d: %s", n, sql)
	}
}

func TestBindingChainsWithOrdinaryConditions(t *testing.T) {
	for _, query := range []string{
		`let &l = lower(image) =~ "mshta.exe"; * | &l AND user = "bob"`,
		`let &l = lower(image) =~ "mshta.exe"; * | user = "bob" AND &l`,
		`let &c = lower(image) = "cmd.exe"; * | &c OR user = "bob"`,
		`let &l = lower(image) =~ "mshta.exe"; let &o = lower(parent_image) =~ "winword.exe"; * | &l AND &o`,
	} {
		sql := bindingSQL(t, query)
		// The tenant guard must stay joined to whatever the binding expanded into.
		if !strings.Contains(sql, "fractal_id = 'f1' AND ") {
			t.Errorf("%s: fractal guard is not joined: %s", query, sql)
		}
	}
}

func TestBindingNegates(t *testing.T) {
	sql := bindingSQL(t, `let &l = lower(image) =~ "mshta.exe"; * | NOT &l`)
	if !strings.Contains(sql, "NOT (multiSearchAnyCaseInsensitive") {
		t.Errorf("want the expansion negated, got: %s", sql)
	}
}

// A binding shares no namespace with log fields, so a typo is an error rather
// than a JSON lookup that matches nothing.
func TestUnknownBindingIsAnError(t *testing.T) {
	cases := []string{
		`* | &undeclared`,
		`let &a = 1; * | &b > 1`,
		`let &a = 1; * | table(&b)`,
		`let &a = 1; * | x := &b + 1`,
		`let &a = &a + 1; * | &a > 1`,
	}
	for _, query := range cases {
		if _, err := ParseQuery(query); err == nil {
			t.Errorf("%s: expected an unknown-binding error", query)
		} else if !strings.Contains(err.Error(), "unknown binding") {
			t.Errorf("%s: want an unknown-binding error, got: %v", query, err)
		}
	}
}

func TestBindingStatementErrors(t *testing.T) {
	cases := map[string]string{
		`let &a = 1; let &a = 2; * | &a > 1`:                        "already declared",
		`let &a = ; * | &a > 1`:                                     "has no value",
		`let &a = 1 * | &a > 1`:                                     "let &a",
		`let &admins = user_type="admin" | groupby(user); * | &a`:   "cannot be a pipeline yet",
		`let &a 1; * | &a > 1`:                                      "expected '='",
	}
	for query, want := range cases {
		_, err := ParseQuery(query)
		if err == nil {
			t.Errorf("%s: expected an error", query)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want an error mentioning %q, got: %v", query, want, err)
		}
	}
}

// & was a lex error before bindings existed, so nothing that parsed before can
// change meaning. =$ in particular still reads as ends-with-any.
func TestSigilDoesNotDisturbExistingSyntax(t *testing.T) {
	sql := bindingSQL(t, `image=$admins`)
	if !strings.Contains(sql, "endsWith(") {
		t.Errorf("=$ must still be ends-with-any, got: %s", sql)
	}
	if _, err := ParseQuery(`* | a & b`); err == nil {
		t.Error("a bare & is not a binding and must stay an error")
	}
}

// A column a command produces is named after the binding, not after the
// expression text, so table(&cmdlen) returns a column called cmdlen.
func TestBindingNamesTheColumnItProduces(t *testing.T) {
	sql := bindingSQL(t, `let &cmdlen = len(commandline); * | table(&cmdlen)`)
	if !strings.Contains(sql, "length(fields.`commandline`::String) AS cmdlen") {
		t.Errorf("want the column named after the binding, got: %s", sql)
	}
}

// A binding name is a plain identifier. &a.b is &a followed by .b, which no
// stage can consume, rather than one oddly named binding.
func TestBindingNameIsAPlainIdentifier(t *testing.T) {
	for _, query := range []string{`let &a.b = 1; * | &a.b > 1`, `let &a-b = 1; * | &a-b > 1`} {
		if _, err := ParseQuery(query); err == nil {
			t.Errorf("%s: expected a parse error", query)
		}
	}
}

// A query whose own field is called "let" must keep parsing as it did.
func TestLetIsOnlyAKeywordBeforeABinding(t *testing.T) {
	if _, err := ParseQuery(`let="x"`); err != nil {
		t.Errorf("a field named let must still parse: %v", err)
	}
}

func TestBindingsAreRecordedOnThePipeline(t *testing.T) {
	pipeline, err := ParseQuery(`let &a = 1; let &l = lower(image) =~ "x"; * | &l`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(pipeline.Bindings) != 2 {
		t.Fatalf("want 2 bindings recorded, got %d", len(pipeline.Bindings))
	}
	if pipeline.Bindings[0].Name != "&a" || pipeline.Bindings[0].Kind != BindingValue {
		t.Errorf("first binding: got %+v", pipeline.Bindings[0])
	}
	if pipeline.Bindings[1].Name != "&l" || pipeline.Bindings[1].Kind != BindingCondition {
		t.Errorf("second binding: got %+v", pipeline.Bindings[1])
	}
}
