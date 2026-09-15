package parser

import (
	"fmt"
	"strings"
	"testing"
	"time"
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
		`let &a = 1; let &a = 2; * | &a > 1`: "already declared",
		`let &a = ; * | &a > 1`:              "has no value",
		`let &a = 1 * | &a > 1`:              "let &a",
		`let &a 1; * | &a > 1`:               "expected '='",
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

// A binding whose value is a pipeline names a set of rows, usable where a
// subquery goes.
func TestResultSetBindingInSetMembership(t *testing.T) {
	sql := bindingSQL(t, `let &admins = user_type="admin" | groupby(user); * | in(user, &admins)`)
	if !strings.Contains(sql, "IN (SELECT user FROM (") {
		t.Errorf("want a single-column subquery, got: %s", sql)
	}
	// The subquery is scoped like the outer query, not left open.
	if n := strings.Count(sql, "fractal_id = 'f1'"); n != 2 {
		t.Errorf("want the fractal guard in both the query and the subquery, got %d: %s", n, sql)
	}
}

func TestResultSetBindingNegates(t *testing.T) {
	sql := bindingSQL(t, `let &admins = user_type="admin" | groupby(user); * | NOT in(user, &admins)`)
	if !strings.Contains(sql, "NOT IN (SELECT user FROM (") {
		t.Errorf("want a negated subquery test, got: %s", sql)
	}
}

// join() rejects nested joins, so two set memberships against two subqueries
// could not be written at all before bindings.
func TestTwoResultSetBindingsInOneQuery(t *testing.T) {
	sql := bindingSQL(t, `let &admins = user_type="admin" | groupby(user); `+
		`let &servers = role="server" | groupby(computer_name); `+
		`event_id="4624" | in(user, &admins) | in(computer_name, &servers) | groupby(user, computer_name) | count()`)
	if !strings.Contains(sql, "IN (SELECT user FROM (") || !strings.Contains(sql, "IN (SELECT computer_name FROM (") {
		t.Errorf("want both subqueries, got: %s", sql)
	}
}

func TestResultSetBindingAsAJoinBlock(t *testing.T) {
	bound := bindingSQL(t, `let &admins = user_type="admin" | groupby(user); * | join(user) { &admins }`)
	inline := bindingSQL(t, `* | join(user) { user_type="admin" | groupby(user) }`)
	if bound != inline {
		t.Errorf("a bound block must compile as the block itself\n got: %s\nwant: %s", bound, inline)
	}
}

// A binding may build on one declared before it.
func TestResultSetBindingBuildsOnAnother(t *testing.T) {
	sql := bindingSQL(t, `let &a = user_type="admin" | groupby(user); `+
		`let &b = * | in(user, &a) | groupby(image); * | in(image, &b)`)
	if n := strings.Count(sql, "IN (SELECT"); n != 2 {
		t.Errorf("want the nested subquery carried through, got %d: %s", n, sql)
	}
}

// The column tested is the one named after the field, matching how a join block
// names its key. Anything else is an error rather than a guess.
func TestResultSetBindingColumnSelection(t *testing.T) {
	pipeline, err := ParseQuery(`let &admins = user_type="admin" | groupby(user); * | in(nothere, &admins)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = TranslateToSQLWithOrder(pipeline, serverOpts())
	if err == nil {
		t.Fatal("expected an error naming the columns")
	}
	if !strings.Contains(err.Error(), "returns [user, _count]") {
		t.Errorf("want the columns named, got: %v", err)
	}
	// A binding returning exactly one column needs no name match.
	sql := bindingSQL(t, `let &one = * | groupby(image) | table(image); * | in(nothere, &one)`)
	if !strings.Contains(sql, "IN (SELECT image FROM (") {
		t.Errorf("want the single column used, got: %s", sql)
	}
}

// A result set is only meaningful where a subquery goes.
func TestResultSetBindingIsRejectedElsewhere(t *testing.T) {
	cases := map[string]string{
		`let &a = user_type="admin" | groupby(user); * | &a`:             "not a filter",
		`let &a = user_type="admin" | groupby(user); * | table(&a)`:      "only in() and a join() block",
		`let &a = user_type="admin" | groupby(user); * | in(u, [&a,&a])`: "cannot be a member of a list",
		`let &a = user_type="admin" | groupby(user); * | x := &a + 1`:    "not a value",
		`let &n = 500; * | join(user) { &n }`:                            "not a result set",
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

// A ';' inside a chain or case body does not end the binding statement.
func TestBindingStatementEndIsDepthAware(t *testing.T) {
	sql := bindingSQL(t, `let &seq = * | chain([user], within=5m) { a="x"; b="y" }; * | join(user) { &seq }`)
	if !strings.Contains(sql, "_join_k") {
		t.Errorf("want the chain body carried into the block, got: %s", sql)
	}
}

// A binding read in two places is materialised once instead of having its
// subquery pasted in at each use.
func TestResultSetUsedTwiceIsMaterialisedOnce(t *testing.T) {
	sql := bindingSQL(t, `let &a = * | groupby(image) | table(image); * | in(image, &a) | in(parent_image, &a)`)
	if !strings.HasPrefix(sql, "WITH _b_a AS (") {
		t.Errorf("want a WITH clause, got: %s", sql)
	}
	if n := strings.Count(sql, "IN (SELECT image FROM _b_a)"); n != 2 {
		t.Errorf("want two reads of the materialised binding, got %d: %s", n, sql)
	}
	if n := strings.Count(sql, "GROUP BY image"); n != 1 {
		t.Errorf("want the subquery built once, got %d: %s", n, sql)
	}
}

// One use stays inline: a WITH clause would buy nothing.
func TestResultSetUsedOnceStaysInline(t *testing.T) {
	sql := bindingSQL(t, `let &a = * | groupby(image) | table(image); * | in(image, &a)`)
	if strings.Contains(sql, "WITH ") {
		t.Errorf("want no WITH clause for a single use, got: %s", sql)
	}
}

// A binding set carries its own time bounds, so a caller must not re-translate
// the query over a narrower window. Same contract as join().
func TestResultSetMarksTheQueryTimeScoped(t *testing.T) {
	pipeline, err := ParseQuery(`let &a = * | groupby(image) | table(image); * | in(image, &a)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, serverOpts())
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !result.TimeScopedSubquery {
		t.Error("a query reading a binding set must be marked time-scoped")
	}
}

// Bindings compose, so one that references another more than once doubles at
// every level. Twenty-four such lines is a 500-byte query that expanded to
// sixteen million nodes and never returned.
func TestBindingExpansionIsBounded(t *testing.T) {
	var b strings.Builder
	b.WriteString("let &b0 = len(commandline); ")
	for i := 1; i < 24; i++ {
		fmt.Fprintf(&b, "let &b%d = &b%d + &b%d; ", i, i-1, i-1)
	}
	b.WriteString("* | &b23 > 1")

	done := make(chan error, 1)
	go func() {
		_, err := ParseQuery(b.String())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the expansion to be refused")
		}
		if !strings.Contains(err.Error(), "too large an expression") {
			t.Errorf("want a size error, got: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parsing did not finish; the expansion is unbounded")
	}
}

// The same doubling through result-set bindings costs translations rather than
// nodes: the SQL stays small because each is materialised once, but every
// reference re-translates.
func TestResultSetBindingWorkIsBounded(t *testing.T) {
	var b strings.Builder
	b.WriteString("let &s0 = * | groupby(image) | table(image); ")
	for i := 1; i < 20; i++ {
		fmt.Fprintf(&b, "let &s%d = * | in(image, &s%d) | in(parent_image, &s%d) | groupby(image) | table(image); ", i, i-1, i-1)
	}
	b.WriteString("* | in(image, &s19)")

	pipeline, err := ParseQuery(b.String())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := TranslateToSQLWithOrder(pipeline, serverOpts())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the work to be refused")
		}
		if !strings.Contains(err.Error(), "too many result-set bindings") {
			t.Errorf("want a work error, got: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("translation did not finish; the work is unbounded")
	}
}

// A binding cannot be a way around the rules a nested pipeline already has.
func TestBindingIsNotABackdoor(t *testing.T) {
	cases := map[string]string{
		`let &a = pgr(start="x") | groupby(image) | table(image); * | in(image, &a)`:                                       "pgr",
		`let &a = * | join(user) { event_id="1" | groupby(user) } | groupby(image) | table(image); * | join(image) { &a }`: "nested joins",
	}
	for query, want := range cases {
		pipeline, err := ParseQuery(query)
		if err != nil {
			continue
		}
		_, err = TranslateToSQLWithOrder(pipeline, serverOpts())
		if err == nil {
			t.Errorf("%s: expected a rejection", query)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want an error mentioning %q, got: %v", query, want, err)
		}
	}
}

// A binding name reaches SQL as a CTE name, so it is validated like every other
// generated identifier.
func TestBindingCTENameIsSafe(t *testing.T) {
	sql := bindingSQL(t, `let &drop_table = * | groupby(image) | table(image); * | in(image, &drop_table) | in(parent_image, &drop_table)`)
	if !strings.HasPrefix(sql, "WITH _b_drop_table AS (") {
		t.Errorf("want a prefixed, validated CTE name, got: %s", sql)
	}
}

// A binding stands where a value goes: `user = &tUser`. The sigil is what makes
// that unambiguous, since a bare word on the right of an operator is a literal.
func TestBindingAsAValue(t *testing.T) {
	cases := []struct{ bound, inline string }{
		{`let &u = "CORP\\rpatel"; * | user=&u`, `* | user="CORP\\rpatel"`},
		{`let &u = "bob"; user=&u | table(user)`, `user="bob" | table(user)`},
		{`let &n = 500; * | bytes > &n`, `* | bytes > 500`},
		{`let &u = "bob"; * | user != &u`, `* | user != "bob"`},
		{`let &u = "bob"; * | in(user, &u)`, `* | in(user, "bob")`},
	}
	for _, tc := range cases {
		if got, want := bindingSQL(t, tc.bound), bindingSQL(t, tc.inline); got != want {
			t.Errorf("%s\n got: %s\nwant: %s", tc.bound, got, want)
		}
	}
}

// Only a literal has a value to stand in. A filter has none, and an expression
// would be a comparison against another column, which a condition cannot carry.
func TestNonLiteralBindingIsRejectedAsAValue(t *testing.T) {
	cases := map[string]string{
		`let &l = lower(image) =~ "cmd.exe"; * | user = &l`: "not a value",
		`let &x = lower(image); * | user = &x`:              "not a literal",
		`* | user = &nope`:                                  "unknown binding",
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
