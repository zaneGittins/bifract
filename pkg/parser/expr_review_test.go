package parser

import (
	"regexp"
	"strings"
	"testing"
)

// Regression tests for the defects a review of the expression language found.
// Each names the shape that was wrong and what it produced.

func revOpts() QueryOptions {
	o := exprOpts()
	o.FractalID = "f1"
	return o
}

func revSQL(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, revOpts())
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	return result.SQL
}

// An expression predicate is spliced into the AND chain carrying the fractal and
// time guards. Unbracketed, a top-level OR re-associated with that chain and the
// right disjunct read every fractal at any time.
func TestExprPredicateCannotEscapeGuards(t *testing.T) {
	sql := revSQL(t, `* | lower(image) = "cmd.exe" OR lower(image) = "powershell.exe"`)
	if !strings.Contains(sql, "AND (lower(fields.`image`::String) = 'cmd.exe' OR lower(fields.`image`::String) = 'powershell.exe')") {
		t.Errorf("predicate not bracketed inside the guard chain: %s", sql)
	}
}

// A parenthesised pipeline group converts its conditions through
// havingFromCondition, which did not copy Expr, so the filter vanished.
func TestExprSurvivesParenthesisedGroup(t *testing.T) {
	sql := revSQL(t, `* | (contains(a,"b") OR contains(c,"d"))`)
	if strings.Count(sql, "positionCaseInsensitive") != 2 {
		t.Errorf("group lost its expression filters: %s", sql)
	}
	t.Run("negated group keeps its negation", func(t *testing.T) {
		plain := revSQL(t, `* | (contains(a,"b"))`)
		negated := revSQL(t, `* | !(contains(a,"b"))`)
		if plain == negated {
			t.Errorf("NOT was silently dropped, both forms produced: %s", plain)
		}
		if !strings.Contains(negated, "NOT (") {
			t.Errorf("expected a negated predicate: %s", negated)
		}
	})
}

// A named argument and a comparison are both `name = value`. Treating every one
// as a named argument broke if(), the flagship function, in its most natural form.
func TestExprComparisonInCallArgument(t *testing.T) {
	sql := revSQL(t, `* | x := if(status="500", "err", "ok")`)
	if !strings.Contains(sql, "if(fields.`status`::String = '500', 'err', 'ok')") {
		t.Errorf("comparison misparsed as a named argument: %s", sql)
	}
	t.Run("named arguments still work", func(t *testing.T) {
		positional := revSQL(t, `* | x := substr(commandline, 1, 50)`)
		named := revSQL(t, `* | x := substr(field=commandline, start=1, length=50)`)
		if positional != named {
			t.Errorf("named argument binding broke:\n %s\n %s", positional, named)
		}
	})
	t.Run("comparison against a parameter name", func(t *testing.T) {
		// `field` is a parameter name of substr, but here it is a log field being
		// compared, not a binding.
		sql := revSQL(t, `* | x := if(field="a", "y", "n")`)
		if !strings.Contains(sql, "fields.`field`::String = 'a'") {
			t.Errorf("comparison on a parameter-named field misparsed: %s", sql)
		}
	})
}

// Unary operators were concatenated onto the operand with no bracketing, so they
// rebound to the left operand of a compound expression.
func TestExprUnaryBinding(t *testing.T) {
	t.Run("negation of a sum", func(t *testing.T) {
		sql := revSQL(t, `* | x := -(a + b)`)
		if !strings.Contains(sql, "-(toFloat64OrNull(fields.`a`::String) + toFloat64OrNull(fields.`b`::String))") {
			t.Errorf("unary minus rebound to the left operand: %s", sql)
		}
	})
	t.Run("NOT of a conjunction", func(t *testing.T) {
		sql := revSQL(t, `* | x := if(!(contains(a,"b") AND contains(c,"d")), "y", "n")`)
		if !strings.Contains(sql, "NOT (") {
			t.Errorf("NOT rebound to the left operand: %s", sql)
		}
	})
}

// BQL binds a pipeline NOT to the first leaf, not to the whole boolean chain.
func TestExprNegationBindsToFirstLeaf(t *testing.T) {
	sql := revSQL(t, `* | !contains(commandline,"enc") OR user="bob"`)
	if strings.Contains(sql, "NOT (positionCaseInsensitive(fields.`commandline`::String, 'enc') > 0 OR") {
		t.Errorf("NOT swallowed the OR chain: %s", sql)
	}
	if !strings.Contains(sql, "OR") {
		t.Errorf("the OR was lost entirely: %s", sql)
	}
}

// name=value arguments never captured a call, unlike the three sibling branches.
func TestExprCallAsNamedArgumentValue(t *testing.T) {
	t.Run("sort", func(t *testing.T) {
		sql := revSQL(t, `* | sort(field=lower(user), order=desc)`)
		if strings.Contains(sql, "fields.``") {
			t.Errorf("empty identifier from an uncaptured call: %s", sql)
		}
		if !strings.Contains(sql, "ORDER BY lower(fields.`user`::String) DESC") {
			t.Errorf("expected ordering by the expression: %s", sql)
		}
	})
	t.Run("dedup takes its fields positionally", func(t *testing.T) {
		sql := revSQL(t, `* | dedup(lower(user))`)
		if strings.Contains(sql, "fields.`field=`") || !strings.Contains(sql, "lower(fields.`user`::String)") {
			t.Errorf("expected dedup on the expression: %s", sql)
		}
		// dedup() has no field= parameter, so naming one is a typo, not a column.
		if _, err := ParseQuery(`* | dedup(field=lower(user))`); err == nil {
			t.Error("expected dedup(field=) to be rejected")
		}
	})
}

// Word operators were written with no surrounding space, gluing them onto the
// neighbouring token so the captured text could not be re-parsed.
func TestExprCapturedCallKeepsWordOperators(t *testing.T) {
	sql := revSQL(t, `* | table(if(startsWith(image,"C:") AND endsWith(image,".exe"), "a", "b"))`)
	if !strings.Contains(sql, "startsWith(") || !strings.Contains(sql, "endsWith(") {
		t.Errorf("compound condition lost inside a captured call: %s", sql)
	}
	t.Run("NOT", func(t *testing.T) {
		sql := revSQL(t, `* | table(if(NOT isEmpty(user), "a", "b"))`)
		if !strings.Contains(sql, "NOT") {
			t.Errorf("NOT glued to the following identifier: %s", sql)
		}
	})
}

// Validation must not read a quoted literal as a call: a regex pattern often
// looks like one.
func TestExprValidationIgnoresStringLiterals(t *testing.T) {
	for _, q := range []string{
		`* | regex("powershell(?<ps>.+)", field=commandline)`,
		`* | regex("foo, bar(?<baz>.+)", field=commandline)`,
		`* | replace("user(\\d+)", "u", norm_log)`,
		`* | table(a) | "some(text)"`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			t.Errorf("parse %q: %v", q, err)
			continue
		}
		if _, err := TranslateToSQLWithOrder(pipeline, revOpts()); err != nil {
			t.Errorf("%q rejected a string literal as a call: %v", q, err)
		}
	}
	t.Run("a real typo is still caught", func(t *testing.T) {
		pipeline, _ := ParseQuery(`* | groupby(lowr(user))`)
		if _, err := TranslateToSQLWithOrder(pipeline, revOpts()); err == nil ||
			!strings.Contains(err.Error(), "unknown function lowr()") {
			t.Errorf("typo no longer caught: %v", err)
		}
	})
}

// A value carrying an apostrophe used to open a quote that never closed, and
// the whole list collapsed into one value.
func TestListValuesWithApostrophes(t *testing.T) {
	pipeline, err := ParseQuery(`* | in(user, ["O'Brien", "bob"])`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sql, err := TranslateToSQLWithOrder(pipeline, revOpts())
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	for _, want := range []string{`'O\'Brien'`, `'bob'`} {
		if !strings.Contains(sql.SQL, want) {
			t.Errorf("%s missing from the SQL: %s", want, sql.SQL)
		}
	}
}

// Field names may contain '-'. Expression mode split it as subtraction, so a
// hyphenated name silently became arithmetic on two fields that do not exist.
func TestExprHyphenatedFieldNames(t *testing.T) {
	sql := revSQL(t, `* | ua := user-agent`)
	if !strings.Contains(sql, "fields.`user-agent`::String AS ua") {
		t.Errorf("hyphenated field name split into subtraction: %s", sql)
	}
	t.Run("subtraction still works", func(t *testing.T) {
		sql := revSQL(t, `* | d := bytes - 1`)
		if !strings.Contains(sql, "toFloat64OrNull(fields.`bytes`::String) - 1") {
			t.Errorf("subtraction broke: %s", sql)
		}
	})
	t.Run("subtraction without spaces", func(t *testing.T) {
		sql := revSQL(t, `* | d := bytes-1`)
		if !strings.Contains(sql, "- 1") {
			t.Errorf("expected subtraction, got: %s", sql)
		}
	})
}

// An expression OR-ed with an aggregate condition was classified independently,
// so the disjunction was split across WHERE and HAVING and evaluated as AND.
func TestExprOrAggregateStaysOneCondition(t *testing.T) {
	pipeline, err := ParseQuery(`* | groupby(user) | _count > 1 OR len(user) > 3`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, revOpts())
	if err != nil {
		// Rejecting a cross-stage disjunction is acceptable; silently ANDing it is not.
		if !strings.Contains(err.Error(), "stage") && !strings.Contains(err.Error(), "aggregat") {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	sql := result.SQL
	whereHasLen := strings.Contains(sql[:strings.Index(sql, "GROUP BY")], "length(")
	if whereHasLen && strings.Contains(sql, "HAVING") {
		t.Errorf("disjunction split across WHERE and HAVING, silently becoming AND: %s", sql)
	}
}

// buildWhereClause passes a nil registry; without the base-column check,
// norm_log resolved as a JSON path, so the histogram filtered differently from
// the query it is meant to describe.
func TestExprBaseColumnsWithoutRegistry(t *testing.T) {
	pipeline := mustParseRev(t, `contains(norm_log, "powershell")`)
	sql, err := buildWhereClause(pipeline.Filter.Conditions)
	if err != nil {
		t.Fatalf("buildWhereClause: %v", err)
	}
	if strings.Contains(sql, "fields.`norm_log`") {
		t.Errorf("base column resolved as a JSON path: %s", sql)
	}
}

func mustParseRev(t *testing.T, q string) *PipelineNode {
	t.Helper()
	pipeline, err := ParseQuery(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	return pipeline
}

// eval() compiled only the first complete expression in its text and discarded
// the rest with no diagnostic.
func TestEvalRejectsTrailingText(t *testing.T) {
	for _, q := range []string{`* | eval("x = a b")`, `* | eval(x = a b)`} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			continue
		}
		if _, err := TranslateToSQLWithOrder(pipeline, revOpts()); err == nil {
			t.Errorf("%q: trailing text accepted silently", q)
		}
	}
}

// Two different expressions could derive the same alias, and the later one
// replaced the earlier, dropping a group key.
func TestExprAliasCollisionRejected(t *testing.T) {
	pipeline, err := ParseQuery(`* | groupby(lower(a.b), lower(a_b))`)
	if err != nil {
		return
	}
	if _, err := TranslateToSQLWithOrder(pipeline, revOpts()); err == nil {
		t.Error("colliding derived aliases accepted, one group key is silently dropped")
	}
}

// Only top-level expression leaves were compiled, so one nested inside a
// parenthesised group reached materialisation with no PredicateSQL and no Field
// and was dropped, taking the whole group with it. Adding brackets to a rule
// silently widened it to every row.
func TestExprLeafInsideCompoundSurvives(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | (lower(a)="x" OR b="y") AND c="z"`, "lower(fields.`a`::String) = 'x'"},
		{`* | !(contains(a,"b") OR contains(c,"d")) AND image="p"`, "NOT ((positionCaseInsensitive("},
		{`* | c="z" AND (contains(a,"b") OR b="y")`, "(positionCaseInsensitive(fields.`a`::String, 'b') > 0)"},
		{`* | (lower(a)="x") AND c="z"`, "lower(fields.`a`::String) = 'x'"},
	}
	for _, c := range cases {
		sql := revSQL(t, c.query)
		if !strings.Contains(sql, c.want) {
			t.Errorf("%s\n  group dropped, want %q in: %s", c.query, c.want, sql)
		}
		// The rest of the conjunction must survive too.
		if !strings.Contains(sql, "fields.`c`::String = 'z'") && !strings.Contains(sql, "fields.`image`::String = 'p'") {
			t.Errorf("%s: the neighbouring conjunct was lost: %s", c.query, sql)
		}
	}
}

// extractFieldAlias returns the whole string for a carried column with no
// " AS ", and slicing on the missing separator panicked the translator on query
// text a user controls: a 500 rather than a query error.
func TestExprAliasClashDoesNotPanicOnCarriedColumn(t *testing.T) {
	for _, q := range []string{
		`* | groupby(lower(user)) | table(lower_user) | groupby(lower(user))`,
		`* | groupby(upper(host)) | table(upper_host) | groupby(upper(host), function=count())`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			continue
		}
		// Must not panic; an error is an acceptable outcome.
		_, _ = TranslateToSQLWithOrder(pipeline, revOpts())
	}
}

// A condition function used as a boolean operand had its negation applied twice
// when the handler honoured cmd.Negate itself (in, cidr) and not at all when it
// did not (comment, tlsh), so `!in(x) AND y` returned exactly what was excluded.
func TestNegatedConditionFunctionNegatesOnce(t *testing.T) {
	cases := []struct{ query, want, reject string }{
		{`* | !in(status,"200,404") AND user="bob"`,
			"NOT (fields.`status`::String IN ('200', '404'))", "NOT IN"},
		{`* | !cidr(dst_ip,"10.0.0.0/8") AND dst_port="445"`,
			"NOT (", "NOT (NOT ("},
		{`* | !cidr(dst_ip,"10.0.0.0/8")`, "NOT (isIPAddressInRange(", "NOT (NOT ("},
	}
	for _, c := range cases {
		sql := revSQL(t, c.query)
		if !strings.Contains(sql, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql)
		}
		if strings.Contains(sql, c.reject) {
			t.Errorf("%s\n  negation applied twice: %s", c.query, sql)
		}
	}
	t.Run("non-negated is unchanged", func(t *testing.T) {
		sql := revSQL(t, `* | in(status,"200,404") AND user="bob"`)
		if strings.Contains(sql, "NOT") {
			t.Errorf("unexpected negation: %s", sql)
		}
	})
}

// contains() renders an infix comparison behind a call node. Unbracketed, it
// regrouped when used as an operand, because ClickHouse puts =, >, < at one
// left-associative level.
func TestContainsBracketsItsOwnRender(t *testing.T) {
	sql := revSQL(t, `* | x := if(contains(a,"x") = contains(b,"y"), "same", "diff")`)
	if !strings.Contains(sql, "(positionCaseInsensitive(fields.`a`::String, 'x') > 0) = (positionCaseInsensitive(fields.`b`::String, 'y') > 0)") {
		t.Errorf("contains() render is not bracketed: %s", sql)
	}
}

// A token opening with a digit is a number or a mistake. Reading it as a field
// name compiled 1e6 to fields.`1e6`, NULL on every row.
func TestNumericLiteralForms(t *testing.T) {
	accept := []struct{ query, want string }{
		{`* | x := bytes / 1e6`, "/ 1e6"},
		{`* | x := bytes * 1.5e3`, "* 1.5e3"},
		{`* | x := bytes * 0x10`, "* 0x10"},
		{`* | x := bytes * 2`, "* 2"},
		{`* | x := bytes / 1.5`, "/ 1.5"},
	}
	for _, c := range accept {
		sql := revSQL(t, c.query)
		if !strings.Contains(sql, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql)
		}
		if strings.Contains(sql, "fields.`1") || strings.Contains(sql, "fields.`0x") {
			t.Errorf("%s: numeric literal compiled as a field: %s", c.query, sql)
		}
	}
	t.Run("malformed number is rejected", func(t *testing.T) {
		if _, err := ParseQuery(`* | x := 1.2.3`); err == nil {
			t.Error("expected 1.2.3 to be rejected")
		}
	})
}

// table() projected an expression under a derived name without registering it,
// so every downstream reference resolved as a JSON path that does not exist.
func TestTableRegistersDerivedAlias(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | table(lower(user)) | sort(lower_user)`, "ORDER BY lower_user"},
		{`* | table(lower(user)) | dedup(lower_user)`, "LIMIT 1 BY lower_user"},
		{`* | table(lower(user)) | lower_user = "bob"`, "lower_user = 'bob'"},
	}
	for _, c := range cases {
		sql := revSQL(t, c.query)
		if !strings.Contains(sql, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql)
		}
		if strings.Contains(sql, "fields.`lower_user`") {
			t.Errorf("%s: derived alias resolved as a JSON path: %s", c.query, sql)
		}
	}
}

// A computed assignment with no aggregation is projected by the outer formatter,
// so a downstream filter has to fold in the expression rather than reference a
// column the inner scan does not have.
func TestDeferredAssignmentIsAddressable(t *testing.T) {
	sql := revSQL(t, `* | x := lower(user) | x = "bob"`)
	if !strings.Contains(sql, "lower(fields.`user`::String) = 'bob'") {
		t.Errorf("filter did not fold in the expression: %s", sql)
	}
	if strings.Contains(sql, "fields.`x`") {
		t.Errorf("filter referenced a column the scan does not have: %s", sql)
	}
	t.Run("no duplicate alias when a later command projects it", func(t *testing.T) {
		sql := revSQL(t, `* | x := bytes * 8 | table(x)`)
		if n := strings.Count(sql, " AS x"); n != 1 {
			t.Errorf("expected one projection of x, got %d: %s", n, sql)
		}
	})
}

// eval() assigns into the same SELECT, so a self-reference reads the column the
// previous eval produced, unlike := where it means the log field.
func TestEvalSelfReferenceReadsTheColumn(t *testing.T) {
	sql := revSQL(t, `* | eval(total=bytes*2) | eval(total=total*3)`)
	if strings.Contains(sql, "fields.`total`") {
		t.Errorf("eval self-reference read a log field that does not exist: %s", sql)
	}
}

// Chain steps parse from block tokens whose offsets point into the original
// query, so the sub-parser needs that source to support expressions at all.
func TestChainStepsAcceptExpressions(t *testing.T) {
	for _, q := range []string{
		`* | chain(host) { lower(image)="cmd.exe"; b="2" }`,
		`* | chain(host) { a="1"; len(commandline) > 5 }`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			t.Errorf("parse %q: %v", q, err)
			continue
		}
		if _, err := TranslateToSQLWithOrder(pipeline, revOpts()); err != nil {
			t.Errorf("%q: %v", q, err)
		}
	}
}

// injectedColumn matches the payload appearing as a column of its own, rather
// than inside the string literal an operand legitimately compiles to.
var injectedColumn = regexp.MustCompile(`(?:SELECT|,)\s*injected\b`)

// A generated alias carries the operand's name and reaches SQL unquoted, so a
// field name containing a comma would add a column of its own.
func TestAggregateAliasRejectsInjectedColumn(t *testing.T) {
	reached := 0
	for _, q := range []string{
		`* | stddev("x, 1 AS injected")`,
		`* | percentile("x, 1 AS injected")`,
		`* | selectFirst("x, 1 AS injected")`,
		`* | selectLast("x, 1 AS injected")`,
		`* | multi(iqr("x, 1 AS injected"))`,
		`* | multi(mad("x, 1 AS injected"))`,
		`* | multi(median("x, 1 AS injected"))`,
		`* | multi(top("x, 1 AS injected"))`,
		`* | multi(collect("x, 1 AS injected"))`,
		`* | multi(count("x, 1 AS injected", distinct=true))`,
		// Renamed rather than rejected: the operand is an expression, so the alias
		// is derived from it instead of carrying a field name through.
		`* | table(user, stddev(concat(a, "x, 1 AS injected")))`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			continue
		}
		sql, err := TranslateToSQLWithOrder(pipeline, revOpts())
		if err != nil {
			continue
		}
		reached++
		if injectedColumn.MatchString(sql.SQL) {
			t.Errorf("%s: alias injected a column: %s", q, sql.SQL)
		}
	}
	// Rejection is a valid outcome, but not for every case: without this the test
	// would pass vacuously if the queries stopped parsing for another reason.
	if reached == 0 {
		t.Error("no case reached translation; the guard is not being exercised")
	}
}
