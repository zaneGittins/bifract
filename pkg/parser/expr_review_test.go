package parser

import (
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
		// dedup() has no field= parameter; the handler would read the whole
		// "field=..." text as a column name.
		pipeline, err := ParseQuery(`* | dedup(field=lower(user))`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, err := TranslateToSQLWithOrder(pipeline, revOpts()); err == nil {
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

// listArg splits on top-level commas. An apostrophe inside a value opened a
// quote that never closed, so the whole list collapsed into one value.
func TestListArgHandlesApostrophes(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{`O'Brien,bob`, []string{"O'Brien", "bob"}},
		{`foo(bar,baz`, []string{"foo(bar", "baz"}},
		{`a),b`, []string{"a)", "b"}},
		{`substr(b,1,2),c`, []string{"substr(b,1,2)", "c"}},
	}
	for _, c := range cases {
		got := listArg(c.in)
		if len(got) != len(c.want) {
			t.Errorf("listArg(%q) = %q, want %q", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("listArg(%q) = %q, want %q", c.in, got, c.want)
				break
			}
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
	for _, q := range []string{`* | eval("x = a b")`, `* | eval(msg="hello world")`} {
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
		{`* | !(contains(a,"b") OR contains(c,"d")) AND image="p"`, "NOT (positionCaseInsensitive("},
		{`* | c="z" AND (contains(a,"b") OR b="y")`, "positionCaseInsensitive(fields.`a`::String, 'b')"},
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
