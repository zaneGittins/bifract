package parser

import (
	"strings"
	"testing"
	"time"
)

func exprOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}
}

// translateExpr compiles a query and returns its SQL, failing the test on error.
func translateExpr(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, exprOpts())
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	return result.SQL
}

// exprError returns the error a query produces, failing the test if it succeeds.
func exprError(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		return err.Error()
	}
	if _, err := TranslateToSQLWithOrder(pipeline, exprOpts()); err != nil {
		return err.Error()
	}
	t.Fatalf("expected an error for %q", query)
	return ""
}

// TestExprSilentBugsAreFixed covers the three shapes that previously compiled to
// a wrong value with no error.
func TestExprSilentBugsAreFixed(t *testing.T) {
	t.Run("function call is no longer swallowed to a literal", func(t *testing.T) {
		sql := translateExpr(t, `* | x := lower(user)`)
		if !strings.Contains(sql, "lower(fields.`user`::String) AS x") {
			t.Errorf("expected lower() applied to the field, got: %s", sql)
		}
		if strings.Contains(sql, "'lower()'") {
			t.Errorf("function call still compiled to a string literal: %s", sql)
		}
	})

	t.Run("function arguments are no longer discarded", func(t *testing.T) {
		sql := translateExpr(t, `* | x := len(user) * 2`)
		if !strings.Contains(sql, "length(fields.`user`::String)") {
			t.Errorf("expected length() over the field, got: %s", sql)
		}
	})

	t.Run("string concatenation with + errors instead of yielding NULL", func(t *testing.T) {
		msg := exprError(t, `* | x := user + "-" + host`)
		if !strings.Contains(msg, "expects numbers") || !strings.Contains(msg, "concat(") {
			t.Errorf("expected a typed error naming concat(), got: %v", msg)
		}
	})
}

func TestExprScopedTyping(t *testing.T) {
	t.Run("untyped field coerces in arithmetic", func(t *testing.T) {
		sql := translateExpr(t, `* | score := bytes * 2`)
		if !strings.Contains(sql, "toFloat64OrNull(fields.`bytes`::String) * 2") {
			t.Errorf("expected coerced field arithmetic, got: %s", sql)
		}
	})

	t.Run("string literal in arithmetic is rejected", func(t *testing.T) {
		msg := exprError(t, `* | x := bytes + "2"`)
		if !strings.Contains(msg, "expects numbers") {
			t.Errorf("expected a type error, got: %v", msg)
		}
	})

	t.Run("string-returning function in arithmetic is rejected", func(t *testing.T) {
		msg := exprError(t, `* | x := upper(user) + 1`)
		if !strings.Contains(msg, "expects numbers") {
			t.Errorf("expected a type error, got: %v", msg)
		}
	})

	t.Run("string in non-additive arithmetic reports the type", func(t *testing.T) {
		msg := exprError(t, `* | x := bytes * "2"`)
		if !strings.Contains(msg, "expected a number") {
			t.Errorf("expected a type error, got: %v", msg)
		}
	})

	t.Run("number in a string parameter is stringified", func(t *testing.T) {
		sql := translateExpr(t, `* | x := concat(user, 1)`)
		if !strings.Contains(sql, "toString(1)") {
			t.Errorf("expected the number coerced to text, got: %s", sql)
		}
	})

	t.Run("condition where text is expected is rejected", func(t *testing.T) {
		msg := exprError(t, `* | x := upper(isEmpty(user))`)
		if !strings.Contains(msg, "condition") {
			t.Errorf("expected a type error naming the condition, got: %v", msg)
		}
	})
}

func TestExprNesting(t *testing.T) {
	sql := translateExpr(t, `* | tag := substr(lower(image), 1, 10)`)
	if !strings.Contains(sql, "substring(lower(fields.`image`::String), 1, 10) AS tag") {
		t.Errorf("expected nested calls, got: %s", sql)
	}
}

func TestExprPrecedence(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		// Multiplication binds tighter, so no parentheses are needed.
		{`* | x := a + b * c`, "toFloat64OrNull(fields.`a`::String) + toFloat64OrNull(fields.`b`::String) * toFloat64OrNull(fields.`c`::String)"},
		// Written grouping must survive.
		{`* | x := (a + b) * c`, "(toFloat64OrNull(fields.`a`::String) + toFloat64OrNull(fields.`b`::String)) * toFloat64OrNull(fields.`c`::String)"},
		// A right operand of equal precedence keeps its grouping.
		{`* | x := a - (b - c)`, "toFloat64OrNull(fields.`a`::String) - (toFloat64OrNull(fields.`b`::String) - toFloat64OrNull(fields.`c`::String))"},
		// Left associativity needs no parentheses.
		{`* | x := a - b - c`, "toFloat64OrNull(fields.`a`::String) - toFloat64OrNull(fields.`b`::String) - toFloat64OrNull(fields.`c`::String)"},
	}
	for _, c := range cases {
		sql := translateExpr(t, c.query)
		if !strings.Contains(sql, c.want) {
			t.Errorf("%s\n  want: %s\n  got:  %s", c.query, c.want, sql)
		}
	}
}

func TestExprNamedArguments(t *testing.T) {
	positional := translateExpr(t, `* | x := substr(commandline, 1, 50)`)
	named := translateExpr(t, `* | x := substr(field=commandline, start=1, length=50)`)
	reordered := translateExpr(t, `* | x := substr(commandline, length=50, start=1)`)
	if positional != named || positional != reordered {
		t.Errorf("named arguments changed the SQL:\n positional: %s\n named:      %s\n reordered:  %s", positional, named, reordered)
	}

	t.Run("duplicate binding is rejected", func(t *testing.T) {
		msg := exprError(t, `* | x := substr(commandline, 1, field=user)`)
		if !strings.Contains(msg, "both positionally and by name") {
			t.Errorf("expected a duplicate-binding error, got: %v", msg)
		}
	})

	t.Run("unknown parameter is rejected", func(t *testing.T) {
		msg := exprError(t, `* | x := substr(commandline, start=1, nope=2)`)
		if !strings.Contains(msg, "unknown parameter nope") {
			t.Errorf("expected an unknown-parameter error, got: %v", msg)
		}
	})

	t.Run("positional after named is rejected", func(t *testing.T) {
		msg := exprError(t, `* | x := substr(field=commandline, 1)`)
		if !strings.Contains(msg, "must come before named") {
			t.Errorf("expected an ordering error, got: %v", msg)
		}
	})
}

func TestExprArity(t *testing.T) {
	t.Run("missing required argument", func(t *testing.T) {
		msg := exprError(t, `* | x := substr(commandline)`)
		if !strings.Contains(msg, "missing required argument start") {
			t.Errorf("expected a missing-argument error, got: %v", msg)
		}
	})

	t.Run("too many arguments", func(t *testing.T) {
		msg := exprError(t, `* | x := lower(user, image)`)
		if !strings.Contains(msg, "at most 1") {
			t.Errorf("expected an arity error, got: %v", msg)
		}
	})

	t.Run("optional argument may be omitted", func(t *testing.T) {
		sql := translateExpr(t, `* | x := substr(commandline, 5)`)
		if !strings.Contains(sql, "substring(fields.`commandline`::String, 5)") {
			t.Errorf("expected the two-argument form, got: %s", sql)
		}
	})
}

// TestExprUnknownFunctionErrors is the property that matters most for a
// detection platform: an unknown name must never degrade into a field
// reference, which would silently match nothing.
func TestExprUnknownFunctionErrors(t *testing.T) {
	t.Run("typo suggests the intended function", func(t *testing.T) {
		msg := exprError(t, `* | x := lowr(user)`)
		if !strings.Contains(msg, "did you mean lower()") {
			t.Errorf("expected a suggestion, got: %v", msg)
		}
	})

	t.Run("unrelated name errors plainly", func(t *testing.T) {
		msg := exprError(t, `* | x := frobnicate(user)`)
		if !strings.Contains(msg, "unknown function frobnicate()") {
			t.Errorf("expected an unknown-function error, got: %v", msg)
		}
	})
}

func TestExprConditional(t *testing.T) {
	t.Run("if with string branches", func(t *testing.T) {
		sql := translateExpr(t, `* | zone := if(isEmpty(src_ip), "unknown", "known")`)
		if !strings.Contains(sql, "if((fields.`src_ip`::String = ''), 'unknown', 'known') AS zone") {
			t.Errorf("expected an if() expression, got: %s", sql)
		}
	})

	t.Run("comparison is a usable condition", func(t *testing.T) {
		sql := translateExpr(t, `* | big := if(len(commandline) > 500, "yes", "no")`)
		if !strings.Contains(sql, "length(fields.`commandline`::String) > 500") {
			t.Errorf("expected the comparison inline, got: %s", sql)
		}
	})

	t.Run("non-condition first argument is rejected", func(t *testing.T) {
		msg := exprError(t, `* | x := if(user, "a", "b")`)
		if !strings.Contains(msg, "expected a condition") {
			t.Errorf("expected a type error, got: %v", msg)
		}
	})

	t.Run("mismatched branches are rejected", func(t *testing.T) {
		msg := exprError(t, `* | x := if(len(user) > 1, "a", 2)`)
		if !strings.Contains(msg, "same type") {
			t.Errorf("expected a branch-type error, got: %v", msg)
		}
	})
}

// TestExprPreservesSimpleAssignments pins the two shapes production queries rely
// on: a rename and a constant.
func TestExprPreservesSimpleAssignments(t *testing.T) {
	t.Run("rename stays a string column", func(t *testing.T) {
		sql := translateExpr(t, `* | user := source_account_name`)
		if !strings.Contains(sql, "fields.`source_account_name`::String AS user") {
			t.Errorf("expected a plain string rename, got: %s", sql)
		}
		if strings.Contains(sql, "toFloat64") {
			t.Errorf("rename must not coerce numerically: %s", sql)
		}
	})

	t.Run("string literal", func(t *testing.T) {
		sql := translateExpr(t, `* | severity := "high"`)
		if !strings.Contains(sql, "'high' AS severity") {
			t.Errorf("expected a literal column, got: %s", sql)
		}
	})

	t.Run("timestamp resolves to the base column", func(t *testing.T) {
		sql := translateExpr(t, `* | seen := timestamp`)
		if !strings.Contains(sql, "timestamp AS seen") {
			t.Errorf("expected the base column, got: %s", sql)
		}
	})
}

func TestExprSelfReference(t *testing.T) {
	sql := translateExpr(t, `* | cpu := cpu * 100`)
	if !strings.Contains(sql, "fields.`cpu`::String") {
		t.Errorf("self-reference must read the log field, got: %s", sql)
	}
}

// TestExprLexingMode covers arithmetic written without spaces, which the filter
// lexer folds into a single identifier.
func TestExprLexingMode(t *testing.T) {
	spaced := translateExpr(t, `* | x := a * 2`)
	tight := translateExpr(t, `* | x := a*2`)
	if spaced != tight {
		t.Errorf("spacing changed the SQL:\n spaced: %s\n tight:  %s", spaced, tight)
	}

	t.Run("wildcard filters still lex as wildcards", func(t *testing.T) {
		sql := translateExpr(t, `image=* | x := 1`)
		if !strings.Contains(sql, "fields.`image`::String != ''") {
			t.Errorf("wildcard filter broke: %s", sql)
		}
	})

	t.Run("hyphenated field names still lex as one field", func(t *testing.T) {
		sql := translateExpr(t, `some-field=value`)
		if !strings.Contains(sql, "fields.`some-field`::String") {
			t.Errorf("hyphenated field broke: %s", sql)
		}
	})
}

// TestExprAssignmentInChainStepRejected: chain steps are row conditions, and a
// step is compiled at translation time from its own tokens.
func TestExprAssignmentInChainStepRejected(t *testing.T) {
	msg := exprError(t, `* | chain(user) { x := 1; event_id=1 }`)
	if !strings.Contains(msg, "not supported here") {
		t.Errorf("expected a clear rejection, got: %v", msg)
	}
}

func TestExprEvalCommand(t *testing.T) {
	sql := translateExpr(t, `* | eval("score = bytes * 2")`)
	if !strings.Contains(sql, "toFloat64OrNull(fields.`bytes`::String) * 2 AS score") {
		t.Errorf("eval() did not compile through the expression path: %s", sql)
	}

	t.Run("eval reports type errors", func(t *testing.T) {
		msg := exprError(t, `* | eval("x = user + \"a\"")`)
		if !strings.Contains(msg, "eval()") || !strings.Contains(msg, "expects numbers") {
			t.Errorf("expected a typed eval error, got: %v", msg)
		}
	})
}
