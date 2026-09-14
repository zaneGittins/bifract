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
	if !strings.Contains(msg, "cannot be used inside a chain step") {
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

// TestExprFilters covers phase 2: an expression used as a condition.
func TestExprFilters(t *testing.T) {
	t.Run("after a pipe", func(t *testing.T) {
		sql := translateExpr(t, `* | lower(image) = "cmd.exe"`)
		if !strings.Contains(sql, "lower(fields.`image`::String) = 'cmd.exe'") {
			t.Errorf("expected the expression in the WHERE, got: %s", sql)
		}
	})

	t.Run("as the leading filter", func(t *testing.T) {
		sql := translateExpr(t, `lower(image) = "cmd.exe"`)
		if !strings.Contains(sql, "lower(fields.`image`::String) = 'cmd.exe'") {
			t.Errorf("expected the expression in the WHERE, got: %s", sql)
		}
	})

	t.Run("numeric comparison", func(t *testing.T) {
		sql := translateExpr(t, `* | len(commandline) > 500`)
		if !strings.Contains(sql, "length(fields.`commandline`::String) > 500") {
			t.Errorf("expected a length comparison, got: %s", sql)
		}
	})

	t.Run("boolean function needs no comparison", func(t *testing.T) {
		sql := translateExpr(t, `* | startsWith(image, "C:\\Windows")`)
		if !strings.Contains(sql, "startsWith(fields.`image`::String, 'C:\\\\Windows')") {
			t.Errorf("expected startsWith in the WHERE, got: %s", sql)
		}
	})

	t.Run("negated", func(t *testing.T) {
		sql := translateExpr(t, `* | !contains(commandline, "-enc")`)
		if !strings.Contains(sql, "NOT ((positionCaseInsensitive(") {
			t.Errorf("expected a negated predicate, got: %s", sql)
		}
	})

	t.Run("combines with ordinary conditions", func(t *testing.T) {
		sql := translateExpr(t, `event_id=1 | lower(image) = "cmd.exe" | groupby(user)`)
		if !strings.Contains(sql, "fields.`event_id`::String = '1'") ||
			!strings.Contains(sql, "lower(fields.`image`::String) = 'cmd.exe'") {
			t.Errorf("expected both conditions, got: %s", sql)
		}
	})

	t.Run("a value is not a condition", func(t *testing.T) {
		msg := exprError(t, `* | lower(image)`)
		if !strings.Contains(msg, "not a condition") {
			t.Errorf("expected a clear rejection, got: %v", msg)
		}
	})
}

// TestExprFilterDoesNotHijackCommands pins the conservative dispatch rule: an
// existing query must keep meaning exactly what it meant.
func TestExprFilterDoesNotHijackCommands(t *testing.T) {
	t.Run("condition functions keep the boolean-operand path", func(t *testing.T) {
		sql := translateExpr(t, `cidr(dst_ip, "10.0.0.0/8") OR cidr(dst_ip, "192.168.0.0/16")`)
		if !strings.Contains(sql, " OR ") {
			t.Errorf("OR between condition functions was lost: %s", sql)
		}
		if strings.Count(sql, "isIPAddressInRange") != 2 {
			t.Errorf("expected both ranges, got: %s", sql)
		}
	})

	t.Run("a command call stays a command", func(t *testing.T) {
		sql := translateExpr(t, `* | len(commandline) | _len > 500`)
		if !strings.Contains(sql, "AS _len") {
			t.Errorf("len() stopped binding its output column: %s", sql)
		}
	})

	t.Run("in() is still dispatched as a command", func(t *testing.T) {
		sql := translateExpr(t, `* | in(status, "200,404")`)
		if !strings.Contains(sql, "IN ('200', '404')") {
			t.Errorf("in() stopped working: %s", sql)
		}
	})

	t.Run("groupby is not read as an expression", func(t *testing.T) {
		sql := translateExpr(t, `* | groupby(user)`)
		if !strings.Contains(sql, "GROUP BY user") {
			t.Errorf("groupby() stopped working: %s", sql)
		}
	})
}

// modelOpts adds a beacon model so model_lookup() resolves, producing the joined
// columns that only exist above the source scan.
func modelOpts() QueryOptions {
	o := exprOpts()
	o.FractalID = "f1"
	o.Models = map[string]AnalyticsModelInfo{
		"beacons": {ID: "m1", TableName: "model_beacons", ModelType: "beacon", MinSample: 5, FractalID: "f1"},
	}
	return o
}

func translateWith(t *testing.T, query string, opts QueryOptions) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	return result.SQL
}

// TestExprFilterOverDeferredColumns covers filters that sit above the source
// scan. A source-scope leaf there must be exported as a hidden column, because a
// raw JSON sub-column does not resolve at that layer (ClickHouse code 47).
func TestExprFilterOverDeferredColumns(t *testing.T) {
	t.Run("join output only", func(t *testing.T) {
		sql := translateWith(t, `* | model_lookup(model="beacons", key=[src_ip,dst_ip,dst_port]) | round(beacon_score, 2) > 0.9`, modelOpts())
		// The joined column is read by name, coerced like any registered column.
		if !strings.Contains(sql, "round(toFloat64OrNull(toString(beacon_score)), 2) > 0.9") {
			t.Errorf("expected the joined column read directly, got: %s", sql)
		}
		if strings.Contains(sql, "_dfr_") {
			t.Errorf("a column that already exists at the deferred layer must not be exported: %s", sql)
		}
	})

	t.Run("mixed with a log field exports the source leaf", func(t *testing.T) {
		sql := translateWith(t, `* | model_lookup(model="beacons", key=[src_ip,dst_ip,dst_port]) | round(beacon_score,2) > 0.9 AND lower(image) = "chrome.exe"`, modelOpts())
		if !strings.Contains(sql, "fields.`image`::String AS _dfr_0") {
			t.Errorf("source leaf not exported into the scan: %s", sql)
		}
		if !strings.Contains(sql, "lower(_dfr_0) = 'chrome.exe'") {
			t.Errorf("deferred filter did not read the exported column: %s", sql)
		}
		if !strings.Contains(sql, "EXCEPT (_dfr_0)") {
			t.Errorf("hidden column not stripped from the result: %s", sql)
		}
		if strings.Contains(sql, "lower(fields.`image`::String) = 'chrome.exe'") {
			t.Errorf("raw JSON reference survived to the deferred layer: %s", sql)
		}
	})

	t.Run("window output", func(t *testing.T) {
		sql := translateExpr(t, `* | madOutlier(bytes) | abs(_modified_z) > 4`)
		if !strings.Contains(sql, "abs(toFloat64OrNull(toString(_modified_z))) > 4") {
			t.Errorf("expected the window column read directly, got: %s", sql)
		}
		if strings.Contains(sql, "_dfr_") {
			t.Errorf("a window column must not be exported: %s", sql)
		}
	})
}

// TestExprCommandArguments covers phase 3: an expression in a field position.
func TestExprCommandArguments(t *testing.T) {
	t.Run("group key", func(t *testing.T) {
		sql := translateExpr(t, `* | groupby(lower(user))`)
		if !strings.Contains(sql, "lower(fields.`user`::String) AS lower_user") {
			t.Errorf("expected the key projected under a derived name, got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY lower_user") {
			t.Errorf("expected grouping by the alias, got: %s", sql)
		}
	})

	t.Run("multi-argument group key", func(t *testing.T) {
		sql := translateExpr(t, `* | groupby(substr(image,1,10), function=count())`)
		if !strings.Contains(sql, "substring(fields.`image`::String, 1, 10) AS substr_image_1_10") {
			t.Errorf("commas inside the call must not split the argument: %s", sql)
		}
	})

	t.Run("derived alias is addressable downstream", func(t *testing.T) {
		sql := translateExpr(t, `* | groupby(lower(user)) | lower_user = "admin"`)
		if !strings.Contains(sql, "lower_user") {
			t.Errorf("expected the alias usable downstream, got: %s", sql)
		}
	})

	t.Run("sort key", func(t *testing.T) {
		sql := translateExpr(t, `* | sort(len(commandline), order=desc)`)
		if !strings.Contains(sql, "ORDER BY length(fields.`commandline`::String) DESC") {
			t.Errorf("expected ordering by the expression, got: %s", sql)
		}
	})

	t.Run("table column", func(t *testing.T) {
		sql := translateExpr(t, `* | table(user, len(commandline))`)
		if !strings.Contains(sql, "AS len_commandline") {
			t.Errorf("expected a derived column, got: %s", sql)
		}
	})

	t.Run("dedup key", func(t *testing.T) {
		sql := translateExpr(t, `* | dedup(lower(user))`)
		if !strings.Contains(sql, "lower(fields.`user`::String)") {
			t.Errorf("expected dedup on the expression, got: %s", sql)
		}
	})

	t.Run("aggregate input", func(t *testing.T) {
		sql := translateExpr(t, `* | groupby(user, function=sum(len(commandline)))`)
		if !strings.Contains(sql, "sum(toFloat64OrNull(length(fields.`commandline`::String)))") {
			t.Errorf("expected the expression inside the aggregate, got: %s", sql)
		}
	})

	t.Run("aggregate input inside multi", func(t *testing.T) {
		sql := translateExpr(t, `* | groupby(user, function=multi(count(), avg(len(commandline))))`)
		if !strings.Contains(sql, "avg(toFloat64OrNull(length(fields.`commandline`::String)))") {
			t.Errorf("expected the expression inside multi(), got: %s", sql)
		}
	})
}

// TestExprArgumentsDoNotBreakBrackets pins the corpus constraint: a bracket list
// is command-argument syntax, not an array literal, and must keep working.
func TestExprArgumentsDoNotBreakBrackets(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | table([timestamp,user,image])`, "AS image"},
		{`* | concat([user, host], as=uh)`, "AS uh"},
		{`* | in(status, values=[200,404])`, "IN ('200', '404')"},
		{`* | groupby(computer_name, function=count(field=user, unique=true))`, "uniqExact("},
		{`* | groupby(user, function=count())`, "COUNT(*)"},
	}
	for _, c := range cases {
		sql := translateExpr(t, c.query)
		if !strings.Contains(sql, c.want) {
			t.Errorf("%s\n  want %q in: %s", c.query, c.want, sql)
		}
	}
}

// TestExprNetworkAndTimeFunctions covers the network and time additions. Every
// network function guards its input: ClickHouse throws CANNOT_PARSE_IPV4 on a
// value that is not an address, and one bad row would abort the whole query.
func TestExprNetworkAndTimeFunctions(t *testing.T) {
	t.Run("isPrivateIP covers every non-routable range", func(t *testing.T) {
		sql := translateExpr(t, `* | isPrivateIP(dst_ip) = false`)
		for _, r := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
			"127.0.0.0/8", "169.254.0.0/16", "100.64.0.0/10", "fc00::/7", "::1/128", "fe80::/10"} {
			if !strings.Contains(sql, "'"+r+"'") {
				t.Errorf("range %s missing from: %s", r, sql)
			}
		}
		if !strings.Contains(sql, "'0.0.0.0'") {
			t.Errorf("no sentinel guarding the address conversion: %s", sql)
		}
	})

	t.Run("ipPrefix handles both families and guards conversion", func(t *testing.T) {
		sql := translateExpr(t, `* | groupby(ipPrefix(src_ip, 24))`)
		if !strings.Contains(sql, "IPv4CIDRToRange") || !strings.Contains(sql, "IPv6CIDRToRange") {
			t.Errorf("expected both address families, got: %s", sql)
		}
		if !strings.Contains(sql, "toIPv4(if(isIPv4String(") || !strings.Contains(sql, "toIPv6(if(isIPv6String(") {
			t.Errorf("conversion is not guarded by a validity check: %s", sql)
		}
		if !strings.Contains(sql, "AS ipPrefix_src_ip_24") {
			t.Errorf("expected a derived group key, got: %s", sql)
		}
	})

	t.Run("isIPv4 and isIPv6", func(t *testing.T) {
		if sql := translateExpr(t, `* | isIPv4(src_ip)`); !strings.Contains(sql, "isIPv4String(fields.`src_ip`::String)") {
			t.Errorf("got: %s", sql)
		}
		if sql := translateExpr(t, `* | isIPv6(src_ip)`); !strings.Contains(sql, "isIPv6String(fields.`src_ip`::String)") {
			t.Errorf("got: %s", sql)
		}
	})

	t.Run("dateDiff coerces both sides leniently", func(t *testing.T) {
		sql := translateExpr(t, `* | age := dateDiff("second", first_seen, last_seen)`)
		if !strings.Contains(sql, "dateDiff('second'") {
			t.Errorf("expected dateDiff, got: %s", sql)
		}
		if strings.Count(sql, "parseDateTime64BestEffortOrNull") < 2 && !strings.Contains(sql, "toDateTime64") {
			t.Errorf("time arguments are not leniently parsed, a bad row would abort: %s", sql)
		}
	})
}

// Without boolean literals, isPrivateIP(ip) = false compared the condition to a
// log field named "false", which exists on no row.
func TestExprBooleanLiterals(t *testing.T) {
	for _, q := range []string{`* | isPrivateIP(dst_ip) = false`, `* | isIPv4(src_ip) = true`} {
		sql := translateExpr(t, q)
		if strings.Contains(sql, "fields.`false`") || strings.Contains(sql, "fields.`true`") {
			t.Errorf("%s: boolean literal resolved as a field: %s", q, sql)
		}
	}
	if sql := translateExpr(t, `* | isIPv4(src_ip) = true`); !strings.Contains(sql, "isIPv4String(fields.`src_ip`::String) = 1") {
		t.Errorf("expected a boolean literal, got: %s", sql)
	}
}
