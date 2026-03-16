package parser

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode"
)

// bqlSeeds contains diverse BQL queries covering all syntax features.
var bqlSeeds = []string{
	// Simple field conditions
	`status=200`,
	`host="web-01"`,
	`level!=info`,
	// Regex values
	`image=/powershell/i`,
	`command_line!=/mp/i`,
	`message=/error.*failed/i`,
	`path=/api\/v[0-9]+/`,
	// Quoted strings
	`message="hello world"`,
	`"connection refused"`,
	`"powershell.exe"`,
	// Boolean operators
	`status=500 AND host="web-01"`,
	`status=404 OR status=500`,
	`NOT status=200`,
	`NOT (status=200 OR status=301)`,
	// Implicit AND (space-separated)
	`image=/powershell/i user=admin`,
	// Parenthesized groups
	`(status=500 OR status=502) AND host="web-01"`,
	`(level=error AND host="db-01") OR (level=warn AND host="db-02")`,
	// Pipeline commands with function-call syntax
	`* | count()`,
	`* | groupBy(host)`,
	`* | groupBy(host) | count()`,
	`* | groupBy(host, status)`,
	`* | groupBy([image, user])`,
	`* | sort(timestamp, asc)`,
	`* | sort(timestamp, desc)`,
	`* | sort(user)`,
	`* | limit(10)`,
	`* | limit(50)`,
	`* | table(host, status, message)`,
	`* | table(image, count)`,
	`* | sum(bytes)`,
	`* | avg(response_time)`,
	`* | min(response_time)`,
	`* | max(bytes)`,
	`* | head(5)`,
	`* | tail(10)`,
	`* | median(duration)`,
	// Multi-pipe queries
	`image=/powershell/i | groupBy(user) | count() | sort(count, desc) | limit(10)`,
	`image=/powershell/i | count()`,
	`event_id=1 | image=/powershell/i | command_line!=/mp/i`,
	`* | groupBy(image) | sum(bytes)`,
	// Assignments
	`* | duration := response_time`,
	// Numeric comparisons
	`status>400`,
	`status>=500`,
	`status<300`,
	`status<=200`,
	`status_code>=500`,
	`bytes>1000 AND status=200`,
	// Wildcards
	`*`,
	// Quoted string searches with boolean
	`"prod-billing-9" OR "prod-billing-10"`,
	`"prod-billing-9" AND "prod-billing-10"`,
	`"foo" OR "bar" OR "baz"`,
	// Mixed piped filters
	`* | service="test" | "A" OR "B" | user=/admin/i`,
	`* | service="test" | "A" AND "B" | user=/admin/i`,
	// NOT in pipeline (bare strings, fields, regex)
	`* | NOT "error"`,
	`* | NOT /error/i`,
	`* | NOT status="500"`,
	`* | NOT "error" OR "warning"`,
	`* | "error" OR NOT "warning"`,
	`* | NOT status="500" AND service="web"`,
	`* | status="200" OR NOT status="500"`,
	// Parenthesized groups in pipeline
	`* | (status="200" OR status="201")`,
	`* | (status="200" OR status="201") AND service="web"`,
	`* | service="web" AND (status="200" OR status="201")`,
	`* | ("error" OR "warning") AND service="web"`,
	`* | NOT (status="500" OR status="503")`,
	`* | NOT ("error" OR "warning")`,
	`* | (status="200" OR status="201") | service="web"`,
	`service="web" | (status="200" OR status="201") | "success"`,
	// Nested parens in pipeline
	`* | (status="200" OR status="201") AND (level="info" OR level="debug")`,
	// Bare regex
	`/powershell/i | command_line!=/cmd/i`,
	`/Convert-GuidToCompressedGuid/i`,
	`/Convert-GuidToCompressedGuid/`,
	// Nested NOT groups
	`NOT (NOT status=200)`,
	`NOT (status=200 AND (host="web-01" OR host="web-02"))`,
	// Edge cases
	``,
	`|`,
	`()`,
	`NOT`,
	`AND`,
	`OR`,
	`| | |`,
	// Complex queries with HAVING (post-groupby filter conditions)
	`event_id=11 | target_filename=/\.exe$/i | groupBy([image,target_filename])`,
	// Having-style conditions after groupby
	`status=500 | groupBy(host) | count() | sort(count, desc) | limit(5)`,
	// Dedup
	`* | dedup(host)`,
	// Negated command
	`* | !in(status, 200, 301)`,
	// Safe SQL keywords in search values
	`message="DROP TABLE logs"`,
	`message="INSERT INTO users"`,
	`raw_log="UNION SELECT * FROM secrets"`,

	// --- Commands missing from seeds above ---
	// Transform commands
	`* | regex(message, "error: (?P<code>\d+)")`,
	`* | replace(message, "error", "warning")`,
	`* | concat(host, ":", port)`,
	`* | lowercase(message)`,
	`* | uppercase(level)`,
	`* | hash(user, algo=sha256)`,
	`* | len(message)`,
	`* | substr(message, 0, 50)`,
	`* | urldecode(path)`,
	`* | base64decode(payload)`,
	`* | split(path, "/")`,
	`* | coalesce(user, "unknown")`,
	`* | sprintf("%s:%s", host, port)`,
	`* | levenshtein(field1, field2)`,
	`* | eval(bytes_sent + bytes_received)`,
	`* | strftime(timestamp, "%Y-%m-%d")`,
	`* | match(message, "error")`,
	`* | lookupIP(src_ip)`,
	// Filter commands
	`* | in(status, 200, 301, 404)`,
	`* | cidr(src_ip, "10.0.0.0/8")`,
	// Aggregate commands
	`* | groupBy(host) | percentile(response_time, 99)`,
	`* | groupBy(host) | stddev(response_time)`,
	`* | groupBy(host) | multi(count(), avg(response_time), max(bytes))`,
	`* | groupBy(host) | top(5)`,
	`* | groupBy(host) | frequency(status)`,
	`* | groupBy(host) | iqr(response_time)`,
	`* | groupBy(host) | mad(response_time)`,
	`* | groupBy(host) | skewness(response_time)`,
	`* | groupBy(host) | kurtosis(response_time)`,
	`* | bucket(timestamp, 1h)`,
	`* | groupBy(host) | selectfirst(message)`,
	`* | groupBy(host) | selectlast(message)`,
	`* | groupBy(host) | headtail(5)`,
	// Visualization commands
	`* | groupBy(status) | count() | piechart()`,
	`* | groupBy(status) | count() | barchart()`,
	`* | groupBy(status) | count() | singleval()`,
	`* | timechart(1h)`,
	`* | groupBy(src_ip, dst_ip) | count() | graph(src_ip, dst_ip)`,
	// Window commands
	`* | modifiedzscore(response_time)`,
	`* | madoutlier(response_time)`,
	`* | histogram(response_time)`,
	// Special commands
	`* | table(timestamp, host, message)`,
	`* | analyzefields()`,
	`* | heatmap(timestamp, status)`,
	// Case (special syntax)
	`* | severity := case { status>=500: "critical"; status>=400: "warning"; default: "ok" }`,
	// Chain (special syntax)
	`* | chain(image) { groupBy(image) | count(); sort(count, desc) }`,
	// Deep multi-pipe (6+ stages)
	`image=/powershell/i | user!="SYSTEM" | groupBy(user, image) | count() | sort(count, desc) | limit(20) | table(user, image, count)`,
	`status>=500 | replace(message, "\n", " ") | groupBy(host) | count() | sort(count, desc) | head(10)`,
	// Pipe with assignment then filter
	`* | total := bytes_sent + bytes_received | total>1000 | groupBy(host) | sum(total)`,
	// Nested function in multi
	`* | groupBy(service) | multi(count(), avg(response_time), percentile(response_time, 95), max(bytes))`,
	// Comment command
	`* | comment(log_id)`,
}

func FuzzParseQuery(f *testing.F) {
	for _, seed := range bqlSeeds {
		f.Add(seed)
	}

	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	f.Fuzz(func(t *testing.T, input string) {
		// Panics are bugs; parse/translate errors are expected.
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ParseQuery panicked on input %q: %v", input, r)
			}
		}()

		pipeline, err := ParseQuery(input)
		if err != nil {
			return
		}

		// If parsing succeeds, translation must not panic either.
		_, _ = TranslateToSQLWithOrder(pipeline, opts)
	})
}

func FuzzLexer(f *testing.F) {
	for _, seed := range bqlSeeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Lexer panicked on input %q: %v", input, r)
			}
		}()

		lexer := NewLexer(input)
		_, _ = lexer.Tokenize()
	})
}

// ---------------------------------------------------------------------------
// Structural property checks for generated SQL
// ---------------------------------------------------------------------------

// checkBalancedParentheses verifies that parentheses are balanced in the SQL,
// ignoring parentheses inside single-quoted string literals.
func checkBalancedParentheses(sql string) error {
	depth := 0
	inString := false
	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		if inString {
			if ch == '\'' {
				// Handle escaped quotes ('')
				if i+1 < len(runes) && runes[i+1] == '\'' {
					i++
					continue
				}
				inString = false
			}
			continue
		}
		switch ch {
		case '\'':
			inString = true
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("unbalanced parentheses: extra ')' at position %d", i)
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("unbalanced parentheses: %d unclosed '('", depth)
	}
	return nil
}

// checkValidClauseStructure ensures SQL clause keywords appear in the correct
// order. Only checks the outermost query (skips content inside parenthesized
// subqueries).
func checkValidClauseStructure(sql string) error {
	// Strip single-quoted string literals to avoid false keyword matches.
	stripped := stripOuterStringLiterals(sql)

	// Remove content inside parentheses to avoid matching subquery keywords.
	flat := removeParenthesizedContent(stripped)

	upper := strings.ToUpper(flat)

	clauses := []struct {
		keyword string
		pos     int
	}{
		{"SELECT", -1},
		{"FROM", -1},
		{"WHERE", -1},
		{"GROUP BY", -1},
		{"HAVING", -1},
		{"ORDER BY", -1},
		{"LIMIT", -1},
	}

	for i := range clauses {
		clauses[i].pos = strings.Index(upper, clauses[i].keyword)
	}

	// Check ordering: each found clause must appear after all previously found clauses.
	lastPos := -1
	lastName := ""
	for _, c := range clauses {
		if c.pos == -1 {
			continue
		}
		if c.pos < lastPos {
			return fmt.Errorf("clause %s (pos %d) appears before %s (pos %d)",
				c.keyword, c.pos, lastName, lastPos)
		}
		lastPos = c.pos
		lastName = c.keyword
	}
	return nil
}

// checkNoEmptyConditions looks for patterns that indicate missing or empty
// conditions in the generated SQL.
func checkNoEmptyConditions(sql string) error {
	stripped := stripOuterStringLiterals(sql)
	upper := strings.ToUpper(stripped)

	badPatterns := []string{
		"WHERE AND",
		"WHERE OR",
		"AND AND",
		"OR OR",
		"AND OR AND",
		"OR AND OR",
	}
	for _, pat := range badPatterns {
		if strings.Contains(upper, pat) {
			return fmt.Errorf("empty condition pattern %q found in SQL", pat)
		}
	}

	// Check for trailing boolean operators before closing paren: "AND )" or "OR )"
	for _, op := range []string{"AND )", "OR )"} {
		if strings.Contains(upper, op) {
			return fmt.Errorf("trailing operator before ')': %q found in SQL", op)
		}
	}

	// Check for empty parenthesized conditions "()" in the WHERE/HAVING clauses.
	// Ignore function calls by checking that the char before '(' is not a letter.
	for i := 0; i < len(stripped)-1; i++ {
		if stripped[i] == '(' && stripped[i+1] == ')' {
			// If preceded by a letter or underscore, it is likely a function call - skip.
			if i > 0 && (unicode.IsLetter(rune(stripped[i-1])) || stripped[i-1] == '_') {
				continue
			}
			return fmt.Errorf("empty parentheses '()' found at position %d", i)
		}
	}

	return nil
}

// checkNoDoubleWhitespace verifies there are no double spaces in the SQL,
// which often indicates a missing or dropped condition.
func checkNoDoubleWhitespace(sql string) error {
	// Strip string literals so user search values with double spaces don't trigger.
	stripped := stripOuterStringLiterals(sql)
	if strings.Contains(stripped, "  ") {
		return fmt.Errorf("double whitespace found in generated SQL (possible missing condition)")
	}
	return nil
}

// stripOuterStringLiterals replaces single-quoted string contents with empty
// strings, preserving the quotes themselves.
func stripOuterStringLiterals(sql string) string {
	var b strings.Builder
	inString := false
	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		if inString {
			if ch == '\'' {
				if i+1 < len(runes) && runes[i+1] == '\'' {
					i++ // skip escaped quote
					continue
				}
				inString = false
				b.WriteRune(ch)
			}
			// Skip characters inside string literals.
			continue
		}
		if ch == '\'' {
			inString = true
			b.WriteRune(ch)
			continue
		}
		b.WriteRune(ch)
	}
	return b.String()
}

// removeParenthesizedContent replaces everything inside parentheses with
// empty strings, handling nesting. Used so subquery keywords don't confuse
// outer clause order checks.
func removeParenthesizedContent(sql string) string {
	var b strings.Builder
	depth := 0
	for _, ch := range sql {
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func TestSQLStructuralProperties(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	queries := []struct {
		name  string
		query string
	}{
		// Every operator
		{"eq", `status=200`},
		{"neq", `status!=200`},
		{"gt", `status>400`},
		{"gte", `status>=400`},
		{"lt", `status<300`},
		{"lte", `status<=200`},
		{"regex_field", `image=/powershell/i`},
		{"regex_neg", `command_line!=/mp/i`},
		{"regex_complex", `message=/error.*failed/i`},

		// Value types
		{"numeric_value", `bytes>1024`},
		{"string_value", `host="web-01"`},
		{"quoted_phrase", `"connection refused"`},
		{"quoted_exe", `"powershell.exe"`},

		// Wildcards
		{"wildcard", `*`},

		// Boolean operators
		{"and", `status=500 AND host="web-01"`},
		{"or", `status=404 OR status=500`},
		{"not_simple", `NOT status=200`},
		{"not_group", `NOT (status=200 OR status=301)`},
		{"nested_not", `NOT (NOT status=200)`},
		{"complex_bool", `(status=500 OR status=502) AND host="web-01"`},
		{"multi_group", `(level=error AND host="db-01") OR (level=warn AND host="db-02")`},
		{"and_not", `host="web-01" AND NOT level=debug`},
		{"implicit_and", `image=/powershell/i user=admin`},

		// Groupby
		{"groupby_single", `* | groupBy(host)`},
		{"groupby_multi", `* | groupBy(host, status)`},
		{"groupby_bracket", `* | groupBy([image, user])`},

		// Count
		{"count_simple", `* | count()`},
		{"count_with_groupby", `* | groupBy(host) | count()`},
		{"count_with_filter", `image=/powershell/i | count()`},

		// Stats functions
		{"sum", `* | sum(bytes)`},
		{"avg", `* | avg(response_time)`},
		{"min", `* | min(response_time)`},
		{"max", `* | max(bytes)`},
		{"median", `* | median(duration)`},
		{"sum_with_groupby", `* | groupBy(image) | sum(bytes)`},

		// Sort
		{"sort_asc", `* | sort(timestamp, asc)`},
		{"sort_desc", `* | sort(timestamp, desc)`},
		{"sort_default", `* | sort(user)`},

		// Limit
		{"limit_10", `* | limit(10)`},
		{"limit_50", `* | limit(50)`},

		// Table
		{"table_fields", `* | table(timestamp, image, user)`},
		{"table_with_count", `* | table(image, count)`},

		// Head / tail
		{"head", `* | head(5)`},
		{"tail", `* | tail(10)`},

		// Dedup
		{"dedup", `* | dedup(host)`},

		// Assignments
		{"assignment", `* | duration := response_time`},

		// Complex multi-pipe
		{"multi_pipe_full", `image=/powershell/i | groupBy(user) | count() | sort(count, desc) | limit(10)`},
		{"multi_pipe_filters", `event_id=1 | image=/powershell/i | command_line!=/mp/i`},
		{"filter_then_count", `image=/powershell/i | count()`},

		// Quoted string boolean searches
		{"quoted_or", `"prod-billing-9" OR "prod-billing-10"`},
		{"quoted_and", `"prod-billing-9" AND "prod-billing-10"`},
		{"quoted_multi_or", `"foo" OR "bar" OR "baz"`},

		// Mixed piped filters
		{"mixed_pipe_or", `* | service="test" | "A" OR "B" | user=/admin/i`},
		{"mixed_pipe_and", `* | service="test" | "A" AND "B" | user=/admin/i`},

		// NOT in pipeline
		{"not_string_pipeline", `* | NOT "error"`},
		{"not_field_pipeline", `* | NOT status="500"`},
		{"not_string_or_pipeline", `* | NOT "error" OR "warning"`},
		{"not_field_and_pipeline", `* | NOT status="500" AND service="web"`},
		{"mid_compound_not", `* | "error" OR NOT "warning"`},

		// Parenthesized groups in pipeline
		{"paren_group_pipeline", `* | (status="200" OR status="201")`},
		{"paren_group_and_field", `* | (status="200" OR status="201") AND service="web"`},
		{"field_and_paren_group", `* | service="web" AND (status="200" OR status="201")`},
		{"not_paren_group", `* | NOT (status="500" OR status="503")`},
		{"paren_strings_and_field", `* | ("error" OR "warning") AND service="web"`},
		{"nested_paren_groups", `* | (status="200" OR status="201") AND (level="info" OR level="debug")`},
		{"paren_group_multi_pipe", `* | (status="200" OR status="201") | service="web"`},

		// Bare regex
		{"bare_regex_i", `/powershell/i | command_line!=/cmd/i`},
		{"bare_regex_plain", `/Convert-GuidToCompressedGuid/`},

		// Post-groupby filter (HAVING)
		{"post_groupby_regex", `event_id=11 | target_filename=/\.exe$/i | groupBy([image,target_filename])`},

		// Numeric comparison with boolean
		{"numeric_bool", `bytes>1000 AND status=200`},
		{"numeric_range", `status>=400 AND status<500`},
		{"numeric_gte", `status_code>=500`},

		// Safe SQL keywords in search values
		{"sql_in_value_drop", `message="DROP TABLE logs"`},
		{"sql_in_value_insert", `message="INSERT INTO users"`},
		{"sql_in_value_union", `raw_log="UNION SELECT * FROM secrets"`},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tc.query)
			if err != nil {
				t.Skipf("parse error (acceptable): %v", err)
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Skipf("translate error (acceptable): %v", err)
			}

			sql := result.SQL

			if err := checkBalancedParentheses(sql); err != nil {
				t.Errorf("query %q: %v\nSQL: %s", tc.query, err, sql)
			}
			if err := checkValidClauseStructure(sql); err != nil {
				t.Errorf("query %q: %v\nSQL: %s", tc.query, err, sql)
			}
			if err := checkNoEmptyConditions(sql); err != nil {
				t.Errorf("query %q: %v\nSQL: %s", tc.query, err, sql)
			}
			if err := checkNoDoubleWhitespace(sql); err != nil {
				t.Errorf("query %q: %v\nSQL: %s", tc.query, err, sql)
			}
		})
	}
}
