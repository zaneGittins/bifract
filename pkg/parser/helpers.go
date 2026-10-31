package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// buildAnalyzeFieldsSQL generates a query that computes per-field statistics.
func buildAnalyzeFieldsSQL(
	fieldsList []string,
	scanLimit int,
	whereConditions []string,
	havingConditions []string,
	orderByFields []string,
	limitClause string,
	chartType string,
	chartConfig map[string]interface{},
	opts QueryOptions,
) (*TranslationResult, error) {
	var sql strings.Builder

	// Build WHERE clause for the inner scan
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = " WHERE " + andJoin(whereConditions)
	}

	// Build optional path filter for specific fields
	pathFilter := ""
	if len(fieldsList) > 0 {
		var paths []string
		for _, f := range fieldsList {
			paths = append(paths, fmt.Sprintf("'%s'", escapeString(f)))
		}
		pathFilter = fmt.Sprintf(" WHERE kv.1 IN (%s)", strings.Join(paths, ", "))
	}

	// HAVING conditions become WHERE on the outermost query
	outerFilter := ""
	if len(havingConditions) > 0 {
		outerFilter = " WHERE " + andJoin(havingConditions)
	}

	// Order: default to _events DESC, respect user sort if provided
	orderClause := "ORDER BY _events DESC"
	if len(orderByFields) > 0 {
		orderClause = "ORDER BY " + strings.Join(orderByFields, ", ")
	}

	// Output limit: default high for field metadata (not raw logs)
	outputLimit := "LIMIT 10000"
	if limitClause != "" {
		outputLimit = limitClause
	}

	// Two-level query using JSONExtractKeysAndValues to get all field names + values:
	// 1. Inner: explode each log row into (key, value) tuples
	// 2. Outer: aggregate per-field statistics
	sql.WriteString("SELECT field_name, _events, _distinct_vals, _mean, _min, _max, _stdev FROM (")
	sql.WriteString("SELECT kv.1 AS field_name, ")
	sql.WriteString("count() AS _events, ")
	sql.WriteString("uniqExact(kv.2) AS _distinct_vals, ")
	sql.WriteString("round(avg(toFloat64OrNull(kv.2)), 2) AS _mean, ")
	sql.WriteString("min(toFloat64OrNull(kv.2)) AS _min, ")
	sql.WriteString("max(toFloat64OrNull(kv.2)) AS _max, ")
	sql.WriteString("round(stddevPop(toFloat64OrNull(kv.2)), 2) AS _stdev ")
	sql.WriteString("FROM (")
	sql.WriteString(fmt.Sprintf("SELECT arrayJoin(JSONExtractKeysAndValues(norm_log, 'String')) AS kv FROM %s%s LIMIT %d",
		opts.EffectiveTableName(), whereClause, scanLimit))
	sql.WriteString(")")
	sql.WriteString(pathFilter)
	sql.WriteString(" GROUP BY kv.1 ")
	sql.WriteString(orderClause)
	sql.WriteString(" ")
	sql.WriteString(outputLimit)
	sql.WriteString(")")
	sql.WriteString(outerFilter)

	finalSQL := sql.String()
	if err := validateGeneratedSQL(finalSQL); err != nil {
		return nil, err
	}

	return &TranslationResult{
		SQL:          finalSQL,
		FieldOrder:   []string{"field_name", "_events", "_distinct_vals", "_mean", "_min", "_max", "_stdev"},
		IsAggregated: true,
		ChartType:    chartType,
		ChartConfig:  chartConfig,
	}, nil
}

func procLineageFractalCond(opts QueryOptions, prefix string) string {
	col := prefix + "fractal_id"
	if len(opts.FractalIDs) > 0 {
		quoted := make([]string, 0, len(opts.FractalIDs)+1)
		for _, id := range opts.FractalIDs {
			quoted = append(quoted, fmt.Sprintf("'%s'", escapeString(id)))
		}
		if opts.IncludeEmptyFractalID {
			quoted = append(quoted, "''")
		}
		return col + " IN (" + strings.Join(quoted, ", ") + ")"
	}
	if opts.FractalID != "" {
		if opts.IncludeEmptyFractalID {
			return fmt.Sprintf("%s IN ('%s', '')", col, escapeString(opts.FractalID))
		}
		return fmt.Sprintf("%s = '%s'", col, escapeString(opts.FractalID))
	}
	return ""
}

var procTreeFieldOrder = []string{"timestamp", "process_guid", "parent_guid", "image", "parent_image", "commandline", "computer_name", "_depth", "_path", "group_size", "group_first", "group_last"}

// ptg() projected into pgr()'s edge shape for pgraph(). Column names, order, and expressions
// mirror provenance spawn rows (see pkg/query/provenance.go) so pgraph stays a single-shape
// renderer. command_line is capped like pgr's; proc_user is empty because ptg() does not
// read it.
const procTreePgraphCols = "parent_guid AS parent, process_guid AS child, image AS label, 'spawn' AS event_type, " +
	"log_id, timestamp, fractal_id, substring(commandline, 1, 300) AS command_line, " +
	"'' AS proc_user, computer_name AS host, parent_image AS parent_label, group_size, group_first, group_last"

var procTreePgraphFieldOrder = []string{"parent", "child", "label", "event_type", "log_id", "timestamp", "fractal_id", "command_line", "proc_user", "host", "parent_label", "group_size", "group_first", "group_last"}

// buildProcessTreeSQL translates ptg() over its tree, which the query layer walked before
// translation (QueryOptions.ProcessTree): a literal source, one row per node. User pre-filters
// do not apply (ptg() is scoped by start=, the time range and the fractal); only _depth/_path
// post-filters do.
func buildProcessTreeSQL(
	startValue string,
	havingConditions []string,
	chartType string, chartConfig map[string]interface{},
	opts QueryOptions,
) (*TranslationResult, error) {
	if opts.ProcessTree == nil {
		return nil, fmt.Errorf("ptg() is not available here: its process tree is only walked for searches, dashboards and alerts")
	}

	// Post-traversal filters route here from window-field conditions (_depth/_path). Drop
	// any that reference logs-only columns (a compound like `_depth<=3 AND image="x"` would
	// otherwise inject fields.image and crash the flat-table query).
	var safeHaving []string
	for _, h := range havingConditions {
		if strings.Contains(h, "fields.") || strings.Contains(h, "norm_log") || strings.Contains(h, "raw_log") {
			continue
		}
		safeHaving = append(safeHaving, h)
	}
	havingConditions = safeHaving

	const unionCols = "timestamp, log_id, fractal_id, process_guid, parent_guid, image, parent_image, commandline, computer_name, _depth, _path, group_size, group_first, group_last"
	const finalCols = "substring(timestamp, 1, 19) AS timestamp, process_guid, parent_guid, image, parent_image, commandline, computer_name, log_id, toString(_depth) AS _depth, _path, group_size, group_first, group_last"
	traversal := processTreeValuesSQL(opts.ProcessTree)

	// Post-traversal filters (e.g. _depth <= 3) MUST be applied in an inner scope where
	// _depth is still UInt32. If the filter shares a SELECT with `toString(_depth) AS _depth`,
	// ClickHouse resolves the WHERE identifier to the String alias (NO_COMMON_TYPE, code 386),
	// so the toString projection lives in a strictly outer SELECT than the filter.
	inner := "SELECT " + unionCols + " FROM " + traversal
	if len(havingConditions) > 0 {
		inner += " WHERE " + andJoin(havingConditions)
	}

	// pgraph() renders the pgr() edge shape, so a `ptg() | pgraph()` tree is projected into
	// those same columns: every row is one spawn edge (parent_guid -> process_guid). There is
	// no anomaly_score column -- proc_lineage carries no baseline, and pgraph treats a missing
	// score as unscored -- and no leaf/reconnection edges: a ptg graph is process creation only.
	outCols, fieldOrder := finalCols, procTreeFieldOrder
	if chartType == "pgraph" {
		outCols = procTreePgraphCols
		fieldOrder = procTreePgraphFieldOrder
		if chartConfig == nil {
			chartConfig = map[string]interface{}{}
		}
		chartConfig["focus"] = startValue // center/highlight the seed process, as pgr() does
		chartConfig["scored"] = false     // hide the anomaly chrome: these edges carry no score
	}

	var sql strings.Builder
	sql.WriteString("SELECT ")
	sql.WriteString(outCols)
	sql.WriteString(" FROM (")
	sql.WriteString(inner)
	sql.WriteString(") ORDER BY _path ASC")
	if opts.MaxRows > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", opts.MaxRows))
	}

	finalSQL := sql.String()
	if err := validateGeneratedSQL(finalSQL); err != nil {
		return nil, err
	}

	return &TranslationResult{
		SQL:          finalSQL,
		FieldOrder:   fieldOrder,
		IsAggregated: false,
		ChartType:    chartType,
		ChartConfig:  chartConfig,
	}, nil
}

// qualifyColumnRefs prefixes bare column references with a table alias,
// skipping content inside SQL string literals to avoid corrupting values.
func qualifyColumnRefs(sql, alias string) string {
	var result strings.Builder
	inString := false

	for i := 0; i < len(sql); i++ {
		if inString {
			result.WriteByte(sql[i])
			if sql[i] == '\\' && i+1 < len(sql) {
				i++
				result.WriteByte(sql[i])
			} else if sql[i] == '\'' {
				inString = false
			}
			continue
		}

		if sql[i] == '\'' {
			result.WriteByte(sql[i])
			inString = true
			continue
		}

		rest := sql[i:]
		replaced := false

		// fields.`...` - JSON subcolumn reference; may have multiple backtick-quoted
		// segments for nested paths (e.g. fields.`event`.`name`.:String)
		if strings.HasPrefix(rest, "fields.`") && (i == 0 || !isWordByte(sql[i-1])) {
			end := 6 // len("fields") - start scanning from the first dot
			for end < len(rest) && rest[end] == '.' && end+1 < len(rest) && rest[end+1] == '`' {
				end += 2 // skip .`
				for end < len(rest) {
					if rest[end] == '`' {
						if end+1 < len(rest) && rest[end+1] == '`' {
							end += 2 // escaped backtick
						} else {
							end++ // closing backtick
							break
						}
					} else {
						end++
					}
				}
			}
			// Include .:String type suffix if present
			if end < len(rest) && strings.HasPrefix(rest[end:], ".:String") {
				end += len(".:String")
			}
			result.WriteString(alias + "." + rest[:end])
			i += end - 1
			replaced = true
		}

		if !replaced {
			for _, col := range []string{"fractal_id", "timestamp", "norm_log", "log_id"} {
				if strings.HasPrefix(rest, col) {
					prevOk := i == 0 || !isWordByte(sql[i-1])
					nextOk := i+len(col) >= len(sql) || !isWordByte(sql[i+len(col)])
					if prevOk && nextOk {
						result.WriteString(alias + "." + col)
						i += len(col) - 1
						replaced = true
						break
					}
				}
			}
		}

		if !replaced {
			result.WriteByte(sql[i])
		}
	}

	return result.String()
}

func isWordByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// collectHavingConditionFields recursively collects all leaf field names from
// HavingConditions (including compound nodes) into the provided map.
func collectHavingConditionFields(conditions []HavingCondition, fields map[string]bool) {
	for _, cond := range conditions {
		if cond.IsCompound {
			collectHavingConditionFields(cond.Children, fields)
			continue
		}
		if cond.Field != "" {
			fields[cond.Field] = true
		}
		if cond.ValueField != "" {
			fields[cond.ValueField] = true
		}
	}
}

func escapeString(s string) string {
	// Escape backslashes first (for ClickHouse regex patterns)
	s = strings.ReplaceAll(s, "\\", "\\\\")
	// Escape single quotes for SQL
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}

// unescapeSQLString reverses escapeString (\\ -> \, \' -> '). A single left-to-
// right scan is used rather than two ReplaceAlls so overlapping sequences like
// \\' (an escaped backslash followed by an escaped quote) decode correctly.
func unescapeSQLString(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			b.WriteByte(s[i]) // emit the escaped char (\ or ') literally
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// namedGroupOpenRe matches the opening syntax of a named capture group in every
// flavor users write: (?<n>, (?P<n>, (?P'n' and (?'n'.
var namedGroupOpenRe = regexp.MustCompile(`^\(\?P?(?:<([a-zA-Z_][a-zA-Z0-9_]*)>|'([a-zA-Z_][a-zA-Z0-9_]*)')`)

// captureToken is one capturing group's opening syntax within a pattern.
type captureToken struct {
	start, end int
	name       string // empty for an unnamed group
}

// scanCaptureGroups locates every capturing group in a pattern, skipping escaped
// characters, character classes, and non-capturing constructs ((?:, (?i), (?=).
func scanCaptureGroups(pattern string) []captureToken {
	var out []captureToken
	inClass := false
	for i := 0; i < len(pattern); i++ {
		switch ch := pattern[i]; {
		case ch == '\\':
			i++
		case ch == '[' && !inClass:
			inClass = true
		case ch == ']' && inClass:
			inClass = false
		case ch == '(' && !inClass:
			if m := namedGroupOpenRe.FindStringSubmatchIndex(pattern[i:]); m != nil {
				name := ""
				for g := 1; g <= 2; g++ {
					if m[2*g] >= 0 {
						name = pattern[i+m[2*g] : i+m[2*g+1]]
					}
				}
				out = append(out, captureToken{start: i, end: i + m[1], name: name})
				i += m[1] - 1
			} else if i+1 >= len(pattern) || pattern[i+1] != '?' {
				out = append(out, captureToken{start: i, end: i + 1})
			}
		}
	}
	return out
}

// rewriteCaptureGroups returns a pattern safe for ClickHouse plus the names of its
// named capture groups in order. Named-group syntax is stripped to a plain group
// (RE2 in ClickHouse does not accept every flavor), and when named groups exist the
// unnamed ones become non-capturing so group indices line up with the names.
func rewriteCaptureGroups(pattern string) (string, []string) {
	tokens := scanCaptureGroups(pattern)
	var names []string
	for _, t := range tokens {
		if t.name != "" {
			names = append(names, t.name)
		}
	}
	if len(names) == 0 {
		return pattern, nil
	}
	var b strings.Builder
	b.Grow(len(pattern) + 8)
	prev := 0
	for _, t := range tokens {
		b.WriteString(pattern[prev:t.start])
		if t.name != "" {
			b.WriteString("(")
		} else {
			b.WriteString("(?:")
		}
		prev = t.end
	}
	b.WriteString(pattern[prev:])
	return b.String(), names
}

// NamedCaptureGroups returns the names of a pattern's named capture groups, in
// order. Callers outside the parser use it to agree with regex() on which output
// columns a pattern produces.
func NamedCaptureGroups(pattern string) []string {
	_, names := rewriteCaptureGroups(pattern)
	return names
}

// captureGroupCount reports how many capturing groups a pattern has.
func captureGroupCount(pattern string) int {
	return len(scanCaptureGroups(pattern))
}

// dictRef returns the name to pass to dictGet/dictHas, qualified with the
// database the dictionary objects live in. A Distributed fan-out executes the
// remote half of a query with the shard connection's own current database, which
// is not the one holding the dictionaries, so an unqualified name resolves to
// nothing there and the shard fails the whole query with BAD_ARGUMENTS (36).
func dictRef(db, name string) string {
	if db == "" {
		return name
	}
	return db + "." + name
}

// validateGeneratedSQL checks the final SQL for dangerous patterns that should never
// appear in translator output, ignoring strings and quoted names so a search value
// or field name containing a keyword is not mistaken for one.
func validateGeneratedSQL(sql string) error {
	stripped := stripStringLiterals(sql)

	// Normalize to uppercase for case-insensitive matching
	upper := strings.ToUpper(stripped)

	// Dangerous SQL statements that our translator should never produce.
	// These are checked as word-boundary patterns in the structural SQL only.
	dangerousPatterns := []string{
		"DROP ",
		"ALTER ",
		"TRUNCATE ",
		"INSERT ",
		"UPDATE ",
		"DELETE ",
		"CREATE ",
		"ATTACH ",
		"DETACH ",
		"RENAME ",
		"GRANT ",
		"REVOKE ",
		"KILL ",
		"SYSTEM ",
		"; SELECT",
		"; DROP",
		"INTO OUTFILE",
		"INTO DUMPFILE",
	}

	// Recursive CTEs legitimately use UNION ALL; only check for injection-style
	// UNION patterns in non-recursive queries.
	if !strings.Contains(upper, "WITH RECURSIVE") {
		dangerousPatterns = append(dangerousPatterns, "UNION SELECT", "UNION ALL SELECT")
	}

	for _, pattern := range dangerousPatterns {
		if strings.Contains(upper, pattern) {
			return fmt.Errorf("query rejected: generated SQL contains dangerous pattern %q", pattern)
		}
	}
	return nil
}

// stripStringLiterals empties every single-quoted string and backquoted
// identifier, honouring backslash escapes, so the structure check never reads a
// search value or a name as SQL.
func stripStringLiterals(sql string) string {
	var result strings.Builder
	var quote byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case quote == 0:
			result.WriteByte(c)
			if c == '\'' || c == '`' {
				quote = c
			}
		case c == '\\':
			i++
		case c == quote:
			result.WriteByte(c)
			quote = 0
		}
	}
	return result.String()
}

// validIdentifier matches safe SQL identifier characters only
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.\-]*$`)

// sanitizeIdentifier validates and quotes an identifier for use as a SQL alias or field name.
// Returns the identifier wrapped in backticks if it contains special chars, or as-is if simple.
// Returns error if the identifier contains characters that could enable SQL injection.
func sanitizeIdentifier(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("empty identifier")
	}
	if !validIdentifier.MatchString(s) {
		return "", fmt.Errorf("invalid identifier %q: contains unsafe characters", s)
	}
	// Backtick-quote if it contains dots or hyphens (valid in field names but not safe as bare SQL aliases)
	if strings.ContainsAny(s, ".-") {
		return "`" + s + "`", nil
	}
	return s, nil
}

// jsonDefaultTypeHintedFields holds the project-level defaults. Never modified after init.
var jsonDefaultTypeHintedFields = map[string]bool{
	"computer_name":       true,
	"user":                true,
	"src_ip":              true,
	"dst_ip":              true,
	"src_port":            true,
	"dst_port":            true,
	"commandline":         true,
	"hash":                true,
	"event_id":            true,
	"image":               true,
	"parent_image":        true,
	"call_chain":          true,
	"operation":           true,
	"artifact":            true,
	"query":               true,
	"original_file_name":  true,
	"proto":               true,
	"conn_state":          true,
	"duration":            true,
	"orig_bytes":          true,
	"resp_bytes":          true,
	"bifract_category":    true,
	"process_guid":        true,
	"parent_process_guid": true,
	"target_image":        true,
	"target_file":         true,
}

// SetCustomTypeHintedFields is retained as a no-op for API stability (called at
// startup and from the schema-fields admin handler). It formerly told the parser
// which custom fields were type-hinted so jsonFieldRef could emit a bare (no
// ::String) sub-column ref for them. jsonFieldRef now casts all refs uniformly
// (the bare-ref optimization was based on a false premise; see jsonFieldRef), so
// this set no longer affects query generation. The ClickHouse-side schema hint /
// skip-index management is driven by ReconcileSchemaFields (from Postgres),
// independent of this call.
func SetCustomTypeHintedFields(custom map[string]bool) {}

// jsonFieldRef returns the ClickHouse JSON subcolumn reference for a field name.
// Dots in the field name are treated as nested path separators, producing
// fields.`event`.`name` for "event.name".
//
// ALL subcolumn refs are cast to ::String, uniformly. This is the single
// invariant that makes every downstream context safe: GROUP BY / ORDER BY /
// DISTINCT / LIMIT BY / IN / JOIN keys / multiSearch / isIPv4String all REJECT a
// bare Dynamic subcolumn (error 44/43), and on a mixed-history table a path is
// stored as Dynamic in parts ingested before it was type-hinted. The ::String
// cast is a no-op for concretely-typed paths and does NOT defeat the skip index:
// a cast to the column's own type is elided by the ClickHouse analyzer before
// index analysis (verified on CH 26.6 for bloom_filter AND set indexes, both =
// and IN, with identical granule pruning). Bifract runs CH 26.6+ everywhere.
//
// The former "bare ref for type-hinted fields" optimization was based on the
// false premise that CAST breaks index matching; it also caused a whole class of
// error-44/43 bugs on mixed-history tables. Removed. See groupableCast (now an
// idempotent no-op given this invariant, kept as a defensive guard).
func jsonFieldRef(field string) string {
	parts := strings.Split(field, ".")
	var b strings.Builder
	b.WriteString("fields")
	for _, p := range parts {
		b.WriteString(".`")
		b.WriteString(EscapeCHBacktickIdent(p))
		b.WriteString("`")
	}
	return b.String() + "::String"
}

// EscapeCHBacktickIdent escapes a string for use inside a ClickHouse
// backtick-quoted identifier. ClickHouse honours BOTH a doubled backtick and a
// backslash-escaped backtick inside the quotes, so a name may contain a
// backslash that escapes the closing quote: doubling only backticks then leaves
// `\` + “ “ “ reading as an escaped-backtick-plus-close, breaking out of the
// identifier. Escaping the backslash first closes that hole. Field names reach
// here straight from a user's query (sort, dedup, concat, ...), so this is the
// boundary that keeps an attacker-supplied name from becoming SQL.
func EscapeCHBacktickIdent(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "`", "``")
}

// quoteIdent renders a column name as an identifier, backtick-quoted unless it is
// a plain one (a dotted or hyphenated field name is not).
func quoteIdent(s string) string {
	if isBareIdentifier(s) {
		return s
	}
	return "`" + EscapeCHBacktickIdent(s) + "`"
}

// validateNumeric ensures a value is a valid number, preventing SQL injection in numeric contexts.
func validateNumeric(s string) error {
	_, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("expected numeric value, got %q", s)
	}
	return nil
}

// validateInt ensures a value is a valid positive integer (for LIMIT clauses).
func validateInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("expected integer value, got %q", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("expected positive integer, got %d", n)
	}
	return n, nil
}

func escapeRegexForClickHouse(pattern string) string {
	// For ClickHouse regex patterns, we need to:
	// 1. Escape backslashes for string literal
	// 2. Wrap in single quotes
	escaped := strings.ReplaceAll(pattern, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "'", "\\'")
	return "'" + escaped + "'"
}

// extractLiteralTokens pulls contiguous alphabetic sequences from a regex
// pattern, matching what splitByNonAlpha produces. Only tokens >= 3 chars
// are returned (shorter ones are too common for useful index pruning).
func extractLiteralTokens(pattern string) []string {
	p := pattern
	if strings.HasPrefix(p, "(?i)") {
		p = p[4:]
	}

	var tokens []string
	var current []byte
	i := 0
	for i < len(p) {
		ch := p[i]
		if ch == '\\' {
			if len(current) > 0 {
				tokens = append(tokens, string(current))
				current = current[:0]
			}
			i += 2
			continue
		}
		if ch == '[' {
			if len(current) > 0 {
				tokens = append(tokens, string(current))
				current = current[:0]
			}
			for i < len(p) && p[i] != ']' {
				i++
			}
			i++
			continue
		}
		if ch == '.' || ch == '*' || ch == '+' || ch == '?' ||
			ch == '(' || ch == ')' || ch == '{' || ch == '}' ||
			ch == '|' || ch == '^' || ch == '$' {
			if len(current) > 0 {
				tokens = append(tokens, string(current))
				current = current[:0]
			}
			i++
			continue
		}
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			current = append(current, ch)
		} else {
			if len(current) > 0 {
				tokens = append(tokens, string(current))
				current = current[:0]
			}
		}
		i++
	}
	if len(current) > 0 {
		tokens = append(tokens, string(current))
	}

	seen := make(map[string]bool)
	var result []string
	for _, t := range tokens {
		if len(t) < 3 {
			continue
		}
		lower := strings.ToLower(t)
		if seen[lower] {
			continue
		}
		// Pure-digit tokens (e.g. "123") are unsafe as hasToken pre-filters:
		// they can appear embedded inside larger alphanumeric tokens in raw_log
		// (e.g. "error123" is ONE tokenbf_v1 token), causing false negatives.
		// Mixed tokens like "namtws003" are kept because they contain alpha chars
		// and are therefore unlikely to be substrings of other tokens.
		hasAlpha := false
		for _, c := range lower {
			if c >= 'a' && c <= 'z' {
				hasAlpha = true
				break
			}
		}
		if hasAlpha {
			seen[lower] = true
			result = append(result, lower)
		}
	}
	return result
}

// normLogColumn is the canonical ClickHouse text column (the flat serialized normalized
// fields). It carries the lower(norm_log) n-gram text index, so case-insensitive searches
// against it are rewritten to match(lower(norm_log), ...) to enable granule pruning.
// (raw_log is a demoted, non-BQL-addressable troubleshooting column.)
const normLogColumn = "norm_log"

// ingestTimeColumn is the hidden alias ingest_timestamp is projected under for the
// detail panel. Dropped from the field order like _shard_num, so it is row data and
// not a result column; the unprefixed name stays free for | table(ingest_timestamp).
const ingestTimeColumn = "_ingest_timestamp"

// caseInsensitiveFlag is RE2's inline case-insensitivity flag, prepended to a
// pattern by the lexer for /regex/i and by the parser for bare-term searches.
const caseInsensitiveFlag = "(?i)"

// buildEqualityListSQL renders a comma-separated equality list (field="a","b")
// as an IN / NOT IN predicate. For JSON sub-columns the negated form also
// admits NULL so rows missing the field are not silently dropped (NULL NOT IN
// (...) evaluates to NULL, which is falsy).
func buildEqualityListSQL(fieldRef string, values []string, negate, isJSONField bool) string {
	// IN / NOT IN reject a bare Dynamic subcolumn (error 43). Cast raw JSON refs
	// to ::String so a mixed-history (pre-type-hint) path still works; the skip
	// index is preserved (a no-op cast to the column's own type is elided by the
	// analyzer). Matches the existing fields.X::String IN (...) pattern in
	// provenance.go.
	fieldRef = groupableCast(fieldRef)
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "'" + escapeString(v) + "'"
	}
	list := strings.Join(quoted, ", ")
	if negate {
		if isJSONField {
			return fmt.Sprintf("(%s IS NULL OR %s NOT IN (%s))", fieldRef, fieldRef, list)
		}
		return fmt.Sprintf("%s NOT IN (%s)", fieldRef, list)
	}
	return fmt.Sprintf("%s IN (%s)", fieldRef, list)
}

// buildIcebergEqualityListSQL renders a comma-separated equality list against an
// Iceberg source, reusing icebergEqualityPredicate per value so MAP correctness
// and promoted `_ice_` column pruning are preserved.
func buildIcebergEqualityListSQL(field string, values []string, negate bool, promoted map[string]bool) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = icebergEqualityPredicate(field, v, promoted)
	}
	inner := strings.Join(parts, " OR ")
	if negate {
		return "NOT (" + inner + ")"
	}
	return "(" + inner + ")"
}

// buildContainsAnySQL returns a case-insensitive substring-contains-any expression.
// Uses multiSearchAnyCaseInsensitive (Volnitsky/SIMD multi-pattern search), which is
// significantly faster than equivalent regex alternation and is accelerated by text
// (inverted) skip indexes when those are present on the target column.
func buildContainsAnySQL(fieldRef string, values []string, negate bool, mode SourceMode) string {
	values = normLogSearchValues(fieldRef, values, mode)
	// multiSearchAnyCaseInsensitive rejects a bare Dynamic subcolumn. Cast raw
	// JSON refs to ::String so a mixed-history (pre-type-hint) path still works;
	// bloom_filter/set indexes do not accelerate substring search anyway, so no
	// index is lost.
	fieldRef = groupableCast(fieldRef)
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "'" + escapeString(v) + "'"
	}
	// multiSearch* functions take at most 255 needles, so a longer list is split.
	const maxNeedles = 255
	var calls []string
	for start := 0; start == 0 || start < len(quoted); start += maxNeedles {
		chunk := quoted[start:min(start+maxNeedles, len(quoted))]
		calls = append(calls, fmt.Sprintf("multiSearchAnyCaseInsensitive(%s, [%s])", fieldRef, strings.Join(chunk, ", ")))
	}
	expr := strings.Join(calls, " OR ")
	if negate {
		return "NOT (" + expr + ")"
	}
	if len(calls) > 1 {
		expr = "(" + expr + ")"
	}
	return expr
}

// buildStartsWithAnySQL returns a case-insensitive prefix-match-any expression.
// Uses startsWith(lower(field), lowered_term) which is accelerated by text indexes.
func buildStartsWithAnySQL(fieldRef string, values []string, negate bool, mode SourceMode) string {
	values = normLogSearchValues(fieldRef, values, mode)
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("startsWith(lower(%s), '%s')", fieldRef, escapeString(strings.ToLower(v)))
	}
	var expr string
	if len(parts) == 1 {
		expr = parts[0]
	} else {
		expr = "(" + strings.Join(parts, " OR ") + ")"
	}
	if negate {
		return "NOT (" + expr + ")"
	}
	return expr
}

// buildEndsWithAnySQL returns a case-insensitive suffix-match-any expression.
// Uses endsWith(lower(field), lowered_term) which is accelerated by text indexes.
func buildEndsWithAnySQL(fieldRef string, values []string, negate bool, mode SourceMode) string {
	values = normLogSearchValues(fieldRef, values, mode)
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("endsWith(lower(%s), '%s')", fieldRef, escapeString(strings.ToLower(v)))
	}
	var expr string
	if len(parts) == 1 {
		expr = parts[0]
	} else {
		expr = "(" + strings.Join(parts, " OR ") + ")"
	}
	if negate {
		return "NOT (" + expr + ")"
	}
	return expr
}

// buildRegexMatchSQL returns a match() expression for use in WHERE clauses.
//
// For case-insensitive searches on norm_log we emit match(lower(norm_log), <lowered
// pattern>) rather than match(norm_log, '(?i)...'). ClickHouse cannot use a text
// index when the (?i) inline flag is present, so the (?i) form always scans; the
// lower(norm_log) form aligns with the lower(norm_log) n-gram index and prunes
// granules while returning identical results (the indexed column is lowercased,
// the pattern's literals are lowercased to match). When the pattern contains a
// construct that byte-wise lowering cannot safely transform, we fall back to the
// plain (?i) match (correct, just unaccelerated).
//
// We do NOT add explicit hasToken pre-filters: hasToken requires an exact complete
// token, but regex/substring search is not token-aligned (/http/ matches
// "https://..." but hasToken(norm_log,'http') = FALSE), which would cause false
// negatives. The text index prunes match() automatically and correctly.
func buildRegexMatchSQL(fieldRef string, pattern string, literal string, negate bool, mode SourceMode) string {
	matchExpr := buildMatchExpr(fieldRef, pattern, literal, mode)
	if negate {
		return "NOT " + matchExpr
	}
	return matchExpr
}

// buildMatchExpr builds the match() call, routing case-insensitive norm_log
// searches through lower(norm_log) so the n-gram text index can be used.
//
// A bare-term search carries literal (the analyst's raw text) and its pattern is
// rebuilt here rather than at parse time: norm_log stores JSON-serialized text
// and the hot and archive stores escape it differently, so the pattern cannot be
// finalized until the source mode is known. User-authored /regex/ literals carry
// no literal and pass through untouched -- their escaping is the author's.
func buildMatchExpr(fieldRef, pattern, literal string, mode SourceMode) string {
	if fieldRef == normLogColumn && literal != "" {
		pattern = normLogLiteralPattern(literal, mode)
	}
	if fieldRef == normLogColumn && strings.HasPrefix(pattern, caseInsensitiveFlag) {
		if lowered, ok := lowerRegexForLowercasedColumn(pattern[len(caseInsensitiveFlag):]); ok {
			return fmt.Sprintf("match(lower(%s), %s)", normLogColumn, escapeRegexForClickHouse(lowered))
		}
	}
	return fmt.Sprintf("match(%s, %s)", fieldRef, escapeRegexForClickHouse(pattern))
}

// lowerRegexForLowercasedColumn lowercases the literal portions of an RE2 pattern
// so it matches correctly against a lowercased column (lower(raw_log)). The (?i)
// flag must already be stripped by the caller. On lowercased data this is
// equivalent to a case-insensitive match: literals and contiguous letter ranges
// ([A-Z] -> [a-z]) lower cleanly, and class/anchor escapes (\d \w \s \b \. ...)
// are preserved by copying the byte after a backslash verbatim.
//
// It returns ok=false when the pattern contains a construct whose meaning byte-wise
// lowering would change, so the caller falls back to a plain (?i) match:
//   - hex/unicode-property/octal/backreference escapes: \xNN, \pX, \PX, \1..\9
//   - inline-flag, named, or non-capturing groups: (?...) — flag letters and
//     named-group identifiers must not be lowercased.
func lowerRegexForLowercasedColumn(pattern string) (string, bool) {
	var b strings.Builder
	b.Grow(len(pattern))
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch {
		case c == '\\':
			if i+1 >= len(pattern) {
				b.WriteByte(c)
				continue
			}
			next := pattern[i+1]
			if next == 'x' || next == 'p' || next == 'P' || (next >= '0' && next <= '9') {
				return "", false
			}
			b.WriteByte(c)
			b.WriteByte(next) // preserve escape letter (\D stays \D, not \d)
			i++
		case c == '(' && i+1 < len(pattern) && pattern[i+1] == '?':
			return "", false
		case c >= 'A' && c <= 'Z':
			b.WriteByte(c + ('a' - 'A'))
		case c >= utf8.RuneSelf:
			// lower() folds ASCII only, so a letter with other cases must stay (?i).
			r, size := utf8.DecodeRuneInString(pattern[i:])
			if unicode.SimpleFold(r) != r {
				return "", false
			}
			b.WriteString(pattern[i : i+size])
			i += size - 1
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), true
}

func extractFieldName(fieldRef string) string {
	// Iceberg norm_log access: JSONExtractString(norm_log, 'a.b') -> a.b. The key
	// was escaped by escapeString (mapFieldRef), so reverse both escapes (\\ and \').
	const jsonExtractPrefix = "JSONExtractString(norm_log, '"
	if strings.HasPrefix(fieldRef, jsonExtractPrefix) && strings.HasSuffix(fieldRef, "')") {
		inner := fieldRef[len(jsonExtractPrefix) : len(fieldRef)-len("')")]
		return unescapeSQLString(inner)
	}
	// Extract field name from JSON subcolumn ref: fields.`a`.`b`::String -> a.b
	ref := fieldRef
	ref = strings.TrimSuffix(ref, ".:String")
	ref = strings.TrimSuffix(ref, "::String")
	if !strings.HasPrefix(ref, "fields.`") {
		return fieldRef
	}
	// Strip "fields." prefix, then split backtick-quoted segments
	ref = ref[7:] // remove "fields."
	var parts []string
	for len(ref) > 0 {
		if ref[0] != '`' {
			return fieldRef
		}
		ref = ref[1:] // skip opening backtick
		end := 0
		for end < len(ref) {
			if ref[end] == '`' {
				if end+1 < len(ref) && ref[end+1] == '`' {
					end += 2 // escaped backtick
					continue
				}
				break
			}
			end++
		}
		if end >= len(ref) {
			return fieldRef
		}
		part := strings.ReplaceAll(ref[:end], "``", "`")
		parts = append(parts, part)
		ref = ref[end+1:] // skip closing backtick
		if len(ref) > 0 && ref[0] == '.' {
			ref = ref[1:] // skip separator dot
		}
	}
	return strings.Join(parts, ".")
}

func extractFieldAlias(selectField string) string {
	// Extract alias from "expression AS alias" or return the field as-is
	parts := strings.Split(selectField, " AS ")
	if len(parts) >= 2 {
		return strings.TrimSpace(parts[len(parts)-1])
	}
	// Try lowercase "as"
	parts = strings.Split(selectField, " as ")
	if len(parts) >= 2 {
		return strings.TrimSpace(parts[len(parts)-1])
	}
	return selectField
}

// selectName is the unquoted output column name of a SELECT expression.
func selectName(sel string) string {
	return strings.Trim(extractFieldAlias(sel), "`")
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// resolveFieldRef returns the SQL expression for a field, using a computed
// expression if one exists (e.g. from lowercase, eval) or falling back to
// the JSON subcolumn reference.
func resolveFieldRef(field string, registry *FieldRegistry) string {
	return groupableCast(registry.Resolve(field))
}

// groupableCast makes a resolved reference safe for GROUP BY / ORDER BY /
// DISTINCT / display projection contexts. Type-hinted JSON subcolumns are
// emitted bare (no ::String) by jsonFieldRef so WHERE-clause bloom/set skip
// indexes fire, but a bare reference is NOT groupable when the underlying path
// is stored as Dynamic -- which happens for rows ingested before the field was
// type-hinted (a mixed-history table). ClickHouse rejects GROUP/ORDER on a
// Dynamic column with error 44 (ILLEGAL_COLUMN). Casting to ::String is a no-op
// for a concretely typed String path and makes a Dynamic path groupable, so it
// is always safe here. resolveFieldRef is never used to build the base-scan
// WHERE clause (that path calls registry.fieldRef directly), so the skip-index
// optimization is unaffected.
//
// Only raw hot-mode JSON subcolumn refs (fields.`x`) need it; iceberg norm_log
// refs (JSONExtractString(norm_log, 'x')) are already String, computed/base
// columns and already-cast refs are left untouched.
func groupableCast(ref string) string {
	if strings.HasPrefix(ref, "fields.`") && !strings.HasSuffix(ref, "::String") {
		return ref + "::String"
	}
	return ref
}

// lenientDateTime coerces an arbitrary log field into a DateTime64 without ever
// aborting the query. Time-bearing fields arrive in every shape: ISO8601 with or
// without fractional seconds or a zone, "YYYY-MM-DD hh:mm:ss", epoch
// seconds/millis/micros/nanos, and empty on rows where the field is simply
// absent. A bare toDateTime() throws code 41 (CANNOT_PARSE_DATETIME) on the
// first value it dislikes and kills the whole search, so parse best-effort and
// let unparseable values fall out as NULL. Callers must render the result
// NULL-safe (see timeFormatExpr).
func lenientDateTime(ref string) string {
	s := ref
	if !strings.HasSuffix(s, "::String") {
		s = fmt.Sprintf("toString(%s)", ref)
	}
	return fmt.Sprintf("multiIf("+
		"match(%[1]s, '^[0-9]{19}$'), toDateTime64(toFloat64(%[1]s) / 1000000000, 3, 'UTC'), "+
		"match(%[1]s, '^[0-9]{16}$'), toDateTime64(toFloat64(%[1]s) / 1000000, 3, 'UTC'), "+
		"match(%[1]s, '^[0-9]{13}$'), toDateTime64(toFloat64(%[1]s) / 1000, 3, 'UTC'), "+
		"parseDateTime64BestEffortOrNull(%[1]s, 3, 'UTC'))", s)
}

// timeFormatExpr renders formatDateTime for a strftime-style field. The base
// timestamp column formats directly; any other field goes through the lenient
// coercion, and its NULL (unparseable) result is collapsed to an empty string so
// the row still comes back with a blank time. A Nullable(String) projection would
// also break the generic row scanner, which reads String columns into a *string.
func timeFormatExpr(field Argument, chFormat, timezone string, registry *FieldRegistry) (string, error) {
	if field.FieldName() == "timestamp" {
		return fmt.Sprintf("formatDateTime(timestamp, '%s', '%s')", escapeString(chFormat), escapeString(timezone)), nil
	}
	ref, err := ResolveArg(field, registry)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ifNull(formatDateTime(%s, '%s', '%s'), '')",
		lenientDateTime(ref), escapeString(chFormat), escapeString(timezone)), nil
}

// numericCast wraps a resolved field reference for use inside aggregate
// functions. Fields that are already numeric (FieldKindAssignment, e.g.
// length(), levenshtein()) use toFloat64; string-typed fields use
// toFloat64OrNull which handles non-numeric strings gracefully.
func numericCast(fieldName, resolvedExpr string, registry *FieldRegistry) string {
	if registry != nil && registry.IsNumericComputed(fieldName) {
		return fmt.Sprintf("toFloat64(%s)", resolvedExpr)
	}
	// Join-output columns are concretely typed (Float64 scores, String flags);
	// toFloat64 handles both, while toFloat64OrNull rejects non-String input.
	if registry != nil {
		if e := registry.Get(fieldName); e != nil && e.Kind == FieldKindJoined {
			return fmt.Sprintf("toFloat64(%s)", resolvedExpr)
		}
	}
	return fmt.Sprintf("toFloat64OrNull(%s)", resolvedExpr)
}

// convertTimeFormat converts BQL time format to ClickHouse format
func convertTimeFormat(bqlFormat string) string {
	// Convert common format patterns to ClickHouse
	format := bqlFormat
	format = strings.ReplaceAll(format, "%Y", "%Y")    // Year (4 digits)
	format = strings.ReplaceAll(format, "%m", "%m")    // Month (01-12)
	format = strings.ReplaceAll(format, "%d", "%d")    // Day (01-31)
	format = strings.ReplaceAll(format, "%H", "%H")    // Hour (00-23)
	format = strings.ReplaceAll(format, "%M", "%M")    // Minute (00-59)
	format = strings.ReplaceAll(format, "%S", "%S")    // Second (00-59)
	format = strings.ReplaceAll(format, "%R", "%H:%M") // Hour:Minute

	// Handle some common patterns
	if format == "%A %d %B %Y, %R" {
		return "%W %d %B %Y, %H:%M"
	}
	return format
}

// parseBucketSpan parses spans like "1h", "30m", "5s" into a numeric value and unit.
func parseBucketSpan(span string) (int, string) {
	if len(span) < 2 {
		return 1, "HOUR"
	}
	unit := span[len(span)-1:]
	numStr := span[:len(span)-1]
	n, err := strconv.Atoi(numStr)
	if err != nil || n <= 0 {
		n = 1
	}
	switch unit {
	case "s":
		return n, "SECOND"
	case "m":
		return n, "MINUTE"
	case "h":
		return n, "HOUR"
	case "d":
		return n, "DAY"
	case "w":
		return n, "WEEK"
	default:
		return 1, "HOUR"
	}
}

// getBucketExpression returns a ClickHouse expression for time bucketing.
// Uses toStartOfInterval for arbitrary intervals, or built-in functions for common ones.
//
// tz is the IANA zone the bucket boundaries snap to. Empty (and "UTC") emit no
// zone argument, leaving the default install's SQL byte-identical. A qualified
// bucket comes back typed DateTime('<tz>'), which the row scanner renders as
// that zone's wall clock, so the label arrives already correct.
func getBucketExpression(n int, unit string, tz string) string {
	zone := ""
	if tz != "" && tz != "UTC" {
		zone = fmt.Sprintf(", '%s'", escapeString(tz))
	}
	// For common 1-unit spans use the simpler built-in functions
	if n == 1 {
		switch unit {
		case "MINUTE":
			return fmt.Sprintf("toStartOfMinute(timestamp%s)", zone)
		case "HOUR":
			return fmt.Sprintf("toStartOfHour(timestamp%s)", zone)
		case "DAY":
			return fmt.Sprintf("toStartOfDay(timestamp%s)", zone)
		case "WEEK":
			if zone == "" {
				return "toStartOfWeek(timestamp)"
			}
			// toStartOfWeek takes mode before timezone; 0 is its own default.
			return fmt.Sprintf("toStartOfWeek(timestamp, 0%s)", zone)
		}
	}
	// For 5 minutes ClickHouse has a built-in
	if n == 5 && unit == "MINUTE" {
		return fmt.Sprintf("toStartOfFiveMinutes(timestamp%s)", zone)
	}
	if n == 15 && unit == "MINUTE" {
		return fmt.Sprintf("toStartOfFifteenMinutes(timestamp%s)", zone)
	}
	// For arbitrary intervals use toStartOfInterval
	return fmt.Sprintf("toStartOfInterval(timestamp, INTERVAL %d %s%s)", n, unit, zone)
}

// bucketTimezone reports the zone getBucketExpression should snap to, and
// records it on the plan so the client can label an axis whose zone differs
// from the viewer's own.
//
// A zone the embedded tzdata cannot resolve falls back to UTC rather than
// letting ClickHouse fail the whole query. Zones are validated where they are
// written, so reaching this means the tzdata moved underneath a stored value.
// The recorded zone is the one actually used, never the one asked for.
func bucketTimezone(ctx *CommandContext) string {
	tz := ctx.Opts.DisplayTimezone
	if tz == "" {
		tz = "UTC"
	} else if tz != "UTC" {
		if _, err := time.LoadLocation(tz); err != nil {
			tz = "UTC"
		}
	}
	ctx.Plan.ChartConfig["bucketTimezone"] = tz
	return tz
}

// spanToSeconds converts a duration string (e.g., "5m", "1h", "30s") to seconds.
func spanToSeconds(span string) int {
	n, unit := parseBucketSpan(span)
	switch unit {
	case "SECOND":
		return n
	case "MINUTE":
		return n * 60
	case "HOUR":
		return n * 3600
	case "DAY":
		return n * 86400
	case "WEEK":
		return n * 604800
	default:
		return n * 3600
	}
}

// parseChainSteps parses chain block tokens into per-step SQL boolean expressions
// plus the fields those steps filter on.
//
// Each step is parsed as a full pipeline and its commands are run against a
// throwaway plan, so condition commands (cidr, in, comment) contribute their
// predicate to that step instead of the whole query. Anything that is not a row
// predicate (projections, aggregates, structural commands) is rejected.
func parseChainSteps(tokens []Token, source []rune, opts QueryOptions, parentReg *FieldRegistry) ([]string, []string, error) {
	var allSteps [][]Token
	var current []Token
	for _, tok := range tokens {
		if tok.Type == TokenSemicolon {
			if len(current) > 0 {
				allSteps = append(allSteps, current)
				current = nil
			}
			continue
		}
		current = append(current, tok)
	}
	if len(current) > 0 {
		allSteps = append(allSteps, current)
	}

	var steps []string
	var fields []string
	seen := make(map[string]bool)
	for _, stepTokens := range allSteps {
		sql, stepFields, err := buildChainStep(append(stepTokens, Token{Type: TokenEOF}), source, opts, parentReg)
		if err != nil {
			return nil, nil, err
		}
		steps = append(steps, sql)
		for _, f := range stepFields {
			if !seen[f] {
				seen[f] = true
				fields = append(fields, f)
			}
		}
	}

	return steps, fields, nil
}

// buildChainStep compiles one step into a single boolean expression, or errors.
// It never returns an empty expression: a step that matched nothing would be
// dropped from the pattern and silently change which sequences match.
func buildChainStep(stepTokens []Token, source []rune, opts QueryOptions, parentReg *FieldRegistry) (string, []string, error) {
	// The step's tokens carry absolute offsets into the original query, so giving
	// the sub-parser that source lets a step use source-backed syntax such as an
	// expression filter.
	stepParser := NewParser(stepTokens)
	stepParser.input = source
	pl, err := stepParser.Parse()
	if err != nil {
		return "", nil, fmt.Errorf("chain step: %w", err)
	}

	// A clone: the step's own projections must resolve its own predicates without
	// leaking into the query that contains the chain.
	reg := NewFieldRegistry(opts.SourceMode, opts.IcePromoted)
	if parentReg != nil {
		reg = parentReg.Clone()
	}

	var conjuncts []string
	var fields []string
	seen := make(map[string]bool)

	// Conditions before the step's first command resolve before its projections
	// are registered, ahead of them; later ones (`a=1 | _len > 500`) after.
	first, later := SplitFirstStage(pl.HavingConditions)
	if w, err := conditionsPredicate(first, reg.rowLevelClone(), opts); err != nil {
		return "", nil, fmt.Errorf("chain step: %w", err)
	} else if w != "" {
		conjuncts = append(conjuncts, w)
	}
	for _, c := range first {
		fields = collectHavingFieldsOrdered(c, seen, &fields)
	}

	// Before the conditions below: len()/match()/lookupIP() register the expressions
	// that a later `_len > 500` in the same step resolves against.
	if len(pl.Commands) > 0 {
		w, cf, err := harvestChainCommands(pl, opts, reg)
		if err != nil {
			return "", nil, fmt.Errorf("chain step: %w", err)
		}
		conjuncts = append(conjuncts, w...)
		for _, f := range cf {
			if !seen[f] {
				seen[f] = true
				fields = append(fields, f)
			}
		}
	}

	if w, err := conditionsPredicate(later, reg.rowLevelClone(), opts); err != nil {
		return "", nil, fmt.Errorf("chain step: %w", err)
	} else if w != "" {
		conjuncts = append(conjuncts, w)
	}
	for _, c := range later {
		fields = collectHavingFieldsOrdered(c, seen, &fields)
	}

	if len(pl.Assignments) > 0 {
		return "", nil, fmt.Errorf("chain step: field assignments cannot be used inside a chain step")
	}

	switch len(conjuncts) {
	case 0:
		// Dropping it silently would shorten the pattern and quietly change which
		// sequences match, so a step that only projects is an error.
		return "", nil, fmt.Errorf("chain step: no condition; each step must match events")
	case 1:
		return conjuncts[0], fields, nil
	default:
		return "(" + strings.Join(conjuncts, " AND ") + ")", fields, nil
	}
}

// CommandPredicate compiles a condition function into a boolean expression whose
// fields resolve through registry. The handler runs against a throwaway plan and
// registry copy, so its effect is captured as a predicate instead of being
// appended to the query's WHERE.
func CommandPredicate(cmd CommandNode, registry *FieldRegistry, opts QueryOptions) (string, error) {
	pl := &PipelineNode{Commands: []CommandNode{cmd}}
	where, _, err := harvestChainCommands(pl, opts, registry.Clone())
	if err != nil {
		return "", err
	}
	switch len(where) {
	case 0:
		return "", notConditionError(fmt.Sprintf("%s() is not a condition", cmd.Name))
	case 1:
		return where[0], nil
	default:
		return "(" + andJoin(where) + ")", nil
	}
}

// harvestChainCommands runs a step's commands against a throwaway plan and returns
// the predicates they produced, rejecting anything that reshapes the result. A
// command that only projects contributes no predicate but registers an expression
// the step's own conditions can reference.
func harvestChainCommands(pl *PipelineNode, opts QueryOptions, parentReg *FieldRegistry) ([]string, []string, error) {
	plan := NewQueryPlan()
	reg := parentReg
	if reg == nil {
		reg = NewFieldRegistry(opts.SourceMode, opts.IcePromoted)
	}
	ctx := &CommandContext{Registry: reg, Plan: plan, Opts: opts, Pipeline: pl, AsCondition: true}

	for _, cmd := range pl.Commands {
		if getCommandHandler(cmd.Name) == nil {
			return nil, nil, fmt.Errorf("unsupported command %s()", cmd.Name)
		}
	}
	for i, cmd := range pl.Commands {
		ctx.CmdIndex = i
		if err := getCommandHandler(cmd.Name).Declare(cmd, ctx); err != nil {
			return nil, nil, err
		}
	}
	for i, cmd := range pl.Commands {
		ctx.CmdIndex = i
		if err := getCommandHandler(cmd.Name).Execute(cmd, ctx); err != nil {
			return nil, nil, err
		}
	}

	src := &plan.SourceStage().Layer
	if len(src.GroupBy) > 0 || len(src.OrderBy) > 0 || src.Limit != "" ||
		src.LimitBy != "" || len(src.Having) > 0 || plan.IsAggregated || plan.IsJoin ||
		plan.IsChain || plan.IsProcessTree || len(plan.WindowLayers) > 0 ||
		plan.ModelLookupSQL != "" || len(plan.Stages) > 1 {
		return nil, nil, notConditionError(fmt.Sprintf("only row conditions are allowed; %s() changes the shape of the result", pl.Commands[0].Name))
	}

	// A projection (len, match, lookupIP) is a per-row scalar, so it belongs in a
	// step even though a step projects nothing: the registry resolves references to
	// it as the expression itself, folding it into the predicate. An alias the
	// registry does not know would resolve to a column that never exists.
	for _, sel := range src.Selects {
		alias := extractFieldAlias(sel.String())
		if alias == "" || !reg.Has(alias) {
			return nil, nil, notConditionError(fmt.Sprintf("only row conditions are allowed; %s() projects a column that cannot be scoped to one step", pl.Commands[0].Name))
		}
	}

	if len(src.Where) == 0 && len(src.Selects) == 0 {
		return nil, nil, notConditionError(fmt.Sprintf("%s() produced no condition", pl.Commands[0].Name))
	}

	var fields []string
	for _, cmd := range pl.Commands {
		fields = append(fields, conditionCommandFields(cmd)...)
	}
	return src.Where, fields, nil
}

// notConditionError reports a command that yields no row predicate.
type notConditionError string

func (e notConditionError) Error() string { return string(e) }

// conditionCommandFields names the fields a condition command reads: modelLookup()'s
// keys, otherwise the field it names first (cidr(dst_ip, ...), in(user, [...])).
func conditionCommandFields(cmd CommandNode) []string {
	if canonicalCommandName(cmd.Name) == canonicalCommandName("modelLookup") {
		if _, keys, _, err := parseModelLookupArgs(cmd); err == nil {
			return keys
		}
		return nil
	}
	if len(cmd.Args) == 0 {
		return nil
	}
	first := cmd.Args[0]
	if first.Kind == ArgList && len(first.List) > 0 {
		first = first.List[0]
	}
	if f := first.Value(); f != "" && isPlainFieldName(f) {
		return []string{f}
	}
	return nil
}

// IsPlainFieldName reports whether s is shaped like a log field name, and so is
// safe to render as a ClickHouse identifier or alias. The plan derives output
// column names by string-parsing SQL expressions, so a name carrying a backtick,
// a quote or " AS " cannot be made safe by escaping alone and has to be rejected
// where it enters.
func IsPlainFieldName(s string) bool { return isPlainFieldName(s) }

// isPlainFieldName reports whether s looks like a bare field reference rather than
// a literal or expression.
func isPlainFieldName(s string) bool {
	if s == "" || strings.ContainsAny(s, "\"'()[]=/,") {
		return false
	}
	for _, r := range s {
		if !(r == '_' || r == '.' || r == '-' || unicode.IsLetter(r) || unicode.IsDigit(r)) {
			return false
		}
	}
	return true
}

// collectHavingFieldsOrdered appends a having condition's leaf field names in order.
func collectHavingFieldsOrdered(c HavingCondition, seen map[string]bool, out *[]string) []string {
	if c.IsCompound {
		for _, ch := range c.Children {
			collectHavingFieldsOrdered(ch, seen, out)
		}
		return *out
	}
	for _, f := range []string{c.Field, c.ValueField} {
		if f != "" && f != normLogColumn && !seen[f] {
			seen[f] = true
			*out = append(*out, f)
		}
	}
	return *out
}

// commandIndex returns the index of the first top-level command answering to the
// given name under any of its aliases, or -1 when absent.
func commandIndex(commands []CommandNode, name string) int {
	name = canonicalCommandName(name)
	for i, cmd := range commands {
		if canonicalCommandName(cmd.Name) == name {
			return i
		}
	}
	return -1
}

// firstAggregatingCommandIndex returns the index of the first command that
// aggregates (collapses rows), or len(commands) if none do. Aggregation is
// sourced from aggregatingCommandNames, populated at command registration.
func firstAggregatingCommandIndex(commands []CommandNode) int {
	for i, cmd := range commands {
		if IsAggregatingCommand(cmd.Name) {
			return i
		}
	}
	return len(commands)
}
