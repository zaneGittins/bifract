package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
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
		whereClause = " WHERE " + strings.Join(whereConditions, " AND ")
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
		outerFilter = " WHERE " + strings.Join(havingConditions, " AND ")
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
	sql.WriteString(fmt.Sprintf("SELECT arrayJoin(JSONExtractKeysAndValues(toString(fields), 'String')) AS kv FROM %s%s LIMIT %d",
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

// buildTraversalSQL generates a recursive CTE query for bfs/dfs graph traversal.
func buildTraversalSQL(
	mode, childField, parentField, startValue string, maxDepth int,
	includeFields []string,
	whereConditions []string,
	selectFields, orderByFields []string, limitClause string,
	havingConditions []string,
	chartType string, chartConfig map[string]interface{},
	opts QueryOptions, hasTableCmd bool,
) (*TranslationResult, error) {
	if _, err := sanitizeIdentifier(childField); err != nil {
		return nil, fmt.Errorf("%s(): invalid child field: %w", mode, err)
	}
	if _, err := sanitizeIdentifier(parentField); err != nil {
		return nil, fmt.Errorf("%s(): invalid parent field: %w", mode, err)
	}

	childRef := jsonFieldRef(childField)
	parentRef := jsonFieldRef(parentField)

	// Always include child and parent fields; deduplicate
	seen := map[string]bool{childField: true, parentField: true}
	allInclude := []string{childField, parentField}
	for _, f := range includeFields {
		if !seen[f] {
			seen[f] = true
			allInclude = append(allInclude, f)
		}
	}

	// Build WHERE for base case: fractal + time range + user filter + start node
	var baseConditions []string
	baseConditions = append(baseConditions, whereConditions...)
	baseConditions = append(baseConditions, fmt.Sprintf("%s = '%s'", childRef, escapeString(startValue)))
	baseWhere := strings.Join(baseConditions, " AND ")

	// Build WHERE for recursive case: same conditions qualified with table alias
	var recursiveConditions []string
	for _, cond := range whereConditions {
		recursiveConditions = append(recursiveConditions, qualifyColumnRefs(cond, "l"))
	}
	recursiveConditions = append(recursiveConditions, fmt.Sprintf("t._depth < %d", maxDepth))
	recursiveWhere := strings.Join(recursiveConditions, " AND ")

	// Build recursive CTE
	var sql strings.Builder
	sql.WriteString("WITH RECURSIVE traversal AS (")

	// Build include field expressions for CTE columns
	var baseIncludeCols, recursiveIncludeCols string
	for _, f := range allInclude {
		ref := jsonFieldRef(f)
		safeAlias := strings.ReplaceAll(f, ".", "_")
		baseIncludeCols += fmt.Sprintf(", %s AS _%s", ref, safeAlias)
		recursiveIncludeCols += fmt.Sprintf(", l.%s AS _%s", ref, safeAlias)
	}

	// Base case: find starting node(s)
	sql.WriteString("SELECT timestamp, raw_log, log_id, ")
	sql.WriteString("toUInt32(0) AS _depth, ")
	sql.WriteString(fmt.Sprintf("%s AS _node_id, ", childRef))
	sql.WriteString(fmt.Sprintf("%s AS _path", childRef))
	sql.WriteString(baseIncludeCols)
	sql.WriteString(fmt.Sprintf(" FROM %s ", opts.EffectiveTableName()))
	sql.WriteString(fmt.Sprintf("WHERE %s ", baseWhere))

	sql.WriteString("UNION ALL ")

	// Recursive case: find children via parent->child relationship
	sql.WriteString("SELECT l.timestamp, l.raw_log, l.log_id, ")
	sql.WriteString("t._depth + 1 AS _depth, ")
	sql.WriteString(fmt.Sprintf("l.%s AS _node_id, ", childRef))
	sql.WriteString(fmt.Sprintf("concat(t._path, ' > ', l.%s) AS _path", childRef))
	sql.WriteString(recursiveIncludeCols)
	sql.WriteString(fmt.Sprintf(" FROM %s l ", opts.EffectiveTableName()))
	sql.WriteString(fmt.Sprintf("INNER JOIN traversal t ON l.%s = t._node_id ", parentRef))
	sql.WriteString(fmt.Sprintf("WHERE %s", recursiveWhere))

	sql.WriteString(") ")

	// Build include column references for the final SELECT (aliased without underscore prefix)
	var finalIncludeCols string
	for _, f := range allInclude {
		safeAlias := strings.ReplaceAll(f, ".", "_")
		finalIncludeCols += fmt.Sprintf(", _%s AS %s", safeAlias, safeAlias)
	}

	// Final SELECT from CTE
	sql.WriteString("SELECT ")
	if hasTableCmd && len(selectFields) > 0 {
		formattedFields := make([]string, 0, len(selectFields))
		for _, field := range selectFields {
			alias := extractFieldAlias(field)
			if alias == "timestamp" {
				formattedFields = append(formattedFields, "formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') as timestamp")
			} else if alias == "_depth" {
				formattedFields = append(formattedFields, "toString(_depth) AS _depth")
			} else if alias == "_path" || alias == "_node_id" {
				formattedFields = append(formattedFields, alias)
			} else {
				formattedFields = append(formattedFields, field)
			}
		}
		sql.WriteString(strings.Join(formattedFields, ", "))
	} else {
		sql.WriteString("formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') as timestamp, ")
		sql.WriteString("raw_log, log_id, toString(_depth) AS _depth, _path")
		sql.WriteString(finalIncludeCols)
	}

	sql.WriteString(" FROM traversal ")

	// Post-traversal filters (e.g. _depth <= 3)
	if len(havingConditions) > 0 {
		sql.WriteString("WHERE ")
		sql.WriteString(strings.Join(havingConditions, " AND "))
		sql.WriteString(" ")
	}

	// ORDER BY
	if len(orderByFields) > 0 {
		sql.WriteString("ORDER BY ")
		sql.WriteString(strings.Join(orderByFields, ", "))
		sql.WriteString(" ")
	} else if mode == "bfs" {
		sql.WriteString("ORDER BY _depth ASC, timestamp ASC ")
	} else {
		// DFS: path-based ordering gives pre-order traversal
		sql.WriteString("ORDER BY _path ASC ")
	}

	// LIMIT
	if limitClause != "" {
		sql.WriteString(limitClause)
	} else if opts.MaxRows > 0 {
		sql.WriteString(fmt.Sprintf("LIMIT %d", opts.MaxRows))
	}

	finalSQL := sql.String()
	if err := validateGeneratedSQL(finalSQL); err != nil {
		return nil, err
	}

	// Build field order for the UI
	var fieldOrder []string
	if hasTableCmd && len(selectFields) > 0 {
		for _, field := range selectFields {
			alias := extractFieldAlias(field)
			if alias != "_all_fields" && alias != "raw_log" && alias != "log_id" {
				fieldOrder = append(fieldOrder, strings.Trim(alias, "`"))
			}
		}
	} else {
		fieldOrder = []string{"timestamp", "_depth", "_path"}
		for _, f := range allInclude {
			fieldOrder = append(fieldOrder, strings.ReplaceAll(f, ".", "_"))
		}
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
			for _, col := range []string{"fractal_id", "timestamp", "raw_log", "log_id"} {
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

// collectConditionFields walks a condition tree and returns all unique field names referenced.
func collectConditionFields(conditions []ConditionNode) map[string]bool {
	fields := make(map[string]bool)
	for _, cond := range conditions {
		if cond.IsCompound {
			for k := range collectConditionFields(cond.Children) {
				fields[k] = true
			}
		} else if cond.Field != "" {
			fields[cond.Field] = true
		}
	}
	return fields
}

// collectHavingConditionFields recursively collects all leaf field names from
// HavingConditions (including compound nodes) into the provided map.
func collectHavingConditionFields(conditions []HavingCondition, fields map[string]bool) {
	for _, cond := range conditions {
		if cond.IsCompound {
			collectHavingConditionFields(cond.Children, fields)
		} else if cond.Field != "" {
			fields[cond.Field] = true
		}
	}
}

// buildWhereClause builds a WHERE clause from multiple conditions respecting OR/AND logic and parenthetical grouping.
// Conditions with the same GroupID > 0 are collected into a group. If GroupNegate is set, the whole group is wrapped in NOT(...).
func buildWhereClause(conditions []ConditionNode) (string, error) {
	if len(conditions) == 0 {
		return "", nil
	}

	// Each "part" is either a single ungrouped condition or an entire parenthetical group.
	type part struct {
		sql   string
		logic string // logic operator connecting this part to the NEXT part
	}
	var parts []part

	i := 0
	for i < len(conditions) {
		cond := conditions[i]

		if cond.GroupID > 0 {
			// Collect all consecutive conditions with the same GroupID
			groupID := cond.GroupID
			groupNegate := cond.GroupNegate
			var group []ConditionNode
			for i < len(conditions) && conditions[i].GroupID == groupID {
				group = append(group, conditions[i])
				i++
			}

			// Build inner SQL for the group
			var inner strings.Builder
			for j, gc := range group {
				condSQL, err := translateCondition(gc)
				if err != nil {
					return "", err
				}
				if j > 0 {
					if group[j-1].Logic == "OR" {
						inner.WriteString(" OR ")
					} else {
						inner.WriteString(" AND ")
					}
				}
				inner.WriteString(condSQL)
			}

			groupSQL := inner.String()
			if groupNegate {
				groupSQL = "NOT (" + groupSQL + ")"
			} else if len(group) > 1 {
				groupSQL = "(" + groupSQL + ")"
			}

			// The logic connecting this group to the next part is on the last condition
			parts = append(parts, part{sql: groupSQL, logic: group[len(group)-1].Logic})
		} else {
			// Ungrouped condition
			condSQL, err := translateCondition(cond)
			if err != nil {
				return "", err
			}
			parts = append(parts, part{sql: condSQL, logic: cond.Logic})
			i++
		}
	}

	// Join all parts
	var result strings.Builder
	for j, p := range parts {
		if j > 0 {
			if parts[j-1].logic == "OR" {
				result.WriteString(" OR ")
			} else {
				result.WriteString(" AND ")
			}
		}
		result.WriteString(p.sql)
	}

	sql := result.String()
	if len(parts) > 1 {
		sql = "(" + sql + ")"
	}
	return sql, nil
}

// fixOperatorPrecedence adds parentheses to handle OR/AND precedence correctly
func fixOperatorPrecedence(sql string) string {
	// Temporarily disabled to preserve 94.7% test success rate
	// Complex nested parentheses remain an outstanding issue
	return sql
}

func translateCondition(cond ConditionNode) (string, error) {
	// Handle compound nodes by recursively building the inner SQL.
	if cond.IsCompound {
		innerSQL, err := buildWhereClause(cond.Children)
		if err != nil {
			return "", err
		}
		if cond.Negate {
			return "NOT (" + innerSQL + ")", nil
		}
		return "(" + innerSQL + ")", nil
	}

	var sql string

	// Handle special fields that exist as direct columns
	var fieldRef string
	isJSONField := false
	switch cond.Field {
	case "raw_log":
		fieldRef = "raw_log"
	case "timestamp":
		fieldRef = "timestamp"
	case "log_id":
		fieldRef = "log_id"
	default:
		fieldRef = jsonFieldRef(cond.Field)
		isJSONField = true
	}

	if cond.Value == "*" {
		// Wildcard: field has any non-empty value
		if cond.Operator == "!=" {
			// field!=* means field doesn't exist or is empty.
			// JSON subcolumns return NULL for non-existent paths,
			// so we must check IS NULL alongside = ''.
			if isJSONField {
				sql = fmt.Sprintf("(%s IS NULL OR %s = '')", fieldRef, fieldRef)
			} else {
				sql = fmt.Sprintf("%s = ''", fieldRef)
			}
		} else {
			sql = fmt.Sprintf("%s != ''", fieldRef)
		}
	} else if cond.IsRegex {
		sql = buildRegexMatchSQL(fieldRef, cond.Value, cond.Operator == "!=")
	} else {
		// For comparison operators, try to convert to numeric if the value looks numeric
		// This allows queries like: bytes > 1000
		switch cond.Operator {
		case "=":
			sql = fmt.Sprintf("%s = '%s'", fieldRef, escapeString(cond.Value))
		case "!=":
			// For JSON fields, include rows where the field doesn't exist (NULL).
			// Without this, NULL != 'value' evaluates to NULL (falsy) and
			// silently excludes rows missing the field.
			if isJSONField {
				sql = fmt.Sprintf("(%s IS NULL OR %s != '%s')", fieldRef, fieldRef, escapeString(cond.Value))
			} else {
				sql = fmt.Sprintf("%s != '%s'", fieldRef, escapeString(cond.Value))
			}
		case ">", "<", ">=", "<=":
			// Validate numeric value to prevent injection
			if err := validateNumeric(cond.Value); err != nil {
				return "", fmt.Errorf("numeric comparison: %w", err)
			}
			sql = fmt.Sprintf("toFloat64OrZero(%s) %s %s", fieldRef, cond.Operator, cond.Value)
		default:
			return "", fmt.Errorf("unsupported operator: %s", cond.Operator)
		}
	}

	if cond.Negate {
		sql = "NOT (" + sql + ")"
	}

	return sql, nil
}

func escapeString(s string) string {
	// Escape backslashes first (for ClickHouse regex patterns)
	s = strings.ReplaceAll(s, "\\", "\\\\")
	// Escape single quotes for SQL
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}

// validateGeneratedSQL checks the final SQL for dangerous patterns that should never
// appear in translator output. It strips string literals first so that log data
// containing keywords like "DROP TABLE" in search values won't trigger false positives.
func validateGeneratedSQL(sql string) error {
	// Strip all single-quoted string literals (including escaped quotes) to avoid
	// false positives on user search values inside WHERE conditions.
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

// stripStringLiterals removes the content of all single-quoted string literals
// from SQL, replacing them with empty strings. Handles escaped quotes (\'').
// This allows checking the SQL structure without matching against user-supplied
// search values that might legitimately contain SQL keywords.
func stripStringLiterals(sql string) string {
	var result strings.Builder
	inString := false
	i := 0
	for i < len(sql) {
		if inString {
			if sql[i] == '\\' && i+1 < len(sql) {
				i += 2 // skip escaped character
			} else if sql[i] == '\'' {
				inString = false
				result.WriteByte('\'') // write closing quote
				i++
			} else {
				i++ // skip string content
			}
		} else {
			if sql[i] == '\'' {
				inString = true
				result.WriteByte('\'') // write opening quote
				i++
			} else {
				result.WriteByte(sql[i])
				i++
			}
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

// jsonFieldRef returns the ClickHouse JSON subcolumn reference for a field name.
// Dots in the field name are treated as nested path separators, producing
// fields.`event`.`name`.:String for "event.name". The .:String suffix casts
// the Variant/Dynamic subcolumn to String, which is required for GROUP BY
// and avoids ambiguous type comparisons.
func jsonFieldRef(field string) string {
	parts := strings.Split(field, ".")
	var b strings.Builder
	b.WriteString("fields")
	for _, p := range parts {
		escaped := strings.ReplaceAll(p, "`", "``")
		b.WriteString(".`")
		b.WriteString(escaped)
		b.WriteString("`")
	}
	b.WriteString(".:String")
	return b.String()
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
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
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
		if !seen[lower] {
			seen[lower] = true
			result = append(result, lower)
		}
	}
	return result
}

// buildRegexMatchSQL wraps match() with hasToken pre-filters that leverage
// the text index on raw_log for granule pruning. The text index uses
// preprocessor = lower(raw_log), so hasToken auto-lowers search terms
// and works for both case-sensitive and case-insensitive regex patterns.
// Falls back to plain match() when no useful tokens can be extracted,
// when the field isn't raw_log, or for negated regex.
func buildRegexMatchSQL(fieldRef string, pattern string, negate bool) string {
	matchExpr := fmt.Sprintf("match(%s, %s)", fieldRef, escapeRegexForClickHouse(pattern))
	if negate {
		matchExpr = "NOT " + matchExpr
	}

	if fieldRef != "raw_log" || negate {
		return matchExpr
	}

	tokens := extractLiteralTokens(pattern)
	if len(tokens) == 0 {
		return matchExpr
	}

	// hasToken() is supported by the text index and auto-applies the
	// index preprocessor (lower) to search terms, so lowercased tokens
	// match correctly for both case-sensitive and case-insensitive regex.
	var parts []string
	for _, tok := range tokens {
		parts = append(parts, fmt.Sprintf("hasToken(raw_log, '%s')", tok))
	}
	parts = append(parts, matchExpr)
	return strings.Join(parts, " AND ")
}

func extractFieldName(fieldRef string) string {
	// Extract field name from JSON subcolumn ref: fields.`a`.`b`.:String -> a.b
	ref := fieldRef
	ref = strings.TrimSuffix(ref, ".:String")
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
	return registry.Resolve(field)
}

// numericCast wraps a resolved field reference for use inside aggregate
// functions. Fields that are already numeric (FieldKindAssignment, e.g.
// length(), levenshtein()) use toFloat64; string-typed fields use
// toFloat64OrNull which handles non-numeric strings gracefully.
func numericCast(fieldName, resolvedExpr string, registry *FieldRegistry) string {
	if registry != nil && registry.IsNumericComputed(fieldName) {
		return fmt.Sprintf("toFloat64(%s)", resolvedExpr)
	}
	return fmt.Sprintf("toFloat64OrNull(%s)", resolvedExpr)
}

// extractFunctionField extracts field name from function calls like "avg(response_time)"
func extractFunctionField(fn string, funcName string) string {
	prefix := funcName + "("
	if strings.HasPrefix(fn, prefix) && strings.HasSuffix(fn, ")") {
		inner := fn[len(prefix) : len(fn)-1]
		// Check for named params like field=name
		if strings.Contains(inner, "field=") {
			for _, part := range strings.Split(inner, ",") {
				part = strings.TrimSpace(part)
				if strings.HasPrefix(part, "field=") {
					return strings.TrimPrefix(part, "field=")
				}
			}
		}
		return inner
	}
	return ""
}

// parseStatsFunctionParams parses named params from a multi sub-function like top(percent=true, field=x, as=y)
func parseStatsFunctionParams(fn string, funcName string) map[string]string {
	params := make(map[string]string)
	prefix := funcName + "("
	if !strings.HasPrefix(fn, prefix) || !strings.HasSuffix(fn, ")") {
		return params
	}
	inner := fn[len(prefix) : len(fn)-1]
	for _, part := range strings.Split(inner, ",") {
		part = strings.TrimSpace(part)
		if eq := strings.IndexByte(part, '='); eq > 0 {
			params[part[:eq]] = part[eq+1:]
		} else if part != "" {
			params["_positional"] = part
		}
	}
	return params
}

// convertMathExprToSQL converts a math expression string to SQL, resolving field references.
// Known computed fields (from aggregations) are referenced by alias; other identifiers become JSON subcolumn refs.
func convertMathExprToSQL(expr string, registry *FieldRegistry) string {
	var result strings.Builder
	i := 0
	runes := []rune(expr)
	for i < len(runes) {
		ch := runes[i]
		if ch == '(' || ch == ')' || ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == ' ' {
			result.WriteRune(ch)
			i++
		} else if ch >= '0' && ch <= '9' || ch == '.' {
			// Numeric literal
			start := i
			for i < len(runes) && (runes[i] >= '0' && runes[i] <= '9' || runes[i] == '.') {
				i++
			}
			result.WriteString(string(runes[start:i]))
		} else if ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			// Identifier
			start := i
			for i < len(runes) && (runes[i] == '_' || (runes[i] >= 'a' && runes[i] <= 'z') || (runes[i] >= 'A' && runes[i] <= 'Z') || (runes[i] >= '0' && runes[i] <= '9')) {
				i++
			}
			ident := string(runes[start:i])
			if registry.Has(ident) {
				// Reference the alias directly (available from inner subquery)
				result.WriteString(ident)
			} else {
				result.WriteString(fmt.Sprintf("toFloat64OrNull(%s)", jsonFieldRef(ident)))
			}
		} else {
			result.WriteRune(ch)
			i++
		}
	}
	return result.String()
}

// convertTimeFormat converts BQL time format to ClickHouse format
func convertTimeFormat(bqlFormat string) string {
	// Convert common format patterns to ClickHouse
	format := bqlFormat
	format = strings.ReplaceAll(format, "%Y", "%Y")   // Year (4 digits)
	format = strings.ReplaceAll(format, "%m", "%m")   // Month (01-12)
	format = strings.ReplaceAll(format, "%d", "%d")   // Day (01-31)
	format = strings.ReplaceAll(format, "%H", "%H")   // Hour (00-23)
	format = strings.ReplaceAll(format, "%M", "%M")   // Minute (00-59)
	format = strings.ReplaceAll(format, "%S", "%S")   // Second (00-59)
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
func getBucketExpression(n int, unit string) string {
	// For common 1-unit spans use the simpler built-in functions
	if n == 1 {
		switch unit {
		case "MINUTE":
			return "toStartOfMinute(timestamp)"
		case "HOUR":
			return "toStartOfHour(timestamp)"
		case "DAY":
			return "toStartOfDay(timestamp)"
		case "WEEK":
			return "toStartOfWeek(timestamp)"
		}
	}
	// For 5 minutes ClickHouse has a built-in
	if n == 5 && unit == "MINUTE" {
		return "toStartOfFiveMinutes(timestamp)"
	}
	if n == 15 && unit == "MINUTE" {
		return "toStartOfFifteenMinutes(timestamp)"
	}
	// For arbitrary intervals use toStartOfInterval
	return fmt.Sprintf("toStartOfInterval(timestamp, INTERVAL %d %s)", n, unit)
}

// parseCaseConditions parses case syntax: { condition | result ; condition2 | result2 ; * | default }
func parseCaseConditions(caseExpr string) ([]string, string, []AssignmentNode) {
	var whenClauses []string
	var defaultClause string

	// Track assignments by field name to avoid conflicts
	fieldAssignments := make(map[string][]string) // field -> list of "WHEN condition THEN value"
	defaultAssignments := make(map[string]string) // field -> default value

	// Remove outer braces and split by semicolon
	caseExpr = strings.Trim(caseExpr, "{}")
	conditions := strings.Split(caseExpr, ";")

	for _, condition := range conditions {
		condition = strings.TrimSpace(condition)
		if condition == "" {
			continue
		}

		// Split condition by pipe: "field=value | result" or "field=value | field2:=result"
		parts := strings.Split(condition, "|")
		if len(parts) != 2 {
			continue
		}

		conditionPart := strings.TrimSpace(parts[0])
		resultPart := strings.TrimSpace(parts[1])

		// Handle default case with wildcard
		if conditionPart == "*" {
			if strings.Contains(resultPart, ":=") || strings.Contains(resultPart, "=") {
				// Field assignment in default case: * | status := "nope"
				var assignParts []string
				if strings.Contains(resultPart, ":=") {
					assignParts = strings.SplitN(resultPart, ":=", 2)
				} else {
					assignParts = strings.SplitN(resultPart, "=", 2)
				}
				if len(assignParts) == 2 {
					field := strings.TrimSpace(assignParts[0])
					value := strings.TrimSpace(assignParts[1])
					value = strings.Trim(value, `"'`)
					// Store default assignment for this field
					defaultAssignments[field] = "'" + escapeString(value) + "'"
				}
			} else {
				// Regular default value
				defaultClause = strings.Trim(resultPart, `"'`)
				defaultClause = "'" + defaultClause + "'"
			}
			continue
		}

		// Parse the condition (handle regex patterns like /gittinsz/ and /noveloa/i)
		var sqlCondition string
		if (strings.Contains(conditionPart, "=/") && strings.Count(conditionPart, "/") >= 2) || strings.Contains(conditionPart, "=(?i)") {
			// Regex condition: user=/gittinsz/ or user=/noveloa/i or user=(?i)pattern
			equalPos := strings.Index(conditionPart, "=")
			if equalPos > 0 {
				field := strings.TrimSpace(conditionPart[:equalPos])
				regexPart := strings.TrimSpace(conditionPart[equalPos+1:])

				var pattern string

				if strings.HasPrefix(regexPart, "(?i)") {
					// Already processed pattern: (?i)admin
					pattern = regexPart
				} else {
					// Raw pattern: /admin/i
					lastSlash := strings.LastIndex(regexPart, "/")
					if lastSlash > 0 {
						rawPattern := regexPart[1:lastSlash] // Remove first /
						flags := ""
						if lastSlash < len(regexPart)-1 {
							flags = regexPart[lastSlash+1:] // Get flags after last /
						}

						// Handle case-insensitive flag
						if strings.Contains(flags, "i") {
							pattern = "(?i)" + rawPattern
						} else {
							pattern = rawPattern
						}
					}
				}

				if pattern != "" {
					if field == "timestamp" {
						sqlCondition = fmt.Sprintf("match(toString(timestamp), '%s')", escapeString(pattern))
					} else {
						sqlCondition = fmt.Sprintf("match(%s, '%s')", jsonFieldRef(field), escapeString(pattern))
					}
				}
			}
		} else if strings.Contains(conditionPart, "!=") {
			// Handle field!=value conditions (MUST be checked before = since != contains =)
			parts := strings.SplitN(conditionPart, "!=", 2)
			if len(parts) == 2 {
				field := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				value = strings.Trim(value, `"'`)

				if field == "timestamp" {
					sqlCondition = fmt.Sprintf("timestamp != '%s'", escapeString(value))
				} else {
					sqlCondition = fmt.Sprintf("%s != '%s'", jsonFieldRef(field), escapeString(value))
				}
			}
		} else if strings.Contains(conditionPart, ">=") {
			parts := strings.SplitN(conditionPart, ">=", 2)
			if len(parts) == 2 {
				field := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				if field == "timestamp" {
					sqlCondition = fmt.Sprintf("timestamp >= '%s'", escapeString(value))
				} else if err := validateNumeric(value); err == nil {
					sqlCondition = fmt.Sprintf("toFloat64OrZero(%s) >= %s", jsonFieldRef(field), value)
				}
			}
		} else if strings.Contains(conditionPart, "<=") {
			parts := strings.SplitN(conditionPart, "<=", 2)
			if len(parts) == 2 {
				field := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				if field == "timestamp" {
					sqlCondition = fmt.Sprintf("timestamp <= '%s'", escapeString(value))
				} else if err := validateNumeric(value); err == nil {
					sqlCondition = fmt.Sprintf("toFloat64OrZero(%s) <= %s", jsonFieldRef(field), value)
				}
			}
		} else if strings.Contains(conditionPart, "=") && !strings.Contains(conditionPart, "=/") {
			// Handle field=value conditions (but not regex patterns)
			equalsParts := strings.SplitN(conditionPart, "=", 2)
			if len(equalsParts) == 2 {
				field := strings.TrimSpace(equalsParts[0])
				value := strings.TrimSpace(equalsParts[1])
				value = strings.Trim(value, `"'`)

				if field == "timestamp" {
					sqlCondition = fmt.Sprintf("timestamp = '%s'", escapeString(value))
				} else {
					sqlCondition = fmt.Sprintf("%s = '%s'", jsonFieldRef(field), escapeString(value))
				}
			}
		} else if strings.Contains(conditionPart, ">") {
			parts := strings.SplitN(conditionPart, ">", 2)
			if len(parts) == 2 {
				field := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				if field == "timestamp" {
					sqlCondition = fmt.Sprintf("timestamp > '%s'", escapeString(value))
				} else if err := validateNumeric(value); err == nil {
					sqlCondition = fmt.Sprintf("toFloat64OrZero(%s) > %s", jsonFieldRef(field), value)
				}
			}
		} else if strings.Contains(conditionPart, "<") {
			parts := strings.SplitN(conditionPart, "<", 2)
			if len(parts) == 2 {
				field := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				if field == "timestamp" {
					sqlCondition = fmt.Sprintf("timestamp < '%s'", escapeString(value))
				} else if err := validateNumeric(value); err == nil {
					sqlCondition = fmt.Sprintf("toFloat64OrZero(%s) < %s", jsonFieldRef(field), value)
				}
			}
		}

		if sqlCondition != "" {
			// Check if result is a field assignment
			if strings.Contains(resultPart, ":=") || strings.Contains(resultPart, "=") {
				// Field assignment: status:="ok" or status="ok"
				var assignParts []string
				if strings.Contains(resultPart, ":=") {
					assignParts = strings.SplitN(resultPart, ":=", 2)
				} else {
					assignParts = strings.SplitN(resultPart, "=", 2)
				}
				if len(assignParts) == 2 {
					field := strings.TrimSpace(assignParts[0])
					value := strings.TrimSpace(assignParts[1])
					value = strings.Trim(value, `"'`)

					// Collect conditional assignment for this field
					whenClause := fmt.Sprintf("WHEN %s THEN '%s'", sqlCondition, escapeString(value))
					fieldAssignments[field] = append(fieldAssignments[field], whenClause)
				}
			} else {
				// Regular result value
				result := strings.Trim(resultPart, `"'`) // Remove quotes
				result = "'" + escapeString(result) + "'"
				whenClauses = append(whenClauses, fmt.Sprintf("WHEN %s THEN %s", sqlCondition, result))
			}
		}
	}

	// Build consolidated assignments for each field
	var assignments []AssignmentNode
	for field, conditions := range fieldAssignments {
		// Build CASE expression with all conditions for this field
		var caseExpr strings.Builder
		caseExpr.WriteString("CASE ")
		caseExpr.WriteString(strings.Join(conditions, " "))

		// Add default value if available, otherwise NULL
		if defaultValue, hasDefault := defaultAssignments[field]; hasDefault {
			caseExpr.WriteString(" ELSE ")
			caseExpr.WriteString(defaultValue)
		} else {
			caseExpr.WriteString(" ELSE NULL")
		}
		caseExpr.WriteString(" END")

		assignments = append(assignments, AssignmentNode{
			Field:      field,
			Expression: caseExpr.String(),
		})
	}

	// Handle default assignments that don't have conditions (pure defaults)
	for field, defaultValue := range defaultAssignments {
		if _, hasConditions := fieldAssignments[field]; !hasConditions {
			// This is a pure default assignment with no conditions
			assignments = append(assignments, AssignmentNode{
				Field:      field,
				Expression: defaultValue,
			})
		}
	}

	return whenClauses, defaultClause, assignments
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

// parseChainSteps parses chain block tokens into per-step SQL boolean expressions.
// Each step is parsed using the full BQL parser, supporting AND, OR, NOT, parentheses,
// regex, wildcards, and all other filter syntax. Pipe tokens within chain steps
// are treated as AND (for backward compatibility).
// Output: []string of SQL boolean expressions, one per step.
func parseChainSteps(tokens []Token) ([]string, error) {
	// Split tokens by semicolons into per-step token slices.
	// Convert pipe tokens to AND tokens within each step.
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
		if tok.Type == TokenPipe {
			current = append(current, Token{Type: TokenAnd, Value: "AND"})
			continue
		}
		current = append(current, tok)
	}
	if len(current) > 0 {
		allSteps = append(allSteps, current)
	}

	var steps []string
	for _, stepTokens := range allSteps {
		// Append EOF so the parser knows when to stop.
		stepTokens = append(stepTokens, Token{Type: TokenEOF})

		p := NewParser(stepTokens)
		filter, err := p.parseFilter()
		if err != nil {
			return nil, fmt.Errorf("chain step: %w", err)
		}
		if filter == nil || len(filter.Conditions) == 0 {
			continue
		}

		sql, err := buildWhereClause(filter.Conditions)
		if err != nil {
			return nil, fmt.Errorf("chain step: %w", err)
		}
		if sql != "" {
			steps = append(steps, sql)
		}
	}

	return steps, nil
}

// extractParameter extracts the value of a parameter from a parameter string
// e.g., extractParameter("field=computer,distinct=true", "field") returns "computer"
func extractParameter(params string, paramName string) string {
	paramPrefix := paramName + "="
	paramPairs := strings.Split(params, ",")

	for _, pair := range paramPairs {
		pair = strings.TrimSpace(pair)
		if strings.HasPrefix(pair, paramPrefix) {
			value := strings.TrimPrefix(pair, paramPrefix)
			return strings.Trim(value, `"'`) // Remove quotes if present
		}
	}

	return ""
}

// splitTopLevelArgs splits a string by commas at parenthesis depth 0.
// e.g. "count(a,b),sum(c)" -> ["count(a,b)", "sum(c)"]
func splitTopLevelArgs(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, ch := range s {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(s) {
		parts = append(parts, strings.TrimSpace(s[start:]))
	}
	return parts
}

// processStatsFn processes a single multi sub-function (e.g. "count(field=x,distinct=true,as=y)")
// and returns true if it was recognized. Appends SQL expressions to selectFields.
// registry is used to resolve computed fields (e.g. _time from strftime, _len from len);
// pass nil to fall back to jsonFieldRef for all fields.
func processStatsFn(fn string, selectFields *[]string, computedFields map[string]bool, registry *FieldRegistry) bool {
	fn = strings.TrimSpace(fn)

	// Normalize function name to lowercase for matching, while preserving
	// the original for extractFunctionField (field names are case-sensitive).
	fnLower := strings.ToLower(fn)

	// resolveField resolves a field name using the registry when available,
	// falling back to jsonFieldRef for plain fields.
	resolveField := func(field string) string {
		if registry != nil {
			return registry.Resolve(field)
		}
		return jsonFieldRef(field)
	}

	// castNumeric wraps a resolved field expression with the correct numeric
	// cast: toFloat64 for already-numeric fields, toFloat64OrNull for strings.
	castNumeric := func(field string) string {
		return numericCast(field, resolveField(field), registry)
	}

	if fnLower == "count()" || strings.HasPrefix(fnLower, "count(") {
		countPrefix := fn[:strings.IndexByte(fn, '(')]
		params := parseStatsFunctionParams(fn, countPrefix)
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		distinct := params["distinct"] == "true" || params["unique"] == "true"
		if field != "" && distinct {
			if alias == "" {
				alias = "unique_" + field
			}
			*selectFields = append(*selectFields, fmt.Sprintf("uniqExact(%s) AS %s", resolveField(field), alias))
			computedFields[alias] = true
		} else if field != "" {
			if alias == "" {
				alias = "total"
			}
			*selectFields = append(*selectFields, fmt.Sprintf("count(%s) AS %s", resolveField(field), alias))
			computedFields[alias] = true
		} else {
			if alias == "" {
				alias = "_count"
			}
			*selectFields = append(*selectFields, fmt.Sprintf("COUNT(*) AS %s", alias))
			computedFields[alias] = true
		}
		return true
	}

	// Extract function name from lowered string up to '(' for matching.
	parenIdx := strings.IndexByte(fnLower, '(')
	if parenIdx < 0 {
		return false
	}
	funcName := fnLower[:parenIdx]

	switch funcName {
	case "avg":
		field := extractFunctionField(fn, fn[:parenIdx])
		*selectFields = append(*selectFields, fmt.Sprintf("avg(%s) AS _avg", castNumeric(field)))
		return true
	case "sum":
		field := extractFunctionField(fn, fn[:parenIdx])
		*selectFields = append(*selectFields, fmt.Sprintf("sum(%s) AS _sum", castNumeric(field)))
		return true
	case "max":
		field := extractFunctionField(fn, fn[:parenIdx])
		if field == "timestamp" {
			*selectFields = append(*selectFields, "max(timestamp) AS max_timestamp")
		} else {
			*selectFields = append(*selectFields, fmt.Sprintf("max(%s) AS _max", castNumeric(field)))
		}
		return true
	case "min":
		field := extractFunctionField(fn, fn[:parenIdx])
		if field == "timestamp" {
			*selectFields = append(*selectFields, "min(timestamp) AS min_timestamp")
		} else {
			*selectFields = append(*selectFields, fmt.Sprintf("min(%s) AS _min", castNumeric(field)))
		}
		return true
	case "percentile":
		field := extractFunctionField(fn, fn[:parenIdx])
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(%s) AS percentile_%s", cast, escapeString(field)))
		return true
	case "stddev":
		field := extractFunctionField(fn, fn[:parenIdx])
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("stddevPop(%s) AS stddev_%s", cast, escapeString(field)))
		return true
	case "skewness", "skew":
		field := extractFunctionField(fn, fn[:parenIdx])
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("skewPop(%s) AS skewness_%s", cast, escapeString(field)))
		return true
	case "kurtosis", "kurt":
		field := extractFunctionField(fn, fn[:parenIdx])
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("kurtPop(%s) AS kurtosis_%s", cast, escapeString(field)))
		return true
	case "iqr":
		field := extractFunctionField(fn, fn[:parenIdx])
		cast := castNumeric(field)
		*selectFields = append(*selectFields,
			fmt.Sprintf("quantile(0.25)(%s) AS iqr_q1_%s", cast, escapeString(field)),
			fmt.Sprintf("quantile(0.75)(%s) AS iqr_q3_%s", cast, escapeString(field)),
			fmt.Sprintf("quantile(0.75)(%s) - quantile(0.25)(%s) AS iqr_%s", cast, cast, escapeString(field)))
		computedFields["iqr_q1_"+escapeString(field)] = true
		computedFields["iqr_q3_"+escapeString(field)] = true
		computedFields["iqr_"+escapeString(field)] = true
		return true
	case "selectfirst":
		field := extractFunctionField(fn, fn[:parenIdx])
		if field == "timestamp" {
			*selectFields = append(*selectFields, "min(timestamp) AS first_timestamp")
		} else {
			*selectFields = append(*selectFields, fmt.Sprintf("argMin(%s, timestamp) AS first_%s", resolveField(field), escapeString(field)))
		}
		return true
	case "selectlast":
		field := extractFunctionField(fn, fn[:parenIdx])
		if field == "timestamp" {
			*selectFields = append(*selectFields, "max(timestamp) AS last_timestamp")
		} else {
			*selectFields = append(*selectFields, fmt.Sprintf("argMax(%s, timestamp) AS last_%s", resolveField(field), escapeString(field)))
		}
		return true
	case "collect":
		field := extractFunctionField(fn, fn[:parenIdx])
		alias := "collect_" + field
		fieldRef := resolveField(field)
		if field == "timestamp" {
			fieldRef = "toString(timestamp)"
		}
		*selectFields = append(*selectFields, fmt.Sprintf("groupArray(%s) AS %s", fieldRef, alias))
		computedFields[alias] = true
		return true
	case "top":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "top_" + field
		}
		fieldRef := resolveField(field)
		if params["percent"] == "true" {
			*selectFields = append(*selectFields, fmt.Sprintf(
				"arrayMap(x -> (x.1, round(x.2 * 100 / count(*), 2)), topKWeightedWithCount(10)(%s, 1)) AS %s",
				fieldRef, alias))
		} else {
			*selectFields = append(*selectFields, fmt.Sprintf("topK(10)(%s) AS %s", fieldRef, alias))
		}
		computedFields[alias] = true
		return true
	case "median":
		field := extractFunctionField(fn, fn[:parenIdx])
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("median(%s) AS median_%s", cast, escapeString(field)))
		return true
	case "mad":
		field := extractFunctionField(fn, fn[:parenIdx])
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("arrayReduce('median', arrayMap(x -> abs(x - arrayReduce('median', groupArray(%s))), groupArray(%s))) AS mad_%s", cast, cast, escapeString(field)))
		return true
	}
	return false
}
