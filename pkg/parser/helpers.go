package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
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

	// Cast to ::String: childRef/parentRef feed the recursive INNER JOIN ON key
	// and concat(_path); a bare Dynamic subcolumn errors 44 there. The base-case
	// equality childRef = 'startvalue' stays index-safe (no-op cast is elided).
	childRef := groupableCast(jsonFieldRef(childField))
	parentRef := groupableCast(jsonFieldRef(parentField))

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
		ref := groupableCast(jsonFieldRef(f))
		safeAlias := strings.ReplaceAll(f, ".", "_")
		baseIncludeCols += fmt.Sprintf(", %s AS _%s", ref, safeAlias)
		recursiveIncludeCols += fmt.Sprintf(", l.%s AS _%s", ref, safeAlias)
	}

	// Base case: find starting node(s)
	sql.WriteString("SELECT timestamp, norm_log, log_id, ")
	sql.WriteString("toUInt32(0) AS _depth, ")
	sql.WriteString(fmt.Sprintf("%s AS _node_id, ", childRef))
	sql.WriteString(fmt.Sprintf("%s AS _path", childRef))
	sql.WriteString(baseIncludeCols)
	sql.WriteString(fmt.Sprintf(" FROM %s ", opts.EffectiveTableName()))
	sql.WriteString(fmt.Sprintf("WHERE %s ", baseWhere))

	sql.WriteString("UNION ALL ")

	// Recursive case: find children via parent->child relationship
	sql.WriteString("SELECT l.timestamp, l.norm_log, l.log_id, ")
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
				// For fields that are part of the CTE output (child, parent, or include
				// fields), the CTE exposes them as _alias, not as JSON subcolumn refs.
				// table() generates jsonFieldRef expressions that are invalid inside
				// SELECT FROM traversal; remap them to the CTE column form.
				lookupAlias := strings.Trim(alias, "`")
				safeAlias := strings.ReplaceAll(lookupAlias, ".", "_")
				if seen[lookupAlias] {
					formattedFields = append(formattedFields, fmt.Sprintf("_%s AS %s", safeAlias, safeAlias))
				} else {
					formattedFields = append(formattedFields, field)
				}
			}
		}
		sql.WriteString(strings.Join(formattedFields, ", "))
	} else {
		sql.WriteString("formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') as timestamp, ")
		sql.WriteString("norm_log, log_id, toString(_depth) AS _depth, _path")
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
			if alias != "_all_fields" && alias != "norm_log" && alias != "log_id" {
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

// procLineageFractalCond builds the fractal-isolation predicate for proc_lineage,
// mirroring addBaseConditions exactly (prism FractalIDs, single FractalID, and the
// IncludeEmptyFractalID legacy-data case). prefix is "" for the base case or "l." for the
// recursive JOIN alias. Returns "" when no fractal scope is set (same as logs queries).
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

var procTreeFieldOrder = []string{"timestamp", "process_guid", "parent_guid", "image", "parent_image", "commandline", "computer_name", "_depth", "_path"}

// ptg() projected into pgr()'s edge shape for pgraph(). Column names, order, and expressions
// mirror provenance spawn rows (see pkg/query/provenance.go) so pgraph stays a single-shape
// renderer. command_line is capped like pgr's; proc_user is empty because proc_lineage carries
// no user column and ptg() deliberately does not join logs to get one.
const procTreePgraphCols = "parent_guid AS parent, process_guid AS child, image AS label, 'spawn' AS event_type, " +
	"log_id, toString(timestamp) AS timestamp, fractal_id, substring(commandline, 1, 300) AS command_line, " +
	"'' AS proc_user, computer_name AS host, parent_image AS parent_label"

var procTreePgraphFieldOrder = []string{"parent", "child", "label", "event_type", "log_id", "timestamp", "fractal_id", "command_line", "proc_user", "host", "parent_label"}

// buildProcessTreeSQL generates a recursive CTE for ptg() over the flat proc_lineage
// table (MV-backed process lineage): the fast replacement for dfs/bfs-on-logs for process
// trees. Unlike buildTraversalSQL it uses bare columns (no fields.* JSON access, no
// norm_log) and reads proc_lineage FINAL so re-ingested / iceberg-replayed duplicates
// collapse. Only the time/fractal base conditions are applied (proc_lineage is flat, so
// user fields.* filters are dropped -- v1 scoping).
func buildProcessTreeSQL(
	startValue string, maxDepth int, direction string,
	havingConditions []string,
	chartType string, chartConfig map[string]interface{},
	opts QueryOptions,
) (*TranslationResult, error) {
	tbl := opts.ProcLineageTable
	if tbl == "" {
		tbl = "proc_lineage"
	}

	// Reconstruct ONLY base time + fractal scoping from opts. proc_lineage is flat, so
	// user pre-filters (fields.* field filters, norm_log free-text search, etc.) do not
	// apply and are intentionally dropped (v1 scoping). Rebuilding from opts -- rather than
	// reusing source.Layer.Where -- guarantees fractal isolation and prevents leaking
	// logs-only columns (norm_log/raw_log/ingest_timestamp) that would crash the query.
	tsStart := chTimeLiteral(opts.StartTime)
	tsEnd := chTimeLiteral(opts.EndTime)
	baseConds := []string{
		fmt.Sprintf("timestamp >= '%s'", tsStart),
		fmt.Sprintf("timestamp <= '%s'", tsEnd),
	}
	recConds := []string{
		fmt.Sprintf("l.timestamp >= '%s'", tsStart),
		fmt.Sprintf("l.timestamp <= '%s'", tsEnd),
	}
	if fc := procLineageFractalCond(opts, ""); fc != "" {
		baseConds = append(baseConds, fc)
		recConds = append(recConds, procLineageFractalCond(opts, "l."))
	}
	baseConds = append(baseConds, fmt.Sprintf("process_guid = '%s'", escapeString(startValue)))
	baseWhere := strings.Join(baseConds, " AND ")
	recTail := " AND " + strings.Join(recConds, " AND ")

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

	// fractal_id rides the CTE (it is not shown in the table projection) because the pgraph
	// projection below needs it for the node click -> log detail fetch.
	const dataCols = "timestamp, log_id, fractal_id, process_guid, parent_guid, image, parent_image, commandline, computer_name"
	const dataColsL = "l.timestamp, l.log_id, l.fractal_id, l.process_guid, l.parent_guid, l.image, l.parent_image, l.commandline, l.computer_name"
	const unionCols = "timestamp, log_id, fractal_id, process_guid, parent_guid, image, parent_image, commandline, computer_name, _depth, _path"
	const finalCols = "formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') AS timestamp, process_guid, parent_guid, image, parent_image, commandline, computer_name, log_id, toString(_depth) AS _depth, _path"

	// One recursive CTE body per direction. forward = descendants (join children on
	// l.parent_guid = t._node_id); backward = ancestors (walk up via l.process_guid =
	// t._node_id, carrying parent_guid as the next node to find).
	cte := func(name, dir string) string {
		if dir == "backward" {
			return fmt.Sprintf(
				"%s AS (SELECT %s, toUInt32(0) AS _depth, parent_guid AS _node_id, process_guid AS _path FROM %s FINAL WHERE %s"+
					" UNION ALL "+
					"SELECT %s, t._depth + 1 AS _depth, l.parent_guid AS _node_id, concat(l.process_guid, ' > ', t._path) AS _path"+
					" FROM %s AS l FINAL INNER JOIN %s t ON l.process_guid = t._node_id WHERE t._depth < %d%s)",
				name, dataCols, tbl, baseWhere, dataColsL, tbl, name, maxDepth, recTail)
		}
		return fmt.Sprintf(
			"%s AS (SELECT %s, toUInt32(0) AS _depth, process_guid AS _node_id, process_guid AS _path FROM %s FINAL WHERE %s"+
				" UNION ALL "+
				"SELECT %s, t._depth + 1 AS _depth, l.process_guid AS _node_id, concat(t._path, ' > ', l.process_guid) AS _path"+
				" FROM %s AS l FINAL INNER JOIN %s t ON l.parent_guid = t._node_id WHERE t._depth < %d%s)",
			name, dataCols, tbl, baseWhere, dataColsL, tbl, name, maxDepth, recTail)
	}

	var withClause, traversal string
	switch direction {
	case "forward", "backward":
		withClause = "WITH RECURSIVE " + cte("tree", direction) + " "
		traversal = "tree"
	default: // both
		withClause = "WITH RECURSIVE " + cte("tree_fwd", "forward") + ", " + cte("tree_bwd", "backward") + " "
		traversal = fmt.Sprintf("(SELECT %s FROM tree_fwd UNION DISTINCT SELECT %s FROM tree_bwd)", unionCols, unionCols)
	}

	// Post-traversal filters (e.g. _depth <= 3) MUST be applied in an inner scope where
	// _depth is still UInt32. If the filter shares a SELECT with `toString(_depth) AS _depth`,
	// ClickHouse resolves the WHERE identifier to the String alias (NO_COMMON_TYPE, code 386),
	// so the toString projection lives in a strictly outer SELECT than the filter.
	inner := "SELECT " + unionCols + " FROM " + traversal
	if len(havingConditions) > 0 {
		inner += " WHERE " + strings.Join(havingConditions, " AND ")
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
	sql.WriteString(withClause)
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
	return buildWhereClauseCtx(conditions, nil)
}

// buildWhereClauseCtx builds the WHERE clause SQL for a list of condition nodes,
// respecting OR/AND logic, parenthetical grouping, and NOT negation. When
// registry is non-nil, fields that name a computed column (e.g. a prior
// assignment or aggregate) resolve to that column instead of a raw JSON
// sub-column; pass nil for queries against the base log fields.
func buildWhereClauseCtx(conditions []ConditionNode, registry *FieldRegistry) (string, error) {
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
				condSQL, err := translateConditionCtx(gc, registry)
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
			condSQL, err := translateConditionCtx(cond, registry)
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

func translateConditionCtx(cond ConditionNode, registry *FieldRegistry) (string, error) {
	// A condition function was compiled to SQL before materialization.
	if cond.CommandSQL != "" {
		if cond.Negate {
			return "NOT (" + cond.CommandSQL + ")", nil
		}
		return cond.CommandSQL, nil
	}

	// Handle compound nodes by recursively building the inner SQL.
	if cond.IsCompound {
		innerSQL, err := buildWhereClauseCtx(cond.Children, registry)
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
	// resolvedComputed is set when the field names a computed column produced
	// earlier in the pipeline (assignment, aggregate, ...). Such a column is
	// referenced directly rather than as a raw JSON sub-column.
	resolvedComputed := false
	switch cond.Field {
	case normLogColumn:
		fieldRef = normLogColumn
		if registry != nil {
			fieldRef = contentColMode(registry.sourceMode)
		}
	case "timestamp":
		fieldRef = "timestamp"
	case "log_id":
		fieldRef = "log_id"
	case "normalizer", "_normalizer":
		// The per-row normalizer stamp ("name@version"). Addressable under either name;
		// _normalizer is how it surfaces in the detail grid.
		fieldRef = "normalizer"
	default:
		if registry != nil {
			if e := registry.Get(cond.Field); e != nil && e.Kind != FieldKindBase && e.Kind != FieldKindJSON {
				fieldRef = registry.Resolve(cond.Field)
				resolvedComputed = true
			}
		}
		if !resolvedComputed {
			if registry != nil {
				fieldRef = registry.fieldRef(cond.Field)
			} else {
				fieldRef = jsonFieldRef(cond.Field)
			}
			isJSONField = true
		}
	}

	// numericRef coerces the field to Float64 for comparison. Raw JSON
	// sub-columns are already String (::String), so toFloat64OrZero suffices; a
	// resolved computed column may already be numeric, so normalize via
	// toString() first to keep toFloat64OrZero well-typed across column types.
	numericRef := func() string {
		if resolvedComputed {
			return fmt.Sprintf("toFloat64OrZero(toString(%s))", fieldRef)
		}
		return fmt.Sprintf("toFloat64OrZero(%s)", fieldRef)
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
		negate := cond.Operator == "!="
		// Never use hasToken pre-filters for regex: regex is a substring match but
		// hasToken requires an exact complete token. /http/ matches "https://..." but
		// hasToken(raw_log, 'http') = FALSE because the Bloom filter token is "https".
		// False negatives are unacceptable; ClickHouse's built-in granule pruning for
		// match() is sufficient. match() is called with the negate flag only.
		sql = buildRegexMatchSQL(fieldRef, cond.Value, cond.LiteralTerm, negate, sourceModeOf(registry))
	} else if cond.Operator == "=~" || cond.Operator == "=^" || cond.Operator == "=$" {
		values := cond.Values
		if len(values) == 0 && cond.Value != "" {
			values = []string{cond.Value}
		}
		switch cond.Operator {
		case "=~":
			sql = buildContainsAnySQL(fieldRef, values, false, sourceModeOf(registry))
		case "=^":
			sql = buildStartsWithAnySQL(fieldRef, values, false, sourceModeOf(registry))
		case "=$":
			sql = buildEndsWithAnySQL(fieldRef, values, false, sourceModeOf(registry))
		}
	} else if (cond.Operator == "=" || cond.Operator == "!=") && len(cond.Values) > 1 {
		// Comma-separated equality list -> IN / NOT IN.
		negate := cond.Operator == "!="
		if registry != nil && registry.sourceMode == SourceIceberg && isJSONField {
			sql = buildIcebergEqualityListSQL(cond.Field, cond.Values, negate, registry.icePromoted)
		} else {
			sql = buildEqualityListSQL(fieldRef, cond.Values, negate, isJSONField)
		}
	} else {
		// For comparison operators, try to convert to numeric if the value looks numeric
		// This allows queries like: bytes > 1000
		switch cond.Operator {
		case "=":
			// Field-qualified equality is answered solely by the JSON sub-column (or the
			// direct column for raw_log/timestamp/log_id). Type-hinted fields prune granules
			// via their dedicated bloom_filter/set skip index; dynamic fields scan within the
			// time+fractal partition. No raw_log token pre-filter is added: the value is not
			// guaranteed to appear verbatim in raw_log (e.g. normalized/derived fields), so
			// such a pre-filter can drop real matches. raw_log is for unqualified search only.
			if registry != nil && registry.sourceMode == SourceIceberg && isJSONField {
				// MAP correctness + promoted `_ice_` column pruning (icebergEqualityPredicate).
				sql = icebergEqualityPredicate(cond.Field, cond.Value, registry.icePromoted)
			} else if resolvedComputed && validateNumeric(cond.Value) == nil {
				sql = fmt.Sprintf("%s = %s", numericRef(), cond.Value)
			} else {
				sql = fmt.Sprintf("%s = '%s'", fieldRef, escapeString(cond.Value))
			}
		case "!=":
			switch {
			case resolvedComputed && validateNumeric(cond.Value) == nil:
				sql = fmt.Sprintf("%s != %s", numericRef(), cond.Value)
			case isJSONField:
				// For JSON fields, include rows where the field doesn't exist (NULL).
				// Without this, NULL != 'value' evaluates to NULL (falsy) and
				// silently excludes rows missing the field.
				sql = fmt.Sprintf("(%s IS NULL OR %s != '%s')", fieldRef, fieldRef, escapeString(cond.Value))
			default:
				sql = fmt.Sprintf("%s != '%s'", fieldRef, escapeString(cond.Value))
			}
		case ">", "<", ">=", "<=":
			// Validate numeric value to prevent injection
			if err := validateNumeric(cond.Value); err != nil {
				return "", fmt.Errorf("numeric comparison: %w", err)
			}
			sql = fmt.Sprintf("%s %s %s", numericRef(), cond.Operator, cond.Value)
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
// from SQL, replacing them with empty strings. Handles escaped quotes (\”).
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
		escaped := strings.ReplaceAll(p, "`", "``")
		b.WriteString(".`")
		b.WriteString(escaped)
		b.WriteString("`")
	}
	return b.String() + "::String"
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
	expr := fmt.Sprintf("multiSearchAnyCaseInsensitive(%s, [%s])", fieldRef, strings.Join(quoted, ", "))
	if negate {
		return "NOT (" + expr + ")"
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
func timeFormatExpr(field, chFormat, timezone string, registry *FieldRegistry) string {
	if field == "timestamp" {
		return fmt.Sprintf("formatDateTime(timestamp, '%s', '%s')", escapeString(chFormat), escapeString(timezone))
	}
	return fmt.Sprintf("ifNull(formatDateTime(%s, '%s', '%s'), '')",
		lenientDateTime(resolveFieldRef(field, registry)), escapeString(chFormat), escapeString(timezone))
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
// selfField optionally names the assignment being defined; if an identifier matches it the registry is bypassed
// so that self-referential assignments (e.g. x := x * 100) resolve x as a JSON field rather than a nonexistent alias.
func convertMathExprToSQL(expr string, registry *FieldRegistry, selfField ...string) string {
	currentField := ""
	if len(selfField) > 0 {
		currentField = selfField[0]
	}
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
			if registry.Has(ident) && ident != currentField {
				if registry.IsInline(ident) {
					// Inline-only field (e.g. a pre-aggregation assignment): fold in
					// its expression, which is already numeric.
					result.WriteString(fmt.Sprintf("(%s)", registry.Resolve(ident)))
				} else {
					// Known column (aggregate output, group key, carried column) used
					// in arithmetic. It may be String -- selectFirst/selectLast of a JSON
					// field yield argMin/argMax over fields.`x`::String -- so coerce to
					// Float64. toString() first keeps toFloat64OrNull well-typed whether
					// the column is already numeric or a string.
					result.WriteString(fmt.Sprintf("toFloat64OrNull(toString(%s))", ident))
				}
			} else if registry != nil {
				result.WriteString(fmt.Sprintf("toFloat64OrNull(%s)", groupableCast(registry.fieldRef(ident))))
			} else {
				result.WriteString(fmt.Sprintf("toFloat64OrNull(%s)", groupableCast(jsonFieldRef(ident))))
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
func parseChainSteps(tokens []Token, opts QueryOptions, parentReg *FieldRegistry) ([]string, []string, error) {
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
		sql, stepFields, err := buildChainStep(append(stepTokens, Token{Type: TokenEOF}), opts, parentReg)
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
func buildChainStep(stepTokens []Token, opts QueryOptions, parentReg *FieldRegistry) (string, []string, error) {
	pl, err := NewParser(stepTokens).Parse()
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

	if pl.Filter != nil && len(pl.Filter.Conditions) > 0 {
		if err := resolveCommandConditionNodes(pl.Filter.Conditions, opts); err != nil {
			return "", nil, fmt.Errorf("chain step: %w", err)
		}
		w, err := buildWhereClauseCtx(pl.Filter.Conditions, reg)
		if err != nil {
			return "", nil, fmt.Errorf("chain step: %w", err)
		}
		if w != "" {
			conjuncts = append(conjuncts, w)
		}
		fields = collectConditionFieldsOrdered(pl.Filter.Conditions, seen, fields)
	}

	// Before the conditions below: len()/match()/lookupIP() register the expressions
	// that a later `_len > 500` in the same step resolves against.
	if len(pl.Commands) > 0 {
		w, cf, err := harvestChainCommands(pl, opts, reg)
		if err != nil {
			return "", nil, err
		}
		conjuncts = append(conjuncts, w...)
		for _, f := range cf {
			if !seen[f] {
				seen[f] = true
				fields = append(fields, f)
			}
		}
	}

	// Later filter stages in the step (`a=1 | b=2`) arrive as HavingConditions, as
	// do condition functions used as boolean operands.
	if len(pl.HavingConditions) > 0 {
		if err := resolveCommandConditions(pl.HavingConditions, opts); err != nil {
			return "", nil, fmt.Errorf("chain step: %w", err)
		}
		if h := materializeCondGroup(pl.HavingConditions, reg, nil); h != "" {
			conjuncts = append(conjuncts, h)
		}
		for _, c := range pl.HavingConditions {
			fields = collectHavingFieldsOrdered(c, seen, &fields)
		}
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

// CommandPredicate compiles a condition function into a boolean expression. The
// handler runs against a throwaway plan, so its effect is captured as a predicate
// instead of being appended to the query's WHERE.
func CommandPredicate(cmd CommandNode, opts QueryOptions) (string, error) {
	pl := &PipelineNode{Commands: []CommandNode{cmd}}
	where, _, err := harvestChainCommands(pl, opts, nil)
	if err != nil {
		return "", err
	}
	switch len(where) {
	case 0:
		return "", fmt.Errorf("%s() is not a condition", cmd.Name)
	case 1:
		return where[0], nil
	default:
		return "(" + strings.Join(where, " AND ") + ")", nil
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
	ctx := &CommandContext{Registry: reg, Plan: plan, Opts: opts, Pipeline: pl}

	for _, cmd := range pl.Commands {
		if getCommandHandler(cmd.Name) == nil {
			return nil, nil, fmt.Errorf("chain step: unsupported command %s()", cmd.Name)
		}
	}
	for i, cmd := range pl.Commands {
		ctx.CmdIndex = i
		if err := getCommandHandler(cmd.Name).Declare(cmd, ctx); err != nil {
			return nil, nil, fmt.Errorf("chain step: %w", err)
		}
	}
	for i, cmd := range pl.Commands {
		ctx.CmdIndex = i
		if err := getCommandHandler(cmd.Name).Execute(cmd, ctx); err != nil {
			return nil, nil, fmt.Errorf("chain step: %w", err)
		}
	}

	src := &plan.SourceStage().Layer
	if len(src.GroupBy) > 0 || len(src.OrderBy) > 0 || src.Limit != "" ||
		src.LimitBy != "" || len(src.Having) > 0 || plan.IsAggregated || plan.IsJoin ||
		plan.IsTraversal || plan.IsChain || plan.IsProcessTree || len(plan.WindowLayers) > 0 ||
		plan.ModelLookupSQL != "" || len(plan.Stages) > 1 {
		return nil, nil, fmt.Errorf("chain step: only row conditions are allowed; %s() changes the shape of the result", pl.Commands[0].Name)
	}

	// A projection (len, match, lookupIP) is a per-row scalar, so it belongs in a
	// step even though a step projects nothing: the registry resolves references to
	// it as the expression itself, folding it into the predicate. An alias the
	// registry does not know would resolve to a column that never exists.
	for _, sel := range src.Selects {
		alias := extractFieldAlias(sel.String())
		if alias == "" || !reg.Has(alias) {
			return nil, nil, fmt.Errorf("chain step: only row conditions are allowed; %s() projects a column that cannot be scoped to one step", pl.Commands[0].Name)
		}
	}

	if len(src.Where) == 0 && len(src.Selects) == 0 {
		return nil, nil, fmt.Errorf("chain step: %s() produced no condition", pl.Commands[0].Name)
	}

	// A condition command names its field first (cidr(dst_ip, ...), in(user, [...])).
	var fields []string
	for _, cmd := range pl.Commands {
		if len(cmd.Arguments) > 0 {
			if f := strings.TrimSpace(unwrapList(cmd.Arguments[0])); f != "" && isPlainFieldName(f) {
				fields = append(fields, f)
			}
		}
	}
	return src.Where, fields, nil
}

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
	if c.Field != "" && c.Field != normLogColumn && !seen[c.Field] {
		seen[c.Field] = true
		*out = append(*out, c.Field)
	}
	return *out
}

// splitTopLevelArgs splits a string by commas at parenthesis depth 0.
// e.g. "count(a,b),sum(c)" -> ["count(a,b)", "sum(c)"]
func splitTopLevelArgs(s string) []string {
	var parts []string
	depth := 0
	start := 0
	var quote rune
	for i, ch := range s {
		// Ignore separators inside quoted strings (e.g. sprintf("%d,%d", ...)).
		if quote != 0 {
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
		case '(', '[':
			depth++
		case ')', ']':
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

// commandIndex returns the index of the first top-level command with the given
// name, or -1 when absent.
func commandIndex(commands []CommandNode, name string) int {
	for i, cmd := range commands {
		if cmd.Name == name {
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
		if aggregatingCommandNames[cmd.Name] {
			return i
		}
	}
	return len(commands)
}

// unwrapList strips a single surrounding [ ... ] from a list-style argument and
// returns the inner content. Function-call arguments preserve brackets verbatim
// from the source, so the bracketed list form (e.g. multi([f1, f2])) and the
// plain form (multi(f1, f2)) must both be accepted. Input that is not
// bracket-wrapped is returned unchanged.
func unwrapList(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
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
		// Feeds stats sub-functions (uniq/values/first/last/group); cast raw JSON
		// refs to ::String so Dynamic-stored paths are groupable/correct.
		if registry != nil {
			return groupableCast(registry.Resolve(field))
		}
		return groupableCast(jsonFieldRef(field))
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
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "_avg"
		}
		*selectFields = append(*selectFields, fmt.Sprintf("avg(%s) AS %s", castNumeric(field), alias))
		computedFields[alias] = true
		return true
	case "sum":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "_sum"
		}
		*selectFields = append(*selectFields, fmt.Sprintf("sum(%s) AS %s", castNumeric(field), alias))
		computedFields[alias] = true
		return true
	case "max":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if field == "timestamp" {
			if alias == "" {
				alias = "max_timestamp"
			}
			*selectFields = append(*selectFields, fmt.Sprintf("max(timestamp) AS %s", alias))
		} else {
			if alias == "" {
				alias = "_max"
			}
			*selectFields = append(*selectFields, fmt.Sprintf("max(%s) AS %s", castNumeric(field), alias))
		}
		computedFields[alias] = true
		return true
	case "min":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if field == "timestamp" {
			if alias == "" {
				alias = "min_timestamp"
			}
			*selectFields = append(*selectFields, fmt.Sprintf("min(timestamp) AS %s", alias))
		} else {
			if alias == "" {
				alias = "_min"
			}
			*selectFields = append(*selectFields, fmt.Sprintf("min(%s) AS %s", castNumeric(field), alias))
		}
		computedFields[alias] = true
		return true
	case "percentile":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "percentile_" + escapeString(field)
		}
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(%s) AS %s", cast, alias))
		computedFields[alias] = true
		return true
	case "stddev":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "stddev_" + escapeString(field)
		}
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("stddevPop(%s) AS %s", cast, alias))
		computedFields[alias] = true
		return true
	case "skewness", "skew":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "skewness_" + escapeString(field)
		}
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("skewPop(%s) AS %s", cast, alias))
		computedFields[alias] = true
		return true
	case "kurtosis", "kurt":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "kurtosis_" + escapeString(field)
		}
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("kurtPop(%s) AS %s", cast, alias))
		computedFields[alias] = true
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
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if field == "timestamp" {
			if alias == "" {
				alias = "first_timestamp"
			}
			*selectFields = append(*selectFields, fmt.Sprintf("min(timestamp) AS %s", alias))
		} else {
			if alias == "" {
				alias = "first_" + escapeString(field)
			}
			*selectFields = append(*selectFields, fmt.Sprintf("argMin(%s, timestamp) AS %s", resolveField(field), alias))
		}
		computedFields[alias] = true
		return true
	case "selectlast":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if field == "timestamp" {
			if alias == "" {
				alias = "last_timestamp"
			}
			*selectFields = append(*selectFields, fmt.Sprintf("max(timestamp) AS %s", alias))
		} else {
			if alias == "" {
				alias = "last_" + escapeString(field)
			}
			*selectFields = append(*selectFields, fmt.Sprintf("argMax(%s, timestamp) AS %s", resolveField(field), alias))
		}
		computedFields[alias] = true
		return true
	case "collect":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "collect_" + field
		}
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
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "median_" + escapeString(field)
		}
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("median(%s) AS %s", cast, alias))
		computedFields[alias] = true
		return true
	case "mad":
		params := parseStatsFunctionParams(fn, fn[:parenIdx])
		field := params["field"]
		if field == "" {
			field = params["_positional"]
		}
		alias := params["as"]
		if alias == "" {
			alias = "mad_" + escapeString(field)
		}
		cast := castNumeric(field)
		*selectFields = append(*selectFields, fmt.Sprintf("arrayReduce('median', arrayMap(x -> abs(x - arrayReduce('median', groupArray(%s))), groupArray(%s))) AS %s", cast, cast, alias))
		computedFields[alias] = true
		return true
	}
	return false
}
