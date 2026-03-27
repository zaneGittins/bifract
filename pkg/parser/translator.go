package parser

import (
	"fmt"
	"strings"
	"time"
)

type QueryOptions struct {
	StartTime             time.Time
	EndTime               time.Time
	MaxRows               int
	FractalID             string                        // Fractal UUID for filtering logs to specific fractal
	FractalIDs            []string                      // Multiple fractal UUIDs (prism context); overrides FractalID when set
	IncludeEmptyFractalID bool                          // Include logs with no fractal_id (legacy data) when querying default fractal
	Dictionaries          map[string]map[string]string  // dict name -> key col -> ClickHouse lookup name
	HasCommentFilter      bool                          // True when query uses comment() and log_ids have been pre-fetched
	CommentLogIDs         []string                      // Pre-fetched log_ids from PostgreSQL for comment() filtering
	UseIngestTimestamp    bool                          // Filter on ingest_timestamp instead of timestamp (used by alerts)
	AlertExtraFields     []string                      // Additional fields to project in alert auto-projection (throttle field, template fields)
	GeoIPEnabled         bool                          // True when MaxMind GeoLite2 dictionaries are loaded
	TableName            string                        // Override source table (default "logs", use "logs_distributed" in cluster mode)
}

// EffectiveTableName returns the table name to query, defaulting to "logs".
func (o QueryOptions) EffectiveTableName() string {
	if o.TableName != "" {
		return o.TableName
	}
	return "logs"
}

type TranslationResult struct {
	SQL          string
	FieldOrder   []string
	IsAggregated bool
	ChartType    string                 // "piechart", "barchart", "heatmap", "histogram", "" (empty for table)
	ChartConfig  map[string]interface{} // Chart-specific configuration
}

func TranslateToSQL(pipeline *PipelineNode, opts QueryOptions) (string, error) {
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

func TranslateToSQLWithOrder(pipeline *PipelineNode, opts QueryOptions) (*TranslationResult, error) {
	registry := NewFieldRegistry()
	plan := NewQueryPlan()
	ctx := &CommandContext{
		Registry: registry,
		Plan:     plan,
		Opts:     opts,
		Pipeline: pipeline,
	}

	// ---------------------------------------------------------------
	// 1. Base WHERE conditions (time range, fractal isolation)
	// ---------------------------------------------------------------
	addBaseConditions(plan, opts)

	// ---------------------------------------------------------------
	// 2. Process filter conditions from the parser
	// ---------------------------------------------------------------
	if pipeline.Filter != nil {
		whereSQL, err := buildWhereClause(pipeline.Filter.Conditions)
		if err != nil {
			return nil, err
		}
		if whereSQL != "" {
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, whereSQL)
		}
	}

	// ---------------------------------------------------------------
	// 3. DECLARE PHASE: every command registers what fields it produces
	// ---------------------------------------------------------------
	for i, cmd := range pipeline.Commands {
		ctx.CmdIndex = i
		handler := getCommandHandler(cmd.Name)
		if handler == nil {
			return nil, fmt.Errorf("unsupported command: %s", cmd.Name)
		}
		if err := handler.Declare(cmd, ctx); err != nil {
			return nil, err
		}
	}

	// Register assignment fields
	for _, assignment := range pipeline.Assignments {
		registry.Register(assignment.Field, FieldKindAssignment, assignment.Field, -1)
	}

	// ---------------------------------------------------------------
	// 4. CONDITION CLASSIFICATION: classify HavingConditions by kind.
	//    SQL generation is deferred to after Execute (step 6b).
	// ---------------------------------------------------------------
	classifyConditions(pipeline.HavingConditions, registry, plan)

	// ---------------------------------------------------------------
	// 5. Process field assignments
	// ---------------------------------------------------------------
	var assignmentFields []string
	var deferredAssignments []AssignmentNode
	for _, assignment := range pipeline.Assignments {
		safeField, err := sanitizeIdentifier(assignment.Field)
		if err != nil {
			return nil, fmt.Errorf("invalid assignment field: %w", err)
		}

		expr := assignment.Expression
		isMathExpr := assignment.ExpressionType == TokenValue &&
			(strings.ContainsAny(expr, "+/()") || strings.Contains(expr, " - ") || strings.Contains(expr, " -") || strings.Contains(expr, "- "))
		if isMathExpr {
			deferredAssignments = append(deferredAssignments, assignment)
			continue
		}

		var expression string
		switch assignment.ExpressionType {
		case TokenString:
			expression = fmt.Sprintf("'%s'", escapeString(assignment.Expression))
		case TokenValue:
			if err := validateNumeric(assignment.Expression); err != nil {
				return nil, fmt.Errorf("invalid numeric assignment: %w", err)
			}
			expression = fmt.Sprintf("toString(%s)", assignment.Expression)
		case TokenField:
			if assignment.Expression == "timestamp" {
				expression = "timestamp"
			} else {
				expression = jsonFieldRef(assignment.Expression)
			}
		case TokenFunction:
			expression = fmt.Sprintf("'%s'", escapeString(assignment.Expression))
		default:
			if assignment.Expression == "timestamp" {
				expression = "timestamp"
			} else {
				expression = jsonFieldRef(assignment.Expression)
			}
		}

		af := fmt.Sprintf("%s AS %s", expression, safeField)
		assignmentFields = append(assignmentFields, af)
		ctx.Registry.SetResolveExpr(safeField, af)
	}

	// ---------------------------------------------------------------
	// 6. EXECUTE PHASE: every command reads registry, writes to plan
	// ---------------------------------------------------------------
	for i, cmd := range pipeline.Commands {
		ctx.CmdIndex = i
		handler := getCommandHandler(cmd.Name)
		if err := handler.Execute(cmd, ctx); err != nil {
			return nil, err
		}
	}

	// ---------------------------------------------------------------
	// 6b. CONDITION MATERIALIZATION: generate SQL from classified
	//     conditions using registry populated by Execute phase.
	// ---------------------------------------------------------------
	materializeConditions(registry, plan)

	// ---------------------------------------------------------------
	// 7. FINALIZE: assemble final SQL from structured plan data
	// ---------------------------------------------------------------
	return finalizePlan(ctx, assignmentFields, deferredAssignments)
}

// addBaseConditions adds time range and fractal isolation conditions.
func addBaseConditions(plan *QueryPlan, opts QueryOptions) {
	source := plan.SourceStage()

	tsCol := "timestamp"
	if opts.UseIngestTimestamp {
		tsCol = "ingest_timestamp"
	}
	source.Layer.Where = append(source.Layer.Where,
		fmt.Sprintf("%s >= '%s'", tsCol, opts.StartTime.Format("2006-01-02 15:04:05")),
		fmt.Sprintf("%s <= '%s'", tsCol, opts.EndTime.Format("2006-01-02 15:04:05")),
	)

	if len(opts.FractalIDs) > 0 {
		quoted := make([]string, len(opts.FractalIDs))
		for i, id := range opts.FractalIDs {
			quoted[i] = fmt.Sprintf("'%s'", escapeString(id))
		}
		if opts.IncludeEmptyFractalID {
			quoted = append(quoted, "''")
		}
		source.Layer.Where = append(source.Layer.Where, "fractal_id IN ("+strings.Join(quoted, ", ")+")")
	} else if opts.FractalID != "" {
		if opts.IncludeEmptyFractalID {
			source.Layer.Where = append(source.Layer.Where, fmt.Sprintf("fractal_id IN ('%s', '')", escapeString(opts.FractalID)))
		} else {
			source.Layer.Where = append(source.Layer.Where, fmt.Sprintf("fractal_id = '%s'", escapeString(opts.FractalID)))
		}
	}
}

// finalizePlan assembles the final SQL from structured plan data using the
// Declare-Execute-Render architecture. It populates the QueryPlan's stages,
// window layers, formatters, and deferred conditions, then calls plan.Render().
//
// This replaces the old monolithic string-surgery approach with registry-based
// field identification and structured rendering.
func finalizePlan(ctx *CommandContext, assignmentFields []string, deferredAssignments []AssignmentNode) (*TranslationResult, error) {
	plan := ctx.Plan
	source := plan.SourceStage()
	opts := ctx.Opts

	// --- Special query modes (generate entirely different SQL) ---
	if plan.IsTraversal {
		if plan.IsAggregated {
			return nil, fmt.Errorf("%s() cannot be combined with aggregation functions", plan.TraversalMode)
		}
		if plan.IsChain {
			return nil, fmt.Errorf("%s() cannot be combined with chain()", plan.TraversalMode)
		}
		return buildTraversalSQL(
			plan.TraversalMode, plan.TraversalChild, plan.TraversalParent, plan.TraversalStart,
			plan.TraversalDepth, plan.TraversalInclude,
			source.Layer.Where,
			selectExprStrings(source.Layer.Selects),
			source.Layer.OrderBy,
			source.Layer.Limit,
			source.Layer.Having,
			plan.ChartType, plan.ChartConfig, opts, plan.HasTableCmd,
		)
	}
	if plan.IsAnalyze {
		return buildAnalyzeFieldsSQL(
			plan.AnalyzeFieldsList, plan.AnalyzeFieldsScanLimit,
			source.Layer.Where, source.Layer.Having,
			source.Layer.OrderBy, source.Layer.Limit,
			plan.ChartType, plan.ChartConfig, opts,
		)
	}

	// --- Assemble SELECT for the active (last) stage ---
	// For multi-stage pipelines, earlier stages were already assembled when
	// the groupby handler pushed a new stage. We only need to assemble the
	// final stage here.
	activeStage := plan.CurrentStage()
	if len(activeStage.Layer.GroupBy) > 0 {
		if err := assembleGroupBySelects(ctx, activeStage, assignmentFields); err != nil {
			return nil, err
		}
	} else if activeStage.IsSource {
		assembleNonGroupBySelects(ctx, activeStage, assignmentFields)
	}

	// --- Set default ORDER BY and LIMIT ---
	if len(activeStage.Layer.OrderBy) == 0 && len(activeStage.Layer.GroupBy) == 0 && !plan.IsAggregated {
		if activeStage.IsSource {
			activeStage.Layer.OrderBy = []string{"timestamp DESC"}
		}
	}
	if activeStage.Layer.Limit == "" && opts.MaxRows > 0 {
		activeStage.Layer.Limit = fmt.Sprintf("LIMIT %d", opts.MaxRows)
	}

	// --- Defer window-field ORDER BY to post-window layer ---
	deferWindowOrderBy(ctx, plan, activeStage)

	// --- Build chained aggregation stages ---
	if len(plan.outerAggregations) > 0 {
		buildChainedAggStages(plan)
	}

	// --- Build formatters and compute field order ---
	selectStrings := selectExprStrings(activeStage.Layer.Selects)
	var fieldOrder []string

	if len(plan.outerAggregations) > 0 {
		// Chained aggregation: no formatter wrapping, field order from outer agg
		fieldOrder = plan.outerAggFieldOrder
	} else {
		// Standard: build outer SELECT with timestamp formatting + deferred math
		plan.Formatters = buildFormatters(selectStrings, ctx.Registry, deferredAssignments)
		fieldOrder = computeFieldOrder(selectStrings, deferredAssignments)
	}

	// --- Join: skip explicit formatter to let all join columns pass through ---
	if plan.IsJoin && plan.JoinSubSQL != "" {
		// The join wrapper adds columns from the subquery. An explicit formatter
		// SELECT would drop them. Use SELECT * with timestamp formatting instead.
		plan.Formatters = nil
	}

	// --- Build z-score window layers ---
	if plan.ModifiedZScoreExpr != "" {
		buildZScoreWindowLayers(plan)
		fieldOrder = append(fieldOrder, "_median", "_mad", "_modified_z")
		if plan.OutlierThreshold != "" {
			fieldOrder = append(fieldOrder, "_is_outlier")
		}
	}

	// --- Build histogram window layers ---
	if plan.HistogramBuckets > 0 {
		buildHistogramLayers(plan, ctx)
		fieldOrder = []string{"_bin_lower", "_bin_upper", "_bin_count"}
		plan.IsAggregated = true
	}

	// --- Render SQL ---
	sql, err := plan.Render(opts)
	if err != nil {
		return nil, err
	}

	return &TranslationResult{
		SQL:          sql,
		FieldOrder:   fieldOrder,
		IsAggregated: plan.IsAggregated || len(activeStage.Layer.GroupBy) > 0,
		ChartType:    plan.ChartType,
		ChartConfig:  plan.ChartConfig,
	}, nil
}

// assembleGroupBySelects builds the source SELECT for GROUP BY queries using
// the FieldRegistry to identify aggregation fields instead of string matching
// on SQL function names.
func assembleGroupBySelects(ctx *CommandContext, source *QueryStage, assignmentFields []string) error {
	existingSelects := selectExprStrings(source.Layer.Selects)

	// Index existing selects by alias and by expression prefix
	existingByAlias := make(map[string]string)
	for _, sel := range existingSelects {
		alias := extractFieldAlias(sel)
		existingByAlias[alias] = sel
	}

	var selects []string
	addedAliases := make(map[string]bool)

	// 1. Add grouped field expressions
	for i, gf := range source.Layer.GroupBy {
		fieldName := extractFieldName(gf)

		// Check if an existing SELECT already produces this field (by alias)
		if existing, ok := existingByAlias[fieldName]; ok {
			selects = append(selects, existing)
			addedAliases[fieldName] = true
			// Update GroupBy to use the alias for cleaner SQL
			source.Layer.GroupBy[i] = fieldName
			continue
		}

		// Check if an existing SELECT matches by expression prefix (e.g., "expr AS alias")
		found := false
		for _, sel := range existingSelects {
			if strings.HasPrefix(sel, gf+" AS ") {
				alias := extractFieldAlias(sel)
				selects = append(selects, sel)
				addedAliases[alias] = true
				source.Layer.GroupBy[i] = alias
				found = true
				break
			}
		}
		if found {
			continue
		}

		// Check registry for per-row transforms or assignments registered during Execute
		if entry := ctx.Registry.Get(gf); entry != nil && (entry.Kind == FieldKindPerRow || entry.Kind == FieldKindAssignment) {
			safeName, _ := sanitizeIdentifier(gf)
			resolveExpr := ctx.Registry.Resolve(gf)
			selects = append(selects, fmt.Sprintf("%s AS %s", resolveExpr, safeName))
			addedAliases[safeName] = true
			source.Layer.GroupBy[i] = safeName
			continue
		}

		// Generate SELECT from raw field reference
		quotedName, err := sanitizeIdentifier(fieldName)
		if err != nil {
			return fmt.Errorf("groupBy: %w", err)
		}
		selects = append(selects, fmt.Sprintf("%s AS %s", gf, quotedName))
		addedAliases[quotedName] = true
		source.Layer.GroupBy[i] = quotedName
	}

	// 2. Add aggregation selects: first check registry/aggregationOutputs,
	// then keep remaining non-per-row selects as a fallback for handlers
	// (table, bucket, heatmap, etc.) that add aggregation selects directly
	// without registering them in the registry.
	hasExplicitAgg := false
	for _, sel := range existingSelects {
		alias := extractFieldAlias(sel)
		if addedAliases[alias] {
			continue
		}
		_, inAggOutputs := ctx.Plan.aggregationOutputs[alias]
		if inAggOutputs || ctx.Registry.IsAggregate(alias) || !ctx.Registry.IsPerRow(alias) {
			selects = append(selects, sel)
			addedAliases[alias] = true
			hasExplicitAgg = true
		}
	}

	// 3. Handle chained aggregation dependencies
	if len(ctx.Plan.outerAggregations) > 0 {
		outerProduced := make(map[string]bool)
		for _, f := range ctx.Plan.outerAggFieldOrder {
			outerProduced[f] = true
		}
		for name, expr := range ctx.Plan.aggregationOutputs {
			if outerProduced[name] || addedAliases[name] {
				continue
			}
			for _, outerExpr := range ctx.Plan.outerAggregations {
				if strings.Contains(outerExpr, name) {
					selects = append(selects, fmt.Sprintf("%s AS %s", expr, name))
					addedAliases[name] = true
					hasExplicitAgg = true
					break
				}
			}
		}
	}

	// 4. Default COUNT(*) if no explicit aggregation found
	if !hasExplicitAgg {
		if !ctx.Plan.IsAggregated || len(ctx.Plan.outerAggregations) > 0 {
			selects = append(selects, "COUNT(*) AS _count")
		}
	}

	// 5. Assignment fields
	for _, af := range assignmentFields {
		if !contains(selects, af) {
			selects = append(selects, af)
		}
	}

	// Update source stage selects
	source.Layer.Selects = nil
	for _, s := range selects {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: s})
	}
	return nil
}

// assembleNonGroupBySelects handles SELECT assembly for queries without GROUP BY.
func assembleNonGroupBySelects(ctx *CommandContext, source *QueryStage, assignmentFields []string) {
	plan := ctx.Plan

	// No commands and no assignments: use default field set
	if len(ctx.Pipeline.Commands) == 0 && len(ctx.Pipeline.Assignments) == 0 {
		// Alert queries: project only referenced fields + log_id + timestamp
		if ctx.Opts.UseIngestTimestamp && ctx.Pipeline.Filter != nil {
			fields := collectConditionFields(ctx.Pipeline.Filter.Conditions)
			collectHavingConditionFields(ctx.Pipeline.HavingConditions, fields)
			// Include alert-configured extra fields (throttle field, template fields)
			for _, f := range ctx.Opts.AlertExtraFields {
				if f != "" {
					fields[f] = true
				}
			}
			if !fields["*"] {
				for _, base := range []string{"raw_log", "timestamp", "log_id", "fractal_id", "ingest_timestamp"} {
					delete(fields, base)
				}
				source.Layer.Selects = []SelectExpr{
					{Expr: "timestamp"},
					{Expr: "log_id"},
					{Expr: "fractal_id"},
				}
				for field := range fields {
					safe := fmt.Sprintf("%s AS `%s`", jsonFieldRef(field), field)
					source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: safe})
				}
				return
			}
		}
		source.Layer.Selects = []SelectExpr{
			{Expr: "timestamp"}, {Expr: "raw_log"}, {Expr: "log_id"}, {Expr: "toString(fields) AS fields"}, {Expr: "fractal_id"},
		}
		return
	}

	// No commands but has assignments
	if len(ctx.Pipeline.Commands) == 0 && len(ctx.Pipeline.Assignments) > 0 {
		source.Layer.Selects = []SelectExpr{{Expr: "timestamp"}, {Expr: "raw_log"}, {Expr: "log_id"}, {Expr: "fractal_id"}}
		for _, af := range assignmentFields {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: af})
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "toString(fields) AS fields"})
		return
	}

	// When the query is aggregated (e.g. | len(raw_log) | sum(_len)),
	// per-row computed fields like _len or _time are already inlined into the
	// aggregate expressions. Strip them from the SELECT to avoid mixing
	// non-aggregated columns with aggregate functions.
	if plan.IsAggregated {
		filtered := source.Layer.Selects[:0]
		for _, sel := range source.Layer.Selects {
			alias := extractFieldAlias(sel.String())
			if alias != "" && ctx.Registry.IsPerRowOrAssignment(alias) {
				if _, isAgg := plan.aggregationOutputs[alias]; !isAgg {
					continue
				}
			}
			filtered = append(filtered, sel)
		}
		source.Layer.Selects = filtered
	}

	// Has commands: add _all_fields for table commands, assignment fields
	if plan.HasTableCmd && len(source.Layer.Selects) > 0 {
		hasFieldsMap := false
		for _, sel := range source.Layer.Selects {
			if strings.Contains(sel.String(), "_all_fields") {
				hasFieldsMap = true
				break
			}
		}
		if !hasFieldsMap {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "toString(fields) AS _all_fields"})
		}
	}

	// Add assignment fields
	existingSelects := selectExprStrings(source.Layer.Selects)
	for _, af := range assignmentFields {
		if !contains(existingSelects, af) {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: af})
		}
	}

	// Ensure base fields for non-aggregated queries
	if !plan.IsAggregated {
		ensureSelectExpr := func(name string) {
			for _, sel := range source.Layer.Selects {
				if extractFieldAlias(sel.String()) == name {
					return
				}
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: name})
		}
		ensureSelectExpr("timestamp")
		ensureSelectExpr("log_id")
		ensureSelectExpr("raw_log")

		hasFields := false
		for _, sel := range source.Layer.Selects {
			alias := extractFieldAlias(sel.String())
			if alias == "fields" || strings.Contains(sel.String(), "_all_fields") {
				hasFields = true
				break
			}
		}
		if !hasFields {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "toString(fields) AS fields"})
		}
	}
}

// deferWindowOrderBy moves ORDER BY clauses that reference window fields
// (z-score, outlier) from the source stage to DeferredOrder, using the
// FieldRegistry to identify window fields.
func deferWindowOrderBy(ctx *CommandContext, plan *QueryPlan, source *QueryStage) {
	if plan.ModifiedZScoreExpr == "" || len(source.Layer.OrderBy) == 0 {
		return
	}

	var innerOrder []string
	var deferredOrder []string

	for _, ob := range source.Layer.OrderBy {
		// Extract field name (strip direction suffix)
		fieldName := strings.Fields(ob)[0]
		if ctx.Registry.IsWindow(fieldName) {
			deferredOrder = append(deferredOrder, ob)
		} else {
			innerOrder = append(innerOrder, ob)
		}
	}

	source.Layer.OrderBy = innerOrder
	if len(deferredOrder) > 0 {
		plan.DeferredOrder = append(plan.DeferredOrder, deferredOrder...)
		plan.DeferredLimit = source.Layer.Limit
		source.Layer.Limit = ""
	}
}

// buildChainedAggStages creates additional QueryStages for chained aggregation
// (e.g., sum/avg/etc. operating on prior aggregation outputs).
func buildChainedAggStages(plan *QueryPlan) {
	// If MAD window is needed, insert an intermediate stage for the median computation
	if plan.MADWindowExpr != "" {
		plan.Stages = append(plan.Stages, QueryStage{
			Layer: QueryLayer{
				Selects: []SelectExpr{
					{Expr: fmt.Sprintf("*, median(%s) OVER () AS _median_val", plan.MADWindowExpr)},
				},
			},
		})
	}

	// Add the outer aggregation stage
	var outerSelects []SelectExpr
	for _, expr := range plan.outerAggregations {
		outerSelects = append(outerSelects, SelectExpr{Expr: expr})
	}
	plan.Stages = append(plan.Stages, QueryStage{
		Layer: QueryLayer{Selects: outerSelects},
	})
}

// buildZScoreWindowLayers creates window layers for modified z-score computation:
// Layer 1: median(expr) OVER () AS _median
// Layer 2: median(abs(expr - _median)) OVER () AS _mad
// Layer 3: z-score calculation + optional outlier flag
func buildZScoreWindowLayers(plan *QueryPlan) {
	expr := plan.ModifiedZScoreExpr

	// Layer 1: median
	plan.WindowLayers = append(plan.WindowLayers, QueryLayer{
		Selects: []SelectExpr{{Expr: fmt.Sprintf("*, median(%s) OVER () AS _median", expr)}},
	})

	// Layer 2: MAD (median absolute deviation)
	plan.WindowLayers = append(plan.WindowLayers, QueryLayer{
		Selects: []SelectExpr{{Expr: fmt.Sprintf("*, median(abs(%s - _median)) OVER () AS _mad", expr)}},
	})

	// Layer 3: modified z-score + optional outlier
	outlierCol := ""
	if plan.OutlierThreshold != "" {
		outlierCol = fmt.Sprintf(", toString(if(abs(_modified_z) > %s, 1, 0)) AS _is_outlier", plan.OutlierThreshold)
	}
	plan.WindowLayers = append(plan.WindowLayers, QueryLayer{
		Selects: []SelectExpr{{Expr: fmt.Sprintf(
			"*, if(_mad = 0, 0, round(0.6745 * (%s - _median) / _mad, 4)) AS _modified_z%s",
			expr, outlierCol,
		)}},
	})
}

// buildHistogramLayers creates window layers for histogram bucketing:
// Layer 1: compute val, min, max using window functions
// Layer 2: bucket, aggregate, and order
func buildHistogramLayers(plan *QueryPlan, ctx *CommandContext) {
	computedFields := ctx.Registry.AllComputed()

	var valExpr string
	if _, ok := computedFields["_hist_val"]; ok {
		valExpr = "_hist_val"
	} else {
		valExpr = fmt.Sprintf("toFloat64OrNull(toString(%s))", plan.HistogramField)
	}

	buckets := plan.HistogramBuckets
	bucketExpr := fmt.Sprintf(
		"least(toUInt32(floor((_val - _min_val) / nullIf(_max_val - _min_val, 0) * %d)), %d)",
		buckets, buckets-1,
	)
	lowerExpr := fmt.Sprintf("round(_min_val + _bucket * (_max_val - _min_val) / %d, 4)", buckets)
	upperExpr := fmt.Sprintf("round(_min_val + (_bucket + 1) * (_max_val - _min_val) / %d, 4)", buckets)

	// Layer 1: window functions for val, min, max
	plan.WindowLayers = append(plan.WindowLayers, QueryLayer{
		Selects: []SelectExpr{{Expr: fmt.Sprintf(
			"%s AS _val, min(%s) OVER () AS _min_val, max(%s) OVER () AS _max_val",
			valExpr, valExpr, valExpr,
		)}},
	})

	// Layer 2: bucketing with GROUP BY
	plan.WindowLayers = append(plan.WindowLayers, QueryLayer{
		Selects: []SelectExpr{
			{Expr: fmt.Sprintf("%s AS _bin_lower", lowerExpr)},
			{Expr: fmt.Sprintf("%s AS _bin_upper", upperExpr)},
			{Expr: "count(*) AS _bin_count"},
		},
		Where:   []string{"_val IS NOT NULL"},
		GroupBy: []string{fmt.Sprintf("%s AS _bucket", bucketExpr), "_min_val", "_max_val"},
		OrderBy: []string{"_bin_lower ASC"},
	})
}

// buildFormatters creates the outer SELECT expressions for timestamp formatting
// and deferred math assignments.
func buildFormatters(selectFields []string, registry *FieldRegistry, deferredAssignments []AssignmentNode) []SelectExpr {
	if len(selectFields) == 0 {
		return []SelectExpr{
			{Expr: "formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') as timestamp"},
			{Expr: "raw_log"},
			{Expr: "fields"},
		}
	}

	var formatters []SelectExpr
	for _, field := range selectFields {
		alias := extractFieldAlias(field)
		if alias == "timestamp" {
			formatters = append(formatters, SelectExpr{Expr: "formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') as timestamp"})
		} else if alias != "" {
			formatters = append(formatters, SelectExpr{Expr: alias})
		}
	}

	// Add deferred math assignments to the outer SELECT
	if len(deferredAssignments) > 0 {
		// Ensure select field aliases are known to the registry for math expression resolution
		for _, field := range selectFields {
			alias := extractFieldAlias(field)
			if alias != "" && !registry.Has(alias) {
				registry.SetResolveExpr(alias, alias)
			}
		}
		for _, da := range deferredAssignments {
			safeName, _ := sanitizeIdentifier(da.Field)
			sqlExpr := convertMathExprToSQL(da.Expression, registry)
			formatters = append(formatters, SelectExpr{Expr: fmt.Sprintf("%s AS %s", sqlExpr, safeName)})
		}
	}

	return formatters
}

// computeFieldOrder extracts the field order from SELECT expressions.
func computeFieldOrder(selectFields []string, deferredAssignments []AssignmentNode) []string {
	fieldOrder := make([]string, 0, len(selectFields))
	for _, field := range selectFields {
		alias := extractFieldAlias(field)
		if alias != "_all_fields" && alias != "raw_log" && alias != "log_id" {
			fieldOrder = append(fieldOrder, strings.Trim(alias, "`"))
		}
	}
	for _, da := range deferredAssignments {
		fieldOrder = append(fieldOrder, da.Field)
	}
	return fieldOrder
}

// selectExprStrings converts SelectExpr slice to string slice.
func selectExprStrings(exprs []SelectExpr) []string {
	result := make([]string, len(exprs))
	for i, e := range exprs {
		result[i] = e.String()
	}
	return result
}
