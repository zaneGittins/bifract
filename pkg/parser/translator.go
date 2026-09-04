package parser

import (
	"fmt"
	"strings"
	"time"
)

// AnalyticsModelInfo is a lightweight representation of an analytics model used
// in QueryOptions for BQL model_lookup() command execution.
type AnalyticsModelInfo struct {
	ID         string
	TableName  string // distributed table name in cluster mode, local otherwise
	ModelType  string // "rarity", "first_seen", or "volume_baseline"
	MinSample  int
	TimeBucket string // volume_baseline bucket granularity ("day"/"hour")
	FractalID  string
	// Distributed reports that TableName is a Distributed table (cluster mode). The
	// model join and its strict-mode prefilter must then be GLOBAL: a non-GLOBAL
	// subquery over a Distributed table inside a query that already reads
	// logs_distributed is rejected outright (distributed_product_mode='deny').
	Distributed bool
}

type QueryOptions struct {
	StartTime             time.Time
	EndTime               time.Time
	EndExclusive          bool // emit "timestamp < EndTime" instead of "<="; lets adjacent chunks abut without dropping or duplicating a row
	MaxRows               int
	FractalID             string                        // Fractal UUID for filtering logs to specific fractal
	FractalIDs            []string                      // Multiple fractal UUIDs (prism context); overrides FractalID when set
	IncludeEmptyFractalID bool                          // Include logs with no fractal_id (legacy data) when querying default fractal
	Dictionaries          map[string]map[string]string  // dict name -> key col -> ClickHouse lookup name
	Models                map[string]AnalyticsModelInfo // model name -> ModelInfo for model_lookup() BQL command
	HasCommentFilter      bool                          // True when query uses comment() and log_ids have been pre-fetched
	CommentLogIDs         []string                      // Pre-fetched log_ids from PostgreSQL for comment() filtering
	UseIngestTimestamp    bool                          // Filter on ingest_timestamp instead of timestamp (used by alerts)
	AlertExtraFields      []string                      // Additional fields to project in alert auto-projection (throttle field, template fields)
	GeoIPEnabled          bool                          // True when MaxMind GeoLite2 dictionaries are loaded
	DictionaryDatabase    string                        // ClickHouse database holding the dictionary objects; qualifies every dictGet
	TableName             string                        // Override source table (default "logs", use "logs_distributed" in cluster mode)
	ProcLineageTable      string                        // Process-lineage read table for ptg() ("proc_lineage" or "proc_lineage_distributed")
	ProcFreqTable         string                        // Frequency-baseline read table for pgr() ("proc_freq" or "proc_freq_distributed")
	ProcEdgesTable        string                        // Edge-rollup read table for pgr() leaf edges ("process_edges" or "process_edges_distributed")
	IncludeShardNum       bool                          // Include _shard_num virtual column for direct-shard detail lookup (cluster mode only)
	SourceMode            SourceMode                    // Hot (default, JSON logs) vs Iceberg (MAP archive); gates iceberg field-access codegen
	// IcePromoted lists the field names whose `_ice_` promoted column exists on
	// the Iceberg table this query targets. Iceberg mode only. Leave nil when the
	// target table's schema is unknown: pruning is skipped, results stay correct.
	// Supplying names absent from the table makes ClickHouse fail the query with
	// UNKNOWN_IDENTIFIER, so this must reflect the table, not the current build's
	// default promoted set. See icebergEqualityPredicate.
	IcePromoted map[string]bool
	// SourceSubquery makes the pipeline read FROM a pre-built SQL subquery instead of the
	// logs table. Used to compose downstream BQL (filter/aggregate/sort/table) on top of a
	// pgr() scored edge list: the two-pass pgr SQL becomes the source and its flat output
	// columns (SourceColumns) resolve as bare columns. Base time/fractal conditions are NOT
	// re-applied (the subquery already scoped them).
	SourceSubquery       string
	SourceColumns        []string // flat column names exposed by SourceSubquery (resolve bare, not fields.x)
	SourceNumericColumns []string // subset of SourceColumns that are already numeric (no string-coercion on compare)
	// SourceOrderBy overrides the implicit "timestamp DESC" default ORDER BY applied to a bare
	// (no explicit sort()) query over SourceSubquery. Set by a source resolver (e.g. pgr(), via
	// ProvenanceOrderBy) whose subquery rows are already meaningfully pre-ordered -- timestamp
	// ordering would discard that and, combined with a LIMIT, truncate the wrong rows.
	SourceOrderBy []string
	// DisplayTimezone is the IANA zone that calendar-aligned time buckets
	// (bucket(), timechart) snap to. Empty means UTC, which is what an alert,
	// an API-key request and any headless path get: those have no viewer, so
	// their boundaries must not move. Owned by the running user for an ad-hoc
	// query and by the dashboard for a dashboard widget.
	DisplayTimezone string
}

// sourceColumnSelects renders a subquery source's flat columns as a default SELECT list.
func sourceColumnSelects(cols []string) []SelectExpr {
	out := make([]SelectExpr, 0, len(cols))
	for _, c := range cols {
		out = append(out, SelectExpr{Expr: c})
	}
	return out
}

// EffectiveTableName returns the table name to query, defaulting to "logs".
func (o QueryOptions) EffectiveTableName() string {
	if o.SourceSubquery != "" {
		return "(" + o.SourceSubquery + ") AS pgr_src"
	}
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
	// DefaultTimeOrder is true when the translator applied its implicit
	// "timestamp DESC" ordering (no user ORDER BY / GROUP BY / aggregation).
	// Such queries emit rows newest-first and can be progressively streamed.
	DefaultTimeOrder bool
	// TimeScopedSubquery is true when the SQL embeds a subquery that carries its
	// own copy of the query's time bounds (join / model_lookup). Re-translating
	// such a query over a narrower window also narrows that subquery, which drops
	// rows whose join partner sits outside the window, so the caller must not
	// slice the range.
	TimeScopedSubquery bool
	// Chain is set for chain() queries, describing how to fetch the events behind
	// each matched sequence. Nil otherwise.
	Chain *ChainMeta
}

func TranslateToSQL(pipeline *PipelineNode, opts QueryOptions) (string, error) {
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

func TranslateToSQLWithOrder(pipeline *PipelineNode, opts QueryOptions) (*TranslationResult, error) {
	if opts.SourceMode == SourceIceberg {
		if err := icebergSupportedFeatures(pipeline); err != nil {
			return nil, err
		}
	}
	registry := NewFieldRegistry(opts.SourceMode, opts.IcePromoted)
	// A subquery source (a resolved source command) exposes flat columns; register them so
	// bare references resolve to the column name (not fields.`x` JSON access). Numeric columns
	// register as Assignment (already numeric -> no toFloat64OrZero coercion, which errors on a
	// non-String); string columns as Base (coerce on numeric compare, harmless for strings).
	if opts.SourceSubquery != "" {
		numeric := make(map[string]bool, len(opts.SourceNumericColumns))
		for _, c := range opts.SourceNumericColumns {
			numeric[c] = true
		}
		for _, c := range opts.SourceColumns {
			if numeric[c] {
				// Assignment kind = already numeric (no coercion); SetResolveExpr pins the bare
				// column so Resolve returns it directly instead of falling through to fields.`x`.
				registry.Register(c, FieldKindAssignment, c, -1)
				registry.SetResolveExpr(c, c)
			} else {
				registry.Register(c, FieldKindBase, c, -1)
			}
		}
	}
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
		// Condition functions used as boolean operands compile to SQL here, before
		// the tree is materialized.
		if err := resolveCommandConditionNodes(pipeline.Filter.Conditions, opts); err != nil {
			return nil, err
		}
		// Pass the (base-fields-only) registry so filter conditions pick up the
		// source mode. In hot mode this is behavior-identical to the old nil arg
		// (no computed fields are declared yet); in iceberg mode it routes field
		// refs to MAP access + the promoted-column predicate.
		whereSQL, err := buildWhereClauseCtx(pipeline.Filter.Conditions, registry)
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

	// model_lookup() placement: with an aggregation (groupby/chain/stats) AFTER it,
	// the model join must attach to the row scan so the aggregation sees model
	// columns per-row. With aggregation only before it (or none at all), the join
	// stays outermost, where it touches only the final rows. Decided before Execute
	// so chain/groupby handlers can route model-column references accordingly.
	if mlIdx := commandIndex(pipeline.Commands, "model_lookup"); mlIdx >= 0 {
		aggBefore, aggAfter := false, false
		for i, cmd := range pipeline.Commands {
			if aggregatingCommandNames[cmd.Name] || cmd.Name == "chain" {
				if i < mlIdx {
					aggBefore = true
				} else if i > mlIdx {
					aggAfter = true
				}
			}
		}
		plan.ModelLookupAtScan = aggAfter && !aggBefore
	}

	// Register assignment fields
	for _, assignment := range pipeline.Assignments {
		registry.Register(assignment.Field, FieldKindAssignment, assignment.Field, -1)
	}

	// ---------------------------------------------------------------
	// 5. Process field assignments
	// ---------------------------------------------------------------
	var assignmentFields []string
	var deferredAssignments []AssignmentNode
	// postAggByPos holds post-aggregation math assignments keyed by the number of
	// commands that precede them, so the Execute loop can materialize each as a
	// stage at its pipeline position (see applyAssignmentStage).
	postAggByPos := make(map[int][]AssignmentNode)
	firstAgg := firstAggregatingCommandIndex(pipeline.Commands)
	hasAggregation := firstAgg < len(pipeline.Commands)
	for _, assignment := range pipeline.Assignments {
		safeField, err := sanitizeIdentifier(assignment.Field)
		if err != nil {
			return nil, fmt.Errorf("invalid assignment field: %w", err)
		}

		expr := assignment.Expression
		isMathExpr := assignment.ExpressionType == TokenValue &&
			(strings.ContainsAny(expr, "+*/()") || strings.Contains(expr, " - ") || strings.Contains(expr, " -") || strings.Contains(expr, "- "))
		if isMathExpr {
			switch {
			case hasAggregation && assignment.CmdIndex <= firstAgg:
				// Pre-aggregation: inline so aggregations/filters reference the
				// computed value, and so later pre-aggregation assignments
				// referencing this one fold in the full expression (it is never
				// materialized as a column).
				sqlExpr := convertMathExprToSQL(expr, registry, assignment.Field)
				registry.RegisterInlineExpr(safeField, sqlExpr, -1)
			case hasAggregation:
				// Post-aggregation: materialized as a stage at its pipeline position
				// (not the outermost formatter) so commands after it -- table, sort,
				// case -- can reference it even when they drop other columns.
				postAggByPos[assignment.CmdIndex] = append(postAggByPos[assignment.CmdIndex], assignment)
			default:
				// No aggregation in the pipeline: computed in the outer formatter.
				deferredAssignments = append(deferredAssignments, assignment)
			}
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
				// Materialized as a column and referenced downstream (possibly in
				// GROUP BY via its alias); cast raw JSON refs to ::String so a
				// Dynamic-stored path stays groupable.
				expression = groupableCast(jsonFieldRef(assignment.Expression))
			}
		case TokenFunction:
			expression = fmt.Sprintf("'%s'", escapeString(assignment.Expression))
		default:
			if assignment.Expression == "timestamp" {
				expression = "timestamp"
			} else {
				expression = groupableCast(jsonFieldRef(assignment.Expression))
			}
		}

		af := fmt.Sprintf("%s AS %s", expression, safeField)
		assignmentFields = append(assignmentFields, af)
		ctx.Registry.SetResolveExpr(safeField, af)
	}

	// ---------------------------------------------------------------
	// 6. EXECUTE PHASE: every command reads registry, writes to plan.
	//     Post-aggregation math assignments are materialized as stages at their
	//     pipeline position (between the commands they sit between), so a later
	//     projection (e.g. table) cannot strand the columns they depend on.
	// ---------------------------------------------------------------
	for i := 0; i <= len(pipeline.Commands); i++ {
		for _, assignment := range postAggByPos[i] {
			if err := applyAssignmentStage(ctx, assignment); err != nil {
				return nil, err
			}
		}
		if i == len(pipeline.Commands) {
			break
		}
		ctx.CmdIndex = i
		cmd := pipeline.Commands[i]
		// A per-row transform that runs on a GROUP BY stage would add a
		// non-grouped column to an aggregate SELECT (invalid, silently dropped).
		// Move it onto a post-aggregation projection stage first.
		if transformCommandNames[cmd.Name] && len(ctx.Plan.CurrentStage().Layer.GroupBy) > 0 {
			if _, err := pushCarryForwardStage(ctx); err != nil {
				return nil, fmt.Errorf("%s (post-aggregation stage): %w", cmd.Name, err)
			}
		}
		handler := getCommandHandler(cmd.Name)
		if err := handler.Execute(cmd, ctx); err != nil {
			return nil, err
		}
	}

	// ---------------------------------------------------------------
	// 6b. CONDITION CLASSIFICATION + MATERIALIZATION.
	//     Classification runs AFTER Execute so the registry fully reflects every
	//     produced field: aggregate aliases from multi()/function=, and aggregates
	//     redefined across multi-stage groupby boundaries. Routing a post-pipe
	//     filter (WHERE vs HAVING) by Declare-time kinds alone misroutes those
	//     (e.g. _avg from multi() would land in WHERE). Materialization then emits
	//     SQL from the now-complete registry and binds HAVING to its proper stage.
	// ---------------------------------------------------------------
	if err := resolveCommandConditions(pipeline.HavingConditions, opts); err != nil {
		return nil, err
	}
	classifyConditions(pipeline.HavingConditions, registry, plan)
	materializeConditions(registry, plan)

	// ---------------------------------------------------------------
	// 7. FINALIZE: assemble final SQL from structured plan data
	// ---------------------------------------------------------------
	return finalizePlan(ctx, assignmentFields, deferredAssignments)
}

// HistogramIsScopeOnly reports whether a histogram over this pipeline would be
// filtered by nothing but time range and fractal scope. Such a histogram counts
// every log in the window, which the pre-aggregated per-minute rollup already
// knows, so the caller can read it from there instead of scanning the logs table.
// Anything that narrows the count (a user filter, comment(), a computed-column
// filter) or moves it off the timestamp axis makes the rollup inapplicable.
func HistogramIsScopeOnly(pipeline *PipelineNode, opts QueryOptions) bool {
	if opts.HasCommentFilter || opts.UseIngestTimestamp || opts.SourceSubquery != "" {
		return false
	}
	if pipeline.Filter != nil && len(pipeline.Filter.Conditions) > 0 {
		return false
	}
	// Mirrors BuildHistogramSQL: the computed-column pass only matters when there
	// are having conditions to inline, and it is too costly to run otherwise.
	return len(pipeline.HavingConditions) == 0 || histogramComputedWhere(pipeline, opts) == ""
}

// BuildHistogramSQL generates a lightweight COUNT(*) GROUP BY time-bucket query.
// It applies the base filter conditions and, when the pipeline contains computed
// column commands (e.g. len()), also inlines their downstream filters (e.g. _len > 500)
// into the WHERE clause so the histogram count matches the results table.
func BuildHistogramSQL(pipeline *PipelineNode, opts QueryOptions, bucketSeconds int) (string, error) {
	plan := NewQueryPlan()
	addBaseConditions(plan, opts)

	// Apply user's filter conditions (the WHERE portion of their BQL query)
	if pipeline.Filter != nil {
		whereSQL, err := buildWhereClause(pipeline.Filter.Conditions)
		if err != nil {
			return "", err
		}
		if whereSQL != "" {
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, whereSQL)
		}
	}

	// Apply comment() filter if present in the pipeline
	if opts.HasCommentFilter {
		if len(opts.CommentLogIDs) == 0 {
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, "1 = 0")
		} else {
			quoted := make([]string, len(opts.CommentLogIDs))
			for i, id := range opts.CommentLogIDs {
				quoted[i] = fmt.Sprintf("'%s'", escapeString(id))
			}
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where,
				fmt.Sprintf("log_id IN (%s)", strings.Join(quoted, ", ")))
		}
	}

	// Inline computed-column filters from HavingConditions into the histogram WHERE.
	// Only FieldKindAssignment and FieldKindPerRow conditions are included; aggregate
	// and window conditions reference outputs that don't exist in the flat histogram
	// query and are intentionally skipped.
	if len(pipeline.HavingConditions) > 0 {
		if computedWhere := histogramComputedWhere(pipeline, opts); computedWhere != "" {
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, computedWhere)
		}
	}

	where := strings.Join(plan.SourceStage().Layer.Where, " AND ")
	tbl := opts.EffectiveTableName()

	tsCol := "timestamp"
	if opts.UseIngestTimestamp {
		tsCol = "ingest_timestamp"
	}

	sql := fmt.Sprintf(
		"SELECT toStartOfInterval(%s, INTERVAL %d SECOND) AS bucket, count() AS cnt FROM %s WHERE %s GROUP BY bucket ORDER BY bucket",
		tsCol, bucketSeconds, tbl, where,
	)
	return sql, nil
}

// FieldStatsParams configures the sampled field-statistics aggregation.
type FieldStatsParams struct {
	SampleSize int // max rows scanned (a bounded, most-recent superset of the results window)
	TopN       int // top values reported per field
	MaxFields  int // cap on the number of distinct fields returned
	ValueLen   int // per-value character cap (payload safety for long values)
}

// FieldSampleParams configures BuildFieldSampleSQL.
type FieldSampleParams struct {
	Table      string // logs or logs_distributed
	Where      string // caller-built predicate; must prune to a bounded, recent slice
	SampleSize int
	TopN       int
	MaxFields  int
	ValueLen   int
}

// fieldSampleTopKReserve is approx_top_k's counter capacity per field. It is a
// per-group, per-thread allocation of that many values (each up to ValueLen
// bytes), so it multiplies by the number of fields sampled: at 2048 a wide
// schema exceeded the sweep's memory budget on its own. 256 still counts the
// low-cardinality fields exactly, and anything wider is reported as approximate
// via the error bound rather than being worth the state.
const fieldSampleTopKReserve = 256

// BuildFieldSampleSQL builds the schema sweep's per-field distribution query.
//
// It answers the same question as BuildFieldStatsSQL but under different
// constraints, which is why it is a separate builder rather than a parameter:
//
//  1. No ORDER BY. Recency comes from the caller's ingest_timestamp predicate,
//     which prunes partitions. `ORDER BY <ts> DESC LIMIT n` does not read in
//     order on the logs table, so its cost scales with the rows in the window
//     rather than with n (measured: a constant LIMIT 20000 read 23k rows over a
//     1-day window and 323k over 60 days). At retention scale that reads the entire norm_log
//     column, which is what made the schema tab time out. The predicate form is
//     O(SampleSize) whatever the table holds. See also OverflowMonitor.detect,
//     which hit the same trap.
//
//  2. One GROUP BY of ~MaxFields groups. uniq and approx_top_k accumulate in
//     bounded state, replacing a GROUP BY (key, value) over millions of distinct
//     values feeding a row_number() window function.
//
// The empty-value rule is shared with BuildFieldStatsSQL and load-bearing in both:
// the `fields` JSON has typed sub-columns that serialize as "" on every row, so
// counting raw keys would report every field as 100% present. Excluding empties
// makes `present` mean "populated in N events".
//
// The caller supplies the sample size separately (a cheap count over the same
// predicate) because this query cannot carry the __rows__ sentinel: it would
// distort the single GROUP BY it exists to keep small.
func BuildFieldSampleSQL(p FieldSampleParams) string {
	if p.SampleSize <= 0 {
		p.SampleSize = 50000
	}
	if p.TopN <= 0 {
		p.TopN = 5
	}
	if p.MaxFields <= 0 {
		p.MaxFields = 500
	}
	if p.ValueLen <= 0 {
		p.ValueLen = 256
	}
	where := p.Where
	if strings.TrimSpace(where) == "" {
		where = "1 = 1"
	}
	// approx_top_k's reserved counter size is raised above TopN so the returned
	// counts are exact for the low-cardinality fields whose value distribution is
	// worth showing at all; its third tuple member is the error bound, which the
	// caller uses to mark a count as approximate rather than present it as fact.
	// The tuples are split into parallel arrays here so the result scans as plain
	// typed slices.
	return fmt.Sprintf(`SELECT
    key,
    present,
    cardinality,
    arrayMap(t -> t.1, top) AS top_values,
    arrayMap(t -> t.2, top) AS top_counts,
    arrayMap(t -> t.3, top) AS top_errors
FROM (
    SELECT
        kv.1 AS key,
        count() AS present,
        uniq(kv.2) AS cardinality,
        approx_top_k(%d, %d)(substringUTF8(kv.2, 1, %d)) AS top
    FROM (
        SELECT norm_log FROM %s WHERE %s LIMIT %d
    )
    ARRAY JOIN JSONExtractKeysAndValues(norm_log, 'String') AS kv
    WHERE kv.1 != '' AND kv.2 != ''
    GROUP BY key
    ORDER BY present DESC
    LIMIT %d
)`, p.TopN, fieldSampleTopKReserve, p.ValueLen, p.Table, where, p.SampleSize, p.MaxFields)
}

// BuildFieldStatsSQL builds a bounded, sampled aggregation reporting per-field
// coverage, cardinality, and top values for a BQL query's matched events. Like
// BuildHistogramSQL it reuses ONLY the WHERE portion of the pipeline (time range,
// fractal scope, user filters, comment() and computed-column filters); downstream
// aggregation/transform/sort commands never affect it, so the stats always describe
// the underlying events the search matched.
//
// The scan is bounded by SampleSize via an inner `ORDER BY <ts> DESC LIMIT`, so
// ClickHouse can stop early and cost stays predictable no matter how many rows
// match. The sample is the most-recent SampleSize matching events, a strict
// superset of the results table (which shows fewer, also newest-first).
//
// Fields are read from norm_log (the flat ZSTD serialization of the JSON `fields`
// column) rather than reconstructing the JSON sub-columns, then exploded with
// JSONExtractKeysAndValues. A synthetic `__rows__` entry -- one per sampled row --
// carries the exact sample size back in the same query (its `present` is the
// denominator for coverage); the caller strips it from the field list.
//
// Returns ("", nil) for source-command compositions (pgr() etc.), whose source is
// a subquery without a norm_log column: the caller treats that as "unsupported".
func BuildFieldStatsSQL(pipeline *PipelineNode, opts QueryOptions, p FieldStatsParams) (string, error) {
	if opts.SourceSubquery != "" {
		return "", nil
	}
	if p.SampleSize <= 0 {
		p.SampleSize = 50000
	}
	if p.TopN <= 0 {
		p.TopN = 10
	}
	if p.MaxFields <= 0 {
		p.MaxFields = 250
	}
	if p.ValueLen <= 0 {
		p.ValueLen = 256
	}

	plan := NewQueryPlan()
	addBaseConditions(plan, opts)

	// User's filter conditions (the WHERE portion of their BQL query).
	if pipeline.Filter != nil {
		whereSQL, err := buildWhereClause(pipeline.Filter.Conditions)
		if err != nil {
			return "", err
		}
		if whereSQL != "" {
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, whereSQL)
		}
	}

	// comment() filter, resolved to log_ids upstream (mirrors BuildHistogramSQL).
	if opts.HasCommentFilter {
		if len(opts.CommentLogIDs) == 0 {
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, "1 = 0")
		} else {
			quoted := make([]string, len(opts.CommentLogIDs))
			for i, id := range opts.CommentLogIDs {
				quoted[i] = fmt.Sprintf("'%s'", escapeString(id))
			}
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where,
				fmt.Sprintf("log_id IN (%s)", strings.Join(quoted, ", ")))
		}
	}

	// Computed-column filters (e.g. len(field) | _len > 500), same as the histogram.
	if len(pipeline.HavingConditions) > 0 {
		if computedWhere := histogramComputedWhere(pipeline, opts); computedWhere != "" {
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, computedWhere)
		}
	}

	where := strings.Join(plan.SourceStage().Layer.Where, " AND ")
	if where == "" {
		where = "1 = 1"
	}
	tbl := opts.EffectiveTableName()
	tsCol := "timestamp"
	if opts.UseIngestTimestamp {
		tsCol = "ingest_timestamp"
	}

	// Nested build:
	//   sample  -> newest SampleSize matching norm_log values (bounded scan)
	//   explode -> arrayJoin JSON keys/values, prefixed with a __rows__ sentinel
	//   perval  -> count per (key, value), EMPTY VALUES EXCLUDED
	//   ranked  -> row_number() per key by frequency (bounds top-N materialization)
	//   outer   -> present, cardinality (distinct-in-sample), top values + counts
	//
	// Empty values are dropped (kv.2 != ''): the `fields` JSON has typed sub-columns
	// that serialize as "" for every row, so counting keys would make every field
	// look 100% present. Excluding empties makes `present` mean "populated in N
	// events", makes coverage the populated fraction, and makes fields that are empty
	// across the whole sample fall out entirely (no surviving rows). The __rows__
	// sentinel carries a non-empty value ('1') so it survives that same filter and
	// still reports the exact sample size.
	sql := fmt.Sprintf(`SELECT
    key,
    sum(cnt) AS present,
    count() AS cardinality,
    groupArrayIf(val, rnk <= %d) AS top_values,
    groupArrayIf(cnt, rnk <= %d) AS top_counts
FROM (
    SELECT key, val, cnt,
           row_number() OVER (PARTITION BY key ORDER BY cnt DESC, val ASC) AS rnk
    FROM (
        SELECT kv.1 AS key, substringUTF8(kv.2, 1, %d) AS val, count() AS cnt
        FROM (
            SELECT norm_log FROM %s WHERE %s ORDER BY %s DESC LIMIT %d
        )
        ARRAY JOIN arrayConcat([('__rows__', '1')], JSONExtractKeysAndValues(norm_log, 'String')) AS kv
        WHERE kv.1 != '' AND kv.2 != ''
        GROUP BY key, val
    )
)
GROUP BY key
ORDER BY (key = '__rows__') DESC, present DESC
LIMIT %d`,
		p.TopN, p.TopN, p.ValueLen, tbl, where, tsCol, p.SampleSize, p.MaxFields+1)

	return sql, nil
}

// histogramComputedWhere returns a WHERE clause fragment that makes the histogram
// respect pipe commands that filter on computed columns (e.g. len(field) | _len > 500).
// It mirrors the main translator flow — Declare, classifyConditions, Execute,
// materializeConditions — on a throwaway plan, then reads SourceStage.Layer.Where,
// which contains two categories of condition:
//
//  1. Materialized HavingConditions of kind FieldKindAssignment or FieldKindPerRow
//     (e.g. _len > 500 resolved to length(fields.`commandline`::String) > 500).
//     Aggregate and window conditions are classified into other pending buckets
//     and do not appear here.
//
//  2. Raw WHERE conditions appended by Execute handlers directly (e.g. match()
//     with strict=true appends dictHas(...)). These are also valid histogram filters.
//
// Returns "" on any error so the histogram degrades gracefully.
func histogramComputedWhere(pipeline *PipelineNode, opts QueryOptions) string {
	registry := NewFieldRegistry(opts.SourceMode, opts.IcePromoted)
	helperPlan := NewQueryPlan()
	ctx := &CommandContext{
		Registry: registry,
		Plan:     helperPlan,
		Opts:     opts,
		Pipeline: pipeline,
	}

	// Declare phase: populate field kinds in the registry.
	for i, cmd := range pipeline.Commands {
		ctx.CmdIndex = i
		handler := getCommandHandler(cmd.Name)
		if handler == nil {
			continue
		}
		if err := handler.Declare(cmd, ctx); err != nil {
			return ""
		}
	}
	for _, a := range pipeline.Assignments {
		registry.Register(a.Field, FieldKindAssignment, a.Field, -1)
	}

	// Execute phase: populate resolve expressions via SetResolveExpr.
	for i, cmd := range pipeline.Commands {
		ctx.CmdIndex = i
		handler := getCommandHandler(cmd.Name)
		if handler == nil {
			continue
		}
		if err := handler.Execute(cmd, ctx); err != nil {
			return ""
		}
	}

	// Classify after Execute (matching the main translator ordering) so the
	// registry fully reflects every produced field kind before routing.
	classifyConditions(pipeline.HavingConditions, registry, helperPlan)

	// Materialize: generate SQL from classified conditions using the now-populated registry.
	// Only pendingWhereConditions (FieldKindAssignment, FieldKindPerRow) land in
	// helperPlan.SourceStage().Layer.Where; aggregate and window conditions are ignored.
	materializeConditions(registry, helperPlan)

	return strings.Join(helperPlan.SourceStage().Layer.Where, " AND ")
}

// chTimeLiteral renders a time as a ClickHouse datetime literal for comparison
// against a DateTime64(3) column. Second precision silently excludes rows in the
// sub-second remainder of the range end. Never use it for a plain DateTime column
// (e.g. logs_histogram.minute): those reject a fractional literal with code 53.
func chTimeLiteral(t time.Time) string {
	return t.Format("2006-01-02 15:04:05.000")
}

// addBaseConditions adds time range and fractal isolation conditions.
func addBaseConditions(plan *QueryPlan, opts QueryOptions) {
	// A subquery source (pgr composition) is already scoped by time + fractal inside the
	// subquery, and its timestamp column is a formatted String -- re-applying datetime range
	// / fractal predicates here would be redundant and type-incorrect. Skip them.
	if opts.SourceSubquery != "" {
		return
	}
	source := plan.SourceStage()

	tsCol := "timestamp"
	if opts.UseIngestTimestamp {
		tsCol = "ingest_timestamp"
	}
	endOp := "<="
	if opts.EndExclusive {
		endOp = "<"
	}
	source.Layer.Where = append(source.Layer.Where,
		fmt.Sprintf("%s >= '%s'", tsCol, chTimeLiteral(opts.StartTime)),
		fmt.Sprintf("%s %s '%s'", tsCol, endOp, chTimeLiteral(opts.EndTime)),
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
	if plan.IsProcessTree {
		if plan.IsAggregated {
			return nil, fmt.Errorf("ptg() cannot be combined with aggregation functions")
		}
		if plan.IsChain {
			return nil, fmt.Errorf("ptg() cannot be combined with chain()")
		}
		// proc_lineage is a live ClickHouse table, not part of the Iceberg archive; reject
		// ptg() over archived/recall data rather than silently querying the hot table.
		if opts.SourceMode == SourceIceberg {
			return nil, fmt.Errorf("ptg() operates on live process lineage and is not available over archived data")
		}
		return buildProcessTreeSQL(
			plan.ProcessTreeStart, plan.ProcessTreeDepth, plan.ProcessTreeDirection,
			source.Layer.Having,
			plan.ChartType, plan.ChartConfig, opts,
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

	// Deferred math assignments run in the formatter outer SELECT, which wraps the
	// inner query as a subquery. JSON sub-column expressions (fields.`name`) generated
	// from those assignments require `fields` to be present in the inner SELECT.
	// Add it here if it is absent and there are deferred assignments to process.
	if len(deferredAssignments) > 0 && !plan.IsAggregated {
		hasFields := false
		for _, sel := range activeStage.Layer.Selects {
			if extractFieldAlias(sel.String()) == "fields" {
				hasFields = true
				break
			}
		}
		if !hasFields {
			activeStage.Layer.Selects = append(activeStage.Layer.Selects, SelectExpr{Expr: "fields"})
		}
	}

	// --- Set default ORDER BY and LIMIT ---
	defaultTimeOrder := false
	if len(activeStage.Layer.OrderBy) == 0 && len(activeStage.Layer.GroupBy) == 0 && !plan.IsAggregated {
		if len(opts.SourceOrderBy) > 0 {
			activeStage.Layer.OrderBy = opts.SourceOrderBy
		} else if activeStage.IsSource {
			// log_id breaks ties: it completes the logs sorting key (timestamp, log_id),
			// so ClickHouse still reads in reverse key order, and without it the rows
			// that survive the LIMIT at the boundary timestamp are arbitrary -- which
			// also drifts the cursor page boundary between runs.
			activeStage.Layer.OrderBy = []string{"timestamp DESC", "log_id DESC"}
			defaultTimeOrder = true
		}
	}
	if activeStage.Layer.Limit == "" && opts.MaxRows > 0 {
		activeStage.Layer.Limit = fmt.Sprintf("LIMIT %d", opts.MaxRows)
	}

	// --- Defer out-of-scope ORDER BY (window fields, join/model_lookup outputs) ---
	deferOutOfScopeOrderBy(ctx, plan, activeStage)

	// --- Export the source columns those deferred expressions reference ---
	if err := plan.exportDeferredColumns(); err != nil {
		return nil, err
	}

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
		// Columns exported purely to make deferred expressions resolvable are
		// stripped from the SQL result, so they must not be listed for display.
		fieldOrder = dropHiddenDeferredFields(fieldOrder, plan)
		// chain()'s internal columns stay in the result data but out of the display.
		if plan.IsChain {
			fieldOrder = dropField(fieldOrder, chainAnchorColumn)
			fieldOrder = dropField(fieldOrder, chainDoneColumn)
		}

		// When formatters add a subquery wrapper AND the query uses the default
		// timestamp-DESC order, mirror ORDER BY and LIMIT onto the formatter outer
		// SELECT so the final output stays ordered after projection.
		//
		// The source keeps its own ORDER BY LIMIT. It must: the formatter projects
		// toString(timestamp) AS timestamp, so an outer ORDER BY timestamp binds to
		// that String alias rather than to the DateTime64 sorting key, which disables
		// optimize_read_in_order and forces a full scan of the range no matter how
		// small the LIMIT. Keeping the sort on the source lets ClickHouse read the
		// MergeTree in reverse sorting-key order and stop at the LIMIT (measured on a
		// 2M-row day: 130k rows read instead of 2M). On a cluster it also makes each
		// shard limit its own output before the coordinator merge-sorts, rather than
		// shipping every matching row. Join queries null out Formatters after this
		// block, so only mirror when formatters will actually be rendered.
		if defaultTimeOrder && len(plan.Formatters) > 0 && !plan.IsJoin && plan.ModelLookupSQL == "" {
			plan.FormatterOrderBy = activeStage.Layer.OrderBy
			plan.FormatterLimit = activeStage.Layer.Limit
		}
		// norm_log is the default content column (normalized fields). Strip it from the
		// display order when the user has explicitly chosen columns via | table(...).
		if plan.TableHasExplicitColumns {
			filtered := fieldOrder[:0]
			for _, f := range fieldOrder {
				if f != "norm_log" && f != "log_id" {
					filtered = append(filtered, f)
				}
			}
			fieldOrder = filtered
		}
	}

	// --- Join wraps (join / model_lookup): skip the explicit formatter so the
	// wrapper's added columns pass through. An explicit formatter SELECT would drop
	// them, breaking both the enrichment display and any trailing filter (deferred
	// to a post-join WHERE that references those columns).
	//
	// Their outputs are then appended to the result column order, mirroring how
	// z-score/histogram surface their computed fields. Skip any already present
	// (e.g. a key column the source projected). An explicit `| table(...)` picks the
	// columns, so show only the joined outputs it named; the wrap still projects them
	// all, since a deferred filter or sort may reference one that is not displayed.
	if plan.IsJoin && plan.JoinSubSQL != "" {
		plan.Formatters = nil
		fieldOrder = appendJoinedFields(fieldOrder, joinDisplayNames(plan), plan)
	}
	// Scan-level model_lookup is exempt: its columns are per-row inputs to the
	// aggregation, not appended outputs, and the formatter path applies as normal.
	if plan.ModelLookupSQL != "" && !plan.ModelLookupAtScan {
		plan.Formatters = nil
		fieldOrder = appendJoinedFields(fieldOrder, plan.ModelLookupFields, plan)
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
		SQL:                sql,
		FieldOrder:         fieldOrder,
		IsAggregated:       plan.IsAggregated || len(activeStage.Layer.GroupBy) > 0,
		ChartType:          plan.ChartType,
		ChartConfig:        plan.ChartConfig,
		DefaultTimeOrder:   defaultTimeOrder,
		TimeScopedSubquery: plan.IsJoin || plan.ModelLookupSQL != "",
		Chain:              plan.Chain,
	}, nil
}

// pushCarryForwardStage finalizes the current aggregation stage and pushes a new
// projection stage that carries every prior output forward as a column. It is
// used to position a post-aggregation computation (an assignment, or a per-row
// transform command) after the GROUP BY rather than inside it, where a
// non-grouped column would be invalid. Returns the carried output names.
func pushCarryForwardStage(ctx *CommandContext) ([]string, error) {
	prevStage := ctx.Plan.CurrentStage()
	if len(prevStage.Layer.GroupBy) > 0 {
		if err := assembleGroupBySelects(ctx, prevStage, nil); err != nil {
			return nil, err
		}
	}

	var carried []string
	seen := make(map[string]bool)
	for _, sel := range prevStage.Layer.Selects {
		alias := strings.Trim(extractFieldAlias(sel.String()), "`")
		if alias != "" && !seen[alias] {
			carried = append(carried, alias)
			seen[alias] = true
		}
	}

	ctx.Plan.PushStage()
	ctx.Plan.aggregationOutputs = make(map[string]string)
	ctx.Plan.outerAggregations = nil
	ctx.Plan.outerAggFieldOrder = nil

	outputs := make(map[string]bool, len(carried))
	for _, c := range carried {
		outputs[c] = true
	}
	ctx.Registry.ScopeToOutputs(outputs)

	newStage := ctx.Plan.CurrentStage()
	for _, c := range carried {
		newStage.Layer.Selects = append(newStage.Layer.Selects, SelectExpr{Expr: c})
	}
	return carried, nil
}

// applyAssignmentStage materializes a post-aggregation math assignment as its
// own projection stage at its pipeline position, so commands after it (table,
// sort, case, ...) can reference it instead of it being hoisted to the outermost
// formatter where a later projection may have dropped the columns it depends on.
func applyAssignmentStage(ctx *CommandContext, a AssignmentNode) error {
	safeField, err := sanitizeIdentifier(a.Field)
	if err != nil {
		return fmt.Errorf("invalid assignment field: %w", err)
	}
	// Compute against the current registry (aggregate aliases are columns now),
	// before scoping replaces them with bare-alias entries.
	sqlExpr := convertMathExprToSQL(a.Expression, ctx.Registry, a.Field)

	if _, err := pushCarryForwardStage(ctx); err != nil {
		return fmt.Errorf("assignment %q stage finalize: %w", a.Field, err)
	}

	newStage := ctx.Plan.CurrentStage()
	newStage.Layer.Selects = append(newStage.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", sqlExpr, safeField)})

	// The computed value is now a materialized column of this stage; references
	// resolve to its alias.
	ctx.Registry.Register(safeField, FieldKindAssignment, safeField, ctx.CmdIndex)
	ctx.Registry.SetResolveExpr(safeField, safeField)
	return nil
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

		// Generate SELECT from raw field reference. Cast raw JSON subcolumns to
		// ::String so the grouped column is groupable even when the underlying
		// path is stored as Dynamic (rows ingested before the field was
		// type-hinted); a bare Dynamic ref triggers ClickHouse error 44.
		quotedName, err := sanitizeIdentifier(fieldName)
		if err != nil {
			return fmt.Errorf("groupBy: %w", err)
		}
		selects = append(selects, fmt.Sprintf("%s AS %s", groupableCast(gf), quotedName))
		addedAliases[quotedName] = true
		source.Layer.GroupBy[i] = quotedName
	}

	// 2. Add aggregation selects: first check registry/aggregationOutputs,
	// then keep remaining selects the registry cannot classify as a fallback for
	// handlers (table, bucket, heatmap, etc.) that add aggregation selects
	// directly without registering them. A select the registry knows holds a
	// per-row value (a base column, or a transform output such as lowercase, len,
	// logSize) is dropped instead: past this point it is neither grouped nor
	// aggregated, so projecting it fails with ClickHouse code 215.
	hasExplicitAgg := false
	for _, sel := range existingSelects {
		alias := extractFieldAlias(sel)
		if addedAliases[alias] {
			continue
		}
		_, inAggOutputs := ctx.Plan.aggregationOutputs[alias]
		if inAggOutputs || ctx.Registry.IsAggregate(alias) || !ctx.Registry.IsRowLevel(alias) {
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

	// Subquery source (a resolved source command like pgr()): project the source's flat
	// columns, never the logs-table norm_log/base defaults. Explicit table() columns are set
	// by tableHandler (already bare); aggregate/select handlers set their own Selects.
	if ctx.Opts.SourceSubquery != "" {
		if !plan.IsAggregated {
			if !(plan.HasTableCmd && plan.TableHasExplicitColumns && len(source.Layer.Selects) > 0) {
				source.Layer.Selects = sourceColumnSelects(ctx.Opts.SourceColumns)
			}
			existing := selectExprStrings(source.Layer.Selects)
			for _, af := range assignmentFields {
				if !contains(existing, af) {
					source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: af})
				}
			}
		}
		return
	}

	// Both hot and iceberg archives carry a norm_log column (DEFAULT
	// toString(fields) in the hot store, no n-gram index there; a plain JSON
	// String in the archive), so the projection is identical and the result
	// shape matches hot Query.
	normLogSel := "norm_log"

	// No commands and no assignments: use default field set
	if len(ctx.Pipeline.Commands) == 0 && len(ctx.Pipeline.Assignments) == 0 {
		// Alert queries: project only referenced fields + log_id + timestamp.
		// This minimal projection keys on UseIngestTimestamp, which Recall also
		// sets for ingest-time pruning; exclude iceberg so Recall gets the normal
		// full projection.
		if ctx.Opts.UseIngestTimestamp && ctx.Opts.SourceMode != SourceIceberg && ctx.Pipeline.Filter != nil {
			fields := collectConditionFields(ctx.Pipeline.Filter.Conditions)
			collectHavingConditionFields(ctx.Pipeline.HavingConditions, fields)
			// Include alert-configured extra fields (throttle field, template fields)
			for _, f := range ctx.Opts.AlertExtraFields {
				if f != "" {
					fields[f] = true
				}
			}
			if !fields["*"] {
				for _, base := range []string{"raw_log", normLogColumn, "timestamp", "log_id", "fractal_id", "ingest_timestamp"} {
					delete(fields, base)
				}
				source.Layer.Selects = []SelectExpr{
					{Expr: "timestamp"},
					{Expr: "log_id"},
					{Expr: "fractal_id"},
				}
				for field := range fields {
					safe := fmt.Sprintf("%s AS `%s`", groupableCast(jsonFieldRef(field)), field)
					source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: safe})
				}
				return
			}
		}
		source.Layer.Selects = []SelectExpr{
			{Expr: "timestamp"}, {Expr: normLogSel}, {Expr: "log_id"}, {Expr: "fractal_id"},
		}
		if ctx.Opts.IncludeShardNum {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "toString(_shard_num) AS _shard_num"})
		}
		return
	}

	// No commands but has assignments
	if len(ctx.Pipeline.Commands) == 0 && len(ctx.Pipeline.Assignments) > 0 {
		source.Layer.Selects = []SelectExpr{{Expr: "timestamp"}, {Expr: normLogSel}, {Expr: "log_id"}, {Expr: "fractal_id"}}
		for _, af := range assignmentFields {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: af})
		}
		if ctx.Opts.IncludeShardNum {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "toString(_shard_num) AS _shard_num"})
		}
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
	if plan.HasTableCmd && !plan.TableHasExplicitColumns && len(source.Layer.Selects) > 0 {
		hasFieldsMap := false
		for _, sel := range source.Layer.Selects {
			if strings.Contains(sel.String(), "_all_fields") {
				hasFieldsMap = true
				break
			}
		}
		if !hasFieldsMap {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: contentColMode(ctx.Opts.SourceMode) + " AS _all_fields"})
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
			expr := name
			if name == normLogColumn {
				expr = normLogSel
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: expr})
		}
		ensureSelectExpr("timestamp")
		ensureSelectExpr("log_id")
		ensureSelectExpr("fractal_id")
		// norm_log (flat normalized fields) is the default content column. Skip it when
		// the user picks explicit columns. raw_log (original) is no longer projected;
		// the Raw tab lazy-loads it on demand within its retention window.
		if !plan.TableHasExplicitColumns {
			ensureSelectExpr("norm_log")
		}

		hasFields := false
		for _, sel := range source.Layer.Selects {
			alias := extractFieldAlias(sel.String())
			if alias == "fields" || strings.Contains(sel.String(), "_all_fields") {
				hasFields = true
				break
			}
		}
		if !hasFields && plan.HasTableCmd && !plan.TableHasExplicitColumns {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: contentColMode(ctx.Opts.SourceMode) + " AS fields"})
		}
		if ctx.Opts.IncludeShardNum {
			hasShardNum := false
			for _, sel := range source.Layer.Selects {
				if extractFieldAlias(sel.String()) == "_shard_num" {
					hasShardNum = true
					break
				}
			}
			if !hasShardNum {
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "toString(_shard_num) AS _shard_num"})
			}
		}
	}
}

// deferOutOfScopeOrderBy moves ORDER BY clauses that reference columns produced
// after the source scan -- window fields (z-score, outlier) and the outputs of a
// model_lookup()/join() wrap -- to the deferred outer layer, where they exist.
//
// It also lifts the source LIMIT past the join whenever a filter on a joined column
// was deferred. Leaving the LIMIT on the source would apply it BEFORE that filter, so
// the filter would only ever see the newest MaxRows logs: the result would be a
// near-arbitrary subset that changes run to run whenever rows tie on timestamp (and
// especially across a prism's member fractals).
func deferOutOfScopeOrderBy(ctx *CommandContext, plan *QueryPlan, source *QueryStage) {
	// A scan-level model join is not a wrap: its columns are per-row before the
	// aggregation, so nothing needs relocating and the stage LIMIT already applies
	// after the join.
	hasJoinWrap := (plan.ModelLookupSQL != "" && !plan.ModelLookupAtScan) || (plan.IsJoin && plan.JoinSubSQL != "")
	if plan.ModifiedZScoreExpr == "" && !hasJoinWrap {
		return
	}

	var innerOrder []string
	var deferredOrder []string

	for _, ob := range source.Layer.OrderBy {
		// Extract field name (strip direction suffix)
		fieldName := strings.Fields(ob)[0]
		if ctx.Registry.IsWindow(fieldName) || (hasJoinWrap && isJoinedField(ctx.Registry, fieldName)) {
			deferredOrder = append(deferredOrder, ob)
		} else {
			innerOrder = append(innerOrder, ob)
		}
	}

	// A strict model_lookup drops rows at the join, so a LIMIT left on the scan would
	// be applied before that filter and return an arbitrary subset of the newest
	// MaxRows logs. The exception is an exact prefilter: the scan's own WHERE then
	// rejects precisely the rows the join would, so the LIMIT is already counting
	// only matches and can stay where read-in-order can use it.
	strictInexactJoin := plan.ModelLookupSQL != "" && !plan.ModelLookupAtScan && plan.ModelLookupStrict && !plan.ModelLookupPrefilterExact
	liftLimit := len(deferredOrder) > 0 || (hasJoinWrap && len(plan.DeferredWhere) > 0) || strictInexactJoin
	if liftLimit && hasJoinWrap {
		// The ordering must follow the filter, so it moves out with the LIMIT.
		deferredOrder = append(deferredOrder, innerOrder...)
		innerOrder = nil
	}

	source.Layer.OrderBy = innerOrder
	if liftLimit {
		// Relocated ORDER BY expressions face the same scope limit as relocated
		// filters: a `sort(image)` carried past the join must read the exported
		// hidden column, not the source-only `fields.image`.
		for i, ob := range deferredOrder {
			deferredOrder[i] = scopeOrderByExpr(ob, plan.deferredScope())
		}
		plan.DeferredOrder = append(plan.DeferredOrder, deferredOrder...)
		plan.DeferredLimit = source.Layer.Limit
		source.Layer.Limit = ""
	}
}

// dropHiddenDeferredFields removes the deferred scope's exported columns from the
// display order; they are EXCEPT-ed out of the result set.
func dropHiddenDeferredFields(fieldOrder []string, plan *QueryPlan) []string {
	if !plan.deferred.used() {
		return fieldOrder
	}
	hidden := make(map[string]bool, len(plan.deferred.names))
	for _, n := range plan.deferred.names {
		hidden[n] = true
	}
	out := fieldOrder[:0]
	for _, f := range fieldOrder {
		if !hidden[f] {
			out = append(out, f)
		}
	}
	return out
}

// dropField removes one column from the display order, leaving it in the result set.
func dropField(fieldOrder []string, name string) []string {
	out := fieldOrder[:0]
	for _, f := range fieldOrder {
		if f != name {
			out = append(out, f)
		}
	}
	return out
}

// scopeOrderByExpr rewrites the expression part of an "<expr> ASC|DESC" ORDER BY
// entry through the deferred scope, leaving the direction intact.
func scopeOrderByExpr(orderBy string, scope *deferredScope) string {
	expr, dir := orderBy, ""
	if i := strings.LastIndex(orderBy, " "); i > 0 {
		switch strings.ToUpper(orderBy[i+1:]) {
		case "ASC", "DESC":
			expr, dir = orderBy[:i], orderBy[i:]
		}
	}
	return scope.ref(expr, "") + dir
}

// appendJoinedFields adds a JOIN wrap's output columns to the display order,
// narrowed to what an explicit `| table(...)` asked for.
func appendJoinedFields(fieldOrder, outputs []string, plan *QueryPlan) []string {
	if plan.TableHasExplicitColumns {
		outputs = plan.TableJoinedFields
	}
	seen := make(map[string]bool, len(fieldOrder))
	for _, f := range fieldOrder {
		seen[f] = true
	}
	for _, f := range outputs {
		if !seen[f] {
			fieldOrder = append(fieldOrder, f)
			seen[f] = true
		}
	}
	return fieldOrder
}

// isJoinedField reports whether a name resolves to a column added by a JOIN wrap,
// which exists only after that wrap.
func isJoinedField(registry *FieldRegistry, name string) bool {
	entry := registry.Get(strings.Trim(name, "`"))
	return entry != nil && entry.Kind == FieldKindJoined
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
			{Expr: "toString(timestamp) as timestamp"},
			{Expr: "log_id"},
			{Expr: "norm_log AS fields"},
		}
	}

	var formatters []SelectExpr
	for _, field := range selectFields {
		alias := extractFieldAlias(field)
		if alias == "timestamp" {
			formatters = append(formatters, SelectExpr{Expr: "toString(timestamp) as timestamp"})
		} else if field == "fields" {
			// The raw JSON `fields` column is kept in the inner SELECT only so
			// deferred math assignments can reference fields.`x`. The ClickHouse
			// driver cannot scan the native JSON type, so it must never be
			// projected to the final result. (Table commands use the explicit
			// toString(fields) form, which is unaffected by this guard.)
			continue
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
			sqlExpr := convertMathExprToSQL(da.Expression, registry, da.Field)
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
		if alias != "_all_fields" && alias != "fields" && alias != "_shard_num" {
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
