package parser

import (
	"fmt"
	"maps"
	"slices"
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
	// HistoryDays is the window a rarity or first_seen model scores over, 0 for
	// all of its history. A windowed model's table has a day column to bound.
	HistoryDays int
	FractalID   string
	// Distributed reports that TableName is a Distributed table (cluster mode). The
	// model join and its strict-mode prefilter must then be GLOBAL: a non-GLOBAL
	// subquery over a Distributed table inside a query that already reads
	// logs_distributed is rejected outright (distributed_product_mode='deny').
	Distributed bool
}

type QueryOptions struct {
	// bindingWork bounds how many result-set bindings one query may translate.
	// Bindings compose, so a binding read twice by a binding read twice doubles
	// the work at every level: twenty such lines is a 1.7KB query that takes the
	// translator minutes. Carried through subqueryOptions so nested translations
	// share one allowance. Unexported: a caller never sets it.
	bindingWork *int

	StartTime             time.Time
	EndTime               time.Time
	EndExclusive          bool // emit "timestamp < EndTime" instead of "<="; lets adjacent chunks abut without dropping or duplicating a row
	MaxRows               int
	EventTimes            []time.Time                  // narrow the scan to rows at exactly these event times (timestamp, never ingest_timestamp)
	NormLogIndexHints     bool                         // hint eligible field filters to the norm_log n-gram index (see normLogHint)
	FractalID             string                       // Fractal UUID for filtering logs to specific fractal
	FractalIDs            []string                     // Multiple fractal UUIDs (prism context); overrides FractalID when set
	IncludeEmptyFractalID bool                         // Include logs with no fractal_id (legacy data) when querying default fractal
	Dictionaries          map[string]map[string]string // dict name -> key col -> ClickHouse lookup name
	// CaseInsensitiveDicts names the dictionaries whose ClickHouse objects were built
	// over lower(key); match() must lowercase the looked-up value to hit them.
	CaseInsensitiveDicts map[string]bool
	Models               map[string]AnalyticsModelInfo // model name -> ModelInfo for model_lookup() BQL command
	HasCommentFilter     bool                          // True when query uses comment() and log_ids have been pre-fetched
	CommentLogIDs        []string                      // Pre-fetched log_ids from PostgreSQL for comment() filtering
	HasTLSHFilter        bool                          // True when query uses tlsh() and the digest match has been resolved
	TLSHMatches          []TLSHMatch                   // Digests within threshold of a needle, resolved server-side against the tlsh model index
	UseIngestTimestamp   bool                          // Filter on ingest_timestamp instead of timestamp (alerts, live search, Recall)
	// MinimalProjection projects only the fields the filter references plus log_id
	// and timestamp, for callers that match rows rather than display them (alert
	// evaluation, the rule tester).
	MinimalProjection bool
	// ModelNewSince is the start of an alert's evaluation window. A first_seen
	// model's is_new also admits an entity recorded since then, so an alert
	// catching up on a backlog older than the usual hour still sees what was new
	// in it. Zero outside alert evaluation.
	ModelNewSince time.Time
	// MaxEventLagSeconds drops rows whose event time trails their ingest time by
	// more than this many seconds (0 = keep every row). Alerts read forward on
	// ingest_timestamp, so a source flushing a backlog presents week-old events as
	// new; this keeps them out of the result set rather than filtering afterwards,
	// where they would consume the row budget and starve genuinely new matches.
	// One-sided on purpose: an event dated ahead of its arrival (a fast clock) is
	// never dropped.
	MaxEventLagSeconds int
	AlertExtraFields   []string // Additional fields to project in alert auto-projection (throttle field, template fields)
	GeoIPEnabled       bool     // True when MaxMind GeoLite2 dictionaries are loaded
	// NetworkDicts marks the dictionary objects built as an IP_TRIE. Their keys are
	// CIDR ranges, so a lookup probes an address and matches the longest range
	// holding it; probing one with a string is rejected by the server.
	NetworkDicts map[string]bool
	// PatternDicts marks the objects built as a REGEXP_TREE. Their keys are
	// regular expressions matched against the probe, first match wins, and the
	// server has no dictHas for them.
	PatternDicts map[string]bool
	// PatternPrefilters holds, per pattern object, SQL for an array of the list's
	// expressions. Set only for a list ClickHouse has compiled for Hyperscan:
	// multiMatchAny over it then screens each row before the REGEXP_TREE lookup.
	//
	// All three are keyed by ClickHouse object name, not list name: a secondary
	// key column on any list resolves to an ordinary HASHED dictionary.
	PatternPrefilters      map[string]string
	DictionaryDatabase     string       // ClickHouse database holding the dictionary objects; qualifies every dictGet
	TableName              string       // Override source table (default "logs", use "logs_distributed" in cluster mode)
	ProcLineageTable       string       // Process-lineage read table for ptg() ("proc_lineage" or "proc_lineage_distributed")
	ProcessTree            *ProcessTree // ptg()'s tree, walked by the query layer before translation; nil = not resolved
	ProcFreqTable          string       // Frequency-baseline read table for pgr() ("proc_freq" or "proc_freq_distributed")
	ProcEdgesTable         string       // Edge-rollup read table for pgr() leaf edges ("process_edges" or "process_edges_distributed")
	ProvenanceSummaryTable string       // Fleet-totals read table for pgr() ("provenance_summary" or "provenance_summary_distributed")
	IncludeShardNum        bool         // Include _shard_num virtual column for direct-shard detail lookup (cluster mode only)
	// IncludeIngestTime projects ingest_timestamp as the hidden _ingest_timestamp
	// column for the detail panel. Set by the paths feeding it (search, recall); off
	// for alerts and the rule tester, whose rows are matched rather than displayed.
	IncludeIngestTime bool
	SourceMode        SourceMode // Hot (default, JSON logs) vs Iceberg (MAP archive); gates iceberg field-access codegen
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
	// SourceWhere holds the predicates bound to the source scan. A caller that
	// composes its own aggregation over the same rows takes these rather than the
	// rendered SQL, whose outer projection formats for display.
	SourceWhere []string
	// SourceWhereComplete is true when SourceWhere is the query's entire row
	// selection: one scan stage, no aggregation, no wrapper layer and no
	// predicate bound above the scan. A caller that keeps only SourceWhere and
	// discards the rendered SQL reads the same rows only when this is set.
	SourceWhereComplete bool
	// SourceProjections are the columns the scan computes, in the order the query
	// produced them. A predicate in SourceWhere may name one of these rather than
	// inline it, because a registry-known field compiles to its own alias, so a
	// caller keeping SourceWhere has to project these alongside it or the scan
	// references a column nothing defines.
	SourceProjections []SourceProjection
	// TimeScopedSubquery is true when the SQL embeds a subquery that carries its
	// own copy of the query's time bounds (join / model_lookup). Re-translating
	// such a query over a narrower window also narrows that subquery, which drops
	// rows whose join partner sits outside the window, so the caller must not
	// slice the range.
	TimeScopedSubquery bool
	// RowCapped is true when an embedded subquery (join, result-set binding) is
	// truncated at a row cap, so the rows the query selects may be incomplete.
	RowCapped bool
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
	q, err := planQuery(pipeline, opts)
	if err != nil {
		return nil, err
	}
	return finalizePlan(q.ctx, q.assignmentFields, q.deferredAssignments)
}

// plannedQuery is a pipeline planned up to final assembly.
type plannedQuery struct {
	ctx                 *CommandContext
	assignmentFields    []string
	deferredAssignments []deferredAssignment
}

// scanWhere is the predicate the pipeline puts on its source scan beyond the time
// range and fractal scope: what the histogram and field statistics filter by, so
// they count exactly the events the results show.
func (q *plannedQuery) scanWhere() string {
	return andJoin(q.ctx.Plan.SourceStage().Layer.Where[q.ctx.Plan.baseWhereLen:])
}

// planQuery runs every step of translation but the final assembly. The histogram
// and field statistics plan the same way rather than mirroring these steps, which
// drifted: they dropped condition-function operands and assignments.
func planQuery(pipeline *PipelineNode, opts QueryOptions) (*plannedQuery, error) {
	if opts.SourceMode == SourceIceberg {
		if err := icebergSupportedFeatures(pipeline); err != nil {
			return nil, err
		}
	}
	registry := NewFieldRegistry(opts.SourceMode, opts.IcePromoted)
	registry.normLogIndexHints = opts.NormLogIndexHints && opts.SourceMode != SourceIceberg && opts.SourceSubquery == ""
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
	plan.baseWhereLen = len(plan.SourceStage().Layer.Where)

	// ---------------------------------------------------------------
	// 3. DECLARE PHASE: every command registers what fields it produces
	// ---------------------------------------------------------------
	// Declare registers every command's outputs up front for validation; each
	// command's entries are kept so Execute can reinstate them at its position.
	undeclared := registry.Clone()
	declaredBy := make([][]FieldEntry, len(pipeline.Commands))
	for i, cmd := range pipeline.Commands {
		ctx.CmdIndex = i
		handler := getCommandHandler(cmd.Name)
		if handler == nil {
			return nil, fmt.Errorf("unsupported command: %s", cmd.Name)
		}
		before := registry.Clone()
		if err := handler.Declare(cmd, ctx); err != nil {
			return nil, err
		}
		for _, name := range registry.changedSince(before) {
			declaredBy[i] = append(declaredBy[i], *registry.Get(name))
		}
	}

	if err := validateCommandSchemas(pipeline); err != nil {
		return nil, err
	}
	if err := validateProcessTreeTail(pipeline); err != nil {
		return nil, err
	}

	if err := validateExprArgs(pipeline, registry); err != nil {
		return nil, err
	}

	// tlsh() used as a boolean operand never reaches its handler, so its projected
	// columns are declared here instead. See DeclareTLSHOperandColumns.
	if err := DeclareTLSHOperandColumns(pipeline, ctx); err != nil {
		return nil, err
	}

	// model_lookup() placement: with no aggregation before it, the model join sits in
	// the row scan's FROM, so every command after it (an aggregation included) reads
	// the model's columns per row. After an aggregation it enriches the aggregated
	// rows instead, joined below the stage that reads them. Decided before Execute so
	// the handlers route model-column references accordingly.
	if mlIdx := commandIndex(pipeline.Commands, "modelLookup"); mlIdx >= 0 {
		plan.ModelLookupAtScan = firstAggregatingCommandIndex(pipeline.Commands) > mlIdx
	}

	// Registered only to capture declared: commands and assignments enter the
	// registry at their position in the Execute loop, so nothing before them
	// sees them.
	for _, assignment := range pipeline.Assignments {
		registry.Register(assignment.Field, FieldKindAssignment, assignment.Field, -1)
	}
	// Captured before Execute, whose stage pushes scope the registry and drop
	// columns a later command will still add.
	declared := registry.ProducedNames()
	ctx.Declared = registry.Clone()
	var pending []string
	for _, name := range registry.order {
		if !undeclared.Has(name) || *undeclared.Get(name) != *registry.Get(name) {
			pending = append(pending, name)
		}
	}
	registry.unassign(pending, undeclared)

	// ---------------------------------------------------------------
	// 5. Field assignments, compiled at their position in the Execute loop
	// ---------------------------------------------------------------
	var assignmentFields []string
	var deferredAssignments []deferredAssignment
	firstAgg := firstAggregatingCommandIndex(pipeline.Commands)
	hasAggregation := firstAgg < len(pipeline.Commands)
	applyAssignment := func(assignment AssignmentNode) error {
		safeField, err := sanitizeIdentifier(assignment.Field)
		if err != nil {
			return fmt.Errorf("invalid assignment field: %w", err)
		}

		// Past the aggregation every assignment is its own stage, so what ran
		// before it keeps the value it replaces.
		if hasAggregation && assignment.CmdIndex > firstAgg {
			return applyAssignmentStage(ctx, assignment)
		}
		// A reassignment replaces the earlier column; filters before it already
		// folded the earlier expression in.
		assignmentFields = slices.DeleteFunc(assignmentFields, func(af string) bool { return selectName(af) == assignment.Field })
		deferredAssignments = slices.DeleteFunc(deferredAssignments, func(da deferredAssignment) bool { return da.Field == assignment.Field })
		layer := &ctx.Plan.CurrentStage().Layer
		layer.Selects = slices.DeleteFunc(layer.Selects, func(sel SelectExpr) bool { return selectName(sel.String()) == assignment.Field })

		// A computed assignment binds to a pipeline stage, because where it is
		// evaluated decides what it can see. A bare field or literal has nothing
		// to compute and is materialized straight away.
		if !isComputedExpr(assignment.Expr) {
			expression, _, err := compileExpr(assignment.Expr, registry)
			if err != nil {
				return assignmentError(assignment.Field, err)
			}
			af := fmt.Sprintf("%s AS %s", expression, safeField)
			assignmentFields = append(assignmentFields, af)
			registry.Register(safeField, FieldKindAssignment, safeField, -1)
			registry.SetResolveExpr(safeField, af)
			return nil
		}
		switch {
		case hasAggregation:
			// Pre-aggregation: inline so aggregations/filters reference the
			// computed value, and so later pre-aggregation assignments
			// referencing this one fold in the full expression (it is never
			// materialized as a column).
			sqlExpr, exprType, err := compileExpr(assignment.Expr, registry)
			if err != nil {
				return assignmentError(assignment.Field, err)
			}
			registry.RegisterInlineExpr(safeField, sqlExpr, -1)
			if exprType == TypeNumber {
				registry.SetNumeric(safeField)
			}
		default:
			// No aggregation: computed in the outer formatter, but registered so
			// a downstream filter folds in the expression. Without this the
			// filter referenced a column the inner scan does not have and
			// silently matched nothing.
			sqlExpr, exprType, err := compileExpr(assignment.Expr, registry)
			if err != nil {
				return assignmentError(assignment.Field, err)
			}
			registry.Register(safeField, FieldKindPerRow, sqlExpr, -1)
			registry.SetResolveExpr(safeField, sqlExpr)
			if exprType == TypeNumber {
				registry.SetNumeric(safeField)
			}
			if registry.DependsOnJoin(safeField) {
				// The model's columns exist in the SELECT that joins them, which the
				// formatter only sees as far as that SELECT projects them.
				assignmentFields = append(assignmentFields, fmt.Sprintf("%s AS %s", sqlExpr, safeField))
				return nil
			}
			deferredAssignments = append(deferredAssignments, deferredAssignment{Field: assignment.Field, SQL: sqlExpr})
		}
		return nil
	}

	// ---------------------------------------------------------------
	// 6. EXECUTE PHASE: commands and assignments run in pipeline order, each
	//     reading the registry as the pipeline left it there. The registry at
	//     every position is kept for the conditions written there.
	// ---------------------------------------------------------------
	plan.positionAt = map[[2]int]*FieldRegistry{}
	capture := func(cmds, assigns int) {
		if cmds == 0 {
			plan.firstStageAt = append(plan.firstStageAt, registry.firstStageView())
			return
		}
		view := registry.Clone()
		view.rowLevel = cmds <= firstAgg
		plan.positionAt[[2]int{cmds, assigns}] = view
	}
	next := 0
	for i := 0; i <= len(pipeline.Commands); i++ {
		ctx.CmdIndex = i
		for next < len(pipeline.Assignments) && pipeline.Assignments[next].CmdIndex == i {
			capture(i, next)
			if err := applyAssignment(pipeline.Assignments[next]); err != nil {
				return nil, err
			}
			next++
		}
		capture(i, next)
		if i == len(pipeline.Commands) {
			break
		}
		cmd := pipeline.Commands[i]
		registry.reinstate(declaredBy[i], undeclared)
		// A per-row transform that runs on a GROUP BY stage would add a
		// non-grouped column to an aggregate SELECT (invalid, silently dropped).
		// Move it onto a post-aggregation projection stage first.
		if IsTransformCommand(cmd.Name) && len(ctx.Plan.CurrentStage().Layer.GroupBy) > 0 {
			if _, err := pushCarryForwardStage(ctx); err != nil {
				return nil, fmt.Errorf("%s (post-aggregation stage): %w", cmd.Name, err)
			}
		}
		// A stats command after chain() aggregates the matched entities, not
		// the events inside each one.
		if ctx.Plan.CurrentStage().IsChain && closedOutputCommands[strings.ToLower(cmd.Name)] && !strings.EqualFold(cmd.Name, "groupby") {
			if err := pushAggregationStage(ctx); err != nil {
				return nil, fmt.Errorf("%s (post-chain stage): %w", cmd.Name, err)
			}
		}
		handler := getCommandHandler(cmd.Name)
		whereLen := len(plan.SourceStage().Layer.Where)
		if err := handler.Execute(cmd, ctx); err != nil {
			return nil, err
		}
		// A filter the command put on the scan applies to the rows a cap kept, and
		// one that reads the scan-level model join applies to the joined rows.
		if src := &plan.SourceStage().Layer; len(src.Where) > whereLen {
			added := slices.Clone(src.Where[whereLen:])
			src.Where = src.Where[:whereLen]
			after := src.capAfter(i)
			for _, w := range added {
				switch {
				case plan.ModelLookupAtScan && registry.SQLDependsOnJoin(w):
					plan.PostJoinWhere = append(plan.PostJoinWhere, postJoinFilter{At: i + 1, SQL: w})
				case after != nil:
					*after = append(*after, w)
				default:
					src.Where = append(src.Where, w)
				}
			}
		}
		ctx.Plan.stageAfterCmd = append(ctx.Plan.stageAfterCmd, len(ctx.Plan.Stages)-1)
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
	// Compiled into a copy: the parsed pipeline is planned more than once.
	conditions := cloneConditions(pipeline.HavingConditions)
	if err := resolveCommandConditions(conditions, registry, plan, opts); err != nil {
		return nil, err
	}
	if err := validatePostAggregationFields(pipeline, registry, declared); err != nil {
		return nil, err
	}
	if err := classifyConditions(conditions, registry, plan); err != nil {
		return nil, err
	}
	materializeConditions(registry, plan)
	return &plannedQuery{ctx: ctx, assignmentFields: assignmentFields, deferredAssignments: deferredAssignments}, nil
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
	// A pipeline that could not be planned cannot show the histogram is
	// unfiltered, and answering from the rollup on that basis would count every log
	// in the window beside a filtered result set, so it falls back to scanning.
	q, err := planQuery(pipeline, opts)
	return err == nil && q.scanWhere() == ""
}

// BuildHistogramSQL generates a lightweight COUNT(*) GROUP BY time-bucket query.
// It applies the base filter conditions and, when the pipeline contains computed
// column commands (e.g. len()), also inlines their downstream filters (e.g. _len > 500)
// into the WHERE clause so the histogram count matches the results table.
func BuildHistogramSQL(pipeline *PipelineNode, opts QueryOptions, bucketSeconds int) (string, error) {
	with, where, err := scanFilter(pipeline, opts)
	if err != nil {
		return "", err
	}
	tbl := opts.EffectiveTableName()

	tsCol := "timestamp"
	if opts.UseIngestTimestamp {
		tsCol = "ingest_timestamp"
	}

	sql := fmt.Sprintf(
		"%sSELECT toStartOfInterval(%s, INTERVAL %d SECOND) AS bucket, count() AS cnt FROM %s WHERE %s GROUP BY bucket ORDER BY bucket",
		with, tsCol, bucketSeconds, tbl, where,
	)
	return sql, nil
}

// BuildHistogramNewestSQL counts BuildHistogramSQL's buckets as one array from
// opts.StartTime and, in the same scan, the event times (negated Unix ms, newest
// first) of the newest matches in [from, to).
func BuildHistogramNewestSQL(pipeline *PipelineNode, opts QueryOptions, bucketSeconds, newest int, from, to time.Time) (string, error) {
	if newest < 1 {
		return "", fmt.Errorf("newest must be at least 1, got %d", newest)
	}
	if bucketSeconds <= 0 || opts.StartTime.Unix()%int64(bucketSeconds) != 0 {
		return "", fmt.Errorf("window start %s is not on a %ds bucket boundary", opts.StartTime, bucketSeconds)
	}
	with, where, err := scanFilter(pipeline, opts)
	if err != nil {
		return "", err
	}
	sql := fmt.Sprintf(
		"%sSELECT countResample(%d, %d, %d)(toUnixTimestamp(toStartOfInterval(timestamp, INTERVAL %d SECOND))) AS hist_counts, "+
			"groupArraySortedIf(%d)(-toUnixTimestamp64Milli(timestamp), timestamp >= '%s' AND timestamp < '%s') AS newest_ms FROM %s WHERE %s",
		with, opts.StartTime.Unix(), opts.EndTime.Unix(), bucketSeconds, bucketSeconds,
		newest, chTimeLiteral(from), chTimeLiteral(to), opts.EffectiveTableName(), where,
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

// FieldSampleDistinctCap bounds the value hashes each field returns. One past
// the set-index threshold of 256 is enough to merge slices into an exact count
// below it and a definite "more" above it, at 8 bytes a value.
const FieldSampleDistinctCap = 257

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
    arrayMap(t -> t.3, top) AS top_errors,
    distinct_hashes
FROM (
    SELECT
        kv.1 AS key,
        count() AS present,
        uniq(kv.2) AS cardinality,
        groupUniqArray(%d)(cityHash64(kv.2)) AS distinct_hashes,
        approx_top_k(%d, %d)(substringUTF8(kv.2, 1, %d)) AS top
    FROM (
%s
    )
    ARRAY JOIN JSONExtractKeysAndValues(norm_log, 'String') AS kv
    WHERE kv.1 != '' AND kv.2 != ''
    GROUP BY key
    ORDER BY present DESC
    LIMIT %d
)`, FieldSampleDistinctCap, p.TopN, fieldSampleTopKReserve, p.ValueLen,
		BuildSampleScanSQL(p.Table, "norm_log", p.Where, nil, p.SampleSize), p.MaxFields)
}

// BuildSampleScanSQL is the schema sweep's unsorted row scan: one LIMIT per
// stratum, or a single LIMIT without strata. Each scan stays O(limit) and prunes
// parts by their ingest_timestamp min-max.
//
// The sweep samples each stratum in its own query and counts the rows with this
// UNION ALL. Sampling through the UNION ALL keeps every stratum's reader open at
// once and measured over four times the memory of one scan; the count reads only
// the predicate column, so its UNION ALL stays small.
func BuildSampleScanSQL(table, column, where string, strata []string, limit int) string {
	if strings.TrimSpace(where) == "" {
		where = "1 = 1"
	}
	if len(strata) == 0 {
		return fmt.Sprintf("        SELECT %s FROM %s WHERE %s LIMIT %d", column, table, where, limit)
	}
	scans := make([]string, len(strata))
	for i, st := range strata {
		scans[i] = fmt.Sprintf("        SELECT %s FROM %s WHERE %s AND (%s) LIMIT %d", column, table, where, st, limit)
	}
	return strings.Join(scans, "\n        UNION ALL\n")
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

	with, where, err := scanFilter(pipeline, opts)
	if err != nil {
		return "", err
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
	sql := with + fmt.Sprintf(`SELECT
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

// scanFilter is the full source-scan predicate (time range, fractal scope and
// every filter the pipeline puts on the scan) that the histogram and field
// statistics read, so both count the events the results show.
//
// with is the WITH clause defining the bindings the predicate reads, or "".
func scanFilter(pipeline *PipelineNode, opts QueryOptions) (with, where string, err error) {
	q, err := planQuery(pipeline, opts)
	if err != nil {
		return "", "", err
	}
	return q.ctx.Plan.withClause(), andJoin(q.ctx.Plan.SourceStage().Layer.Where), nil
}

// chTimeLiteral renders a time as a ClickHouse datetime literal for comparison
// against a DateTime64(3) column. Second precision silently excludes rows in the
// sub-second remainder of the range end. Never use it for a plain DateTime column
// (e.g. logs_histogram.minute): those reject a fractional literal with code 53.
func chTimeLiteral(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000")
}

// windowConditions bounds a logs read to the query window, in the window's own
// time basis, honoring EndExclusive.
func windowConditions(opts QueryOptions) []string {
	col := "timestamp"
	if opts.UseIngestTimestamp {
		col = "ingest_timestamp"
	}
	endOp := "<="
	if opts.EndExclusive {
		endOp = "<"
	}
	return []string{
		fmt.Sprintf("%s >= '%s'", col, chTimeLiteral(opts.StartTime)),
		fmt.Sprintf("%s %s '%s'", col, endOp, chTimeLiteral(opts.EndTime)),
	}
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
	source.Layer.Where = append(source.Layer.Where, windowConditions(opts)...)
	if len(opts.EventTimes) > 0 {
		lits := make([]string, len(opts.EventTimes))
		for i, t := range opts.EventTimes {
			lits[i] = "'" + chTimeLiteral(t) + "'"
		}
		source.Layer.Where = append(source.Layer.Where, "timestamp IN ("+strings.Join(lits, ", ")+")")
	}

	if opts.MaxEventLagSeconds > 0 {
		source.Layer.Where = append(source.Layer.Where,
			fmt.Sprintf("dateDiff('second', timestamp, ingest_timestamp) <= %d", opts.MaxEventLagSeconds))
	}

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
func finalizePlan(ctx *CommandContext, assignmentFields []string, deferredAssignments []deferredAssignment) (*TranslationResult, error) {
	plan := ctx.Plan
	source := plan.SourceStage()
	opts := ctx.Opts

	readThrough := plan.IsAggregated || plan.IsChain
	if n := len(source.Layer.Caps); n > 0 {
		readThrough = readThrough || plan.readsThroughCap(source.Layer.Caps[n-1])
	}
	source.Layer.foldLastCap(readThrough)
	if len(source.Layer.Caps) > 0 {
		if plan.IsProcessTree || plan.IsAnalyze {
			return nil, fmt.Errorf("head(), tail(), limit() and dedup() can only be followed by a transform here; move the filter before them")
		}
		plan.rowLimitByCommand = true
		// Shown in the cap's order unless something after it sorts.
		if last := source.Layer.Caps[len(source.Layer.Caps)-1]; len(source.Layer.OrderBy) == 0 && !plan.IsAggregated {
			source.Layer.OrderBy = last.OrderBy
		}
	}

	// --- Special query modes (generate entirely different SQL) ---
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
			plan.ProcessTreeStart,
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
	if len(activeStage.Layer.OrderBy) == 0 && len(activeStage.Layer.GroupBy) == 0 && !plan.IsAggregated && !plan.ChartReadsAllRows {
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
	if activeStage.Layer.Limit == "" {
		// A chained aggregate (groupby | ... | avg(x)) reads every group, so the
		// display limit belongs on its single result row, not on its input.
		if opts.MaxRows > 0 && !plan.ChartReadsAllRows && len(plan.outerAggregations) == 0 {
			activeStage.Layer.Limit = fmt.Sprintf("LIMIT %d", opts.MaxRows)
		}
	} else {
		// A command bounded the rows, not the caller's MaxRows. That is row
		// selection rather than presentation, so a caller reading only the scan
		// predicates would read past it.
		plan.rowLimitByCommand = true
		// The command may narrow the caller's cap but not lift it: the caller
		// buffers every returned row, so head(999999999) would load them all.
		if opts.MaxRows > 0 && !plan.ChartReadsAllRows && len(plan.outerAggregations) == 0 {
			var n int
			if _, err := fmt.Sscanf(activeStage.Layer.Limit, "LIMIT %d", &n); err == nil && n > opts.MaxRows {
				activeStage.Layer.Limit = fmt.Sprintf("LIMIT %d", opts.MaxRows)
			}
		}
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
		plan.Formatters = buildFormatters(selectStrings, deferredAssignments)
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
		// block, so only mirror when formatters will actually be rendered. Without a
		// LIMIT the outer sort would buffer and re-sort every row on the String alias.
		if defaultTimeOrder && activeStage.Layer.Limit != "" && len(plan.Formatters) > 0 && !plan.IsJoin {
			plan.FormatterOrderBy = activeStage.Layer.OrderBy
			plan.FormatterLimit = activeStage.Layer.Limit
		}
		// table(...) shows the columns it names. Every row still carries the ones
		// projected for its identity, content and window value, which are only
		// displayed if named.
		if plan.TableHasExplicitColumns {
			fieldOrder = slices.DeleteFunc(fieldOrder, func(f string) bool {
				return (rowIdentityColumns[f] || f == windowValueColumn) && !slices.Contains(plan.TableColumns, f)
			})
		}
	}

	// --- join() wrap: skip the explicit formatter so the
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
	if len(plan.ChartLayers) > 0 {
		plan.WindowLayers = append(plan.WindowLayers, plan.ChartLayers...)
		fieldOrder = plan.ChartFieldOrder
		plan.IsAggregated = true
	}

	// --- Render SQL ---
	sql, err := plan.Render(opts)
	if err != nil {
		return nil, err
	}

	return &TranslationResult{
		SQL:                 sql,
		SourceWhere:         append([]string(nil), plan.SourceStage().Layer.Where...),
		SourceWhereComplete: plan.sourceWhereComplete(),
		SourceProjections:   plan.sourceProjections(),
		FieldOrder:          fieldOrder,
		IsAggregated:        plan.IsAggregated || len(activeStage.Layer.GroupBy) > 0,
		ChartType:           plan.ChartType,
		ChartConfig:         plan.ChartConfig,
		DefaultTimeOrder:    defaultTimeOrder,
		TimeScopedSubquery:  plan.IsJoin || plan.ModelLookupSQL != "" || plan.usesBindingSet,
		RowCapped:           plan.IsJoin || plan.usesBindingSet,
		Chain:               plan.Chain,
	}, nil
}

// pushAggregationStage finalizes the current aggregation stage and pushes an
// empty stage that aggregates over its output rows, which are all it can see.
func pushAggregationStage(ctx *CommandContext) error {
	prevStage := ctx.Plan.CurrentStage()
	// A projection of aggregated rows (no GROUP BY of its own) is already
	// assembled; assembling it again would clear its SELECT.
	if len(prevStage.Layer.GroupBy) > 0 {
		if err := assembleGroupBySelects(ctx, prevStage, nil); err != nil {
			return err
		}
	}
	prevOutputs := stageOutputAliases(prevStage)
	ctx.Plan.PushStage()
	ctx.Plan.CurrentStage().Aggregates = true
	ctx.Plan.IsAggregated = false
	ctx.Plan.aggregationOutputs = make(map[string]string)
	ctx.Plan.outerAggregations = nil
	ctx.Plan.outerAggFieldOrder = nil
	ctx.Registry.ScopeToOutputs(prevOutputs)
	return nil
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
		newStage.Layer.Selects = append(newStage.Layer.Selects, SelectExpr{Expr: quoteIdent(c)})
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
	sqlExpr, _, err := compileExpr(a.Expr, ctx.Registry)
	if err != nil {
		return assignmentError(a.Field, err)
	}

	if _, err := pushCarryForwardStage(ctx); err != nil {
		return fmt.Errorf("assignment %q stage finalize: %w", a.Field, err)
	}

	ctx.Plan.CurrentStage().Layer.project(fmt.Sprintf("%s AS %s", sqlExpr, safeField))

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
		if !addedAliases[extractFieldAlias(af)] {
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

// appendIngestTime projects ingest_timestamp under its hidden alias, once. Only the
// non-aggregated logs-source paths call it: after a GROUP BY the column is neither
// grouped nor aggregated (code 215), and a pgr() subquery source lacks it (code 47).
func appendIngestTime(ctx *CommandContext, source *QueryStage) {
	if !ctx.Opts.IncludeIngestTime {
		return
	}
	for _, sel := range source.Layer.Selects {
		// Already added, or the query binds the bare name itself (an explicit
		// | table(ingest_timestamp), or an eval() that rebinds it). A second
		// reference would resolve to that alias rather than to the column, so
		// leave it out rather than carry a value that is not the ingest time.
		switch extractFieldAlias(sel.String()) {
		case ingestTimeColumn, "ingest_timestamp":
			return
		}
	}
	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: "ingest_timestamp AS " + ingestTimeColumn})
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
			source.Layer.addAssignments(assignmentFields)
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
		if ctx.Opts.MinimalProjection && ctx.Opts.SourceMode != SourceIceberg && len(ctx.Pipeline.HavingConditions) > 0 {
			fields := map[string]bool{}
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
				for _, field := range slices.Sorted(maps.Keys(fields)) {
					// These names include alert-configured ones (throttle field, name
					// template), which are not query text and are not constrained by
					// the lexer. An unescaped backtick closed the alias and appended a
					// select expression of the caller's choosing, so the name is both
					// checked and escaped: the outer projection re-derives column names
					// by string-parsing this expression, which escaping alone cannot
					// make safe.
					if !isPlainFieldName(field) {
						continue
					}
					safe := fmt.Sprintf("%s AS `%s`", groupableCast(jsonFieldRef(field)), EscapeCHBacktickIdent(field))
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
		appendIngestTime(ctx, source)
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
		appendIngestTime(ctx, source)
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

	source.Layer.addAssignments(assignmentFields)

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
		// The scan-level model join's columns, after the log's own. An explicit
		// table() projects the ones it names itself.
		if plan.ModelLookupSQL != "" && plan.ModelLookupAtScan && !plan.TableHasExplicitColumns {
			for _, f := range plan.ModelLookupFields {
				source.Layer.project(f)
			}
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
		appendIngestTime(ctx, source)
	}
}

// deferOutOfScopeOrderBy moves ORDER BY clauses that reference columns produced
// after the source scan -- window fields (z-score, outlier) and the outputs of a
// join() wrap -- to the deferred outer layer, where they exist.
//
// It also lifts the source LIMIT past the join whenever a filter on a joined column
// was deferred. Leaving the LIMIT on the source would apply it BEFORE that filter, so
// the filter would only ever see the newest MaxRows logs: the result would be a
// near-arbitrary subset that changes run to run whenever rows tie on timestamp (and
// especially across a prism's member fractals).
func deferOutOfScopeOrderBy(ctx *CommandContext, plan *QueryPlan, source *QueryStage) {
	// A model join is not a wrap: it sits in the FROM of the stage that reads its
	// columns, so nothing needs relocating and that stage's LIMIT follows the join.
	hasJoinWrap := plan.IsJoin && plan.JoinSubSQL != ""
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

	liftLimit := len(deferredOrder) > 0 || (hasJoinWrap && len(plan.DeferredWhere) > 0)
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

// rowIdentityColumns are projected on every non-aggregated row, named or not, so a
// result row can be opened and located.
var rowIdentityColumns = map[string]bool{"timestamp": true, "log_id": true, "fractal_id": true, normLogColumn: true}

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
	valExpr := plan.HistogramValueExpr
	if valExpr == "" {
		valExpr = "_hist_val"
	}

	buckets := plan.HistogramBuckets
	// A range of zero (one row, or every value the same) put every row in bucket
	// 0 via a division that the server rejected as inf/nan, so the degenerate
	// case is branched on rather than divided through.
	bucketExpr := fmt.Sprintf(
		"least(toUInt32(if(_max_val = _min_val, 0, floor((_val - _min_val) / (_max_val - _min_val) * %d))), %d)",
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

// deferredAssignment is a computed assignment evaluated in the outer formatter,
// over the limited rows, as compiled where it is written.
type deferredAssignment struct {
	Field, SQL string
}

// buildFormatters creates the outer SELECT expressions for timestamp formatting
// and deferred math assignments.
func buildFormatters(selectFields []string, deferredAssignments []deferredAssignment) []SelectExpr {
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
		projected := map[string]bool{}
		for _, f := range selectFields {
			projected[strings.Trim(extractFieldAlias(f), "`")] = true
		}
		for _, da := range deferredAssignments {
			// A later command may already project this column. Recomputing it here
			// would alias two expressions to one name (ClickHouse code 179).
			if projected[da.Field] {
				continue
			}
			safeName, _ := sanitizeIdentifier(da.Field)
			formatters = append(formatters, SelectExpr{Expr: fmt.Sprintf("%s AS %s", da.SQL, safeName)})
		}
	}

	return formatters
}

// computeFieldOrder extracts the field order from SELECT expressions.
func computeFieldOrder(selectFields []string, deferredAssignments []deferredAssignment) []string {
	fieldOrder := make([]string, 0, len(selectFields))
	for _, field := range selectFields {
		alias := extractFieldAlias(field)
		if alias != "_all_fields" && alias != "fields" && alias != "_shard_num" && alias != ingestTimeColumn {
			fieldOrder = append(fieldOrder, strings.Trim(alias, "`"))
		}
	}
	for _, da := range deferredAssignments {
		if !slices.Contains(fieldOrder, da.Field) {
			fieldOrder = append(fieldOrder, da.Field)
		}
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
