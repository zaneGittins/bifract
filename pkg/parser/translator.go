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
	ID        string
	TableName string // distributed table name in cluster mode, local otherwise
	ModelType string // "rarity", "first_seen", or "volume_baseline"
	// Keys are the log fields modelLookup() must name, in order.
	Keys       []string
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
	// bindingHost is the plan of the query being translated. A predicate compiles
	// against a throwaway plan, so what a binding adds to the query goes here.
	bindingHost *QueryPlan

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
	ProcAncestryTable      string       // Ancestry read table for the pgrHas filters ("proc_ancestry" or "proc_ancestry_distributed")
	ProcAncestryMarkTable  string       // The ancestry builder's watermark ("proc_ancestry_watermark" or its _distributed table)
	ProvenanceMarksTable   string       // When each provenance rollup began ("provenance_marks" or its _distributed table)
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
	// SourceOrderBy overrides the implicit desc("timestamp") default ORDER BY applied to a bare
	// (no explicit sort()) query over SourceSubquery. Set by a source resolver (e.g. pgr(), via
	// ProvenanceOrderBy) whose subquery rows are already meaningfully pre-ordered -- timestamp
	// ordering would discard that and, combined with a LIMIT, truncate the wrong rows.
	SourceOrderBy []OrderTerm
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

// readsDistributed reports whether logs are read through a Distributed table, which
// storage.Topology.Table names with this suffix.
func (o QueryOptions) readsDistributed() bool {
	return strings.HasSuffix(o.TableName, "_distributed")
}

// subqueryIn is the IN operator for a subquery over the logs. A Distributed table
// refuses a plain one (code 288), and on a local table GLOBAL keeps the test out of
// PREWHERE, reading every column of every row the other filters leave.
func (o QueryOptions) subqueryIn(negate bool) string {
	op := "IN"
	if negate {
		op = "NOT IN"
	}
	if o.readsDistributed() {
		op = "GLOBAL " + op
	}
	return op
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
	// RangeDependentRows is true when a row's presence or values depend on the
	// other rows in range (a command's row cap or dedup, a window score such as
	// madoutlier), so reading the range in time slices or sliding windows would
	// compute them over each slice instead of the whole range.
	RangeDependentRows bool
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
	return q.finalize()
}

// plannedQuery is a pipeline planned up to final assembly.
type plannedQuery struct {
	ctx *CommandContext
	// undeclared is the registry before any command declared its outputs, and
	// declaredBy[i] what command i declares, added to the registry when it runs.
	undeclared *FieldRegistry
	declaredBy [][]FieldEntry
	// firstAgg is the index of the first aggregating command, or the number of
	// commands when none aggregates.
	firstAgg int
	// assignmentFields are the row-level assignment columns the scan projects, and
	// deferredAssignments those computed in the outer formatter.
	assignmentFields    []SelectExpr
	deferredAssignments []deferredAssignment
}

// scanWhere is the predicate the pipeline puts on its source scan beyond the time
// range and fractal scope: what the histogram and field statistics filter by, so
// they count exactly the events the results show.
func (q *plannedQuery) scanWhere() string {
	return andOf(q.ctx.Plan.SourceStage().Layer.Where[q.ctx.Plan.baseWhereLen:]...).sql
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
	plan := NewQueryPlan()
	opts.bindingHost = plan
	if opts.bindingWork == nil {
		n := bindingWorkBudget
		opts.bindingWork = &n
	}
	q := &plannedQuery{ctx: &CommandContext{Registry: sourceRegistry(opts), Plan: plan, Opts: opts, Pipeline: pipeline}}
	// The time range and fractal scope come first in the scan's WHERE.
	addBaseConditions(plan, opts)
	plan.baseWhereLen = len(plan.SourceStage().Layer.Where)
	for _, phase := range []func() error{q.declare, q.validate, q.prepare, q.execute, q.planFilters} {
		if err := phase(); err != nil {
			return nil, err
		}
	}
	return q, nil
}

// sourceRegistry is the registry a query starts from. A subquery source (a
// resolved source command) exposes flat columns, registered so bare references
// resolve to the column name rather than JSON access. Numeric columns register as
// Assignment, already numeric so never coerced (textToNumber rejects a
// non-String); string columns as Base.
func sourceRegistry(opts QueryOptions) *FieldRegistry {
	registry := NewFieldRegistry(opts.SourceMode, opts.IcePromoted)
	registry.normLogIndexHints = opts.NormLogIndexHints && opts.SourceMode != SourceIceberg && opts.SourceSubquery == ""
	for _, c := range opts.SourceColumns {
		if slices.Contains(opts.SourceNumericColumns, c) {
			registry.Register(c, FieldKindAssignment, c, -1)
			registry.SetResolveExpr(c, c)
		} else {
			registry.Register(c, FieldKindBase, c, -1)
		}
	}
	return registry
}

// declare has every command register the fields it produces, up front, into a
// look-ahead view (CommandContext.Declared) that checks reading the whole
// pipeline use. The registry itself stays as the scan leaves it: each command's
// entries are kept and enter it where the command runs.
func (q *plannedQuery) declare() error {
	ctx, pipeline := q.ctx, q.ctx.Pipeline
	commands := pipeline.Commands()
	live := ctx.Registry
	q.undeclared = live.Clone()
	q.declaredBy = make([][]FieldEntry, len(commands))
	q.firstAgg = firstAggregatingCommandIndex(commands)
	ctx.Registry = live.Clone()
	defer func() { ctx.Declared, ctx.Registry = ctx.Registry, live }()
	for i, cmd := range commands {
		ctx.CmdIndex = i
		handler := getCommandHandler(cmd.Name)
		if handler == nil {
			return fmt.Errorf("unsupported command: %s", cmd.Name)
		}
		before := ctx.Registry.Clone()
		if err := handler.Declare(cmd, ctx); err != nil {
			return err
		}
		for _, name := range ctx.Registry.changedSince(before) {
			q.declaredBy[i] = append(q.declaredBy[i], *ctx.Registry.Get(name))
		}
	}
	// Assignments enter the look-ahead view too; the registry gets them where
	// they are written.
	for _, a := range pipeline.Assignments() {
		ctx.Registry.Register(a.Field, FieldKindAssignment, a.Field, -1)
	}
	return nil
}

// validate refuses what the pipeline's commands cannot do, before any runs.
func (q *plannedQuery) validate() error {
	pipeline := q.ctx.Pipeline
	for _, check := range []func(*PipelineNode) error{validateCommandSchemas, validateProcessTreeTail, ValidatePgrFilters} {
		if err := check(pipeline); err != nil {
			return err
		}
	}
	return validateExprArgs(pipeline, q.ctx.Declared)
}

// prepare readies the plan for execute.
func (q *plannedQuery) prepare() error {
	ctx, pipeline := q.ctx, q.ctx.Pipeline
	// With no aggregation before it, the model join sits in the row scan's FROM, so
	// every command after it (an aggregation included) reads the model's columns
	// per row. After an aggregation it enriches the aggregated rows instead, joined
	// below the stage that reads them. Decided before Execute so the handlers route
	// model-column references accordingly.
	if mlIdx := lookupCommandIndex(pipeline.Commands()); mlIdx >= 0 {
		ctx.Plan.lookup.AtScan = q.firstAgg > mlIdx
	}
	// tlsh() is a condition, and the columns it projects come from the scan, so
	// they are there from the start.
	return projectTLSHColumns(pipeline, ctx)
}

// execute runs the commands and assignments in pipeline order, each reading the
// registry as the pipeline left it there. The registry at every position is kept
// for the filters written there.
func (q *plannedQuery) execute() error {
	ctx, plan, registry := q.ctx, q.ctx.Plan, q.ctx.Registry
	plan.pos.registry = map[position]*FieldRegistry{}
	plan.pos.outputs = map[int][]string{}
	plan.pos.stage = map[position]int{}
	plan.pos.declared = q.declaredBy
	plan.pos.firstAgg = q.firstAgg
	plan.pos.commands = ctx.Pipeline.Commands()
	capture := func(at position) {
		view := registry.Clone()
		view.rowLevel = at.Cmd <= q.firstAgg
		plan.pos.registry[at] = view
		plan.pos.stage[at] = len(plan.Stages) - 1
	}
	var pos position
	capture(pos)
	for _, st := range ctx.Pipeline.Steps {
		ctx.CmdIndex = pos.Cmd
		switch {
		case st.Assign != nil:
			if err := q.applyAssignment(*st.Assign, pos); err != nil {
				return err
			}
			pos.Assign++
		case st.Command != nil:
			if err := executeCommand(ctx, *st.Command, q.declaredBy[pos.Cmd], q.undeclared); err != nil {
				return err
			}
			pos.Cmd++
		default:
			continue
		}
		capture(pos)
	}
	ctx.CmdIndex = pos.Cmd
	return nil
}

// planFilters places every filter in the clause its stage owns. It runs after
// execute so the registry reflects every produced field: aggregate aliases from
// multi()/function=, and aggregates redefined across groupby stages, which
// Declare-time kinds alone misroute. The filters are compiled into a copy, since
// the parsed pipeline is planned more than once.
func (q *plannedQuery) planFilters() error {
	ctx, registry, plan := q.ctx, q.ctx.Registry, q.ctx.Plan
	conditions := plannedFilters(ctx.Pipeline)
	if err := resolveCommandConditions(conditions, registry, plan, ctx.Opts); err != nil {
		return err
	}
	if err := validatePostAggregationFields(ctx.Pipeline, registry, plan); err != nil {
		return err
	}
	if err := classifyConditions(conditions, registry, plan); err != nil {
		return err
	}
	materializeConditions(registry, plan)
	narrowPgrCandidates(ctx)
	return nil
}

// applyAssignment compiles an assignment where it is written.
func (q *plannedQuery) applyAssignment(assignment AssignmentNode, at position) error {
	ctx, registry := q.ctx, q.ctx.Registry
	safeField, err := sanitizeIdentifier(assignment.Field)
	if err != nil {
		return fmt.Errorf("invalid assignment field: %w", err)
	}
	hasAggregation := q.firstAgg < len(ctx.Plan.pos.commands)
	// Past the aggregation every assignment is its own stage, so what ran before it
	// keeps the value it replaces.
	if hasAggregation && at.Cmd > q.firstAgg {
		return applyAssignmentStage(ctx, assignment)
	}
	// A reassignment replaces the earlier column; filters before it already folded
	// the earlier expression in.
	q.assignmentFields = slices.DeleteFunc(q.assignmentFields, func(af SelectExpr) bool { return af.Name() == assignment.Field })
	q.deferredAssignments = slices.DeleteFunc(q.deferredAssignments, func(da deferredAssignment) bool { return da.Field == assignment.Field })
	layer := &ctx.Plan.CurrentStage().Layer
	layer.Selects = slices.DeleteFunc(layer.Selects, func(sel SelectExpr) bool { return sel.Name() == assignment.Field })

	sqlExpr, exprType, err := compileExpr(assignment.Expr, registry)
	if err != nil {
		return assignmentError(assignment.Field, err)
	}
	// A reference to the field is spliced beside other operators (x = 1, in(x, ...)),
	// so the registry holds it as one operand.
	ref := operandSQL(assignment.Expr, sqlExpr)
	switch {
	case !isComputedExpr(assignment.Expr):
		// A bare field or literal has nothing to compute and is materialized
		// straight away.
		q.assignmentFields = append(q.assignmentFields, selectAs(sqlExpr, safeField))
		registry.Register(safeField, FieldKindAssignment, safeField, -1)
		registry.SetResolveExpr(safeField, sqlExpr)
		if exprType == TypeText {
			registry.setText(safeField)
		}
		return nil
	case hasAggregation:
		// Before the aggregation it is inlined, so aggregations, filters and later
		// assignments referencing it fold in the full expression; it is never
		// materialized as a column.
		registry.RegisterInlineExpr(safeField, ref, -1)
	default:
		// Without an aggregation it is computed in the outer formatter, and
		// registered so a filter downstream folds the expression in rather than
		// reading a column the scan does not have.
		registry.Register(safeField, FieldKindPerRow, ref, -1)
		registry.SetResolveExpr(safeField, ref)
	}
	registry.setOperand(safeField)
	if exprType == TypeText {
		registry.setText(safeField)
	}
	if exprType == TypeNumber {
		registry.SetNumeric(safeField)
	}
	if hasAggregation {
		return nil
	}
	if registry.DependsOnJoin(safeField) {
		// The model's columns exist in the SELECT that joins them, which the
		// formatter only sees as far as that SELECT projects them.
		q.assignmentFields = append(q.assignmentFields, selectAs(sqlExpr, safeField))
		return nil
	}
	q.deferredAssignments = append(q.deferredAssignments, deferredAssignment{Field: assignment.Field, Alias: safeField, SQL: sqlExpr})
	return nil
}

// executeCommand runs one command at its position, reinstating what it declared,
// and records what the query returns after it.
func executeCommand(ctx *CommandContext, cmd CommandNode, declared []FieldEntry, undeclared *FieldRegistry) error {
	plan, registry, i := ctx.Plan, ctx.Registry, ctx.CmdIndex
	registry.reinstate(declared, undeclared)
	// A per-row transform that runs on a GROUP BY stage would add a
	// non-grouped column to an aggregate SELECT (invalid, silently dropped).
	// Move it onto a post-aggregation projection stage first.
	if IsTransformCommand(cmd.Name) && len(plan.CurrentStage().Layer.GroupBy) > 0 {
		if _, err := pushCarryForwardStage(ctx); err != nil {
			return fmt.Errorf("%s (post-aggregation stage): %w", cmd.Name, err)
		}
	}
	// A stats command after chain() aggregates the matched entities, not
	// the events inside each one.
	if plan.CurrentStage().IsChain && statsCommands[canonicalCommandName(cmd.Name)] {
		if err := pushAggregationStage(ctx); err != nil {
			return fmt.Errorf("%s (post-chain stage): %w", cmd.Name, err)
		}
	}
	whereLen := len(plan.SourceStage().Layer.Where)
	if err := getCommandHandler(cmd.Name).Execute(cmd, ctx); err != nil {
		return err
	}
	// A filter the command put on the scan applies to the rows a cap kept, and
	// one that reads the scan-level model join applies to the joined rows.
	if src := &plan.SourceStage().Layer; len(src.Where) > whereLen {
		added := slices.Clone(src.Where[whereLen:])
		src.Where = src.Where[:whereLen]
		after := src.capAfter(i)
		for _, w := range added {
			switch {
			case plan.lookup.AtScan && registry.SQLDependsOnJoin(w.sql):
				plan.lookup.PostJoinWhere = append(plan.lookup.PostJoinWhere, postJoinFilter{At: i + 1, Pred: w})
			case after != nil:
				*after = append(*after, w)
			default:
				src.Where = append(src.Where, w)
			}
		}
	}
	// A cap on aggregated rows bounds what follows, which reads only the rows it
	// kept: the next stage wraps it.
	if capsAggregatedRows(cmd, ctx) && pipelineContinuesAfter(ctx.Pipeline, i) {
		if _, err := pushCarryForwardStage(ctx); err != nil {
			return fmt.Errorf("%s (after the cap): %w", cmd.Name, err)
		}
		plan.CurrentStage().AfterCap = true
	}
	plan.pos.stageAfter = append(plan.pos.stageAfter, len(plan.Stages)-1)
	plan.pos.outputs[i] = plan.outputNames(registry)
	return nil
}

// plannedFilters copies the pipeline's filters for planning, each placed where
// it is written.
func plannedFilters(pipeline *PipelineNode) []Condition {
	var out []Condition
	pipeline.forEachStep(func(st Step, at position) {
		if st.Filter != nil {
			c := cloneConditions([]Condition{*st.Filter})[0]
			placeAt(&c, at)
			out = append(out, c)
		}
	})
	return out
}

// placeAt records where a condition and every condition beneath it is written.
func placeAt(c *Condition, at position) {
	c.at = at
	for i := range c.Children {
		placeAt(&c.Children[i], at)
	}
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
	return q.ctx.Plan.withClause(), andOf(q.ctx.Plan.SourceStage().Layer.Where...).sql, nil
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
	source.Layer.Where = append(source.Layer.Where, atoms(windowConditions(opts)...)...)
	if len(opts.EventTimes) > 0 {
		lits := make([]string, len(opts.EventTimes))
		for i, t := range opts.EventTimes {
			lits[i] = "'" + chTimeLiteral(t) + "'"
		}
		source.Layer.Where = append(source.Layer.Where, atom("timestamp IN ("+strings.Join(lits, ", ")+")"))
	}

	if opts.MaxEventLagSeconds > 0 {
		source.Layer.Where = append(source.Layer.Where,
			atom(fmt.Sprintf("dateDiff('second', timestamp, ingest_timestamp) <= %d", opts.MaxEventLagSeconds)))
	}

	if scope := fractalScopeCond(opts, ""); scope != "" {
		source.Layer.Where = append(source.Layer.Where, atom(scope))
	}
}

// scopeColumns are the columns the scan's scope guards read.
var scopeColumns = []string{"fractal_id", "timestamp", "ingest_timestamp", "log_id"}

// checkScopeNotShadowed refuses a scan projection named after a scope column.
// ClickHouse binds a WHERE identifier to a SELECT alias before the table column,
// so `'x' AS fractal_id` would turn the fractal guard into a test of that alias.
func checkScopeNotShadowed(selects []SelectExpr) error {
	for _, sel := range selects {
		if name := sel.Name(); sel.Alias != "" && slices.Contains(scopeColumns, name) {
			return fmt.Errorf("%s is a built-in column and cannot name an output; choose another name", name)
		}
	}
	return nil
}

// finalize assembles the planned query into its SQL: the last stage's projection,
// ordering and limit, the layers that wrap the stages, and the result's metadata.
func (q *plannedQuery) finalize() (*TranslationResult, error) {
	plan := q.ctx.Plan
	if err := q.foldCaps(); err != nil {
		return nil, err
	}
	if plan.special != nil {
		result, err := plan.special.translate(plan, q.ctx.Opts)
		if err != nil {
			return nil, err
		}
		result.SQL = plan.withClause() + result.SQL
		return result, nil
	}
	// Earlier stages were assembled when the stage after them was pushed.
	active := plan.CurrentStage()
	if err := q.assembleSelects(active); err != nil {
		return nil, err
	}
	defaultTimeOrder := q.orderAndLimit(active)
	deferOutOfScopeOrderBy(q.ctx, plan, active)
	// Export the source columns those deferred expressions reference.
	if err := plan.exportDeferredColumns(); err != nil {
		return nil, err
	}
	if len(plan.outer.selects) > 0 {
		buildChainedAggStages(plan)
	}
	fieldOrder := q.wrapLayers(q.formatOutput(active, defaultTimeOrder))
	sql, err := plan.Render(q.ctx.Opts)
	if err != nil {
		return nil, err
	}
	return &TranslationResult{
		SQL:                 sql,
		SourceWhere:         conjuncts(plan.SourceStage().Layer.Where),
		SourceWhereComplete: plan.sourceWhereComplete(),
		SourceProjections:   plan.sourceProjections(),
		FieldOrder:          fieldOrder,
		IsAggregated:        plan.IsAggregated || len(active.Layer.GroupBy) > 0,
		ChartType:           plan.chart.kind,
		ChartConfig:         plan.chart.config,
		DefaultTimeOrder:    defaultTimeOrder,
		TimeScopedSubquery:  plan.join != nil || plan.hasModelLookup() || plan.usesBindingSet,
		RowCapped:           plan.join != nil || plan.usesBindingSet,
		RangeDependentRows:  plan.rowLimitByCommand || len(plan.window.layers) > 0,
		Chain:               plan.Chain,
	}, nil
}

// foldCaps renders a last row cap nothing reads through as the scan's own order
// and limit, and records that a command bounded the rows.
func (q *plannedQuery) foldCaps() error {
	plan := q.ctx.Plan
	layer := &plan.SourceStage().Layer
	readThrough := plan.IsAggregated || plan.Chain != nil
	if n := len(layer.Caps); n > 0 {
		readThrough = readThrough || plan.readsThroughCap(layer.Caps[n-1])
	}
	layer.foldLastCap(readThrough)
	if layer.LimitBy != "" || layer.ScanLimitBy != "" {
		plan.rowLimitByCommand = true
	}
	if len(layer.Caps) == 0 {
		return nil
	}
	if plan.special != nil {
		return fmt.Errorf("head(), tail(), limit() and dedup() can only be followed by a transform here; move the filter before them")
	}
	plan.rowLimitByCommand = true
	// Shown in the cap's order unless something after it sorts.
	if last := layer.Caps[len(layer.Caps)-1]; len(layer.OrderBy) == 0 && !plan.IsAggregated {
		layer.OrderBy = last.OrderBy
	}
	return nil
}

// assembleSelects builds the active stage's SELECT list.
func (q *plannedQuery) assembleSelects(active *QueryStage) error {
	if len(active.Layer.GroupBy) > 0 {
		if err := assembleGroupBySelects(q.ctx, active, q.assignmentFields); err != nil {
			return err
		}
	} else if active.IsSource {
		assembleNonGroupBySelects(q.ctx, active, q.assignmentFields)
	}
	// Deferred assignments run in the formatter's outer SELECT, and the JSON
	// sub-columns they read need `fields` in the inner one.
	if len(q.deferredAssignments) > 0 && !q.ctx.Plan.IsAggregated &&
		!hasSelectAlias(active.Layer.Selects, "fields") {
		active.Layer.Selects = append(active.Layer.Selects, SelectExpr{Expr: "fields"})
	}
	return nil
}

// orderAndLimit sets the active stage's default order and the caller's row limit,
// reporting whether the order is the implicit newest-first one.
func (q *plannedQuery) orderAndLimit(active *QueryStage) (defaultTimeOrder bool) {
	plan, opts, layer := q.ctx.Plan, q.ctx.Opts, &active.Layer
	if len(layer.OrderBy) == 0 && len(layer.GroupBy) == 0 && !plan.IsAggregated && !plan.chart.readsAllRows {
		if len(opts.SourceOrderBy) > 0 {
			layer.OrderBy = opts.SourceOrderBy
		} else if active.IsSource {
			// log_id breaks ties: it completes the logs sorting key (timestamp, log_id),
			// so ClickHouse still reads in reverse key order, and without it the rows
			// that survive the LIMIT at the boundary timestamp are arbitrary -- which
			// also drifts the cursor page boundary between runs.
			layer.OrderBy = []OrderTerm{desc("timestamp"), desc("log_id")}
			defaultTimeOrder = true
		}
	}
	// A chained aggregate (groupby | ... | avg(x)) reads every group, so the display
	// limit belongs on its single result row, not on its input.
	capRows := opts.MaxRows > 0 && !plan.chart.readsAllRows && len(plan.outer.selects) == 0
	if layer.Limit == "" {
		if capRows {
			layer.Limit = fmt.Sprintf("LIMIT %d", opts.MaxRows)
		}
		return defaultTimeOrder
	}
	// A command bounded the rows, not the caller's MaxRows. That is row selection
	// rather than presentation, so a caller reading only the scan predicates would
	// read past it.
	plan.rowLimitByCommand = true
	// The command may narrow the caller's cap but not lift it: the caller buffers
	// every returned row, so head(999999999) would load them all.
	var n int
	if _, err := fmt.Sscanf(layer.Limit, "LIMIT %d", &n); capRows && err == nil && n > opts.MaxRows {
		layer.Limit = fmt.Sprintf("LIMIT %d", opts.MaxRows)
	}
	return defaultTimeOrder
}

// formatOutput builds the outer formatter SELECT and returns the columns shown.
func (q *plannedQuery) formatOutput(active *QueryStage, defaultTimeOrder bool) []string {
	plan := q.ctx.Plan
	// A chained aggregation is not wrapped by a formatter.
	if len(plan.outer.selects) > 0 {
		return columnNames(plan.outer.selects)
	}
	plan.format.selects = buildFormatters(active.Layer.Selects, q.deferredAssignments)
	fieldOrder := computeFieldOrder(active.Layer.Selects, q.deferredAssignments)
	// Columns exported purely to make deferred expressions resolvable are stripped
	// from the SQL result, so they must not be listed for display.
	fieldOrder = dropHiddenDeferredFields(fieldOrder, plan)
	// chain()'s internal columns stay in the result data but out of the display.
	if plan.Chain != nil {
		fieldOrder = dropField(fieldOrder, chainAnchorColumn)
		fieldOrder = dropField(fieldOrder, chainDoneColumn)
	}
	// With the default newest-first order, the formatter repeats the ORDER BY and
	// LIMIT so the output stays ordered after projection. The source keeps its own:
	// the formatter projects toString(timestamp) AS timestamp, so an outer ORDER BY
	// alone binds to that String alias rather than the DateTime64 sorting key, which
	// disables optimize_read_in_order and scans the whole range however small the
	// LIMIT (measured on a 2M-row day: 130k rows read instead of 2M). On a cluster it
	// also lets each shard limit its own output before the coordinator merges.
	// Without a LIMIT the outer sort would re-sort every row on the String alias,
	// and a join() renders no formatter.
	if defaultTimeOrder && active.Layer.Limit != "" && len(plan.format.selects) > 0 && plan.join == nil {
		plan.format.orderBy = active.Layer.OrderBy
		plan.format.limit = active.Layer.Limit
	}
	// table(...) shows the columns it names. Every row still carries the ones
	// projected for its identity, content and window value, which are only
	// displayed if named.
	if plan.table.explicit {
		fieldOrder = slices.DeleteFunc(fieldOrder, func(f string) bool {
			return (rowIdentityColumns[f] || f == windowValueColumn) && !slices.Contains(plan.table.columns, f)
		})
	}
	return fieldOrder
}

// wrapLayers adds the layers that wrap the stages (join, z-score, histogram and
// chart) and returns the columns shown once they have.
func (q *plannedQuery) wrapLayers(fieldOrder []string) []string {
	plan := q.ctx.Plan
	// join() renders no formatter, so the columns its wrapper adds pass through to
	// the display and to any filter deferred above the join. An explicit table()
	// shows only the joined outputs it names; the wrap still projects them all,
	// since a deferred filter or sort may read one that is not displayed.
	if plan.join != nil && plan.join.SubSQL != "" {
		plan.format.selects = nil
		fieldOrder = appendJoinedFields(fieldOrder, joinDisplayNames(plan), plan)
	}
	if plan.window.zScoreExpr != "" {
		buildZScoreWindowLayers(plan)
		fieldOrder = append(fieldOrder, "_median", "_mad", "_modified_z")
		if plan.window.outlierThreshold != "" {
			fieldOrder = append(fieldOrder, "_is_outlier")
		}
	}
	if plan.histogram.buckets > 0 {
		buildHistogramLayers(plan, q.ctx)
		fieldOrder = []string{"_bin_lower", "_bin_upper", "_bin_count"}
		plan.IsAggregated = true
	}
	if len(plan.chart.layers) > 0 {
		plan.window.layers = append(plan.window.layers, plan.chart.layers...)
		fieldOrder = plan.chart.fieldOrder
		plan.IsAggregated = true
	}
	return fieldOrder
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
	ctx.Plan.outer.selects, ctx.Plan.outer.reads = nil, nil
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
	for _, name := range columnNames(prevStage.Layer.Selects) {
		if !slices.Contains(carried, name) {
			carried = append(carried, name)
		}
	}

	ctx.Plan.PushStage()
	ctx.Plan.aggregationOutputs = make(map[string]string)
	ctx.Plan.outer.selects, ctx.Plan.outer.reads = nil, nil

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

	ctx.Plan.CurrentStage().Layer.project(selectAs(sqlExpr, safeField))

	// The computed value is now a materialized column of this stage; references
	// resolve to its alias.
	ctx.Registry.Register(safeField, FieldKindAssignment, safeField, ctx.CmdIndex)
	ctx.Registry.SetResolveExpr(safeField, safeField)
	return nil
}

// assembleGroupBySelects builds the SELECT of a GROUP BY stage: each key as a
// named column, then the aggregates, then the row-level assignments.
func assembleGroupBySelects(ctx *CommandContext, source *QueryStage, assignments []SelectExpr) error {
	existing := source.Layer.Selects
	var selects []SelectExpr
	added := make(map[string]bool)
	add := func(sel SelectExpr) {
		selects = append(selects, sel)
		added[sel.Name()] = true
	}

	// 1. The grouped keys, each read by its output name.
	for i, key := range source.Layer.GroupBy {
		// A SELECT already producing this key, by name or as its expression.
		j := slices.IndexFunc(existing, func(sel SelectExpr) bool { return sel.Name() == key.Name })
		if j < 0 {
			j = slices.IndexFunc(existing, func(sel SelectExpr) bool { return sel.Alias != "" && sel.Expr == key.Expr })
		}
		if j >= 0 {
			add(existing[j])
			source.Layer.GroupBy[i].Expr = outputRef(existing[j])
			continue
		}
		expr := key.Expr
		// A per-row transform or assignment registered during Execute.
		if entry := ctx.Registry.Get(key.Expr); entry != nil && (entry.Kind == FieldKindPerRow || entry.Kind == FieldKindAssignment) {
			expr = ctx.Registry.Resolve(key.Expr)
		}
		// Raw JSON subcolumns are cast to ::String so the grouped column is
		// groupable even when the path is stored as Dynamic (rows ingested before
		// the field was type-hinted); a bare Dynamic ref triggers ClickHouse error 44.
		safe, err := sanitizeIdentifier(key.Name)
		if err != nil {
			return fmt.Errorf("groupBy: %w", err)
		}
		add(selectAs(expr, safe))
		source.Layer.GroupBy[i].Expr = safe
	}

	// 2. The aggregates. Selects the registry cannot classify are kept as a
	// fallback for handlers (table, bucket, heatmap, etc.) that add aggregation
	// selects directly without registering them. One the registry knows holds a
	// per-row value (a base column, or a transform output such as lowercase, len,
	// logSize) is dropped instead: past this point it is neither grouped nor
	// aggregated, so projecting it fails with ClickHouse code 215.
	hasExplicitAgg := false
	for _, sel := range existing {
		name := sel.Name()
		if added[name] {
			continue
		}
		_, inAggOutputs := ctx.Plan.aggregationOutputs[name]
		if inAggOutputs || ctx.Registry.IsAggregate(name) || !ctx.Registry.IsRowLevel(name) {
			add(sel)
			hasExplicitAgg = true
		}
	}

	// 3. The aggregates a chained outer aggregation reads.
	if len(ctx.Plan.outer.selects) > 0 {
		outer := columnNames(ctx.Plan.outer.selects)
		for _, name := range slices.Sorted(maps.Keys(ctx.Plan.aggregationOutputs)) {
			if slices.Contains(outer, name) || added[name] {
				continue
			}
			if slices.Contains(ctx.Plan.outer.reads, name) {
				add(selectAs(ctx.Plan.aggregationOutputs[name], name))
				hasExplicitAgg = true
			}
		}
	}

	// 4. COUNT(*) when nothing else aggregates.
	if !hasExplicitAgg && (!ctx.Plan.IsAggregated || len(ctx.Plan.outer.selects) > 0) {
		add(aggSelect(countAll, "_count"))
	}

	// 5. The assignments.
	for _, a := range assignments {
		if !added[a.Name()] {
			selects = append(selects, a)
		}
	}
	source.Layer.Selects = selects
	return nil
}

// appendIngestTime projects ingest_timestamp under its hidden alias, once. Only the
// non-aggregated logs-source paths call it: after a GROUP BY the column is neither
// grouped nor aggregated (code 215), and a pgr() subquery source lacks it (code 47).
func appendIngestTime(ctx *CommandContext, source *QueryStage) {
	if !ctx.Opts.IncludeIngestTime {
		return
	}
	// Already added, or the query binds the bare name itself (an explicit
	// | table(ingest_timestamp), or an eval() that rebinds it). A second reference
	// would resolve to that alias rather than to the column, so leave it out rather
	// than carry a value that is not the ingest time.
	if hasSelectAlias(source.Layer.Selects, ingestTimeColumn) || hasSelectAlias(source.Layer.Selects, "ingest_timestamp") {
		return
	}
	source.Layer.Selects = append(source.Layer.Selects,
		selectAs("ingest_timestamp", ingestTimeColumn))
}

// assembleNonGroupBySelects builds the scan's SELECT list for a query without a
// GROUP BY.
func assembleNonGroupBySelects(ctx *CommandContext, source *QueryStage, assignmentFields []SelectExpr) {
	plan, layer := ctx.Plan, &source.Layer
	// A subquery source (a resolved source command like pgr()) projects its flat
	// columns, never the logs table's defaults. Explicit table() columns are set by
	// tableHandler (already bare); aggregate/select handlers set their own Selects.
	if ctx.Opts.SourceSubquery != "" {
		if !plan.IsAggregated {
			if !(plan.table.present && plan.table.explicit && len(layer.Selects) > 0) {
				layer.Selects = sourceColumnSelects(ctx.Opts.SourceColumns)
			}
			layer.addAssignments(assignmentFields)
		}
		return
	}
	if len(ctx.Pipeline.Commands()) == 0 {
		// Columns a filter projects (tlsh()) follow the row's own.
		projected := layer.Selects
		defer func() { layer.addAssignments(projected) }()
		if len(ctx.Pipeline.Assignments()) == 0 && minimalProjection(ctx, layer) {
			return
		}
		layer.Selects = []SelectExpr{{Expr: "timestamp"}, {Expr: normLogColumn}, {Expr: "log_id"}, {Expr: "fractal_id"}}
		layer.Selects = append(layer.Selects, assignmentFields...)
		if ctx.Opts.IncludeShardNum {
			layer.Selects = append(layer.Selects, SelectExpr{Expr: "toString(_shard_num)", Alias: "_shard_num"})
		}
		appendIngestTime(ctx, source)
		return
	}
	// Aggregated (| len(raw_log) | sum(_len)), per-row computed fields such as _len
	// are already inlined into the aggregate expressions, and mixing them in as
	// columns would be invalid.
	if plan.IsAggregated {
		layer.Selects = slices.DeleteFunc(layer.Selects, func(sel SelectExpr) bool {
			_, isAgg := plan.aggregationOutputs[sel.Name()]
			return ctx.Registry.IsPerRowOrAssignment(sel.Name()) && !isAgg
		})
	}
	// table() without columns shows every field.
	if plan.table.present && !plan.table.explicit && len(layer.Selects) > 0 &&
		!slices.ContainsFunc(layer.Selects, func(sel SelectExpr) bool { return sel.Name() == "_all_fields" }) {
		layer.Selects = append(layer.Selects, SelectExpr{Expr: contentColMode(ctx.Opts.SourceMode), Alias: "_all_fields"})
	}
	layer.addAssignments(assignmentFields)
	if !plan.IsAggregated {
		ensureRowColumns(ctx, source)
	}
}

// minimalProjection projects only the fields a filter-only alert query reads, plus
// log_id and timestamp, and reports whether it did.
func minimalProjection(ctx *CommandContext, layer *QueryLayer) bool {
	if !ctx.Opts.MinimalProjection || ctx.Opts.SourceMode == SourceIceberg || len(ctx.Pipeline.Filters()) == 0 {
		return false
	}
	fields := map[string]bool{}
	for _, f := range conditionFields(ctx.Pipeline.Filters()...) {
		fields[f] = true
	}
	// Include alert-configured extra fields (throttle field, template fields).
	for _, f := range ctx.Opts.AlertExtraFields {
		if f != "" {
			fields[f] = true
		}
	}
	if fields["*"] {
		return false
	}
	for _, base := range []string{"raw_log", normLogColumn, "timestamp", "log_id", "fractal_id", "ingest_timestamp"} {
		delete(fields, base)
	}
	layer.Selects = []SelectExpr{{Expr: "timestamp"}, {Expr: "log_id"}, {Expr: "fractal_id"}}
	for _, field := range slices.Sorted(maps.Keys(fields)) {
		// These names include alert-configured ones (throttle field, name template),
		// which are not query text and are not constrained by the lexer. An unescaped
		// backtick closed the alias and appended a select expression of the caller's
		// choosing, so the name is both checked and escaped: the outer projection
		// re-derives column names by string-parsing this expression, which escaping
		// alone cannot make safe.
		if !isPlainFieldName(field) {
			continue
		}
		layer.Selects = append(layer.Selects, selectAs(jsonFieldRef(field), "`"+EscapeCHBacktickIdent(field)+"`"))
	}
	return true
}

// ensureRowColumns adds the columns every row of a row-level result carries: its
// identity, its content unless table() picks the columns, the scan-level model
// join's columns, and the shard and ingest time when asked for.
func ensureRowColumns(ctx *CommandContext, source *QueryStage) {
	plan, layer := ctx.Plan, &source.Layer
	ensure := func(sel SelectExpr) {
		if !hasSelectAlias(layer.Selects, sel.Name()) {
			layer.Selects = append(layer.Selects, sel)
		}
	}
	ensure(SelectExpr{Expr: "timestamp"})
	ensure(SelectExpr{Expr: "log_id"})
	ensure(SelectExpr{Expr: "fractal_id"})
	// norm_log (flat normalized fields) is the default content column. raw_log
	// (original) is not projected; the Raw tab lazy-loads it within its retention.
	if !plan.table.explicit {
		ensure(SelectExpr{Expr: normLogColumn})
	}
	hasFields := slices.ContainsFunc(layer.Selects, func(sel SelectExpr) bool {
		return sel.Name() == "fields" || sel.Name() == "_all_fields"
	})
	if !hasFields && plan.table.present && !plan.table.explicit {
		layer.Selects = append(layer.Selects, SelectExpr{Expr: contentColMode(ctx.Opts.SourceMode), Alias: "fields"})
	}
	// The scan-level model join's columns, after the log's own. An explicit table()
	// projects the ones it names itself.
	if plan.hasModelLookup() && plan.lookup.AtScan && !plan.table.explicit {
		for _, f := range plan.lookup.Fields {
			layer.project(SelectExpr{Expr: f})
		}
	}
	if ctx.Opts.IncludeShardNum {
		ensure(selectAs("toString(_shard_num)", "_shard_num"))
	}
	appendIngestTime(ctx, source)
}

// hasSelectAlias reports whether a select list names a column.
func hasSelectAlias(selects []SelectExpr, name string) bool {
	return slices.ContainsFunc(selects, func(sel SelectExpr) bool { return sel.Name() == name })
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
	hasJoinWrap := plan.join != nil && plan.join.SubSQL != ""
	if plan.window.zScoreExpr == "" && !hasJoinWrap {
		return
	}

	var innerOrder []OrderTerm
	var deferredOrder []OrderTerm

	for _, ob := range source.Layer.OrderBy {
		if ctx.Registry.IsWindow(ob.Expr) || (hasJoinWrap && isJoinedField(ctx.Registry, ob.Expr)) {
			deferredOrder = append(deferredOrder, ob)
		} else {
			innerOrder = append(innerOrder, ob)
		}
	}

	liftLimit := len(deferredOrder) > 0 || (hasJoinWrap && len(plan.deferred.where) > 0)
	// Without a key prefilter the scan would cap rows the inner join then drops.
	liftLimit = liftLimit || (plan.innerJoinWrap() && plan.joinPrefilter(source, ctx.Opts).empty())
	if liftLimit && hasJoinWrap {
		// The ordering must follow the filter, so it moves out with the LIMIT.
		deferredOrder = append(deferredOrder, innerOrder...)
		innerOrder = nil
	}

	source.Layer.OrderBy = innerOrder
	if hasJoinWrap && !liftLimit {
		// The scan already selects the rows the join keeps (see joinPrefilter), so
		// its cap stays where the scan can stop early. A join's output order is
		// unspecified and a key the subquery repeats multiplies rows, so the joined
		// rows are ordered and capped again.
		for _, ob := range innerOrder {
			plan.deferred.orderBy = append(plan.deferred.orderBy, scopeOrderByExpr(ob, plan.deferredScope()))
		}
		plan.deferred.limit = source.Layer.Limit
		return
	}
	if liftLimit {
		// Relocated ORDER BY expressions face the same scope limit as relocated
		// filters: a `sort(image)` carried past the join must read the exported
		// hidden column, not the source-only `fields.image`.
		for i, ob := range deferredOrder {
			deferredOrder[i] = scopeOrderByExpr(ob, plan.deferredScope())
		}
		plan.deferred.orderBy = append(plan.deferred.orderBy, deferredOrder...)
		plan.deferred.limit = source.Layer.Limit
		source.Layer.Limit = ""
	}
}

// dropHiddenDeferredFields removes the deferred scope's exported columns from the
// display order; they are EXCEPT-ed out of the result set.
func dropHiddenDeferredFields(fieldOrder []string, plan *QueryPlan) []string {
	if !plan.deferred.scope.used() {
		return fieldOrder
	}
	hidden := make(map[string]bool, len(plan.deferred.scope.names))
	for _, n := range plan.deferred.scope.names {
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
func scopeOrderByExpr(o OrderTerm, scope *deferredScope) OrderTerm {
	return OrderTerm{Expr: scope.ref(o.Expr, ""), Desc: o.Desc}
}

// appendJoinedFields adds a JOIN wrap's output columns to the display order,
// narrowed to what an explicit `| table(...)` asked for.
func appendJoinedFields(fieldOrder, outputs []string, plan *QueryPlan) []string {
	if plan.table.explicit {
		outputs = plan.table.joinedFields
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
	if plan.outer.madExpr != "" {
		plan.Stages = append(plan.Stages, QueryStage{
			Layer: QueryLayer{
				Selects: []SelectExpr{{Expr: "*"}, selectAs(fmt.Sprintf("median(%s) OVER ()", plan.outer.madExpr), "_median_val")},
			},
		})
	}

	// Add the outer aggregation stage
	plan.Stages = append(plan.Stages, QueryStage{Layer: QueryLayer{Selects: plan.outer.selects}})
}

// buildZScoreWindowLayers creates window layers for modified z-score computation:
// Layer 1: median(expr) OVER () AS _median
// Layer 2: median(abs(expr - _median)) OVER () AS _mad
// Layer 3: z-score calculation + optional outlier flag
func buildZScoreWindowLayers(plan *QueryPlan) {
	expr := plan.window.zScoreExpr
	all := SelectExpr{Expr: "*"}
	// The median, then the median absolute deviation from it, then the modified
	// z-score and, with a threshold, whether the row is an outlier.
	scored := []SelectExpr{all, selectAs(fmt.Sprintf("if(_mad = 0, 0, round(0.6745 * (%s - _median) / _mad, 4))", expr), "_modified_z")}
	if plan.window.outlierThreshold != "" {
		scored = append(scored, selectAs(fmt.Sprintf("toString(if(abs(_modified_z) > %s, 1, 0))", plan.window.outlierThreshold), "_is_outlier"))
	}
	plan.window.layers = append(plan.window.layers,
		QueryLayer{Selects: []SelectExpr{all, selectAs(fmt.Sprintf("median(%s) OVER ()", expr), "_median")}},
		QueryLayer{Selects: []SelectExpr{all, selectAs(fmt.Sprintf("median(abs(%s - _median)) OVER ()", expr), "_mad")}},
		QueryLayer{Selects: scored},
	)
}

// buildHistogramLayers creates window layers for histogram bucketing:
// Layer 1: compute val, min, max using window functions
// Layer 2: bucket, aggregate, and order
func buildHistogramLayers(plan *QueryPlan, ctx *CommandContext) {
	valExpr := plan.histogram.valueExpr
	if valExpr == "" {
		valExpr = "_hist_val"
	}

	buckets := plan.histogram.buckets
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
	plan.window.layers = append(plan.window.layers, QueryLayer{
		Selects: []SelectExpr{
			selectAs(valExpr, "_val"),
			selectAs(fmt.Sprintf("min(%s) OVER ()", valExpr), "_min_val"),
			selectAs(fmt.Sprintf("max(%s) OVER ()", valExpr), "_max_val"),
		},
	})

	// Layer 2: bucketing with GROUP BY
	plan.window.layers = append(plan.window.layers, QueryLayer{
		Selects: []SelectExpr{
			{Expr: lowerExpr, Alias: "_bin_lower"},
			{Expr: upperExpr, Alias: "_bin_upper"},
			{Expr: "count(*)", Alias: "_bin_count"},
		},
		Where:   atoms("_val IS NOT NULL"),
		GroupBy: []groupKey{{Expr: bucketExpr + " AS _bucket", Name: "_bucket"}, {Expr: "_min_val", Name: "_min_val"}, {Expr: "_max_val", Name: "_max_val"}},
		OrderBy: []OrderTerm{asc("_bin_lower")},
	})
}

// deferredAssignment is a computed assignment evaluated in the outer formatter,
// over the limited rows, as compiled where it is written.
type deferredAssignment struct {
	Field, Alias, SQL string
}

// buildFormatters creates the outer SELECT expressions for timestamp formatting
// and deferred math assignments.
func buildFormatters(selects []SelectExpr, deferredAssignments []deferredAssignment) []SelectExpr {
	if len(selects) == 0 {
		return []SelectExpr{selectAs("toString(timestamp)", "timestamp"), {Expr: "log_id"}, {Expr: "norm_log", Alias: "fields"}}
	}
	var formatters []SelectExpr
	for _, sel := range selects {
		switch {
		case sel.Name() == "timestamp":
			formatters = append(formatters, selectAs("toString(timestamp)", "timestamp"))
		case sel.Expr == "fields" && sel.Alias == "":
			// The raw JSON `fields` column is kept in the inner SELECT only so
			// deferred math assignments can reference fields.`x`. The ClickHouse
			// driver cannot scan the native JSON type, so it must never be
			// projected to the final result. (Table commands use the explicit
			// toString(fields) form, which is unaffected by this guard.)
		default:
			formatters = append(formatters, SelectExpr{Expr: outputRef(sel)})
		}
	}
	projected := columnNames(selects)
	for _, da := range deferredAssignments {
		// A later command may already project this column. Recomputing it here
		// would alias two expressions to one name (ClickHouse code 179).
		if !slices.Contains(projected, da.Field) {
			formatters = append(formatters, SelectExpr{Expr: da.SQL, Alias: da.Alias})
		}
	}
	return formatters
}

// outputRef is how a query over the column's SELECT references it: its alias, or
// the column itself.
func outputRef(sel SelectExpr) string {
	if sel.Alias != "" {
		return sel.Alias
	}
	return sel.Expr
}

// computeFieldOrder lists the columns a result shows, in order.
func computeFieldOrder(selects []SelectExpr, deferredAssignments []deferredAssignment) []string {
	fieldOrder := make([]string, 0, len(selects))
	for _, sel := range selects {
		switch name := sel.Name(); name {
		case "_all_fields", "fields", "_shard_num", ingestTimeColumn:
		default:
			fieldOrder = append(fieldOrder, name)
		}
	}
	for _, da := range deferredAssignments {
		if !slices.Contains(fieldOrder, da.Field) {
			fieldOrder = append(fieldOrder, da.Field)
		}
	}
	return fieldOrder
}

// capsAggregatedRows reports whether cmd, just run, capped the rows of an
// aggregated stage.
func capsAggregatedRows(cmd CommandNode, ctx *CommandContext) bool {
	switch canonicalCommandName(cmd.Name) {
	case "head", "tail", "limit", "dedup":
		return !onScan(ctx) && isAggregatedStage(ctx.Plan, ctx.Plan.CurrentStage())
	}
	return false
}

// pipelineContinuesAfter reports whether a command, assignment or filter follows
// command i.
func pipelineContinuesAfter(pipeline *PipelineNode, i int) bool {
	seen := -1
	for _, st := range pipeline.Steps {
		if seen == i {
			return true
		}
		if st.Command != nil {
			seen++
		}
	}
	return false
}
