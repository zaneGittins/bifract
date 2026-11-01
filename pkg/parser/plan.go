package parser

import (
	"fmt"
	"slices"
	"strings"
)

// SelectExpr represents a single expression in a SELECT clause with an optional alias.
type SelectExpr struct {
	Expr  string
	Alias string
}

// String returns the SQL representation: "Expr AS Alias" or just "Expr".
func (s SelectExpr) String() string {
	if s.Alias != "" && s.Alias != s.Expr {
		return s.Expr + " AS " + s.Alias
	}
	return s.Expr
}

// QueryLayer holds the parts of a single SQL query level.
type QueryLayer struct {
	Selects []SelectExpr
	Where   []string
	GroupBy []string
	Having  []string
	OrderBy []string
	LimitBy string
	// ScanLimitBy is a dedup that must collapse rows BEFORE this layer reads
	// them. ClickHouse applies LIMIT BY after GROUP BY, so a dedup written into
	// an aggregating SELECT would both fail (the key is not an output column)
	// and mean the wrong thing (dedup of the groups, not of the rows counted).
	// Rendering moves it into a subquery around the scan when the layer
	// aggregates, and emits it exactly like LimitBy when it does not.
	ScanLimitBy string
	Limit       string
	// Caps are the row caps (head, tail, limit, dedup) on the scan, in pipeline
	// order. Where holds the filters before the first; the rest of the layer
	// reads only the rows the last one keeps.
	Caps []RowCap
	// ModelJoin, on a stage after an aggregation, joins the model onto the rows the
	// stage reads: "<join> ON <keys>", the left side aliased _outer, adding the
	// ModelJoinFields columns. See modelJoinLayer.
	ModelJoin       string
	ModelJoinFields []string
}

// project adds a column, replacing one of the same name in place.
func (l *QueryLayer) project(sel string) {
	for i, s := range l.Selects {
		if selectName(s.String()) == selectName(sel) {
			l.Selects[i] = SelectExpr{Expr: sel}
			return
		}
	}
	l.Selects = append(l.Selects, SelectExpr{Expr: sel})
}

// addAssignments projects assignment columns a command after them has not
// already replaced.
func (l *QueryLayer) addAssignments(assignmentFields []string) {
	for _, af := range assignmentFields {
		if !slices.ContainsFunc(l.Selects, func(s SelectExpr) bool { return selectName(s.String()) == selectName(af) }) {
			l.Selects = append(l.Selects, SelectExpr{Expr: af})
		}
	}
}

// RowCap is a row selection the rest of the pipeline reads through, rendered as
// a subquery around the scan.
type RowCap struct {
	At      int // index of the capping command
	OrderBy []string
	LimitBy string
	// ScanLimitBy is LimitBy over the raw scan, "" when a key exists only as a
	// computed column.
	ScanLimitBy string
	Limit       string
	After       []string // filters written after this cap and before the next
}

// capAfter is the filter list for a position k commands in: the After of the last
// cap before it, or nil ahead of every cap.
func (l *QueryLayer) capAfter(k int) *[]string {
	for i := len(l.Caps) - 1; i >= 0; i-- {
		if l.Caps[i].At < k {
			return &l.Caps[i].After
		}
	}
	return nil
}

// foldLastCap renders a cap nothing reads through as this layer's own ordering
// and limit, which is the same rows without a subquery.
func (l *QueryLayer) foldLastCap(readThrough bool) {
	n := len(l.Caps)
	if n == 0 || readThrough {
		return
	}
	last := l.Caps[n-1]
	if len(last.After) > 0 || len(l.GroupBy) > 0 || len(l.Having) > 0 || len(l.OrderBy) > 0 ||
		l.Limit != "" || l.LimitBy != "" || l.ScanLimitBy != "" {
		return
	}
	l.OrderBy, l.LimitBy, l.Limit = last.OrderBy, last.LimitBy, last.Limit
	l.Caps = l.Caps[:n-1]
}

// UpsertSelect adds expr to the select list, replacing any existing entry with
// the same output alias. This enforces last-write-wins semantics: when a pipeline
// transform overwrites a field produced by an earlier command in the same stage
// (e.g. regex creates "download_domain", then lowercase(download_domain) updates
// it), the SELECT clause contains exactly one entry for that alias, preventing
// ClickHouse error 179 (multiple expressions for the same alias).
//
// If expr has no extractable alias (bare column reference, no AS clause), it is
// always appended.
func (l *QueryLayer) UpsertSelect(expr SelectExpr) {
	alias := expr.Alias
	if alias == "" {
		alias = extractFieldAlias(expr.Expr)
		if alias == expr.Expr {
			// No AS clause found; bare expression — always append.
			l.Selects = append(l.Selects, expr)
			return
		}
	}
	for i, sel := range l.Selects {
		existing := sel.Alias
		if existing == "" {
			existing = extractFieldAlias(sel.Expr)
		}
		if existing == alias {
			l.Selects[i] = expr
			return
		}
	}
	l.Selects = append(l.Selects, expr)
}

// QueryStage represents one aggregation stage in the pipeline.
// For now there are at most 2 (inner + one chained aggregation).
// The structure supports N stages for future multi-groupby pipelines.
type QueryStage struct {
	Layer    QueryLayer
	IsSource bool // true for the innermost stage (FROM logs)
	// IsChain marks the stage chain() aggregates in. Its columns are fixed, so a
	// later stats command aggregates over its rows in a stage of its own.
	IsChain bool
	// Aggregates marks a stage pushed to aggregate the previous stage's rows,
	// which may have no GROUP BY (count() after groupby() is one COUNT(*)).
	Aggregates bool
}

// QueryPlan holds the structured representation of a query before rendering to SQL.
type QueryPlan struct {
	Stages []QueryStage // pipeline of aggregation stages (first = innermost)
	// stageAfterCmd[i] is the current stage index once command i has executed.
	stageAfterCmd    []int
	WindowLayers     []QueryLayer // z-score, histogram wrapping (applied after all stages)
	DeferredWhere    []string     // conditions on window fields
	DeferredOrder    []string     // ORDER BY on window fields
	DeferredLimit    string       // LIMIT when sorting by window fields
	Formatters       []SelectExpr // outer SELECT: timestamp formatting, deferred math
	FormatterOrderBy []string     // ORDER BY lifted to formatter outer SELECT for streaming
	FormatterLimit   string       // LIMIT lifted to formatter outer SELECT for streaming

	FieldOrder   []string
	IsAggregated bool
	HasGroupBy   bool
	GroupByCount int // number of groupby commands encountered (for multi-groupby)
	ChartType    string
	ChartConfig  map[string]interface{}

	// Special query modes (generate entirely different SQL)
	IsAnalyze bool
	IsChain   bool
	Chain     *ChainMeta

	// Process-tree (ptg) fields: MV-backed process lineage traversal over proc_lineage.
	IsProcessTree    bool
	ProcessTreeStart string

	// AnalyzeFields-specific fields
	AnalyzeFieldsList      []string
	AnalyzeFieldsScanLimit int

	// rowLimitByCommand records that a command (head, limit, dedup, a
	// limit= option) bounded the row set, rather than the caller's MaxRows.
	rowLimitByCommand bool

	// usesBindingSet records that a result-set binding was rendered. Like join(),
	// its subquery carries its own copy of the time bounds, so a caller must not
	// re-translate the query over a narrower window.
	usesBindingSet bool

	// bindingCTEs holds the result-set bindings this query reads more than once.
	bindingCTEs []bindingCTE

	// Histogram-specific fields
	HistogramField   string
	HistogramBuckets int
	// HistogramValueExpr is the numeric expression the buckets are computed from,
	// resolved while the registry still knows the field. Rebuilding it at window
	// layer time emitted a bare identifier the scan never projected (code 47).
	HistogramValueExpr string

	// ChartLayers wrap the result for a chart that reshapes it (boxplot,
	// scatter); they run after any z-score layers. ChartFieldOrder is their output.
	ChartLayers     []QueryLayer
	ChartFieldOrder []string
	// ChartReadsAllRows marks a chart that summarizes every row, so the default
	// display LIMIT must not truncate its input.
	ChartReadsAllRows bool

	// Z-score/MAD window-specific fields
	ModifiedZScoreExpr string
	OutlierThreshold   string
	MADWindowExpr      string
	ZScoreFilters      []string

	// Join-specific fields
	IsJoin      bool
	JoinType    string   // "inner" or "left"
	JoinKey     string   // field to join on
	JoinKeyExpr string   // outer join-key expression, projected as the hidden _join_k column
	JoinSubSQL  string   // translated subquery SQL
	JoinInclude []string // fields to include from subquery (empty = all)
	JoinOutputs []string // subquery output columns, surfaced as _join_<col>
	JoinMaxRows int      // max rows for subquery safety limit

	// ModelLookup-specific fields (set by model_lookup() BQL command)
	ModelLookupFields   []string // output field names added to outer SELECT
	ModelLookupKeyExprs []string // key expressions over the log scan: the scan-level ON's left side and the prefilter's
	ModelLookupStrict   bool     // true (default) = INNER JOIN: keep only rows the model scored
	ModelLookupCmd      int      // index of the model_lookup command
	// ModelLookupWhere: filters on model columns alone, applied to the model side of a
	// strict join (and so to its scan prefilter) rather than to the joined rows. Each
	// is a property of the key, so the join keeps exactly the rows it would have kept.
	ModelLookupWhere []string
	// modelKeys is the model's key set: its key columns, its topology, its scoring
	// subquery and the set the strict scan prefilter tests. nil without a lookup.
	modelKeys *modelKeySet
	// ModelLookupOutputs: the model's declared output column names. Set at Declare
	// (unlike ModelLookupFields, set at Execute) so commands that run before
	// model_lookup's Execute can still recognize a model column by name.
	ModelLookupOutputs []string
	// ModelLookupAtScan: no aggregation precedes model_lookup, so the model JOIN sits
	// in the source stage's FROM and every command after the lookup reads the model's
	// columns as ordinary per-row columns: assignments, transforms, filters, sorts,
	// table() and aggregations alike. Kept flat so a GLOBAL join still distributes:
	// shards join their local scan against the broadcast model set and pre-aggregate,
	// rather than shipping scan rows to the initiator. After an aggregation it joins
	// the aggregated rows instead, below the stage that reads them (ModelJoin).
	ModelLookupAtScan bool
	// PostJoinWhere: filters that read the scan-level join's columns, at the pipeline
	// position each was written, applied to the joined rows.
	PostJoinWhere []postJoinFilter

	// Table command tracking
	HasTableCmd             bool
	TableHasExplicitColumns bool
	TableColumns            []string // the columns table() names, as written
	TableJoinedFields       []string // join() outputs named in table(), in the order given

	// Pending conditions: classified by kind after Declare, materialized after Execute
	pendingWhereConditions    []HavingCondition
	pendingHavingConditions   []HavingCondition
	pendingDeferredConditions []HavingCondition

	// firstStageAt[i] is the registry a condition before the first command
	// resolves against when i assignments precede it (FieldRegistry.firstStageView).
	firstStageAt []*FieldRegistry
	// positionAt is the registry a condition after the first command resolves
	// against, keyed by the commands and assignments that precede it.
	positionAt map[[2]int]*FieldRegistry
	// baseWhereLen counts the source WHERE entries that scope the scan (time range,
	// fractal) rather than filter it. First-stage conditions follow them, ahead of
	// anything a command adds.
	baseWhereLen int

	// Source expressions exported to the deferred layer as hidden columns.
	deferred *deferredScope

	// Chained aggregation state (for sum/avg/etc. on prior aggregation outputs)
	outerAggregations  []string          // expressions for outer (chained) aggregation query
	outerAggFieldOrder []string          // field order for outer aggregation results
	aggregationOutputs map[string]string // tracks agg aliases (_count, _sum, etc.) -> SQL expression
}

// postJoinFilter is a filter on the scan-level model join's rows. At is the number
// of commands before it, which orders it against the row caps.
type postJoinFilter struct {
	At  int
	SQL string
}

// NewQueryPlan creates a plan with a single source stage.
func NewQueryPlan() *QueryPlan {
	return &QueryPlan{
		Stages: []QueryStage{
			{IsSource: true},
		},
		ChartConfig:            make(map[string]interface{}),
		AnalyzeFieldsScanLimit: 50000,
		aggregationOutputs:     make(map[string]string),
	}
}

// CurrentStage returns the active stage commands should write to.
func (p *QueryPlan) CurrentStage() *QueryStage {
	return &p.Stages[len(p.Stages)-1]
}

// conditionRegistry is the registry a condition resolves its fields against: the
// registry as the pipeline left it where the condition is written.
func (p *QueryPlan) conditionRegistry(c HavingCondition, registry *FieldRegistry) *FieldRegistry {
	if c.CmdIndex == 0 && len(p.firstStageAt) > 0 {
		return p.firstStageAt[min(c.AssignIndex, len(p.firstStageAt)-1)]
	}
	if v, ok := p.positionAt[[2]int{c.CmdIndex, c.AssignIndex}]; ok {
		return v
	}
	return registry
}

// SourceStage returns the innermost (FROM logs) stage.
func (p *QueryPlan) SourceStage() *QueryStage {
	return &p.Stages[0]
}

// havingStage returns the stage that post-aggregation (HAVING) conditions bind
// to: the outermost stage that has a GROUP BY. For single-stage queries this is
// the source stage; for chained groupby (groupby | groupby) it is the last
// pushed groupby stage, so the filter applies to the final aggregates rather
// than the inner ones. Chained-aggregation wrapper stages (sum/avg on a prior
// aggregate) are appended after materialization, so at materialize time this
// correctly resolves to the groupby stage that produced the referenced aggregate.
func (p *QueryPlan) havingStage() *QueryStage {
	return &p.Stages[p.havingStageIndex()]
}

// havingStageIndexAt returns the stage a HAVING filter written after cmdIndex
// commands binds to: the nearest aggregating stage that existed at that point.
// A filter between groupby() and a following count() filters the groups, one
// after it filters the count. Without positions it is the outermost.
func (p *QueryPlan) havingStageIndexAt(cmdIndex int) int {
	if cmdIndex <= 0 || cmdIndex > len(p.stageAfterCmd) {
		return p.havingStageIndex()
	}
	for i := p.stageAfterCmd[cmdIndex-1]; i >= 0; i-- {
		if len(p.Stages[i].Layer.GroupBy) > 0 || p.Stages[i].Aggregates {
			return i
		}
	}
	return p.havingStageIndex()
}

// havingStageIndex returns the index of the stage havingStage() resolves to.
func (p *QueryPlan) havingStageIndex() int {
	for i := len(p.Stages) - 1; i >= 0; i-- {
		if len(p.Stages[i].Layer.GroupBy) > 0 {
			return i
		}
	}
	return 0
}

// hasDeferredWrap reports whether renderStandard will emit the outer layer that
// carries deferred filters/ordering (and strips the exported hidden columns).
func (p *QueryPlan) hasDeferredWrap() bool {
	return len(p.DeferredWhere) > 0 || len(p.DeferredOrder) > 0 || p.DeferredLimit != ""
}

// deferredScope returns the plan's scope for expressions relocated above the source
// scan, creating it on first use.
func (p *QueryPlan) deferredScope() *deferredScope {
	if p.deferred == nil {
		p.deferred = newDeferredScope()
	}
	return p.deferred
}

// exportDeferredColumns projects the hidden columns the deferred layer needs into the
// source scan. Nothing to do unless a relocated expression actually referenced a
// source expression.
func (p *QueryPlan) exportDeferredColumns() error {
	if !p.deferred.used() {
		return nil
	}
	// A scope reference can be allocated for a condition whose SQL then comes out
	// empty (an operator with no builder), leaving nothing to strip the hidden
	// columns. Without a deferred wrap they would leak into the result, and nothing
	// references them anyway, so skip the export.
	if !p.hasDeferredWrap() {
		return nil
	}
	source := p.SourceStage()
	if len(p.Stages) > 1 {
		return fmt.Errorf("a filter or sort on a log field cannot be combined with this pipeline: the field is not available after the aggregation stages; move the filter before the aggregation")
	}
	if err := p.validateJoinKeysSurviveAggregation(source, "the filtered or sorted field", p.deferred.exprs, p.deferred.labels); err != nil {
		return err
	}
	if len(source.Layer.Selects) == 0 {
		// The scan would fall back to renderStandard's default projection, which
		// appending to would replace rather than extend. Assembly runs before this,
		// so no real pipeline reaches here; fail loudly rather than silently drop
		// the query's columns.
		return fmt.Errorf("cannot evaluate a filter or sort on a log field at this point in the pipeline")
	}
	source.Layer.Selects = append(source.Layer.Selects, p.deferred.projections()...)
	p.deferred.markExported()
	return nil
}

// PushStage adds a new empty stage to the pipeline.
// Subsequent commands writing to CurrentStage() will write to this new stage.
// The new stage's SELECT, GROUP BY, etc. are initially empty.
func (p *QueryPlan) PushStage() {
	p.Stages = append(p.Stages, QueryStage{})
}

// Render converts the QueryPlan into a final SQL string.
func (p *QueryPlan) Render(opts QueryOptions) (string, error) {
	sql, err := p.render(opts)
	if err != nil {
		return "", err
	}
	// A binding the query uses more than once is materialised once, ahead of
	// everything, instead of having its subquery pasted in at each use.
	return p.withClause() + sql, nil
}

func (p *QueryPlan) render(opts QueryOptions) (string, error) {
	if p.IsProcessTree {
		return p.renderProcessTree(opts)
	}
	if p.IsAnalyze {
		return p.renderAnalyze(opts)
	}
	// Chain queries use the normal rendering path (chainHandler populates
	// source stage with sequenceMatch/sequenceCount SQL during Execute).
	return p.renderStandard(opts)
}

// bindingCTE is one result-set binding materialised as a common table
// expression, because more than one place in the query reads it.
type bindingCTE struct {
	Name string
	SQL  string
}

// withClause renders the query's materialised bindings, or "" when it has none.
func (p *QueryPlan) withClause() string {
	if len(p.bindingCTEs) == 0 {
		return ""
	}
	parts := make([]string, len(p.bindingCTEs))
	for i, c := range p.bindingCTEs {
		parts[i] = fmt.Sprintf("%s AS (%s)", c.Name, c.SQL)
	}
	return "WITH " + strings.Join(parts, ", ") + " "
}

// addBindingCTE registers a materialised binding once, however many places read
// it, and reports the name they read it by.
func (p *QueryPlan) addBindingCTE(name, sql string) string {
	for _, c := range p.bindingCTEs {
		if c.Name == name {
			return name
		}
	}
	p.bindingCTEs = append(p.bindingCTEs, bindingCTE{Name: name, SQL: sql})
	return name
}

func (p *QueryPlan) renderStandard(opts QueryOptions) (string, error) {
	source := p.SourceStage()

	// Build SELECT clause
	var selectClause string
	if len(source.Layer.Selects) > 0 {
		parts := make([]string, len(source.Layer.Selects))
		for i, s := range source.Layer.Selects {
			parts[i] = s.String()
		}
		selectClause = strings.Join(parts, ", ")
	} else if opts.SourceSubquery != "" && len(opts.SourceColumns) > 0 {
		// Subquery source (pgr composition): default to its flat columns (there is no
		// norm_log to project).
		selectClause = strings.Join(opts.SourceColumns, ", ")
	} else {
		// Both hot and iceberg archives project the norm_log column directly
		// (materialized in the hot store; a plain JSON String in the archive).
		selectClause = "toString(timestamp) as timestamp, norm_log, log_id"
	}

	// join() resolves its key the same way: the outer key expression derives from
	// `fields.X`, which exists only in this scan, so project it as a hidden
	// `_join_k` column for the ON clause to reference (stripped by wrapWithJoin).
	// A key the outer SELECT already names (a groupby output) needs no projection,
	// and over a subquery source every column is already flat.
	if p.IsJoin && p.JoinSubSQL != "" && opts.SourceSubquery == "" && !p.outerHasColumn(p.JoinKey) {
		p.JoinKeyExpr = groupableCast(jsonFieldRef(p.JoinKey))
	}
	if p.JoinKeyExpr != "" {
		if err := p.validateJoinKeysSurviveAggregation(source, "join() key", []string{p.JoinKeyExpr}, []string{p.JoinKey}); err != nil {
			// The aggregation runs before the join, which is not what a reader of
			// `join() | count()` expects, so name the filter that does run first.
			return "", fmt.Errorf("join() is applied to the aggregated rows, so the aggregation must keep the key %[1]s: group by it, or to count or aggregate only the rows the subquery matches, filter first with let &keys := <subquery>; ... | in(%[1]s, &keys)", p.JoinKey)
		}
		selectClause += fmt.Sprintf(", %s AS %s", p.JoinKeyExpr, joinHiddenKeyCol)
	}

	// The row source this SELECT reads, plus any wrapper that must collapse rows
	// before it does. Each wrapper takes the pending filters with it.
	rowSource := opts.EffectiveTableName()

	// Strict mode's semi-join prefilter belongs in the scan's own WHERE: it is the
	// only place the model's key set can prune granules through the skip indexes on
	// the key sub-columns, and it turns the INNER JOIN above from a filter over
	// every log in the range into a join over the rows that can possibly match.
	// It filters at the lookup's position, so a cap written before it still caps
	// the rows the lookup was handed.
	if pf := p.modelPrefilter(); pf != "" {
		if after := source.Layer.capAfter(p.ModelLookupCmd); after != nil {
			*after = append(*after, pf)
		} else {
			source.Layer.Where = append(source.Layer.Where, pf)
		}
	}

	// pending holds the filters this SELECT, or the next wrapper, still has to apply.
	pending := source.Layer.Where

	if opts.SourceMode == SourceIceberg {
		// Collapse at-least-once archive duplicates before any aggregation or the
		// row cap. The spool is replayed from its last checkpoint after a crash, so
		// the same log_id can be committed to Iceberg twice; restore dedups the same
		// way (LIMIT 1 BY log_id) and without this recall count()/sum()/frequency
		// would over-report exactly those duplicates. The source WHERE stays INSIDE
		// this subquery so partition pruning and the `_ice_` bloom/min-max predicates
		// still reach the Parquet scan -- ClickHouse will not push predicates down
		// through LIMIT BY, so pruning would be lost if the WHERE were applied around
		// it. Duplicate rows are byte-identical, so filtering after the dedup keeps
		// exactly the row a pre-dedup filter would have kept.
		rowSource = "(SELECT * FROM " + rowSource + whereClause(pending) + " LIMIT 1 BY log_id)"
		pending = nil
	}

	// Each row cap reads the rows filtered before it and keeps its own ordering;
	// the filters after the last one are this SELECT's. A scan-level model join sits
	// among the caps at its position (see scanJoinPosition).
	joinAt, joined := -1, false
	if p.hasModelLookup() && p.ModelLookupAtScan {
		joinAt = p.scanJoinPosition(&source.Layer)
	}
	for i, c := range source.Layer.Caps {
		where := pending
		if i > 0 {
			where = source.Layer.Caps[i-1].After
		}
		if joinAt >= 0 && i >= joinAt {
			after := -1
			if i == joinAt {
				rowSource = p.joinModelAtScan(rowSource)
				joined = true
			} else {
				after = source.Layer.Caps[i-1].At
			}
			where = append(slices.Clone(where), p.postJoinSQL(after, c.At)...)
		}
		order := c.OrderBy
		if len(order) == 0 {
			order = defaultRowOrder(opts)
		}
		capSQL := "(SELECT * FROM " + rowSource + whereClause(where)
		if len(order) > 0 {
			capSQL += " ORDER BY " + strings.Join(order, ", ")
		}
		if c.LimitBy != "" && c.ScanLimitBy == "" {
			return "", fmt.Errorf("dedup() on a computed field can only be followed by a transform; dedup on a log field instead")
		}
		for _, part := range []string{c.ScanLimitBy, c.Limit} {
			if part != "" {
				capSQL += " " + part
			}
		}
		rowSource = capSQL + ")"
	}
	if n := len(source.Layer.Caps); n > 0 {
		pending = source.Layer.Caps[n-1].After
		if joined {
			pending = append(slices.Clone(pending), p.postJoinSQL(source.Layer.Caps[n-1].At, -1)...)
		}
	}

	// A pre-aggregation dedup has to collapse rows before this SELECT aggregates
	// them (see QueryLayer.ScanLimitBy), so it wraps the scan. Without an
	// aggregation it renders on this SELECT, exactly as it did before.
	scanDedup := source.Layer.ScanLimitBy != "" && (len(source.Layer.GroupBy) > 0 || p.IsAggregated)
	limitBy := source.Layer.LimitBy
	if source.Layer.ScanLimitBy != "" && !scanDedup {
		limitBy = source.Layer.ScanLimitBy
	}
	if scanDedup {
		// SELECT * rather than a column list: ClickHouse prunes the columns the
		// outer query does not read out of a star projection through LIMIT BY, but
		// reads every column an explicit list names (measured on one window: 1.8MiB
		// vs 168MiB), and the wrapper stays independent of what the aggregation
		// above it references.
		order := ""
		if o := dedupScanOrder(opts); o != "" {
			// LIMIT BY keeps the first row of each key, so ordering the scan is what
			// makes the survivor the newest event (the documented behaviour) rather
			// than whichever row was read first.
			order = " ORDER BY " + o
		}
		rowSource = "(SELECT * FROM " + rowSource + whereClause(pending) + order + " " + source.Layer.ScanLimitBy + ")"
		pending = nil
	}

	// Scan-level model join after every cap: this SELECT reads (scan JOIN model) so
	// its rows carry the model columns. Filters on those columns go in this SELECT's
	// WHERE; scan-only predicates stay wherever they already are (the same WHERE, or
	// inside a dedup wrap), where ClickHouse pushes them into the scan.
	if p.hasModelLookup() && p.ModelLookupAtScan && !joined {
		joinedWhere := pending
		if p.selectsRowsBelowModelJoin(source, opts) {
			// Nothing after the join selects rows, so this SELECT returns the rows the
			// scan alone would. A narrow scan picks them, reading in sorting-key order
			// and stopping early, and only those are joined, read back by sorting key.
			order := strings.Join(source.Layer.OrderBy, ", ")
			limit := source.Layer.Limit
			if source.Layer.LimitBy != "" {
				limit = source.Layer.LimitBy + " " + limit
			}
			// GLOBAL whatever the topology, as pgr's re-read does: on a cluster the
			// subquery reads logs_distributed, and on one node it costs nothing.
			picked := fmt.Sprintf("(timestamp, log_id) GLOBAL IN (SELECT timestamp, log_id FROM %s%s ORDER BY %s %s)",
				rowSource, whereClause(pending), order, limit)
			// The joined read keeps the filters: log_id is not unique, so a row sharing
			// a picked (timestamp, log_id) may fail them. The strict prefilter is left
			// to the INNER JOIN, which applies exactly the same key set.
			pf := p.modelPrefilter()
			joinedWhere = append(slices.DeleteFunc(slices.Clone(source.Layer.Where), func(w string) bool { return w == pf }), picked)
			// The join hashes the few picked rows and streams the model past them,
			// rather than hashing every scored key. They carry their filters with them,
			// so a GLOBAL join on a cluster ships only those rows.
			star := "*"
			if opts.IncludeShardNum {
				star = "*, _shard_num"
			}
			kind := " INNER JOIN "
			if !p.ModelLookupStrict {
				kind = " RIGHT JOIN "
			}
			rowSource = "(" + p.modelJoinSource() + ") AS _mlookup" + p.globalJoin() + kind +
				"(SELECT " + star + " FROM " + rowSource + whereClause(joinedWhere) + ") AS _outer ON " + p.modelJoinKeys(p.ModelLookupKeyExprs)
			pending = nil
		} else {
			rowSource = p.joinModelAtScan(rowSource)
			pending = append(slices.Clone(joinedWhere), p.postJoinSQL(-1, -1)...)
		}
	}

	var sql strings.Builder
	sql.WriteString("SELECT ")
	sql.WriteString(selectClause)
	sql.WriteString(" FROM ")
	sql.WriteString(rowSource)
	sql.WriteString(whereClause(pending))

	// GROUP BY
	if len(source.Layer.GroupBy) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(source.Layer.GroupBy, ", "))
	}

	// HAVING
	if len(source.Layer.Having) > 0 {
		sql.WriteString(" HAVING ")
		sql.WriteString(andJoin(source.Layer.Having))
	}

	// ORDER BY
	if len(source.Layer.OrderBy) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(source.Layer.OrderBy, ", "))
	}

	// LIMIT BY
	if limitBy != "" {
		sql.WriteString(" ")
		sql.WriteString(limitBy)
	}

	// LIMIT
	if source.Layer.Limit != "" {
		sql.WriteString(" ")
		sql.WriteString(source.Layer.Limit)
	}

	innerSQL := sql.String()

	// Apply additional stages (chained aggregation, stage index > 0).
	for i := 1; i < len(p.Stages); i++ {
		innerSQL = wrapWithLayer(innerSQL, p.Stages[i].Layer)
	}

	// Apply join wrapping (subquery JOIN)
	if p.IsJoin && p.JoinSubSQL != "" {
		innerSQL = p.wrapWithJoin(innerSQL)
	}

	// Apply formatters (outer SELECT for timestamp formatting, deferred math)
	// Formatters go BEFORE window layers so z-score/histogram wrap the formatted output.
	if len(p.Formatters) > 0 {
		var outer strings.Builder
		outer.WriteString("SELECT ")
		parts := make([]string, len(p.Formatters))
		for i, f := range p.Formatters {
			parts[i] = f.String()
		}
		outer.WriteString(strings.Join(parts, ", "))
		outer.WriteString(" FROM (")
		outer.WriteString(innerSQL)
		outer.WriteString(")")
		if len(p.FormatterOrderBy) > 0 {
			outer.WriteString(" ORDER BY ")
			outer.WriteString(strings.Join(p.FormatterOrderBy, ", "))
		}
		if p.FormatterLimit != "" {
			outer.WriteString(" ")
			outer.WriteString(p.FormatterLimit)
		}
		innerSQL = outer.String()
	}

	// Apply window layers (z-score, histogram wrapping)
	for _, wl := range p.WindowLayers {
		innerSQL = wrapWithLayer(innerSQL, wl)
	}

	// Apply deferred conditions/ordering (post-window filters)
	if p.hasDeferredWrap() {
		var outer strings.Builder
		outer.WriteString("SELECT *")
		if p.deferred.exported {
			// The exported columns exist only to make this layer's expressions
			// resolvable; drop them from the result.
			outer.WriteString(" EXCEPT (" + strings.Join(p.deferred.names, ", ") + ")")
		}
		outer.WriteString(" FROM (")
		outer.WriteString(innerSQL)
		outer.WriteString(")")
		if len(p.DeferredWhere) > 0 {
			outer.WriteString(" WHERE ")
			outer.WriteString(andJoin(p.DeferredWhere))
		}
		if len(p.DeferredOrder) > 0 {
			outer.WriteString(" ORDER BY ")
			outer.WriteString(strings.Join(p.DeferredOrder, ", "))
		}
		if p.DeferredLimit != "" {
			outer.WriteString(" ")
			outer.WriteString(p.DeferredLimit)
		}
		innerSQL = outer.String()
	}

	// Validate for injection-style patterns. A subquery source (a resolved source command
	// like pgr()) is trusted machine-generated SQL that legitimately contains UNION ALL (its
	// edge-type union) and its only user input (guids/start) is already escaped; exempt it
	// from the check while still validating the user-generated wrapper around it.
	toValidate := innerSQL
	if opts.SourceSubquery != "" {
		toValidate = strings.Replace(toValidate, opts.SourceSubquery, "source_subquery", 1)
	}
	if err := validateGeneratedSQL(toValidate); err != nil {
		return "", err
	}

	return innerSQL, nil
}

func (p *QueryPlan) renderProcessTree(opts QueryOptions) (string, error) {
	source := p.SourceStage()
	result, err := buildProcessTreeSQL(
		p.ProcessTreeStart,
		source.Layer.Having,
		p.ChartType,
		p.ChartConfig,
		opts,
	)
	if err != nil {
		return "", err
	}
	p.FieldOrder = result.FieldOrder
	p.IsAggregated = result.IsAggregated
	return result.SQL, nil
}

func (p *QueryPlan) renderAnalyze(opts QueryOptions) (string, error) {
	source := p.SourceStage()
	result, err := buildAnalyzeFieldsSQL(
		p.AnalyzeFieldsList,
		p.AnalyzeFieldsScanLimit,
		source.Layer.Where,
		source.Layer.Having,
		source.Layer.OrderBy,
		source.Layer.Limit,
		p.ChartType,
		p.ChartConfig,
		opts,
	)
	if err != nil {
		return "", err
	}
	p.FieldOrder = result.FieldOrder
	p.IsAggregated = result.IsAggregated
	return result.SQL, nil
}

// dedupScanOrder returns the ORDER BY that decides which row of each dedup key
// survives: newest first. A subquery source (pgr()) exposes flat columns
// instead of the logs table, so it is ordered only by the ones it actually has.
func dedupScanOrder(opts QueryOptions) string {
	if opts.SourceSubquery == "" {
		return "timestamp DESC, log_id DESC"
	}
	var terms []string
	for _, c := range opts.SourceColumns {
		if c == "timestamp" || c == "log_id" {
			terms = append(terms, c+" DESC")
		}
	}
	return strings.Join(terms, ", ")
}

// whereClause renders filters as a WHERE clause, or "" when there are none.
func whereClause(where []string) string {
	if len(where) == 0 {
		return ""
	}
	return " WHERE " + andJoin(where)
}

// defaultRowOrder is the ordering a row cap keeps rows by when its command gave
// none: the source's own, else newest first.
func defaultRowOrder(opts QueryOptions) []string {
	if len(opts.SourceOrderBy) > 0 {
		return opts.SourceOrderBy
	}
	if o := dedupScanOrder(opts); o != "" {
		return strings.Split(o, ", ")
	}
	return nil
}

// wrapWithLayer wraps an inner SQL string with a QueryLayer's SELECT/WHERE/GROUP BY/HAVING/ORDER BY/LIMIT.
func wrapWithLayer(innerSQL string, layer QueryLayer) string {
	if layer.ModelJoin != "" {
		innerSQL = modelJoinLayer(innerSQL, layer.ModelJoin, layer.ModelJoinFields)
	}
	// Same split as renderStandard: a dedup that precedes this layer's
	// aggregation has to collapse the rows feeding it, not its output. Nothing to
	// order by here: these rows come from a stage that has already aggregated.
	limitBy := layer.LimitBy
	if layer.ScanLimitBy != "" {
		if len(layer.GroupBy) > 0 {
			innerSQL = "SELECT * FROM (" + innerSQL + ") " + layer.ScanLimitBy
		} else {
			limitBy = layer.ScanLimitBy
		}
	}

	var outer strings.Builder
	outer.WriteString("SELECT ")
	if len(layer.Selects) > 0 {
		parts := make([]string, len(layer.Selects))
		for i, s := range layer.Selects {
			parts[i] = s.String()
		}
		outer.WriteString(strings.Join(parts, ", "))
	} else {
		outer.WriteString("*")
	}
	outer.WriteString(" FROM (")
	outer.WriteString(innerSQL)
	outer.WriteString(")")
	if len(layer.Where) > 0 {
		outer.WriteString(" WHERE ")
		outer.WriteString(andJoin(layer.Where))
	}
	if len(layer.GroupBy) > 0 {
		outer.WriteString(" GROUP BY ")
		outer.WriteString(strings.Join(layer.GroupBy, ", "))
	}
	if len(layer.Having) > 0 {
		outer.WriteString(" HAVING ")
		outer.WriteString(andJoin(layer.Having))
	}
	if len(layer.OrderBy) > 0 {
		outer.WriteString(" ORDER BY ")
		outer.WriteString(strings.Join(layer.OrderBy, ", "))
	}
	if limitBy != "" {
		outer.WriteString(" ")
		outer.WriteString(limitBy)
	}
	if layer.Limit != "" {
		outer.WriteString(" ")
		outer.WriteString(layer.Limit)
	}
	return outer.String()
}

// wrapWithJoin wraps the outer query SQL with a JOIN against the subquery.
func (p *QueryPlan) wrapWithJoin(outerSQL string) string {
	joinType := "INNER"
	if p.JoinType == "left" {
		joinType = "LEFT"
	}

	// Resolve the join key for the outer query. If the outer SELECT has the join
	// key as a named column (e.g. from groupby), use it directly; otherwise the
	// source scan projected it as the hidden _join_k column (see renderStandard),
	// which is the only reference that resolves inside _outer.
	outerKeyRef := p.JoinKey
	if p.JoinKeyExpr != "" {
		outerKeyRef = joinHiddenKeyCol
	}

	var sql strings.Builder
	sql.WriteString("SELECT _outer.*")
	if p.JoinKeyExpr != "" {
		sql.WriteString(fmt.Sprintf(" EXCEPT (%s)", joinHiddenKeyCol))
	}

	// Every subquery column comes through under a `_join_` prefix. A bare
	// `_join_sub.*` would re-emit the join key and collide with same-named outer
	// columns (an aggregating outer query and its subquery both produce `_count`),
	// leaving the result with duplicate names that no filter or sort can reference.
	for _, col := range joinCarriedColumns(p.JoinOutputs, p.JoinInclude, p.JoinKey) {
		sql.WriteString(fmt.Sprintf(", _join_sub.%s AS %s", col, joinPrefixed(col)))
	}

	sql.WriteString(" FROM (")
	sql.WriteString(outerSQL)
	sql.WriteString(") AS _outer ")
	sql.WriteString(joinType)
	sql.WriteString(" JOIN (")
	sql.WriteString(p.JoinSubSQL)
	sql.WriteString(") AS _join_sub ON _outer.")
	sql.WriteString(outerKeyRef)
	sql.WriteString(" = _join_sub.")
	sql.WriteString(p.JoinKey)

	return sql.String()
}

// validateJoinKeysSurviveAggregation rejects a join whose keys cannot be projected
// from an aggregating source scan. The join runs outside that scan, so each key must
// survive the aggregation as a group expression (or an alias of one); a key that does
// not is not an aggregate, and ClickHouse would reject the query (code 215) with an
// error that says nothing about the real problem. Shared by model_lookup() and join().
func (p *QueryPlan) validateJoinKeysSurviveAggregation(source *QueryStage, what string, keyExprs, keyFields []string) error {
	missing := p.keysLostToAggregation(source, keyExprs, keyFields)
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%s %s must be part of the grouping to survive aggregation: add %s to the group/table columns, or drop the aggregation",
		what, strings.Join(missing, ", "), strings.Join(missing, ", "))
}

// keysLostToAggregation names the keys an aggregating source stage does not group
// by, so cannot be read after it.
func (p *QueryPlan) keysLostToAggregation(source *QueryStage, keyExprs, keyFields []string) []string {
	if len(source.Layer.GroupBy) == 0 && !p.IsAggregated {
		return nil
	}
	grouped := make(map[string]bool, len(source.Layer.GroupBy)*2)
	for _, g := range source.Layer.GroupBy {
		grouped[strings.Trim(strings.TrimSpace(g), "`")] = true
	}
	// A group key written as an alias (GROUP BY src_ip) also covers the expression
	// that alias was defined from in this stage's SELECT.
	for _, sel := range source.Layer.Selects {
		s := sel.String()
		alias := strings.Trim(extractFieldAlias(s), "`")
		if alias == "" || !grouped[alias] {
			continue
		}
		if idx := strings.LastIndex(s, " AS "); idx > 0 {
			grouped[strings.TrimSpace(s[:idx])] = true
		}
	}

	var missing []string
	for i, keyExpr := range keyExprs {
		if grouped[strings.Trim(keyExpr, "`")] {
			continue
		}
		name := keyExpr
		if i < len(keyFields) {
			name = keyFields[i]
		}
		missing = append(missing, name)
	}
	return missing
}

// modelJoinOn renders the model join onto rows whose key columns are left:
// "[GLOBAL] INNER|LEFT JOIN (scoring) AS _mlookup ON ...". One key per model key
// column joins on plain equalities, which is both the cheaper hash join (no string
// built per row) and the exact predicate the strict prefilter pushes into the scan.
// Several keys against a single model column (first_seen and volume_baseline
// encode a composite entity that way) use the model's own char(30) encoding.
func (p *QueryPlan) modelJoinOn(left []string) string {
	kind := " INNER JOIN ("
	if !p.ModelLookupStrict {
		kind = " LEFT JOIN ("
	}
	return p.globalJoin() + kind + p.modelJoinSource() + ") AS _mlookup ON " + p.modelJoinKeys(left)
}

// globalJoin is " GLOBAL" when the model table is Distributed. Without it ClickHouse
// refuses the double-distributed subquery outright (distributed_product_mode='deny',
// code 288); with it the initiator reads the model across every shard once.
func (p *QueryPlan) globalJoin() string {
	if p.modelKeys.global {
		return " GLOBAL"
	}
	return ""
}

// modelJoinKeys is the ON condition matching key columns left to the model's.
func (p *QueryPlan) modelJoinKeys(left []string) string {
	right := make([]string, len(p.modelKeys.rightCols))
	for i, c := range p.modelKeys.rightCols {
		right[i] = "_mlookup." + c
	}
	if !modelKeysAligned(left, right) {
		return concatModelKeys(left) + " = " + concatModelKeys(right)
	}
	eq := make([]string, len(right))
	for i := range right {
		eq[i] = left[i] + " = " + right[i]
	}
	return strings.Join(eq, " AND ")
}

// modelJoinLayer joins the model onto the rows of a finished stage, for the stage
// after it to read: exactly those rows' columns and the model's, so nothing above
// it can meet a name both sides have.
func modelJoinLayer(innerSQL, join string, fields []string) string {
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = "_mlookup." + f + " AS " + f
	}
	return "SELECT _outer.*, " + strings.Join(cols, ", ") + " FROM (" + innerSQL + ") AS _outer" + join
}

// modelJoinSource is the model side of the join: the scoring subquery, narrowed by
// the filters that read only its columns.
func (p *QueryPlan) modelJoinSource() string {
	if len(p.ModelLookupWhere) == 0 {
		return p.modelKeys.scoringSQL
	}
	return "SELECT * FROM (" + p.modelKeys.scoringSQL + ") WHERE " + andJoin(p.ModelLookupWhere)
}

// hasModelLookup reports whether a model_lookup() joins the model's scores.
func (p *QueryPlan) hasModelLookup() bool { return p.modelKeys != nil }

// modelPrefilter is a strict join's semi-join pushed down into the source scan:
// only logs whose key the model scores can survive the INNER JOIN, and the scan is
// the one place the key sub-columns' skip indexes can prune on that set. Filters on
// model columns narrow the set exactly as they narrow the model side.
func (p *QueryPlan) modelPrefilter() string {
	if !p.hasModelLookup() || !p.ModelLookupStrict || len(p.ModelLookupKeyExprs) == 0 {
		return ""
	}
	return p.modelKeys.membership(p.ModelLookupKeyExprs, p.ModelLookupWhere)
}

// readsThroughCap reports whether the scan-level model join, or a filter on its
// columns, comes after the row cap c and changes which rows c's own ordering and
// limit select if they are applied in the same SELECT: a strict join drops rows,
// and a post-join filter is written after it.
func (p *QueryPlan) readsThroughCap(c RowCap) bool {
	if !p.hasModelLookup() || !p.ModelLookupAtScan {
		return false
	}
	if len(p.postJoinSQL(c.At, -1)) > 0 {
		return true
	}
	return p.ModelLookupStrict && c.At < p.ModelLookupCmd && !p.dedupsByModelKeys(c)
}

// dedupsByModelKeys reports whether c is a dedup whose keys include every model
// key. Each group it keeps one row of then shares one key, which the strict join
// keeps or drops whole, so the join selects the same rows on either side of it.
func (p *QueryPlan) dedupsByModelKeys(c RowCap) bool {
	if c.Limit != "" || c.ScanLimitBy == "" {
		return false
	}
	keys := strings.Split(strings.TrimPrefix(c.ScanLimitBy, "LIMIT 1 BY "), ", ")
	for _, k := range p.ModelLookupKeyExprs {
		if !slices.Contains(keys, k) {
			return false
		}
	}
	return true
}

// postJoinSQL returns the post-join filters written after command index after and
// up to command index upTo (RowCap.At values); -1 leaves that side open.
func (p *QueryPlan) postJoinSQL(after, upTo int) []string {
	var out []string
	for _, f := range p.PostJoinWhere {
		if (after < 0 || f.At > after) && (upTo < 0 || f.At <= upTo) {
			out = append(out, f.SQL)
		}
	}
	return out
}

// scanJoinPosition returns how many of the scan's row caps run before the model
// join. A cap written after model_lookup() reads joined rows, but the join keeps
// every row the prefilter passed, so the cap can run first, where the scan stops
// early, unless a filter between them reads the join or its ordering does.
func (p *QueryPlan) scanJoinPosition(l *QueryLayer) int {
	if l.ScanLimitBy != "" {
		// A pre-aggregation dedup wraps every cap and is joined after (see the
		// scanDedup wrap in renderStandard).
		return len(l.Caps)
	}
	n := 0
	for n < len(l.Caps) && (l.Caps[n].At < p.ModelLookupCmd || len(p.postJoinSQL(-1, l.Caps[n].At)) == 0 && capReadsOnlyScan(l.Caps[n])) {
		n++
	}
	return n
}

// capReadsOnlyScan reports whether a row cap's ordering and dedup keys can be
// evaluated over the logs table's own columns.
func capReadsOnlyScan(c RowCap) bool {
	if c.LimitBy != "" && c.ScanLimitBy == "" {
		return false
	}
	return orderingReadsOnlyScan(c.OrderBy, c.ScanLimitBy)
}

// selectsRowsBelowModelJoin reports whether the source SELECT's own ORDER BY, LIMIT
// BY and LIMIT may pick its rows from the scan before the scan-level model join: the
// join keeps every row, nothing after it selects or aggregates rows, and the
// ordering and dedup keys read only the logs table's columns.
func (p *QueryPlan) selectsRowsBelowModelJoin(source *QueryStage, opts QueryOptions) bool {
	l := source.Layer
	if len(p.PostJoinWhere) > 0 || l.Limit == "" || len(l.OrderBy) == 0 {
		return false
	}
	// Rows are picked from the table itself, so no row cap may read through first.
	if len(p.Stages) > 1 || len(l.GroupBy) > 0 || len(l.Having) > 0 || l.ScanLimitBy != "" || len(l.Caps) > 0 ||
		p.IsAggregated || p.IsChain || opts.SourceMode == SourceIceberg || opts.SourceSubquery != "" {
		return false
	}
	if len(p.WindowLayers) > 0 || p.hasDeferredWrap() || p.ModifiedZScoreExpr != "" || p.HistogramBuckets > 0 || len(p.ChartLayers) > 0 {
		return false
	}
	return orderingReadsOnlyScan(l.OrderBy, l.LimitBy)
}

// joinModelAtScan appends the scan-level model join to a row source.
func (p *QueryPlan) joinModelAtScan(rowSource string) string {
	// A wrapped scan (a cap or dedup subquery) must carry an alias to be joined
	// against (ClickHouse code 206); a subquery source already has one.
	if strings.HasPrefix(rowSource, "(") && strings.HasSuffix(rowSource, ")") {
		rowSource += " AS _outer"
	}
	return rowSource + p.modelJoinOn(p.ModelLookupKeyExprs)
}

// outerHasColumn reports whether the source stage's SELECT already exposes the given
// column under that name.
func (p *QueryPlan) outerHasColumn(column string) bool {
	// Check source stage selects for an alias matching the column
	source := p.SourceStage()
	for _, sel := range source.Layer.Selects {
		alias := extractFieldAlias(sel.String())
		if alias == column {
			return true
		}
	}
	return false
}

// sourceWhereComplete reports whether the source stage's WHERE is the query's
// whole row selection. Every construct that selects rows somewhere else is
// listed: another stage, a GROUP BY or HAVING, a wrapper layer, a predicate
// deferred above the scan, a dedup, a command-set row limit, or a subquery
// carrying its own scope.
func (p *QueryPlan) sourceWhereComplete() bool {
	if len(p.Stages) != 1 || len(p.WindowLayers) > 0 {
		return false
	}
	src := p.SourceStage().Layer
	if len(src.GroupBy) > 0 || len(src.Having) > 0 || src.LimitBy != "" || src.ScanLimitBy != "" {
		return false
	}
	if len(p.DeferredWhere) > 0 || len(p.PostJoinWhere) > 0 || len(p.ModelLookupWhere) > 0 {
		return false
	}
	if p.IsAggregated || p.HasGroupBy || p.IsAnalyze || p.IsChain || p.IsProcessTree {
		return false
	}
	if p.IsJoin || p.hasModelLookup() || p.usesBindingSet || p.rowLimitByCommand {
		return false
	}
	return p.HistogramBuckets == 0 && p.ModifiedZScoreExpr == "" && len(p.ChartLayers) == 0
}

// SourceProjection is one column the source scan computes, as the alias it is
// known by and the expression behind it.
type SourceProjection struct {
	Alias string
	Expr  string
}

// sourceProjections lists the columns the source stage computes, skipping the
// passthrough of the table's own columns. A command may write the alias into the
// expression ("expr AS name") rather than into the Alias field, so both spellings
// are read; an entry whose alias is the whole expression is a passthrough.
func (p *QueryPlan) sourceProjections() []SourceProjection {
	var out []SourceProjection
	for _, sel := range p.SourceStage().Layer.Selects {
		alias, expr := sel.Alias, sel.Expr
		if alias == "" {
			alias = extractFieldAlias(expr)
			// An expression can contain the separator without being aliased, as
			// CAST(x AS String) does, and its trailing token is not a name. Only a
			// plain one is an alias.
			if alias == expr || !IsPlainFieldName(alias) {
				continue
			}
			expr = strings.TrimSpace(expr[:len(expr)-len(alias)])
			expr = strings.TrimSuffix(strings.TrimSuffix(expr, " AS"), " as")
		}
		if alias == "" || alias == expr {
			continue
		}
		out = append(out, SourceProjection{Alias: alias, Expr: strings.TrimSpace(expr)})
	}
	return out
}
