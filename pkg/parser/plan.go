package parser

import (
	"fmt"
	"slices"
	"strings"
)

// SelectExpr is one column of a SELECT: an expression and the name it is output
// under. Alias is empty when the expression is itself a column, bare or quoted.
type SelectExpr struct {
	Expr  string
	Alias string
	// agg is the aggregate Expr renders, when the column is one, and window marks
	// a window function over the SELECT's rows.
	agg    aggregate
	window bool
	// cols are the columns an aggregate reads, recorded where it was built;
	// colsKnown is set once they are.
	cols      []string
	colsKnown bool
}

// selectAs is the column computing expr under alias.
func selectAs(expr, alias string) SelectExpr { return SelectExpr{Expr: expr, Alias: alias} }

// String returns the SQL representation: "Expr AS Alias" or just "Expr".
func (s SelectExpr) String() string {
	if s.Alias != "" && s.Alias != s.Expr {
		return s.Expr + " AS " + s.Alias
	}
	return s.Expr
}

// Name is the column's output name, unquoted.
func (s SelectExpr) Name() string {
	n := s.Alias
	if n == "" {
		n = s.Expr
	}
	return unquoteIdent(n)
}

// unquoteIdent returns a backtick-quoted identifier's name, undoing
// EscapeCHBacktickIdent, or s unchanged.
func unquoteIdent(s string) string {
	if len(s) < 2 || s[0] != '`' || s[len(s)-1] != '`' {
		return s
	}
	var b strings.Builder
	inner := s[1 : len(s)-1]
	for i := 0; i < len(inner); i++ {
		if (inner[i] == '\\' || inner[i] == '`') && i+1 < len(inner) && inner[i+1] == inner[i] {
			i++
		}
		b.WriteByte(inner[i])
	}
	return b.String()
}

// groupKey is one GROUP BY key: what is grouped, and the name its column is
// output as. Once the stage's SELECT is assembled, Expr is that column.
type groupKey struct {
	Expr string
	Name string
}

// groupBy adds a key unless the layer already groups by the same expression.
func (l *QueryLayer) groupBy(expr, name string) {
	if !slices.ContainsFunc(l.GroupBy, func(k groupKey) bool { return k.Expr == expr }) {
		l.GroupBy = append(l.GroupBy, groupKey{Expr: expr, Name: name})
	}
}

// groupExprs lists what a layer groups by.
func groupExprs(keys []groupKey) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k.Expr
	}
	return out
}

// OrderTerm is one ORDER BY term: an expression and its direction.
type OrderTerm struct {
	Expr string
	Desc bool
	// form and deps are how Expr names its value and what it reads, recorded where
	// the term was built.
	form refForm
	deps deps
}

// asc and desc order by a column the translator itself names.
func asc(col string) OrderTerm  { return orderBy(columnRef(col), false) }
func desc(col string) OrderTerm { return orderBy(columnRef(col), true) }

// orderBy orders by a reference.
func orderBy(r sqlRef, desc bool) OrderTerm {
	return OrderTerm{Expr: r.sql, Desc: desc, form: r.form, deps: r.deps}
}

func (o OrderTerm) String() string {
	if o.Desc {
		return o.Expr + " DESC"
	}
	return o.Expr + " ASC"
}

// orderSQL renders ORDER BY terms.
func orderSQL(terms []OrderTerm) string {
	out := make([]string, len(terms))
	for i, t := range terms {
		out[i] = t.String()
	}
	return strings.Join(out, ", ")
}

// QueryLayer holds the parts of a single SQL query level.
type QueryLayer struct {
	Selects []SelectExpr
	Where   []predicate
	GroupBy []groupKey
	Having  []predicate
	OrderBy []OrderTerm
	LimitBy string
	// limitByDeps and scanLimitByDeps are what the LIMIT BY keys read.
	limitByDeps, scanLimitByDeps deps
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
func (l *QueryLayer) project(sel SelectExpr) {
	if i := l.columnIndex(sel.Name()); i >= 0 {
		l.Selects[i] = sel
		return
	}
	l.Selects = append(l.Selects, sel)
}

// addColumn adds a column unless the layer already projects the same one.
func (l *QueryLayer) addColumn(sel SelectExpr) {
	if !slices.ContainsFunc(l.Selects, func(s SelectExpr) bool { return s.Expr == sel.Expr && s.Alias == sel.Alias }) {
		l.Selects = append(l.Selects, sel)
	}
}

// addAssignments projects the columns the layer does not already have by name,
// such as assignments a command after them has not replaced.
func (l *QueryLayer) addAssignments(assignments []SelectExpr) {
	for _, a := range assignments {
		if l.columnIndex(a.Name()) < 0 {
			l.Selects = append(l.Selects, a)
		}
	}
}

// columnIndex is the position of the column named name, or -1.
func (l *QueryLayer) columnIndex(name string) int {
	return slices.IndexFunc(l.Selects, func(s SelectExpr) bool { return s.Name() == name })
}

// RowCap is a row selection the rest of the pipeline reads through, rendered as
// a subquery around the scan.
type RowCap struct {
	At      int // index of the capping command
	OrderBy []OrderTerm
	LimitBy string
	// ScanLimitBy is LimitBy over the raw scan, "" when a key exists only as a
	// computed column.
	ScanLimitBy string
	Limit       string
	After       []predicate // filters written after this cap and before the next
	// scanKeys are the ScanLimitBy keys. limitByDeps and scanDeps are what the
	// LimitBy and ScanLimitBy keys read.
	scanKeys              []string
	limitByDeps, scanDeps deps
}

// combinesRows reports whether the layer's SELECT reads its rows together: it
// groups them, aggregates them or computes a window over them. A row cap must then
// run before it, not as its own LIMIT, which runs after.
func (l *QueryLayer) combinesRows() bool {
	return len(l.GroupBy) > 0 || slices.ContainsFunc(l.Selects, func(s SelectExpr) bool { return s.agg != nil || s.window })
}

// capAfter is the filter list for a position k commands in: the After of the last
// cap before it, or nil ahead of every cap.
func (l *QueryLayer) capAfter(k int) *[]predicate {
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
	l.OrderBy, l.LimitBy, l.limitByDeps, l.Limit = last.OrderBy, last.LimitBy, last.limitByDeps, last.Limit
	l.Caps = l.Caps[:n-1]
}

// UpsertSelect adds a column, replacing any existing one of the same name. This
// enforces last-write-wins semantics: when a pipeline transform overwrites a field
// produced by an earlier command in the same stage (e.g. regex creates
// "download_domain", then lowercase(download_domain) updates it), the SELECT holds
// exactly one entry for that name, preventing ClickHouse error 179 (multiple
// expressions for the same alias). A bare column with no alias is always appended.
func (l *QueryLayer) UpsertSelect(expr SelectExpr) {
	if expr.Alias == "" {
		l.Selects = append(l.Selects, expr)
		return
	}
	l.project(expr)
}

// QueryStage is one SELECT of the pipeline: the scan, then one per aggregation or
// projection of an earlier stage's rows.
type QueryStage struct {
	Layer    QueryLayer
	IsSource bool // true for the innermost stage (FROM logs)
	// IsChain marks the stage chain() aggregates in. Its columns are fixed, so a
	// later stats command aggregates over its rows in a stage of its own.
	IsChain bool
	// Aggregates marks a stage pushed to aggregate the previous stage's rows,
	// which may have no GROUP BY (count() after groupby() is one COUNT(*)).
	Aggregates bool
	// AfterCap marks a stage that wraps a cap on aggregated rows, so filters after
	// the cap read only the rows it kept.
	AfterCap bool
}

// QueryPlan holds the structured representation of a query before rendering to SQL.
type QueryPlan struct {
	Stages []QueryStage // pipeline of aggregation stages (first = innermost)

	// IsAggregated records that an aggregate function runs, so the result is
	// aggregated rows. HasGroupBy records that a grouping command ran and its
	// groups are still the rows, which a later bare count() collapses.
	IsAggregated bool
	HasGroupBy   bool

	// special is a query whose SQL replaces the standard rendering, nil for most.
	special queryMode
	// Chain describes the chain() the query matches, nil without one.
	Chain *ChainMeta
	// join is the plan's join() of its rows with a subquery's, nil without one.
	join *joinPlan
	// lookup is the model join modelLookup() or pgrContext() adds.
	lookup lookupPlan

	// What renders around the stages, and table()'s request.
	format    formatPlan
	window    windowPlan
	chart     chartPlan
	histogram histogramPlan
	deferred  deferredPlan
	table     tablePlan

	// aggregationOutputs are the current stage's aggregate aliases (_count, _sum, ...):
	// an aggregate reading one aggregates the stage's groups (see aggregateOver).
	aggregationOutputs map[string]bool

	// pending holds the filters classified by stage, placed once every command ran.
	pending pendingFilters
	// pos is what the planner knows about the pipeline and each position in it.
	pos positions
	// baseWhereLen counts the source WHERE entries that scope the scan (time range,
	// fractal) rather than filter it. First-stage conditions follow them, ahead of
	// anything a command adds.
	baseWhereLen int
	// pgr holds the provenance filters and what narrows their candidates.
	pgr pgrPlan

	// rowLimitByCommand records that a command (head, limit, dedup, a
	// limit= option) bounded the row set, rather than the caller's MaxRows.
	rowLimitByCommand bool
	// usesBindingSet records that a result-set binding was rendered. Like join(),
	// its subquery carries its own copy of the time bounds, so a caller must not
	// re-translate the query over a narrower window.
	usesBindingSet bool
	// bindingCTEs holds the result-set bindings this query reads more than once.
	bindingCTEs []bindingCTE
}

// formatPlan is the outer SELECT that formats the rows: timestamp formatting and
// deferred math, with the ordering and limit lifted to it for streaming.
type formatPlan struct {
	selects []SelectExpr
	orderBy []OrderTerm
	limit   string
}

// windowPlan is the z-score and outlier layers over the stages.
type windowPlan struct {
	layers           []QueryLayer
	zScoreExpr       string
	outlierThreshold string
}

// chartPlan is the chart the result is drawn as and the layers that reshape it
// (boxplot, scatter), which run after any window layers.
type chartPlan struct {
	kind       string
	config     map[string]interface{}
	layers     []QueryLayer
	fieldOrder []string // the chart layers' output
	// readsAllRows marks a chart that summarizes every row, so the default display
	// LIMIT must not truncate its input.
	readsAllRows bool
}

// histogramPlan is a histogram's buckets over valueExpr, the numeric expression
// resolved while the registry still knows the field. Rebuilding it at window layer
// time emitted a bare identifier the scan never projected (code 47).
type histogramPlan struct {
	buckets   int
	valueExpr string
}

// deferredPlan is the outermost layer, for filters, ordering and a limit on window
// and join outputs, and the source expressions the scan exports to it as hidden
// columns.
type deferredPlan struct {
	where   []predicate
	orderBy []OrderTerm
	limit   string
	scope   *deferredScope
}

// tablePlan is what table() asked for.
type tablePlan struct {
	present      bool
	explicit     bool     // it names its columns
	columns      []string // the columns it names, as written
	joinedFields []string // join() outputs it names, in the order given
}

// pendingFilters are the filters classified by the stage they bind to.
type pendingFilters struct {
	where, having, deferred []Condition
}

// positions is what the planner knows about the pipeline: its commands, and the
// registry, stage and outputs at each position.
type positions struct {
	// registry is the registry a filter resolves its fields against, keyed by
	// where it is written, and stage the stage in effect there.
	registry map[position]*FieldRegistry
	stage    map[position]int
	// stageAfter[i] is the current stage index once command i has executed, and
	// outputs[i] the columns the query returns then.
	stageAfter []int
	outputs    map[int][]string
	// declared[i] is what command i registers as its outputs.
	declared [][]FieldEntry
	// firstAgg is the index of the first aggregating command, or the number of
	// commands when none aggregates.
	firstAgg int
	// commands is the pipeline being planned.
	commands []CommandNode
}

// pgrPlan holds the provenance filters this plan compiled, and the scan filters
// that narrow their candidates (see narrowPgrCandidates).
type pgrPlan struct {
	filters   []*candidateFilter
	narrowing []predicate
}

// lookupPlan is the join of a model's scores, or pgrContext()'s context, onto the
// rows, set by modelLookup() or pgrContext().
type lookupPlan struct {
	Fields   []string // output field names added to outer SELECT
	KeyExprs []string // key expressions over the log scan: the scan-level ON's left side and the prefilter's
	Strict   bool     // true (default) = INNER JOIN: keep only rows the model scored
	Cmd      int      // index of the lookup command
	name     string   // the command holding the lookup join: modelLookup or pgrContext
	// Where: filters on model columns alone, applied to the model side of a strict
	// join (and so to its scan prefilter) rather than to the joined rows. Each is a
	// property of the key, so the join keeps exactly the rows it would have kept.
	Where []predicate
	// keys is the model's key set: its key columns, its topology, its scoring
	// subquery and the set the strict scan prefilter tests. nil without a lookup.
	keys *modelKeySet
	// Outputs: the model's declared output column names. Set at Declare (unlike
	// Fields, set at Execute) so commands that run before the lookup's Execute can
	// still recognize a model column by name.
	Outputs []string
	// AtScan: no aggregation precedes the lookup, so the model JOIN sits in the
	// source stage's FROM and every command after it reads the model's columns as
	// ordinary per-row columns: assignments, transforms, filters, sorts, table()
	// and aggregations alike. Kept flat so a GLOBAL join still distributes: shards
	// join their local scan against the broadcast model set and pre-aggregate,
	// rather than shipping scan rows to the initiator. After an aggregation it
	// joins the aggregated rows instead, below the stage that reads them (ModelJoin).
	AtScan bool
	// PostJoinWhere: filters that read the scan-level join's columns, at the
	// pipeline position each was written, applied to the joined rows.
	PostJoinWhere []postJoinFilter
}

// joinPlan is a join() of the query's rows with a subquery's.
type joinPlan struct {
	Type    string   // "inner" or "left"
	Key     string   // field to join on
	KeyExpr string   // outer join-key expression, projected as the hidden _join_k column
	SubSQL  string   // translated subquery SQL, "" until join() has run
	Include []string // fields to include from subquery (empty = all)
	Outputs []string // subquery output columns, surfaced as _join_<col>
	MaxRows int      // max rows for subquery safety limit
	Cmd     int      // index of the join command
}

// postJoinFilter is a filter on the scan-level model join's rows. At is the number
// of commands before it, which orders it against the row caps.
type postJoinFilter struct {
	At   int
	Pred predicate
}

// NewQueryPlan creates a plan with a single source stage.
func NewQueryPlan() *QueryPlan {
	return &QueryPlan{
		Stages: []QueryStage{
			{IsSource: true},
		},
		chart:              chartPlan{config: make(map[string]interface{})},
		aggregationOutputs: make(map[string]bool),
	}
}

// CurrentStage returns the active stage commands should write to.
func (p *QueryPlan) CurrentStage() *QueryStage {
	return &p.Stages[len(p.Stages)-1]
}

// registryAt is the registry a filter written at pos resolves its fields
// against: the registry as the pipeline left it there, or fallback outside a
// planned pipeline.
func (p *QueryPlan) registryAt(pos position, fallback *FieldRegistry) *FieldRegistry {
	if v, ok := p.pos.registry[pos]; ok {
		return v
	}
	return fallback
}

// rewrittenAfterAggregation reports whether a command between the first
// aggregation and the first n commands declares field.
func (p *QueryPlan) rewrittenAfterAggregation(field string, n int) bool {
	return n > p.pos.firstAgg+1 && p.declaredBetween(field, p.pos.firstAgg+1, n)
}

// declaredBetween reports whether a command in [from, to) declares name.
func (p *QueryPlan) declaredBetween(name string, from, to int) bool {
	for _, entries := range p.pos.declared[from:min(to, len(p.pos.declared))] {
		if slices.ContainsFunc(entries, func(e FieldEntry) bool { return e.Name == name }) {
			return true
		}
	}
	return false
}

// outputNames lists the columns the query returns as planned so far: the stage's
// selects and, while it groups, its keys.
func (p *QueryPlan) outputNames(registry *FieldRegistry) []string {
	stage := p.CurrentStage()
	names := columnNames(stage.Layer.Selects)
	if len(stage.Layer.GroupBy) > 0 {
		for _, alias := range registry.groupKeys {
			if !slices.Contains(names, alias) {
				names = append(names, alias)
			}
		}
	}
	return names
}

// selectStrings renders each column of a select list.
func selectStrings(selects []SelectExpr) []string {
	out := make([]string, len(selects))
	for i, sel := range selects {
		out[i] = sel.String()
	}
	return out
}

// columnNames lists the names a select list outputs.
func columnNames(selects []SelectExpr) []string {
	names := make([]string, len(selects))
	for i, sel := range selects {
		names[i] = sel.Name()
	}
	return names
}

// SourceStage returns the innermost (FROM logs) stage.
func (p *QueryPlan) SourceStage() *QueryStage {
	return &p.Stages[0]
}

// havingStageIndexAt returns the stage a HAVING filter written after cmdIndex
// commands binds to: the nearest aggregating stage that existed at that point.
// A filter between groupby() and a following count() filters the groups, one
// after it filters the count. Without positions it is the outermost.
func (p *QueryPlan) havingStageIndexAt(cmdIndex int) int {
	if cmdIndex <= 0 || cmdIndex > len(p.pos.stageAfter) {
		return p.havingStageIndex()
	}
	for i := p.pos.stageAfter[cmdIndex-1]; i >= 0; i-- {
		if len(p.Stages[i].Layer.GroupBy) > 0 || p.Stages[i].Aggregates {
			return i
		}
	}
	return p.havingStageIndex()
}

// afterRowKeeping reports whether the last aggregating command among the first n
// scores each row against all of them.
func (p *QueryPlan) afterRowKeeping(n int) bool {
	at := lastAggregatingCommandIndex(p.pos.commands, n)
	return at >= 0 && commandScoresAgainstAllRows(p.pos.commands[at].Name)
}

// capStageAt returns the stage wrapping the last cap on aggregated rows before the
// first n commands, or -1 when none precedes.
func (p *QueryPlan) capStageAt(n int) int {
	if n <= 0 || n > len(p.pos.stageAfter) {
		return -1
	}
	for i := p.pos.stageAfter[n-1]; i >= 0; i-- {
		if p.Stages[i].AfterCap {
			return i
		}
	}
	return -1
}

// havingStageIndex returns the stage a HAVING filter binds to without a position:
// the outermost stage with a GROUP BY, or the source stage.
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
	return len(p.deferred.where) > 0 || len(p.deferred.orderBy) > 0 || p.deferred.limit != ""
}

// deferredScope returns the plan's scope for expressions relocated above the source
// scan, creating it on first use.
func (p *QueryPlan) deferredScope() *deferredScope {
	if p.deferred.scope == nil {
		p.deferred.scope = newDeferredScope()
	}
	return p.deferred.scope
}

// exportDeferredColumns projects the hidden columns the deferred layer needs into the
// source scan. Nothing to do unless a relocated expression actually referenced a
// source expression.
func (p *QueryPlan) exportDeferredColumns() error {
	if !p.deferred.scope.used() {
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
	if err := p.validateJoinKeysSurviveAggregation(source, "the filtered or sorted field", p.deferred.scope.exprs, p.deferred.scope.labels); err != nil {
		return err
	}
	if len(source.Layer.Selects) == 0 {
		// The scan would fall back to renderStandard's default projection, which
		// appending to would replace rather than extend. Assembly runs before this,
		// so no real pipeline reaches here; fail loudly rather than silently drop
		// the query's columns.
		return fmt.Errorf("cannot evaluate a filter or sort on a log field at this point in the pipeline")
	}
	source.Layer.Selects = append(source.Layer.Selects, p.deferred.scope.projections()...)
	p.deferred.scope.markExported()
	return nil
}

// PushStage adds a new empty stage to the pipeline.
// Subsequent commands writing to CurrentStage() will write to this new stage.
// The new stage's SELECT, GROUP BY, etc. are initially empty.
func (p *QueryPlan) PushStage() {
	p.Stages = append(p.Stages, QueryStage{})
}

// Render converts the QueryPlan into a final SQL string.
// renderHook, when set, sees every plan as it renders. Tests use it to check
// invariants across the whole suite.
var renderHook func(*QueryPlan)

func (p *QueryPlan) Render(opts QueryOptions) (string, error) {
	if renderHook != nil {
		renderHook(p)
	}
	sql, err := p.renderStandard(opts)
	if err != nil {
		return "", err
	}
	// A binding the query uses more than once is materialised once, ahead of
	// everything, instead of having its subquery pasted in at each use.
	return p.withClause() + sql, nil
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

// renderStandard renders the plan's stages: the scan SELECT, then the stages and
// layers that wrap it.
func (p *QueryPlan) renderStandard(opts QueryOptions) (string, error) {
	source := p.SourceStage()
	if err := checkScopeNotShadowed(source.Layer.Selects); err != nil {
		return "", err
	}
	selectClause, err := p.scanSelectClause(source, opts)
	if err != nil {
		return "", err
	}
	p.placePrefilters(source, opts)
	scan, err := p.scanFrom(source, opts)
	if err != nil {
		return "", err
	}
	limitBy := p.wrapScanDedup(&scan, source, opts)
	p.joinModelAfterCaps(&scan, source, opts)
	sql := p.wrapOuterLayers(renderSelect(selectClause, scan.from, scan.pending, &source.Layer, limitBy))

	// Validate for injection-style patterns. A subquery source (a resolved source
	// command like pgr()) is trusted machine-generated SQL that legitimately contains
	// UNION ALL (its edge-type union) and its only user input (guids/start) is already
	// escaped; exempt it from the check while still validating the user-generated
	// wrapper around it.
	toValidate := sql
	if opts.SourceSubquery != "" {
		toValidate = strings.Replace(toValidate, opts.SourceSubquery, "source_subquery", 1)
	}
	if err := validateGeneratedSQL(toValidate); err != nil {
		return "", err
	}
	return sql, nil
}

// scanSelectClause is the scan's SELECT list, with the hidden join key a join()
// on a log field needs.
func (p *QueryPlan) scanSelectClause(source *QueryStage, opts QueryOptions) (string, error) {
	var selectClause string
	switch {
	case len(source.Layer.Selects) > 0:
		selectClause = strings.Join(selectStrings(source.Layer.Selects), ", ")
	case opts.SourceSubquery != "" && len(opts.SourceColumns) > 0:
		// A subquery source (pgr composition) has its flat columns, no norm_log.
		selectClause = strings.Join(opts.SourceColumns, ", ")
	default:
		// Both hot and iceberg archives project the norm_log column directly
		// (materialized in the hot store; a plain JSON String in the archive).
		selectClause = "toString(timestamp) as timestamp, norm_log, log_id"
	}
	// The outer key expression of a join() derives from `fields.X`, which exists
	// only in this scan, so it is projected as a hidden `_join_k` column for the ON
	// clause (stripped by wrapWithJoin). A key the outer SELECT already names (a
	// groupby output) needs no projection, and over a subquery source every column
	// is already flat.
	j := p.join
	if j == nil || j.SubSQL == "" {
		return selectClause, nil
	}
	if opts.SourceSubquery == "" && !p.outerHasColumn(j.Key) {
		j.KeyExpr = jsonFieldRef(j.Key)
	}
	if j.KeyExpr == "" {
		return selectClause, nil
	}
	if err := p.validateJoinKeysSurviveAggregation(source, "join() key", []string{j.KeyExpr}, []string{j.Key}); err != nil {
		// The aggregation runs before the join, which is not what a reader of
		// `join() | count()` expects, so name the filter that does run first.
		return "", fmt.Errorf("join() is applied to the aggregated rows, so the aggregation must keep the key %[1]s: group by it, or to count or aggregate only the rows the subquery matches, filter first with let &keys := <subquery>; ... | in(%[1]s, &keys)", j.Key)
	}
	return selectClause + fmt.Sprintf(", %s AS %s", j.KeyExpr, joinHiddenKeyCol), nil
}

// placePrefilters filters the scan by the keys an inner join() or a strict model
// lookup keeps, each at the position of its command, so a row cap after it counts
// only rows the join keeps. The model's prefilter in the scan's own WHERE is also
// the only place its key set can prune granules through the key sub-columns' skip
// indexes.
func (p *QueryPlan) placePrefilters(source *QueryStage, opts QueryOptions) {
	place := func(pf predicate, at int) {
		if pf.empty() {
			return
		}
		if after := source.Layer.capAfter(at); after != nil {
			*after = append(*after, pf)
		} else {
			source.Layer.Where = append(source.Layer.Where, pf)
		}
	}
	if pf := p.joinPrefilter(source, opts); !pf.empty() {
		place(pf, p.join.Cmd)
	}
	place(p.modelPrefilter(), p.lookup.Cmd)
}

// scanSource is what the scan SELECT reads FROM and the filters it still has to
// apply. joined records that the scan-level model join is already in from.
type scanSource struct {
	from    string
	pending []predicate
	joined  bool
}

// scanFrom builds the row source of the scan: the table, wrapped by the archive's
// dedup and by each row cap in turn, each taking the filters written before it.
func (p *QueryPlan) scanFrom(source *QueryStage, opts QueryOptions) (scanSource, error) {
	scan := scanSource{from: opts.EffectiveTableName(), pending: source.Layer.Where}
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
		scan.from = "(SELECT * FROM " + scan.from + whereClause(scan.pending) + " LIMIT 1 BY log_id)"
		scan.pending = nil
	}
	// A scan-level model join sits among the caps at its position (see
	// scanJoinPosition).
	joinAt := -1
	if p.hasModelLookup() && p.lookup.AtScan {
		joinAt = p.scanJoinPosition(&source.Layer)
	}
	caps := source.Layer.Caps
	for i, c := range caps {
		where := scan.pending
		if i > 0 {
			where = caps[i-1].After
		}
		if joinAt >= 0 && i >= joinAt {
			after := -1
			if i == joinAt {
				scan.from = p.joinModelAtScan(scan.from)
				scan.joined = true
			} else {
				after = caps[i-1].At
			}
			where = append(slices.Clone(where), p.postJoinSQL(after, c.At)...)
		}
		if c.LimitBy != "" && c.ScanLimitBy == "" {
			return scanSource{}, fmt.Errorf("dedup() on a computed field can only be followed by a transform; dedup on a log field instead")
		}
		order := c.OrderBy
		if len(order) == 0 {
			order = defaultRowOrder(opts)
		}
		capSQL := "(SELECT * FROM " + scan.from + whereClause(where)
		if len(order) > 0 {
			capSQL += " ORDER BY " + orderSQL(order)
		}
		for _, part := range []string{c.ScanLimitBy, c.Limit} {
			if part != "" {
				capSQL += " " + part
			}
		}
		scan.from = capSQL + ")"
	}
	if n := len(caps); n > 0 {
		scan.pending = caps[n-1].After
		if scan.joined {
			scan.pending = append(slices.Clone(scan.pending), p.postJoinSQL(caps[n-1].At, -1)...)
		}
	}
	return scan, nil
}

// wrapScanDedup wraps the scan in a pre-aggregation dedup, which has to collapse
// rows before the SELECT aggregates them (see QueryLayer.ScanLimitBy). It returns
// the LIMIT BY the scan SELECT itself renders.
func (p *QueryPlan) wrapScanDedup(scan *scanSource, source *QueryStage, opts QueryOptions) string {
	layer := &source.Layer
	if layer.ScanLimitBy == "" {
		return layer.LimitBy
	}
	if len(layer.GroupBy) == 0 && !p.IsAggregated {
		// Without an aggregation it renders on this SELECT.
		return layer.ScanLimitBy
	}
	// SELECT * rather than a column list: ClickHouse prunes the columns the outer
	// query does not read out of a star projection through LIMIT BY, but reads
	// every column an explicit list names (measured on one window: 1.8MiB vs
	// 168MiB), and the wrapper stays independent of what the aggregation above it
	// references.
	order := ""
	if o := dedupScanOrder(opts); len(o) > 0 {
		// LIMIT BY keeps the first row of each key, so ordering the scan is what
		// makes the survivor the newest event (the documented behaviour) rather
		// than whichever row was read first.
		order = " ORDER BY " + orderSQL(o)
	}
	scan.from = "(SELECT * FROM " + scan.from + whereClause(scan.pending) + order + " " + layer.ScanLimitBy + ")"
	scan.pending = nil
	return layer.LimitBy
}

// joinModelAfterCaps puts a scan-level model join no cap reads through into the
// scan's FROM, so its rows carry the model columns. Filters on those columns go
// in the scan's WHERE; scan-only predicates stay wherever they already are (the
// same WHERE, or inside a dedup wrap), where ClickHouse pushes them into the scan.
func (p *QueryPlan) joinModelAfterCaps(scan *scanSource, source *QueryStage, opts QueryOptions) {
	if !p.hasModelLookup() || !p.lookup.AtScan || scan.joined {
		return
	}
	if !p.selectsRowsBelowModelJoin(source, opts) {
		scan.from = p.joinModelAtScan(scan.from)
		scan.pending = append(slices.Clone(scan.pending), p.postJoinSQL(-1, -1)...)
		return
	}
	// Nothing after the join selects rows, so this SELECT returns the rows the scan
	// alone would. A narrow scan picks them, reading in sorting-key order and
	// stopping early, and only those are joined, read back by sorting key.
	order := orderSQL(source.Layer.OrderBy)
	limit := source.Layer.Limit
	if source.Layer.LimitBy != "" {
		limit = source.Layer.LimitBy + " " + limit
	}
	picked := fmt.Sprintf("(timestamp, log_id) %s (SELECT timestamp, log_id FROM %s%s ORDER BY %s %s)",
		opts.subqueryIn(false), scan.from, whereClause(scan.pending), order, limit)
	// The joined read keeps the filters: log_id is not unique, so a row sharing a
	// picked (timestamp, log_id) may fail them. The strict prefilter is left to the
	// INNER JOIN, which applies exactly the same key set.
	pf := p.modelPrefilter()
	joinedWhere := append(slices.DeleteFunc(slices.Clone(source.Layer.Where), func(w predicate) bool { return w == pf }), foreignAtom(picked))
	// The join hashes the few picked rows and streams the model past them, rather
	// than hashing every scored key. They carry their filters with them, so a
	// GLOBAL join on a cluster ships only those rows.
	star := "*"
	if opts.IncludeShardNum {
		star = "*, _shard_num"
	}
	kind := " INNER JOIN "
	if !p.lookup.Strict {
		kind = " RIGHT JOIN "
	}
	scan.from = "(" + p.modelJoinSource() + ") AS _mlookup" + p.globalJoin() + kind +
		"(SELECT " + star + " FROM " + scan.from + whereClause(joinedWhere) + ") AS _outer ON " + p.modelJoinKeys(p.lookup.KeyExprs)
	scan.pending = nil
}

// renderSelect renders one SELECT over from with the layer's grouping, ordering
// and limits.
func renderSelect(selectClause, from string, where []predicate, layer *QueryLayer, limitBy string) string {
	var sql strings.Builder
	sql.WriteString("SELECT " + selectClause + " FROM " + from + whereClause(where))
	if len(layer.GroupBy) > 0 {
		sql.WriteString(" GROUP BY " + strings.Join(groupExprs(layer.GroupBy), ", "))
	}
	if len(layer.Having) > 0 {
		sql.WriteString(" HAVING " + andOf(layer.Having...).sql)
	}
	if len(layer.OrderBy) > 0 {
		sql.WriteString(" ORDER BY " + orderSQL(layer.OrderBy))
	}
	for _, part := range []string{limitBy, layer.Limit} {
		if part != "" {
			sql.WriteString(" " + part)
		}
	}
	return sql.String()
}

// wrapOuterLayers wraps the scan in the later stages, the join() wrapper, the
// formatter, the window layers and the deferred filters, in that order. The
// formatter goes before the window layers so z-score and histogram wrap the
// formatted output.
func (p *QueryPlan) wrapOuterLayers(sql string) string {
	for _, stage := range p.Stages[1:] {
		sql = wrapWithLayer(sql, stage.Layer)
	}
	if p.join != nil && p.join.SubSQL != "" {
		sql = p.wrapWithJoin(sql)
	}
	if len(p.format.selects) > 0 {
		sql = "SELECT " + strings.Join(selectStrings(p.format.selects), ", ") + " FROM (" + sql + ")"
		if len(p.format.orderBy) > 0 {
			sql += " ORDER BY " + orderSQL(p.format.orderBy)
		}
		if p.format.limit != "" {
			sql += " " + p.format.limit
		}
	}
	for _, wl := range p.window.layers {
		sql = wrapWithLayer(sql, wl)
	}
	if !p.hasDeferredWrap() {
		return sql
	}
	outer := "SELECT *"
	if p.deferred.scope.exported {
		// The exported columns exist only to make this layer's expressions
		// resolvable; drop them from the result.
		outer += " EXCEPT (" + strings.Join(p.deferred.scope.names, ", ") + ")"
	}
	outer += " FROM (" + sql + ")"
	outer += whereClause(p.deferred.where)
	if len(p.deferred.orderBy) > 0 {
		outer += " ORDER BY " + orderSQL(p.deferred.orderBy)
	}
	if p.deferred.limit != "" {
		outer += " " + p.deferred.limit
	}
	return outer
}

// dedupScanOrder returns the ORDER BY that decides which row of each dedup key
// survives: newest first. A subquery source (pgr()) exposes flat columns
// instead of the logs table, so it is ordered only by the ones it actually has.
func dedupScanOrder(opts QueryOptions) []OrderTerm {
	if opts.SourceSubquery == "" {
		return []OrderTerm{desc("timestamp"), desc("log_id")}
	}
	var terms []OrderTerm
	for _, c := range opts.SourceColumns {
		if c == "timestamp" || c == "log_id" {
			terms = append(terms, desc(c))
		}
	}
	return terms
}

// defaultRowOrder is the ordering a row cap keeps rows by when its command gave
// none: the source's own, else newest first.
func defaultRowOrder(opts QueryOptions) []OrderTerm {
	if len(opts.SourceOrderBy) > 0 {
		return opts.SourceOrderBy
	}
	return dedupScanOrder(opts)
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
	outer.WriteString(whereClause(layer.Where))
	if len(layer.GroupBy) > 0 {
		outer.WriteString(" GROUP BY ")
		outer.WriteString(strings.Join(groupExprs(layer.GroupBy), ", "))
	}
	if len(layer.Having) > 0 {
		outer.WriteString(" HAVING ")
		outer.WriteString(andOf(layer.Having...).sql)
	}
	if len(layer.OrderBy) > 0 {
		outer.WriteString(" ORDER BY ")
		outer.WriteString(orderSQL(layer.OrderBy))
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
	if p.join.Type == "left" {
		joinType = "LEFT"
	}

	// Resolve the join key for the outer query. If the outer SELECT has the join
	// key as a named column (e.g. from groupby), use it directly; otherwise the
	// source scan projected it as the hidden _join_k column (see renderStandard),
	// which is the only reference that resolves inside _outer.
	outerKeyRef := p.join.Key
	if p.join.KeyExpr != "" {
		outerKeyRef = joinHiddenKeyCol
	}

	var sql strings.Builder
	sql.WriteString("SELECT _outer.*")
	if p.join.KeyExpr != "" {
		sql.WriteString(fmt.Sprintf(" EXCEPT (%s)", joinHiddenKeyCol))
	}

	// Every subquery column comes through under a `_join_` prefix. A bare
	// `_join_sub.*` would re-emit the join key and collide with same-named outer
	// columns (an aggregating outer query and its subquery both produce `_count`),
	// leaving the result with duplicate names that no filter or sort can reference.
	for _, col := range joinCarriedColumns(p.join.Outputs, p.join.Include, p.join.Key) {
		sql.WriteString(fmt.Sprintf(", _join_sub.%s AS %s", col, joinPrefixed(col)))
	}

	sql.WriteString(" FROM (")
	sql.WriteString(outerSQL)
	sql.WriteString(") AS _outer ")
	sql.WriteString(joinType)
	sql.WriteString(" JOIN (")
	sql.WriteString(p.join.SubSQL)
	sql.WriteString(") AS _join_sub ON _outer.")
	sql.WriteString(outerKeyRef)
	sql.WriteString(" = _join_sub.")
	sql.WriteString(p.join.Key)

	if joinType == "INNER" {
		return sql.String()
	}
	// Unmatched rows read NULL instead of the column default, so 0 is not a count
	// a filter can match. Wrapped because callers append ORDER BY and LIMIT.
	return "SELECT * FROM (" + sql.String() + " SETTINGS join_use_nulls = 1)"
}

// innerJoinWrap reports whether a join() INNER JOINs the outer rows.
func (p *QueryPlan) innerJoinWrap() bool {
	return p.join != nil && p.join.SubSQL != "" && p.join.Type != "left"
}

// joinPrefilter is the scan predicate that keeps only rows whose join key the
// subquery returns, or "" when the key has no scan-level expression.
func (p *QueryPlan) joinPrefilter(source *QueryStage, opts QueryOptions) predicate {
	if !p.innerJoinWrap() || len(p.Stages) != 1 || opts.SourceSubquery != "" || p.Chain != nil {
		return predicate{}
	}
	// Same resolution as renderStandard's hidden _join_k projection.
	keyExpr := jsonFieldRef(p.join.Key)
	if p.outerHasColumn(p.join.Key) {
		keyExpr = p.outerKeyScanExpr(source)
	}
	if keyExpr == "" {
		return predicate{}
	}
	return foreignAtom(fmt.Sprintf("%s %s (SELECT %s FROM (%s))", keyExpr, opts.subqueryIn(false), p.join.Key, p.join.SubSQL))
}

// outerKeyScanExpr returns the per-row expression the outer SELECT names the join
// key after. An aggregating scan qualifies only when it groups by the key, so
// every row of a group shares it and the group is kept or dropped whole.
func (p *QueryPlan) outerKeyScanExpr(source *QueryStage) string {
	for _, sel := range source.Layer.Selects {
		if sel.Alias == "" || sel.Name() != p.join.Key {
			continue
		}
		expr := sel.Expr
		if len(source.Layer.GroupBy) > 0 || p.IsAggregated {
			if !slices.ContainsFunc(source.Layer.GroupBy, func(g groupKey) bool {
				return g.Name == p.join.Key || g.Expr == expr || unquoteIdent(g.Expr) == p.join.Key
			}) {
				return ""
			}
		}
		return expr
	}
	return ""
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
		grouped[unquoteIdent(g.Expr)] = true
	}
	// A group key written as an alias (GROUP BY src_ip) also covers the expression
	// that alias was defined from in this stage's SELECT.
	for _, sel := range source.Layer.Selects {
		if sel.Alias != "" && grouped[sel.Name()] {
			grouped[sel.Expr] = true
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
	if !p.lookup.Strict {
		kind = " LEFT JOIN ("
	}
	return p.globalJoin() + kind + p.modelJoinSource() + ") AS _mlookup ON " + p.modelJoinKeys(left)
}

// globalJoin is " GLOBAL" when the model table is Distributed. Without it ClickHouse
// refuses the double-distributed subquery outright (distributed_product_mode='deny',
// code 288); with it the initiator reads the model across every shard once.
func (p *QueryPlan) globalJoin() string {
	if p.lookup.keys.global {
		return " GLOBAL"
	}
	return ""
}

// modelJoinKeys is the ON condition matching key columns left to the model's.
func (p *QueryPlan) modelJoinKeys(left []string) string {
	right := make([]string, len(p.lookup.keys.rightCols))
	for i, c := range p.lookup.keys.rightCols {
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
	if len(p.lookup.Where) == 0 {
		return p.lookup.keys.scoringSQL
	}
	return "SELECT * FROM (" + p.lookup.keys.scoringSQL + ")" + whereClause(p.lookup.Where)
}

// hasModelLookup reports whether a model_lookup() joins the model's scores.
func (p *QueryPlan) hasModelLookup() bool { return p.lookup.keys != nil }

// modelPrefilter is a strict join's semi-join pushed down into the source scan:
// only logs whose key the model scores can survive the INNER JOIN, and the scan is
// the one place the key sub-columns' skip indexes can prune on that set. Filters on
// model columns narrow the set exactly as they narrow the model side.
func (p *QueryPlan) modelPrefilter() predicate {
	if !p.hasModelLookup() || !p.lookup.Strict || len(p.lookup.KeyExprs) == 0 {
		return predicate{}
	}
	return foreignAtom(p.lookup.keys.membership(p.lookup.KeyExprs, p.lookup.Where))
}

// readsThroughCap reports whether the scan-level model join, or a filter on its
// columns, comes after the row cap c and changes which rows c's own ordering and
// limit select if they are applied in the same SELECT: a strict join drops rows,
// and a post-join filter is written after it.
func (p *QueryPlan) readsThroughCap(c RowCap) bool {
	if p.innerJoinWrap() && c.At < p.join.Cmd {
		return true
	}
	if !p.hasModelLookup() || !p.lookup.AtScan {
		return false
	}
	if len(p.postJoinSQL(c.At, -1)) > 0 {
		return true
	}
	return p.lookup.Strict && c.At < p.lookup.Cmd && !p.dedupsByModelKeys(c)
}

// dedupsByModelKeys reports whether c is a dedup whose keys include every model
// key. Each group it keeps one row of then shares one key, which the strict join
// keeps or drops whole, so the join selects the same rows on either side of it.
func (p *QueryPlan) dedupsByModelKeys(c RowCap) bool {
	if c.Limit != "" || c.ScanLimitBy == "" {
		return false
	}
	for _, k := range p.lookup.KeyExprs {
		if !slices.Contains(c.scanKeys, k) {
			return false
		}
	}
	return true
}

// postJoinSQL returns the post-join filters written after command index after and
// up to command index upTo (RowCap.At values); -1 leaves that side open.
func (p *QueryPlan) postJoinSQL(after, upTo int) []predicate {
	var out []predicate
	for _, f := range p.lookup.PostJoinWhere {
		if (after < 0 || f.At > after) && (upTo < 0 || f.At <= upTo) {
			out = append(out, f.Pred)
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
	for n < len(l.Caps) && (l.Caps[n].At < p.lookup.Cmd || len(p.postJoinSQL(-1, l.Caps[n].At)) == 0 && capReadsOnlyScan(l.Caps[n])) {
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
	return orderingReadsOnlyScan(c.OrderBy, c.ScanLimitBy, c.scanDeps)
}

// orderingReadsOnlyScan reports whether ORDER BY terms, and LIMIT BY keys reading
// keys, can be evaluated over the logs table's own columns.
func orderingReadsOnlyScan(orderBy []OrderTerm, limitBy string, keys deps) bool {
	for _, o := range orderBy {
		if !o.deps.scan {
			return false
		}
	}
	return limitBy == "" || keys.scan
}

// selectsRowsBelowModelJoin reports whether the source SELECT's own ORDER BY, LIMIT
// BY and LIMIT may pick its rows from the scan before the scan-level model join: the
// join keeps every row, nothing after it selects or aggregates rows, and the
// ordering and dedup keys read only the logs table's columns.
func (p *QueryPlan) selectsRowsBelowModelJoin(source *QueryStage, opts QueryOptions) bool {
	l := source.Layer
	if len(p.lookup.PostJoinWhere) > 0 || l.Limit == "" || len(l.OrderBy) == 0 {
		return false
	}
	// Rows are picked from the table itself, so no row cap may read through first.
	if len(p.Stages) > 1 || len(l.GroupBy) > 0 || len(l.Having) > 0 || l.ScanLimitBy != "" || len(l.Caps) > 0 ||
		p.IsAggregated || p.Chain != nil || opts.SourceMode == SourceIceberg || opts.SourceSubquery != "" {
		return false
	}
	if len(p.window.layers) > 0 || p.hasDeferredWrap() || p.window.zScoreExpr != "" || p.histogram.buckets > 0 || len(p.chart.layers) > 0 {
		return false
	}
	return orderingReadsOnlyScan(l.OrderBy, l.LimitBy, l.limitByDeps)
}

// joinModelAtScan appends the scan-level model join to a row source.
func (p *QueryPlan) joinModelAtScan(rowSource string) string {
	// A wrapped scan (a cap or dedup subquery) must carry an alias to be joined
	// against (ClickHouse code 206); a subquery source already has one.
	if strings.HasPrefix(rowSource, "(") && strings.HasSuffix(rowSource, ")") {
		rowSource += " AS _outer"
	}
	return rowSource + p.modelJoinOn(p.lookup.KeyExprs)
}

// outerHasColumn reports whether the source stage's SELECT already exposes the given
// column under that name.
func (p *QueryPlan) outerHasColumn(column string) bool {
	return p.SourceStage().Layer.columnIndex(column) >= 0
}

// sourceWhereComplete reports whether the source stage's WHERE is the query's
// whole row selection. Every construct that selects rows somewhere else is
// listed: another stage, a GROUP BY or HAVING, a wrapper layer, a predicate
// deferred above the scan, a dedup, a command-set row limit, or a subquery
// carrying its own scope.
func (p *QueryPlan) sourceWhereComplete() bool {
	if len(p.Stages) != 1 || len(p.window.layers) > 0 {
		return false
	}
	src := p.SourceStage().Layer
	if len(src.GroupBy) > 0 || len(src.Having) > 0 || src.LimitBy != "" || src.ScanLimitBy != "" {
		return false
	}
	if len(p.deferred.where) > 0 || len(p.lookup.PostJoinWhere) > 0 || len(p.lookup.Where) > 0 {
		return false
	}
	if p.IsAggregated || p.HasGroupBy || p.Chain != nil || p.special != nil {
		return false
	}
	if p.join != nil || p.hasModelLookup() || p.usesBindingSet || p.rowLimitByCommand {
		return false
	}
	return p.histogram.buckets == 0 && p.window.zScoreExpr == "" && len(p.chart.layers) == 0
}

// SourceProjection is one column the source scan computes, as the alias it is
// known by and the expression behind it.
type SourceProjection struct {
	Alias string
	Expr  string
}

// sourceProjections lists the columns the source stage computes, skipping the
// passthrough of the table's own columns.
func (p *QueryPlan) sourceProjections() []SourceProjection {
	var out []SourceProjection
	for _, sel := range p.SourceStage().Layer.Selects {
		if sel.Alias != "" && sel.Alias != sel.Expr {
			out = append(out, SourceProjection{Alias: sel.Alias, Expr: sel.Expr})
		}
	}
	return out
}
