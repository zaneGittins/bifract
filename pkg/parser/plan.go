package parser

import (
	"fmt"
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
	Limit   string
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
}

// QueryPlan holds the structured representation of a query before rendering to SQL.
type QueryPlan struct {
	Stages           []QueryStage // pipeline of aggregation stages (first = innermost)
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
	IsTraversal bool
	IsAnalyze   bool
	IsChain     bool

	// Traversal-specific fields
	TraversalMode    string
	TraversalChild   string
	TraversalParent  string
	TraversalStart   string
	TraversalInclude []string
	TraversalDepth   int

	// Process-tree (ptg) fields: MV-backed process lineage traversal over proc_lineage.
	IsProcessTree        bool
	ProcessTreeStart     string
	ProcessTreeDepth     int
	ProcessTreeDirection string // "forward" | "backward" | "both"

	// AnalyzeFields-specific fields
	AnalyzeFieldsList      []string
	AnalyzeFieldsScanLimit int

	// Histogram-specific fields
	HistogramField   string
	HistogramBuckets int

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
	ModelLookupSQL       string   // pre-built scoring subquery SQL
	ModelLookupOn        string   // JOIN ON condition (references _outer._mlk_k<i>)
	ModelLookupFields    []string // output field names added to outer SELECT
	ModelLookupKeyExprs  []string // outer join-key expressions, projected as hidden _mlk_k<i> columns
	ModelLookupKeyFields []string // the BQL field names behind ModelLookupKeyExprs (for error messages)

	// Table command tracking
	HasTableCmd             bool
	TableHasExplicitColumns bool
	TableJoinedFields       []string // join/model_lookup outputs named in table(), in the order given

	// Pending conditions: classified by kind after Declare, materialized after Execute
	pendingWhereConditions    []HavingCondition
	pendingHavingConditions   []HavingCondition
	pendingDeferredConditions []HavingCondition

	// Source expressions exported to the deferred layer as hidden columns.
	deferred *deferredScope

	// Chained aggregation state (for sum/avg/etc. on prior aggregation outputs)
	outerAggregations  []string          // expressions for outer (chained) aggregation query
	outerAggFieldOrder []string          // field order for outer aggregation results
	aggregationOutputs map[string]string // tracks agg aliases (_count, _sum, etc.) -> SQL expression
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
	if p.IsTraversal {
		return p.renderTraversal(opts)
	}
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

	// model_lookup() join keys derive from `fields.X`, which only exist in this
	// direct `FROM logs` scan. Project them here as hidden `_mlk_k<i>` columns so
	// the JOIN ON (in wrapWithModelLookup) can reference `_outer._mlk_k<i>`; the
	// wrap strips them from the final output via EXCEPT.
	if len(p.ModelLookupKeyExprs) > 0 {
		if err := p.validateJoinKeysSurviveAggregation(source, "model_lookup() key", p.ModelLookupKeyExprs, p.ModelLookupKeyFields); err != nil {
			return "", err
		}
	}
	for i, keyExpr := range p.ModelLookupKeyExprs {
		selectClause += fmt.Sprintf(", %s AS _mlk_k%d", keyExpr, i)
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
			return "", err
		}
		selectClause += fmt.Sprintf(", %s AS %s", p.JoinKeyExpr, joinHiddenKeyCol)
	}

	var sql strings.Builder
	sql.WriteString("SELECT ")
	sql.WriteString(selectClause)

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
		sql.WriteString(" FROM (SELECT * FROM " + opts.EffectiveTableName())
		if len(source.Layer.Where) > 0 {
			sql.WriteString(" WHERE ")
			sql.WriteString(strings.Join(source.Layer.Where, " AND "))
		}
		sql.WriteString(" LIMIT 1 BY log_id)")
	} else {
		sql.WriteString(" FROM " + opts.EffectiveTableName())

		// WHERE
		if len(source.Layer.Where) > 0 {
			sql.WriteString(" WHERE ")
			sql.WriteString(strings.Join(source.Layer.Where, " AND "))
		}
	}

	// GROUP BY
	if len(source.Layer.GroupBy) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(source.Layer.GroupBy, ", "))
	}

	// HAVING
	if len(source.Layer.Having) > 0 {
		sql.WriteString(" HAVING ")
		sql.WriteString(strings.Join(source.Layer.Having, " AND "))
	}

	// ORDER BY
	if len(source.Layer.OrderBy) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(source.Layer.OrderBy, ", "))
	}

	// LIMIT BY
	if source.Layer.LimitBy != "" {
		sql.WriteString(" ")
		sql.WriteString(source.Layer.LimitBy)
	}

	// LIMIT
	if source.Layer.Limit != "" {
		sql.WriteString(" ")
		sql.WriteString(source.Layer.Limit)
	}

	innerSQL := sql.String()

	// Apply additional stages (chained aggregation, stage index > 0)
	for i := 1; i < len(p.Stages); i++ {
		innerSQL = wrapWithLayer(innerSQL, p.Stages[i].Layer)
	}

	// Apply join wrapping (subquery JOIN)
	if p.IsJoin && p.JoinSubSQL != "" {
		innerSQL = p.wrapWithJoin(innerSQL)
	}

	// Apply model_lookup JOIN wrapping
	if p.ModelLookupSQL != "" {
		innerSQL = p.wrapWithModelLookup(innerSQL)
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
			outer.WriteString(strings.Join(p.DeferredWhere, " AND "))
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

func (p *QueryPlan) renderTraversal(opts QueryOptions) (string, error) {
	source := p.SourceStage()
	result, err := buildTraversalSQL(
		p.TraversalMode,
		p.TraversalChild,
		p.TraversalParent,
		p.TraversalStart,
		p.TraversalDepth,
		p.TraversalInclude,
		source.Layer.Where,
		p.selectFieldStrings(),
		source.Layer.OrderBy,
		source.Layer.Limit,
		source.Layer.Having,
		p.ChartType,
		p.ChartConfig,
		opts,
		p.HasTableCmd,
	)
	if err != nil {
		return "", err
	}
	// Copy result metadata back to plan
	p.FieldOrder = result.FieldOrder
	p.IsAggregated = result.IsAggregated
	return result.SQL, nil
}

func (p *QueryPlan) renderProcessTree(opts QueryOptions) (string, error) {
	source := p.SourceStage()
	result, err := buildProcessTreeSQL(
		p.ProcessTreeStart,
		p.ProcessTreeDepth,
		p.ProcessTreeDirection,
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

// wrapWithLayer wraps an inner SQL string with a QueryLayer's SELECT/WHERE/GROUP BY/HAVING/ORDER BY/LIMIT.
func wrapWithLayer(innerSQL string, layer QueryLayer) string {
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
		outer.WriteString(strings.Join(layer.Where, " AND "))
	}
	if len(layer.GroupBy) > 0 {
		outer.WriteString(" GROUP BY ")
		outer.WriteString(strings.Join(layer.GroupBy, ", "))
	}
	if len(layer.Having) > 0 {
		outer.WriteString(" HAVING ")
		outer.WriteString(strings.Join(layer.Having, " AND "))
	}
	if len(layer.OrderBy) > 0 {
		outer.WriteString(" ORDER BY ")
		outer.WriteString(strings.Join(layer.OrderBy, ", "))
	}
	if layer.LimitBy != "" {
		outer.WriteString(" ")
		outer.WriteString(layer.LimitBy)
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
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%s %s must be part of the grouping to survive aggregation: add %s to the group/table columns, or drop the aggregation",
		what, strings.Join(missing, ", "), strings.Join(missing, ", "))
}

// wrapWithModelLookup wraps the outer query with a LEFT JOIN against the model
// scoring subquery. The outer query projects the join keys as hidden `_mlk_k<i>`
// columns (see renderStandard); the JOIN ON matches them against the model side and
// they are dropped from the result via `EXCEPT`, so only the original columns plus
// the model output fields are returned.
func (p *QueryPlan) wrapWithModelLookup(outerSQL string) string {
	var b strings.Builder
	b.WriteString("SELECT _outer.*")
	if n := len(p.ModelLookupKeyExprs); n > 0 {
		excepts := make([]string, n)
		for i := range p.ModelLookupKeyExprs {
			excepts[i] = fmt.Sprintf("_mlk_k%d", i)
		}
		b.WriteString(fmt.Sprintf(" EXCEPT (%s)", strings.Join(excepts, ", ")))
	}
	for _, f := range p.ModelLookupFields {
		b.WriteString(fmt.Sprintf(", _mlookup.%s", f))
	}
	b.WriteString(" FROM (")
	b.WriteString(outerSQL)
	b.WriteString(") AS _outer LEFT JOIN (")
	b.WriteString(p.ModelLookupSQL)
	b.WriteString(") AS _mlookup ON ")
	b.WriteString(p.ModelLookupOn)
	return b.String()
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

// selectFieldStrings converts the source stage Selects to a flat string slice for legacy functions.
func (p *QueryPlan) selectFieldStrings() []string {
	source := p.SourceStage()
	result := make([]string, len(source.Layer.Selects))
	for i, s := range source.Layer.Selects {
		result[i] = s.String()
	}
	return result
}
