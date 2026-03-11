package parser

import (
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
	Selects  []SelectExpr
	Where    []string
	GroupBy  []string
	Having   []string
	OrderBy  []string
	LimitBy  string
	Limit    string
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
	Stages       []QueryStage  // pipeline of aggregation stages (first = innermost)
	WindowLayers []QueryLayer  // z-score, histogram wrapping (applied after all stages)
	DeferredWhere []string     // conditions on window fields
	DeferredOrder []string     // ORDER BY on window fields
	DeferredLimit string       // LIMIT when sorting by window fields
	Formatters   []SelectExpr  // outer SELECT: timestamp formatting, deferred math

	FieldOrder   []string
	IsAggregated bool
	HasGroupBy   bool
	GroupByCount int            // number of groupby commands encountered (for multi-groupby)
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

	// AnalyzeFields-specific fields
	AnalyzeFieldsList     []string
	AnalyzeFieldsScanLimit int

	// Histogram-specific fields
	HistogramField   string
	HistogramBuckets int

	// Z-score/MAD window-specific fields
	ModifiedZScoreExpr string
	OutlierThreshold   string
	MADWindowExpr      string
	ZScoreFilters      []string

	// Table command tracking
	HasTableCmd bool

	// Pending conditions: classified by kind after Declare, materialized after Execute
	pendingWhereConditions    []HavingCondition
	pendingHavingConditions   []HavingCondition
	pendingDeferredConditions []HavingCondition

	// Chained aggregation state (for sum/avg/etc. on prior aggregation outputs)
	outerAggregations  []string            // expressions for outer (chained) aggregation query
	outerAggFieldOrder []string            // field order for outer aggregation results
	aggregationOutputs map[string]string   // tracks agg aliases (_count, _sum, etc.) -> SQL expression
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
	} else {
		selectClause = "formatDateTime(timestamp, '%Y-%m-%d %H:%i:%S') as timestamp, raw_log, log_id"
	}

	var sql strings.Builder
	sql.WriteString("SELECT ")
	sql.WriteString(selectClause)
	sql.WriteString(" FROM logs")

	// WHERE
	if len(source.Layer.Where) > 0 {
		sql.WriteString(" WHERE ")
		sql.WriteString(strings.Join(source.Layer.Where, " AND "))
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
		innerSQL = outer.String()
	}

	// Apply window layers (z-score, histogram wrapping)
	for _, wl := range p.WindowLayers {
		innerSQL = wrapWithLayer(innerSQL, wl)
	}

	// Apply deferred conditions/ordering (post-window filters)
	if len(p.DeferredWhere) > 0 || len(p.DeferredOrder) > 0 || p.DeferredLimit != "" {
		var outer strings.Builder
		outer.WriteString("SELECT * FROM (")
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

	if err := validateGeneratedSQL(innerSQL); err != nil {
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

// selectFieldStrings converts the source stage Selects to a flat string slice for legacy functions.
func (p *QueryPlan) selectFieldStrings() []string {
	source := p.SourceStage()
	result := make([]string, len(source.Layer.Selects))
	for i, s := range source.Layer.Selects {
		result[i] = s.String()
	}
	return result
}
