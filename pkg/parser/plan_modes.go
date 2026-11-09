package parser

import "fmt"

// queryMode is a query whose SQL replaces the standard rendering of the plan's
// stages: ptg() reads the process tree walked for it, and analyzeFields()
// summarises fields over a sample of the rows.
type queryMode interface {
	translate(plan *QueryPlan, opts QueryOptions) (*TranslationResult, error)
}

// processTreeMode is ptg(), walking from start.
type processTreeMode struct{ start string }

func (m *processTreeMode) translate(plan *QueryPlan, opts QueryOptions) (*TranslationResult, error) {
	if plan.IsAggregated {
		return nil, fmt.Errorf("ptg() cannot be combined with aggregation functions")
	}
	if plan.Chain != nil {
		return nil, fmt.Errorf("ptg() cannot be combined with chain()")
	}
	// proc_lineage is a live ClickHouse table, not part of the Iceberg archive; reject
	// ptg() over archived/recall data rather than silently querying the hot table.
	if opts.SourceMode == SourceIceberg {
		return nil, fmt.Errorf("ptg() operates on live process lineage and is not available over archived data")
	}
	return buildProcessTreeSQL(m.start, plan.SourceStage().Layer.Having, plan.chart.kind, plan.chart.config, opts)
}

// analyzeDefaultScan is how many rows analyzeFields() samples without limit=.
const analyzeDefaultScan = 50000

// analyzeMode is analyzeFields(): the fields it summarises and how many rows it
// samples.
type analyzeMode struct {
	fields    []string
	scanLimit int
}

func (m *analyzeMode) translate(plan *QueryPlan, opts QueryOptions) (*TranslationResult, error) {
	src := plan.SourceStage().Layer
	return buildAnalyzeFieldsSQL(m.fields, m.scanLimit, src.Where, src.Having, src.OrderBy, src.Limit, plan.chart.kind, plan.chart.config, opts)
}

// isProcessTree reports whether the plan is a ptg() query.
func (p *QueryPlan) isProcessTree() bool {
	_, ok := p.special.(*processTreeMode)
	return ok
}
