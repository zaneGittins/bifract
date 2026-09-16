package models

import (
	"fmt"
	"strings"
	"time"

	"bifract/pkg/parser"
)

// A model's source query is compiled by the BQL translator rather than rendered
// from the structured Filter list. ddl.go could only render what it had been
// taught (four commands), so every other filter BQL supports was refused in a
// model source. The translator already knows them all, and compiling there means
// one implementation instead of two that must be kept in step.

// sourceScopeFractal and the scope window are the values the predicates are
// compiled with, then removed: the model supplies its own scope and window, and
// leaving the translator's in would bind the model to whatever range happened to
// be used at compile time.
const sourceScopeFractal = "_bf_model_source"

var (
	sourceScopeStart = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	sourceScopeEnd   = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
)

// compileSourcePredicates returns the WHERE predicates a model's source query
// contributes to its scan, with the translator's own scope guards removed.
func compileSourcePredicates(bql string, dicts parser.QueryOptions) ([]string, error) {
	bql = strings.TrimSpace(bql)
	if bql == "" {
		return nil, nil
	}
	pipeline, err := parser.ParseQuery(bql)
	if err != nil {
		return nil, fmt.Errorf("source query: %w", err)
	}
	if err := validateSourcePipeline(pipeline); err != nil {
		return nil, err
	}

	opts := dicts
	opts.StartTime = sourceScopeStart
	opts.EndTime = sourceScopeEnd
	opts.FractalID = sourceScopeFractal
	opts.MaxRows = 1

	res, err := parser.TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("source query: %w", err)
	}
	return stripScopeGuards(res.SourceWhere)
}

// stripScopeGuards removes the three predicates the translator adds for its own
// scope. Their absence is an error rather than a shrug: a changed guard shape
// would otherwise be silently carried into every model's scan, pinning it to a
// sentinel fractal and a century-wide window.
func stripScopeGuards(where []string) ([]string, error) {
	want := map[string]bool{
		fmt.Sprintf("fractal_id = '%s'", sourceScopeFractal):                    false,
		fmt.Sprintf("timestamp >= '%s'", sourceScopeStart.Format(chTimeLayout)): false,
		fmt.Sprintf("timestamp <= '%s'", sourceScopeEnd.Format(chTimeLayout)):   false,
	}
	var out []string
	for _, w := range where {
		if seen, ok := want[w]; ok && !seen {
			want[w] = true
			continue
		}
		out = append(out, w)
	}
	for guard, seen := range want {
		if !seen {
			return nil, fmt.Errorf("source query: the translator's scope guard %q was not found; "+
				"its shape changed and the model's scan would carry it", guard)
		}
	}
	return out, nil
}

// validateSourcePipeline enforces what a model source can mean. A model
// aggregates the rows its source yields, so the source has to yield one row per
// log; and it is evaluated over an incremental window, so it cannot depend on
// data outside that window.
func validateSourcePipeline(pipeline *parser.PipelineNode) error {
	for _, cmd := range pipeline.Commands {
		name := strings.ToLower(cmd.Name)
		// Window safety first: it is the more specific reason where both apply.
		if reason, bad := windowUnsafeCommands[name]; bad {
			return fmt.Errorf("%s() cannot be a model source: %s", cmd.Name, reason)
		}
		if parser.IsAggregatingCommand(name) {
			return fmt.Errorf("%s() cannot be a model source: it collapses rows, and a model "+
				"aggregates the rows its source yields", cmd.Name)
		}
		if rowChanging[name] {
			return fmt.Errorf("%s() cannot be a model source: it reorders or drops rows, which "+
				"has no meaning when the model reads one window at a time", cmd.Name)
		}
	}
	return nil
}

// windowUnsafeCommands depend on data outside the window a cycle reads, so they
// mean something different every time the model runs.
var windowUnsafeCommands = map[string]string{
	"join":         "it reads a second query, which a windowed read would re-evaluate each cycle",
	"chain":        "a sequence can straddle the window boundary",
	"model_lookup": "a model cannot be built from another model's output",
	"modellookup":  "a model cannot be built from another model's output",
	"tlsh":         "its candidate set is resolved per query, not per window",
	"pgr":          "it generates its own source",
	"ptg":          "it generates its own source",
	"comment":      "comment state is not part of the log",
	"comments":     "comment state is not part of the log",
}

// rowChanging commands reorder or truncate, which the model's own aggregation owns.
var rowChanging = map[string]bool{
	"sort": true, "head": true, "tail": true, "limit": true, "dedup": true, "table": true,
}
