package models

import (
	"fmt"
	"sort"
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
	// A model reads only the scan predicates, so anything that selects rows above
	// the scan would be silently dropped and the model would aggregate more rows
	// than the author asked for.
	if !res.SourceWhereComplete {
		return nil, fmt.Errorf("source query: this query selects rows after the scan " +
			"(an aggregation, a window, a dedup or a subquery), and a model reads only " +
			"the filter its source applies to each log")
	}
	preds, err := stripScopeGuards(res.SourceWhere)
	if err != nil {
		return nil, err
	}
	if err := checkNoScopeLeak(preds); err != nil {
		return nil, err
	}
	return preds, nil
}

// checkNoScopeLeak refuses predicates that still carry the compile-time scope.
// stripScopeGuards only removes the translator's own top-level guards; a
// construct that embeds a subquery (a result-set binding) carries a second copy
// inside it, which would pin the model's scan to a sentinel fractal and match
// nothing.
func checkNoScopeLeak(preds []string) error {
	for _, p := range preds {
		switch {
		case strings.Contains(p, sourceScopeFractal):
			return fmt.Errorf("source query: a subquery in this source carries its own fractal scope, "+
				"which a model cannot re-scope; predicate was %q", p)
		case strings.Contains(p, sourceScopeStart.Format(chTimeLayout)),
			strings.Contains(p, sourceScopeEnd.Format(chTimeLayout)):
			return fmt.Errorf("source query: a subquery in this source carries its own time window, "+
				"which a model reads one window at a time; predicate was %q", p)
		}
	}
	return nil
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

// sourceProducedFields names the columns a source query introduces: assignment
// targets and command outputs. They live only inside the translated query, and a
// model builds its own SELECT from the scan predicates, so keying on one would
// read an absent log field and index empty values.
func sourceProducedFields(bql string) map[string]bool {
	out := map[string]bool{}
	if strings.TrimSpace(bql) == "" {
		return out
	}
	pipeline, err := parser.ParseQuery(bql)
	if err != nil {
		return out
	}
	for _, a := range pipeline.Assignments {
		if a.Field != "" {
			out[a.Field] = true
		}
	}
	for _, cmd := range pipeline.Commands {
		b, err := parser.BindCommand(cmd)
		if err != nil {
			continue
		}
		// as=/output= name a single output; include= names one per entry, which is
		// how match(), geoip() and lookupIP() add their enrichment columns.
		for _, name := range append(b.Strings("include"), b.Str("as", ""), b.Str("output", "")) {
			if name != "" {
				out[name] = true
			}
		}
		if p := b.StrOf("pattern", "regex"); p != "" {
			for _, g := range parser.NamedCaptureGroups(p) {
				out[g] = true
			}
		}
	}
	return out
}

// validateSourceProducedKeys rejects a definition that shapes its state around a
// column only the source query computes.
func validateSourceProducedKeys(def ModelDefinition) error {
	produced := map[string]bool{}
	for _, f := range computedFields(def.SourceBQL, def.Extractions) {
		produced[f] = true
	}
	if len(produced) == 0 {
		return nil
	}
	nf := def.Network.WithDefaults()
	named := []struct{ what, value string }{
		{"partition_key", def.PartitionKey},
		{"value_key", def.ValueKey},
		{"src_field", nf.SrcField},
		{"dst_field", nf.DstField},
		{"port_field", nf.PortField},
		{"duration_field", nf.DurationField},
		{"bytes_field", nf.BytesField},
	}
	for _, kf := range def.KeyFields {
		named = append(named, struct{ what, value string }{"key_fields", kf})
	}
	for _, ext := range def.Extractions {
		named = append(named, struct{ what, value string }{"extraction from_field", ext.FromField})
	}
	for _, n := range named {
		if n.value != "" && produced[n.value] {
			return fmt.Errorf("%s %q is computed by the source query, not stored on the log: "+
				"a model reads only the source's filter, so that column does not exist when its state is built", n.what, n.value)
		}
	}
	return nil
}

// computedFields is sourceProducedFields as a sorted list, less the extraction
// outputs the model renders itself, which do reach the model's state.
func computedFields(bql string, extractions []ExtractionStep) []string {
	produced := sourceProducedFields(bql)
	for _, ext := range extractions {
		delete(produced, ext.OutputField)
	}
	out := make([]string, 0, len(produced))
	for name := range produced {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
