package models

import (
	"fmt"
	"regexp"
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

// compiledSource is what a model keeps from its source query: the predicates its
// scan applies, and the columns that scan computes. Both are needed: a predicate
// may name a computed column rather than inline it, and a key may be one.
type compiledSource struct {
	preds       []string
	projections []parser.SourceProjection
}

// compileSourcePredicates returns the WHERE predicates a model's source query
// contributes to its scan, with the translator's own scope guards removed, plus
// the columns the scan computes.
func compileSourcePredicates(bql string, dicts parser.QueryOptions) (compiledSource, error) {
	var out compiledSource
	bql = strings.TrimSpace(bql)
	if bql == "" {
		return out, nil
	}
	pipeline, err := parser.ParseQuery(bql)
	if err != nil {
		return out, fmt.Errorf("source query: %w", err)
	}
	if err := validateSourcePipeline(pipeline); err != nil {
		return out, err
	}

	opts := dicts
	opts.StartTime = sourceScopeStart
	opts.EndTime = sourceScopeEnd
	opts.FractalID = sourceScopeFractal
	opts.MaxRows = 1

	res, err := parser.TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		return out, fmt.Errorf("source query: %w", err)
	}
	// A model reads only the scan predicates, so anything that selects rows above
	// the scan would be silently dropped and the model would aggregate more rows
	// than the author asked for.
	if !res.SourceWhereComplete {
		return out, fmt.Errorf("source query: this query selects rows after the scan " +
			"(an aggregation, a window, a dedup or a subquery), and a model reads only " +
			"the filter its source applies to each log")
	}
	preds, err := stripScopeGuards(res.SourceWhere)
	if err != nil {
		return out, err
	}
	if err := checkNoScopeLeak(preds); err != nil {
		return out, err
	}
	if err := checkProjectionAliases(res.SourceProjections); err != nil {
		return out, err
	}
	out.preds = preds
	out.projections = res.SourceProjections
	return out, nil
}

// checkProjectionAliases refuses an alias that is not a plain name. A projection
// is written into the model's CTE unquoted, the way an extraction output is, so a
// name carrying punctuation would become a second select expression in a stored
// statement. Every command sanitizes its own output today; this is the boundary
// where that stops being something to take on trust.
func checkProjectionAliases(projections []parser.SourceProjection) error {
	for _, pr := range projections {
		if !parser.IsPlainFieldName(pr.Alias) {
			return fmt.Errorf("source query: %q is not a usable column name: use letters, digits, "+
				"dot, dash or underscore", pr.Alias)
		}
	}
	return nil
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
		if vizCommands[name] {
			return fmt.Errorf("%s() cannot be a model source: it renders a picture from a bounded "+
				"sample, and a model has nothing to draw", cmd.Name)
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

// vizCommands draw a result from a bounded sample of the rows. Their limit is row
// selection the model would not apply, so a source carrying one would aggregate
// every matching log while the author saw a few hundred.
var vizCommands = map[string]bool{
	"graph": true, "graphworld": true, "pgraph": true, "worldmap": true,
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
		// A transform that writes back to the field it read leaves a column holding
		// a log field's name and a different value. The model stores what the log
		// stored, so keying on it would index the value the author did not mean.
		if parser.RewritesFieldInPlace(cmd.Name) {
			if f := b.Str("field", ""); f != "" {
				out[f] = true
			}
		}
	}
	return out
}

// validateSourceProducedKeys checks the columns a definition shapes its state
// around. A column the source query computes is renderable for the streaming
// types, whose scan is built as a CTE that can project it, and is not for a
// network model, whose state is one flat aggregate over the log table. Only a
// definition carrying a source query is checked: without one there is no query to
// have computed anything, and a model stored before source queries existed may
// legitimately key on any field name its logs carry.
func validateSourceProducedKeys(mt ModelType, def ModelDefinition) error {
	if strings.TrimSpace(def.SourceBQL) == "" {
		return nil
	}
	produced := map[string]bool{}
	for _, f := range computedFields(def.SourceBQL, def.Extractions) {
		produced[f] = true
	}
	shadowing := computedFields(def.SourceBQL, def.Extractions)
	for _, ext := range def.Extractions {
		shadowing = append(shadowing, ext.OutputField)
	}
	if err := checkNoOwnedColumnShadowed(shadowing); err != nil {
		return err
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
		if n.value == "" {
			continue
		}
		if produced[n.value] && mt.IsNetwork() {
			return fmt.Errorf("%s %q is computed by the source query, and a network model's state "+
				"is one aggregate over the log table with nowhere to compute it: key on a field "+
				"the log stores", n.what, n.value)
		}
		// BQL names every column a command generates with a leading underscore, so
		// such a name is one of those. Renderable, but the author did not choose it
		// and it is not stable, so ask for a name instead.
		if strings.HasPrefix(n.value, "_") {
			return fmt.Errorf("%s %q names a column the query generates rather than one you named: "+
				"give the value a name with as=, and key on that", n.what, n.value)
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

// checkNoProjectedRefs refuses predicates that lean on a column the query's own
// SELECT projects, for a builder with no layer to compute one. The translator
// resolves a registry-known field to its bare alias, valid in the statement that
// defines it and undefined anywhere else. A plain filter inlines the expression
// and is unaffected; a binding routes through expression compilation, which does
// not.
func checkNoProjectedRefs(preds []string, projections []parser.SourceProjection) error {
	if len(projections) == 0 {
		return nil
	}
	bare := stripSQLStrings(strings.Join(preds, " AND "))
	for _, pr := range projections {
		if !regexp.MustCompile(`\b` + regexp.QuoteMeta(pr.Alias) + `\b`).MatchString(bare) {
			continue
		}
		return fmt.Errorf("this source filters on %q, a column the query computes, and a network "+
			"model's state is one aggregate over the log table with nowhere to compute it; "+
			"filter on the stored value instead", pr.Alias)
	}
	return nil
}

// stripSQLStrings blanks single-quoted literals so a column name is not found
// inside one: dictGetOrDefault(..., 'tool', ...) names the attribute, not a column.
func stripSQLStrings(sql string) string {
	var b strings.Builder
	inStr := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if c == '\'' {
			// A doubled quote escapes itself inside a literal.
			if inStr && i+1 < len(sql) && sql[i+1] == '\'' {
				i++
				continue
			}
			inStr = !inStr
			continue
		}
		if !inStr {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// modelProjections drops the source projections the model renders for itself. A
// regex extraction already becomes a CTE column of the same name, so carrying the
// translator's version too would define the column twice.
func modelProjections(all []parser.SourceProjection, extractions []ExtractionStep) []parser.SourceProjection {
	if len(all) == 0 {
		return nil
	}
	own := make(map[string]bool, len(extractions))
	for _, ext := range extractions {
		own[ext.OutputField] = true
	}
	out := make([]parser.SourceProjection, 0, len(all))
	for _, p := range all {
		if !own[p.Alias] {
			out = append(out, p)
		}
	}
	return out
}

// modelOwnedColumns are the names a model's own statement defines. A source query
// that computes a column of the same name would either collide with it or shadow
// it: a projection aliased `timestamp` replaced the event time with a dictionary
// string, and first_seen, last_seen and the day set were built from that.
var modelOwnedColumns = map[string]bool{
	"fractal_id": true, "timestamp": true,
	"partition_val": true, "value_val": true,
	"entity_key": true, "entity_val": true, "digest": true,
	"event_count": true, "days": true, "bucket": true,
	"first_seen": true, "last_seen": true,
	"src": true, "dst": true, "port": true, "day": true,
	"conn_count": true, "ts_state": true, "size_state": true, "dur_sum": true,
	"first_ts": true, "last_ts": true,
}

// checkNoOwnedColumnShadowed refuses a source that computes a column, or an
// extraction that produces one, named like a column the model's own statement
// defines. Both land in the same CTE namespace.
func checkNoOwnedColumnShadowed(computed []string) error {
	for _, name := range computed {
		if modelOwnedColumns[name] {
			return fmt.Errorf("the source query computes a column called %q, which is the name a "+
				"model's own state gives one of its columns: name it something else with as=", name)
		}
	}
	return nil
}
