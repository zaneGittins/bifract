package models

import (
	"strings"
	"testing"

	"bifract/pkg/parser"
)

func rmmOpts() parser.QueryOptions {
	return parser.QueryOptions{
		DictionaryDatabase: "logs",
		Dictionaries:       map[string]map[string]string{"rmm": {"pattern": "dict_rmm_pattern"}},
		PatternDicts:       map[string]bool{"dict_rmm_pattern": true},
	}
}

// The case this work exists for: a pattern list of RMM tool paths, consulted as a
// filter, which the structured Filter list has no shape for.
func TestSourceBQLCompilesAPatternListFilter(t *testing.T) {
	src, err := compileSourcePredicates(
		`event_id="1" | match(dict="rmm", field=image, column=pattern, include=[pattern], require=true)`, rmmOpts())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	joined := strings.Join(src.preds, " AND ")
	if !strings.Contains(joined, "dictGetOrDefault('logs.dict_rmm_pattern', '_match'") {
		t.Errorf("want the pattern-list membership test, got: %s", joined)
	}
	if !strings.Contains(joined, "fields.`event_id`::String = '1'") {
		t.Errorf("want the plain filter too, got: %s", joined)
	}
}

// The translator's own scope must never reach a model's scan: the model supplies
// its own fractal and window.
func TestSourceBQLStripsTheTranslatorScope(t *testing.T) {
	src, err := compileSourcePredicates(`event_id="1"`, parser.QueryOptions{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	for _, p := range src.preds {
		if strings.Contains(p, sourceScopeFractal) || strings.Contains(p, "timestamp >=") || strings.Contains(p, "timestamp <=") {
			t.Errorf("scope guard leaked into the model scan: %s", p)
		}
	}
	if len(src.preds) != 1 {
		t.Errorf("want just the author's predicate, got %v", src.preds)
	}
}

// A guard that changed shape must fail loudly: silently carrying it would pin
// every model's scan to a sentinel fractal.
func TestScopeGuardRemovalIsAsserted(t *testing.T) {
	if _, err := stripScopeGuards([]string{"fields.`a`::String = 'x'"}); err == nil {
		t.Error("a missing scope guard must be an error, not a shrug")
	}
}

// A model aggregates the rows its source yields, over one window at a time.
func TestSourceBQLRejectsWhatCannotBeASource(t *testing.T) {
	cases := map[string]string{
		`* | groupby(user) | count()`: "collapses rows",
		`* | top(user)`:               "collapses rows",
		`* | frequency(user)`:         "collapses rows",
		`* | headTail(user)`:          "collapses rows",
		`* | sort(timestamp)`:         "reorders or drops",
		`* | head(10)`:                "reorders or drops",
		`* | dedup(user)`:             "reorders or drops",
		`* | join(user) { event_id="1" | groupby(user) }`: "reads a second query",
		`* | model_lookup(model="m", key=[user])`:         "another model's output",
		`* | chain([user], within=5m) { a="x"; b="y" }`:   "straddle the window",
	}
	for query, want := range cases {
		_, err := compileSourcePredicates(query, parser.QueryOptions{})
		if err == nil {
			t.Errorf("%s: expected a rejection", query)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want an error mentioning %q, got: %v", query, want, err)
		}
	}
}

// Everything that keeps one row per log is admissible, which is the point: the
// structured form could carry four commands.
func TestSourceBQLAcceptsRowPreservingSources(t *testing.T) {
	for _, q := range []string{
		`event_id="1"`,
		`* | len(commandline) > 500`,
		`* | lower(image) =~ "a.exe","b.exe"`,
		`* | cidr(src_ip, "10.0.0.0/8")`,
		`* | image = /powershell.*-enc/`,
		`let &lolbin := lower(image) =~ "mshta.exe"; * | &lolbin`,
		`* | in(user, "a","b")`,
		`* | isPrivateIP(src_ip) = false`,
	} {
		if _, err := compileSourcePredicates(q, parser.QueryOptions{}); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// A model keeps only the scan predicates, so a source that selects rows anywhere
// else must be refused rather than silently widened.
func TestSourceBQLRejectsSelectionAboveTheScan(t *testing.T) {
	// A result-set binding embeds a subquery carrying the compile-time fractal and
	// window, which the model has no way to re-scope. It passes the command walk
	// (the aggregation is inside the binding, not in the pipeline), so only the
	// completeness check catches it.
	cases := map[string]string{
		`let &hosts := * | groupby(host); * | in(host, &hosts)`: "selects rows after the scan",
	}
	for query, want := range cases {
		_, err := compileSourcePredicates(query, parser.QueryOptions{})
		if err == nil {
			t.Errorf("%s: expected a rejection", query)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want an error mentioning %q, got: %v", query, want, err)
		}
	}
}

// The compile-time scope must not survive inside a predicate either. This is the
// backstop for a construct that embeds a subquery without the plan noticing.
func TestScopeLeakInsideAPredicateIsRefused(t *testing.T) {
	leaks := []string{
		"host IN (SELECT host FROM logs WHERE fractal_id = '" + sourceScopeFractal + "')",
		"host IN (SELECT host FROM logs WHERE timestamp >= '" + sourceScopeStart.Format(chTimeLayout) + "')",
	}
	for _, p := range leaks {
		if err := checkNoScopeLeak([]string{p}); err == nil {
			t.Errorf("%s: expected a rejection", p)
		}
	}
	if err := checkNoScopeLeak([]string{"fields.`event_id`::String = '1'"}); err != nil {
		t.Errorf("a clean predicate must pass: %v", err)
	}
}

// A network model's state is one flat aggregate over the log table, with no layer
// that could compute a column, so a key it computes has nowhere to come from.
func TestNetworkModelCannotKeyOnAComputedColumn(t *testing.T) {
	def := ModelDefinition{
		SourceBQL: `* | match(dict="d", field=image, column=k, include=[tool], require=true)`,
		Network:   &NetworkFieldMap{SrcField: "tool", DstField: "dst_ip", PortField: "dst_port"},
	}
	if err := validateSourceProducedKeys(ModelTypeBeacon, def); err == nil {
		t.Fatal("a network model keying on a computed column must be refused")
	}
	// The streaming types build their scan as a CTE, which can compute it.
	if err := validateSourceProducedKeys(ModelTypeRarity, ModelDefinition{
		SourceBQL:    def.SourceBQL,
		PartitionKey: "computer_name",
		ValueKey:     "tool",
	}); err != nil {
		t.Fatalf("a rarity model must be able to key on a computed column: %v", err)
	}
}

// Every column BQL generates is underscore-prefixed. Those are renderable, but
// the author did not choose the name and it is not stable, so ask for one.
func TestGeneratedColumnCannotBeAKey(t *testing.T) {
	def := ModelDefinition{
		SourceBQL:    `event_id="1" | concat(image, user, as=k)`,
		PartitionKey: "computer_name",
		ValueKey:     "_concat",
	}
	if err := validateSourceProducedKeys(ModelTypeRarity, def); err == nil {
		t.Fatal("keying on a generated column must be refused")
	}
	// A model with no source query is unaffected.
	legacy := ModelDefinition{Filter: []FilterCondition{{Field: "a", Op: "=", Value: "1"}}, KeyFields: []string{"_id"}}
	if err := validateSourceProducedKeys(ModelTypeFirstSeen, legacy); err != nil {
		t.Fatalf("a definition without a source query must be unaffected: %v", err)
	}
}

// The columns a source adds are what include=/as= name, not the dictionary key
// the lookup probes: flagging the key would block a real log field of that name
// from being a model key, and missing the enrichment column would let one be
// picked that the model's state can never hold.
func TestComputedFieldsNamesEnrichmentColumns(t *testing.T) {
	got := computedFields(`event_id="1" | match(dict="rmm", field=image, column=pattern, include=[tool], require=true)`, nil)
	want := []string{"tool"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("computedFields = %v, want %v", got, want)
	}
}

// A command's own row limit is row selection the model never applies: the author
// sees a few hundred rows and the model would aggregate every matching log.
func TestSourceBQLRejectsACommandRowLimit(t *testing.T) {
	cases := map[string]string{
		`* | pgraph(limit=200)`: "nothing to draw",
		`* | graph(child=process_guid, parent=parent_process_guid, limit=25)`: "nothing to draw",
		`* | worldmap(lat=latitude, lon=longitude, limit=50)`:                 "nothing to draw",
		`* | limit(10)`: "reorders or drops rows",
	}
	for query, want := range cases {
		_, err := compileSourcePredicates(query, parser.QueryOptions{})
		if err == nil {
			t.Errorf("%s: expected a rejection", query)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want an error mentioning %q, got: %v", query, want, err)
		}
	}
}

// A source that rewrites a field in place leaves a column carrying that field's
// name and the rewritten value, which is what the author filtered on. The model
// projects it, so keying on it now stores the rewritten value rather than
// disagreeing with the query.
func TestRewrittenFieldIsRenderableAsAKey(t *testing.T) {
	def := ModelDefinition{
		SourceBQL: `event_id="1" | lowercase(image)`,
		KeyFields: []string{"computer_name", "image"},
	}
	if err := validateSourceProducedKeys(ModelTypeFirstSeen, def); err != nil {
		t.Fatalf("a rewritten field must be renderable as a key: %v", err)
	}
}

// A streaming model projects the columns its source computes, so a filter reading
// one resolves. A network model has no such layer and must refuse rather than
// render a scan referencing a column nothing defines.
func TestPredicateOnAProjectedColumn(t *testing.T) {
	opts := parser.QueryOptions{
		DictionaryDatabase: "logs",
		Dictionaries:       map[string]map[string]string{"rmm": {"path": "dict_rmm"}},
		PatternDicts:       map[string]bool{"dict_rmm": true},
	}
	// Through a binding the filter compiles to a bare `tool` rather than inlining.
	const bql = `let &a := tool =~ "putty"; event_id="1" | match(dict="rmm", field=image, column=path, include=[tool], require=true) | NOT &a`
	src, err := compileSourcePredicates(bql, opts)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(src.projections) == 0 {
		t.Fatal("the lookup column should be reported as a projection")
	}

	def := ModelDefinition{
		SourceBQL:   bql,
		compiled:    src.preds,
		compiledSet: true,
		projections: src.projections,
		KeyFields:   []string{"computer_name"},
	}
	// Streaming: the CTE projects it, so the predicate resolves.
	sql, err := BuildBackfillInsert(def, ModelTypeFirstSeen, "tgt", "logs", "", "f1")
	if err != nil {
		t.Fatalf("streaming build: %v", err)
	}
	if !strings.Contains(sql, "AS tool") {
		t.Errorf("the scan must project the column its filter reads:\n%s", sql)
	}

	// Network: one flat aggregate, nowhere to compute it.
	def.Network = &NetworkFieldMap{SrcField: "src_ip", DstField: "dst_ip", PortField: "dst_port"}
	if _, err := BuildNetStateInsert(def, "state", "logs", "", "f1"); err == nil {
		t.Error("a network model must refuse a filter on a computed column")
	}
}

// A column name appearing inside a SQL string literal is not a column reference:
// dictGetOrDefault(..., 'tool', ...) names the dictionary attribute.
func TestStripSQLStrings(t *testing.T) {
	in := `dictGetOrDefault('logs.d', 'tool', toString(fields.` + "`image`" + `::String), '') = '1' AND tool > 2`
	got := stripSQLStrings(in)
	if strings.Count(got, "tool") != 1 {
		t.Fatalf("want only the bare reference left, got %q", got)
	}
	if strings.Contains(got, "logs.d") {
		t.Fatalf("quoted literal survived: %q", got)
	}
}
