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
	preds, err := compileSourcePredicates(
		`event_id="1" | match(dict="rmm", field=image, column=pattern, include=[pattern], require=true)`, rmmOpts())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	joined := strings.Join(preds, " AND ")
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
	preds, err := compileSourcePredicates(`event_id="1"`, parser.QueryOptions{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	for _, p := range preds {
		if strings.Contains(p, sourceScopeFractal) || strings.Contains(p, "timestamp >=") || strings.Contains(p, "timestamp <=") {
			t.Errorf("scope guard leaked into the model scan: %s", p)
		}
	}
	if len(preds) != 1 {
		t.Errorf("want just the author's predicate, got %v", preds)
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

// A model builds its own SELECT from the scan predicates, so a column only the
// source query computes does not exist when its state is built.
func TestSourceProducedColumnCannotBeAKey(t *testing.T) {
	def := ModelDefinition{
		SourceBQL: `event_id="1" | sprintf("%s-%s", user, image, as=who)`,
		KeyFields: []string{"who"},
	}
	if err := validateSourceProducedKeys(def); err == nil {
		t.Fatal("keying on a source-computed column must be refused")
	}
	// A regex extraction the model renders itself is exempt.
	ok := ModelDefinition{
		SourceBQL:   `event_id="1" | regex(field=norm_log, regex="(?<tool>[a-z]+)")`,
		Extractions: []ExtractionStep{{FromField: "norm_log", Pattern: "(?<tool>[a-z]+)", OutputField: "tool"}},
		KeyFields:   []string{"tool"},
	}
	if err := validateSourceProducedKeys(ok); err != nil {
		t.Fatalf("an extraction the model renders must be allowed: %v", err)
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
