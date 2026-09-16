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
