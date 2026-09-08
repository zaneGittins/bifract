package parser

import (
	"strings"
	"testing"
	"time"
)

const tlshTestDigestA = "8aa32957b3e520f9e1b28a3884954a49d775f8361b219fef03b442961f237e48d3ab31"
const tlshTestDigestB = "c1716de2793a163c2cb95403ff9c33dae415c8c44f192262786270eb9136a0c8b1d549"

func tlshOpts(matches []TLSHMatch) QueryOptions {
	return QueryOptions{
		StartTime:     time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:       100,
		FractalID:     "f1",
		HasTLSHFilter: true,
		TLSHMatches:   matches,
	}
}

func translateTLSH(t *testing.T, query string, opts QueryOptions) (string, error) {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	return TranslateToSQL(pipeline, opts)
}

func TestTLSHParams(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantField string
		wantThr   int
		wantDict  string
		wantHash  int
		wantErr   string
	}{
		{name: "hash with default threshold", query: `* | tlsh(field=tlsh, hash="` + tlshTestDigestA + `")`,
			wantField: "tlsh", wantThr: DefaultTLSHThreshold, wantHash: 1},
		{name: "explicit threshold", query: `* | tlsh(field=tlsh, hash="` + tlshTestDigestA + `", threshold=50)`,
			wantField: "tlsh", wantThr: 50, wantHash: 1},
		{name: "dict", query: `* | tlsh(field=tlsh, dict="known_bad")`,
			wantField: "tlsh", wantThr: DefaultTLSHThreshold, wantDict: "known_bad"},
		{name: "bare field", query: `* | tlsh(tlsh, hash="` + tlshTestDigestA + `")`,
			wantField: "tlsh", wantThr: DefaultTLSHThreshold, wantHash: 1},
		{name: "two hashes", query: `* | tlsh(field=tlsh, hash="` + tlshTestDigestA + `,` + tlshTestDigestB + `")`,
			wantField: "tlsh", wantThr: DefaultTLSHThreshold, wantHash: 2},

		{name: "missing field", query: `* | tlsh(hash="` + tlshTestDigestA + `")`, wantErr: "field="},
		{name: "no needle", query: `* | tlsh(field=tlsh)`, wantErr: "hash="},
		{name: "both needle sources", query: `* | tlsh(field=tlsh, hash="` + tlshTestDigestA + `", dict="d")`, wantErr: "not both"},
		{name: "bad threshold", query: `* | tlsh(field=tlsh, dict="d", threshold=abc)`, wantErr: "must be a number"},
		{name: "threshold too high", query: `* | tlsh(field=tlsh, dict="d", threshold=9999)`, wantErr: "maximum"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pipeline, err := ParseQuery(c.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			p, found, err := ExtractTLSHParams(pipeline)
			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got none", c.wantErr)
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("error %q does not mention %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !found {
				t.Fatal("tlsh() not found in pipeline")
			}
			if p.Field != c.wantField {
				t.Errorf("field = %q, want %q", p.Field, c.wantField)
			}
			if p.Threshold != c.wantThr {
				t.Errorf("threshold = %d, want %d", p.Threshold, c.wantThr)
			}
			if p.Dict != c.wantDict {
				t.Errorf("dict = %q, want %q", p.Dict, c.wantDict)
			}
			if len(p.Hashes) != c.wantHash {
				t.Errorf("hashes = %d, want %d", len(p.Hashes), c.wantHash)
			}
		})
	}
}

// The resolved digests become an equality filter over the log field, so they must
// be emitted exactly as stored. Canonicalising them here would match nothing.
func TestTLSHRendersInFilterAndProjections(t *testing.T) {
	matches := []TLSHMatch{
		{Digest: tlshTestDigestA, Distance: 12, Needle: "dropper.a"},
		{Digest: tlshTestDigestB, Distance: 27, Needle: "dropper.b"},
	}
	sql, err := translateTLSH(t, `event_id=1 | tlsh(field=tlsh, dict="known_bad")`, tlshOpts(matches))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}

	if !strings.Contains(sql, "IN ('"+tlshTestDigestA+"', '"+tlshTestDigestB+"')") {
		t.Errorf("expected verbatim digest IN list, got:\n%s", sql)
	}
	if !strings.Contains(sql, "transform(") {
		t.Errorf("expected transform() distance projection, got:\n%s", sql)
	}
	for _, want := range []string{TLSHDistanceField, TLSHMatchField, "12", "27", "dropper.a"} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected %q in SQL, got:\n%s", want, sql)
		}
	}
	// The preceding pipeline filter must survive alongside the digest filter.
	if !strings.Contains(sql, "event_id") {
		t.Errorf("preceding filter dropped:\n%s", sql)
	}
}

// No match must produce an empty result, never an unfiltered scan.
func TestTLSHNoMatchesYieldsEmptyResult(t *testing.T) {
	sql, err := translateTLSH(t, `* | tlsh(field=tlsh, dict="known_bad")`, tlshOpts(nil))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(sql, "1 = 0") {
		t.Errorf("expected an always-false predicate, got:\n%s", sql)
	}
	// The projected columns must still resolve so a downstream sort renders.
	if !strings.Contains(sql, TLSHDistanceField) {
		t.Errorf("distance column missing from empty-result form:\n%s", sql)
	}
}

// tlsh_distance is projected at the source stage precisely so ranking by closeness
// composes with the rest of the pipeline.
func TestTLSHComposesWithSortAndAggregation(t *testing.T) {
	matches := []TLSHMatch{{Digest: tlshTestDigestA, Distance: 3, Needle: "n"}}

	sql, err := translateTLSH(t, `* | tlsh(field=tlsh, dict="d") | sort(tlsh_distance)`, tlshOpts(matches))
	if err != nil {
		t.Fatalf("sort after tlsh: %v", err)
	}
	if !strings.Contains(sql, "ORDER BY") || !strings.Contains(sql, TLSHDistanceField) {
		t.Errorf("sort by tlsh_distance did not render:\n%s", sql)
	}

	if _, err := translateTLSH(t, `* | tlsh(field=tlsh, dict="d") | groupby(computer_name)`, tlshOpts(matches)); err != nil {
		t.Fatalf("groupby after tlsh: %v", err)
	}
}

// Hoisting a similarity filter above an aggregation, the way cidr() and in() do,
// would quietly change what it filters. Reject instead.
func TestTLSHAfterAggregationRejected(t *testing.T) {
	matches := []TLSHMatch{{Digest: tlshTestDigestA, Distance: 3, Needle: "n"}}
	_, err := translateTLSH(t, `* | groupby(computer_name) | tlsh(field=tlsh, dict="d")`, tlshOpts(matches))
	if err == nil {
		t.Fatal("expected tlsh() after groupby to be rejected")
	}
	if !strings.Contains(err.Error(), "groupby") {
		t.Errorf("error should explain the ordering constraint, got: %v", err)
	}
}

// The probe is a plain source scan, so it cannot see a column invented downstream.
func TestTLSHOnComputedFieldRejected(t *testing.T) {
	matches := []TLSHMatch{{Digest: tlshTestDigestA, Distance: 3, Needle: "n"}}
	_, err := translateTLSH(t, `* | eval("h = tlsh") | tlsh(field=h, dict="d")`, tlshOpts(matches))
	if err == nil {
		t.Fatal("expected tlsh() on a computed field to be rejected")
	}
	if !strings.Contains(err.Error(), "computed") {
		t.Errorf("error should say the field is computed, got: %v", err)
	}
}

// Without server-side resolution the command has no matches to render, and must
// not silently degrade into an unfiltered query.
func TestTLSHWithoutResolutionFails(t *testing.T) {
	opts := tlshOpts(nil)
	opts.HasTLSHFilter = false
	_, err := translateTLSH(t, `* | tlsh(field=tlsh, dict="d")`, opts)
	if err == nil {
		t.Fatal("expected an error when tlsh() was not pre-resolved")
	}
}

// Resolution produces a single match set, so a second tlsh() would filter its own
// field using the first field's digests and overwrite the first's projections.
// Both are silent, so this must be rejected rather than ignored.
func TestSecondTLSHRejected(t *testing.T) {
	pipeline, err := ParseQuery(`* | tlsh(field=tlsh, dict="d1") | tlsh(field=other_hash, dict="d2")`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, _, err := ExtractTLSHParams(pipeline); err == nil {
		t.Fatal("expected a second tlsh() to be rejected at extraction")
	}

	// The translator guards independently, for callers that bypass extraction.
	matches := []TLSHMatch{{Digest: tlshTestDigestA, Distance: 5, Needle: "n"}}
	sql, terr := TranslateToSQL(pipeline, tlshOpts(matches))
	if terr == nil {
		t.Fatalf("expected the translator to reject a second tlsh(), got SQL:\n%s", sql)
	}
}

// An empty IN list is a ClickHouse syntax error, so a match set that cannot be
// rendered must fall back to the empty-result form rather than emitting "IN ()".
func TestTLSHNeverEmitsEmptyInList(t *testing.T) {
	// A needle label far larger than the literal budget forces the render loop to
	// exhaust its allowance on the very first match.
	huge := strings.Repeat("x", tlshMaxLiteralBytes+1)
	matches := []TLSHMatch{{Digest: tlshTestDigestA, Distance: 1, Needle: huge}}

	sql, err := translateTLSH(t, `* | tlsh(field=tlsh, dict="d")`, tlshOpts(matches))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if strings.Contains(sql, "IN ()") {
		t.Fatalf("emitted an empty IN list:\n%s", sql)
	}
}

// A dropped argument is a silently different query: a typo'd threshold runs at the
// default, and an unquoted multi-value hash list hunts for only the first digest.
func TestTLSHRejectsUnknownArguments(t *testing.T) {
	cases := []struct{ name, query, want string }{
		{"typo'd threshold", `* | tlsh(field=tlsh, dict="d", treshold=50)`, "unknown argument"},
		{"typo'd dict", `* | tlsh(field=tlsh, dictionary="d")`, "unknown argument"},
		{"unquoted hash list", `* | tlsh(field=tlsh, hash=` + tlshTestDigestA + `,` + tlshTestDigestB + `)`, "unexpected argument"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pipeline, err := ParseQuery(c.query)
			if err != nil {
				t.Skipf("query does not parse: %v", err)
			}
			_, _, err = ExtractTLSHParams(pipeline)
			if err == nil {
				t.Fatalf("expected rejection, got none")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}
}

// tlsh() is the one command whose field name reaches a hand-built SQL identifier
// (the fallback probe for an unindexed fractal), and a quoted argument reaches the
// parser verbatim. An unvalidated name closed the identifier and ran as SQL, which
// crossed the fractal isolation boundary.
func TestTLSHRejectsHostileFieldNames(t *testing.T) {
	hostile := []string{
		"a`::String, 1 AS z FROM system.tables --",
		"a`",
		"a'b",
		"a b",
		"a;DROP",
		"a)",
		"*",
		"",
	}
	for _, f := range hostile {
		p := TLSHParams{Field: f, Dict: "d", Threshold: DefaultTLSHThreshold}
		if err := p.validate(); err == nil {
			t.Errorf("field %q accepted, want rejected", f)
		}
	}

	// Real field names must keep working, including nested and hyphenated forms.
	for _, f := range []string{"tlsh", "target_tlsh", "host.name", "a-b", "field_1", "authenticode__extra_info_catalog"} {
		p := TLSHParams{Field: f, Dict: "d", Threshold: DefaultTLSHThreshold}
		if err := p.validate(); err != nil {
			t.Errorf("field %q rejected: %v", f, err)
		}
	}
}

// End to end: a hostile name must not survive extraction either.
func TestTLSHHostileFieldRejectedAtExtraction(t *testing.T) {
	pipeline, err := ParseQuery(`* | tlsh(field="a` + "`" + `::String, 1 AS z FROM system.tables --", dict="d")`)
	if err != nil {
		t.Skipf("query does not parse: %v", err)
	}
	if _, _, err := ExtractTLSHParams(pipeline); err == nil {
		t.Fatal("hostile field name accepted by ExtractTLSHParams")
	}
}

// table() clears the source SELECT and rebuilds it from the registry, so a column
// registered only as a placeholder (Expr == name) resolves to a raw fields.`name`
// JSON path: an always-empty column rather than the computed one. The projected
// tlsh columns must therefore publish their real expression via SetResolveExpr.
func TestTLSHColumnsSurviveTable(t *testing.T) {
	matches := []TLSHMatch{{Digest: tlshTestDigestA, Distance: 42, Needle: "dropper.a"}}
	q := `event_id=1 | tlsh=* | tlsh(field=tlsh, dict="d", threshold=85) | table(timestamp, image, tlsh_distance, tlsh_match)`

	sql, err := translateTLSH(t, q, tlshOpts(matches))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if strings.Contains(sql, "fields.`tlsh_distance`") || strings.Contains(sql, "fields.`tlsh_match`") {
		t.Fatalf("tlsh columns resolved as raw log fields, which are always empty:\n%s", sql)
	}
	if !strings.Contains(sql, "transform(") {
		t.Fatalf("table() dropped the distance projection:\n%s", sql)
	}
	for _, want := range []string{"42", "dropper.a", "AS " + TLSHDistanceField, "AS " + TLSHMatchField} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected %q in the projection:\n%s", want, sql)
		}
	}
}

// The empty-result and negated forms project constants, and those must survive
// table() the same way rather than falling back to a phantom JSON field.
func TestTLSHConstantColumnsSurviveTable(t *testing.T) {
	for _, c := range []struct {
		name  string
		query string
		opts  QueryOptions
	}{
		{"no matches", `* | tlsh(field=tlsh, dict="d") | table(timestamp, tlsh_distance)`, tlshOpts(nil)},
		{"negated", `* | !tlsh(field=tlsh, dict="d") | table(timestamp, tlsh_distance)`,
			tlshOpts([]TLSHMatch{{Digest: tlshTestDigestA, Distance: 1, Needle: "n"}})},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql, err := translateTLSH(t, c.query, c.opts)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			if strings.Contains(sql, "fields.`tlsh_distance`") {
				t.Errorf("distance column resolved as a raw log field:\n%s", sql)
			}
			if !strings.Contains(sql, "toInt32(-1)") {
				t.Errorf("expected the constant distance projection:\n%s", sql)
			}
		})
	}
}

// Pulling extra dictionary columns onto a hit composes out of match(), but only
// keyed on tlsh_match. The log row's own digest is merely SIMILAR to the needle,
// never equal (except at distance 0), so match() on the log field would look up a
// key the dictionary does not contain and enrich nothing. tlsh_match carries the
// needle that actually matched, which is a real dictionary key.
//
// This composition depends on tlsh_match publishing its expression to the registry
// (see publishTLSHExprs): as a bare placeholder it resolved to an empty JSON field
// and the lookup silently returned defaults.
func TestTLSHMatchEnrichesViaMatch(t *testing.T) {
	opts := tlshOpts([]TLSHMatch{{Digest: tlshTestDigestA, Distance: 42, Needle: "T1NEEDLE"}})
	opts.Dictionaries = map[string]map[string]string{"tlsh": {"key": "dict_tlsh", "name": "dict_tlsh"}}
	opts.DictionaryDatabase = "bifract"

	sql, err := translateTLSH(t,
		`* | tlsh(field=tlsh, dict="tlsh") | match(dict="tlsh", field=tlsh_match, column=key, include=[name]) | table(timestamp, tlsh_distance, name)`,
		opts)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	// The lookup key must be the matched needle, not the log's own digest.
	if !strings.Contains(sql, "dictGetOrDefault('bifract.dict_tlsh', 'name'") {
		t.Fatalf("expected a dictionary lookup for the extra column:\n%s", sql)
	}
	if !strings.Contains(sql, "'T1NEEDLE'") {
		t.Errorf("lookup should key on the needle carried by tlsh_match:\n%s", sql)
	}
	if strings.Contains(sql, "fields.`tlsh_match`") {
		t.Errorf("tlsh_match resolved as a raw log field, so the lookup would always miss:\n%s", sql)
	}
}
