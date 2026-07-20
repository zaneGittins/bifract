package schemafields

import (
	"strings"
	"testing"
)

// TestReferencedFieldsIgnoresStringLiterals is the reason this parses BQL rather
// than pattern-matching it. A value that happens to contain a field name, or an
// arbitrary phrase in a search string, must not be mistaken for a field
// reference: doing so would inflate the usage score of fields nobody queries and
// surface junk at the top of the suggestions list.
func TestReferencedFieldsIgnoresStringLiterals(t *testing.T) {
	got := referencedFields(`src_ip="user_agent is not a field here"`)
	if len(got) != 1 || got[0] != "src_ip" {
		t.Fatalf("expected only src_ip, got %v", got)
	}
}

func TestReferencedFieldsWalksConditions(t *testing.T) {
	cases := []struct {
		name  string
		bql   string
		want  []string
		notIn []string
	}{
		{name: "simple equality", bql: `src_ip="1.2.3.4"`, want: []string{"src_ip"}},
		{name: "conjunction", bql: `src_ip="1.2.3.4" AND user="alice"`,
			want: []string{"src_ip", "user"}},
		{name: "grouped disjunction",
			bql:  `(src_ip="1.2.3.4" OR dst_ip="5.6.7.8") AND event_id="4624"`,
			want: []string{"src_ip", "dst_ip", "event_id"}},
		{name: "multi-value equality", bql: `event_id="4624","4625"`,
			want: []string{"event_id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := referencedFields(tc.bql)
			idx := map[string]bool{}
			for _, g := range got {
				idx[g] = true
			}
			for _, w := range tc.want {
				if !idx[w] {
					t.Errorf("missing %q in %v", w, got)
				}
			}
			for _, n := range tc.notIn {
				if idx[n] {
					t.Errorf("unexpected %q in %v", n, got)
				}
			}
		})
	}
}

// TestReferencedFieldsCoversCommands guards the gap found against real query
// history: fields named only in a table() or a stats-by clause are queried just
// as much as filtered ones, and counting only filters under-reports them.
func TestReferencedFieldsCoversCommands(t *testing.T) {
	got := referencedFields(`level=info | table(timestamp, level, dest_ip)`)
	idx := map[string]bool{}
	for _, g := range got {
		idx[g] = true
	}
	for _, want := range []string{"level", "dest_ip"} {
		if !idx[want] {
			t.Errorf("missing %q in %v", want, got)
		}
	}
}

// TestReferencedFieldsRejectsNonFieldArgs keeps command values and parameters
// out of the usage counts. pgr(start="WROOT") dominates this project's real
// history, so leaking its parameters would badly skew the ranking.
func TestReferencedFieldsRejectsNonFieldArgs(t *testing.T) {
	for _, q := range []string{
		`pgr(start="WROOT") | pgraph()`,
		`pgr(start="RB1",diffuse=true) | pgraph()`,
	} {
		for _, f := range referencedFields(q) {
			if f == "start" || f == "WROOT" || f == "diffuse" || f == "true" {
				t.Errorf("%q leaked non-field %q", q, f)
			}
		}
	}
}

func TestLooksLikeFieldRef(t *testing.T) {
	for _, ok := range []string{"src_ip", "user", "dest_ip", "_internal"} {
		if !looksLikeFieldRef(ok) {
			t.Errorf("%q should be a field ref", ok)
		}
	}
	for _, bad := range []string{
		"", "by", "asc", "DESC", "count()", `start="W2"`, "diffuse=true",
		"5m", "*", "a,b", "has space", "true",
	} {
		if looksLikeFieldRef(bad) {
			t.Errorf("%q should not be a field ref", bad)
		}
	}
}

// TestSuggestableRejectsDeadEnds stops the list proposing fields the create path
// would refuse. Real log data contains keys like "content-_security-_policy"
// that validFieldName rejects; offering an Add button that always errors is
// worse than not listing them.
func TestSuggestableRejectsDeadEnds(t *testing.T) {
	for _, ok := range []string{"tenant_id", "level", "_custom"} {
		if !suggestable(ok) {
			t.Errorf("%q should be suggestable", ok)
		}
	}
	for _, bad := range []string{
		"timestamp", "log_id", "norm_log", "raw_log", "fractal_id",
		"content-_security-_policy", "accept-_encoding", "9lives", "",
	} {
		if suggestable(bad) {
			t.Errorf("%q should not be suggestable", bad)
		}
	}
}

func TestReferencedFieldsHandlesJunk(t *testing.T) {
	// Unparseable or empty history rows must yield nothing rather than panic:
	// query_history holds whatever users typed, including broken queries.
	for _, q := range []string{"", "   ", `!!! not bql (((`} {
		if got := referencedFields(q); len(got) != 0 {
			t.Errorf("expected no fields for %q, got %v", q, got)
		}
	}
}

// TestRecommendIndexMatchesDefaultsReasoning pins the rule that a field nothing
// queries gets no index. A skip index costs write and merge time on every part
// forever, so it should follow evidence of use, not just presence in the data.
func TestRecommendIndexMatchesDefaultsReasoning(t *testing.T) {
	cases := []struct {
		name        string
		cardinality uint64
		refs        int
		want        IndexType
	}{
		{"unqueried high cardinality", 50000, 0, IndexTypeNone},
		{"unqueried low cardinality", 6, 0, IndexTypeNone},
		{"queried low cardinality", 6, 3, IndexTypeSet},
		{"queried at set boundary", 256, 3, IndexTypeSet},
		{"queried just past boundary", 257, 3, IndexTypeBloomFilter},
		{"queried high cardinality", 412000, 12, IndexTypeBloomFilter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := recommendIndex(FieldInsight{Cardinality: tc.cardinality}, tc.refs)
			if got != string(tc.want) {
				t.Errorf("cardinality=%d refs=%d: got %q, want %q",
					tc.cardinality, tc.refs, got, tc.want)
			}
		})
	}
}

// TestBuildSuggestionsExcludesAndRanks covers the two things that make the list
// trustworthy: it never proposes something already configured or explicitly
// dismissed, and query usage outranks mere prevalence.
func TestBuildSuggestionsExcludesAndRanks(t *testing.T) {
	stats := map[string]FieldInsight{
		"src_ip":    {Name: "src_ip", Coverage: 0.99, Cardinality: 1000},
		"noise":     {Name: "noise", Coverage: 0.99, Cardinality: 90000},
		"tenant_id": {Name: "tenant_id", Coverage: 0.40, Cardinality: 47000},
		"prevalent": {Name: "prevalent", Coverage: 0.95, Cardinality: 12},
	}
	configured := map[string]bool{"src_ip": true}
	ignored := map[string]bool{"noise": true}
	usage := map[string]int{"tenant_id": 12}

	got := buildSuggestions(stats, usage, configured, ignored, nil)

	for _, s := range got {
		if s.Name == "src_ip" {
			t.Error("suggested an already-configured field")
		}
		if s.Name == "noise" {
			t.Error("suggested an ignored field")
		}
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 suggestions, got %d: %+v", len(got), got)
	}
	// tenant_id is in far fewer logs than prevalent, but 12 saved queries
	// reference it, so it must rank first.
	if got[0].Name != "tenant_id" {
		t.Errorf("expected query-referenced field first, got %q", got[0].Name)
	}
	if !strings.Contains(strings.Join(got[0].Reasons, " "), "12 saved queries") {
		t.Errorf("reasons should explain the rank, got %v", got[0].Reasons)
	}
}

func TestFormatHelpers(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{formatCount(999), "999"}, {formatCount(1200), "1k"},
		{formatCount(412000), "412k"}, {formatCount(2_100_000), "2.1M"},
		{formatPct(0.004), "<1%"}, {formatPct(0.94), "94%"}, {formatPct(1), "100%"},
	} {
		if tc.in != tc.want {
			t.Errorf("got %q, want %q", tc.in, tc.want)
		}
	}
}
