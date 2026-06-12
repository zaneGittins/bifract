package models

import (
	"sort"
	"strings"
	"testing"
)

// Round-trip invariant: ParseSourceQuery(GenerateSourceQuery(def)) reproduces the
// filter + extraction structure of def (def-stable, not string-stable). Lowercase
// and MinLength are UI adornments not encoded in BQL, so they are excluded here.
func TestSourceQueryRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		def  ModelDefinition
	}{
		{"empty", ModelDefinition{}},
		{"eq", ModelDefinition{Filter: []FilterCondition{{Field: "level", Op: "=", Value: "error"}}}},
		{"neq", ModelDefinition{Filter: []FilterCondition{{Field: "level", Op: "!=", Value: "info"}}}},
		{"multi-and", ModelDefinition{Filter: []FilterCondition{
			{Field: "level", Op: "=", Value: "error"},
			{Field: "env", Op: "=", Value: "prod"},
		}}},
		{"regex-match", ModelDefinition{Filter: []FilterCondition{{Field: "raw_log", Op: "~", Value: `failed login`}}}},
		{"regex-neg", ModelDefinition{Filter: []FilterCondition{{Field: "raw_log", Op: "!~", Value: `health`}}}},
		{"regex-backslash", ModelDefinition{Filter: []FilterCondition{{Field: "raw_log", Op: "~", Value: `\d+\.\d+`}}}},
		{"regex-slash", ModelDefinition{Filter: []FilterCondition{{Field: "raw_log", Op: "~", Value: `a/b/c`}}}},
		{"value-with-quote", ModelDefinition{Filter: []FilterCondition{{Field: "msg", Op: "=", Value: `say "hi"`}}}},
		{"cidr", ModelDefinition{Filter: []FilterCondition{{Field: "src_ip", Op: "cidr", Value: "10.0.0.0/8"}}}},
		{"notcidr", ModelDefinition{Filter: []FilterCondition{{Field: "src_ip", Op: "!cidr", Value: "192.168.0.0/16"}}}},
		{"extract-as", ModelDefinition{Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `([a-z]+)$`, OutputField: "tld"},
		}}},
		{"extract-named", ModelDefinition{Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `(?<tld>[a-z]+)$`, OutputField: "tld"},
		}}},
		{"extract-backslash", ModelDefinition{Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `(\d+\.\d+\.\d+\.\d+)`, OutputField: "ip"},
		}}},
		{"multi-extract", ModelDefinition{Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `host=(\S+)`, OutputField: "host"},
			{FromField: "raw_log", Pattern: `user=(\S+)`, OutputField: "usr"},
		}}},
		{"extract-lowercase", ModelDefinition{Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `([A-Za-z]+)$`, OutputField: "tld", Lowercase: true},
		}}},
		{"extract-minlength", ModelDefinition{Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `(\S+)`, OutputField: "tok", MinLength: 4},
		}}},
		{"extract-lower-and-minlength", ModelDefinition{Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `([A-Za-z]+)$`, OutputField: "tld", Lowercase: true, MinLength: 2},
		}}},
		{"multi-extract-minlength", ModelDefinition{Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `host=(\S+)`, OutputField: "host", MinLength: 3},
			{FromField: "raw_log", Pattern: `user=(\S+)`, OutputField: "usr", MinLength: 5},
		}}},
		{"filter-and-extract-and-cidr", ModelDefinition{
			Filter: []FilterCondition{
				{Field: "level", Op: "=", Value: "dns"},
				{Field: "src_ip", Op: "cidr", Value: "10.0.0.0/8"},
			},
			Extractions: []ExtractionStep{
				{FromField: "raw_log", Pattern: `query: (\S+)`, OutputField: "domain"},
			},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := GenerateSourceQuery(tc.def)
			got := ParseSourceQuery(q, ModelTypeRarity)
			if len(got.Errors) != 0 {
				t.Fatalf("generated query %q produced parse errors: %v", q, got.Errors)
			}
			assertFiltersEqual(t, q, tc.def.Filter, got.Filter)
			assertExtractionsEqual(t, q, tc.def.Extractions, got.Extractions)
		})
	}
}

// GenerateQuery (the alert query) must emit cidr as a command, not as a broken
// string equality, and the result must be parseable BQL.
func TestGenerateQueryCidrIsCommand(t *testing.T) {
	def := ModelDefinition{
		Filter:       []FilterCondition{{Field: "src_ip", Op: "cidr", Value: "10.0.0.0/8"}},
		PartitionKey: "src_ip",
		ValueKey:     "src_ip",
	}
	q := GenerateQuery("m", def, ModelTypeRarity)
	if !strings.Contains(q, `| cidr(src_ip, "10.0.0.0/8")`) {
		t.Fatalf("expected a cidr() command, got:\n%s", q)
	}
	if strings.Contains(q, `src_ip = "10.0.0.0/8"`) {
		t.Fatalf("cidr emitted as string equality:\n%s", q)
	}
}

func filterKey(f FilterCondition) string { return f.Field + "\x00" + f.Op + "\x00" + f.Value }

func assertFiltersEqual(t *testing.T, q string, want, got []FilterCondition) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("query %q: filter count = %d, want %d (%+v)", q, len(got), len(want), got)
	}
	wk := make([]string, len(want))
	gk := make([]string, len(got))
	for i := range want {
		wk[i] = filterKey(want[i])
	}
	for i := range got {
		gk[i] = filterKey(got[i])
	}
	sort.Strings(wk)
	sort.Strings(gk)
	for i := range wk {
		if wk[i] != gk[i] {
			t.Fatalf("query %q: filters differ\n want %v\n got  %v", q, want, got)
		}
	}
}

func assertExtractionsEqual(t *testing.T, q string, want, got []ExtractionStep) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("query %q: extraction count = %d, want %d (%+v)", q, len(got), len(want), got)
	}
	for i := range want {
		if want[i].FromField != got[i].FromField || want[i].Pattern != got[i].Pattern ||
			want[i].OutputField != got[i].OutputField || want[i].Lowercase != got[i].Lowercase ||
			want[i].MinLength != got[i].MinLength {
			t.Fatalf("query %q: extraction[%d] = %+v, want %+v", q, i, got[i], want[i])
		}
	}
}
