package models

import (
	"strings"
	"testing"
)

func TestParseSourceQuerySubsetRejections(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantSubst string // substring expected in one of the errors
	}{
		{"or", `level = "a" OR level = "b"`, "OR is not supported"},
		{"group", `(level = "a" AND env = "p") OR level = "b"`, "not supported"},
		{"comparison", `count > 5`, "comparison operator"},
		{"in", `level = "a" | in(host, values=[x,y])`, "in() is not supported"},
		{"group-cmd", `level = "a" | group(host)`, "group() is not supported"},
		{"eval", `level = "a" | eval(x = 1)`, "eval() is not supported"},
		{"model_lookup", `level = "a" | model_lookup(model="m", key=[a,b])`, "model_lookup() is not supported"},
		{"assignment", `x := 1`, "assignments"},
		{"regex-no-output", `level = "a" | regex(field=raw_log, regex="([a-z]+)")`, "needs an output name"},
		{"lowercase-no-extraction", `level = "a" | lowercase(level)`, "must target a field produced by a preceding regex"},
		{"len-no-extraction", `level = "a" | len(level) | _len >= 3`, "must target a field produced by a preceding regex"},
		{"uppercase", `level = "a" | regex(field=raw_log, regex="([a-z]+)", as=t) | uppercase(t)`, "uppercase is not supported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := ParseSourceQuery(tc.query, ModelTypeRarity)
			if len(res.Errors) == 0 {
				t.Fatalf("query %q: expected an error containing %q, got none", tc.query, tc.wantSubst)
			}
			found := false
			for _, e := range res.Errors {
				if strings.Contains(e, tc.wantSubst) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("query %q: errors %v do not contain %q", tc.query, res.Errors, tc.wantSubst)
			}
		})
	}
}

// TestEndToEndBQLToDDL exercises the whole authoring chain the way the builder
// does: a stored definition is rendered to a source query, parsed back, merged
// with per-extraction adornments, and finally compiled to ClickHouse DDL.
func TestEndToEndBQLToDDL(t *testing.T) {
	orig := ModelDefinition{
		Filter: []FilterCondition{
			{Field: "level", Op: "=", Value: "dns"},
			{Field: "src_ip", Op: "cidr", Value: "10.0.0.0/8"},
		},
		Extractions: []ExtractionStep{
			{FromField: "raw_log", Pattern: `query:\s+(\S+?)\.([a-z]+)$`, OutputField: "tld", Lowercase: true, MinLength: 2},
		},
		PartitionKey: "level",
		ValueKey:     "tld",
		MinSample:    5,
	}

	// 1. Render the source query (filter + extraction only).
	src := GenerateSourceQuery(orig)

	// 2. Parse it back (as the parse-query endpoint does).
	parsed := ParseSourceQuery(src, ModelTypeRarity)
	if len(parsed.Errors) != 0 {
		t.Fatalf("source query %q produced errors: %v", src, parsed.Errors)
	}

	// 3. Merge per-extraction adornments (as the frontend does on save).
	def := ModelDefinition{
		Filter:       parsed.Filter,
		Extractions:  parsed.Extractions,
		PartitionKey: orig.PartitionKey,
		ValueKey:     orig.ValueKey,
		MinSample:    orig.MinSample,
	}
	for i := range def.Extractions {
		if def.Extractions[i].OutputField == "tld" {
			def.Extractions[i].Lowercase = true
			def.Extractions[i].MinLength = 2
		}
	}

	// 4. Compile to ClickHouse DDL — must succeed and reference the extracted field.
	tableSQL, mvSQL, err := GenerateDDL(def, ModelTypeRarity, "model_test", "model_mv_test")
	if err != nil {
		t.Fatalf("GenerateDDL failed: %v", err)
	}
	if tableSQL == "" || mvSQL == "" {
		t.Fatal("expected non-empty table and MV DDL")
	}
	if !strings.Contains(mvSQL, "tld") {
		t.Fatalf("MV DDL does not reference extracted field tld:\n%s", mvSQL)
	}
	if !strings.Contains(mvSQL, "isIPAddressInRange") {
		t.Fatalf("MV DDL does not include the cidr guard:\n%s", mvSQL)
	}
}

func TestParseSourceQueryRefinements(t *testing.T) {
	res := ParseSourceQuery(`level = "dns" | regex(field=raw_log, regex="([a-z]+)", as=tld) | len(tld) | _len >= 4 | lowercase(tld)`, ModelTypeRarity)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	if len(res.Extractions) != 1 {
		t.Fatalf("want 1 extraction, got %d", len(res.Extractions))
	}
	ext := res.Extractions[0]
	if ext.OutputField != "tld" || !ext.Lowercase || ext.MinLength != 4 {
		t.Fatalf("refinements not applied: %+v", ext)
	}

	// `>` maps to n+1.
	res2 := ParseSourceQuery(`level = "a" | regex(field=raw_log, regex="([a-z]+)", as=t) | len(t) | _len > 4`, ModelTypeRarity)
	if len(res2.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res2.Errors)
	}
	if res2.Extractions[0].MinLength != 5 {
		t.Fatalf("len > 4 should map to MinLength 5, got %d", res2.Extractions[0].MinLength)
	}

	// Multiple length filters with named len outputs stay independent (no collision,
	// no warning) and map to the correct extraction.
	res3 := ParseSourceQuery(`level = "a" | regex(field=raw_log, regex="(\\S+)", as=a) | len(a, as=a_len) | a_len >= 2 | regex(field=raw_log, regex="(\\d+)", as=b) | len(b, as=b_len) | b_len >= 3`, ModelTypeRarity)
	if len(res3.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res3.Errors)
	}
	if len(res3.Warnings) != 0 {
		t.Fatalf("expected no warnings with named len outputs, got %v", res3.Warnings)
	}
	if res3.Extractions[0].MinLength != 2 || res3.Extractions[1].MinLength != 3 {
		t.Fatalf("min lengths mis-paired: %+v", res3.Extractions)
	}
}

// A named capture group must win over as=, matching the regex() runtime, so the
// parsed OutputField equals the column the live preview produces.
func TestParseSourceQueryNamedGroupWinsOverAs(t *testing.T) {
	res := ParseSourceQuery(`level = "a" | regex(field=raw_log, regex="(?<tld>[a-z]+)", as=foo)`, ModelTypeRarity)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	if len(res.Extractions) != 1 || res.Extractions[0].OutputField != "tld" {
		t.Fatalf("expected OutputField 'tld' (named group wins), got %+v", res.Extractions)
	}

	// More than one named group has no single output column and is rejected.
	res2 := ParseSourceQuery(`level = "a" | regex(field=raw_log, regex="(?<a>x)(?<b>y)")`, ModelTypeRarity)
	if len(res2.Errors) == 0 {
		t.Fatal("expected an error for a multi-named-group pattern")
	}
}

// A filter regex value containing slashes must still produce a parseable query.
func TestSlashInRegexFilterIsParseable(t *testing.T) {
	for _, val := range []string{`a/b/c`, `a\/b`, `x\d+/y`} {
		def := ModelDefinition{Filter: []FilterCondition{{Field: "raw_log", Op: "~", Value: val}}}
		got := ParseSourceQuery(GenerateSourceQuery(def), ModelTypeRarity)
		if len(got.Errors) != 0 {
			t.Fatalf("value %q produced an unparseable source query: %v", val, got.Errors)
		}
	}
}

func TestParseSourceQueryCandidateFields(t *testing.T) {
	res := ParseSourceQuery(`level = "dns" | regex(field=raw_log, regex="([a-z]+)", as=tld)`, ModelTypeRarity)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	want := map[string]bool{"raw_log": true, "level": true, "tld": true}
	for w := range want {
		found := false
		for _, c := range res.CandidateFields {
			if c == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("candidate fields %v missing %q", res.CandidateFields, w)
		}
	}
}
