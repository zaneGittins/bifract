package models

import (
	"strings"
	"testing"

	"bifract/pkg/parser"
)

// A model source is no longer limited to what the structured filter can hold:
// SourceBQL carries the query and the translator compiles it. What stays refused
// is what a windowed, row-preserving read cannot mean.
func TestParseSourceQueryAcceptsWiderBQL(t *testing.T) {
	queries := []string{
		`level = "a" OR level = "b"`,
		`(level = "a" AND env = "p") OR level = "b"`,
		`count > 5`,
		`level = "a" | in(host, values=[x,y])`,
		`level = "a" | eval(x = 1)`,
		`x := 1`,
		`level = "a" | regex(field=norm_log, regex="([a-z]+)", as=t) | uppercase(t)`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			res := ParseSourceQuery(q)
			if len(res.Errors) != 0 {
				t.Fatalf("query %q: unexpected errors %v", q, res.Errors)
			}
			if res.SourceBQL != q {
				t.Fatalf("query %q: SourceBQL not carried, got %q", q, res.SourceBQL)
			}
		})
	}
}

func TestParseSourceQuerySubsetRejections(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantSubst string // substring expected in one of the errors
	}{
		{"model_lookup", `level = "a" | model_lookup(model="m", key=[a,b])`, "another model's output"},
		{"join", `level = "a" | join(host) { env = "p" }`, "reads a second query"},
		{"chain", `level = "a" | chain(host, within=10m) { event_id = "1"; event_id = "3" }`, "straddle the window"},
		{"groupby", `level = "a" | groupby(host)`, "collapses rows"},
		{"sort", `level = "a" | sort(host)`, "reorders or drops rows"},
		{"dedup", `level = "a" | dedup(host)`, "reorders or drops rows"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := ParseSourceQuery(tc.query)
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
			{FromField: "norm_log", Pattern: `query:\s+(\S+?)\.([a-z]+)$`, OutputField: "tld", Lowercase: true, MinLength: 2},
		},
		PartitionKey: "level",
		ValueKey:     "tld",
		MinSample:    5,
	}

	// 1. Render the source query (filter + extraction only).
	src := GenerateSourceQuery(orig)

	// 2. Parse it back (as the parse-query endpoint does).
	parsed := ParseSourceQuery(src)
	if len(parsed.Errors) != 0 {
		t.Fatalf("source query %q produced errors: %v", src, parsed.Errors)
	}

	// 3. Merge per-extraction adornments (as the frontend does on save).
	def := ModelDefinition{
		SourceBQL:    parsed.SourceBQL,
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
	if err := validateDefinitionShape(ModelTypeRarity, def); err != nil {
		t.Fatalf("definition rejected: %v", err)
	}

	// 4. Resolve the source query, as every render path does.
	compiled, cerr := compileSourcePredicates(def.SourceBQL, parser.QueryOptions{})
	if cerr != nil {
		t.Fatalf("compile source: %v", cerr)
	}
	def.compiled, def.compiledSet = compiled.preds, true
	def.projections = modelProjections(compiled.projections, def.Extractions)

	// 5. Compile to ClickHouse DDL: must succeed and reference the extracted field.
	tableSQL, err := GenerateDDL(def, ModelTypeRarity, "model_test")
	if err != nil {
		t.Fatalf("GenerateDDL failed: %v", err)
	}
	mvSQL, err := BuildBackfillInsert(def, ModelTypeRarity, "model_test", "logs", "", "f1")
	if err != nil {
		t.Fatalf("state insert failed: %v", err)
	}
	if tableSQL == "" || mvSQL == "" {
		t.Fatal("expected non-empty table DDL and state insert")
	}
	if !strings.Contains(mvSQL, "tld") {
		t.Fatalf("state insert does not reference extracted field tld:\n%s", mvSQL)
	}
	if !strings.Contains(mvSQL, "isIPAddressInRange") {
		t.Fatalf("state insert does not include the cidr guard:\n%s", mvSQL)
	}
}

func TestParseSourceQueryRefinements(t *testing.T) {
	res := ParseSourceQuery(`level = "dns" | regex(field=norm_log, regex="([a-z]+)", as=tld) | len(tld) | _len >= 4 | lowercase(tld)`)
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
	res2 := ParseSourceQuery(`level = "a" | regex(field=norm_log, regex="([a-z]+)", as=t) | len(t) | _len > 4`)
	if len(res2.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res2.Errors)
	}
	if res2.Extractions[0].MinLength != 5 {
		t.Fatalf("len > 4 should map to MinLength 5, got %d", res2.Extractions[0].MinLength)
	}

	// Multiple length filters with named len outputs stay independent (no collision,
	// no warning) and map to the correct extraction.
	res3 := ParseSourceQuery(`level = "a" | regex(field=norm_log, regex="(\\S+)", as=a) | len(a, as=a_len) | a_len >= 2 | regex(field=norm_log, regex="(\\d+)", as=b) | len(b, as=b_len) | b_len >= 3`)
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
	res := ParseSourceQuery(`level = "a" | regex(field=norm_log, regex="(?<tld>[a-z]+)", as=foo)`)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	if len(res.Extractions) != 1 || res.Extractions[0].OutputField != "tld" {
		t.Fatalf("expected OutputField 'tld' (named group wins), got %+v", res.Extractions)
	}

	// More than one named group has no single output column. The command still
	// runs in the source, so it warns rather than refusing, and contributes no
	// extraction the model could key on.
	res2 := ParseSourceQuery(`level = "a" | regex(field=norm_log, regex="(?<a>x)(?<b>y)")`)
	if len(res2.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res2.Errors)
	}
	if len(res2.Warnings) == 0 || len(res2.Extractions) != 0 {
		t.Fatalf("expected a warning and no extraction, got warnings %v extractions %+v", res2.Warnings, res2.Extractions)
	}
}

// A filter regex value containing slashes must still produce a parseable query.
func TestSlashInRegexFilterIsParseable(t *testing.T) {
	for _, val := range []string{`a/b/c`, `a\/b`, `x\d+/y`} {
		def := ModelDefinition{Filter: []FilterCondition{{Field: "norm_log", Op: "~", Value: val}}}
		got := ParseSourceQuery(GenerateSourceQuery(def))
		if len(got.Errors) != 0 {
			t.Fatalf("value %q produced an unparseable source query: %v", val, got.Errors)
		}
	}
}

func TestParseSourceQueryCandidateFields(t *testing.T) {
	res := ParseSourceQuery(`level = "dns" | regex(field=norm_log, regex="([a-z]+)", as=tld)`)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	want := map[string]bool{"norm_log": true, "level": true, "tld": true}
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

// The editor presents Filter as the model's filter, so it must know when that
// list is only part of the query.
func TestParseSourceQueryFilterComplete(t *testing.T) {
	complete := []string{
		``,
		`level = "dns"`,
		`level = "dns" | cidr(src_ip, "10.0.0.0/8")`,
		`level = "dns" | regex(field=norm_log, regex="([a-z]+)", as=tld) | len(tld) | _len >= 4 | lowercase(tld)`,
		`level = "dns" AND user = "x"`,
		`* | level = "dns"`,
		`* | level = "dns" AND user = "x" | cidr(src_ip, "10.0.0.0/8")`,
	}
	for _, q := range complete {
		if res := ParseSourceQuery(q); !res.FilterComplete {
			t.Errorf("%q: FilterComplete should hold, filter %+v", q, res.Filter)
		}
	}
	partial := []string{
		`level = "a" OR level = "b"`,
		`(level = "a" AND env = "p") OR level = "b"`,
		`count > 5`,
		`level = "a" | in(host, values=[x,y])`,
		`level = "a" | lowercase(level)`,
		`level = "a" | regex(field=norm_log, regex="(?<a>x)(?<b>y)")`,
	}
	for _, q := range partial {
		if res := ParseSourceQuery(q); res.FilterComplete {
			t.Errorf("%q: FilterComplete should not hold; the chips would misdescribe the query", q)
		}
	}
}

// Each filter operator and its negation lowers to the structured form the editor
// shows, and back to a query that means the same thing.
func TestParseSourceQueryLowersOperators(t *testing.T) {
	cases := []struct {
		query string
		want  FilterCondition
	}{
		{`user != "bob"`, FilterCondition{Field: "user", Op: "!=", Value: "bob"}},
		{`NOT user = "bob"`, FilterCondition{Field: "user", Op: "!=", Value: "bob"}},
		{`image = /evil\/x/`, FilterCondition{Field: "image", Op: "~", Value: "evil/x"}},
		{`NOT image = /evil/`, FilterCondition{Field: "image", Op: "!~", Value: "evil"}},
		{`* | NOT user = "bob"`, FilterCondition{Field: "user", Op: "!=", Value: "bob"}},
		{`* | image != /evil/`, FilterCondition{Field: "image", Op: "!~", Value: "evil"}},
	}
	for _, c := range cases {
		res := ParseSourceQuery(c.query)
		if len(res.Errors) != 0 {
			t.Fatalf("%s: %v", c.query, res.Errors)
		}
		if len(res.Filter) != 1 || res.Filter[0] != c.want {
			t.Errorf("%s: filter %+v, want %+v", c.query, res.Filter, c.want)
			continue
		}
		round := ParseSourceQuery(GenerateSourceQuery(ModelDefinition{Filter: res.Filter}))
		if len(round.Filter) != 1 || round.Filter[0] != c.want {
			t.Errorf("%s: round trip %+v", c.query, round.Filter)
		}
	}
}

// A grouped condition still offers its fields to shape the model on, though the
// structured filter cannot hold it.
func TestParseSourceQueryGroupedFieldsAreCandidates(t *testing.T) {
	res := ParseSourceQuery(`(user = "a" OR host = "b") AND level = "x"`)
	if len(res.Errors) != 0 {
		t.Fatal(res.Errors)
	}
	for _, f := range []string{"user", "host", "level"} {
		found := false
		for _, c := range res.CandidateFields {
			found = found || c == f
		}
		if !found {
			t.Errorf("candidate fields %v missing %q", res.CandidateFields, f)
		}
	}
}

// A regex() the model cannot key on still runs in the source; it warns and adds
// no extraction rather than offering a column that will not exist.
func TestParseSourceQueryRegexWarnings(t *testing.T) {
	cases := map[string]string{
		`level = "a" | regex(field=raw_log, regex="([a-z]+)", as=t)`: "raw_log cannot be a model extraction source",
		`level = "a" | regex(field=norm_log, regex="([a-z]+)")`:      "needs an output name",
	}
	for q, want := range cases {
		res := ParseSourceQuery(q)
		if len(res.Extractions) != 0 || res.FilterComplete {
			t.Errorf("%s: extractions %+v complete %v", q, res.Extractions, res.FilterComplete)
		}
		found := false
		for _, w := range res.Warnings {
			found = found || strings.Contains(w, want)
		}
		if !found {
			t.Errorf("%s: warnings %v, want %q", q, res.Warnings, want)
		}
	}
	if res := ParseSourceQuery(`level = = "a"`); len(res.Errors) == 0 || res.SourceBQL != "" {
		t.Errorf("unparseable query: %+v", res)
	}
}
