package sigma

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
)

// toSQL runs generated BQL through the real parser and SQL translator. Checking the
// SQL, not just that the BQL parses, is what proves a value survived both layers of
// escaping intact: a Sigma value that reaches ClickHouse altered matches nothing.
func toSQL(t *testing.T, bql string) string {
	t.Helper()
	p, err := parser.ParseQuery(bql)
	if err != nil {
		t.Fatalf("generated query is not valid BQL: %s\n  error: %v", bql, err)
	}
	sql, err := parser.TranslateToSQL(p, parser.QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("BQL %s does not translate to SQL: %v", bql, err)
	}
	return sql
}

func singleSelection(fc FieldCondition) *SigmaRule {
	return &SigmaRule{Title: "t", Detection: Detection{
		Condition:  "sel",
		Selections: map[string]SelectionGroup{"sel": {FieldConditions: []FieldCondition{fc}}},
	}}
}

func fc(field string, mods []string, values ...string) FieldCondition {
	return FieldCondition{Field: field, Modifiers: mods, Values: values}
}

const (
	cmdRef = "fields.`CommandLine`::String"
	imgRef = "fields.`Image`::String"
)

func TestTranslateModifiers(t *testing.T) {
	contains, starts, ends, re := []string{"contains"}, []string{"startswith"}, []string{"endswith"}, []string{"re"}
	cases := []struct {
		name string
		cond FieldCondition
		bql  string
		sql  []string // fragments the final SQL must contain
	}{
		// Exact match is case-insensitive in Sigma: a value with letters compares
		// lower(field), its value intact through both escapes.
		{"exact", fc("Image", nil, "cmd.exe"), `in(lower(Image), values=["cmd.exe"])`,
			[]string{"lower(" + imgRef + ") IN ('cmd.exe')"}},
		{"exact with backslash and double quote", fc("Image", nil, `C:\a "b"`), `in(lower(Image), values=["c:\\a \"b\""])`,
			[]string{"lower(" + imgRef + `) IN ('c:\\a "b"')`}},
		{"exact with single quote", fc("User", nil, `O'Brien`), `in(lower(User), values=["o'brien"])`,
			[]string{"lower(fields.`User`::String) IN ('o\\'brien')"}},
		// A value with no letters has no other case, so it stays an equality: the one
		// form the field's skip index can prune with.
		{"exact without letters stays equality", fc("EventID", nil, "4688"), `EventID="4688"`,
			[]string{"fields.`EventID`::String = '4688'"}},
		{"exact list stays OR'd equality", fc("EventID", nil, "1", "4688"), `(EventID="1" OR EventID="4688")`,
			[]string{"fields.`EventID`::String = '1'", "fields.`EventID`::String = '4688'"}},
		{"exact empty value stays equality", fc("Description", nil, ""), `Description=""`,
			[]string{"fields.`Description`::String = ''"}},

		// contains/startswith/endswith: native case-insensitive operators.
		{"contains bare value", fc("CommandLine", contains, "powershell"), `CommandLine=~powershell`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['powershell'])"}},
		// Values the lexer cannot read back as one bare word are quoted. A leading
		// hyphen is the common case: "-enc" bare lexes as a minus sign.
		{"contains leading hyphen", fc("CommandLine", contains, "-enc"), `CommandLine=~"-enc"`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['-enc'])"}},
		{"contains quoted value", fc("CommandLine", contains, `IEX "a b"`), `CommandLine=~"IEX \"a b\""`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + `, ['IEX "a b"'])`}},
		{"contains list", fc("CommandLine", contains, "wget", "curl -o"), `CommandLine=~wget,"curl -o"`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['wget', 'curl -o'])"}},
		{"list with a leading-hyphen value", fc("CommandLine", contains, "wget", "-O"), `CommandLine=~wget,"-O"`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['wget', '-O'])"}},
		{"endswith leading dot", fc("Image", ends, ".hta"), `Image=$".hta"`,
			[]string{"endsWith(lower(" + imgRef + "), '.hta')"}},
		{"contains escaped asterisk", fc("CommandLine", contains, `\*x`), `CommandLine=~"*x"`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['*x'])"}},
		{"contains a keyword", fc("CommandLine", contains, "NOT"), `CommandLine=~"NOT"`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['NOT'])"}},
		{"digit-led value stays bare", fc("Version", starts, "10.0"), `Version=^10.0`,
			[]string{"startsWith(lower(fields.`Version`::String), '10.0')"}},
		{"startswith lowers the needle", fc("Image", starts, `C:\Windows`), `Image=^"C:\\Windows"`,
			[]string{"startsWith(lower(" + imgRef + `), 'c:\\windows')`}},
		{"endswith", fc("Image", ends, `\CMD.exe`), `Image=$"\\CMD.exe"`,
			[]string{"endsWith(lower(" + imgRef + `), '\\cmd.exe')`}},

		// A literal comma would split the native list, so that value alone falls back
		// to a regex with the value escaped; the rest keep the native operator.
		{"contains with comma", fc("CommandLine", contains, "a,b.c"), `CommandLine=/a,b\.c/i`,
			[]string{"match(" + cmdRef + ", '(?i)a,b\\\\.c')"}},
		{"startswith with comma", fc("CommandLine", starts, "a,b"), `CommandLine=/^a,b/i`, nil},
		{"endswith with comma", fc("CommandLine", ends, "a,b"), `CommandLine=/a,b$/i`, nil},
		{"contains list with a comma value", fc("CommandLine", contains, "a,b", "c(d)"), `(CommandLine=~"c(d)" OR CommandLine=/a,b/i)`, nil},
		{"startswith list with a comma value", fc("CommandLine", starts, "a,b", "c"), `(CommandLine=^c OR CommandLine=/^a,b/i)`, nil},
		{"endswith list with a comma value", fc("CommandLine", ends, "a,b", "c"), `(CommandLine=$c OR CommandLine=/a,b$/i)`, nil},
		{"slash in a fallback value is escaped", fc("CommandLine", contains, "/tmp/x,y"), `CommandLine=/\/tmp\/x,y/i`, nil},

		// re: the user's pattern, case-sensitive as Sigma specifies (no /i).
		{"regex", fc("CommandLine", re, `foo.*bar`), `CommandLine=/foo.*bar/`,
			[]string{"match(" + cmdRef + ", 'foo.*bar')"}},

		// |all: every value must match, one predicate each.
		{"contains all", fc("CommandLine", []string{"contains", "all"}, "-nop", "-w hidden"), `(CommandLine=~"-nop" AND CommandLine=~"-w hidden")`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['-nop'])", "multiSearchAnyCaseInsensitive(" + cmdRef + ", ['-w hidden'])"}},
		{"contains all with a comma value", fc("CommandLine", []string{"contains", "all"}, "a,b", "c"), `(CommandLine=/a,b/i AND CommandLine=~c)`, nil},
		{"startswith all", fc("Image", []string{"startswith", "all"}, "a", "b"), `(Image=^a AND Image=^b)`, nil},
		{"regex all", fc("CommandLine", []string{"re", "all"}, "a.", "b+"), `(CommandLine=/a./ AND CommandLine=/b+/)`, nil},
		{"all with one value is just the value", fc("CommandLine", []string{"contains", "all"}, "x"), `CommandLine=~x`, nil},

		// Keywords (no field): case-insensitive substring over the whole event.
		{"keyword", fc("", nil, "mimikatz"), `/.*mimikatz.*/i`, []string{"norm_log"}},
		{"keyword is regex-escaped", fc("", nil, "a.b/c(d)"), `/.*a\.b\/c\(d\).*/i`, []string{"norm_log"}},
		{"keyword list", fc("", nil, "a+b", "c"), `/.*(?:a\+b|c).*/i`, nil},

		// null: the field must be absent or empty.
		{"null", FieldCondition{Field: "ParentImage"}, `NOT ParentImage=*`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Translate(singleSelection(tc.cond), nil)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.bql {
				t.Fatalf("BQL\n got  %s\n want %s", got, tc.bql)
			}
			sql := toSQL(t, got)
			for _, frag := range tc.sql {
				if !strings.Contains(sql, frag) {
					t.Errorf("SQL is missing %s\n%s", frag, sql)
				}
			}
		})
	}
}

// A null keyword selection constrains nothing.
func TestTranslateNullKeyword(t *testing.T) {
	got, err := Translate(singleSelection(FieldCondition{}), nil)
	if err != nil || got != "*" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// Modifiers that would change what matches but cannot be expressed must fail the
// rule, never translate into a different (weaker or broader) query.
func TestTranslateUnsupportedModifiersFail(t *testing.T) {
	cases := []struct {
		mods []string
		want string
	}{
		{[]string{"base64"}, "base64"},
		{[]string{"base64offset", "contains"}, "base64"},
		{[]string{"wide", "base64offset", "contains"}, "base64"},
		{[]string{"cidr"}, "cidr"},
		// Every modifier the translator does not implement is refused by name, never
		// dropped: each of these used to translate as if the modifier were absent.
		{[]string{"windash", "contains"}, "'windash'"},
		{[]string{"exists"}, "'exists'"},
		{[]string{"gt"}, "'gt'"},
		{[]string{"gte"}, "'gte'"},
		{[]string{"lt"}, "'lt'"},
		{[]string{"lte"}, "'lte'"},
		{[]string{"fieldref"}, "'fieldref'"},
		{[]string{"expand"}, "'expand'"},
		{[]string{"utf16le", "base64"}, "'utf16le', 'base64'"},
		{[]string{"utf16be", "base64"}, "'utf16be'"},
		{[]string{"utf16", "base64"}, "'utf16'"},
		{[]string{"wide", "base64"}, "'wide'"},
		{[]string{"cased"}, "'cased'"},
		// i, m and s are |re sub-modifiers and mean nothing elsewhere.
		{[]string{"contains", "i"}, "'i'"},
		{[]string{"m"}, "'m'"},
		{[]string{"endswith", "s"}, "'s'"},
		{[]string{"contains", "nosuchmodifier"}, "'nosuchmodifier'"},
		// Two comparison modifiers have no Sigma meaning; the last one used to win.
		{[]string{"contains", "endswith"}, "'contains', 'endswith' cannot be combined"},
		{[]string{"re", "contains"}, "cannot be combined"},
	}
	for _, c := range cases {
		rule := singleSelection(fc("DestinationIp", c.mods, "10.0.0.0/8"))
		got, err := Translate(rule, nil)
		if err == nil {
			t.Errorf("%v: translated to %q, want an error", c.mods, got)
			continue
		}
		for _, frag := range []string{c.want, "DestinationIp", "selection 'sel'"} {
			if !strings.Contains(err.Error(), frag) {
				t.Errorf("%v: error %q should mention %q", c.mods, err, frag)
			}
		}
	}
}

// The refusal reaches a parsed rule too, so a feed rule using windash is reported as
// untranslatable rather than deployed without its dash variants.
func TestParsedRuleWithUnsupportedModifierFails(t *testing.T) {
	rule, err := ParseSigmaRule(`
title: Encoded command
detection:
  selection:
    Image|endswith: \powershell.exe
    CommandLine|windash|contains: ' -enc '
  condition: selection
`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Translate(rule, nil)
	if err == nil {
		t.Fatalf("translated to %q, want an error", got)
	}
	for _, frag := range []string{"selection 'selection'", "field 'CommandLine'", "'windash'"} {
		if !strings.Contains(err.Error(), frag) {
			t.Errorf("error %q should mention %s", err, frag)
		}
	}
}

func TestParseModifiersAreNormalized(t *testing.T) {
	conds, err := parseFieldConditions(map[string]interface{}{"CommandLine | CONTAINS |All ": []interface{}{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(conds) != 1 || conds[0].Field != "CommandLine" || strings.Join(conds[0].Modifiers, ",") != "contains,all" {
		t.Fatalf("got %+v", conds)
	}
	got, err := Translate(singleSelection(conds[0]), nil)
	if err != nil || got != `(CommandLine=~a AND CommandLine=~b)` {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestParseValueTypes(t *testing.T) {
	rule, err := ParseSigmaRule(`
title: types
detection:
  sel:
    EventID: 4688
    Ratio: 1.5
    Whole: 2.0
    Big: 4294967296
    Flag: true
    Missing: null
    Mixed: [1, 'a', false]
  condition: sel
`)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{}
	for _, c := range rule.Detection.Selections["sel"].FieldConditions {
		got[c.Field] = c.Values
	}
	want := map[string][]string{
		"EventID": {"4688"},
		"Ratio":   {"1.5"},
		"Whole":   {"2"},
		"Big":     {"4294967296"},
		"Flag":    {"true"},
		"Missing": nil,
		"Mixed":   {"1", "a", "false"},
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("%s: missing", k)
			continue
		}
		if fmt.Sprint(g) != fmt.Sprint(w) || (w == nil) != (g == nil) {
			t.Errorf("%s = %#v, want %#v", k, g, w)
		}
	}
}

func TestParseSelectionShapes(t *testing.T) {
	rule, err := ParseSigmaRule(`
title: shapes
detection:
  alts:
    - Image|endswith: \a.exe
    - Image|endswith: \b.exe
  one_alt:
    - User: root
  keywords:
    - mimikatz
    - sekurlsa
  keyword: lsass
  condition: alts or one_alt or keywords or keyword
`)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"alts":     `(Image=$"\\a.exe" OR Image=$"\\b.exe")`,
		"one_alt":  `in(lower(User), values=["root"])`,
		"keywords": `/.*(?:mimikatz|sekurlsa).*/i`,
		"keyword":  `/.*lsass.*/i`,
	}
	for name, want := range cases {
		got, err := translateSelection(rule.Detection.Selections[name], func(s string) string { return s })
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != want {
			t.Errorf("%s: got %s, want %s", name, got, want)
		}
		toSQL(t, got)
	}
}

func TestParseSigmaRuleRejectsMalformed(t *testing.T) {
	cases := map[string]struct{ yaml, want string }{
		"invalid yaml":       {"title: [", "failed to parse"},
		"no detection":       {"title: x\n", "missing detection"},
		"no condition":       {"title: x\ndetection:\n  sel:\n    a: b\n", "missing 'condition'"},
		"list condition":     {"title: x\ndetection:\n  sel:\n    a: b\n  condition: [sel]\n", "must be a string"},
		"mixed alternatives": {"title: x\ndetection:\n  sel:\n    - a: b\n    - plain\n  condition: sel\n", "alternative 1: expected map"},
		"no title":           {"detection:\n  sel:\n    a: b\n  condition: sel\n", "missing title"},
	}
	for name, c := range cases {
		_, err := ParseSigmaRule(c.yaml)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want error containing %q", name, err, c.want)
		}
	}
}

// --- Condition semantics ---
//
// The condition parser's output is checked by evaluating it as a boolean formula
// over every assignment of its selections, against the meaning Sigma gives the
// condition. That pins precedence and quantifiers without depending on how many
// parentheses the output carries or which order map iteration put OR'd terms in.

func evalBool(t *testing.T, expr string, vals map[string]bool) bool {
	t.Helper()
	toks := strings.Fields(strings.NewReplacer("(", " ( ", ")", " ) ").Replace(expr))
	pos := 0
	var or, and, factor func() bool
	factor = func() bool {
		tok := toks[pos]
		pos++
		switch tok {
		case "NOT":
			return !factor()
		case "(":
			v := or()
			if toks[pos] != ")" {
				t.Fatalf("unbalanced %q", expr)
			}
			pos++
			return v
		}
		v, ok := vals[tok]
		if !ok {
			t.Fatalf("unknown term %q in %q", tok, expr)
		}
		return v
	}
	and = func() bool {
		v := factor()
		for pos < len(toks) && toks[pos] == "AND" {
			pos++
			r := factor()
			v = v && r
		}
		return v
	}
	or = func() bool {
		v := and()
		for pos < len(toks) && toks[pos] == "OR" {
			pos++
			r := and()
			v = v || r
		}
		return v
	}
	v := or()
	if pos != len(toks) {
		t.Fatalf("trailing tokens in %q", expr)
	}
	return v
}

func TestConditionSemantics(t *testing.T) {
	names := []string{"a", "b", "c", "filter_1", "filter_2", "_aux"}
	exprs := map[string]string{}
	for _, n := range names {
		exprs[n] = strings.ToUpper(n)
	}
	type env = map[string]bool
	cases := []struct {
		cond string
		want func(v env) bool
	}{
		{"a", func(v env) bool { return v["a"] }},
		{"a or b and c", func(v env) bool { return v["a"] || (v["b"] && v["c"]) }},
		{"a and b or c", func(v env) bool { return (v["a"] && v["b"]) || v["c"] }},
		{"(a or b) and c", func(v env) bool { return (v["a"] || v["b"]) && v["c"] }},
		{"not a and b", func(v env) bool { return !v["a"] && v["b"] }},
		{"not (a or b)", func(v env) bool { return !(v["a"] || v["b"]) }},
		{"not not a", func(v env) bool { return v["a"] }},
		{"a AND NOT b OR c", func(v env) bool { return (v["a"] && !v["b"]) || v["c"] }},
		{"a and not 1 of filter_*", func(v env) bool { return v["a"] && !(v["filter_1"] || v["filter_2"]) }},
		{"a and not all of filter_*", func(v env) bool { return v["a"] && !(v["filter_1"] && v["filter_2"]) }},
		{"1 of them", func(v env) bool { return v["a"] || v["b"] || v["c"] || v["filter_1"] || v["filter_2"] }},
		{"all of them", func(v env) bool { return v["a"] && v["b"] && v["c"] && v["filter_1"] && v["filter_2"] }},
		// them leaves out underscore-prefixed selections; naming or a pattern still reaches them.
		{"a and _aux", func(v env) bool { return v["a"] && v["_aux"] }},
		{"1 of _a*", func(v env) bool { return v["_aux"] }},
		{"all of them and not _aux", func(v env) bool {
			return v["a"] && v["b"] && v["c"] && v["filter_1"] && v["filter_2"] && !v["_aux"]
		}},
		{"ALL OF filter*", func(v env) bool { return v["filter_1"] && v["filter_2"] }},
		{"1 of c*", func(v env) bool { return v["c"] }},
		{"1 of b", func(v env) bool { return v["b"] }},
		{"(1 of filter_*) or (a and (b or c))", func(v env) bool {
			return v["filter_1"] || v["filter_2"] || (v["a"] && (v["b"] || v["c"]))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.cond, func(t *testing.T) {
			got, err := evaluateCondition(tc.cond, exprs)
			if err != nil {
				t.Fatal(err)
			}
			for mask := 0; mask < 1<<len(names); mask++ {
				v := env{}
				for i, n := range names {
					v[n] = mask&(1<<i) != 0
				}
				upper := env{}
				for k, b := range v {
					upper[strings.ToUpper(k)] = b
				}
				if evalBool(t, got, upper) != tc.want(v) {
					t.Fatalf("%q translated to %q, which disagrees with Sigma for %v", tc.cond, got, v)
				}
			}
		})
	}
}

func TestConditionErrors(t *testing.T) {
	exprs := map[string]string{"a": "A", "b": "B"}
	cases := map[string]string{
		"":                        "empty condition",
		"   ":                     "empty condition",
		"a or":                    "unknown selection ''",
		"a b":                     "unexpected token 'b'",
		"(a or b":                 "expected ')'",
		"a and (b":                "expected ')'",
		"a or (b":                 "expected ')'",
		"not (b":                  "expected ')'",
		"a)":                      "unexpected token ')'",
		"1 a":                     "expected 'of'",
		"all of":                  "expected target",
		"1 of z*":                 "no selections matched 'z*'",
		"1 of zz":                 "unknown selection 'zz'",
		"missing":                 "unknown selection 'missing'",
		"A":                       "unknown selection 'A'", // selection names are case-sensitive
		"2 of them":               "unknown selection '2'",
		"a | count() by host > 5": "aggregation",
		"a and b | near c":        "aggregation",
	}
	for cond, want := range cases {
		got, err := evaluateCondition(cond, exprs)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: got (%q, %v), want error containing %q", cond, got, err, want)
		}
	}
}

// An error in any selection fails the whole rule, even one the condition never uses:
// a half-translated rule is never deployed.
func TestTranslateFailsOnAnyBadSelection(t *testing.T) {
	rule := &SigmaRule{Title: "t", Detection: Detection{
		Condition: "good",
		Selections: map[string]SelectionGroup{
			"good": {FieldConditions: []FieldCondition{fc("Image", nil, "a")}},
			"bad":  {Alternatives: [][]FieldCondition{{fc("Ip", []string{"cidr"}, "10.0.0.0/8")}}},
		},
	}}
	if got, err := Translate(rule, nil); err == nil || !strings.Contains(err.Error(), "selection 'bad'") {
		t.Fatalf("got (%q, %v)", got, err)
	}
}

// Field names go through the normalizer exactly as ingestion renames them; keyword
// searches have no field and are left alone.
func TestTranslateMapsFieldsThroughNormalizer(t *testing.T) {
	norm := (&normalizers.Normalizer{
		Transforms: []normalizers.Transform{normalizers.TransformFlattenLeaf, normalizers.TransformSnakeCase, normalizers.TransformLowercase},
		FieldMappings: []normalizers.FieldMapping{
			{Sources: []string{"image"}, Target: "process_path"},
			{Sources: []string{"$.Event.ParentImage"}, Target: "parent_path"}, // path sources never apply to Sigma names
		},
	}).Compile()
	rule := &SigmaRule{Title: "t", LogSource: LogSource{Category: "process_creation"}, Detection: Detection{
		Condition: "img and cmd and not parent and kw",
		Selections: map[string]SelectionGroup{
			"img":    {FieldConditions: []FieldCondition{fc("Image", []string{"endswith"}, `\rundll32.exe`)}},
			"cmd":    {FieldConditions: []FieldCondition{fc("CommandLine", []string{"contains"}, "javascript:")}},
			"parent": {FieldConditions: []FieldCondition{{Field: "ParentImage"}}},
			"kw":     {FieldConditions: []FieldCondition{fc("", nil, "RunHTMLApplication")}},
		},
	}}
	got, err := Translate(rule, BuildFieldMapper(norm))
	if err != nil {
		t.Fatal(err)
	}
	want := `bifract_category=process_creation AND ((((process_path=$"\\rundll32.exe" AND command_line=~"javascript:") AND NOT (NOT parent_image=*)) AND /.*RunHTMLApplication.*/i))`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	sql := toSQL(t, got)
	for _, frag := range []string{"fields.`process_path`::String", "fields.`command_line`::String", "fields.`parent_image`::String", "fields.`bifract_category`::String = 'process_creation'"} {
		if !strings.Contains(sql, frag) {
			t.Errorf("SQL is missing %s\n%s", frag, sql)
		}
	}
	if strings.Contains(sql, "`Image`") || strings.Contains(sql, "`CommandLine`") {
		t.Errorf("unmapped Sigma field names leaked into SQL:\n%s", sql)
	}
}

func TestCategoryFilterNormalizesAndQuotes(t *testing.T) {
	cases := map[string]string{
		"  Process_Creation ": "bifract_category=process_creation",
		"File_Event":          "bifract_category=file_write",
		"my category":         `bifract_category="my category"`,
	}
	for in, want := range cases {
		got := CategoryFilter(LogSource{Category: in, Product: "windows"})
		if got != want {
			t.Errorf("CategoryFilter(%q) = %q, want %q", in, got, want)
		}
		toSQL(t, got)
	}
	if CategoryFilter(LogSource{Product: "windows", Service: "security"}) != "" {
		t.Error("product/service-only rules are not category-scoped")
	}
}

func TestBuildLabels(t *testing.T) {
	rule := &SigmaRule{
		Level:     "high",
		Status:    "test",
		Tags:      []string{"attack.execution", "attack.t1059.001", "cve.2021-44228"},
		LogSource: LogSource{Product: "windows", Category: "process_creation", Service: "sysmon"},
	}
	got := BuildLabels(rule)
	want := []string{"sigma:high", "status:test", "attack.execution", "attack.t1059.001", "cve.2021-44228", "product:windows", "category:process_creation"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got  %v\nwant %v", got, want)
	}

	if got := BuildLabels(&SigmaRule{}); len(got) != 0 {
		t.Fatalf("a rule without metadata must carry no labels, got %v", got)
	}
}

// A complete rule as it arrives from a feed: every selection shape and condition
// form together must yield BQL the engine accepts.
func TestTranslateRealisticRuleEndToEnd(t *testing.T) {
	rule, err := ParseSigmaRule(`
title: Suspicious Encoded PowerShell
id: 00000000-0000-0000-0000-000000000001
status: experimental
level: high
tags: [attack.execution, attack.t1059.001]
logsource:
  category: process_creation
  product: windows
detection:
  selection_img:
    - Image|endswith: \powershell.exe
    - OriginalFileName: PowerShell.EXE
  selection_cli:
    CommandLine|contains|all:
      - ' -e'
      - 'JAB'
  filter_main_sccm:
    ParentImage|startswith: C:\Windows\CCM\
  filter_optional_null:
    ParentImage: null
  condition: all of selection_* and not 1 of filter_*
`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Translate(rule, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "bifract_category=process_creation AND (") {
		t.Fatalf("missing category scope: %s", got)
	}
	for _, frag := range []string{
		`Image=$"\\powershell.exe"`,
		`in(lower(OriginalFileName), values=["powershell.exe"])`,
		`CommandLine=~" -e"`,
		`CommandLine=~JAB`,
		`ParentImage=^"C:\\Windows\\CCM\\"`,
		`NOT ParentImage=*`,
	} {
		if !strings.Contains(got, frag) {
			t.Errorf("translation is missing %s:\n%s", frag, got)
		}
	}
	sql := toSQL(t, got)
	if !strings.Contains(sql, `startsWith(lower(fields.`+"`ParentImage`"+`::String), 'c:\\windows\\ccm\\')`) {
		t.Errorf("filter value did not reach SQL intact:\n%s", sql)
	}
	labels := BuildLabels(rule)
	sort.Strings(labels)
	if strings.Join(labels, ",") != "attack.execution,attack.t1059.001,category:process_creation,product:windows,sigma:high,status:experimental" {
		t.Errorf("labels = %v", labels)
	}
}

// --- Value semantics ---
//
// Sigma values are case-insensitive globs: '*' and '?' are wildcards, '\' escapes
// them (or itself). The tests below run the BQL a field condition translates to
// against sample field values, with each operator given the meaning its SQL has:
// = is byte equality, =~ =^ =$ are ASCII case-insensitive (multiSearchAny...
// CaseInsensitive and startsWith/endsWith over lower()), and /re/ is RE2.

// matchBQL evaluates a single-field BQL expression, as Translate emits for one
// field condition, against that field's value.
func matchBQL(t *testing.T, expr, value string) bool {
	t.Helper()
	if strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		inner := expr[1 : len(expr)-1]
		for _, op := range []string{" OR ", " AND "} {
			if parts := splitTopLevel(inner, op); len(parts) > 1 {
				any, all := false, true
				for _, p := range parts {
					m := matchBQL(t, p, value)
					any, all = any || m, all && m
				}
				if op == " OR " {
					return any
				}
				return all
			}
		}
	}
	if strings.HasPrefix(expr, "in(lower(") && strings.HasSuffix(expr, "])") {
		list := expr[strings.Index(expr, "values=[")+len("values=[") : len(expr)-2]
		lv := asciiLower(value)
		for _, v := range bqlValueList(t, list) {
			if lv == v {
				return true
			}
		}
		return false
	}
	i := strings.IndexByte(expr, '=')
	if i < 0 {
		t.Fatalf("not a field condition: %s", expr)
	}
	rest := expr[i+1:]
	switch {
	case rest == "*":
		return value != ""
	case strings.HasPrefix(rest, `"`):
		vals := bqlValueList(t, rest)
		return len(vals) == 1 && value == vals[0]
	case strings.HasPrefix(rest, "/"):
		end := strings.LastIndexByte(rest, '/')
		pattern := rest[1:end]
		if strings.Contains(rest[end+1:], "i") {
			pattern = "(?i)" + pattern
		}
		return regexp.MustCompile(pattern).MatchString(value)
	}
	lv := asciiLower(value)
	for _, v := range bqlValueList(t, rest[1:]) {
		v = asciiLower(v)
		if rest[0] == '~' && strings.Contains(lv, v) || rest[0] == '^' && strings.HasPrefix(lv, v) || rest[0] == '$' && strings.HasSuffix(lv, v) {
			return true
		}
	}
	return false
}

// splitTopLevel splits s on sep outside parentheses, quotes and regex literals.
func splitTopLevel(s, sep string) []string {
	var parts []string
	depth, start := 0, 0
	inQuote, inRegex := false, false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case (inQuote || inRegex) && c == '\\':
			i++
		case inQuote:
			inQuote = c != '"'
		case inRegex:
			inRegex = c != '/'
		case c == '"':
			inQuote = true
		case c == '/':
			inRegex = true
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(s[i:], sep):
			parts = append(parts, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	return append(parts, s[start:])
}

// bqlValueList decodes a comma-separated list of bare or double-quoted BQL values.
func bqlValueList(t *testing.T, s string) []string {
	t.Helper()
	var out []string
	for len(s) > 0 {
		var b strings.Builder
		if s[0] == '"' {
			i := 1
			for ; i < len(s) && s[i] != '"'; i++ {
				if s[i] == '\\' {
					i++
				}
				b.WriteByte(s[i])
			}
			s = s[i+1:]
		} else {
			i := strings.IndexByte(s, ',')
			if i < 0 {
				i = len(s)
			}
			b.WriteString(s[:i])
			s = s[i:]
		}
		out = append(out, b.String())
		if len(s) > 0 {
			if s[0] != ',' {
				t.Fatalf("malformed value list near %q", s)
			}
			s = s[1:]
		}
	}
	return out
}

func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

func TestTranslateValueSemantics(t *testing.T) {
	const sys32 = `C:\Windows\System32\`
	contains, starts, ends := []string{"contains"}, []string{"startswith"}, []string{"endswith"}
	cases := []struct {
		name  string
		cond  FieldCondition
		match []string
		miss  []string
	}{
		// Case-insensitive, as Sigma specifies for every non-regex value.
		{"exact ignores case", fc("OriginalFileName", nil, "PowerShell.EXE"),
			[]string{"PowerShell.EXE", "powershell.exe", "POWERSHELL.EXE"},
			[]string{"powershell.exe ", "xpowershell.exe", "powershellXexe"}},
		{"exact digits", fc("EventID", nil, "4688"), []string{"4688"}, []string{"46888", "468"}},
		{"exact list mixes cased and caseless", fc("EventID", nil, "1", "Create"),
			[]string{"1", "create", "CREATE"}, []string{"11", "created"}},
		{"endswith ignores case", fc("Image", ends, `\cmd.exe`),
			[]string{sys32 + "CMD.EXE", `\cmd.exe`}, []string{"cmd.exe", sys32 + "cmd.exe.bak"}},

		// Wildcards in exact values. A leading or trailing '*' is a suffix/prefix match.
		{"exact leading star", fc("Image", nil, `*\cmd.exe`),
			[]string{sys32 + "cmd.exe", sys32 + "CMD.exe", `\cmd.exe`}, []string{"cmd.exe", sys32 + "cmd.exe2"}},
		{"exact trailing star", fc("Image", nil, `C:\Windows\\*`),
			[]string{sys32 + "cmd.exe", `c:\windows\`}, []string{`D:\Windows\x`, `C:\Windows`}},
		{"exact inner star", fc("Image", nil, `C:\Users\\*\AppData\\*.exe`),
			[]string{`C:\Users\bob\AppData\Local\Temp\x.EXE`}, []string{`C:\Users\AppData\x.exe`, `C:\Users\bob\AppData\x.dll`}},
		{"exact question mark", fc("Image", nil, `*\cmd?.exe`),
			[]string{`C:\cmd1.exe`, `C:\CMDX.EXE`}, []string{`C:\cmd.exe`, `C:\cmd12.exe`}},
		{"exact lone star matches any value", fc("Image", nil, "*"), []string{"x"}, []string{""}},

		// Wildcards inside contains/startswith/endswith.
		{"contains inner star", fc("CommandLine", contains, "foo*bar"),
			[]string{"xFOOyyBARz", "foobar", "foo\nbar"}, []string{"barfoo", "foo*", "fobar"}},
		{"contains question mark", fc("CommandLine", contains, "-e?c"),
			[]string{"powershell -enc AAA", "-E C"}, []string{"-ec", "-e"}},
		{"startswith inner star", fc("Image", starts, `C:\Users\\*\AppData\`),
			[]string{`C:\Users\bob\AppData\Roaming\x.exe`}, []string{`D:\Users\bob\AppData\`, `C:\Users\bob\Temp\`}},
		{"endswith inner star", fc("TargetFilename", ends, `\Start Menu\\*.lnk`),
			[]string{`C:\x\Start Menu\Programs\a.LNK`}, []string{`C:\x\Start Menu\a.lnk.txt`}},
		{"contains all with wildcards", fc("CommandLine", []string{"contains", "all"}, "-w*hidden", "-e"),
			[]string{"powershell -W Hidden -enc x"}, []string{"powershell -w hidden", "powershell -enc x"}},
		{"wildcard list mixes forms", fc("CommandLine", contains, "a*b", "plain", "c,d"),
			[]string{"xAyyBx", "PLAIN", "c,d"}, []string{"ba", "c d"}},

		// Escapes make wildcards and backslashes literal.
		{"escaped star is literal", fc("CommandLine", contains, `a\*b`),
			[]string{"xa*bx", "A*B"}, []string{"axb", "ab"}},
		{"escaped question mark is literal", fc("Url", ends, `index.php\?`),
			[]string{"/x/index.php?"}, []string{"/x/index.phpx"}},
		// A backslash before a wildcard escapes it, so a path separator followed by a
		// wildcard is written \\* (Sigma specification, "Escaping").
		{"escaped backslash before a wildcard", fc("Image", nil, `C:\\*`),
			[]string{`C:\anything`}, []string{`C:x`, `D:\x`}},
		{"single backslash before a star escapes it", fc("Image", nil, `C:\Windows\*`),
			[]string{`C:\Windows*`, `c:\windows*`}, []string{`C:\Windows\x`, `C:\Windows\`}},
		{"three backslashes are a backslash and a literal star", fc("Image", nil, `C:\\\*`),
			[]string{`C:\*`}, []string{`C:\x`}},
		{"four backslashes are two", fc("Share", nil, `\\\\srv`),
			[]string{`\\SRV`}, []string{`\srv`}},
		{"plain backslash before a letter is literal", fc("Image", ends, `\Temp\x.exe`),
			[]string{`C:\TEMP\X.EXE`}, []string{`C:Temp\x.exe`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Translate(singleSelection(tc.cond), nil)
			if err != nil {
				t.Fatal(err)
			}
			toSQL(t, got)
			for _, v := range tc.match {
				if !matchBQL(t, got, v) {
					t.Errorf("%s should match %q", got, v)
				}
			}
			for _, v := range tc.miss {
				if matchBQL(t, got, v) {
					t.Errorf("%s should not match %q", got, v)
				}
			}
		})
	}
}

// The BQL and SQL each value form produces. Literal ends use the native operators,
// a value with no letters keeps the indexable equality, and only what neither can
// express becomes a regex, with (?s) so a wildcard spans newlines in a command line.
func TestTranslateWildcardForms(t *testing.T) {
	contains, ends := []string{"contains"}, []string{"endswith"}
	cases := []struct {
		name string
		cond FieldCondition
		bql  string
		sql  string
	}{
		{"leading star is a suffix match", fc("Image", nil, `*\cmd.exe`), `Image=$"\\cmd.exe"`,
			"endsWith(lower(" + imgRef + `), '\\cmd.exe')`},
		{"trailing star is a prefix match", fc("Image", nil, `C:\Windows\\*`), `Image=^"C:\\Windows\\"`,
			"startsWith(lower(" + imgRef + `), 'c:\\windows\\')`},
		{"stars on both ends is a substring match", fc("CommandLine", nil, "*mimikatz*"), `CommandLine=~mimikatz`,
			"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['mimikatz'])"},
		{"stars already implied by contains", fc("CommandLine", contains, "*mimikatz*"), `CommandLine=~mimikatz`, ""},
		{"inner star is a regex", fc("CommandLine", contains, "foo*bar"), `CommandLine=/(?s)foo.*bar/i`,
			"match(" + cmdRef + ", '(?i)(?s)foo.*bar')"},
		{"inner star in an exact value is anchored", fc("Image", nil, `C:\\*\cmd.exe`), `Image=/(?s)^C:\\.*\\cmd\.exe$/i`,
			"match(" + imgRef + `, '(?i)(?s)^C:\\\\.*\\\\cmd\\.exe$')`},
		{"question mark is one character", fc("Image", ends, `\cmd?.exe`), `Image=/(?s)\\cmd.\.exe$/i`, ""},
		{"cased exact value is a lowercase equality", fc("OriginalFileName", nil, "Cmd.Exe"), `in(lower(OriginalFileName), values=["cmd.exe"])`,
			"lower(fields.`OriginalFileName`::String) IN ('cmd.exe')"},
		{"cased exact path keeps its backslashes", fc("Image", nil, `C:\Windows\System32\cmd.exe`), `in(lower(Image), values=["c:\\windows\\system32\\cmd.exe"])`,
			"lower(" + imgRef + `) IN ('c:\\windows\\system32\\cmd.exe')`},
		{"cased exact list is one set", fc("OriginalFileName", nil, "PowerShell.EXE", "pwsh.dll"), `in(lower(OriginalFileName), values=["powershell.exe","pwsh.dll"])`,
			"lower(fields.`OriginalFileName`::String) IN ('powershell.exe', 'pwsh.dll')"},
		{"non-ASCII letters stay a regex", fc("TargetFilename", nil, "Ärger.txt"), `TargetFilename=/^Ärger\.txt$/i`, ""},
		{"caseless exact value stays equality", fc("EventID", nil, "4688"), `EventID="4688"`,
			"fields.`EventID`::String = '4688'"},
		{"escaped star stays native", fc("CommandLine", contains, `a\*b`), `CommandLine=~a*b`,
			"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['a*b'])"},
		{"escaped star in an exact value", fc("CommandLine", nil, `a\*b`), `in(lower(CommandLine), values=["a*b"])`, ""},
		{"escaped backslash", fc("Image", ends, `\\x.exe`), `Image=$"\\x.exe"`, ""},
		{"lone star is existence", fc("Image", nil, "*"), `Image=*`, ""},
		{"list consolidates per form", fc("Image", ends, `\a.exe`, `\b?.exe`, `\c.exe`, `\d*.exe`),
			`(Image=$"\\a.exe","\\c.exe" OR Image=/(?s)\\b.\.exe$|\\d.*\.exe$/i)`, ""},
		{"exact list mixes equality, lowercase equality and native", fc("Image", nil, "1", `*\a.exe`, "B.exe"),
			`(Image="1" OR in(lower(Image), values=["b.exe"]) OR Image=$"\\a.exe")`, ""},
		{"keyword wildcard", fc("", nil, "mimi*katz"), `/.*mimi.*katz.*/i`, "match(lower(norm_log), '.*mimi.*katz.*')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Translate(singleSelection(tc.cond), nil)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.bql {
				t.Fatalf("BQL\n got  %s\n want %s", got, tc.bql)
			}
			if sql := toSQL(t, got); tc.sql != "" && !strings.Contains(sql, tc.sql) {
				t.Errorf("SQL is missing %s\n%s", tc.sql, sql)
			}
		})
	}
}

// A realistic process-creation rule: every value form together, end to end.
func TestTranslateWildcardRuleEndToEnd(t *testing.T) {
	rule, err := ParseSigmaRule(`
title: Suspicious cmd child
logsource:
  category: process_creation
  product: windows
detection:
  selection:
    Image: '*\cmd.exe'
    ParentImage|endswith: '\WINWORD.EXE'
    CommandLine|contains: 'http*://'
  filter:
    CurrentDirectory: 'C:\Program Files\\*'
  condition: selection and not filter
`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Translate(rule, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, frag := range []string{
		`Image=$"\\cmd.exe"`,
		`ParentImage=$"\\WINWORD.EXE"`,
		`CommandLine=/(?s)http.*:\/\//i`,
		`CurrentDirectory=^"C:\\Program Files\\"`,
	} {
		if !strings.Contains(got, frag) {
			t.Errorf("translation is missing %s:\n%s", frag, got)
		}
	}
	sql := toSQL(t, got)
	for _, frag := range []string{
		"endsWith(lower(" + imgRef + `), '\\cmd.exe')`,
		"endsWith(lower(fields.`ParentImage`::String), '\\\\winword.exe')",
		"match(" + cmdRef + `, '(?i)(?s)http.*:\\/\\/')`,
	} {
		if !strings.Contains(sql, frag) {
			t.Errorf("SQL is missing %s\n%s", frag, sql)
		}
	}
}

// |re|i, |re|m and |re|s set RE2 flags on the author's pattern: i as BQL's /i, m and
// s inline so they cover every alternative of a consolidated list.
func TestTranslateRegexFlags(t *testing.T) {
	cases := []struct {
		name  string
		cond  FieldCondition
		bql   string
		sql   string
		match []string
		miss  []string
	}{
		{"re is case-sensitive", fc("CommandLine", []string{"re"}, `Invoke-\w+`), `CommandLine=/Invoke-\w+/`,
			"match(" + cmdRef + ", 'Invoke-\\\\w+')", []string{"Invoke-Expr"}, []string{"invoke-expr"}},
		{"re|i", fc("CommandLine", []string{"re", "i"}, `Invoke-\w+`), `CommandLine=/Invoke-\w+/i`,
			"match(" + cmdRef + ", '(?i)Invoke-\\\\w+')", []string{"invoke-expr", "INVOKE-X"}, []string{"Invoke-"}},
		{"re|m", fc("CommandLine", []string{"re", "m"}, `^whoami$`), `CommandLine=/(?m)^whoami$/`,
			"", []string{"cd x\nwhoami\nexit"}, []string{"whoami /all", "WHOAMI"}},
		{"re|s", fc("CommandLine", []string{"re", "s"}, `a.b`), `CommandLine=/(?s)a.b/`,
			"", []string{"a\nb"}, []string{"ab"}},
		{"re|i|m|s", fc("CommandLine", []string{"re", "s", "i", "m"}, `^A.B$`), `CommandLine=/(?ms)^A.B$/i`,
			"match(" + cmdRef + ", '(?i)(?ms)^A.B$')", []string{"x\na\nb\ny"}, []string{"a\n\nb"}},
		{"re|i list", fc("CommandLine", []string{"re", "i"}, `^a`, `b$`), `CommandLine=/(?:^a)|(?:b$)/i`,
			"", []string{"Ax", "xB"}, []string{"xa", "bx"}},
		{"re|m list covers every alternative", fc("CommandLine", []string{"re", "m"}, `^a$`, `^b$`), `CommandLine=/(?m)(?:^a$)|(?:^b$)/`,
			"", []string{"x\na", "b\nx"}, []string{"ab", "A"}},
		{"re|i|all", fc("CommandLine", []string{"re", "i", "all"}, `a`, `b`), `(CommandLine=/a/i AND CommandLine=/b/i)`,
			"", []string{"AB"}, []string{"A"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Translate(singleSelection(tc.cond), nil)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.bql {
				t.Fatalf("BQL\n got  %s\n want %s", got, tc.bql)
			}
			if sql := toSQL(t, got); tc.sql != "" && !strings.Contains(sql, tc.sql) {
				t.Errorf("SQL is missing %s\n%s", tc.sql, sql)
			}
			for _, v := range tc.match {
				if !matchBQL(t, got, v) {
					t.Errorf("%s should match %q", got, v)
				}
			}
			for _, v := range tc.miss {
				if matchBQL(t, got, v) {
					t.Errorf("%s should not match %q", got, v)
				}
			}
		})
	}
}

// A '/' in an author's regex would end the BQL regex literal early, failing the rule.
// It is escaped, and the pattern still matches what it did.
func TestTranslateRegexWithSlash(t *testing.T) {
	cases := []struct {
		name  string
		cond  FieldCondition
		bql   string
		match []string
		miss  []string
	}{
		{"url", fc("CommandLine", []string{"re"}, `https?://[^/]+/raw`), `CommandLine=/https?:\/\/[^\/]+\/raw/`,
			[]string{"curl https://x.example/raw"}, []string{"https://x/y/raw", "https:/x/raw"}},
		{"already escaped slash", fc("CommandLine", []string{"re"}, `a\/b`), `CommandLine=/a\/b/`,
			[]string{"a/b"}, []string{"ab"}},
		{"escaped backslash then slash", fc("Path", []string{"re"}, `a\\/b`), `Path=/a\\\/b/`,
			[]string{`a\/b`}, []string{"a/b"}},
		{"list", fc("CommandLine", []string{"re"}, `/tmp/`, `/dev/shm`), `CommandLine=/(?:\/tmp\/)|(?:\/dev\/shm)/`,
			[]string{"cd /dev/shm", "/tmp/x"}, []string{"tmp"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Translate(singleSelection(tc.cond), nil)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.bql {
				t.Fatalf("BQL\n got  %s\n want %s", got, tc.bql)
			}
			toSQL(t, got)
			for _, v := range tc.match {
				if !matchBQL(t, got, v) {
					t.Errorf("%s should match %q", got, v)
				}
			}
			for _, v := range tc.miss {
				if matchBQL(t, got, v) {
					t.Errorf("%s should not match %q", got, v)
				}
			}
		})
	}
}

// The same rule always translates to the same query: fields within a selection and
// selections under a quantifier come out sorted by name, not in Go's random map order.
func TestTranslateIsDeterministic(t *testing.T) {
	const yaml = `
title: Determinism
detection:
  sel_d:
    Image|endswith: '\\d.exe'
  sel_b:
    Image|endswith: '\\b.exe'
  sel_c:
    Image|endswith: '\\c.exe'
  sel_a:
    Image|endswith: '\\a.exe'
  filter:
    User: x
    EventID: 1
    Computer: y
    ParentImage: z
    CommandLine|contains: w
  condition: 1 of sel_* and not filter or all of them
`
	var first string
	for i := 0; i < 50; i++ {
		rule, err := ParseSigmaRule(yaml)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Translate(rule, nil)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("translation changed between runs:\n%s\n%s", first, got)
		}
	}
	for _, frag := range []string{
		`(Image=$"\\a.exe" OR Image=$"\\b.exe" OR Image=$"\\c.exe" OR Image=$"\\d.exe")`,
		`(CommandLine=~w AND in(lower(Computer), values=["y"]) AND EventID="1" AND in(lower(ParentImage), values=["z"]) AND in(lower(User), values=["x"]))`,
	} {
		if !strings.Contains(first, frag) {
			t.Errorf("translation is missing %s:\n%s", frag, first)
		}
	}
}
