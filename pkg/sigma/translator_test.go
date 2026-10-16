package sigma

import (
	"fmt"
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
		// Exact match: equality on the JSON sub-column, value intact through both escapes.
		{"exact", fc("Image", nil, "cmd.exe"), `Image="cmd.exe"`,
			[]string{imgRef + " = 'cmd.exe'"}},
		{"exact with backslash and double quote", fc("Image", nil, `C:\a "b"`), `Image="C:\\a \"b\""`,
			[]string{imgRef + ` = 'C:\\a "b"'`}},
		{"exact with single quote", fc("User", nil, `O'Brien`), `User="O'Brien"`,
			[]string{"fields.`User`::String = 'O\\'Brien'"}},
		{"exact list stays OR'd equality", fc("EventID", nil, "1", "4688"), `(EventID="1" OR EventID="4688")`,
			[]string{"fields.`EventID`::String = '1'", "fields.`EventID`::String = '4688'"}},

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
		{"contains leading asterisk", fc("CommandLine", contains, "*x"), `CommandLine=~"*x"`, nil},
		{"contains a keyword", fc("CommandLine", contains, "NOT"), `CommandLine=~"NOT"`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['NOT'])"}},
		{"digit-led value stays bare", fc("Version", starts, "10.0"), `Version=^10.0`,
			[]string{"startsWith(lower(fields.`Version`::String), '10.0')"}},
		{"startswith lowers the needle", fc("Image", starts, `C:\Windows`), `Image=^"C:\\Windows"`,
			[]string{"startsWith(lower(" + imgRef + `), 'c:\\windows')`}},
		{"endswith", fc("Image", ends, `\CMD.exe`), `Image=$"\\CMD.exe"`,
			[]string{"endsWith(lower(" + imgRef + `), '\\cmd.exe')`}},

		// A literal comma would split the native list, so those fall back to a regex
		// with the value escaped.
		{"contains with comma", fc("CommandLine", contains, "a,b.c"), `CommandLine=/.*a,b\.c.*/i`,
			[]string{"match("}},
		{"startswith with comma", fc("CommandLine", starts, "a,b"), `CommandLine=/^a,b.*/i`, nil},
		{"endswith with comma", fc("CommandLine", ends, "a,b"), `CommandLine=/.*a,b$/i`, nil},
		{"contains list with a comma value", fc("CommandLine", contains, "a,b", "c(d)"), `CommandLine=/.*(?:a,b|c\(d\)).*/i`, nil},
		{"startswith list with a comma value", fc("CommandLine", starts, "a,b", "c"), `CommandLine=/^(?:a,b|c).*/i`, nil},
		{"endswith list with a comma value", fc("CommandLine", ends, "a,b", "c"), `CommandLine=/.*(?:a,b|c)$/i`, nil},
		{"slash in a fallback value is escaped", fc("CommandLine", contains, "/tmp/x,y"), `CommandLine=/.*\/tmp\/x,y.*/i`, nil},

		// re: the user's pattern, case-sensitive as Sigma specifies (no /i).
		{"regex", fc("CommandLine", re, `foo.*bar`), `CommandLine=/foo.*bar/`,
			[]string{"match(" + cmdRef + ", 'foo.*bar')"}},

		// |all: every value must match, one predicate each.
		{"contains all", fc("CommandLine", []string{"contains", "all"}, "-nop", "-w hidden"), `(CommandLine=~"-nop" AND CommandLine=~"-w hidden")`,
			[]string{"multiSearchAnyCaseInsensitive(" + cmdRef + ", ['-nop'])", "multiSearchAnyCaseInsensitive(" + cmdRef + ", ['-w hidden'])"}},
		{"contains all with a comma value", fc("CommandLine", []string{"contains", "all"}, "a,b", "c"), `(CommandLine=/.*a,b.*/i AND CommandLine=~c)`, nil},
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
		"one_alt":  `User="root"`,
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
	names := []string{"a", "b", "c", "filter_1", "filter_2"}
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
		`OriginalFileName="PowerShell.EXE"`,
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
