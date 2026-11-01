package sigma

import (
	"strings"
	"testing"
)

func translateYAML(t *testing.T, detection string) (string, error) {
	t.Helper()
	rule, err := ParseSigmaRule("title: t\nlogsource:\n  category: process_creation\ndetection:\n" + detection)
	if err != nil {
		return "", err
	}
	return Translate(rule, nil)
}

// null in a value list means "or the field is absent", alongside the other values.
func TestNullInValueList(t *testing.T) {
	got, err := translateYAML(t, "  sel:\n    Image|endswith: ['\\a.exe', null]\n  condition: sel\n")
	if err != nil {
		t.Fatal(err)
	}
	want := `bifract_category=process_creation AND ((Image=$"\\a.exe" OR NOT Image=*))`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	toSQL(t, got)
}

func TestNullInValueListWithAll(t *testing.T) {
	got, err := translateYAML(t, "  sel:\n    Image|all: [~]\n  condition: sel\n")
	if err != nil {
		t.Fatal(err)
	}
	if want := `bifract_category=process_creation AND (NOT Image=*)`; got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestNullKeywordIsRefused(t *testing.T) {
	for _, det := range []string{
		"  kw:\n    - a\n    - null\n  condition: kw\n",
		"  kw: null\n  condition: kw\n",
	} {
		if _, err := translateYAML(t, det); err == nil || !strings.Contains(err.Error(), "null") {
			t.Errorf("%q: want a null keyword error, got %v", det, err)
		}
	}
}

func TestNullWithStandaloneModifierIsRefused(t *testing.T) {
	for _, det := range []string{"Port|gt: [5, null]", "Port|exists: null"} {
		_, err := translateYAML(t, "  sel:\n    "+det+"\n  condition: sel\n")
		if err == nil || !strings.Contains(err.Error(), "null") {
			t.Errorf("%s: want a null error, got %v", det, err)
		}
	}
}

// Scalars keep the text the author wrote, not a Go rendering of YAML's typed value.
func TestScalarsKeepSourceText(t *testing.T) {
	rule, err := ParseSigmaRule("title: t\ndetection:\n  sel:\n    A: 2020-01-01\n    B: 1.10\n    C: [0x1F, 'null', True]\n  condition: sel\n")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{}
	for _, c := range rule.Detection.Selections["sel"].FieldConditions {
		got[c.Field] = c.Values
	}
	want := map[string][]string{"A": {"2020-01-01"}, "B": {"1.10"}, "C": {"0x1F", "null", "True"}}
	for f, w := range want {
		if strings.Join(got[f], "|") != strings.Join(w, "|") {
			t.Errorf("%s: got %q, want %q", f, got[f], w)
		}
	}
}

func TestDuplicateFieldIsRefused(t *testing.T) {
	_, err := ParseSigmaRule("title: t\ndetection:\n  sel:\n    A: x\n    A: y\n  condition: sel\n")
	if err == nil {
		t.Fatal("want a duplicate key error")
	}
}

// A selection that matches every event ('*' keyword, empty list) folds out of
// compound conditions instead of being pasted in as "(*)", which BQL rejects.
func TestMatchAllInCompoundConditions(t *testing.T) {
	const sels = "  sel:\n    EventID: 1\n  any:\n    - '*'\n"
	cases := []struct{ cond, want string }{
		{"sel and any", `bifract_category=process_creation AND (EventID="1")`},
		{"sel or any", `bifract_category=process_creation`},
		{"sel or not any", `bifract_category=process_creation AND (EventID="1")`},
		{"not not any", `bifract_category=process_creation`},
		{"all of them", `bifract_category=process_creation AND (EventID="1")`},
		{"1 of them", `bifract_category=process_creation`},
		{"(any) and sel", `bifract_category=process_creation AND (EventID="1")`},
		{"(sel) and any", `bifract_category=process_creation AND ((EventID="1"))`},
	}
	for _, tc := range cases {
		t.Run(tc.cond, func(t *testing.T) {
			got, err := translateYAML(t, sels+"  condition: "+tc.cond+"\n")
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
			toSQL(t, got)
		})
	}
}

func TestMatchAllWithoutCategory(t *testing.T) {
	rule, err := ParseSigmaRule("title: t\ndetection:\n  sel:\n    Image: a.exe\n  any: ['*']\n  condition: sel or any\n")
	if err != nil {
		t.Fatal(err)
	}
	got, err := Translate(rule, nil)
	if err != nil || got != "*" {
		t.Fatalf("got %q, %v", got, err)
	}
	toSQL(t, got)
}

// A condition that can never match is a broken rule, refused at conversion rather
// than deployed as BQL that fails or silently never fires.
func TestMatchNothingIsRefused(t *testing.T) {
	for _, cond := range []string{"not any", "sel and not any", "not (any or sel)"} {
		_, err := translateYAML(t, "  sel:\n    Image: a.exe\n  any: ['*']\n  condition: "+cond+"\n")
		if err == nil || !strings.Contains(err.Error(), "never match") {
			t.Errorf("%q: want a never-match error, got %v", cond, err)
		}
	}
}

func TestFieldNameWithLeadingDashIsRefused(t *testing.T) {
	_, err := translateYAML(t, "  sel:\n    -Image: a.exe\n  condition: sel\n")
	if err == nil || !strings.Contains(err.Error(), "valid field name") {
		t.Fatalf("want an invalid field name error, got %v", err)
	}
}
