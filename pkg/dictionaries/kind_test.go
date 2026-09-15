package dictionaries

import (
	"strings"
	"testing"
)

// A kind decides the ClickHouse layout. An unknown one must never fall back to a
// layout the author did not ask for: every lookup would miss and nothing would
// say why.
func TestKindValidation(t *testing.T) {
	for _, k := range []string{KindValue, KindNetwork, KindPattern} {
		if !ValidKind(k) {
			t.Errorf("%q must be a valid kind", k)
		}
	}
	for _, k := range []string{"", "exact", "ip_trie", "NETWORK", "regexp"} {
		if ValidKind(k) {
			t.Errorf("%q must not be a valid kind", k)
		}
	}
	if NormalizeKind("") != KindValue {
		t.Errorf("a dictionary stored before kinds existed must read as a value list, got %q", NormalizeKind(""))
	}
	if NormalizeKind(KindNetwork) != KindNetwork {
		t.Error("a stated kind must survive normalisation")
	}
}

func TestDictLayoutFollowsKind(t *testing.T) {
	cases := map[string]string{
		KindValue:   "HASHED()",
		KindNetwork: "IP_TRIE()",
		"":          "HASHED()",
	}
	for kind, want := range cases {
		if got := dictLayout(kind); got != want {
			t.Errorf("dictLayout(%q) = %q, want %q", kind, got, want)
		}
	}
}

// An IP_TRIE silently drops a key it cannot parse, so the dictionary would load
// fewer rows than were saved with nothing to say which went missing.
func TestNetworkKeyValidation(t *testing.T) {
	valid := []string{"10.0.0.0/8", "192.168.1.0/24", "0.0.0.0/0", "2001:db8::/32", "::/0", " 10.0.0.0/8 "}
	for _, k := range valid {
		if err := ValidateNetworkKey(k); err != nil {
			t.Errorf("%q must be accepted: %v", k, err)
		}
	}
	invalid := []string{"", "  ", "10.0.0.0", "10.0.0.0/", "/8", "10.0.0.0/33", "1.2.3.4/40", "notanip/8", "10.0.0.0-10.0.0.255"}
	for _, k := range invalid {
		if err := ValidateNetworkKey(k); err == nil {
			t.Errorf("%q must be refused", k)
		}
	}
}

// The error names the value and the shape expected, because it is shown to
// whoever pasted the row in.
func TestNetworkKeyErrorIsSpecific(t *testing.T) {
	err := ValidateNetworkKey("10.0.0.0")
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"10.0.0.0", "prefix length"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error must mention %q, got: %v", want, err)
		}
	}
}

// A CIDR range has no casing, so lowering it would only corrupt the key.
func TestNetworkKeysAreNotLowered(t *testing.T) {
	m := &Manager{}
	dict := &Dictionary{KeyColumn: "network", Kind: KindNetwork, CaseInsensitiveKeys: true}
	cols := []DictionaryColumn{{Name: "network"}, {Name: "owner"}}
	if q := m.dictSourceQuery(dict, "network", cols); strings.Contains(q, "lower(") {
		t.Errorf("a network list must not lower its key: %s", q)
	}
	dict.Kind = KindValue
	if q := m.dictSourceQuery(dict, "network", cols); !strings.Contains(q, "lower(") {
		t.Errorf("a case-insensitive value list must lower its key: %s", q)
	}
}

func TestPatternKeyValidation(t *testing.T) {
	valid := []string{"mimikatz", "(?i)powershell", `\.exe$`, "a|b", "^svchost\\.exe$", " spaced "}
	for _, k := range valid {
		if err := ValidatePatternKey(k); err != nil {
			t.Errorf("%q must be accepted: %v", k, err)
		}
	}
	invalid := []string{"", "  ", "(unclosed", "a{2,1}", "[z-a]", "*bad"}
	for _, k := range invalid {
		if err := ValidatePatternKey(k); err == nil {
			t.Errorf("%q must be refused", k)
		}
	}
}

// Each kind's key rule is applied by kind, so one call site covers all three.
func TestValidateKeyForRoutesByKind(t *testing.T) {
	if err := ValidateKeyFor(KindValue, "(unclosed"); err != nil {
		t.Errorf("a value list takes any non-empty key: %v", err)
	}
	if err := ValidateKeyFor(KindNetwork, "(unclosed"); err == nil {
		t.Error("a network list must refuse a non-range")
	}
	if err := ValidateKeyFor(KindPattern, "(unclosed"); err == nil {
		t.Error("a pattern list must refuse an uncompilable expression")
	}
	if err := ValidateKeyFor(KindPattern, "10.0.0.0/8"); err != nil {
		t.Errorf("a CIDR range is a valid regular expression: %v", err)
	}
}

// The source query has to produce the five columns REGEXP_TREE reads, in order,
// with the id driving which pattern is tried first.
func TestPatternSourceQueryShape(t *testing.T) {
	m := &Manager{chDB: "bifract"}
	dict := &Dictionary{ID: "abc", KeyColumn: "pattern", Kind: KindPattern}
	cols := []DictionaryColumn{{Name: "pattern"}, {Name: "tool"}, {Name: "risk"}}
	q := m.primarySourceQuery(dict, cols)
	for _, want := range []string{"AS id", "AS parent_id", "AS regexp", "AS keys", "AS values", "ORDER BY", PatternMatchAttr} {
		if !strings.Contains(q, want) {
			t.Errorf("pattern source query missing %q: %s", want, q)
		}
	}
	// The marker is first in both arrays, so it lines up with its own attribute.
	if strings.Index(q, PatternMatchAttr) > strings.Index(q, "tool") {
		t.Errorf("the match marker must lead the key array: %s", q)
	}
	// Every other kind is untouched.
	dict.Kind = KindValue
	if plain := m.primarySourceQuery(dict, cols); strings.Contains(plain, "AS regexp") {
		t.Errorf("a value list must not read the regexp-tree shape: %s", plain)
	}
}

func TestPatternLayoutIsRegexpTree(t *testing.T) {
	if got := dictLayout(KindPattern); got != "REGEXP_TREE" {
		t.Errorf("dictLayout(pattern) = %q", got)
	}
}
