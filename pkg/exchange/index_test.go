package exchange

import (
	"slices"
	"strings"
	"testing"
)

const validIndex = `
items:
  - id: lolrmm
    kind: lookup
    name: LOLRMM tools
    description: Remote management tools attackers abuse.
    homepage: https://lolrmm.io
    lookup:
      list_kind: pattern
      source:
        url: https://example.com/rmm_tools.csv
        key_field: InstallationPaths
        columns: [Name, Description]
        split_key: ","
        key_format: glob
        refresh_minutes: 1440
  - id: rmm_users
    kind: model
    name: RMM tools by user
    path: models/rmm_users.yaml
    requires: [lolrmm]
  - id: rare-rmm
    kind: detection
    name: Rare RMM tool for a user
    path: detections/rare-rmm.yml
    requires: [rmm_users, sysmon]
  - id: sysmon
    kind: normalizer
    name: Sysmon
    description: Maps Sysmon events onto Bifract fields.
    path: normalizers/sysmon/sysmon-normalizer.yaml
  - id: rmm-search
    kind: search
    name: RMM tools run
    path: searches/rmm.yaml
    requires: [lolrmm]
  - id: rmm
    kind: bundle
    name: RMM hunting
    items: [lolrmm, rmm_users, rare-rmm, rmm-search]
`

func TestParseIndex(t *testing.T) {
	idx, err := ParseIndex([]byte(validIndex))
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if len(idx.Items) != 6 {
		t.Fatalf("got %d items, want 6", len(idx.Items))
	}
	lookup := idx.Items[0]
	if lookup.Lookup == nil || lookup.Lookup.Source.KeyField != "InstallationPaths" || lookup.Lookup.Source.SplitKey != "," {
		t.Errorf("lookup source not parsed: %+v", lookup.Lookup)
	}
	if got := idx.Find("rmm"); got == nil || len(got.Items) != 4 {
		t.Errorf("Find(rmm) = %+v", got)
	}
	if idx.Find("missing") != nil {
		t.Error("Find returned an item for an unknown id")
	}
}

// Requirements come first, each once, so adding an item can add what it needs.
func TestInstallOrder(t *testing.T) {
	idx, err := ParseIndex([]byte(validIndex))
	if err != nil {
		t.Fatal(err)
	}
	ids := func(items []Item) []string {
		var out []string
		for _, it := range items {
			out = append(out, it.ID)
		}
		return out
	}
	if got := ids(idx.InstallOrder("rare-rmm")); !slices.Equal(got, []string{"lolrmm", "rmm_users", "sysmon", "rare-rmm"}) {
		t.Errorf("InstallOrder(rare-rmm) = %v", got)
	}
	if got := ids(idx.InstallOrder("rmm")); !slices.Equal(got, []string{"lolrmm", "rmm_users", "sysmon", "rare-rmm", "rmm-search"}) {
		t.Errorf("InstallOrder(rmm) = %v", got)
	}
}

func TestParseIndexRejects(t *testing.T) {
	cases := []struct {
		name, yaml, want string
	}{
		{"unknown kind", `items: [{id: a, kind: notebook, name: A, path: a.yaml}]`, "kind"},
		{"bad id", `items: [{id: "A B", kind: normalizer, name: A, path: a.yaml}]`, "id"},
		{"duplicate id", `items: [{id: a, kind: normalizer, name: A, path: a.yaml}, {id: a, kind: policy, name: B, path: b.yaml}]`, "duplicate"},
		{"no name", `items: [{id: a, kind: normalizer, path: a.yaml}]`, "name"},
		{"file kind without path", `items: [{id: a, kind: normalizer, name: A}]`, "path"},
		{"path escapes", `items: [{id: a, kind: normalizer, name: A, path: ../secret.yaml}]`, "path"},
		{"absolute path", `items: [{id: a, kind: normalizer, name: A, path: /etc/passwd}]`, "path"},
		{"path with scheme", `items: [{id: a, kind: normalizer, name: A, path: "https://evil.example/a.yaml"}]`, "path"},
		{"encoded dot segment", `items: [{id: a, kind: policy, name: A, path: "%2e%2e/b.yaml"}]`, "path"},
		{"query in path", `items: [{id: a, kind: policy, name: A, path: "a.yaml?x=1"}]`, "path"},
		{"fragment in path", `items: [{id: a, kind: policy, name: A, path: "a.yaml#x"}]`, "path"},
		{"dot path", `items: [{id: a, kind: policy, name: A, path: "."}]`, "path"},
		{"script homepage", `items: [{id: a, kind: policy, name: A, path: a.yaml, homepage: "javascript:alert(1)"}]`, "homepage"},
		{"unknown field", `items: [{id: a, kind: policy, name: A, path: a.yaml, colour: red}]`, "colour"},
		{"lookup without source", `items: [{id: a, kind: lookup, name: A}]`, "lookup"},
		{"lookup with path", `items: [{id: a, kind: lookup, name: A, path: a.csv, lookup: {source: {url: "https://x.example/a.csv", key_field: k}}}]`, "path"},
		{"lookup bad url", `items: [{id: a, kind: lookup, name: A, lookup: {source: {url: "file:///etc/passwd", key_field: k}}}]`, "url"},
		{"lookup id with hyphen", `items: [{id: a-b, kind: lookup, name: A, lookup: {source: {url: "https://x.example/a.csv", key_field: k}}}]`, "id"},
		{"lookup no key", `items: [{id: a, kind: lookup, name: A, lookup: {source: {url: "https://x.example/a.csv"}}}]`, "key_field"},
		{"model id with hyphen", `items: [{id: a-b, kind: model, name: A, path: a.yaml}]`, "id"},
		{"requires unknown", `items: [{id: a, kind: policy, name: A, path: a.yaml, requires: [nope]}]`, "requires"},
		{"requires itself", `items: [{id: a, kind: search, name: A, path: a.yaml, requires: [a]}]`, "requires"},
		{"requirement cycle", `items: [{id: a, kind: model, name: A, path: a.yaml, requires: [b]}, {id: b, kind: model, name: B, path: b.yaml, requires: [a]}]`, "cycle"},
		{"two normalizers", `items: [{id: n, kind: normalizer, name: N, path: n.yaml}, {id: m, kind: normalizer, name: M, path: m.yaml}, {id: d, kind: detection, name: D, path: d.yml, requires: [n, m]}]`, "normalizer"},
		{"bundle with path", `items: [{id: p, kind: policy, name: P, path: p.yaml}, {id: b, kind: bundle, name: B, path: b.yaml, items: [p]}]`, "bundle"},
		{"empty bundle", `items: [{id: b, kind: bundle, name: B}]`, "items"},
		{"bundle member unknown", `items: [{id: b, kind: bundle, name: B, items: [nope]}]`, "nope"},
		{"nested bundle", `items: [{id: p, kind: policy, name: P, path: p.yaml}, {id: a, kind: bundle, name: A, items: [p]}, {id: b, kind: bundle, name: B, items: [a]}]`, "bundle"},
		{"requires a bundle", `items: [{id: p, kind: policy, name: P, path: p.yaml}, {id: a, kind: bundle, name: A, items: [p]}, {id: q, kind: policy, name: Q, path: q.yaml, requires: [a]}]`, "bundle"},
		{"items on a non-bundle", `items: [{id: p, kind: policy, name: P, path: p.yaml}, {id: q, kind: policy, name: Q, path: q.yaml, items: [p]}]`, "items"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseIndex([]byte(c.yaml))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}
}

// An install reads the index from main, so an index written for a newer Bifract
// must still list what this one understands rather than fail as a whole.
func TestReadIndexSkipsWhatItCannotUse(t *testing.T) {
	idx, err := ReadIndex([]byte(`
version_note: added later
items:
  - {id: good, kind: policy, name: Good, path: good.yaml}
  - {id: future, kind: notebook, name: Future, path: n.yaml}
  - {id: extra, kind: policy, name: Extra, path: e.yaml, min_version: "2.0"}
  - {id: good, kind: policy, name: Duplicate, path: dup.yaml}
  - {id: needs_future, kind: model, name: Model, path: m.yaml, requires: [future]}
  - {id: needs-model, kind: search, name: Search, path: s.yaml, requires: [needs_future]}
  - {id: bad-path, kind: policy, name: Bad, path: ../x.yaml}
  - {id: kit, kind: bundle, name: Kit, items: [good, future, needs-model]}
  - {id: empty-kit, kind: bundle, name: Empty, items: [future]}
`))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, it := range idx.Items {
		ids = append(ids, it.ID)
	}
	if strings.Join(ids, ",") != "good,extra,kit" {
		t.Errorf("kept %v, want [good extra kit]", ids)
	}
	if idx.Find("good").Name != "Good" {
		t.Error("the first item with an id wins")
	}
	if got := idx.Find("kit").Items; !slices.Equal(got, []string{"good"}) {
		t.Errorf("a bundle keeps only the members still listed: %v", got)
	}
}

func TestParseSearch(t *testing.T) {
	s, err := ParseSearch([]byte("name: RMM tools run\ndescription: d\nquery: |-\n  event_id=1\n  | head(10)\ntags: [rmm]\ntime_range: 24h\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Name != "RMM tools run" || s.Query != "event_id=1\n| head(10)" || s.TimeRange != "24h" || len(s.Tags) != 1 {
		t.Errorf("unexpected search: %+v", s)
	}
	for _, bad := range []string{"name: x\n", "query: '*'\n", "name: x\nquery: '| | |'\n", "name: x\nquery: '*'\nextra: 1\n"} {
		if _, err := ParseSearch([]byte(bad)); err == nil {
			t.Errorf("ParseSearch(%q) succeeded, want an error", bad)
		}
	}
}
