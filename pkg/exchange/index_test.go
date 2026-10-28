package exchange

import (
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
  - id: sysmon
    kind: normalizer
    name: Sysmon
    description: Maps Sysmon events onto Bifract fields.
    path: normalizers/sysmon/sysmon-normalizer.yaml
  - id: certutil-download
    kind: detection
    name: Certutil download
    description: certutil.exe fetching a remote file.
    path: detections/certutil-download/rule.yml
    normalizer: sysmon
`

func TestParseIndex(t *testing.T) {
	idx, err := ParseIndex([]byte(validIndex))
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if len(idx.Items) != 3 {
		t.Fatalf("got %d items, want 3", len(idx.Items))
	}
	lookup := idx.Items[0]
	if lookup.Lookup == nil || lookup.Lookup.Source.KeyField != "InstallationPaths" || lookup.Lookup.Source.SplitKey != "," {
		t.Errorf("lookup source not parsed: %+v", lookup.Lookup)
	}
	if got := idx.Find("certutil-download"); got == nil || got.Normalizer != "sysmon" {
		t.Errorf("Find(certutil-download) = %+v", got)
	}
	if idx.Find("missing") != nil {
		t.Error("Find returned an item for an unknown id")
	}
}

func TestParseIndexRejects(t *testing.T) {
	cases := []struct {
		name, yaml, want string
	}{
		{"unknown kind", `items: [{id: a, kind: dashboard, name: A, path: a.yaml}]`, "kind"},
		{"bad id", `items: [{id: "A B", kind: normalizer, name: A, path: a.yaml}]`, "id"},
		{"duplicate id", `items: [{id: a, kind: normalizer, name: A, path: a.yaml}, {id: a, kind: policy, name: B, path: b.yaml}]`, "duplicate"},
		{"no name", `items: [{id: a, kind: normalizer, path: a.yaml}]`, "name"},
		{"file kind without path", `items: [{id: a, kind: normalizer, name: A}]`, "path"},
		{"path escapes", `items: [{id: a, kind: normalizer, name: A, path: ../secret.yaml}]`, "path"},
		{"absolute path", `items: [{id: a, kind: normalizer, name: A, path: /etc/passwd}]`, "path"},
		{"path with scheme", `items: [{id: a, kind: normalizer, name: A, path: "https://evil.example/a.yaml"}]`, "path"},
		{"lookup without source", `items: [{id: a, kind: lookup, name: A}]`, "lookup"},
		{"lookup with path", `items: [{id: a, kind: lookup, name: A, path: a.csv, lookup: {source: {url: "https://x.example/a.csv", key_field: k}}}]`, "path"},
		{"lookup bad url", `items: [{id: a, kind: lookup, name: A, lookup: {source: {url: "file:///etc/passwd", key_field: k}}}]`, "url"},
		{"lookup id with hyphen", `items: [{id: a-b, kind: lookup, name: A, lookup: {source: {url: "https://x.example/a.csv", key_field: k}}}]`, "lookup id"},
		{"lookup no key", `items: [{id: a, kind: lookup, name: A, lookup: {source: {url: "https://x.example/a.csv"}}}]`, "key_field"},
		{"normalizer on non-detection", `items: [{id: a, kind: policy, name: A, path: a.yaml, normalizer: b}]`, "normalizer"},
		{"normalizer not in index", `items: [{id: a, kind: detection, name: A, path: a.yml, normalizer: nope}]`, "normalizer"},
		{"unknown field", `items: [{id: a, kind: policy, name: A, path: a.yaml, colour: red}]`, "colour"},
		{"encoded dot segment", `items: [{id: a, kind: policy, name: A, path: "%2e%2e/b.yaml"}]`, "path"},
		{"query in path", `items: [{id: a, kind: policy, name: A, path: "a.yaml?x=1"}]`, "path"},
		{"fragment in path", `items: [{id: a, kind: policy, name: A, path: "a.yaml#x"}]`, "path"},
		{"dot path", `items: [{id: a, kind: policy, name: A, path: "."}]`, "path"},
		{"script homepage", `items: [{id: a, kind: policy, name: A, path: a.yaml, homepage: "javascript:alert(1)"}]`, "homepage"},
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
  - {id: future, kind: dashboard, name: Future, path: d.yaml}
  - {id: extra, kind: policy, name: Extra, path: e.yaml, min_version: "2.0"}
  - {id: good, kind: policy, name: Duplicate, path: dup.yaml}
  - {id: needs-missing, kind: detection, name: Rule, path: r.yml, normalizer: future}
  - {id: bad-path, kind: policy, name: Bad, path: ../x.yaml}
`))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, it := range idx.Items {
		ids = append(ids, it.ID)
	}
	if strings.Join(ids, ",") != "good,extra" {
		t.Errorf("kept %v, want [good extra]", ids)
	}
	if idx.Find("good").Name != "Good" {
		t.Error("the first item with an id wins")
	}
}
