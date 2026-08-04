package ruletest

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseExpectation(t *testing.T) {
	match := []string{"match", "MATCH", " match ", "true", "trigger"}
	for _, s := range match {
		got, err := parseExpectation(s)
		if err != nil || got != ExpectMatch {
			t.Errorf("parseExpectation(%q) = %v, %v; want match", s, got, err)
		}
	}

	noMatch := []string{"no_match", "no-match", "nomatch", "false", "none"}
	for _, s := range noMatch {
		got, err := parseExpectation(s)
		if err != nil || got != ExpectNoMatch {
			t.Errorf("parseExpectation(%q) = %v, %v; want no_match", s, got, err)
		}
	}

	// A typo must be an error. Defaulting it to either verdict would silently turn a
	// broken assertion into a passing one.
	for _, s := range []string{"", "matches", "no match", "yes"} {
		if _, err := parseExpectation(s); err == nil {
			t.Errorf("parseExpectation(%q) succeeded; want an error", s)
		}
	}
}

func TestLoadSpecResolvesRelativePaths(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "rules/r.yml", "name: x\nqueryString: '*'\n")
	spec := writeFile(t, dir, "tests/a.test.yaml", `
rule: ../rules/r.yml
cases:
  - name: c1
    expect: match
    log: {a: 1}
`)

	s, err := LoadSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "rules", "r.yml")
	if s.Rule != want {
		t.Errorf("Rule = %q, want %q", s.Rule, want)
	}
	if len(s.Cases) != 1 || s.Cases[0].Expectation() != ExpectMatch {
		t.Fatalf("unexpected cases: %+v", s.Cases)
	}
	if got := len(s.Cases[0].ResolvedLogs()); got != 1 {
		t.Errorf("resolved %d logs, want 1", got)
	}
}

func TestLoadSpecRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"missing rule": `
cases:
  - name: c
    expect: match
    log: {a: 1}
`,
		"no cases": "rule: r.yml\ncases: []\n",
		"unnamed case": `
rule: r.yml
cases:
  - expect: match
    log: {a: 1}
`,
		"no logs": `
rule: r.yml
cases:
  - name: c
    expect: match
`,
		"two log sources": `
rule: r.yml
cases:
  - name: c
    expect: match
    log: {a: 1}
    logs: [{b: 2}]
`,
		"count with no_match": `
rule: r.yml
cases:
  - name: c
    expect: no_match
    count: 3
    log: {a: 1}
`,
		"unknown field": `
rule: r.yml
cases:
  - name: c
    expect: match
    log: {a: 1}
    expectt: match
`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeFile(t, dir, "x.test.yaml", body)
			if _, err := LoadSpec(p); err == nil {
				t.Fatal("LoadSpec succeeded; want an error")
			}
		})
	}
}

func TestParseLogFileShapes(t *testing.T) {
	tests := map[string]struct {
		body string
		want int
	}{
		"single object": {`{"a":1}`, 1},
		"json array":    {`[{"a":1},{"a":2}]`, 2},
		"ndjson":        {"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n", 3},
		"ndjson blanks": {"{\"a\":1}\n\n{\"a\":2}\n", 2},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ParseLogFile([]byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tc.want {
				t.Errorf("parsed %d logs, want %d", len(got), tc.want)
			}
		})
	}

	for _, bad := range []string{"", "   ", "not json", "{\"a\":1}\nnope\n"} {
		if _, err := ParseLogFile([]byte(bad)); err == nil {
			t.Errorf("ParseLogFile(%q) succeeded; want an error", bad)
		}
	}
}

// Inline YAML logs must reach the normalizer with the same Go types a JSON body
// would produce, or a test can disagree with production over value formatting
// rather than over the rule.
func TestInlineLogsAreJSONTyped(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "x.test.yaml", `
rule: r.yml
cases:
  - name: c
    expect: match
    log:
      EventID: 1
      Ratio: 1.5
      Flag: true
      Name: certutil.exe
`)
	s, err := LoadSpec(p)
	if err != nil {
		t.Fatal(err)
	}
	got := s.Cases[0].ResolvedLogs()[0]

	if _, ok := got["EventID"].(float64); !ok {
		t.Errorf("EventID is %T, want float64 (JSON number)", got["EventID"])
	}
	if _, ok := got["Ratio"].(float64); !ok {
		t.Errorf("Ratio is %T, want float64", got["Ratio"])
	}
	if _, ok := got["Flag"].(bool); !ok {
		t.Errorf("Flag is %T, want bool", got["Flag"])
	}
	if _, ok := got["Name"].(string); !ok {
		t.Errorf("Name is %T, want string", got["Name"])
	}
}

// Discovery is a recursive suffix match, which is what lets a repo group detections
// however it likes (folder per detection, by platform, by tactic).
func TestDiscoverSpecs(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.test.yaml", "x")
	writeFile(t, dir, "certutil-download/rule.test.yaml", "x")
	writeFile(t, dir, "deep/nested/tree/c.test.yml", "x")
	// Not specs: the rules, the events and unrelated files must all be ignored.
	writeFile(t, dir, "certutil-download/rule.yml", "x")
	writeFile(t, dir, "certutil-download/true-positives.ndjson", "x")
	writeFile(t, dir, "normalizers/sysmon.yaml", "x")
	writeFile(t, dir, "notes.md", "x")

	found, err := DiscoverSpecs([]string{dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 3 {
		t.Fatalf("found %v, want 3 spec files", found)
	}
	for _, f := range found {
		if !IsSpecFile(f) {
			t.Errorf("discovered non-spec file %q", f)
		}
	}
}

// A .yml-spelled spec must not be silently skipped: a repo on that convention would
// otherwise run zero tests and report success.
func TestIsSpecFile(t *testing.T) {
	for _, name := range []string{"a.test.yaml", "a.test.yml", "certutil-download/rule.test.yaml"} {
		if !IsSpecFile(name) {
			t.Errorf("IsSpecFile(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"rule.yml", "rule.yaml", "events.ndjson", "a.tests.yaml", "readme.md"} {
		if IsSpecFile(name) {
			t.Errorf("IsSpecFile(%q) = true, want false", name)
		}
	}
}

// count describes a single query's row count, so it only means something when the
// logs are batched.
func TestCountRequiresTogether(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "x.test.yaml", `
rule: r.yml
cases:
  - name: c
    expect: match
    count: 2
    logs: [{a: 1}, {b: 2}]
`)
	if _, err := LoadSpec(p); err == nil {
		t.Fatal("LoadSpec accepted count without together; want an error")
	}

	ok := writeFile(t, dir, "y.test.yaml", `
rule: r.yml
cases:
  - name: c
    expect: match
    together: true
    count: 2
    logs: [{a: 1}, {b: 2}]
`)
	if _, err := LoadSpec(ok); err != nil {
		t.Fatalf("LoadSpec rejected count with together: %v", err)
	}
}

// By default each log is judged on its own, so every log must meet the expectation.
// together: true collapses them into a single batched evaluation.
func TestBuildUnitsGrouping(t *testing.T) {
	logs := []map[string]interface{}{{"a": "1"}, {"a": "2"}, {"a": "3"}}

	batched, err := buildUnits(&Case{Together: true, logs: logs}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batched) != 1 || len(batched[0].entries) != 3 {
		t.Fatalf("together grouping = %d units; want 1 unit of 3 entries", len(batched))
	}

	split, err := buildUnits(&Case{logs: logs}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(split) != 3 {
		t.Fatalf("default grouping = %d units; want 3", len(split))
	}

	seen := map[string]bool{}
	for i, u := range split {
		if len(u.entries) != 1 {
			t.Errorf("unit %d has %d entries; want 1", i, len(u.entries))
		}
		if u.fractalID == "" || seen[u.fractalID] {
			t.Errorf("unit %d has a missing or duplicate fractal id", i)
		}
		seen[u.fractalID] = true
		if u.entries[0].FractalID != u.fractalID {
			t.Errorf("unit %d entry fractal %q does not match unit fractal %q",
				i, u.entries[0].FractalID, u.fractalID)
		}
	}
}
