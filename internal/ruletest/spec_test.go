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
	writeFile(t, dir, "tests/events.json", `{"a":1}`)
	spec := writeFile(t, dir, "tests/a.test.yaml", `
rule: ../rules/r.yml
cases:
  - name: c1
    expect: match
    logFile: events.json
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
		t.Errorf("resolved %d events, want 1", got)
	}
}

func TestLoadSpecRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"missing rule": `
cases:
  - name: c
    expect: match
    logFile: e.json
`,
		"no cases": "rule: r.yml\ncases: []\n",
		"unnamed case": `
rule: r.yml
cases:
  - expect: match
    logFile: e.json
`,
		"missing logFile": `
rule: r.yml
cases:
  - name: c
    expect: match
`,
		// Inline events were removed deliberately; a spec still using them must fail
		// loudly rather than silently run zero events.
		"inline log": `
rule: r.yml
cases:
  - name: c
    expect: match
    log: {a: 1}
`,
		"inline logs": `
rule: r.yml
cases:
  - name: c
    expect: match
    logs: [{a: 1}]
`,
		"count with no_match": `
rule: r.yml
cases:
  - name: c
    expect: no_match
    together: true
    count: 3
    logFile: e.json
`,
		"unknown field": `
rule: r.yml
cases:
  - name: c
    expect: match
    logFile: e.json
    expectt: match
`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, "e.json", `{"a":1}`)
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

// Events are decoded straight from JSON, so they reach the normalizer with exactly
// the Go types an HTTP-ingested event would have.
func TestEventsAreJSONTyped(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "e.json", `{"EventID":1,"Ratio":1.5,"Flag":true,"Name":"certutil.exe"}`)
	p := writeFile(t, dir, "x.test.yaml", `
rule: r.yml
cases:
  - name: c
    expect: match
    logFile: e.json
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

func TestDiscoverSpecs(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.test.yaml", "x")
	writeFile(t, dir, "certutil-download/rule.test.yaml", "x")
	writeFile(t, dir, "deep/nested/tree/c.test.yml", "x")
	// Not specs: the rules, the events and unrelated files must all be ignored.
	writeFile(t, dir, "certutil-download/rule.yml", "x")
	writeFile(t, dir, "certutil-download/true-positives.json", "x")
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
	for _, name := range []string{"rule.yml", "rule.yaml", "events.json", "a.tests.yaml", "readme.md"} {
		if IsSpecFile(name) {
			t.Errorf("IsSpecFile(%q) = true, want false", name)
		}
	}
}

// count describes a single query's row count, so it only means something when the
// logs are batched.
func TestCountRequiresTogether(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "e.json", "{\"a\":1}\n{\"b\":2}\n")

	p := writeFile(t, dir, "x.test.yaml", `
rule: r.yml
cases:
  - name: c
    expect: match
    count: 2
    logFile: e.json
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
    logFile: e.json
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
	if len(batched) != 1 || len(batched[0].Entries) != 3 {
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
		if len(u.Entries) != 1 {
			t.Errorf("unit %d has %d entries; want 1", i, len(u.Entries))
		}
		if u.FractalID == "" || seen[u.FractalID] {
			t.Errorf("unit %d has a missing or duplicate fractal id", i)
		}
		seen[u.FractalID] = true
		if u.Entries[0].FractalID != u.FractalID {
			t.Errorf("unit %d entry fractal %q does not match unit fractal %q",
				i, u.Entries[0].FractalID, u.FractalID)
		}
	}
}
