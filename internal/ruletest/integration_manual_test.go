//go:build ruletestch

// Package-level integration test for the full rule-testing path against a real
// ClickHouse. Excluded from `go test ./...` by the build tag; CI vets it so it
// cannot rot silently.
//
//	BIFRACT_TEST_CLICKHOUSE=localhost:9000 go test -tags ruletestch ./internal/ruletest/ -v
//
// Any ClickHouse works, including a blank one: the tester provisions the schema.
package ruletest

import (
	"context"
	"os"
	"testing"
)

func backendForTest(t *testing.T) *Backend {
	t.Helper()
	addr := os.Getenv("BIFRACT_TEST_CLICKHOUSE")
	if addr == "" {
		t.Skip("set BIFRACT_TEST_CLICKHOUSE=host:port to run")
	}

	user := envOr("BIFRACT_TEST_CH_USER", "default")
	pass := envOr("BIFRACT_TEST_CH_PASSWORD", "bifract")

	target, err := ParseTarget(addr, user, pass)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Connect(context.Background(), &target, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close(context.Background()) })
	return b
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// The shipped examples must pass. They cover a Sigma rule (including its
// logsource category prefilter) and a native BQL threshold rule whose negative
// cases can only be judged by actually aggregating.
func TestExampleDetectionsPass(t *testing.T) {
	b := backendForTest(t)

	paths, err := DiscoverSpecs([]string{"../../example-detections"})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no example specs found")
	}

	loads := make([]SpecLoad, 0, len(paths))
	for _, p := range paths {
		spec, err := LoadSpec(p)
		loads = append(loads, SpecLoad{Path: p, Spec: spec, Err: err})
	}

	summary, err := Run(context.Background(), b, loads, Options{})
	if err != nil {
		t.Fatal(err)
	}

	for _, sr := range summary.Specs {
		if sr.Error != "" {
			t.Errorf("%s: %s", sr.Path, sr.Error)
		}
		for _, c := range sr.Results {
			if !c.Passed {
				t.Errorf("%s / %s: %s", sr.Rule, c.Case, c.Reason)
			}
		}
	}
	if summary.Passed == 0 {
		t.Fatal("no cases ran")
	}
	t.Logf("%d passed, %d failed, %d errored", summary.Passed, summary.Failed, summary.Errored)
}

// A wrong expectation must fail. Without this the suite could pass simply because
// every query returns nothing, which is the failure mode a detection gate can least
// afford. Run() itself guards against that with a visibility check, and this asserts
// the comparison logic end to end.
func TestWrongExpectationFails(t *testing.T) {
	b := backendForTest(t)

	spec := &Spec{
		Rule: "../../example-detections/certutil-download/rule.yml",
		Path: "synthetic",
		Cases: []Case{{
			Name:        "benign certutil must not match, but we assert it does",
			Expect:      "match",
			expectation: ExpectMatch,
			logs: []map[string]interface{}{{
				"EventID":     float64(1),
				"Image":       `C:\Windows\System32\certutil.exe`,
				"CommandLine": "certutil.exe -encode a.bin a.b64",
			}},
		}},
	}

	opts := Options{NormalizerPath: "../../example-normalizers/sysmon/sysmon-normalizer.yaml"}
	summary, err := Run(context.Background(), b, []SpecLoad{{Path: spec.Path, Spec: spec}}, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Check for a spec error first. Without this, a broken rule path reports zero
	// failures and the assertion below misreads that as the comparison being wrong.
	for _, sr := range summary.Specs {
		if sr.Error != "" {
			t.Fatalf("spec did not load: %s", sr.Error)
		}
	}
	if summary.Failed != 1 {
		t.Fatalf("expected exactly 1 failure, got %d failed / %d passed", summary.Failed, summary.Passed)
	}
	if summary.OK() {
		t.Fatal("summary reported OK for a run containing a failure")
	}
}

// The scratch table must be gone after Close, and the real logs table untouched.
func TestScratchTableIsDropped(t *testing.T) {
	addr := os.Getenv("BIFRACT_TEST_CLICKHOUSE")
	if addr == "" {
		t.Skip("set BIFRACT_TEST_CLICKHOUSE=host:port to run")
	}
	target, err := ParseTarget(addr, envOr("BIFRACT_TEST_CH_USER", "default"), envOr("BIFRACT_TEST_CH_PASSWORD", "bifract"))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	b, err := Connect(ctx, &target, false)
	if err != nil {
		t.Fatal(err)
	}
	scratch := b.Scratch.Table()
	probe := b.Client

	rows, err := probe.Query(ctx, "SELECT count() AS c FROM system.tables WHERE database='logs' AND name='"+scratch+"'")
	if err != nil {
		t.Fatal(err)
	}
	if toUint64(rows[0]["c"]) != 1 {
		t.Fatalf("scratch table %s was not created", scratch)
	}

	b.Close(ctx)

	verify, err := Connect(ctx, &target, false)
	if err != nil {
		t.Fatal(err)
	}
	defer verify.Close(ctx)

	rows, err = verify.Client.Query(ctx, "SELECT count() AS c FROM system.tables WHERE database='logs' AND name='"+scratch+"'")
	if err != nil {
		t.Fatal(err)
	}
	if toUint64(rows[0]["c"]) != 0 {
		t.Errorf("scratch table %s survived Close", scratch)
	}
}
