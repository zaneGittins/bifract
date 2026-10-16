package alerts

import (
	"context"
	"database/sql/driver"
	"regexp"
	"strings"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

var unitFractal = regexp.MustCompile(`fractal_id = '([0-9a-f-]{36})'`)

// scratchClickHouse answers the statements a test run makes against its scratch
// table: the logs keys it clones, the visibility check over what was inserted, and
// each rule query, which "matches" a unit whose events carry status 500.
func scratchClickHouse(t *testing.T) (*storage.ClickHouseClient, *fakedb.CH) {
	t.Helper()
	client, ch := storagetest.ClickHouse()
	inserted := func() map[string][]map[string]string {
		out := map[string][]map[string]string{}
		for _, b := range ch.Batches() {
			for _, r := range b.Appended {
				out[r[3].(string)] = append(out[r[3].(string)], r[2].(map[string]string))
			}
		}
		return out
	}
	ch.Respond(func(q string, _ []any) (fakedb.CHResult, bool) {
		switch {
		case strings.Contains(q, "FROM system.tables"):
			return fakedb.CHResult{
				Cols: []fakedb.Col{{Column: "sorting_key", Type: "String"}, {Column: "partition_key", Type: "String"}},
				Rows: [][]any{{"fractal_id, timestamp", "toDate(ingest_timestamp)"}},
			}, true
		case strings.Contains(q, "count() AS c"):
			res := fakedb.CHResult{Cols: []fakedb.Col{{Column: "fractal_id", Type: "String"}, {Column: "c", Type: "UInt64"}}}
			for fid, rows := range inserted() {
				res.Rows = append(res.Rows, []any{fid, uint64(len(rows))})
			}
			return res, true
		case strings.Contains(q, "ruletest_"):
			m := unitFractal.FindStringSubmatch(q)
			res := fakedb.CHResult{Cols: []fakedb.Col{{Column: "log_id", Type: "String"}}}
			if m != nil {
				for _, fields := range inserted()[m[1]] {
					if fields["status"] == "500" {
						res.Rows = append(res.Rows, []any{"l"})
					}
				}
			}
			return res, true
		}
		return fakedb.CHResult{}, false
	})
	return client, ch
}

var runnerTests = []AlertTest{
	{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"status": "500"}}},
	{Name: "quiet", Expectation: "no_match", Events: []map[string]interface{}{{"status": "200"}}},
}

// A session loads its corpus once: editing the query re-runs against the same scratch
// table, editing the events rebuilds it and drops the old one.
func TestTestRunnerReusesTheLoadedCorpus(t *testing.T) {
	client, ch := scratchClickHouse(t)
	r := NewTestRunner(client)
	t.Cleanup(r.Close)
	ctx := context.Background()

	res, err := r.Run(ctx, "editor-1", `status=500`, runnerTests, "f1", "")
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK() || res.Passed != 2 || res.Outcomes[0].Rows != 1 || res.Outcomes[1].Matched {
		t.Fatalf("result = %+v", res)
	}
	creates := ch.ExecutedMatching("CREATE TABLE")
	if len(creates) != 1 || !strings.Contains(creates[0], "ENGINE = MergeTree PARTITION BY toDate(ingest_timestamp) ORDER BY (fractal_id, timestamp)") {
		t.Errorf("the scratch clone keeps the logs keys without replication: %v", creates)
	}

	// Same events, different query: no reload.
	if _, err := r.Run(ctx, "editor-1", `status=404`, runnerTests, "f1", ""); err != nil {
		t.Fatal(err)
	}
	if len(ch.ExecutedMatching("CREATE TABLE")) != 1 || len(ch.Batches()) != 1 {
		t.Error("an unchanged corpus was reloaded")
	}

	// Different events: rebuilt, and the previous table dropped.
	changed := []AlertTest{{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"status": "200"}}}}
	res, err = r.Run(ctx, "editor-1", `status=500`, changed, "f1", "")
	if err != nil {
		t.Fatal(err)
	}
	if res.OK() || res.Failed != 1 || res.Outcomes[0].Reason == "" {
		t.Errorf("a should-match test that matched nothing fails with a reason: %+v", res)
	}
	if len(ch.ExecutedMatching("CREATE TABLE")) != 2 || len(ch.ExecutedMatching("DROP TABLE IF EXISTS")) != 1 {
		t.Errorf("rebuild: creates %d drops %d", len(ch.ExecutedMatching("CREATE TABLE")), len(ch.ExecutedMatching("DROP TABLE")))
	}

	r.Release(ctx, "editor-1")
	if len(ch.ExecutedMatching("DROP TABLE IF EXISTS")) != 2 {
		t.Error("releasing a session drops its table")
	}
	r.Release(ctx, "editor-1")
	if len(ch.ExecutedMatching("DROP TABLE IF EXISTS")) != 2 {
		t.Error("a released session is gone")
	}
}

// RunOnce leaves nothing behind; refusals happen before any table exists.
func TestTestRunnerRunOnceAndRefusals(t *testing.T) {
	client, ch := scratchClickHouse(t)
	r := NewTestRunner(client)
	t.Cleanup(r.Close)
	ctx := context.Background()

	res, err := r.RunOnce(ctx, `status=500`, runnerTests, "f1", "")
	if err != nil || !res.OK() {
		t.Fatalf("result = %+v %v", res, err)
	}
	if len(ch.ExecutedMatching("CREATE TABLE")) != len(ch.ExecutedMatching("DROP TABLE IF EXISTS")) {
		t.Error("a one-off run must drop what it created")
	}

	if _, err := r.Run(ctx, "s", `status=(`, runnerTests, "f1", ""); err == nil || !strings.Contains(err.Error(), "invalid query syntax") {
		t.Errorf("err = %v", err)
	}
	if _, err := r.Run(ctx, "s", `a=1`, []AlertTest{{Name: "x", Expectation: "sometimes"}}, "f1", ""); err == nil {
		t.Error("an invalid test must be refused")
	}
	if res, err := r.Run(ctx, "s", `a=1`, nil, "f1", ""); err != nil || len(res.Outcomes) != 0 {
		t.Errorf("no tests is an empty run: %+v %v", res, err)
	}
	if _, err := (&TestRunner{}).Run(ctx, "s", `a=1`, runnerTests, "f1", ""); err == nil {
		t.Error("a runner without ClickHouse cannot run")
	}

	ch.FailExec("CREATE TABLE", fakedb.Exception(241, "memory limit"))
	if _, err := r.Run(ctx, "fresh", `status=500`, runnerTests, "f1", ""); err == nil || !strings.Contains(err.Error(), "creating scratch table") {
		t.Errorf("err = %v", err)
	}
}

// With a test runner wired in, a policy that reads test outcomes runs the tests on
// save, and the merge path stores the run against the proposal.
func TestPoliciesThatReadTestsRunThem(t *testing.T) {
	client, _ := scratchClickHouse(t)
	runner := NewTestRunner(client)
	t.Cleanup(runner.Close)
	ctx := context.Background()
	passing := fakedb.RowsOf([]driver.Value{"p1", "tests.all_passing", "is_true", "", "tests must pass", PolicyBlock, true, int64(0)})

	m, pg := newTestManager(t)
	m.SetTestRunner(runner)
	pg.answer(sqlPolicies, passing)
	failing := []AlertTest{{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"status": "200"}}}}
	_, err := m.CreateAlert(ctx, AlertCreateRequest{Name: "n", QueryString: `status=500`, Tests: &failing}, "alice", "f1", "")
	if err == nil || !strings.Contains(err.Error(), "tests must pass") {
		t.Fatalf("a failing corpus must block the save: %v", err)
	}

	store := newCRStore(t, pg)
	store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Tests: runnerTests, Author: "alice"}, "f1", "")
	cr, _ := m.GetChangeRequest(ctx, "cr1")
	readiness, err := m.EvaluateChangeRequest(ctx, cr, TestsIfNeeded)
	if err != nil {
		t.Fatal(err)
	}
	if readiness.Tests == nil || !readiness.Tests.OK() || readiness.Tests.RanAt == "" {
		t.Errorf("tests = %+v", readiness.Tests)
	}
	saved := pg.args("SET test_result = $2, test_result_hash = $3")
	if len(saved) != 1 || saved[0][0] != "cr1" || saved[0][2] != cr.testsHash() {
		t.Errorf("the run is stored against exactly what it ran: %v", saved)
	}
}
