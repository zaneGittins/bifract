package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

func newEndpointToggleFakes() (*ClickHouseClient, *fakedb.CH, *PostgresClient, *fakedb.PG) {
	c, f := newFakeClient()
	f.On("FROM system.detached_tables", fakedb.Strs("table", "proc_lineage_mv"))
	pg, p := newFakePostgres()
	p.On("pg_advisory_unlock", fakedb.Row(true))
	return c, f, pg, p
}

// pgSteps names each Postgres statement of the toggle, in order.
func pgSteps(p *fakedb.PG) []string {
	var out []string
	for _, call := range p.Calls() {
		switch q := call.Query; {
		case strings.Contains(q, "pg_advisory_lock"):
			out = append(out, "lock")
		case strings.Contains(q, "pg_advisory_unlock"):
			out = append(out, "unlock")
		case strings.Contains(q, "INSERT INTO settings"):
			out = append(out, "save="+call.Args[1].(string))
		case strings.Contains(q, "FROM settings"):
			out = append(out, "read")
		}
	}
	return out
}

// Two toggles (from two admins, or two pods) must not interleave their ATTACH/DETACH,
// and the saved flag must match what was applied, so the whole toggle runs under one
// cluster-wide advisory lock.
func TestSetEndpointAnalysisSerializesOnAdvisoryLock(t *testing.T) {
	c, f, pg, p := newEndpointToggleFakes()
	var stepsAtAttach []string
	f.FailExecFunc(func(string) error {
		stepsAtAttach = pgSteps(p)
		return nil
	})
	if err := SetEndpointAnalysis(context.Background(), pg, c, true); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(stepsAtAttach, ","); got != "lock" {
		t.Errorf("ATTACH ran after %q, want only the lock taken", got)
	}
	if got := strings.Join(pgSteps(p), ","); got != "lock,save=true,unlock" {
		t.Errorf("postgres steps %q", got)
	}
	if lock := p.Matching("pg_advisory_lock"); lock[0].Args[0] != LockEndpointAnalysis {
		t.Errorf("lock id %v", lock[0].Args[0])
	}
}

// A failed reconcile leaves the saved flag alone and still releases the lock.
func TestSetEndpointAnalysisFailureKeepsSetting(t *testing.T) {
	c, f, pg, p := newEndpointToggleFakes()
	f.FailExec("ATTACH", errors.New("boom"))
	if err := SetEndpointAnalysis(context.Background(), pg, c, true); err == nil {
		t.Fatal("expected the ATTACH failure")
	}
	if got := strings.Join(pgSteps(p), ","); got != "lock,unlock" {
		t.Errorf("postgres steps %q", got)
	}
}

// Startup reads the flag under the lock, so a pod starting mid-toggle applies the
// toggle's result instead of undoing it with a value read before.
func TestSyncEndpointAnalysisReadsSettingUnderLock(t *testing.T) {
	c, f, pg, p := newEndpointToggleFakes()
	p.On("FROM settings", fakedb.Row("true"))
	if err := SyncEndpointAnalysis(context.Background(), pg, c); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(pgSteps(p), ","); got != "lock,read,unlock" {
		t.Errorf("postgres steps %q", got)
	}
	if got := f.Executed(); len(got) != 1 || got[0] != "ATTACH TABLE proc_lineage_mv" {
		t.Errorf("executed %q", got)
	}
}

// A peer that got there first (an older pod without the lock, say) is not an error:
// the view is already in the wanted state.
func TestReconcileEndpointAnalysisMVsToleratesPeerWins(t *testing.T) {
	c, f := newFakeClient()
	f.On("FROM system.detached_tables", fakedb.Strs("table", "proc_lineage_mv"))
	f.FailExec("ATTACH", fakedb.Exception(57, "Table logs.proc_lineage_mv already exists"))
	if err := c.ReconcileEndpointAnalysisMVs(context.Background(), true); err != nil {
		t.Errorf("ATTACH of an attached view: %v", err)
	}

	c, f = newFakeClient()
	f.On("FROM system.tables", fakedb.Strs("name", "proc_lineage_mv"))
	f.FailExec("DETACH", fakedb.Exception(60, "Table logs.proc_lineage_mv does not exist"))
	if err := c.ReconcileEndpointAnalysisMVs(context.Background(), false); err != nil {
		t.Errorf("DETACH of a detached view: %v", err)
	}
}
