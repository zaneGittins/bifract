package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

// unreachableAddr refuses connections immediately, so an identity's pool can be
// opened (lazily) but its probe fails without waiting on a dial timeout.
const unreachableAddr = "127.0.0.1:1"

// newWorkloadClient is a single-node client whose class identities can never be
// probed, so reconcile exercises the full DDL sequence and then falls back.
func newWorkloadClient() (*ClickHouseClient, *fakedb.CH) {
	c, f := newFakeClient()
	c.addrs = []string{unreachableAddr}
	c.Password = "admin"
	return c, f
}

func budgetRows(v string) fakedb.CHResult { return fakedb.Strs("value", v) }

// Shares resolve against the server's own memory budget, so the same percentage
// means the same thing on any node size.
func TestReconcileQueryWorkloadsResolvesMemoryShares(t *testing.T) {
	c, f := newWorkloadClient()
	f.On("max_server_memory_usage", budgetRows("1000000"))
	f.On("system.workloads", fakedb.Strs("name"))

	err := c.ReconcileQueryWorkloads(context.Background(), WorkloadLimits{
		SearchCPUPercent: 50, SearchMemoryPercent: 40,
		RecallCPUPercent: 0, RecallMemoryPercent: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.queryMemoryCap(QuerySearchWorkload); got != 400000 {
		t.Errorf("search cap = %d", got)
	}
	if got := c.queryMemoryCap(QueryRecallWorkload); got != 200000 {
		t.Errorf("recall cap = %d", got)
	}
	if !c.workloadActive(QuerySearchWorkload) || c.workloadActive(QueryRecallWorkload) {
		t.Error("only search has a CPU share, so only search is scheduled")
	}

	var workloadDDL []string
	for _, s := range f.Executed() {
		if strings.Contains(s, "WORKLOAD") || strings.Contains(s, "RESOURCE") {
			workloadDDL = append(workloadDDL, s)
		}
	}
	want := []string{
		"CREATE RESOURCE IF NOT EXISTS cpu (MASTER THREAD, WORKER THREAD)",
		"CREATE OR REPLACE WORKLOAD bifract",
		classWorkloadDDL(QuerySearchWorkload, 50, 0),
		"DROP WORKLOAD IF EXISTS bifract_recall",
		"DROP RESOURCE IF EXISTS memory",
	}
	if !reflect.DeepEqual(workloadDDL, want) {
		t.Errorf("workload DDL:\n%s\nwant:\n%s", strings.Join(workloadDDL, "\n"), strings.Join(want, "\n"))
	}
	if c.Capabilities()[CapWorkloadScheduling].State != CapAvailable {
		t.Error("a created resource answers the scheduling capability")
	}
}

// Shares that together exceed the combined ceiling are scaled down rather than
// leaving merges and ingestion without headroom.
func TestReconcileQueryWorkloadsScalesOversizedMemory(t *testing.T) {
	c, f := newWorkloadClient()
	f.On("max_server_memory_usage", budgetRows("1000"))
	if err := c.ReconcileQueryWorkloads(context.Background(), WorkloadLimits{SearchMemoryPercent: 60, RecallMemoryPercent: 40}); err != nil {
		t.Fatal(err)
	}
	if got := c.queryMemoryCap(QuerySearchWorkload) + c.queryMemoryCap(QueryRecallWorkload); got > 750 {
		t.Errorf("combined cap = %d, want at most %d%% of the budget", got, MaxCombinedQueryMemoryPercent)
	}
}

// With no CPU share anywhere the tree is torn down, children before the root, and
// nothing is tagged. Memory ceilings still apply.
func TestReconcileQueryWorkloadsAllCPUOff(t *testing.T) {
	c, f := newWorkloadClient()
	c.setActiveWorkloads(map[string]bool{QuerySearchWorkload: true})
	f.On("max_server_memory_usage", budgetRows("100"))
	if err := c.ReconcileQueryWorkloads(context.Background(), WorkloadLimits{SearchMemoryPercent: 50}); err != nil {
		t.Fatal(err)
	}
	if c.workloadActive(QuerySearchWorkload) {
		t.Error("nothing may be tagged with the tree gone")
	}
	if got := c.queryMemoryCap(QuerySearchWorkload); got != 50 {
		t.Errorf("search cap = %d, want the memory share to survive", got)
	}
	drops := f.ExecutedMatching("DROP WORKLOAD IF EXISTS")
	if !reflect.DeepEqual(drops, []string{"DROP WORKLOAD IF EXISTS bifract_search", "DROP WORKLOAD IF EXISTS bifract_recall", "DROP WORKLOAD IF EXISTS bifract"}) {
		t.Errorf("drops = %q, want children before the root", drops)
	}
	if len(f.ExecutedMatching("DROP RESOURCE IF EXISTS memory")) != 1 {
		t.Error("the reservation resource must be dropped on every reconcile")
	}
}

// A failed reconcile must stop tagging: naming a workload that does not exist
// would be worse than running unscheduled.
func TestReconcileQueryWorkloadsFailuresStopTagging(t *testing.T) {
	c, f := newWorkloadClient()
	c.setActiveWorkloads(map[string]bool{QuerySearchWorkload: true})
	f.FailExec("CREATE RESOURCE", fakedb.Exception(497, "denied"))
	if err := c.ReconcileQueryWorkloads(context.Background(), WorkloadLimits{SearchCPUPercent: 50}); err == nil {
		t.Fatal("expected the resource failure")
	}
	if c.workloadActive(QuerySearchWorkload) {
		t.Error("tagging must stop after a failed reconcile")
	}
	if c.Capabilities()[CapWorkloadScheduling].State != CapUnavailable {
		t.Error("a refused resource marks scheduling unavailable")
	}

	c, f = newWorkloadClient()
	f.FailExec("CREATE OR REPLACE WORKLOAD bifract_search", errors.New("timeout"))
	if err := c.ReconcileQueryWorkloads(context.Background(), WorkloadLimits{SearchCPUPercent: 50}); err == nil {
		t.Fatal("expected the class failure")
	}
	if c.workloadActive(QuerySearchWorkload) {
		t.Error("a class that failed to provision must not be tagged")
	}

	// An unresolvable budget clears the caps rather than keeping stale ones.
	c, f = newWorkloadClient()
	c.setQueryMemoryCaps(map[string]int64{QuerySearchWorkload: 1})
	f.On("max_server_memory_usage", fakedb.CHResult{Err: errors.New("denied")})
	f.On("asynchronous_metrics", fakedb.CHResult{Err: fakedb.Exception(497, "denied")})
	if err := c.ReconcileQueryWorkloads(context.Background(), WorkloadLimits{SearchMemoryPercent: 50}); err == nil {
		t.Fatal("expected the budget failure")
	}
	if c.queryMemoryCap(QuerySearchWorkload) != 0 {
		t.Error("stale caps survived a failed budget read")
	}
}

func TestServerMemoryBudgetFallbacks(t *testing.T) {
	metrics := func(rows ...[]any) fakedb.CHResult {
		return fakedb.CHResult{Cols: []fakedb.Col{{Column: "metric", Type: "String"}, {Column: "value", Type: "Float64"}}, Rows: rows}
	}
	cases := []struct {
		name   string
		setup  func(f *fakedb.CH)
		want   int64
		state  CapabilityState
		hasErr bool
	}{
		{"server setting", func(f *fakedb.CH) { f.On("max_server_memory_usage", budgetRows("5000")) }, 5000, CapAvailable, false},
		{"cgroup limit", func(f *fakedb.CH) {
			f.On("max_server_memory_usage", budgetRows("0"))
			f.On("asynchronous_metrics", metrics([]any{"CGroupMemoryTotal", 3000.0}, []any{"OSMemoryTotal", 9000.0}))
		}, 3000, CapAvailable, false},
		{"host memory", func(f *fakedb.CH) {
			f.On("asynchronous_metrics", metrics([]any{"OSMemoryTotal", 9000.0}))
		}, 9000, CapAvailable, false},
		{"nothing usable", func(f *fakedb.CH) {
			f.On("asynchronous_metrics", metrics())
		}, 0, CapUnavailable, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, f := newFakeClient()
			tc.setup(f)
			got, err := c.serverMemoryBudget(context.Background())
			if got != tc.want || (err != nil) != tc.hasErr {
				t.Errorf("budget = %d, %v; want %d", got, err, tc.want)
			}
			if st := c.Capabilities()[CapServerMemoryBudget].State; st != tc.state {
				t.Errorf("capability = %v, want %v", st, tc.state)
			}
		})
	}
}

// ClickHouse allows a single root workload. Ours is reused, a pre-tree layout with a
// class at the root is rebuilt, and an operator's own root is never displaced.
func TestEnsureRootWorkload(t *testing.T) {
	cases := []struct {
		root string
		want []string
	}{
		{"", []string{"CREATE OR REPLACE WORKLOAD bifract"}},
		{QueryRootWorkload, []string{"CREATE OR REPLACE WORKLOAD bifract"}},
		{QuerySearchWorkload, []string{
			"DROP WORKLOAD IF EXISTS bifract_search", "DROP WORKLOAD IF EXISTS bifract_recall", "DROP WORKLOAD IF EXISTS bifract",
			"CREATE OR REPLACE WORKLOAD bifract",
		}},
		{"ops_root", []string{"CREATE OR REPLACE WORKLOAD bifract IN ops_root"}},
	}
	for _, tc := range cases {
		c, f := newFakeClient()
		if tc.root != "" {
			f.On("system.workloads", fakedb.Strs("name", tc.root))
		}
		if err := c.ensureRootWorkload(context.Background()); err != nil {
			t.Fatalf("root %q: %v", tc.root, err)
		}
		if got := f.Executed(); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("root %q executed %q, want %q", tc.root, got, tc.want)
		}
	}

	c, f := newFakeClient()
	f.On("system.workloads", fakedb.CHResult{Err: errors.New("unknown table")})
	if err := c.ensureRootWorkload(context.Background()); err == nil {
		t.Error("an unreadable hierarchy must not be guessed at")
	}
}

func TestPercentLabel(t *testing.T) {
	if percentLabel(0) != "uncapped" || percentLabel(25) != "25%" {
		t.Errorf("labels = %q, %q", percentLabel(0), percentLabel(25))
	}
}

// The identity DDL runs in a fixed order: the user first, then any stale broad
// grant is narrowed (ClickHouse revokes hierarchically, so a revoke after the
// grants would undo them), then the scoped grants and the memory ceiling. Only
// recall may reach object storage, and nobody gets URL.
func TestEnsureQueryIdentityStatementOrder(t *testing.T) {
	c, f := newWorkloadClient()
	c.topo.DDLCluster = "c1"
	f.On("SHOW GRANTS FOR bifract_recall", fakedb.Strs("grant", "GRANT SELECT, CREATE TEMPORARY TABLE ON *.* TO bifract_recall"))

	_, err := c.ensureQueryIdentity(context.Background(), RecallCHUser, 1234)
	if err == nil || !strings.Contains(err.Error(), "probe as bifract_recall") {
		t.Fatalf("err = %v, want the probe against the unreachable node to fail", err)
	}
	pw := EscCHStr(c.queryIdentityPassword(RecallCHUser))
	want := []string{
		"CREATE USER IF NOT EXISTS bifract_recall ON CLUSTER 'c1' IDENTIFIED BY '" + pw + "'",
		"ALTER USER bifract_recall ON CLUSTER 'c1' IDENTIFIED BY '" + pw + "'",
		"REVOKE ON CLUSTER 'c1' SELECT ON *.* FROM bifract_recall",
		"GRANT ON CLUSTER 'c1' SELECT, dictGet ON `logs`.* TO bifract_recall",
		"GRANT ON CLUSTER 'c1' SELECT ON system.* TO bifract_recall",
		"ALTER USER bifract_recall ON CLUSTER 'c1' SETTINGS max_memory_usage_for_user = 1234 CONST, serialize_query_plan = 0 CONST",
		"REVOKE ON CLUSTER 'c1' SELECT ON system.query_log FROM bifract_recall",
		"REVOKE ON CLUSTER 'c1' SELECT ON system.query_thread_log FROM bifract_recall",
		"REVOKE ON CLUSTER 'c1' SELECT ON system.query_views_log FROM bifract_recall",
		"REVOKE ON CLUSTER 'c1' SELECT ON system.processes FROM bifract_recall",
		"GRANT ON CLUSTER 'c1' CREATE TEMPORARY TABLE ON *.* TO bifract_recall",
		"GRANT ON CLUSTER 'c1' REMOTE ON *.* TO bifract_recall",
		"GRANT ON CLUSTER 'c1' S3 ON *.* TO bifract_recall",
		"GRANT ON CLUSTER 'c1' AZURE ON *.* TO bifract_recall",
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// Search: uncapped replaces a previous ceiling, and gets no object-storage or
	// remote() reach.
	c, f = newWorkloadClient()
	c.ensureQueryIdentity(context.Background(), SearchCHUser, 0)
	execs := strings.Join(f.Executed(), "\n")
	if !strings.Contains(execs, "ALTER USER bifract_search SETTINGS serialize_query_plan = 0 CONST\n") {
		t.Error("an uncapped identity must clear a previous ceiling")
	}
	for _, forbidden := range []string{" S3 ", " AZURE ", " URL ", " REMOTE "} {
		if strings.Contains(execs, forbidden) {
			t.Errorf("search identity granted%s", forbidden)
		}
	}
	if len(f.ExecutedMatching("ON *.* FROM")) != 0 {
		t.Error("nothing stale to revoke on a fresh identity")
	}

	// A server without serialize_query_plan still gets the memory ceiling.
	c, f = newWorkloadClient()
	f.FailExec("serialize_query_plan", fakedb.Exception(115, "Unknown setting"))
	c.ensureQueryIdentity(context.Background(), SearchCHUser, 99)
	if len(f.ExecutedMatching("SETTINGS max_memory_usage_for_user = 99 CONST")) != 2 {
		t.Errorf("executed %q, want the ceiling retried without the unknown setting", f.ExecutedMatching("ALTER USER bifract_search SETTINGS"))
	}
	if c.Capabilities()[CapQueryIdentity].State != CapAvailable {
		t.Error("an unknown optional setting must not cost the identity")
	}
}

// An optional privilege the server lacks costs only that capability; a required
// one costs the identity.
func TestEnsureQueryIdentityFailures(t *testing.T) {
	c, f := newWorkloadClient()
	f.FailExec("GRANT S3", fakedb.Exception(508, "unknown access type"))
	f.FailExec("GRANT REMOTE", fakedb.Exception(497, "denied"))
	_, err := c.ensureQueryIdentity(context.Background(), RecallCHUser, 0)
	if err == nil || !strings.Contains(err.Error(), "probe") {
		t.Errorf("err = %v: an optional grant failure must not stop provisioning", err)
	}
	if c.Capabilities()[CapQueryIdentity].State != CapAvailable {
		t.Error("optional grant failures must not mark the identity unavailable")
	}

	c, f = newWorkloadClient()
	f.FailExec("SELECT, dictGet", fakedb.Exception(497, "denied"))
	if _, err := c.ensureQueryIdentity(context.Background(), SearchCHUser, 0); err == nil || strings.Contains(err.Error(), "probe") {
		t.Errorf("err = %v, want the required grant failure", err)
	}
	if c.Capabilities()[CapQueryIdentity].State != CapUnavailable {
		t.Error("a refused required grant marks the identity unavailable")
	}
	if len(f.ExecutedMatching("REMOTE")) != 0 {
		t.Error("optional grants must not run after a required one failed")
	}

	c, f = newWorkloadClient()
	f.FailExec("CREATE USER", fakedb.Exception(36, "password policy"))
	if _, err := c.ensureQueryIdentity(context.Background(), SearchCHUser, 0); err == nil {
		t.Error("expected the CREATE USER failure")
	}
}

// An identity whose ceiling only drifted is reused, keeping its live pool; one that
// cannot be provisioned is absent, so its class falls back to the default
// connection; and a pool that was replaced is closed only after the swap.
func TestReconcileQueryIdentities(t *testing.T) {
	c, f := newWorkloadClient()
	live := fakedb.NewCH()
	c.queryConns.Store(&map[string]*queryIdentity{QuerySearchWorkload: {conn: live, memCapBytes: 1000}})
	f.On("system.row_policies", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "short_name", Type: "String"}, {Column: "n", Type: "UInt64"}},
		Rows: [][]any{{fractalScopePolicy, uint64(len(fractalScopeTables))}, {scopeRequiredPolicy, uint64(len(fractalScopeTables))}},
	})

	c.reconcileQueryIdentities(context.Background(), map[string]int64{QuerySearchWorkload: 1050, QueryRecallWorkload: 500})

	m := *c.queryConns.Load()
	if m[QuerySearchWorkload] == nil || m[QuerySearchWorkload].conn != live {
		t.Error("a drifted ceiling must reuse the live identity")
	}
	if live.Closed() != 0 {
		t.Error("a reused pool must not be closed")
	}
	if m[QueryRecallWorkload] != nil {
		t.Error("an identity that failed its probe must not be used")
	}
	if len(f.ExecutedMatching("bifract_search")) != 0 {
		t.Errorf("no DDL may run for a reused identity: %v", f.ExecutedMatching("bifract_search"))
	}

	// A real change rebuilds; when the rebuild fails the old pool is retired.
	c.reconcileQueryIdentities(context.Background(), map[string]int64{QuerySearchWorkload: 5000})
	if live.Closed() != 1 {
		t.Errorf("replaced pool closed %d times, want 1", live.Closed())
	}
	if (*c.queryConns.Load())[QuerySearchWorkload] != nil {
		t.Error("a failed rebuild falls back to the default connection")
	}

	other := fakedb.NewCH()
	c.queryConns.Store(&map[string]*queryIdentity{QuerySearchWorkload: {conn: other}})
	c.closeQueryIdentities()
	if other.Closed() != 1 || c.queryConns.Load() != nil {
		t.Error("Close must close every class pool")
	}
}

func TestStaleGrantsForIsConservative(t *testing.T) {
	c, f := newFakeClient()
	f.On("SHOW GRANTS", fakedb.CHResult{Err: errors.New("denied")})
	if got := c.staleGrantsFor(context.Background(), "u"); got != nil {
		t.Errorf("unreadable grants = %v: guessing would revoke what is in use", got)
	}
	c, f = newFakeClient()
	f.On("SHOW GRANTS", fakedb.Strs("g", "GRANT URL ON *.* TO u", "GRANT SELECT ON logs.* TO u"))
	f.FailExec("REVOKE", errors.New("busy"))
	c.revokeStaleGrants(context.Background(), "u") // failure only logs
	if got := f.Executed(); !reflect.DeepEqual(got, []string{"REVOKE URL ON *.* FROM u"}) {
		t.Errorf("executed %q", got)
	}
}
