package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// Deleting a fractal drops its partitions in every partitioned table, after first
// recording the clear against any conversion in progress (or the conversion would
// copy the rows back). includeUnscoped also clears the default fractal's
// empty-id rows.
func TestDeleteLogsByFractalID(t *testing.T) {
	c, f := newFakeClient()
	f.OnArg(describeQuery, "logs_histogram__skip", describes("", ""))
	f.OnArg(partsQuery, "logs", fakedb.Strs("partition_id", "l1", "l2"))
	f.OnArg(partsQuery, "process_edges", fakedb.Strs("partition_id", "e1"))

	if err := c.DeleteLogsByFractalID(context.Background(), "F1", true); err != nil {
		t.Fatal(err)
	}
	execs := f.Executed()
	skip := indexOf(execs, "INSERT INTO `logs_histogram__skip`")
	drop := indexOf(execs, "ALTER TABLE `logs` DROP PARTITION ID 'l1', DROP PARTITION ID 'l2'"+dropPartitionSettings)
	if skip < 0 || drop < 0 || skip > drop {
		t.Errorf("the skip must be recorded before the live drop:\n%s", strings.Join(execs, "\n"))
	}
	if indexOf(execs, "ALTER TABLE `process_edges` DROP PARTITION ID 'e1'") < 0 {
		t.Error("derived tables must be dropped too")
	}
	// Both the fractal and the unscoped id were looked up and recorded.
	if got := f.ExecArgs()[skip]; !reflect.DeepEqual(got, []any{"F1"}) {
		t.Errorf("skip args = %v", got)
	}
	if got := f.ExecArgs()[skip+1]; !reflect.DeepEqual(got, []any{""}) {
		t.Errorf("second skip args = %v, want the unscoped id", got)
	}

	c, f = newFakeClient()
	f.On(partsQuery, fakedb.CHResult{Err: errors.New("down")})
	if err := c.DeleteLogsByFractalID(context.Background(), "F1", false); err == nil || !strings.Contains(err.Error(), "F1") {
		t.Errorf("err = %v, want the failure attributed to the fractal", err)
	}
}

// Dropping one (fractal, day) partition also drops the matching histogram
// partition, which an MV fills and which is partitioned the same way.
func TestDropLogPartition(t *testing.T) {
	c, f := newFakeClient()
	const part = "('f1','2026-07-01')"
	f.OnArg("table = 'logs_histogram'", part, fakedb.Strs("partition_id", "h1"))
	if err := c.DropLogPartition(context.Background(), part); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ALTER TABLE logs DROP PARTITION " + part + dropPartitionSettings,
		"ALTER TABLE logs_histogram DROP PARTITION ID 'h1'" + dropPartitionSettings,
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed %q, want %q", got, want)
	}

	c, f = newFakeClient()
	f.FailExec("ALTER TABLE logs DROP", errors.New("busy"))
	if err := c.DropLogPartition(context.Background(), part); err == nil {
		t.Error("expected the failure")
	}
	if len(f.ExecutedMatching("logs_histogram")) != 0 {
		t.Error("the rollup must not be dropped when the logs drop failed")
	}
}

// Quota rollover drops oldest-first, so usage must sum bytes per partition and keep
// the earliest min_time, skipping anything that is not a (fractal, day) partition.
func TestFractalPartitionUsage(t *testing.T) {
	c, f := newFakeClient()
	d1 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	f.On("sum(bytes_on_disk)", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "partition", Type: "String"}, {Column: "bytes", Type: "UInt64"}, {Column: "mn", Type: "DateTime"}},
		Rows: [][]any{
			{"('f1','2026-07-01')", uint64(100), d1},
			{"tuple()", uint64(5), d1},
			{"('it''s','2026-07-02')", uint64(7), d1.AddDate(0, 0, 1)},
		},
	})
	got, err := c.FractalPartitionUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	byPart := map[string]FractalPartition{}
	for _, p := range got {
		byPart[p.Partition] = p
	}
	if len(byPart) != 2 {
		t.Fatalf("partitions = %v, want the unparseable one skipped", got)
	}
	if p := byPart["('f1','2026-07-01')"]; p.FractalID != "f1" || p.Bytes != 100 || !p.MinTime.Equal(d1) {
		t.Errorf("f1 = %+v", p)
	}
	if p := byPart["('it''s','2026-07-02')"]; p.FractalID != "it's" {
		t.Errorf("quoted id = %+v", p)
	}
}

// Only partitions past the two-hour window are dropped, locally and per partition;
// one failed drop does not stop the rest.
func TestDropHotPartitionsOnConn(t *testing.T) {
	f := fakedb.NewCH()
	f.On("table = 'logs_hot'", fakedb.Strs("partition", "p1", "p2"))
	f.FailExec("'p1'", errors.New("busy"))
	dropHotPartitionsOnConn(context.Background(), f, "test")
	want := []string{
		"ALTER TABLE logs_hot DROP PARTITION 'p1'" + dropPartitionSettings,
		"ALTER TABLE logs_hot DROP PARTITION 'p2'" + dropPartitionSettings,
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed %q", got)
	}
	if q := f.Queried()[0]; !strings.Contains(q, "max_time < now() - INTERVAL 2 HOUR") {
		t.Errorf("query = %q", q)
	}

	c, f := newFakeClient()
	f.On("table = 'logs_hot'", fakedb.CHResult{Err: errors.New("down")})
	c.dropExpiredHotPartitions(context.Background()) // logs only, never panics
	if len(f.Executed()) != 0 {
		t.Error("nothing to drop when the listing failed")
	}
}

// Leaving the legacy tiered policy: every cold partition must move home BEFORE the
// policy switch, which ClickHouse rejects while parts sit on a disk the default
// policy lacks. A failed move must leave the policy alone so the next start retries.
func TestRevertTieredStoragePolicy(t *testing.T) {
	c, f := newFakeClient()
	f.On("SELECT storage_policy", fakedb.Strs("storage_policy", "default"))
	if err := c.RevertTieredStoragePolicy(context.Background()); err != nil || len(f.Executed()) != 0 {
		t.Fatalf("default policy: err=%v executed=%v", err, f.Executed())
	}

	c, f = newFakeClient()
	f.On("SELECT storage_policy", fakedb.Strs("storage_policy", "tiered"))
	f.On("disk_name != 'default'", fakedb.Strs("partition", "('f','2026-01-01')"))
	if err := c.RevertTieredStoragePolicy(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ALTER TABLE logs MOVE PARTITION ('f','2026-01-01') TO VOLUME 'default'",
		"ALTER TABLE logs MODIFY SETTING storage_policy = 'default'",
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed %q", got)
	}

	c, f = newFakeClient()
	f.On("SELECT storage_policy", fakedb.Strs("storage_policy", "tiered"))
	f.On("disk_name != 'default'", fakedb.Strs("partition", "p"))
	f.FailExec("MOVE PARTITION", errors.New("no space"))
	if err := c.RevertTieredStoragePolicy(context.Background()); err == nil {
		t.Fatal("expected the move failure")
	}
	if len(f.ExecutedMatching("MODIFY SETTING")) != 0 {
		t.Error("the policy must not switch while parts remain on cold")
	}

	c, f = newFakeClient()
	if err := c.RevertTieredStoragePolicy(context.Background()); err == nil {
		t.Error("an unreadable policy must be reported")
	}
}

// After a reset, orphaned Keeper replica entries keep each table's path alive and
// re-provisioning fails with code 342. Every dropped replicated table's replicas
// are dropped by path; an absent path is the healthy case.
func TestResetLogDataCleansKeeperPaths(t *testing.T) {
	c, f := newFakeClient()
	c.topo.ReplicatedEngines = true
	f.On(ShardMacroQuery, fakedb.Strs("substitution", "01"))
	f.On("path = '/clickhouse/tables/01/logs/logs/replicas'", fakedb.Strs("name", "chi-0", ""))
	f.On("system.zookeeper", fakedb.CHResult{Err: errors.New("no node")})

	if err := c.ResetLogData(context.Background(), nil, []string{"model_x"}); err != nil {
		t.Fatal(err)
	}
	stmts := ResetLogDataStatements(nil, []string{"model_x"})
	execs := f.Executed()
	if !reflect.DeepEqual(execs[:len(stmts)], stmts) {
		t.Error("the reset must run ResetLogDataStatements in order")
	}
	if got := execs[len(stmts):]; !reflect.DeepEqual(got, []string{"SYSTEM DROP REPLICA 'chi-0' FROM ZKPATH '/clickhouse/tables/01/logs/logs'"}) {
		t.Errorf("keeper cleanup = %q", got)
	}
	var paths []string
	for _, q := range f.Queried() {
		if strings.Contains(q, "system.zookeeper") {
			paths = append(paths, q)
		}
	}
	if len(paths) != len(ResetKeeperTables([]string{"model_x"})) {
		t.Errorf("checked %d keeper paths, want one per table", len(paths))
	}

	// Without replicated engines there is nothing registered in Keeper.
	c, f = newFakeClient()
	if err := c.ResetLogData(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(f.Queried()) != 0 {
		t.Error("a non-replicated reset must not touch Keeper")
	}

	c, f = newFakeClient()
	f.FailExec("DROP VIEW", errors.New("denied"))
	if err := c.ResetLogData(context.Background(), nil, nil); err == nil {
		t.Error("a failed drop must fail the reset")
	}
}

func TestKeeperHelpersEscape(t *testing.T) {
	if got := KeeperTablePath("01", "logs", "t"); got != "/clickhouse/tables/01/logs/t" {
		t.Errorf("path = %q", got)
	}
	if got := KeeperReplicasQuery("/a'b"); got != "SELECT name FROM system.zookeeper WHERE path = '/a''b/replicas'" {
		t.Errorf("query = %q", got)
	}
	if got := DropKeeperReplicaStatement("r'1", "/p"); got != "SYSTEM DROP REPLICA 'r''1' FROM ZKPATH '/p'" {
		t.Errorf("stmt = %q", got)
	}
	if got := ResetKeeperTables([]string{"m"}); got[0] != "m" || len(got) != len(resetShardedTables)+1 {
		t.Errorf("tables = %v", got)
	}
	if got := ModelCHDataTables("a-b"); !reflect.DeepEqual(got, []string{"model_a_b", "model_state_a_b"}) {
		t.Errorf("data tables = %v", got)
	}
}

func TestClearAllLogDataTruncatesEverything(t *testing.T) {
	c, f := newFakeClient()
	if err := c.ClearAllLogData(context.Background(), []string{"m-1"}); err != nil {
		t.Fatal(err)
	}
	execs := strings.Join(f.Executed(), "\n")
	for _, table := range append(withConversionTables(resetShardedTables), "model_m_1", "model_state_m_1") {
		if !strings.Contains(execs, "TRUNCATE TABLE IF EXISTS `"+table+"` SETTINGS max_table_size_to_drop = 0") {
			t.Errorf("%s not truncated", table)
		}
	}

	c, f = newFakeClient()
	if err := c.ClearEndpointAnalysisBaselines(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(f.Executed(), "\n"), "`logs`") {
		t.Error("clearing baselines must not touch the logs")
	}
	f.FailExec("TRUNCATE", errors.New("x"))
	if err := c.ClearEndpointAnalysisBaselines(context.Background()); err == nil {
		t.Error("expected the truncate failure")
	}
}

// The toggle only ATTACHes MVs that exist detached (never one that was never
// created) and DETACHes PERMANENTLY only those attached.
func TestReconcileEndpointAnalysisMVs(t *testing.T) {
	setup := func() (*ClickHouseClient, *fakedb.CH) {
		c, f := newFakeClient()
		f.On("FROM system.tables", fakedb.Strs("name", "proc_lineage_mv", "process_edges_mv"))
		f.On("FROM system.detached_tables", fakedb.Strs("table", "proc_freq_dns_mv"))
		return c, f
	}

	c, f := setup()
	if err := c.ReconcileEndpointAnalysisMVs(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if got := f.Executed(); !reflect.DeepEqual(got, []string{"ATTACH TABLE proc_freq_dns_mv"}) {
		t.Errorf("enable executed %q", got)
	}
	for _, q := range f.Queried() {
		if !strings.Contains(q, endpointMVInList()) {
			t.Errorf("state query %q must derive from the gated MV list", q)
		}
	}

	c, f = setup()
	if err := c.ReconcileEndpointAnalysisMVs(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	want := []string{"DETACH TABLE proc_lineage_mv PERMANENTLY", "DETACH TABLE process_edges_mv PERMANENTLY"}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("disable executed %q", got)
	}

	c, f = setup()
	f.FailExec("DETACH", errors.New("busy"))
	if err := c.ReconcileEndpointAnalysisMVs(context.Background(), false); err == nil {
		t.Error("expected the DETACH failure")
	}
	c, f = newFakeClient()
	f.On("FROM system.detached_tables", fakedb.CHResult{Err: errors.New("denied")})
	if err := c.ReconcileEndpointAnalysisMVs(context.Background(), true); err == nil {
		t.Error("expected the state read failure")
	}
}

// Every MV must run as its definer so the INSERT-only ingest user needs no read on
// logs. One failure must not stop the others being converted: a single missed view
// fails every insert.
func TestReconcileMaterializedViewSecurity(t *testing.T) {
	c, f := newFakeClient()
	f.On("engine = 'MaterializedView'", fakedb.Strs("name", "mv_a", "mv_b", "mv_c"))
	f.FailExec("`mv_b`", fakedb.Exception(497, "denied"))

	err := c.ReconcileMaterializedViewSecurity(context.Background())
	if err == nil || !strings.Contains(err.Error(), "mv_b") {
		t.Fatalf("err = %v, want mv_b named", err)
	}
	if got := f.Executed(); len(got) != 3 || got[2] != "ALTER TABLE `mv_c` MODIFY SQL SECURITY DEFINER DEFINER = default" {
		t.Errorf("executed %q: every view must be attempted", got)
	}
	if c.Capabilities()[CapMVSecurityDefiner].State != CapUnavailable {
		t.Error("a permanent refusal must mark the capability unavailable")
	}

	c, f = newFakeClient()
	if err := c.ReconcileMaterializedViewSecurity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Capabilities()[CapMVSecurityDefiner].State != CapAvailable {
		t.Error("success must mark the capability available")
	}
}

func TestEnsureIngestUser(t *testing.T) {
	c, f := newFakeClient()
	if err := c.EnsureIngestUser(context.Background(), ""); err != nil || len(f.Executed()) != 0 {
		t.Fatalf("empty password must be a no-op: %v %v", err, f.Executed())
	}

	c, f = newFakeClient()
	c.topo.DDLCluster = "c1"
	if err := c.EnsureIngestUser(context.Background(), `p'w\`); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`CREATE USER IF NOT EXISTS bifract_ingest ON CLUSTER 'c1' IDENTIFIED BY 'p\'w\\'`,
		`ALTER USER bifract_ingest ON CLUSTER 'c1' IDENTIFIED BY 'p\'w\\'`,
		"GRANT ON CLUSTER 'c1' INSERT ON logs.* TO bifract_ingest",
		"GRANT ON CLUSTER 'c1' SELECT ON system.* TO bifract_ingest",
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed:\n%s", strings.Join(got, "\n"))
	}
	for _, s := range f.Executed() {
		if strings.Contains(s, "REMOTE") || strings.Contains(s, "SELECT ON logs") {
			t.Errorf("%q: the ingest user must never be able to read logs", s)
		}
	}

	c, f = newFakeClient()
	f.FailExec("GRANT", fakedb.Exception(497, "denied"))
	if err := c.EnsureIngestUser(context.Background(), "pw"); err == nil {
		t.Error("expected the grant failure")
	}
	if c.Capabilities()[CapIngestIdentity].State != CapUnavailable {
		t.Error("the refusal must be reported, not only logged")
	}
}

// Only names that are provably stranded upgrade copies are dropped; the SQL
// pre-filter is not trusted on its own.
func TestDropOrphanedSystemLogTables(t *testing.T) {
	c, f := newFakeClient()
	f.On("database = 'system'", fakedb.Strs("name", "text_log_0", "query_log_12", "text_log", "evil`_log_1", "x_log_1; DROP"))
	f.FailExec("query_log_12", errors.New("busy"))
	if err := c.DropOrphanedSystemLogTables(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"DROP TABLE IF EXISTS system.`text_log_0` SYNC SETTINGS max_table_size_to_drop = 0",
		"DROP TABLE IF EXISTS system.`query_log_12` SYNC SETTINGS max_table_size_to_drop = 0",
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed %q", got)
	}

	c, f = newFakeClient()
	f.On("database = 'system'", fakedb.CHResult{Err: errors.New("denied")})
	if err := c.DropOrphanedSystemLogTables(context.Background()); err == nil {
		t.Error("expected the listing failure")
	}
}

func TestKillQueryEscapesAndTargetsCluster(t *testing.T) {
	c, f := newFakeClient()
	c.topo.DDLCluster = "c1"
	c.KillQuery(context.Background(), "")
	c.KillQueryPrefix(context.Background(), "")
	if len(f.Executed()) != 0 {
		t.Fatal("an empty id must kill nothing (it would match every query)")
	}
	c.KillQuery(context.Background(), "q'1")
	c.KillQueryPrefix(context.Background(), `p\`)
	want := []string{
		`KILL QUERY ON CLUSTER 'c1' WHERE query_id = 'q\'1' OR initial_query_id = 'q\'1' ASYNC`,
		`KILL QUERY ON CLUSTER 'c1' WHERE startsWith(query_id, 'p\\') OR startsWith(initial_query_id, 'p\\') ASYNC`,
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed %q", got)
	}
}

// Retention changes are metadata-only: a mutation here would rewrite every part
// on every startup the variable is set.
func TestReconcileTTLsAreMetadataOnly(t *testing.T) {
	c, f := newFakeClient()
	c.ReconcileProcLineageTTL(context.Background(), 0)
	c.ReconcileProcFreqTTL(context.Background(), -1)
	if len(f.Executed()) != 0 {
		t.Fatal("a non-positive retention leaves the DDL default in place")
	}
	c.ReconcileProcLineageTTL(context.Background(), 400)
	c.ReconcileProcFreqTTL(context.Background(), 30)
	want := []string{
		"ALTER TABLE proc_lineage MODIFY TTL toDateTime(ingest_timestamp) + INTERVAL 400 DAY SETTINGS materialize_ttl_after_modify = 0",
		"ALTER TABLE proc_freq MODIFY TTL day + INTERVAL 30 DAY SETTINGS materialize_ttl_after_modify = 0",
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed %q", got)
	}
}

func TestCheckVersionFloor(t *testing.T) {
	c, f := newFakeClient()
	f.On("SELECT version()", fakedb.Strs("version()", "26.6.2.81"))
	v, ok, err := c.CheckVersionFloor(context.Background())
	if err != nil || !ok || v.Major != 26 || v.Minor != 6 {
		t.Errorf("26.6 = %+v %v %v", v, ok, err)
	}

	c, f = newFakeClient()
	f.On("SELECT version()", fakedb.Strs("version()", "23.8.1"))
	if _, ok, err := c.CheckVersionFloor(context.Background()); ok || err != nil {
		t.Errorf("23.8 ok=%v err=%v, want below the floor", ok, err)
	}

	// Introspection failures never fail closed: boot must not block on them.
	c, _ = newFakeClient()
	if _, ok, err := c.CheckVersionFloor(context.Background()); !ok || err == nil {
		t.Errorf("unreadable version ok=%v err=%v", ok, err)
	}
	c, f = newFakeClient()
	f.On("SELECT version()", fakedb.Strs("version()", "garbage"))
	if _, ok, err := c.CheckVersionFloor(context.Background()); !ok || err == nil {
		t.Errorf("unparseable version ok=%v err=%v", ok, err)
	}
}

func TestProbeCapabilities(t *testing.T) {
	metrics := func(rows ...[]any) fakedb.CHResult {
		return fakedb.CHResult{Cols: []fakedb.Col{{Column: "metric", Type: "String"}, {Column: "value", Type: "Float64"}}, Rows: rows}
	}
	disks := func(total, free uint64) fakedb.CHResult {
		return fakedb.CHResult{Cols: []fakedb.Col{{Column: "total_space", Type: "UInt64"}, {Column: "free_space", Type: "UInt64"}}, Rows: [][]any{{total, free}}}
	}

	c, f := newFakeClient()
	f.On("asynchronous_metrics", metrics([]any{"OSUserTime", 1.0}, []any{"OSIdleTime", 3.0}))
	f.On("system.disks", disks(100, 50))
	c.ProbeCapabilities(context.Background())
	caps := c.Capabilities()
	if caps[CapHostCPUMetrics].State != CapAvailable || caps[CapHostDiskMetrics].State != CapAvailable {
		t.Errorf("caps = %+v", caps)
	}

	// Managed storage reports no local disk; a server with no CPU metrics family
	// answered successfully but the answer means "absent".
	c, f = newFakeClient()
	f.On("asynchronous_metrics", metrics())
	f.On("system.disks", disks(0, 0))
	c.ProbeCapabilities(context.Background())
	caps = c.Capabilities()
	if caps[CapHostCPUMetrics].State != CapUnavailable || caps[CapHostDiskMetrics].State != CapUnavailable {
		t.Errorf("caps = %+v", caps)
	}
}

// Inserts go to the write table and the raw copy is best-effort: rows with no raw
// text are skipped, and a logs_raw failure never fails ingestion.
func TestInsertLogs(t *testing.T) {
	c, f := newFakeClient()
	ingest := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	logs := []LogEntry{
		{LogID: "a", RawLog: "raw-a", FractalID: "F", IngestTimestamp: ingest, Normalizer: "n@1", Fields: map[string]string{"k": "v"}},
		{LogID: "b", FractalID: "F", IngestTimestamp: ingest},
	}
	if err := c.InsertLogs(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	if len(f.Batches()) != 2 {
		t.Fatalf("%d batches, want logs and logs_raw", len(f.Batches()))
	}
	main, raw := f.Batches()[0], f.Batches()[1]
	if !strings.HasPrefix(main.Query, "INSERT INTO logs (") || !main.Sent || len(main.Appended) != 2 {
		t.Errorf("logs batch = %+v", main)
	}
	if main.Appended[0][4] != ingest || main.Appended[0][5] != "n@1" {
		t.Errorf("row = %v", main.Appended[0])
	}
	if !strings.HasPrefix(raw.Query, "INSERT INTO logs_raw (") || len(raw.Appended) != 1 || raw.Appended[0][3] != "raw-a" || !raw.Sent {
		t.Errorf("raw batch = %+v", raw)
	}

	// No raw text anywhere: no logs_raw batch is prepared at all (PrepareBatch is
	// a server round trip).
	c, f = newFakeClient()
	c.InsertLogs(context.Background(), logs[1:])
	if len(f.Batches()) != 1 {
		t.Errorf("%d batches, want only the logs batch", len(f.Batches()))
	}

	// A failed raw send is swallowed: logs is the source of truth.
	c, f = newFakeClient()
	f.OnBatch = func(b *fakedb.Batch) {
		if strings.Contains(b.Query, "logs_raw") {
			b.SendErr = errors.New("logs_raw unavailable")
		}
	}
	if err := c.InsertLogs(context.Background(), logs); err != nil {
		t.Errorf("a logs_raw failure failed ingestion: %v", err)
	}

	// A failed raw append aborts that batch only.
	c, f = newFakeClient()
	f.OnBatch = func(b *fakedb.Batch) {
		if strings.Contains(b.Query, "logs_raw") {
			b.AppendErr = errors.New("bad row")
		}
	}
	if err := c.InsertLogs(context.Background(), logs); err != nil || !f.Batches()[1].Aborted {
		t.Errorf("err = %v, raw batch = %+v", err, f.Batches()[1])
	}

	c, f = newFakeClient()
	if err := c.InsertLogs(context.Background(), nil); err != nil || len(f.Batches()) != 0 {
		t.Errorf("empty insert = %v, %d batches", err, len(f.Batches()))
	}

	c, f = newFakeClient()
	f.OnBatch = func(b *fakedb.Batch) { b.SendErr = errors.New("too many parts") }
	if err := c.InsertLogsInto(context.Background(), "scratch", logs); err == nil {
		t.Error("a failed send must fail the insert")
	}
	if len(f.Batches()) != 1 || !strings.HasPrefix(f.Batches()[0].Query, "INSERT INTO scratch (") {
		t.Errorf("InsertLogsInto must write only the named table: %+v", f.Batches())
	}

	c, f = newFakeClient()
	f.PrepareErr = errors.New("down")
	if err := c.InsertLogs(context.Background(), logs); err == nil {
		t.Error("a failed logs insert must fail")
	}
}

func TestLogEntryIngestTimeNeverZero(t *testing.T) {
	if (LogEntry{}).IngestTime().IsZero() {
		t.Error("ingest_timestamp is the partition key and must never be written zero")
	}
	ts := time.Unix(100, 0)
	if got := (LogEntry{IngestTimestamp: ts}).IngestTime(); !got.Equal(ts) {
		t.Errorf("IngestTime = %v", got)
	}
}

// The spill ratio is on by default; async_insert is opt-in. Both reach every insert.
func TestConfigureInsertSettings(t *testing.T) {
	t.Setenv("BIFRACT_INSERT_GROUPBY_SPILL_RATIO", "")
	t.Setenv("BIFRACT_ASYNC_INSERT", "")
	c := &ClickHouseClient{}
	c.configureInsertSettings()
	if c.insertSettings["max_bytes_ratio_before_external_group_by"] != 0.5 || c.insertSettings["async_insert"] != nil {
		t.Errorf("defaults = %v", c.insertSettings)
	}

	t.Setenv("BIFRACT_INSERT_GROUPBY_SPILL_RATIO", "0")
	t.Setenv("BIFRACT_ASYNC_INSERT", "1")
	c = &ClickHouseClient{}
	c.configureInsertSettings()
	if _, ok := c.insertSettings["max_bytes_ratio_before_external_group_by"]; ok {
		t.Error("a zero ratio disables the spill")
	}
	if c.insertSettings["async_insert"] != 1 || c.insertSettings["wait_for_async_insert"] != 1 {
		t.Errorf("async = %v; wait_for_async_insert keeps acks meaning durably written", c.insertSettings)
	}

	t.Setenv("BIFRACT_INSERT_GROUPBY_SPILL_RATIO", "0")
	t.Setenv("BIFRACT_ASYNC_INSERT", "")
	c = &ClickHouseClient{}
	c.configureInsertSettings()
	if c.insertSettings != nil {
		t.Errorf("settings = %v, want none", c.insertSettings)
	}

	t.Setenv("BIFRACT_INSERT_GROUPBY_SPILL_RATIO", "lots")
	if got := getenvFloat("BIFRACT_INSERT_GROUPBY_SPILL_RATIO", 0.5); got != 0.5 {
		t.Errorf("unparseable = %v, want the default", got)
	}
}
