//go:build archiveminio

// Manual harness for archive compaction against a real object store and a real
// SQL catalog. Needs the MinIO + Postgres pair in test/archiveminio (see the
// README there):
//
//	docker compose -p archivetest -f test/archiveminio/docker-compose.yml up -d
//	go test -tags archiveminio ./pkg/archive/ -run TestCompaction -v
//
// The bug this exists to pin: compaction committed nothing for days on a busy
// fractal. Every group lost the branch-head assertion, because the rewrite and
// the commit were retried as a unit (a multi-minute rewrite cannot outrun a
// writer committing every few minutes), and because the SQL catalog returns that
// conflict as a bare "requirement failed: ..." which does not satisfy the
// errors.Is(err, ErrCommitFailed) gate on iceberg-go's own refresh-and-replay
// retry. Only a real catalog and a real object store reproduce it.
package archive

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	archiveTestPGDSN = "host=localhost port=15433 user=bifract password=bifract dbname=bifract sslmode=disable"
)

func archiveTestObjConfig() objstore.Config {
	return objstore.Config{
		Backend:     objstore.BackendMinIO,
		S3Endpoint:  "http://localhost:19090",
		S3Region:    "us-east-1",
		S3Bucket:    "bifract-archive-test",
		S3AccessKey: "bifracttest",
		S3SecretKey: "bifracttest",
	}
}

func newArchiveTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	t.Setenv("BIFRACT_ARCHIVE_INIT_CATALOG", "true")
	obj := archiveTestObjConfig()
	ApplyBackendEnv(obj)
	cat, err := NewCatalog(context.Background(), "bifract", archiveTestPGDSN, obj)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	return cat
}

// appendBatch commits one batch to a fractal's table exactly the way the live
// archiver does (EnsureTable -> Append -> Commit), so the harness generates real
// branch-head movement rather than a simulation of it.
func appendBatch(t *testing.T, cat *Catalog, fractalID string, n int, seq int) error {
	t.Helper()
	logs := make([]storage.LogEntry, 0, n)
	now := time.Now()
	for i := range n {
		logs = append(logs, storage.LogEntry{
			LogID:      fmt.Sprintf("log-%d-%d", seq, i),
			FractalID:  fractalID,
			Timestamp:  now,
			Normalizer: "test",
			// Wide enough that a batch is a meaningful file rather than a stub.
			RawLog: fmt.Sprintf(`{"seq":%d,"i":%d,"msg":%q}`, seq, i, "compaction harness payload padding padding padding"),
			Fields: map[string]string{"seq": fmt.Sprint(seq), "i": fmt.Sprint(i), "src": "harness"},
			// IngestTimestamp drives the ingest_date partition. Backdated by a day
			// so the group lands in a SEALED partition: compactTable deliberately
			// skips today's, which is the only one the archiver may still append to.
			IngestTimestamp: now.Add(-24 * time.Hour),
		})
	}

	tbl, err := cat.EnsureTable(context.Background(), fractalID)
	if err != nil {
		return err
	}
	mem := memory.NewGoAllocator()
	rec := buildRecord(mem, logs)
	defer rec.Release()
	rdr, err := array.NewRecordReader(arrowSchema(), []arrow.RecordBatch{rec})
	if err != nil {
		return err
	}
	defer rdr.Release()

	txn := tbl.NewTransaction()
	if err := txn.Append(context.Background(), rdr, nil); err != nil {
		return err
	}
	_, err = txn.Commit(context.Background())
	return err
}

// TestCompactGroupSurvivesMovedBranchHead is the deterministic half: no race,
// no timing. The branch head is moved out from under a loaded table handle
// before compaction is asked to commit against it, and the retry must reload and
// land it.
//
// Scope, so it is not mistaken for more than it is: a single moved head is
// something the pre-fix code also survived, so this does NOT guard the livelock.
// It guards the retry path itself -- if reload-and-re-stage ever breaks outright,
// this fails immediately and without flakiness, which the timing-dependent stress
// test below cannot promise.
func TestCompactGroupSurvivesMovedBranchHead(t *testing.T) {
	cat := newArchiveTestCatalog(t)
	fractalID := fmt.Sprintf("stale-%d", time.Now().UnixNano())
	for seq := range 8 {
		if err := appendBatch(t, cat, fractalID, 2000, seq); err != nil {
			t.Fatalf("seed append %d: %v", seq, err)
		}
	}

	ctx := context.Background()
	ident := catalog.ToIdentifier(Namespace, tableName(fractalID))
	tbl, err := cat.cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}

	// Move the head. tbl is now stale in precisely the way a long rewrite leaves it.
	if err := appendBatch(t, cat, fractalID, 100, 99); err != nil {
		t.Fatalf("append to move branch head: %v", err)
	}

	opts := DefaultMaintainOptions()
	opts.ScanConcurrency = 1
	res, err := compactTable(ctx, cat, ident, tbl, opts.ByteBudget, time.Now().Add(2*time.Minute), opts)
	if err != nil {
		t.Fatalf("compactTable: %v", err)
	}
	if res.compactedBytes == 0 {
		t.Fatalf("nothing compacted against a stale table handle (%d group(s) failed): the commit retry is not reloading",
			res.failedGroups)
	}
	if res.failedGroups > 0 {
		t.Errorf("%d group(s) failed against a single moved head; the retry should absorb that outright", res.failedGroups)
	}
}

// TestCompactionCommitsUnderConcurrentAppends is the stress half. A writer
// commits continuously throughout the rewrite, which is what the old code could
// never survive: it retried the rewrite itself, so the head had always moved
// again by the time each attempt finished, forever.
//
// The cadence below is deliberately ~100x harsher than production (a real
// archiver rolls on RollBytes or RollInterval, minutes apart) but still slower
// than a single commit takes. That bound is not tuning: optimistic concurrency
// cannot converge against a writer that commits faster than the committer can,
// so a cadence under the commit duration would assert something the design does
// not promise.
func TestCompactionCommitsUnderConcurrentAppends(t *testing.T) {
	cat := newArchiveTestCatalog(t)
	fractalID := fmt.Sprintf("cmp-%d", time.Now().UnixNano())

	// Enough files (MinInputFiles is 5) and enough bytes that the rewrite takes
	// seconds, not milliseconds. That matters: the failure being pinned is a race
	// between the rewrite's duration and the writer's commit cadence, so a rewrite
	// that finishes before the writer's next commit proves nothing.
	const seedBatches = 24
	for seq := range seedBatches {
		if err := appendBatch(t, cat, fractalID, 4000, seq); err != nil {
			t.Fatalf("seed append %d: %v", seq, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	writerDone := make(chan struct{})
	writerCtx, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	go func() {
		defer close(writerDone)
		for seq := seedBatches; ; seq++ {
			select {
			case <-writerCtx.Done():
				return
			case <-time.After(time.Second):
			}
			if err := appendBatch(t, cat, fractalID, 100, seq); err != nil && writerCtx.Err() == nil {
				t.Logf("concurrent append %d: %v", seq, err)
			}
		}
	}()

	opts := DefaultMaintainOptions()
	opts.ScanConcurrency = 1
	// CommitRetries is left at the shipped default on purpose: the point is that
	// the default survives a writer far busier than any real archiver.

	ident := catalog.ToIdentifier(Namespace, tableName(fractalID))
	tbl, err := cat.cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	start := time.Now()
	res, err := compactTable(ctx, cat, ident, tbl, opts.ByteBudget, time.Now().Add(4*time.Minute), opts)
	elapsed := time.Since(start)
	stopWriter()
	<-writerDone

	if err != nil {
		t.Fatalf("compactTable returned an error: %v", err)
	}
	if res.candidateBytes == 0 {
		t.Fatalf("no compaction candidates found; the harness seeded nothing compactable")
	}
	if res.compactedBytes == 0 {
		t.Fatalf("compaction committed nothing under concurrent appends (%d group(s) failed, %d candidate bytes): the livelock is back",
			res.failedGroups, res.candidateBytes)
	}
	if res.failedGroups > 0 {
		t.Errorf("%d group(s) failed; expected the commit retry to absorb every append race", res.failedGroups)
	}
	t.Logf("compacted %d/%d candidate bytes, %d group(s) failed, took %s",
		res.compactedBytes, res.candidateBytes, res.failedGroups, elapsed.Round(time.Millisecond))
}

// TestMaintainReportsDeadline pins the status-reporting fix: a pass abandoned at
// its deadline must surface that, not return nil and be recorded as a healthy
// 'ok' run alongside a duration exactly equal to the timeout.
func TestMaintainReportsDeadline(t *testing.T) {
	cat := newArchiveTestCatalog(t)
	fractalID := fmt.Sprintf("dl-%d", time.Now().UnixNano())
	for seq := range 6 {
		if err := appendBatch(t, cat, fractalID, 500, seq); err != nil {
			t.Fatalf("seed append %d: %v", seq, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	if _, err := cat.Maintain(ctx, DefaultMaintainOptions()); err == nil {
		t.Fatal("Maintain returned nil for a pass that never had time to run; a timeout would be recorded as 'ok'")
	}
}

// snapshotCount reports how many snapshots a table currently has. One rewrite
// commit adds exactly one, which makes it the direct measure of how many commits
// a compaction pass performed.
func snapshotCount(t *testing.T, cat *Catalog, ident icetable.Identifier) int {
	t.Helper()
	tbl, err := cat.cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	return len(tbl.Metadata().Snapshots())
}

// TestCompactionBatchesGroupsIntoOneCommit is the regression test for the cost
// model. A rewrite commit walks every manifest entry in the table, so it costs
// O(files in table) no matter how little it changes; committing one group at a
// time paid that scan per group and was measured at ~5m30s each on a real 96GB
// table against ~10s to rewrite a group. Many groups, one commit.
//
// Snapshot count is the assertion because it is not a proxy: each rewrite commit
// adds exactly one snapshot, so "8 groups compacted, 1 new snapshot" is direct
// evidence the batch went in as a unit.
func TestCompactionBatchesGroupsIntoOneCommit(t *testing.T) {
	cat := newArchiveTestCatalog(t)
	fractalID := fmt.Sprintf("batch-%d", time.Now().UnixNano())

	// Enough files to plan several groups: MinInputFiles is 5 per group and
	// Analyze bin-packs to ~512MB, so this seeds multiple full groups.
	const seedBatches = 24
	for seq := range seedBatches {
		if err := appendBatch(t, cat, fractalID, 4000, seq); err != nil {
			t.Fatalf("seed append %d: %v", seq, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ident := catalog.ToIdentifier(Namespace, tableName(fractalID))
	tbl, err := cat.cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}

	plan, err := compaction.Analyze(ctx, tbl, compaction.DefaultConfig())
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	open := iceberg.Date(epochDay(time.Now()))
	var sealed int
	for _, g := range plan.Groups {
		if !isOpenPartitionGroup(g, open) {
			sealed++
		}
	}
	if sealed < 2 {
		t.Fatalf("harness seeded only %d compactable group(s); batching cannot be observed with fewer than 2", sealed)
	}

	before := snapshotCount(t, cat, ident)

	opts := DefaultMaintainOptions()
	opts.ScanConcurrency = 1
	// Budget and batch cap generous enough that every sealed group fits in one
	// batch. The cap must be explicit here: the test process has no cgroup memory
	// limit, and the unknown-limit fallback is deliberately one group.
	opts.ByteBudget = 32 << 30
	opts.MaxBatchBytes = 32 << 30

	res, err := compactTable(ctx, cat, ident, tbl, opts.ByteBudget, time.Now().Add(4*time.Minute), opts)
	if err != nil {
		t.Fatalf("compactTable: %v", err)
	}
	if res.failedGroups > 0 {
		t.Fatalf("%d group(s) failed with no concurrent writer at all", res.failedGroups)
	}
	if res.compactedBytes == 0 {
		t.Fatal("nothing compacted")
	}

	commits := snapshotCount(t, cat, ident) - before
	if commits != 1 {
		t.Errorf("compacted %d sealed group(s) in %d commit(s), want 1: the batch is not being committed as a unit",
			sealed, commits)
	}
	t.Logf("compacted %d sealed group(s)/%d bytes in %d commit(s)", sealed, res.compactedBytes, commits)
}

// TestCompactionBatchSplitsAtMemoryCap is the other half of the batching
// contract: MaxBatchBytes must actually bound what one commit carries. A
// production Docker deployment at a 4g limit OOMKilled in a loop because the
// batch grew to whatever the byte budget allowed (2.07GB -> 4.04GiB RSS), so an
// unenforced cap is not a smaller optimisation, it is the crash coming back.
func TestCompactionBatchSplitsAtMemoryCap(t *testing.T) {
	cat := newArchiveTestCatalog(t)
	fractalID := fmt.Sprintf("split-%d", time.Now().UnixNano())

	const seedBatches = 24
	for seq := range seedBatches {
		if err := appendBatch(t, cat, fractalID, 4000, seq); err != nil {
			t.Fatalf("seed append %d: %v", seq, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ident := catalog.ToIdentifier(Namespace, tableName(fractalID))
	tbl, err := cat.cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}

	plan, err := compaction.Analyze(ctx, tbl, compaction.DefaultConfig())
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	open := iceberg.Date(epochDay(time.Now()))
	var sealed int
	for _, g := range plan.Groups {
		if !isOpenPartitionGroup(g, open) {
			sealed++
		}
	}
	if sealed < 2 {
		t.Fatalf("harness seeded only %d compactable group(s); a split cannot be observed with fewer than 2", sealed)
	}

	before := snapshotCount(t, cat, ident)

	opts := DefaultMaintainOptions()
	opts.ScanConcurrency = 1
	opts.ByteBudget = 32 << 30
	// Under one full group: each batch carries exactly one group via the
	// always-attempt-the-first-group valve, so N sealed groups need N commits.
	opts.MaxBatchBytes = 100 << 20

	res, err := compactTable(ctx, cat, ident, tbl, opts.ByteBudget, time.Now().Add(4*time.Minute), opts)
	if err != nil {
		t.Fatalf("compactTable: %v", err)
	}
	if res.failedGroups > 0 {
		t.Fatalf("%d group(s) failed with no concurrent writer", res.failedGroups)
	}
	commits := snapshotCount(t, cat, ident) - before
	if commits != sealed {
		t.Errorf("compacted %d sealed group(s) in %d commit(s), want %d: MaxBatchBytes is not bounding the batch",
			sealed, commits, sealed)
	}
	t.Logf("cap %d bytes split %d group(s) into %d commit(s)", opts.MaxBatchBytes, sealed, commits)
}
