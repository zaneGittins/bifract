//go:build archiveminio

// Manual harness for the two archive-maintenance costs that only a real catalog
// and object store can show: how many manifests repeated appends leave behind,
// and whether compaction planning actually prunes partitions it was told to skip.
// Shares the MinIO + Postgres pair in test/archiveminio with
// compaction_manual_test.go:
//
//	docker compose -p archivetest -f test/archiveminio/docker-compose.yml up -d
//	go test -tags archiveminio ./pkg/archive/ -run TestManifest -v
package archive

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table/compaction"

	"bifract/pkg/storage"
)

// appendAt commits one batch the way the archiver does, at a chosen ingest time
// so the batch lands in a chosen (sealed) ingest_date partition.
func appendAt(t *testing.T, cat *Catalog, fractalID string, n, seq int, ingestAt time.Time) {
	t.Helper()
	logs := make([]storage.LogEntry, 0, n)
	for i := range n {
		logs = append(logs, storage.LogEntry{
			LogID:           fmt.Sprintf("log-%d-%d", seq, i),
			FractalID:       fractalID,
			Timestamp:       ingestAt,
			IngestTimestamp: ingestAt,
			Normalizer:      "test",
			RawLog:          fmt.Sprintf(`{"seq":%d,"i":%d,"msg":%q}`, seq, i, "manifest harness payload padding"),
			Fields:          map[string]string{"seq": fmt.Sprint(seq), "src": "harness"},
		})
	}
	ctx := context.Background()
	tbl, err := cat.EnsureTable(ctx, fractalID)
	if err != nil {
		t.Fatalf("ensure table: %v", err)
	}
	rec := buildRecord(memory.NewGoAllocator(), logs)
	defer rec.Release()
	rdr, err := array.NewRecordReader(arrowSchema(), []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("record reader: %v", err)
	}
	defer rdr.Release()
	txn := tbl.NewTransaction()
	if err := txn.Append(ctx, rdr, nil); err != nil {
		t.Fatalf("append %d: %v", seq, err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit %d: %v", seq, err)
	}
}

func manifestCount(t *testing.T, cat *Catalog, fractalID string) int {
	t.Helper()
	ctx := context.Background()
	tbl, err := cat.cat.LoadTable(ctx, catalog.ToIdentifier(Namespace, tableName(fractalID)))
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		t.Fatalf("table fs: %v", err)
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		t.Fatalf("manifests: %v", err)
	}
	return len(manifests)
}

// Without commit.manifest-merge.enabled every append leaves a manifest that is
// never merged, so the list grows one per commit forever. That is what makes a
// rewrite commit cost O(files ever written): overwriteFiles.existingManifests()
// walks every entry of every manifest with no partition pruning. Pin that the
// table properties keep the list bounded well below the commit count.
func TestManifestMergeBoundsManifestCount(t *testing.T) {
	cat := newArchiveTestCatalog(t)
	fractalID := fmt.Sprintf("manifests-%d", time.Now().UnixNano())
	yesterday := time.Now().Add(-24 * time.Hour)

	const appends = 30
	for seq := range appends {
		appendAt(t, cat, fractalID, 200, seq, yesterday)
	}

	got := manifestCount(t, cat, fractalID)
	t.Logf("%d appends left %d manifest(s)", appends, got)
	// The merge threshold is manifestMinCountToMerge, so the steady state is at
	// most that many unmerged manifests plus the merged ones. Asserting well
	// under the append count is the real claim: unbounded growth is the bug.
	if got >= appends {
		t.Errorf("manifest count %d did not fall below the %d appends: merging is not happening", got, appends)
	}
	if got > manifestMinCountToMerge+2 {
		t.Errorf("manifest count %d exceeds the merge threshold %d by more than slack", got, manifestMinCountToMerge)
	}
}

// A bounded lookback must actually prune: partitions older than the window are
// not planned, and a deep pass (lookback 0) sees them again. This is the whole
// point of not calling compaction.Analyze, which is hardcoded to scan everything.
func TestManifestLookbackPrunesOldPartitions(t *testing.T) {
	cat := newArchiveTestCatalog(t)
	fractalID := fmt.Sprintf("lookback-%d", time.Now().UnixNano())
	old := time.Now().Add(-30 * 24 * time.Hour)
	recent := time.Now().Add(-24 * time.Hour)

	for seq := range 4 {
		appendAt(t, cat, fractalID, 500, seq, old)
	}
	for seq := 4; seq < 8; seq++ {
		appendAt(t, cat, fractalID, 500, seq, recent)
	}

	ctx := context.Background()
	tbl, err := cat.cat.LoadTable(ctx, catalog.ToIdentifier(Namespace, tableName(fractalID)))
	if err != nil {
		t.Fatalf("load table: %v", err)
	}

	// The deep plan establishes the premise and names the partition: it must see
	// both days. Deriving oldKey from it is also the only way to learn the
	// grouping key, which the compaction package builds privately.
	deep, err := planCompaction(ctx, tbl, 0)
	if err != nil {
		t.Fatalf("deep plan: %v", err)
	}
	oldKey := groupKeyForDay(deep, epochDayIce(old))
	if oldKey == "" {
		t.Fatalf("deep plan has no group in the 30-day-old partition; groups: %d", len(deep.Groups))
	}
	if groupKeyForDay(deep, epochDayIce(recent)) == "" {
		t.Fatalf("deep plan has no group in the recent partition; groups: %d", len(deep.Groups))
	}

	bounded, err := planCompaction(ctx, tbl, defaultCompactLookback)
	if err != nil {
		t.Fatalf("bounded plan: %v", err)
	}
	if groupKeyForDay(bounded, epochDayIce(old)) != "" {
		t.Errorf("bounded plan included the 30-day-old partition %s", oldKey)
	}
	if groupKeyForDay(bounded, epochDayIce(recent)) == "" {
		t.Errorf("bounded plan dropped the recent partition it was supposed to keep")
	}
	t.Logf("bounded plan: %d group(s) over %d file(s); deep plan: %d group(s) over %d file(s)",
		len(bounded.Groups), bounded.TotalInputFiles, len(deep.Groups), deep.TotalInputFiles)

	// Planning COST, not just plan contents: the bounded scan must read fewer
	// files than the deep one, or the filter pruned nothing and only the grouping
	// happened to differ.
	if bounded.TotalInputFiles >= deep.TotalInputFiles {
		t.Errorf("bounded scan planned %d file(s) vs deep %d: no pruning happened",
			bounded.TotalInputFiles, deep.TotalInputFiles)
	}
}

// groupKeyForDay returns the plan's grouping key for the partition of the given
// ingest day, or "" when the plan has no group there.
func groupKeyForDay(plan compaction.Plan, day iceberg.Date) string {
	for _, g := range plan.Groups {
		if len(g.Tasks) == 0 {
			continue
		}
		if v, ok := g.Tasks[0].File.Partition()[partitionFieldID]; ok {
			if d, ok := v.(iceberg.Date); ok && d == day {
				return g.PartitionKey
			}
		}
	}
	return ""
}
