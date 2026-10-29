package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	dbsql "bifract/db"
	"bifract/internal/fakedb"
)

// A partition still taking inserts is never repaired: its counts race the MV, and
// the rollup would be rebuilt every pass for a mismatch that is only in flight.
// Once closed, any disagreement with logs is repaired, and a rollup over its cap
// is trimmed.
func TestPlanFieldPathRepair(t *testing.T) {
	today := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	closed := today.AddDate(0, 0, -2)
	tests := []struct {
		name string
		p    fieldPathPartition
		want fieldPathRepair
	}{
		{"today is open", fieldPathPartition{Day: today, LogRows: 10, Counted: 3}, repairNone},
		{"yesterday is open", fieldPathPartition{Day: today.AddDate(0, 0, -1), LogRows: 10}, repairNone},
		{"closed and matching", fieldPathPartition{Day: closed, LogsID: "l", RollupID: "r", LogRows: 10, Counted: 10, PathRows: 30}, repairNone},
		{"closed and never counted", fieldPathPartition{Day: closed, LogsID: "l", LogRows: 10}, repairRebuild},
		{"closed and short", fieldPathPartition{Day: closed, LogsID: "l", LogRows: 10, Counted: 4}, repairRebuild},
		{"closed and double counted", fieldPathPartition{Day: closed, LogsID: "l", LogRows: 10, Counted: 20}, repairRebuild},
		{"logs dropped", fieldPathPartition{Day: closed, RollupID: "r", Counted: 10}, repairDrop},
		{"logs dropped, no row count", fieldPathPartition{Day: closed, RollupID: "r", PathRows: 3}, repairDrop},
		{"logs partition empty", fieldPathPartition{Day: closed, LogsID: "l", RollupID: "r", Counted: 10}, repairRebuild},
		{"over the cap", fieldPathPartition{Day: closed, LogsID: "l", RollupID: "r", LogRows: 10, Counted: 10, PathRows: FieldPathCap + 1}, repairCompact},
		{"at the cap", fieldPathPartition{Day: closed, LogsID: "l", RollupID: "r", LogRows: 10, Counted: 10, PathRows: FieldPathCap}, repairNone},
	}
	for _, tt := range tests {
		if got := planFieldPathRepair(tt.p, today); got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.want)
		}
	}
}

// Newest first: recent days are what the tab most needs exact, and a pass that runs
// out of time should have spent it there.
func TestFieldPathPartitionsNewestFirst(t *testing.T) {
	d := func(day int) time.Time { return time.Date(2026, 10, day, 0, 0, 0, 0, time.UTC) }
	logs := map[string]partStat{
		"('a','2026-10-01')": {ID: "la1", Rows: 5},
		"('a','2026-10-03')": {ID: "la3", Rows: 7},
		"('b','2026-10-02')": {ID: "lb2", Rows: 9},
	}
	rollup := map[string]partStat{
		"('a','2026-10-03')": {ID: "ra3", Rows: 40},
		"('z','2026-10-02')": {ID: "rz2", Rows: 3},
	}
	counted := map[string]uint64{"ra3": 7, "rz2": 2}

	got := joinFieldPathPartitions(logs, rollup, counted)
	want := []fieldPathPartition{
		{Partition: "('a','2026-10-03')", LogsID: "la3", RollupID: "ra3", Day: d(3), LogRows: 7, Counted: 7, PathRows: 40},
		{Partition: "('b','2026-10-02')", LogsID: "lb2", Day: d(2), LogRows: 9},
		{Partition: "('z','2026-10-02')", RollupID: "rz2", Day: d(2), Counted: 2, PathRows: 3},
		{Partition: "('a','2026-10-01')", LogsID: "la1", Day: d(1), LogRows: 5},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d partitions, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("partition %d:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
}

// A partition too large for one bounded query is rebuilt in event-time slices that
// tile it exactly, so every row is counted once.
func TestFieldPathChunkBounds(t *testing.T) {
	if got := fieldPathChunks(fieldPathChunkRows); got != 1 {
		t.Errorf("one chunk's worth of rows: got %d chunks, want 1", got)
	}
	if got := fieldPathChunks(fieldPathChunkRows*3 + 1); got != 4 {
		t.Errorf("just over three chunks: got %d, want 4", got)
	}

	conds := chunkConditions([]string{"t1", "t2"})
	want := []string{"timestamp < t1", "timestamp >= t1 AND timestamp < t2", "timestamp >= t2"}
	if strings.Join(conds, "|") != strings.Join(want, "|") {
		t.Errorf("chunk conditions:\n got %q\nwant %q", conds, want)
	}
	if got := chunkConditions(nil); len(got) != 1 || got[0] != "1" {
		t.Errorf("no bounds: got %q, want one unbounded chunk", got)
	}
}

// The rebuild must count exactly what the MV counts, or a repaired partition would
// disagree with the days around it.
func TestFieldPathsViewMatchesRebuild(t *testing.T) {
	for _, src := range []struct{ name, sql string }{
		{"init-clickhouse.sql", dbsql.ClickHouseSQL},
		{"018 migration", migrationBody(t, "018_logs_field_paths.sql")},
	} {
		sql := strings.Join(strings.Fields(src.sql), " ")
		if !strings.Contains(sql, "ARRAY JOIN "+fieldPathsExpr+" AS path") {
			t.Errorf("%s: logs_field_paths_mv does not use fieldPathsExpr", src.name)
		}
		if !strings.Contains(sql, fieldPathsCapClause) {
			t.Errorf("%s: logs_field_paths_mv does not apply %q", src.name, fieldPathsCapClause)
		}
	}
}

func migrationBody(t *testing.T, name string) string {
	t.Helper()
	b, err := dbsql.ClickHouseMigrations.ReadFile("migrations/clickhouse/" + name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// Approximate quantiles carry no ordering guarantee, and an unsorted bound list
// would overlap or gap the slices, so every pass would see a mismatch and rebuild.
func TestSliceBoundsSortedAndDistinct(t *testing.T) {
	at := func(s int) time.Time { return time.Date(2026, 10, 1, 0, 0, s, 0, time.UTC) }
	got := sliceBounds([]time.Time{at(30), at(10), at(30), at(20)})
	want := []string{
		"toDateTime64('2026-10-01 00:00:10.000', 3, 'UTC')",
		"toDateTime64('2026-10-01 00:00:20.000', 3, 'UTC')",
		"toDateTime64('2026-10-01 00:00:30.000', 3, 'UTC')",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("bounds:\n got %q\nwant %q", got, want)
	}
}

// One partition that cannot be repaired must not starve every older one on its
// shard: the pass logs it, moves on, and still reports the failure.
func TestReconcileContinuesPastAFailedPartition(t *testing.T) {
	_, f := newFakeClient()
	f.Respond(func(q string, args []any) (fakedb.CHResult, bool) {
		switch {
		case strings.Contains(q, "FROM system.parts") && len(args) == 1 && args[0] == "logs":
			return fakedb.CHResult{
				Cols: []fakedb.Col{{Column: "partition", Type: "String"}, {Column: "id", Type: "String"}, {Column: "rows", Type: "UInt64"}},
				Rows: [][]any{{"('a','2026-10-01')", "a1", uint64(5)}, {"('a','2026-10-02')", "a2", uint64(5)}},
			}, true
		case strings.Contains(q, "ARRAY JOIN") && args[0] == "a2":
			return fakedb.CHResult{Err: errors.New("timeout")}, true
		}
		return fakedb.CHResult{}, false
	})
	today := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	if err := reconcileFieldPathsOn(context.Background(), f, today); err == nil {
		t.Error("the failed partition must still be reported")
	}
	if n := len(f.QueriedMatching("ARRAY JOIN")); n != 2 {
		t.Errorf("recounted %d partitions, want both attempted", n)
	}
}

// A recount is staged and replaces the live partition only after it succeeds; a
// recount that fails leaves the existing counts untouched.
func TestRebuildSwapsInOnlyAfterTheRecount(t *testing.T) {
	p := fieldPathPartition{Partition: "('a','2026-10-01')", LogsID: "l1", RollupID: "r1", LogRows: 5}
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	recount := fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "fractal_id", Type: "String"}, {Column: "ingest_day", Type: "Date"}, {Column: "path", Type: "String"}, {Column: "cnt", Type: "UInt64"}},
		Rows: [][]any{{"a", day, "", uint64(5)}, {"a", day, "src", uint64(5)}},
	}

	_, f := newFakeClient()
	f.On("ARRAY JOIN", recount)
	f.OnArg(partsQuery, fieldPathsStage, fakedb.Strs("partition_id", "s1"))
	if err := rebuildFieldPaths(context.Background(), f, p); err != nil {
		t.Fatal(err)
	}
	if b := f.Batches(); len(b) != 1 || !b[0].Sent || len(b[0].Appended) != 2 || b[0].Appended[0][2] != "" {
		t.Errorf("want the two recounted rows staged, row count first: %+v", b)
	}
	if indexOf(f.Executed(), "ALTER TABLE logs_field_paths REPLACE PARTITION ID 's1' FROM "+fieldPathsStage) < 0 {
		t.Errorf("staged recount was not swapped in:\n%s", strings.Join(f.Executed(), "\n"))
	}
	if indexOf(f.Executed(), "ALTER TABLE `logs_field_paths` DROP") >= 0 {
		t.Error("the live partition must never be dropped by a rebuild")
	}

	_, f = newFakeClient()
	f.On("ARRAY JOIN", fakedb.CHResult{Err: errors.New("memory limit")})
	if err := rebuildFieldPaths(context.Background(), f, p); err == nil {
		t.Fatal("want the recount failure")
	}
	if len(f.ExecutedMatching("REPLACE PARTITION")) != 0 || len(f.Batches()) != 0 {
		t.Error("a failed recount must not be staged or swapped in")
	}
}

// Unmerged parts can make a day look over the cap. Merging it stops the next pass
// from reading it again; nothing is swapped.
func TestCompactMergesWhenNotOverTheCap(t *testing.T) {
	_, f := newFakeClient()
	p := fieldPathPartition{Partition: "('a','2026-10-01')", RollupID: "r1"}
	if err := compactFieldPaths(context.Background(), f, p); err != nil {
		t.Fatal(err)
	}
	if got := f.Executed(); len(got) != 1 || got[0] != "OPTIMIZE TABLE logs_field_paths PARTITION ID 'r1' FINAL" {
		t.Errorf("executed %q, want only the merge", got)
	}
}

// A partition whose day cannot be read is never treated as closed.
func TestJoinSkipsUnparseablePartitions(t *testing.T) {
	got := joinFieldPathPartitions(map[string]partStat{"tuple()": {ID: "x", Rows: 1}}, nil, nil)
	if len(got) != 0 {
		t.Errorf("got %+v, want nothing", got)
	}
}

// The ratio settings do not spill under a per-query memory limit, so reconcile
// statements carry absolute thresholds below that limit.
func TestFieldPathSettingsSpillUnderTheLimit(t *testing.T) {
	s := fieldPathSettings(900)
	if s["max_bytes_before_external_group_by"] != int64(300) || s["max_bytes_before_external_sort"] != int64(300) {
		t.Errorf("spill thresholds = %v / %v, want 300", s["max_bytes_before_external_group_by"], s["max_bytes_before_external_sort"])
	}
	if s["max_memory_usage"] != int64(900) {
		t.Errorf("memory limit = %v", s["max_memory_usage"])
	}
}
