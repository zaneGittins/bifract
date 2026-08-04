package storage

import (
	"testing"
	"time"
)

func ts(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		panic(err)
	}
	return t
}

// TestGroupKeysByDateNeverSpansPartitions is the reason the cold path groups at
// all: logs is partitioned by toDate(timestamp) but alerts match on
// ingest_timestamp, so a single backfilled row would otherwise stretch a
// min/max range across every partition in between.
func TestGroupKeysByDateNeverSpansPartitions(t *testing.T) {
	keys := []LogKey{
		{LogID: "a", Timestamp: ts("2026-08-03 10:00:00")},
		{LogID: "b", Timestamp: ts("2026-08-03 10:00:05")},
		{LogID: "c", Timestamp: ts("2020-01-01 03:00:00")}, // backfilled outlier
	}
	batches := groupKeysByDate(keys)
	if len(batches) != 2 {
		t.Fatalf("expected 2 date batches, got %d", len(batches))
	}
	for _, b := range batches {
		if b.from.UTC().Format("2006-01-02") != b.to.UTC().Format("2006-01-02") {
			t.Errorf("batch spans dates: %s..%s", b.from, b.to)
		}
	}
}

// TestGroupKeysByDateNarrowsToObservedRange keeps the primary-key range as tight
// as the matched rows allow rather than widening to the day boundary.
func TestGroupKeysByDateNarrowsToObservedRange(t *testing.T) {
	keys := []LogKey{
		{LogID: "a", Timestamp: ts("2026-08-03 10:00:05")},
		{LogID: "b", Timestamp: ts("2026-08-03 10:00:01")},
		{LogID: "c", Timestamp: ts("2026-08-03 10:00:09")},
	}
	batches := groupKeysByDate(keys)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if !batches[0].from.Equal(ts("2026-08-03 10:00:01")) {
		t.Errorf("from = %s, want 10:00:01", batches[0].from)
	}
	if !batches[0].to.Equal(ts("2026-08-03 10:00:09")) {
		t.Errorf("to = %s, want 10:00:09", batches[0].to)
	}
}

func TestGroupKeysByDateSkipsUnusableKeys(t *testing.T) {
	keys := []LogKey{
		{LogID: "a", Timestamp: ts("2026-08-03 10:00:00")},
		{LogID: "", Timestamp: ts("2026-08-03 10:00:00")}, // no id
		{LogID: "b"},                                      // unparseable timestamp
		{LogID: "a", Timestamp: ts("2026-08-03 10:00:00")}, // duplicate
	}
	batches := groupKeysByDate(keys)
	if len(batches) != 1 || len(batches[0].logIDs) != 1 || batches[0].logIDs[0] != "a" {
		t.Fatalf("expected only log a, got %+v", batches)
	}
}

// TestGroupKeysByDateCapsPartitionBudget keeps the largest groups when a replay
// spreads matched rows across more dates than the budget allows.
func TestGroupKeysByDateCapsPartitionBudget(t *testing.T) {
	var keys []LogKey
	day := ts("2026-08-03 12:00:00")
	for d := 0; d < hydrateMaxDateGroups+3; d++ {
		rows := 1
		if d == 0 {
			rows = 5 // the group that must survive the cap
		}
		for r := 0; r < rows; r++ {
			keys = append(keys, LogKey{
				LogID:     string(rune('a'+d)) + string(rune('0'+r)),
				Timestamp: day.AddDate(0, 0, -d),
			})
		}
	}
	batches := groupKeysByDate(keys)
	if len(batches) != hydrateMaxDateGroups {
		t.Fatalf("expected %d batches, got %d", hydrateMaxDateGroups, len(batches))
	}
	if len(batches[0].logIDs) != 5 {
		t.Errorf("largest group was dropped: first batch has %d ids", len(batches[0].logIDs))
	}
}

func TestChunkStrings(t *testing.T) {
	if got := chunkStrings(nil, 500); got != nil {
		t.Errorf("empty input should chunk to nil, got %v", got)
	}
	ids := make([]string, hydrateChunkSize+1)
	for i := range ids {
		ids[i] = "id"
	}
	chunks := chunkStrings(ids, hydrateChunkSize)
	if len(chunks) != 2 || len(chunks[0]) != hydrateChunkSize || len(chunks[1]) != 1 {
		t.Fatalf("unexpected chunking: %d chunks", len(chunks))
	}
}

// TestObservedFractalIDs covers the prism case: the filter must come from the
// rows that actually matched, not the whole prism membership.
func TestObservedFractalIDs(t *testing.T) {
	keys := []LogKey{
		{LogID: "a", FractalID: "f1"},
		{LogID: "b", FractalID: "f2"},
		{LogID: "c", FractalID: "f1"},
		{LogID: "d"},
	}
	got := observedFractalIDs(keys)
	if len(got) != 2 || got[0] != "f1" || got[1] != "f2" {
		t.Errorf("observedFractalIDs = %v, want [f1 f2]", got)
	}
}

func TestDedupeLogIDs(t *testing.T) {
	keys := []LogKey{{LogID: "a"}, {LogID: "b"}, {LogID: "a"}, {LogID: ""}}
	got := dedupeLogIDs(keys)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("dedupeLogIDs = %v, want [a b]", got)
	}
}

// TestHydrateLogFieldsRejectsUnknownTable guards the one interpolated token in
// the hydration SQL. The table always comes from parser.QueryOptions.TableName.
func TestHydrateLogFieldsRejectsUnknownTable(t *testing.T) {
	c := &ClickHouseClient{}
	_, err := c.HydrateLogFields(nil, "logs; DROP TABLE logs", false, []LogKey{{LogID: "a"}}, time.Time{}, time.Time{})
	if err == nil {
		t.Fatal("expected an error for an unsupported table")
	}
	for _, table := range []string{"logs", "logs_distributed", "logs_hot", "logs_hot_distributed"} {
		if !hydrateTables[table] {
			t.Errorf("%s should be an allowed hydration table", table)
		}
	}
}

func TestChTimeArgIsUTC(t *testing.T) {
	loc := time.FixedZone("UTC+5", 5*60*60)
	got := chTimeArg(time.Date(2026, 8, 3, 15, 0, 0, 0, loc))
	if got != "2026-08-03 10:00:00.000" {
		t.Errorf("chTimeArg = %q, want the UTC rendering", got)
	}
}
