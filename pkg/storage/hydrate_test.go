package storage

import (
	"strings"
	"testing"
	"time"
)

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
	_, err := c.HydrateLogFields(nil, "logs; DROP TABLE logs", []LogKey{{LogID: "a"}}, time.Time{}, time.Time{})
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

// An unset window must drop the time bound rather than render as a range around
// year zero, which would match nothing and silently return every row unhydrated.
// That is the same silent-loss shape as the date grouping this replaced.
func TestHydrateQueryUnsetWindowIsUnbounded(t *testing.T) {
	bounded := hydrateQuery("logs", true, true)
	if !strings.Contains(bounded, "ingest_timestamp >=") || !strings.Contains(bounded, "fractal_id IN") {
		t.Errorf("bounded query missing predicates:\n%s", bounded)
	}

	unbounded := hydrateQuery("logs", false, false)
	if strings.Contains(unbounded, "ingest_timestamp") {
		t.Errorf("zero window must not emit a time bound:\n%s", unbounded)
	}
	if !strings.Contains(unbounded, "log_id IN (?)") {
		t.Errorf("unbounded query lost its log_id filter:\n%s", unbounded)
	}
	// The dedup clause is what keeps a re-ingested log_id from double-counting.
	for _, q := range []string{bounded, unbounded} {
		if !strings.HasSuffix(q, "LIMIT 1 BY log_id") {
			t.Errorf("query must end in the dedup clause:\n%s", q)
		}
	}
}

// The argument list must line up with the placeholders in every shape, or the
// driver binds the fractal filter to a timestamp.
func TestHydrateQueryPlaceholderCount(t *testing.T) {
	for _, tc := range []struct {
		bounded, scoped bool
		want            int
	}{
		{true, true, 4}, {true, false, 3}, {false, true, 2}, {false, false, 1},
	} {
		q := hydrateQuery("logs", tc.bounded, tc.scoped)
		if got := strings.Count(q, "?"); got != tc.want {
			t.Errorf("bounded=%v scoped=%v: %d placeholders, want %d\n%s", tc.bounded, tc.scoped, got, tc.want, q)
		}
	}
}
