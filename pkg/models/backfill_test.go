package models

import (
	"strings"
	"testing"
	"time"
)

func mustContain(t *testing.T, hay, needle, label string) {
	t.Helper()
	if !strings.Contains(hay, needle) {
		t.Errorf("%s: expected to contain %q\n--- got ---\n%s", label, needle, hay)
	}
}

func mustNotContain(t *testing.T, hay, needle, label string) {
	t.Helper()
	if strings.Contains(hay, needle) {
		t.Errorf("%s: expected NOT to contain %q\n--- got ---\n%s", label, needle, hay)
	}
}

// The backfill INSERT must reuse the MV's SELECT shape, but read from the given
// source table and carry the extra time/ingest predicate.
func TestBuildBackfillInsert_RarityDirect(t *testing.T) {
	def := ModelDefinition{
		Filter:       []FilterCondition{{Field: "event_name", Op: "=", Value: "security_socket_connect"}},
		PartitionKey: "computer_name",
		ValueKey:     "sin_port",
	}
	where := "timestamp >= '2026-06-12 00:00:00' AND timestamp < '2026-06-13 00:00:00' AND ingest_timestamp < '2026-06-13 09:00:00'"
	sql, err := BuildBackfillInsert(def, ModelTypeRarity, "`model_x`", "`logs_distributed`", where)
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, sql, "INSERT INTO `model_x`", "target")
	mustContain(t, sql, "FROM `logs_distributed`", "source table")
	mustContain(t, sql, "ingest_timestamp < '2026-06-13 09:00:00'", "dedup boundary")
	mustContain(t, sql, "timestamp >= '2026-06-12 00:00:00'", "window start")
	mustContain(t, sql, "fields.`event_name`::String = 'security_socket_connect'", "filter")
	mustContain(t, sql, "AS partition_val", "partition")
	mustContain(t, sql, "AS value_val", "value")
	mustContain(t, sql, "GROUP BY fractal_id, partition_val, value_val", "group by")
	// Must not silently read the local logs table.
	mustNotContain(t, sql, "FROM logs\n", "must use source table, not local logs")
}

func TestBuildBackfillInsert_FirstSeenDirect(t *testing.T) {
	def := ModelDefinition{KeyFields: []string{"computer_name", "user"}}
	where := "timestamp >= '2026-06-01 00:00:00' AND timestamp < '2026-06-02 00:00:00' AND ingest_timestamp < '2026-06-13 09:00:00'"
	sql, err := BuildBackfillInsert(def, ModelTypeFirstSeen, "`m`", "`logs`", where)
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, sql, "INSERT INTO `m`", "target")
	mustContain(t, sql, "FROM `logs`", "source table")
	mustContain(t, sql, "AS entity_key", "entity key")
	mustContain(t, sql, "first_seen", "first_seen")
	mustContain(t, sql, "last_seen", "last_seen")
	mustContain(t, sql, "ingest_timestamp < '2026-06-13 09:00:00'", "dedup boundary")
	mustContain(t, sql, "char(30)", "multi-key concat separator")
}

func TestBuildBackfillInsert_RarityWithExtraction(t *testing.T) {
	def := ModelDefinition{
		Filter:       []FilterCondition{{Field: "level", Op: "=", Value: "dns"}},
		Extractions:  []ExtractionStep{{FromField: "raw_log", Pattern: "([a-z]+)$", OutputField: "tld"}},
		PartitionKey: "computer_name",
		ValueKey:     "tld",
	}
	where := "timestamp >= '2026-06-12 00:00:00' AND timestamp < '2026-06-13 00:00:00' AND ingest_timestamp < '2026-06-13 09:00:00'"
	sql, err := BuildBackfillInsert(def, ModelTypeRarity, "`m`", "`logs_distributed`", where)
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, sql, "WITH", "CTE")
	mustContain(t, sql, "base AS (", "base CTE")
	mustContain(t, sql, "FROM `logs_distributed`", "base CTE reads source table")
	mustContain(t, sql, "ingest_timestamp < '2026-06-13 09:00:00'", "predicate in base CTE")
	mustContain(t, sql, "extract(", "extraction")
	mustContain(t, sql, "GROUP BY fractal_id, partition_val, value_val", "group by")
}

// Critical safety: the materialized view DDL must be unaffected by the refactor —
// it reads the local logs table with no extra predicate and performs no backfill.
func TestMVDDLUnchanged(t *testing.T) {
	def := ModelDefinition{
		Filter:       []FilterCondition{{Field: "event_name", Op: "=", Value: "x"}},
		PartitionKey: "computer_name",
		ValueKey:     "sin_port",
	}
	_, mvSQL, err := GenerateDDL(def, ModelTypeRarity, "`t`", "`mv`")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, mvSQL, "CREATE MATERIALIZED VIEW IF NOT EXISTS `mv` TO `t`", "mv ddl")
	mustContain(t, mvSQL, "FROM logs\n", "mv reads local logs")
	mustNotContain(t, mvSQL, "logs_distributed", "mv must not read distributed table")
	mustNotContain(t, mvSQL, "ingest_timestamp <", "mv must not carry a backfill predicate")
}

func TestBackfillChunks(t *testing.T) {
	// Exact day boundaries: 7 days -> 7 full-day chunks.
	end := time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)
	chunks := backfillChunks(end, 7)
	if len(chunks) != 7 {
		t.Fatalf("expected 7 chunks, got %d", len(chunks))
	}

	// Partial first/last day: now mid-day, 1-day window -> spans 2 partitions.
	end = time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	chunks = backfillChunks(end, 1)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks for partial 24h, got %d", len(chunks))
	}

	start := end.Add(-24 * time.Hour)
	if !chunks[0].start.Equal(start) {
		t.Errorf("first chunk start = %v, want %v", chunks[0].start, start)
	}
	if !chunks[len(chunks)-1].end.Equal(end) {
		t.Errorf("last chunk end = %v, want %v", chunks[len(chunks)-1].end, end)
	}
	// Contiguous, day-aligned, and fully covering the window.
	var covered time.Duration
	for i, c := range chunks {
		if !c.end.After(c.start) {
			t.Errorf("chunk %d non-positive interval: %v..%v", i, c.start, c.end)
		}
		if i > 0 && !c.start.Equal(chunks[i-1].end) {
			t.Errorf("chunk %d not contiguous with previous: %v vs %v", i, c.start, chunks[i-1].end)
		}
		// No chunk crosses a UTC midnight (except its exclusive end touching it).
		dayStart := time.Date(c.start.Year(), c.start.Month(), c.start.Day(), 0, 0, 0, 0, time.UTC)
		if c.end.After(dayStart.Add(24 * time.Hour)) {
			t.Errorf("chunk %d crosses a day boundary: %v..%v", i, c.start, c.end)
		}
		covered += c.end.Sub(c.start)
	}
	if covered != 24*time.Hour {
		t.Errorf("covered = %v, want 24h", covered)
	}
}

func TestBackfillWindowDays(t *testing.T) {
	cases := map[string]int{"24h": 1, "7d": 7, "30d": 30, "90d": 90}
	for w, want := range cases {
		got, ok := BackfillWindowDays(w)
		if !ok || got != want {
			t.Errorf("BackfillWindowDays(%q) = %d,%v want %d,true", w, got, ok, want)
		}
	}
	if _, ok := BackfillWindowDays("180d"); ok {
		t.Error("180d should be invalid (90d is the max)")
	}
	if _, ok := BackfillWindowDays(""); ok {
		t.Error("empty window should be invalid")
	}
}
