package parser

import (
	"strings"
	"testing"
	"time"
)

func fieldStatsTestOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC),
		FractalID: "f-123",
		TableName: "logs",
	}
}

func TestBuildFieldStatsSQL_ReusesFilterAndBounds(t *testing.T) {
	p, err := ParseQuery(`user="SYSTEM" event_id=1`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sql, err := BuildFieldStatsSQL(p, fieldStatsTestOpts(), FieldStatsParams{SampleSize: 12345})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Time range + fractal scope from base conditions.
	for _, want := range []string{
		"timestamp >= '2026-07-01 00:00:00'",
		"timestamp <= '2026-07-11 00:00:00'",
		"fractal_id = 'f-123'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing base condition %q in:\n%s", want, sql)
		}
	}

	// User filter is reused (WHERE portion only).
	if !strings.Contains(sql, "fields.`user`::String = 'SYSTEM'") ||
		!strings.Contains(sql, "fields.`event_id`::String = '1'") {
		t.Errorf("user filter not reused in:\n%s", sql)
	}

	// Scan is bounded by the sample size and reads the flat norm_log column.
	if !strings.Contains(sql, "ORDER BY timestamp DESC LIMIT 12345") {
		t.Errorf("sample bound missing in:\n%s", sql)
	}
	if !strings.Contains(sql, "SELECT norm_log FROM logs") {
		t.Errorf("should aggregate over norm_log in:\n%s", sql)
	}

	// Empty values are excluded so 100%-empty typed sub-columns drop out.
	if !strings.Contains(sql, "kv.2 != ''") {
		t.Errorf("empty-value exclusion missing in:\n%s", sql)
	}

	// Sample-size sentinel survives the empty filter (non-empty value).
	if !strings.Contains(sql, "'__rows__', '1'") {
		t.Errorf("__rows__ sentinel missing/empty in:\n%s", sql)
	}
}

func TestBuildFieldStatsSQL_Defaults(t *testing.T) {
	p, _ := ParseQuery(`*`)
	sql, err := BuildFieldStatsSQL(p, fieldStatsTestOpts(), FieldStatsParams{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Defaults: sample 50000, top 10, maxFields 250 (+1 for the sentinel row).
	if !strings.Contains(sql, "LIMIT 50000") {
		t.Errorf("default sample size not applied:\n%s", sql)
	}
	if !strings.Contains(sql, "rnk <= 10") {
		t.Errorf("default top-N not applied:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT 251") {
		t.Errorf("default max fields (+sentinel) not applied:\n%s", sql)
	}
}

func TestBuildFieldStatsSQL_SourceCommandUnsupported(t *testing.T) {
	// A source-command composition has no norm_log column to aggregate; the builder
	// signals "unsupported" by returning an empty string (no error).
	opts := fieldStatsTestOpts()
	opts.SourceSubquery = "SELECT ... FROM logs" // pgr()-style source
	p, _ := ParseQuery(`*`)
	sql, err := BuildFieldStatsSQL(p, opts, FieldStatsParams{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if sql != "" {
		t.Errorf("expected empty SQL for source-subquery composition, got:\n%s", sql)
	}
}
