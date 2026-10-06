package query

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// The landing sample reads newest-first, so without an upper bound a handful of
// future-dated rows would fill all 50 slots ahead of every real recent log.
func TestRecentLogsSQLBoundsAboveAtNow(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 30, 15, 250_000_000, time.UTC)
	sql := recentLogsSQL("logs", "fractal_id = 'f1'", false, now.Add(-24*time.Hour), now)

	for _, want := range []string{
		"WHERE timestamp >= '2026-10-05 12:30:15.250' AND timestamp <= '2026-10-06 12:30:15.250' AND fractal_id = 'f1'",
		"ORDER BY timestamp DESC LIMIT 50",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
	// Bounds must be constants on the bare sort-key column: now() or a function
	// over timestamp would defeat primary-key pruning and read-in-order.
	if strings.Contains(sql, "now") || regexp.MustCompile(`\w\(timestamp\)`).MatchString(sql) || strings.Contains(sql, " AS timestamp") {
		t.Errorf("time bound is not a constant on the sort key:\n%s", sql)
	}
}

func TestRecentLogsSQLUpperBoundNotInFuture(t *testing.T) {
	now := time.Now()
	sql := recentLogsSQL("logs", "", true, now.Add(-24*time.Hour), now)
	m := regexp.MustCompile(`timestamp <= '([^']+)'`).FindStringSubmatch(sql)
	if m == nil {
		t.Fatalf("no upper bound in:\n%s", sql)
	}
	end, err := time.Parse("2006-01-02 15:04:05.000", m[1])
	if err != nil {
		t.Fatal(err)
	}
	if end.After(time.Now().UTC()) {
		t.Fatalf("upper bound %v is in the future", end)
	}
	if !strings.Contains(sql, "' ORDER BY") || !strings.Contains(sql, "toString(_shard_num) AS _shard_num") {
		t.Errorf("unexpected SQL without a fractal condition:\n%s", sql)
	}
}

func TestRecentHistogramSQLBoundsAboveAtNow(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 30, 15, 0, time.UTC)
	sql := recentHistogramSQL("logs_histogram", "fractal_id = 'f1'", now.Add(-24*time.Hour), now)
	want := "WHERE minute >= '2026-10-05 12:30:15' AND minute <= '2026-10-06 12:30:15' AND fractal_id = 'f1'"
	if !strings.Contains(sql, want) {
		t.Errorf("missing %q in:\n%s", want, sql)
	}
}
