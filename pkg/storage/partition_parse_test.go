package storage

import "testing"

// The logs partition key renders as ('<fractal_id>','<ingest_date>'). Both halves drive
// the histogram rollup prune, so a mis-parse would leave the rollup counting data
// that no longer exists (or, worse, prune a different fractal's counts).
func TestParseLogPartition(t *testing.T) {
	tests := []struct {
		partition string
		wantID    string
		wantDay   string
	}{
		{"('my-fractal','2026-07-01')", "my-fractal", "2026-07-01"},
		{"('550e8400-e29b-41d4-a716-446655440000','2026-12-31')", "550e8400-e29b-41d4-a716-446655440000", "2026-12-31"},
		// The default fractal's real scope, not a parse failure.
		{"('','2026-07-01')", "", "2026-07-01"},
		// ClickHouse doubles single quotes in its canonical rendering.
		{"('o''brien','2026-07-01')", "o'brien", "2026-07-01"},
		// An id whose own text contains the separator: only the last one is real.
		{"('a'',''b','2026-07-01')", "a','b", "2026-07-01"},
		// A hyphenated name must not confuse the day split.
		{"('team-a-b-c','2026-01-02')", "team-a-b-c", "2026-01-02"},
	}
	for _, tt := range tests {
		id, day, ok := ParseLogPartition(tt.partition)
		if !ok {
			t.Errorf("ParseLogPartition(%q) not ok", tt.partition)
			continue
		}
		if id != tt.wantID {
			t.Errorf("ParseLogPartition(%q) id = %q, want %q", tt.partition, id, tt.wantID)
		}
		if got := day.Format("2006-01-02"); got != tt.wantDay {
			t.Errorf("ParseLogPartition(%q) day = %q, want %q", tt.partition, got, tt.wantDay)
		}
	}
}

// Callers must branch on ok, never on an empty fractal id, so a malformed partition has
// to report not-ok rather than degrade into the default fractal's scope.
func TestParseLogPartitionRejectsMalformed(t *testing.T) {
	for _, p := range []string{
		"",
		"2026-07-01",
		"('no-day-here')",
		"('unterminated','2026-07-01'",
		"('my-fractal','not-a-date')",
		"('my-fractal','2026-13-45')",
	} {
		if _, _, ok := ParseLogPartition(p); ok {
			t.Errorf("ParseLogPartition(%q) = ok, want not ok", p)
		}
	}
}

func TestEscCHLiteral(t *testing.T) {
	if got := escCHLiteral("o'brien"); got != "o''brien" {
		t.Errorf("escCHLiteral = %q, want %q", got, "o''brien")
	}
	if got := escCHLiteral("plain"); got != "plain" {
		t.Errorf("escCHLiteral = %q, want %q", got, "plain")
	}
}
