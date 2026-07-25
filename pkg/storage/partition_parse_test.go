package storage

import "testing"

// The logs partition key renders as ('<fractal_id>','<date>'). Both halves drive
// the histogram rollup prune, so a mis-parse would leave the rollup counting data
// that no longer exists (or, worse, prune a different fractal's counts).
func TestParsePartitionFractalAndDay(t *testing.T) {
	tests := []struct {
		partition string
		wantID    string
		wantDay   string
	}{
		{"('my-fractal','2026-07-01')", "my-fractal", "2026-07-01"},
		{"('550e8400-e29b-41d4-a716-446655440000','2026-12-31')", "550e8400-e29b-41d4-a716-446655440000", "2026-12-31"},
		{"('','2026-07-01')", "", "2026-07-01"},
		// ClickHouse doubles single quotes in its canonical rendering.
		{"('o''brien','2026-07-01')", "o'brien", "2026-07-01"},
		// A hyphenated name must not confuse the day split.
		{"('team-a-b-c','2026-01-02')", "team-a-b-c", "2026-01-02"},
	}
	for _, tt := range tests {
		if got := parseFractalFromPartition(tt.partition); got != tt.wantID {
			t.Errorf("parseFractalFromPartition(%q) = %q, want %q", tt.partition, got, tt.wantID)
		}
		if got := parsePartitionDay(tt.partition); got != tt.wantDay {
			t.Errorf("parsePartitionDay(%q) = %q, want %q", tt.partition, got, tt.wantDay)
		}
	}
}

// A malformed partition must yield an empty day rather than a bogus one: an
// empty day widens the prune to the whole fractal, which is wrong, so callers
// depend on the fractal id being empty too in that case.
func TestParsePartitionDayRejectsMalformed(t *testing.T) {
	for _, p := range []string{"", "2026-07-01", "('no-day-here')", "('unterminated','2026-07-01'"} {
		if got := parsePartitionDay(p); got != "" {
			t.Errorf("parsePartitionDay(%q) = %q, want empty", p, got)
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
