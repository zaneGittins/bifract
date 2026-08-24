package storage

import (
	"testing"
	"time"
)

func TestUnwrapSimpleAggregateFunction(t *testing.T) {
	cases := []struct{ in, want string }{
		{"SimpleAggregateFunction(min, DateTime64(3))", "DateTime64(3)"},
		{"SimpleAggregateFunction(max, DateTime64(3, 'UTC'))", "DateTime64(3, 'UTC')"},
		{"SimpleAggregateFunction(sum, UInt64)", "UInt64"},
		{"SimpleAggregateFunction(anyLast, Map(String, String))", "Map(String, String)"},
		// Passthrough: plain types and non-simple aggregate state.
		{"String", "String"},
		{"DateTime64(3)", "DateTime64(3)"},
		{"Nullable(String)", "Nullable(String)"},
		{"AggregateFunction(groupUniqArray(365), Date)", "AggregateFunction(groupUniqArray(365), Date)"},
		// Malformed input must not panic or truncate.
		{"SimpleAggregateFunction(", "SimpleAggregateFunction("},
		{"SimpleAggregateFunction(min)", "SimpleAggregateFunction(min)"},
	}
	for _, c := range cases {
		if got := unwrapSimpleAggregateFunction(c.in); got != c.want {
			t.Errorf("unwrapSimpleAggregateFunction(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// IngestTime backs the logs and logs_raw partition key and the archive's
// ingest_date. A zero value would partition at the epoch and put the archive and
// the hot store on different days, so it must never surface as one.
func TestLogEntryIngestTime(t *testing.T) {
	set := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	if got := (LogEntry{IngestTimestamp: set}).IngestTime(); !got.Equal(set) {
		t.Errorf("IngestTime = %s, want the value already set (%s)", got, set)
	}

	got := LogEntry{}.IngestTime()
	if got.IsZero() {
		t.Fatal("an unset IngestTimestamp must fall back to now, not stay zero")
	}
	if d := time.Since(got); d < 0 || d > time.Minute {
		t.Errorf("fallback %s is not close to now", got)
	}
}
