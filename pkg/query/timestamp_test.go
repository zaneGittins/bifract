package query

import (
	"testing"
	"time"
)

func TestParseLogTimestamp(t *testing.T) {
	want := time.Date(2026, 3, 22, 18, 37, 11, 329000000, time.UTC)

	cases := []struct {
		name  string
		input string
		want  time.Time
		zero  bool
	}{
		{"clickhouse millis", "2026-03-22 18:37:11.329", want, false},
		{"clickhouse seconds", "2026-03-22 18:37:11", time.Date(2026, 3, 22, 18, 37, 11, 0, time.UTC), false},
		{"rfc3339 z", "2026-03-22T18:37:11.329Z", want, false},
		{"rfc3339 offset is normalized to utc", "2026-03-22T20:37:11.329+02:00", want, false},
		{"empty", "", time.Time{}, true},
		{"garbage", "not-a-time", time.Time{}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseLogTimestamp(tc.input)
			if tc.zero {
				if !got.IsZero() {
					t.Fatalf("expected zero time, got %v", got)
				}
				return
			}
			if !got.Equal(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("expected UTC location, got %v", got.Location())
			}
		})
	}
}
