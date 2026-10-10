package ingest

import (
	"testing"
	"time"
)

func TestParseTimestampWithFormat_Epoch(t *testing.T) {
	cases := []struct {
		val, format string
		want        time.Time
	}{
		{"1791581172", "unix", time.Unix(1791581172, 0)},
		// Velociraptor's TimeCreated.SystemTime: the fraction must survive exactly.
		{"1791581172.686968", "unix", time.Unix(1791581172, 686968000)},
		{"1791581172.123456789123", "unix", time.Unix(1791581172, 123456789)},
		{"1791581172686", "unixms", time.Unix(1791581172, 686000000)},
		{"1791581172686.5", "unixms", time.Unix(1791581172, 686500000)},
		{"1791581172686968", "unixmicro", time.Unix(1791581172, 686968000)},
		{"1791581172686968123", "unixns", time.Unix(1791581172, 686968123)},
		{"-1.5", "unix", time.Unix(-2, 500000000)},
		{" 1791581172 ", "unix", time.Unix(1791581172, 0)},
		{"9223372036854775807", "unixns", time.Unix(0, 9223372036854775807)},
	}
	for _, c := range cases {
		got := parseTimestampWithFormat(c.val, c.format)
		if !got.Equal(c.want) {
			t.Errorf("%s %q: got %v, want %v", c.format, c.val, got.UTC(), c.want.UTC())
		}
	}
}

func TestParseTimestampWithFormat_EpochRejectsMalformed(t *testing.T) {
	for _, val := range []string{"", "abc", "123abc", "1.", "1.2x", "1.2.3",
		// A millisecond count read as seconds, and values that would overflow.
		"1791581172686", "-9223372036854775808", "9223372036854775807"} {
		if got := parseTimestampWithFormat(val, "unix"); !got.IsZero() {
			t.Errorf("%q: expected zero time so the next field is tried, got %v", val, got)
		}
	}
}

func TestParseTimestampWithFormat_Layout(t *testing.T) {
	got := parseTimestampWithFormat("2026-10-09 21:26:12.684", "2006-01-02 15:04:05.000")
	want := time.Date(2026, 10, 9, 21, 26, 12, 684000000, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
