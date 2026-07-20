package storage

import (
	"testing"
	"time"
)

func TestRowTime(t *testing.T) {
	want := time.Date(2026, 7, 15, 10, 30, 45, 123000000, time.UTC)

	t.Run("scanRowMap's own layout", func(t *testing.T) {
		got, ok := RowTime(map[string]interface{}{"ts": "2026-07-15 10:30:45.123"}, "ts")
		if !ok || !got.Equal(want) {
			t.Errorf("RowTime = %v, %v; want %v, true", got, ok, want)
		}
	})

	t.Run("accepts a real time.Time", func(t *testing.T) {
		got, ok := RowTime(map[string]interface{}{"ts": want}, "ts")
		if !ok || !got.Equal(want) {
			t.Errorf("RowTime = %v, %v; want %v, true", got, ok, want)
		}
	})

	t.Run("other layouts", func(t *testing.T) {
		cases := map[string]time.Time{
			"2026-07-15 10:30:45":      time.Date(2026, 7, 15, 10, 30, 45, 0, time.UTC),
			"2026-07-15":               time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC),
			"2026-07-15T10:30:45Z":     time.Date(2026, 7, 15, 10, 30, 45, 0, time.UTC),
			"2026-07-15T10:30:45.123Z": want,
		}
		for in, exp := range cases {
			got, ok := RowTime(map[string]interface{}{"ts": in}, "ts")
			if !ok || !got.Equal(exp) {
				t.Errorf("RowTime(%q) = %v, %v; want %v, true", in, got, ok, exp)
			}
		}
	})

	t.Run("absent or unusable returns false", func(t *testing.T) {
		cases := []struct {
			name string
			row  map[string]interface{}
		}{
			{"missing column", map[string]interface{}{}},
			{"nil value", map[string]interface{}{"ts": nil}},
			{"empty string", map[string]interface{}{"ts": ""}},
			{"unparseable", map[string]interface{}{"ts": "not a time"}},
			{"wrong type", map[string]interface{}{"ts": 12345}},
			{"zero time", map[string]interface{}{"ts": time.Time{}}},
		}
		for _, c := range cases {
			if got, ok := RowTime(c.row, "ts"); ok {
				t.Errorf("%s: RowTime = %v, true; want zero, false", c.name, got)
			}
		}
	})

	// The zero-time case matters: callers fold these into min/max comparisons,
	// where a zero would always win Before() and blank out a good value.
	t.Run("zero is reported as absent, not as a valid time", func(t *testing.T) {
		if _, ok := RowTime(map[string]interface{}{"ts": "0001-01-01 00:00:00.000"}, "ts"); ok {
			t.Error("zero timestamp reported as valid")
		}
	})
}

// Pins the scanRowMap contract that RowTime exists to absorb: datetime columns
// arrive as formatted strings, so a direct time.Time assertion fails. If this
// ever starts failing, scanRowMap changed and the RowTime call sites should be
// revisited (they will still work -- RowTime accepts both).
func TestScanRowMapDatetimeContract(t *testing.T) {
	formatted := time.Date(2026, 7, 15, 10, 30, 45, 123000000, time.UTC).Format(chRowTimeLayout)
	row := map[string]interface{}{"ts": formatted}

	if _, ok := row["ts"].(time.Time); ok {
		t.Error("row value asserted as time.Time; scanRowMap contract changed")
	}
	if _, ok := RowTime(row, "ts"); !ok {
		t.Error("RowTime failed on scanRowMap's own output format")
	}
}
