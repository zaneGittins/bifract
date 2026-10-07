package models

import (
	"strings"
	"testing"
	"time"
)

func TestResolvePreviewRange(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	at := func(s string) *time.Time {
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return &v
	}

	r, err := ResolvePreviewRange("", nil, nil, now)
	if err != nil || r.Label != "7d" || !r.End.Equal(now) || !r.Start.Equal(now.Add(-7*24*time.Hour)) {
		t.Errorf("default preset: %+v %v", r, err)
	}
	if _, err := ResolvePreviewRange("90d", nil, nil, now); err == nil {
		t.Error("90d preset accepted: presets stay short, a long look-back is an explicit range")
	}

	// A past window: the case presets could not express.
	r, err = ResolvePreviewRange("7d", at("2026-08-29T00:00:00Z"), at("2026-09-10T00:00:00Z"), now)
	if err != nil || r.Label != "" || r.Start.Format(time.RFC3339) != "2026-08-29T00:00:00Z" || r.End.Format(time.RFC3339) != "2026-09-10T00:00:00Z" {
		t.Errorf("explicit range: %+v %v", r, err)
	}
	if r.days() != 12 || r.endsNow(now) {
		t.Errorf("explicit range: days %d endsNow %v", r.days(), r.endsNow(now))
	}

	// Offsets normalize to UTC.
	r, err = ResolvePreviewRange("", at("2026-09-01T02:00:00+02:00"), at("2026-09-02T00:00:00Z"), now)
	if err != nil || r.Start.Location() != time.UTC || r.Start.Hour() != 0 {
		t.Errorf("offset start: %+v %v", r, err)
	}

	// An end in the future is clamped to now, so it still reads as ending now.
	r, err = ResolvePreviewRange("", at("2026-10-01T00:00:00Z"), at("2026-10-09T00:00:00Z"), now)
	if err != nil || !r.End.Equal(now) || !r.endsNow(now) {
		t.Errorf("future end: %+v %v", r, err)
	}

	bad := []struct {
		name       string
		start, end *time.Time
		want       string
	}{
		{"start only", at("2026-09-01T00:00:00Z"), nil, "both"},
		{"end only", nil, at("2026-09-01T00:00:00Z"), "both"},
		{"reversed", at("2026-09-02T00:00:00Z"), at("2026-09-01T00:00:00Z"), "before"},
		{"empty", at("2026-09-01T00:00:00Z"), at("2026-09-01T00:00:00Z"), "before"},
		{"too long", at("2026-06-01T00:00:00Z"), at("2026-09-01T00:00:00Z"), "90 days"},
		{"entirely future", at("2026-10-08T00:00:00Z"), at("2026-10-09T00:00:00Z"), "before"},
	}
	for _, c := range bad {
		if _, err := ResolvePreviewRange("", c.start, c.end, now); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one mentioning %q", c.name, err, c.want)
		}
	}
	if _, err := ResolvePreviewRange("", at("2026-06-03T12:00:00Z"), at("2026-09-01T12:00:00Z"), now); err != nil {
		t.Errorf("exactly 90 days refused: %v", err)
	}
}

func TestNetPreviewBoundsSQL(t *testing.T) {
	b := NetPreviewBounds{Start: time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC), Days: 12}
	q, err := BuildNetPreviewAgg(ModelDefinition{}, ModelTypeBeacon, "logs", "f", b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "timestamp >= toDateTime64('2026-08-29 00:00:00.000', 3, 'UTC') AND timestamp < toDateTime64('2026-09-10 00:00:00.000', 3, 'UTC')") {
		t.Errorf("network preview is not bounded by the range:\n%s", q)
	}
	if strings.Contains(q, "now()") {
		t.Errorf("network preview still reads relative to now:\n%s", q)
	}
	// The strobe limit follows the range length (12 days of seconds).
	if !strings.Contains(q, "cnt < 1036800") {
		t.Errorf("strobe limit does not follow the range:\n%s", q)
	}
}

func TestSplitDistRows(t *testing.T) {
	rows := []map[string]interface{}{
		{"c": int64(95), "p": int64(20), "h": int64(9), "s": int64(1), "n": int64(3)},
		{"c": int64(95), "p": int64(20), "h": int64(9), "s": int64(1), "n": int64(2)},
		{"c": int64(50), "p": int64(400), "h": int64(4), "s": int64(0), "n": int64(7)},
	}
	hist, cells := splitDistRows(rows, rarityHistLabels, []string{"c", "p"}, "s")
	if hist[9].Count != 5 || hist[4].Count != 7 {
		t.Errorf("histogram keeps every scored row: %+v", hist)
	}
	if len(cells) != 1 || cells[0][0] != 95 || cells[0][1] != 20 || cells[0][2] != 5 {
		t.Errorf("cells merge equal bins and drop rows below min days seen: %v", cells)
	}
}

func TestDistSQLBinsOnThresholdGrid(t *testing.T) {
	r := rarityDistSQL("SELECT 1", 3)
	for _, want := range []string{"ceil(round(confidence * 100, 6))", "floor(round(percent * 10, 6))", "model_total >= 3", "LIMIT 50001"} {
		if !strings.Contains(r, want) {
			t.Errorf("rarity distribution missing %q", want)
		}
	}
	v := volumeDistSQL("SELECT 1")
	if !strings.Contains(v, "ceil(round(greatest(least(z_score, 1000), -1000) * 10, 6))") {
		t.Errorf("volume distribution bins:\n%s", v)
	}
}
