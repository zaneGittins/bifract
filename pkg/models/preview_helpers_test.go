package models

import (
	"math"
	"testing"
	"time"
)

// A NaN or Inf in a preview's stats (avg over an empty window) would fail the
// whole response's JSON encoding.
func TestSanitizeStats(t *testing.T) {
	s := sanitizeStats(map[string]interface{}{
		"nan": math.NaN(), "inf": math.Inf(-1), "f32": float32(math.Inf(1)),
		"ok": 1.5, "n": uint64(3), "s": "x",
	})
	if s["nan"] != float64(0) || s["inf"] != float64(0) || s["f32"] != float64(0) {
		t.Errorf("non-finite not zeroed: %v", s)
	}
	if s["ok"] != 1.5 || s["n"] != uint64(3) || s["s"] != "x" {
		t.Errorf("finite values changed: %v", s)
	}
}

func TestPreviewConfigFromEnv(t *testing.T) {
	if got := loadPreviewConfig().settings(); got != " SETTINGS max_execution_time=25, max_threads=4, max_bytes_before_external_group_by=2000000000" {
		t.Errorf("defaults: %q", got)
	}
	t.Setenv("BIFRACT_MODEL_PREVIEW_MAX_EXEC", "5")
	t.Setenv("BIFRACT_MODEL_PREVIEW_MAX_THREADS", "0") // below the floor: ignored
	t.Setenv("BIFRACT_MODEL_PREVIEW_MAX_GROUPBY_BYTES", "1000")
	c := loadPreviewConfig()
	if c.maxExecutionSec != 5 || c.maxThreads != 4 || c.maxGroupByBytes != 1000 {
		t.Errorf("overrides: %+v", c)
	}
}

func TestPreviewRangeDays(t *testing.T) {
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		span time.Duration
		want int
	}{
		{time.Hour, 1}, {24 * time.Hour, 1}, {25 * time.Hour, 2}, {7 * 24 * time.Hour, 7}, {0, 1},
	}
	for _, c := range cases {
		if got := (PreviewRange{Start: base, End: base.Add(c.span)}).days(); got != c.want {
			t.Errorf("span %v: %d days, want %d", c.span, got, c.want)
		}
	}
	r := PreviewRange{Start: base, End: base.Add(time.Hour)}
	if !r.endsNow(base.Add(time.Hour + 30*time.Second)) {
		t.Error("a range ending within the minute ends now")
	}
	if r.endsNow(base.Add(2 * time.Hour)) {
		t.Error("a range ending an hour ago is historical")
	}
}

func TestToInt64(t *testing.T) {
	for in, want := range map[interface{}]int64{int64(-2): -2, uint64(3): 3, float64(4.9): 4, "5": 0} {
		if got := toInt64(in); got != want {
			t.Errorf("toInt64(%#v) = %d, want %d", in, got, want)
		}
	}
}
