package dashboards

import (
	"encoding/json"
	"testing"
	"time"

	"bifract/pkg/storage"
)

func TestSubstituteVariables(t *testing.T) {
	vars := json.RawMessage(`[{"name":"host","value":"web01"},{"name":"empty","value":""},{"name":"","value":"ignored"}]`)
	cases := []struct {
		in, want string
	}{
		{"status=500 host=@host", "status=500 host=web01"},
		{"field=@empty", "field=*"},                       // empty value becomes wildcard
		{"no vars here", "no vars here"},                  // untouched
		{"@host and @host again", "web01 and web01 again"}, // all occurrences
	}
	for _, c := range cases {
		if got := substituteVariables(c.in, vars); got != c.want {
			t.Errorf("substituteVariables(%q) = %q, want %q", c.in, got, c.want)
		}
	}

	// Nil/empty variable set returns the query unchanged.
	if got := substituteVariables("@host", nil); got != "@host" {
		t.Errorf("nil vars: got %q, want %q", got, "@host")
	}
	// Malformed JSON returns the query unchanged rather than erroring.
	if got := substituteVariables("@host", json.RawMessage(`{bad`)); got != "@host" {
		t.Errorf("bad vars: got %q, want %q", got, "@host")
	}
}

func TestComputeTimeRange(t *testing.T) {
	approx := func(got, want time.Duration) bool {
		d := got - want
		if d < 0 {
			d = -d
		}
		return d < 2*time.Second
	}
	cases := []struct {
		typ  string
		span time.Duration
	}{
		{"last1h", time.Hour},
		{"last24h", 24 * time.Hour},
		{"last7d", 7 * 24 * time.Hour},
		{"last30d", 30 * 24 * time.Hour},
		{"", 24 * time.Hour}, // default
	}
	for _, c := range cases {
		start, end := computeTimeRange(&storage.Dashboard{TimeRangeType: c.typ})
		if !approx(end.Sub(start), c.span) {
			t.Errorf("computeTimeRange(%q) span = %s, want ~%s", c.typ, end.Sub(start), c.span)
		}
	}

	// custom uses the stored bounds verbatim.
	s := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	start, end := computeTimeRange(&storage.Dashboard{TimeRangeType: "custom", TimeRangeStart: &s, TimeRangeEnd: &e})
	if !start.Equal(s) || !end.Equal(e) {
		t.Errorf("custom range = (%s,%s), want (%s,%s)", start, end, s, e)
	}

	// custom without bounds falls back to last 24h.
	start, end = computeTimeRange(&storage.Dashboard{TimeRangeType: "custom"})
	if !approx(end.Sub(start), 24*time.Hour) {
		t.Errorf("custom fallback span = %s, want ~24h", end.Sub(start))
	}
}

func TestEffectiveInterval(t *testing.T) {
	e := NewExecutor(nil, nil, nil, nil, ExecutorConfig{MinInterval: 10 * time.Second})

	cases := []struct {
		name string
		d    *storage.Dashboard
		want time.Duration
	}{
		{"off", &storage.Dashboard{RefreshInterval: 0}, 0},
		{"fixed above floor", &storage.Dashboard{RefreshInterval: 60}, 60 * time.Second},
		{"fixed below floor clamps up", &storage.Dashboard{RefreshInterval: 3}, 10 * time.Second},
		{"auto last1h", &storage.Dashboard{RefreshInterval: -1, TimeRangeType: "last1h"}, 30 * time.Second},
		{"auto last30d", &storage.Dashboard{RefreshInterval: -1, TimeRangeType: "last30d"}, 3600 * time.Second},
	}
	for _, c := range cases {
		if got := e.effectiveInterval(c.d); got != c.want {
			t.Errorf("%s: effectiveInterval = %s, want %s", c.name, got, c.want)
		}
	}
}
