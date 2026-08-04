package alerts

import (
	"testing"
	"time"
)

// shouldSkipAlert is an optimization only: correctness comes from the cursor.
// Every ambiguous case must therefore fail open (evaluate), so these cases
// guard against a silent-no-fire regression like the process-local ingest
// signal that broke when ingestion moved to its own tier.
func TestShouldSkipAlert(t *testing.T) {
	const fid = "f1"
	now := time.Now()

	newEngine := func(fresh bool, lastIngest time.Time) *Engine {
		e := &Engine{recentIngestFresh: fresh, recentIngest: map[string]time.Time{}}
		if !lastIngest.IsZero() {
			e.recentIngest[fid] = lastIngest
		}
		return e
	}
	alert := func(mut func(*Alert)) *Alert {
		a := &Alert{FractalID: fid, AlertType: "event", LastEvaluatedAt: now.Add(-2 * time.Minute)}
		if mut != nil {
			mut(a)
		}
		return a
	}

	cases := []struct {
		name  string
		e     *Engine
		a     *Alert
		want  bool
		about string
	}{
		{"new data after cursor", newEngine(true, now.Add(-1*time.Minute)), alert(nil), false,
			"data arrived after the cursor: must evaluate"},
		{"no new data", newEngine(true, now.Add(-5*time.Minute)), alert(nil), true,
			"newest data predates the cursor: safe to skip"},
		{"fractal absent from hot", newEngine(true, time.Time{}), alert(nil), true,
			"fresh map, cursor inside hot window, no rows: genuinely idle"},
		{"stale map fails open", newEngine(false, time.Time{}), alert(nil), false,
			"refresh failed: never skip on a missing signal"},
		{"cursor older than hot window", newEngine(true, time.Time{}),
			alert(func(a *Alert) { a.LastEvaluatedAt = now.Add(-3 * time.Hour) }), false,
			"hot table cannot answer for a cursor that predates it"},
		{"never evaluated", newEngine(true, time.Time{}),
			alert(func(a *Alert) { a.LastEvaluatedAt = time.Time{} }), false,
			"zero cursor must always run"},
		{"scheduled alert", newEngine(true, now.Add(-5*time.Minute)),
			alert(func(a *Alert) { a.AlertType = "scheduled" }), false,
			"cron alerts may assert on absence of data"},
		{"prism alert", newEngine(true, now.Add(-5*time.Minute)),
			alert(func(a *Alert) { a.PrismID = "p1" }), false,
			"prism spans fractals: skip logic does not apply"},
		{"no fractal", newEngine(true, now.Add(-5*time.Minute)),
			alert(func(a *Alert) { a.FractalID = "" }), false,
			"unscoped alert cannot be matched against the map"},
	}
	for _, c := range cases {
		if got := c.e.shouldSkipAlert(c.a); got != c.want {
			t.Errorf("%s: shouldSkipAlert = %v, want %v (%s)", c.name, got, c.want, c.about)
		}
	}
}

// skipFloor bounds the recent-ingest scan. It must never be older than a cursor
// that shouldSkipAlert would actually consult, or the map could report "no new
// data" for a fractal whose data simply fell outside the scan.
func TestSkipFloor(t *testing.T) {
	now := time.Now()
	ev := func(cursor time.Time, mut func(*Alert)) *Alert {
		a := &Alert{FractalID: "f1", AlertType: "event", LastEvaluatedAt: cursor}
		if mut != nil {
			mut(a)
		}
		return a
	}

	t.Run("oldest skippable cursor wins", func(t *testing.T) {
		got := skipFloor([]*Alert{
			ev(now.Add(-2*time.Minute), nil),
			ev(now.Add(-9*time.Minute), nil),
			ev(now.Add(-5*time.Minute), nil),
		})
		if want := now.Add(-9 * time.Minute); !got.Equal(want) {
			t.Errorf("floor = %v, want %v", got, want)
		}
	})

	t.Run("never-skipped alerts do not widen the scan", func(t *testing.T) {
		old := now.Add(-90 * time.Minute)
		for name, a := range map[string]*Alert{
			"scheduled": ev(old, func(a *Alert) { a.AlertType = "scheduled" }),
			"prism":     ev(old, func(a *Alert) { a.PrismID = "p1" }),
			"unscoped":  ev(old, func(a *Alert) { a.FractalID = "" }),
			"zero":      ev(time.Time{}, nil),
			"pre-hot":   ev(now.Add(-3*time.Hour), nil),
		} {
			got := skipFloor([]*Alert{ev(now.Add(-2*time.Minute), nil), a})
			if want := now.Add(-2 * time.Minute); !got.Equal(want) {
				t.Errorf("%s: floor = %v, want %v", name, got, want)
			}
		}
	})

	t.Run("nothing skippable suppresses the query", func(t *testing.T) {
		if got := skipFloor([]*Alert{ev(time.Time{}, nil)}); !got.IsZero() {
			t.Errorf("floor = %v, want zero", got)
		}
		if got := skipFloor(nil); !got.IsZero() {
			t.Errorf("floor = %v, want zero", got)
		}
	})
}
