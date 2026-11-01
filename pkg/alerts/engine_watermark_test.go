package alerts

import (
	"context"
	"errors"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// noHold is an ingest watermark far ahead of any window, for tests about other bounds.
var noHold = time.Now().Add(100 * 365 * 24 * time.Hour)

// An event window ends at the ingest watermark: rows stamped after it may not be
// written yet, and a cursor past them would never read them.
func TestEventWindowEndsAtTheIngestWatermark(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	w := start.Add(4 * time.Minute)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: start})

	if err := e.evaluateAlertCursor(context.Background(), alert, nil, w); err != nil {
		t.Fatal(err)
	}
	q := alertQueries(ch)
	if len(q) != 1 {
		t.Fatalf("want one query, got %d", len(q))
	}
	if from, to := windowOf(t, q[0]); from != chTime(start) || to != chTime(w) {
		t.Errorf("window %s..%s, want %s..%s", from, to, chTime(start), chTime(w))
	}
	if !alert.LastEvaluatedAt.Equal(w) || len(pg.Matching(sqlLastEvaluated)) != 1 {
		t.Errorf("cursor = %v, want the watermark", alert.LastEvaluatedAt)
	}
}

// A watermark just past the cursor leaves less than the minimum window: wait.
func TestEventAlertWaitsForTheWatermark(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	start := time.Now().Add(-10 * time.Minute)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: start})
	if err := e.evaluateAlertCursor(context.Background(), alert, nil, start.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(alertQueries(ch)) != 0 || len(pg.Matching(sqlLastEvaluated)) != 0 || !alert.LastEvaluatedAt.Equal(start) {
		t.Error("an alert held by the ingest watermark must neither query nor advance")
	}
}

// A tumbling window runs only once every row in it is written.
func TestCompoundWindowWaitsForTheWatermark(t *testing.T) {
	e, _, ch := newTestEngine(t)
	window := 300
	start := time.Now().Add(-12 * time.Minute).Truncate(time.Millisecond)
	alert := parsedAlert(t, &Alert{ID: "c1", Name: "c", AlertType: "compound", FractalID: "f1",
		QueryString: `status=500 | groupby(host)`, WindowDuration: &window, LastEvaluatedAt: start})

	if err := e.evaluateCompoundAlert(context.Background(), alert, nil, start.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(alertQueries(ch)) != 0 || !alert.LastEvaluatedAt.Equal(start) {
		t.Fatal("a window ending past the watermark must wait")
	}
	if err := e.evaluateCompoundAlert(context.Background(), alert, nil, start.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(alertQueries(ch)) != 1 {
		t.Error("a window ending at the watermark is complete")
	}
}

// A scheduled run looks back from the watermark, not from now.
func TestScheduledWindowEndsAtTheWatermark(t *testing.T) {
	e, _, ch := newTestEngine(t)
	qw := 3600
	sched, err := cronParser().Parse("*/5 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	alert := parsedAlert(t, &Alert{ID: "s1", Name: "s", AlertType: "scheduled", FractalID: "f1",
		QueryString: `status=500 | count()`, QueryWindowSeconds: &qw, CronSchedule: sched,
		LastEvaluatedAt: time.Now().Add(-11 * time.Minute)})
	w := time.Now().Add(-40 * time.Second).Truncate(time.Millisecond)

	if err := e.evaluateScheduledAlert(context.Background(), alert, nil, w); err != nil {
		t.Fatal(err)
	}
	q := alertQueries(ch)
	if len(q) != 1 {
		t.Fatalf("want one query, got %d", len(q))
	}
	if from, to := windowOf(t, q[0]); to != chTime(w) || from != chTime(w.Add(-time.Hour)) {
		t.Errorf("window %s..%s, want an hour back from the watermark %s", from, to, chTime(w))
	}
}

// An unreadable watermark skips the cycle: guessing now is the bug it prevents.
func TestCycleSkipsWhenTheWatermarkIsUnknown(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	pg.answer(sqlAdvisoryLock, fakedb.Row(true))
	e.SetIngestWatermark(func(context.Context) (time.Time, error) { return time.Time{}, errors.New("pg down") })
	e.alertsCache = map[string]*Alert{"a": parsedAlert(t, &Alert{ID: "a", Name: "a", Enabled: true, FractalID: "f1", LastEvaluatedAt: time.Now().Add(-10 * time.Minute)})}
	e.cacheRefreshedAt = time.Now()

	e.evaluateAllAlerts()
	if len(alertQueries(ch)) != 0 || len(pg.Matching(sqlLastEvaluated)) != 0 {
		t.Error("no alert may evaluate or advance without a watermark")
	}
	if len(pg.Matching("pg_advisory_unlock")) != 1 {
		t.Error("the lock must be released when the cycle is skipped")
	}
}

// A cycle passes the watermark it read to every alert.
func TestCycleCapsWindowsAtTheWatermark(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	pg.answer(sqlAdvisoryLock, fakedb.Row(true))
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	w := start.Add(3 * time.Minute)
	e.SetIngestWatermark(func(context.Context) (time.Time, error) { return w, nil })
	e.alertsCache = map[string]*Alert{"a": parsedAlert(t, &Alert{ID: "a", Name: "a", Enabled: true, LastEvaluatedAt: start, PrismID: "p1"})}
	e.cacheRefreshedAt = time.Now()
	pg.answer("FROM prism_members WHERE prism_id", fakedb.Row("f1"))

	e.evaluateAllAlerts()
	q := alertQueries(ch)
	if len(q) != 1 {
		t.Fatalf("want one query, got %d", len(q))
	}
	if _, to := windowOf(t, q[0]); to != chTime(w) {
		t.Errorf("window ends %s, want the watermark %s", to, chTime(w))
	}
}
