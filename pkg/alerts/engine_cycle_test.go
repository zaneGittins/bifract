package alerts

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/parser"
)

func parsedAlert(t *testing.T, a *Alert) *Alert {
	t.Helper()
	if a.QueryString == "" {
		a.QueryString = `status=500`
	}
	p, err := parser.ParseQuery(a.QueryString)
	if err != nil {
		t.Fatal(err)
	}
	a.ParsedQuery = p
	return a
}

var ingestBounds = regexp.MustCompile(`ingest_timestamp >= '([^']+)' AND ingest_timestamp (<=?) '([^']+)'`)

// windowOf extracts the ingest window an alert query read.
func windowOf(t *testing.T, sql string) (from, to string) {
	t.Helper()
	from, _, to = boundsOf(t, sql)
	return from, to
}

// boundsOf extracts the ingest window and the operator bounding its end.
func boundsOf(t *testing.T, sql string) (from, endOp, to string) {
	t.Helper()
	m := ingestBounds.FindStringSubmatch(sql)
	if m == nil {
		t.Fatalf("query is not bounded on ingest_timestamp: %s", sql)
	}
	return m[1], m[2], m[3]
}

// alertQueries are the window reads, not the recent-ingest probe, whose bound is a
// toDateTime64 call rather than a literal.
func alertQueries(ch *fakedb.CH) []string { return ch.QueriedMatching("ingest_timestamp >= '") }

// The cursor is the only thing that decides what an event alert reads. Catching up
// from a cursor older than the window cap must advance in slices that abut: the
// second tick starts exactly where the first ended, so no log falls between them.
func TestCursorCatchesUpInAbuttingSlices(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	start := time.Now().Add(-45 * time.Minute).Truncate(time.Millisecond)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: start})

	for tick := 0; tick < 2; tick++ {
		if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
	}

	queries := alertQueries(ch)
	if len(queries) != 2 {
		t.Fatalf("want one query per tick, got %d", len(queries))
	}
	from1, to1 := windowOf(t, queries[0])
	from2, _ := windowOf(t, queries[1])
	if from1 != chTime(start) {
		t.Errorf("tick 1 starts at %s, want the cursor %s", from1, chTime(start))
	}
	if to1 != chTime(start.Add(maxEvalWindow)) {
		t.Errorf("tick 1 ends at %s, want the cursor plus the %v cap", to1, maxEvalWindow)
	}
	if from2 != to1 {
		t.Errorf("tick 2 starts at %s but tick 1 ended at %s: logs between them are never read", from2, to1)
	}
	// Abutting windows share the boundary, so each must exclude its end or a row
	// stamped on it is read by both.
	for i, q := range queries {
		if _, op, _ := boundsOf(t, q); op != "<" {
			t.Errorf("tick %d bounds its end with %q, want an exclusive <", i+1, op)
		}
	}
	// Routed to the hot table: the cursor is inside its retention.
	if !strings.Contains(queries[0], "logs_hot") {
		t.Errorf("a recent cursor reads logs_hot: %s", queries[0])
	}

	// Each advance is persisted with the window end and the query latency.
	writes := pg.args(sqlLastEvaluated)
	if len(writes) != 2 {
		t.Fatalf("want a cursor write per tick, got %d", len(writes))
	}
	if writes[0][0] != "a1" || !writes[0][1].(time.Time).Equal(start.Add(maxEvalWindow)) {
		t.Errorf("first write = %v", writes[0])
	}
	if !alert.LastEvaluatedAt.Equal(writes[1][1].(time.Time)) || alert.LastExecutionTimeMs == nil {
		t.Errorf("the in-memory cursor must follow the persisted one: %v vs %v", alert.LastEvaluatedAt, writes[1][1])
	}
}

func chTime(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000") }

// A cursor older than the hot table's retention must read the full table, or the
// catch-up window is silently empty.
func TestOldCursorReadsTheFullTable(t *testing.T) {
	e, _, ch := newTestEngine(t)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: time.Now().Add(-3 * time.Hour)})
	if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err != nil {
		t.Fatal(err)
	}
	q := alertQueries(ch)
	if len(q) != 1 || strings.Contains(q[0], "logs_hot") || !strings.Contains(q[0], "logs") {
		t.Errorf("want one query on logs, got %v", q)
	}
}

// A never-evaluated alert starts five minutes back rather than at the epoch, which
// would be a full-table scan on its first tick.
func TestZeroCursorStartsFiveMinutesBack(t *testing.T) {
	e, _, ch := newTestEngine(t)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1"})
	before := time.Now()
	if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err != nil {
		t.Fatal(err)
	}
	q := alertQueries(ch)
	if len(q) != 1 {
		t.Fatalf("want one query, got %d", len(q))
	}
	from, _ := windowOf(t, q[0])
	got, err := time.Parse("2006-01-02 15:04:05.000", from)
	if err != nil {
		t.Fatal(err)
	}
	if d := before.Add(-5 * time.Minute).Sub(got); d < -time.Second || d > time.Second {
		t.Errorf("first window starts at %v, want about five minutes before %v", got, before)
	}
}

// A failed query must leave the cursor where it was, so the next tick re-reads the
// same window instead of skipping it.
func TestCursorHoldsWhenTheQueryFails(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	ch.On("ingest_timestamp", fakedb.CHResult{Err: errors.New("connection reset")})
	start := time.Now().Add(-10 * time.Minute)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: start})

	err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold)
	if err == nil || !strings.Contains(err.Error(), "query failed") {
		t.Fatalf("err = %v", err)
	}
	if !alert.LastEvaluatedAt.Equal(start) {
		t.Error("the in-memory cursor moved past a window that was never read")
	}
	if n := len(pg.Matching(sqlLastEvaluated)); n != 0 {
		t.Errorf("the cursor was persisted %d times after a failed query", n)
	}
	if n := len(pg.Matching(sqlDisable)); n != 0 {
		t.Error("a transient failure must not disable the alert")
	}
}

// Delivery is at-least-once: when the cursor cannot be persisted the matches are
// still delivered, and the in-memory cursor stays put so the window is read again.
func TestCursorWriteFailureDeliversAndRereads(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	pg.answer(sqlLastEvaluated, fakedb.PGResult{Err: errors.New("pg down")})
	ch.On("ingest_timestamp", fakedb.CHResult{Cols: []fakedb.Col{{Column: "log_id", Type: "String"}}, Rows: [][]any{{"l1"}}})
	start := time.Now().Add(-10 * time.Minute)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: start})

	if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err != nil {
		t.Fatal(err)
	}
	if !alert.LastEvaluatedAt.Equal(start) {
		t.Error("the in-memory cursor advanced although Postgres did not")
	}
	if n := len(pg.Matching(sqlExecution)); n != 1 {
		t.Errorf("the match must still be delivered and recorded, got %d execution rows", n)
	}
}

func TestShortWindowIsNotEvaluated(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: time.Now().Add(-20 * time.Second)})
	if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err != nil {
		t.Fatal(err)
	}
	if len(alertQueries(ch)) != 0 || len(pg.Matching(sqlLastEvaluated)) != 0 {
		t.Error("a window under the minimum must be left for a later tick")
	}
}

// A model's state trails ingestion, so an alert consulting one never evaluates past
// its watermark: the window is capped there, held entirely when the watermark is
// unknown, and skipped when what remains is under the minimum window.
func TestModelWatermarkCapsTheWindow(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	alert := &Alert{ID: "a1", QueryString: `* | model_lookup(model="logins", key=[user])`}
	to := time.Now()
	watermark := to.Add(-5 * time.Minute)

	pg.answer("FROM analytics_models WHERE name = ANY", fakedb.Row(watermark))
	if got := e.modelWatermark(context.Background(), alert, to); !got.Equal(watermark) {
		t.Errorf("capped at %v, want the watermark %v", got, watermark)
	}
	if args := pg.args("FROM analytics_models"); args[0][0] != `{"logins"}` {
		t.Errorf("watermark lookup args = %v", args)
	}
	pg.answer("FROM analytics_models WHERE name = ANY", fakedb.Row(to.Add(time.Hour)))
	if got := e.modelWatermark(context.Background(), alert, to); !got.Equal(to) {
		t.Errorf("a watermark ahead of the window must not extend it: %v", got)
	}
	pg.answer("FROM analytics_models WHERE name = ANY", fakedb.Row(nil))
	if got := e.modelWatermark(context.Background(), alert, to); !got.Equal(to) {
		t.Errorf("an unknown model is not held: %v", got)
	}
	pg.answer("FROM analytics_models WHERE name = ANY", fakedb.PGResult{Err: errors.New("boom")})
	if got := e.modelWatermark(context.Background(), alert, to); !got.IsZero() {
		t.Errorf("an unreadable watermark must hold the alert, got %v", got)
	}

	// Held at a watermark just past the cursor, the window is too short to run.
	start := time.Now().Add(-10 * time.Minute)
	pg.answer("FROM analytics_models WHERE name = ANY", fakedb.Row(start.Add(30*time.Second)))
	alert = parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: start, QueryString: alert.QueryString})
	if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err != nil {
		t.Fatal(err)
	}
	if len(ch.Queried()) != 0 || !alert.LastEvaluatedAt.Equal(start) {
		t.Error("an alert held by its model's watermark must neither query nor advance")
	}
}

// A query ClickHouse rejects as malformed will fail the same way forever, so it is
// disabled with the reason recorded and the operator told, and the cursor stays.
func TestUnrecoverableErrorDisablesWithReason(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	notes := newRecordingNotifier()
	e.SetNotificationWriter(notes)
	ch.On("ingest_timestamp", fakedb.CHResult{Err: fakedb.Exception(46, "Unknown function frobnicate")})
	start := time.Now().Add(-10 * time.Minute)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "Bad BQL", FractalID: "f1", LastEvaluatedAt: start})

	err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold)
	if err == nil || !strings.Contains(err.Error(), "code 46") {
		t.Fatalf("err = %v", err)
	}
	disables := pg.args(sqlDisable)
	if len(disables) != 1 {
		t.Fatalf("want one disable, got %d", len(disables))
	}
	if reason := disables[0][0].(string); !strings.Contains(reason, "unrecoverable query error (code 46)") {
		t.Errorf("reason = %q", reason)
	}
	if disables[0][1] != "a1" {
		t.Errorf("disabled %v", disables[0][1])
	}
	if len(pg.Matching(sqlCacheRefresh)) == 0 {
		t.Error("disabling must refresh the cache so the alert stops evaluating")
	}
	if got := notes.wait(t, 1); !strings.HasPrefix(got[0], "alerts.unrecoverable_error|Alert Auto-Disabled: Bad BQL") {
		t.Errorf("notification = %v", got)
	}
	if !alert.LastEvaluatedAt.Equal(start) || len(pg.Matching(sqlLastEvaluated)) != 0 {
		t.Error("a failed evaluation must not advance the cursor")
	}
}

// When the disable itself fails nobody is told the alert was disabled, because it
// was not.
func TestUnrecoverableErrorWithFailedDisableDoesNotNotify(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	notes := newRecordingNotifier()
	e.SetNotificationWriter(notes)
	pg.answer(sqlDisable, fakedb.PGResult{Err: errors.New("pg down")})
	ch.On("ingest_timestamp", fakedb.CHResult{Err: fakedb.Exception(62, "Syntax error")})
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: time.Now().Add(-10 * time.Minute)})

	if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err == nil {
		t.Fatal("want the query error")
	}
	time.Sleep(50 * time.Millisecond)
	if notes.count() != 0 {
		t.Error("a notification claimed a disable that did not happen")
	}
}

// A schema missing one of Bifract's own columns is a deployment fault: the alert
// stays enabled, the operator hears about it once, and a recovery re-arms the notice.
func TestMissingBaseColumnLeavesTheAlertEnabled(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	notes := newRecordingNotifier()
	e.SetNotificationWriter(notes)
	broken := fakedb.CHResult{Err: fakedb.Exception(47, "Unknown expression identifier `ingest_timestamp` in scope SELECT")}
	ch.On("ingest_timestamp", broken)
	opts := parser.QueryOptions{StartTime: time.Now().Add(-time.Minute), EndTime: time.Now(), UseIngestTimestamp: true, FractalID: "f1", TableName: "logs"}
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "Schema", FractalID: "f1"})

	for i := 0; i < 3; i++ {
		_, err := e.runAlertQuery(context.Background(), alert, opts, 5, opts.StartTime)
		if err == nil || !strings.Contains(err.Error(), `missing base column "ingest_timestamp"`) {
			t.Fatalf("run %d: err = %v", i, err)
		}
	}
	if got := notes.wait(t, 1); !strings.HasPrefix(got[0], "alerts.schema_error|Alert Query Blocked: Schema") {
		t.Errorf("notification = %v", got)
	}
	if len(pg.Matching(sqlDisable)) != 0 {
		t.Error("a schema fault must never disable the alert")
	}
	time.Sleep(50 * time.Millisecond)
	if n := notes.count(); n != 1 {
		t.Fatalf("a persisting fault notified %d times", n)
	}

	// Recovery clears the latch; the next break is announced again.
	ch.ClearRules()
	if _, err := e.runAlertQuery(context.Background(), alert, opts, 5, opts.StartTime); err != nil {
		t.Fatal(err)
	}
	ch.On("ingest_timestamp", broken)
	_, _ = e.runAlertQuery(context.Background(), alert, opts, 5, opts.StartTime)
	if got := notes.wait(t, 1); len(got) != 2 {
		t.Errorf("want a second notice after recovery, got %v", got)
	}
}

// A query that only fails to translate never reaches ClickHouse and is not disabled.
func TestUntranslatableQueryIsReportedNotDisabled(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", QueryString: `* | model_lookup(model="nope", key=[user])`})
	opts := parser.QueryOptions{StartTime: time.Now().Add(-time.Minute), EndTime: time.Now(), UseIngestTimestamp: true, FractalID: "f1", TableName: "logs"}
	_, err := e.runAlertQuery(context.Background(), alert, opts, 5, time.Now())
	if err == nil || !strings.Contains(err.Error(), "translate query") {
		t.Fatalf("err = %v", err)
	}
	if len(ch.Queried()) != 0 || len(pg.Matching(sqlDisable)) != 0 {
		t.Error("nothing should run or be disabled")
	}
}

// A compound alert evaluates whole tumbling windows and advances by exactly one.
func TestCompoundAlertAdvancesByOneWindow(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	window := 300
	start := time.Now().Add(-12 * time.Minute).Truncate(time.Millisecond)
	alert := parsedAlert(t, &Alert{ID: "c1", Name: "c", AlertType: "compound", FractalID: "f1",
		QueryString: `status=500 | groupby(host)`, WindowDuration: &window, LastEvaluatedAt: start})

	if err := e.evaluateCompoundAlert(context.Background(), alert, nil, noHold); err != nil {
		t.Fatal(err)
	}
	q := alertQueries(ch)
	if len(q) != 1 {
		t.Fatalf("want one query, got %d", len(q))
	}
	from, to := windowOf(t, q[0])
	if from != chTime(start) || to != chTime(start.Add(5*time.Minute)) {
		t.Errorf("window %s..%s, want one tumbling window from the cursor", from, to)
	}
	if !alert.LastEvaluatedAt.Equal(start.Add(5 * time.Minute)) {
		t.Errorf("cursor = %v", alert.LastEvaluatedAt)
	}
	if len(pg.Matching(sqlLastEvaluated)) != 1 {
		t.Error("the window end must be persisted")
	}

	// A compound alert's failure surfaces without moving the cursor.
	ch.On("ingest_timestamp", fakedb.CHResult{Err: errors.New("down")})
	before := alert.LastEvaluatedAt
	if err := e.evaluateCompoundAlert(context.Background(), alert, nil, noHold); err == nil {
		t.Fatal("want the query error")
	}
	if !alert.LastEvaluatedAt.Equal(before) {
		t.Error("cursor moved on a failed window")
	}
}

// A scheduled alert runs when its cron fires and reads a fixed window back from now.
func TestScheduledAlertReadsItsQueryWindow(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	qw := 3600
	sched, err := cronParser().Parse("*/5 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	alert := parsedAlert(t, &Alert{ID: "s1", Name: "s", AlertType: "scheduled", FractalID: "f1",
		QueryString: `status=500 | count()`, QueryWindowSeconds: &qw, CronSchedule: sched,
		LastEvaluatedAt: time.Now().Add(-11 * time.Minute)})

	before := time.Now()
	if err := e.evaluateScheduledAlert(context.Background(), alert, nil, noHold); err != nil {
		t.Fatal(err)
	}
	q := alertQueries(ch)
	if len(q) != 1 {
		t.Fatalf("want one query, got %d", len(q))
	}
	from, _ := windowOf(t, q[0])
	got, _ := time.Parse("2006-01-02 15:04:05.000", from)
	if d := before.Add(-time.Hour).Sub(got); d < -2*time.Second || d > 2*time.Second {
		t.Errorf("window starts %v, want query_window_seconds before now", got)
	}
	// The run time, to the whole millisecond the window ends on.
	if len(pg.Matching(sqlLastEvaluated)) != 1 || alert.LastEvaluatedAt.Before(before.Truncate(time.Millisecond)) {
		t.Error("a run stamps the cursor at the run time")
	}

	// Not due again until the next cron tick.
	if err := e.evaluateScheduledAlert(context.Background(), alert, nil, noHold); err != nil {
		t.Fatal(err)
	}
	if len(alertQueries(ch)) != 1 {
		t.Error("ran again before the schedule was due")
	}
}

// A prism alert reads every member fractal, resolved once per cycle.
func TestPrismAlertResolvesMembersOncePerCycle(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	pg.answer("FROM prism_members WHERE prism_id", fakedb.RowsOf([]driver.Value{"f1"}, []driver.Value{"f2"}))
	cache := &prismResolveCache{entries: map[string][]string{}}
	for i, id := range []string{"p-a1", "p-a2"} {
		alert := parsedAlert(t, &Alert{ID: id, Name: id, PrismID: "p1", LastEvaluatedAt: time.Now().Add(-time.Duration(10+i) * time.Minute)})
		if err := e.evaluateAlertCursor(context.Background(), alert, cache, noHold); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(pg.Matching("FROM prism_members")); n != 1 {
		t.Errorf("members resolved %d times in one cycle", n)
	}
	for _, q := range alertQueries(ch) {
		if !strings.Contains(q, "'f1'") || !strings.Contains(q, "'f2'") {
			t.Errorf("a prism alert must read both members: %s", q)
		}
	}
}

// A prism with no members has nothing to read: the alert errors rather than
// running unscoped.
func TestEmptyPrismRefusesToEvaluate(t *testing.T) {
	e, _, ch := newTestEngine(t)
	alert := parsedAlert(t, &Alert{ID: "a", Name: "a", PrismID: "p-empty", LastEvaluatedAt: time.Now().Add(-10 * time.Minute)})
	err := e.evaluateAlertCursor(context.Background(), alert, &prismResolveCache{entries: map[string][]string{}}, noHold)
	if err == nil || !strings.Contains(err.Error(), "no member fractals") {
		t.Fatalf("err = %v", err)
	}
	if len(ch.Queried()) != 0 {
		t.Error("an empty prism must not run a query")
	}
}

func TestResolvePrismFractalIDsReportsLookupFailure(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	pg.answer("FROM prism_members", fakedb.PGResult{Err: errors.New("boom")})
	if _, err := e.resolvePrismFractalIDs(context.Background(), "p1", nil); err == nil || !strings.Contains(err.Error(), "resolve prism members") {
		t.Errorf("err = %v", err)
	}
}

// One replica evaluates at a time: without the advisory lock nothing runs.
func TestEvaluateAllAlertsNeedsTheLock(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	pg.answer(sqlAdvisoryLock, fakedb.Row(false))
	e.alertsCache = map[string]*Alert{"a": parsedAlert(t, &Alert{ID: "a", Name: "a", Enabled: true, FractalID: "f1", LastEvaluatedAt: time.Now().Add(-10 * time.Minute)})}
	e.cacheRefreshedAt = time.Now()

	e.evaluateAllAlerts()
	if len(ch.Queried()) != 0 {
		t.Error("evaluated without holding the lock")
	}
}

// Active ingestion defers the whole cycle; the cursor catches up afterwards.
func TestEvaluateAllAlertsDefersUnderIngestPressure(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	pg.answer(sqlAdvisoryLock, fakedb.Row(true))
	e.SetIngestPressureFunc(func() bool { return true })
	e.alertsCache = map[string]*Alert{"a": parsedAlert(t, &Alert{ID: "a", Name: "a", Enabled: true, FractalID: "f1", LastEvaluatedAt: time.Now().Add(-10 * time.Minute)})}
	e.cacheRefreshedAt = time.Now()

	e.evaluateAllAlerts()
	if len(ch.Queried()) != 0 || len(pg.Matching(sqlLastEvaluated)) != 0 {
		t.Error("a deferred cycle must not touch any alert")
	}
	if len(pg.Matching("pg_advisory_unlock")) != 1 {
		t.Error("the lock must be released when the cycle is deferred")
	}
}

// A full cycle evaluates each enabled alert by its type, skips one whose fractal
// has had no new data, and releases the lock.
func TestEvaluateAllAlertsDispatchesByType(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	pg.answer(sqlAdvisoryLock, fakedb.Row(true))
	recent := time.Now().Add(-10 * time.Minute)
	window := 300
	e.alertsCache = map[string]*Alert{
		"ev":   parsedAlert(t, &Alert{ID: "ev", Name: "ev", Enabled: true, FractalID: "busy", LastEvaluatedAt: recent}),
		"idle": parsedAlert(t, &Alert{ID: "idle", Name: "idle", Enabled: true, FractalID: "quiet", LastEvaluatedAt: recent}),
		"cmp": parsedAlert(t, &Alert{ID: "cmp", Name: "cmp", Enabled: true, AlertType: "compound", FractalID: "busy",
			QueryString: `* | groupby(host)`, WindowDuration: &window, LastEvaluatedAt: recent}),
		"off": parsedAlert(t, &Alert{ID: "off", Name: "off", Enabled: false, FractalID: "busy", LastEvaluatedAt: recent}),
	}
	e.cacheRefreshedAt = time.Now()
	// Only "busy" has had data since the cursors.
	ch.On("max(ingest_timestamp) AS last_ingest", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "fractal_id", Type: "String"}, {Column: "last_ingest", Type: "DateTime64(3)"}},
		Rows: [][]any{{"busy", time.Now()}},
	})

	e.evaluateAllAlerts()

	evaluated := map[string]bool{}
	for _, args := range pg.args(sqlLastEvaluated) {
		evaluated[args[0].(string)] = true
	}
	if !evaluated["ev"] || !evaluated["cmp"] {
		t.Errorf("evaluated %v, want ev and cmp", evaluated)
	}
	if evaluated["idle"] {
		t.Error("an alert whose fractal had no new data must be skipped")
	}
	if evaluated["off"] {
		t.Error("a disabled alert was evaluated")
	}
	if len(pg.Matching("pg_advisory_unlock")) != 1 {
		t.Error("the lock must be released after the cycle")
	}
	if e.IsRunning() {
		t.Error("the overlap guard must be released after the cycle")
	}
}

// Consecutive failures trip the breaker so a broken ClickHouse is not hammered by
// every remaining alert, and the operator is told.
func TestCircuitBreakerStopsTheCycle(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	notes := newRecordingNotifier()
	e.SetNotificationWriter(notes)
	pg.answer(sqlAdvisoryLock, fakedb.Row(true))
	ch.On("ingest_timestamp >=", fakedb.CHResult{Err: errors.New("too many simultaneous queries")})
	ch.On("last_ingest", fakedb.CHResult{Err: errors.New("too many simultaneous queries")})

	e.alertsCache = map[string]*Alert{}
	for i := 0; i < 40; i++ {
		id := "a" + string(rune('A'+i))
		e.alertsCache[id] = parsedAlert(t, &Alert{ID: id, Name: id, Enabled: true, FractalID: "f1", LastEvaluatedAt: time.Now().Add(-10 * time.Minute)})
	}
	e.cacheRefreshedAt = time.Now()

	e.evaluateAllAlerts()
	if n := len(alertQueries(ch)); n >= 40 {
		t.Errorf("the breaker never tripped: %d of 40 alerts queried", n)
	}
	if got := notes.wait(t, 1); !strings.HasPrefix(got[0], "alerts.circuit_breaker|") {
		t.Errorf("notification = %v", got)
	}
}

// A cycle already in progress makes the next tick a no-op.
func TestEvaluateAllAlertsDoesNotOverlap(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	e.running.Store(true)
	e.evaluateAllAlerts()
	if len(pg.Matching(sqlAdvisoryLock)) != 0 {
		t.Error("an overlapping cycle must not even take the lock")
	}
}

// The cache refresh parses queries and cron once, drops what cannot run, and
// disables a scheduled alert whose cron does not parse.
func TestRefreshAlertsCache(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	qw := int64(600)
	pg.answer(sqlCacheRefresh, fakedb.RowsOf(
		alertSpec{ID: "ok", Name: "ok", Enabled: true, FractalID: "f1", ThrottleSecs: 60, ThrottleField: "host",
			Webhooks: `[{"id":"w1","name":"hook","enabled":true}]`}.cacheRow(),
		alertSpec{ID: "off", Name: "off", Enabled: false, FractalID: "f1"}.cacheRow(),
		alertSpec{ID: "unparsable", Name: "unparsable", Enabled: true, FractalID: "f1", Query: `status=(`}.cacheRow(),
		alertSpec{ID: "sched", Name: "sched", Enabled: true, FractalID: "f1", Type: "scheduled", Cron: "0 * * * *", QueryWindow: &qw, Query: `* | count()`}.cacheRow(),
		alertSpec{ID: "badcron", Name: "badcron", Enabled: true, FractalID: "f1", Type: "scheduled", Cron: "every day", QueryWindow: &qw}.cacheRow(),
	))

	enabled, err := e.refreshAlertsCache(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]*Alert{}
	for _, a := range enabled {
		ids[a.ID] = a
	}
	if len(ids) != 2 || ids["ok"] == nil || ids["sched"] == nil {
		t.Fatalf("enabled = %v, want ok and sched", ids)
	}
	if ids["ok"].ParsedQuery == nil || len(ids["ok"].WebhookActions) != 1 || ids["ok"].WebhookActions[0].ID != "w1" {
		t.Errorf("ok alert = %+v", ids["ok"])
	}
	if ids["sched"].CronSchedule == nil {
		t.Error("a scheduled alert's cron is parsed at load")
	}
	if e.CachedAlertCount() != 3 {
		t.Errorf("cache holds %d, want ok, off and sched", e.CachedAlertCount())
	}

	disables := pg.args(sqlDisable)
	if len(disables) != 1 || disables[0][1] != "badcron" || !strings.Contains(disables[0][0].(string), "invalid cron expression") {
		t.Errorf("disables = %v", disables)
	}
}

// A row that keeps failing to load must not make the refresh call itself: the
// disable used to refresh the cache from inside the refresh, which re-read the same
// row and recursed until the stack overflowed. Once disabled it is left alone.
func TestInvalidCronDoesNotRecurseOrRewriteADisabledAlert(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	qw := int64(600)
	row := alertSpec{ID: "badcron", Name: "badcron", Enabled: true, FractalID: "f1", Type: "scheduled", Cron: "every day", QueryWindow: &qw}
	pg.answer(sqlCacheRefresh, fakedb.RowsOf(row.cacheRow()))

	if err := e.RefreshAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(pg.Matching(sqlCacheRefresh)); n != 1 {
		t.Errorf("one refresh read the table %d times", n)
	}
	if n := len(pg.Matching(sqlDisable)); n != 1 {
		t.Fatalf("want one disable, got %d", n)
	}

	row.Enabled = false
	pg.answer(sqlCacheRefresh, fakedb.RowsOf(row.cacheRow()))
	if err := e.RefreshAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(pg.Matching(sqlDisable)); n != 1 {
		t.Errorf("an already disabled alert was disabled again (%d writes)", n)
	}
}

func TestRefreshAlertsCacheFailures(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	pg.answer(sqlCacheRefresh, fakedb.PGResult{Err: errors.New("pg down")})
	if err := e.RefreshAlerts(context.Background()); err == nil {
		t.Error("want the query error")
	}

	bad := alertSpec{ID: "x", Name: "x", Enabled: true, FractalID: "f1", Webhooks: `{not json`}.cacheRow()
	pg.answer(sqlCacheRefresh, fakedb.RowsOf(bad))
	if err := e.RefreshAlerts(context.Background()); err == nil || !strings.Contains(err.Error(), "webhook actions") {
		t.Errorf("err = %v", err)
	}
}

// Retention runs at most hourly, drops executions past the retention window and
// expired throttles.
func TestMaybeRunRetention(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	pg.answer("DELETE FROM alert_executions", fakedb.Affected(3))
	pg.answer("DELETE FROM alert_throttles", fakedb.Affected(2))

	e.maybeRunRetention()
	e.maybeRunRetention()

	runs := pg.args("DELETE FROM alert_executions")
	if len(runs) != 1 {
		t.Fatalf("retention ran %d times in one interval", len(runs))
	}
	if runs[0][0] != int64(retentionDays) {
		t.Errorf("retention days = %v", runs[0][0])
	}
	if len(pg.Matching("DELETE FROM alert_throttles")) != 1 {
		t.Error("expired throttles must be cleaned with the executions")
	}

	// A failed execution cleanup skips the throttle cleanup until the next run.
	e2, pg2, _ := newTestEngine(t)
	pg2.answer("DELETE FROM alert_executions", fakedb.PGResult{Err: errors.New("boom")})
	e2.maybeRunRetention()
	if len(pg2.Matching("DELETE FROM alert_throttles")) != 0 {
		t.Error("throttle cleanup ran after the execution cleanup failed")
	}
}

// The execution row mirrors the alert's scope, and an alert deleted mid-evaluation
// is not an error.
func TestRecordExecution(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	if err := e.recordExecution(context.Background(), "a1", "", "p1", 4, 1, false, "host (2 values)", 12,
		[]WebhookResult{{WebhookName: "hook", Success: true}}, nil, []EmailResult{}); err != nil {
		t.Fatal(err)
	}
	args := pg.args(sqlExecution)[0]
	if args[1] != nil || args[2] != "p1" {
		t.Errorf("scope = %v/%v, want a prism-only row", args[1], args[2])
	}
	if args[3] != int64(4) || args[4] != int64(1) || args[5] != false || args[6] != "host (2 values)" {
		t.Errorf("counts = %v", args[3:7])
	}
	if !strings.Contains(args[8].(string), `"hook"`) || args[9] != "null" {
		t.Errorf("results = %v %v", args[8], args[9])
	}

	pg.answer(sqlExecution, fakedb.PGResult{Err: pqErr("23503")})
	if err := e.recordExecution(context.Background(), "gone", "f1", "", 1, 0, false, "", 1, nil, nil, nil); err != nil {
		t.Errorf("a deleted alert's execution is dropped quietly, got %v", err)
	}
	pg.answer(sqlExecution, fakedb.PGResult{Err: errors.New("disk full")})
	if err := e.recordExecution(context.Background(), "a1", "f1", "", 1, 0, false, "", 1, nil, nil, nil); err == nil {
		t.Error("any other failure is reported")
	}
}

// A row whose ingest time sits exactly on the boundary between two consecutive
// windows is read by exactly one of them, so it is delivered once. The fake answers
// from the bounds the query actually rendered, as ClickHouse would.
func TestBoundaryRowIsDeliveredOnce(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	start := time.Now().Add(-45 * time.Minute).Truncate(time.Millisecond)
	boundary := chTime(start.Add(maxEvalWindow))
	ch.Respond(func(q string, _ []any) (fakedb.CHResult, bool) {
		m := ingestBounds.FindStringSubmatch(q)
		if m == nil {
			return fakedb.CHResult{}, false
		}
		from, op, to := m[1], m[2], m[3]
		// Same fixed-width format, so the literals order as strings.
		inside := from <= boundary && (boundary < to || (op == "<=" && boundary == to))
		if !inside {
			return fakedb.CHResult{}, true
		}
		return fakedb.CHResult{Cols: []fakedb.Col{{Column: "log_id", Type: "String"}}, Rows: [][]any{{"on-boundary"}}}, true
	})
	alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: start})

	for tick := 0; tick < 2; tick++ {
		if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
	}
	if n := len(alertQueries(ch)); n != 2 {
		t.Fatalf("want two consecutive windows, got %d queries", n)
	}
	if n := len(pg.Matching(sqlExecution)); n != 1 {
		t.Errorf("a row on the window boundary was delivered %d times, want once", n)
	}
}

// Every alert window ends on a whole millisecond, exclusive. The stored cursor is
// that same value: the query literal is milliseconds and Postgres keeps
// microseconds, so a finer end would be read at its floor but stored rounded, and
// the next window could start past a millisecond nothing read.
func TestWindowEndIsAWholeMillisecondAndExclusive(t *testing.T) {
	wholeMs := func(t *testing.T, what string, v time.Time) {
		t.Helper()
		if !v.Equal(v.Truncate(time.Millisecond)) {
			t.Errorf("%s %v is not a whole millisecond", what, v)
		}
	}
	// A cursor with a sub-millisecond remainder, as an older build may have stored.
	cursor := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond).Add(456789 * time.Nanosecond)

	t.Run("event", func(t *testing.T) {
		e, pg, ch := newTestEngine(t)
		alert := parsedAlert(t, &Alert{ID: "a1", Name: "n", FractalID: "f1", LastEvaluatedAt: cursor})
		if err := e.evaluateAlertCursor(context.Background(), alert, nil, noHold); err != nil {
			t.Fatal(err)
		}
		q := alertQueries(ch)
		if len(q) != 1 {
			t.Fatalf("want one query, got %d", len(q))
		}
		from, op, to := boundsOf(t, q[0])
		if op != "<" {
			t.Errorf("end operator %q, want <", op)
		}
		if from != chTime(cursor) {
			t.Errorf("starts at %s, want the cursor %s", from, chTime(cursor))
		}
		writes := pg.args(sqlLastEvaluated)
		if len(writes) != 1 {
			t.Fatalf("want one cursor write, got %d", len(writes))
		}
		stored := writes[0][1].(time.Time)
		wholeMs(t, "stored cursor", stored)
		if chTime(stored) != to {
			t.Errorf("stored cursor %s, but the query read up to %s", chTime(stored), to)
		}
		if !stored.Equal(stored.Round(time.Microsecond)) {
			t.Error("the stored cursor does not survive Postgres' microsecond rounding unchanged")
		}
		if !alert.LastEvaluatedAt.Equal(stored) {
			t.Errorf("in-memory cursor %v, stored %v", alert.LastEvaluatedAt, stored)
		}
	})

	t.Run("compound", func(t *testing.T) {
		e, pg, ch := newTestEngine(t)
		window := 300
		alert := parsedAlert(t, &Alert{ID: "c1", Name: "c", AlertType: "compound", FractalID: "f1",
			QueryString: `status=500 | groupby(host)`, WindowDuration: &window, LastEvaluatedAt: cursor})
		if err := e.evaluateCompoundAlert(context.Background(), alert, nil, noHold); err != nil {
			t.Fatal(err)
		}
		q := alertQueries(ch)
		if len(q) != 1 {
			t.Fatalf("want one query, got %d", len(q))
		}
		if _, op, to := boundsOf(t, q[0]); op != "<" || to != chTime(alert.LastEvaluatedAt) {
			t.Errorf("window end %s %s, cursor %s", op, to, chTime(alert.LastEvaluatedAt))
		}
		wholeMs(t, "stored cursor", pg.args(sqlLastEvaluated)[0][1].(time.Time))
	})

	t.Run("scheduled", func(t *testing.T) {
		e, pg, ch := newTestEngine(t)
		qw := 3600
		sched, err := cronParser().Parse("*/5 * * * *")
		if err != nil {
			t.Fatal(err)
		}
		alert := parsedAlert(t, &Alert{ID: "s1", Name: "s", AlertType: "scheduled", FractalID: "f1",
			QueryString: `status=500 | count()`, QueryWindowSeconds: &qw, CronSchedule: sched, LastEvaluatedAt: cursor})
		if err := e.evaluateScheduledAlert(context.Background(), alert, nil, noHold); err != nil {
			t.Fatal(err)
		}
		q := alertQueries(ch)
		if len(q) != 1 {
			t.Fatalf("want one query, got %d", len(q))
		}
		if _, op, to := boundsOf(t, q[0]); op != "<" || to != chTime(alert.LastEvaluatedAt) {
			t.Errorf("window end %s %s, cursor %s", op, to, chTime(alert.LastEvaluatedAt))
		}
		wholeMs(t, "stored cursor", pg.args(sqlLastEvaluated)[0][1].(time.Time))
	})
}

// The recent-ingest probe counts a row stamped exactly at the floor cursor: that
// row belongs to the window starting there, which has not run yet.
func TestRecentIngestProbeIncludesTheCursor(t *testing.T) {
	e, _, ch := newTestEngine(t)
	cursor := time.Now().Add(-5 * time.Minute).Truncate(time.Millisecond)
	e.refreshRecentIngest(context.Background(), cursor)
	q := ch.QueriedMatching("last_ingest")
	if len(q) != 1 {
		t.Fatalf("want one probe, got %d", len(q))
	}
	if want := "ingest_timestamp >= toDateTime64('" + chTime(cursor) + "'"; !strings.Contains(q[0], want) {
		t.Errorf("probe %s, want %s", q[0], want)
	}
}
