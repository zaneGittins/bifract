package alerts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/robfig/cron/v3"

	"bifract/internal/fakedb"
	"bifract/pkg/settings"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// pgFake routes each statement to the newest handler whose match it contains, so a
// test can register defaults and then override the one answer it is about. Anything
// unrouted falls through to the fakedb rules, and then to zero rows.
type pgFake struct {
	*fakedb.PG
	client *storage.PostgresClient

	mu     sync.Mutex
	routes []pgRoute
}

type pgRoute struct {
	match string
	fn    func(args []driver.Value) fakedb.PGResult
}

func newPGFake(t *testing.T) *pgFake {
	t.Helper()
	client, fake := storagetest.Postgres()
	f := &pgFake{PG: fake, client: client}
	fake.Respond(func(q string, args []driver.Value) (fakedb.PGResult, bool) {
		f.mu.Lock()
		routes := append([]pgRoute(nil), f.routes...)
		f.mu.Unlock()
		for i := len(routes) - 1; i >= 0; i-- {
			if strings.Contains(q, routes[i].match) {
				return routes[i].fn(args), true
			}
		}
		return fakedb.PGResult{}, false
	})
	t.Cleanup(func() { client.Close() })
	return f
}

// handle answers statements containing match with fn.
func (f *pgFake) handle(match string, fn func(args []driver.Value) fakedb.PGResult) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.routes = append(f.routes, pgRoute{match: match, fn: fn})
}

// answer answers statements containing match with a fixed result.
func (f *pgFake) answer(match string, res fakedb.PGResult) {
	f.handle(match, func([]driver.Value) fakedb.PGResult { return res })
}

// args returns the bound arguments of every statement containing sub.
func (f *pgFake) args(sub string) [][]driver.Value {
	var out [][]driver.Value
	for _, c := range f.Matching(sub) {
		out = append(out, c.Args)
	}
	return out
}

// committed reports how many transactions committed and rolled back.
func (f *pgFake) txCounts() (commits, rollbacks int) {
	_, _, c, r := f.Stats()
	return c, r
}

var fixedTime = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

// pgArray renders a string slice the way Postgres hands an array back.
func pgArray(ss []string) []byte {
	v, _ := pq.StringArray(ss).Value()
	if v == nil {
		return []byte("{}")
	}
	return []byte(v.(string))
}

// alertSpec is one alerts row as the fake database holds it.
type alertSpec struct {
	ID, Name, Query, Type, Severity string
	FractalID, PrismID, FeedID      string
	Enabled                         bool
	ThrottleSecs                    int64
	ThrottleField                   string
	Labels                          []string
	// Action aggregates as the json_agg subqueries return them; "" is "[]".
	Webhooks, Fractals, Dicts, Emails string
	LastEval                          time.Time
	Window, QueryWindow               *int64
	Cron                              string
}

func (a alertSpec) defaults() alertSpec {
	if a.Type == "" {
		a.Type = "event"
	}
	if a.Severity == "" {
		a.Severity = "medium"
	}
	if a.Query == "" {
		a.Query = `status=500`
	}
	if a.LastEval.IsZero() {
		a.LastEval = fixedTime
	}
	return a
}

func jsonOr(s string) []byte {
	if s == "" {
		return []byte("[]")
	}
	return []byte(s)
}

func intOrZero(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func intOrNil(p *int64) driver.Value {
	if p == nil {
		return nil
	}
	return *p
}

// getRow answers GetAlert.
func (a alertSpec) getRow() []driver.Value {
	a = a.defaults()
	return []driver.Value{
		a.ID, a.Name, "desc", a.Query, a.Type, a.Enabled,
		a.ThrottleSecs, a.ThrottleField, int64(0), pgArray(a.Labels), pgArray(nil),
		a.Severity, a.FractalID, a.PrismID, a.FeedID, "",
		"creator", "", fixedTime, fixedTime, nil,
		"", intOrZero(a.Window), a.Cron, intOrZero(a.QueryWindow),
		jsonOr(a.Webhooks), jsonOr(a.Fractals), jsonOr(a.Dicts), jsonOr(a.Emails),
	}
}

// listRow answers ListAlerts.
func (a alertSpec) listRow() []driver.Value {
	a = a.defaults()
	return []driver.Value{
		a.ID, a.Name, "desc", a.Query, a.Type, a.Enabled,
		a.ThrottleSecs, a.ThrottleField, int64(0), pgArray(a.Labels), pgArray(nil),
		a.Severity, a.FractalID, a.PrismID,
		"creator", "", fixedTime, fixedTime, nil, "",
		nil, intOrNil(a.Window), a.Cron, intOrZero(a.QueryWindow),
		jsonOr(a.Webhooks), jsonOr(a.Fractals), jsonOr(a.Dicts), jsonOr(a.Emails),
	}
}

// cacheRow answers the engine's cache refresh.
func (a alertSpec) cacheRow() []driver.Value {
	a = a.defaults()
	return []driver.Value{
		a.ID, a.Name, "desc", a.Query, a.Type, a.Enabled,
		a.ThrottleSecs, a.ThrottleField, int64(0), pgArray(a.Labels),
		a.Severity, a.FractalID, a.PrismID,
		"creator", fixedTime, fixedTime, nil,
		a.LastEval, "", intOrNil(a.Window), a.Cron, intOrNil(a.QueryWindow),
		a.FeedID, "", "",
		jsonOr(a.Webhooks), jsonOr(a.Fractals), jsonOr(a.Emails),
	}
}

// Statement fragments that identify one query each.
const (
	sqlGetAlert       = "WHERE a.id = $1"
	sqlListAlerts     = "a.feed_id IS NULL"
	sqlCacheRefresh   = "a.last_evaluated_at, COALESCE(a.disabled_reason"
	sqlAlertScope     = "feed_id IS NOT NULL FROM alerts WHERE id = $1"
	sqlLockAlert      = "FOR UPDATE"
	sqlInsertAlert    = "INSERT INTO alerts (name, description, query_string, alert_type, enabled"
	sqlGateConfig     = "FROM alert_gate_config"
	sqlPolicies       = "FROM alert_policies"
	sqlHeadHash       = "SELECT content_hash FROM alert_revisions"
	sqlRevisionInsert = "INSERT INTO alert_revisions"
	sqlCRSelect       = "FROM alert_change_requests cr"
	sqlCRScope        = "FROM alert_change_requests WHERE id = $1"
	sqlReviews        = "FROM alert_change_reviews"
	sqlClaim          = "SET status = 'merged'"
	sqlAdvisoryLock   = "pg_try_advisory_lock"
	sqlLastEvaluated  = "SET last_evaluated_at = $2"
	sqlLastTriggered  = "SET last_triggered = NOW()"
	sqlExecution      = "INSERT INTO alert_executions"
	sqlDisable        = "SET enabled = false, disabled_reason = $1"
	sqlThrottleRead   = "SELECT throttle_key FROM alert_throttles"
	sqlThrottleWrite  = "INSERT INTO alert_throttles"
	sqlSystemFractal  = "FROM fractals WHERE name = 'alerts'"
)

// gateRow answers GateConfigFor.
func gateRow(enabled bool, min int64, self bool) fakedb.PGResult {
	return fakedb.Row(enabled, min, self)
}

// crSpec is one alert_change_requests row.
type crSpec struct {
	ID, AlertID, AlertName, Kind, Status string
	Title, Summary                       string
	Content                              *RevisionContent
	Tests                                []AlertTest
	BaseHash, Author                     string
	// TestResult and TestResultHash are the stored test run, if any.
	TestResult     []byte
	TestResultHash string
}

func (c crSpec) hash(t *testing.T) string {
	t.Helper()
	if c.Content == nil {
		return ""
	}
	cc := *c.Content
	cc.canonicalize()
	h, err := cc.Hash()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (c crSpec) row(t *testing.T) []driver.Value {
	t.Helper()
	var content, tests driver.Value
	if c.Content != nil {
		raw, _ := json.Marshal(c.Content)
		content = raw
	}
	if c.Tests != nil {
		raw, _ := json.Marshal(c.Tests)
		tests = raw
	}
	return []driver.Value{
		c.ID, c.AlertID, c.AlertName, c.Kind, c.Status,
		c.Title, c.Summary, content, tests, c.hash(t), c.BaseHash,
		c.Author, c.Author, fixedTime, fixedTime,
		nil, "", nilIfEmpty(c.TestResult), c.TestResultHash,
	}
}

func nilIfEmpty(b []byte) driver.Value {
	if len(b) == 0 {
		return nil
	}
	return b
}

// reviewRow answers listReviews.
func reviewRow(id, reviewer, decision, hash string) []driver.Value {
	return []driver.Value{id, reviewer, reviewer, decision, "", hash, fixedTime}
}

// eventContent is a valid event definition.
func eventContent(name string) *RevisionContent {
	return &RevisionContent{Name: name, QueryString: `status=500`, AlertType: "event", Severity: "high"}
}

// newTestManager wires a manager and its engine to one scripted Postgres.
func newTestManager(t *testing.T) (*Manager, *pgFake) {
	t.Helper()
	pg := newPGFake(t)
	eng := &Engine{pg: pg.client, alertsCache: map[string]*Alert{}, webhookClient: NewWebhookClient("", allowPrivate), emailClient: NewEmailClient(pg.client, "")}
	return &Manager{pg: pg.client, engine: eng}, pg
}

// allowPrivate admits the loopback test servers webhooks deliver to.
func allowPrivate(context.Context) bool { return true }

// newTestEngine wires an engine to scripted Postgres and ClickHouse.
func newTestEngine(t *testing.T) (*Engine, *pgFake, *fakedb.CH) {
	t.Helper()
	pg := newPGFake(t)
	chClient, ch := storagetest.ClickHouse()
	e := NewEngineWithDicts(pg.client, chClient, nil, "https://bifract.example")
	// Hydration is pinned in its own tests; here rows reach actions as queried.
	e.hydrator = nil
	// Webhooks deliver to loopback test servers, through the real policy lookup.
	pg.handle("SELECT value FROM settings WHERE key", func(args []driver.Value) fakedb.PGResult {
		if args[0] == settings.OutboundAllowPrivateKey {
			return fakedb.RowsOf([]driver.Value{"true"})
		}
		return fakedb.PGResult{}
	})
	return e, pg, ch
}

// recordingNotifier captures health notifications.
type recordingNotifier struct {
	mu    sync.Mutex
	notes []string
	done  chan struct{}
}

func newRecordingNotifier() *recordingNotifier {
	return &recordingNotifier{done: make(chan struct{}, 16)}
}

func (n *recordingNotifier) Write(notifType, severity, title, message string) error {
	n.mu.Lock()
	n.notes = append(n.notes, notifType+"|"+title+"|"+message)
	n.mu.Unlock()
	n.done <- struct{}{}
	return nil
}

func (n *recordingNotifier) wait(t *testing.T, count int) []string {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-n.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("waited for %d notifications, got %d", count, i)
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.notes...)
}

func (n *recordingNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.notes)
}

func cronParser() cron.Parser {
	return cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
}

func pqErr(code string) error { return &pq.Error{Code: pq.ErrorCode(code), Message: "fake " + code} }
