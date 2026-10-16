package alerts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/dictionaries"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// hookServer records every body a webhook delivers.
type hookServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []string
}

func newHookServer(t *testing.T) *hookServer {
	t.Helper()
	h := &hookServer{}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.bodies = append(h.bodies, string(b))
		h.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(h.Close)
	return h
}

func (h *hookServer) received() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.bodies...)
}

func row(logID, host string) map[string]interface{} {
	return map[string]interface{}{"log_id": logID, "fractal_id": "f1", "host": host, "timestamp": "2026-03-04 05:06:07.000"}
}

// execution decodes the one alert_executions insert.
type execution struct {
	logCount, suppressed int64
	throttled            bool
	key                  string
	webhooks             []WebhookResult
	fractals             []FractalResult
	emails               []EmailResult
}

func onlyExecution(t *testing.T, pg *pgFake) execution {
	t.Helper()
	all := pg.args(sqlExecution)
	if len(all) != 1 {
		t.Fatalf("want one execution row, got %d", len(all))
	}
	a := all[0]
	ex := execution{logCount: a[3].(int64), suppressed: a[4].(int64), throttled: a[5].(bool), key: a[6].(string)}
	for i, dst := range []interface{}{&ex.webhooks, &ex.fractals, &ex.emails} {
		if err := json.Unmarshal([]byte(a[8+i].(string)), dst); err != nil {
			t.Fatal(err)
		}
	}
	return ex
}

// logBatches returns what fractal actions inserted, by target fractal.
func logBatches(ch *fakedb.CH) map[string][][]any {
	out := map[string][][]any{}
	for _, b := range ch.Batches() {
		if !strings.HasPrefix(b.Query, "INSERT INTO logs (") {
			continue
		}
		for _, r := range b.Appended {
			fid := r[3].(string)
			out[fid] = append(out[fid], r)
		}
	}
	return out
}

// A throttle field suppresses per value: the value inside its window is withheld,
// the other is delivered and opens its own window, and the audit row counts both.
func TestProcessResultsThrottlesPerValue(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	hook := newHookServer(t)
	pg.answer(sqlThrottleRead, fakedb.RowsOf([]driver.Value{"host=a"}))
	alert := &Alert{ID: "a1", Name: "Login {{host}}", FractalID: "f1", ThrottleTimeSeconds: 300, ThrottleField: "host",
		WebhookActions: []WebhookAction{{ID: "w1", Name: "hook", URL: hook.URL, Method: "POST", RetryCount: 1, Enabled: true}}}
	results := []map[string]interface{}{row("l1", "a"), row("l2", "b"), row("l3", "a")}

	if err := e.processAlertResults(context.Background(), alert, results, parser.QueryOptions{}, 7); err != nil {
		t.Fatal(err)
	}

	if args := pg.args(sqlThrottleRead); len(args) != 1 || args[0][0] != "a1" || args[0][1] != `{"host=a","host=b"}` {
		t.Errorf("throttle lookup args = %v", args)
	}
	bodies := hook.received()
	if len(bodies) != 1 {
		t.Fatalf("webhook called %d times", len(bodies))
	}
	if !strings.Contains(bodies[0], `"l2"`) || strings.Contains(bodies[0], `"l1"`) || strings.Contains(bodies[0], `"l3"`) {
		t.Errorf("only the unthrottled value is delivered: %s", bodies[0])
	}
	if !strings.Contains(bodies[0], "Login b") {
		t.Errorf("the name template resolves against the delivered rows: %s", bodies[0])
	}

	writes := pg.args(sqlThrottleWrite)
	if len(writes) != 1 || writes[0][1] != `{"host=b"}` || writes[0][2] != float64(300) {
		t.Errorf("only the delivered value opens a window: %v", writes)
	}
	ex := onlyExecution(t, pg)
	if ex.logCount != 3 || ex.suppressed != 2 || ex.throttled || ex.key != "host (2 values)" {
		t.Errorf("execution = %+v", ex)
	}
	if len(ex.webhooks) != 1 || !ex.webhooks[0].Success {
		t.Errorf("webhook results = %+v", ex.webhooks)
	}
	if len(pg.Matching(sqlLastTriggered)) != 1 {
		t.Error("last_triggered must be stamped")
	}
}

// When every row is inside its window nothing is delivered, but the evaluation is
// still audited as throttled and last_triggered still moves.
func TestProcessResultsFullySuppressed(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	hook := newHookServer(t)
	pg.answer(sqlThrottleRead, fakedb.RowsOf([]driver.Value{globalThrottleKey}))
	alert := &Alert{ID: "a1", Name: "n", PrismID: "p1", ThrottleTimeSeconds: 60,
		WebhookActions: []WebhookAction{{ID: "w1", URL: hook.URL, Method: "POST", RetryCount: 1, Enabled: true}}}

	if err := e.processAlertResults(context.Background(), alert, []map[string]interface{}{row("l1", "a"), row("l2", "b")}, parser.QueryOptions{}, 1); err != nil {
		t.Fatal(err)
	}
	if len(hook.received()) != 0 {
		t.Error("a suppressed batch reached its webhook")
	}
	if len(pg.Matching(sqlThrottleWrite)) != 0 {
		t.Error("a suppressed batch must not extend its own window")
	}
	ex := onlyExecution(t, pg)
	if ex.logCount != 2 || ex.suppressed != 2 || !ex.throttled || ex.key != globalThrottleKey {
		t.Errorf("execution = %+v", ex)
	}
	if args := pg.args(sqlExecution)[0]; args[1] != nil || args[2] != "p1" {
		t.Errorf("a prism alert's execution is prism-scoped: %v/%v", args[1], args[2])
	}
	if len(pg.Matching(sqlLastTriggered)) != 1 {
		t.Error("last_triggered must be stamped for a throttled match too")
	}
}

// The throttle store failing must never drop a detection: everything is delivered.
func TestProcessResultsDeliversWhenTheThrottleStoreFails(t *testing.T) {
	for name, err := range map[string]error{"missing table": pqErr("42P01"), "other": errors.New("timeout")} {
		t.Run(name, func(t *testing.T) {
			e, pg, _ := newTestEngine(t)
			hook := newHookServer(t)
			pg.answer(sqlThrottleRead, fakedb.PGResult{Err: err})
			pg.answer(sqlThrottleWrite, fakedb.PGResult{Err: err})
			alert := &Alert{ID: "a1", Name: "n", FractalID: "f1", ThrottleTimeSeconds: 60,
				WebhookActions: []WebhookAction{{ID: "w1", URL: hook.URL, Method: "POST", RetryCount: 1, Enabled: true}}}
			if err := e.processAlertResults(context.Background(), alert, []map[string]interface{}{row("l1", "a")}, parser.QueryOptions{}, 1); err != nil {
				t.Fatal(err)
			}
			if len(hook.received()) != 1 {
				t.Error("a throttle store failure suppressed a detection")
			}
			if ex := onlyExecution(t, pg); ex.throttled || ex.suppressed != 0 {
				t.Errorf("execution = %+v", ex)
			}
		})
	}
}

// Each kind of action runs once per trigger, disabled ones are skipped, a failing
// one is recorded rather than stopping the rest, and the system alerts fractal gets
// every trigger.
func TestProcessResultsFansOutToEveryAction(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	hook := newHookServer(t)
	off := newHookServer(t)
	pg.answer(sqlSystemFractal, fakedb.Row("sys-fractal"))
	pg.answer("SELECT name FROM fractals WHERE id = $1", fakedb.Row("source-name"))

	alert := &Alert{ID: "a1", Name: "Fan out", FractalID: "f1", Severity: "high", Labels: []string{"attack.t1078"},
		WebhookActions: []WebhookAction{
			{ID: "w1", Name: "on", URL: hook.URL, Method: "POST", RetryCount: 1, Enabled: true},
			{ID: "w2", Name: "off", URL: off.URL, Method: "POST", RetryCount: 1, Enabled: false},
		},
		FractalActions: []FractalAction{
			{ID: "fa1", Name: "copy", TargetFractalID: "target", MaxLogsPerTrigger: 1, AddAlertContext: true, Enabled: true},
			{ID: "fa2", Name: "disabled", TargetFractalID: "never", Enabled: false},
		},
		EmailActions: []EmailAction{{ID: "e1", Name: "mail", Recipients: []string{"soc@example.test"}, Enabled: true}},
	}
	results := []map[string]interface{}{row("l1", "a"), row("l2", "b")}

	if err := e.processAlertResults(context.Background(), alert, results, parser.QueryOptions{}, 3); err != nil {
		t.Fatal(err)
	}

	if len(hook.received()) != 1 || len(off.received()) != 0 {
		t.Errorf("webhooks: enabled %d, disabled %d", len(hook.received()), len(off.received()))
	}
	batches := logBatches(ch)
	if len(batches["target"]) != 1 {
		t.Errorf("the fractal action forwards up to its cap, got %d rows", len(batches["target"]))
	}
	if len(batches["never"]) != 0 {
		t.Error("a disabled fractal action forwarded logs")
	}
	if len(batches["sys-fractal"]) != 2 {
		t.Errorf("the system alerts fractal receives every row, got %d", len(batches["sys-fractal"]))
	}
	fields := batches["target"][0][2].(map[string]string)
	if fields["alert_name"] != "Fan out" || fields["source_fractal"] != "source-name" || fields["source_log_id"] != "l1" {
		t.Errorf("forwarded fields = %v", fields)
	}

	ex := onlyExecution(t, pg)
	if ex.logCount != 2 || ex.throttled {
		t.Errorf("execution = %+v", ex)
	}
	if len(ex.webhooks) != 1 || ex.webhooks[0].WebhookID != "w1" {
		t.Errorf("webhook results = %+v", ex.webhooks)
	}
	got := map[string]FractalResult{}
	for _, r := range ex.fractals {
		got[r.FractalActionID] = r
	}
	if !got["fa1"].Success || got["fa1"].LogsSent != 1 || !got["system-alerts"].Success || len(got) != 2 {
		t.Errorf("fractal results = %+v", ex.fractals)
	}
	if len(ex.emails) != 1 || ex.emails[0].Success || !strings.Contains(ex.emails[0].Error, "SMTP not configured") {
		t.Errorf("an email failure is recorded, not fatal: %+v", ex.emails)
	}
	// With no throttle configured nothing touches the throttle table.
	if len(pg.Matching("alert_throttles")) != 0 {
		t.Error("an unthrottled alert read or wrote throttles")
	}
}

// Only fractal actions get the refetched full rows; webhooks keep the pruned shape.
func TestProcessResultsHydratesOnlyForFractalActions(t *testing.T) {
	e, pg, ch := newTestEngine(t)
	hook := newHookServer(t)
	e.hydrator = &fakeHydrator{fields: map[string]map[string]interface{}{"l1": {"command_line": "whoami /all"}}}
	_ = pg
	alert := &Alert{ID: "a1", Name: "n", FractalID: "f1",
		WebhookActions: []WebhookAction{{ID: "w1", URL: hook.URL, Method: "POST", RetryCount: 1, Enabled: true}},
		FractalActions: []FractalAction{{ID: "fa1", TargetFractalID: "target", MaxLogsPerTrigger: 10, Enabled: true}}}
	opts := parser.QueryOptions{TableName: "logs_hot"}

	if err := e.processAlertResults(context.Background(), alert, []map[string]interface{}{row("l1", "a")}, opts, 1); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hook.received()[0], "whoami") {
		t.Error("a webhook received the hydrated row")
	}
	fields := logBatches(ch)["target"][0][2].(map[string]string)
	if fields["command_line"] != "whoami /all" {
		t.Errorf("the forwarded log lacks the hydrated field: %v", fields)
	}
}

// Dictionary actions run against the alert's own scope, a disabled one is skipped,
// and a failure does not stop the evaluation from being recorded.
func TestProcessResultsRunsDictionaryActions(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	e.dictManager = dictionaries.NewManager(pg.client, nil)
	pg.answer("FROM dictionaries", fakedb.PGResult{Err: errors.New("dictionary store down")})
	alert := &Alert{ID: "a1", Name: "n", FractalID: "f1", DictionaryActions: []*dictionaries.DictionaryAction{
		{Name: "off", DictionaryName: "skipped", Enabled: false},
		{Name: "on", DictionaryName: "blocklist", Enabled: true},
	}}

	if err := e.processAlertResults(context.Background(), alert, []map[string]interface{}{row("l1", "a")}, parser.QueryOptions{}, 1); err != nil {
		t.Fatal(err)
	}
	lookups := pg.args("FROM dictionaries")
	if len(lookups) != 1 {
		t.Fatalf("want one dictionary lookup (the disabled action is skipped), got %d", len(lookups))
	}
	if lookups[0][0] != "f1" || lookups[0][1] != "blocklist" {
		t.Errorf("the dictionary is resolved in the alert's scope: %v", lookups[0])
	}
	onlyExecution(t, pg)
}

// The engine loads dictionary actions with the cache and offers the scope's
// dictionaries to match().
func TestCacheAndOptsCarryDictionaries(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	e.dictManager = dictionaries.NewManager(pg.client, nil)
	pg.answer(sqlCacheRefresh, fakedb.RowsOf(alertSpec{ID: "a1", Name: "a1", Enabled: true, FractalID: "f1"}.cacheRow()))
	pg.answer("JOIN alert_dictionary_actions ada", fakedb.RowsOf([]driver.Value{
		"d1", "da", "", "blocklist", int64(10), true, "", fixedTime, fixedTime, "f1", "",
	}))
	enabled, err := e.refreshAlertsCache(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(enabled) != 1 || len(enabled[0].DictionaryActions) != 1 || enabled[0].DictionaryActions[0].DictionaryName != "blocklist" {
		t.Fatalf("dictionary actions not loaded: %+v", enabled)
	}

	pg.answer("WHERE fractal_id = $1 OR is_global = true", fakedb.RowsOf([]driver.Value{
		"dict-1", "blocklist", "ip", []byte(`[{"name":"ip"},{"name":"reason"}]`), false, "value",
	}))
	opts, err := e.buildQueryOpts(context.Background(), enabled[0], fixedTime, fixedTime.Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.Dictionaries) == 0 {
		t.Error("the scope's dictionaries must reach match()")
	}
	if opts.DictionaryDatabase != "logs" {
		t.Errorf("dictGet must be qualified with the logs database, got %q", opts.DictionaryDatabase)
	}
}

// EmailClient.Send loads SMTP from settings and delivers with the alert rendered in.
func TestEmailSendUsesStoredSMTP(t *testing.T) {
	srv := startFakeSMTP(t)
	pg := newPGFake(t)
	cfg := fmt.Sprintf(`{"host":"127.0.0.1","port":%d,"from_address":"alerts@example.test","tls_mode":"none"}`, srv.port())
	pg.answer("SELECT value FROM settings WHERE key = $1", fakedb.Row(cfg))
	client := NewEmailClient(pg.client, "https://bifract.example/")

	res := client.Send(context.Background(), EmailAction{ID: "e1", Name: "mail", Recipients: []string{"soc@example.test"}},
		&Alert{ID: "a1", Name: "Brute force", Severity: "high"}, "Brute force", []map[string]interface{}{{"user": "alice"}})
	if !res.Success || res.AttemptCount != 1 {
		t.Fatalf("result = %+v", res)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.rcpts) != 1 || !strings.Contains(srv.data, "Brute force") {
		t.Errorf("delivered to %v: %s", srv.rcpts, srv.data)
	}
}

func TestLoadSMTPConfigValidates(t *testing.T) {
	cases := map[string]string{
		`not json`:                     "invalid SMTP config",
		`{"port":25}`:                  "host is required",
		`{"host":"mail.example.test"}`: "from_address is required",
	}
	for raw, want := range cases {
		pg := newPGFake(t)
		pg.answer("FROM settings", fakedb.Row(raw))
		_, err := NewEmailClient(pg.client, "").loadSMTPConfig(context.Background())
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", raw, err, want)
		}
	}
	pg := newPGFake(t)
	pg.answer("FROM settings", fakedb.Row(`{"host":"mail.example.test","from_address":"a@example.test"}`))
	cfg, err := NewEmailClient(pg.client, "").loadSMTPConfig(context.Background())
	if err != nil || cfg.Port != 587 || cfg.TLSMode != "starttls" {
		t.Errorf("defaults: %+v %v", cfg, err)
	}
}

// A fractal action whose insert fails reports the failure in its result.
func TestFractalActionReportsInsertFailure(t *testing.T) {
	_, pg, ch := newTestEngine(t)
	ch.PrepareErr = errors.New("too many parts")
	client := NewFractalActionClient(storageClientOf(t, ch), pg.client)
	res := client.Send(context.Background(), FractalAction{ID: "fa", TargetFractalID: "t"}, &Alert{ID: "a", FractalID: "f1"}, "a",
		[]map[string]interface{}{row("l1", "a")})
	if res.Success || !strings.Contains(res.Error, "Failed to ingest logs to fractal t") {
		t.Errorf("result = %+v", res)
	}
}

func storageClientOf(t *testing.T, ch *fakedb.CH) *storage.ClickHouseClient {
	t.Helper()
	return storage.NewClickHouseClientFromConn(ch, "logs", storage.Topology{Kind: storage.DeploymentSingleNode})
}
