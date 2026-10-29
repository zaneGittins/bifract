package alerts

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
)

// hcall is one request against a handler backed by the scripted database.
type hcall struct {
	as     caller
	method string
	body   string
	query  string
	params map[string]string
	header map[string]string
}

func serve(h *Handler, fn func(*Handler) http.HandlerFunc, c hcall) *httptest.ResponseRecorder {
	method := c.method
	if method == "" {
		method = http.MethodGet
	}
	r := c.as.request(method, c.body, c.params)
	r.URL.RawQuery = c.query
	for k, v := range c.header {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	fn(h)(rec, r)
	return rec
}

func newDBHandler(t *testing.T) (*Handler, *pgFake) {
	t.Helper()
	m, pg := newTestManager(t)
	return &Handler{manager: m}, pg
}

func id(v string) map[string]string { return map[string]string{"id": v} }

// expect checks a status and, when set, that the body mentions want.
func expect(t *testing.T, rec *httptest.ResponseRecorder, status int, want string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status %d, want %d: %s", rec.Code, status, rec.Body.String())
	}
	if want != "" && !strings.Contains(rec.Body.String(), want) {
		t.Errorf("body %s does not mention %q", rec.Body.String(), want)
	}
}

var (
	prismAnalyst = caller{user: &storage.User{Username: "pat"}, prismRole: "analyst", prism: "p1"}
	// keyF1 is an API key scoped to f1 with an analyst role there.
	keyF1 = caller{user: &storage.User{Username: "apikey_k1"}, fractalRole: "analyst", fractal: "f1", apiKey: true}
)

func TestGetAlertHandler(t *testing.T) {
	get := func(h *Handler) http.HandlerFunc { return h.HandleGetAlert }
	h, pg := newDBHandler(t)
	expect(t, serve(h, get, hcall{as: viewer, params: id("missing")}), 404, "Alert not found")

	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "Rule", FractalID: "f1"}.getRow()))
	expect(t, serve(h, get, hcall{as: viewer, params: id("a1")}), 200, `"name":"Rule"`)
	expect(t, serve(h, get, hcall{as: nobody, params: id("a1")}), 403, "")

	// A key scoped to another fractal cannot read this one's alert.
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a2", Name: "Other", FractalID: "f2"}.getRow()))
	expect(t, serve(h, get, hcall{as: keyF1, params: id("a2")}), 403, "")

	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a3", Name: "Prism rule", PrismID: "p1"}.getRow()))
	expect(t, serve(h, get, hcall{as: prismAnalyst, params: id("a3")}), 200, "Prism rule")
	expect(t, serve(h, get, hcall{as: analyst, params: id("a3")}), 403, "")

	pg.answer(sqlGetAlert, fakedb.PGResult{Err: errors.New("connection refused")})
	expect(t, serve(h, get, hcall{as: viewer, params: id("a1")}), 500, "Failed to load alert")
}

func TestCreateAlertHandler(t *testing.T) {
	create := func(h *Handler) http.HandlerFunc { return h.HandleCreateAlert }
	body := `{"name":"Rule","query_string":"status=500"}`

	h, pg := newDBHandler(t)
	pg.answer(sqlInsertAlert, fakedb.Row("new"))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new", Name: "Rule", FractalID: "f1"}.getRow()))
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: body}), 200, `"id":"new"`)
	if ins := pg.args(sqlInsertAlert)[0]; ins[11] != "ana" {
		t.Errorf("created_by = %v", ins[11])
	}

	// An API key has no users row, so it is not written as the creator.
	h, pg = newDBHandler(t)
	pg.answer(sqlInsertAlert, fakedb.Row("new"))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new", Name: "Rule", FractalID: "f1"}.getRow()))
	expect(t, serve(h, create, hcall{as: keyF1, method: "POST", body: body}), 200, "")
	if ins := pg.args(sqlInsertAlert)[0]; ins[11] != nil {
		t.Errorf("an API key was written as created_by: %v", ins[11])
	}

	h, pg = newDBHandler(t)
	pg.answer(sqlInsertAlert, fakedb.PGResult{Err: errors.New(`pq: duplicate key value violates unique constraint "idx_alerts_manual_name"`)})
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: body}), 409, "An alert with this name already exists in this fractal or prism")

	h, pg = newDBHandler(t)
	pg.answer(sqlGateConfig, gateRow(true, 1, true))
	rec := serve(h, create, hcall{as: analyst, method: "POST", body: body})
	expect(t, rec, 409, `"gate":"required"`)

	h, pg = newDBHandler(t)
	pg.answer(sqlPolicies, blockingLabelPolicy())
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: body}), 422, "tag it")

	h, _ = newDBHandler(t)
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"Rule","query_string":"* | count()"}`}), 400, "aggregate")

	h, pg = newDBHandler(t)
	pg.answer(sqlInsertAlert, fakedb.PGResult{Err: errors.New("disk full")})
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: body}), 500, "Failed to create alert")
}

func TestUpdateAlertHandler(t *testing.T) {
	update := func(h *Handler) http.HandlerFunc { return h.HandleUpdateAlert }
	body := `{"name":"after","query_string":"a=1"}`
	head := RevisionContent{Name: "before", QueryString: `a=1`, AlertType: "event", Severity: "medium"}
	head.canonicalize()

	h, _ := newDBHandler(t)
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: body, params: id("a1")}), 404, "")

	m, pg := updateFixture(t, head, false)
	h = &Handler{manager: m}
	expect(t, serve(h, update, hcall{as: viewer, method: "PUT", body: body, params: id("a1")}), 403, "")
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: `{`, params: id("a1")}), 400, "Invalid request body")
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: `{"name":"x"}`, params: id("a1")}), 400, "Query string is required")
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: `{"name":"x","query_string":"a=("}`, params: id("a1")}), 400, "invalid query syntax")
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: body, params: id("a1")}), 200, `"id":"a1"`)

	pg.answer(sqlGateConfig, gateRow(true, 1, true))
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: body, params: id("a1")}), 409, `"kind":"update"`)
	pg.answer(sqlGateConfig, fakedb.PGResult{})
	pg.answer(sqlPolicies, blockingLabelPolicy())
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: body, params: id("a1")}), 422, "")
	pg.answer(sqlPolicies, fakedb.PGResult{})
	pg.answer(sqlLockAlert, fakedb.PGResult{})
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: body, params: id("a1")}), 404, "Alert not found")
	pg.answer(sqlLockAlert, fakedb.PGResult{Err: errors.New("deadlock detected")})
	expect(t, serve(h, update, hcall{as: analyst, method: "PUT", body: body, params: id("a1")}), 500, "Failed to update alert")
}

func TestDeleteAlertHandler(t *testing.T) {
	del := func(h *Handler) http.HandlerFunc { return h.HandleDeleteAlert }
	h, pg := newDBHandler(t)
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("a1")}), 404, "")

	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "n", PrismID: "p1"}.getRow()))
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("a1")}), 403, "")

	pg.answer(sqlAlertScope, fakedb.Row("", "p1", false))
	pg.answer(sqlGateConfig, gateRow(true, 1, true))
	expect(t, serve(h, del, hcall{as: prismAnalyst, method: "DELETE", params: id("a1")}), 409, `"kind":"delete"`)

	pg.answer(sqlGateConfig, fakedb.PGResult{})
	expect(t, serve(h, del, hcall{as: prismAnalyst, method: "DELETE", params: id("a1")}), 404, "Alert not found")
	pg.answer("DELETE FROM alerts WHERE id = $1", fakedb.Affected(1))
	expect(t, serve(h, del, hcall{as: prismAnalyst, method: "DELETE", params: id("a1")}), 200, "deleted")
	pg.answer("DELETE FROM alerts WHERE id = $1", fakedb.PGResult{Err: errors.New("fk")})
	expect(t, serve(h, del, hcall{as: prismAnalyst, method: "DELETE", params: id("a1")}), 500, "")
}

func TestListAlertsHandlerFiltersAndPages(t *testing.T) {
	h, pg := newDBHandler(t)
	pg.answer(sqlListAlerts, fakedb.RowsOf(
		alertSpec{ID: "a1", Name: "Brute force", FractalID: "f1"}.listRow(),
		alertSpec{ID: "a2", Name: "Brute other", FractalID: "f1"}.listRow(),
		alertSpec{ID: "a3", Name: "Exfil", FractalID: "f1"}.listRow(),
	))
	rec := serve(h, func(h *Handler) http.HandlerFunc { return h.HandleListAlerts }, hcall{as: viewer, query: "search=BRUTE&limit=1&offset=1&enabled=true"})
	expect(t, rec, 200, "Brute other")
	if strings.Contains(rec.Body.String(), "Exfil") || strings.Contains(rec.Body.String(), `"a1"`) {
		t.Errorf("search then page: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"total":2`) {
		t.Errorf("the total counts the filtered set: %s", rec.Body.String())
	}
	if args := pg.Matching(sqlListAlerts)[0].Args; len(args) != 2 || args[1] != true {
		t.Errorf("enabled filter args = %v", args)
	}

	pg.answer(sqlListAlerts, fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, func(h *Handler) http.HandlerFunc { return h.HandleListAlerts }, hcall{as: viewer}), 500, "")
}

func TestImportYAMLHandlerOutcomes(t *testing.T) {
	imp := func(h *Handler) http.HandlerFunc { return h.HandleImportYAML }
	h, pg := newDBHandler(t)
	pg.answer(sqlInsertAlert, fakedb.Row("new"))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new", Name: "Rule", FractalID: "f1"}.getRow()))
	expect(t, serve(h, imp, hcall{as: analyst, method: "POST", body: "name: Rule\nqueryString: a=1\n"}), 200, "imported")

	h, pg = newDBHandler(t)
	pg.answer(sqlGateConfig, gateRow(true, 1, true))
	expect(t, serve(h, imp, hcall{as: analyst, method: "POST", body: "name: Rule\nqueryString: a=1\n"}), 409, `"gate":"required"`)

	h, pg = newDBHandler(t)
	pg.answer("FROM alerts WHERE name = $1 AND feed_id IS NULL", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, imp, hcall{as: analyst, method: "POST", body: "name: Rule\nqueryString: a=1\n"}), 500, "Failed to import alert")
}

func TestExecutionsAndActivityHandlers(t *testing.T) {
	h, pg := newDBHandler(t)
	execs := func(h *Handler) http.HandlerFunc { return h.HandleGetExecutions }
	activity := func(h *Handler) http.HandlerFunc { return h.HandleGetActivity }
	expect(t, serve(h, execs, hcall{as: viewer, params: id("a1")}), 404, "")
	expect(t, serve(h, activity, hcall{as: viewer, params: id("a1")}), 404, "")

	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "n", FractalID: "f1"}.getRow()))
	pg.answer("FROM alert_executions\n\t\tWHERE alert_id = $1", fakedb.RowsOf([]driver.Value{
		"x1", "2026-03-04T05:06:07Z", int64(3), int64(1), false, "host=a", int64(12),
		`[{"webhook_name":"hook","success":true}]`, "null",
	}))
	rec := serve(h, execs, hcall{as: viewer, params: id("a1"), query: "limit=5000&offset=2"})
	expect(t, rec, 200, `"webhook_name":"hook"`)
	if args := pg.args("FROM alert_executions\n\t\tWHERE alert_id = $1")[0]; args[1] != int64(50) || args[2] != int64(2) {
		t.Errorf("an out-of-range limit falls back to the default: %v", args)
	}

	pg.answer("generate_series", fakedb.RowsOf([]driver.Value{fixedTime, int64(2), int64(10)}, []driver.Value{fixedTime, int64(1), int64(5)}))
	rec = serve(h, activity, hcall{as: viewer, params: id("a1"), query: "days=400"})
	expect(t, rec, 200, `"total_executions":3`)
	if args := pg.args("generate_series")[0]; args[1] != int64(retentionDays) {
		t.Errorf("the window is capped at retention: %v", args)
	}
	expect(t, rec, 200, `"total_logs":15`)

	pg.answer("generate_series", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, activity, hcall{as: viewer, params: id("a1")}), 500, "")
	pg.answer("FROM alert_executions\n\t\tWHERE alert_id = $1", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, execs, hcall{as: viewer, params: id("a1")}), 500, "")
}

func TestDuplicateAndToggleHandlers(t *testing.T) {
	dup := func(h *Handler) http.HandlerFunc { return h.HandleDuplicateAlert }
	toggle := func(h *Handler) http.HandlerFunc { return h.HandleToggleFeedAlert }
	batch := func(h *Handler) http.HandlerFunc { return h.HandleBatchToggleFeedAlerts }
	h, pg := newDBHandler(t)

	expect(t, serve(h, dup, hcall{as: analyst, method: "POST", params: id("missing")}), 404, "")
	expect(t, serve(h, toggle, hcall{as: analyst, method: "POST", params: id("missing"), body: `{"enabled":true}`}), 404, "")

	pg.handle(sqlGetAlert, func(args []driver.Value) fakedb.PGResult {
		return fakedb.RowsOf(alertSpec{ID: args[0].(string), Name: "Feed rule", FractalID: "f1", FeedID: "feed-1"}.getRow())
	})
	pg.answer(sqlInsertAlert, fakedb.Row("copy"))
	expect(t, serve(h, dup, hcall{as: analyst, method: "POST", params: id("a1")}), 200, `"id":"copy"`)
	expect(t, serve(h, dup, hcall{as: viewer, method: "POST", params: id("a1")}), 403, "")

	expect(t, serve(h, toggle, hcall{as: analyst, method: "POST", params: id("a1"), body: `{`}), 400, "")
	expect(t, serve(h, toggle, hcall{as: analyst, method: "POST", params: id("a1"), body: `{"enabled":true}`}), 500, "")
	pg.answer("WHERE id = $3 AND feed_id IS NOT NULL", fakedb.Affected(1))
	expect(t, serve(h, toggle, hcall{as: analyst, method: "POST", params: id("a1"), body: `{"enabled":true}`}), 200, "")

	pg.answer("WHERE id IN (SELECT a.id FROM alerts a WHERE", fakedb.Affected(4))
	rec := serve(h, batch, hcall{as: analyst, method: "POST", body: `{"enabled":true,"filter":{"status":"all","feed_id":"all","severity":"high","label":"all"}}`})
	expect(t, rec, 200, `"toggled":4`)
	c := pg.Matching("WHERE id IN (SELECT a.id FROM alerts a WHERE")[0]
	if strings.Contains(c.Query, "a.feed_id = $") || strings.Contains(c.Query, "a.enabled = true") || !strings.Contains(c.Query, "= ANY(a.labels)") {
		t.Errorf(`"all" must mean no filter: %s`, c.Query)
	}
	pg.answer("WHERE id = ANY($3) AND feed_id IS NOT NULL", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, batch, hcall{as: analyst, method: "POST", body: `{"alert_ids":["a1"]}`}), 500, "")
	pg.answer("WHERE id = ANY($3) AND feed_id IS NULL", fakedb.Affected(2))
	expect(t, serve(h, func(h *Handler) http.HandlerFunc { return h.HandleBatchToggleAlerts }, hcall{as: analyst, method: "POST", body: `{"alert_ids":["a1","a2"],"enabled":false}`}), 200, `"toggled":2`)
}

func TestWebhookHandlersOverStorage(t *testing.T) {
	get := func(h *Handler) http.HandlerFunc { return h.HandleGetWebhook }
	upd := func(h *Handler) http.HandlerFunc { return h.HandleUpdateWebhook }
	del := func(h *Handler) http.HandlerFunc { return h.HandleDeleteWebhook }
	test := func(h *Handler) http.HandlerFunc { return h.HandleTestWebhook }
	list := func(h *Handler) http.HandlerFunc { return h.HandleListWebhooks }
	create := func(h *Handler) http.HandlerFunc { return h.HandleCreateWebhook }

	h, pg := newDBHandler(t)
	for _, fn := range []func(h *Handler) http.HandlerFunc{get, upd, del, test} {
		expect(t, serve(h, fn, hcall{as: analyst, params: id("w1"), body: `{}`}), 404, "Webhook not found")
	}

	hook := newHookServer(t)
	other := webhookRow("w2", "f2", "")
	mine := webhookRow("w1", "f1", "")
	mine[2] = hook.URL
	pg.handle("FROM webhook_actions\n\t\tWHERE id = $1", func(args []driver.Value) fakedb.PGResult {
		if args[0] == "w2" {
			return fakedb.RowsOf(other)
		}
		return fakedb.RowsOf(mine)
	})
	// Another scope's action is indistinguishable from a missing one.
	for _, fn := range []func(h *Handler) http.HandlerFunc{get, upd, del, test} {
		expect(t, serve(h, fn, hcall{as: analyst, params: id("w2"), body: `{}`}), 404, "Webhook not found")
	}
	expect(t, serve(h, get, hcall{as: analyst, params: id("w1")}), 200, `"id":"w1"`)
	expect(t, serve(h, test, hcall{as: analyst, method: "POST", params: id("w1")}), 200, `"sent":true`)

	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("w1"), body: `{`}), 400, "")
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("w1"), body: `{"url":"https://u:p@x"}`}), 400, "username or password")
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("w1"), body: `{"url":"https://hooks.example/u"}`}), 404, "")
	pg.answer("UPDATE webhook_actions", fakedb.Affected(1))
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("w1"), body: `{"url":"https://hooks.example/u"}`}), 200, "")
	pg.answer("UPDATE webhook_actions", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("w1"), body: `{"url":"https://hooks.example/u"}`}), 500, "")

	pg.answer("SELECT COUNT(*) FROM alert_webhook_actions", fakedb.Row(int64(3)))
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("w1")}), 409, "associated with 3 alert(s)")
	pg.answer("SELECT COUNT(*) FROM alert_webhook_actions", fakedb.Row(int64(0)))
	pg.answer("DELETE FROM webhook_actions", fakedb.Affected(1))
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("w1")}), 200, "deleted")
	pg.answer("SELECT COUNT(*) FROM alert_webhook_actions", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("w1")}), 500, "")

	pg.answer("ORDER BY name", fakedb.RowsOf(mine))
	expect(t, serve(h, list, hcall{as: analyst}), 200, `"id":"w1"`)
	pg.answer("ORDER BY name", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, list, hcall{as: analyst}), 500, "")

	pg.answer("INSERT INTO webhook_actions", fakedb.PGResult{Err: errors.New("pq: duplicate key value")})
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"hook","url":"https://x.example.test"}`}), 409, "already exists")
	pg.answer("INSERT INTO webhook_actions", fakedb.Row("w1"))
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"hook","url":"https://x.example.test"}`}), 200, "")
	pg.answer("INSERT INTO webhook_actions", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"hook","url":"https://x.example.test"}`}), 500, "")
}

func TestFractalActionHandlersOverStorage(t *testing.T) {
	get := func(h *Handler) http.HandlerFunc { return h.HandleGetFractalAction }
	upd := func(h *Handler) http.HandlerFunc { return h.HandleUpdateFractalAction }
	del := func(h *Handler) http.HandlerFunc { return h.HandleDeleteFractalAction }
	list := func(h *Handler) http.HandlerFunc { return h.HandleListFractalActions }
	create := func(h *Handler) http.HandlerFunc { return h.HandleCreateFractalAction }

	h, pg := newDBHandler(t)
	for _, fn := range []func(h *Handler) http.HandlerFunc{get, upd, del} {
		expect(t, serve(h, fn, hcall{as: analyst, params: id("fa1"), body: `{}`}), 404, "Fractal action not found")
	}
	pg.handle("FROM fractal_actions fa\n\t\tWHERE fa.id = $1", func(args []driver.Value) fakedb.PGResult {
		if args[0] == "theirs" {
			return fakedb.RowsOf(fractalActionRow("theirs", "f2"))
		}
		return fakedb.RowsOf(fractalActionRow("fa1", "f1"))
	})
	for _, fn := range []func(h *Handler) http.HandlerFunc{get, upd, del} {
		expect(t, serve(h, fn, hcall{as: analyst, params: id("theirs"), body: `{}`}), 404, "")
	}
	expect(t, serve(h, get, hcall{as: analyst, params: id("fa1")}), 200, `"id":"fa1"`)

	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("fa1"), body: `{`}), 400, "")
	pg.answer("SELECT EXISTS(SELECT 1 FROM fractals", fakedb.Row(true))
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("fa1"), body: `{"name":"n","target_fractal_id":"t"}`}), 404, "")
	pg.answer("UPDATE fractal_actions", fakedb.Affected(1))
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("fa1"), body: `{"name":"n","target_fractal_id":"t"}`}), 200, "")
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("fa1"), body: `{"target_fractal_id":"t"}`}), 500, "")

	pg.answer("SELECT COUNT(*) FROM alert_fractal_actions", fakedb.Row(int64(1)))
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("fa1")}), 409, "associated")
	pg.answer("SELECT COUNT(*) FROM alert_fractal_actions", fakedb.Row(int64(0)))
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("fa1")}), 404, "")
	pg.answer("DELETE FROM fractal_actions", fakedb.Affected(1))
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("fa1")}), 200, "deleted")
	pg.answer("DELETE FROM fractal_actions", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("fa1")}), 500, "")

	pg.answer("JOIN fractals f ON fa.target_fractal_id", fakedb.RowsOf(append(fractalActionRow("fa1", "f1"), "Target")))
	expect(t, serve(h, list, hcall{as: analyst}), 200, "Target")
	pg.answer("JOIN fractals f ON fa.target_fractal_id", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, list, hcall{as: analyst}), 500, "")

	pg.answer("INSERT INTO fractal_actions", fakedb.PGResult{Err: errors.New("pq: duplicate key value")})
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"n","target_fractal_id":"t"}`}), 409, "already exists")
	pg.answer("INSERT INTO fractal_actions", fakedb.Row("fa1", fixedTime, fixedTime))
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"n","target_fractal_id":"t"}`}), 200, "")
	pg.answer("SELECT EXISTS(SELECT 1 FROM fractals", fakedb.Row(false))
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"n","target_fractal_id":"t"}`}), 500, "")
}

func TestEmailAndSMTPHandlersOverStorage(t *testing.T) {
	get := func(h *Handler) http.HandlerFunc { return h.HandleGetEmailAction }
	upd := func(h *Handler) http.HandlerFunc { return h.HandleUpdateEmailAction }
	del := func(h *Handler) http.HandlerFunc { return h.HandleDeleteEmailAction }
	test := func(h *Handler) http.HandlerFunc { return h.HandleTestEmailAction }

	h, pg := newDBHandler(t)
	for _, fn := range []func(h *Handler) http.HandlerFunc{get, upd, del, test} {
		expect(t, serve(h, fn, hcall{as: analyst, params: id("e1"), body: `{}`}), 404, "Email action not found")
	}
	pg.handle("FROM email_actions WHERE id = $1", func(args []driver.Value) fakedb.PGResult {
		if args[0] == "theirs" {
			return fakedb.RowsOf(emailActionRow("theirs", "f2"))
		}
		return fakedb.RowsOf(emailActionRow("e1", "f1"))
	})
	for _, fn := range []func(h *Handler) http.HandlerFunc{get, upd, del, test} {
		expect(t, serve(h, fn, hcall{as: analyst, params: id("theirs"), body: `{}`}), 404, "")
	}
	expect(t, serve(h, get, hcall{as: analyst, params: id("e1")}), 200, "soc@example.test")
	// No SMTP configured: the test reports the failure rather than erroring.
	expect(t, serve(h, test, hcall{as: analyst, method: "POST", params: id("e1")}), 200, "SMTP not configured")

	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("e1"), body: `{`}), 400, "")
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("e1"), body: `{"name":"n","recipients":["x"]}`}), 404, "")
	pg.answer("UPDATE email_actions", fakedb.PGResult{Err: errors.New("pq: duplicate key value")})
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("e1"), body: `{"name":"n","recipients":["x"]}`}), 409, "")
	pg.answer("UPDATE email_actions", fakedb.Affected(1))
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("e1"), body: `{"name":"n","recipients":["x"]}`}), 200, "")
	expect(t, serve(h, upd, hcall{as: analyst, method: "PUT", params: id("e1"), body: `{"name":"n"}`}), 500, "")

	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("e1")}), 404, "")
	pg.answer("DELETE FROM email_actions", fakedb.Affected(1))
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("e1")}), 200, "")
	pg.answer("DELETE FROM email_actions", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("e1")}), 500, "")

	pg.answer("FROM email_actions WHERE fractal_id", fakedb.RowsOf(emailActionRow("e1", "f1")))
	expect(t, serve(h, func(h *Handler) http.HandlerFunc { return h.HandleListEmailActions }, hcall{as: analyst}), 200, `"e1"`)
	pg.answer("FROM email_actions WHERE fractal_id", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, func(h *Handler) http.HandlerFunc { return h.HandleListEmailActions }, hcall{as: analyst}), 500, "")

	create := func(h *Handler) http.HandlerFunc { return h.HandleCreateEmailAction }
	pg.answer("INSERT INTO email_actions", fakedb.PGResult{Err: errors.New("pq: duplicate key value")})
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"n","recipients":["x"]}`}), 409, "")
	pg.answer("INSERT INTO email_actions", fakedb.Row("e1"))
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"n","recipients":["x"]}`}), 200, "")
	expect(t, serve(h, create, hcall{as: analyst, method: "POST", body: `{"name":"n"}`}), 500, "")
}

// The stored SMTP password never leaves the server, and saving the masked form keeps it.
func TestSMTPSettingsHandlers(t *testing.T) {
	getSMTP := func(h *Handler) http.HandlerFunc { return h.HandleGetSMTPSettings }
	putSMTP := func(h *Handler) http.HandlerFunc { return h.HandleUpdateSMTPSettings }
	h, pg := newDBHandler(t)

	expect(t, serve(h, getSMTP, hcall{as: admin}), 200, "")
	pg.answer("SELECT value FROM settings WHERE key = $1", fakedb.Row(`{"host":"mail.example.test","password":"hunter2","from_address":"a@example.test"}`))
	rec := serve(h, getSMTP, hcall{as: admin})
	expect(t, rec, 200, `"password":"********"`)
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatal("the stored password was returned")
	}

	expect(t, serve(h, putSMTP, hcall{as: admin, method: "PUT", body: `{"host":"mail.example.test","from_address":"a@example.test","password":"********"}`}), 200, "")
	var saved SMTPConfig
	args := pg.args("INSERT INTO settings")
	if len(args) != 1 {
		t.Fatalf("settings writes = %v", pg.Matching("settings"))
	}
	if err := json.Unmarshal([]byte(args[0][1].(string)), &saved); err != nil || saved.Password != "hunter2" {
		t.Errorf("the masked password must keep the stored one: %+v %v", saved, err)
	}

	pg.answer("INSERT INTO settings", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, putSMTP, hcall{as: admin, method: "PUT", body: `{"host":"h","from_address":"a@example.test"}`}), 500, "")

	pg.answer("SELECT value FROM settings WHERE key = $1", fakedb.Row(`{`))
	expect(t, serve(h, getSMTP, hcall{as: admin}), 500, "")
}

// Alert IDs are global, so a batch toggle must only reach alerts in the caller's own scope.
func TestBatchTogglesStayInCallerScope(t *testing.T) {
	plain := func(h *Handler) http.HandlerFunc { return h.HandleBatchToggleAlerts }
	feed := func(h *Handler) http.HandlerFunc { return h.HandleBatchToggleFeedAlerts }
	prismAnalyst := caller{user: &storage.User{Username: "ana"}, prismRole: "analyst", prism: "p1"}

	for _, tc := range []struct {
		name  string
		fn    func(*Handler) http.HandlerFunc
		as    caller
		where string
		scope string
	}{
		{"alerts in a fractal", plain, analyst, "feed_id IS NULL AND fractal_id = $4", "f1"},
		{"alerts in a prism", plain, prismAnalyst, "feed_id IS NULL AND prism_id = $4", "p1"},
		{"feed alerts in a fractal", feed, analyst, "feed_id IS NOT NULL AND fractal_id = $4", "f1"},
		{"feed alerts in a prism", feed, prismAnalyst, "feed_id IS NOT NULL AND prism_id = $4", "p1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, pg := newDBHandler(t)
			pg.answer("WHERE id = ANY($3)", fakedb.Affected(1))
			expect(t, serve(h, tc.fn, hcall{as: tc.as, method: "POST", body: `{"alert_ids":["mine","other-fractal"],"enabled":true}`}), 200, `"toggled":1`)
			calls := pg.Matching("WHERE id = ANY($3)")
			if len(calls) != 1 || !strings.Contains(calls[0].Query, tc.where) || calls[0].Args[3] != tc.scope {
				t.Fatalf("update not limited to %q: %+v", tc.scope, calls)
			}
		})
	}
}
