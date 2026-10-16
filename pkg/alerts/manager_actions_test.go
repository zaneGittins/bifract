package alerts

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/models"
)

func webhookRow(id, fractalID, prismID string) []driver.Value {
	return []driver.Value{
		id, "hook", "https://hooks.example.test/x", "POST", `{"X-Env":"test"}`, "bearer", `{"token":"t"}`,
		int64(30), int64(3), true, true, "alice", fixedTime, fixedTime, fractalID, prismID,
		BodyModeEnvelope, "", "",
	}
}

func fractalActionRow(id, fractalID string) []driver.Value {
	return []driver.Value{id, "copy", "desc", "target", true, true, []byte(`{"src":"dst"}`), int64(100), true, fractalID, ""}
}

func emailActionRow(id, fractalID string) []driver.Value {
	return []driver.Value{id, "mail", pgArray([]string{"soc@example.test"}), "subj", "body", true, fractalID, ""}
}

func TestWebhookActionCRUD(t *testing.T) {
	ctx := context.Background()

	t.Run("create defaults and scope", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("INSERT INTO webhook_actions", fakedb.Row("w1"))
		pg.answer("FROM webhook_actions\n\t\tWHERE id = $1", fakedb.RowsOf(webhookRow("w1", "", "p1")))
		hook, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{Name: "hook", URL: "https://hooks.example.test/x", RetryCount: -1}, "alice", "", "p1")
		if err != nil {
			t.Fatal(err)
		}
		if hook.Headers["X-Env"] != "test" || hook.AuthConfig["token"] != "t" || hook.PrismID != "p1" {
			t.Errorf("hook = %+v", hook)
		}
		args := pg.args("INSERT INTO webhook_actions")[0]
		if args[2] != "POST" || args[6] != int64(30) || args[7] != int64(3) || args[8] != true || args[11] != nil || args[12] != "p1" {
			t.Errorf("defaults: method %v timeout %v retries %v link %v scope %v/%v", args[2], args[6], args[7], args[8], args[11], args[12])
		}
	})
	t.Run("create refusals", func(t *testing.T) {
		m, pg := newTestManager(t)
		if _, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{URL: "u"}, "a", "f1", "p1"); err == nil {
			t.Error("both scopes must be refused")
		}
		if _, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{URL: " "}, "a", "f1", ""); err == nil {
			t.Error("a blank URL must be refused")
		}
		if _, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{URL: "u", BodyMode: BodyModeTemplate, BodyTemplate: "{{"}, "a", "f1", ""); err == nil {
			t.Error("a broken template must be refused")
		}
		if len(pg.Calls()) != 0 {
			t.Error("a refused create reached the database")
		}
		pg.answer("INSERT INTO webhook_actions", fakedb.PGResult{Err: errors.New(`duplicate key value violates unique constraint`)})
		if _, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{URL: "u"}, "a", "f1", ""); err == nil || !strings.Contains(err.Error(), "duplicate key") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("update", func(t *testing.T) {
		m, pg := newTestManager(t)
		if _, err := m.UpdateWebhookAction(ctx, "w1", WebhookUpdateRequest{URL: "u"}); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("an update matching no row: %v", err)
		}
		pg.answer("UPDATE webhook_actions", fakedb.Affected(1))
		pg.answer("FROM webhook_actions\n\t\tWHERE id = $1", fakedb.RowsOf(webhookRow("w1", "f1", "")))
		off := false
		if _, err := m.UpdateWebhookAction(ctx, "w1", WebhookUpdateRequest{URL: "u", IncludeAlertLink: &off, ContentType: " text/plain "}); err != nil {
			t.Fatal(err)
		}
		args := pg.args("UPDATE webhook_actions")[1]
		if args[9] != false || args[13] != "text/plain" {
			t.Errorf("link %v content type %q", args[9], args[13])
		}
	})
	t.Run("list and get", func(t *testing.T) {
		m, pg := newTestManager(t)
		bad := webhookRow("w2", "f1", "")
		bad[4], bad[6] = "not json", "not json"
		pg.answer("ORDER BY name", fakedb.RowsOf(webhookRow("w1", "f1", ""), bad))
		list, err := m.ListWebhookActions(ctx, true, "f1", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 2 || list[1].Headers == nil || list[1].AuthConfig == nil {
			t.Errorf("unparsable maps fall back to empty: %+v", list)
		}
		c := pg.Matching("FROM webhook_actions")[0]
		if !strings.Contains(c.Query, "fractal_id = $1 AND enabled = true") || c.Args[0] != "f1" {
			t.Errorf("query = %s", c.Query)
		}
		if _, err := m.GetWebhookAction(ctx, "missing"); err == nil {
			t.Error("a missing webhook is an error")
		}
	})
	t.Run("delete", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("SELECT COUNT(*) FROM alert_webhook_actions", fakedb.Row(int64(2)))
		if err := m.DeleteWebhookAction(ctx, "w1"); err == nil || !strings.Contains(err.Error(), "associated with 2 alert(s)") {
			t.Errorf("a linked webhook must not be deleted: %v", err)
		}
		pg.answer("SELECT COUNT(*) FROM alert_webhook_actions", fakedb.Row(int64(0)))
		if err := m.DeleteWebhookAction(ctx, "w1"); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("err = %v", err)
		}
		pg.answer("DELETE FROM webhook_actions", fakedb.Affected(1))
		if err := m.DeleteWebhookAction(ctx, "w1"); err != nil {
			t.Error(err)
		}
	})
	t.Run("test a stored webhook", func(t *testing.T) {
		m, pg := newTestManager(t)
		hook := newHookServer(t)
		row := webhookRow("w1", "f1", "")
		row[2] = hook.URL
		pg.handle("FROM webhook_actions\n\t\tWHERE id = $1", func(args []driver.Value) fakedb.PGResult {
			if args[0] != "w1" {
				return fakedb.PGResult{}
			}
			return fakedb.RowsOf(row)
		})
		res, err := m.TestWebhookAction(ctx, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if !res.Sent || res.Result == nil || !res.Result.Success || len(hook.received()) != 1 {
			t.Errorf("result = %+v", res)
		}
		if _, err := m.TestWebhookAction(ctx, "missing"); err == nil {
			t.Error("testing a missing webhook is an error")
		}
	})
}

func TestFractalActionCRUD(t *testing.T) {
	ctx := context.Background()

	t.Run("create needs an existing target", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("SELECT EXISTS(SELECT 1 FROM fractals", fakedb.Row(false))
		if _, err := m.CreateFractalAction(ctx, FractalActionCreateRequest{Name: "copy", TargetFractalID: "nope"}, "a", "f1", ""); err == nil || !strings.Contains(err.Error(), "target fractal not found") {
			t.Errorf("err = %v", err)
		}
		pg.answer("SELECT EXISTS(SELECT 1 FROM fractals", fakedb.Row(true))
		pg.answer("INSERT INTO fractal_actions", fakedb.Row("fa1", fixedTime, fixedTime))
		pg.answer("FROM fractal_actions fa\n\t\tWHERE fa.id = $1", fakedb.RowsOf(fractalActionRow("fa1", "f1")))
		fa, err := m.CreateFractalAction(ctx, FractalActionCreateRequest{Name: "copy", TargetFractalID: "target"}, "a", "f1", "")
		if err != nil {
			t.Fatal(err)
		}
		if fa.FieldMappings["src"] != "dst" {
			t.Errorf("action = %+v", fa)
		}
		if args := pg.args("INSERT INTO fractal_actions")[0]; args[6] != int64(1000) || args[5] != "{}" || args[9] != "f1" || args[10] != nil {
			t.Errorf("defaults: max %v mappings %v scope %v/%v", args[6], args[5], args[9], args[10])
		}
	})
	t.Run("create refusals", func(t *testing.T) {
		m, pg := newTestManager(t)
		for _, req := range []FractalActionCreateRequest{{TargetFractalID: "t"}, {Name: "n"}} {
			if _, err := m.CreateFractalAction(ctx, req, "a", "f1", ""); err == nil {
				t.Errorf("%+v must be refused", req)
			}
		}
		if _, err := m.CreateFractalAction(ctx, FractalActionCreateRequest{Name: "n", TargetFractalID: "t"}, "a", "", ""); err == nil {
			t.Error("no scope must be refused")
		}
		if len(pg.Calls()) != 0 {
			t.Error("a refused create reached the database")
		}
	})
	t.Run("update", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("SELECT EXISTS(SELECT 1 FROM fractals", fakedb.Row(true))
		if _, err := m.UpdateFractalAction(ctx, "fa1", FractalActionUpdateRequest{Name: "n", TargetFractalID: "t"}); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("err = %v", err)
		}
		pg.answer("UPDATE fractal_actions", fakedb.Affected(1))
		pg.answer("FROM fractal_actions fa\n\t\tWHERE fa.id = $1", fakedb.RowsOf(fractalActionRow("fa1", "f1")))
		if _, err := m.UpdateFractalAction(ctx, "fa1", FractalActionUpdateRequest{Name: "n", TargetFractalID: "t"}); err != nil {
			t.Fatal(err)
		}
		pg.answer("SELECT EXISTS(SELECT 1 FROM fractals", fakedb.Row(false))
		if _, err := m.UpdateFractalAction(ctx, "fa1", FractalActionUpdateRequest{Name: "n", TargetFractalID: "gone"}); err == nil || !strings.Contains(err.Error(), "target fractal not found") {
			t.Errorf("err = %v", err)
		}
		if _, err := m.UpdateFractalAction(ctx, "fa1", FractalActionUpdateRequest{TargetFractalID: "t"}); err == nil {
			t.Error("a blank name must be refused")
		}
	})
	t.Run("list labels the target", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("JOIN fractals f ON fa.target_fractal_id", fakedb.RowsOf(append(fractalActionRow("fa1", "f1"), "Target Fractal")))
		list, err := m.ListFractalActions(ctx, true, "", "p1")
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || list[0].Description != "desc (→ Target Fractal)" {
			t.Errorf("list = %+v", list)
		}
		if c := pg.Matching("JOIN fractals f")[0]; !strings.Contains(c.Query, "fa.prism_id = $1 AND fa.enabled = true") {
			t.Errorf("query = %s", c.Query)
		}
	})
	t.Run("delete", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("SELECT COUNT(*) FROM alert_fractal_actions", fakedb.Row(int64(1)))
		if err := m.DeleteFractalAction(ctx, "fa1"); err == nil || !strings.Contains(err.Error(), "associated with 1 alert(s)") {
			t.Errorf("err = %v", err)
		}
		pg.answer("SELECT COUNT(*) FROM alert_fractal_actions", fakedb.Row(int64(0)))
		pg.answer("DELETE FROM fractal_actions", fakedb.Affected(1))
		if err := m.DeleteFractalAction(ctx, "fa1"); err != nil {
			t.Error(err)
		}
		pg.answer("DELETE FROM fractal_actions", fakedb.Affected(0))
		if err := m.DeleteFractalAction(ctx, "fa1"); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("err = %v", err)
		}
	})
}

func TestEmailActionCRUD(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)

	for _, req := range []EmailActionCreateRequest{{Recipients: []string{"x"}}, {Name: "n"}} {
		if _, err := m.CreateEmailAction(ctx, req, "a", "f1", ""); err == nil {
			t.Errorf("%+v must be refused", req)
		}
	}
	if _, err := m.CreateEmailAction(ctx, EmailActionCreateRequest{Name: "n", Recipients: []string{"x"}}, "a", "", ""); err == nil {
		t.Error("no scope must be refused")
	}

	pg.answer("INSERT INTO email_actions", fakedb.Row("e1"))
	pg.answer("FROM email_actions WHERE id = $1", fakedb.RowsOf(emailActionRow("e1", "f1")))
	ea, err := m.CreateEmailAction(ctx, EmailActionCreateRequest{Name: "mail", Recipients: []string{"soc@example.test"}}, "a", "f1", "")
	if err != nil {
		t.Fatal(err)
	}
	if ea.Recipients[0] != "soc@example.test" {
		t.Errorf("action = %+v", ea)
	}
	if args := pg.args("INSERT INTO email_actions")[0]; args[1] != `{"soc@example.test"}` {
		t.Errorf("recipients bound as %v", args[1])
	}

	pg.answer("FROM email_actions WHERE prism_id", fakedb.RowsOf(emailActionRow("e1", "")))
	list, err := m.ListEmailActions(ctx, "", "p1")
	if err != nil || len(list) != 1 {
		t.Errorf("list = %v %v", list, err)
	}

	if _, err := m.UpdateEmailAction(ctx, "e1", EmailActionUpdateRequest{Name: "n", Recipients: []string{"x"}}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v", err)
	}
	if _, err := m.UpdateEmailAction(ctx, "e1", EmailActionUpdateRequest{Name: "n"}); err == nil {
		t.Error("an update with no recipients must be refused")
	}
	pg.answer("UPDATE email_actions", fakedb.Affected(1))
	if _, err := m.UpdateEmailAction(ctx, "e1", EmailActionUpdateRequest{Name: "n", Recipients: []string{"x"}}); err != nil {
		t.Fatal(err)
	}

	if err := m.DeleteEmailAction(ctx, "e1"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v", err)
	}
	pg.answer("DELETE FROM email_actions", fakedb.Affected(1))
	if err := m.DeleteEmailAction(ctx, "e1"); err != nil {
		t.Error(err)
	}
	// Both an update and a delete change what alerts run, so the engine reloads.
	if n := len(pg.Matching(sqlCacheRefresh)); n != 2 {
		t.Errorf("cache refreshed %d times, want after the update and the delete", n)
	}
}

// Feed alerts arrive disabled with their first revision credited to feed sync.
func TestCreateFeedAlert(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer("INSERT INTO alerts (name, description, query_string, alert_type, severity, enabled", fakedb.Row("fa-1"))
	a, err := m.CreateFeedAlert(context.Background(), "Rule", "d", `status=500`, "", "", []string{"sigma:high"}, nil,
		"feed-1", "rules/a.yml", "hash-1", "f1", "", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != "fa-1" {
		t.Errorf("alert = %+v", a)
	}
	ins := pg.args("INSERT INTO alerts (name, description, query_string, alert_type, severity, enabled")[0]
	if ins[3] != "event" || ins[4] != "medium" || ins[8] != "f1" || ins[9] != nil || ins[10] != "feed-1" {
		t.Errorf("insert = %v", ins)
	}
	rev := pg.args(sqlRevisionInsert)[0]
	if rev[4] != "created" || rev[5] != "owner" || rev[6] != feedAuthorLabel {
		t.Errorf("revision = %v", rev)
	}
	allInTx(t, pg, sqlRevisionInsert)
	if len(pg.Matching(sqlCacheRefresh)) != 0 {
		t.Error("feed sync refreshes once at the end, not per alert")
	}

	if _, err := m.CreateFeedAlert(context.Background(), "Agg", "", `* | groupby(host)`, "event", "", nil, nil, "feed-1", "p", "h", "f1", "", "o"); err == nil {
		t.Error("an aggregating event rule must be refused")
	}
	if _, err := m.CreateFeedAlert(context.Background(), "Bad", "", `a=(`, "", "", nil, nil, "feed-1", "p", "h", "f1", "", "o"); err == nil {
		t.Error("an unparsable rule must be refused")
	}
}

// A feed update keeps the fields the feed does not own and re-asserts the feed's
// scope; a non-feed alert matches nothing and the revision rolls back.
func TestUpdateFeedAlert(t *testing.T) {
	ctx := context.Background()
	if err := (&Manager{}).UpdateFeedAlert(ctx, "a1", "n", "", `a=1`, "", "", nil, nil, "h", "o", "f1", "p1"); err == nil {
		t.Error("both scopes must be refused")
	}

	m, pg := newTestManager(t)
	pg.answer("FROM alerts WHERE id = $1", alertRow("Rule", `status=500`))
	pg.answer("SELECT webhook_id::text FROM alert_webhook_actions", rowsOf(1, "w1"))
	pg.answer("UPDATE alerts\n\t\tSET name = $2, description = $3, query_string = $4, alert_type = $5", fakedb.Affected(1))

	if err := m.UpdateFeedAlert(ctx, "a1", "Rule v2", "desc", `status=500`, "", "high", []string{"b", "a"}, nil, "hash-2", "owner", "", "p1"); err != nil {
		t.Fatal(err)
	}
	revs := pg.args(sqlRevisionInsert)
	if len(revs) != 2 {
		t.Fatalf("want the seed and the update, got %d revisions", len(revs))
	}
	if revs[1][6] != feedAuthorLabel || !strings.Contains(string(revs[1][2].([]byte)), `"webhook_action_ids":["w1"]`) {
		t.Errorf("the update keeps the operator's wiring: %s", revs[1][2])
	}
	upd := pg.args("UPDATE alerts\n\t\tSET name = $2, description = $3")[0]
	if upd[10] != nil || upd[11] != "p1" {
		t.Errorf("scope = %v/%v, want the feed's prism", upd[10], upd[11])
	}
	if commits, _ := pg.txCounts(); commits != 1 || len(pg.Matching(sqlCacheRefresh)) != 1 {
		t.Error("the update commits and refreshes")
	}

	pg.answer("UPDATE alerts\n\t\tSET name = $2, description = $3, query_string = $4, alert_type = $5", fakedb.Affected(0))
	if err := m.UpdateFeedAlert(ctx, "manual", "n", "", `a=1`, "", "", nil, nil, "h", "o", "f1", ""); err == nil || !strings.Contains(err.Error(), "not feed-managed") {
		t.Errorf("err = %v", err)
	}
	if _, rollbacks := pg.txCounts(); rollbacks != 1 {
		t.Error("an update that matched nothing must roll its revision back")
	}
}

func TestFeedAlertMaintenance(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)

	pg.answer("DELETE FROM alerts WHERE feed_id = $1", fakedb.Affected(3))
	if n, err := m.DeleteFeedAlertsNotIn(ctx, "feed-1", nil); err != nil || n != 3 {
		t.Errorf("delete all = %d %v", n, err)
	}
	if n, err := m.DeleteFeedAlertsNotIn(ctx, "feed-1", []string{"keep.yml"}); err != nil || n != 3 {
		t.Errorf("delete others = %d %v", n, err)
	}
	if args := pg.args("feed_rule_path != ALL")[0]; args[1] != `{"keep.yml"}` {
		t.Errorf("keep args = %v", args)
	}
	pg.answer("DELETE FROM alerts WHERE feed_id = $1", fakedb.PGResult{Err: errors.New("x")})
	if _, err := m.DeleteFeedAlertsNotIn(ctx, "feed-1", nil); err == nil {
		t.Error("want the delete error")
	}

	pg.answer("WHERE feed_id = $1 AND feed_rule_path = $2", fakedb.Row("a1", "hash"))
	a, err := m.GetFeedAlertByPath(ctx, "feed-1", "rules/a.yml")
	if err != nil || a.ID != "a1" || a.FeedRuleHash != "hash" || a.FeedRulePath != "rules/a.yml" {
		t.Errorf("by path = %+v %v", a, err)
	}

	pg.answer("WHERE feed_id = $1\n\t\tORDER BY name", fakedb.RowsOf([]driver.Value{
		"a1", "Rule", "d", "q", "event", true, pgArray([]string{"x"}), pgArray(nil), "high", "f1", "", "feed-1", "p", "h", "o", fixedTime, fixedTime, "",
	}))
	list, err := m.ListFeedAlerts(ctx, "feed-1")
	if err != nil || len(list) != 1 || list[0].FeedRulePath != "p" {
		t.Errorf("list = %+v %v", list, err)
	}

	if err := m.EnableFeedAlerts(ctx, "feed-1", true, "bob"); err != nil {
		t.Error(err)
	}
	if args := pg.args("WHERE feed_id = $3")[0]; args[0] != true || args[1] != "bob" || args[2] != "feed-1" {
		t.Errorf("enable args = %v", args)
	}

	if err := m.ToggleFeedAlert(ctx, "a1", false, "bob"); err == nil || !strings.Contains(err.Error(), "feed alert not found") {
		t.Errorf("toggling a non-feed alert: %v", err)
	}
	pg.answer("WHERE id = $3 AND feed_id IS NOT NULL", fakedb.Affected(1))
	if err := m.ToggleFeedAlert(ctx, "a1", false, "bob"); err != nil {
		t.Error(err)
	}

	if n, err := m.BatchToggleAlerts(ctx, nil, true, "bob"); n != 0 || err != nil {
		t.Error("an empty batch is a no-op")
	}
	pg.answer("WHERE id = ANY($3) AND feed_id IS NULL", fakedb.Affected(2))
	if n, err := m.BatchToggleAlerts(ctx, []string{"a1", "a2"}, true, "bob"); n != 2 || err != nil {
		t.Errorf("batch = %d %v", n, err)
	}
	pg.answer("WHERE id = ANY($3) AND feed_id IS NOT NULL", fakedb.Affected(1))
	if n, err := m.BatchToggleFeedAlerts(ctx, []string{"a1"}, false, "bob"); n != 1 || err != nil {
		t.Errorf("feed batch = %d %v", n, err)
	}
	if n, err := m.BatchToggleFeedAlerts(ctx, nil, false, "bob"); n != 0 || err != nil {
		t.Error("an empty feed batch is a no-op")
	}
	m.RefreshCache(ctx)
}

// A copy of a feed alert is a disabled manual alert that keeps the source's
// definition, including its severity.
func TestDuplicateAlertKeepsTheDefinition(t *testing.T) {
	m, pg := newTestManager(t)
	pg.handle(sqlGetAlert, func(args []driver.Value) fakedb.PGResult {
		if args[0] == "missing" {
			return fakedb.PGResult{}
		}
		if args[0] == "copy-id" {
			return fakedb.RowsOf(alertSpec{ID: "copy-id", Name: "Rule (copy)", FractalID: "f1"}.getRow())
		}
		return fakedb.RowsOf(alertSpec{ID: "src", Name: "Rule", FractalID: "f1", FeedID: "feed-1", Severity: "critical",
			Labels: []string{"sigma:critical"}, Enabled: true}.getRow())
	})
	pg.answer(sqlInsertAlert, fakedb.Row("copy-id"))

	a, err := m.DuplicateAlert(context.Background(), "src", "bob")
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != "copy-id" {
		t.Errorf("alert = %+v", a)
	}
	ins := pg.args(sqlInsertAlert)[0]
	if ins[0] != "Rule (copy)" || ins[4] != false || ins[10] != "critical" || ins[12] != "f1" {
		t.Errorf("copy = name %v enabled %v severity %v fractal %v", ins[0], ins[4], ins[10], ins[12])
	}

	if _, err := m.DuplicateAlert(context.Background(), "missing", "bob"); err == nil {
		t.Error("duplicating a missing alert is an error")
	}
}

// A model owns only its alert's name, description and query; everything else stays
// as the operator set it.
func TestLinkedAlerts(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)
	pg.answer(sqlInsertAlert, fakedb.Row("linked-1"))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "linked-1", Name: "Model alert", FractalID: "f1"}.getRow()))

	id, err := m.CreateLinkedAlert(ctx, models.LinkedAlertSpec{Name: "Model alert", QueryString: `status=500`, Severity: "low", FractalID: "f1", CreatedBy: "alice"})
	if err != nil || id != "linked-1" {
		t.Fatalf("create = %q %v", id, err)
	}
	if ins := pg.args(sqlInsertAlert)[0]; ins[3] != "event" || ins[10] != "low" {
		t.Errorf("insert = %v", ins)
	}

	if err := m.UpdateLinkedAlert(ctx, "linked-1", models.LinkedAlertSpec{QueryString: `a=(`}); err == nil {
		t.Error("an unparsable generated query must never be written")
	}
	if err := m.UpdateLinkedAlert(ctx, "linked-1", models.LinkedAlertSpec{Name: "n", QueryString: `a=1`}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v", err)
	}
	pg.answer("UPDATE alerts SET name = $1, description = $2, query_string = $3", fakedb.Affected(1))
	if err := m.UpdateLinkedAlert(ctx, "linked-1", models.LinkedAlertSpec{Name: "n", QueryString: `a=1`}); err != nil {
		t.Error(err)
	}

	if err := m.SetLinkedAlertEnabled(ctx, "linked-1", true); err == nil {
		t.Error("toggling a missing alert is an error")
	}
	pg.answer("WHERE id = $2", fakedb.Affected(1))
	if err := m.SetLinkedAlertEnabled(ctx, "linked-1", true); err != nil {
		t.Error(err)
	}

	pg.answer(sqlAlertScope, fakedb.Row("f1", "", false))
	pg.answer("DELETE FROM alerts WHERE id = $1", fakedb.Affected(1))
	if err := m.DeleteLinkedAlert(ctx, "linked-1"); err != nil {
		t.Error(err)
	}
}
