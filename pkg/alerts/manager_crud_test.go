package alerts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

var sampleTests = []AlertTest{{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"status": "500"}}}}

// statementsInTx reports whether every statement containing sub ran inside a
// transaction, and that at least one did.
func allInTx(t *testing.T, pg *pgFake, sub string) {
	t.Helper()
	calls := pg.Matching(sub)
	if len(calls) == 0 {
		t.Errorf("no statement matched %q", sub)
	}
	for _, c := range calls {
		if !c.InTx {
			t.Errorf("%q ran outside the transaction", sub)
		}
	}
}

func blockingLabelPolicy() fakedb.PGResult {
	return fakedb.RowsOf([]driver.Value{"p1", "labels", "any_matches", `^attack\.t\d{4}`, "tag it", PolicyBlock, true, int64(0)})
}

// A create writes the alert, its first revision, its tests and every action link in
// one transaction, then refreshes the engine and returns the stored alert.
func TestCreateAlertCommitsEverythingTogether(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer("SELECT COUNT(*) FROM webhook_actions", fakedb.Row(int64(1)))
	pg.answer("SELECT COUNT(*) FROM fractal_actions", fakedb.Row(int64(1)))
	pg.answer("SELECT COUNT(*) FROM dictionary_actions", fakedb.Row(int64(1)))
	pg.answer("SELECT COUNT(*) FROM email_actions", fakedb.Row(int64(1)))
	pg.answer(sqlInsertAlert, fakedb.Row("new-id"))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new-id", Name: "Brute force", FractalID: "f1"}.getRow()))
	pg.answer("SELECT dictionary_action_id FROM alert_dictionary_actions", fakedb.RowsOf([]driver.Value{"d1"}))

	tests := sampleTests
	alert, err := m.CreateAlert(context.Background(), AlertCreateRequest{
		Name: "Brute force", QueryString: `status=500`, Labels: []string{"b", "a"},
		WebhookActionIDs: []string{"w1"}, FractalActionIDs: []string{"fa1"}, DictionaryActionIDs: []string{"d1"}, EmailActionIDs: []string{"e1"},
		Tests: &tests,
	}, "alice", "f1", "")
	if err != nil {
		t.Fatal(err)
	}
	if alert.ID != "new-id" || alert.ParsedQuery == nil || strings.Join(alert.DictionaryActionIDs, ",") != "d1" {
		t.Errorf("alert = %+v", alert)
	}

	insert := pg.args(sqlInsertAlert)[0]
	if insert[3] != "event" || insert[10] != "medium" || insert[12] != "f1" || insert[13] != nil {
		t.Errorf("defaults and scope: type %v severity %v fractal %v prism %v", insert[3], insert[10], insert[12], insert[13])
	}
	for _, sub := range []string{sqlInsertAlert, sqlRevisionInsert, "INSERT INTO alert_tests", "INSERT INTO alert_webhook_actions",
		"INSERT INTO alert_fractal_actions", "INSERT INTO alert_dictionary_actions", "INSERT INTO alert_email_actions"} {
		allInTx(t, pg, sub)
	}
	rev := pg.args(sqlRevisionInsert)[0]
	if rev[0] != "new-id" || rev[1] != int64(1) || rev[4] != "created" || rev[5] != "alice" {
		t.Errorf("revision = %v", rev)
	}
	var stored RevisionContent
	if err := json.Unmarshal(rev[2].([]byte), &stored); err != nil || strings.Join(stored.Labels, ",") != "b,a" || strings.Join(stored.WebhookActionIDs, ",") != "w1" {
		t.Errorf("revision content = %+v (%v)", stored, err)
	}
	if commits, _ := pg.txCounts(); commits != 1 {
		t.Errorf("commits = %d", commits)
	}
	if len(pg.Matching(sqlCacheRefresh)) != 1 {
		t.Error("a create must refresh the engine cache")
	}
}

// Any failure inside the transaction rolls the whole create back: no half-wired
// alert, no orphan revision, no cache refresh.
func TestCreateAlertRollsBackOnALinkFailure(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer("SELECT COUNT(*) FROM webhook_actions", fakedb.Row(int64(1)))
	pg.answer(sqlInsertAlert, fakedb.Row("new-id"))
	pg.answer("INSERT INTO alert_webhook_actions", fakedb.PGResult{Err: errors.New("fk violation")})

	_, err := m.CreateAlert(context.Background(), AlertCreateRequest{Name: "n", QueryString: `a=1`, WebhookActionIDs: []string{"w1"}}, "alice", "f1", "")
	if err == nil || !strings.Contains(err.Error(), "associate webhook action") {
		t.Fatalf("err = %v", err)
	}
	commits, rollbacks := pg.txCounts()
	if commits != 0 || rollbacks != 1 {
		t.Errorf("commits %d rollbacks %d", commits, rollbacks)
	}
	if len(pg.Matching(sqlCacheRefresh)) != 0 || len(pg.Matching(sqlGetAlert)) != 0 {
		t.Error("nothing should be refreshed or read back after a rollback")
	}
}

func TestCreateAlertRefusals(t *testing.T) {
	t.Run("an action outside the scope", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("SELECT COUNT(*) FROM email_actions", fakedb.Row(int64(0)))
		_, err := m.CreateAlert(context.Background(), AlertCreateRequest{Name: "n", QueryString: `a=1`, EmailActionIDs: []string{"e-other"}}, "alice", "", "p1")
		if err == nil || !strings.Contains(err.Error(), "invalid email actions") {
			t.Fatalf("err = %v", err)
		}
		if args := pg.args("SELECT COUNT(*) FROM email_actions")[0]; args[1] != "p1" {
			t.Errorf("a prism alert validates against the prism: %v", args)
		}
		if len(pg.Matching(sqlInsertAlert)) != 0 {
			t.Error("nothing may be written")
		}
	})
	t.Run("a gated scope", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer(sqlGateConfig, gateRow(true, 1, true))
		_, err := m.CreateAlert(context.Background(), AlertCreateRequest{Name: "n", QueryString: `a=1`}, "alice", "f1", "")
		var gated *GateRequiredError
		if !errors.As(err, &gated) || gated.Kind != ChangeCreate {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching(sqlInsertAlert)) != 0 {
			t.Error("a gated scope wrote directly")
		}
	})
	t.Run("a blocking policy", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer(sqlPolicies, blockingLabelPolicy())
		_, err := m.CreateAlert(context.Background(), AlertCreateRequest{Name: "n", QueryString: `a=1`, Labels: []string{"noise"}}, "alice", "f1", "")
		var blocked *PolicyBlockedError
		if !errors.As(err, &blocked) || len(blocked.Blocking()) != 1 || !strings.Contains(err.Error(), "tag it") {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching(sqlInsertAlert)) != 0 {
			t.Error("a blocked create wrote directly")
		}
	})
	t.Run("an invalid definition", func(t *testing.T) {
		m, pg := newTestManager(t)
		_, err := m.CreateAlert(context.Background(), AlertCreateRequest{Name: "n", QueryString: `a=1 | groupby(host)`}, "alice", "f1", "")
		if !errors.Is(err, ErrInvalidAlert) {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Calls()) != 0 {
			t.Error("an invalid definition reached the database")
		}
	})
	t.Run("a failed begin", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.FailBegin = errors.New("too many connections")
		if _, err := m.CreateAlert(context.Background(), AlertCreateRequest{Name: "n", QueryString: `a=1`}, "alice", "f1", ""); err == nil || !strings.Contains(err.Error(), "start transaction") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a failed commit", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer(sqlInsertAlert, fakedb.Row("new-id"))
		pg.FailCommit = errors.New("serialization failure")
		if _, err := m.CreateAlert(context.Background(), AlertCreateRequest{Name: "n", QueryString: `a=1`}, "alice", "f1", ""); err == nil || !strings.Contains(err.Error(), "commit") {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching(sqlCacheRefresh)) != 0 {
			t.Error("a failed commit refreshed the cache")
		}
	})
}

// updateFixture scripts an existing manual alert in fractal f1 whose head revision
// is the given content.
func updateFixture(t *testing.T, head RevisionContent, feed bool) (*Manager, *pgFake) {
	t.Helper()
	m, pg := newTestManager(t)
	pg.answer(sqlAlertScope, fakedb.Row("f1", "", feed))
	pg.answer(sqlLockAlert, fakedb.Row("a1"))
	pg.answer("SELECT revision, content_hash, content", headAnswer(t, 3, head))
	pg.answer("UPDATE alerts\n\t\tSET name = $2", fakedb.Affected(1))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "after", FractalID: "f1"}.getRow()))
	return m, pg
}

// An update records the new head revision, rewrites every action link and the alert
// row in one transaction, and only then refreshes the engine.
func TestUpdateAlertRewritesInOneTransaction(t *testing.T) {
	head := RevisionContent{Name: "before", QueryString: `a=1`, AlertType: "event", Severity: "medium"}
	head.canonicalize()
	m, pg := updateFixture(t, head, false)
	pg.answer("SELECT COUNT(*) FROM webhook_actions", fakedb.Row(int64(1)))

	alert, err := m.UpdateAlert(context.Background(), "a1", AlertUpdateRequest{
		Name: "after", QueryString: `a=1`, Enabled: true, WebhookActionIDs: []string{"w1"},
	}, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if alert.ParsedQuery == nil {
		t.Error("the parsed query is returned with the alert")
	}

	revs := pg.args(sqlRevisionInsert)
	if len(revs) != 1 || revs[0][1] != int64(4) || revs[0][4] != "name, actions" || revs[0][5] != "bob" {
		t.Errorf("revisions = %v", revs)
	}
	for _, sub := range []string{sqlLockAlert, sqlRevisionInsert, "UPDATE alerts\n\t\tSET name = $2",
		"DELETE FROM alert_webhook_actions", "INSERT INTO alert_webhook_actions",
		"DELETE FROM alert_fractal_actions", "DELETE FROM alert_dictionary_actions", "DELETE FROM alert_email_actions"} {
		allInTx(t, pg, sub)
	}
	upd := pg.args("UPDATE alerts\n\t\tSET name = $2")[0]
	if upd[0] != "a1" || upd[1] != "after" || upd[4] != true || upd[10] != "bob" || upd[11] != "event" {
		t.Errorf("update args = %v", upd)
	}
	if commits, _ := pg.txCounts(); commits != 1 {
		t.Errorf("commits = %d", commits)
	}
	if len(pg.Matching(sqlCacheRefresh)) != 1 {
		t.Error("an update must refresh the engine cache")
	}
}

// An unchanged definition records no revision, which keeps feed syncs and no-op
// saves out of the history.
func TestUpdateAlertWithoutChangeAddsNoRevision(t *testing.T) {
	head := RevisionContent{Name: "same", QueryString: `a=1`, AlertType: "event", Severity: "medium"}
	head.canonicalize()
	m, pg := updateFixture(t, head, false)
	if _, err := m.UpdateAlert(context.Background(), "a1", AlertUpdateRequest{Name: "same", QueryString: `a=1`}, "bob"); err != nil {
		t.Fatal(err)
	}
	if n := len(pg.Matching(sqlRevisionInsert)); n != 0 {
		t.Errorf("an unchanged save wrote %d revisions", n)
	}
}

func TestUpdateAlertRefusalsAndRollback(t *testing.T) {
	head := RevisionContent{Name: "before", QueryString: `a=1`, AlertType: "event", Severity: "medium"}
	head.canonicalize()
	req := AlertUpdateRequest{Name: "after", QueryString: `a=1`}

	t.Run("a missing alert", func(t *testing.T) {
		m, pg := newTestManager(t)
		if _, err := m.UpdateAlert(context.Background(), "nope", req, "bob"); err == nil || !strings.Contains(err.Error(), "load alert scope") {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching("UPDATE alerts")) != 0 {
			t.Error("nothing may be written")
		}
	})
	t.Run("deleted between the scope read and the lock", func(t *testing.T) {
		m, pg := updateFixture(t, head, false)
		pg.answer(sqlLockAlert, fakedb.PGResult{})
		if _, err := m.UpdateAlert(context.Background(), "a1", req, "bob"); !errors.Is(err, ErrAlertNotFound) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a gated scope", func(t *testing.T) {
		m, pg := updateFixture(t, head, false)
		pg.answer(sqlGateConfig, gateRow(true, 2, false))
		var gated *GateRequiredError
		if _, err := m.UpdateAlert(context.Background(), "a1", req, "bob"); !errors.As(err, &gated) || gated.Kind != ChangeUpdate {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching(sqlLockAlert)) != 0 {
			t.Error("a gated update opened a transaction")
		}
	})
	t.Run("a feed alert skips the gate and policies", func(t *testing.T) {
		m, pg := updateFixture(t, head, true)
		pg.answer(sqlGateConfig, gateRow(true, 2, false))
		pg.answer(sqlPolicies, blockingLabelPolicy())
		if _, err := m.UpdateAlert(context.Background(), "a1", req, "bob"); err != nil {
			t.Fatalf("a feed alert is exempt from the gate and policies: %v", err)
		}
		if len(pg.Matching(sqlPolicies)) != 0 {
			t.Error("policies were evaluated for a feed alert")
		}
	})
	t.Run("a blocking policy judges the stored tests", func(t *testing.T) {
		m, pg := updateFixture(t, head, false)
		pg.answer(sqlPolicies, blockingLabelPolicy())
		var blocked *PolicyBlockedError
		if _, err := m.UpdateAlert(context.Background(), "a1", req, "bob"); !errors.As(err, &blocked) {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching("FROM alert_tests WHERE alert_id")) != 1 {
			t.Error("with no tests in the request, the stored ones are judged")
		}
	})
	t.Run("a failed write rolls back the revision", func(t *testing.T) {
		m, pg := updateFixture(t, head, false)
		pg.answer("DELETE FROM alert_email_actions", fakedb.PGResult{Err: errors.New("deadlock")})
		if _, err := m.UpdateAlert(context.Background(), "a1", req, "bob"); err == nil {
			t.Fatal("want the write error")
		}
		commits, rollbacks := pg.txCounts()
		if commits != 0 || rollbacks != 1 {
			t.Errorf("commits %d rollbacks %d: the revision would outlive the failed edit", commits, rollbacks)
		}
	})
	t.Run("replaced tests are validated in the transaction", func(t *testing.T) {
		m, pg := updateFixture(t, head, false)
		bad := []AlertTest{{Name: "", Expectation: "match"}}
		if _, err := m.UpdateAlert(context.Background(), "a1", AlertUpdateRequest{Name: "after", QueryString: `a=1`, Tests: &bad}, "bob"); err == nil || !strings.Contains(err.Error(), "test name is required") {
			t.Fatalf("err = %v", err)
		}
		if _, rollbacks := pg.txCounts(); rollbacks != 1 {
			t.Error("the revision must roll back with the refused tests")
		}
	})
}

func TestDeleteAlert(t *testing.T) {
	t.Run("deleted and refreshed", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer(sqlAlertScope, fakedb.Row("f1", "", false))
		pg.answer("DELETE FROM alerts WHERE id = $1", fakedb.Affected(1))
		if err := m.DeleteAlert(context.Background(), "a1"); err != nil {
			t.Fatal(err)
		}
		if len(pg.Matching(sqlCacheRefresh)) != 1 {
			t.Error("a delete must refresh the engine cache")
		}
	})
	t.Run("missing", func(t *testing.T) {
		m, _ := newTestManager(t)
		if err := m.DeleteAlert(context.Background(), "a1"); !errors.Is(err, ErrAlertNotFound) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("gated", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer(sqlAlertScope, fakedb.Row("", "p1", false))
		pg.answer(sqlGateConfig, gateRow(true, 1, true))
		var gated *GateRequiredError
		if err := m.DeleteAlert(context.Background(), "a1"); !errors.As(err, &gated) || gated.Kind != ChangeDelete {
			t.Fatalf("err = %v", err)
		}
		if args := pg.args(sqlGateConfig)[0]; args[0] != "p1" {
			t.Errorf("a prism alert's gate is the prism's: %v", args)
		}
		if len(pg.Matching("DELETE FROM alerts")) != 0 {
			t.Error("a gated delete went through")
		}
	})
	t.Run("feed alert bypasses the gate", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer(sqlAlertScope, fakedb.Row("f1", "", true))
		pg.answer(sqlGateConfig, gateRow(true, 1, true))
		pg.answer("DELETE FROM alerts WHERE id = $1", fakedb.Affected(1))
		if err := m.DeleteAlert(context.Background(), "a1"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("lost race", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer(sqlAlertScope, fakedb.Row("f1", "", false))
		if err := m.DeleteAlert(context.Background(), "a1"); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestGetAlert(t *testing.T) {
	m, pg := newTestManager(t)
	if _, err := m.GetAlert(context.Background(), "nope"); !errors.Is(err, ErrAlertNotFound) {
		t.Errorf("a missing alert is ErrAlertNotFound, got %v", err)
	}
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "n", FractalID: "f1",
		Webhooks: `[{"id":"w1","name":"hook","enabled":true}]`, Fractals: `[{"id":"fa1","target_fractal_id":"t"}]`,
		Dicts: `[{"id":"d1","name":"dict"}]`, Emails: `[{"id":"e1","name":"mail","recipients":["soc@example.test"]}]`}.getRow()))
	pg.answer("SELECT email_action_id FROM alert_email_actions", fakedb.RowsOf([]driver.Value{"e1"}))
	a, err := m.GetAlert(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if len(a.WebhookActions) != 1 || len(a.FractalActions) != 1 || len(a.DictionaryActionRefs) != 1 || len(a.EmailActions) != 1 ||
		strings.Join(a.EmailActionIDs, ",") != "e1" {
		t.Errorf("alert = %+v", a)
	}

	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "n", Emails: `{broken`}.getRow()))
	if _, err := m.GetAlert(context.Background(), "a1"); err == nil || !strings.Contains(err.Error(), "email actions") {
		t.Errorf("err = %v", err)
	}
}

// The list is scope-bound and lists every action id the alert runs, including
// dictionary actions: export and policy compliance both read ids from it.
func TestListAlerts(t *testing.T) {
	m, pg := newTestManager(t)
	if _, err := m.ListAlerts(context.Background(), false, "", ""); err == nil {
		t.Error("an unscoped list must be refused, not span every scope")
	}

	pg.answer(sqlListAlerts, fakedb.RowsOf(alertSpec{ID: "a1", Name: "n", FractalID: "f1",
		Dicts: `[{"id":"d1","name":"dict"}]`, Emails: `[{"id":"e1","name":"mail"}]`}.listRow()))
	alerts, err := m.ListAlerts(context.Background(), true, "", "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 {
		t.Fatalf("got %d alerts", len(alerts))
	}
	if strings.Join(alerts[0].EmailActionIDs, ",") != "e1" || strings.Join(alerts[0].DictionaryActionIDs, ",") != "d1" {
		t.Errorf("action ids = email %v dictionary %v", alerts[0].EmailActionIDs, alerts[0].DictionaryActionIDs)
	}
	call := pg.Matching(sqlListAlerts)[0]
	if !strings.Contains(call.Query, "a.prism_id = $1") || !strings.Contains(call.Query, "a.enabled = $2") || call.Args[0] != "p1" {
		t.Errorf("query = %s %v", call.Query, call.Args)
	}

	pg.answer(sqlListAlerts, fakedb.PGResult{Err: errors.New("down")})
	if _, err := m.ListAlerts(context.Background(), false, "f1", ""); err == nil {
		t.Error("want the query error")
	}
}

func TestListCoverageRows(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer("LEFT JOIN alert_feeds f ON f.id = a.feed_id", fakedb.RowsOf(
		[]driver.Value{"a1", "Rule", "high", true, pgArray([]string{"attack.t1078"}), "feed-1", "Sigma"},
	))
	rows, err := m.ListCoverageRows(context.Background(), "f1", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].FeedName != "Sigma" || rows[0].Labels[0] != "attack.t1078" {
		t.Errorf("rows = %+v", rows)
	}
	if c := pg.Matching("LEFT JOIN alert_feeds")[0]; !strings.Contains(c.Query, "a.fractal_id = $1") || c.Args[0] != "f1" {
		t.Errorf("query = %s", c.Query)
	}
}

// Import resolves action names across every kind in scope, refuses an ambiguous or
// missing name, and replaces an alert of the same name in the same scope.
func TestImportFromYAMLResolvesActionsAndReplaces(t *testing.T) {
	doc := "name: Brute force\nqueryString: status=500\nactionNames: [notify, copy]\nseverity: high\n"

	t.Run("creates with resolved actions", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("UNION ALL", fakedb.RowsOf([]driver.Value{"w1", "notify", "webhook"}, []driver.Value{"fa1", "copy", "fractal"}))
		pg.answer("SELECT COUNT(*) FROM webhook_actions", fakedb.Row(int64(1)))
		pg.answer("SELECT COUNT(*) FROM fractal_actions", fakedb.Row(int64(1)))
		pg.answer(sqlInsertAlert, fakedb.Row("new-id"))
		pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new-id", Name: "Brute force", FractalID: "f1"}.getRow()))
		if _, err := m.ImportFromYAML(context.Background(), doc, "alice", "f1", "", ""); err != nil {
			t.Fatal(err)
		}
		if args := pg.args("INSERT INTO alert_webhook_actions"); len(args) != 1 || args[0][1] != "w1" {
			t.Errorf("webhook links = %v", args)
		}
		if args := pg.args("UNION ALL")[0]; args[0] != `{"notify","copy"}` || args[1] != "f1" {
			t.Errorf("resolution args = %v", args)
		}
	})
	t.Run("ambiguous name", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("UNION ALL", fakedb.RowsOf([]driver.Value{"w1", "notify", "webhook"}, []driver.Value{"e1", "notify", "email"}, []driver.Value{"fa1", "copy", "fractal"}))
		_, err := m.ImportFromYAML(context.Background(), doc, "alice", "f1", "", "")
		if err == nil || !strings.Contains(err.Error(), `"notify" is ambiguous: it matches email and webhook`) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing name", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("UNION ALL", fakedb.RowsOf([]driver.Value{"w1", "notify", "webhook"}))
		if _, err := m.ImportFromYAML(context.Background(), doc, "alice", "f1", "", ""); err == nil || !strings.Contains(err.Error(), "actions not found: copy") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("replaces the same-named alert in scope", func(t *testing.T) {
		head := RevisionContent{Name: "Brute force", QueryString: `status=500`, AlertType: "event", Severity: "medium"}
		head.canonicalize()
		m, pg := updateFixture(t, head, false)
		pg.answer("FROM alerts WHERE name = $1 AND feed_id IS NULL", fakedb.Row("a1"))
		if _, err := m.ImportFromYAML(context.Background(), "name: Brute force\nqueryString: status=500\nseverity: high\n", "alice", "f1", "", ""); err != nil {
			t.Fatal(err)
		}
		if len(pg.Matching(sqlInsertAlert)) != 0 || len(pg.Matching("UPDATE alerts\n\t\tSET name = $2")) != 1 {
			t.Error("a same-named alert in scope is updated, not duplicated")
		}
	})
	// Names are unique per fractal or prism, so the same rule can live in several.
	t.Run("looks the name up in its own scope only", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer(sqlInsertAlert, fakedb.Row("new-id"))
		pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new-id", Name: "Brute force", FractalID: "f1"}.getRow()))
		if _, err := m.ImportFromYAML(context.Background(), "name: Brute force\nqueryString: status=500\n", "alice", "f1", "", ""); err != nil {
			t.Fatal(err)
		}
		lookup := pg.args("FROM alerts WHERE name = $1 AND feed_id IS NULL")
		if len(lookup) != 1 || lookup[0][0] != "Brute force" || lookup[0][1] != "f1" || lookup[0][2] != "" {
			t.Errorf("name lookup args = %v, want the name and this scope", lookup)
		}
		if len(pg.Matching(sqlInsertAlert)) != 1 {
			t.Error("with no same-named alert in scope, the import creates one")
		}
	})
	t.Run("unreadable documents", func(t *testing.T) {
		m, _ := newTestManager(t)
		for doc, want := range map[string]string{
			"name: [":                   "failed to parse YAML",
			"queryString: a=1":          "alert name is required",
			"name: n":                   "query string is required",
			"name: n\nqueryString: a=(": "invalid query syntax",
		} {
			if _, err := m.ImportFromYAML(context.Background(), doc, "alice", "f1", "", ""); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("%q: err = %v, want %q", doc, err, want)
			}
		}
	})
}

// A Sigma document imports disabled with its provenance in the description.
func TestImportSigmaRuleArrivesDisabled(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer(sqlInsertAlert, fakedb.Row("new-id"))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new-id", Name: "Whoami", FractalID: "f1"}.getRow()))
	sigmaDoc := `title: Whoami
id: 11111111-2222-3333-4444-555555555555
status: test
description: Detects whoami
author: someone
level: high
tags:
  - attack.t1033
logsource:
  product: windows
  category: process_creation
detection:
  selection:
    Image|endswith: '\whoami.exe'
  condition: selection
`
	if _, err := m.ImportFromYAML(context.Background(), sigmaDoc, "alice", "f1", "", ""); err != nil {
		t.Fatal(err)
	}
	insert := pg.args(sqlInsertAlert)[0]
	if insert[0] != "Whoami" || insert[4] != false || insert[10] != "high" {
		t.Errorf("name %v enabled %v severity %v", insert[0], insert[4], insert[10])
	}
	if desc := insert[1].(string); !strings.Contains(desc, "Sigma ID: 11111111") || !strings.Contains(desc, "Author: someone") {
		t.Errorf("description = %q", desc)
	}
}
