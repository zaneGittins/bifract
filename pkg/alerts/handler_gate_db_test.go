package alerts

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
)

var reviewer = caller{user: &storage.User{Username: "bob"}, fractalRole: "analyst", fractal: "f1"}

func TestChangeRequestDetailHandler(t *testing.T) {
	get := func(h *Handler) http.HandlerFunc { return h.HandleGetChangeRequest }
	h, pg := newDBHandler(t)
	store := newCRStore(t, pg)
	store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeOpen, Content: eventContent("new"), Author: "alice"}, "f1", "")
	store.add(crSpec{ID: "draft", Kind: ChangeCreate, Status: ChangeDraft, Content: eventContent("wip"), Author: "alice"}, "f1", "")
	store.add(crSpec{ID: "elsewhere", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("x"), Author: "alice"}, "f2", "")
	store.approve("cr1", "carol")
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "old", FractalID: "f1"}.getRow()))

	rec := serve(h, get, hcall{as: reviewer, params: id("cr1")})
	expect(t, rec, 200, `"can_merge":true`)
	for _, want := range []string{`"can_review":true`, `"is_author":false`, `"current":{"name":"old"`, `"approvals":1`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("detail lacks %s: %s", want, rec.Body.String())
		}
	}

	// A draft does not exist for anyone but its author.
	expect(t, serve(h, get, hcall{as: reviewer, params: id("draft")}), 404, "Proposal not found")
	expect(t, serve(h, get, hcall{as: analyst, params: id("missing")}), 404, "Proposal not found")
	// Authorized against the proposal's scope, not the caller's selection.
	expect(t, serve(h, get, hcall{as: keyF1, params: id("elsewhere")}), 403, "")
}

func TestChangeRequestActionHandlers(t *testing.T) {
	review := func(h *Handler) http.HandlerFunc { return h.HandleReviewChangeRequest }
	merge := func(h *Handler) http.HandlerFunc { return h.HandleMergeChangeRequest }
	withdraw := func(h *Handler) http.HandlerFunc { return h.HandleWithdrawChangeRequest }
	del := func(h *Handler) http.HandlerFunc { return h.HandleDeleteChangeRequest }

	h, pg := newDBHandler(t)
	store := newCRStore(t, pg)
	store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")

	expect(t, serve(h, review, hcall{as: reviewer, method: "POST", params: id("cr1"), body: `{`}), 400, "")
	expect(t, serve(h, review, hcall{as: viewer, method: "POST", params: id("cr1"), body: `{"decision":"approve"}`}), 403, "")
	expect(t, serve(h, review, hcall{as: reviewer, method: "POST", params: id("cr1"), body: `{"decision":"meh"}`}), 400, "must be")
	expect(t, serve(h, review, hcall{as: caller{user: &storage.User{Username: "alice"}, fractalRole: "analyst", fractal: "f1"}, method: "POST",
		params: id("cr1"), body: `{"decision":"approve"}`}), 403, "cannot be approved by its author")
	expect(t, serve(h, review, hcall{as: reviewer, method: "POST", params: id("cr1"), body: `{"decision":"approve","comment":"ok"}`}), 200, "")

	// Unapproved in the store's view: merge answers 409 with the readiness attached.
	rec := serve(h, merge, hcall{as: reviewer, method: "POST", params: id("cr1")})
	expect(t, rec, 409, `"min_approvals":1`)

	expect(t, serve(h, withdraw, hcall{as: reviewer, method: "POST", params: id("cr1")}), 403, "only the author or an admin")
	expect(t, serve(h, withdraw, hcall{as: admin, method: "POST", params: id("cr1")}), 409, "no longer open")

	expect(t, serve(h, del, hcall{as: reviewer, method: "DELETE", params: id("cr1")}), 403, "")
	expect(t, serve(h, del, hcall{as: admin, method: "DELETE", params: id("cr1")}), 404, "")
	pg.answer("DELETE FROM alert_change_requests WHERE id = $1", fakedb.Affected(1))
	expect(t, serve(h, del, hcall{as: admin, method: "DELETE", params: id("cr1")}), 200, `"deleted":true`)

	// A merge that clears every check.
	store.approve("cr1", "bob")
	pg.answer(sqlInsertAlert, fakedb.Row("new"))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new", Name: "n", FractalID: "f1"}.getRow()))
	expect(t, serve(h, merge, hcall{as: reviewer, method: "POST", params: id("cr1")}), 200, `"status":"merged"`)
	// Merged now: a second merge is refused as a conflict.
	expect(t, serve(h, merge, hcall{as: reviewer, method: "POST", params: id("cr1")}), 409, "is merged")
	// The failure classification for everything else.
	pg.answer("WHERE cr.id = $1", fakedb.PGResult{Err: errors.New("connection reset")})
	expect(t, serve(h, merge, hcall{as: reviewer, method: "POST", params: id("cr1")}), 500, "")
}

func TestChangeRequestQueueHandlers(t *testing.T) {
	list := func(h *Handler) http.HandlerFunc { return h.HandleListChangeRequests }
	submit := func(h *Handler) http.HandlerFunc { return h.HandleSubmitChangeRequest }
	getCfg := func(h *Handler) http.HandlerFunc { return h.HandleGetGateConfig }
	saveCfg := func(h *Handler) http.HandlerFunc { return h.HandleSaveGateConfig }

	h, pg := newDBHandler(t)
	store := newCRStore(t, pg)
	store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "ana"}, "f1", "")

	expect(t, serve(h, list, hcall{as: viewer, query: "open=true"}), 200, `"id":"cr1"`)
	expect(t, serve(h, getCfg, hcall{as: viewer}), 200, `"min_approvals":1`)
	expect(t, serve(h, saveCfg, hcall{as: analyst, method: "PUT", body: `{"enabled":true,"min_approvals":1}`}), 403, "")
	expect(t, serve(h, saveCfg, hcall{as: admin, method: "PUT", body: `{"enabled":true,"min_approvals":0}`}), 400, "at least one approval")
	expect(t, serve(h, saveCfg, hcall{as: admin, method: "PUT", body: `{"enabled":true,"min_approvals":2}`}), 200, `"min_approvals":2`)

	pg.answer("INSERT INTO alert_change_requests", fakedb.Row("cr1"))
	expect(t, serve(h, submit, hcall{as: analyst, method: "POST", body: `{"kind":"create","title":"t","content":{"name":"n","query_string":"a=1"}}`}), 200, `"id":"cr1"`)
	expect(t, serve(h, submit, hcall{as: analyst, method: "POST", body: `{"kind":"create","title":"t","content":{"name":"n","query_string":"* | count()"}}`}), 400, "aggregate")
	// Revising: the author's own proposal in its own scope.
	expect(t, serve(h, submit, hcall{as: analyst, method: "PUT", params: id("cr1"), body: `{"kind":"create","title":"t2","content":{"name":"n","query_string":"a=2"}}`}), 200, "")
	expect(t, serve(h, submit, hcall{as: reviewer, method: "PUT", params: id("cr1"), body: `{"kind":"create","title":"t2","content":{"name":"n","query_string":"a=2"}}`}), 403, "only the author")

	pg.answer(sqlGateConfig, fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, getCfg, hcall{as: viewer}), 500, "")
	pg.answer(sqlCRSelect, fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, list, hcall{as: viewer}), 500, "")
}

func TestProposeAndDraftFromYAMLHandlers(t *testing.T) {
	propose := func(h *Handler) http.HandlerFunc { return h.HandleProposeFromYAML }
	draft := func(h *Handler) http.HandlerFunc { return h.HandleDraftFromYAML }
	h, pg := newDBHandler(t)
	store := newCRStore(t, pg)
	pg.handle("INSERT INTO alert_change_requests", func([]driver.Value) fakedb.PGResult {
		store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("Rule"), Author: "ana"}, "f1", "")
		return fakedb.Row("cr1")
	})
	expect(t, serve(h, propose, hcall{as: analyst, method: "POST", body: `{"content":"name: Rule\nqueryString: a=1\n","summary":"new rule"}`}), 200, `"id":"cr1"`)
	expect(t, serve(h, propose, hcall{as: analyst, method: "POST", body: `{"content":"name: [","summary":"x"}`}), 400, "failed to parse YAML")

	pg.handle("VALUES ($1, $2, $3, 'draft'", func([]driver.Value) fakedb.PGResult {
		store.add(crSpec{ID: "d1", Kind: ChangeCreate, Status: ChangeDraft, Content: eventContent("Rule"), Author: "ana"}, "f1", "")
		return fakedb.Row("d1")
	})
	expect(t, serve(h, draft, hcall{as: analyst, method: "POST", body: `{"content":"name: Rule\nqueryString: a=1\n"}`}), 200, `"id":"d1"`)
}

func TestDraftHandlers(t *testing.T) {
	list := func(h *Handler) http.HandlerFunc { return h.HandleListDrafts }
	forAlert := func(h *Handler) http.HandlerFunc { return h.HandleDraftForAlert }
	save := func(h *Handler) http.HandlerFunc { return h.HandleSaveDraft }
	submit := func(h *Handler) http.HandlerFunc { return h.HandleSubmitDraft }
	del := func(h *Handler) http.HandlerFunc { return h.HandleDeleteDraft }

	h, pg := newDBHandler(t)
	store := newCRStore(t, pg)
	store.add(crSpec{ID: "d1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeDraft, Content: eventContent("n"), Author: "ana"}, "f1", "")
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "n", FractalID: "f1"}.getRow()))

	expect(t, serve(h, list, hcall{as: analyst}), 200, `"id":"d1"`)
	expect(t, serve(h, forAlert, hcall{as: analyst, params: id("a1")}), 200, `"data":null`)
	pg.answer("AND created_by = $2 AND status = 'draft'", fakedb.Row("d1"))
	expect(t, serve(h, forAlert, hcall{as: analyst, params: id("a1")}), 200, `"id":"d1"`)

	pg.answer("COALESCE(prism_id::text, '') FROM alerts WHERE id", fakedb.Row("f1", ""))
	expect(t, serve(h, save, hcall{as: analyst, method: "POST", body: `{`}), 400, "")
	expect(t, serve(h, save, hcall{as: analyst, method: "POST", body: `{"alert_id":"a1","content":{"name":"n2"}}`}), 200, `"id":"d1"`)
	expect(t, serve(h, save, hcall{as: analyst, method: "POST", body: `{"alert_id":"a1"}`}), 400, "needs a definition")

	expect(t, serve(h, submit, hcall{as: analyst, method: "POST", params: id("d1")}), 200, "")
	expect(t, serve(h, submit, hcall{as: analyst, method: "POST", params: id("missing")}), 404, "")

	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("d1")}), 404, "Draft not found")
	pg.answer("DELETE FROM alert_change_requests WHERE id = $1 AND created_by = $2", fakedb.Affected(1))
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("d1")}), 200, "")
	pg.answer("DELETE FROM alert_change_requests WHERE id = $1 AND created_by = $2", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, del, hcall{as: analyst, method: "DELETE", params: id("d1")}), 500, "")

	pg.answer(sqlCRSelect, fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, list, hcall{as: analyst}), 500, "")
	pg.answer("AND created_by = $2 AND status = 'draft'", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, forAlert, hcall{as: analyst, params: id("a1")}), 500, "")
}

func TestRevisionAndTestHandlers(t *testing.T) {
	list := func(h *Handler) http.HandlerFunc { return h.HandleListRevisions }
	get := func(h *Handler) http.HandlerFunc { return h.HandleGetRevision }
	restore := func(h *Handler) http.HandlerFunc { return h.HandleRestoreRevision }
	tests := func(h *Handler) http.HandlerFunc { return h.HandleListTests }
	withRev := func(alert, rev string) map[string]string { return map[string]string{"id": alert, "revision": rev} }

	h, pg := newDBHandler(t)
	expect(t, serve(h, list, hcall{as: viewer, params: id("a1")}), 404, "")
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "a1", Name: "n", FractalID: "f1"}.getRow()))

	c := RevisionContent{Name: "n", QueryString: `a=1`, AlertType: "event", Severity: "medium"}
	pg.answer("FROM alert_revisions r", fakedb.RowsOf(revisionRow(t, 1, "created", c)))
	expect(t, serve(h, list, hcall{as: viewer, params: id("a1")}), 200, `"is_head":true`)
	expect(t, serve(h, get, hcall{as: viewer, params: withRev("a1", "x")}), 400, "must be a number")
	expect(t, serve(h, get, hcall{as: viewer, params: withRev("a1", "7")}), 404, "Revision not found")
	pg.answer("FROM alert_revisions WHERE alert_id = $1 AND revision = $2", fakedb.RowsOf(revisionRow(t, 1, "created", c)))
	expect(t, serve(h, get, hcall{as: viewer, params: withRev("a1", "1")}), 200, `"revision":1`)

	expect(t, serve(h, restore, hcall{as: viewer, method: "POST", params: withRev("a1", "1")}), 403, "")
	expect(t, serve(h, restore, hcall{as: analyst, method: "POST", params: withRev("a1", "x")}), 400, "")
	// The alert row the restore reads its state from is gone.
	expect(t, serve(h, restore, hcall{as: analyst, method: "POST", params: withRev("a1", "1")}), 404, "Alert not found")

	withHook := c
	withHook.WebhookActionIDs = []string{"w-gone"}
	pg.answer("FROM alert_revisions WHERE alert_id = $1 AND revision = $2", fakedb.RowsOf(revisionRow(t, 1, "created", withHook)))
	pg.answer("SELECT enabled, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '') FROM alerts", fakedb.Row(true, "f1", ""))
	expect(t, serve(h, restore, hcall{as: analyst, method: "POST", params: withRev("a1", "1"), body: `{}`}), 409, `"id":"w-gone"`)
	pg.answer(sqlGateConfig, gateRow(true, 1, true))
	pg.answer("FROM alert_revisions WHERE alert_id = $1 AND revision = $2", fakedb.RowsOf(revisionRow(t, 1, "created", c)))
	pg.answer(sqlAlertScope, fakedb.Row("f1", "", false))
	expect(t, serve(h, restore, hcall{as: analyst, method: "POST", params: withRev("a1", "1")}), 409, `"gate":"required"`)

	pg.answer("FROM alert_tests WHERE alert_id", fakedb.RowsOf([]driver.Value{"t1", "fires", "match", []byte(`[{"a":"1"}]`), int64(0)}))
	expect(t, serve(h, tests, hcall{as: viewer, params: id("a1")}), 200, `"name":"fires"`)
	pg.answer("FROM alert_tests WHERE alert_id", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, tests, hcall{as: viewer, params: id("a1")}), 500, "")
	pg.answer("FROM alert_revisions r", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, list, hcall{as: viewer, params: id("a1")}), 500, "")
}

func TestPolicyHandlersOverStorage(t *testing.T) {
	list := func(h *Handler) http.HandlerFunc { return h.HandleListPolicies }
	compliance := func(h *Handler) http.HandlerFunc { return h.HandlePolicyCompliance }
	export := func(h *Handler) http.HandlerFunc { return h.HandleExportPolicies }
	importP := func(h *Handler) http.HandlerFunc { return h.HandleImportPolicies }
	evaluate := func(h *Handler) http.HandlerFunc { return h.HandleEvaluatePolicies }
	save := func(h *Handler) http.HandlerFunc { return h.HandleSavePolicies }

	h, pg := newDBHandler(t)
	pg.answer(sqlPolicies, blockingLabelPolicy())
	pg.answer(sqlListAlerts, fakedb.RowsOf(
		alertSpec{ID: "a1", Name: "Untagged", FractalID: "f1"}.listRow(),
		alertSpec{ID: "a2", Name: "Tagged", FractalID: "f1", Labels: []string{"attack.t1078"}}.listRow(),
	))

	expect(t, serve(h, list, hcall{as: viewer}), 200, `"field":"labels"`)
	rec := serve(h, compliance, hcall{as: viewer})
	expect(t, rec, 200, `"alert_id":"a1"`)
	if strings.Contains(rec.Body.String(), `"a2"`) {
		t.Errorf("a compliant alert is not listed: %s", rec.Body.String())
	}
	expect(t, serve(h, export, hcall{as: viewer}), 200, "Alert policies")
	expect(t, serve(h, evaluate, hcall{as: viewer, method: "POST", body: `{"name":"n","query_string":"a=1","labels":["attack.t1003"]}`}), 200, `"blocking":0`)

	expect(t, serve(h, importP, hcall{as: analyst, method: "POST", body: `{}`}), 403, "")
	doc, err := RenderPolicyDocument("baseline", []Policy{mustPolicy(t, Policy{Field: "labels", Operator: "any_matches", Value: "^x"})})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := jsonString(map[string]interface{}{"content": doc})
	expect(t, serve(h, importP, hcall{as: admin, method: "POST", body: body}), 200, "")
	if n := len(pg.args("INSERT INTO alert_policies")); n != 2 {
		t.Errorf("an import appends to the existing set: %d rules written", n)
	}
	expect(t, serve(h, importP, hcall{as: admin, method: "POST", body: `{"content":"::"}`}), 400, "")

	expect(t, serve(h, save, hcall{as: admin, method: "PUT", body: `{"policies":[{"field":"nope"}]}`}), 400, "")
	pg.answer("DELETE FROM alert_policies", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, save, hcall{as: admin, method: "PUT", body: `{"policies":[]}`}), 500, "")

	pg.answer(sqlPolicies, fakedb.PGResult{Err: errors.New("down")})
	for _, fn := range []func(h *Handler) http.HandlerFunc{list, compliance, export} {
		expect(t, serve(h, fn, hcall{as: viewer}), 500, "")
	}
	expect(t, serve(h, evaluate, hcall{as: viewer, method: "POST", body: `{}`}), 500, "")
}

func TestBundleHandlersOverStorage(t *testing.T) {
	export := func(h *Handler) http.HandlerFunc { return h.HandleExportBundle }
	importB := func(h *Handler) http.HandlerFunc { return h.HandleImportBundle }

	h, pg := newDBHandler(t)
	pg.answer(sqlListAlerts, fakedb.RowsOf(alertSpec{ID: "a1", Name: "Rule", FractalID: "f1"}.listRow()))
	rec := serve(h, export, hcall{as: analyst})
	expect(t, rec, 200, "")
	if rec.Header().Get("Content-Type") != "application/zip" || !strings.Contains(rec.Header().Get("Content-Disposition"), "bifract-alerts-") {
		t.Errorf("headers = %v", rec.Header())
	}
	files := readZip(t, rec.Body.Bytes())
	if !strings.Contains(files["manifest.json"], `"origin": "fractal:f1"`) {
		t.Errorf("manifest = %s", files["manifest.json"])
	}

	bundle := rec.Body.Bytes()
	pg.answer(sqlGateConfig, gateRow(true, 1, true))
	expect(t, serve(h, importB, hcall{as: analyst, method: "POST", body: string(bundle)}), 409, "cannot be applied directly")
	pg.answer(sqlGateConfig, fakedb.PGResult{})
	expect(t, serve(h, importB, hcall{as: analyst, method: "POST", body: ""}), 400, "empty")
	expect(t, serve(h, importB, hcall{as: analyst, method: "POST", body: "not a zip"}), 400, "")

	pg.answer("FROM alerts WHERE name = $1 AND feed_id IS NULL", fakedb.Row("a1", "f1", ""))
	rec = serve(h, importB, hcall{as: analyst, method: "POST", body: string(bytes.Clone(bundle))})
	expect(t, rec, 200, `"skipped"`)

	pg.answer(sqlListAlerts, fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, export, hcall{as: analyst}), 500, "Failed to export")
}

func TestAttackHandlersOverStorage(t *testing.T) {
	coverage := func(h *Handler) http.HandlerFunc { return h.HandleAttackCoverage }
	rules := func(h *Handler) http.HandlerFunc { return h.HandleAttackTechniqueRules }
	gap := func(h *Handler) http.HandlerFunc { return h.HandleAttackTechniqueGap }
	layer := func(h *Handler) http.HandlerFunc { return h.HandleAttackLayer }

	h, pg := newDBHandler(t)
	pg.answer("LEFT JOIN alert_feeds f ON f.id = a.feed_id", fakedb.RowsOf(
		[]driver.Value{"a1", "Valid accounts", "high", true, pgArray([]string{"attack.t1078"}), "", ""},
	))
	pg.answer("FROM feed_rule_catalog c", fakedb.RowsOf([]driver.Value{
		"feed-1", "Sigma", "rules/x.yml", "Dump", "high", "stable", "", "", pgArray([]string{"attack.t1003"}),
	}))

	// No scope must not mean every scope.
	for _, fn := range []func(h *Handler) http.HandlerFunc{coverage, rules, gap, layer} {
		expect(t, serve(h, fn, hcall{as: unscoped}), 400, "No fractal or prism selected")
	}
	if len(pg.Matching("LEFT JOIN alert_feeds")) != 0 {
		t.Fatal("an unscoped request read the rules")
	}

	expect(t, serve(h, coverage, hcall{as: viewer}), 200, `"T1078"`)
	expect(t, serve(h, coverage, hcall{as: viewer, query: "severity=nope"}), 400, "")
	expect(t, serve(h, rules, hcall{as: viewer, params: id("t1078")}), 200, "Valid accounts")
	expect(t, serve(h, rules, hcall{as: viewer, params: id("T9999")}), 404, "Unknown ATT")
	expect(t, serve(h, rules, hcall{as: viewer, params: id("T1078"), query: "severity=nope"}), 400, "")
	expect(t, serve(h, gap, hcall{as: viewer, params: id("T1003")}), 200, "rules/x.yml")
	expect(t, serve(h, gap, hcall{as: viewer, params: id("T9999")}), 404, "")
	rec := serve(h, layer, hcall{as: viewer, query: "scope_name=SOC%20Prod"})
	expect(t, rec, 200, `"techniqueID":"T1078"`)
	if !strings.Contains(rec.Header().Get("Content-Disposition"), "soc-prod-attack-coverage.json") {
		t.Errorf("disposition = %q", rec.Header().Get("Content-Disposition"))
	}
	expect(t, serve(h, layer, hcall{as: viewer, query: "severity=nope"}), 400, "")

	// The catalog is optional; the rules are not.
	pg.answer("FROM feed_rule_catalog c", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, coverage, hcall{as: viewer}), 200, "")
	expect(t, serve(h, gap, hcall{as: viewer, params: id("T1003")}), 200, "")
	pg.answer("LEFT JOIN alert_feeds f ON f.id = a.feed_id", fakedb.PGResult{Err: errors.New("down")})
	expect(t, serve(h, coverage, hcall{as: viewer}), 500, "Failed to load rules")
}

func jsonString(v interface{}) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}
