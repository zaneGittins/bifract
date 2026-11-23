package alerts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/lib/pq"

	"bifract/internal/fakedb"
)

const (
	sqlBulkLock     = "AND feed_id IS NULL AND fractal_id = $2 ORDER BY id"
	sqlBulkLoad     = "COALESCE(max_event_lag_seconds, 0),"
	sqlBulkWebhooks = "FROM alert_webhook_actions WHERE alert_id = ANY($1)"
	sqlBulkUsable   = "FROM webhook_actions WHERE id = ANY($1) AND enabled = true"
	sqlBulkNames    = "SELECT id::text, name FROM webhook_actions WHERE id = ANY($1)"
	sqlBulkAddLinks = "INSERT INTO alert_webhook_actions (alert_id, webhook_id)"
	sqlBulkDelLinks = "DELETE FROM alert_webhook_actions l"
	sqlBulkTouch    = "UPDATE alerts SET updated_by = $2, updated_at = NOW() WHERE id = ANY($1)"
)

// arrayArg decodes a pq.Array argument back into its elements.
func arrayArg(v driver.Value) []string {
	var out pq.StringArray
	if err := out.Scan(v); err != nil {
		return nil
	}
	return out
}

// bulkStore answers a bulk edit's reads: alerts in f1 with their webhooks, and the
// webhooks usable in f1.
func bulkStore(pg *pgFake, alerts map[string][]string, usable map[string]string) {
	pg.handle(sqlBulkLock, func(args []driver.Value) fakedb.PGResult {
		var rows []driver.Value
		for _, id := range arrayArg(args[0]) {
			if _, ok := alerts[id]; ok {
				rows = append(rows, id)
			}
		}
		return rowsOf(1, rows...)
	})
	pg.handle(sqlBulkLoad, func(args []driver.Value) fakedb.PGResult {
		var rows [][]driver.Value
		for _, id := range arrayArg(args[0]) {
			if _, ok := alerts[id]; ok {
				rows = append(rows, []driver.Value{id, "Rule " + id, "", `a="1"`, "event", "medium",
					int64(0), int64(0), "", []byte("{}"), []byte("{}"), nil, nil, nil})
			}
		}
		return fakedb.RowsOf(rows...)
	})
	pg.handle(sqlBulkWebhooks, func(args []driver.Value) fakedb.PGResult {
		var rows [][]driver.Value
		for _, id := range arrayArg(args[0]) {
			for _, w := range alerts[id] {
				rows = append(rows, []driver.Value{id, w})
			}
		}
		return fakedb.RowsOf(rows...)
	})
	names := func(args []driver.Value) fakedb.PGResult {
		var rows [][]driver.Value
		for _, id := range arrayArg(args[0]) {
			if name, ok := usable[id]; ok {
				rows = append(rows, []driver.Value{id, name})
			}
		}
		return fakedb.RowsOf(rows...)
	}
	pg.handle(sqlBulkNames, names)
	pg.handle(sqlBulkUsable, names)
}

func addSlack(ids ...string) BulkActionChange {
	return BulkActionChange{Changes: []ActionChange{{AlertIDs: ids, Add: []ActionRef{slack}}}}
}

func TestApplyActionChangesWritesOnlyTheDelta(t *testing.T) {
	m, pg := newTestManager(t)
	bulkStore(pg, map[string][]string{"a1": nil, "a2": {"w-slack"}, "a3": {"w-pager"}}, map[string]string{"w-slack": "Slack"})

	plan, err := m.ApplyActionChanges(context.Background(), addSlack("a1", "a2", "a3"), "", "f1", "", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Changed != 2 || plan.Unchanged != 1 || plan.Summary != "Add Slack to 2 alerts" {
		t.Fatalf("plan = %+v", plan)
	}

	adds := pg.args(sqlBulkAddLinks)
	if len(adds) != 1 {
		t.Fatalf("want one insert for every alert, got %d", len(adds))
	}
	if got := arrayArg(adds[0][0]); strings.Join(got, ",") != "a1,a3" {
		t.Errorf("inserted for %v, want a1,a3", got)
	}
	if len(pg.Matching(sqlBulkDelLinks)) != 0 {
		t.Error("an add-only edit deletes nothing")
	}

	// One insert for every revision: a seed and the edit for each changed alert.
	revs := pg.args(sqlRevisionInsert)
	if len(revs) != 1 {
		t.Fatalf("revision inserts = %d, want one for the whole edit", len(revs))
	}
	if got := strings.Join(arrayArg(revs[0][0]), ","); got != "a1,a1,a3,a3" {
		t.Errorf("revision alerts = %s", got)
	}
	if got := strings.Join(arrayArg(revs[0][4]), ","); got != "initial,actions,initial,actions" {
		t.Errorf("revision summaries = %s", got)
	}
	if revs[0][5] != "alice" {
		t.Errorf("author = %v", revs[0][5])
	}
	if len(pg.Matching("DELETE FROM alert_revisions r")) != 1 {
		t.Error("history is pruned in one statement")
	}
	if !allBefore(pg, sqlRevisionInsert, sqlBulkAddLinks) {
		t.Error("revisions must be written before the links change, so a seed reads the old wiring")
	}
	allInTx(t, pg, sqlBulkAddLinks)
	if commits, _ := pg.txCounts(); commits != 1 || len(pg.Matching(sqlCacheRefresh)) != 1 {
		t.Error("the edit commits once and refreshes the engine once")
	}
	if len(plan.Undo) != 1 || strings.Join(plan.Undo[0].AlertIDs, ",") != "a1,a3" || plan.Undo[0].Remove[0] != slack {
		t.Errorf("undo = %+v", plan.Undo)
	}
}

func allBefore(pg *pgFake, first, then string) bool {
	last, firstThen := -1, -1
	for i, c := range pg.Calls() {
		if strings.Contains(c.Query, first) {
			last = i
		}
		if firstThen == -1 && strings.Contains(c.Query, then) {
			firstThen = i
		}
	}
	return last >= 0 && firstThen > last
}

func TestApplyActionChangesRefusals(t *testing.T) {
	ctx := context.Background()

	t.Run("an alert outside the scope", func(t *testing.T) {
		m, pg := newTestManager(t)
		bulkStore(pg, map[string][]string{"a1": nil}, map[string]string{"w-slack": "Slack"})
		_, err := m.ApplyActionChanges(ctx, addSlack("a1", "elsewhere"), "", "f1", "", "alice")
		if !errors.Is(err, ErrBulkActionAlerts) {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching(sqlBulkAddLinks)) != 0 {
			t.Error("nothing may be written")
		}
	})

	t.Run("an action that is disabled or elsewhere", func(t *testing.T) {
		m, pg := newTestManager(t)
		bulkStore(pg, map[string][]string{"a1": nil}, map[string]string{})
		pg.answer(sqlBulkNames, fakedb.RowsOf([]driver.Value{"w-slack", "Slack"}))
		pg.answer(sqlBulkUsable, fakedb.PGResult{})
		_, err := m.ApplyActionChanges(ctx, addSlack("a1"), "", "f1", "", "alice")
		if !errors.Is(err, ErrInvalidActionChange) || !strings.Contains(err.Error(), "webhook action Slack") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("a policy the edit would newly break", func(t *testing.T) {
		m, pg := newTestManager(t)
		bulkStore(pg, map[string][]string{"a1": {"w-slack"}, "a2": {"w-slack", "w-pager"}}, map[string]string{"w-slack": "Slack"})
		pg.answer(sqlPolicies, fakedb.RowsOf([]driver.Value{"p1", "actions.count", "gte", "1", "route it", PolicyBlock, true, int64(0)}))
		_, err := m.ApplyActionChanges(ctx, BulkActionChange{Changes: []ActionChange{{AlertIDs: []string{"a1", "a2"}, Remove: []ActionRef{slack}}}}, "", "f1", "", "alice")
		var blocked *ActionChangeBlockedError
		if !errors.As(err, &blocked) || blocked.Plan.Blocked != 1 {
			t.Fatalf("err = %v", err)
		}
		if commits, _ := pg.txCounts(); commits != 0 || len(pg.Matching(sqlBulkDelLinks)) != 0 {
			t.Error("a blocked edit writes nothing, not even for the alerts it would not break")
		}
	})
}

// With the review gate on, the whole edit becomes one proposal and nothing is applied.
func TestApplyActionChangesProposesWhenGated(t *testing.T) {
	m, pg := newTestManager(t)
	bulkStore(pg, map[string][]string{"a1": nil, "a2": nil}, map[string]string{"w-slack": "Slack"})
	pg.answer(sqlGateConfig, gateRow(true, 1, false))
	store := newCRStore(t, pg)
	var inserted []driver.Value
	pg.handle("INSERT INTO alert_change_requests", func(args []driver.Value) fakedb.PGResult {
		inserted = args
		var change BulkActionChange
		if err := json.Unmarshal(args[5].([]byte), &change); err != nil {
			t.Fatal(err)
		}
		store.add(crSpec{ID: "cr1", Kind: ChangeActions, Status: ChangeOpen, Actions: &change, Author: "alice"}, "f1", "")
		return fakedb.Row("cr1")
	})

	plan, err := m.ApplyActionChanges(context.Background(), addSlack("a2", "a1"), "route to slack", "f1", "", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Gated || plan.Proposal == nil || plan.Proposal.Kind != ChangeActions || plan.Proposal.Actions == nil {
		t.Fatalf("plan = %+v", plan)
	}
	if inserted[2] != ChangeActions || inserted[1] != nil || inserted[3] != "Add Slack to 2 alerts" || inserted[4] != "route to slack" {
		t.Errorf("proposal = %v", inserted)
	}
	if len(pg.Matching(sqlBulkAddLinks)) != 0 || len(pg.Matching(sqlRevisionInsert)) != 0 {
		t.Error("a gated edit must not apply anything")
	}

	// Merging it applies the edit, attributed to the author.
	store.approve("cr1", "bob")
	if _, err := m.MergeChangeRequest(context.Background(), "cr1", "bob"); err != nil {
		t.Fatal(err)
	}
	if adds := pg.args(sqlBulkAddLinks); len(adds) != 1 || strings.Join(arrayArg(adds[0][0]), ",") != "a1,a2" {
		t.Fatalf("merge adds = %v", adds)
	}
	if touch := pg.args(sqlBulkTouch); len(touch) != 1 || touch[0][1] != "alice" {
		t.Errorf("touch = %v, want the author credited", touch)
	}
}

// An approved proposal still merges after one of its alerts is deleted: there is
// nothing left to change on that one.
func TestMergeActionsSkipsDeletedAlerts(t *testing.T) {
	m, pg := newTestManager(t)
	bulkStore(pg, map[string][]string{"a1": nil}, map[string]string{"w-slack": "Slack"})
	pg.answer(sqlGateConfig, gateRow(true, 1, false))
	store := newCRStore(t, pg)
	change := addSlack("a1", "gone")
	store.add(crSpec{ID: "cr1", Kind: ChangeActions, Status: ChangeOpen, Actions: &change, Author: "alice"}, "f1", "")
	store.approve("cr1", "bob")

	if _, err := m.MergeChangeRequest(context.Background(), "cr1", "bob"); err != nil {
		t.Fatal(err)
	}
	if adds := pg.args(sqlBulkAddLinks); len(adds) != 1 || strings.Join(arrayArg(adds[0][0]), ",") != "a1" {
		t.Fatalf("adds = %v", adds)
	}
}

func TestActionsProposalCannotBeRevised(t *testing.T) {
	m, pg := newTestManager(t)
	store := newCRStore(t, pg)
	change := addSlack("a1")
	store.add(crSpec{ID: "cr1", Kind: ChangeActions, Status: ChangeOpen, Actions: &change, Author: "alice"}, "f1", "")
	_, err := m.SubmitChangeRequest(context.Background(), "cr1", "f1", "", ChangeRequestInput{Kind: ChangeActions, Actions: &change}, "alice")
	if err == nil || !strings.Contains(err.Error(), "cannot be revised") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateActionsProposal(t *testing.T) {
	change := addSlack("a1")
	cases := []struct {
		in   ChangeRequestInput
		want string
	}{
		{ChangeRequestInput{Kind: ChangeActions}, "needs actions"},
		{ChangeRequestInput{Kind: ChangeActions, AlertID: "a1", Actions: &change}, "names its alerts in actions"},
		{ChangeRequestInput{Kind: ChangeActions, Actions: &BulkActionChange{}}, "no changes"},
		{ChangeRequestInput{Kind: ChangeUpdate, AlertID: "a1", Content: eventContent("x"), Actions: &change}, "only an actions proposal"},
	}
	for _, tc := range cases {
		if err := tc.in.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: err = %v, want %q", tc.in, err, tc.want)
		}
	}
	ok := ChangeRequestInput{Kind: ChangeActions, Actions: &change}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBulkActionHandlers(t *testing.T) {
	preview := func(h *Handler) http.HandlerFunc { return h.HandlePreviewBulkActions }
	apply := func(h *Handler) http.HandlerFunc { return h.HandleApplyBulkActions }
	h, pg := newDBHandler(t)
	bulkStore(pg, map[string][]string{"a1": nil, "a2": {"w-slack"}}, map[string]string{"w-slack": "Slack"})
	body := `{"changes":[{"alert_ids":["a1","a2"],"add":[{"kind":"webhook","id":"w-slack"}]}]}`

	expect(t, serve(h, preview, hcall{as: viewer, method: "POST", body: body}), 403, "")
	expect(t, serve(h, preview, hcall{as: analyst, method: "POST", body: `{`}), 400, "invalid request body")
	expect(t, serve(h, preview, hcall{as: analyst, method: "POST", body: `{"changes":[]}`}), 400, "no changes")
	expect(t, serve(h, preview, hcall{as: analyst, method: "POST",
		body: `{"changes":[{"alert_ids":["a1","nope"],"add":[{"kind":"webhook","id":"w-slack"}]}]}`}), 404, "not found")

	rec := serve(h, preview, hcall{as: analyst, method: "POST", body: body})
	expect(t, rec, 200, `"changed":1`)
	if len(pg.Matching(sqlBulkAddLinks)) != 0 {
		t.Fatal("a preview must not write")
	}
	expect(t, serve(h, apply, hcall{as: analyst, method: "POST", body: body}), 200, `"undo":[{"alert_ids":["a1"],"remove":[{"kind":"webhook","id":"w-slack"}]}]`)

	pg.answer(sqlPolicies, fakedb.RowsOf([]driver.Value{"p1", "actions.count", "gte", "1", "route it", PolicyBlock, true, int64(0)}))
	expect(t, serve(h, apply, hcall{as: analyst, method: "POST",
		body: `{"changes":[{"alert_ids":["a2"],"remove":[{"kind":"webhook","id":"w-slack"}]}]}`}), 422, `"blocked":1`)
}

// With history present, the edit continues the numbering from each alert's head, and an
// alert whose definition would not change records nothing.
func TestWriteRevisionsContinuesHistory(t *testing.T) {
	same := RevisionContent{Name: "same", QueryString: "a=1", AlertType: "event", Severity: "low"}
	old := RevisionContent{Name: "old", QueryString: "a=1", AlertType: "event", Severity: "low"}
	edited := old
	edited.WebhookActionIDs = []string{"w1"}
	same.canonicalize()
	old.canonicalize()
	rawSame, _ := json.Marshal(same)
	rawOld, _ := json.Marshal(old)

	tx, script := newFakeTx(t, func(q string, args []driver.Value) fakedb.PGResult {
		if strings.Contains(q, "SELECT DISTINCT ON (alert_id)") {
			return fakedb.RowsOf(
				[]driver.Value{"a1", int64(7), mustHash(t, old), rawOld},
				[]driver.Value{"a2", int64(3), mustHash(t, same), rawSame},
			)
		}
		return fakedb.Affected(1)
	})
	edits := map[string]RevisionContent{"a1": edited, "a2": same}
	if err := writeRevisions(context.Background(), tx, edits, nil, "u", "u", 5); err != nil {
		t.Fatal(err)
	}
	ins := script.Matching("INSERT INTO alert_revisions")
	if len(ins) != 1 {
		t.Fatalf("inserts = %d", len(ins))
	}
	if got := strings.Join(arrayArg(ins[0].Args[0]), ","); got != "a1" {
		t.Errorf("revisions for %s, want only a1", got)
	}
	if got := strings.Join(arrayArg(ins[0].Args[1]), ","); got != "8" {
		t.Errorf("revision numbers %s, want 8", got)
	}
	prune := script.Matching("DELETE FROM alert_revisions r")
	if len(prune) != 1 || strings.Join(arrayArg(prune[0].Args[1]), ",") != "3" {
		t.Errorf("prune = %+v, want a floor of 8-5", prune)
	}
}
