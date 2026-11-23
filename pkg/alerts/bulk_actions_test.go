package alerts

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func defWith(name string, webhooks, emails []string) RevisionContent {
	c := RevisionContent{Name: name, QueryString: "a=b", AlertType: "event", Severity: "medium",
		WebhookActionIDs: webhooks, EmailActionIDs: emails}
	c.canonicalize()
	return c
}

var (
	slack = ActionRef{Kind: ActionKindWebhook, ID: "w-slack"}
	pager = ActionRef{Kind: ActionKindWebhook, ID: "w-pager"}
	leads = ActionRef{Kind: ActionKindEmail, ID: "e-leads"}
)

func TestPlanAddsOnlyWhereMissing(t *testing.T) {
	defs := map[string]RevisionContent{
		"a1": defWith("one", nil, nil),
		"a2": defWith("two", []string{"w-slack"}, nil),
	}
	plan := planActionChanges(defs, []ActionChange{{AlertIDs: []string{"a1", "a2"}, Add: []ActionRef{slack}}}, nil)

	if plan.Selected != 2 || plan.Changed != 1 || plan.Unchanged != 1 {
		t.Fatalf("selected/changed/unchanged = %d/%d/%d, want 2/1/1", plan.Selected, plan.Changed, plan.Unchanged)
	}
	if len(plan.Alerts) != 1 || plan.Alerts[0].AlertID != "a1" || !reflect.DeepEqual(plan.Alerts[0].Added, []ActionRef{slack}) {
		t.Fatalf("alerts = %+v, want only a1 gaining slack", plan.Alerts)
	}
	if got := plan.after["a1"].WebhookActionIDs; !reflect.DeepEqual(got, []string{"w-slack"}) {
		t.Errorf("a1 webhooks after = %v", got)
	}
	want := []ActionTally{{ActionRef: slack, Added: 1, Skipped: 1}}
	if !reflect.DeepEqual(plan.Actions, want) {
		t.Errorf("tallies = %+v, want %+v", plan.Actions, want)
	}
}

// A remove leaves every other action in place, including ones the caller never saw:
// a bulk edit must not clobber wiring made since the list was loaded.
func TestPlanRemoveKeepsOtherActions(t *testing.T) {
	defs := map[string]RevisionContent{
		"a1": defWith("one", []string{"w-slack", "w-pager"}, []string{"e-leads"}),
		"a2": defWith("two", []string{"w-pager"}, nil),
	}
	plan := planActionChanges(defs, []ActionChange{{AlertIDs: []string{"a1", "a2"}, Remove: []ActionRef{slack, leads}}}, nil)

	if plan.Changed != 1 {
		t.Fatalf("changed = %d, want 1", plan.Changed)
	}
	after := plan.after["a1"]
	if !reflect.DeepEqual(after.WebhookActionIDs, []string{"w-pager"}) || len(after.EmailActionIDs) != 0 {
		t.Errorf("a1 after = webhooks %v emails %v", after.WebhookActionIDs, after.EmailActionIDs)
	}
	if _, touched := plan.after["a2"]; touched {
		t.Error("a2 had nothing to remove and must not be rewritten")
	}
}

// The undo of a bulk edit is not the inverse of the request: an alert that already had
// the added action must keep it. Undo is built from what actually changed.
func TestPlanUndoRevertsOnlyWhatChanged(t *testing.T) {
	defs := map[string]RevisionContent{
		"a1": defWith("one", nil, []string{"e-leads"}),
		"a2": defWith("two", []string{"w-slack"}, []string{"e-leads"}),
		"a3": defWith("three", nil, nil),
	}
	plan := planActionChanges(defs, []ActionChange{{
		AlertIDs: []string{"a1", "a2", "a3"}, Add: []ActionRef{slack}, Remove: []ActionRef{leads},
	}}, nil)

	want := []ActionChange{
		{AlertIDs: []string{"a1"}, Add: []ActionRef{leads}, Remove: []ActionRef{slack}},
		{AlertIDs: []string{"a2"}, Add: []ActionRef{leads}},
		{AlertIDs: []string{"a3"}, Remove: []ActionRef{slack}},
	}
	if !reflect.DeepEqual(plan.Undo, want) {
		t.Fatalf("undo = %+v\nwant %+v", plan.Undo, want)
	}

	// Applying the undo restores every alert exactly.
	for id, after := range plan.after {
		defs[id] = after
	}
	back := planActionChanges(defs, plan.Undo, nil)
	for id, after := range back.after {
		orig := map[string][]string{"a1": nil, "a2": {"w-slack"}, "a3": nil}[id]
		if !reflect.DeepEqual(normalizeList(after.WebhookActionIDs, true), normalizeList(orig, true)) {
			t.Errorf("%s webhooks after undo = %v, want %v", id, after.WebhookActionIDs, orig)
		}
	}
}

// Undo groups alerts that changed the same way, so undoing a 300-alert edit is one
// small request, not 300.
func TestPlanUndoGroupsIdenticalDeltas(t *testing.T) {
	defs := map[string]RevisionContent{}
	ids := []string{}
	for _, id := range []string{"a1", "a2", "a3", "a4"} {
		defs[id] = defWith(id, nil, nil)
		ids = append(ids, id)
	}
	plan := planActionChanges(defs, []ActionChange{{AlertIDs: ids, Add: []ActionRef{slack, pager}}}, nil)
	if len(plan.Undo) != 1 || len(plan.Undo[0].AlertIDs) != 4 {
		t.Fatalf("undo = %+v, want one group of four", plan.Undo)
	}
}

// Policies judge what the edit changes, not the alert's whole history: an old alert
// missing a description must still be able to gain an action, but no alert may be left
// failing a rule it passed before.
func TestPlanBlocksOnlyNewlyFailingRules(t *testing.T) {
	policies := []Policy{
		{ID: "p-actions", Field: "actions.count", Operator: "gte", Value: "1", Severity: PolicyBlock, Enabled: true, Message: "route it somewhere"},
		{ID: "p-desc", Field: "description", Operator: "not_empty", Severity: PolicyBlock, Enabled: true, Message: "describe it"},
		{ID: "p-warn", Field: "actions.count", Operator: "gte", Value: "2", Severity: PolicyWarn, Enabled: true, Message: "two is nicer"},
	}
	defs := map[string]RevisionContent{
		"keeps":  defWith("keeps", []string{"w-slack", "w-pager"}, nil),
		"loses":  defWith("loses", []string{"w-slack"}, nil),
		"absent": defWith("absent", nil, nil),
	}
	plan := planActionChanges(defs, []ActionChange{{AlertIDs: []string{"keeps", "loses", "absent"}, Remove: []ActionRef{slack}}}, policies)

	if plan.Blocked != 1 {
		t.Fatalf("blocked = %d, want 1 (%+v)", plan.Blocked, plan.Alerts)
	}
	for _, a := range plan.Alerts {
		switch a.AlertID {
		case "loses":
			if len(a.Blocking) != 1 || a.Blocking[0].PolicyID != "p-actions" {
				t.Errorf("loses blocking = %+v, want only p-actions", a.Blocking)
			}
		default:
			if len(a.Blocking) != 0 {
				t.Errorf("%s blocked by %+v", a.AlertID, a.Blocking)
			}
		}
	}
}

func TestValidateActionChanges(t *testing.T) {
	cases := []struct {
		name    string
		changes []ActionChange
		want    string
	}{
		{"nothing", nil, "no changes"},
		{"no alerts", []ActionChange{{Add: []ActionRef{slack}}}, "no alerts"},
		{"no actions", []ActionChange{{AlertIDs: []string{"a1"}}}, "add or remove at least one action"},
		{"bad kind", []ActionChange{{AlertIDs: []string{"a1"}, Add: []ActionRef{{Kind: "sms", ID: "x"}}}}, `unknown action kind "sms"`},
		{"no id", []ActionChange{{AlertIDs: []string{"a1"}, Add: []ActionRef{{Kind: ActionKindWebhook}}}}, "action id"},
		{"add and remove", []ActionChange{{AlertIDs: []string{"a1"}, Add: []ActionRef{slack}, Remove: []ActionRef{slack}}}, "both added and removed"},
		{"alert twice", []ActionChange{
			{AlertIDs: []string{"a1"}, Add: []ActionRef{slack}},
			{AlertIDs: []string{"a1"}, Remove: []ActionRef{pager}},
		}, "more than once"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateActionChanges(tc.changes)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}

	ok := []ActionChange{{AlertIDs: []string{"a1", "a2"}, Add: []ActionRef{slack}, Remove: []ActionRef{leads}}}
	if err := validateActionChanges(ok); err != nil {
		t.Fatalf("valid change refused: %v", err)
	}

	many := make([]string, MaxBulkActionAlerts+1)
	for i := range many {
		many[i] = fmt.Sprintf("a%d", i)
	}
	if err := validateActionChanges([]ActionChange{{AlertIDs: many, Add: []ActionRef{slack}}}); err == nil {
		t.Fatal("an oversized change was accepted")
	}
}

// A proposal binds approvals to its hash, so the hash must not depend on the order the
// client happened to list alerts or actions in.
func TestBulkActionChangeHashIgnoresOrder(t *testing.T) {
	a := BulkActionChange{Changes: []ActionChange{{AlertIDs: []string{"a2", "a1"}, Add: []ActionRef{slack, leads}}}}
	b := BulkActionChange{Changes: []ActionChange{{AlertIDs: []string{"a1", "a2"}, Add: []ActionRef{leads, slack}}}}
	ha, err := a.Hash()
	if err != nil {
		t.Fatal(err)
	}
	hb, _ := b.Hash()
	if ha != hb {
		t.Fatal("reordering alerts or actions changed the hash")
	}
	c := BulkActionChange{Changes: []ActionChange{{AlertIDs: []string{"a1", "a2"}, Remove: []ActionRef{leads, slack}}}}
	if hc, _ := c.Hash(); hc == ha {
		t.Fatal("an add and a remove hash the same")
	}
}

func TestDescribeActionChange(t *testing.T) {
	names := map[ActionRef]string{slack: "Slack #soc", leads: "SOC leads"}
	plan := &BulkActionPlan{Actions: []ActionTally{
		{ActionRef: slack, Added: 40, Skipped: 12},
		{ActionRef: leads, Removed: 1, Skipped: 51},
	}}
	got := describeActionChange(plan, names)
	want := "Add Slack #soc to 40 alerts, remove SOC leads from 1 alert"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
