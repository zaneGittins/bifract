package alerts

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"bifract/pkg/storage"
)

// ActionChangeBlockedError reports a bulk edit refused because it would newly break a
// policy rule on some alerts. The plan names them.
type ActionChangeBlockedError struct {
	Plan *BulkActionPlan
}

func (e *ActionChangeBlockedError) Error() string {
	return fmt.Sprintf("blocked by policy on %s", pluralAlerts(e.Plan.Blocked))
}

// PreviewActionChanges works out a bulk edit without writing anything.
func (m *Manager) PreviewActionChanges(ctx context.Context, change BulkActionChange, fractalID, prismID string) (*BulkActionPlan, error) {
	if err := validateActionChanges(change.Changes); err != nil {
		return nil, err
	}
	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	plan, err := m.planActionChangesTx(ctx, tx, change, fractalID, prismID, false, true)
	if err != nil {
		return nil, err
	}
	cfg, err := m.GateConfigFor(ctx, fractalID, prismID)
	if err != nil {
		return nil, err
	}
	plan.Gated = cfg.Enabled
	return plan, nil
}

// planActionChangesReadOnly plans an approved proposal against the current alerts,
// skipping any deleted since it was opened.
func (m *Manager) planActionChangesReadOnly(ctx context.Context, change BulkActionChange, fractalID, prismID string) (*BulkActionPlan, error) {
	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("start transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	return m.planActionChangesTx(ctx, tx, change, fractalID, prismID, false, false)
}

// ApplyActionChanges adds and removes actions across many alerts in one transaction.
//
// When the scope reviews changes, nothing is written: the edit becomes a single
// proposal covering every alert, and the plan carries it.
func (m *Manager) ApplyActionChanges(ctx context.Context, change BulkActionChange, summary, fractalID, prismID, username string) (*BulkActionPlan, error) {
	if err := validateActionChanges(change.Changes); err != nil {
		return nil, err
	}

	if !gateBypassed(ctx) {
		cfg, err := m.GateConfigFor(ctx, fractalID, prismID)
		if err != nil {
			return nil, err
		}
		if cfg.Enabled {
			return m.proposeActionChanges(ctx, change, summary, fractalID, prismID, username)
		}
	}
	return m.applyActionChanges(ctx, change, fractalID, prismID, username, true)
}

func (m *Manager) proposeActionChanges(ctx context.Context, change BulkActionChange, summary, fractalID, prismID, username string) (*BulkActionPlan, error) {
	plan, err := m.PreviewActionChanges(ctx, change, fractalID, prismID)
	if err != nil {
		return nil, err
	}
	if plan.Blocked > 0 {
		return nil, &ActionChangeBlockedError{Plan: plan}
	}
	if plan.Changed == 0 {
		return plan, nil
	}
	cr, err := m.SubmitChangeRequest(ctx, "", fractalID, prismID, ChangeRequestInput{
		Kind:    ChangeActions,
		Title:   plan.Summary,
		Summary: summary,
		Actions: &change,
	}, username)
	if err != nil {
		return nil, err
	}
	plan.Gated = true
	plan.Proposal = cr
	return plan, nil
}

// applyActionChanges writes a bulk edit. strict refuses alerts that are missing; a
// merge passes false, since an alert deleted while the proposal sat in review has
// nothing left to change.
func (m *Manager) applyActionChanges(ctx context.Context, change BulkActionChange, fractalID, prismID, username string, strict bool) (*BulkActionPlan, error) {
	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	plan, err := m.planActionChangesTx(ctx, tx, change, fractalID, prismID, true, strict)
	if err != nil {
		return nil, err
	}
	if plan.Blocked > 0 {
		return nil, &ActionChangeBlockedError{Plan: plan}
	}
	if plan.Changed == 0 {
		return plan, nil
	}

	// Revisions first, seeded from the pre-edit definitions the plan was made from.
	if err := writeRevisions(ctx, tx, plan.after, plan.before, username, username, revisionRetention()); err != nil {
		return nil, err
	}
	changed := make([]string, 0, len(plan.Alerts))
	for _, a := range plan.Alerts {
		changed = append(changed, a.AlertID)
	}
	if err := writeActionLinks(ctx, tx, plan.Alerts); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE alerts SET updated_by = $2, updated_at = NOW() WHERE id = ANY($1)`,
		pq.Array(changed), storage.NullableUser(username),
	); err != nil {
		return nil, fmt.Errorf("touch alerts: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	if err := m.engine.RefreshAlerts(ctx); err != nil {
		fmt.Printf("Warning: failed to refresh alert cache: %v\n", err)
	}
	return plan, nil
}

// planActionChangesTx loads the named alerts and plans the edit against them. lock
// holds their rows until the transaction ends, so the plan is what gets written.
func (m *Manager) planActionChangesTx(ctx context.Context, tx storage.Tx, change BulkActionChange, fractalID, prismID string, lock, strict bool) (*BulkActionPlan, error) {
	ids := change.AlertIDs()
	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return nil, invalidChange("no fractal or prism scope")
	}

	query := fmt.Sprintf(`SELECT id::text FROM alerts WHERE id = ANY($1) AND feed_id IS NULL AND %s = $2 ORDER BY id`, column)
	if lock {
		query += " FOR UPDATE"
	}
	rows, err := tx.Query(ctx, query, pq.Array(ids), scopeID)
	if err != nil {
		return nil, fmt.Errorf("load alerts: %w", err)
	}
	found := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		found[id] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if strict && len(found) != len(ids) {
		return nil, ErrBulkActionAlerts
	}

	if err := m.requireActionsAvailable(ctx, change.addedRefs(), fractalID, prismID); err != nil {
		return nil, err
	}

	present := make([]string, 0, len(found))
	for id := range found {
		present = append(present, id)
	}
	contents, err := loadRevisionContentsTx(ctx, tx, present)
	if err != nil {
		return nil, err
	}

	policies, err := m.ListPolicies(ctx, fractalID, prismID)
	if err != nil {
		return nil, err
	}
	plan := planActionChanges(contents, change.Changes, policies)

	names, err := m.actionRefNames(ctx, plan.Actions)
	if err != nil {
		return nil, err
	}
	for i := range plan.Actions {
		plan.Actions[i].Name = names[plan.Actions[i].ActionRef]
	}
	plan.Summary = describeActionChange(plan, names)
	return plan, nil
}

// requireActionsAvailable refuses adding an action that is missing, disabled or in
// another scope, naming each one. Removing needs no such check: removing a link that
// is not there changes nothing.
func (m *Manager) requireActionsAvailable(ctx context.Context, refs []ActionRef, fractalID, prismID string) error {
	if len(refs) == 0 {
		return nil
	}
	var wanted RevisionContent
	for _, r := range refs {
		k, _ := lookupActionKind(r.Kind)
		ids := k.get(&wanted)
		*ids = append(*ids, r.ID)
	}
	missing, err := m.missingActionRefs(ctx, &wanted, fractalID, prismID)
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		return nil
	}
	parts := make([]string, len(missing))
	for i, ref := range missing {
		label := ref.Name
		if label == "" {
			label = ref.ID
		}
		parts[i] = fmt.Sprintf("%s action %s", ref.Kind, label)
	}
	return invalidChange("cannot add actions that are missing, disabled or in another scope: %s", strings.Join(parts, ", "))
}

func (m *Manager) actionRefNames(ctx context.Context, tallies []ActionTally) (map[ActionRef]string, error) {
	byKind := map[string][]string{}
	for _, t := range tallies {
		byKind[t.Kind] = append(byKind[t.Kind], t.ID)
	}
	out := map[ActionRef]string{}
	for _, k := range actionKinds {
		if len(byKind[k.kind]) == 0 {
			continue
		}
		names, err := m.actionNames(ctx, k.table, byKind[k.kind])
		if err != nil {
			return nil, err
		}
		for id, name := range names {
			out[ActionRef{Kind: k.kind, ID: id}] = name
		}
	}
	return out, nil
}

// writeActionLinks applies the deltas in two statements per action kind, however many
// alerts there are.
func writeActionLinks(ctx context.Context, tx storage.Tx, deltas []AlertActionDelta) error {
	type pairs struct{ alerts, actions []string }
	adds := map[string]*pairs{}
	removes := map[string]*pairs{}
	collect := func(into map[string]*pairs, alertID string, refs []ActionRef) {
		for _, r := range refs {
			p, ok := into[r.Kind]
			if !ok {
				p = &pairs{}
				into[r.Kind] = p
			}
			p.alerts = append(p.alerts, alertID)
			p.actions = append(p.actions, r.ID)
		}
	}
	for _, d := range deltas {
		collect(adds, d.AlertID, d.Added)
		collect(removes, d.AlertID, d.Removed)
	}

	for _, k := range actionKinds {
		if p := removes[k.kind]; p != nil {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`
				DELETE FROM %[1]s l
				 USING unnest($1::uuid[], $2::uuid[]) AS d(alert_id, action_id)
				 WHERE l.alert_id = d.alert_id AND l.%[2]s = d.action_id`, k.link, k.column),
				pq.Array(p.alerts), pq.Array(p.actions)); err != nil {
				return fmt.Errorf("remove %s actions: %w", k.kind, err)
			}
		}
		if p := adds[k.kind]; p != nil {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (alert_id, %s)
				SELECT * FROM unnest($1::uuid[], $2::uuid[])
				ON CONFLICT DO NOTHING`, k.link, k.column),
				pq.Array(p.alerts), pq.Array(p.actions)); err != nil {
				return fmt.Errorf("add %s actions: %w", k.kind, err)
			}
		}
	}
	return nil
}
