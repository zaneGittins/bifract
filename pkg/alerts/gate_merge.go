package alerts

import (
	"context"
	"fmt"

	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// MergeReadiness is everything a reviewer needs to judge a proposal, and everything
// merge re-verifies before applying it.
type MergeReadiness struct {
	Blocker  string         `json:"blocker,omitempty"`
	Policy   *PolicyResult  `json:"policy,omitempty"`
	Tests    *TestRunResult `json:"tests,omitempty"`
	HeadHash string         `json:"head_hash,omitempty"`
	// Approvals counts only those describing the current content.
	Approvals    int `json:"approvals"`
	MinApprovals int `json:"min_approvals"`
}

// OK reports whether the proposal may merge.
func (r *MergeReadiness) OK() bool { return r.Blocker == "" }

// EvaluateChangeRequest measures a proposal against everything that gates it: the
// review policy, the fractal's alert policies, and its own tests.
//
// The same call backs the review screen and the merge itself, so a reviewer sees
// exactly what merge will check rather than a summary recorded when it was opened.
func (m *Manager) EvaluateChangeRequest(ctx context.Context, cr *ChangeRequest, runTests bool) (*MergeReadiness, error) {
	fractalID, prismID, err := m.changeRequestScope(ctx, cr.ID)
	if err != nil {
		return nil, err
	}
	cfg, err := m.GateConfigFor(ctx, fractalID, prismID)
	if err != nil {
		return nil, err
	}

	headHash, err := m.headHashFor(ctx, cr.AlertID)
	if err != nil {
		return nil, err
	}

	readiness := &MergeReadiness{
		HeadHash:     headHash,
		Approvals:    len(cr.Approvals()),
		MinApprovals: cfg.MinApprovals,
		Blocker:      cr.MergeBlocker(cfg, headHash),
	}

	// A delete proposes no definition, so there is nothing to check it against.
	if cr.Kind == ChangeDelete || cr.Content == nil {
		return readiness, nil
	}

	subject := NewPolicySubject(*cr.Content, cr.Tests)

	policies, err := m.ListPolicies(ctx, fractalID, prismID)
	if err != nil {
		return nil, err
	}

	// Running the corpus means DDL, an insert and a query per test. Merge always pays
	// it, but a reviewer merely opening the proposal only should when a rule actually
	// reads the outcome, or when they asked for it.
	if runTests && len(cr.Tests) > 0 && m.testRunner.Available() && policiesNeedTests(policies) {
		run, err := m.testRunner.RunOnce(ctx, cr.Content.QueryString, cr.Tests)
		if err != nil {
			readiness.Tests = &TestRunResult{Error: err.Error()}
		} else {
			readiness.Tests = run
			subject.TestsRun = true
			subject.TestsPassing = run.OK()
		}
	}

	if len(policies) > 0 {
		result := EvaluatePolicies(policies, subject)
		readiness.Policy = &result
		if readiness.Blocker == "" && !result.OK() {
			readiness.Blocker = fmt.Sprintf("%d policy check(s) must be fixed", result.Blocking)
		}
	}

	return readiness, nil
}

// MergeChangeRequest applies an approved proposal.
//
// Everything is re-verified here rather than trusted from when the proposal was
// reviewed: approvals must still describe the current content, the alert must not have
// moved underneath, and the policy checks and tests are re-run against the exact
// definition being written. A gate that trusts a recorded verdict is decorative.
func (m *Manager) MergeChangeRequest(ctx context.Context, crID, username string) (*ChangeRequest, error) {
	cr, err := m.GetChangeRequest(ctx, crID)
	if err != nil {
		return nil, err
	}

	readiness, err := m.EvaluateChangeRequest(ctx, cr, true)
	if err != nil {
		return nil, err
	}
	if !readiness.OK() {
		return nil, &MergeBlockedError{Reason: readiness.Blocker, Readiness: readiness}
	}

	fractalID, prismID, err := m.changeRequestScope(ctx, crID)
	if err != nil {
		return nil, err
	}

	// Claim the proposal before applying it. Two reviewers pressing Merge together
	// would otherwise both pass the readiness check and both apply, which for a create
	// proposal means two alerts from one approval.
	claimed, err := m.claimForMerge(ctx, crID, username)
	if err != nil {
		return nil, err
	}
	if !claimed {
		return nil, &MergeBlockedError{Reason: "this proposal is already being merged", Readiness: readiness}
	}

	// The gate is bypassed for the write itself: this call is the approval.
	ctx = withGateBypass(ctx)

	// The proposal is already marked merged, so a failure here leaves a claimed row
	// rather than one that can be applied twice. Releasing it on error would reopen
	// exactly the race the claim closes.
	applyErr := func() error {
		switch cr.Kind {
		case ChangeCreate:
			// A reviewed and approved detection is meant to run. Leaving it disabled
			// reads as success to the reviewer while nothing fires.
			req := AlertCreateRequest(cr.Content.ToUpdateRequest(true))
			if cr.Tests != nil {
				tests := cr.Tests
				req.Tests = &tests
			}
			if _, err := m.CreateAlert(ctx, req, username, fractalID, prismID); err != nil {
				return fmt.Errorf("applying proposal: %w", err)
			}

		case ChangeUpdate:
			enabled, err := m.alertEnabled(ctx, cr.AlertID)
			if err != nil {
				return err
			}
			req := cr.Content.ToUpdateRequest(enabled)
			if cr.Tests != nil {
				tests := cr.Tests
				req.Tests = &tests
			}
			if _, err := m.UpdateAlert(ctx, cr.AlertID, req, username); err != nil {
				return fmt.Errorf("applying proposal: %w", err)
			}

		case ChangeDelete:
			if err := m.DeleteAlert(ctx, cr.AlertID); err != nil {
				return fmt.Errorf("applying proposal: %w", err)
			}
		}
		return nil
	}()
	if applyErr != nil {
		return nil, applyErr
	}

	// A delete cascades its proposals away, so there may be nothing left to return.
	merged, err := m.GetChangeRequest(ctx, crID)
	if err == ErrChangeRequestNotFound {
		cr.Status = ChangeMerged
		return cr, nil
	}
	return merged, err
}

func (m *Manager) alertEnabled(ctx context.Context, alertID string) (bool, error) {
	var enabled bool
	if err := m.pg.QueryRow(ctx, "SELECT enabled FROM alerts WHERE id = $1", alertID).Scan(&enabled); err != nil {
		return false, fmt.Errorf("load alert state: %w", err)
	}
	return enabled, nil
}

// MergeBlockedError reports a merge refused by the gate.
type MergeBlockedError struct {
	Reason    string
	Readiness *MergeReadiness
}

func (e *MergeBlockedError) Error() string { return "cannot merge: " + e.Reason }

// GateRequiredError reports a direct write refused because the scope reviews changes.
type GateRequiredError struct {
	Kind string
}

func (e *GateRequiredError) Error() string {
	return "this scope reviews alert changes: open a proposal instead"
}

// gateBypassKey marks a write that is itself the outcome of an approved proposal.
type gateBypassKey struct{}

func withGateBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, gateBypassKey{}, true)
}

func gateBypassed(ctx context.Context) bool {
	bypass, _ := ctx.Value(gateBypassKey{}).(bool)
	return bypass
}

// requireGate refuses a direct write when the scope reviews changes.
//
// Feed-managed alerts are exempt, as they are from tests and policies: upstream owns
// the definition, and a gate an analyst cannot satisfy would only break the feed.
func (m *Manager) requireGate(ctx context.Context, fractalID, prismID, kind string, isFeedAlert bool) error {
	if gateBypassed(ctx) || isFeedAlert {
		return nil
	}
	cfg, err := m.GateConfigFor(ctx, fractalID, prismID)
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return nil
	}
	return &GateRequiredError{Kind: kind}
}

// ReviewerRole is the role a principal needs to approve or reject. Analyst rather than
// admin: requiring an admin for every detection edit rebuilds the bottleneck the gate
// is meant to replace.
const ReviewerRole = rbac.RoleAnalyst

// claimForMerge marks a proposal merged, and reports whether this caller is the one that
// did it. The status precondition makes the claim exclusive: a second concurrent merge
// updates no rows and is refused.
func (m *Manager) claimForMerge(ctx context.Context, crID, username string) (bool, error) {
	result, err := m.pg.Exec(ctx, `
		UPDATE alert_change_requests
		   SET status = 'merged', merged_at = NOW(), merged_by = $2, updated_at = NOW()
		 WHERE id = $1 AND status IN ('open', 'changes_requested')`,
		crID, storage.NullableUser(username))
	if err != nil {
		return false, fmt.Errorf("claiming proposal: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claiming proposal: %w", err)
	}
	return rows == 1, nil
}
