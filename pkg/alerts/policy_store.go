package alerts

import (
	"context"
	"fmt"

	"bifract/pkg/storage"
)

// ListPolicies returns a scope's rule set in author order.
//
// A prism carries its own rules rather than inheriting from its member fractals: an
// alert scoped to a prism is one rule spanning several fractals, so the fractals it
// happens to read cannot each claim authority over it.
func (m *Manager) ListPolicies(ctx context.Context, fractalID, prismID string) ([]Policy, error) {
	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return []Policy{}, nil
	}

	rows, err := m.pg.Query(ctx, fmt.Sprintf(`
		SELECT id::text, field, operator, value, message, severity, enabled, position
		  FROM alert_policies WHERE %s = $1 ORDER BY position, created_at`, column), scopeID)
	if err != nil {
		return nil, fmt.Errorf("list alert policies: %w", err)
	}
	defer rows.Close()

	policies := []Policy{}
	for rows.Next() {
		var p Policy
		if err := rows.Scan(&p.ID, &p.Field, &p.Operator, &p.Value, &p.Message, &p.Severity, &p.Enabled, &p.Position); err != nil {
			return nil, err
		}
		policies = append(policies, p)
	}
	return policies, rows.Err()
}

// ReplacePolicies rewrites a scope's whole rule set.
func (m *Manager) ReplacePolicies(ctx context.Context, fractalID, prismID string, policies []Policy, username string) ([]Policy, error) {
	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return nil, fmt.Errorf("policies are configured per fractal or per prism")
	}
	if err := ValidatePolicies(policies); err != nil {
		return nil, err
	}

	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM alert_policies WHERE %s = $1", column), scopeID); err != nil {
		return nil, fmt.Errorf("clearing alert policies: %w", err)
	}

	for i := range policies {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO alert_policies (%s, field, operator, value, message, severity, enabled, position, created_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, column),
			scopeID, policies[i].Field, policies[i].Operator, policies[i].Value, policies[i].Message,
			policies[i].Severity, policies[i].Enabled, i, storage.NullableUser(username),
		); err != nil {
			return nil, fmt.Errorf("saving rule %d: %w", i+1, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return m.ListPolicies(ctx, fractalID, prismID)
}

// policyScope resolves which column a rule set hangs off. Exactly one of the two is
// set, mirroring the constraint on the alerts a rule set judges.
func policyScope(fractalID, prismID string) (column, scopeID string) {
	if prismID != "" {
		return "prism_id", prismID
	}
	if fractalID != "" {
		return "fractal_id", fractalID
	}
	return "", ""
}

// policiesNeedTests reports whether any enabled rule reads a test outcome, so the
// expense of a test run is only paid when a rule actually asks for one.
func policiesNeedTests(policies []Policy) bool {
	for i := range policies {
		if !policies[i].Enabled {
			continue
		}
		if f, ok := LookupField(policies[i].Field); ok && f.RunsTests {
			return true
		}
	}
	return false
}

// enforcePolicies blocks a save that violates the scope's rule set.
//
// Feed-managed alerts are exempt throughout: their definition is owned upstream, so a
// policy an analyst cannot satisfy would only make the feed unusable.
func (m *Manager) enforcePolicies(ctx context.Context, fractalID, prismID string, subject PolicySubject) error {
	policies, err := m.ListPolicies(ctx, fractalID, prismID)
	if err != nil {
		return err
	}
	if len(policies) == 0 {
		return nil
	}

	// A rule reading test outcomes needs them to exist. The editor cannot be trusted
	// for this, so the run happens here even though the client already ran the same
	// tests while the author was typing.
	if policiesNeedTests(policies) && len(subject.Tests) > 0 && m.testRunner.Available() {
		run, err := m.testRunner.RunOnce(ctx, subject.Content.QueryString, subject.Tests)
		if err != nil {
			return fmt.Errorf("evaluating tests for policy: %w", err)
		}
		subject.TestsRun = true
		subject.TestsPassing = run.OK()
	}

	result := EvaluatePolicies(policies, subject)
	if result.OK() {
		return nil
	}
	return &PolicyBlockedError{Result: result}
}

// PolicyBlockedError reports a save refused by policy. The violations travel with it so
// the client can show each one against the field it concerns.
type PolicyBlockedError struct {
	Result PolicyResult
}

func (e *PolicyBlockedError) Error() string {
	if len(e.Result.Violations) == 1 {
		return "blocked by policy: " + e.Result.Violations[0].Message
	}
	return fmt.Sprintf("blocked by %d policy rules", e.Result.Blocking)
}

// Blocking returns only the violations that stopped the save.
func (e *PolicyBlockedError) Blocking() []Violation {
	out := make([]Violation, 0, e.Result.Blocking)
	for _, v := range e.Result.Violations {
		if v.Severity == PolicyBlock {
			out = append(out, v)
		}
	}
	return out
}

// SetTestRunner wires in the test runner, so policies that read a test outcome can be
// enforced on save rather than trusting whatever the editor reported.
func (m *Manager) SetTestRunner(runner *TestRunner) {
	m.testRunner = runner
}

// testsForPolicy resolves the test corpus a policy should judge: the one being saved
// when the request carries it, otherwise what the alert already has.
func (m *Manager) testsForPolicy(ctx context.Context, alertID string, proposed *[]AlertTest) ([]AlertTest, error) {
	if proposed != nil {
		return *proposed, nil
	}
	return m.ListTests(ctx, alertID)
}
