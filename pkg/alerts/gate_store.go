package alerts

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"bifract/pkg/storage"
)

// GateConfigFor returns a scope's review policy, or the default when it has none.
func (m *Manager) GateConfigFor(ctx context.Context, fractalID, prismID string) (GateConfig, error) {
	cfg := DefaultGateConfig()
	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return cfg, nil
	}

	err := m.pg.QueryRow(ctx, fmt.Sprintf(
		"SELECT enabled, min_approvals, allow_self_approval FROM alert_gate_config WHERE %s = $1", column), scopeID,
	).Scan(&cfg.Enabled, &cfg.MinApprovals, &cfg.AllowSelfApproval)
	if err == sql.ErrNoRows {
		return DefaultGateConfig(), nil
	}
	if err != nil {
		return cfg, fmt.Errorf("load gate config: %w", err)
	}
	return cfg, nil
}

// SaveGateConfig stores a scope's review policy.
func (m *Manager) SaveGateConfig(ctx context.Context, fractalID, prismID string, cfg GateConfig, username string) (GateConfig, error) {
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return cfg, fmt.Errorf("the review gate is configured per fractal or per prism")
	}

	if _, err := m.pg.Exec(ctx, fmt.Sprintf(`
		INSERT INTO alert_gate_config (%s, enabled, min_approvals, allow_self_approval, updated_by, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (%s) WHERE %s IS NOT NULL
		DO UPDATE SET enabled = EXCLUDED.enabled, min_approvals = EXCLUDED.min_approvals,
		              allow_self_approval = EXCLUDED.allow_self_approval,
		              updated_by = EXCLUDED.updated_by, updated_at = NOW()`, column, column, column),
		scopeID, cfg.Enabled, cfg.MinApprovals, cfg.AllowSelfApproval, storage.NullableUser(username),
	); err != nil {
		return cfg, fmt.Errorf("save gate config: %w", err)
	}
	return cfg, nil
}

// headHashFor returns the hash of an alert's current definition, for the staleness
// check. An alert with no revisions yet has no head to compare against.
func (m *Manager) headHashFor(ctx context.Context, alertID string) (string, error) {
	if alertID == "" {
		return "", nil
	}
	var hash string
	err := m.pg.QueryRow(ctx,
		"SELECT content_hash FROM alert_revisions WHERE alert_id = $1 ORDER BY revision DESC LIMIT 1", alertID,
	).Scan(&hash)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load head revision: %w", err)
	}
	return hash, nil
}

// SubmitChangeRequest opens a proposal, or resubmits one the author has revised.
//
// crID is empty for a new proposal. Resubmitting keeps the same row, so a rejection
// never destroys the author's work: they revise it and it becomes open again.
func (m *Manager) SubmitChangeRequest(ctx context.Context, crID, fractalID, prismID string, in ChangeRequestInput, username string) (*ChangeRequest, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	// The alert must live in the scope the proposal is being opened in. Without this
	// an analyst could propose against another fractal's alert, have their own
	// colleagues approve it, and merge into a fractal they cannot otherwise touch.
	if in.AlertID != "" {
		if err := m.assertAlertInScope(ctx, in.AlertID, fractalID, prismID); err != nil {
			return nil, err
		}
	}

	contentHash, baseHash, contentJSON, testsJSON, err := m.prepareProposal(ctx, in)
	if err != nil {
		return nil, err
	}

	if crID != "" {
		existing, err := m.GetChangeRequest(ctx, crID)
		if err != nil {
			return nil, err
		}
		// A revision keeps the proposal's own scope: adopting the caller's would let a
		// proposal be walked from one fractal into another.
		fractalID, prismID, err = m.changeRequestScope(ctx, crID)
		if err != nil {
			return nil, err
		}
		if !existing.Open() {
			return nil, fmt.Errorf("this proposal is %s and can no longer be edited", existing.Status)
		}
		if existing.Author != username {
			return nil, fmt.Errorf("only the author can revise a proposal")
		}

		// kind and alert_id belong to the proposal, not to the revision. Taking them
		// from the request would let a PUT recompute base_hash against a different
		// alert, or none, while the row kept its original kind: the stale-proposal
		// guard would be erased without anything looking wrong.
		in.Kind = existing.Kind
		in.AlertID = existing.AlertID
		if err := in.Validate(); err != nil {
			return nil, err
		}

		contentHash, baseHash, contentJSON, testsJSON, err = m.prepareProposal(ctx, in)
		if err != nil {
			return nil, err
		}

		if _, err := m.pg.Exec(ctx, `
			UPDATE alert_change_requests
			   SET title = $2, summary = $3, content = $4, tests = $5,
			       content_hash = $6, base_hash = $7, status = 'open', updated_at = NOW()
			 WHERE id = $1`,
			crID, in.Title, in.Summary, contentJSON, testsJSON, contentHash, baseHash,
		); err != nil {
			return nil, fmt.Errorf("update proposal: %w", err)
		}
		return m.GetChangeRequest(ctx, crID)
	}

	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return nil, fmt.Errorf("a proposal needs a fractal or prism")
	}

	var alertArg interface{}
	if in.AlertID != "" {
		alertArg = in.AlertID
	}

	var id string
	if err := m.pg.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO alert_change_requests (%s, alert_id, kind, title, summary, content, tests,
		                                   content_hash, base_hash, created_by, author_label)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id::text`, column),
		scopeID, alertArg, in.Kind, in.Title, in.Summary, contentJSON, testsJSON,
		contentHash, baseHash, storage.NullableUser(username), username,
	).Scan(&id); err != nil {
		return nil, fmt.Errorf("open proposal: %w", err)
	}
	return m.GetChangeRequest(ctx, id)
}

func marshalProposal(in ChangeRequestInput) (contentJSON, testsJSON []byte, err error) {
	if in.Content != nil {
		if contentJSON, err = json.Marshal(in.Content); err != nil {
			return nil, nil, fmt.Errorf("encode proposal: %w", err)
		}
	}
	if in.Tests != nil {
		if testsJSON, err = json.Marshal(in.Tests); err != nil {
			return nil, nil, fmt.Errorf("encode proposal tests: %w", err)
		}
	}
	return contentJSON, testsJSON, nil
}

const changeRequestColumns = `cr.id::text, COALESCE(cr.alert_id::text, ''), COALESCE(a.name, ''), cr.kind, cr.status,
	cr.title, cr.summary, cr.content, cr.tests, cr.content_hash, cr.base_hash,
	COALESCE(cr.created_by, ''), cr.author_label, cr.created_at, cr.updated_at,
	cr.merged_at, COALESCE(cr.merged_by, ''), cr.test_result, cr.test_result_hash`

// saveTestResult keeps a proposal's last run beside it. A failure here only costs a
// re-run later, so the caller logs rather than fails.
func (m *Manager) saveTestResult(ctx context.Context, cr *ChangeRequest, run *TestRunResult) error {
	raw, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = m.pg.DB().ExecContext(ctx,
		`UPDATE alert_change_requests SET test_result = $2, test_result_hash = $3 WHERE id = $1`,
		cr.ID, raw, cr.testsHash())
	return err
}

func scanChangeRequest(scan func(...interface{}) error) (*ChangeRequest, error) {
	var cr ChangeRequest
	var contentRaw, testsRaw, testResultRaw []byte
	var createdAt, updatedAt time.Time
	var mergedAt sql.NullTime

	if err := scan(&cr.ID, &cr.AlertID, &cr.AlertName, &cr.Kind, &cr.Status,
		&cr.Title, &cr.Summary, &contentRaw, &testsRaw, &cr.ContentHash, &cr.BaseHash,
		&cr.Author, &cr.AuthorLabel, &createdAt, &updatedAt, &mergedAt, &cr.MergedBy,
		&testResultRaw, &cr.testResultHash); err != nil {
		return nil, err
	}
	if len(testResultRaw) > 0 {
		var run TestRunResult
		if err := json.Unmarshal(testResultRaw, &run); err == nil {
			cr.testResult = &run
		}
	}

	if len(contentRaw) > 0 {
		var content RevisionContent
		if err := json.Unmarshal(contentRaw, &content); err != nil {
			return nil, fmt.Errorf("decode proposal %s: %w", cr.ID, err)
		}
		content.canonicalize()
		cr.Content = &content
	}
	if len(testsRaw) > 0 {
		if err := json.Unmarshal(testsRaw, &cr.Tests); err != nil {
			return nil, fmt.Errorf("decode proposal tests %s: %w", cr.ID, err)
		}
	}

	cr.CreatedAt = createdAt.Format(time.RFC3339)
	cr.UpdatedAt = updatedAt.Format(time.RFC3339)
	if mergedAt.Valid {
		cr.MergedAt = mergedAt.Time.Format(time.RFC3339)
	}
	return &cr, nil
}

// GetChangeRequest loads one proposal with its reviews.
func (m *Manager) GetChangeRequest(ctx context.Context, crID string) (*ChangeRequest, error) {
	cr, err := scanChangeRequest(m.pg.QueryRow(ctx, `
		SELECT `+changeRequestColumns+`
		  FROM alert_change_requests cr
		  LEFT JOIN alerts a ON a.id = cr.alert_id
		 WHERE cr.id = $1`, crID).Scan)
	if err == sql.ErrNoRows {
		return nil, ErrChangeRequestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load proposal: %w", err)
	}

	if cr.Reviews, err = m.listReviews(ctx, crID, cr.ContentHash); err != nil {
		return nil, err
	}
	return cr, nil
}

func (m *Manager) listReviews(ctx context.Context, crID, currentHash string) ([]Review, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT id::text, COALESCE(reviewer, ''), reviewer_label, decision, comment, content_hash, created_at
		  FROM alert_change_reviews WHERE change_request_id = $1 ORDER BY created_at`, crID)
	if err != nil {
		return nil, fmt.Errorf("load reviews: %w", err)
	}
	defer rows.Close()

	reviews := []Review{}
	for rows.Next() {
		var r Review
		var createdAt time.Time
		if err := rows.Scan(&r.ID, &r.Reviewer, &r.ReviewerLabel, &r.Decision, &r.Comment, &r.ContentHash, &createdAt); err != nil {
			return nil, err
		}
		r.CreatedAt = createdAt.Format(time.RFC3339)
		r.Stale = r.ContentHash != currentHash
		reviews = append(reviews, r)
	}
	return reviews, rows.Err()
}

// ListChangeRequests returns a scope's proposals, newest activity first.
func (m *Manager) ListChangeRequests(ctx context.Context, fractalID, prismID string, openOnly bool) ([]*ChangeRequest, error) {
	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return []*ChangeRequest{}, nil
	}

	query := `
		SELECT ` + changeRequestColumns + `
		  FROM alert_change_requests cr
		  LEFT JOIN alerts a ON a.id = cr.alert_id
		 WHERE cr.` + column + ` = $1`
	if openOnly {
		query += " AND cr.status IN ('open', 'changes_requested')"
	} else {
		// Drafts are private work in progress, not proposals: they are listed by their
		// author through ListDrafts and never appear in the queue.
		query += " AND cr.status <> 'draft'"
	}
	query += " ORDER BY cr.updated_at DESC LIMIT 500"

	rows, err := m.pg.Query(ctx, query, scopeID)
	if err != nil {
		return nil, fmt.Errorf("list proposals: %w", err)
	}
	defer rows.Close()

	list := []*ChangeRequest{}
	for rows.Next() {
		cr, err := scanChangeRequest(rows.Scan)
		if err != nil {
			return nil, err
		}
		list = append(list, cr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Reviews drive the approval count in the list, so they are loaded per row rather
	// than left for the drawer.
	for _, cr := range list {
		if cr.Reviews, err = m.listReviews(ctx, cr.ID, cr.ContentHash); err != nil {
			return nil, err
		}
	}
	return list, nil
}

// ReviewChangeRequest records an approval or a rejection.
//
// A rejection never destroys the proposal: it moves to changes_requested, where its
// author can revise and resubmit it.
func (m *Manager) ReviewChangeRequest(ctx context.Context, crID, decision, comment, username string, isAdmin bool) (*ChangeRequest, error) {
	if decision != ReviewApprove && decision != ReviewReject {
		return nil, fmt.Errorf("decision must be approve or reject")
	}

	cr, err := m.GetChangeRequest(ctx, crID)
	if err != nil {
		return nil, err
	}

	cfg, err := m.gateConfigForChangeRequest(ctx, cr)
	if err != nil {
		return nil, err
	}
	if decision == ReviewApprove {
		if err := cr.CanApprove(username, isAdmin, cfg); err != nil {
			return nil, err
		}
	} else if !cr.Open() {
		return nil, fmt.Errorf("this proposal is %s", cr.Status)
	}

	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO alert_change_reviews (change_request_id, reviewer, reviewer_label, decision, comment, content_hash)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		crID, storage.NullableUser(username), username, decision, comment, cr.ContentHash,
	); err != nil {
		return nil, fmt.Errorf("record review: %w", err)
	}

	status := ChangeOpen
	if decision == ReviewReject {
		status = ChangeRejected
	}
	if _, err := tx.Exec(ctx,
		"UPDATE alert_change_requests SET status = $2, updated_at = NOW() WHERE id = $1", crID, status,
	); err != nil {
		return nil, fmt.Errorf("update proposal: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return m.GetChangeRequest(ctx, crID)
}

// DiscardChangeRequest withdraws a proposal without deleting it, so the work stays
// readable. Authors withdraw their own; admins may withdraw any.
func (m *Manager) DiscardChangeRequest(ctx context.Context, crID, username string, isAdmin bool) error {
	cr, err := m.GetChangeRequest(ctx, crID)
	if err != nil {
		return err
	}
	if !cr.Open() {
		return fmt.Errorf("this proposal is %s", cr.Status)
	}
	if cr.Author != username && !isAdmin {
		return fmt.Errorf("only the author or an admin can withdraw a proposal")
	}

	_, err = m.pg.Exec(ctx,
		"UPDATE alert_change_requests SET status = 'discarded', updated_at = NOW() WHERE id = $1", crID)
	return err
}

// DeleteChangeRequest removes a proposal for good. Admin only: rejecting is the
// reviewer's verb, and it deliberately keeps the author's work.
func (m *Manager) DeleteChangeRequest(ctx context.Context, crID string) error {
	result, err := m.pg.Exec(ctx, "DELETE FROM alert_change_requests WHERE id = $1", crID)
	if err != nil {
		return fmt.Errorf("delete proposal: %w", err)
	}
	if n, err := result.RowsAffected(); err == nil && n == 0 {
		return ErrChangeRequestNotFound
	}
	return nil
}

func (m *Manager) gateConfigForChangeRequest(ctx context.Context, cr *ChangeRequest) (GateConfig, error) {
	fractalID, prismID, err := m.changeRequestScope(ctx, cr.ID)
	if err != nil {
		return DefaultGateConfig(), err
	}
	return m.GateConfigFor(ctx, fractalID, prismID)
}

func (m *Manager) changeRequestScope(ctx context.Context, crID string) (string, string, error) {
	var fractalID, prismID string
	if err := m.pg.QueryRow(ctx,
		`SELECT COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '') FROM alert_change_requests WHERE id = $1`,
		crID,
	).Scan(&fractalID, &prismID); err != nil {
		if err == sql.ErrNoRows {
			return "", "", ErrChangeRequestNotFound
		}
		return "", "", fmt.Errorf("load proposal scope: %w", err)
	}
	return fractalID, prismID, nil
}

// assertAlertInScope refuses an alert that belongs to a different fractal or prism.
func (m *Manager) assertAlertInScope(ctx context.Context, alertID, fractalID, prismID string) error {
	var alertFractal, alertPrism string
	if err := m.pg.QueryRow(ctx,
		`SELECT COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '') FROM alerts WHERE id = $1`,
		alertID,
	).Scan(&alertFractal, &alertPrism); err != nil {
		if err == sql.ErrNoRows {
			return ErrAlertNotFound
		}
		return fmt.Errorf("load alert scope: %w", err)
	}

	if alertFractal != fractalID || alertPrism != prismID {
		return ErrAlertNotFound
	}
	return nil
}

// prepareProposal canonicalizes a proposal and derives everything stored alongside it.
func (m *Manager) prepareProposal(ctx context.Context, in ChangeRequestInput) (contentHash, baseHash string, contentJSON, testsJSON []byte, err error) {
	if in.Content != nil {
		in.Content.canonicalize()
		if contentHash, err = in.Content.Hash(); err != nil {
			return "", "", nil, nil, err
		}
	}
	if baseHash, err = m.headHashFor(ctx, in.AlertID); err != nil {
		return "", "", nil, nil, err
	}
	if contentJSON, testsJSON, err = marshalProposal(in); err != nil {
		return "", "", nil, nil, err
	}
	return contentHash, baseHash, contentJSON, testsJSON, nil
}
