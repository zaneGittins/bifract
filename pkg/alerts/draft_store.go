package alerts

import (
	"context"
	"database/sql"
	"fmt"

	"bifract/pkg/storage"
)

// SaveDraft creates or updates the author's draft.
//
// An alert-scoped draft is one per author per alert, so repeated autosaves land on the
// same row. A draft for an alert that does not exist yet has no such key and is
// addressed by the id the first save returned.
func (m *Manager) SaveDraft(ctx context.Context, crID, fractalID, prismID string, in ChangeRequestInput, username string) (*ChangeRequest, error) {
	if in.Kind == "" {
		in.Kind = ChangeCreate
		if in.AlertID != "" {
			in.Kind = ChangeUpdate
		}
	}
	// A draft is allowed to be incomplete: that is what makes it a draft. Only the
	// shape is checked here; full validation happens when it is submitted or saved.
	if in.Kind != ChangeCreate && in.Kind != ChangeUpdate {
		return nil, fmt.Errorf("a draft is a create or an update")
	}
	if in.Content == nil {
		return nil, fmt.Errorf("a draft needs a definition, however incomplete")
	}

	if in.AlertID != "" {
		if err := m.assertAlertInScope(ctx, in.AlertID, fractalID, prismID); err != nil {
			return nil, err
		}
		// Autosave does not carry the id back, so an alert-scoped draft is found by
		// its key rather than trusted from the request.
		if existing, err := m.DraftForAlert(ctx, in.AlertID, username); err == nil && existing != nil {
			crID = existing.ID
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
		if !existing.Draft() {
			return nil, fmt.Errorf("this is no longer a draft")
		}
		if existing.Author != username {
			return nil, ErrChangeRequestNotFound // another author's draft does not exist, as far as this one is concerned
		}

		if _, err := m.pg.Exec(ctx, `
			UPDATE alert_change_requests
			   SET title = $2, summary = $3, content = $4, tests = $5,
			       content_hash = $6, base_hash = $7, updated_at = NOW()
			 WHERE id = $1 AND status = 'draft'`,
			crID, in.Title, in.Summary, contentJSON, testsJSON, contentHash, baseHash,
		); err != nil {
			return nil, fmt.Errorf("update draft: %w", err)
		}
		return m.GetChangeRequest(ctx, crID)
	}

	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return nil, fmt.Errorf("a draft needs a fractal or prism")
	}
	var alertArg interface{}
	if in.AlertID != "" {
		alertArg = in.AlertID
	}

	var id string
	if err := m.pg.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO alert_change_requests (%s, alert_id, kind, status, title, summary, content, tests,
		                                   content_hash, base_hash, created_by, author_label)
		VALUES ($1, $2, $3, 'draft', $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id::text`, column),
		scopeID, alertArg, in.Kind, in.Title, in.Summary, contentJSON, testsJSON,
		contentHash, baseHash, storage.NullableUser(username), username,
	).Scan(&id); err != nil {
		return nil, fmt.Errorf("create draft: %w", err)
	}
	return m.GetChangeRequest(ctx, id)
}

// DraftForAlert returns the author's draft for an alert, or nil.
func (m *Manager) DraftForAlert(ctx context.Context, alertID, username string) (*ChangeRequest, error) {
	var id string
	err := m.pg.QueryRow(ctx, `
		SELECT id::text FROM alert_change_requests
		 WHERE alert_id = $1 AND created_by = $2 AND status = 'draft'`, alertID, username,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find draft: %w", err)
	}
	return m.GetChangeRequest(ctx, id)
}

// ListDrafts returns the author's drafts in scope, most recently touched first.
func (m *Manager) ListDrafts(ctx context.Context, fractalID, prismID, username string) ([]*ChangeRequest, error) {
	column, scopeID := policyScope(fractalID, prismID)
	if scopeID == "" {
		return []*ChangeRequest{}, nil
	}

	rows, err := m.pg.Query(ctx, `
		SELECT `+changeRequestColumns+`
		  FROM alert_change_requests cr
		  LEFT JOIN alerts a ON a.id = cr.alert_id
		 WHERE cr.`+column+` = $1 AND cr.created_by = $2 AND cr.status = 'draft'
		 ORDER BY cr.updated_at DESC LIMIT 100`, scopeID, username)
	if err != nil {
		return nil, fmt.Errorf("list drafts: %w", err)
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
	return list, rows.Err()
}

// SubmitDraft turns a draft into an open proposal. Everything a proposal must satisfy
// is checked here, since a draft was allowed to skip it.
func (m *Manager) SubmitDraft(ctx context.Context, crID, username string) (*ChangeRequest, error) {
	cr, err := m.GetChangeRequest(ctx, crID)
	if err != nil {
		return nil, err
	}
	if !cr.Draft() {
		return nil, fmt.Errorf("this is no longer a draft")
	}
	if cr.Author != username {
		return nil, ErrChangeRequestNotFound
	}

	in := ChangeRequestInput{Kind: cr.Kind, AlertID: cr.AlertID, Title: cr.Title, Summary: cr.Summary, Content: cr.Content, Tests: cr.Tests}
	if err := in.Validate(); err != nil {
		return nil, err
	}

	// The base moves on submit, not on the first autosave: what matters for the
	// staleness check is the head at the moment review begins.
	baseHash, err := m.headHashFor(ctx, cr.AlertID)
	if err != nil {
		return nil, err
	}

	if _, err := m.pg.Exec(ctx, `
		UPDATE alert_change_requests SET status = 'open', base_hash = $2, updated_at = NOW()
		 WHERE id = $1 AND status = 'draft'`, crID, baseHash); err != nil {
		return nil, fmt.Errorf("submit draft: %w", err)
	}
	return m.GetChangeRequest(ctx, crID)
}

// DeleteDraft removes the author's own draft. Unlike a proposal, a draft has no reviewer
// with a stake in it, so its author may delete it outright.
func (m *Manager) DeleteDraft(ctx context.Context, crID, username string) error {
	result, err := m.pg.Exec(ctx,
		"DELETE FROM alert_change_requests WHERE id = $1 AND created_by = $2 AND status = 'draft'", crID, username)
	if err != nil {
		return fmt.Errorf("delete draft: %w", err)
	}
	if n, err := result.RowsAffected(); err == nil && n == 0 {
		return ErrChangeRequestNotFound
	}
	return nil
}
