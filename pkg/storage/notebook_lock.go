package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Locking freezes a notebook as the record of an investigation: no edits, and
// query sections serve their stored results instead of running again. A reader
// weeks later sees the rows the author saw, rather than whatever the same query
// returns after retention has aged data out.
//
// A workflow control, not tamper evidence: unlocking is allowed and recorded.

var (
	// ErrNotebookAlreadyLocked distinguishes a redundant lock from a rejected write.
	ErrNotebookAlreadyLocked = errors.New("notebook is already locked")
	// ErrNotebookNotLocked is returned when unlocking a notebook that is not locked.
	ErrNotebookNotLocked = errors.New("notebook is not locked")
)

// UnrunQueriesError refuses a lock that would seal a query section which has
// never run, and says how many. A locked notebook cannot execute, so the
// section would stay blank for as long as the lock stands.
type UnrunQueriesError struct{ Count int }

func (e *UnrunQueriesError) Error() string {
	return fmt.Sprintf("%d query sections have never been run", e.Count)
}

// IsLocked reports whether the notebook is frozen.
func (n *Notebook) IsLocked() bool { return n != nil && n.LockedAt != nil }

// LockedMessage explains a refused write, naming who sealed the notebook and when.
func (n *Notebook) LockedMessage() string {
	who := n.LockedBy
	if who == "" {
		who = "another user"
	}
	return fmt.Sprintf("This notebook was locked by %s on %s. Unlock it to make changes.",
		who, n.LockedAt.UTC().Format("2 Jan 2006 15:04 UTC"))
}

// LockNotebook freezes a notebook and clears it as every user's capture target.
//
// The two happen together: the active notebook is what a star files into, so
// leaving a locked one selected turns the next capture into an error the person
// who set it never sees.
func (c *PostgresClient) LockNotebook(ctx context.Context, notebookID, username string) (*Notebook, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin lock: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		UPDATE notebooks SET locked_at = NOW(), locked_by = $2, updated_at = NOW()
		WHERE id = $1 AND locked_at IS NULL
	`, notebookID, NullableUser(username))
	if err != nil {
		return nil, fmt.Errorf("failed to lock notebook: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, lockNoopReason(ctx, c, notebookID, true)
	}

	// Counted inside the transaction, so a section added between the check and
	// the lock cannot slip through and be sealed blank.
	var unrun int
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*) FROM notebook_sections
		WHERE notebook_id = $1 AND section_type = 'query' AND last_results IS NULL
	`, notebookID).Scan(&unrun); err != nil {
		return nil, fmt.Errorf("failed to check the notebook's query sections: %w", err)
	}
	if unrun > 0 {
		return nil, &UnrunQueriesError{Count: unrun}
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM user_active_notebooks WHERE notebook_id = $1`, notebookID); err != nil {
		return nil, fmt.Errorf("failed to clear the capture target: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit lock: %w", err)
	}
	return c.GetNotebook(ctx, notebookID)
}

// UnlockNotebook returns a notebook to editable.
func (c *PostgresClient) UnlockNotebook(ctx context.Context, notebookID string) (*Notebook, error) {
	res, err := c.db.ExecContext(ctx, `
		UPDATE notebooks SET locked_at = NULL, locked_by = NULL, updated_at = NOW()
		WHERE id = $1 AND locked_at IS NOT NULL
	`, notebookID)
	if err != nil {
		return nil, fmt.Errorf("failed to unlock notebook: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, lockNoopReason(ctx, c, notebookID, false)
	}
	return c.GetNotebook(ctx, notebookID)
}

// lockNoopReason explains an update that changed nothing: either the notebook is
// gone, or it was already in the state asked for.
func lockNoopReason(ctx context.Context, c *PostgresClient, notebookID string, locking bool) error {
	var exists bool
	if err := c.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM notebooks WHERE id = $1)`, notebookID).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check notebook: %w", err)
	}
	if !exists {
		return sql.ErrNoRows
	}
	if locking {
		return ErrNotebookAlreadyLocked
	}
	return ErrNotebookNotLocked
}
