package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Evidence is a comment filed into a notebook. The comment is the record; the
// notebook_sections row is the edge, so the same comment can be evidence in
// more than one notebook and editing it is reflected everywhere it appears.

// EvidenceItem places one comment into a notebook. A nil OrderIndex appends.
type EvidenceItem struct {
	CommentID  string
	Title      string
	EventTime  time.Time
	OrderIndex *int
}

// CommentWrite is the outcome of filing a comment. Existing is set when the log
// was already evidence in the notebook and nothing was written.
type CommentWrite struct {
	Comment  *Comment
	Section  *NotebookSection
	Existing bool
}

// InsertCommentWithEvidence inserts a comment and, when notebookID is set,
// files it into that notebook as an evidence section in the same transaction.
// A comment that fails to file must not be left behind as an orphan the UI
// cannot show, which is why both writes share one transaction.
//
// dedupe makes the write a no-op when the log is already evidence in the
// notebook, which is what a repeated star has to be. The check runs under an
// advisory lock on (notebook, log) because two clicks racing would otherwise
// both see nothing and both insert.
func (c *PostgresClient) InsertCommentWithEvidence(ctx context.Context, comment Comment, notebookID string, title *string, dedupe bool) (*CommentWrite, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if notebookID != "" && dedupe {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, notebookID+":"+comment.LogID); err != nil {
			return nil, fmt.Errorf("failed to lock notebook evidence: %w", err)
		}
		var existingID string
		err := tx.QueryRowContext(ctx, `
			SELECT c.id FROM notebook_sections s JOIN comments c ON c.id = s.comment_id
			WHERE s.notebook_id = $1 AND c.log_id = $2 LIMIT 1
		`, notebookID, comment.LogID).Scan(&existingID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("failed to check notebook evidence: %w", err)
		}
		if err == nil {
			if err := tx.Commit(); err != nil {
				return nil, fmt.Errorf("failed to release evidence lock: %w", err)
			}
			existing, err := c.GetComment(ctx, existingID)
			if err != nil {
				return nil, err
			}
			return &CommentWrite{Comment: existing, Existing: true}, nil
		}
	}

	var tags interface{}
	if len(comment.Tags) > 0 {
		tags = pq.Array(comment.Tags)
	}
	var fractalIDPtr, prismIDPtr interface{}
	if comment.FractalID != "" {
		fractalIDPtr = comment.FractalID
	}
	if comment.PrismID != "" {
		prismIDPtr = comment.PrismID
	}

	var newComment Comment
	err = tx.QueryRowContext(ctx, `
		INSERT INTO comments (log_id, log_timestamp, text, author, tags, query, fractal_id, prism_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, log_id, log_timestamp, text, author, COALESCE(tags, '{}'), query, created_at, updated_at,
		          COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '')
	`, comment.LogID, comment.LogTimestamp, comment.Text, comment.Author, tags, comment.Query, fractalIDPtr, prismIDPtr).Scan(
		&newComment.ID, &newComment.LogID, &newComment.LogTimestamp, &newComment.Text, &newComment.Author,
		pq.Array(&newComment.Tags), &newComment.Query, &newComment.CreatedAt, &newComment.UpdatedAt,
		&newComment.FractalID, &newComment.PrismID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert comment: %w", err)
	}

	var sectionID string
	if notebookID != "" {
		var orderIndex int
		if err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(MAX(order_index) + 1, 0) FROM notebook_sections WHERE notebook_id = $1
		`, notebookID).Scan(&orderIndex); err != nil {
			return nil, fmt.Errorf("failed to place evidence section: %w", err)
		}

		eventTime := newComment.LogTimestamp.UTC()
		sectionID, err = insertNotebookSectionRow(ctx, tx, NotebookSection{
			NotebookID:  notebookID,
			SectionType: "comment_context",
			Title:       title,
			OrderIndex:  orderIndex,
			CommentID:   &newComment.ID,
			EventTime:   &eventTime,
		})
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit comment: %w", err)
	}

	if author, err := c.GetUser(ctx, newComment.Author); err == nil {
		newComment.AuthorDisplayName = author.DisplayName
		newComment.AuthorGravatarColor = author.GravatarColor
		newComment.AuthorGravatarInitial = author.GravatarInitial
	}

	write := &CommentWrite{Comment: &newComment}
	if sectionID != "" {
		write.Section, err = c.GetNotebookSection(ctx, sectionID)
		if err != nil {
			return nil, err
		}
	}
	return write, nil
}

// InsertEvidenceSections files comments into a notebook, appending after its
// current last section and skipping any already filed there. Returns the
// sections created, in order.
func (c *PostgresClient) InsertEvidenceSections(ctx context.Context, notebookID string, items []EvidenceItem) ([]NotebookSection, error) {
	if len(items) == 0 {
		return nil, nil
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Keyed by comment: two analysts writing about the same log are two pieces
	// of evidence, and a re-run must add neither of them twice.
	rows, err := tx.QueryContext(ctx, `
		SELECT comment_id::text FROM notebook_sections WHERE notebook_id = $1 AND comment_id IS NOT NULL
	`, notebookID)
	if err != nil {
		return nil, fmt.Errorf("failed to read existing evidence: %w", err)
	}
	present := map[string]bool{}
	for rows.Next() {
		var commentID string
		if err := rows.Scan(&commentID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan existing evidence: %w", err)
		}
		present[commentID] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read existing evidence: %w", err)
	}

	var orderIndex int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(order_index) + 1, 0) FROM notebook_sections WHERE notebook_id = $1
	`, notebookID).Scan(&orderIndex); err != nil {
		return nil, fmt.Errorf("failed to place evidence sections: %w", err)
	}

	var ids []string
	for _, item := range items {
		if present[item.CommentID] {
			continue
		}
		present[item.CommentID] = true

		// A caller-supplied index is clamped: a negative one would sort the
		// section above everything, including sections the caller never saw.
		at := orderIndex
		if item.OrderIndex != nil && *item.OrderIndex >= 0 {
			at = *item.OrderIndex
		}
		title := item.Title
		eventTime := item.EventTime.UTC()
		id, err := insertNotebookSectionRow(ctx, tx, NotebookSection{
			NotebookID:  notebookID,
			SectionType: "comment_context",
			Title:       &title,
			OrderIndex:  at,
			CommentID:   &item.CommentID,
			EventTime:   &eventTime,
		})
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
		if item.OrderIndex == nil || *item.OrderIndex < 0 {
			orderIndex++
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit evidence sections: %w", err)
	}

	sections := make([]NotebookSection, 0, len(ids))
	for _, id := range ids {
		section, err := c.GetNotebookSection(ctx, id)
		if err != nil {
			return nil, err
		}
		sections = append(sections, *section)
	}
	return sections, nil
}

// GetActiveNotebook returns the notebook this user captures into for a scope,
// or empty when they have not chosen one or it has since been deleted.
func (c *PostgresClient) GetActiveNotebook(ctx context.Context, username, scopeKey string) (string, error) {
	var notebookID string
	err := c.db.QueryRowContext(ctx, `
		SELECT notebook_id::text FROM user_active_notebooks WHERE username = $1 AND scope_key = $2
	`, username, scopeKey).Scan(&notebookID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to get active notebook: %w", err)
	}
	return notebookID, nil
}

// SetActiveNotebook records the notebook this user captures into for a scope.
func (c *PostgresClient) SetActiveNotebook(ctx context.Context, username, scopeKey, notebookID string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO user_active_notebooks (username, scope_key, notebook_id, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (username, scope_key) DO UPDATE SET notebook_id = EXCLUDED.notebook_id, updated_at = NOW()
	`, username, scopeKey, notebookID)
	if err != nil {
		return fmt.Errorf("failed to set active notebook: %w", err)
	}
	return nil
}

// ClearActiveNotebook forgets this user's capture target for a scope.
func (c *PostgresClient) ClearActiveNotebook(ctx context.Context, username, scopeKey string) error {
	_, err := c.db.ExecContext(ctx, `
		DELETE FROM user_active_notebooks WHERE username = $1 AND scope_key = $2
	`, username, scopeKey)
	if err != nil {
		return fmt.Errorf("failed to clear active notebook: %w", err)
	}
	return nil
}

// ScopeCaptureState reports whether a scope has ever used notebooks or comments.
// The results table renders its star gutter only once one of them is true, so
// the column costs nothing for anyone who does not use the feature.
func (c *PostgresClient) ScopeCaptureState(ctx context.Context, fractalID, prismID string) (hasNotebooks, hasComments bool, err error) {
	var fractalArg, prismArg interface{}
	if fractalID != "" {
		fractalArg = fractalID
	}
	if prismID != "" {
		prismArg = prismID
	}
	err = c.db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM notebooks  WHERE fractal_id IS NOT DISTINCT FROM $1 AND prism_id IS NOT DISTINCT FROM $2),
		       EXISTS(SELECT 1 FROM comments   WHERE fractal_id IS NOT DISTINCT FROM $1 AND prism_id IS NOT DISTINCT FROM $2)
	`, fractalArg, prismArg).Scan(&hasNotebooks, &hasComments)
	if err != nil {
		return false, false, fmt.Errorf("failed to read scope capture state: %w", err)
	}
	return hasNotebooks, hasComments, nil
}

// DeleteEvidence removes a log from a notebook and returns the section ids it
// removed. Comments left with no text and no remaining section were stars
// carrying nothing, so they go too; one that someone wrote in stays, as an
// annotation on a log the notebook no longer holds.
func (c *PostgresClient) DeleteEvidence(ctx context.Context, notebookID, logID string) ([]string, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		DELETE FROM notebook_sections s
		USING comments c
		WHERE s.notebook_id = $1 AND s.comment_id = c.id AND c.log_id = $2
		RETURNING s.id, c.id
	`, notebookID, logID)
	if err != nil {
		return nil, fmt.Errorf("failed to remove evidence: %w", err)
	}
	var sectionIDs, commentIDs []string
	for rows.Next() {
		var sectionID, commentID string
		if err := rows.Scan(&sectionID, &commentID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan removed evidence: %w", err)
		}
		sectionIDs = append(sectionIDs, sectionID)
		commentIDs = append(commentIDs, commentID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to remove evidence: %w", err)
	}

	for _, id := range commentIDs {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM comments
			WHERE id = $1 AND text = ''
			  AND NOT EXISTS (SELECT 1 FROM notebook_sections WHERE comment_id = $1)
		`, id); err != nil {
			return nil, fmt.Errorf("failed to remove an emptied star: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit evidence removal: %w", err)
	}
	return sectionIDs, nil
}

// ScratchNotebookName is what a notebook created by the first capture is called.
// It reads as something to rename, and renaming it is what promotes it: the next
// capture with nothing active then starts a fresh one rather than reopening an
// investigation someone considered finished.
const ScratchNotebookName = "Untitled notebook"

// GetOrCreateScratchNotebook returns this user's scratch notebook for a scope,
// creating it on first use. Capture must never stop to ask for a name at the
// moment someone found something, and the alternative to this is a pile of empty
// notebooks, one per star. A locked scratch notebook is skipped, so sealing one
// starts the next capture in a fresh one rather than failing.
func (c *PostgresClient) GetOrCreateScratchNotebook(ctx context.Context, fractalID, prismID, username string) (*Notebook, error) {
	scopeCol, scopeVal, err := notebookScopePredicate("", fractalID, prismID)
	if err != nil {
		return nil, err
	}

	var id string
	err = c.db.QueryRowContext(ctx, `
		SELECT id::text FROM notebooks
		WHERE name = $1 AND created_by = $2 AND `+scopeCol+` = $3 AND locked_at IS NULL
		ORDER BY created_at DESC LIMIT 1
	`, ScratchNotebookName, username, scopeVal).Scan(&id)
	if err == nil {
		return c.GetNotebook(ctx, id)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("failed to look up the scratch notebook: %w", err)
	}

	notebook := Notebook{
		Name:                 ScratchNotebookName,
		Description:          "Created by the first capture in this scope. Rename it to keep it.",
		TimeRangeType:        "24h",
		MaxResultsPerSection: 1000,
		CreatedBy:            username,
	}
	if prismID != "" {
		notebook.PrismID = prismID
	} else {
		notebook.FractalID = fractalID
	}
	return c.InsertNotebook(ctx, notebook)
}

// UpdateCommentTags replaces a comment's tags without touching its text.
//
// Separate from UpdateComment because the two carry different authority: the
// text is what one person wrote and only they may change it, while tags are how
// a team organises shared evidence and any analyst in the scope may set them.
func (c *PostgresClient) UpdateCommentTags(ctx context.Context, id string, tags []string) error {
	var arr interface{}
	if len(tags) > 0 {
		arr = pq.Array(tags)
	}
	result, err := c.db.ExecContext(ctx, `
		UPDATE comments SET tags = $2, updated_at = NOW() WHERE id = $1
	`, id, arr)
	if err != nil {
		return fmt.Errorf("failed to update comment tags: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to update comment tags: %w", err)
	}
	if affected == 0 {
		return errors.New("comment not found")
	}
	return nil
}
