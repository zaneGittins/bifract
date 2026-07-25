package auth

import (
	"context"
	"database/sql"
	"time"
)

// SessionStore abstracts session persistence.
type SessionStore interface {
	Get(sessionID string) (*Session, bool)
	Set(sessionID string, session *Session)
	Delete(sessionID string)
	DeleteByUsername(username string)
	Cleanup()
	UpdateFractal(sessionID, fractalID string) error
	UpdatePrism(sessionID, prismID string) error
}

// pgSessionStore persists sessions in PostgreSQL.
type pgSessionStore struct {
	db *sql.DB
}

func newPgSessionStore(db *sql.DB) *pgSessionStore {
	return &pgSessionStore{db: db}
}

func (p *pgSessionStore) Get(sessionID string) (*Session, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s := &Session{}
	var fractal, prism sql.NullString
	// The joins resolve a scope that no longer exists to NULL. selected_fractal
	// and selected_prism carry no foreign key, so deleting a fractal leaves every
	// session that had it selected pinned to a dangling id, and callers only fall
	// back to the default fractal when the value is empty. Resolving it here fixes
	// that for every consumer at once, and covers ids that went stale by any route
	// (delete, restore, manual SQL), not just through the delete handler.
	err := p.db.QueryRowContext(ctx,
		`SELECT s.username, s.created_at, s.expires_at,
		        CASE WHEN f.id IS NULL THEN NULL ELSE s.selected_fractal END,
		        CASE WHEN p.id IS NULL THEN NULL ELSE s.selected_prism END
		 FROM sessions s
		 LEFT JOIN fractals f ON f.id::text = s.selected_fractal
		 LEFT JOIN prisms   p ON p.id::text = s.selected_prism
		 WHERE s.session_id = $1 AND s.expires_at > NOW()`,
		sessionID,
	).Scan(&s.Username, &s.CreatedAt, &s.ExpiresAt, &fractal, &prism)
	if err != nil {
		return nil, false
	}
	s.SelectedFractal = fractal.String
	s.SelectedPrism = prism.String
	return s, true
}

func (p *pgSessionStore) Set(sessionID string, session *Session) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var fractal, prism *string
	if session.SelectedFractal != "" {
		fractal = &session.SelectedFractal
	}
	if session.SelectedPrism != "" {
		prism = &session.SelectedPrism
	}

	p.db.ExecContext(ctx,
		`INSERT INTO sessions (session_id, username, created_at, expires_at, selected_fractal, selected_prism)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (session_id) DO UPDATE SET
		   username = EXCLUDED.username,
		   expires_at = EXCLUDED.expires_at,
		   selected_fractal = EXCLUDED.selected_fractal,
		   selected_prism = EXCLUDED.selected_prism`,
		sessionID, session.Username, session.CreatedAt, session.ExpiresAt, fractal, prism,
	)
}

func (p *pgSessionStore) Delete(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p.db.ExecContext(ctx, `DELETE FROM sessions WHERE session_id = $1`, sessionID)
}

func (p *pgSessionStore) DeleteByUsername(username string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p.db.ExecContext(ctx, `DELETE FROM sessions WHERE username = $1`, username)
}

func (p *pgSessionStore) Cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p.db.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at < NOW()`)
}

func (p *pgSessionStore) UpdateFractal(sessionID, fractalID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := p.db.ExecContext(ctx,
		`UPDATE sessions SET selected_fractal = $1, selected_prism = NULL
		 WHERE session_id = $2 AND expires_at > NOW()`,
		fractalID, sessionID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSessionNotFound
	}
	return nil
}

func (p *pgSessionStore) UpdatePrism(sessionID, prismID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := p.db.ExecContext(ctx,
		`UPDATE sessions SET selected_prism = $1, selected_fractal = NULL
		 WHERE session_id = $2 AND expires_at > NOW()`,
		prismID, sessionID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSessionNotFound
	}
	return nil
}
