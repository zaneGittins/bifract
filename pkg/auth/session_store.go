package auth

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// SessionStore abstracts session persistence so multiple Bifract replicas can
// share sessions via Postgres (K8s) while single-instance deployments keep
// using a fast in-memory map (Docker Compose).
type SessionStore interface {
	Get(sessionID string) (*Session, bool)
	Set(sessionID string, session *Session)
	Delete(sessionID string)
	DeleteByUsername(username string)
	Cleanup()
	UpdateFractal(sessionID, fractalID string) error
	UpdatePrism(sessionID, prismID string) error
}

// memorySessionStore is the default in-memory session store.
type memorySessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func newMemorySessionStore() *memorySessionStore {
	return &memorySessionStore{sessions: make(map[string]*Session)}
}

func (m *memorySessionStore) Get(sessionID string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[sessionID]
	if !ok || time.Now().After(s.ExpiresAt) {
		return nil, false
	}
	return s, true
}

func (m *memorySessionStore) Set(sessionID string, session *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[sessionID] = session
}

func (m *memorySessionStore) Delete(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
}

func (m *memorySessionStore) DeleteByUsername(username string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, s := range m.sessions {
		if s.Username == username {
			delete(m.sessions, id)
		}
	}
}

func (m *memorySessionStore) Cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for id, s := range m.sessions {
		if now.After(s.ExpiresAt) {
			delete(m.sessions, id)
		}
	}
}

func (m *memorySessionStore) UpdateFractal(sessionID, fractalID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return errSessionNotFound
	}
	s.SelectedFractal = fractalID
	s.SelectedPrism = ""
	return nil
}

func (m *memorySessionStore) UpdatePrism(sessionID, prismID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return errSessionNotFound
	}
	s.SelectedPrism = prismID
	s.SelectedFractal = ""
	return nil
}

// pgSessionStore persists sessions in PostgreSQL for multi-replica deployments.
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
	err := p.db.QueryRowContext(ctx,
		`SELECT username, created_at, expires_at, selected_fractal, selected_prism
		 FROM sessions WHERE session_id = $1 AND expires_at > NOW()`,
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
