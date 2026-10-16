package auth

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
)

// fakeSessionStore is an in-memory SessionStore that records what was removed,
// so tests can assert a handler did or did not end a session.
type fakeSessionStore struct {
	mu             sync.Mutex
	sessions       map[string]*Session
	deleted        []string
	deletedUsers   []string
	updatedFractal map[string]string
	updatedPrism   map[string]string
	updateErr      error
}

func newFakeSessionStore() *fakeSessionStore {
	return &fakeSessionStore{
		sessions:       map[string]*Session{},
		updatedFractal: map[string]string{},
		updatedPrism:   map[string]string{},
	}
}

func (f *fakeSessionStore) Get(id string) (*Session, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sessions[id]
	return s, ok, nil
}

func (f *fakeSessionStore) Set(id string, s *Session) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[id] = s
	return nil
}

func (f *fakeSessionStore) Delete(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, id)
	delete(f.sessions, id)
}

func (f *fakeSessionStore) DeleteByUsername(u string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedUsers = append(f.deletedUsers, u)
}

func (f *fakeSessionStore) Cleanup() {}

func (f *fakeSessionStore) UpdateFractal(id, fractalID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updatedFractal[id] = fractalID
	return nil
}

func (f *fakeSessionStore) UpdatePrism(id, prismID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updatedPrism[id] = prismID
	return nil
}

// newTestHandler builds an AuthHandler with no database behind it. Any path that
// reaches Postgres or ClickHouse dereferences a nil client and panics, so a test
// passing against it also proves the handler decided before touching storage.
// logAuthEvent is a no-op here because ch is nil.
func newTestHandler() (*AuthHandler, *fakeSessionStore) {
	store := newFakeSessionStore()
	return &AuthHandler{
		store:        store,
		loginLimiter: newLoginRateLimiter(),
		mfaLimiter:   newLoginRateLimiter(),
	}, store
}

// fakeValidator answers ValidateAPIKey from a fixed map and counts calls.
type fakeValidator struct {
	mu    sync.Mutex
	keys  map[string]*ValidatedAPIKey
	err   error
	calls int
}

func (v *fakeValidator) ValidateAPIKey(_ context.Context, key string) (*ValidatedAPIKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	if v.err != nil {
		return nil, v.err
	}
	if k, ok := v.keys[key]; ok {
		return k, nil
	}
	return nil, ErrAPIKeyRejected
}

func (v *fakeValidator) UpdateLastUsed(context.Context, string) error { return nil }

func (v *fakeValidator) callCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.calls
}

// decodeBody parses a JSON response body into a generic map.
func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("response is not JSON: %v: %s", err, rec.Body.String())
	}
	return m
}
