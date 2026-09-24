package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// stubSessionStore answers Get with a fixed result; the rest is unused here.
type stubSessionStore struct {
	SessionStore
	getErr error
}

func (s stubSessionStore) Get(string) (*Session, bool, error) { return nil, false, s.getErr }

// A store that cannot answer must not read as logged out: the client redirects
// to login on 401, which an SSO provider then satisfies silently, in a loop.
func TestAuthMiddlewareSessionStoreFailure(t *testing.T) {
	cases := []struct {
		name   string
		getErr error
		want   int
	}{
		{name: "store unavailable", getErr: errors.New("pq: sorry, too many clients already"), want: http.StatusServiceUnavailable},
		{name: "no such session", want: http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &AuthHandler{store: stubSessionStore{getErr: tc.getErr}}
			called := false
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })

			req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/user", nil)
			req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "sid"})
			rec := httptest.NewRecorder()
			h.AuthMiddleware(next).ServeHTTP(rec, req)

			if called {
				t.Fatal("request reached the handler without a session")
			}
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.want == http.StatusServiceUnavailable && rec.Header().Get("Retry-After") == "" {
				t.Error("503 without Retry-After")
			}
		})
	}
}
