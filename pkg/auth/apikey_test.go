package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/settings"
	"bifract/pkg/storage"
)

const (
	testKey      = "bifract_0123456789abcdef"
	otherScopeID = "8d2c1a6e-4b2f-4c55-9a0e-3e9f5b7c2d11"
)

func TestExtractAPIKey(t *testing.T) {
	h := &AuthHandler{}
	tests := []struct {
		name    string
		target  string
		headers map[string]string
		want    string
	}{
		{name: "bearer", headers: map[string]string{"Authorization": "Bearer " + testKey}, want: testKey},
		{name: "x-api-key", headers: map[string]string{"X-API-Key": testKey}, want: testKey},
		{name: "bearer wins over x-api-key", headers: map[string]string{"Authorization": "Bearer bifract_a", "X-API-Key": "bifract_b"}, want: "bifract_a"},
		{name: "foreign bearer falls back to x-api-key", headers: map[string]string{"Authorization": "Bearer eyJhbGciOi", "X-API-Key": testKey}, want: testKey},
		{name: "bearer without our prefix", headers: map[string]string{"Authorization": "Bearer eyJhbGciOi"}},
		{name: "x-api-key without our prefix", headers: map[string]string{"X-API-Key": "sk-live-123"}},
		{name: "basic auth", headers: map[string]string{"Authorization": "Basic " + testKey}},
		{name: "bare token", headers: map[string]string{"Authorization": testKey}},
		// Keys in a URL end up in access logs, proxy logs and browser history.
		{name: "query string is ignored", target: "/api/v1/query?api_key=" + testKey + "&key=" + testKey},
		{name: "nothing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := tt.target
			if target == "" {
				target = "/api/v1/query"
			}
			r := httptest.NewRequest(http.MethodGet, target, nil)
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}
			if got := h.extractAPIKey(r); got != tt.want {
				t.Errorf("extractAPIKey = %q, want %q", got, tt.want)
			}
		})
	}
}

// Each key gets its own bucket: one integration exhausting its budget must not
// throttle another key.
func TestAPIKeyAllowedPerKey(t *testing.T) {
	limit := settings.Get().APIKeyRateLimit
	if limit <= 0 {
		t.Skip("API key rate limit is uncapped in this configuration")
	}
	h := &AuthHandler{}

	allowed := 0
	for allowed < 100*limit && h.apiKeyAllowed("key-a") {
		allowed++
	}
	if allowed >= 100*limit {
		t.Fatalf("key-a was never throttled after %d calls with a limit of %d/s", allowed, limit)
	}
	if allowed < limit {
		t.Errorf("key-a throttled after %d calls, before its burst of %d", allowed, limit)
	}
	if !h.apiKeyAllowed("key-b") {
		t.Error("key-b throttled by key-a's usage")
	}
}

// ctxCapture is a terminal handler that records the context it was given.
type ctxCapture struct {
	called bool
	ctx    context.Context
}

func (c *ctxCapture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.called = true
	c.ctx = r.Context()
	w.WriteHeader(http.StatusNoContent)
}

func serveWithKey(h *AuthHandler, method, path string, headers map[string]string) (*httptest.ResponseRecorder, *ctxCapture) {
	next := &ctxCapture{}
	r := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.AuthMiddleware(next).ServeHTTP(rec, r)
	return rec, next
}

func ctxString(ctx context.Context, key string) string {
	s, _ := ctx.Value(key).(string)
	return s
}

func TestAuthMiddlewareRejectsMissingOrBadCredentials(t *testing.T) {
	validator := &fakeValidator{keys: map[string]*ValidatedAPIKey{
		testKey: {KeyID: "k1", FractalID: testScopeID, Role: "viewer"},
	}}

	tests := []struct {
		name      string
		validator APIKeyValidator
		cookie    string
		headers   map[string]string
		target    string
		want      int
	}{
		{name: "no credentials", validator: validator, want: http.StatusUnauthorized},
		{name: "unknown key", validator: validator, headers: map[string]string{"X-API-Key": "bifract_unknown"}, want: http.StatusUnauthorized},
		{name: "key in query string", validator: validator, target: "/api/v1/query?api_key=" + testKey, want: http.StatusUnauthorized},
		{name: "unknown session cookie", validator: validator, cookie: "nope", want: http.StatusUnauthorized},
		{name: "no validator configured", headers: map[string]string{"X-API-Key": testKey}, want: http.StatusUnauthorized},
		{
			name:      "wrapped rejection is still a rejection",
			validator: &fakeValidator{err: fmt.Errorf("key expired: %w", ErrAPIKeyRejected)},
			headers:   map[string]string{"X-API-Key": testKey},
			want:      http.StatusUnauthorized,
		},
		{
			// A validator that cannot answer must not read as a bad key: the
			// client would discard a working credential.
			name:      "validator unavailable",
			validator: &fakeValidator{err: errors.New("pq: connection refused")},
			headers:   map[string]string{"X-API-Key": testKey},
			want:      http.StatusServiceUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler()
			h.apiKeyValidator = tt.validator
			target := tt.target
			if target == "" {
				target = "/api/v1/query"
			}
			next := &ctxCapture{}
			r := httptest.NewRequest(http.MethodGet, target, nil)
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}
			if tt.cookie != "" {
				r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: tt.cookie})
			}
			rec := httptest.NewRecorder()
			h.AuthMiddleware(next).ServeHTTP(rec, r)

			if next.called {
				t.Fatal("request reached the handler")
			}
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if tt.want == http.StatusServiceUnavailable && rec.Header().Get("Retry-After") == "" {
				t.Error("503 without Retry-After")
			}
		})
	}
}

// A key read from the query string must never even be looked up.
func TestAuthMiddlewareDoesNotValidateQueryStringKeys(t *testing.T) {
	v := &fakeValidator{keys: map[string]*ValidatedAPIKey{testKey: {KeyID: "k1"}}}
	h, _ := newTestHandler()
	h.apiKeyValidator = v
	serveWithKey(h, http.MethodGet, "/api/v1/query?api_key="+testKey, nil)
	if n := v.callCount(); n != 0 {
		t.Errorf("validator called %d times for a query-string key", n)
	}
}

func TestAuthMiddlewareFractalScopedKey(t *testing.T) {
	key := &ValidatedAPIKey{KeyID: "k1", Name: "ci", FractalID: testScopeID, Role: "analyst", CreatedBy: "alice"}
	h, _ := newTestHandler()
	h.apiKeyValidator = &fakeValidator{keys: map[string]*ValidatedAPIKey{testKey: key}}

	// The scope header names a different fractal. A scoped key was issued for
	// one fractal and must not be redirected to another by the caller.
	rec, next := serveWithKey(h, http.MethodGet, "/api/v1/query", map[string]string{
		"Authorization": "Bearer " + testKey,
		ScopeHeader:     "fractal:" + otherScopeID,
	})
	if !next.called {
		t.Fatalf("request refused: %d %s", rec.Code, rec.Body.String())
	}
	ctx := next.ctx

	user, _ := ctx.Value("user").(*storage.User)
	if user == nil || user.Username != "apikey_k1" {
		t.Fatalf("principal = %+v, want apikey_k1", user)
	}
	if user.IsAdmin {
		t.Error("a fractal-scoped key became a tenant admin")
	}
	if !IsAPIKey(ctx) {
		t.Error("IsAPIKey = false for a key request")
	}
	if got := ctxString(ctx, "selected_fractal"); got != testScopeID {
		t.Errorf("selected_fractal = %q, want the key's own fractal %q", got, testScopeID)
	}
	if got := ctxString(ctx, "selected_prism"); got != "" {
		t.Errorf("selected_prism = %q, want none", got)
	}
	if got := rbac.RoleFromContext(ctx); got != rbac.RoleAnalyst {
		t.Errorf("fractal_role = %q, want analyst", got)
	}
	if got := rbac.PrismRoleFromContext(ctx); got != rbac.RoleNone {
		t.Errorf("prism_role = %q, want none", got)
	}
	// Writes attribute to the person who made the key, never the synthetic
	// principal, which has no users row.
	if got := AttributionUsername(ctx); got != "alice" {
		t.Errorf("AttributionUsername = %q, want alice", got)
	}
	if got, _ := ctx.Value(storage.AttributionUserKey).(string); got != "alice" {
		t.Errorf("storage attribution = %q, want alice", got)
	}
	// The resolver honours the key's grant on its own fractal only.
	r := rbac.NewResolver(nil)
	if got := r.ResolveRole(ctx, user, testScopeID); got != rbac.RoleAnalyst {
		t.Errorf("ResolveRole on the key's fractal = %q, want analyst", got)
	}
}

func TestAuthMiddlewarePrismScopedKey(t *testing.T) {
	key := &ValidatedAPIKey{KeyID: "k2", PrismID: testScopeID, Role: "viewer"}
	h, _ := newTestHandler()
	h.apiKeyValidator = &fakeValidator{keys: map[string]*ValidatedAPIKey{testKey: key}}

	_, next := serveWithKey(h, http.MethodGet, "/api/v1/query", map[string]string{
		"X-API-Key": testKey,
		ScopeHeader: "prism:" + otherScopeID,
	})
	if !next.called {
		t.Fatal("request refused")
	}
	if got := ctxString(next.ctx, "selected_prism"); got != testScopeID {
		t.Errorf("selected_prism = %q, want the key's own prism", got)
	}
	if got := ctxString(next.ctx, "selected_fractal"); got != "" {
		t.Errorf("selected_fractal = %q, want none", got)
	}
	if got := rbac.PrismRoleFromContext(next.ctx); got != rbac.RoleViewer {
		t.Errorf("prism_role = %q, want viewer", got)
	}
	if got := rbac.RoleFromContext(next.ctx); got != rbac.RoleNone {
		t.Errorf("fractal_role = %q, want none", got)
	}
}

// An instance-wide key was issued for no scope, so it picks one per request with
// the scope header and is admin on it.
func TestAuthMiddlewareTenantAdminKey(t *testing.T) {
	key := &ValidatedAPIKey{KeyID: "k3", TenantAdmin: true, CreatedBy: "root"}

	tests := []struct {
		name        string
		path        string
		header      string
		wantStatus  int
		wantFractal string
		wantPrism   string
		wantNoScope bool
	}{
		{name: "no header", path: "/api/v1/query", wantStatus: http.StatusNoContent},
		{name: "fractal", path: "/api/v1/query", header: "fractal:" + testScopeID, wantStatus: http.StatusNoContent, wantFractal: testScopeID},
		{name: "prism", path: "/api/v1/query", header: "prism:" + testScopeID, wantStatus: http.StatusNoContent, wantPrism: testScopeID},
		{name: "none", path: "/api/v1/query", header: ScopeNone, wantStatus: http.StatusNoContent, wantNoScope: true},
		{name: "malformed", path: "/api/v1/query", header: "fractal:../admin", wantStatus: http.StatusBadRequest},
		{name: "exempt path ignores the header", path: "/api/v1/fractals", header: "fractal:../admin", wantStatus: http.StatusNoContent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler()
			h.apiKeyValidator = &fakeValidator{keys: map[string]*ValidatedAPIKey{testKey: key}}
			headers := map[string]string{"X-API-Key": testKey}
			if tt.header != "" {
				headers[ScopeHeader] = tt.header
			}
			rec, next := serveWithKey(h, http.MethodGet, tt.path, headers)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantStatus == http.StatusBadRequest {
				if next.called {
					t.Fatal("malformed scope reached the handler")
				}
				if rec.Header().Get(ScopeInvalidHeader) == "" {
					t.Error("scope rejection without the scope-invalid marker")
				}
				return
			}
			ctx := next.ctx
			user, _ := ctx.Value("user").(*storage.User)
			if user == nil || !user.IsAdmin {
				t.Fatalf("tenant admin key is not an admin principal: %+v", user)
			}
			if rbac.RoleFromContext(ctx) != rbac.RoleAdmin || rbac.PrismRoleFromContext(ctx) != rbac.RoleAdmin {
				t.Error("tenant admin key is not admin on its scope")
			}
			if got := ctxString(ctx, "selected_fractal"); got != tt.wantFractal {
				t.Errorf("selected_fractal = %q, want %q", got, tt.wantFractal)
			}
			if got := ctxString(ctx, "selected_prism"); got != tt.wantPrism {
				t.Errorf("selected_prism = %q, want %q", got, tt.wantPrism)
			}
			if got := fractals.NoScopeDeclared(ctx); got != tt.wantNoScope {
				t.Errorf("NoScopeDeclared = %v, want %v", got, tt.wantNoScope)
			}
		})
	}
}

// An unknown session cookie is not a reason to refuse a valid key on the same
// request; the middleware falls through to key authentication.
func TestAuthMiddlewareStaleCookieFallsThroughToKey(t *testing.T) {
	h, _ := newTestHandler()
	h.apiKeyValidator = &fakeValidator{keys: map[string]*ValidatedAPIKey{
		testKey: {KeyID: "k1", FractalID: testScopeID, Role: "viewer"},
	}}
	next := &ctxCapture{}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "expired"})
	r.Header.Set("X-API-Key", testKey)
	rec := httptest.NewRecorder()
	h.AuthMiddleware(next).ServeHTTP(rec, r)
	if !next.called {
		t.Fatalf("status = %d, want the key to authenticate", rec.Code)
	}
	if ctxString(next.ctx, "auth_type") != "api_key" {
		t.Error("request did not authenticate as an API key")
	}
}

func TestAuthMiddlewareKeyRateLimit(t *testing.T) {
	limit := settings.Get().APIKeyRateLimit
	if limit <= 0 {
		t.Skip("API key rate limit is uncapped in this configuration")
	}
	h, _ := newTestHandler()
	h.apiKeyValidator = &fakeValidator{keys: map[string]*ValidatedAPIKey{
		testKey: {KeyID: "k1", FractalID: testScopeID, Role: "viewer"},
	}}
	mw := h.AuthMiddleware(&ctxCapture{})

	for i := 0; i < 100*limit; i++ {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
		r.Header.Set("X-API-Key", testKey)
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, r)
		if rec.Code == http.StatusTooManyRequests {
			if i < limit {
				t.Errorf("throttled after %d requests, before the burst of %d", i, limit)
			}
			if rec.Header().Get("Retry-After") == "" {
				t.Error("429 without Retry-After")
			}
			return
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("request %d: status %d", i, rec.Code)
		}
	}
	t.Fatalf("never throttled after %d requests", 100*limit)
}

func TestDenyAPIKey(t *testing.T) {
	mw := DenyAPIKey("Favorites")

	t.Run("api key refused", func(t *testing.T) {
		next := &ctxCapture{}
		r := httptest.NewRequest(http.MethodGet, "/api/v1/favorites", nil)
		r = r.WithContext(context.WithValue(r.Context(), "auth_type", "api_key"))
		rec := httptest.NewRecorder()
		mw(next).ServeHTTP(rec, r)
		if next.called {
			t.Fatal("API key reached per-user state")
		}
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", rec.Code)
		}
		body := decodeBody(t, rec)
		if body["success"] != false || !strings.Contains(fmt.Sprint(body["error"]), "Favorites") {
			t.Errorf("unexpected body %v", body)
		}
	})

	for _, authType := range []any{"session", nil} {
		t.Run(fmt.Sprintf("auth_type %v passes", authType), func(t *testing.T) {
			next := &ctxCapture{}
			r := httptest.NewRequest(http.MethodGet, "/api/v1/favorites", nil)
			if authType != nil {
				r = r.WithContext(context.WithValue(r.Context(), "auth_type", authType))
			}
			rec := httptest.NewRecorder()
			mw(next).ServeHTTP(rec, r)
			if !next.called {
				t.Fatalf("session request refused with %d", rec.Code)
			}
		})
	}
}
