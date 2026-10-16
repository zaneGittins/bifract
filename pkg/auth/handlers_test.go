package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"golang.org/x/crypto/bcrypt"
)

// withUser attaches an authenticated principal the way AuthMiddleware does.
func withUser(r *http.Request, u *storage.User) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), "user", u))
}

// withURLParam routes a request through chi's URL parameter context.
func withURLParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// Every case here is refused on input alone. The handler has no database, so a
// case that got as far as storage would panic instead of answering.
func TestHandlersRejectBeforeStorage(t *testing.T) {
	admin := &storage.User{Username: "root", IsAdmin: true}
	local := &storage.User{Username: "alice", AuthProvider: "local"}
	sso := &storage.User{Username: "bob", AuthProvider: "oidc"}
	long := strings.Repeat("p", minPasswordLength)
	short := strings.Repeat("p", minPasswordLength-1)

	type handlerFn func(h *AuthHandler) http.HandlerFunc
	tests := []struct {
		name    string
		handler handlerFn
		method  string
		target  string
		body    string
		user    *storage.User
		param   string
		want    int
	}{
		{name: "login GET", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleLogin }, method: http.MethodGet, want: http.StatusMethodNotAllowed},
		{name: "login bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleLogin }, method: http.MethodPost, body: "{", want: http.StatusBadRequest},

		{name: "register GET", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleRegister }, method: http.MethodGet, want: http.StatusMethodNotAllowed},
		{name: "register bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleRegister }, method: http.MethodPost, body: "[]", want: http.StatusBadRequest},
		{name: "register no username", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleRegister }, method: http.MethodPost, body: `{"role":"admin"}`, want: http.StatusBadRequest},

		{name: "validate invite no token", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleValidateInvite }, method: http.MethodGet, want: http.StatusBadRequest},

		{name: "accept invite GET", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAcceptInvite }, method: http.MethodGet, want: http.StatusMethodNotAllowed},
		{name: "accept invite bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAcceptInvite }, method: http.MethodPost, body: "{", want: http.StatusBadRequest},
		{name: "accept invite no token", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAcceptInvite }, method: http.MethodPost, body: `{"password":"` + long + `"}`, want: http.StatusBadRequest},
		{name: "accept invite no password", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAcceptInvite }, method: http.MethodPost, body: `{"token":"abc"}`, want: http.StatusBadRequest},
		{name: "accept invite short password", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAcceptInvite }, method: http.MethodPost, body: `{"token":"abc","password":"` + short + `"}`, want: http.StatusBadRequest},

		{name: "reset invite GET", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleResetInvite }, method: http.MethodGet, want: http.StatusMethodNotAllowed},
		{name: "reset invite bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleResetInvite }, method: http.MethodPost, body: "{", want: http.StatusBadRequest},
		{name: "reset invite no username", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleResetInvite }, method: http.MethodPost, body: `{}`, want: http.StatusBadRequest},

		{name: "preferences bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleUpdatePreferences }, method: http.MethodPost, body: "{", user: local, want: http.StatusBadRequest},
		{name: "preferences unknown timezone", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleUpdatePreferences }, method: http.MethodPost, body: `{"display_timezone":"Mars/Olympus_Mons"}`, user: local, want: http.StatusBadRequest},

		{name: "change password GET", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleChangePassword }, method: http.MethodGet, user: local, want: http.StatusMethodNotAllowed},
		{name: "change password sso", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleChangePassword }, method: http.MethodPost, body: `{"current_password":"x","new_password":"` + long + `"}`, user: sso, want: http.StatusBadRequest},
		{name: "change password bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleChangePassword }, method: http.MethodPost, body: "{", user: local, want: http.StatusBadRequest},
		{name: "change password missing current", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleChangePassword }, method: http.MethodPost, body: `{"new_password":"` + long + `"}`, user: local, want: http.StatusBadRequest},
		{name: "change password short new", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleChangePassword }, method: http.MethodPost, body: `{"current_password":"x","new_password":"` + short + `"}`, user: local, want: http.StatusBadRequest},

		{name: "admin reset GET", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAdminResetPassword }, method: http.MethodGet, user: admin, want: http.StatusMethodNotAllowed},
		{name: "admin reset bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAdminResetPassword }, method: http.MethodPost, body: "{", user: admin, want: http.StatusBadRequest},
		{name: "admin reset no username", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAdminResetPassword }, method: http.MethodPost, body: `{}`, user: admin, want: http.StatusBadRequest},

		{name: "delete user POST", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleDeleteUser }, method: http.MethodPost, target: "/?username=alice", user: admin, want: http.StatusMethodNotAllowed},
		{name: "delete user no username", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleDeleteUser }, method: http.MethodDelete, user: admin, want: http.StatusBadRequest},
		{name: "delete self", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleDeleteUser }, method: http.MethodDelete, target: "/?username=root", user: admin, want: http.StatusBadRequest},

		{name: "update user no username", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleUpdateUser }, method: http.MethodPut, body: `{}`, user: admin, want: http.StatusBadRequest},
		{name: "update user bad escape", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleUpdateUser }, method: http.MethodPut, body: `{}`, user: admin, param: "%zz", want: http.StatusBadRequest},
		{name: "update user bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleUpdateUser }, method: http.MethodPut, body: "{", user: admin, param: "alice", want: http.StatusBadRequest},
		{name: "update user unknown role", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleUpdateUser }, method: http.MethodPut, body: `{"role":"superuser"}`, user: admin, param: "alice", want: http.StatusBadRequest},
		{name: "demote self", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleUpdateUser }, method: http.MethodPut, body: `{"role":"user"}`, user: admin, param: "root", want: http.StatusBadRequest},
		{name: "demote self escaped", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleUpdateUser }, method: http.MethodPut, body: `{"role":"user"}`, user: &storage.User{Username: "a b", IsAdmin: true}, param: "a%20b", want: http.StatusBadRequest},

		{name: "enable no username", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleSetUserEnabled }, method: http.MethodPut, body: `{}`, user: admin, want: http.StatusBadRequest},
		{name: "enable bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleSetUserEnabled }, method: http.MethodPut, body: "{", user: admin, param: "alice", want: http.StatusBadRequest},
		{name: "disable self", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleSetUserEnabled }, method: http.MethodPut, body: `{"enabled":false}`, user: admin, param: "root", want: http.StatusBadRequest},

		{name: "client cert no username", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleGenerateClientCert }, method: http.MethodPost, user: admin, want: http.StatusBadRequest},

		{name: "admin reset mfa GET", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAdminResetMFA }, method: http.MethodGet, user: admin, want: http.StatusMethodNotAllowed},
		{name: "admin reset mfa bad body", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAdminResetMFA }, method: http.MethodPost, body: "{", user: admin, want: http.StatusBadRequest},
		{name: "admin reset mfa no username", handler: func(h *AuthHandler) http.HandlerFunc { return h.HandleAdminResetMFA }, method: http.MethodPost, body: `{}`, user: admin, want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, store := newTestHandler()
			target := tt.target
			if target == "" {
				target = "/"
			}
			r := httptest.NewRequest(tt.method, target, strings.NewReader(tt.body))
			if tt.user != nil {
				r = withUser(r, tt.user)
			}
			if tt.param != "" {
				r = withURLParam(r, "username", tt.param)
			}
			rec := httptest.NewRecorder()
			tt.handler(h)(rec, r)

			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body.String())
			}
			if body := decodeBody(t, rec); body["success"] != false {
				t.Errorf("refusal reported success: %v", body)
			}
			if len(store.deleted) != 0 || len(store.deletedUsers) != 0 {
				t.Error("a refused request ended sessions")
			}
		})
	}
}

// The limiter is consulted before the body is read or any account is looked up,
// so a blocked address costs the server nothing and learns nothing.
func TestHandleLoginRateLimitedBeforeLookup(t *testing.T) {
	h, _ := newTestHandler()
	for i := 0; i < loginIPMaxFailures; i++ {
		h.loginLimiter.recordFailure("203.0.113.7")
	}

	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"x"}`))
	r.RemoteAddr = "203.0.113.7:4000"
	rec := httptest.NewRecorder()
	h.HandleLogin(rec, r)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Set-Cookie") != "" {
		t.Error("rate limited login set a cookie")
	}

	// Another address is unaffected (and gets as far as decoding the body).
	r = httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader("{"))
	r.RemoteAddr = "203.0.113.8:4000"
	rec = httptest.NewRecorder()
	h.HandleLogin(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unrelated address: status = %d, want 400", rec.Code)
	}
}

func TestHandleChangePasswordWrongCurrentPassword(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", "")
	hash, err := bcrypt.GenerateFromPassword([]byte("the-current-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	user := &storage.User{Username: "alice", AuthProvider: "local", PasswordHash: string(hash)}

	h, store := newTestHandler()
	body := `{"current_password":"not-the-password","new_password":"` + strings.Repeat("n", minPasswordLength) + `"}`
	r := withUser(httptest.NewRequest(http.MethodPost, "/api/v1/auth/change-password", strings.NewReader(body)), user)
	rec := httptest.NewRecorder()
	h.HandleChangePassword(rec, r)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if len(store.deletedUsers) != 0 || len(store.sessions) != 0 {
		t.Error("a refused change touched sessions")
	}
	if rec.Header().Get("Set-Cookie") != "" {
		t.Error("a refused change issued a cookie")
	}
}

func TestHandleLogout(t *testing.T) {
	t.Run("ends the session and clears the cookie", func(t *testing.T) {
		h, store := newTestHandler()
		h.secureCookies = true
		store.sessions["sid-1"] = &Session{Username: "alice"}

		r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "sid-1"})
		rec := httptest.NewRecorder()
		h.HandleLogout(rec, r)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if len(store.deleted) != 1 || store.deleted[0] != "sid-1" {
			t.Errorf("deleted sessions = %v, want [sid-1]", store.deleted)
		}
		cookies := rec.Result().Cookies()
		if len(cookies) != 1 {
			t.Fatalf("got %d cookies, want 1", len(cookies))
		}
		c := cookies[0]
		if c.Name != sessionCookieName || c.Value != "" || c.MaxAge >= 0 {
			t.Errorf("cookie not cleared: %+v", c)
		}
		if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteStrictMode || c.Path != "/" {
			t.Errorf("clearing cookie lost its attributes: %+v", c)
		}
	})

	t.Run("without a cookie still answers", func(t *testing.T) {
		h, store := newTestHandler()
		rec := httptest.NewRecorder()
		h.HandleLogout(rec, httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil))
		if rec.Code != http.StatusOK || len(store.deleted) != 0 {
			t.Errorf("status = %d, deleted = %v", rec.Code, store.deleted)
		}
	})

	// A cross-site GET (an <img> tag) must not be able to log someone out.
	t.Run("GET does nothing", func(t *testing.T) {
		h, store := newTestHandler()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/auth/logout", nil)
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "sid-1"})
		rec := httptest.NewRecorder()
		h.HandleLogout(rec, r)
		if rec.Code != http.StatusMethodNotAllowed || len(store.deleted) != 0 {
			t.Errorf("status = %d, deleted = %v", rec.Code, store.deleted)
		}
	})
}

func TestHandleCurrentUser(t *testing.T) {
	user := &storage.User{
		Username:            "alice",
		DisplayName:         "Alice",
		PasswordHash:        "$2a$12$secrethashsecrethashsecrethashsecrethashsecrethash",
		TOTPSecret:          "sealed-totp-secret",
		ForcePasswordChange: true,
	}

	ctx := context.WithValue(context.Background(), "user", user)
	ctx = context.WithValue(ctx, "fractal_role", "analyst")
	ctx = context.WithValue(ctx, "selected_fractal", testScopeID)
	ctx = context.WithValue(ctx, "mfa_pending", true)

	h, _ := newTestHandler()
	rec := httptest.NewRecorder()
	h.HandleCurrentUser(rec, httptest.NewRequest(http.MethodGet, "/api/v1/auth/user", nil).WithContext(ctx))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if raw := rec.Body.String(); strings.Contains(raw, "secrethash") || strings.Contains(raw, "sealed-totp-secret") {
		t.Fatalf("credential material in the response: %s", raw)
	}
	got, _ := decodeBody(t, rec)["user"].(map[string]any)
	want := map[string]any{
		"username":              "alice",
		"fractal_role":          "analyst",
		"prism_role":            "",
		"selected_fractal":      testScopeID,
		"selected_prism":        "",
		"is_admin":              false,
		"force_password_change": true,
		"mfa_required":          true,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if _, ok := got["mfa_enrollment_required"]; ok {
		t.Error("enrollment flag set alongside a pending verification")
	}

	// A fully authenticated user carries neither flag.
	plain := &storage.User{Username: "bob", AuthProvider: "local", TOTPEnrolled: true}
	rec = httptest.NewRecorder()
	h.HandleCurrentUser(rec, withUser(httptest.NewRequest(http.MethodGet, "/api/v1/auth/user", nil), plain))
	got, _ = decodeBody(t, rec)["user"].(map[string]any)
	for _, k := range []string{"mfa_required", "mfa_enrollment_required", "force_password_change"} {
		if _, ok := got[k]; ok {
			t.Errorf("%s set for a fully authenticated user", k)
		}
	}
}

func TestHandleUpdatePreferencesNoChange(t *testing.T) {
	h, _ := newTestHandler()
	r := withUser(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`)), &storage.User{Username: "alice"})
	rec := httptest.NewRecorder()
	h.HandleUpdatePreferences(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestSessionUser(t *testing.T) {
	t.Run("no cookie", func(t *testing.T) {
		h, _ := newTestHandler()
		u, err := h.SessionUser(httptest.NewRequest(http.MethodGet, "/", nil))
		if u != nil || err != nil {
			t.Errorf("got (%v, %v), want (nil, nil)", u, err)
		}
	})

	t.Run("unknown session", func(t *testing.T) {
		h, _ := newTestHandler()
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "nope"})
		u, err := h.SessionUser(r)
		if u != nil || err != nil {
			t.Errorf("got (%v, %v), want (nil, nil)", u, err)
		}
	})

	// A store outage is an error, never "logged out": the caller would bounce a
	// signed-in user to the login page.
	t.Run("store unavailable", func(t *testing.T) {
		h := &AuthHandler{store: stubSessionStore{getErr: errors.New("pq: too many clients")}}
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "sid"})
		u, err := h.SessionUser(r)
		if u != nil || err == nil {
			t.Errorf("got (%v, %v), want (nil, error)", u, err)
		}
	})

	// A session waiting on its second factor is not a signed-in user, and is
	// decided before any account lookup.
	t.Run("mfa pending", func(t *testing.T) {
		h, store := newTestHandler()
		store.sessions["sid"] = &Session{Username: "alice", MFAPending: true}
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "sid"})
		u, err := h.SessionUser(r)
		if u != nil || err != nil {
			t.Errorf("got (%v, %v), want (nil, nil)", u, err)
		}
	})
}

func TestSetSelectedScopeFromRequest(t *testing.T) {
	h, store := newTestHandler()

	noCookie := httptest.NewRequest(http.MethodPost, "/", nil)
	if err := h.SetSelectedFractalInSessionFromRequest(noCookie, testScopeID); err == nil {
		t.Error("fractal selection without a session succeeded")
	}
	if err := h.SetSelectedPrismInSessionFromRequest(noCookie, testScopeID); err == nil {
		t.Error("prism selection without a session succeeded")
	}

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "sid-7"})
	if err := h.SetSelectedFractalInSessionFromRequest(r, "f-1"); err != nil {
		t.Fatal(err)
	}
	if err := h.SetSelectedPrismInSessionFromRequest(r, "p-1"); err != nil {
		t.Fatal(err)
	}
	if store.updatedFractal["sid-7"] != "f-1" || store.updatedPrism["sid-7"] != "p-1" {
		t.Errorf("selection written to the wrong session: fractal %v, prism %v", store.updatedFractal, store.updatedPrism)
	}

	store.updateErr = errSessionNotFound
	if err := h.SetSelectedFractalInSessionFromRequest(r, "f-2"); !errors.Is(err, errSessionNotFound) {
		t.Errorf("err = %v, want errSessionNotFound", err)
	}
}

func TestCanAccessScopeWithoutResolver(t *testing.T) {
	h := &AuthHandler{}
	ctx := context.Background()

	ok, err := h.canAccessScope(ctx, &storage.User{Username: "root", IsAdmin: true}, testScopeID, "")
	if !ok || err != nil {
		t.Errorf("tenant admin: got (%v, %v), want (true, nil)", ok, err)
	}
	// With nothing to resolve roles against, a non-admin is refused rather than
	// waved through.
	ok, err = h.canAccessScope(ctx, &storage.User{Username: "alice"}, testScopeID, "")
	if ok || err != nil {
		t.Errorf("non-admin without a resolver: got (%v, %v), want (false, nil)", ok, err)
	}
	ok, _ = h.canAccessScope(ctx, &storage.User{Username: "alice"}, "", testScopeID)
	if ok {
		t.Error("non-admin without a resolver was granted a prism")
	}
}

func TestHandleMTLSStatus(t *testing.T) {
	write := func(t *testing.T, dir, name string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name  string
		files []string
		unset bool
		want  bool
	}{
		{name: "not configured", unset: true, want: false},
		{name: "empty dir", want: false},
		{name: "cert without key", files: []string{"ca.pem"}, want: false},
		{name: "key without cert", files: []string{"ca-key.pem"}, want: false},
		{name: "cert and key", files: []string{"ca.pem", "ca-key.pem"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &AuthHandler{}
			if !tt.unset {
				h.clientCADir = t.TempDir()
				for _, f := range tt.files {
					write(t, h.clientCADir, f)
				}
			}
			rec := httptest.NewRecorder()
			h.HandleMTLSStatus(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			data, _ := decodeBody(t, rec)["data"].(map[string]any)
			if got := data["mtls_enabled"]; got != tt.want {
				t.Errorf("mtls_enabled = %v, want %v", got, tt.want)
			}
		})
	}
}
