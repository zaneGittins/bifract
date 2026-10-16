package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"bifract/pkg/fractals"
	"bifract/pkg/storage"
)

func TestAuthMiddlewareSessionUser(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice")
	sid := db.newSession("sid-alice", "alice", onFractal(opsFractalID))
	db.grantFractal(opsFractalID, "alice", "analyst")

	rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, nil)
	if !next.called {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	user, _ := next.ctx.Value("user").(*storage.User)
	if user == nil || user.Username != "alice" || user.IsAdmin {
		t.Fatalf("user in context = %+v", user)
	}
	for key, want := range map[string]string{
		"auth_type":        "session",
		"selected_fractal": opsFractalID,
		"selected_prism":   "",
		"fractal_role":     "analyst",
	} {
		if got := ctxString(next.ctx, key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if got, _ := next.ctx.Value(storage.AttributionUserKey).(string); got != "alice" {
		t.Errorf("attribution = %q, want alice", got)
	}
	// No prism selected: no prism role is resolved or claimed.
	if _, ok := next.ctx.Value("prism_role").(string); ok {
		t.Error("prism_role set with no prism in scope")
	}
}

// A role is the strongest of the user's own grant and their groups' grants on
// the scope in question, never a grant on another fractal or another user's.
func TestAuthMiddlewareRoleResolution(t *testing.T) {
	cases := []struct {
		name  string
		setup func(db *authDB)
		want  string
	}{
		{"no grant", func(db *authDB) {}, ""},
		{"direct grant", func(db *authDB) { db.grantFractal(opsFractalID, "alice", "viewer") }, "viewer"},
		{"group grant", func(db *authDB) {
			db.joinGroup("g-soc", "alice")
			db.grantFractal(opsFractalID, "group:g-soc", "analyst")
		}, "analyst"},
		{"group outranks direct", func(db *authDB) {
			db.grantFractal(opsFractalID, "alice", "viewer")
			db.joinGroup("g-soc", "alice")
			db.grantFractal(opsFractalID, "group:g-soc", "admin")
		}, "admin"},
		{"direct outranks group", func(db *authDB) {
			db.grantFractal(opsFractalID, "alice", "admin")
			db.joinGroup("g-soc", "alice")
			db.grantFractal(opsFractalID, "group:g-soc", "viewer")
		}, "admin"},
		{"grant on another fractal", func(db *authDB) { db.grantFractal(labFractalID, "alice", "admin") }, ""},
		{"another user's grant", func(db *authDB) { db.grantFractal(opsFractalID, "bob", "admin") }, ""},
		{"group alice is not in", func(db *authDB) {
			db.joinGroup("g-other", "bob")
			db.grantFractal(opsFractalID, "group:g-other", "admin")
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, db := newDBHandler(t)
			db.addUser("alice")
			db.addUser("bob")
			tc.setup(db)
			sid := db.newSession("sid", "alice", onFractal(opsFractalID))

			_, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, nil)
			if !next.called {
				t.Fatal("request did not reach the handler")
			}
			if got := ctxString(next.ctx, "fractal_role"); got != tc.want {
				t.Errorf("fractal_role = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAuthMiddlewarePrismRole(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.joinGroup("g-soc", "alice")
	db.grantPrism(opsPrismID, "group:g-soc", "analyst")
	sid := db.newSession("sid", "alice", onPrism(opsPrismID))

	_, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, nil)
	if !next.called {
		t.Fatal("request did not reach the handler")
	}
	if got := ctxString(next.ctx, "prism_role"); got != "analyst" {
		t.Errorf("prism_role = %q, want analyst", got)
	}
	if got := ctxString(next.ctx, "selected_fractal"); got != "" {
		t.Errorf("selected_fractal = %q with a prism selected", got)
	}
	if _, ok := next.ctx.Value("fractal_role").(string); ok {
		t.Error("fractal_role set with only a prism in scope")
	}
}

// A tenant admin is admin everywhere without a permission lookup.
func TestAuthMiddlewareTenantAdmin(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("root", asAdmin)
	sid := db.newSession("sid", "root", onFractal(labFractalID))

	_, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, map[string]string{ScopeHeader: "prism:" + opsPrismID})
	if !next.called {
		t.Fatal("request did not reach the handler")
	}
	if ctxString(next.ctx, "fractal_role") != "admin" || ctxString(next.ctx, "prism_role") != "admin" {
		t.Errorf("roles = %q/%q, want admin/admin", ctxString(next.ctx, "fractal_role"), ctxString(next.ctx, "prism_role"))
	}
	if ctxString(next.ctx, "selected_prism") != opsPrismID {
		t.Errorf("selected_prism = %q, want the header's prism", ctxString(next.ctx, "selected_prism"))
	}
	if n := len(db.pg.Matching("_permissions")); n != 0 {
		t.Errorf("%d permission lookups for a tenant admin", n)
	}
}

// Because the user is reloaded on every request, disabling an account locks out
// a session that is already open. Logout stays reachable so the cookie can go.
func TestAuthMiddlewareDisabledAccount(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice", disabled)
	db.grantFractal(opsFractalID, "alice", "admin")
	sid := db.newSession("sid", "alice", onFractal(opsFractalID))

	for _, path := range []string{"/api/v1/query", "/api/v1/auth/user", "/api/v1/auth/change-password", "/api/v1/auth/mfa/verify"} {
		rec, next := throughMiddleware(h, http.MethodGet, path, sid, nil)
		if next.called || rec.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, reached handler %v; want 403", path, rec.Code, next.called)
		}
	}
	if _, next := throughMiddleware(h, http.MethodPost, "/api/v1/auth/logout", sid, nil); !next.called {
		t.Error("a disabled user cannot reach logout")
	}
}

// A session whose user row is gone reads as logged out; a user lookup that
// fails reads as unavailable, never as logged out.
func TestAuthMiddlewareUserLookup(t *testing.T) {
	t.Run("deleted user", func(t *testing.T) {
		h, db := newDBHandler(t)
		sid := db.newSession("sid", "ghost")
		rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, nil)
		if next.called || rec.Code != http.StatusUnauthorized {
			t.Errorf("status %d, reached handler %v; want 401", rec.Code, next.called)
		}
	})
	t.Run("lookup fails", func(t *testing.T) {
		h, db := newDBHandler(t)
		db.addUser("alice")
		sid := db.newSession("sid", "alice")
		db.failOn("COALESCE(totp_last_counter, 0)", errors.New("pq: connection reset"))
		rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, nil)
		if next.called || rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status %d, reached handler %v; want 503", rec.Code, next.called)
		}
	})
	t.Run("role lookup fails", func(t *testing.T) {
		h, db := newDBHandler(t)
		db.addUser("alice")
		sid := db.newSession("sid", "alice", onFractal(opsFractalID))
		db.failOn("FROM fractal_permissions", errors.New("pq: connection reset"))
		rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, nil)
		if next.called || rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status %d, reached handler %v; want 503", rec.Code, next.called)
		}
	})
}

// A session past the password but not the second factor authorizes nothing but
// finishing the login, and is refused before any scope is resolved.
func TestAuthMiddlewareMFAPending(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.grantFractal(opsFractalID, "alice", "admin")
	sid := db.newSession("sid", "alice", onFractal(opsFractalID), mfaPending)

	for _, path := range []string{"/api/v1/query", "/api/v1/auth/change-password", "/api/v1/auth/mfa/disable", "/api/v1/fractals"} {
		rec, next := throughMiddleware(h, http.MethodGet, path, sid, map[string]string{ScopeHeader: "fractal:" + labFractalID})
		if next.called || rec.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, reached handler %v; want 403", path, rec.Code, next.called)
		}
	}
	if n := len(db.pg.Matching("_permissions")); n != 0 {
		t.Errorf("%d permission lookups for a half-authenticated session", n)
	}

	for _, path := range []string{"/api/v1/auth/mfa/verify", "/api/v1/auth/user", "/api/v1/auth/logout"} {
		_, next := throughMiddleware(h, http.MethodPost, path, sid, nil)
		if !next.called {
			t.Errorf("%s unreachable for a pending session", path)
			continue
		}
		if pending, _ := next.ctx.Value("mfa_pending").(bool); !pending {
			t.Errorf("%s: mfa_pending not carried into the context", path)
		}
	}
}

// The scope header lets a tab hold its own scope, but only one the user may see.
func TestAuthMiddlewareScopeHeader(t *testing.T) {
	setup := func(t *testing.T) (*AuthHandler, *authDB, string) {
		h, db := newDBHandler(t)
		db.addUser("alice")
		db.grantFractal(opsFractalID, "alice", "viewer")
		db.grantFractal(labFractalID, "alice", "admin")
		db.grantPrism(opsPrismID, "alice", "analyst")
		return h, db, db.newSession("sid", "alice", onFractal(opsFractalID))
	}

	t.Run("granted fractal replaces the session scope", func(t *testing.T) {
		h, _, sid := setup(t)
		_, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, map[string]string{ScopeHeader: "fractal:" + labFractalID})
		if !next.called {
			t.Fatal("refused")
		}
		if ctxString(next.ctx, "selected_fractal") != labFractalID || ctxString(next.ctx, "fractal_role") != "admin" {
			t.Errorf("scope %q role %q, want the header's fractal as admin", ctxString(next.ctx, "selected_fractal"), ctxString(next.ctx, "fractal_role"))
		}
	})

	t.Run("granted prism clears the session fractal", func(t *testing.T) {
		h, _, sid := setup(t)
		_, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, map[string]string{ScopeHeader: "prism:" + opsPrismID})
		if !next.called {
			t.Fatal("refused")
		}
		if ctxString(next.ctx, "selected_fractal") != "" || ctxString(next.ctx, "selected_prism") != opsPrismID || ctxString(next.ctx, "prism_role") != "analyst" {
			t.Errorf("fractal %q prism %q role %q", ctxString(next.ctx, "selected_fractal"), ctxString(next.ctx, "selected_prism"), ctxString(next.ctx, "prism_role"))
		}
	})

	t.Run("ungranted fractal", func(t *testing.T) {
		h, _, sid := setup(t)
		rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, map[string]string{ScopeHeader: "fractal:" + defaultFractalID})
		if next.called || rec.Code != http.StatusForbidden || rec.Header().Get(ScopeInvalidHeader) != "1" {
			t.Errorf("status %d, invalid header %q, reached %v; want 403 marked invalid", rec.Code, rec.Header().Get(ScopeInvalidHeader), next.called)
		}
	})

	t.Run("deleted fractal", func(t *testing.T) {
		h, _, sid := setup(t)
		rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, map[string]string{ScopeHeader: "fractal:" + otherScopeID})
		if next.called || rec.Code != http.StatusForbidden {
			t.Errorf("status %d, reached %v; want 403", rec.Code, next.called)
		}
	})

	t.Run("malformed", func(t *testing.T) {
		h, _, sid := setup(t)
		rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, map[string]string{ScopeHeader: "fractal:" + labFractalID + "x"})
		if next.called || rec.Code != http.StatusBadRequest || rec.Header().Get(ScopeInvalidHeader) != "1" {
			t.Errorf("status %d, reached %v; want 400 marked invalid", rec.Code, next.called)
		}
	})

	t.Run("lookup fails", func(t *testing.T) {
		h, db, sid := setup(t)
		db.failOn("FROM prism_permissions", errors.New("pq: connection reset"))
		rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, map[string]string{ScopeHeader: "prism:" + opsPrismID})
		if next.called || rec.Code != http.StatusServiceUnavailable || rec.Header().Get(ScopeInvalidHeader) != "" {
			t.Errorf("status %d, invalid %q; want a plain 503", rec.Code, rec.Header().Get(ScopeInvalidHeader))
		}
	})

	// "none" drops the session scope and forbids a default in its place.
	t.Run("none", func(t *testing.T) {
		h, _, sid := setup(t)
		_, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", sid, map[string]string{ScopeHeader: ScopeNone})
		if !next.called {
			t.Fatal("refused")
		}
		if ctxString(next.ctx, "selected_fractal") != "" || ctxString(next.ctx, "selected_prism") != "" {
			t.Error("session scope survived an explicit none")
		}
		if !fractals.NoScopeDeclared(next.ctx) {
			t.Error("no-scope marker missing")
		}
		if _, ok := next.ctx.Value("fractal_role").(string); ok {
			t.Error("a role was resolved for no scope")
		}
	})

	// Recovery paths ignore the header so a stale tab can pick a new scope.
	t.Run("exempt path keeps the session scope", func(t *testing.T) {
		h, _, sid := setup(t)
		_, next := throughMiddleware(h, http.MethodGet, "/api/v1/fractals", sid, map[string]string{ScopeHeader: "fractal:" + defaultFractalID})
		if !next.called {
			t.Fatal("exempt path refused")
		}
		if ctxString(next.ctx, "selected_fractal") != opsFractalID || ctxString(next.ctx, "fractal_role") != "viewer" {
			t.Errorf("scope %q role %q, want the session's", ctxString(next.ctx, "selected_fractal"), ctxString(next.ctx, "fractal_role"))
		}
	})
}

// A user who must change their password reaches only what the change needs.
func TestAuthMiddlewareForcedPasswordChange(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("admin", asAdmin, mustChange)
	sid := db.newSession("sid", "admin")

	for _, path := range []string{"/api/v1/query", "/api/v1/users", "/api/v1/auth/mfa/enroll"} {
		rec, next := throughMiddleware(h, http.MethodGet, path, sid, nil)
		if next.called || rec.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, reached %v; want 403", path, rec.Code, next.called)
		}
	}
	for _, path := range []string{"/api/v1/auth/change-password", "/api/v1/auth/user", "/api/v1/auth/logout"} {
		if _, next := throughMiddleware(h, http.MethodPost, path, sid, nil); !next.called {
			t.Errorf("%s unreachable while a change is required", path)
		}
	}
}

// With two-factor required, an unenrolled local user reaches only enrollment.
func TestAuthMiddlewareRequireMFA(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	setRequireMFA(t, true)

	h, db := newDBHandler(t)
	db.addUser("alice")
	db.addUser("sso", viaSSO)
	db.addUser("enrolled", func(r row) { r["totp_secret"] = "sealed"; r["totp_enrolled_at"] = db.user("alice")["created_at"] })
	db.addUser("changer", mustChange)
	for _, u := range []string{"alice", "sso", "enrolled", "changer"} {
		db.newSession("sid-"+u, u)
	}

	gated := func(user, path string) bool {
		rec, next := throughMiddleware(h, http.MethodGet, path, "sid-"+user, nil)
		if !next.called && rec.Code != http.StatusForbidden {
			t.Errorf("%s %s: status %d", user, path, rec.Code)
		}
		return !next.called
	}

	if !gated("alice", "/api/v1/query") {
		t.Error("unenrolled user reached the API with two-factor required")
	}
	for path := range mfaEnrollPaths {
		if gated("alice", path) {
			t.Errorf("unenrolled user cannot reach %s", path)
		}
	}
	if gated("sso", "/api/v1/query") {
		t.Error("SSO user gated on a local authenticator")
	}
	if gated("enrolled", "/api/v1/query") {
		t.Error("enrolled user gated")
	}
	// The pending password change wins, or the user could reach neither step.
	if gated("changer", "/api/v1/auth/change-password") {
		t.Error("user who must change their password cannot reach the change endpoint")
	}

	// Without the pepper nobody can enroll, so the requirement must not lock
	// every local account out.
	t.Setenv("BIFRACT_PASSWORD_PEPPER", "")
	if gated("alice", "/api/v1/query") {
		t.Error("requirement enforced with no key to enroll against")
	}
}

func TestSessionUserWithDatabase(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.addUser("off", disabled)
	db.addUser("changer", mustChange)
	db.newSession("sid-alice", "alice")
	db.newSession("sid-off", "off")
	db.newSession("sid-changer", "changer")
	db.newSession("sid-ghost", "ghost")

	resolve := func(sid string) (*storage.User, error) {
		r := httptest.NewRequest(http.MethodGet, "/search", nil)
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sid})
		return h.SessionUser(r)
	}

	if u, err := resolve("sid-alice"); err != nil || u == nil || u.Username != "alice" {
		t.Errorf("active user: got (%v, %v)", u, err)
	}
	for _, sid := range []string{"sid-off", "sid-changer", "sid-ghost"} {
		if u, err := resolve(sid); u != nil || err != nil {
			t.Errorf("%s: got (%v, %v), want (nil, nil)", sid, u, err)
		}
	}

	db.failOn("COALESCE(totp_last_counter, 0)", errors.New("pq: connection reset"))
	if u, err := resolve("sid-alice"); u != nil || err == nil {
		t.Errorf("lookup failure: got (%v, %v), want an error", u, err)
	}
}

// A user who must change their password and also has an authenticator signs in
// to a pending session. They must be able to finish the second factor first,
// or the password change, which needs a verified session, is out of reach.
func TestAuthMiddlewareMFAPendingWithForcedPasswordChange(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("admin", asAdmin, mustChange)
	sid := db.newSession("sid", "admin", mfaPending)

	if _, next := throughMiddleware(h, http.MethodPost, "/api/v1/auth/mfa/verify", sid, nil); !next.called {
		t.Error("mfa/verify unreachable for a pending session that must also change its password")
	}
	// The change itself still waits for the second factor.
	for _, path := range []string{"/api/v1/auth/change-password", "/api/v1/query"} {
		if rec, next := throughMiddleware(h, http.MethodPost, path, sid, nil); next.called || rec.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, reached %v; want 403 before the second factor", path, rec.Code, next.called)
		}
	}

	// Once verified, the forced change gate applies as usual: verify is no
	// longer an exemption, and the change endpoint opens.
	full := db.newSession("full", "admin")
	if rec, next := throughMiddleware(h, http.MethodPost, "/api/v1/auth/mfa/verify", full, nil); next.called || rec.Code != http.StatusForbidden {
		t.Errorf("mfa/verify on a full session: status %d, reached %v; want 403 while a change is required", rec.Code, next.called)
	}
	if _, next := throughMiddleware(h, http.MethodPost, "/api/v1/auth/change-password", full, nil); !next.called {
		t.Error("change-password unreachable after verification")
	}
}
