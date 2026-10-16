package auth

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func TestHandleLoginSuccess(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	t.Setenv("BIFRACT_SECURE_COOKIES", "true")
	h, db := newDBHandler(t)
	db.addUser("alice", asAdmin)

	// Earlier failures from this address are forgiven by a correct password.
	for i := 0; i < loginMaxFailures-1; i++ {
		if rec := login(h, "alice", "wrong", "203.0.113.9"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("wrong password: status %d", rec.Code)
		}
	}

	rec := login(h, "alice", userPassword, "203.0.113.9")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	c := sessionCookie(rec)
	if c == nil || c.Value == "" {
		t.Fatal("no session cookie")
	}
	if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteStrictMode || c.Path != "/" || c.MaxAge != int(sessionDuration.Seconds()) {
		t.Errorf("cookie = %+v, want HttpOnly, Secure, SameSite=Strict, Path=/, MaxAge=%d", c, int(sessionDuration.Seconds()))
	}

	stored := db.rows("sessions", row{"session_id": c.Value})
	if len(stored) != 1 {
		t.Fatalf("%d sessions stored under the cookie", len(stored))
	}
	s := stored[0]
	if s["username"] != "alice" || s["mfa_pending"] != false || s["selected_fractal"] != defaultFractalID {
		t.Errorf("session = %v, want alice, not pending, on the default fractal", s)
	}
	if exp := s["expires_at"].(time.Time); time.Until(exp) < sessionDuration-time.Minute || time.Until(exp) > sessionDuration {
		t.Errorf("session expires in %v, want %v", time.Until(exp), sessionDuration)
	}
	if db.user("alice")["last_login"] == nil {
		t.Error("last_login not recorded")
	}
	body := decodeBody(t, rec)
	user := body["user"].(map[string]any)
	if user["username"] != "alice" || user["is_admin"] != true || user["mfa_required"] != nil || user["force_password_change"] != nil {
		t.Errorf("user = %v", user)
	}

	// The failure count was reset: a full budget of failures is needed to block again.
	for i := 0; i < loginMaxFailures-1; i++ {
		login(h, "alice", "wrong", "203.0.113.9")
	}
	if h.loginLimiter.check("203.0.113.9") {
		t.Error("address blocked before a fresh run of failures")
	}

	// The cookie authenticates.
	if _, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", c.Value, nil); !next.called {
		t.Error("the new session does not authenticate")
	}
}

// A user with an authenticator gets a short-lived session that is good for the
// code and nothing else, and has not logged in yet.
func TestHandleLoginWithAuthenticator(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice", func(r row) { r["totp_secret"] = "sealed"; r["totp_enrolled_at"] = time.Now() })

	rec := login(h, "alice", userPassword, "203.0.113.9")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	c := sessionCookie(rec)
	if c == nil || c.MaxAge != int(mfaPendingDuration.Seconds()) {
		t.Fatalf("cookie = %+v, want MaxAge %d", c, int(mfaPendingDuration.Seconds()))
	}
	s := db.rows("sessions", row{"session_id": c.Value})[0]
	if s["mfa_pending"] != true {
		t.Error("session not marked pending")
	}
	if exp := s["expires_at"].(time.Time); time.Until(exp) > mfaPendingDuration {
		t.Errorf("pending session lives %v, longer than %v", time.Until(exp), mfaPendingDuration)
	}
	if db.user("alice")["last_login"] != nil {
		t.Error("last_login recorded before the second factor")
	}
	if user := decodeBody(t, rec)["user"].(map[string]any); user["mfa_required"] != true {
		t.Errorf("user = %v, want mfa_required", user)
	}
	if rec, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", c.Value, nil); next.called || rec.Code != http.StatusForbidden {
		t.Errorf("pending session reached the API: %d", rec.Code)
	}
}

func TestHandleLoginFlags(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	setRequireMFA(t, true)
	h, db := newDBHandler(t)
	db.addUser("admin", asAdmin, mustChange)
	db.addUser("alice")

	user := decodeBody(t, login(h, "admin", userPassword, "203.0.113.1"))["user"].(map[string]any)
	if user["force_password_change"] != true {
		t.Errorf("admin: %v, want force_password_change", user)
	}
	user = decodeBody(t, login(h, "alice", userPassword, "203.0.113.2"))["user"].(map[string]any)
	if user["mfa_enrollment_required"] != true {
		t.Errorf("alice: %v, want mfa_enrollment_required", user)
	}
}

// Refusals create no session.
func TestHandleLoginRefusals(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	cases := []struct {
		name     string
		mod      func(row)
		username string
		password string
		want     int
		counted  bool
	}{
		{name: "wrong password", password: "nope", want: http.StatusUnauthorized, counted: true},
		{name: "unknown user", username: "mallory", password: userPassword, want: http.StatusUnauthorized, counted: true},
		{name: "disabled", mod: disabled, password: userPassword, want: http.StatusForbidden},
		{name: "sso account", mod: viaSSO, password: userPassword, want: http.StatusBadRequest},
		{name: "pending invite", mod: func(r row) { r["password_hash"] = "!invite" }, password: "!invite", want: http.StatusBadRequest},
		// Hashed before the pepper existed: the plain password no longer verifies.
		{name: "pepper changed", mod: func(r row) { r["password_hash"] = mustHash(t, userPassword) }, password: userPassword, want: http.StatusUnauthorized, counted: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, db := newDBHandler(t)
			mods := []func(row){}
			if tc.mod != nil {
				mods = append(mods, tc.mod)
			}
			db.addUser("alice", mods...)
			username := tc.username
			if username == "" {
				username = "alice"
			}
			rec := login(h, username, tc.password, "198.51.100.7")
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
			if sessionCookie(rec) != nil || len(db.rows("sessions", nil)) != 0 {
				t.Error("a refused login created a session")
			}
			h.loginLimiter.mu.Lock()
			a := h.loginLimiter.attempts["198.51.100.7"]
			h.loginLimiter.mu.Unlock()
			if counted := a != nil && a.failures == 1; counted != tc.counted {
				t.Errorf("failure counted = %v, want %v", counted, tc.counted)
			}
		})
	}
}

// mustHash bcrypts a password with no pepper.
func mustHash(t *testing.T, password string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

func TestHandleLoginBlocksAfterRepeatedFailures(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")

	for i := 0; i < loginMaxFailures; i++ {
		login(h, "alice", "guess", "198.51.100.8")
	}
	// The right password no longer helps from that address, and costs no lookup.
	before := len(db.pg.Matching("FROM users"))
	if rec := login(h, "alice", userPassword, "198.51.100.8"); rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
	if after := len(db.pg.Matching("FROM users")); after != before {
		t.Error("a blocked address still reached the user table")
	}
	if rec := login(h, "alice", userPassword, "198.51.100.9"); rec.Code != http.StatusOK {
		t.Errorf("another address: status %d, want 200", rec.Code)
	}
}

// A session that cannot be stored is a failed login, not a cookie that bounces
// straight back to the login page.
func TestHandleLoginSessionStoreFailure(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.failOn("INSERT INTO sessions", errors.New("pq: disk full"))

	rec := login(h, "alice", userPassword, "203.0.113.9")
	if rec.Code != http.StatusInternalServerError || sessionCookie(rec) != nil {
		t.Errorf("status %d, cookie %v; want 500 and no cookie", rec.Code, sessionCookie(rec))
	}
}

// A non-admin's first fractal is the default only when they can see it;
// otherwise one they can see, otherwise none.
func TestLoginSelectsAnAccessibleFractal(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	cases := []struct {
		name  string
		grant func(db *authDB)
		want  any
	}{
		{"granted the default", func(db *authDB) { db.grantFractal(defaultFractalID, "alice", "viewer") }, defaultFractalID},
		{"granted another", func(db *authDB) { db.grantFractal(labFractalID, "alice", "viewer") }, labFractalID},
		{"granted through a group", func(db *authDB) {
			db.joinGroup("g", "alice")
			db.grantFractal(opsFractalID, "group:g", "analyst")
		}, opsFractalID},
		{"granted nothing", func(db *authDB) {}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, db := newDBHandler(t)
			db.addUser("alice")
			tc.grant(db)
			c := sessionCookie(login(h, "alice", userPassword, "203.0.113.9"))
			if c == nil {
				t.Fatal("login failed")
			}
			if got := db.rows("sessions", row{"session_id": c.Value})[0]["selected_fractal"]; got != tc.want {
				t.Errorf("selected_fractal = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCreateSessionForUser(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("sso", viaSSO, asAdmin)
	sid, err := h.CreateSessionForUser("sso")
	if err != nil {
		t.Fatal(err)
	}
	s := db.rows("sessions", row{"session_id": sid})
	if len(s) != 1 || s[0]["username"] != "sso" || s[0]["mfa_pending"] != false {
		t.Errorf("sessions = %v", s)
	}
}

func TestNewAuthHandlerWiring(t *testing.T) {
	t.Setenv("BIFRACT_SECURE_COOKIES", "")
	h, _ := newDBHandler(t)
	if h.systemFractalID != systemFractalID {
		t.Errorf("system fractal = %q, want %q", h.systemFractalID, systemFractalID)
	}
	if h.IsSecureCookies() {
		t.Error("secure cookies on without BIFRACT_SECURE_COOKIES=true")
	}
	if h.RBACResolver() == nil {
		t.Error("no RBAC resolver")
	}
	// Auth events need ClickHouse; without it logging is a no-op, not a panic.
	h.LogAuthEvent("login_success", "alice", "203.0.113.9", "")
}
