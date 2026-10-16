package auth

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func TestHandleLoginSuccess(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	t.Setenv("BIFRACT_SECURE_COOKIES", "true")
	h, db := newDBHandler(t)
	db.addUser("alice", asAdmin)

	// Earlier failures at this account are forgiven by a correct password.
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

	// The account's count was reset: a full budget of failures is needed to
	// block it again.
	for i := 0; i < loginMaxFailures-1; i++ {
		login(h, "alice", "wrong", "203.0.113.9")
	}
	if h.accountLimiter.check("alice") {
		t.Error("account blocked before a fresh run of failures")
	}
	// The address's count was not: a success never forgives an address.
	h.loginLimiter.mu.Lock()
	ipFailures := h.loginLimiter.attempts["203.0.113.9"].failures
	h.loginLimiter.mu.Unlock()
	if ipFailures != 2*(loginMaxFailures-1) {
		t.Errorf("address failures = %d, want %d carried across the success", ipFailures, 2*(loginMaxFailures-1))
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

// One address spraying usernames is blocked by the address limit.
func TestHandleLoginBlocksAfterRepeatedFailures(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")

	for i := 0; i < loginIPMaxFailures; i++ {
		login(h, fmt.Sprintf("user%d", i), "guess", "198.51.100.8")
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

// A client connecting directly cannot escape the per-address limit by sending a
// fresh X-Forwarded-For with each guess: only a trusted proxy's header counts.
func TestHandleLoginForwardedForSpoofing(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")

	guess := func(peer, xff string) int {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login",
			strings.NewReader(fmt.Sprintf(`{"username":%q,"password":"wrong"}`, "nobody")))
		r.RemoteAddr = peer + ":40000"
		r.Header.Set("X-Forwarded-For", xff)
		rec := httptest.NewRecorder()
		h.HandleLogin(rec, r)
		return rec.Code
	}

	blocked := false
	for i := 0; i < 50 && !blocked; i++ {
		blocked = guess("203.0.113.50", fmt.Sprintf("198.51.100.%d", i)) == http.StatusTooManyRequests
	}
	if !blocked {
		t.Fatal("rotating X-Forwarded-For from an untrusted peer never hit the limit")
	}

	// Behind the bundled proxy the header is the client, so two clients
	// behind it are limited separately.
	for i := 0; i < 50; i++ {
		if guess("172.18.0.5", "198.51.100.200") == http.StatusTooManyRequests {
			break
		}
	}
	if code := guess("172.18.0.5", "198.51.100.201"); code == http.StatusTooManyRequests {
		t.Error("a client behind the proxy was blocked by another client's failures")
	}
}

// Guesses at one account are limited however many addresses they come from, and
// the block lands on the account, not on the people sharing an address.
func TestHandleLoginBlocksAccountAcrossAddresses(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.addUser("bob")

	for i := 0; i < loginMaxFailures; i++ {
		if rec := login(h, "alice", "guess", fmt.Sprintf("198.51.100.%d", i+1)); rec.Code != http.StatusUnauthorized {
			t.Fatalf("guess %d: status %d", i+1, rec.Code)
		}
	}
	rec := login(h, "alice", userPassword, "192.0.2.77")
	if rec.Code != http.StatusTooManyRequests || sessionCookie(rec) != nil {
		t.Fatalf("right password on a blocked account from a fresh address: status %d", rec.Code)
	}
	// Another account behind one of the same addresses is unaffected.
	if rec := login(h, "bob", userPassword, "198.51.100.1"); rec.Code != http.StatusOK {
		t.Errorf("bob: status %d, want 200", rec.Code)
	}

	h.accountLimiter.mu.Lock()
	a := h.accountLimiter.attempts["alice"]
	h.accountLimiter.mu.Unlock()
	if d := time.Until(a.blockedUntil); d <= loginAccountBlockDuration-time.Minute || d > loginAccountBlockDuration {
		t.Errorf("account block lasts %v, want about %v", d, loginAccountBlockDuration)
	}
	// Once the block lapses the right password works.
	h.accountLimiter.mu.Lock()
	a.blockedUntil = time.Now().Add(-time.Second)
	h.accountLimiter.mu.Unlock()
	if rec := login(h, "alice", userPassword, "192.0.2.77"); rec.Code != http.StatusOK {
		t.Errorf("after the block: status %d, want 200", rec.Code)
	}
}

// Guessing at an account blocks the guessers, not its owner signing in from
// where they signed in before.
func TestHandleLoginAccountBlockSparesKnownAddress(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("admin", asAdmin)

	if rec := login(h, "admin", userPassword, "192.0.2.10"); rec.Code != http.StatusOK {
		t.Fatalf("first login: status %d", rec.Code)
	}
	for i := 0; i < loginMaxFailures; i++ {
		login(h, "admin", "guess", fmt.Sprintf("198.51.100.%d", i+1))
	}
	if rec := login(h, "admin", userPassword, "198.51.100.1"); rec.Code != http.StatusTooManyRequests {
		t.Errorf("right password from a guessing address: status %d, want 429", rec.Code)
	}
	if rec := login(h, "admin", "guess", "192.0.2.10"); rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong password from the known address: status %d, want 401", rec.Code)
	}
	if rec := login(h, "admin", userPassword, "192.0.2.10"); rec.Code != http.StatusOK {
		t.Errorf("owner at the known address: status %d, want 200", rec.Code)
	}
}

// Someone with an account of their own cannot keep guessing at another account
// by logging in successfully between guesses: a success forgives only the
// account that succeeded, never the address.
func TestHandleLoginSuccessDoesNotResetAddress(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("mallory")
	for i := 0; i < loginIPMaxFailures; i++ {
		db.addUser(fmt.Sprintf("victim%d", i))
	}

	const ip = "198.51.100.66"
	blocked := false
	for i := 0; i < 3*loginIPMaxFailures && !blocked; i++ {
		// Spread over many victims so no single account limit is reached.
		victim := fmt.Sprintf("victim%d", i%loginIPMaxFailures)
		blocked = login(h, victim, "guess", ip).Code == http.StatusTooManyRequests
		if !blocked && i%2 == 1 {
			login(h, "mallory", userPassword, ip)
		}
	}
	if !blocked {
		t.Fatal("interleaved successful logins kept the address under its limit")
	}
}

// Unknown usernames count against the address only, so garbage names cannot
// grow the per-account table.
func TestHandleLoginUnknownUsersNotTrackedPerAccount(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, _ := newDBHandler(t)
	for i := 0; i < 10; i++ {
		login(h, fmt.Sprintf("nobody%d", i), "guess", "198.51.100.70")
	}
	h.accountLimiter.mu.Lock()
	n := len(h.accountLimiter.attempts)
	h.accountLimiter.mu.Unlock()
	if n != 0 {
		t.Errorf("%d per-account entries for unknown usernames", n)
	}
}
