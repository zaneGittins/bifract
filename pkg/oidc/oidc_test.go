package oidc

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

func TestLoadConfigFromEnv(t *testing.T) {
	vars := []string{
		"BIFRACT_OIDC_ISSUER_URL", "BIFRACT_OIDC_CLIENT_ID", "BIFRACT_OIDC_CLIENT_SECRET",
		"BIFRACT_OIDC_REDIRECT_URL", "BIFRACT_OIDC_SCOPES", "BIFRACT_OIDC_DEFAULT_ROLE",
		"BIFRACT_OIDC_ALLOWED_DOMAINS", "BIFRACT_OIDC_BUTTON_TEXT", "BIFRACT_BASE_URL", "BIFRACT_DOMAIN",
	}
	base := map[string]string{"BIFRACT_OIDC_ISSUER_URL": "https://idp.example", "BIFRACT_OIDC_CLIENT_ID": "cid"}
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	tests := []struct {
		name  string
		env   map[string]string
		check func(t *testing.T, c *Config)
	}{
		// OIDC stays off unless both the issuer and the client are named.
		{name: "nothing set", env: nil, check: wantNil},
		{name: "issuer only", env: map[string]string{"BIFRACT_OIDC_ISSUER_URL": "https://idp.example"}, check: wantNil},
		{name: "client only", env: map[string]string{"BIFRACT_OIDC_CLIENT_ID": "cid"}, check: wantNil},
		{name: "defaults", env: base, check: func(t *testing.T, c *Config) {
			if c.IssuerURL != "https://idp.example" || c.ClientID != "cid" || c.ClientSecret != "" {
				t.Errorf("config = %+v", c)
			}
			if strings.Join(c.Scopes, " ") != "openid profile email" {
				t.Errorf("scopes = %q", c.Scopes)
			}
			// New SSO users are not administrators unless configured so.
			if c.DefaultRole != "user" {
				t.Errorf("default role = %q, want user", c.DefaultRole)
			}
			if c.AllowedDomains != nil {
				t.Errorf("allowed domains = %q, want none", c.AllowedDomains)
			}
			if c.RedirectURL != "http://localhost:8080/api/v1/auth/oidc/callback" {
				t.Errorf("redirect = %q", c.RedirectURL)
			}
			if c.ButtonText != "Sign in with SSO" {
				t.Errorf("button = %q", c.ButtonText)
			}
		}},
		{name: "explicit values", env: with(map[string]string{
			"BIFRACT_OIDC_CLIENT_SECRET":   "s3cret",
			"BIFRACT_OIDC_REDIRECT_URL":    "https://bifract.example/cb",
			"BIFRACT_OIDC_SCOPES":          " openid , email,groups ",
			"BIFRACT_OIDC_DEFAULT_ROLE":    "admin",
			"BIFRACT_OIDC_ALLOWED_DOMAINS": " Example.COM, ,corp.example ,",
			"BIFRACT_OIDC_BUTTON_TEXT":     "Okta",
			"BIFRACT_BASE_URL":             "https://ignored.example",
		}), check: func(t *testing.T, c *Config) {
			if c.ClientSecret != "s3cret" || c.RedirectURL != "https://bifract.example/cb" || c.DefaultRole != "admin" || c.ButtonText != "Okta" {
				t.Errorf("config = %+v", c)
			}
			if fmt.Sprint(c.Scopes) != "[openid email groups]" {
				t.Errorf("scopes = %q", c.Scopes)
			}
			// Domains compare lowercase against the email's domain; blanks are dropped
			// so a trailing comma cannot allow an empty domain.
			if fmt.Sprint(c.AllowedDomains) != "[example.com corp.example]" {
				t.Errorf("domains = %q", c.AllowedDomains)
			}
		}},
		{name: "redirect from base URL", env: with(map[string]string{"BIFRACT_BASE_URL": "https://bifract.example/", "BIFRACT_DOMAIN": "ignored.example"}), check: func(t *testing.T, c *Config) {
			if c.RedirectURL != "https://bifract.example/api/v1/auth/oidc/callback" {
				t.Errorf("redirect = %q", c.RedirectURL)
			}
		}},
		{name: "redirect from domain", env: with(map[string]string{"BIFRACT_DOMAIN": "logs.example"}), check: func(t *testing.T, c *Config) {
			if c.RedirectURL != "https://logs.example/api/v1/auth/oidc/callback" {
				t.Errorf("redirect = %q", c.RedirectURL)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, v := range vars {
				t.Setenv(v, tt.env[v])
			}
			tt.check(t, LoadConfigFromEnv())
		})
	}
}

func wantNil(t *testing.T, c *Config) {
	t.Helper()
	if c != nil {
		t.Fatalf("config = %+v, want nil (OIDC disabled)", c)
	}
}

func TestDeriveUsername(t *testing.T) {
	long := strings.Repeat("x", 60)
	tests := []struct {
		name, email, preferred, sub, want string
	}{
		{name: "email local part, lowercased", email: "Alice.Smith@Example.com", preferred: "asmith", sub: "s", want: "alice.smith"},
		{name: "preferred username without email", preferred: "BobX", sub: "s", want: "bobx"},
		{name: "subject as last resort", sub: "AUTH0|123", want: "auth0|123"},
		{name: "email with empty local part falls back to subject", email: "@example.com", preferred: "p", sub: "sub1", want: "sub1"},
		{name: "email without at sign is all local part", email: "carol", sub: "s", want: "carol"},
		{name: "truncated to the column width", email: long + "@x.com", sub: "s", want: long[:50]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deriveUsername(tt.email, tt.preferred, tt.sub); got != tt.want {
				t.Errorf("deriveUsername = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStateIsSingleUseAndExpires(t *testing.T) {
	h := &Handler{states: map[string]stateEntry{}}
	hexRe := regexp.MustCompile(`^[0-9a-f]{64}$`)

	s1, n1, err := h.generateState()
	if err != nil {
		t.Fatal(err)
	}
	s2, n2, _ := h.generateState()
	if !hexRe.MatchString(s1) || !hexRe.MatchString(n1) {
		t.Fatalf("state %q / nonce %q are not 32 random bytes", s1, n1)
	}
	if s1 == s2 || n1 == n2 || s1 == n1 {
		t.Fatal("state and nonce values repeat")
	}

	if nonce, ok := h.consumeState(s1); !ok || nonce != n1 {
		t.Fatalf("consumeState = (%q, %v), want (%q, true)", nonce, ok, n1)
	}
	// Replaying a state, or presenting one never issued, fails.
	for _, s := range []string{s1, "", "deadbeef"} {
		if _, ok := h.consumeState(s); ok {
			t.Errorf("state %q accepted", s)
		}
	}

	h.states[s2] = stateEntry{nonce: n2, expiresAt: time.Now().Add(-time.Second)}
	if _, ok := h.consumeState(s2); ok {
		t.Fatal("expired state accepted")
	}
	if len(h.states) != 0 {
		t.Fatalf("%d states left behind", len(h.states))
	}
}

// userRec is a users row as the fake database holds it.
type userRec struct {
	username string
	enabled  bool
}

type harness struct {
	t   *testing.T
	idp *fakeIdP
	h   *Handler
	pg  *fakedb.PG

	mu                   sync.Mutex
	subjects             map[string]userRec // oidc_subject -> user
	taken                map[string]bool    // usernames already in users
	lookupErr, createErr error
	sessErr              error
	sessions             []string
	events               []string
}

func newHarness(t *testing.T, configure func(*Config)) *harness {
	t.Helper()
	idp := newFakeIdP(t)
	client, pg := storagetest.Postgres()
	hs := &harness{t: t, idp: idp, pg: pg, subjects: map[string]userRec{}, taken: map[string]bool{}}
	pg.Respond(hs.respond)

	cfg := &Config{
		IssuerURL:   idp.URL,
		ClientID:    testClientID,
		RedirectURL: "https://bifract.example/api/v1/auth/oidc/callback",
		Scopes:      []string{"openid", "profile", "email"},
		DefaultRole: "user",
		ButtonText:  "SSO",
	}
	if configure != nil {
		configure(cfg)
	}
	h, err := NewHandler(cfg, client, hs.createSession, hs.logEvent, true)
	if err != nil {
		t.Fatal(err)
	}
	hs.h = h
	return hs
}

func (hs *harness) createSession(username string) (string, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.sessErr != nil {
		return "", hs.sessErr
	}
	hs.sessions = append(hs.sessions, username)
	return "session-for-" + username, nil
}

func (hs *harness) logEvent(event, user, _, detail string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.events = append(hs.events, strings.TrimSpace(event+" "+user+" "+detail))
}

func (hs *harness) respond(q string, args []driver.Value) (fakedb.PGResult, bool) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	arg := func(i int) string { s, _ := args[i].(string); return s }
	now := time.Now()
	switch {
	case strings.Contains(q, "WHERE oidc_subject = $1"):
		if hs.lookupErr != nil {
			return fakedb.PGResult{Err: hs.lookupErr}, true
		}
		u, ok := hs.subjects[arg(0)]
		if !ok {
			return fakedb.PGResult{}, true
		}
		return fakedb.Row(u.username, u.username, "#fff", "A", now, nil, false, "oidc", arg(0), u.enabled), true
	case strings.Contains(q, "password_hash") && strings.Contains(q, "WHERE username = $1"):
		if !hs.taken[arg(0)] {
			return fakedb.PGResult{}, true
		}
		return fakedb.Row(arg(0), nil, arg(0), "#fff", "A", now, nil, false, "local", false, true, "UTC", "", false, int64(0)), true
	case strings.Contains(q, "INSERT INTO users"):
		if hs.createErr != nil {
			return fakedb.PGResult{Err: hs.createErr}, true
		}
		return fakedb.Affected(1), true
	}
	return fakedb.PGResult{}, false
}

// login runs HandleLogin and returns the state and nonce it sent to the IdP.
func (hs *harness) login() (state, nonce string) {
	hs.t.Helper()
	rec := httptest.NewRecorder()
	hs.h.HandleLogin(rec, httptest.NewRequest(http.MethodGet, "/api/v1/auth/oidc/login", nil))
	if rec.Code != http.StatusFound {
		hs.t.Fatalf("login status = %d", rec.Code)
	}
	loc, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		hs.t.Fatal(err)
	}
	return loc.Query().Get("state"), loc.Query().Get("nonce")
}

func (hs *harness) callback(query url.Values) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/auth/oidc/callback?"+query.Encode(), nil)
	r.RemoteAddr = "192.0.2.1:5555"
	hs.h.HandleCallback(rec, r)
	return rec
}

// signIn runs a whole login with the claims edit applied to a valid claim set.
func (hs *harness) signIn(edit func(claims map[string]any)) *httptest.ResponseRecorder {
	hs.t.Helper()
	state, nonce := hs.login()
	claims := hs.idp.claims(nonce)
	if edit != nil {
		edit(claims)
	}
	hs.idp.issue(claims, hs.idp.key)
	return hs.callback(url.Values{"state": {state}, "code": {"code-1"}})
}

func redirectedTo(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", rec.Code)
	}
	return rec.Header().Get("Location")
}

func sessionCookie(rec *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == "bifract_session" {
			return c
		}
	}
	return nil
}

func (hs *harness) inserts() []fakedb.Call { return hs.pg.Matching("INSERT INTO users") }

func TestHandleConfig(t *testing.T) {
	h := &Handler{config: &Config{ButtonText: "Sign in with Okta"}}
	rec := httptest.NewRecorder()
	h.HandleConfig(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := strings.TrimSpace(rec.Body.String()); got != `{"button_text":"Sign in with Okta","enabled":true}` {
		t.Fatalf("body = %s", got)
	}
}

func TestNewHandlerFailsClosed(t *testing.T) {
	// An issuer that does not answer leaves OIDC off rather than half-configured.
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	if _, err := NewHandler(&Config{IssuerURL: dead.URL, ClientID: "c"}, nil, nil, nil, true); err == nil {
		t.Error("unreachable issuer accepted")
	}
	// Discovery that names a different issuer is refused (mix-up protection).
	idp := newFakeIdP(t)
	if _, err := NewHandler(&Config{IssuerURL: idp.URL + "/other", ClientID: "c"}, nil, nil, nil, true); err == nil {
		t.Error("issuer mismatch accepted")
	}
}

func TestLoginRedirect(t *testing.T) {
	hs := newHarness(t, nil)
	rec := httptest.NewRecorder()
	hs.h.HandleLogin(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	loc, err := url.Parse(redirectedTo(t, rec))
	if err != nil {
		t.Fatal(err)
	}
	if got := loc.Scheme + "://" + loc.Host + loc.Path; got != hs.idp.URL+"/authorize" {
		t.Errorf("redirected to %q", got)
	}
	q := loc.Query()
	for k, want := range map[string]string{
		"client_id":     testClientID,
		"response_type": "code",
		"redirect_uri":  "https://bifract.example/api/v1/auth/oidc/callback",
		"scope":         "openid profile email",
	} {
		if q.Get(k) != want {
			t.Errorf("%s = %q, want %q", k, q.Get(k), want)
		}
	}
	if q.Get("state") == "" || q.Get("nonce") == "" || q.Get("state") == q.Get("nonce") {
		t.Errorf("state %q nonce %q", q.Get("state"), q.Get("nonce"))
	}
	if q.Get("client_secret") != "" {
		t.Error("client secret in the browser redirect")
	}
}

func TestCallbackSignsInExistingUser(t *testing.T) {
	hs := newHarness(t, nil)
	hs.subjects["sub-123"] = userRec{username: "alice", enabled: true}

	rec := hs.signIn(nil)
	if loc := redirectedTo(t, rec); loc != "/" {
		t.Fatalf("redirected to %q: events %v", loc, hs.events)
	}
	c := sessionCookie(rec)
	if c == nil || c.Value != "session-for-alice" {
		t.Fatalf("session cookie = %+v", c)
	}
	// SameSite=Lax is the strictest mode that survives the IdP's cross-site redirect.
	if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || c.MaxAge != 86400 {
		t.Errorf("cookie flags = %+v", c)
	}
	if len(hs.inserts()) != 0 {
		t.Error("existing user was provisioned again")
	}
	if c := hs.pg.Matching("SET last_login = NOW()"); len(c) != 1 || c[0].Args[0] != "alice" {
		t.Errorf("last login update = %+v", c)
	}
	if fmt.Sprint(hs.events) != "[oidc_login_success alice]" {
		t.Errorf("events = %v", hs.events)
	}
	if fmt.Sprint(hs.idp.codes) != "[code-1]" {
		t.Errorf("IdP redeemed %v", hs.idp.codes)
	}
}

func TestCallbackSecureCookieFollowsConfig(t *testing.T) {
	hs := newHarness(t, nil)
	hs.h.secureCookies = false
	hs.subjects["sub-123"] = userRec{username: "alice", enabled: true}
	if c := sessionCookie(hs.signIn(nil)); c == nil || c.Secure {
		t.Fatalf("cookie = %+v, want non-Secure for a plain-HTTP deployment", c)
	}
}

func TestCallbackProvisionsNewUser(t *testing.T) {
	tests := []struct {
		name      string
		role      string
		taken     []string
		edit      func(map[string]any)
		wantUser  string
		wantName  string
		wantAdmin bool
	}{
		{name: "username from email", role: "user", wantUser: "alice", wantName: "Alice Example"},
		// Only the exact value "admin" grants tenant admin to SSO users.
		{name: "default role admin", role: "admin", wantUser: "alice", wantName: "Alice Example", wantAdmin: true},
		{name: "role spelled differently is not admin", role: "Admin", wantUser: "alice", wantName: "Alice Example"},
		// A new SSO identity never takes over an existing account by name.
		{name: "name collision gets a suffix", role: "user", taken: []string{"alice", "alice_1"}, wantUser: "alice_2", wantName: "Alice Example"},
		{name: "display name defaults to username", role: "user", edit: func(c map[string]any) { delete(c, "name") }, wantUser: "alice", wantName: "alice"},
		{name: "preferred username without email", role: "user", edit: func(c map[string]any) {
			delete(c, "email")
			c["preferred_username"] = "Al"
		}, wantUser: "al", wantName: "Alice Example"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := newHarness(t, func(c *Config) { c.DefaultRole = tt.role })
			for _, u := range tt.taken {
				hs.taken[u] = true
			}
			rec := hs.signIn(tt.edit)
			if loc := redirectedTo(t, rec); loc != "/" {
				t.Fatalf("redirected to %q", loc)
			}
			ins := hs.inserts()
			if len(ins) != 1 {
				t.Fatalf("%d inserts", len(ins))
			}
			if got := fmt.Sprint(ins[0].Args); got != fmt.Sprint([]any{tt.wantUser, tt.wantName, tt.wantAdmin, "sub-123"}) {
				t.Errorf("provisioned %s", got)
			}
			if c := sessionCookie(rec); c == nil || c.Value != "session-for-"+tt.wantUser {
				t.Errorf("session cookie = %+v", c)
			}
		})
	}
}

func TestCallbackCollisionExhausted(t *testing.T) {
	hs := newHarness(t, nil)
	hs.taken["alice"] = true
	for i := 1; i <= 10; i++ {
		hs.taken[fmt.Sprintf("alice_%d", i)] = true
	}
	if loc := redirectedTo(t, hs.signIn(nil)); loc != "/login.html?error=oidc_failed" {
		t.Fatalf("redirected to %q", loc)
	}
	if len(hs.inserts()) != 0 || len(hs.sessions) != 0 {
		t.Fatal("signed in despite no free username")
	}
}

// Every failure redirects to the login page with no session and no account.
func TestCallbackFailsClosed(t *testing.T) {
	_, otherKey := testKeys(t)
	type setup func(hs *harness) url.Values
	// valid scripts a correct token for a fresh login, letting edit spoil it.
	valid := func(edit func(hs *harness, claims map[string]any) *bool) setup {
		return func(hs *harness) url.Values {
			state, nonce := hs.login()
			claims := hs.idp.claims(nonce)
			wrongKey := false
			if edit != nil {
				if w := edit(hs, claims); w != nil {
					wrongKey = *w
				}
			}
			key := hs.idp.key
			if wrongKey {
				key = otherKey
			}
			hs.idp.issue(claims, key)
			return url.Values{"state": {state}, "code": {"c"}}
		}
	}
	yes := true

	tests := []struct {
		name  string
		setup setup
		want  string
	}{
		{name: "no state", setup: func(hs *harness) url.Values {
			hs.login()
			return url.Values{"code": {"c"}}
		}, want: "oidc_failed"},
		{name: "forged state", setup: func(hs *harness) url.Values {
			hs.login()
			return url.Values{"state": {"0123"}, "code": {"c"}}
		}, want: "oidc_failed"},
		{name: "expired state", setup: func(hs *harness) url.Values {
			state, nonce := hs.login()
			hs.h.states[state] = stateEntry{nonce: nonce, expiresAt: time.Now().Add(-time.Second)}
			return url.Values{"state": {state}, "code": {"c"}}
		}, want: "oidc_failed"},
		{name: "provider error", setup: func(hs *harness) url.Values {
			state, _ := hs.login()
			return url.Values{"state": {state}, "error": {"access_denied"}}
		}, want: "oidc_denied"},
		{name: "no code", setup: func(hs *harness) url.Values {
			state, _ := hs.login()
			return url.Values{"state": {state}}
		}, want: "oidc_failed"},
		{name: "code rejected by IdP", setup: func(hs *harness) url.Values {
			state, _ := hs.login()
			hs.idp.tokenStatus = http.StatusBadRequest
			return url.Values{"state": {state}, "code": {"c"}}
		}, want: "oidc_failed"},
		{name: "no id_token", setup: func(hs *harness) url.Values {
			state, _ := hs.login()
			return url.Values{"state": {state}, "code": {"c"}}
		}, want: "oidc_failed"},
		{name: "signed by another key", setup: valid(func(*harness, map[string]any) *bool { return &yes }), want: "oidc_failed"},
		{name: "another issuer", setup: valid(func(_ *harness, c map[string]any) *bool { c["iss"] = "https://evil.example"; return nil }), want: "oidc_failed"},
		{name: "another audience", setup: valid(func(_ *harness, c map[string]any) *bool { c["aud"] = "someone-else"; return nil }), want: "oidc_failed"},
		{name: "expired token", setup: valid(func(_ *harness, c map[string]any) *bool {
			c["exp"] = time.Now().Add(-time.Hour).Unix()
			return nil
		}), want: "oidc_failed"},
		// A token minted for another login attempt (or replayed) carries the wrong nonce.
		{name: "nonce mismatch", setup: valid(func(_ *harness, c map[string]any) *bool { c["nonce"] = "stolen"; return nil }), want: "oidc_failed"},
		{name: "no nonce", setup: valid(func(_ *harness, c map[string]any) *bool { delete(c, "nonce"); return nil }), want: "oidc_failed"},
		{name: "no subject", setup: valid(func(_ *harness, c map[string]any) *bool { delete(c, "sub"); return nil }), want: "oidc_failed"},
		{name: "malformed claims", setup: valid(func(_ *harness, c map[string]any) *bool { c["email"] = 42; return nil }), want: "oidc_failed"},
		{name: "subject lookup fails", setup: valid(func(hs *harness, _ map[string]any) *bool {
			hs.lookupErr = errors.New("db down")
			return nil
		}), want: "oidc_failed"},
		{name: "username check fails", setup: valid(func(hs *harness, _ map[string]any) *bool {
			hs.pg.Respond(func(q string, a []driver.Value) (fakedb.PGResult, bool) {
				if strings.Contains(q, "password_hash") {
					return fakedb.PGResult{Err: errors.New("db down")}, true
				}
				return hs.respond(q, a)
			})
			return nil
		}), want: "oidc_failed"},
		{name: "provisioning fails", setup: valid(func(hs *harness, _ map[string]any) *bool {
			hs.createErr = errors.New("unique violation")
			return nil
		}), want: "oidc_failed"},
		{name: "session store fails", setup: valid(func(hs *harness, _ map[string]any) *bool {
			hs.subjects["sub-123"] = userRec{username: "alice", enabled: true}
			hs.sessErr = errors.New("redis down")
			return nil
		}), want: "oidc_failed"},
		{name: "disabled account", setup: valid(func(hs *harness, _ map[string]any) *bool {
			hs.subjects["sub-123"] = userRec{username: "alice", enabled: false}
			return nil
		}), want: "account_disabled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := newHarness(t, nil)
			rec := hs.callback(tt.setup(hs))
			if loc := redirectedTo(t, rec); loc != "/login.html?error="+tt.want {
				t.Fatalf("redirected to %q, want error=%s", loc, tt.want)
			}
			if c := sessionCookie(rec); c != nil {
				t.Fatalf("session cookie set: %+v", c)
			}
			if len(hs.sessions) != 0 {
				t.Fatalf("session created for %v", hs.sessions)
			}
			if tt.name != "provisioning fails" && len(hs.inserts()) != 0 {
				t.Fatal("account provisioned")
			}
		})
	}
}

// A state is consumed by its first callback, whatever the outcome, so a
// captured callback URL cannot be replayed.
func TestCallbackStateCannotBeReplayed(t *testing.T) {
	hs := newHarness(t, nil)
	hs.subjects["sub-123"] = userRec{username: "alice", enabled: true}
	state, nonce := hs.login()
	hs.idp.issue(hs.idp.claims(nonce), hs.idp.key)
	q := url.Values{"state": {state}, "code": {"c"}}

	if loc := redirectedTo(t, hs.callback(q)); loc != "/" {
		t.Fatalf("first callback redirected to %q", loc)
	}
	rec := hs.callback(q)
	if loc := redirectedTo(t, rec); loc != "/login.html?error=oidc_failed" || sessionCookie(rec) != nil {
		t.Fatalf("replayed callback redirected to %q", loc)
	}
	if len(hs.sessions) != 1 {
		t.Fatalf("%d sessions, want 1", len(hs.sessions))
	}
}

func TestCallbackDomainRestriction(t *testing.T) {
	tests := []struct {
		name  string
		email any // nil removes the claim
		want  string
	}{
		{name: "allowed, case-insensitive", email: "Alice@EXAMPLE.com", want: "/"},
		{name: "second allowed domain", email: "alice@corp.example", want: "/"},
		{name: "other domain", email: "alice@evil.example", want: "/login.html?error=oidc_domain"},
		// Suffix tricks must not pass an exact-match list.
		{name: "subdomain of allowed", email: "alice@sub.example.com", want: "/login.html?error=oidc_domain"},
		{name: "allowed as prefix", email: "alice@example.com.evil", want: "/login.html?error=oidc_domain"},
		{name: "no email", email: nil, want: "/login.html?error=oidc_domain"},
		{name: "email without domain", email: "alice", want: "/login.html?error=oidc_domain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := newHarness(t, func(c *Config) { c.AllowedDomains = []string{"example.com", "corp.example"} })
			hs.subjects["sub-123"] = userRec{username: "alice", enabled: true}
			rec := hs.signIn(func(c map[string]any) {
				if tt.email == nil {
					delete(c, "email")
				} else {
					c["email"] = tt.email
				}
			})
			if loc := redirectedTo(t, rec); loc != tt.want {
				t.Fatalf("redirected to %q, want %q", loc, tt.want)
			}
			if tt.want != "/" {
				if sessionCookie(rec) != nil || len(hs.sessions) != 0 {
					t.Fatal("signed in from a disallowed domain")
				}
				// The domain check runs before any account lookup.
				if len(hs.pg.Calls()) != 0 {
					t.Fatalf("touched the database: %+v", hs.pg.Calls())
				}
				if len(hs.events) != 1 || !strings.HasPrefix(hs.events[0], "oidc_login_failed") {
					t.Errorf("events = %v", hs.events)
				}
			}
		})
	}
}
