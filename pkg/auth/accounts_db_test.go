package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/pkg/storage"
)

func post(t *testing.T, handler http.HandlerFunc, target, body string, mods ...func(*http.Request) *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	for _, m := range mods {
		r = m(r)
	}
	rec := httptest.NewRecorder()
	handler(rec, r)
	return rec
}

// asRoot attaches a tenant admin as the caller.
func asRoot(r *http.Request) *http.Request {
	return withUser(r, &storage.User{Username: "root", IsAdmin: true})
}

func inviteToken(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	data, _ := decodeBody(t, rec)["data"].(map[string]any)
	tok, _ := data["invite_token"].(string)
	if tok == "" || data["invite_url"] != "/login.html?invite="+tok {
		t.Fatalf("invite data = %v", data)
	}
	return tok
}

// An invite is the only way a new local account gets a password. The token is
// handed out once and stored only as a hash; it works until used or expired.
func TestInviteLifecycle(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("root", asAdmin)

	rec := post(t, h.HandleRegister, "/", `{"username":"carol","display_name":"Carol","role":"owner"}`, asRoot)
	if rec.Code != http.StatusOK {
		t.Fatalf("register: %d %s", rec.Code, rec.Body.String())
	}
	token := inviteToken(t, rec)
	carol := db.user("carol")
	if carol["password_hash"] != "!invite" || carol["is_admin"] != false {
		t.Errorf("new user = %v, want an invite marker and no admin (an unknown role is a plain user)", carol)
	}
	if carol["invite_token_hash"] != hashInviteToken(token) {
		t.Error("stored invite is not the hash of the issued token")
	}
	if exp := carol["invite_expires_at"].(time.Time); time.Until(exp) < inviteExpiry-time.Minute {
		t.Errorf("invite expires in %v", time.Until(exp))
	}

	// The pending account cannot sign in.
	if rec := login(h, "carol", "!invite", "203.0.113.5"); rec.Code != http.StatusUnauthorized {
		t.Errorf("pending account login: %d", rec.Code)
	}

	validate := func(tok string) int {
		r := httptest.NewRequest(http.MethodGet, "/?token="+tok, nil)
		rec := httptest.NewRecorder()
		h.HandleValidateInvite(rec, r)
		return rec.Code
	}
	if code := validate(token); code != http.StatusOK {
		t.Errorf("validate: %d", code)
	}
	if code := validate(hashInviteToken(token)); code != http.StatusNotFound {
		t.Errorf("the stored hash works as a token: %d", code)
	}

	// An admin may reissue it; the old link stops working.
	rec = post(t, h.HandleResetInvite, "/", `{"username":"carol"}`, asRoot)
	if rec.Code != http.StatusOK {
		t.Fatalf("reset invite: %d %s", rec.Code, rec.Body.String())
	}
	reissued := inviteToken(t, rec)
	if code := validate(token); code != http.StatusNotFound {
		t.Errorf("superseded token still validates: %d", code)
	}

	newPassword := strings.Repeat("n", minPasswordLength)
	if rec := post(t, h.HandleAcceptInvite, "/", `{"token":"`+token+`","password":"`+newPassword+`"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("accept with superseded token: %d", rec.Code)
	}
	if rec := post(t, h.HandleAcceptInvite, "/", `{"token":"`+reissued+`","password":"`+newPassword+`"}`); rec.Code != http.StatusOK {
		t.Fatalf("accept: %d %s", rec.Code, rec.Body.String())
	}
	carol = db.user("carol")
	if carol["invite_token_hash"] != nil || carol["invite_expires_at"] != nil {
		t.Error("invite not cleared on acceptance")
	}
	if err := verifyPassword(carol["password_hash"].(string), newPassword); err != nil {
		t.Errorf("accepted password does not verify: %v", err)
	}

	// Single use.
	if rec := post(t, h.HandleAcceptInvite, "/", `{"token":"`+reissued+`","password":"`+newPassword+`x"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("second acceptance: %d", rec.Code)
	}
	// A user with a password has no invite to reissue.
	if rec := post(t, h.HandleResetInvite, "/", `{"username":"carol"}`, asRoot); rec.Code != http.StatusBadRequest {
		t.Errorf("reset invite on an active account: %d", rec.Code)
	}
	if rec := login(h, "carol", newPassword, "203.0.113.5"); rec.Code != http.StatusOK {
		t.Errorf("login with the accepted password: %d", rec.Code)
	}
}

func TestInviteRoleAndExpiry(t *testing.T) {
	h, db := newDBHandler(t)

	if rec := post(t, h.HandleRegister, "/", `{"username":"dana","role":"admin"}`, asRoot); rec.Code != http.StatusOK {
		t.Fatalf("register: %d", rec.Code)
	}
	if db.user("dana")["is_admin"] != true {
		t.Error("role admin did not make an admin")
	}

	db.addUser("eve", func(r row) {
		r["password_hash"] = "!invite"
		r["invite_token_hash"] = hashInviteToken("expired-token")
		r["invite_expires_at"] = time.Now().Add(-time.Minute)
	})
	r := httptest.NewRequest(http.MethodGet, "/?token=expired-token", nil)
	rec := httptest.NewRecorder()
	h.HandleValidateInvite(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expired invite validates: %d", rec.Code)
	}
	if rec := post(t, h.HandleAcceptInvite, "/", `{"token":"expired-token","password":"`+strings.Repeat("p", minPasswordLength)+`"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("expired invite accepted: %d", rec.Code)
	}
	if db.user("eve")["password_hash"] != "!invite" {
		t.Error("expired invite set a password")
	}

	db.failOn("INSERT INTO users", errors.New("pq: duplicate key"))
	if rec := post(t, h.HandleRegister, "/", `{"username":"dana"}`, asRoot); rec.Code != http.StatusInternalServerError {
		t.Errorf("failed insert: %d", rec.Code)
	}
}

// Changing a password ends every session for the account, including ones an
// attacker may hold, and leaves the caller signed in on a fresh one.
func TestHandleChangePasswordSuccess(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice", mustChange)
	db.addUser("bob")
	db.newSession("alice-laptop", "alice")
	db.newSession("alice-stolen", "alice")
	db.newSession("bob-1", "bob")

	user := db.user("alice")
	newPassword := strings.Repeat("z", minPasswordLength)
	rec := post(t, h.HandleChangePassword, "/", `{"current_password":"`+userPassword+`","new_password":"`+newPassword+`"}`,
		func(r *http.Request) *http.Request {
			return withUser(r, &storage.User{Username: "alice", AuthProvider: "local", PasswordHash: user["password_hash"].(string)})
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}

	alice := db.user("alice")
	if err := verifyPassword(alice["password_hash"].(string), newPassword); err != nil {
		t.Errorf("new password does not verify: %v", err)
	}
	if alice["force_password_change"] != false {
		t.Error("forced change not cleared")
	}
	sessions := db.sessionsOf("alice")
	c := sessionCookie(rec)
	if c == nil || len(sessions) != 1 || sessions[0]["session_id"] != c.Value {
		t.Errorf("sessions after change = %v, cookie %v; want only the new one", sessions, c)
	}
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode {
		t.Errorf("cookie = %+v", c)
	}
	if len(db.sessionsOf("bob")) != 1 {
		t.Error("another user's session was ended")
	}
	if rec := login(h, "alice", userPassword, "203.0.113.4"); rec.Code != http.StatusUnauthorized {
		t.Errorf("old password still works: %d", rec.Code)
	}
}

func TestHandleChangePasswordStoreFailure(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.newSession("alice-1", "alice")
	db.failOn("SET password_hash", errors.New("pq: connection reset"))

	rec := post(t, h.HandleChangePassword, "/", `{"current_password":"`+userPassword+`","new_password":"`+strings.Repeat("z", minPasswordLength)+`"}`,
		func(r *http.Request) *http.Request {
			return withUser(r, &storage.User{Username: "alice", AuthProvider: "local", PasswordHash: db.user("alice")["password_hash"].(string)})
		})
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if len(db.sessionsOf("alice")) != 1 {
		t.Error("sessions ended although the password did not change")
	}
}

// An admin reset puts the account back in the invite flow and signs it out.
func TestHandleAdminResetPassword(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.addUser("sso", viaSSO)
	db.newSession("alice-1", "alice")
	db.newSession("sso-1", "sso")

	rec := post(t, h.HandleAdminResetPassword, "/", `{"username":"alice"}`, asRoot)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	token := inviteToken(t, rec)
	alice := db.user("alice")
	if alice["password_hash"] != "!invite" || alice["invite_token_hash"] != hashInviteToken(token) {
		t.Errorf("alice = %v, want reset to an invite for the issued token", alice)
	}
	if len(db.sessionsOf("alice")) != 0 {
		t.Error("sessions survived a password reset")
	}
	if rec := login(h, "alice", userPassword, "203.0.113.4"); rec.Code == http.StatusOK {
		t.Error("old password works after a reset")
	}

	if rec := post(t, h.HandleAdminResetPassword, "/", `{"username":"sso"}`, asRoot); rec.Code != http.StatusBadRequest {
		t.Errorf("SSO reset: %d", rec.Code)
	}
	if len(db.sessionsOf("sso")) != 1 || db.user("sso")["invite_token_hash"] != nil {
		t.Error("a refused SSO reset changed the account")
	}
	if rec := post(t, h.HandleAdminResetPassword, "/", `{"username":"nobody"}`, asRoot); rec.Code != http.StatusNotFound {
		t.Errorf("unknown user: %d", rec.Code)
	}
}

func TestHandleListUsers(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("root", asAdmin)
	db.addUser("alice", func(r row) { r["totp_enrolled_at"] = time.Now() })
	db.addUser("carol", func(r row) { r["password_hash"] = "!invite"; r["invite_token_hash"] = "h" })
	db.addUser("off", disabled, viaSSO)

	r := httptest.NewRequest(http.MethodGet, "/api/v1/users?limit=2", nil)
	rec := httptest.NewRecorder()
	h.HandleListUsers(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	items, _ := body["data"].([]any)
	if len(items) != 2 {
		t.Fatalf("page = %v, want 2 items", body)
	}

	r = httptest.NewRequest(http.MethodGet, "/api/v1/users", nil)
	rec = httptest.NewRecorder()
	h.HandleListUsers(rec, r)
	all := map[string]map[string]any{}
	for _, it := range decodeBody(t, rec)["data"].([]any) {
		u := it.(map[string]any)
		all[u["username"].(string)] = u
	}
	checks := []struct {
		user, field string
		want        any
	}{
		{"root", "role", "admin"},
		{"alice", "role", "user"},
		{"alice", "totp_enrolled", true},
		{"carol", "invite_pending", true},
		{"alice", "invite_pending", false},
		{"off", "enabled", false},
		{"off", "auth_provider", "oidc"},
	}
	for _, c := range checks {
		if got := all[c.user][c.field]; got != c.want {
			t.Errorf("%s.%s = %v, want %v", c.user, c.field, got, c.want)
		}
	}
	for _, u := range all {
		for _, secret := range []string{"password_hash", "totp_secret", "invite_token_hash"} {
			if _, ok := u[secret]; ok {
				t.Errorf("list exposes %s", secret)
			}
		}
	}

	db.failOn("AS invite_pending", errors.New("pq: connection reset"))
	rec = httptest.NewRecorder()
	h.HandleListUsers(rec, httptest.NewRequest(http.MethodGet, "/api/v1/users", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("failed list: %d", rec.Code)
	}
}

func TestHandleDeleteUserEndsSessions(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.newSession("alice-1", "alice")

	r := withUser(httptest.NewRequest(http.MethodDelete, "/?username=alice", nil), &storage.User{Username: "root", IsAdmin: true})
	rec := httptest.NewRecorder()
	h.HandleDeleteUser(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if len(db.rows("users", row{"username": "alice"})) != 0 || len(db.sessionsOf("alice")) != 0 {
		t.Error("user or sessions survived deletion")
	}

	db.addUser("bob")
	db.newSession("bob-1", "bob")
	db.failOn("DELETE FROM users", errors.New("pq: foreign key"))
	r = withUser(httptest.NewRequest(http.MethodDelete, "/?username=bob", nil), &storage.User{Username: "root", IsAdmin: true})
	rec = httptest.NewRecorder()
	h.HandleDeleteUser(rec, r)
	if rec.Code != http.StatusInternalServerError || len(db.sessionsOf("bob")) != 1 {
		t.Errorf("failed delete: status %d, sessions %v", rec.Code, db.sessionsOf("bob"))
	}
}

func TestHandleUpdateUser(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.addUser("bob")
	update := func(name, body string) int {
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
		r = withURLParam(withUser(r, &storage.User{Username: "root", IsAdmin: true}), "username", name)
		rec := httptest.NewRecorder()
		h.HandleUpdateUser(rec, r)
		return rec.Code
	}

	if code := update("alice", `{"display_name":"Alice A","role":"admin"}`); code != http.StatusOK {
		t.Fatalf("update: %d", code)
	}
	if a := db.user("alice"); a["display_name"] != "Alice A" || a["is_admin"] != true {
		t.Errorf("alice = %v", a)
	}
	if code := update("alice", `{"role":"user"}`); code != http.StatusOK || db.user("alice")["is_admin"] != false {
		t.Errorf("demote: %d, is_admin %v", code, db.user("alice")["is_admin"])
	}
	if code := update("bob", `{"display_name":"Robert"}`); code != http.StatusOK || db.user("bob")["display_name"] != "Robert" || db.user("bob")["is_admin"] != false {
		t.Errorf("rename: %d, bob %v", code, db.user("bob"))
	}
	if code := update("nobody", `{"display_name":"X"}`); code != http.StatusInternalServerError {
		t.Errorf("unknown user: %d", code)
	}
}

// Disabling ends the account's sessions; enabling does not touch them.
func TestHandleSetUserEnabled(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.newSession("alice-1", "alice")
	set := func(name, body string) int {
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
		r = withURLParam(withUser(r, &storage.User{Username: "root", IsAdmin: true}), "username", name)
		rec := httptest.NewRecorder()
		h.HandleSetUserEnabled(rec, r)
		return rec.Code
	}

	if code := set("alice", `{"enabled":true}`); code != http.StatusOK || len(db.sessionsOf("alice")) != 1 {
		t.Errorf("enable: %d, sessions %d", code, len(db.sessionsOf("alice")))
	}
	if code := set("alice", `{"enabled":false}`); code != http.StatusOK {
		t.Fatalf("disable: %d", code)
	}
	if db.user("alice")["enabled"] != false || len(db.sessionsOf("alice")) != 0 {
		t.Error("disable left the account enabled or its sessions open")
	}
	if code := set("nobody", `{"enabled":false}`); code != http.StatusInternalServerError {
		t.Errorf("unknown user: %d", code)
	}
}

func TestHandleUpdatePreferencesStoresTimezone(t *testing.T) {
	h, db := newDBHandler(t)
	db.addUser("alice")
	u := &storage.User{Username: "alice"}
	rec := post(t, h.HandleUpdatePreferences, "/", `{"display_timezone":" Europe/Berlin "}`, func(r *http.Request) *http.Request { return withUser(r, u) })
	if rec.Code != http.StatusOK || db.user("alice")["display_timezone"] != "Europe/Berlin" || u.DisplayTimezone != "Europe/Berlin" {
		t.Errorf("status %d, stored %v", rec.Code, db.user("alice")["display_timezone"])
	}

	db.failOn("SET display_timezone", errors.New("pq: connection reset"))
	rec = post(t, h.HandleUpdatePreferences, "/", `{"display_timezone":"UTC"}`, func(r *http.Request) *http.Request { return withUser(r, u) })
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("failed store: %d", rec.Code)
	}
}
