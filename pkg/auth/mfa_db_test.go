package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/pkg/mfa"
	"bifract/pkg/storage"
)

// loadUser reads the user the way AuthMiddleware does on every request.
func loadUser(t *testing.T, db *authDB, name string) *storage.User {
	t.Helper()
	u, err := db.client.GetUser(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func mfaPost(h http.HandlerFunc, user *storage.User, pending bool, sessionID, body string) *httptest.ResponseRecorder {
	r := mfaRequest(user, pending, body)
	if sessionID != "" {
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	}
	rec := httptest.NewRecorder()
	h(rec, r)
	return rec
}

func currentCode(t *testing.T, secret string) string {
	return totpAt(t, secret, mfa.Counter(time.Now().Unix()))
}

// enroll runs enrollment for name end to end and returns the secret and the
// recovery codes handed back.
func enroll(t *testing.T, h *AuthHandler, db *authDB, name string) (string, []string) {
	t.Helper()
	rec := mfaPost(h.HandleMFAEnroll, loadUser(t, db, name), false, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("enroll: %d %s", rec.Code, rec.Body.String())
	}
	data := decodeBody(t, rec)["data"].(map[string]any)
	secret := data["secret"].(string)

	rec = mfaPost(h.HandleMFAConfirm, loadUser(t, db, name), false, "", `{"code":"`+currentCode(t, secret)+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", rec.Code, rec.Body.String())
	}
	var codes []string
	for _, c := range decodeBody(t, rec)["data"].(map[string]any)["recovery_codes"].([]any) {
		codes = append(codes, c.(string))
	}
	return secret, codes
}

func TestMFAEnrollAndConfirm(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")

	rec := mfaPost(h.HandleMFAEnroll, loadUser(t, db, "alice"), false, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("enroll: %d %s", rec.Code, rec.Body.String())
	}
	data := decodeBody(t, rec)["data"].(map[string]any)
	secret := data["secret"].(string)
	if !strings.HasPrefix(data["uri"].(string), "otpauth://totp/") || !strings.Contains(data["qr_svg"].(string), "<svg") {
		t.Errorf("enrollment data = %v", data)
	}

	// Stored sealed, and not yet enrolled: an abandoned setup changes nothing.
	stored := db.user("alice")
	if stored["totp_secret"] == secret {
		t.Fatal("secret stored in the clear")
	}
	if opened, err := mfa.Open(stored["totp_secret"].(string)); err != nil || opened != secret {
		t.Errorf("stored secret opens to %q (%v), want the issued one", opened, err)
	}
	if stored["totp_enrolled_at"] != nil {
		t.Error("enrolled before a code was confirmed")
	}
	if rec := login(h, "alice", userPassword, "203.0.113.3"); decodeBody(t, rec)["user"].(map[string]any)["mfa_required"] != nil {
		t.Error("an unconfirmed secret gates login")
	}

	if rec := mfaPost(h.HandleMFAConfirm, loadUser(t, db, "alice"), false, "", `{"code":"`+wrongCode(t, secret)+`"}`); rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong code confirmed: %d", rec.Code)
	}
	if db.user("alice")["totp_enrolled_at"] != nil {
		t.Error("a wrong code enrolled the user")
	}

	rec = mfaPost(h.HandleMFAConfirm, loadUser(t, db, "alice"), false, "", `{"code":"`+currentCode(t, secret)+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", rec.Code, rec.Body.String())
	}
	alice := db.user("alice")
	if alice["totp_enrolled_at"] == nil || alice["totp_last_counter"].(int64) < mfa.Counter(time.Now().Unix())-1 {
		t.Errorf("after confirm: enrolled %v, counter %v", alice["totp_enrolled_at"], alice["totp_last_counter"])
	}
	var codes []string
	for _, c := range decodeBody(t, rec)["data"].(map[string]any)["recovery_codes"].([]any) {
		codes = append(codes, c.(string))
	}
	storedCodes := db.rows("user_recovery_codes", row{"username": "alice"})
	if len(codes) != recoveryCodeCount || len(storedCodes) != recoveryCodeCount {
		t.Fatalf("%d codes issued, %d stored", len(codes), len(storedCodes))
	}
	for _, c := range codes {
		if len(db.rows("user_recovery_codes", row{"username": "alice", "code_hash": hashRecoveryCode(c)})) != 1 {
			t.Errorf("code %s not stored by its hash", c)
		}
		if len(db.rows("user_recovery_codes", row{"code_hash": c})) != 0 {
			t.Errorf("code %s stored in the clear", c)
		}
	}

	// Now the account is enrolled, status says so and a second enrollment is refused.
	rec = httptest.NewRecorder()
	r := withUser(httptest.NewRequest(http.MethodGet, "/", nil), loadUser(t, db, "alice"))
	h.HandleMFAStatus(rec, r)
	status := decodeBody(t, rec)["data"].(map[string]any)
	if status["enrolled"] != true || status["recovery_codes_remaining"] != float64(recoveryCodeCount) {
		t.Errorf("status = %v", status)
	}
	if rec := mfaPost(h.HandleMFAEnroll, loadUser(t, db, "alice"), false, "", ""); rec.Code != http.StatusConflict {
		t.Errorf("re-enroll over an authenticator: %d", rec.Code)
	}
}

func TestMFAEnrollStoreFailures(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")

	db.failOn("SET totp_secret = $2", errors.New("pq: connection reset"))
	if rec := mfaPost(h.HandleMFAEnroll, loadUser(t, db, "alice"), false, "", ""); rec.Code != http.StatusInternalServerError {
		t.Errorf("enroll with a failing store: %d", rec.Code)
	}

	db.mu.Lock()
	delete(db.fail, "SET totp_secret = $2")
	db.mu.Unlock()
	rec := mfaPost(h.HandleMFAEnroll, loadUser(t, db, "alice"), false, "", "")
	secret := decodeBody(t, rec)["data"].(map[string]any)["secret"].(string)

	db.failOn("INSERT INTO user_recovery_codes", errors.New("pq: disk full"))
	rec = mfaPost(h.HandleMFAConfirm, loadUser(t, db, "alice"), false, "", `{"code":"`+currentCode(t, secret)+`"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("confirm with codes unstorable: %d", rec.Code)
	}
	// The replace is one transaction, rolled back on failure, so the old codes
	// are not lost to a half-written new set.
	for _, c := range db.pg.Matching("user_recovery_codes") {
		if !c.InTx {
			t.Errorf("recovery code statement outside the transaction: %s", c.Query)
		}
	}
	if _, _, commits, rollbacks := db.pg.Stats(); rollbacks == 0 || commits != 0 {
		t.Errorf("commits %d, rollbacks %d; want the failed replace rolled back", commits, rollbacks)
	}
}

// Verifying a code replaces the pending session with a full one, so a session
// id seen before the second factor is worthless after it.
func TestMFAVerifyWithAuthenticator(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice", asAdmin)
	secret, _ := enroll(t, h, db, "alice")

	pending := sessionCookie(login(h, "alice", userPassword, "203.0.113.3"))
	if pending == nil {
		t.Fatal("no pending session")
	}
	before := loadUser(t, db, "alice")
	// Confirmation spent the current step, which therefore cannot log in; the
	// next one is inside the accepted skew window.
	if rec := mfaPost(h.HandleMFAVerify, before, true, pending.Value, `{"code":"`+totpAt(t, secret, before.TOTPLastCounter)+`"}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("the step spent at enrollment logged in: %d", rec.Code)
	}
	code := totpAt(t, secret, before.TOTPLastCounter+1)

	rec := mfaPost(h.HandleMFAVerify, before, true, pending.Value, `{"code":"`+code+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("verify: %d %s", rec.Code, rec.Body.String())
	}
	full := sessionCookie(rec)
	if full == nil || full.Value == pending.Value || full.MaxAge != int(sessionDuration.Seconds()) || !full.HttpOnly {
		t.Fatalf("cookie after verify = %+v", full)
	}
	if len(db.rows("sessions", row{"session_id": pending.Value})) != 0 {
		t.Error("pending session still valid after verification")
	}
	s := db.rows("sessions", row{"session_id": full.Value})
	if len(s) != 1 || s[0]["mfa_pending"] != false {
		t.Errorf("new session = %v, want one full session", s)
	}
	if db.user("alice")["last_login"] == nil {
		t.Error("last_login not recorded")
	}
	if _, next := throughMiddleware(h, http.MethodGet, "/api/v1/query", full.Value, nil); !next.called {
		t.Error("verified session does not authenticate")
	}

	// The same code again loses, even to a caller that read the user before it
	// was spent: the database holds the spent step.
	pending2 := sessionCookie(login(h, "alice", userPassword, "203.0.113.3"))
	if rec := mfaPost(h.HandleMFAVerify, before, true, pending2.Value, `{"code":"`+code+`"}`); rec.Code != http.StatusUnauthorized {
		t.Errorf("replayed code: %d", rec.Code)
	}
	if len(db.rows("sessions", row{"session_id": pending2.Value, "mfa_pending": true})) != 1 {
		t.Error("a failed code changed the pending session")
	}
}

func TestMFAVerifyWithRecoveryCode(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	_, codes := enroll(t, h, db, "alice")

	pending := sessionCookie(login(h, "alice", userPassword, "203.0.113.3"))
	// Typed loosely: upper case, no dashes.
	typed := strings.ToUpper(strings.ReplaceAll(codes[3], "-", ""))
	rec := mfaPost(h.HandleMFAVerify, loadUser(t, db, "alice"), true, pending.Value, `{"recovery_code":"`+typed+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("verify: %d %s", rec.Code, rec.Body.String())
	}
	data := decodeBody(t, rec)["data"].(map[string]any)
	if data["used_recovery_code"] != true || data["recovery_codes_remaining"] != float64(recoveryCodeCount-1) {
		t.Errorf("data = %v", data)
	}
	used := db.rows("user_recovery_codes", row{"code_hash": hashRecoveryCode(codes[3])})
	if len(used) != 1 || used[0]["used_at"] == nil {
		t.Errorf("code not marked used: %v", used)
	}

	pending2 := sessionCookie(login(h, "alice", userPassword, "203.0.113.3"))
	if rec := mfaPost(h.HandleMFAVerify, loadUser(t, db, "alice"), true, pending2.Value, `{"recovery_code":"`+codes[3]+`"}`); rec.Code != http.StatusUnauthorized {
		t.Errorf("recovery code used twice: %d", rec.Code)
	}
	// Another user's code is not this user's.
	db.addUser("bob")
	_, bobCodes := enroll(t, h, db, "bob")
	if rec := mfaPost(h.HandleMFAVerify, loadUser(t, db, "alice"), true, pending2.Value, `{"recovery_code":"`+bobCodes[0]+`"}`); rec.Code != http.StatusUnauthorized {
		t.Errorf("another user's recovery code: %d", rec.Code)
	}
}

// Removing the authenticator needs the password and a code, and clears the
// secret and every recovery code together.
func TestMFADisableSuccess(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.addUser("bob")
	_, codes := enroll(t, h, db, "alice")
	enroll(t, h, db, "bob")

	rec := mfaPost(h.HandleMFADisable, loadUser(t, db, "alice"), false, "", `{"password":"`+userPassword+`","recovery_code":"`+codes[0]+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("disable: %d %s", rec.Code, rec.Body.String())
	}
	alice := db.user("alice")
	if alice["totp_secret"] != nil || alice["totp_enrolled_at"] != nil || alice["totp_last_counter"] != int64(0) {
		t.Errorf("after disable: %v", alice)
	}
	if n := len(db.rows("user_recovery_codes", row{"username": "alice"})); n != 0 {
		t.Errorf("%d recovery codes survived", n)
	}
	if n := len(db.rows("user_recovery_codes", row{"username": "bob"})); n != recoveryCodeCount {
		t.Errorf("bob's codes: %d", n)
	}
	for _, c := range db.pg.Matching("WHERE username = $1") {
		if strings.Contains(c.Query, "totp_secret = NULL") && !c.InTx {
			t.Error("clearing the secret ran outside the transaction")
		}
	}
}

func TestMFADisableBlockedWhenRequired(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	secret, _ := enroll(t, h, db, "alice")
	setRequireMFA(t, true)

	rec := mfaPost(h.HandleMFADisable, loadUser(t, db, "alice"), false, "", `{"password":"`+userPassword+`","code":"`+currentCode(t, secret)+`"}`)
	if rec.Code != http.StatusForbidden || db.user("alice")["totp_enrolled_at"] == nil {
		t.Errorf("disable under a requirement: %d, enrolled %v", rec.Code, db.user("alice")["totp_enrolled_at"])
	}
}

func TestMFARecoveryCodesRegenerate(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	_, old := enroll(t, h, db, "alice")

	rec := mfaPost(h.HandleMFARecoveryCodes, loadUser(t, db, "alice"), false, "", `{"password":"`+userPassword+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("regenerate: %d %s", rec.Code, rec.Body.String())
	}
	fresh := decodeBody(t, rec)["data"].(map[string]any)["recovery_codes"].([]any)
	if len(fresh) != recoveryCodeCount || len(db.rows("user_recovery_codes", row{"username": "alice"})) != recoveryCodeCount {
		t.Fatalf("%d issued, %d stored", len(fresh), len(db.rows("user_recovery_codes", row{"username": "alice"})))
	}
	for _, c := range old {
		if len(db.rows("user_recovery_codes", row{"code_hash": hashRecoveryCode(c)})) != 0 {
			t.Errorf("old code %s still stored", c)
		}
	}

	db.failOn("DELETE FROM user_recovery_codes", errors.New("pq: connection reset"))
	if rec := mfaPost(h.HandleMFARecoveryCodes, loadUser(t, db, "alice"), false, "", `{"password":"`+userPassword+`"}`); rec.Code != http.StatusInternalServerError {
		t.Errorf("failed replace: %d", rec.Code)
	}
}

// An admin reset is the way back in for a user who lost everything; it signs
// them out so the next login enrolls again.
func TestHandleAdminResetMFASuccess(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db := newDBHandler(t)
	db.addUser("alice")
	db.addUser("half", func(r row) { r["totp_secret"] = "sealed-but-unconfirmed" })
	db.addUser("plain")
	enroll(t, h, db, "alice")
	db.newSession("alice-1", "alice")

	if rec := post(t, h.HandleAdminResetMFA, "/", `{"username":"alice"}`, asRoot); rec.Code != http.StatusOK {
		t.Fatalf("reset: %d %s", rec.Code, rec.Body.String())
	}
	if a := db.user("alice"); a["totp_secret"] != nil || a["totp_enrolled_at"] != nil {
		t.Errorf("after reset: %v", a)
	}
	if len(db.rows("user_recovery_codes", row{"username": "alice"})) != 0 || len(db.sessionsOf("alice")) != 0 {
		t.Error("recovery codes or sessions survived the reset")
	}
	// A started but unconfirmed enrollment can be cleared too.
	if rec := post(t, h.HandleAdminResetMFA, "/", `{"username":"half"}`, asRoot); rec.Code != http.StatusOK || db.user("half")["totp_secret"] != nil {
		t.Errorf("reset of an unconfirmed enrollment: %d", rec.Code)
	}
	if rec := post(t, h.HandleAdminResetMFA, "/", `{"username":"plain"}`, asRoot); rec.Code != http.StatusBadRequest {
		t.Errorf("reset with nothing enrolled: %d", rec.Code)
	}
	if rec := post(t, h.HandleAdminResetMFA, "/", `{"username":"nobody"}`, asRoot); rec.Code != http.StatusNotFound {
		t.Errorf("unknown user: %d", rec.Code)
	}

	db.failOn("totp_secret = NULL", errors.New("pq: connection reset"))
	db.newSession("half-1", "half")
	db.mu.Lock()
	for _, r := range db.tables["users"] {
		if r["username"] == "half" {
			r["totp_secret"] = "sealed"
		}
	}
	db.mu.Unlock()
	if rec := post(t, h.HandleAdminResetMFA, "/", `{"username":"half"}`, asRoot); rec.Code != http.StatusInternalServerError || len(db.sessionsOf("half")) != 1 {
		t.Errorf("failed reset: %d, sessions %d", rec.Code, len(db.sessionsOf("half")))
	}
}
