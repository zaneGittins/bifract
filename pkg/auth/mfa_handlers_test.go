package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/pkg/mfa"
	"bifract/pkg/storage"

	"golang.org/x/crypto/bcrypt"
)

// totpAt computes an RFC 6238 code independently of pkg/mfa, so these tests do
// not just agree with whatever that package does.
func totpAt(t *testing.T, secret string, counter int64) string {
	t.Helper()
	key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(secret)
	if err != nil {
		t.Fatal(err)
	}
	var msg [8]byte
	binary.BigEndian.PutUint64(msg[:], uint64(counter))
	mac := hmac.New(sha1.New, key)
	mac.Write(msg[:])
	sum := mac.Sum(nil)
	off := sum[len(sum)-1] & 0x0f
	v := binary.BigEndian.Uint32(sum[off:off+4]) & 0x7fffffff
	return fmt.Sprintf("%06d", v%1000000)
}

// enrolledUser returns a local user with a sealed TOTP secret and a cheap
// bcrypt hash of password. The pepper must already be set.
func enrolledUser(t *testing.T, password string, enrolled bool) (*storage.User, string) {
	t.Helper()
	secret, err := mfa.GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := mfa.Seal(secret)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(pepperPassword(password)), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return &storage.User{
		Username:     "alice",
		AuthProvider: "local",
		PasswordHash: string(hash),
		TOTPSecret:   sealed,
		TOTPEnrolled: enrolled,
	}, secret
}

// wrongCode returns a six digit code that matches no step in the accepted window.
func wrongCode(t *testing.T, secret string) string {
	now := mfa.Counter(time.Now().Unix())
	valid := map[string]bool{}
	for c := now - 2; c <= now+2; c++ {
		valid[totpAt(t, secret, c)] = true
	}
	for i := 0; ; i++ {
		c := fmt.Sprintf("%06d", i)
		if !valid[c] {
			return c
		}
	}
}

func mfaRequest(user *storage.User, pending bool, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/mfa", strings.NewReader(body))
	ctx := context.WithValue(r.Context(), "user", user)
	ctx = context.WithValue(ctx, "mfa_pending", pending)
	return r.WithContext(ctx)
}

func TestHandleMFAStatusUnenrolled(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, _ := newTestHandler()

	rec := httptest.NewRecorder()
	h.HandleMFAStatus(rec, withUser(httptest.NewRequest(http.MethodGet, "/", nil),
		&storage.User{Username: "alice", TOTPSecret: "sealed-but-unconfirmed"}))
	data, _ := decodeBody(t, rec)["data"].(map[string]any)
	if data["enrolled"] != false || data["pending"] != true || data["available"] != true {
		t.Errorf("unexpected status %v", data)
	}
	if strings.Contains(rec.Body.String(), "sealed-but-unconfirmed") {
		t.Error("status leaks the stored secret")
	}

	rec = httptest.NewRecorder()
	h.HandleMFAStatus(rec, withUser(httptest.NewRequest(http.MethodPost, "/", nil), &storage.User{}))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d, want 405", rec.Code)
	}
}

func TestHandleMFAEnrollRefusals(t *testing.T) {
	tests := []struct {
		name   string
		pepper string
		user   *storage.User
		method string
		want   int
	}{
		{name: "GET", pepper: testPepper, user: &storage.User{Username: "a"}, method: http.MethodGet, want: http.StatusMethodNotAllowed},
		{name: "sso account", pepper: testPepper, user: &storage.User{Username: "a", AuthProvider: "oidc"}, want: http.StatusBadRequest},
		// Without a pepper the secret would be stored in the clear.
		{name: "no encryption key", user: &storage.User{Username: "a", AuthProvider: "local"}, want: http.StatusServiceUnavailable},
		// Re-enrolling over a live authenticator would let a hijacked session
		// swap in the attacker's device.
		{name: "already enrolled", pepper: testPepper, user: &storage.User{Username: "a", TOTPEnrolled: true}, want: http.StatusConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("BIFRACT_PASSWORD_PEPPER", tt.pepper)
			h, _ := newTestHandler()
			method := tt.method
			if method == "" {
				method = http.MethodPost
			}
			rec := httptest.NewRecorder()
			h.HandleMFAEnroll(rec, withUser(httptest.NewRequest(method, "/", nil), tt.user))
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestHandleMFAConfirmRefusals(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	pendingUser, secret := enrolledUser(t, "pw", false)
	now := mfa.Counter(time.Now().Unix())

	tests := []struct {
		name string
		user *storage.User
		body string
		want int
	}{
		{name: "already enrolled", user: &storage.User{Username: "alice", TOTPEnrolled: true, TOTPSecret: pendingUser.TOTPSecret}, body: `{}`, want: http.StatusConflict},
		{name: "enrollment not started", user: &storage.User{Username: "alice"}, body: `{}`, want: http.StatusBadRequest},
		{name: "bad body", user: pendingUser, body: "{", want: http.StatusBadRequest},
		{name: "wrong code", user: pendingUser, body: `{"code":"` + wrongCode(t, secret) + `"}`, want: http.StatusUnauthorized},
		{name: "no code", user: pendingUser, body: `{}`, want: http.StatusUnauthorized},
		{
			// A code from a step already spent is a replay, even within its window.
			name: "replayed code",
			user: func() *storage.User { u := *pendingUser; u.TOTPLastCounter = now + 1; return &u }(),
			body: `{"code":"` + totpAt(t, secret, now) + `"}`,
			want: http.StatusUnauthorized,
		},
		{
			name: "secret sealed under another key",
			user: func() *storage.User {
				u := *pendingUser
				u.TOTPSecret = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
				return &u
			}(),
			body: `{"code":"123456"}`,
			want: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler()
			rec := httptest.NewRecorder()
			h.HandleMFAConfirm(rec, mfaRequest(tt.user, false, tt.body))
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "recovery_codes") {
				t.Error("refused confirmation returned recovery codes")
			}
		})
	}
}

// Code guessing is limited per account, not per address, so moving to a fresh
// IP does not buy more guesses at a six digit space.
func TestMFAGuessingIsLimitedPerUser(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	user, secret := enrolledUser(t, "pw", true)
	bad := `{"code":"` + wrongCode(t, secret) + `"}`
	h, store := newTestHandler()
	store.sessions["pending-sid"] = &Session{Username: "alice", MFAPending: true}

	for i := 0; i < loginMaxFailures; i++ {
		r := mfaRequest(user, true, bad)
		r.RemoteAddr = fmt.Sprintf("198.51.100.%d:1234", i+1)
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "pending-sid"})
		rec := httptest.NewRecorder()
		h.HandleMFAVerify(rec, r)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status = %d, want 401", i+1, rec.Code)
		}
		if rec.Header().Get("Set-Cookie") != "" {
			t.Fatal("a failed verification issued a cookie")
		}
	}

	r := mfaRequest(user, true, bad)
	r.RemoteAddr = "192.0.2.200:1234"
	rec := httptest.NewRecorder()
	h.HandleMFAVerify(rec, r)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("after %d failures from different addresses: status = %d, want 429", loginMaxFailures, rec.Code)
	}

	// The other MFA endpoints share the per-user budget.
	for name, fn := range map[string]http.HandlerFunc{
		"confirm":        h.HandleMFAConfirm,
		"disable":        h.HandleMFADisable,
		"recovery codes": h.HandleMFARecoveryCodes,
	} {
		u := *user
		if name == "confirm" {
			u.TOTPEnrolled = false
		}
		rec := httptest.NewRecorder()
		fn(rec, mfaRequest(&u, false, `{"password":"pw","code":"000000"}`))
		if rec.Code != http.StatusTooManyRequests {
			t.Errorf("%s: status = %d, want 429 while the user is locked out", name, rec.Code)
		}
	}

	// Failures by a pending session never replaced or ended it.
	if len(store.deleted) != 0 {
		t.Errorf("failed verification deleted sessions %v", store.deleted)
	}
	// Nor did they count against the addresses they came from: the budget is
	// the account's.
	if n := len(h.loginLimiter.attempts); n != 0 {
		t.Errorf("MFA failures recorded %d entries in the per-address login limiter", n)
	}
}

func TestHandleMFAVerifyRefusals(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	user, secret := enrolledUser(t, "pw", true)
	now := mfa.Counter(time.Now().Unix())

	tests := []struct {
		name    string
		method  string
		user    *storage.User
		pending bool
		body    string
		want    int
	}{
		{name: "GET", method: http.MethodGet, user: user, pending: true, want: http.StatusMethodNotAllowed},
		// A full session has nothing to verify; letting it through would turn
		// this into an oracle for codes.
		{name: "session not pending", user: user, body: `{"code":"` + totpAt(t, secret, now) + `"}`, want: http.StatusBadRequest},
		{name: "bad body", user: user, pending: true, body: "{", want: http.StatusBadRequest},
		{name: "wrong code", user: user, pending: true, body: `{"code":"` + wrongCode(t, secret) + `"}`, want: http.StatusUnauthorized},
		{name: "empty code", user: user, pending: true, body: `{"code":""}`, want: http.StatusUnauthorized},
		{
			name:    "replayed code",
			user:    func() *storage.User { u := *user; u.TOTPLastCounter = now + 1; return &u }(),
			pending: true,
			body:    `{"code":"` + totpAt(t, secret, now) + `"}`,
			want:    http.StatusUnauthorized,
		},
		{
			name:    "no authenticator",
			user:    &storage.User{Username: "alice"},
			pending: true,
			body:    `{"code":"123456"}`,
			want:    http.StatusUnauthorized,
		},
		{
			// A blank recovery code is no recovery code: it falls to the
			// authenticator check rather than being looked up.
			name:    "blank recovery code",
			user:    &storage.User{Username: "alice"},
			pending: true,
			body:    `{"recovery_code":"   "}`,
			want:    http.StatusUnauthorized,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, store := newTestHandler()
			method := tt.method
			if method == "" {
				method = http.MethodPost
			}
			r := mfaRequest(tt.user, tt.pending, tt.body)
			r.Method = method
			r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "pending-sid"})
			rec := httptest.NewRecorder()
			h.HandleMFAVerify(rec, r)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body.String())
			}
			if rec.Header().Get("Set-Cookie") != "" || len(store.sessions) != 0 || len(store.deleted) != 0 {
				t.Error("a refused verification changed the session")
			}
		})
	}
}

// Removing the second factor takes the password and a current code; a hijacked
// session holding only one of them must not strip it.
func TestHandleMFADisableRefusals(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	user, secret := enrolledUser(t, "the-password", true)

	tests := []struct {
		name string
		user *storage.User
		body string
		want int
	}{
		{name: "not enrolled", user: &storage.User{Username: "alice"}, body: `{}`, want: http.StatusBadRequest},
		{name: "bad body", user: user, body: "{", want: http.StatusBadRequest},
		{name: "code without password", user: user, body: `{"code":"` + totpAt(t, secret, mfa.Counter(time.Now().Unix())) + `"}`, want: http.StatusUnauthorized},
		{name: "wrong password", user: user, body: `{"password":"guess","code":"` + totpAt(t, secret, mfa.Counter(time.Now().Unix())) + `"}`, want: http.StatusUnauthorized},
		{name: "password without code", user: user, body: `{"password":"the-password"}`, want: http.StatusUnauthorized},
		{name: "password with wrong code", user: user, body: `{"password":"the-password","code":"` + wrongCode(t, secret) + `"}`, want: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler()
			rec := httptest.NewRecorder()
			h.HandleMFADisable(rec, mfaRequest(tt.user, false, tt.body))
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}

	rec := httptest.NewRecorder()
	h, _ := newTestHandler()
	r := mfaRequest(user, false, `{}`)
	r.Method = http.MethodGet
	h.HandleMFADisable(rec, r)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", rec.Code)
	}
}

func TestHandleMFARecoveryCodesRefusals(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	user, _ := enrolledUser(t, "the-password", true)

	tests := []struct {
		name   string
		method string
		user   *storage.User
		body   string
		want   int
	}{
		{name: "GET", method: http.MethodGet, user: user, want: http.StatusMethodNotAllowed},
		{name: "not enrolled", user: &storage.User{Username: "alice"}, body: `{"password":"the-password"}`, want: http.StatusBadRequest},
		{name: "bad body", user: user, body: "{", want: http.StatusBadRequest},
		{name: "wrong password", user: user, body: `{"password":"guess"}`, want: http.StatusUnauthorized},
		{name: "no password", user: user, body: `{}`, want: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler()
			r := mfaRequest(tt.user, false, tt.body)
			if tt.method != "" {
				r.Method = tt.method
			}
			rec := httptest.NewRecorder()
			h.HandleMFARecoveryCodes(rec, r)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if strings.Contains(rec.Body.String(), "recovery_codes") {
				t.Error("refused request returned recovery codes")
			}
		})
	}
}
