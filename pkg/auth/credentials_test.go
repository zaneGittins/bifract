package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

const testPepper = "test-pepper-not-a-secret"

func TestPepperPassword(t *testing.T) {
	t.Run("no pepper passes the password through", func(t *testing.T) {
		t.Setenv("BIFRACT_PASSWORD_PEPPER", "")
		if got := pepperPassword("hunter2hunter2"); got != "hunter2hunter2" {
			t.Errorf("pepperPassword = %q, want the input unchanged", got)
		}
	})

	t.Run("pepper applies HMAC-SHA256", func(t *testing.T) {
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
		mac := hmac.New(sha256.New, []byte(testPepper))
		mac.Write([]byte("hunter2hunter2"))
		want := hex.EncodeToString(mac.Sum(nil))
		if got := pepperPassword("hunter2hunter2"); got != want {
			t.Errorf("pepperPassword = %q, want %q", got, want)
		}
	})

	// The hex digest is what keeps every peppered password inside bcrypt's
	// 72-byte limit, however long the input.
	t.Run("output fits bcrypt regardless of input length", func(t *testing.T) {
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
		got := pepperPassword(strings.Repeat("x", 500))
		if len(got) > 72 {
			t.Errorf("peppered length %d exceeds bcrypt's 72 bytes", len(got))
		}
	})

	t.Run("a different pepper gives a different value", func(t *testing.T) {
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
		a := pepperPassword("hunter2hunter2")
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper+"-rotated")
		if b := pepperPassword("hunter2hunter2"); a == b {
			t.Error("two peppers produced the same value")
		}
	})
}

// bcrypt at cost 12 takes a noticeable fraction of a second per hash, so the
// password tests hash as few times as they can.
func TestHashAndVerifyPassword(t *testing.T) {
	t.Run("peppered", func(t *testing.T) {
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
		hash, err := hashPassword("correct horse battery")
		if err != nil {
			t.Fatal(err)
		}
		if cost, err := bcrypt.Cost([]byte(hash)); err != nil || cost != bcryptCost {
			t.Errorf("bcrypt cost = %d (%v), want %d", cost, err, bcryptCost)
		}
		if strings.Contains(hash, "correct horse") {
			t.Error("hash contains the password")
		}
		if err := verifyPassword(hash, "correct horse battery"); err != nil {
			t.Errorf("correct password rejected: %v", err)
		}
		for _, wrong := range []string{"", "correct horse batter", "Correct horse battery"} {
			if verifyPassword(hash, wrong) == nil {
				t.Errorf("wrong password %q accepted", wrong)
			}
		}

		// The pepper is part of the credential: a hash made under one pepper
		// must not verify without it or under another.
		t.Setenv("BIFRACT_PASSWORD_PEPPER", "")
		if verifyPassword(hash, "correct horse battery") == nil {
			t.Error("peppered hash verified with no pepper configured")
		}
		t.Setenv("BIFRACT_PASSWORD_PEPPER", "other-pepper")
		if verifyPassword(hash, "correct horse battery") == nil {
			t.Error("peppered hash verified under a different pepper")
		}
	})

	// bcrypt only reads 72 bytes. With a pepper, the HMAC runs over the whole
	// password first, so two long passwords sharing a 72-byte prefix must still
	// be told apart.
	t.Run("peppered long passwords are not truncated", func(t *testing.T) {
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
		prefix := strings.Repeat("a", 80)
		hash, err := hashPassword(prefix + "-one")
		if err != nil {
			t.Fatal(err)
		}
		if verifyPassword(hash, prefix+"-two") == nil {
			t.Error("password differing after byte 72 was accepted")
		}
	})

	// Without a pepper a long password reaches bcrypt as-is. It must be refused,
	// not silently cut to 72 bytes.
	t.Run("unpeppered long password is refused", func(t *testing.T) {
		t.Setenv("BIFRACT_PASSWORD_PEPPER", "")
		if _, err := hashPassword(strings.Repeat("a", 73)); err == nil {
			t.Error("73-byte password hashed without a pepper; bcrypt would have truncated it")
		}
	})

	// init-postgres.sql seeds the admin with an unpeppered hash. That hash must
	// verify while no pepper is set, which is what lets the first login (and
	// migrateDefaultAdminHash's check) work.
	t.Run("unpeppered hash verifies without pepper", func(t *testing.T) {
		t.Setenv("BIFRACT_PASSWORD_PEPPER", "")
		hash, err := bcrypt.GenerateFromPassword([]byte(defaultAdminPassword), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		if err := verifyPassword(string(hash), defaultAdminPassword); err != nil {
			t.Errorf("default admin hash rejected: %v", err)
		}
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
		if verifyPassword(string(hash), defaultAdminPassword) == nil {
			t.Error("unpeppered hash verified once a pepper is set; migrateDefaultAdminHash depends on it not doing so")
		}
	})
}

func TestInviteTokens(t *testing.T) {
	// Known vector, so the stored digest format cannot drift: existing invite
	// rows hold SHA-256 hex of the plaintext token.
	if got, want := hashInviteToken("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"; got != want {
		t.Errorf("hashInviteToken(abc) = %s, want %s", got, want)
	}

	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		plain, hash, err := generateInviteToken()
		if err != nil {
			t.Fatal(err)
		}
		if len(plain) != 2*inviteTokenBytes {
			t.Fatalf("token %q has length %d, want %d", plain, len(plain), 2*inviteTokenBytes)
		}
		if _, err := hex.DecodeString(plain); err != nil {
			t.Fatalf("token %q is not hex", plain)
		}
		if hash != hashInviteToken(plain) {
			t.Fatal("returned hash does not match the token")
		}
		if hash == plain {
			t.Fatal("the stored value is the plaintext token")
		}
		if seen[plain] {
			t.Fatalf("token %q repeated", plain)
		}
		seen[plain] = true
	}
}

func TestGenerateSessionID(t *testing.T) {
	h := &AuthHandler{}
	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		id, err := h.generateSessionID()
		if err != nil {
			t.Fatal(err)
		}
		// 32 random bytes; anything shorter is guessable sooner.
		if b, err := hex.DecodeString(id); err != nil || len(b) != 32 {
			t.Fatalf("session id %q is not 32 hex-encoded bytes", id)
		}
		if seen[id] {
			t.Fatalf("session id %q repeated", id)
		}
		seen[id] = true
	}
}

// The login rate limiter keys on this value, and auth events record it. The
// proxy headers count only when the connecting peer is a trusted proxy (by
// default the private ranges the bundled Caddy and a k8s ingress sit in), and
// then the right-most untrusted hop is the client. pkg/clientip covers the
// parsing in depth.
func TestClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		xri        string
		want       string
	}{
		{name: "ipv4 remote addr", remoteAddr: "203.0.113.7:51234", want: "203.0.113.7"},
		{name: "ipv6 remote addr", remoteAddr: "[2001:db8::1]:443", want: "2001:db8::1"},
		{name: "ipv6 loopback", remoteAddr: "[::1]:8080", want: "::1"},
		{name: "remote addr without port", remoteAddr: "203.0.113.7", want: "203.0.113.7"},
		{name: "forwarded for wins", remoteAddr: "10.0.0.2:1", xff: "198.51.100.4", xri: "198.51.100.9", want: "198.51.100.4"},
		{name: "rightmost untrusted hop", remoteAddr: "10.0.0.2:1", xff: " 1.2.3.4 , 198.51.100.4 , 10.0.0.1", want: "198.51.100.4"},
		{name: "real ip", remoteAddr: "10.0.0.2:1", xri: " 198.51.100.9 ", want: "198.51.100.9"},
		{name: "untrusted peer, forwarded for ignored", remoteAddr: "203.0.113.7:1", xff: "198.51.100.4", want: "203.0.113.7"},
		{name: "untrusted peer, real ip ignored", remoteAddr: "203.0.113.7:1", xri: "198.51.100.9", want: "203.0.113.7"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/v1/auth/login", nil)
			r.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				r.Header.Set("X-Forwarded-For", tt.xff)
			}
			if tt.xri != "" {
				r.Header.Set("X-Real-IP", tt.xri)
			}
			if got := clientIP(r); got != tt.want {
				t.Errorf("clientIP = %q, want %q", got, tt.want)
			}
		})
	}

	// Two IPv6 clients in the same /16 must not share a rate limit bucket.
	a := httptest.NewRequest("POST", "/", nil)
	a.RemoteAddr = "[2001:db8::1]:443"
	b := httptest.NewRequest("POST", "/", nil)
	b.RemoteAddr = "[2001:db8::2]:443"
	if clientIP(a) == clientIP(b) {
		t.Errorf("distinct IPv6 clients collapsed to %q", clientIP(a))
	}
}

// A failed login records the typed username verbatim, so the raw audit line
// must stay valid JSON with exactly the fields given, whatever that string is.
func TestAuthEventRawLogEscapesInput(t *testing.T) {
	forged := `x","event":"login_success","user":"admin`
	raw := authEventRawLog("login_failed", forged, "203.0.113.7", `bad "quote" \ and
newline`)

	var got map[string]string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("raw log is not JSON: %v: %s", err, raw)
	}
	want := map[string]string{
		"event":  "login_failed",
		"user":   forged,
		"src_ip": "203.0.113.7",
		"detail": "bad \"quote\" \\ and\nnewline",
	}
	if len(got) != len(want) {
		t.Fatalf("raw log has fields %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("field %s = %q, want %q", k, got[k], v)
		}
	}

	// Benign input keeps the shape existing rows have.
	if got := authEventRawLog("login_success", "alice", "10.0.0.1", ""); got != `{"event":"login_success","user":"alice","src_ip":"10.0.0.1","detail":""}` {
		t.Errorf("raw log shape changed: %s", got)
	}
}
