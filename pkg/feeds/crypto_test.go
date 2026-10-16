package feeds

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
	"sync"
	"testing"
)

// setEncryptionKey configures BIFRACT_FEED_ENCRYPTION_KEY for one test and
// forces the next getKey to reload it. Empty means no key. The key is loaded
// once per process in production, so tests using this must not run in parallel.
func setEncryptionKey(t *testing.T, raw string) {
	t.Helper()
	reset := func() {
		encryptionKey = nil
		encryptionKeyOnce = sync.Once{}
	}
	reset()
	t.Setenv("BIFRACT_FEED_ENCRYPTION_KEY", raw)
	t.Cleanup(reset)
}

var testKey = []byte("0123456789abcdef0123456789abcdef") // 32 bytes

func TestEncryptionKeyFormats(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []byte // nil means no key configured
	}{
		{"unset", "", nil},
		{"hex", hex.EncodeToString(testKey), testKey},
		{"hex with whitespace", "  " + hex.EncodeToString(testKey) + "\n", testKey},
		{"base64", base64.StdEncoding.EncodeToString(testKey), testKey},
		{"raw 32 chars", string(testKey), testKey},
		// A short or malformed key must not silently become a weak key.
		{"too short", "short-key", nil},
		{"hex of 20 bytes", hex.EncodeToString(testKey[:20]), nil},
		{"33 raw chars", string(testKey) + "x", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEncryptionKey(t, tt.raw)
			key, err := getKey()
			if tt.want == nil {
				if err == nil || IsEncryptionConfigured() {
					t.Fatalf("key %q accepted as %x", tt.raw, key)
				}
				return
			}
			if err != nil || string(key) != string(tt.want) {
				t.Fatalf("getKey() = %x, %v; want %x", key, err, tt.want)
			}
			if !IsEncryptionConfigured() {
				t.Error("IsEncryptionConfigured() = false with a valid key")
			}
		})
	}
}

func TestTokenRoundTrip(t *testing.T) {
	setEncryptionKey(t, hex.EncodeToString(testKey))
	const token = "ghp_abcdef1234567890"

	enc, err := EncryptToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(enc, "enc:") || strings.Contains(enc, token) {
		t.Fatalf("EncryptToken = %q, want an enc: ciphertext not containing the token", enc)
	}
	// A fresh nonce per call: equal tokens must not produce equal ciphertexts.
	enc2, _ := EncryptToken(token)
	if enc == enc2 {
		t.Error("two encryptions of the same token are identical (nonce reuse)")
	}

	for _, c := range []string{enc, enc2} {
		got, err := DecryptToken(c)
		if err != nil || got != token {
			t.Fatalf("DecryptToken = %q, %v; want %q", got, err, token)
		}
	}

	if got, err := EncryptToken(""); err != nil || got != "" {
		t.Errorf("EncryptToken(\"\") = %q, %v; empty stays empty so 'no token' survives", got, err)
	}
	if got, err := DecryptToken(""); err != nil || got != "" {
		t.Errorf("DecryptToken(\"\") = %q, %v", got, err)
	}
}

func TestDecryptTokenFailures(t *testing.T) {
	setEncryptionKey(t, hex.EncodeToString(testKey))
	enc, err := EncryptToken("secret-token")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(enc, "enc:"))
	raw[len(raw)-1] ^= 0xff
	tampered := "enc:" + base64.StdEncoding.EncodeToString(raw)

	tests := []struct {
		name, stored, wantErr string
	}{
		{"tampered ciphertext", tampered, "decrypt"},
		{"not base64", "enc:!!!not-base64", "decode base64"},
		{"shorter than the nonce", "enc:" + base64.StdEncoding.EncodeToString([]byte("abc")), "too short"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecryptToken(tt.stored)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("DecryptToken = %q, %v; want error containing %q", got, err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "secret-token") {
				t.Errorf("error leaks the token: %v", err)
			}
		})
	}

	t.Run("a different key cannot decrypt", func(t *testing.T) {
		setEncryptionKey(t, hex.EncodeToString([]byte("ffffffffffffffffffffffffffffffff")))
		if got, err := DecryptToken(enc); err == nil {
			t.Fatalf("decrypted with the wrong key: %q", got)
		}
	})
}

// Without a key, tokens are stored as plaintext (NewManager warns about it) and
// legacy plaintext rows keep working; an encrypted row is refused outright
// rather than handed to git as a garbage credential.
func TestTokenWithoutKey(t *testing.T) {
	setEncryptionKey(t, hex.EncodeToString(testKey))
	enc, _ := EncryptToken("tok")

	setEncryptionKey(t, "")
	if got, err := EncryptToken("plain-tok"); err != nil || got != "plain-tok" {
		t.Errorf("EncryptToken without a key = %q, %v; want plaintext passthrough", got, err)
	}
	if got, err := DecryptToken("plain-tok"); err != nil || got != "plain-tok" {
		t.Errorf("DecryptToken(plaintext) = %q, %v", got, err)
	}
	if got, err := DecryptToken(enc); err == nil || !strings.Contains(err.Error(), "no decryption key") {
		t.Errorf("DecryptToken(enc:...) without a key = %q, %v; want refusal", got, err)
	}
}
