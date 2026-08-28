package mfa

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"os"
	"strings"
)

// TOTP secrets are encrypted at rest with a key derived from the server pepper,
// so a Postgres dump alone is not a second factor bypass. HKDF gives the MFA key
// its own domain: the pepper is already an HMAC key for password hashing and the
// two uses must not share key material.
const keyInfo = "bifract-totp-secret-v1"

var (
	// ErrNoKey means BIFRACT_PASSWORD_PEPPER is unset, so secrets cannot be
	// encrypted. MFA is refused rather than storing them in the clear.
	ErrNoKey       = errors.New("no password pepper configured")
	errCiphertext  = errors.New("malformed encrypted secret")
	errDecryptFail = errors.New("could not decrypt secret")
)

// deriveKey builds the 32-byte AES key from the pepper. Derived per call rather
// than cached so a test or a restart picks up the current environment; HKDF is
// two HMACs and this is never on a hot path.
func deriveKey() ([]byte, error) {
	pepper := strings.TrimSpace(os.Getenv("BIFRACT_PASSWORD_PEPPER"))
	if pepper == "" {
		return nil, ErrNoKey
	}
	return hkdf.Key(sha256.New, []byte(pepper), nil, keyInfo, 32)
}

// KeyAvailable reports whether MFA secrets can be encrypted on this deployment.
// A configured pepper is the only precondition, so this stays cheap enough to
// call on the request path.
func KeyAvailable() bool {
	return strings.TrimSpace(os.Getenv("BIFRACT_PASSWORD_PEPPER")) != ""
}

// Seal encrypts a TOTP secret for storage. Output is base64(nonce || ciphertext).
func Seal(plaintext string) (string, error) {
	key, err := deriveKey()
	if err != nil {
		return "", err
	}
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Open decrypts a stored TOTP secret.
func Open(encoded string) (string, error) {
	key, err := deriveKey()
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", errCiphertext
	}
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", errCiphertext
	}
	nonce, ct := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", errDecryptFail
	}
	return string(plain), nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
