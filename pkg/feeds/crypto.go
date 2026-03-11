package feeds

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

var (
	encryptionKey     []byte
	encryptionKeyOnce sync.Once
	errNoKey          = errors.New("no encryption key configured")
)

// loadEncryptionKey reads BIFRACT_FEED_ENCRYPTION_KEY from the environment.
// Accepts hex-encoded (64 chars) or base64-encoded (44 chars) 32-byte keys.
func loadEncryptionKey() {
	raw := strings.TrimSpace(os.Getenv("BIFRACT_FEED_ENCRYPTION_KEY"))
	if raw == "" {
		return
	}

	// Try hex first (64 hex chars = 32 bytes)
	if key, err := hex.DecodeString(raw); err == nil && len(key) == 32 {
		encryptionKey = key
		return
	}

	// Try base64 (44 chars padded = 32 bytes)
	if key, err := base64.StdEncoding.DecodeString(raw); err == nil && len(key) == 32 {
		encryptionKey = key
		return
	}

	// Try raw bytes if exactly 32 chars
	if len(raw) == 32 {
		encryptionKey = []byte(raw)
		return
	}
}

// getKey returns the encryption key, loading it once from the environment.
func getKey() ([]byte, error) {
	encryptionKeyOnce.Do(loadEncryptionKey)
	if len(encryptionKey) == 0 {
		return nil, errNoKey
	}
	return encryptionKey, nil
}

// IsEncryptionConfigured returns true if an encryption key is available.
func IsEncryptionConfigured() bool {
	key, _ := getKey()
	return key != nil
}

// EncryptToken encrypts plaintext using AES-256-GCM.
// Returns a base64-encoded string prefixed with "enc:" to distinguish from plaintext.
// If no encryption key is configured, returns the plaintext as-is.
func EncryptToken(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}

	key, err := getKey()
	if err != nil {
		// No encryption key configured, store as plaintext
		return plaintext, nil
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := aesGCM.Seal(nonce, nonce, []byte(plaintext), nil)
	return "enc:" + base64.StdEncoding.EncodeToString(ciphertext), nil
}

// DecryptToken decrypts a token that was encrypted with EncryptToken.
// If the token doesn't have the "enc:" prefix, it's returned as-is (plaintext fallback).
func DecryptToken(stored string) (string, error) {
	if stored == "" {
		return "", nil
	}

	if !strings.HasPrefix(stored, "enc:") {
		// Not encrypted, return as plaintext
		return stored, nil
	}

	key, err := getKey()
	if err != nil {
		return "", fmt.Errorf("token is encrypted but no decryption key configured")
	}

	data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, "enc:"))
	if err != nil {
		return "", fmt.Errorf("decode base64: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	nonceSize := aesGCM.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}

	return string(plaintext), nil
}
