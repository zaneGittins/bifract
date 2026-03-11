package setup

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math/big"

	"golang.org/x/crypto/bcrypt"
)

const passwordChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"

func GeneratePassword(length int) (string, error) {
	b := make([]byte, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(passwordChars))))
		if err != nil {
			return "", err
		}
		b[i] = passwordChars[n.Int64()]
	}
	return string(b), nil
}

func GenerateAlphanumeric(length int) (string, error) {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		if err != nil {
			return "", err
		}
		b[i] = chars[n.Int64()]
	}
	return string(b), nil
}

// GenerateHexKey generates n cryptographically random bytes and returns them hex-encoded.
func GenerateHexKey(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func HashPassword(password, pepper string) (string, error) {
	peppered := applyPepper(password, pepper)
	hash, err := bcrypt.GenerateFromPassword([]byte(peppered), 12)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func applyPepper(password, pepper string) string {
	if pepper == "" {
		return password
	}
	mac := hmac.New(sha256.New, []byte(pepper))
	mac.Write([]byte(password))
	return hex.EncodeToString(mac.Sum(nil))
}
