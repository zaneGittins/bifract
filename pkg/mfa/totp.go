package mfa

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// RFC 6238 parameters. SHA-1 with 6 digits over 30 seconds is what every
// authenticator app assumes by default; changing any of them breaks enrollment
// on readers that ignore the URI parameters.
const (
	secretBytes   = 20 // 160-bit, the RFC 4226 recommendation
	Digits        = 6
	PeriodSeconds = 30
	// skewSteps accepts one step either side of now, covering clock drift
	// between the server and the user's phone.
	skewSteps = 1
)

var (
	// ErrInvalidCode is returned for a code that does not match any accepted step.
	ErrInvalidCode = errors.New("invalid verification code")
	// ErrCodeReused is returned for a code that was already accepted. Without
	// this a code stays valid for its whole window and is replayable.
	ErrCodeReused = errors.New("verification code already used")
)

var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

// GenerateSecret returns a new base32 TOTP secret.
func GenerateSecret() (string, error) {
	buf := make([]byte, secretBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return b32.EncodeToString(buf), nil
}

// code computes the TOTP value for one time step.
func code(secret string, counter int64) (string, error) {
	key, err := b32.DecodeString(strings.ToUpper(strings.TrimSpace(secret)))
	if err != nil {
		return "", fmt.Errorf("malformed secret: %w", err)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(counter))

	mac := hmac.New(sha1.New, key)
	mac.Write(buf[:])
	sum := mac.Sum(nil)

	offset := int(sum[len(sum)-1] & 0x0f)
	value := binary.BigEndian.Uint32(sum[offset:offset+4]) & 0x7fffffff

	mod := uint32(1)
	for i := 0; i < Digits; i++ {
		mod *= 10
	}
	return fmt.Sprintf("%0*d", Digits, value%mod), nil
}

// Counter returns the TOTP time step for a unix timestamp.
func Counter(unixSeconds int64) int64 {
	return unixSeconds / PeriodSeconds
}

// Validate checks a submitted code against the accepted window and returns the
// step it matched. lastCounter is the highest step already spent by this user;
// a match at or below it is a replay and is rejected.
func Validate(secret, submitted string, unixSeconds, lastCounter int64) (int64, error) {
	submitted = strings.TrimSpace(submitted)
	if len(submitted) != Digits {
		return 0, ErrInvalidCode
	}

	now := Counter(unixSeconds)
	matched := int64(-1)
	// Every step in the window is evaluated even after a match so that the work
	// does not depend on which step matched.
	for c := now - skewSteps; c <= now+skewSteps; c++ {
		expected, err := code(secret, c)
		if err != nil {
			return 0, err
		}
		if subtle.ConstantTimeCompare([]byte(expected), []byte(submitted)) == 1 {
			matched = c
		}
	}

	if matched < 0 {
		return 0, ErrInvalidCode
	}
	if matched <= lastCounter {
		return 0, ErrCodeReused
	}
	return matched, nil
}

// ProvisioningURI builds the otpauth:// URI an authenticator app scans.
//
// Usernames are not charset-restricted, so a long one made of characters that
// percent-escape can outgrow what a QR code holds. The account label is display
// only (the issuer parameter carries the identity), so it is trimmed to fit
// rather than leaving the user with no code to scan.
func ProvisioningURI(issuer, account, secret string) string {
	q := url.Values{}
	q.Set("secret", secret)
	q.Set("issuer", issuer)
	q.Set("algorithm", "SHA1")
	q.Set("digits", fmt.Sprintf("%d", Digits))
	q.Set("period", fmt.Sprintf("%d", PeriodSeconds))
	query := q.Encode()

	const prefix = "otpauth://totp/"
	budget := MaxPayloadBytes - len(prefix) - len("?") - len(query)

	label := url.PathEscape(issuer + ":" + account)
	for len(label) > budget && account != "" {
		runes := []rune(account)
		account = string(runes[:len(runes)-1])
		label = url.PathEscape(issuer + ":" + account)
	}
	return prefix + label + "?" + query
}
