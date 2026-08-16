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

// chPasswordSpecials are special characters safe everywhere a generated
// ClickHouse password travels: a .env value, a YAML scalar, a Compose command
// argument, and a single-quoted SQL string literal.
//
// The set is deliberately conservative. Excluded and why:
//
//	$ = "        Compose interpolation and .env key splitting
//	' " \ `       quoting and escaping, in YAML and in SQL literals
//	# ;           comment introducers
//	! ^ * ? [ ]   shell history, globbing and pattern expansion
//	& | < > ( )   shell control operators
//	, : { } - ... YAML indicators are handled by quoting, but a value that
//	              survives an unquoted context too is one less thing to get wrong
const chPasswordSpecials = "@%+-_.~"

// GenerateClickHousePassword generates a password that satisfies a managed
// ClickHouse's complexity policy: at least one lowercase, uppercase, digit and
// special character.
//
// Plain alphanumeric is not enough. ClickHouse Cloud rejects CREATE USER with
// code 36 ("the password should contain at least 1 special character"), which
// silently costs the least-privilege ingest identity and the per-class query
// memory ceilings: both fall back rather than failing loudly.
func GenerateClickHousePassword(length int) (string, error) {
	const (
		lower  = "abcdefghijklmnopqrstuvwxyz"
		upper  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		digits = "0123456789"
	)
	if length < 8 {
		length = 8
	}
	all := lower + upper + digits + chPasswordSpecials

	// One from each class first, so the policy is satisfied by construction
	// rather than by chance, then fill and shuffle.
	out := make([]byte, 0, length)
	for _, class := range []string{lower, upper, digits, chPasswordSpecials} {
		c, err := randomByte(class)
		if err != nil {
			return "", err
		}
		out = append(out, c)
	}
	for len(out) < length {
		c, err := randomByte(all)
		if err != nil {
			return "", err
		}
		out = append(out, c)
	}
	for i := len(out) - 1; i > 0; i-- {
		j, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return "", err
		}
		out[i], out[j.Int64()] = out[j.Int64()], out[i]
	}
	return string(out), nil
}

func randomByte(chars string) (byte, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
	if err != nil {
		return 0, err
	}
	return chars[n.Int64()], nil
}

// ClickHousePasswordCompliant reports whether a password satisfies the
// complexity policy a managed ClickHouse enforces on CREATE USER.
//
// Existing installs carry a password generated before this was known, so the
// lifecycle paths rotate a non-compliant one rather than preserving it. That is
// safe because the app reconciles the ingest user's password on every startup
// (ALTER USER ... IDENTIFIED BY), so the new value reaches the server and both
// tiers read it from the same place. Without the rotation an upgrade would
// faithfully carry the broken credential forward and the ingest tier would keep
// failing to authenticate.
func ClickHousePasswordCompliant(pw string) bool {
	if len(pw) < 8 {
		return false
	}
	var lower, upper, digit, special bool
	for _, r := range pw {
		switch {
		case r >= 'a' && r <= 'z':
			lower = true
		case r >= 'A' && r <= 'Z':
			upper = true
		case r >= '0' && r <= '9':
			digit = true
		default:
			special = true
		}
	}
	return lower && upper && digit && special
}
