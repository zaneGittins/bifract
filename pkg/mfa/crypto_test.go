package mfa

import (
	"encoding/base64"
	"errors"
	"testing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", "test-pepper-value")

	secret, err := GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := Seal(secret)
	if err != nil {
		t.Fatal(err)
	}
	if sealed == secret {
		t.Fatal("secret was not encrypted")
	}
	got, err := Open(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if got != secret {
		t.Errorf("got %q, want %q", got, secret)
	}
}

// Two seals of the same secret must differ, or the ciphertext leaks that two
// users enrolled the same secret and the nonce is being reused.
func TestSealIsRandomized(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", "test-pepper-value")

	a, err := Seal("JBSWY3DPEHPK3PXP")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Seal("JBSWY3DPEHPK3PXP")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Error("identical ciphertexts for the same plaintext")
	}
}

func TestOpenRejectsWrongPepper(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", "pepper-one")
	sealed, err := Seal("JBSWY3DPEHPK3PXP")
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("BIFRACT_PASSWORD_PEPPER", "pepper-two")
	if _, err := Open(sealed); err == nil {
		t.Error("decrypted with the wrong pepper")
	}
}

// Tampering happens to the ciphertext, not to its base64 text. The final
// quantum of a base64 string carries bits the decoder ignores, so flipping a
// bit in the encoded character often decodes to the very same bytes: the value
// is then not tampered with at all, and GCM is right to accept it.
func TestOpenRejectsTamperedCiphertext(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", "test-pepper-value")
	sealed, err := Seal("JBSWY3DPEHPK3PXP")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		t.Fatal(err)
	}
	key, err := deriveKey()
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := newGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	nonceSize := gcm.NonceSize()

	// Every byte matters: the nonce, the ciphertext, and the tag.
	for _, at := range []struct {
		name  string
		index int
	}{
		{"nonce", 0},
		{"ciphertext", nonceSize},
		{"tag", len(raw) - 1},
	} {
		tampered := append([]byte(nil), raw...)
		tampered[at.index] ^= 0x01
		if _, err := Open(base64.StdEncoding.EncodeToString(tampered)); err == nil {
			t.Errorf("accepted a secret with a flipped %s byte", at.name)
		}
	}

	// A truncated value must not open either, however it is cut.
	for _, cut := range []int{1, nonceSize, len(raw) - 1} {
		if _, err := Open(base64.StdEncoding.EncodeToString(raw[:cut])); err == nil {
			t.Errorf("accepted a secret truncated to %d bytes", cut)
		}
	}
}

func TestNoKeyWithoutPepper(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", "")

	if KeyAvailable() {
		t.Error("KeyAvailable true without a pepper")
	}
	if _, err := Seal("JBSWY3DPEHPK3PXP"); !errors.Is(err, ErrNoKey) {
		t.Errorf("got %v, want ErrNoKey", err)
	}
}
