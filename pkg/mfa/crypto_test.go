package mfa

import (
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

func TestOpenRejectsTamperedCiphertext(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", "test-pepper-value")
	sealed, _ := Seal("JBSWY3DPEHPK3PXP")

	tampered := []byte(sealed)
	tampered[len(tampered)-2] ^= 0x01
	if _, err := Open(string(tampered)); err == nil {
		t.Error("accepted tampered ciphertext")
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
