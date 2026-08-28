package mfa

import (
	"errors"
	"testing"
)

// rfc4226Secret is the shared secret from RFC 4226 Appendix D.
const rfc4226Secret = "12345678901234567890"

// TestCodeRFC4226 checks the HOTP core against the RFC 4226 test vectors. TOTP
// is HOTP over a time-derived counter, so these pin the whole computation.
func TestCodeRFC4226(t *testing.T) {
	secret := b32.EncodeToString([]byte(rfc4226Secret))
	want := []string{
		"755224", "287082", "359152", "969429", "338314",
		"254676", "287922", "162583", "399871", "520489",
	}
	for counter, expected := range want {
		got, err := code(secret, int64(counter))
		if err != nil {
			t.Fatalf("counter %d: %v", counter, err)
		}
		if got != expected {
			t.Errorf("counter %d: got %s, want %s", counter, got, expected)
		}
	}
}

// TestCounterMatchesRFC6238 checks the time-to-step conversion against the
// RFC 6238 Appendix B times.
func TestCounterMatchesRFC6238(t *testing.T) {
	cases := []struct {
		unix int64
		want int64
	}{
		{59, 1},
		{1111111109, 37037036},
		{1111111111, 37037037},
		{1234567890, 41152263},
		{2000000000, 66666666},
	}
	for _, c := range cases {
		if got := Counter(c.unix); got != c.want {
			t.Errorf("Counter(%d) = %d, want %d", c.unix, got, c.want)
		}
	}
}

func TestValidateAcceptsSkew(t *testing.T) {
	secret, err := GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	const now int64 = 1_700_000_000

	for _, offset := range []int64{-1, 0, 1} {
		c, err := code(secret, Counter(now)+offset)
		if err != nil {
			t.Fatal(err)
		}
		matched, err := Validate(secret, c, now, 0)
		if err != nil {
			t.Fatalf("offset %d rejected: %v", offset, err)
		}
		if matched != Counter(now)+offset {
			t.Errorf("offset %d matched step %d, want %d", offset, matched, Counter(now)+offset)
		}
	}
}

func TestValidateRejectsOutsideWindow(t *testing.T) {
	secret, _ := GenerateSecret()
	const now int64 = 1_700_000_000

	for _, offset := range []int64{-2, 2, 10} {
		c, _ := code(secret, Counter(now)+offset)
		if _, err := Validate(secret, c, now, 0); !errors.Is(err, ErrInvalidCode) {
			t.Errorf("offset %d: got %v, want ErrInvalidCode", offset, err)
		}
	}
}

// A code stays valid for its whole step, so without a spent-counter check it can
// be replayed by anyone who observes it.
func TestValidateRejectsReplay(t *testing.T) {
	secret, _ := GenerateSecret()
	const now int64 = 1_700_000_000

	c, _ := code(secret, Counter(now))
	matched, err := Validate(secret, c, now, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Validate(secret, c, now, matched); !errors.Is(err, ErrCodeReused) {
		t.Errorf("replay accepted: %v", err)
	}
}

// An observed code must not be replayable at an earlier step either: accepting
// step N must spend everything up to N, not just N itself.
func TestValidateRejectsBackwardsStep(t *testing.T) {
	secret, _ := GenerateSecret()
	const now int64 = 1_700_000_000

	previous, _ := code(secret, Counter(now)-1)
	if _, err := Validate(secret, previous, now, Counter(now)); !errors.Is(err, ErrCodeReused) {
		t.Errorf("stale step accepted: %v", err)
	}
}

func TestValidateRejectsMalformed(t *testing.T) {
	secret, _ := GenerateSecret()
	for _, in := range []string{"", "12345", "1234567", "abcdef"} {
		if _, err := Validate(secret, in, 1_700_000_000, 0); err == nil {
			t.Errorf("accepted malformed code %q", in)
		}
	}
}

func TestProvisioningURI(t *testing.T) {
	uri := ProvisioningURI("Bifract", "alice", "JBSWY3DPEHPK3PXP")
	want := "otpauth://totp/Bifract:alice?algorithm=SHA1&digits=6&issuer=Bifract&period=30&secret=JBSWY3DPEHPK3PXP"
	if uri != want {
		t.Errorf("got %s, want %s", uri, want)
	}
}
