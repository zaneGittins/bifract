package auth

import (
	"strings"
	"testing"

	"bifract/pkg/storage"
)

func TestGenerateRecoveryCodes(t *testing.T) {
	codes, hashes, err := generateRecoveryCodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(codes) != recoveryCodeCount || len(hashes) != recoveryCodeCount {
		t.Fatalf("got %d codes and %d hashes, want %d of each", len(codes), len(hashes), recoveryCodeCount)
	}

	seen := map[string]bool{}
	for i, code := range codes {
		groups := strings.Split(code, "-")
		if len(groups) != recoveryCodeGroups {
			t.Errorf("code %q has %d groups, want %d", code, len(groups), recoveryCodeGroups)
		}
		for _, g := range groups {
			if len(g) != recoveryCodeGroup {
				t.Errorf("code %q has a group of length %d, want %d", code, len(g), recoveryCodeGroup)
			}
			for _, r := range g {
				if !strings.ContainsRune(recoveryAlphabet, r) {
					t.Errorf("code %q contains %q, which is outside the alphabet", code, r)
				}
			}
		}
		if seen[code] {
			t.Errorf("duplicate code %q", code)
		}
		seen[code] = true

		if hashes[i] != hashRecoveryCode(code) {
			t.Errorf("code %q does not hash to its stored digest", code)
		}
		if strings.Contains(hashes[i], strings.ReplaceAll(code, "-", "")) {
			t.Errorf("hash for %q leaks the code", code)
		}
	}
}

// A user reading codes off a screen should not be defeated by capitals, spaces,
// or missing separators.
func TestRecoveryCodeNormalization(t *testing.T) {
	codes, _, err := generateRecoveryCodes()
	if err != nil {
		t.Fatal(err)
	}
	code := codes[0]
	want := hashRecoveryCode(code)

	variants := []string{
		strings.ToUpper(code),
		strings.ReplaceAll(code, "-", ""),
		strings.ReplaceAll(code, "-", " "),
		" " + code + " ",
		strings.ReplaceAll(code, "-", " - "),
	}
	for _, v := range variants {
		if got := hashRecoveryCode(v); got != want {
			t.Errorf("variant %q hashed differently", v)
		}
	}
}

// Normalization must not be so loose that different codes collide.
func TestRecoveryCodeNormalizationKeepsCodesDistinct(t *testing.T) {
	codes, _, err := generateRecoveryCodes()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, code := range codes {
		h := hashRecoveryCode(code)
		if prior, ok := seen[h]; ok {
			t.Errorf("%q and %q normalize to the same digest", prior, code)
		}
		seen[h] = code
	}
}

// Settings are not initialized in this test binary, so the requirement reads as
// off. These are the cases that must stay false regardless of the setting.
func TestMFAEnrollmentRequiredExemptions(t *testing.T) {
	cases := []struct {
		name string
		user *storage.User
	}{
		{"nil user", nil},
		{"already enrolled", &storage.User{Username: "a", TOTPEnrolled: true}},
		{"sso account", &storage.User{Username: "a", AuthProvider: "oidc"}},
		{"local account, requirement off", &storage.User{Username: "a", AuthProvider: "local"}},
	}
	for _, c := range cases {
		if mfaEnrollmentRequired(c.user) {
			t.Errorf("%s: enrollment should not be required", c.name)
		}
	}
}

// The gates are path allowlists, so an endpoint added to one by mistake would
// silently widen what a half-authenticated session can reach.
func TestGatePathsAreMinimal(t *testing.T) {
	wantVerify := []string{
		"/api/v1/auth/mfa/verify",
		"/api/v1/auth/user",
		"/api/v1/auth/logout",
	}
	if len(mfaVerifyPaths) != len(wantVerify) {
		t.Errorf("mfaVerifyPaths has %d entries, want %d", len(mfaVerifyPaths), len(wantVerify))
	}
	for _, p := range wantVerify {
		if !mfaVerifyPaths[p] {
			t.Errorf("%s missing from mfaVerifyPaths", p)
		}
	}

	// Nothing that reads or changes log data may appear in either allowlist.
	for _, p := range []string{"/api/v1/query", "/api/v1/fractals", "/api/v1/settings", "/api/v1/alerts"} {
		if mfaVerifyPaths[p] || mfaEnrollPaths[p] {
			t.Errorf("%s is reachable before authentication completes", p)
		}
	}

	wantEnroll := []string{
		"/api/v1/auth/mfa/enroll",
		"/api/v1/auth/mfa/confirm",
		"/api/v1/auth/mfa/status",
		"/api/v1/auth/user",
		"/api/v1/auth/logout",
	}
	if len(mfaEnrollPaths) != len(wantEnroll) {
		t.Errorf("mfaEnrollPaths has %d entries, want %d", len(mfaEnrollPaths), len(wantEnroll))
	}
	for _, p := range wantEnroll {
		if !mfaEnrollPaths[p] {
			t.Errorf("%s missing from mfaEnrollPaths", p)
		}
	}
}

// A pending password change must not collide with the enrollment gate: the
// change endpoint is not in the enrollment allowlist, so gating both at once
// would leave that user with no reachable next step.
func TestPasswordChangeIsNotInTheEnrollmentAllowlist(t *testing.T) {
	if mfaEnrollPaths["/api/v1/auth/change-password"] {
		t.Fatal("change-password is in mfaEnrollPaths; the middleware relies on it not being there")
	}
}
