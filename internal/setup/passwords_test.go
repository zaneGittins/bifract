package setup

import (
	"strings"
	"testing"
)

// ClickHouse Cloud enforces a password policy on CREATE USER and rejects
// anything missing a character class with code 36. When that happens the ingest
// tier cannot create bifract_ingest and falls back to connecting as the admin
// user, which defeats the whole point of the least-privilege identity. Plain
// alphanumeric is not enough: it has no special character.
func TestGenerateClickHousePasswordMeetsComplexityPolicy(t *testing.T) {
	for i := 0; i < 200; i++ {
		pw, err := GenerateClickHousePassword(24)
		if err != nil {
			t.Fatalf("GenerateClickHousePassword: %v", err)
		}
		if len(pw) != 24 {
			t.Fatalf("length = %d, want 24", len(pw))
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
			case strings.ContainsRune(chPasswordSpecials, r):
				special = true
			default:
				t.Fatalf("password %q contains an unexpected character %q", pw, r)
			}
		}
		if !lower || !upper || !digit || !special {
			t.Fatalf("password %q lacks a class (lower=%v upper=%v digit=%v special=%v)", pw, lower, upper, digit, special)
		}
	}
}

// The special set must stay safe everywhere a generated password travels: a
// .env value read by Compose, a YAML manifest, a Compose command argument and a
// single-quoted SQL string literal.
func TestClickHousePasswordSpecialsAreTransportSafe(t *testing.T) {
	for _, bad := range []rune{'$', '=', '\'', '"', '\\', '`', '#', '\n'} {
		if strings.ContainsRune(chPasswordSpecials, bad) {
			t.Errorf("special set contains %q, which is unsafe in .env, YAML or SQL", bad)
		}
	}
}

func TestGenerateClickHousePasswordEnforcesMinimumLength(t *testing.T) {
	pw, err := GenerateClickHousePassword(3)
	if err != nil {
		t.Fatalf("GenerateClickHousePassword: %v", err)
	}
	if len(pw) < 8 {
		t.Errorf("length = %d, want the 8 character floor", len(pw))
	}
}

// An install created before the complexity policy was known carries an
// alphanumeric ingest password. Upgrade must rotate it, not preserve it: a
// preserved credential is one a managed ClickHouse refuses, and the ingest tier
// then cannot authenticate at all.
func TestClickHousePasswordCompliant(t *testing.T) {
	for _, tc := range []struct {
		name string
		pw   string
		want bool
	}{
		{"legacy alphanumeric", "aB3dEfGhIjKlMnOpQrStUvWx", false},
		{"no special", "Abcdefgh1", false},
		{"no uppercase", "abcdefgh1!", false},
		{"no digit", "Abcdefgh!", false},
		{"no lowercase", "ABCDEFGH1!", false},
		{"hex digest", "a3f9c1b2d4e5f60718293a4b5c6d7e8f", false},
		{"too short", "Ab1!", false},
		{"empty", "", false},
		{"compliant", "Abcdefg1!", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClickHousePasswordCompliant(tc.pw); got != tc.want {
				t.Errorf("ClickHousePasswordCompliant(%q) = %v, want %v", tc.pw, got, tc.want)
			}
		})
	}

	// Whatever the generator produces must always pass its own check.
	for i := 0; i < 100; i++ {
		pw, err := GenerateClickHousePassword(24)
		if err != nil {
			t.Fatal(err)
		}
		if !ClickHousePasswordCompliant(pw) {
			t.Fatalf("generated password %q fails the compliance check", pw)
		}
	}
}

// A generated password travels through a .env file, a YAML secret, a Compose
// interpolation and a SQL string literal. Any metacharacter that survives into
// one of those is a deployment that fails somewhere far from here, so the set is
// asserted rather than assumed.
func TestClickHousePasswordSpecialsAvoidMetacharacters(t *testing.T) {
	const unsafe = "$=\"'\\`#;!^*?[]&|<>(){},: \t\n"
	for _, r := range chPasswordSpecials {
		if strings.ContainsRune(unsafe, r) {
			t.Errorf("special %q is a shell, YAML or SQL metacharacter", r)
		}
	}
	if len(chPasswordSpecials) < 4 {
		t.Error("the special set is too small to be worth randomising over")
	}
}
