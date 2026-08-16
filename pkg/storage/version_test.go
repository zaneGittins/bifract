package storage

import "testing"

func TestParseServerVersion(t *testing.T) {
	for _, tc := range []struct {
		in                  string
		major, minor, patch int
	}{
		{"26.6.2.81", 26, 6, 2},
		// The image tags the installer pins carry a suffix; the floor check must
		// read those too, so the installer and the runtime can be compared.
		{"26.6.2.81-alpine", 26, 6, 2},
		{"25.6", 25, 6, 0},
		{" 26.6.2 ", 26, 6, 2},
		{"26.6.2+build7", 26, 6, 2},
	} {
		t.Run(tc.in, func(t *testing.T) {
			v, err := ParseServerVersion(tc.in)
			if err != nil {
				t.Fatalf("ParseServerVersion: %v", err)
			}
			if v.Major != tc.major || v.Minor != tc.minor || v.Patch != tc.patch {
				t.Errorf("= %d.%d.%d, want %d.%d.%d", v.Major, v.Minor, v.Patch, tc.major, tc.minor, tc.patch)
			}
		})
	}
}

func TestParseServerVersionRejects(t *testing.T) {
	for _, in := range []string{"", "   ", "26", "abc", "26.x"} {
		if _, err := ParseServerVersion(in); err == nil {
			t.Errorf("ParseServerVersion(%q) succeeded, want an error", in)
		}
	}
}

// Compare deliberately ignores the patch component: ClickHouse patch releases do
// not add the features a floor exists to guarantee, and comparing them would
// reject a perfectly capable server over a build number.
func TestServerVersionCompare(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"26.6.2.81", "25.6", 1},
		{"25.6.0", "25.6.9", 0},
		{"25.5.99", "25.6", -1},
		{"24.12", "25.1", -1},
		{"26.6", "26.6", 0},
	} {
		t.Run(tc.a+" vs "+tc.b, func(t *testing.T) {
			a, err := ParseServerVersion(tc.a)
			if err != nil {
				t.Fatal(err)
			}
			b, err := ParseServerVersion(tc.b)
			if err != nil {
				t.Fatal(err)
			}
			if got := a.Compare(b); got != tc.want {
				t.Errorf("Compare = %d, want %d", got, tc.want)
			}
		})
	}
}

// The version Bifract ships and pins must itself satisfy the floor, or every
// install would warn or refuse on a correct deployment.
func TestPinnedVersionMeetsFloor(t *testing.T) {
	v, err := ParseServerVersion("26.6.2.81-alpine")
	if err != nil {
		t.Fatal(err)
	}
	if v.Compare(MinClickHouseVersion) < 0 {
		t.Errorf("the pinned ClickHouse image %s is below MinClickHouseVersion %s", v, MinClickHouseVersion)
	}
}
