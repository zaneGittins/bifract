package setup

import "testing"

// CompareVersions gates both upgrade paths: a result >= 0 against the running
// binary means "already up to date", so an ordering bug either skips a real
// upgrade or re-renders an install onto an older release.
func TestCompareVersions(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"v1.2.3", "v1.2.3", 0},
		{"1.2.3", "v1.2.3", 0},
		{"v1.2.3", "v1.2.4", -1},
		{"v1.2.10", "v1.2.9", 1},
		{"v1.10.0", "v1.9.9", 1},
		{"v2.0.0", "v1.99.99", 1},
		{"v1.2", "v1.2.0", 0},
		// A pre-release sorts below its release.
		{"v0.0.2-dev.1", "v0.0.2", -1},
		{"v0.0.2", "v0.0.2-dev.1", 1},
		{"v0.0.2-dev.1", "v0.0.1", 1},
		// Numeric pre-release segments compare as numbers, not text.
		{"v1.0.0-dev.2", "v1.0.0-dev.10", -1},
		{"v1.0.0-rc.1", "v1.0.0-rc.1", 0},
		// Text segments compare lexically; numeric sorts before text.
		{"v1.0.0-alpha", "v1.0.0-beta", -1},
		{"v1.0.0-1", "v1.0.0-alpha", -1},
		{"v1.0.0-alpha", "v1.0.0-1", 1},
		// Fewer segments first.
		{"v1.0.0-dev", "v1.0.0-dev.1", -1},
		// A development build is newer than any release, so it is never "upgraded".
		{"dev", "v9.9.9", 1},
		{"v9.9.9", "dev", -1},
		{"", "v0.0.1", 1},
		{"dev", "", 0},
	}
	for _, tt := range tests {
		if got := CompareVersions(tt.a, tt.b); got != tt.want {
			t.Errorf("CompareVersions(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// Sorting needs a consistent order: swapping the arguments must flip the sign.
func TestCompareVersionsIsAntisymmetric(t *testing.T) {
	versions := []string{"dev", "v0.0.1", "v0.0.2-dev.1", "v0.0.2-dev.10", "v0.0.2-dev.2", "v0.0.2",
		"v0.1.0", "v1.0.0-alpha", "v1.0.0-alpha.1", "v1.0.0-beta", "v1.0.0-1", "v1.0.0", "v1.0.10"}
	for _, a := range versions {
		for _, b := range versions {
			if x, y := CompareVersions(a, b), CompareVersions(b, a); x != -y {
				t.Errorf("CompareVersions(%q, %q) = %d but (%q, %q) = %d", a, b, x, b, a, y)
			}
		}
	}
}
