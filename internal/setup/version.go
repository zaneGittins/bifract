package setup

import (
	"strconv"
	"strings"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// CompareVersions compares two semver strings. Returns -1, 0, or 1.
// Pre-release versions (e.g. v0.0.2-dev.1) sort below their release
// (v0.0.2-dev.1 < v0.0.2). Among pre-releases with the same core
// version, the pre-release string is compared lexically with numeric
// segments compared numerically (dev.2 < dev.10).
func CompareVersions(a, b string) int {
	// "dev" is always considered newer than any released version.
	aIsDev := a == "dev" || a == ""
	bIsDev := b == "dev" || b == ""
	if aIsDev && bIsDev {
		return 0
	}
	if aIsDev {
		return 1
	}
	if bIsDev {
		return -1
	}

	aMajMin, aPre := splitPrerelease(a)
	bMajMin, bPre := splitPrerelease(b)

	// Compare major.minor.patch first.
	for i := 0; i < 3; i++ {
		if aMajMin[i] < bMajMin[i] {
			return -1
		}
		if aMajMin[i] > bMajMin[i] {
			return 1
		}
	}

	// Same core version. A release (no pre-release) beats a pre-release.
	if aPre == "" && bPre == "" {
		return 0
	}
	if aPre == "" {
		return 1
	}
	if bPre == "" {
		return -1
	}

	// Both have pre-release: compare segment by segment (dot-separated).
	return comparePrerelease(aPre, bPre)
}

// splitPrerelease parses "v1.2.3-dev.4" into ([1,2,3], "dev.4").
func splitPrerelease(v string) ([3]int, string) {
	v = strings.TrimPrefix(v, "v")

	var pre string
	if idx := strings.IndexByte(v, '-'); idx >= 0 {
		pre = v[idx+1:]
		v = v[:idx]
	}

	parts := strings.SplitN(v, ".", 3)
	var result [3]int
	for i := 0; i < len(parts) && i < 3; i++ {
		result[i], _ = strconv.Atoi(parts[i])
	}
	return result, pre
}

// comparePrerelease compares dot-separated pre-release identifiers.
// Numeric segments are compared as integers, text segments lexically.
func comparePrerelease(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")

	n := len(aParts)
	if len(bParts) < n {
		n = len(bParts)
	}

	for i := 0; i < n; i++ {
		aNum, aErr := strconv.Atoi(aParts[i])
		bNum, bErr := strconv.Atoi(bParts[i])

		switch {
		case aErr == nil && bErr == nil:
			// Both numeric: compare as integers.
			if aNum < bNum {
				return -1
			}
			if aNum > bNum {
				return 1
			}
		case aErr == nil:
			// Numeric sorts before text (semver spec).
			return -1
		case bErr == nil:
			return 1
		default:
			// Both text: lexical comparison.
			if aParts[i] < bParts[i] {
				return -1
			}
			if aParts[i] > bParts[i] {
				return 1
			}
		}
	}

	// Fewer segments sorts first (dev < dev.1).
	if len(aParts) < len(bParts) {
		return -1
	}
	if len(aParts) > len(bParts) {
		return 1
	}
	return 0
}
