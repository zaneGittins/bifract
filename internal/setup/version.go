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

	pa := parseVersion(a)
	pb := parseVersion(b)
	for i := 0; i < 3; i++ {
		if pa[i] < pb[i] {
			return -1
		}
		if pa[i] > pb[i] {
			return 1
		}
	}
	return 0
}

func parseVersion(v string) [3]int {
	v = strings.TrimPrefix(v, "v")
	parts := strings.SplitN(v, ".", 3)
	var result [3]int
	for i := 0; i < len(parts) && i < 3; i++ {
		n, _ := strconv.Atoi(strings.Split(parts[i], "-")[0])
		result[i] = n
	}
	return result
}
