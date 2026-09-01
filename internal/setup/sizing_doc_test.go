package setup

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// parseCPUQuantity converts a k8s CPU quantity ("500m", "2") to cores.
func parseCPUQuantity(q string) float64 {
	q = strings.TrimSpace(strings.Trim(q, `"`))
	if strings.HasSuffix(q, "m") {
		n, _ := strconv.ParseFloat(strings.TrimSuffix(q, "m"), 64)
		return n / 1000
	}
	n, _ := strconv.ParseFloat(q, 64)
	return n
}

// | Dev | ~1-10 GB/day | 1 | 4 vCPU / 8GB | 2 / 3 | 4Gi / 5Gi |
var sizingDocRow = regexp.MustCompile(
	`^\|\s*([\w-]+)\s*\|[^|]*\|\s*(\d+)\s*\|[^|]*\|\s*([\d.]+)\s*/\s*([\d.]+)\s*\|\s*([\d.]+)Gi\s*/\s*([\d.]+)Gi\s*\|`)

// The sizing doc's profile table is hand-maintained while the numbers behind it
// live in sizeProfiles, so it silently goes stale the first time a profile is
// edited. Operators provision node pools from this table, so a wrong figure
// means pods that will not schedule. Assert it restates the manifests exactly.
func TestSizingDocMatchesProfiles(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "getting-started", "sizing.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sizing doc: %v", err)
	}
	content := string(data)

	section := strings.Index(content, "## Resource Profiles")
	if section < 0 {
		t.Fatal("sizing doc has no 'Resource Profiles' section; the table was renamed or removed")
	}

	documented := map[string][5]float64{}
	for _, line := range strings.Split(content[section:], "\n") {
		m := sizingDocRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		var v [5]float64
		for i := range v {
			v[i], _ = strconv.ParseFloat(m[i+2], 64)
		}
		documented[m[1]] = v
	}

	const Gi = 1 << 30
	for _, p := range sizeProfiles {
		t.Run(p.Name, func(t *testing.T) {
			doc, ok := documented[p.Name]
			if !ok {
				t.Fatalf("profile is missing from the sizing doc's profile table")
			}

			for _, c := range []struct {
				what      string
				doc, real float64
			}{
				{"shards", doc[0], float64(p.CHShards)},
				{"ClickHouse CPU request", doc[1], parseCPUQuantity(p.ClickHouse.CPURequest)},
				{"ClickHouse CPU limit", doc[2], parseCPUQuantity(p.ClickHouse.CPULimit)},
				{"ClickHouse memory request (Gi)", doc[3], float64(parseK8sQuantityBytes(p.ClickHouse.MemRequest)) / Gi},
				{"ClickHouse memory limit (Gi)", doc[4], float64(parseK8sQuantityBytes(p.ClickHouse.MemLimit)) / Gi},
			} {
				const eps = 0.01
				if diff := c.doc - c.real; diff > eps || diff < -eps {
					t.Errorf("doc %s is %g, manifests set %g", c.what, c.doc, c.real)
				}
			}
		})
	}
}
