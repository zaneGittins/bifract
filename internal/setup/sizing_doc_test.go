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

// nonClickHouseFootprint sums every component the size profile sizes except
// ClickHouse and Keeper, which get their own shard nodes. Bifract counts twice:
// the profile sizes both the app deployment and the ingest container. One ingest
// replica, matching the doc's stated basis.
func nonClickHouseFootprint(p SizeProfile) (cpuReq, cpuLim float64, memReq, memLim int64) {
	type entry struct {
		res   ResourceProfile
		count int
	}
	for _, e := range []entry{
		{p.Bifract, 2},
		{p.Archiver, 1},
		{p.ArchiveMaintain, 1},
		{p.Postgres, 1},
		{p.Caddy, 1},
		{p.CaddyShipper, 1},
		{p.LiteLLM, 1},
	} {
		n := float64(e.count)
		cpuReq += parseCPUQuantity(e.res.CPURequest) * n
		cpuLim += parseCPUQuantity(e.res.CPULimit) * n
		memReq += parseK8sQuantityBytes(e.res.MemRequest) * int64(e.count)
		memLim += parseK8sQuantityBytes(e.res.MemLimit) * int64(e.count)
	}
	return
}

var sizingDocRow = regexp.MustCompile(
	`^\|\s*([\w-]+)\s*\|\s*([\d.]+)\s*/\s*([\d.]+)\s*\|\s*([\d.]+)Gi\s*/\s*([\d.]+)Gi\s*\|`)

// The sizing doc's non-ClickHouse capacity table is hand-maintained while the
// numbers behind it live in sizeProfiles, so it silently goes stale the first
// time a profile is edited -- which is exactly what happened when the archiver
// got its own resources. Under-stating capacity is the harmful direction: an
// operator provisions to the doc and the pods will not schedule. Assert the doc
// is never below what the manifests actually ask for.
func TestSizingDocCoversActualFootprint(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "getting-started", "sizing.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sizing doc: %v", err)
	}
	content := string(data)

	section := strings.Index(content, "### Capacity for everything else")
	if section < 0 {
		t.Fatal("sizing doc has no 'Capacity for everything else' section; the table was renamed or removed")
	}

	documented := map[string][4]float64{}
	for _, line := range strings.Split(content[section:], "\n") {
		m := sizingDocRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		var v [4]float64
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
				t.Fatalf("profile is missing from the sizing doc's capacity table")
			}
			cpuReq, cpuLim, memReq, memLim := nonClickHouseFootprint(p)

			for _, c := range []struct {
				what      string
				doc, real float64
			}{
				{"CPU request", doc[0], cpuReq},
				{"CPU limit", doc[1], cpuLim},
				{"memory request (Gi)", doc[2], float64(memReq) / Gi},
				{"memory limit (Gi)", doc[3], float64(memLim) / Gi},
			} {
				// Float tolerance: the real figures are sums of fractional CPU
				// quantities, so an exact-looking 8 lands at 8.0000000001.
				const eps = 0.01
				if c.doc < c.real-eps {
					t.Errorf("doc understates %s: says %g, manifests need %.1f", c.what, c.doc, c.real)
				}
				// Catch a doc left far above reality after a profile shrinks, which
				// would have operators over-provisioning for no reason.
				if c.real > 0 && c.doc > c.real*1.6 {
					t.Errorf("doc overstates %s: says %g, manifests need only %.1f", c.what, c.doc, c.real)
				}
			}
		})
	}
}
