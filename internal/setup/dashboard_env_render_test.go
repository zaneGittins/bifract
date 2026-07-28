package setup

import (
	"strings"
	"testing"
)

// TestDashboardEnvRenders verifies the dashboard executor env vars render into
// the k8s deployment with preserved/fallback values and no unresolved fields.
func TestDashboardEnvRendersK8s(t *testing.T) {
	data := k8sTemplateData{
		ImageTag:            "test",
		Domain:              "example.com",
		CHHostsList:         "host1",
		DashboardTick:       7,
		DashboardMinRefresh: 20,
		DashboardWorkers:    9,
	}
	out, err := renderK8sTemplate("templates/k8s/bifract-deployment.yaml.tmpl", data)
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if strings.Contains(out, "<no value>") {
		t.Fatalf("template produced <no value> (missing field)")
	}
	for _, want := range []string{
		"BIFRACT_DASHBOARD_WORKERS",
		"value: \"9\"",
		"BIFRACT_DASHBOARD_MIN_REFRESH",
		"value: \"20\"",
		"BIFRACT_DASHBOARD_TICK",
		"value: \"7\"",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered deployment missing %q", want)
		}
	}
}

// TestIngestAsyncInsertDefaultOn verifies the k8s ingest deployment ships with
// BIFRACT_ASYNC_INSERT=1 by default. async_insert coalesces small inserts into fewer,
// larger parts, which is what keeps logs_histogram's per-block MV parts from outpacing
// merges under bursty ingest (the 2026-07-28 TOO_MANY_PARTS incident). Only the ingest
// tier runs InsertLogs, so it belongs on this manifest and not bifract-deployment.
func TestIngestAsyncInsertDefaultOn(t *testing.T) {
	out, err := renderK8sTemplate("templates/k8s/bifract-ingest-deployment.yaml.tmpl", k8sTemplateData{
		ImageTag: "test", Domain: "example.com", CHHostsList: "host1",
		IngestReplicas: 1, SpoolPVCSize: "32Gi", SpoolMaxBytes: 27487790694,
		BifractRes:  ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		ArchiverRes: ResourceProfile{"500m", "1", "512Mi", "1Gi"},
	})
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if strings.Contains(out, "<no value>") {
		t.Fatalf("template produced <no value> (missing field)")
	}
	if !strings.Contains(out, "BIFRACT_ASYNC_INSERT") {
		t.Fatalf("ingest deployment missing BIFRACT_ASYNC_INSERT env var")
	}
	// The name and its "1" value are on adjacent lines; assert they render together so a
	// future edit that blanks the value fails loudly.
	i := strings.Index(out, "BIFRACT_ASYNC_INSERT")
	if !strings.Contains(out[i:min(i+80, len(out))], "value: \"1\"") {
		t.Errorf("BIFRACT_ASYNC_INSERT is not set to \"1\" (default-on expected)")
	}
}

// TestWriteK8sManifestsFallback verifies the install path falls back to default
// dashboard values when K8sConfig leaves them zero.
func TestDashboardEnvFallback(t *testing.T) {
	if got := fallbackInt(0, defaultDashboardWorkers); got != defaultDashboardWorkers {
		t.Errorf("fallback workers = %d, want %d", got, defaultDashboardWorkers)
	}
	if got := fallbackInt(12, defaultDashboardWorkers); got != 12 {
		t.Errorf("fallback should preserve set value, got %d", got)
	}
}
