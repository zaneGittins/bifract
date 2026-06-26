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
