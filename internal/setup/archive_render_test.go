package setup

import (
	"strings"
	"testing"
)

// TestArchiveRendersK8s verifies the archive tee env, archiver sidecar, and the
// maintain CronJob render into valid manifests with no unresolved fields.
func TestArchiveRendersK8s(t *testing.T) {
	data := k8sTemplateData{
		ImageTag:    "test",
		Domain:      "example.com",
		CHHostsList: "host1",
		BifractRes:  ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		ArchiverRes: ResourceProfile{"500m", "1", "512Mi", "1Gi"},
	}

	dep, err := renderK8sTemplate("templates/k8s/bifract-deployment.yaml.tmpl", data)
	if err != nil {
		t.Fatalf("deployment render failed: %v", err)
	}
	if strings.Contains(dep, "<no value>") {
		t.Fatalf("deployment produced <no value> (missing field)")
	}
	for _, want := range []string{
		"BIFRACT_ARCHIVE_SPOOL_PATH",
		"value: /var/lib/bifract/spool",
		"name: bifract-archiver",
		"/bifract-archiver",
		"BIFRACT_ARCHIVE_BACKEND",
		"BIFRACT_ARCHIVE_S3_SECRET_KEY",
		"name: spool",
	} {
		if !strings.Contains(dep, want) {
			t.Errorf("rendered deployment missing %q", want)
		}
	}

	cron, err := renderK8sTemplate("templates/k8s/bifract-archive-maintain-cronjob.yaml.tmpl", data)
	if err != nil {
		t.Fatalf("cronjob render failed: %v", err)
	}
	if strings.Contains(cron, "<no value>") {
		t.Fatalf("cronjob produced <no value> (missing field)")
	}
	for _, want := range []string{
		"kind: CronJob",
		"concurrencyPolicy: Forbid",
		`command: ["/bifract-archiver", "maintain"]`,
	} {
		if !strings.Contains(cron, want) {
			t.Errorf("rendered cronjob missing %q", want)
		}
	}

	sec, err := renderK8sTemplate("templates/k8s/bifract-secrets.yaml.tmpl", k8sTemplateData{
		UserSecrets: map[string]string{"ARCHIVE_ENABLED": "true", "ARCHIVE_BACKEND": "s3"},
	})
	if err != nil {
		t.Fatalf("secrets render failed: %v", err)
	}
	for _, want := range []string{"ARCHIVE_ENABLED:", "ARCHIVE_S3_ACCESS_KEY:", "ARCHIVE_AZURE_CONTAINER:"} {
		if !strings.Contains(sec, want) {
			t.Errorf("rendered secrets missing %q", want)
		}
	}
}
