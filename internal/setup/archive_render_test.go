package setup

import (
	"strings"
	"testing"
)

// TestArchiveRendersK8s verifies the archive tee env, archiver sidecar, and the
// always-on maintain Deployment render into valid manifests with no unresolved fields.
func TestArchiveRendersK8s(t *testing.T) {
	data := k8sTemplateData{
		ImageTag:       "test",
		Domain:         "example.com",
		CHHostsList:    "host1",
		IngestReplicas: 1,
		BifractRes:     ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		ArchiverRes:    ResourceProfile{"500m", "1", "512Mi", "1Gi"},
	}

	// The app deployment renders cleanly and keeps the archive-backend display hint,
	// but no longer carries the spool or the archiver sidecar (those moved to the
	// ingest tier).
	dep, err := renderK8sTemplate("templates/k8s/bifract-deployment.yaml.tmpl", data)
	if err != nil {
		t.Fatalf("deployment render failed: %v", err)
	}
	if strings.Contains(dep, "<no value>") {
		t.Fatalf("deployment produced <no value> (missing field)")
	}
	if !strings.Contains(dep, "BIFRACT_ARCHIVE_ENABLED") {
		t.Errorf("app deployment missing BIFRACT_ARCHIVE_ENABLED display hint")
	}
	for _, unwanted := range []string{"name: bifract-archiver", "BIFRACT_ARCHIVE_SPOOL_PATH"} {
		if strings.Contains(dep, unwanted) {
			t.Errorf("app deployment should no longer contain %q (moved to ingest tier)", unwanted)
		}
	}

	// The spool tee + archiver sidecar now live in the ingest deployment.
	ing, err := renderK8sTemplate("templates/k8s/bifract-ingest-deployment.yaml.tmpl", data)
	if err != nil {
		t.Fatalf("ingest deployment render failed: %v", err)
	}
	if strings.Contains(ing, "<no value>") {
		t.Fatalf("ingest deployment produced <no value> (missing field)")
	}
	for _, want := range []string{
		`args: ["ingest"]`,
		"BIFRACT_ARCHIVE_SPOOL_PATH",
		"value: /var/lib/bifract/spool",
		"name: bifract-archiver",
		"/bifract-archiver",
		"BIFRACT_ARCHIVE_BACKEND",
		"BIFRACT_ARCHIVE_S3_SECRET_KEY",
		"name: spool",
	} {
		if !strings.Contains(ing, want) {
			t.Errorf("rendered ingest deployment missing %q", want)
		}
	}

	// Maintenance now runs as an always-on, singleton Deployment (was a CronJob):
	// maintain-loop services scheduled passes plus admin "Run now" requests, with
	// no Kubernetes API/RBAC needed (the trigger is a Postgres row).
	maint, err := renderK8sTemplate("templates/k8s/bifract-archive-maintain-deployment.yaml.tmpl", data)
	if err != nil {
		t.Fatalf("maintain deployment render failed: %v", err)
	}
	if strings.Contains(maint, "<no value>") {
		t.Fatalf("maintain deployment produced <no value> (missing field)")
	}
	for _, want := range []string{
		"kind: Deployment",
		"replicas: 1",
		"type: Recreate",
		`command: ["/bifract-archiver", "maintain-loop"]`,
		"BIFRACT_ARCHIVE_MAINTAIN_INTERVAL",
		"component: archive-maintain",
	} {
		if !strings.Contains(maint, want) {
			t.Errorf("rendered maintain deployment missing %q", want)
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
