package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWriteColdStorageConfig verifies the docker storage.xml rendering for each
// backend, and the preserve-if-present semantics when disabled.
func TestWriteColdStorageConfig(t *testing.T) {
	t.Run("s3 renders s3 disk", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteColdStorageConfig(dir, "s3"); err != nil {
			t.Fatal(err)
		}
		got := readStorage(t, dir)
		if !strings.Contains(got, "<type>s3</type>") || !strings.Contains(got, "<tiered>") {
			t.Fatalf("s3 config missing expected content:\n%s", got)
		}
		if !strings.Contains(got, `from_env="BIFRACT_COLD_STORAGE_ENDPOINT"`) {
			t.Fatalf("s3 config should read endpoint from env:\n%s", got)
		}
	})

	t.Run("azure renders azure disk", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteColdStorageConfig(dir, "azure"); err != nil {
			t.Fatal(err)
		}
		got := readStorage(t, dir)
		if !strings.Contains(got, "<type>azure_blob_storage</type>") {
			t.Fatalf("azure config missing expected content:\n%s", got)
		}
	})

	t.Run("none writes inert and does not clobber existing", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteColdStorageConfig(dir, "none"); err != nil {
			t.Fatal(err)
		}
		got := readStorage(t, dir)
		if strings.Contains(got, "storage_configuration") {
			t.Fatalf("inert config should not contain a storage policy:\n%s", got)
		}
		// A subsequent disabled render must not overwrite an admin-populated file.
		path := filepath.Join(dir, "clickhouse", "config.d", "storage.xml")
		if err := os.WriteFile(path, []byte("CUSTOM"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := WriteColdStorageConfig(dir, ""); err != nil {
			t.Fatal(err)
		}
		if readStorage(t, dir) != "CUSTOM" {
			t.Fatal("disabled render clobbered an existing storage.xml")
		}
	})
}

func readStorage(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "clickhouse", "config.d", "storage.xml"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestColdStorageRenderMode verifies the k8s injection is driven by which cold
// credentials are present, not the on/off backend flag. This makes pausing
// tiering (backend=none) a safe soft-disable that keeps the policy defined.
func TestColdStorageRenderMode(t *testing.T) {
	cases := []struct {
		name    string
		secrets map[string]string
		want    string
	}{
		{"none", map[string]string{}, ""},
		{"s3 by endpoint", map[string]string{"COLD_STORAGE_ENDPOINT": "https://b/cold/"}, "s3"},
		{"azure by url", map[string]string{"AZURE_STORAGE_URL": "https://a.blob/"}, "azure"},
		{"paused keeps s3", map[string]string{"COLD_STORAGE_BACKEND": "none", "COLD_STORAGE_ENDPOINT": "https://b/cold/"}, "s3"},
		{"pg-backup s3 creds alone do not trigger", map[string]string{"S3_ACCESS_KEY": "AK", "S3_SECRET_KEY": "SK"}, ""},
	}
	for _, c := range cases {
		if got := coldStorageRenderMode(c.secrets); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

// TestK8sColdStorageRender verifies the ClickHouse manifest and secret render the
// storage Secret + volume mount only when a backend is selected.
func TestK8sColdStorageRender(t *testing.T) {
	base := k8sTemplateData{
		ImageTag:           "test",
		CHShards:           1,
		CHReplicas:         1,
		CHStorageStr:       "10Gi",
		CHPasswordHash:     "deadbeef",
		ClickHousePassword: "x",
		CH:                 ResourceProfile{CPURequest: "1", MemRequest: "1Gi", CPULimit: "2", MemLimit: "2Gi"},
		CHKeeper:           ResourceProfile{CPURequest: "1", MemRequest: "1Gi", CPULimit: "1", MemLimit: "1Gi"},
		UserSecrets: map[string]string{
			"COLD_STORAGE_ENDPOINT": "https://b.s3.amazonaws.com/cold/",
			"S3_ACCESS_KEY":         "AK",
			"S3_SECRET_KEY":         "SK",
			"S3_REGION":             "us-east-1",
			"AZURE_STORAGE_URL":     "https://acct.blob.core.windows.net/",
			"AZURE_CONTAINER":       "cold",
			"AZURE_STORAGE_ACCOUNT": "acct",
			"AZURE_STORAGE_KEY":     "azkey",
		},
	}

	render := func(backend string) string {
		d := base
		d.ColdStorageRender = backend
		out, err := renderK8sTemplate("templates/k8s/clickhouse-installation.yaml.tmpl", d)
		if err != nil {
			t.Fatalf("render (%s): %v", backend, err)
		}
		return out
	}

	none := render("none")
	if strings.Contains(none, "bifract-ch-storage") || strings.Contains(none, "cold-storage-config") {
		t.Fatalf("disabled backend must not render storage secret/mount:\n%s", none)
	}

	s3 := render("s3")
	for _, want := range []string{"kind: Secret", "name: bifract-ch-storage", "cold-storage-config", "subPath: storage.xml", "<type>s3</type>", "AK"} {
		if !strings.Contains(s3, want) {
			t.Fatalf("s3 manifest missing %q:\n%s", want, s3)
		}
	}
	if strings.Contains(s3, "azure_blob_storage") {
		t.Fatalf("s3 manifest should not contain azure disk")
	}

	az := render("azure")
	for _, want := range []string{"name: bifract-ch-storage", "<type>azure_blob_storage</type>", "azkey"} {
		if !strings.Contains(az, want) {
			t.Fatalf("azure manifest missing %q:\n%s", want, az)
		}
	}
}
