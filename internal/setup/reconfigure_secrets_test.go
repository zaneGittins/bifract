package setup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Reconfigure must not drop the least-privilege ingest DB passwords. Writing them empty
// strands the ingest tier: the app skips provisioning on an empty password, and
// bifract-ingest then fails auth as bifract_ingest with 28P01.
func TestReconfigurePreservesIngestPasswords(t *testing.T) {
	dir := t.TempDir()
	writeMinimalInstall(t, dir, map[string]string{
		"BIFRACT_INGEST_POSTGRES_PASSWORD":   "keepmepg",
		"BIFRACT_INGEST_CLICKHOUSE_PASSWORD": "keepmech",
	})

	if err := RunReconfigure(dir); err != nil {
		t.Fatalf("RunReconfigure: %v", err)
	}

	env, err := ReadEnvFile(filepath.Join(dir, ".env"))
	if err != nil {
		t.Fatalf("read .env: %v", err)
	}
	if got := env["BIFRACT_INGEST_POSTGRES_PASSWORD"]; got != "keepmepg" {
		t.Errorf("ingest postgres password = %q, want it preserved as %q", got, "keepmepg")
	}
	if got := env["BIFRACT_INGEST_CLICKHOUSE_PASSWORD"]; got != "keepmech" {
		t.Errorf("ingest clickhouse password = %q, want it preserved as %q", got, "keepmech")
	}
}

// Installs predating the ingest tier split carry no such values, so reconfigure has to
// mint them rather than write blanks.
func TestReconfigureGeneratesMissingIngestPasswords(t *testing.T) {
	dir := t.TempDir()
	writeMinimalInstall(t, dir, nil)

	if err := RunReconfigure(dir); err != nil {
		t.Fatalf("RunReconfigure: %v", err)
	}

	env, err := ReadEnvFile(filepath.Join(dir, ".env"))
	if err != nil {
		t.Fatalf("read .env: %v", err)
	}
	for _, k := range []string{"BIFRACT_INGEST_POSTGRES_PASSWORD", "BIFRACT_INGEST_CLICKHOUSE_PASSWORD"} {
		if env[k] == "" {
			t.Errorf("%s is empty; reconfigure must generate one when absent", k)
		}
	}
}

func writeMinimalInstall(t *testing.T, dir string, extra map[string]string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "docker-compose.yml"), []byte("services: {}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"POSTGRES_PASSWORD":   "pgpw",
		"CLICKHOUSE_PASSWORD": "chpw",
		"BIFRACT_DOMAIN":      "example.test",
		"BIFRACT_SSL_MODE":    "self-signed",
	}
	for k, v := range extra {
		env[k] = v
	}
	var b strings.Builder
	for k, v := range env {
		fmt.Fprintf(&b, "%s=%s\n", k, v)
	}
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(b.String()), 0600); err != nil {
		t.Fatal(err)
	}
}
