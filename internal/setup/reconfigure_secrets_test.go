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
	// The ClickHouse value must satisfy the complexity policy, or reconfigure
	// rotates it on purpose; see TestReconfigureRotatesNonCompliantClickHousePassword.
	writeMinimalInstall(t, dir, map[string]string{
		"BIFRACT_INGEST_POSTGRES_PASSWORD":   "keepmepg",
		"BIFRACT_INGEST_CLICKHOUSE_PASSWORD": "Keepmech1!",
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
	if got := env["BIFRACT_INGEST_CLICKHOUSE_PASSWORD"]; got != "Keepmech1!" {
		t.Errorf("ingest clickhouse password = %q, want it preserved as %q", got, "Keepmech1!")
	}
}

// An install created before the complexity policy was known carries an
// alphanumeric ingest password. Preserving it would faithfully carry forward a
// credential a managed ClickHouse refuses on CREATE USER, leaving the ingest
// tier unable to authenticate at all -- so reconfigure rotates it instead. Safe
// because the app reconciles the ingest user's password at every startup.
func TestReconfigureRotatesNonCompliantClickHousePassword(t *testing.T) {
	dir := t.TempDir()
	const legacy = "aB3dEfGhIjKlMnOpQrSt"
	writeMinimalInstall(t, dir, map[string]string{
		"BIFRACT_INGEST_POSTGRES_PASSWORD":   "keepmepg",
		"BIFRACT_INGEST_CLICKHOUSE_PASSWORD": legacy,
	})

	if err := RunReconfigure(dir); err != nil {
		t.Fatalf("RunReconfigure: %v", err)
	}
	env, err := ReadEnvFile(filepath.Join(dir, ".env"))
	if err != nil {
		t.Fatalf("read .env: %v", err)
	}
	got := env["BIFRACT_INGEST_CLICKHOUSE_PASSWORD"]
	if got == legacy {
		t.Error("a non-compliant ingest password was preserved; a managed ClickHouse would refuse it")
	}
	if !ClickHousePasswordCompliant(got) {
		t.Errorf("rotated password %q is still not compliant", got)
	}
	// The Postgres credential has no such policy and must be left alone.
	if env["BIFRACT_INGEST_POSTGRES_PASSWORD"] != "keepmepg" {
		t.Error("the ingest Postgres password was rotated; only ClickHouse has this policy")
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
