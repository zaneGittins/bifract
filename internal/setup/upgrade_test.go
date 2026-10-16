package setup

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	dbpkg "bifract/db"
	"bifract/pkg/storage"
)

// writeComposeInstall lays down an install the way --install leaves it, with
// extra lines appended to .env for keys only an operator would have set.
func writeComposeInstall(t *testing.T, cfg *SetupConfig, version string, extra map[string]string) {
	t.Helper()
	cfg.ImageTag = version
	if err := WriteAllFiles(cfg); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	f, err := os.OpenFile(filepath.Join(cfg.InstallDir, ".env"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		fmt.Fprintf(f, "%s=%s\n", k, extra[k])
	}
	f.Close()
}

func installedConfig(t *testing.T) *SetupConfig {
	t.Helper()
	cfg := DefaultConfig()
	cfg.InstallDir = t.TempDir()
	cfg.ApplyDomain("logs.example.com")
	cfg.PostgresPassword = "pg-pass"
	cfg.ClickHousePassword = "ch-pass"
	cfg.IngestPostgresPassword = "ingest-pg"
	cfg.IngestClickHousePassword = "Ingest-Ch-1"
	cfg.LiteLLMMasterKey = "sk-master"
	cfg.PasswordPepper = "pepper-value"
	cfg.FeedEncryptionKey = strings.Repeat("f", 64)
	cfg.BackupEncryptionKey = strings.Repeat("b", 64)
	cfg.IPAccess, cfg.AllowedIPs = IPAccessRestrictApp, []string{"10.0.0.0/8"}
	return cfg
}

func readEnv(t *testing.T, dir string) map[string]string {
	t.Helper()
	env, err := ReadEnvFile(filepath.Join(dir, ".env"))
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func runUpgrade(t *testing.T, dir string) (string, error) {
	t.Helper()
	var err error
	out := captureStdout(t, func() { err = RunUpgrade(dir) })
	return out, err
}

// composeUpFails stops an upgrade at the restart, after everything it writes,
// and before the health check's real-time polling.
var composeUpFails = fakeRule{Cmd: "docker", Match: []string{"compose up -d"}, Stdout: []byte("boom"), Exit: 1}

func TestRunUpgradePreservesInstall(t *testing.T) {
	cfg := installedConfig(t)
	cfg.CH = ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cloud", Host: "abc.clickhouse.cloud"}
	cfg.CH.Normalize()
	writeComposeInstall(t, cfg, "v0.9.0", map[string]string{
		"BIFRACT_OIDC_CLIENT_ID":    "sso-client",
		"BIFRACT_ARCHIVE_ENABLED":   "true",
		"MAXMIND_LICENSE_KEY":       "mm-key",
		"BIFRACT_DASHBOARD_WORKERS": "6",
		"SOME_UNMANAGED_KEY":        "dropped",
	})
	dir := cfg.InstallDir
	fake := newFakeExec(t, composeUpFails)

	_, err := runUpgrade(t, dir)
	if err == nil || !strings.Contains(err.Error(), "docker compose up") {
		t.Fatalf("err = %v, want the restart failure", err)
	}

	env := readEnv(t, dir)
	for k, want := range map[string]string{
		"BIFRACT_VERSION":                    Version,
		"POSTGRES_PASSWORD":                  "pg-pass",
		"CLICKHOUSE_PASSWORD":                "ch-pass",
		"BIFRACT_INGEST_POSTGRES_PASSWORD":   "ingest-pg",
		"BIFRACT_INGEST_CLICKHOUSE_PASSWORD": "Ingest-Ch-1",
		"LITELLM_MASTER_KEY":                 "sk-master",
		// A new pepper would invalidate every stored password hash.
		"BIFRACT_PASSWORD_PEPPER":       "pepper-value",
		"BIFRACT_FEED_ENCRYPTION_KEY":   strings.Repeat("f", 64),
		"BIFRACT_BACKUP_ENCRYPTION_KEY": strings.Repeat("b", 64),
		"BIFRACT_DOMAIN":                "logs.example.com",
		"BIFRACT_SECURE_COOKIES":        "true",
		"BIFRACT_IP_ACCESS":             "restrict-app",
		"BIFRACT_ALLOWED_IPS":           "10.0.0.0/8",
		"BIFRACT_OIDC_CLIENT_ID":        "sso-client",
		"BIFRACT_ARCHIVE_ENABLED":       "true",
		"MAXMIND_LICENSE_KEY":           "mm-key",
		"BIFRACT_DASHBOARD_WORKERS":     "6",
	} {
		if env[k] != want {
			t.Errorf("%s = %q, want %q", k, env[k], want)
		}
	}
	if _, ok := env["SOME_UNMANAGED_KEY"]; ok {
		t.Error("an unlisted key survived; the preserve list is the contract")
	}
	if got := TargetFromEnv(env); got != cfg.CH {
		t.Errorf("external ClickHouse became %+v", got)
	}
	compose := readFile(t, dir, "docker-compose.yml")
	if strings.Contains(compose, "clickhouse/clickhouse-server") {
		t.Error("upgrade rendered a bundled ClickHouse for an external install")
	}
	assertMode(t, filepath.Join(dir, ".env"), 0600)

	backups, _ := filepath.Glob(filepath.Join(dir, ".backups", "*"))
	if len(backups) != 1 {
		t.Fatalf("found %d backup dirs, want 1", len(backups))
	}
	for _, f := range []string{".env", "docker-compose.yml", "Caddyfile"} {
		if _, err := os.Stat(filepath.Join(backups[0], f)); err != nil {
			t.Errorf("backup missing %s", f)
		}
	}
	assertMode(t, filepath.Join(backups[0], ".env"), 0600)
	if old, _ := ReadEnvFile(filepath.Join(backups[0], ".env")); old["BIFRACT_VERSION"] != "v0.9.0" {
		t.Error("backup does not hold the pre-upgrade .env")
	}

	var order []string
	for _, c := range fake.calls() {
		order = append(order, c.joined())
	}
	joined := strings.Join(order, "\n")
	pull, down, up := strings.Index(joined, "compose pull"), strings.Index(joined, "compose down"), strings.Index(joined, "compose up -d")
	if pull < 0 || !(pull < down && down < up) {
		t.Errorf("want pull, down, up in that order; calls:\n%s", joined)
	}
	// The containers were not running, so nothing may have been exec'd into them.
	if strings.Contains(joined, "compose exec") {
		t.Errorf("exec'd into stopped containers:\n%s", joined)
	}
}

// Installs that predate a secret get one generated, except the pepper: hashes
// made without it only verify without it.
func TestRunUpgradeFillsLegacyGaps(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "docker-compose.yml"), []byte("services: {}\n"), 0644)
	os.WriteFile(filepath.Join(dir, ".env"), []byte(
		"POSTGRES_PASSWORD=pg\nCLICKHOUSE_PASSWORD=ch\nBIFRACT_DOMAIN=localhost\n"+
			"BIFRACT_INGEST_CLICKHOUSE_PASSWORD=alllowercase\n"), 0600)
	newFakeExec(t, composeUpFails)

	if _, err := runUpgrade(t, dir); err == nil {
		t.Fatal("expected the restart failure")
	}
	env := readEnv(t, dir)
	if v := env["BIFRACT_PASSWORD_PEPPER"]; v != "" {
		t.Errorf("generated pepper %q for an install whose hashes were made without one", v)
	}
	for _, k := range []string{"BIFRACT_FEED_ENCRYPTION_KEY", "BIFRACT_BACKUP_ENCRYPTION_KEY"} {
		if len(env[k]) != 64 {
			t.Errorf("%s = %q, want a generated 32-byte hex key", k, env[k])
		}
	}
	if env["BIFRACT_INGEST_POSTGRES_PASSWORD"] == "" {
		t.Error("ingest Postgres password not generated")
	}
	if pw := env["BIFRACT_INGEST_CLICKHOUSE_PASSWORD"]; pw == "alllowercase" || !ClickHousePasswordCompliant(pw) {
		t.Errorf("non-compliant ingest ClickHouse password not rotated: %q", pw)
	}
	for _, f := range staticInstallFiles {
		if _, err := os.Stat(filepath.Join(dir, filepath.Join(f.dest...))); err != nil {
			t.Errorf("upgrade did not write %s", filepath.Join(f.dest...))
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "caddy", "certs", "cert.pem")); err != nil {
		t.Error("self-signed certificate not generated")
	}
}

func TestRunUpgradeSkipsWhenCurrent(t *testing.T) {
	for _, installed := range []string{"v1.2.0", "v1.3.0"} {
		t.Run(installed, func(t *testing.T) {
			setVersion(t, "v1.2.0")
			cfg := installedConfig(t)
			writeComposeInstall(t, cfg, installed, nil)
			before := readFile(t, cfg.InstallDir, ".env")
			fake := newFakeExec(t)

			if _, err := runUpgrade(t, cfg.InstallDir); err != nil {
				t.Fatal(err)
			}
			if readFile(t, cfg.InstallDir, ".env") != before {
				t.Error(".env rewritten although the install is not older than this binary")
			}
			if len(fake.calls()) != 0 {
				t.Errorf("ran docker: %+v", fake.calls())
			}
		})
	}
}

func TestRunUpgradeRefuses(t *testing.T) {
	withoutExternalCommands(t)
	tests := []struct {
		name    string
		files   map[string]string
		wantErr string
	}{
		{"no install", nil, "docker-compose.yml missing"},
		{"no env", map[string]string{"docker-compose.yml": ""}, "no .env file"},
		{"no credentials", map[string]string{"docker-compose.yml": "", ".env": "POSTGRES_PASSWORD=x\n"}, "missing POSTGRES_PASSWORD or CLICKHOUSE_PASSWORD"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, body := range tt.files {
				os.WriteFile(filepath.Join(dir, name), []byte(body), 0600)
			}
			_, err := runUpgrade(t, dir)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

// runningUpgradeRules answers an upgrade against a running bundled stack whose
// databases are already at the latest migration.
func runningUpgradeRules(t *testing.T, partitionKey string) []fakeRule {
	t.Helper()
	pg, err := LoadMigrations(postgresMigrations, "migrations/postgres")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := LoadMigrations(dbpkg.ClickHouseMigrations, dbpkg.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	return []fakeRule{
		composeUpFails,
		runningStack,
		{Cmd: "docker", Match: []string{"SELECT partition_key FROM system.tables"}, Stdout: []byte(partitionKey + "\n")},
		{Cmd: "docker", Match: []string{"SELECT COALESCE(MAX(number), 0)"}, Stdout: pgOut(pg[len(pg)-1].Number)},
		{Cmd: "docker", Match: []string{"SELECT max(number) FROM logs._bifract_migrations"}, Stdout: []byte(fmt.Sprintf("%d\n", ch[len(ch)-1].Number))},
		{Cmd: "docker", Match: []string{storage.ModelIDsQuery}, Stdout: []byte("m1\n")},
	}
}

func containsDrop(sql []string) bool {
	for _, s := range sql {
		if strings.HasPrefix(s, "DROP ") {
			return true
		}
	}
	return false
}

func TestRunUpgradeOnRunningStack(t *testing.T) {
	cfg := installedConfig(t)
	writeComposeInstall(t, cfg, "v0.9.0", nil)
	fake := newFakeExec(t, runningUpgradeRules(t, "toDate(ingest_timestamp)")...)

	out, err := runUpgrade(t, cfg.InstallDir)
	if err == nil || !strings.Contains(err.Error(), "docker compose up") {
		t.Fatalf("err = %v", err)
	}
	if containsDrop(fake.sql()) {
		t.Error("dropped data on a schema that needs no reset")
	}
	if !strings.Contains(out, "No new migrations to apply") {
		t.Errorf("migrations not checked against the running stack:\n%s", out)
	}
}

// An event-time logs table cannot be migrated, only dropped. The upgrade must ask
// first, and anything but "yes" leaves the data alone.
func TestRunUpgradeIncompatibleSchemaNeedsConsent(t *testing.T) {
	t.Run("declined", func(t *testing.T) {
		cfg := installedConfig(t)
		writeComposeInstall(t, cfg, "v0.9.0", nil)
		fake := newFakeExec(t, runningUpgradeRules(t, "toYYYYMMDD(timestamp)")...)
		withStdin(t, "y\n")

		_, err := runUpgrade(t, cfg.InstallDir)
		if err == nil || !strings.Contains(err.Error(), "upgrade stopped") {
			t.Fatalf("err = %v", err)
		}
		if containsDrop(fake.sql()) {
			t.Error("dropped log data without an explicit yes")
		}
		for _, c := range fake.calls() {
			if strings.Contains(c.joined(), "compose pull") {
				t.Error("continued the upgrade after the operator declined")
			}
		}
	})

	t.Run("accepted", func(t *testing.T) {
		cfg := installedConfig(t)
		writeComposeInstall(t, cfg, "v0.9.0", nil)
		fake := newFakeExec(t, runningUpgradeRules(t, "toYYYYMMDD(timestamp)")...)
		withStdin(t, "yes\n")

		if _, err := runUpgrade(t, cfg.InstallDir); err == nil || !strings.Contains(err.Error(), "docker compose up") {
			t.Fatalf("err = %v", err)
		}
		views, tables := storage.ModelCHObjectNames("m1")
		want := storage.ResetLogDataStatements(views, tables)
		var drops []string
		for _, s := range fake.sql() {
			if strings.HasPrefix(s, "DROP ") {
				drops = append(drops, s)
			}
		}
		if strings.Join(drops, "\n") != strings.Join(want, "\n") {
			t.Errorf("drops:\n%s\nwant:\n%s", strings.Join(drops, "\n"), strings.Join(want, "\n"))
		}
		if !strings.Contains(strings.Join(fake.sql(), "\n"), postgresResetSQL()) {
			t.Error("Postgres state describing the dropped data was not cleared")
		}
	})
}
