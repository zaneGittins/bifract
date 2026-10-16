package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	dbpkg "bifract/db"
)

// adminInsert returns the statement that seeds the admin user.
func adminInsert(t *testing.T, sql string) string {
	t.Helper()
	start := strings.Index(sql, "INSERT INTO users")
	if start < 0 {
		t.Fatal("no admin user INSERT in init-postgres.sql")
	}
	end := strings.Index(sql[start:], ";")
	return sql[start : start+end]
}

// The docker install seeds the admin user from db/init-postgres.sql by string
// substitution. If that file's hash or layout drifts, the substitution silently
// misses and an install ships the well-known default credential, or keeps the
// forced password change on a generated password nobody else knows.
func TestRenderInitPostgresSeedsGeneratedAdmin(t *testing.T) {
	if !strings.Contains(dbpkg.PostgresSQL, defaultAdminHash) {
		t.Fatal("db/init-postgres.sql no longer carries defaultAdminHash; RenderInitPostgres cannot substitute the generated one")
	}
	cfg := DefaultConfig()
	cfg.AdminPasswordHash = "$2a$12$generatedgeneratedgeneratedgeneratedgeneratedgenerat"
	sql, err := RenderInitPostgres(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, defaultAdminHash) {
		t.Error("rendered SQL still holds the default admin hash")
	}
	insert := adminInsert(t, sql)
	if !strings.Contains(insert, "'"+cfg.AdminPasswordHash+"'") {
		t.Errorf("admin INSERT does not use the generated hash:\n%s", insert)
	}
	// The last two values are is_admin and force_password_change.
	fields := strings.Fields(strings.NewReplacer(",", " ", "(", " ", ")", " ").Replace(insert))
	var flags []string
	for _, f := range fields {
		if f == "TRUE" || f == "FALSE" {
			flags = append(flags, f)
		}
	}
	if strings.Join(flags, " ") != "TRUE FALSE" {
		t.Errorf("is_admin/force_password_change = %v, want [TRUE FALSE]:\n%s", flags, insert)
	}
	// Nothing else in the schema changes.
	if strings.Count(sql, "\n") != strings.Count(dbpkg.PostgresSQL, "\n") {
		t.Error("RenderInitPostgres changed more than the admin row")
	}
}

func TestWriteAllFiles(t *testing.T) {
	t.Run("custom TLS and mTLS", func(t *testing.T) {
		src := t.TempDir()
		cert, key := filepath.Join(src, "c.pem"), filepath.Join(src, "k.pem")
		os.WriteFile(cert, []byte("CERT"), 0644)
		os.WriteFile(key, []byte("KEY"), 0644)

		cfg := installedConfig(t)
		cfg.SSLMode, cfg.CertPath, cfg.KeyPath = SSLCustom, cert, key
		cfg.IPAccess, cfg.AllowedIPs = IPAccessMTLSApp, nil
		cfg.AdminPasswordHash = "hash"
		if err := WriteAllFiles(cfg); err != nil {
			t.Fatal(err)
		}
		dir := cfg.InstallDir
		assertMode(t, filepath.Join(dir, ".env"), 0600)
		assertMode(t, filepath.Join(dir, "caddy", "certs", "key.pem"), 0600)
		if got := readFile(t, dir, "caddy", "certs", "cert.pem"); got != "CERT" {
			t.Errorf("cert.pem = %q", got)
		}
		parseCertPEM(t, []byte(readFile(t, dir, "caddy", "client-ca", "ca.pem")))
		if !strings.Contains(readFile(t, dir, "db", "init-postgres.sql"), "'hash'") {
			t.Error("init-postgres.sql does not seed the generated admin hash")
		}
		if _, err := os.Stat(filepath.Join(dir, "db", "init-clickhouse.sql")); err == nil {
			t.Error("init-clickhouse.sql written; the app provisions and stamps ClickHouse itself")
		}
	})

	t.Run("letsencrypt writes no certificates", func(t *testing.T) {
		cfg := installedConfig(t)
		cfg.SSLMode, cfg.SSLEmail = SSLLetsEncrypt, "ops@example.com"
		if err := WriteAllFiles(cfg); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(cfg.InstallDir, "caddy", "certs")); err == nil {
			t.Error("certificates written for an ACME install")
		}
	})

	t.Run("missing custom certificate", func(t *testing.T) {
		cfg := installedConfig(t)
		cfg.SSLMode, cfg.CertPath, cfg.KeyPath = SSLCustom, "/nonexistent/c.pem", "/nonexistent/k.pem"
		if err := WriteAllFiles(cfg); err == nil || !strings.Contains(err.Error(), "copy custom certs") {
			t.Errorf("err = %v", err)
		}
	})
}

func TestCopyEmbeddedFile(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "a", "b", "entrypoint.sh")
	if err := CopyEmbeddedFile("templates/entrypoint.sh", dst); err != nil {
		t.Fatal(err)
	}
	want, _ := TemplateFS.ReadFile("templates/entrypoint.sh")
	if got := readFile(t, dst); got != string(want) {
		t.Error("copy differs from the embedded file")
	}
	if err := CopyEmbeddedFile("templates/missing", dst); err == nil {
		t.Error("copied a file that is not embedded")
	}
}
