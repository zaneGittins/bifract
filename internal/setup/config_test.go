package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateDomain(t *testing.T) {
	valid := []string{"localhost", "bifract.example.com", "a-b.example", "x1.y2", "192.0.2.10",
		strings.Repeat("a", 63) + ".com"}
	for _, d := range valid {
		if err := ValidateDomain(d); err != nil {
			t.Errorf("ValidateDomain(%q) = %v, want nil", d, err)
		}
	}
	// Each of these would be written into a Caddy site address or a CORS origin
	// verbatim and break the config or widen what it matches.
	invalid := []string{"", "https://bifract.example.com", "bifract.example.com/", "bifract.example.com:443",
		"*.example.com", "-bad.example.com", "bad-.example.com", "bad..example.com", "example.com.",
		"under_score.example.com", "sp ace.example.com", "bifract.example.com {", strings.Repeat("a", 64) + ".com",
		strings.Repeat("a.", 127) + "com"}
	for _, d := range invalid {
		if err := ValidateDomain(d); err == nil {
			t.Errorf("ValidateDomain(%q) = nil, want an error", d)
		}
	}
}

func TestApplyDomain(t *testing.T) {
	c := DefaultConfig()
	c.ApplyDomain("logs.example.com")
	if !c.SecureCookies || c.CORSOrigins != "https://logs.example.com" {
		t.Errorf("public domain: secure=%v cors=%q", c.SecureCookies, c.CORSOrigins)
	}
	// Back to localhost: no stale public origin, and cookies work over plain http.
	c.ApplyDomain("localhost")
	if c.SecureCookies || c.CORSOrigins != DefaultConfig().CORSOrigins {
		t.Errorf("localhost: secure=%v cors=%q", c.SecureCookies, c.CORSOrigins)
	}
}

func TestValidateEmail(t *testing.T) {
	for _, e := range []string{"ops@example.com", "a.b+tag@sub.example.org"} {
		if err := ValidateEmail(e); err != nil {
			t.Errorf("ValidateEmail(%q) = %v", e, err)
		}
	}
	// A display-name form parses as an address but is not one Let's Encrypt takes.
	for _, e := range []string{"", "ops", "ops@", "Ops <ops@example.com>", " ops@example.com"} {
		if err := ValidateEmail(e); err == nil {
			t.Errorf("ValidateEmail(%q) = nil, want an error", e)
		}
	}
}

func TestValidateCertPair(t *testing.T) {
	dir := t.TempDir()
	cert, key := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	os.WriteFile(cert, []byte("c"), 0600)
	os.WriteFile(key, []byte("k"), 0600)

	if err := ValidateCertPair(cert, key); err != nil {
		t.Errorf("valid pair rejected: %v", err)
	}
	cases := map[string][2]string{
		"no cert path":      {"", key},
		"no key path":       {cert, ""},
		"missing cert":      {filepath.Join(dir, "nope"), key},
		"missing key":       {cert, filepath.Join(dir, "nope")},
		"cert is directory": {dir, key},
		"key is directory":  {cert, dir},
	}
	for name, c := range cases {
		if err := ValidateCertPair(c[0], c[1]); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestParseCHEndpoint(t *testing.T) {
	tests := []struct {
		in       string
		host     string
		port     int
		wantErr  bool
		errMatch string
	}{
		{in: "ch.internal", host: "ch.internal"},
		{in: "  ch.internal:9440 ", host: "ch.internal", port: 9440},
		{in: "10.0.0.5:9000", host: "10.0.0.5", port: 9000},
		{in: "", wantErr: true},
		{in: "ch:0", wantErr: true},
		{in: "ch:65536", wantErr: true},
		{in: "ch:abc", wantErr: true},
		{in: "[::1]:9000", wantErr: true, errMatch: "IPv6"},
	}
	for _, tt := range tests {
		h, p, err := ParseCHEndpoint(tt.in)
		if tt.wantErr {
			if err == nil || !strings.Contains(err.Error(), tt.errMatch) {
				t.Errorf("ParseCHEndpoint(%q) err = %v, want an error containing %q", tt.in, err, tt.errMatch)
			}
			continue
		}
		if err != nil || h != tt.host || p != tt.port {
			t.Errorf("ParseCHEndpoint(%q) = %q, %d, %v; want %q, %d", tt.in, h, p, err, tt.host, tt.port)
		}
	}
}

func TestParseMinInt(t *testing.T) {
	if n, err := ParseMinInt("Shards", " 3 ", 1); err != nil || n != 3 {
		t.Errorf("ParseMinInt(3) = %d, %v", n, err)
	}
	if n, err := ParseMinInt("Storage", "10", 10); err != nil || n != 10 {
		t.Errorf("the minimum itself is allowed: %d, %v", n, err)
	}
	for _, v := range []string{"", "abc", "1.5", "-1", "0"} {
		if _, err := ParseMinInt("Shards", v, 1); err == nil {
			t.Errorf("ParseMinInt(%q) accepted", v)
		} else if !strings.HasPrefix(err.Error(), "Shards ") {
			t.Errorf("error %q does not name the field", err)
		}
	}
}

func TestAllowedIPs(t *testing.T) {
	c := &SetupConfig{}
	c.ParseAllowedIPs(" 10.0.0.0/8, ,192.168.1.5,  ")
	if got := c.AllowedIPsString(); got != "10.0.0.0/8,192.168.1.5" {
		t.Errorf("parsed %q", got)
	}
	if err := c.ValidateAllowedIPs(); err != nil {
		t.Errorf("valid list rejected: %v", err)
	}
	// Reparsing replaces rather than appends.
	c.ParseAllowedIPs("")
	if len(c.AllowedIPs) != 0 {
		t.Errorf("empty string left %v", c.AllowedIPs)
	}

	for _, bad := range []string{"10.0.0.0/33", "300.1.1.1", "example.com", "10.0.0.1/"} {
		c.AllowedIPs = []string{"10.0.0.1", bad}
		if err := c.ValidateAllowedIPs(); err == nil || !strings.Contains(err.Error(), bad) {
			t.Errorf("ValidateAllowedIPs(%q) = %v, want an error naming it", bad, err)
		}
	}
}

func TestSetupConfigValidate(t *testing.T) {
	dir := t.TempDir()
	cert, key := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	os.WriteFile(cert, []byte("c"), 0600)
	os.WriteFile(key, []byte("k"), 0600)

	base := func() *SetupConfig {
		c := DefaultConfig()
		c.ApplyDomain("logs.example.com")
		return c
	}
	tests := []struct {
		name    string
		mutate  func(*SetupConfig)
		wantErr string
	}{
		{"defaults", func(*SetupConfig) {}, ""},
		{"no install dir", func(c *SetupConfig) { c.InstallDir = "" }, "install directory"},
		{"bad domain", func(c *SetupConfig) { c.Domain = "a b" }, "invalid domain"},
		{"letsencrypt needs email", func(c *SetupConfig) { c.SSLMode = SSLLetsEncrypt }, "email"},
		{"letsencrypt with email", func(c *SetupConfig) { c.SSLMode, c.SSLEmail = SSLLetsEncrypt, "ops@example.com" }, ""},
		{"custom needs files", func(c *SetupConfig) { c.SSLMode = SSLCustom }, "certificate path"},
		{"custom with files", func(c *SetupConfig) { c.SSLMode, c.CertPath, c.KeyPath = SSLCustom, cert, key }, ""},
		{"unknown tls mode", func(c *SetupConfig) { c.SSLMode = "acme" }, "unknown TLS mode"},
		{"restrict needs ips", func(c *SetupConfig) { c.IPAccess = IPAccessRestrictApp }, "at least one"},
		{"restrict with ips", func(c *SetupConfig) { c.IPAccess, c.AllowedIPs = IPAccessRestrictAll, []string{"10.0.0.0/8"} }, ""},
		{"restrict with a bad ip", func(c *SetupConfig) { c.IPAccess, c.AllowedIPs = IPAccessRestrictApp, []string{"nope"} }, "invalid IP"},
		// An allow list that is silently ignored reads as protection that is not there.
		{"ips with all", func(c *SetupConfig) { c.AllowedIPs = []string{"10.0.0.1"} }, "only apply"},
		{"ips with mtls", func(c *SetupConfig) { c.IPAccess, c.AllowedIPs = IPAccessMTLSApp, []string{"10.0.0.1"} }, "only apply"},
		{"unknown access mode", func(c *SetupConfig) { c.IPAccess = "open" }, "unknown IP access"},
		{"external needs a password", func(c *SetupConfig) {
			c.CH = ClickHouseTarget{Backend: CHBackendExternal, Host: "ch", Port: 9000}
			c.CH.Normalize()
		}, "password is required"},
		{"external with a password", func(c *SetupConfig) {
			c.CH = ClickHouseTarget{Backend: CHBackendExternal, Host: "ch", Port: 9000}
			c.CH.Normalize()
			c.ClickHousePassword = "pw"
		}, ""},
		{"external without a host", func(c *SetupConfig) {
			c.CH = ClickHouseTarget{Backend: CHBackendExternal, Port: 9000}
			c.ClickHousePassword = "pw"
		}, "needs a host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base()
			tt.mutate(c)
			err := c.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() = %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestReadEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	content := "# Generated by bifract\n\n" +
		"POSTGRES_PASSWORD=abc\n" +
		"  BIFRACT_DOMAIN=logs.example.com  \r\n" +
		"BIFRACT_FEED_ENCRYPTION_KEY=a=b==\n" + // base64-style padding stays in the value
		"EMPTY=\n" +
		"no equals sign here\n" +
		"#COMMENTED=1\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	env, err := ReadEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"POSTGRES_PASSWORD":           "abc",
		"BIFRACT_DOMAIN":              "logs.example.com",
		"BIFRACT_FEED_ENCRYPTION_KEY": "a=b==",
		"EMPTY":                       "",
	}
	if len(env) != len(want) {
		t.Errorf("parsed %d keys %v, want %d", len(env), env, len(want))
	}
	for k, v := range want {
		if got, ok := env[k]; !ok || got != v {
			t.Errorf("%s = %q (present %v), want %q", k, got, ok, v)
		}
	}

	if _, err := ReadEnvFile(filepath.Join(t.TempDir(), "missing")); !os.IsNotExist(err) {
		t.Errorf("missing file: err = %v, want not-exist", err)
	}
}

// RenderEnvFile and ReadEnvFile are the two halves of every reconfigure and
// upgrade: whatever one writes, the other must read back unchanged.
func TestRenderEnvFileRoundTrips(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ApplyDomain("logs.example.com")
	cfg.CH = ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cloud", Host: "abc.clickhouse.cloud"}
	cfg.CH.Normalize()
	if err := cfg.GeneratePasswords(); err != nil {
		t.Fatal(err)
	}
	cfg.ClickHousePassword = "Ext3rnal@pw"
	cfg.IPAccess, cfg.AllowedIPs = IPAccessRestrictAll, []string{"10.0.0.0/8", "192.168.1.5"}
	cfg.SSLMode, cfg.SSLEmail = SSLLetsEncrypt, "ops@example.com"
	cfg.S3Bucket, cfg.S3Region = "backups", "us-east-1"

	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(RenderEnvFile(cfg)), 0600); err != nil {
		t.Fatal(err)
	}
	env, err := ReadEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		"BIFRACT_VERSION":                    cfg.ImageTag,
		"BIFRACT_DOMAIN":                     "logs.example.com",
		"POSTGRES_PASSWORD":                  cfg.PostgresPassword,
		"CLICKHOUSE_PASSWORD":                "Ext3rnal@pw",
		"BIFRACT_INGEST_POSTGRES_PASSWORD":   cfg.IngestPostgresPassword,
		"BIFRACT_INGEST_CLICKHOUSE_PASSWORD": cfg.IngestClickHousePassword,
		"LITELLM_MASTER_KEY":                 cfg.LiteLLMMasterKey,
		"BIFRACT_SECURE_COOKIES":             "true",
		"BIFRACT_CORS_ORIGINS":               "https://logs.example.com",
		"BIFRACT_SSL_MODE":                   "letsencrypt",
		"BIFRACT_SSL_EMAIL":                  "ops@example.com",
		"BIFRACT_IP_ACCESS":                  "restrict-all",
		"BIFRACT_ALLOWED_IPS":                "10.0.0.0/8,192.168.1.5",
		"BIFRACT_PASSWORD_PEPPER":            cfg.PasswordPepper,
		"BIFRACT_FEED_ENCRYPTION_KEY":        cfg.FeedEncryptionKey,
		"BIFRACT_BACKUP_ENCRYPTION_KEY":      cfg.BackupEncryptionKey,
		"BIFRACT_S3_BUCKET":                  "backups",
		"BIFRACT_S3_REGION":                  "us-east-1",
	} {
		if env[k] != v {
			t.Errorf("%s = %q, want %q", k, env[k], v)
		}
	}
	if got := TargetFromEnv(env); got != cfg.CH {
		t.Errorf("ClickHouse target did not survive .env: %+v, want %+v", got, cfg.CH)
	}
}
