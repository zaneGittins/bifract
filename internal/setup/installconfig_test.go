package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

func mustParse(t *testing.T, src string) *InstallFile {
	t.Helper()
	f, err := parseInstallFile([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return f
}

func TestInstallFileParseRejects(t *testing.T) {
	cases := map[string]string{
		"missing version": "domain: a.example.com\n",
		"future version":  "version: 2\ndomain: a.example.com\n",
		"typo field":      "version: 1\ndomain: a.example.com\naccess:\n  mode: restrict-app\n  allowed_ip: [10.0.0.0/8]\n",
		"plaintext secret": "version: 1\ndomain: a.example.com\naccess: {mode: all}\n" +
			"clickhouse: {backend: external, host: ch:9000, password: hunter2}\n",
		"two documents": "version: 1\ndomain: a.example.com\n---\nversion: 1\n",
		"empty":         "",
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseInstallFile([]byte(src))
			if err == nil {
				t.Fatal("expected a parse error")
			}
			t.Log(err)
		})
	}
}

func TestInstallFileBuildRejects(t *testing.T) {
	compose := map[string]string{
		"bad domain":               "domain: https://a.example.com\naccess: {mode: all}\n",
		"missing access mode":      "domain: a.example.com\n",
		"ips with mode all":        "domain: a.example.com\naccess: {mode: all, allowed_ips: [10.0.0.1]}\n",
		"restrict without ips":     "domain: a.example.com\naccess: {mode: restrict-app}\n",
		"invalid cidr":             "domain: a.example.com\naccess: {mode: restrict-all, allowed_ips: [10.0.0.0/33]}\n",
		"none with generated":      "domain: a.example.com\naccess: {mode: all}\noutput: {admin_password: none}\n",
		"unknown output":           "domain: a.example.com\naccess: {mode: all}\noutput: {admin_password: email}\n",
		"k8s section":              "domain: a.example.com\naccess: {mode: all}\nk8s: {size_profile: Dev}\n",
		"letsencrypt no email":     "domain: a.example.com\naccess: {mode: all}\ncompose: {tls: {mode: letsencrypt}}\n",
		"email on self-signed":     "domain: a.example.com\naccess: {mode: all}\ncompose: {tls: {mode: self-signed, email: a@b.co}}\n",
		"custom missing files":     "domain: a.example.com\naccess: {mode: all}\ncompose: {tls: {mode: custom, cert_file: /nonexistent.crt, key_file: /nonexistent.key}}\n",
		"bundled with host":        "domain: a.example.com\naccess: {mode: all}\nclickhouse: {host: ch:9000}\n",
		"external without secret":  "domain: a.example.com\naccess: {mode: all}\nclickhouse: {backend: external, host: ch:9000, check_reachable: false}\n",
		"external unset env":       "domain: a.example.com\naccess: {mode: all}\nclickhouse: {backend: external, host: ch:9000, password_env: BIFRACT_TEST_UNSET, check_reachable: false}\n",
		"external both refs":       "domain: a.example.com\naccess: {mode: all}\nclickhouse: {backend: external, host: ch:9000, password_env: X, password_file: /x, check_reachable: false}\n",
		"external bad port":        "domain: a.example.com\naccess: {mode: all}\nclickhouse: {backend: external, host: 'ch:99999', password_env: X, check_reachable: false}\n",
		"short admin password env": "domain: a.example.com\naccess: {mode: all}\nsecrets: {admin_password_env: BIFRACT_TEST_SHORT}\n",
	}
	t.Setenv("BIFRACT_TEST_SHORT", "short")
	for name, body := range compose {
		t.Run("compose/"+name, func(t *testing.T) {
			_, _, err := mustParse(t, "version: 1\n"+body).ComposeConfig()
			if err == nil {
				t.Fatal("expected a build error")
			}
			t.Log(err)
		})
	}

	k8s := map[string]string{
		"missing profile": "domain: a.example.com\naccess: {mode: all}\nk8s: {}\n",
		"unknown profile": "domain: a.example.com\naccess: {mode: all}\nk8s: {size_profile: Huge}\n",
		"zero shards":     "domain: a.example.com\naccess: {mode: all}\nk8s: {size_profile: Dev, ch_shards: 0}\n",
		"small storage":   "domain: a.example.com\naccess: {mode: all}\nk8s: {size_profile: Dev, ch_storage_gb: 5}\n",
		"compose section": "domain: a.example.com\naccess: {mode: all}\nk8s: {size_profile: Dev}\ncompose: {install_dir: /x}\n",
		"shards with external": "domain: a.example.com\naccess: {mode: all}\nk8s: {size_profile: Dev, ch_shards: 2}\n" +
			"clickhouse: {backend: external, host: ch:9000, password_env: BIFRACT_TEST_SHORT, check_reachable: false}\n",
	}
	for name, body := range k8s {
		t.Run("k8s/"+name, func(t *testing.T) {
			_, _, err := mustParse(t, "version: 1\n"+body).K8sConfig()
			if err == nil {
				t.Fatal("expected a build error")
			}
			t.Log(err)
		})
	}
}

func TestInstallFileComposeBuild(t *testing.T) {
	dir := t.TempDir()
	pwFile := filepath.Join(dir, "admin")
	if err := os.WriteFile(pwFile, []byte("correct-horse-battery\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BIFRACT_TEST_CH", "ch-secret")
	cfg, plan, err := mustParse(t, `version: 1
domain: logs.example.com
access:
  mode: restrict-app
  allowed_ips: [10.0.0.0/8, " 192.168.1.5 "]
clickhouse:
  backend: external
  deployment: cloud
  host: abc.clickhouse.cloud
  password_env: BIFRACT_TEST_CH
  check_reachable: false
secrets:
  admin_password_file: `+pwFile+`
compose:
  install_dir: `+filepath.Join(dir, "install")+`
  tls: {mode: letsencrypt, email: ops@example.com}
`).ComposeConfig()
	if err != nil {
		t.Fatalf("ComposeConfig: %v", err)
	}
	if !cfg.SecureCookies || cfg.CORSOrigins != "https://logs.example.com" {
		t.Errorf("domain-derived settings not applied: %v %q", cfg.SecureCookies, cfg.CORSOrigins)
	}
	if got := strings.Join(cfg.AllowedIPs, ","); got != "10.0.0.0/8,192.168.1.5" {
		t.Errorf("allowed IPs = %q", got)
	}
	if cfg.CH.Bundled() || cfg.CH.Port != 9440 || !cfg.CH.Secure {
		t.Errorf("cloud target not normalized: %+v", cfg.CH)
	}
	if cfg.ClickHousePassword != "ch-secret" {
		t.Errorf("ClickHouse password not read from env")
	}
	if plan.adminPassword != "correct-horse-battery" || plan.passwordOutput != AdminPasswordToFile {
		t.Errorf("plan = %+v", plan)
	}

	if err := cfg.GeneratePasswords(); err != nil {
		t.Fatal(err)
	}
	if cfg.ClickHousePassword != "ch-secret" {
		t.Fatalf("GeneratePasswords replaced the external ClickHouse password")
	}
	if err := plan.applyAdminPassword(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.AdminPassword != "correct-horse-battery" || bcrypt.CompareHashAndPassword([]byte(cfg.AdminPasswordHash), []byte(applyPepper(cfg.AdminPassword, cfg.PasswordPepper))) != nil {
		t.Errorf("supplied admin password not hashed with the install pepper")
	}
}

func TestInstallFileDefaults(t *testing.T) {
	cfg, plan, err := mustParse(t, "version: 1\ndomain: localhost\naccess: {mode: all}\n").ComposeConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.InstallDir != "/opt/bifract" || cfg.SSLMode != SSLSelfSigned || !cfg.CH.Bundled() || cfg.SecureCookies {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if plan.passwordOutput != AdminPasswordToFile {
		t.Errorf("admin password output defaults to %q, want file", plan.passwordOutput)
	}

	k, _, err := mustParse(t, "version: 1\ndomain: a.example.com\naccess: {mode: mtls-app}\nk8s: {size_profile: medium}\n").K8sConfig()
	if err != nil {
		t.Fatal(err)
	}
	if k.SizeProfile.Name != "Medium" || k.CHShards != 2 || k.CHStorageGB != 100 || !k.MTLSEnabled {
		t.Errorf("unexpected k8s defaults: profile=%s shards=%d storage=%d mtls=%v", k.SizeProfile.Name, k.CHShards, k.CHStorageGB, k.MTLSEnabled)
	}
}

// A wizard run saved with saveInstallFile must replay to the same config.
func TestInstallFileRoundTrip(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InstallDir = "/srv/bifract"
	cfg.ApplyDomain("logs.example.com")
	cfg.SSLMode, cfg.SSLEmail = SSLLetsEncrypt, "ops@example.com"
	cfg.IPAccess, cfg.AllowedIPs = IPAccessRestrictAll, []string{"10.0.0.0/8"}
	cfg.CH = ClickHouseTarget{Backend: CHBackendExternal, Host: "ch.internal", Port: 9000}
	cfg.CH.Normalize()

	dir := t.TempDir()
	path, err := saveInstallFile(dir, composeInstallFile(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0600 {
		t.Errorf("answers file mode = %v", st.Mode().Perm())
	}
	f, err := LoadInstallFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f.ClickHouse.PasswordEnv = "BIFRACT_TEST_CH"
	f.ClickHouse.CheckReachable = new(bool)
	t.Setenv("BIFRACT_TEST_CH", "x")
	got, _, err := f.ComposeConfig()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got.InstallDir != cfg.InstallDir || got.Domain != cfg.Domain || got.SSLMode != cfg.SSLMode ||
		got.SSLEmail != cfg.SSLEmail || got.IPAccess != cfg.IPAccess || got.CH != cfg.CH ||
		strings.Join(got.AllowedIPs, ",") != "10.0.0.0/8" || got.ImageTag != cfg.ImageTag {
		t.Errorf("replay differs:\n got %+v\nwant %+v", got, cfg)
	}

	k := newK8sConfig()
	k.ApplyDomain("logs.example.com")
	k.IPAccess = IPAccessAll
	k.SizeProfile, k.CHShards, k.CHStorageGB = sizeProfiles[2], 3, 250
	body, err := yaml.Marshal(k8sInstallFile(k))
	if err != nil {
		t.Fatal(err)
	}
	gotK, _, err := mustParse(t, string(body)).K8sConfig()
	if err != nil {
		t.Fatalf("k8s replay: %v", err)
	}
	if gotK.SizeProfile.Name != "Small" || gotK.CHShards != 3 || gotK.CHStorageGB != 250 || gotK.OutputDir != k.OutputDir {
		t.Errorf("k8s replay differs: %+v", gotK)
	}
}

func TestReportAdminPassword(t *testing.T) {
	dir := t.TempDir()
	plan := installPlan{fromFile: true, passwordOutput: AdminPasswordToFile}
	line, shown, err := plan.reportAdminPassword(dir, "generated-password")
	if err != nil || shown || strings.Contains(line, "generated-password") {
		t.Fatalf("file mode leaked or failed: %q %v %v", line, shown, err)
	}
	path := filepath.Join(dir, AdminPasswordFileName)
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatalf("password file: %v %v", st, err)
	}
	if _, _, err := plan.reportAdminPassword(dir, "other"); err == nil {
		t.Error("an existing password file was overwritten")
	}

	supplied := installPlan{adminPassword: "operator-chosen-pw", passwordOutput: AdminPasswordToStdout}
	if line, shown, _ := supplied.reportAdminPassword(dir, "operator-chosen-pw"); shown || strings.Contains(line, "operator-chosen-pw") {
		t.Errorf("a supplied password was echoed: %q", line)
	}
}

// Drives the real --install-k8s --config path end to end on disk.
func TestRunInstallK8sFromConfig(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "manifests")
	cfgPath := filepath.Join(dir, "install.yaml")
	src := "version: 1\ndomain: logs.example.com\naccess: {mode: mtls-app}\nk8s: {size_profile: Medium, ch_shards: 3, ch_storage_gb: 200, output_dir: " + out + "}\n"
	if err := os.WriteFile(cfgPath, []byte(src), 0600); err != nil {
		t.Fatal(err)
	}
	if err := RunInstallK8s(InstallOptions{ConfigPath: cfgPath}); err != nil {
		t.Fatalf("RunInstallK8s: %v", err)
	}

	pw, err := os.ReadFile(filepath.Join(out, AdminPasswordFileName))
	if err != nil || len(strings.TrimSpace(string(pw))) < MinPasswordLength {
		t.Fatalf("admin password file: %q %v", pw, err)
	}
	for _, f := range []string{"kustomization.yaml", "bifract/secrets.yaml", "caddy/mtls-ca.yaml", "clickhouse/lb-service.yaml", "client-ca/ca-key.pem"} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(out, InstallFileName)); err == nil {
		t.Error("a config-file install should not save an answers file")
	}
	ch, _ := os.ReadFile(filepath.Join(out, "clickhouse/clickhouse-installation.yaml"))
	if !strings.Contains(string(ch), "200Gi") {
		t.Error("storage override not rendered")
	}

	if err := RunInstallK8s(InstallOptions{ConfigPath: cfgPath}); err == nil || !strings.Contains(err.Error(), "existing manifests") {
		t.Errorf("second install over the same output dir: %v", err)
	}
}

// A leftover admin-password file must stop the install before anything is
// rendered, or the new password would be generated, hashed and then lost.
func TestRunInstallK8sLeftoverPasswordFile(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "manifests")
	cfgPath := filepath.Join(dir, "install.yaml")
	src := "version: 1\ndomain: logs.example.com\naccess: {mode: all}\nk8s: {size_profile: Small, output_dir: " + out + "}\n"
	if err := os.WriteFile(cfgPath, []byte(src), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, AdminPasswordFileName), []byte("old\n"), 0600); err != nil {
		t.Fatal(err)
	}

	err := RunInstallK8s(InstallOptions{ConfigPath: cfgPath})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected leftover password file to stop the install, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "bifract", "secrets.yaml")); err == nil {
		t.Error("manifests were written despite the password file conflict")
	}
}

func TestIPv6AddressesRejected(t *testing.T) {
	for _, d := range []string{"::1", "2001:db8::1"} {
		if err := ValidateDomain(d); err == nil || !strings.Contains(err.Error(), "IPv6") {
			t.Errorf("ValidateDomain(%q) = %v, want an IPv6 error", d, err)
		}
	}
	if err := ValidateDomain("192.0.2.10"); err != nil {
		t.Errorf("IPv4 domain rejected: %v", err)
	}
	for _, h := range []string{"[::1]:9000", "::1", "[2001:db8::1]"} {
		if _, _, err := ParseCHEndpoint(h); err == nil || !strings.Contains(err.Error(), "IPv6") {
			t.Errorf("ParseCHEndpoint(%q) = %v, want an IPv6 error", h, err)
		}
	}
	if h, p, err := ParseCHEndpoint("ch.internal:9440"); err != nil || h != "ch.internal" || p != 9440 {
		t.Errorf("ParseCHEndpoint(ch.internal:9440) = %q %d %v", h, p, err)
	}
	target := ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cluster", Hosts: "a:9000,[::1]:9000", Cluster: "c", Port: 9000}
	if err := target.Validate(); err == nil || !strings.Contains(err.Error(), "IPv6") {
		t.Errorf("hosts with an IPv6 entry: %v, want an IPv6 error", err)
	}
}

// A failed install must not leave a password file that blocks the rerun, unless the
// file holding its hash was already written, in which case it is the only record.
func TestDiscardAdminPassword(t *testing.T) {
	plan := installPlan{passwordOutput: AdminPasswordToFile}
	write := func(dir string) string {
		path := filepath.Join(dir, AdminPasswordFileName)
		if err := os.WriteFile(path, []byte("pw\n"), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	dir := t.TempDir()
	path := write(dir)
	plan.discardAdminPassword(dir, filepath.Join(dir, ".env"))
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("password file kept although nothing was installed")
	}

	dir = t.TempDir()
	path = write(dir)
	if err := os.WriteFile(filepath.Join(dir, ".env"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	plan.discardAdminPassword(dir, filepath.Join(dir, ".env"))
	if _, err := os.Stat(path); err != nil {
		t.Error("password file removed although .env already holds its hash")
	}

	dir = t.TempDir()
	path = write(dir)
	installPlan{adminPassword: "operator-supplied", passwordOutput: AdminPasswordToFile}.discardAdminPassword(dir, filepath.Join(dir, ".env"))
	if _, err := os.Stat(path); err != nil {
		t.Error("a supplied password never writes the file, so it must not remove one")
	}
}

// Answers left behind by stepping back through the wizard would be saved into the
// replay file, which validation then rejects.
func TestWizardClearsAbandonedTLSAnswers(t *testing.T) {
	m := NewWizardModel(DefaultConfig())
	m.config.SSLEmail, m.config.CertPath, m.config.KeyPath = "ops@example.com", "/c.pem", "/k.pem"
	m.step, m.sslCursor = StepSSL, 0
	next, _ := m.updateSSL(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(WizardModel).config
	if got.SSLMode != SSLSelfSigned || got.SSLEmail != "" || got.CertPath != "" || got.KeyPath != "" {
		t.Errorf("self-signed kept abandoned answers: %+v", got)
	}

	m.inputErr = "invalid email address"
	m.step = StepSSLEmail
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if e := next.(WizardModel).inputErr; e != "" {
		t.Errorf("Esc kept the previous step's error %q", e)
	}
}
