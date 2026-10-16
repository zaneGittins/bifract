package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func runReconfigureK8s(t *testing.T, dir string, opts K8sReconfigureOpts) (string, error) {
	t.Helper()
	var err error
	out := captureStdout(t, func() { err = RunReconfigureK8s(dir, opts) })
	return out, err
}

func TestRunReconfigureK8sAppliesOverrides(t *testing.T) {
	withoutExternalCommands(t)
	setVersion(t, "v9.9.9")
	dir := t.TempDir()
	cfg := freshK8sConfig(sizeProfiles[0], dir)
	cfg.IngestClickHousePassword = "Ingest-Pw-123"
	writeK8sInstall(t, cfg)
	before, _ := parseK8sSecrets(filepath.Join(dir, "bifract", "secrets.yaml"))

	if _, err := runReconfigureK8s(t, dir, K8sReconfigureOpts{
		Domain:     "logs.example.org",
		IPAccess:   string(IPAccessRestrictApp),
		AllowedIPs: "10.0.0.0/8, 192.0.2.4",
		Shards:     2,
	}); err != nil {
		t.Fatal(err)
	}

	s, err := parseK8sSettings(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Reconfigure is not an upgrade: the running version stays.
	if s.imageTag != cfg.ImageTag {
		t.Errorf("image tag = %q, want %q kept", s.imageTag, cfg.ImageTag)
	}
	if s.domain != "logs.example.org" {
		t.Errorf("domain = %q", s.domain)
	}
	if s.ipAccess != IPAccessRestrictApp || strings.Join(s.allowedIPs, ",") != "10.0.0.0/8,192.0.2.4" {
		t.Errorf("access = %q %v", s.ipAccess, s.allowedIPs)
	}
	if s.chShards != 2 {
		t.Errorf("shards = %d", s.chShards)
	}
	after, _ := parseK8sSecrets(filepath.Join(dir, "bifract", "secrets.yaml"))
	for _, k := range []string{"POSTGRES_PASSWORD", "CLICKHOUSE_PASSWORD", "INGEST_CLICKHOUSE_PASSWORD",
		"PASSWORD_PEPPER", "ADMIN_PASSWORD_HASH", "FEED_ENCRYPTION_KEY", "BACKUP_ENCRYPTION_KEY"} {
		if after[k] != before[k] {
			t.Errorf("%s changed: %q -> %q", k, before[k], after[k])
		}
	}
	if b, _ := filepath.Glob(filepath.Join(dir, ".backups", "*", "bifract", "deployment.yaml")); len(b) != 1 {
		t.Errorf("found %d backups of the previous manifests, want 1", len(b))
	}
}

// Switching to mTLS has to mint a CA (Caddy cannot verify clients without one)
// and running reconfigure again must keep it, or every issued client cert dies.
func TestRunReconfigureK8sMTLS(t *testing.T) {
	withoutExternalCommands(t)
	dir := t.TempDir()
	writeK8sInstall(t, freshK8sConfig(sizeProfiles[0], dir))

	if _, err := runReconfigureK8s(t, dir, K8sReconfigureOpts{IPAccess: string(IPAccessMTLSApp)}); err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile(filepath.Join(dir, "client-ca", "ca.pem"))
	if err != nil {
		t.Fatalf("no CA written: %v", err)
	}
	parseCertPEM(t, ca)
	assertMode(t, filepath.Join(dir, "client-ca", "ca-key.pem"), 0600)
	if !strings.Contains(readFile(t, dir, "kustomization.yaml"), "mtls-ca.yaml") {
		t.Error("kustomization does not include the mTLS CA secret")
	}
	s, _ := parseK8sSettings(dir)
	if s.ipAccess != IPAccessMTLSApp {
		t.Errorf("access = %q", s.ipAccess)
	}

	if _, err := runReconfigureK8s(t, dir, K8sReconfigureOpts{}); err != nil {
		t.Fatal(err)
	}
	again, _ := os.ReadFile(filepath.Join(dir, "client-ca", "ca.pem"))
	if string(again) != string(ca) {
		t.Error("a second reconfigure replaced the client CA")
	}
	if !strings.Contains(readFile(t, dir, "caddy", "mtls-ca.yaml"), strings.Split(strings.TrimSpace(string(ca)), "\n")[1]) {
		t.Error("the Caddy CA secret does not carry the preserved CA")
	}

	if _, err := runReconfigureK8s(t, dir, K8sReconfigureOpts{IPAccess: string(IPAccessAll)}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(readFile(t, dir, "kustomization.yaml"), "mtls-ca.yaml") {
		t.Error("switching mTLS off left the CA secret in the kustomization")
	}
}

func TestRunReconfigureK8sSizeProfile(t *testing.T) {
	withoutExternalCommands(t)
	dir := t.TempDir()
	writeK8sInstall(t, freshK8sConfig(sizeProfiles[0], dir))

	out, err := runReconfigureK8s(t, dir, K8sReconfigureOpts{SizeProfile: "Small"})
	if err != nil {
		t.Fatal(err)
	}
	small, _ := lookupSizeProfile("Small")
	if s, _ := parseK8sSettings(dir); s.chResources != small.ClickHouse || s.bifractResources != small.Bifract {
		t.Errorf("Small not applied: %+v", s.chResources)
	}
	if !strings.Contains(out, "Size Profile: custom -> Small") {
		t.Errorf("change not reported:\n%s", out)
	}

	if _, err := runReconfigureK8s(t, dir, K8sReconfigureOpts{SizeProfile: "Gigantic"}); err == nil ||
		!strings.Contains(err.Error(), "unknown size profile") {
		t.Errorf("err = %v", err)
	}
}

func TestRunReconfigureK8sRefuses(t *testing.T) {
	withoutExternalCommands(t)
	if _, err := runReconfigureK8s(t, t.TempDir(), K8sReconfigureOpts{}); err == nil ||
		!strings.Contains(err.Error(), "no K8s installation") {
		t.Errorf("err = %v", err)
	}

	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "bifract"), 0755)
	os.WriteFile(filepath.Join(dir, "bifract", "secrets.yaml"), []byte("stringData:\n  POSTGRES_PASSWORD: \"x\"\n"), 0600)
	os.WriteFile(filepath.Join(dir, "bifract", "deployment.yaml"), []byte("kind: Deployment\n"), 0600)
	if _, err := runReconfigureK8s(t, dir, K8sReconfigureOpts{}); err == nil ||
		!strings.Contains(err.Error(), "missing POSTGRES_PASSWORD or CLICKHOUSE_PASSWORD") {
		t.Errorf("err = %v", err)
	}
}

func TestLookupSizeProfile(t *testing.T) {
	for _, name := range []string{"Dev", "dev", " X-LARGE ", "x-large"} {
		if _, ok := lookupSizeProfile(name); !ok {
			t.Errorf("lookupSizeProfile(%q) not found", name)
		}
	}
	if _, ok := lookupSizeProfile("xlarge"); ok {
		t.Error("matched a name no profile has")
	}
}
