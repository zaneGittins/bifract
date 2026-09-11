package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// renderK8s writes a full manifest set, optionally with mTLS, and returns the dir.
func renderK8s(t *testing.T, mtls bool) string {
	t.Helper()
	dir := t.TempDir()
	cfg := freshK8sConfig(sizeProfiles[0], dir)
	if mtls {
		cfg.MTLSEnabled = true
		cfg.MTLSCACert = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
		cfg.MTLSCAKey = "-----BEGIN EC PRIVATE KEY-----\nMHc\n-----END EC PRIVATE KEY-----\n"
	}
	if err := writeK8sManifests(cfg); err != nil {
		t.Fatalf("render: %v", err)
	}
	return dir
}

func readManifest(t *testing.T, dir, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(b)
}

// mTLS is the only thing standing between the internet and these installs, so
// every piece of its wiring is pinned: losing any one of them silently downgrades
// an install to unauthenticated or locks every client out.
func TestMTLSWiringRenders(t *testing.T) {
	dir := renderK8s(t, true)

	for _, c := range []struct{ file, want, why string }{
		{"caddy/configmap.yaml", "client_auth", "Caddy requires client certs"},
		{"caddy/configmap.yaml", "mode require_and_verify", "verification is enforced, not optional"},
		{"caddy/configmap.yaml", "trusted_ca_cert_file /etc/caddy/client-ca/ca.pem", "CA path matches the mount"},
		{"caddy/deployment.yaml", "name: client-ca", "CA volume reaches Caddy"},
		{"caddy/deployment.yaml", "secretName: caddy-mtls-ca", "CA secret is the source"},
		{"caddy/mtls-ca.yaml", "BEGIN CERTIFICATE", "CA secret carries a cert"},
		{"bifract/deployment.yaml", "BIFRACT_CLIENT_CA_DIR", "app can issue client certs"},
		{"bifract/mtls-ca.yaml", "BEGIN EC PRIVATE KEY", "app holds the CA key"},
	} {
		if !strings.Contains(readManifest(t, dir, c.file), c.want) {
			t.Errorf("mTLS regression: %s missing %q (%s)", c.file, c.want, c.why)
		}
	}
}

// Without mTLS the client_auth block must be absent rather than empty, which
// would make Caddy demand a cert nobody has.
func TestNoMTLSLeavesCaddyOpen(t *testing.T) {
	cm := readManifest(t, renderK8s(t, false), "caddy/configmap.yaml")
	if strings.Contains(cm, "client_auth") {
		t.Error("client_auth rendered with mTLS disabled")
	}
}
