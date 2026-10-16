package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func runReconfigure(t *testing.T, dir string) (string, error) {
	t.Helper()
	var err error
	out := captureStdout(t, func() { err = RunReconfigure(dir) })
	return out, err
}

// Reconfigure re-renders from .env without changing the version, so a hand edit
// (here: switching to mTLS) reaches the running stack.
func TestRunReconfigureAppliesEditedEnv(t *testing.T) {
	cfg := installedConfig(t)
	cfg.CH = ClickHouseTarget{Backend: CHBackendExternal, Host: "ch.internal", Port: 9000}
	cfg.CH.Normalize()
	writeComposeInstall(t, cfg, "v1.1.0", map[string]string{"BIFRACT_OIDC_ISSUER_URL": "https://sso.example.com"})
	dir := cfg.InstallDir

	env := readFile(t, dir, ".env")
	env = strings.Replace(env, "BIFRACT_IP_ACCESS=restrict-app", "BIFRACT_IP_ACCESS=mtls-app", 1)
	env = strings.Replace(env, "BIFRACT_ALLOWED_IPS=10.0.0.0/8", "BIFRACT_ALLOWED_IPS=", 1)
	os.WriteFile(filepath.Join(dir, ".env"), []byte(env), 0600)

	fake := newFakeExec(t, runningStack)
	if _, err := runReconfigure(t, dir); err != nil {
		t.Fatal(err)
	}

	got := readEnv(t, dir)
	for k, want := range map[string]string{
		"BIFRACT_VERSION":         "v1.1.0",
		"BIFRACT_IP_ACCESS":       "mtls-app",
		"BIFRACT_PASSWORD_PEPPER": "pepper-value",
		"BIFRACT_OIDC_ISSUER_URL": "https://sso.example.com",
		"POSTGRES_PASSWORD":       "pg-pass",
	} {
		if got[k] != want {
			t.Errorf("%s = %q, want %q", k, got[k], want)
		}
	}
	if TargetFromEnv(got) != cfg.CH {
		t.Errorf("external ClickHouse lost: %+v", TargetFromEnv(got))
	}
	parseCertPEM(t, []byte(readFile(t, dir, "caddy", "client-ca", "ca.pem")))
	if !strings.Contains(dockerCalls(fake), "compose up -d") {
		t.Errorf("running stack not recreated:\n%s", dockerCalls(fake))
	}
}

func TestRunReconfigureRefuses(t *testing.T) {
	withoutExternalCommands(t)
	tests := []struct {
		name    string
		files   map[string]string
		wantErr string
	}{
		{"no install", nil, "docker-compose.yml missing"},
		{"no env", map[string]string{"docker-compose.yml": ""}, "no .env file"},
		{"no credentials", map[string]string{"docker-compose.yml": "", ".env": "CLICKHOUSE_PASSWORD=x\n"}, "missing POSTGRES_PASSWORD"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, body := range tt.files {
				os.WriteFile(filepath.Join(dir, name), []byte(body), 0600)
			}
			if _, err := runReconfigure(t, dir); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
