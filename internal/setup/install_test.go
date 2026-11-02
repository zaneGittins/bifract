package setup

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeInstallFile(t *testing.T, installDir string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "install.yaml")
	body := "version: 1\ndomain: logs.example.com\naccess: {mode: all}\n" +
		"compose:\n  install_dir: " + installDir + "\n  tls: {mode: self-signed}\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// stubPortCheck replaces the host port preflight, so install tests do not depend
// on what happens to be listening on the machine running them.
func stubPortCheck(t *testing.T, err error) {
	t.Helper()
	orig := checkHostPorts
	checkHostPorts = func([]string) error { return err }
	t.Cleanup(func() { checkHostPorts = orig })
}

// Reinstalling over an existing .env would replace every credential the running
// databases were initialised with.
func TestRunInstallRefusesExistingInstall(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, ".env"), []byte("POSTGRES_PASSWORD=keep\n"), 0600)
	fake := newFakeExec(t)
	// A previous run's stack may hold the ports; the existing install is the
	// more useful thing to report.
	stubPortCheck(t, errors.New("port 443 is already in use"))

	var err error
	captureStdout(t, func() { err = RunInstall(InstallOptions{ConfigPath: writeInstallFile(t, dir)}) })
	if err == nil || !strings.Contains(err.Error(), "existing installation") {
		t.Fatalf("err = %v", err)
	}
	// Removing .env would regenerate passwords that no longer match the volumes.
	for _, hint := range []string{"--start --dir " + dir, "docker compose down -v"} {
		if !strings.Contains(err.Error(), hint) {
			t.Errorf("err does not suggest %q: %v", hint, err)
		}
	}
	if got := readFile(t, dir, ".env"); got != "POSTGRES_PASSWORD=keep\n" {
		t.Errorf(".env overwritten: %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, AdminPasswordFileName)); err == nil {
		t.Error("wrote an admin password for an install that never happened")
	}
	if len(fake.calls()) != 0 {
		t.Errorf("ran docker: %s", dockerCalls(fake))
	}
}

// The admin password is delivered before anything is deployed, so a failed
// deploy cannot lose it; the config is complete by the time docker runs.
func TestRunInstallDeliversPasswordBeforeDeploy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "bifract")
	fake := newFakeExec(t, composeUpFails)
	stubPortCheck(t, nil)

	var err error
	captureStdout(t, func() { err = RunInstall(InstallOptions{ConfigPath: writeInstallFile(t, dir)}) })
	if err == nil || !strings.Contains(err.Error(), "docker compose up") {
		t.Fatalf("err = %v", err)
	}
	pw := strings.TrimSpace(readFile(t, dir, AdminPasswordFileName))
	if len(pw) < 16 {
		t.Fatalf("admin password file holds %q", pw)
	}
	assertMode(t, filepath.Join(dir, AdminPasswordFileName), 0600)
	env := readEnv(t, dir)
	if env["BIFRACT_DOMAIN"] != "logs.example.com" || env["POSTGRES_PASSWORD"] == "" || env["BIFRACT_PASSWORD_PEPPER"] == "" {
		t.Errorf(".env incomplete: %v", env)
	}
	if !strings.Contains(readFile(t, dir, "db", "init-postgres.sql"), "$2a$12$") {
		t.Error("init-postgres.sql does not seed a freshly hashed admin password")
	}
	// A config-file install is already replayable; it does not save answers.
	if _, err := os.Stat(filepath.Join(dir, InstallFileName)); err == nil {
		t.Error("saved an answers file for a config-file install")
	}
	if calls := dockerCalls(fake); !strings.HasPrefix(calls, "compose pull") {
		t.Errorf("docker calls:\n%s", calls)
	}
}

// A busy port is caught before anything is written, so fixing it and rerunning
// is a clean install rather than one blocked by half-initialised volumes.
func TestRunInstallChecksPortsBeforeWriting(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "bifract")
	fake := newFakeExec(t)
	var checked []string
	orig := checkHostPorts
	checkHostPorts = func(addrs []string) error {
		checked = addrs
		return errors.New("port 80 is already in use")
	}
	t.Cleanup(func() { checkHostPorts = orig })

	var err error
	captureStdout(t, func() { err = RunInstall(InstallOptions{ConfigPath: writeInstallFile(t, dir)}) })
	if err == nil || !strings.Contains(err.Error(), "port 80") {
		t.Fatalf("err = %v", err)
	}
	if strings.Join(checked, ",") != ":80,:443,:8443" {
		t.Errorf("checked %v", checked)
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Errorf("install dir created before the port check: %v", statErr)
	}
	if len(fake.calls()) != 0 {
		t.Errorf("ran docker: %s", dockerCalls(fake))
	}
}

func TestRunInstallRejectsBadConfig(t *testing.T) {
	withoutExternalCommands(t)
	path := filepath.Join(t.TempDir(), "install.yaml")
	os.WriteFile(path, []byte("version: 1\ndomain: logs.example.com\naccess: {mode: restrict-app}\n"), 0600)
	var err error
	captureStdout(t, func() { err = RunInstall(InstallOptions{ConfigPath: path}) })
	if err == nil || !strings.Contains(err.Error(), "allowed IP") {
		t.Errorf("err = %v", err)
	}
	captureStdout(t, func() { err = RunInstall(InstallOptions{ConfigPath: filepath.Join(t.TempDir(), "missing.yaml")}) })
	if err == nil || !strings.Contains(err.Error(), "read install config") {
		t.Errorf("err = %v", err)
	}
}

// A source build installs the image built beside it, which no registry has, so
// only the images missing locally are pulled. A release always pulls.
func TestPullKeepsLocalDevImage(t *testing.T) {
	for tag, want := range map[string]string{
		"dev":    "compose pull --policy missing",
		"v0.0.9": "compose pull",
	} {
		fake := newFakeExec(t)
		if err := (&DockerOps{Dir: t.TempDir()}).Pull(tag); err != nil {
			t.Fatal(err)
		}
		calls := fake.calls()
		if len(calls) != 1 || calls[0].joined() != want {
			t.Errorf("%s: calls = %v, want %q", tag, calls, want)
		}
	}
}
