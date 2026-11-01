package scripts

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runInstall runs install.sh against stubbed uname, id, sudo, curl and cosign,
// and returns the script output plus the log of sudo and bifract invocations.
func runInstall(t *testing.T, osName, uid string, args ...string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	stubs := filepath.Join(dir, "stubs")
	installDir := filepath.Join(dir, "bin")
	release := filepath.Join(dir, "release")
	logFile := filepath.Join(dir, "calls.log")
	for _, d := range []string{stubs, installDir, release} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	asset := "bifract-" + osName + "-amd64"
	bin := "#!/bin/sh\necho \"bifract $*\" >> \"$CALL_LOG\"\n"
	sum := sha256.Sum256([]byte(bin))
	files := map[string]string{
		filepath.Join(release, asset):                         bin,
		filepath.Join(release, "checksums.txt"):               hex.EncodeToString(sum[:]) + "  " + asset + "\n",
		filepath.Join(release, "checksums.txt.sigstore.json"): "{}",
		filepath.Join(stubs, "uname"):                         "#!/bin/sh\nif [ \"$1\" = -m ]; then echo x86_64; else echo " + osName + "; fi\n",
		filepath.Join(stubs, "id"):                            "#!/bin/sh\necho " + uid + "\n",
		filepath.Join(stubs, "sudo"):                          "#!/bin/sh\necho sudo >> \"$CALL_LOG\"\nexec \"$@\"\n",
		filepath.Join(stubs, "cosign"):                        "#!/bin/sh\nexit 0\n",
		filepath.Join(stubs, "curl"): "#!/bin/sh\nout=\nfor a; do case \"$prev\" in -o) out=$a ;; esac; prev=$a; url=$a; done\n" +
			"cp \"$RELEASE_DIR/${url##*/}\" \"$out\"\n",
	}
	for path, body := range files {
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	cmd := exec.Command("sh", append([]string{"install.sh"}, args...)...)
	cmd.Env = []string{
		"PATH=" + stubs + ":/usr/bin:/bin",
		"BIFRACT_VERSION=v0.0.0-test",
		"BIFRACT_INSTALL_DIR=" + installDir,
		"RELEASE_DIR=" + release,
		"CALL_LOG=" + logFile,
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(installDir, "bifract")); err != nil {
		t.Fatalf("binary not installed: %v\n%s", err, out)
	}
	calls, _ := os.ReadFile(logFile)
	return string(out), string(calls)
}

func TestInstallAsRootSkipsSudo(t *testing.T) {
	_, calls := runInstall(t, "linux", "0")
	if calls != "bifract --install\n" {
		t.Errorf("calls = %q, want the wizard run directly without sudo", calls)
	}
}

func TestInstallAsUserUsesSudo(t *testing.T) {
	_, calls := runInstall(t, "linux", "1000", "--upgrade")
	if calls != "sudo\nbifract --upgrade\n" {
		t.Errorf("calls = %q, want sudo then the requested command", calls)
	}
}

// The server install is Linux only, so macOS gets the CLI and a note.
func TestInstallOnDarwinOnlyInstallsCLI(t *testing.T) {
	out, calls := runInstall(t, "darwin", "501")
	if calls != "" {
		t.Errorf("calls = %q, want no wizard run on macOS", calls)
	}
	if !strings.Contains(out, "Linux") {
		t.Errorf("output lacks a Linux-only note:\n%s", out)
	}
}
