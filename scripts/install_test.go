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

// cosignOK is a cosign that verifies anything.
const cosignOK = "#!/bin/sh\nexit 0\n"

// runInstall runs install.sh against stubbed uname, id, sudo, curl and cosign,
// and returns the script output plus the log of sudo and bifract invocations.
func runInstall(t *testing.T, osName, uid string, args ...string) (string, string) {
	t.Helper()
	out, calls, err := installWith(t, cosignOK, osName, uid, args...)
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, out)
	}
	return out, calls
}

// installWith runs install.sh with the given cosign stub and returns its error
// rather than failing, so a refused install can be asserted.
func installWith(t *testing.T, cosign, osName, uid string, args ...string) (string, string, error) {
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
		filepath.Join(stubs, "cosign"):                        cosign,
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
		return string(out), "", err
	}
	if _, err := os.Stat(filepath.Join(installDir, "bifract")); err != nil {
		t.Fatalf("binary not installed: %v\n%s", err, out)
	}
	calls, _ := os.ReadFile(logFile)
	return string(out), string(calls), nil
}

// cosignStub reports version and verifies only when the bundle format flag
// matches what that version needs to read a v3 bundle.
func cosignStub(version string, needsFlag bool) string {
	verify := "exit 0"
	if needsFlag {
		verify = "for a; do [ \"$a\" = --new-bundle-format ] && exit 0; done; exit 1"
	}
	return "#!/bin/sh\nif [ \"$1\" = version ]; then echo 'GitVersion:    " + version + "' >&2; exit 0; fi\n" + verify + "\n"
}

// cosign v2 reads the release's v3 bundle only with --new-bundle-format.
func TestInstallVerifiesWithCosignV2(t *testing.T) {
	out, _, err := installWith(t, cosignStub("v2.4.1", true), "linux", "0")
	if err != nil {
		t.Fatalf("install refused under cosign v2.4.1: %v\n%s", err, out)
	}
	if strings.Contains(out, "skipping") {
		t.Errorf("signature check skipped under cosign v2.4.1:\n%s", out)
	}
}

func TestInstallVerifiesWithCosignV3(t *testing.T) {
	out, _, err := installWith(t, cosignStub("v3.0.3", false), "linux", "0")
	if err != nil {
		t.Fatalf("install refused under cosign v3.0.3: %v\n%s", err, out)
	}
}

// Before v2.4.1 cosign cannot read the bundle at all, so it is skipped with a
// note rather than blocking the install; the checksum is still checked.
func TestInstallSkipsSignatureOnOldCosign(t *testing.T) {
	old := "#!/bin/sh\nif [ \"$1\" = version ]; then echo 'GitVersion:    v2.3.0'; exit 0; fi\nexit 1\n"
	out, _, err := installWith(t, old, "linux", "0")
	if err != nil {
		t.Fatalf("install refused under cosign v2.3.0: %v\n%s", err, out)
	}
	if !strings.Contains(out, "skipping signature check") {
		t.Errorf("no skip note for cosign v2.3.0:\n%s", out)
	}
}

// A failed verification still aborts, including when the version is unreadable.
func TestInstallRefusesBadSignature(t *testing.T) {
	for name, stub := range map[string]string{
		"v3":      "#!/bin/sh\nif [ \"$1\" = version ]; then echo 'GitVersion:    v3.0.3'; exit 0; fi\nexit 1\n",
		"unknown": "#!/bin/sh\nexit 1\n",
	} {
		if out, _, err := installWith(t, stub, "linux", "0"); err == nil {
			t.Errorf("%s: install succeeded despite a failed signature check:\n%s", name, out)
		}
	}
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
