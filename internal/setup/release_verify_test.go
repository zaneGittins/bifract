package setup

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sigstore/sigstore-go/pkg/root"
	"github.com/sigstore/sigstore-go/pkg/testing/ca"
	"github.com/sigstore/sigstore-go/pkg/testing/data"
	"github.com/sigstore/sigstore-go/pkg/verify"
)

const testChecksums = "aaaa  bifract-darwin-arm64\n" +
	"bbbb  bifract-linux-amd64\n" +
	"cccc *bifract-linux-arm64\n"

func TestChecksumFor(t *testing.T) {
	cases := []struct {
		name, want string
		wantErr    bool
	}{
		{name: "bifract-linux-amd64", want: "bbbb"},
		{name: "bifract-linux-arm64", want: "cccc"},
		{name: "bifract-darwin-arm64", want: "aaaa"},
		{name: "bifract-windows-amd64", wantErr: true},
		{name: "bifract-linux", wantErr: true},
	}
	for _, c := range cases {
		got, err := checksumFor([]byte(testChecksums), c.name)
		if c.wantErr {
			if err == nil {
				t.Errorf("checksumFor(%q) = %q, want error", c.name, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("checksumFor(%q) = %q, %v; want %q", c.name, got, err, c.want)
		}
	}
}

func TestReleaseSignerIdentity(t *testing.T) {
	want := "https://github.com/zaneGittins/bifract/.github/workflows/release.yml@refs/tags/v1.2.3"
	if got := releaseSignerIdentity("v1.2.3"); got != want {
		t.Errorf("releaseSignerIdentity = %q, want %q", got, want)
	}
}

// The test Sigstore cannot issue SCTs, so identity checks run with the rest of the production policy.
func testVerifier(t *testing.T, trusted root.TrustedMaterial) *verify.Verifier {
	t.Helper()
	v, err := verify.NewVerifier(trusted, verify.WithTransparencyLog(1), verify.WithObserverTimestamps(1))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestReleaseVerifierAcceptsPublicGoodBundle(t *testing.T) {
	v, err := newReleaseVerifier(data.TrustedRoot(t, "public-good.json"))
	if err != nil {
		t.Fatal(err)
	}
	entity := data.Bundle(t, "sigstore.js@2.0.0-provenance.sigstore.json")
	if _, err := v.Verify(entity, verify.NewPolicy(verify.WithoutArtifactUnsafe(), verify.WithoutIdentitiesUnsafe())); err != nil {
		t.Fatalf("real public-good bundle rejected: %v", err)
	}
}

func TestReleaseVerifierRequiresSCT(t *testing.T) {
	vs, err := ca.NewVirtualSigstore()
	if err != nil {
		t.Fatal(err)
	}
	checksums := []byte(testChecksums)
	entity, err := vs.Sign(releaseSignerIdentity("v1.2.3"), githubOIDCIssuer, checksums)
	if err != nil {
		t.Fatal(err)
	}
	v, err := newReleaseVerifier(vs)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyChecksumsSignature(v, entity, checksums, "v1.2.3"); err == nil {
		t.Error("certificate without an SCT accepted")
	}
}

func TestVerifyChecksumsSignature(t *testing.T) {
	vs, err := ca.NewVirtualSigstore()
	if err != nil {
		t.Fatal(err)
	}
	checksums := []byte(testChecksums)
	good, err := vs.Sign(releaseSignerIdentity("v1.2.3"), githubOIDCIssuer, checksums)
	if err != nil {
		t.Fatal(err)
	}

	if err := verifyChecksumsSignature(testVerifier(t, vs), good, checksums, "v1.2.3"); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	if err := verifyChecksumsSignature(testVerifier(t, vs), good, []byte(testChecksums+"dddd  evil\n"), "v1.2.3"); err == nil {
		t.Error("tampered checksums accepted")
	}

	// A signature from an older release must not vouch for a newer tag.
	if err := verifyChecksumsSignature(testVerifier(t, vs), good, checksums, "v1.2.4"); err == nil {
		t.Error("signature for v1.2.3 accepted for v1.2.4")
	}

	otherWorkflow, err := vs.Sign("https://github.com/zaneGittins/bifract/.github/workflows/ci.yml@refs/tags/v1.2.3", githubOIDCIssuer, checksums)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyChecksumsSignature(testVerifier(t, vs), otherWorkflow, checksums, "v1.2.3"); err == nil {
		t.Error("signature from another workflow accepted")
	}

	otherIssuer, err := vs.Sign(releaseSignerIdentity("v1.2.3"), "https://accounts.example.com", checksums)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyChecksumsSignature(testVerifier(t, vs), otherIssuer, checksums, "v1.2.3"); err == nil {
		t.Error("signature from another OIDC issuer accepted")
	}

	untrusted, err := ca.NewVirtualSigstore()
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyChecksumsSignature(testVerifier(t, untrusted), good, checksums, "v1.2.3"); err == nil {
		t.Error("signature verified against an unrelated trust root")
	}
}

func TestParseChecksumsBundleRejectsGarbage(t *testing.T) {
	if _, err := parseChecksumsBundle([]byte(`{"mediaType":"nope"}`)); err == nil {
		t.Error("invalid bundle accepted")
	}
}

func TestDownloadVerified(t *testing.T) {
	payload := []byte("new bifract binary")
	sum := sha256.Sum256(payload)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bin" {
			http.NotFound(w, r)
			return
		}
		w.Write(payload)
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "bifract.update")

	if err := downloadVerified(srv.Client(), srv.URL+"/bin", dest, hex.EncodeToString(sum[:])); err != nil {
		t.Fatalf("matching download rejected: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("dest = %q, %v", got, err)
	}
	if info, _ := os.Stat(dest); info.Mode().Perm()&0o100 == 0 {
		t.Errorf("dest mode %v is not executable", info.Mode())
	}
	os.Remove(dest)

	err = downloadVerified(srv.Client(), srv.URL+"/bin", dest, strings.Repeat("0", 64))
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("mismatched download: err = %v, want checksum mismatch", err)
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Error("mismatched download left a file behind")
	}

	if err := downloadVerified(srv.Client(), srv.URL+"/missing", dest, hex.EncodeToString(sum[:])); err == nil {
		t.Error("404 download accepted")
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Error("failed download left a file behind")
	}
}
