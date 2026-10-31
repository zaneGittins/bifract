package setup

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/sigstore/sigstore-go/pkg/bundle"
	"github.com/sigstore/sigstore-go/pkg/root"
	"github.com/sigstore/sigstore-go/pkg/verify"
)

const (
	releaseRepo          = "zaneGittins/bifract"
	githubOIDCIssuer     = "https://token.actions.githubusercontent.com"
	checksumsAsset       = "checksums.txt"
	checksumsBundleAsset = "checksums.txt.sigstore.json"
	maxReleaseMetaSize   = 1 << 20
)

// releaseSignerIdentity is the certificate SAN that release.yml signs with when run for tag.
func releaseSignerIdentity(tag string) string {
	return "https://github.com/" + releaseRepo + "/.github/workflows/release.yml@refs/tags/" + tag
}

func parseChecksumsBundle(data []byte) (verify.SignedEntity, error) {
	var b bundle.Bundle
	if err := b.UnmarshalJSON(data); err != nil {
		return nil, fmt.Errorf("parse %s: %w", checksumsBundleAsset, err)
	}
	return &b, nil
}

// newReleaseVerifier matches cosign's keyless defaults: Fulcio cert with an SCT, logged in Rekor.
func newReleaseVerifier(trusted root.TrustedMaterial) (*verify.Verifier, error) {
	return verify.NewVerifier(trusted,
		verify.WithSignedCertificateTimestamps(1),
		verify.WithTransparencyLog(1),
		verify.WithObserverTimestamps(1),
	)
}

// verifyChecksumsSignature checks that checksums was signed by release.yml for exactly this tag.
func verifyChecksumsSignature(verifier *verify.Verifier, entity verify.SignedEntity, checksums []byte, tag string) error {
	identity, err := verify.NewShortCertificateIdentity(githubOIDCIssuer, "", releaseSignerIdentity(tag), "")
	if err != nil {
		return err
	}
	_, err = verifier.Verify(entity, verify.NewPolicy(
		verify.WithArtifact(bytes.NewReader(checksums)),
		verify.WithCertificateIdentity(identity),
	))
	if err != nil {
		return fmt.Errorf("verify %s signature: %w", checksumsAsset, err)
	}
	return nil
}

// checksumFor returns the hex SHA-256 for name from sha256sum-formatted checksums.
func checksumFor(checksums []byte, name string) (string, error) {
	sc := bufio.NewScanner(bytes.NewReader(checksums))
	for sc.Scan() {
		sum, file, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			continue
		}
		file = strings.TrimPrefix(strings.TrimPrefix(file, " "), "*")
		if file == name {
			return sum, nil
		}
	}
	return "", fmt.Errorf("%s has no entry for %s", checksumsAsset, name)
}

func fetchSmall(client *http.Client, url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %d", url, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxReleaseMetaSize))
}

// downloadVerified streams url to dest and removes dest unless its SHA-256 equals wantHex.
func downloadVerified(client *http.Client, url, dest, wantHex string) (err error) {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %d", url, resp.StatusCode)
	}

	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			os.Remove(dest)
		}
	}()

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), resp.Body); err != nil {
		return fmt.Errorf("download incomplete: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); !strings.EqualFold(got, wantHex) {
		return fmt.Errorf("checksum mismatch: got %s, want %s", got, wantHex)
	}
	return nil
}

// downloadSignedRelease verifies the signed checksums for tag, then downloads asset to dest.
func downloadSignedRelease(client *http.Client, tag, asset, dest string) error {
	base := "https://github.com/" + releaseRepo + "/releases/download/" + tag + "/"

	checksums, err := fetchSmall(client, base+checksumsAsset)
	if err != nil {
		return err
	}
	bundleJSON, err := fetchSmall(client, base+checksumsBundleAsset)
	if err != nil {
		return err
	}
	entity, err := parseChecksumsBundle(bundleJSON)
	if err != nil {
		return err
	}
	trusted, err := root.FetchTrustedRoot()
	if err != nil {
		return fmt.Errorf("fetch Sigstore trusted root: %w", err)
	}
	verifier, err := newReleaseVerifier(trusted)
	if err != nil {
		return err
	}
	if err := verifyChecksumsSignature(verifier, entity, checksums, tag); err != nil {
		return err
	}
	want, err := checksumFor(checksums, asset)
	if err != nil {
		return err
	}
	return downloadVerified(client, base+asset, dest, want)
}
