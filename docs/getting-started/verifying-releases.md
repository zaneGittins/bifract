# Verifying Releases

Releases and container images are signed in GitHub Actions with keyless [Sigstore cosign](https://docs.sigstore.dev/cosign/), and carry SLSA build provenance. No long-lived signing key exists: each signature is tied to the workflow run that built the release and recorded in the public Rekor transparency log.

The install script and `bifract --upgrade` check these for you. The install script always checks the binary's SHA-256 and also checks the signature when `cosign` is on the `PATH`. The self-update in `--upgrade` checks both and keeps the current binary if either check fails. To check by hand, install [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) v2.4 or later and the [GitHub CLI](https://cli.github.com/).

The examples use `v1.2.3`; substitute the release you are checking.

## Release binaries

Each GitHub release has a `checksums.txt` listing the SHA-256 of every binary, and `checksums.txt.sigstore.json`, its signature bundle.

```bash
TAG=v1.2.3
BASE=https://github.com/zaneGittins/bifract/releases/download/$TAG
curl -fLO "$BASE/bifract-linux-amd64"
curl -fLO "$BASE/checksums.txt"
curl -fLO "$BASE/checksums.txt.sigstore.json"

cosign verify-blob \
  --bundle checksums.txt.sigstore.json \
  --certificate-identity "https://github.com/zaneGittins/bifract/.github/workflows/release.yml@refs/tags/$TAG" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt

sha256sum --ignore-missing -c checksums.txt
```

`cosign` prints `Verified OK` and `sha256sum` prints `bifract-linux-amd64: OK`. On macOS use `shasum -a 256 --ignore-missing -c checksums.txt`.

To check the build provenance:

```bash
gh attestation verify bifract-linux-amd64 --repo zaneGittins/bifract
```

## Container images

Verify the image signature, made by the `docker.yml` workflow for the same tag:

```bash
TAG=v1.2.3
cosign verify ghcr.io/zanegittins/bifract:$TAG \
  --certificate-identity "https://github.com/zaneGittins/bifract/.github/workflows/docker.yml@refs/tags/$TAG" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

And its build provenance:

```bash
gh attestation verify oci://ghcr.io/zanegittins/bifract:$TAG --repo zaneGittins/bifract
```

The signature covers the multi-arch manifest list, so it applies to both the amd64 and arm64 images. To pin what you run, use the digest that `cosign verify` reports (`ghcr.io/zanegittins/bifract@sha256:...`) in place of the tag.
