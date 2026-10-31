#!/bin/sh
set -e

# Bifract Control Installer
# Usage:
#   curl -sfL https://docs.bifract.io/install.sh | sh
#   curl -sfL https://docs.bifract.io/install.sh | sh -s -- --upgrade
#   curl -sfL https://docs.bifract.io/install.sh | sh -s -- --install --dir /opt/bifract

REPO="zaneGittins/bifract"
BINARY="bifract"
INSTALL_DIR="/usr/local/bin"

# Detect OS
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$OS" in
    linux) OS="linux" ;;
    darwin) OS="darwin" ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
    x86_64|amd64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Get latest release tag (or use BIFRACT_VERSION env var)
if [ -n "$BIFRACT_VERSION" ]; then
    TAG="$BIFRACT_VERSION"
else
    TAG=$(curl -sfL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
    if [ -z "$TAG" ]; then
        echo "Failed to determine latest release. Set BIFRACT_VERSION to specify a version."
        exit 1
    fi
fi

BASE_URL="https://github.com/${REPO}/releases/download/${TAG}"
ASSET="${BINARY}-${OS}-${ARCH}"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Downloading ${BINARY} ${TAG} for ${OS}/${ARCH}..."
for f in "$ASSET" checksums.txt checksums.txt.sigstore.json; do
    if ! curl -fL -o "${TMP_DIR}/${f}" "${BASE_URL}/${f}"; then
        echo "Download failed. URL: ${BASE_URL}/${f}"
        echo "Check that release assets exist at https://github.com/${REPO}/releases/tag/${TAG}"
        exit 1
    fi
done

if command -v cosign >/dev/null 2>&1; then
    if ! cosign verify-blob \
        --bundle "${TMP_DIR}/checksums.txt.sigstore.json" \
        --certificate-identity "https://github.com/${REPO}/.github/workflows/release.yml@refs/tags/${TAG}" \
        --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
        "${TMP_DIR}/checksums.txt"; then
        echo "Signature verification of checksums.txt failed."
        exit 1
    fi
else
    echo "cosign not found; skipping signature check (install cosign to verify the release signature)."
fi

EXPECTED=$(awk -v f="$ASSET" '$2 == f || $2 == "*" f { print $1 }' "${TMP_DIR}/checksums.txt")
if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL=$(sha256sum "${TMP_DIR}/${ASSET}" | cut -d' ' -f1)
else
    ACTUAL=$(shasum -a 256 "${TMP_DIR}/${ASSET}" | cut -d' ' -f1)
fi
if [ -z "$EXPECTED" ] || [ "$EXPECTED" != "$ACTUAL" ]; then
    echo "Checksum mismatch for ${ASSET} (expected ${EXPECTED:-none}, got ${ACTUAL})."
    exit 1
fi
echo "Checksum verified."
mv "${TMP_DIR}/${ASSET}" "${TMP_DIR}/${BINARY}"
chmod +x "${TMP_DIR}/${BINARY}"

# Install (may need sudo)
if [ -w "$INSTALL_DIR" ]; then
    mv "${TMP_DIR}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
else
    echo "Installing to ${INSTALL_DIR} (requires sudo)..."
    sudo mv "${TMP_DIR}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
fi

echo ""
echo "bifract ${TAG} installed to ${INSTALL_DIR}/${BINARY}"

# Run bifract with provided args, defaulting to --install
if [ $# -eq 0 ]; then
    set -- --install
fi

echo "Running: sudo ${BINARY} $*"
echo ""
# Under `curl | sh` stdin is this script, so hand the wizard the terminal when there is one.
if [ ! -t 0 ] && (: < /dev/tty) 2>/dev/null; then
    sudo "${INSTALL_DIR}/${BINARY}" "$@" < /dev/tty
else
    sudo "${INSTALL_DIR}/${BINARY}" "$@"
fi
