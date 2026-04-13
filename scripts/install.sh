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

DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${BINARY}-${OS}-${ARCH}"

echo "Downloading ${BINARY} ${TAG} for ${OS}/${ARCH}..."
if ! curl -fL -o "/tmp/${BINARY}" "$DOWNLOAD_URL"; then
    echo "Download failed. URL: ${DOWNLOAD_URL}"
    echo "Check that release assets exist at https://github.com/${REPO}/releases/tag/${TAG}"
    exit 1
fi
chmod +x "/tmp/${BINARY}"

# Install (may need sudo)
if [ -w "$INSTALL_DIR" ]; then
    mv "/tmp/${BINARY}" "${INSTALL_DIR}/${BINARY}"
else
    echo "Installing to ${INSTALL_DIR} (requires sudo)..."
    sudo mv "/tmp/${BINARY}" "${INSTALL_DIR}/${BINARY}"
fi

echo ""
echo "bifract ${TAG} installed to ${INSTALL_DIR}/${BINARY}"

# Run bifract with provided args, defaulting to --install
if [ $# -eq 0 ]; then
    set -- --install
fi

echo "Running: sudo ${BINARY} $*"
echo ""
sudo "${INSTALL_DIR}/${BINARY}" "$@"
