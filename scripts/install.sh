#!/bin/sh
set -e

# Bifract Control Installer
# Usage:
#   curl -sfL https://raw.githubusercontent.com/zaneGittins/bifract/main/scripts/install.sh | sh
#   curl -sfL https://...install.sh | sh -s -- --upgrade
#   curl -sfL https://...install.sh | sh -s -- --install --dir /opt/bifract

REPO="zaneGittins/bifract"
BINARY="bifractctl"
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
curl -sfL -o "/tmp/${BINARY}" "$DOWNLOAD_URL"
chmod +x "/tmp/${BINARY}"

# Install (may need sudo)
if [ -w "$INSTALL_DIR" ]; then
    mv "/tmp/${BINARY}" "${INSTALL_DIR}/${BINARY}"
else
    echo "Installing to ${INSTALL_DIR} (requires sudo)..."
    sudo mv "/tmp/${BINARY}" "${INSTALL_DIR}/${BINARY}"
fi

# Clean up old bifract-setup binary if present
OLD_BINARY="${INSTALL_DIR}/bifract-setup"
if [ -f "$OLD_BINARY" ]; then
    echo "Removing old bifract-setup binary..."
    if [ -w "$INSTALL_DIR" ]; then
        rm -f "$OLD_BINARY"
    else
        sudo rm -f "$OLD_BINARY"
    fi
fi

echo ""
echo "bifractctl ${TAG} installed to ${INSTALL_DIR}/${BINARY}"

# Run bifractctl with provided args, defaulting to --install
if [ $# -eq 0 ]; then
    set -- --install
fi

echo "Running: sudo ${BINARY} $*"
echo ""
sudo "${INSTALL_DIR}/${BINARY}" "$@"
