#!/bin/bash
# Download the golars Zed extension source for local dev installation.
# Usage: curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/editors/zed-golars/install.sh | bash
#
# After running this script, open Zed and install as a dev extension:
#   Extensions (Cmd+Shift+X) → Install Dev Extension → select the downloaded directory

set -e

REPO_URL="https://github.com/Gaurav-Gosain/golars.git"
EXTENSION_SUBDIR="editors/zed-golars"
DEST_DIR="${ZED_GOLARS_DEST:-$HOME/.local/share/zed-dev-extensions/golars}"

echo "Downloading golars Zed extension..."
echo "Destination: $DEST_DIR"

# Clone shallow copy
TEMP_DIR=$(mktemp -d)
git clone --depth 1 "$REPO_URL" "$TEMP_DIR"

# Copy just the extension directory
rm -rf "$DEST_DIR"
mkdir -p "$DEST_DIR"
cp -r "$TEMP_DIR/$EXTENSION_SUBDIR"/* "$DEST_DIR/"

# Clean up
rm -rf "$TEMP_DIR"

echo ""
echo "Downloaded to: $DEST_DIR"
echo ""
echo "To install in Zed:"
echo "  1. Install the LSP:  go install github.com/Gaurav-Gosain/golars/cmd/golars-lsp@latest"
echo "  2. Open Zed → Extensions (Cmd+Shift+X)"
echo "  3. Click 'Install Dev Extension'"
echo "  4. Select: $DEST_DIR"