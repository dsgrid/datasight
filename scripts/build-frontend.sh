#!/usr/bin/env bash
# Build the Svelte frontend and copy output to FastAPI serving directories.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FRONTEND_DIR="$REPO_ROOT/frontend"
TEMPLATES_DIR="$REPO_ROOT/src/datasight/web/templates"
STATIC_DIR="$REPO_ROOT/src/datasight/web/static"

echo "Building frontend..."
cd "$FRONTEND_DIR"
npm ci --silent
npm run build

echo "Copying build output..."

mkdir -p "$TEMPLATES_DIR" "$STATIC_DIR/assets"

# Copy index.html to templates
cp "$FRONTEND_DIR/dist/index.html" "$TEMPLATES_DIR/index.html"

# Copy hashed assets to static/assets/
rm -rf "$STATIC_DIR/assets/"*
cp -r "$FRONTEND_DIR/dist/assets/"* "$STATIC_DIR/assets/"

# Copy the icon (referenced by favicon link)
cp "$FRONTEND_DIR/src/assets/datasight-icon.svg" "$STATIC_DIR/datasight-icon.svg"

echo "Frontend build complete."
echo "  Templates: $TEMPLATES_DIR/index.html"
echo "  Assets:    $STATIC_DIR/assets/"
