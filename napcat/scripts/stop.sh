#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

DOCKER_BIN="${DOCKER_BIN:-docker}"
if ! command -v "$DOCKER_BIN" >/dev/null 2>&1; then
  DOCKER_BIN="/Applications/Docker.app/Contents/Resources/bin/docker"
fi
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"

"$DOCKER_BIN" compose down
