#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

DOCKER_BIN="${DOCKER_BIN:-docker}"
if ! command -v "$DOCKER_BIN" >/dev/null 2>&1; then
  DOCKER_BIN="/Applications/Docker.app/Contents/Resources/bin/docker"
fi
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "已生成 napcat/.env，请先把 NAPCAT_WEBUI_TOKEN 改成自己的随机字符串。"
fi

"$DOCKER_BIN" compose up -d
"$DOCKER_BIN" compose ps
