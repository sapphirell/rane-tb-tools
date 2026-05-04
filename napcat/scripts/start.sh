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

set -a
# shellcheck disable=SC1091
. ./.env
set +a

configure_onebot_http() {
  local callback_token="${QQ_ONEBOT_CALLBACK_TOKEN:-}"
  local callback_url="${NAPCAT_ONEBOT_HTTP_URL:-}"
  local report_self="${NAPCAT_ONEBOT_REPORT_SELF_MESSAGE:-false}"

  if [ -z "$callback_url" ]; then
    if [ -z "$callback_token" ] || [ "$callback_token" = "change-me" ]; then
      echo "未配置 QQ_ONEBOT_CALLBACK_TOKEN，跳过 OneBot HTTP 上报配置。"
      return
    fi
    callback_url="https://api.fantuanpu.com/callback/qq/onebot?token=${callback_token}"
  fi

  mkdir -p data/napcat

  if ! ls data/napcat/onebot11_*.json >/dev/null 2>&1; then
    echo "还没有账号级 OneBot 配置文件。首次扫码登录后请重新执行 ./scripts/start.sh 以写入 HTTP 上报地址。"
    return
  fi

  for config_file in data/napcat/onebot11_*.json; do
    tmp_file="${config_file}.tmp"
    jq \
      --arg url "$callback_url" \
      --argjson reportSelf "$report_self" \
      '
      .network = (.network // {}) |
      .network.httpClients = [
        {
          "name": "hobby-box-qq-group-monitor",
          "enable": true,
          "urls": [$url],
          "messagePostFormat": "array",
          "reportSelfMessage": $reportSelf,
          "token": "",
          "debug": false
        }
      ]
      ' "$config_file" > "$tmp_file"
    mv "$tmp_file" "$config_file"
    echo "已配置 OneBot HTTP 上报：${config_file} -> ${callback_url}"
  done
}

if command -v jq >/dev/null 2>&1; then
  configure_onebot_http
else
  echo "未找到 jq，跳过自动写入 OneBot HTTP 上报配置。请安装 jq 或在 WebUI 手动配置。"
fi

"$DOCKER_BIN" compose up -d
"$DOCKER_BIN" compose ps
