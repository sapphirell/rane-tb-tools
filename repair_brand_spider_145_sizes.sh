#!/usr/bin/env bash
# From Switch 历史商品尺寸脏数据修复启动器。
# 默认只读预览；显式传入 --apply 才会调用商品尺寸 PATCH 接口。

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-${SCRIPT_DIR}/venv/bin/python}"
DB_ENV_FILE="${DOGDOGDOLL_GO_ENV_FILE:-${SCRIPT_DIR}/../dogdogdoll-go/.env.local}"
LOG_FILE="${REPAIR_BRAND_SPIDER_LOG_FILE:-/tmp/repair_brand_spider_145_sizes_$(date +%Y%m%d_%H%M%S).log}"
REPORT_FILE="${REPAIR_BRAND_SPIDER_REPORT_FILE:-/tmp/repair_brand_spider_145_sizes_$(date +%Y%m%d_%H%M%S).json}"
COOKIE_COPY=""

cleanup() {
    if [[ -n "${COOKIE_COPY}" ]]; then
        rm -f "${COOKIE_COPY}"
    fi
}
trap cleanup EXIT

read_config_value() {
    local key="$1"
    local value
    value="$(awk -v key="$key" 'index($0, key "=") == 1 { sub("^[^=]*=", "", $0); print; exit }' "$DB_ENV_FILE")"
    if [[ -n "$value" && "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
        value="${value:1:${#value}-2}"
    elif [[ -n "$value" && "${value:0:1}" == '"' && "${value: -1}" == '"' ]]; then
        value="${value:1:${#value}-2}"
    fi
    printf '%s' "$value"
}

read_chrome_admin_token() {
    local chrome_user_home="${HOME:-/Users/sapphirell}"
    local cookie_db="${CHROME_PROFILE_DIR:-${chrome_user_home}/Library/Application Support/Google/Chrome/Default}/Cookies"
    local safe_storage_key=""
    local encrypted_cookie=""
    local cookie_host=""

    [[ -f "$cookie_db" ]] || return 1
    command -v security >/dev/null 2>&1 || return 1
    command -v sqlite3 >/dev/null 2>&1 || return 1
    command -v python3 >/dev/null 2>&1 || return 1
    python3 -c 'import cryptography' >/dev/null 2>&1 || return 1

    COOKIE_COPY="$(mktemp "${TMPDIR:-/tmp}/repair-brand-spider-145-cookies.XXXXXX")"
    cp "$cookie_db" "$COOKIE_COPY" || return 1
    safe_storage_key="$(security find-generic-password -w -a Chrome -s 'Chrome Safe Storage' 2>/dev/null)" || return 1
    [[ -n "$safe_storage_key" ]] || return 1

    for cookie_host in localhost 222.186.135.83; do
        encrypted_cookie="$(sqlite3 -noheader -batch "$COOKIE_COPY" \
            "select hex(encrypted_value) from cookies where host_key='${cookie_host}' and lower(name)='admin-token' limit 1;" \
            2>/dev/null | tr -d '[:space:]')"
        if [[ -n "$encrypted_cookie" ]]; then
            break
        fi
    done
    [[ -n "$encrypted_cookie" ]] || return 1

    printf '%s' "$encrypted_cookie" |
        SAFE_STORAGE_KEY="$safe_storage_key" python3 -c '
import hashlib
import os
import sys

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

raw = sys.stdin.read().strip()
try:
    key = hashlib.pbkdf2_hmac("sha1", os.environ["SAFE_STORAGE_KEY"].encode(), b"saltysalt", 1003, 16)
    encrypted = bytes.fromhex(raw)
    if encrypted[:3] != b"v10":
        raise ValueError("unsupported cookie format")
    plain = Cipher(algorithms.AES(key), modes.CBC(b" " * 16)).decryptor().update(encrypted[3:])
    pad = plain[-1]
    if not 1 <= pad <= 16:
        raise ValueError("invalid cookie padding")
    value = plain[32:-pad].decode("utf-8")
    if value.count(".") != 2:
        raise ValueError("cookie is not a JWT")
except Exception:
    raise SystemExit(1)
sys.stdout.write(value)
'
}

[[ -x "$PYTHON_BIN" ]] || {
    echo "找不到 Python：$PYTHON_BIN" >&2
    exit 1
}
[[ -f "$DB_ENV_FILE" ]] || {
    echo "找不到 Go 数据库配置：$DB_ENV_FILE" >&2
    exit 1
}

export SPIDER_DB_HOST="${SPIDER_DB_HOST:-$(read_config_value DB_HOST)}"
export SPIDER_DB_PORT="${SPIDER_DB_PORT:-$(read_config_value DB_PORT)}"
export SPIDER_DB_USER="${SPIDER_DB_USER:-$(read_config_value DB_USER)}"
export SPIDER_DB_NAME="${SPIDER_DB_NAME:-$(read_config_value DB_NAME)}"
export SPIDER_DB_PASSWORD="${SPIDER_DB_PASSWORD:-$(read_config_value DB_PASS)}"

for required_variable in SPIDER_DB_HOST SPIDER_DB_PORT SPIDER_DB_USER SPIDER_DB_NAME SPIDER_DB_PASSWORD; do
    if [[ -z "${!required_variable}" ]]; then
        echo "缺少数据库配置：$required_variable" >&2
        exit 1
    fi
done

apply_mode=false
for argument in "$@"; do
    if [[ "$argument" == "--apply" ]]; then
        apply_mode=true
        break
    fi
done

if [[ "$apply_mode" == true ]]; then
    admin_token="${DOGDOGDOLL_ADMIN_TOKEN:-}"
    if [[ -z "$admin_token" ]]; then
        admin_token="$(read_chrome_admin_token)" || {
            echo "未能读取管理员登录态，请先登录 http://localhost:9527，或手动设置 DOGDOGDOLL_ADMIN_TOKEN" >&2
            exit 1
        }
    fi
    [[ -n "$admin_token" ]] || {
        echo "管理员登录态为空" >&2
        exit 1
    }
    export DOGDOGDOLL_ADMIN_TOKEN="$admin_token"
    echo "开始执行 From Switch 尺寸脏数据修复（写入模式）"
else
    echo "开始执行 From Switch 尺寸脏数据修复（只读预览）"
fi
echo "日志：$LOG_FILE"
echo "报告：$REPORT_FILE"

"$PYTHON_BIN" "$SCRIPT_DIR/repair_brand_spider_145_sizes.py" \
    --log-file "$LOG_FILE" \
    --report-file "$REPORT_FILE" \
    "$@"
