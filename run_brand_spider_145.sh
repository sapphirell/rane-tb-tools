#!/usr/bin/env bash
# From Switch 已有数据分类启动器。
# 默认只分类数据库已有未处理记录；需要采集新商品时显式传入 --collect-new。
# 设置 BRAND_SPIDER_DRY_RUN=1 可只做只读的新商品解析。

set -Eeuo pipefail

# 当前启动器所在目录，避免从其它终端目录执行时找不到 Python 脚本和配置。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Python 解释器路径，可由调用方覆盖。
PYTHON_BIN="${PYTHON_BIN:-${SCRIPT_DIR}/venv/bin/python}"
# Go 项目的本地配置文件；该文件可能包含无法被 shell 直接 source 的特殊字符，因此只按键读取。
DB_ENV_FILE="${DOGDOGDOLL_GO_ENV_FILE:-${SCRIPT_DIR}/../dogdogdoll-go/.env.local}"
# 分类过程日志路径，可由调用方覆盖；默认每次运行使用新的时间戳文件，避免混入旧任务。
LOG_FILE="${BRAND_SPIDER_LOG_FILE:-/tmp/brand_spider_145_$(date +%Y%m%d_%H%M%S).log}"
# 管理后台默认地址，可由调用方覆盖。
API_BASE_URL="${DOGDOGDOLL_API_BASE_URL:-http://localhost:8080}"
# 临时复制的 Chrome Cookie 数据库路径，退出时只清理这个临时文件。
COOKIE_COPY=""

# 清理临时 Cookie 副本，不触碰 Chrome 原始数据。
cleanup() {
    if [[ -n "${COOKIE_COPY}" ]]; then
        rm -f "${COOKIE_COPY}"
    fi
}
trap cleanup EXIT

# 从配置文件读取指定键，只取第一行并保留值中的等号。
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

# 判断环境变量是否表示真值。
is_true() {
    case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

# 从已登录 Chrome 的 localhost Admin-Token Cookie 中读取令牌；令牌只通过 stdout 返回给命令替换，不打印到终端。
read_chrome_admin_token() {
    local user_home="${HOME:-/Users/sapphirell}"
    local cookie_db="${CHROME_PROFILE_DIR:-${user_home}/Library/Application Support/Google/Chrome/Default}/Cookies"
    local safe_storage_key=""
    local encrypted_cookie=""
    local cookie_host=""

    [[ -f "$cookie_db" ]] || return 1
    command -v security >/dev/null 2>&1 || return 1
    command -v sqlite3 >/dev/null 2>&1 || return 1
    command -v python3 >/dev/null 2>&1 || return 1
    python3 -c 'import cryptography' >/dev/null 2>&1 || return 1

    COOKIE_COPY="$(mktemp "${TMPDIR:-/tmp}/brand-spider-145-cookies.XXXXXX")"
    cp "$cookie_db" "$COOKIE_COPY" || return 1
    safe_storage_key="$(security find-generic-password -w -a Chrome -s 'Chrome Safe Storage' 2>/dev/null)" || return 1
    [[ -n "$safe_storage_key" ]] || return 1

    # 优先使用本地管理后台 Cookie；兼容浏览器只保留了站点域名 Cookie 的情况。
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
    key = hashlib.pbkdf2_hmac(
        "sha1", os.environ["SAFE_STORAGE_KEY"].encode(), b"saltysalt", 1003, 16
    )
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

# 命令行显式传入 --dry-run 时也切换为只读模式，避免和默认商品模式冲突。
dry_run=false
if is_true "${BRAND_SPIDER_DRY_RUN:-0}"; then
    dry_run=true
fi
for argument in "$@"; do
    if [[ "$argument" == "--dry-run" ]]; then
        dry_run=true
    fi
    if [[ "$argument" == --log-file=* ]]; then
        LOG_FILE="${argument#--log-file=}"
    fi
done
previous_argument=""
for argument in "$@"; do
    if [[ "$previous_argument" == "--log-file" ]]; then
        LOG_FILE="$argument"
    fi
    previous_argument="$argument"
done

[[ -x "$PYTHON_BIN" ]] || {
    echo "找不到 Python：$PYTHON_BIN" >&2
    exit 1
}
[[ -f "$SCRIPT_DIR/brand_spider_145.py" ]] || {
    echo "找不到采集脚本：$SCRIPT_DIR/brand_spider_145.py" >&2
    exit 1
}

run_args=(--detail-delay "${BRAND_SPIDER_DETAIL_DELAY:-0.8}" --log-file "$LOG_FILE")
if [[ "$dry_run" == true ]]; then
    run_args+=(--dry-run --collect-new)
else
    [[ -f "$DB_ENV_FILE" ]] || {
        echo "找不到 Go 数据库配置：$DB_ENV_FILE" >&2
        exit 1
    }

    # 允许环境变量覆盖配置文件，便于切换数据库而不改启动器。
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
    # 一键执行默认只处理数据库已有未完成记录；已处理记录不会重复创建商品。
    run_args+=(--auto-create-goods)
fi

echo "开始执行 From Switch 已有采集数据分类，日志：$LOG_FILE"
echo "From Switch 每条采集详情都按商品创建；AI 分类仅用于记录，不创建贩售记录或新闻"
if [[ "$dry_run" == true ]]; then
    echo "只读试运行：访问来源页面但不写入数据库"
else
    echo "已有数据模式：只读取数据库未处理记录，不访问来源分类页和商品详情页"
fi
"$PYTHON_BIN" "$SCRIPT_DIR/brand_spider_145.py" "${run_args[@]}" "$@"
