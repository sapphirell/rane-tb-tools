#!/bin/bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"

BASE_DIR="${QWEN35_BASE_DIR:-$HOME/qwen35-local}"
VENV_DIR="${QWEN35_VENV_DIR:-$BASE_DIR/venv}"
MODEL_DIR="${QWEN35_MODEL_DIR:-$HOME/models/Qwen3.5-27B-GGUF}"
MODEL_REPO="${QWEN35_MODEL_REPO:-bartowski/Qwen_Qwen3.5-27B-GGUF}"
MODEL_FILE="${QWEN35_MODEL_FILE:-Qwen3.5-27B-Q4_K_M.gguf}"
MODEL_PATH="${MODEL_DIR}/${MODEL_FILE}"
HOST="${QWEN35_HOST:-0.0.0.0}"
PORT="${QWEN35_PORT:-8000}"
N_CTX="${QWEN35_N_CTX:-8192}"
N_GPU_LAYERS="${QWEN35_N_GPU_LAYERS:--1}"
LOG_FILE="${QWEN35_LOG_FILE:-$BASE_DIR/qwen35-api.log}"
PID_FILE="${QWEN35_PID_FILE:-$BASE_DIR/qwen35-api.pid}"
PYTHON_BIN="${QWEN35_PYTHON_BIN:-}"

log() {
  printf '[qwen35] %s\n' "$*"
}

fail() {
  printf '[qwen35] ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<EOF
Usage:
  $SCRIPT_NAME install      # 创建 venv、安装依赖、下载模型
  $SCRIPT_NAME start        # 前台启动 OpenAI 兼容 API
  $SCRIPT_NAME start-bg     # 后台启动 OpenAI 兼容 API
  $SCRIPT_NAME stop         # 停止后台服务
  $SCRIPT_NAME status       # 查看服务状态
  $SCRIPT_NAME test         # 测试 API 可调用性

可选环境变量（覆盖默认值）:
  QWEN35_BASE_DIR       (default: $BASE_DIR)
  QWEN35_VENV_DIR       (default: $VENV_DIR)
  QWEN35_MODEL_DIR      (default: $MODEL_DIR)
  QWEN35_MODEL_REPO     (default: $MODEL_REPO)
  QWEN35_MODEL_FILE     (default: $MODEL_FILE)
  QWEN35_HOST           (default: $HOST)
  QWEN35_PORT           (default: $PORT)
  QWEN35_N_CTX          (default: $N_CTX)
  QWEN35_N_GPU_LAYERS   (default: $N_GPU_LAYERS)
  QWEN35_PYTHON_BIN     (default: auto-detect python3.11/3.12)

示例:
  bash $SCRIPT_NAME install
  QWEN35_HOST=127.0.0.1 QWEN35_PORT=8000 bash $SCRIPT_NAME start-bg
  bash $SCRIPT_NAME test
EOF
}

ensure_macos() {
  if [[ "$(uname -s)" != "Darwin" ]]; then
    fail "该脚本仅支持 macOS。"
  fi
  if [[ "$(uname -m)" != "arm64" ]]; then
    log "警告：当前不是 arm64，Metal 加速可能不可用。"
  fi
}

resolve_python() {
  if [[ -n "$PYTHON_BIN" && -x "$PYTHON_BIN" ]]; then
    return 0
  fi

  local candidates=(
    "/opt/homebrew/bin/python3.12"
    "/opt/homebrew/bin/python3.11"
    "$(command -v python3 2>/dev/null || true)"
  )
  local candidate=""
  for candidate in "${candidates[@]}"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      if "$candidate" -c "import struct, ssl" >/dev/null 2>&1; then
        PYTHON_BIN="$candidate"
        break
      fi
    fi
  done

  if [[ -z "$PYTHON_BIN" ]]; then
    fail "未找到可用的 Python 3.11/3.12，请先安装 Homebrew Python。"
  fi
}

ensure_xcode_clt() {
  if ! xcode-select -p >/dev/null 2>&1; then
    log "未检测到 Xcode Command Line Tools，正在触发安装窗口..."
    xcode-select --install || true
    fail "请完成 Xcode Command Line Tools 安装后重新运行。"
  fi
}

ensure_venv() {
  resolve_python
  mkdir -p "$BASE_DIR"

  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    log "创建虚拟环境: $VENV_DIR"
    "$PYTHON_BIN" -m venv "$VENV_DIR"
  fi

  "$VENV_DIR/bin/python" -m pip install -U pip setuptools wheel
}

install_deps() {
  log "安装 llama-cpp-python (Metal) 与服务依赖..."
  if ! "$VENV_DIR/bin/pip" install "llama-cpp-python[server]" \
    --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/metal; then
    log "检测到安装异常，执行缓存修复并重试..."
    "$VENV_DIR/bin/pip" cache remove llama-cpp-python >/dev/null 2>&1 || true
    "$VENV_DIR/bin/pip" cache purge >/dev/null 2>&1 || true
    "$VENV_DIR/bin/pip" uninstall -y llama-cpp-python fastapi starlette sse-starlette >/dev/null 2>&1 || true
    "$VENV_DIR/bin/pip" install --no-cache-dir --force-reinstall "llama-cpp-python[server]" \
      --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/metal
  fi
  "$VENV_DIR/bin/pip" install -U --no-cache-dir huggingface_hub openai
}

download_model() {
  mkdir -p "$MODEL_DIR"
  if [[ -f "$MODEL_PATH" ]]; then
    log "模型已存在: $MODEL_PATH"
    return 0
  fi

  log "下载模型: $MODEL_REPO / $MODEL_FILE"
  "$VENV_DIR/bin/huggingface-cli" download "$MODEL_REPO" "$MODEL_FILE" \
    --local-dir "$MODEL_DIR"
}

ensure_installed() {
  [[ -x "$VENV_DIR/bin/python" ]] || fail "未发现虚拟环境，请先执行: $SCRIPT_NAME install"
  [[ -f "$MODEL_PATH" ]] || fail "未发现模型文件，请先执行: $SCRIPT_NAME install"
}

is_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

start_server_fg() {
  ensure_installed
  log "启动服务（前台）: http://$HOST:$PORT/v1"
  exec "$VENV_DIR/bin/python" -m llama_cpp.server \
    --model "$MODEL_PATH" \
    --host "$HOST" \
    --port "$PORT" \
    --n_ctx "$N_CTX" \
    --n_gpu_layers "$N_GPU_LAYERS"
}

start_server_bg() {
  ensure_installed
  mkdir -p "$BASE_DIR"

  if is_running; then
    log "服务已在运行，PID=$(cat "$PID_FILE")"
    return 0
  fi

  log "后台启动服务..."
  nohup "$VENV_DIR/bin/python" -m llama_cpp.server \
    --model "$MODEL_PATH" \
    --host "$HOST" \
    --port "$PORT" \
    --n_ctx "$N_CTX" \
    --n_gpu_layers "$N_GPU_LAYERS" \
    >"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"

  sleep 2
  if is_running; then
    log "服务已启动，PID=$(cat "$PID_FILE")"
    log "API: http://127.0.0.1:$PORT/v1"
    log "日志: $LOG_FILE"
  else
    rm -f "$PID_FILE"
    fail "启动失败，请检查日志: $LOG_FILE"
  fi
}

stop_server() {
  if ! is_running; then
    rm -f "$PID_FILE"
    log "服务未运行"
    return 0
  fi

  local pid
  pid="$(cat "$PID_FILE")"
  log "停止服务，PID=$pid"
  kill "$pid" >/dev/null 2>&1 || true

  for _ in {1..10}; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      sleep 1
    else
      break
    fi
  done

  if kill -0 "$pid" >/dev/null 2>&1; then
    log "进程未退出，执行强制停止"
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi

  rm -f "$PID_FILE"
  log "已停止"
}

status_server() {
  if is_running; then
    log "运行中，PID=$(cat "$PID_FILE")"
    log "API: http://127.0.0.1:$PORT/v1"
    log "日志: $LOG_FILE"
  else
    log "未运行"
  fi
}

test_api() {
  local test_host="${QWEN35_TEST_HOST:-127.0.0.1}"
  local endpoint="http://${test_host}:${PORT}/v1"

  log "检查模型列表: ${endpoint}/models"
  curl -fsS "${endpoint}/models"
  echo

  log "发送聊天请求: ${endpoint}/chat/completions"
  curl -fsS "${endpoint}/chat/completions" \
    -H "Content-Type: application/json" \
    -d "{\"model\":\"${MODEL_FILE}\",\"messages\":[{\"role\":\"user\",\"content\":\"用中文一句话介绍你自己。\"}],\"temperature\":0.7,\"max_tokens\":128}"
  echo
}

install_all() {
  ensure_macos
  ensure_xcode_clt
  ensure_venv
  install_deps
  download_model
  log "安装完成。可执行: $SCRIPT_NAME start-bg"
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    install)
      install_all
      ;;
    start)
      start_server_fg
      ;;
    start-bg)
      start_server_bg
      ;;
    stop)
      stop_server
      ;;
    status)
      status_server
      ;;
    test)
      test_api
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      fail "未知命令: $cmd (使用: $SCRIPT_NAME --help)"
      ;;
  esac
}

main "${1:-}"
