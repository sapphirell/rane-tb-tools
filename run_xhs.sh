#!/bin/bash
set -euo pipefail

# 进入脚本所在目录
cd "$(dirname "$0")"

# 优先选择兼容性更好的 Python（先 Homebrew，再系统）
PYTHON_BIN=""
for candidate in \
    /opt/homebrew/bin/python3.13 \
    /opt/homebrew/bin/python3.12 \
    /opt/homebrew/bin/python3.11 \
    "$(command -v python3 2>/dev/null || true)" \
    "$(command -v python 2>/dev/null || true)"; do
    if [ -n "${candidate}" ] && [ -x "${candidate}" ]; then
        if "${candidate}" -c "import struct, ssl" >/dev/null 2>&1; then
            PYTHON_BIN="${candidate}"
            break
        fi
    fi
done

if [ -z "${PYTHON_BIN}" ]; then
    echo "未找到可用的 Python 解释器，请先安装 Python 3。"
    exit 1
fi

echo "使用 Python: ${PYTHON_BIN} ($("${PYTHON_BIN}" -V 2>&1))"

TARGET_PY_VER="$("${PYTHON_BIN}" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")"

# 检查是否存在虚拟环境，不存在则创建
if [ -x "venv/bin/python" ]; then
    VENV_PY_VER="$(./venv/bin/python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo '')"
    if [ "${VENV_PY_VER}" != "${TARGET_PY_VER}" ]; then
        echo "检测到 venv Python 版本(${VENV_PY_VER:-unknown})与目标版本(${TARGET_PY_VER})不一致，重建 venv..."
        rm -rf venv
    fi
fi

if [ ! -d "venv" ] || [ ! -x "venv/bin/python" ]; then
    echo "正在创建虚拟环境..."
    "$PYTHON_BIN" -m venv venv
fi

# 检查关键依赖，缺失时自动安装
if ! ./venv/bin/python -c "import customtkinter, requests, pymysql, selenium" >/dev/null 2>&1; then
    echo "检测到依赖缺失，正在安装 requirements.txt ..."
    ./venv/bin/pip install --upgrade pip
    ./venv/bin/pip install -r requirements.txt
fi

# 运行脚本
echo "正在启动脚本..."
./venv/bin/python gui_xhs.py
