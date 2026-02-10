#!/bin/bash
set -euo pipefail

# 进入脚本所在目录
cd "$(dirname "$0")"

# 选择可用的 Python 解释器
if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
else
    echo "未找到可用的 Python（python3/python），请先安装 Python 3。"
    exit 1
fi

# 检查是否存在虚拟环境，不存在则创建
if [ ! -d "venv" ] || [ ! -x "venv/bin/python" ]; then
    echo "正在创建虚拟环境..."
    "$PYTHON_BIN" -m venv venv
    ./venv/bin/pip install --upgrade pip
    ./venv/bin/pip install -r requirements.txt
fi

# 运行脚本
echo "正在启动脚本..."
./venv/bin/python gui_xhs.py
