#!/bin/bash
# 进入脚本所在目录
cd "$(dirname "$0")"

# 检查是否存在虚拟环境，不存在则创建
if [ ! -d "venv" ]; then
    echo "正在创建虚拟环境..."
    python3.11 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# 运行脚本
echo "正在启动脚本..."
python gui_xhs.py