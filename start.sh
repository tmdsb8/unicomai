#!/bin/bash
# 一键启动（前台运行，Ctrl+C 停止）
# 环境变量：UNICOMAI_HOME（默认脚本所在目录）  PORT（默认 8800）
HERE="$(cd "$(dirname "$0")" && pwd)"
export UNICOMAI_HOME="${UNICOMAI_HOME:-$HERE}"
export PORT="${PORT:-8800}"
mkdir -p "$UNICOMAI_HOME/data" "$UNICOMAI_HOME/logs"
command -v ffmpeg >/dev/null || { echo "缺少 ffmpeg，请先安装（apt install ffmpeg / brew install ffmpeg）"; exit 1; }
cd "$HERE/app"
exec uvicorn main:app --host 0.0.0.0 --port "$PORT" --proxy-headers --forwarded-allow-ips='*'
