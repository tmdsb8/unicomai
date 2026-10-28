#!/bin/bash
# 守护脚本：服务挂了自动拉起，单实例运行
# 用法: UNICOMAI_HOME=/opt/unicomai PORT=8800 ./bin/guard.sh
HOME_DIR="${UNICOMAI_HOME:-/opt/unicomai}"
APP_DIR="$HOME_DIR/app"
PORT="${PORT:-8800}"
LOCK=/tmp/unicomai_guard.lock
LOG="$HOME_DIR/logs/app.log"
mkdir -p "$HOME_DIR/logs"

exec 9>"$LOCK"
if ! flock -n 9; then
    echo "守护脚本已在运行"
    exit 0
fi

while true; do
    if ! ss -tln 2>/dev/null | grep -q ":$PORT "; then
        echo "[$(date '+%F %T')] 启动服务" >> "$LOG"
        cd "$APP_DIR" || exit 1
        rm -rf __pycache__
        uvicorn main:app --host 0.0.0.0 --port "$PORT" \
            --proxy-headers --forwarded-allow-ips='*' >> "$LOG" 2>&1
        echo "[$(date '+%F %T')] 服务退出，5 秒后重启" >> "$LOG"
        sleep 5
    else
        sleep 10
    fi
done
