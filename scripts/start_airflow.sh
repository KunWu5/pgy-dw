#!/usr/bin/env bash
# 启动本机 Airflow（Scheduler + Web UI），等价于 python airflow/start_windows.py standalone
# 用法：
#   bash scripts/start_airflow.sh standalone
#   bash scripts/start_airflow.sh test          # 进程内真跑 5 个任务
#   bash scripts/start_airflow.sh list
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-python}"
ACTION="${1:-standalone}"
shift || true

echo "Airflow: $ACTION  (Web UI 默认 http://localhost:8080)"
exec "$PY" airflow/start_windows.py "$ACTION" "$@"
