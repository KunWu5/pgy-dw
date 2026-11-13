#!/usr/bin/env bash
# pgy_dw 全链路流水线（Linux / Git Bash 均可执行）
# 用法：
#   bash scripts/run_pipeline.sh                 # 完整跑（含 Spark 画像层）
#   bash scripts/run_pipeline.sh --skip-spark   # 跳过 Spark，只跑治理/BI/分析/OLAP
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-python}"

echo "[1/6] 数据治理：数据字典 / 数据标准 / 血缘 / 质量规则"
"$PY" governance/build_governance.py

if [[ "${1:-}" == "--skip-spark" ]]; then
  echo "[2/6] 跳过 Spark 画像层（--skip-spark）"
else
  echo "[2/6] Spark：标签体系与达人价值画像"
  "$PY" governance/build_profile.py
fi

echo "[3/6] 分层 SQL 与数据质量（SQLite 版，幂等全量重跑）"
"$PY" run_pipeline.py >/dev/null 2>&1 || echo "    跳过：SQLite 版未重跑，沿用 output/ 既有产物"

echo "[4/6] BI 离线看板"
"$PY" bi/build_dashboard.py

echo "[5/6] Pandas / NumPy 分析"
"$PY" analysis/pandas_analysis.py

echo "[6/6] DuckDB OLAP"
"$PY" olap/duckdb_olap.py

echo "完成。产物：output/governance/、output/bi/、output/analysis/、output/olap/"
