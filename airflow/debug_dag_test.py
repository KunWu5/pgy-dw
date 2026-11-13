"""诊断脚本：打印 Airflow 3.x Execution API 的 422 响应体。

用法（在 pgy_dw 目录）：
    python airflow/debug_dag_test.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "airflow" / "dags"))

import airflow.sdk.api.client as api_client  # noqa: E402

_orig = api_client.get_json_error


def _patched(response):
    req = response.request
    print("\n================ EXECUTION API 4xx/5xx ================", flush=True)
    print("METHOD:", req.method, flush=True)
    print("URL:", req.url, flush=True)
    body = req.content.decode("utf-8", "replace") if req.content else ""
    print("REQ BODY:", body[:3000], flush=True)
    print("STATUS:", response.status_code, flush=True)
    try:
        response.read()
    except Exception as e:  # noqa: BLE001
        print("READ FAILED:", type(e).__name__, e, flush=True)
    print("RESP BODY:", (response.text or "")[:3000], flush=True)
    print("=======================================================\n", flush=True)
    return _orig(response)


api_client.get_json_error = _patched

from airflow.models.dagbag import DagBag  # noqa: E402

DAG_DIR = ROOT / "airflow" / "dags"
bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
print("DAGS:", list(bag.dags.keys()), "ERRORS:", list(bag.import_errors.keys()), flush=True)
dag = bag.dags["pgy_dw_monthly"]

# 只跑第一个任务，快速定位
try:
    dag.test(
        run_conf={"months": os.environ.get("PGY_MONTHS", "2026-08 2026-10")},
        logical_date=datetime(2026, 11, 12, tzinfo=timezone.utc),
    )
except Exception as exc:  # noqa: BLE001
    print("TEST RAISED:", type(exc).__name__, str(exc)[:500], flush=True)
