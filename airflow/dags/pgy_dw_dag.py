"""蒲公英达人离线数仓 · Airflow DAG（标准 API，兼容 Airflow 2.4+ / 3.x）。

任务图来自同目录 task_graph.py，与本机 local_runner.py 共用同一份定义。
每月 25 日 02:00 调度（对齐原采集系统写入后），支持通过 params.months 指定补数月份。

在装有 Docker 的 Linux 机器上：docker compose up --build 会执行一次
`airflow dags test pgy_dw_monthly <日期>`，真实跑完这 5 个任务。
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
import platform

sys.path.insert(0, str(Path(__file__).resolve().parent))

try:  # Airflow 2.x
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:  # Airflow 3.x
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.bash import BashOperator

from task_graph import TASKS, build_command  # noqa: E402


def alert_on_failure(context) -> None:
    """失败告警：生产环境可替换为邮件 / 企业微信 webhook。"""
    ti = context.get("task_instance")
    task_id = getattr(ti, "task_id", "unknown")
    print(f"[ALERT] pgy_dw_monthly 任务失败: {task_id}, "
          f"run_id={getattr(ti, 'run_id', '-')}, try={getattr(ti, 'try_number', '-')}")


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": alert_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pgy_dw_monthly",
    description="蒲公英达人离线数仓 ODS→DWD→DWS→ADS，Spark SQL + Hive 分区 + 质量校验",
    start_date=datetime(2026, 11, 1),
    schedule="0 2 25 * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    params={"months": "2026-08 2026-10",
            "python": "python" if platform.system() == "Windows" else "python3"},
    tags=["spark", "hive", "pgy", "data-warehouse"],
) as dag:
    operators = {}
    for task in TASKS:
        operators[task["id"]] = BashOperator(
            task_id=task["id"],
            bash_command=build_command(
                task, "{{ params.python }}", "{{ params.months }}"
            ),
            retries=task["retries"],
            retry_delay=timedelta(seconds=task["retry_delay_sec"]),
            # Airflow 3.x 已移除 SLA 机制（传 sla= 只会告警），改用 execution_timeout 兜底
            execution_timeout=timedelta(minutes=20),
            trigger_rule=task.get("trigger_rule", "all_success"),
        )

    for task in TASKS:
        for dep in task["depends_on"]:
            operators[dep] >> operators[task["id"]]
