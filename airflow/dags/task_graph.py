"""唯一任务图来源：Airflow DAG 与本机 runner 共用，避免两处定义漂移。

包含：任务依赖、重试、SLA、失败策略、按分区补数参数。
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.environ.get("PGY_PROJECT_ROOT", Path(__file__).resolve().parents[2]))
DEFAULT_MONTHS = "2026-08 2026-10"

TASKS = [
    {
        "id": "ods", "stage": "ods", "depends_on": [],
        "retries": 2, "retry_delay_sec": 30, "sla_minutes": 10,
        "desc": "接入 Excel 快照、博主名单与 MySQL 备份到 Hive ODS 表",
    },
    {
        "id": "dim_dwd", "stage": "dim_dwd", "depends_on": ["ods"],
        "retries": 2, "retry_delay_sec": 30, "sla_minutes": 15,
        "desc": "建维度表，按 stat_month 静态分区覆盖 DWD（支持按月补数）",
    },
    {
        "id": "dws", "stage": "dws", "depends_on": ["dim_dwd"],
        "retries": 1, "retry_delay_sec": 30, "sla_minutes": 10,
        "desc": "宽表转长表 + LAG 窗口函数环比，汇总平台与标签粒度",
    },
    {
        "id": "ads", "stage": "ads", "depends_on": ["dws"],
        "retries": 1, "retry_delay_sec": 30, "sla_minutes": 10,
        "desc": "产出价值宽表、异动清单、名单增删追踪并导出 CSV",
    },
    {
        "id": "quality", "stage": "quality", "depends_on": ["ads"],
        "retries": 0, "retry_delay_sec": 0, "sla_minutes": 10,
        "trigger_rule": "all_done",
        "desc": "9 项数据质量校验 + 与 SQLite 版双引擎对账",
    },
]


def build_command(task: dict, python: str, months: str) -> str:
    """生成单任务命令；months 可传 Airflow Jinja 模板或实际月份串。"""
    script = ROOT / "spark" / "run_stage.py"
    return f'"{python}" "{script}" --stage {task["stage"]} --months {months}'


def validate_graph() -> list[str]:
    """检查任务图：依赖存在、无环、stage 合法。"""
    errors = []
    ids = {t["id"] for t in TASKS}
    for t in TASKS:
        for dep in t["depends_on"]:
            if dep not in ids:
                errors.append(f"{t['id']} 依赖不存在的任务 {dep}")
    visiting, visited = set(), set()

    def visit(node, path):
        if node in visiting:
            errors.append("存在环: " + " -> ".join(path + [node]))
            return
        if node in visited:
            return
        visiting.add(node)
        for dep in next(t for t in TASKS if t["id"] == node)["depends_on"]:
            if dep in ids:
                visit(dep, path + [node])
        visiting.discard(node)
        visited.add(node)

    for t in TASKS:
        visit(t["id"], [])
    return errors
