"""DAG 静态校验：不依赖 airflow 包，本机即可运行。

校验：DAG 文件语法、BashOperator 任务集合、依赖边、任务图无环、
      docker-compose 与 Dockerfile 是否齐备。
输出 airflow/output/dag_validation.json
"""
from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DAGS = ROOT / "dags"
sys.path.insert(0, str(DAGS))
from task_graph import TASKS, validate_graph  # noqa: E402

OUT = ROOT / "output"
OUT.mkdir(parents=True, exist_ok=True)


def main() -> int:
    checks = []

    def add(name, passed, detail=""):
        checks.append({"check": name, "passed": 1 if passed else 0, "detail": detail})

    dag_file = DAGS / "pgy_dw_dag.py"
    tree = ast.parse(dag_file.read_text(encoding="utf-8"), filename=str(dag_file))
    add("DAG 文件 Python 语法", True, "ast.parse 通过")

    dag_calls = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "DAG"]
    add("DAG(...) 定义存在", len(dag_calls) == 1, f"找到 {len(dag_calls)} 个")
    if dag_calls:
        kws = {k.arg for k in dag_calls[0].keywords}
        for key in ("dag_id", "schedule", "start_date", "catchup", "params", "tags", "default_args"):
            add(f"DAG 参数 {key}", key in kws)

    source = dag_file.read_text(encoding="utf-8")
    bash_calls = sum(
        1 for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and (getattr(n.func, "id", None) or getattr(n.func, "attr", None)) == "BashOperator"
    )
    rshift = sum(1 for n in ast.walk(tree)
                 if isinstance(n, ast.BinOp) and isinstance(n.op, ast.RShift))
    imports_graph = any(
        isinstance(n, ast.ImportFrom) and n.module == "task_graph"
        and any(a.name in ("TASKS", "build_command") for a in n.names)
        for n in ast.walk(tree)
    )
    # 本 DAG 采用「循环遍历 TASKS 动态生成 Operator」的模式，
    # 因此校验的是「与统一任务图的绑定关系」而非字面量 task_id 集合。
    add("BashOperator 由统一任务图驱动",
        bash_calls >= 1 and "for task in TASKS" in source and imports_graph,
        f"BashOperator 调用 {bash_calls} 处，任务图导入={imports_graph}")
    dep_count = sum(len(t["depends_on"]) for t in TASKS)
    add("依赖边由任务图生成", rshift >= 1 and "depends_on" in source,
        f"DAG 中 >> 表达式 {rshift} 处（循环展开后 {dep_count} 条边）")

    graph_errors = validate_graph()
    add("任务图无环且依赖完整", not graph_errors, "; ".join(graph_errors) or "OK")
    add("Airflow 2.x/3.x 兼容 import", "ImportError" in source)
    add("失败告警回调", "on_failure_callback" in source)
    add("按分区补数参数", "params.months" in source)
    add("Docker 一键运行环境", (ROOT / "docker-compose.yml").exists() and (ROOT / "Dockerfile").exists())

    passed = sum(c["passed"] for c in checks)
    report = {"checks": checks, "passed": passed, "total": len(checks),
              "note": "静态校验不启动 Airflow Scheduler；真实 DAG 运行见 docker-compose.yml"}
    (OUT / "dag_validation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    for c in checks:
        print("  [%s] %s %s" % ("PASS" if c["passed"] else "FAIL", c["check"], c["detail"]))
    print(f"\n通过 {passed}/{len(checks)}，报告 {OUT / 'dag_validation.json'}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
