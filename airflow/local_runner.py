"""本机可执行调度器：按与 Airflow DAG 完全相同的任务图串行执行。

用途：在没有 Docker / Linux 的 Windows 机器上，仍可真实验证
「任务依赖 → 重试 → 失败告警 → 按分区补数」这条调度链路，并留下运行报告。
真 Airflow 环境请用 docker-compose.yml 或把 dags/ 挂到 Airflow。
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "dags"))
from task_graph import DEFAULT_MONTHS, TASKS, build_command, validate_graph  # noqa: E402

OUT_DIR = ROOT / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def topo_order(tasks: list[dict]) -> list[dict]:
    done, out = set(), []
    while len(out) < len(tasks):
        progressed = False
        for t in tasks:
            if t["id"] in done:
                continue
            if all(d in done for d in t["depends_on"]):
                out.append(t)
                done.add(t["id"])
                progressed = True
        if not progressed:
            raise RuntimeError("任务图存在环或依赖缺失")
    return out


def run_task(task: dict, months: str, delay_override: int | None) -> dict:
    cmd = [
        sys.executable, str(ROOT.parent / "spark" / "run_stage.py"),
        "--stage", task["stage"], "--months", *months.split(),
    ]
    dag_command = build_command(task, sys.executable, months)
    attempts = []
    for attempt in range(1, task["retries"] + 2):
        started = time.time()
        print(f"    [{task['id']}] 第 {attempt}/{task['retries'] + 1} 次执行: {task['desc']}")
        proc = subprocess.run(cmd, cwd=str(ROOT.parent), capture_output=True,
                              text=True, encoding="utf-8", errors="replace")
        elapsed = round(time.time() - started, 2)
        tail = (proc.stdout or "").strip().splitlines()[-4:]
        attempts.append({"attempt": attempt, "returncode": proc.returncode,
                         "duration_sec": elapsed, "stdout_tail": tail})
        if proc.returncode == 0:
            for line in tail:
                print("      " + line)
            break
        print(f"      失败(rc={proc.returncode})，stderr 尾: {(proc.stderr or '')[-200:]}")
        if attempt <= task["retries"]:
            wait = task["retry_delay_sec"] if delay_override is None else delay_override
            print(f"      {wait}s 后重试")
            time.sleep(wait)
    ok = attempts[-1]["returncode"] == 0
    if not ok:
        print(f"    [ALERT] 任务 {task['id']} 最终失败（对应 Airflow on_failure_callback）")
    return {
        "task_id": task["id"], "stage": task["stage"],
        "depends_on": task["depends_on"], "dag_command": dag_command,
        "status": "success" if ok else "failed", "attempts": attempts,
        "trigger_rule": task.get("trigger_rule", "all_success"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", default=DEFAULT_MONTHS, help='如 "2026-10" 即为按分区补数')
    ap.add_argument("--only", default=None, help="只跑某个 stage（补数场景）")
    ap.add_argument("--retry-delay", type=int, default=1, help="本机验证时缩短重试等待")
    ap.add_argument("--dry-run", action="store_true", help="只打印任务图，不执行")
    args = ap.parse_args()

    errors = validate_graph()
    if errors:
        print("任务图校验失败:", errors)
        return 1
    print("任务图校验通过：5 个任务，依赖无环")
    print("=" * 62)
    print("本机调度执行 | months =", args.months)
    print("=" * 62)
    for t in TASKS:
        print(f"  {t['id']:<9} <- {','.join(t['depends_on']) or '起点':<9} retries={t['retries']}")
    if args.dry_run:
        return 0

    tasks = [t for t in TASKS if t["id"] == args.only] if args.only else topo_order(TASKS)
    results, blocked = [], False
    for t in tasks:
        if blocked and t.get("trigger_rule") != "all_done":
            results.append({"task_id": t["id"], "status": "skipped_upstream_failed"})
            print(f"    [{t['id']}] 上游失败，跳过（trigger_rule=all_success）")
            continue
        r = run_task(t, args.months, args.retry_delay)
        results.append(r)
        blocked = r["status"] == "failed"
        (OUT_DIR / "local_run_report.json").write_text(
            json.dumps({"months": args.months, "ts": datetime.now().isoformat(timespec="seconds"),
                        "tasks": results}, ensure_ascii=False, indent=2), encoding="utf-8")

    ok = all(r["status"] in ("success", "skipped_upstream_failed") for r in results)
    print("\n本机调度结果:", "全部成功" if ok else "存在失败")
    print("报告:", OUT_DIR / "local_run_report.json")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
