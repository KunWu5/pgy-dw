"""数据质量规则目录 + 运行历史 + 监控看板。

对应 JD：图一职责 1（质量）、图二要求 7（数据质量）、图三职责 5（日常质量监控）

规则清单与运行结果都从三份已生成的报告读取，全部是真实跑过的检查：
  output/quality_report.json               SQLite 版 9 项
  spark/output/spark_quality_report.json   Spark/Hive 版 12 项
  spark/bench/output/bench_report.json     132 万行压测版 6 项
规则定义位置见 warehouse/quality.py、spark/run_stage.py、spark/bench/run_bench.py。

产物：quality_rules.csv / quality_history.csv / dq_monitor.html
"""
from __future__ import annotations

import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from governance import OUT  # noqa: E402

ADD_RE = re.compile(r"add\(\s*[\"'](.+?)[\"']\s*,\s*[\"'](.+?)[\"']")
DICT_RE = re.compile(r"\{\s*\"name\"\s*:\s*[\"'](.+?)[\"']\s*,\s*\"rule\"\s*:\s*[\"'](.+?)[\"']")

SOURCES = [
    ("SQLite 分层", ROOT / "warehouse" / "quality.py"),
    ("Spark/Hive 分层", ROOT / "spark" / "run_stage.py"),
    ("132 万行压测", ROOT / "spark" / "bench" / "run_bench.py"),
]
REPORTS = [
    ("SQLite 218 行", ROOT / "output" / "quality_report.json", None),
    ("Spark 218 行", ROOT / "spark" / "output" / "spark_quality_report.json", None),
    ("Spark 132 万行", ROOT / "spark" / "bench" / "output" / "bench_report.json", "quality_at_scale"),
]
SOURCE_HINT = {
    "SQLite 218 行": "warehouse/quality.py",
    "Spark 218 行": "spark/run_stage.py::stage_quality",
    "Spark 132 万行": "spark/bench/run_bench.py::quality_at_scale",
}


def _checks(path: Path, key: str | None) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if key:
        return data.get(key, {}).get("checks", [])
    if isinstance(data, dict) and "checks" in data:
        return data["checks"]
    return data if isinstance(data, list) else []


def rules() -> list[dict]:
    """以「真实跑过的检查」为准：从三份运行报告读规则名与口径，源码只作定义位置说明。"""
    out, seq = [], 0
    for scope, path, key in REPORTS:
        if not path.exists():
            continue
        for c in _checks(path, key):
            name = c.get("check_name") or c.get("name") or "-"
            rule = c.get("rule", "")
            seq += 1
            out.append({
                "rule_id": f"DQ-{seq:03d}", "scope": scope,
                "check_name": name, "rule": rule,
                "definition_source": SOURCE_HINT.get(scope, "-"),
                "severity": "阻断" if any(k in rule for k in ("守恒", "唯一", "非空", "分区", "对账"))
                             or "唯一" in name or "守恒" in name else "告警",
                "owner_role": "数据开发",
            })
    return out


def history() -> list[dict]:
    out = []
    for scope, path, key in REPORTS:
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        if key:
            data = data.get(key, {})
            checks = data.get("checks", [])
            total, passed = data.get("total", len(checks)), data.get("passed", 0)
        elif isinstance(data, dict) and "checks" in data:
            checks = data["checks"]
            total = len(checks)
            passed = sum(1 for c in checks if c.get("passed") in (1, True))
        else:
            checks = data if isinstance(data, list) else []
            total = len(checks)
            passed = sum(1 for c in checks if c.get("passed") in (1, True))
        out.append({
            "run_scope": scope, "report": str(path.relative_to(ROOT)).replace("\\", "/"),
            "checked_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
            "total": total, "passed": passed, "failed": total - passed,
            "pass_rate_pct": round(passed / total * 100, 2) if total else 0,
        })
    return out


def build() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    rs, hs = rules(), history()

    with (OUT / "quality_rules.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["rule_id", "scope", "check_name", "rule",
                                          "definition_source", "severity", "owner_role"])
        w.writeheader()
        w.writerows(rs)
    with (OUT / "quality_history.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["run_scope", "report", "checked_at", "total",
                                          "passed", "failed", "pass_rate_pct"])
        w.writeheader()
        w.writerows(hs)

    parts = [
        "<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>数据质量监控</title>",
        "<style>body{font:14px/1.6 -apple-system,'Segoe UI',sans-serif;color:#222;margin:32px auto;max-width:1000px}"
        "h1{font-size:20px;font-weight:500}h2{font-size:15px;font-weight:500;margin-top:28px}"
        "table{border-collapse:collapse;width:100%;font-size:13px}th,td{border:1px solid #ddd;padding:6px 10px;text-align:left}"
        "th{background:#f6f6f4}.ok{color:#3b6d11;font-weight:500}.bar{height:9px;background:#eef3e8;border-radius:5px;display:inline-block;width:180px;vertical-align:middle}"
        ".fill{height:9px;background:#639922;border-radius:5px}code{background:#f4f4f1;padding:1px 4px}</style>",
        "<h1>数据质量监控看板</h1>",
        "<p>规则从源码解析，结果来自三份真实运行报告；每次跑完流水线刷新本页即可做趋势跟踪。</p>",
        "<h2>运行历史</h2><table><tr><th>场景</th><th>时间</th><th>规则数</th><th>通过</th><th>失败</th><th>通过率</th></tr>",
    ]
    for h in hs:
        parts.append(
            f"<tr><td>{h['run_scope']}</td><td>{h['checked_at']}</td><td>{h['total']}</td>"
            f"<td class='ok'>{h['passed']}</td><td>{h['failed']}</td>"
            f"<td><span class='bar'><span class='fill' style='width:{h['pass_rate_pct'] * 1.8:.0f}px'></span></span> "
            f"{h['pass_rate_pct']}%</td></tr>")
    parts += ["</table><h2>规则目录（全部为实际运行过的检查）</h2>"
              "<table><tr><th>ID</th><th>运行范围</th><th>检查项</th><th>规则</th><th>定义位置</th><th>级别</th></tr>"]
    for r in rs:
        parts.append(
            f"<tr><td><code>{r['rule_id']}</code></td><td>{r['scope']}</td>"
            f"<td>{r['check_name']}</td><td>{r['rule']}</td>"
            f"<td><code>{r['definition_source']}</code></td><td>{r['severity']}</td></tr>")
    parts.append("</table></html>")
    (OUT / "dq_monitor.html").write_text("\n".join(parts), encoding="utf-8")

    return {"rules": len(rs), "runs": len(hs),
            "all_passed": all(h["failed"] == 0 for h in hs) if hs else False,
            "outputs": ["quality_rules.csv", "quality_history.csv", "dq_monitor.html"]}


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False))
