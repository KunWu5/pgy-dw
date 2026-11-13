"""表级血缘：从分层 SQL 与 Airflow 任务图解析，不靠人工维护。

对应 JD：图一职责 1（元数据）、图二要求 7（元数据 / 血缘）

产物：
  output/governance/lineage.json   nodes（表 / 分层 / 类型）+ edges（源→目标 / 阶段）
  output/governance/lineage.html   离线可看的血缘图 + 明细表
"""
from __future__ import annotations

import ast
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from governance import OUT  # noqa: E402

SQL_DIR = ROOT / "spark" / "sql"
DAG_DIR = ROOT / "airflow" / "dags"

TARGET_RE = re.compile(
    r"(?:CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|INSERT\s+OVERWRITE\s+TABLE)\s+"
    r"((?:pgy_dw|pgy_dw_bench)\.[a-zA-Z0-9_]+|v_[a-zA-Z0-9_]+)", re.I)
SRC_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+((?:pgy_dw|pgy_dw_bench)\.[a-zA-Z0-9_]+|v_[a-zA-Z0-9_]+)", re.I)

STAGE_BY_FILE = {
    "01_ods.hql": "ods", "02_dim_dwd.hql": "dim_dwd", "02b_insert_month.hql": "dim_dwd",
    "02c_post_load.hql": "dim_dwd", "03_dws.hql": "dws", "04_ads.hql": "ads",
    "05_profile.hql": "profile",
}
LAYER_PREFIX = {"ods_": "ODS", "dwd_": "DWD", "dim_": "DIM", "dws_": "DWS", "ads_": "ADS"}


def layer_of(table: str) -> str:
    short = table.split(".")[-1]
    if short.startswith("v_"):
        return "STAGING"
    for prefix, layer in LAYER_PREFIX.items():
        if short.startswith(prefix):
            return layer
    return "OTHER"


def parse_sql() -> tuple[dict, list]:
    nodes, edges = {}, []
    for path in sorted(SQL_DIR.glob("*.hql")):
        stage = STAGE_BY_FILE.get(path.name, path.stem)
        text = path.read_text(encoding="utf-8")
        for raw in text.split(";"):
            stmt = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not stmt:
                continue
            targets = TARGET_RE.findall(stmt)
            if not targets:
                continue
            target = targets[-1]
            sources = sorted(set(SRC_RE.findall(stmt)))
            for t in [target, *sources]:
                nodes.setdefault(t, {"table": t, "layer": layer_of(t),
                                    "kind": "view" if t.startswith("v_") else "hive_table"})
            for src in sources:
                if src != target:
                    edges.append({"source": src, "target": target,
                                  "stage": stage, "sql_file": path.name})
    return nodes, edges


def parse_tasks() -> list[dict]:
    path = DAG_DIR / "task_graph.py"
    if not path.exists():
        return []
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "TASKS" for t in node.targets):
                return ast.literal_eval(node.value)
    except Exception:
        pass
    return []


def build() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    nodes, edges = parse_sql()
    tasks = parse_tasks()

    # 每条边去重（同一源表可能在多个语句里出现）
    uniq = {(e["source"], e["target"]): e for e in edges}
    edges = sorted(uniq.values(), key=lambda e: (e["stage"], e["target"], e["source"]))

    payload = {
        "source": "spark/sql/*.hql + airflow/dags/task_graph.py",
        "node_count": len(nodes), "edge_count": len(edges),
        "nodes": sorted(nodes.values(), key=lambda n: (n["layer"], n["table"])),
        "edges": edges,
        "task_graph": [{"task": t["id"], "depends_on": t.get("depends_on", [])}
                       for t in tasks],
    }
    (OUT / "lineage.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    by_target = defaultdict(list)
    for e in edges:
        by_target[e["target"]].append(e["source"])
    layer_counts = defaultdict(int)
    for n in nodes.values():
        layer_counts[n["layer"]] += 1

    parts = [
        "<!doctype html><html lang='zh-CN'><meta charset='utf-8'>",
        "<title>pgy_dw 表级血缘</title>",
        "<style>body{font:14px/1.6 -apple-system,'Segoe UI',sans-serif;color:#222;margin:32px auto;max-width:1000px}"
        "h1{font-size:20px;font-weight:500}h2{font-size:15px;font-weight:500;margin-top:28px}"
        "table{border-collapse:collapse;width:100%;font-size:13px}"
        "th,td{border:1px solid #ddd;padding:6px 10px;text-align:left}"
        "th{background:#f6f6f4}code{background:#f4f4f1;padding:1px 4px;border-radius:3px}"
        ".tag{display:inline-block;padding:1px 8px;border-radius:10px;background:#eef3e8;color:#3b6d11;font-size:12px}</style>",
        "<h1>pgy_dw 表级血缘</h1>",
        "<p>由 <code>spark/sql/*.hql</code> 的 CREATE / INSERT 语句与 FROM / JOIN 自动解析，"
        f"共 <b>{len(nodes)}</b> 个节点、<b>{len(edges)}</b> 条依赖边。修改 SQL 后重跑即可更新。</p>",
        "<svg viewBox='0 0 900 130' width='100%' role='img'>",
    ]
    layers = ["ODS", "STAGING", "DWD", "DIM", "DWS", "ADS"]
    xs = [30 + i * 145 for i in range(len(layers))]
    for i, layer in enumerate(layers):
        x = xs[i]
        parts.append(
            f"<rect x='{x}' y='30' width='120' height='56' rx='8' fill='#f4f7ef' stroke='#97c459' stroke-width='0.5'/>"
            f"<text x='{x + 60}' y='53' text-anchor='middle' font-size='13' font-weight='500' fill='#27500A'>{layer}</text>"
            f"<text x='{x + 60}' y='72' text-anchor='middle' font-size='12' fill='#3B6D11'>{layer_counts.get(layer, 0)} 个对象</text>")
        if i:
            parts.append(
                f"<path d='M{x - 25} 58 L{x - 5} 58' fill='none' stroke='#888' stroke-width='1'/>"
                f"<path d='M{x - 9} 54 L{x - 5} 58 L{x - 9} 62' fill='none' stroke='#888' stroke-width='1'/>")
    parts += ["</svg>", "<h2>目标表 ← 来源表</h2>",
              "<table><tr><th>目标表</th><th>层</th><th>来源表</th><th>阶段</th></tr>"]
    for target, sources in sorted(by_target.items(), key=lambda kv: (layer_of(kv[0]), kv[0])):
        stage = next((e["stage"] for e in edges if e["target"] == target), "-")
        parts.append(
            f"<tr><td><code>{target}</code></td><td><span class='tag'>{layer_of(target)}</span></td>"
            f"<td>{', '.join('<code>' + s + '</code>' for s in sources)}</td><td>{stage}</td></tr>")
    parts.append("</table>")
    if payload["task_graph"]:
        parts.append("<h2>Airflow 任务依赖</h2><table><tr><th>任务</th><th>上游</th></tr>")
        for t in payload["task_graph"]:
            parts.append(f"<tr><td><code>{t['task']}</code></td><td>"
                         f"{', '.join(t['depends_on']) or '（无）'}</td></tr>")
        parts.append("</table>")
    parts.append("</html>")
    (OUT / "lineage.html").write_text("\n".join(parts), encoding="utf-8")

    return {"nodes": len(nodes), "edges": len(edges), "tasks": len(payload["task_graph"]),
            "outputs": ["lineage.json", "lineage.html"]}


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False))
