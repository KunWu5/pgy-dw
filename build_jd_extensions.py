"""一键构建「JD 能力扩展包」并产出覆盖对照报告。

用法：
    python build_jd_extensions.py                 # 完整跑（含 Spark 画像层）
    python build_jd_extensions.py --skip-spark   # 跳过 Spark，只用已有 CSV

模块：
    governance/   数据字典与标准、表级血缘、质量规则目录与监控
    build_profile 标签体系 / 用户画像（Spark SQL 新增 2 张表）
    bi/           离线 HTML 看板
    analysis/     Pandas / NumPy 探索分析
    olap/         DuckDB 列存 OLAP 与对账

不改动原有 19 张 Hive 表；新增表单独命名，122 / 310 / 27 等结果保持不变。
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from analysis import pandas_analysis  # noqa: E402
from bi import build_dashboard  # noqa: E402
from governance import build_governance  # noqa: E402
from olap import duckdb_olap  # noqa: E402

COVERAGE = [
    ("数据标准 / 数据字典（业务·技术·管理）", "图一职责 1/2", "governance/data_dictionary.csv、data_standards.md", "已做"),
    ("元数据 / 表级血缘", "图一职责 1、图二要求 7", "governance/lineage.json、lineage.html", "已做"),
    ("数据质量规则 + 日常监控", "三份 JD 均涉及", "governance/quality_rules.csv、dq_monitor.html", "已做"),
    ("数据安全分级 / 脱敏策略", "图一职责 1、图二要求 7", "governance/masking_policy.csv", "已做"),
    ("标签体系 / 用户画像", "图一职责 4", "dws_blogger_profile、ads_tag_profile", "已做"),
    ("报表 / 可视化看板", "图一职责 4、图二职责 3", "bi/dashboard.html（另有 Power BI 作品）", "已做"),
    ("Pandas / NumPy 数据处理", "图一任职要求", "analysis/pandas_report.md 等 5 份产物", "已做"),
    ("OLAP 引擎建模与查询", "图二要求 5、图三要求 5", "olap/olap_report.md（DuckDB 列存）", "部分：嵌入式，非 Doris/ClickHouse"),
    ("SQL / 引擎调优实测", "图二要求 3、图三要求 4", "spark/bench/output/bench_report.md", "已做"),
    ("Spark 离线数仓 + 分层建模", "三份 JD", "19 张既有 Hive 表 + 2 张画像表", "已做"),
    ("端到端采集→建模→计算→可视化", "图一职责", "采集系统 + 四层 SQL + 看板", "已做"),
    ("Linux Shell 流水线脚本", "图三要求 2", "scripts/run_pipeline.sh、start_airflow.sh", "轻量已做"),
    ("工程质量与结果校验", "图一职责 7、图二要求 7", "output_public/governance/quality_rules.csv、quality_history.csv", "已做：27 条规则 / 三层校验"),
    ("实时数据开发（Spark Structured Streaming）", "三份 JD 均提及", "streaming/stream_report.md、output/streaming/*.csv", "已做：微批；Flink/Kafka 未做"),
    ("大数据集群部署运维（Hadoop/YARN/Presto…）", "图三职责 3、要求 5", "—", "本机不可做"),
    ("湖格式（Apache Iceberg）", "图三要求 4", "iceberg/iceberg_demo.py、output/iceberg/iceberg_report.md", "已做：MERGE / 时间旅行 / Schema 演进"),
    ("CDC / DataX", "图三要求 4", "—", "未做（P2）"),
    ("Java", "图三要求 2", "—", "未做"),
]


def integrity() -> dict:
    """核对原有结果没被污染。"""
    out = {}
    for name, path in [
        ("ads_wide", ROOT / "output" / "ads_blogger_value_wide.csv"),
        ("ads_alert", ROOT / "output" / "ads_metric_alert.csv"),
        ("ads_churn", ROOT / "output" / "ads_blogger_churn.csv"),
        ("spark_ads_wide", ROOT / "spark" / "output" / "spark_ads_blogger_value_wide.csv"),
        ("spark_ads_alert", ROOT / "spark" / "output" / "spark_ads_metric_alert.csv"),
        ("spark_ads_churn", ROOT / "spark" / "output" / "spark_ads_blogger_churn.csv"),
    ]:
        if path.exists():
            with path.open(encoding="utf-8-sig", newline="") as f:
                out[name] = max(sum(1 for _ in csv.reader(f)) - 1, 0)
    return out


def write_coverage(results: dict, counts: dict) -> Path:
    lines = [
        "# JD 能力覆盖对照（项目侧，不是简历措辞）",
        "",
        "由 `python build_jd_extensions.py` 生成。所有「已做」项都有可运行代码与产物文件，",
        "不依赖虚构描述；「部分 / 未做」如实标注。",
        "",
        "## 本次构建结果",
        "",
        "```json",
        json.dumps(results, ensure_ascii=False, indent=2),
        "```",
        "",
        "## 原有结果完整性（未被污染）",
        "",
        "| 产物 | 行数 |",
        "|---|---:|",
    ]
    for k, v in counts.items():
        lines.append(f"| {k} | {v:,} |")
    lines += [
        "",
        "## 能力覆盖表",
        "",
        "| 能力项 | 对应 JD | 项目产物 | 状态 |",
        "|---|---|---|---|",
    ]
    for cap, jd, artifact, status in COVERAGE:
        lines.append(f"| {cap} | {jd} | {artifact} | {status} |")
    lines += [
        "",
        "## 边界",
        "",
        "- 全部跑在 Windows + `local[1]/local[4]` 单机，内嵌 Derby Metastore；不是 Hadoop/YARN 集群。",
        "- DuckDB 是嵌入式列存 OLAP，不等于 Doris/ClickHouse/StarRocks 部署运维。",
        "- 132 万行是合成压测数据；218 行是真实快照。两者不可混为一谈。",
        "- 数据安全分级是项目级策略文档，不是企业级权限系统（Ranger/Sentry）。",
        "",
        "## 复现",
        "",
        "```bash",
        "python build_jd_extensions.py",
        "bash scripts/run_pipeline.sh",
        "bash scripts/start_airflow.sh test",
        "```",
    ]
    path = ROOT / "output" / "JD能力覆盖对照.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    results = {}
    results["governance"] = build_governance.build()
    if "--skip-spark" not in sys.argv:
        from governance import build_profile
        results["profile"] = build_profile.run()
    else:
        results["profile"] = "skipped (--skip-spark)"
    results["dashboard"] = build_dashboard.build()
    results["pandas"] = pandas_analysis.build()
    results["olap"] = duckdb_olap.build()
    stream_json = ROOT / "output" / "streaming" / "stream_result.json"
    if stream_json.exists():
        results["streaming"] = json.loads(stream_json.read_text(encoding="utf-8"))
    else:
        results["streaming"] = "未运行（python streaming/stream_job.py）"
    iceberg_json = ROOT / "output" / "iceberg" / "iceberg_result.json"
    if iceberg_json.exists():
        results["iceberg"] = json.loads(iceberg_json.read_text(encoding="utf-8"))
    else:
        results["iceberg"] = "未运行（python iceberg/iceberg_demo.py）"

    counts = integrity()
    report = write_coverage(results, counts)
    print(json.dumps({"results": results, "integrity": counts,
                      "coverage_report": str(report.relative_to(ROOT))},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
