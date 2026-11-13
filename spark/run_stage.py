"""PySpark / Hive 版数仓：单阶段可独立重跑，供 Airflow DAG 逐任务调用。

用法：
    python spark/run_stage.py --stage all
    python spark/run_stage.py --stage ods
    python spark/run_stage.py --stage dim_dwd --months 2026-10   # 按分区补数
    python spark/run_stage.py --stage quality

阶段：ods -> dim_dwd -> dws -> ads -> quality
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from spark.session import OUT_DIR, prepare_env  # noqa: E402

prepare_env()  # 必须在 import pyspark 之前设置 JAVA_HOME

import config as cfg  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, StringType, StructField, StructType  # noqa: E402
from spark import io_utils as io  # noqa: E402
from spark.session import get_spark  # noqa: E402

SQL_DIR = Path(__file__).resolve().parent / "sql"
STAGES = ["ods", "dim_dwd", "dws", "ads", "quality"]


def exec_hql(spark, name: str, **replacements) -> int:
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    for key, value in replacements.items():
        text = text.replace("${%s}" % key, value)
    count = 0
    for raw in text.split(";"):
        stmt = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if stmt:
            spark.sql(stmt)
            count += 1
    return count


def _pad(row: list, size: int) -> list:
    return (list(row) + [None] * size)[:size]


def stage_ods(spark, months: tuple[str, ...]) -> dict:
    snapshot_rows = []
    for path in sorted(cfg.EXPORTS_DIR.glob("*_pgy_result.xlsx")):
        rows = io.excel_rows(path, cfg.COLUMN_MAP, months)
        batch = datetime.now().strftime("batch_%Y%m%d_%H%M%S")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        snapshot_rows += [r + [path.name, batch, now] for r in rows]
    io.csv_to_view(
        spark,
        io.write_staging_csv(
            snapshot_rows, cfg.ODS_COLUMNS + ["source_file", "ingest_batch", "ingested_at"],
            "ods_snapshot.csv"),
        "v_ods_snapshot")

    list_rows = io.excel_rows(
        cfg.BLOGGER_LIST,
        {"red_id": "red_id", "nickname": "nickname", "group_name": "group_name", "remark": "remark"},
        (),
    )
    io.csv_to_view(
        spark,
        io.write_staging_csv(list_rows, ["red_id", "nickname", "group_name", "remark"],
                             "ods_blogger_list.csv"),
        "v_blogger_list")

    run_rows, mysql_rows = [], []
    for path in sorted(cfg.BACKUPS_DIR.glob("*.sql")):
        sql_text = path.read_text(encoding="utf-8", errors="replace")
        if "INSERT INTO `blogger_snapshots`" not in sql_text:
            continue
        for r in io.backup_insert_rows(sql_text, "crawl_runs"):
            run_rows.append(_pad(r, 8) + [path.name])
        for r in io.backup_insert_rows(sql_text, "blogger_snapshots"):
            mysql_rows.append(_pad(r, 21) + [path.name])
    io.csv_to_view(
        spark,
        io.write_staging_csv(run_rows, ["run_id", "run_month", "started_at", "finished_at",
                                        "total_count", "success_count", "failed_count", "status",
                                        "source_file"], "ods_crawl_run.csv"),
        "v_crawl_run")
    io.csv_to_view(
        spark,
        io.write_staging_csv(mysql_rows, [
            "snapshot_id", "crawl_run_id", "blogger_id", "red_id", "stat_month", "nickname",
            "fans_count", "image_price", "video_price", "tags_raw",
            "note_count_30d", "exposure_median_30d", "read_median_30d", "interaction_median_30d",
            "note_count_90d", "exposure_median_90d", "read_median_90d", "interaction_median_90d",
            "crawl_status", "error_message", "crawled_at", "source_file"],
            "ods_mysql_snapshot.csv"),
        "v_mysql_snapshot")

    exec_hql(spark, "01_ods.hql")
    return {
        "ods_snapshot": io.table_count(spark, "pgy_dw.ods_pgy_monthly_snapshot"),
        "ods_blogger_list": io.table_count(spark, "pgy_dw.ods_blogger_list"),
        "ods_crawl_run": io.table_count(spark, "pgy_dw.ods_crawl_run"),
        "ods_mysql_snapshot": io.table_count(spark, "pgy_dw.ods_mysql_snapshot"),
    }


def stage_dim_dwd(spark, months: tuple[str, ...]) -> dict:
    exec_hql(spark, "02_dim_dwd.hql")

    metric_rows = [[k, l, t, u, r, str(cfg.ALERT_THRESHOLD_PERCENT)]
                   for k, l, t, u, r in cfg.METRICS]
    io.csv_to_view(
        spark,
        io.write_staging_csv(metric_rows, ["metric_key", "metric_label", "metric_type",
                                           "unit", "calc_rule", "alert_threshold_pct"],
                             "dim_metric.csv"),
        "v_dim_metric")
    spark.sql("DROP TABLE IF EXISTS pgy_dw.dim_metric")
    spark.sql("CREATE TABLE pgy_dw.dim_metric USING parquet AS "
              "SELECT metric_key, metric_label, metric_type, unit, calc_rule, "
              "CAST(alert_threshold_pct AS DOUBLE) AS alert_threshold_pct FROM v_dim_metric")

    for month in months:
        exec_hql(spark, "02b_insert_month.hql", MONTH=month)
    exec_hql(spark, "02c_post_load.hql")

    return {
        "dwd_fact": io.table_count(spark, "pgy_dw.dwd_blogger_monthly_snapshot"),
        "failed_record": io.table_count(spark, "pgy_dw.dwd_failed_record"),
        "dim_blogger": io.table_count(spark, "pgy_dw.dim_blogger"),
        "dim_blogger_scd": io.table_count(spark, "pgy_dw.dim_blogger_scd"),
        "dim_month": io.table_count(spark, "pgy_dw.dim_month"),
        "dim_metric": io.table_count(spark, "pgy_dw.dim_metric"),
        "tag_bridge": io.table_count(spark, "pgy_dw.dwd_blogger_tag_bridge"),
        "partitions": len(io.collect_dicts(
            spark, "SHOW PARTITIONS pgy_dw.dwd_blogger_monthly_snapshot", "dwd_parts")),
    }


def stage_dws(spark) -> dict:
    exec_hql(spark, "03_dws.hql")
    return {
        "metric_long": io.table_count(spark, "pgy_dw.dwd_blogger_metric_long"),
        "metric_change": io.table_count(spark, "pgy_dw.dws_metric_monthly_change"),
        "platform_monthly": io.table_count(spark, "pgy_dw.dws_platform_monthly"),
        "tag_monthly": io.table_count(spark, "pgy_dw.dws_tag_monthly"),
    }


def stage_ads(spark) -> dict:
    exec_hql(spark, "04_ads.hql")

    months = [r["stat_month"] for r in io.collect_dicts(
        spark,
        "SELECT DISTINCT stat_month FROM pgy_dw.dwd_blogger_monthly_snapshot ORDER BY stat_month",
        "months")]
    spark.sql("DROP TABLE IF EXISTS pgy_dw.ads_blogger_churn")
    churn = None
    for prev, cur in zip(months, months[1:]):
        p = spark.sql(f"SELECT DISTINCT red_id FROM pgy_dw.dwd_blogger_monthly_snapshot WHERE stat_month='{prev}'")
        c = spark.sql(f"SELECT DISTINCT red_id FROM pgy_dw.dwd_blogger_monthly_snapshot WHERE stat_month='{cur}'")
        part = c.exceptAll(p).withColumn("change_type", F.lit("new")) \
            .unionAll(p.exceptAll(c).withColumn("change_type", F.lit("lost"))) \
            .withColumn("to_month", F.lit(cur)).withColumn("from_month", F.lit(prev))
        churn = part if churn is None else churn.unionAll(part)
    if churn is not None:
        churn.select("to_month", "from_month", "red_id", "change_type").write.mode("overwrite") \
            .saveAsTable("pgy_dw.ads_blogger_churn")

    exported = {}
    for table, name in [
        ("pgy_dw.ads_blogger_value_wide", "spark_ads_blogger_value_wide.csv"),
        ("pgy_dw.ads_metric_alert", "spark_ads_metric_alert.csv"),
        ("pgy_dw.ads_blogger_churn", "spark_ads_blogger_churn.csv"),
        ("pgy_dw.dws_metric_monthly_change", "spark_dws_metric_monthly_change.csv"),
    ]:
        exported[name] = io.write_csv(spark.sql(f"SELECT * FROM {table}"), name)
    return {
        "ads_wide": io.table_count(spark, "pgy_dw.ads_blogger_value_wide"),
        "ads_alert": io.table_count(spark, "pgy_dw.ads_metric_alert"),
        "ads_churn": io.table_count(spark, "pgy_dw.ads_blogger_churn"),
        "exported": exported,
    }


def stage_quality(spark) -> dict:
    checks = []

    def add(name, rule, actual, expected, passed, detail=""):
        checks.append({"check_name": name, "rule": rule, "actual": actual,
                       "expected": expected, "passed": 1 if passed else 0, "detail": detail})

    _tag = [0]

    def n(sql):
        _tag[0] += 1
        return io.scalar(spark, sql, "q%d" % _tag[0])

    ods = n("SELECT COUNT(*) FROM pgy_dw.ods_pgy_monthly_snapshot")
    dwd = n("SELECT COUNT(*) FROM pgy_dw.dwd_blogger_monthly_snapshot")
    fail = n("SELECT COUNT(*) FROM pgy_dw.dwd_failed_record")
    add("ODS 到 DWD 行数守恒", "ODS = DWD 成功 + 失败明细", ods, dwd + fail, abs(ods - dwd - fail) < 0.5)
    dup = n("SELECT COUNT(*) FROM (SELECT stat_month, red_id FROM pgy_dw.dwd_blogger_monthly_snapshot "
            "GROUP BY stat_month, red_id HAVING COUNT(*) > 1)")
    add("DWD 主键唯一", "(stat_month, red_id) 无重复", dup, 0, dup == 0)
    nulls = n("SELECT COUNT(*) FROM pgy_dw.dwd_blogger_monthly_snapshot "
              "WHERE red_id IS NULL OR TRIM(red_id)='' OR stat_month IS NULL")
    add("关键字段非空", "red_id / stat_month 不为空", nulls, 0, nulls == 0)
    invalid = n("SELECT COUNT(*) FROM pgy_dw.dwd_blogger_monthly_snapshot "
                "WHERE fans_count <= 0 OR image_price < 0 OR interaction_median_30d < 0")
    add("数值值域", "粉丝数>0、报价与中位数>=0", invalid, 0, invalid == 0)
    total = n("SELECT COUNT(*) FROM pgy_dw.dwd_blogger_metric_long")
    empty = n("SELECT COUNT(*) FROM pgy_dw.dwd_blogger_metric_long WHERE metric_value IS NULL")
    rate = (total - empty) / total * 100 if total else 0
    add("11 项指标完整率", "非空占比 >= 95%", round(rate, 2), ">= 95", rate >= 95)
    rows = io.collect_dicts(
        spark,
        "SELECT stat_month, COUNT(*) AS t, "
        "SUM(CASE WHEN LOWER(COALESCE(crawl_status,''))='success' THEN 1 ELSE 0 END) AS s "
        "FROM pgy_dw.ods_pgy_monthly_snapshot GROUP BY stat_month ORDER BY stat_month",
        "success_rate")
    for r in rows:
        pct = round(float(r["s"]) / float(r["t"]) * 100, 2)
        add("%s 采集成功率" % r["stat_month"], ">= 95%", pct, ">= 95", pct >= 95)
    parts = len(io.collect_dicts(
        spark, "SHOW PARTITIONS pgy_dw.dwd_blogger_monthly_snapshot", "parts"))
    add("Hive 分区完整性", "分区数 = 快照月份数", parts, len(rows), parts == len(rows))
    tables = io.collect_dicts(spark, "SHOW TABLES IN pgy_dw", "tables")
    add("Hive Metastore 表数量", "信息项", len(tables), "信息项", True)

    # 与 SQLite 版对账：证明两个引擎的分层结果一致
    reconciliation = {}
    for spark_name, sqlite_name in [
        ("spark_ads_blogger_value_wide.csv", "ads_blogger_value_wide.csv"),
        ("spark_ads_metric_alert.csv", "ads_metric_alert.csv"),
        ("spark_ads_blogger_churn.csv", "ads_blogger_churn.csv"),
    ]:
        def count(path):
            with path.open(encoding="utf-8-sig") as f:
                return max(sum(1 for _ in csv.reader(f)) - 1, 0)
        sp = count(OUT_DIR / spark_name)
        sq = count(ROOT / "output" / sqlite_name)
        reconciliation[sqlite_name] = {"spark": sp, "sqlite": sq, "match": sp == sq}
        add("双引擎对账 " + sqlite_name, "Spark 行数 = SQLite 行数", sp, sq, sp == sq)

    (OUT_DIR / "spark_quality_report.json").write_text(
        json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT_DIR / "reconciliation.json").write_text(
        json.dumps(reconciliation, ensure_ascii=False, indent=2), encoding="utf-8")
    failed = [c for c in checks if not c["passed"]]
    return {"checks": len(checks), "passed": len(checks) - len(failed),
            "failed": len(failed), "hive_tables": len(tables), "partitions": parts,
            "reconciliation": reconciliation}


def run(stage: str, months: tuple[str, ...]) -> int:
    spark = get_spark("pgy_dw_%s" % stage)
    print("=" * 62)
    print("PySpark + Hive 数仓 · stage=%s · months=%s" % (stage, ",".join(months)))
    print("=" * 62)
    handlers = {
        "ods": lambda: stage_ods(spark, months),
        "dim_dwd": lambda: stage_dim_dwd(spark, months),
        "dws": lambda: stage_dws(spark),
        "ads": lambda: stage_ads(spark),
        "quality": lambda: stage_quality(spark),
    }
    todo = STAGES if stage == "all" else [stage]
    exit_code = 0
    for s in todo:
        print("[%s]" % s)
        result = handlers[s]()
        io.log(s, result)
        if s == "quality" and result["failed"]:
            exit_code = 1
    print("\nHive 表：")
    for r in io.collect_dicts(spark, "SHOW TABLES IN pgy_dw", "final_tables"):
        print("      pgy_dw.%s" % r["tableName"])
    spark.stop()
    return exit_code


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=STAGES + ["all"], default="all")
    ap.add_argument("--months", nargs="*", default=list(cfg.DEFAULT_MONTHS))
    ap.add_argument("--source-dir", default=None)
    args = ap.parse_args()
    if args.source_dir:
        cfg.SOURCE_DIR = Path(args.source_dir)
        cfg.EXPORTS_DIR = cfg.SOURCE_DIR / "exports"
        cfg.BACKUPS_DIR = cfg.SOURCE_DIR / "backups"
        cfg.BLOGGER_LIST = cfg.SOURCE_DIR / "data" / "blogger_list.xlsx"
    return run(args.stage, tuple(args.months))


if __name__ == "__main__":
    raise SystemExit(main())
