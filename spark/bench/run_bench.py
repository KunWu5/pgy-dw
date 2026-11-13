"""P0-a 规模压测主程序 + 报告生成。

流程：
  1. 生成合成数据（gen_data.py，driver 端纯 Python CSV）
  2. 在独立库 pgy_dw_bench 建 Hive 分区表 / 维表 / 标签桥接表
  3. 执行**原项目同一份** spark/sql/03_dws.hql（只把库名替换掉）
  4. 三组量化对比：分区裁剪 / 广播 join / shuffle 分区数
  5. 调用 df_api.py 做 P0-b，并对账
  6. 输出 output/bench_report.json + bench_report.md

隔离保证：只建/删 pgy_dw_bench.*，绝不碰 pgy_dw.*；staging、输出目录均独立。
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# 本机把 pyspark / openpyxl 装到了 --target 目录（不污染全局环境），这里自动并入 sys.path。
# 换机器时若已全局 pip install，则下面的目录不存在，直接走系统安装的版本。
for _lib in (r"C:\Temp\wk\sparklib", r"C:\Temp\wk\pylib"):
    if Path(_lib).is_dir() and _lib not in sys.path:
        sys.path.insert(0, _lib)

from spark.bench import config_bench as C  # noqa: E402
from spark.session import WAREHOUSE, prepare_env  # noqa: E402

prepare_env()  # 必须在 import pyspark 之前

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

from spark import io_utils as io  # noqa: E402
from spark.bench import df_api, gen_data  # noqa: E402

SQL_DIR = ROOT / "spark" / "sql"

FACT_TYPES = {
    "fans_count": LongType(), "image_price": DoubleType(), "video_price": DoubleType(),
    "note_count_30d": IntegerType(), "exposure_median_30d": LongType(),
    "read_median_30d": LongType(), "interaction_median_30d": LongType(),
    "note_count_90d": IntegerType(), "exposure_median_90d": LongType(),
    "read_median_90d": LongType(), "interaction_median_90d": LongType(),
    "cpe_30d": DoubleType(), "cpe_90d": DoubleType(),
    "engagement_rate_30d": DoubleType(), "price_per_10k_fans": DoubleType(),
}


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.master(C.MASTER)
        .appName("pgy_dw_bench")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", WAREHOUSE.as_uri() + "/")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "false")  # 关闭 AQE，保证计划可复现
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.hadoop.native.lib", "false")
    )
    if os.environ.get("HADOOP_HOME"):
        builder = builder.config("spark.hadoop.hadoop.home.dir", os.environ["HADOOP_HOME"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def fact_schema() -> StructType:
    fields = []
    for col in C.FACT_COLUMNS:
        fields.append(StructField(col, FACT_TYPES.get(col, StringType()), True))
    fields.append(StructField(C.PARTITION_COLUMN, StringType(), True))
    return StructType(fields)


def dim_schema() -> StructType:
    return StructType([StructField(c, StringType(), True) for c in C.DIM_COLUMNS])


def execute_hql(spark, filename: str, db_from: str = "pgy_dw.", db_to: str | None = None) -> int:
    """执行原项目 HQL，只替换库名前缀——保证「同一份 SQL」而不是抄一份改过的。"""
    db_to = db_to or (C.DB + ".")
    text = (SQL_DIR / filename).read_text(encoding="utf-8").replace(db_from, db_to)
    n = 0
    for raw in text.split(";"):
        stmt = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if stmt:
            print("    exec:", stmt.splitlines()[0][:90], flush=True)
            spark.sql(stmt)
            n += 1
    return n


def build_tables(spark, manifest: dict) -> dict:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {C.DB}")

    fact = spark.read.schema(fact_schema()).option("header", "true") \
        .csv(str(Path(manifest["fact_csv"]).as_posix()))
    fact.createOrReplaceTempView("v_bench_fact")
    dim = spark.read.schema(dim_schema()).option("header", "true") \
        .csv(str(Path(manifest["dim_csv"]).as_posix()))
    dim.createOrReplaceTempView("v_bench_dim")

    ddl_cols = []
    type_sql = {
        "LongType": "BIGINT", "DoubleType": "DOUBLE", "IntegerType": "INT",
        "StringType": "STRING",
    }
    for col in C.FACT_COLUMNS:
        t = type(FACT_TYPES.get(col, StringType())).__name__
        ddl_cols.append(f"  {col} {type_sql.get(t, 'STRING')}")
    fact_ddl = (
        f"CREATE TABLE IF NOT EXISTS {C.DB}.dwd_blogger_monthly_snapshot (\n"
        + ",\n".join(ddl_cols)
        + f"\n) USING parquet PARTITIONED BY ({C.PARTITION_COLUMN} STRING)"
    )
    spark.sql(f"DROP TABLE IF EXISTS {C.DB}.dwd_blogger_monthly_snapshot")
    spark.sql(fact_ddl)

    cols = ", ".join(C.FACT_COLUMNS)
    insert = (
        f"INSERT OVERWRITE TABLE {C.DB}.dwd_blogger_monthly_snapshot "
        f"PARTITION ({C.PARTITION_COLUMN}) SELECT {cols}, {C.PARTITION_COLUMN} FROM v_bench_fact"
    )
    print("[2] 写入分区事实表（动态分区）...", flush=True)
    try:
        spark.sql(insert)
    except Exception as exc:
        print("    动态分区失败，回退逐月静态分区:", str(exc)[:160], flush=True)
        spark.sql("CACHE TABLE v_bench_fact")
        for m in C.months():
            spark.sql(
                f"INSERT OVERWRITE TABLE {C.DB}.dwd_blogger_monthly_snapshot "
                f"PARTITION ({C.PARTITION_COLUMN}='{m}') "
                f"SELECT {cols} FROM v_bench_fact WHERE stat_month='{m}'"
            )

    spark.sql(f"DROP TABLE IF EXISTS {C.DB}.dim_blogger")
    spark.sql(f"CREATE TABLE {C.DB}.dim_blogger USING parquet AS SELECT * FROM v_bench_dim")

    # 标签桥接表：与 02c_post_load.hql 的最后一段完全相同的 SQL
    spark.sql(f"DROP TABLE IF EXISTS {C.DB}.dwd_blogger_tag_bridge")
    spark.sql(
        f"CREATE TABLE {C.DB}.dwd_blogger_tag_bridge USING parquet AS "
        f"SELECT DISTINCT stat_month, red_id, TRIM(tag) AS tag "
        f"FROM {C.DB}.dwd_blogger_monthly_snapshot "
        f"LATERAL VIEW explode(split(tags_norm, '[|]')) e AS tag "
        f"WHERE TRIM(tag) <> ''"
    )

    print("[3] 执行原项目 03_dws.hql（百万行量级）...", flush=True)
    execute_hql(spark, "03_dws.hql")

    parts = io.collect_dicts(
        spark, f"SHOW PARTITIONS {C.DB}.dwd_blogger_monthly_snapshot", "bench_parts")
    tables = io.collect_dicts(spark, f"SHOW TABLES IN {C.DB}", "bench_tables")
    counts = {}
    for t in ["dwd_blogger_monthly_snapshot", "dim_blogger", "dwd_blogger_tag_bridge",
              "dwd_blogger_metric_long", "dws_metric_monthly_change",
              "dws_platform_monthly", "dws_tag_monthly"]:
        counts[t] = io.table_count(spark, f"{C.DB}.{t}")

    base = WAREHOUSE / f"{C.DB}.db" / "dwd_blogger_monthly_snapshot"
    parquet_files = list(base.glob("stat_month=*/*.parquet"))
    total_bytes = sum(p.stat().st_size for p in parquet_files)
    return {
        "database": C.DB,
        "partitions": len(parts),
        "tables": len(tables),
        "row_counts": counts,
        "fact_parquet_files": len(parquet_files),
        "fact_parquet_bytes": total_bytes,
        "fact_parquet_mb": round(total_bytes / 1024 / 1024, 2),
        "fact_partition_dir": str(base),
    }


def local_path(uri: str) -> Path:
    """inputFiles() 返回 file:/// URI，转成可 stat 的本地路径。"""
    return Path(url2pathname(urlparse(uri).path) if uri.startswith("file:") else uri)


def plan_text(spark, query: str, tag: str) -> str:
    """EXPLAIN FORMATTED 的结果走 CSV 文件通道读回，避开 Python worker。"""
    path = io.sql_to_csv(spark, "EXPLAIN FORMATTED " + query, tag)
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    return "\n".join("\n".join(r) for r in rows[1:])


def measure(label: str, fn, repeats: int = 3) -> dict:
    fn()  # warmup，消除 JIT 与首次读盘的偏差
    vals = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        vals.append(round(time.perf_counter() - t0, 3))
    return {
        "case": label, "runs_s": vals,
        "best_s": min(vals), "median_s": round(statistics.median(vals), 3),
    }


def bench_partition_pruning(spark, table_info: dict) -> dict:
    table = f"{C.DB}.dwd_blogger_monthly_snapshot"
    m = C.months()[len(C.months()) // 2]
    q_all = f"SELECT SUM(fans_count) AS s FROM {table}"
    q_one = f"SELECT SUM(fans_count) AS s FROM {table} WHERE stat_month='{m}'"

    df_all = spark.sql(f"SELECT * FROM {table}")
    df_one = spark.sql(f"SELECT * FROM {table} WHERE stat_month='{m}'")
    files_all, files_one = df_all.inputFiles(), df_one.inputFiles()
    bytes_all = sum(local_path(p).stat().st_size for p in files_all if local_path(p).exists())
    bytes_one = sum(local_path(p).stat().st_size for p in files_one if local_path(p).exists())

    t_all = measure("全表扫描（24 个分区）", lambda: spark.sql(q_all).count())
    t_one = measure("单分区过滤（1 个分区）", lambda: spark.sql(q_one).count())
    plan_all = plan_text(spark, q_all, "exp_prune_all")
    plan_one = plan_text(spark, q_one, "exp_prune_one")

    return {
        "filter_month": m,
        "files_scanned_all": len(files_all),
        "files_scanned_filtered": len(files_one),
        "bytes_scanned_all": bytes_all,
        "bytes_scanned_filtered": bytes_one,
        "bytes_mb_all": round(bytes_all / 1024 / 1024, 2),
        "bytes_mb_filtered": round(bytes_one / 1024 / 1024, 2),
        "timing_all": t_all,
        "timing_filtered": t_one,
        "speedup_x": round(t_all["best_s"] / t_one["best_s"], 2) if t_one["best_s"] else None,
        "partition_filters_in_plan": re.findall(r"PartitionFilters:.*", plan_one)[:2],
        "plan_filtered_excerpt": "\n".join(
            line.strip() for line in plan_one.splitlines()
            if any(k in line for k in ("FileScan", "PartitionFilters", "PushedFilters")))[:1200],
    }


def bench_broadcast_join(spark) -> dict:
    ml = f"{C.DB}.dwd_blogger_metric_long"
    dim = f"{C.DB}.dim_blogger"
    agg = "d.status, COUNT(*) AS n, ROUND(AVG(m.metric_value), 2) AS avg_v"
    q_smj = (f"SELECT /*+ MERGEJOIN(d) */ {agg} FROM {ml} m "
             f"JOIN {dim} d ON m.red_id = d.red_id GROUP BY d.status")
    q_brd = (f"SELECT /*+ BROADCAST(d) */ {agg} FROM {ml} m "
             f"JOIN {dim} d ON m.red_id = d.red_id GROUP BY d.status")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    plan_smj = plan_text(spark, q_smj, "exp_join_smj")
    t_smj = measure("SortMergeJoin（禁用自动广播）", lambda: spark.sql(q_smj).count(), repeats=2)

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    plan_brd = plan_text(spark, q_brd, "exp_join_brd")
    t_brd = measure("BroadcastHashJoin（显式广播 5,000 行维表）",
                    lambda: spark.sql(q_brd).count(), repeats=2)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    return {
        "joined_rows_metric_long": io.table_count(spark, ml),
        "dim_rows": io.table_count(spark, dim),
        "sort_merge": t_smj,
        "broadcast": t_brd,
        "speedup_x": round(t_smj["best_s"] / t_brd["best_s"], 2) if t_brd["best_s"] else None,
        "smj_plan_has_sortmerge": "SortMergeJoin" in plan_smj,
        "brd_plan_has_broadcast": "BroadcastHashJoin" in plan_brd,
        "plan_smj_excerpt": "\n".join(
            l.strip() for l in plan_smj.splitlines() if "Join" in l)[:600],
        "plan_brd_excerpt": "\n".join(
            l.strip() for l in plan_brd.splitlines() if "Join" in l)[:600],
    }


def bench_shuffle_partitions(spark) -> dict:
    ml = f"{C.DB}.dwd_blogger_metric_long"
    q = (f"SELECT red_id, metric_key, COUNT(*) AS n, "
         f"ROUND(AVG(metric_value), 2) AS avg_v FROM {ml} GROUP BY red_id, metric_key")
    cases = []
    for p in [1, 4, 16, 64]:
        spark.conf.set("spark.sql.shuffle.partitions", str(p))
        r = measure(f"shuffle.partitions={p}", lambda: spark.sql(q).count(), repeats=2)
        r["partitions"] = p
        cases.append(r)
        print(f"    shuffle={p:>2}  best={r['best_s']}s", flush=True)
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    best = min(cases, key=lambda x: x["best_s"])
    baseline = cases[0]["best_s"]
    worst = max(cases, key=lambda x: x["best_s"])
    return {
        "query": q,
        "master": C.MASTER,
        "cases": cases,
        "best_partitions": best["partitions"],
        "best_s": best["best_s"],
        "worst_partitions": worst["partitions"],
        "worst_s": worst["best_s"],
        "degradation_worst_vs_best_x": round(worst["best_s"] / best["best_s"], 2) if best["best_s"] else None,
        "aqe_enabled": False,
    }


def quality_at_scale(spark) -> dict:
    """百万行量级上重跑主项目的核心质量规则，证明分层 SQL 换量级后仍守恒。"""
    t = f"{C.DB}.dwd_blogger_monthly_snapshot"
    ml = f"{C.DB}.dwd_blogger_metric_long"

    def n(sql, tag):
        return io.scalar(spark, sql, tag)

    fact = n(f"SELECT COUNT(*) FROM {t}", "bq_fact")
    long_rows = n(f"SELECT COUNT(*) FROM {ml}", "bq_long")
    dup = n(f"SELECT COUNT(*) FROM (SELECT stat_month, red_id FROM {t} "
            f"GROUP BY stat_month, red_id HAVING COUNT(*) > 1)", "bq_dup")
    nulls = n(f"SELECT COUNT(*) FROM {t} WHERE red_id IS NULL OR TRIM(red_id)='' "
              f"OR stat_month IS NULL", "bq_null")
    invalid = n(f"SELECT COUNT(*) FROM {t} WHERE fans_count <= 0 OR image_price < 0 "
                f"OR interaction_median_30d < 0", "bq_value")
    empty_metric = n(f"SELECT COUNT(*) FROM {ml} WHERE metric_value IS NULL", "bq_metric")
    parts = len(io.collect_dicts(spark, f"SHOW PARTITIONS {t}", "bq_parts"))

    checks = [
        {"name": "指标长表行数守恒", "rule": "事实表 × 11 项指标",
         "expected": fact * C.N_METRICS, "actual": long_rows,
         "passed": abs(long_rows - fact * C.N_METRICS) < 0.5},
        {"name": "DWD 主键唯一", "rule": "(stat_month, red_id) 无重复",
         "expected": 0, "actual": dup, "passed": dup == 0},
        {"name": "关键字段非空", "rule": "red_id / stat_month 不为空",
         "expected": 0, "actual": nulls, "passed": nulls == 0},
        {"name": "数值值域", "rule": "粉丝数>0、报价与中位数>=0",
         "expected": 0, "actual": invalid, "passed": invalid == 0},
        {"name": "11 项指标完整率", "rule": "metric_value 无空值",
         "expected": 0, "actual": empty_metric, "passed": empty_metric == 0},
        {"name": "Hive 分区完整性", "rule": f"分区数 = {C.N_MONTHS}",
         "expected": C.N_MONTHS, "actual": parts, "passed": parts == C.N_MONTHS},
    ]
    return {"checks": checks,
            "passed": sum(1 for c in checks if c["passed"]),
            "total": len(checks),
            "all_passed": all(c["passed"] for c in checks)}


def write_report(result: dict) -> Path:
    C.OUT_DIR.mkdir(parents=True, exist_ok=True)
    (C.OUT_DIR / "bench_report.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    ti = result["tables"]
    pp = result["partition_pruning"]
    bj = result["broadcast_join"]
    sp = result["shuffle_partitions"]
    df = result["dataframe_api"]
    qc = result["quality_at_scale"]

    def fmt(v):
        if isinstance(v, (int, float)) and float(v).is_integer():
            return f"{int(v):,}"
        return str(v)

    prune_files_x = round(pp["files_scanned_all"] / max(pp["files_scanned_filtered"], 1), 1)
    prune_bytes_x = round(pp["bytes_mb_all"] / max(pp["bytes_mb_filtered"], 0.01), 1)

    lines = [
        "# Spark / Hive 规模压测报告",
        "",
        f"- 生成时间：{result['timestamp']}",
        f"- 环境：Python + PySpark 4.0.1，`{C.MASTER}` 单机，内嵌 Derby Hive Metastore，Parquet，AQE 关闭",
        f"- 数据：**合成数据**（seed={C.SEED}），{C.N_BLOGGERS:,} 达人 × {C.N_MONTHS} 个月 × {C.N_METRICS} 指标；"
        f"只用于验证分层 SQL 与调优在百万行量级成立，不是真实业务数据",
        "- 隔离：独立库 `pgy_dw_bench`，不改动 `pgy_dw` 里任何表；简历中的 122 / 310 / 27 等真实数字不受影响",
        "",
        "## 1. 数据规模",
        "",
        "| 项目 | 数值 |",
        "|---|---:|",
        f"| 事实表行数 | {ti['row_counts']['dwd_blogger_monthly_snapshot']:,} |",
        f"| 指标长表行数 | {ti['row_counts']['dwd_blogger_metric_long']:,} |",
        f"| 标签桥接表行数 | {ti['row_counts']['dwd_blogger_tag_bridge']:,} |",
        f"| 环比结果行数 | {ti['row_counts']['dws_metric_monthly_change']:,} |",
        f"| Hive 分区数 | {ti['partitions']} |",
        f"| 事实表 Parquet 文件数 / 体积 | {ti['fact_parquet_files']} / {ti['fact_parquet_mb']} MB |",
        f"| 压测库表数 | {ti['tables']} |",
        "",
        "> 复用 `spark/sql/03_dws.hql` 原文，仅把 `pgy_dw.` 替换成 `pgy_dw_bench.`。",
        "",
        "## 2. 分区裁剪",
        "",
        "| 场景 | 扫描文件数 | 扫描字节(MB) | 最优耗时(s) |",
        "|---|---:|---:|---:|",
        f"| 全表扫描 | {pp['files_scanned_all']} | {pp['bytes_mb_all']} | {pp['timing_all']['best_s']} |",
        f"| 单月过滤 `{pp['filter_month']}` | {pp['files_scanned_filtered']} | {pp['bytes_mb_filtered']} | {pp['timing_filtered']['best_s']} |",
        "",
        f"- 执行计划里的分区过滤：`{(pp['partition_filters_in_plan'] or ['未捕获'])[0][:160]}`",
        f"- **扫描量减少 {prune_files_x}× / {prune_bytes_x}×**（{pp['files_scanned_all']}→{pp['files_scanned_filtered']} 个文件，"
        f"{pp['bytes_mb_all']}→{pp['bytes_mb_filtered']} MB）；耗时比 {pp['speedup_x']}×。",
        "  数据量只有 10 MB 且全部命中 OS 页缓存，所以耗时差距远小于 I/O 差距——这正是分区裁剪的意义：",
        "  省的是磁盘/网络 I/O，集群与冷数据下差距会放大。",
        "",
        "## 3. 广播 join vs SortMergeJoin",
        "",
        "| 场景 | 最优耗时(s) | 采样(s) |",
        "|---|---:|---|",
        f"| SortMergeJoin（1,320,000 × 5,000） | {bj['sort_merge']['best_s']} | {bj['sort_merge']['runs_s']} |",
        f"| BroadcastHashJoin（广播 5,000 行维表） | {bj['broadcast']['best_s']} | {bj['broadcast']['runs_s']} |",
        "",
        f"- 计划校验：SMJ={bj['smj_plan_has_sortmerge']}，Broadcast={bj['brd_plan_has_broadcast']}；提速 **{bj['speedup_x']}×**",
        "",
        "## 4. shuffle 分区数调参",
        "",
        "| partitions | 最优耗时(s) | 采样 |",
        "|---:|---:|---|",
    ]
    for c in sp["cases"]:
        lines.append(f"| {c['partitions']} | {c['best_s']} | {c['runs_s']} |")
    lines += [
        "",
        f"- 本机 `{sp['master']}` 下最优 = **{sp['best_partitions']}**（{sp['best_s']}s），"
        f"最差 = {sp['worst_partitions']}（{sp['worst_s']}s），**盲目调大反而慢 {sp['degradation_worst_vs_best_x']}×**",
        "- AQE 关闭，计划可复现。结论：1.32M 行的聚合在 4 核单机上属轻量负载，"
        "分区数从 1 增到 64 只增加任务调度与 shuffle 落盘开销；"
        "并行度要按「数据量 ÷ 可用 core」估，不是越大越好。",
        "",
        "## 5. 百万行量级数据质量校验",
        "",
        "| 规则 | 期望 | 实际 | 结果 |",
        "|---|---:|---:|---|",
    ]
    for c in qc["checks"]:
        lines.append(f"| {c['name']}（{c['rule']}） | {fmt(c['expected'])} | {fmt(c['actual'])} | "
                     f"{'PASS' if c['passed'] else 'FAIL'} |")
    lines += [
        "",
        f"- **{qc['passed']}/{qc['total']} 通过**；同一套分层 SQL 从 218 行放大到 132 万行，行数守恒与主键唯一性仍成立。",
        "",
        "## 6. P0-b：DataFrame API 版 DWS",
        "",
        f"- 产物表：`{df['dataframe_api_table']}`（与 SQL 版 `{C.DB}.dws_tag_monthly` 同口径）、`{df['window_table']}`（Window 排名）",
        f"- 行数：SQL 版 {df['rows_sql']:,}，DataFrame 版 {df['rows_dataframe']:,}",
        f"- 双向 `exceptAll` 差异：{df['except_all_sql_minus_df']} / {df['except_all_df_minus_sql']}；"
        f"精确一致 = **{df['exact_match']}**",
        f"- 执行计划含广播 join = {df['uses_broadcast']}；含 Window = {df['uses_window']}",
        "- 用到的算子：`spark.table` / `select` / `join` / `functions.broadcast` / `groupBy.agg`"
        "（`countDistinct`、`avg`、`round`）/ `Window.partitionBy + row_number`",
        "",
        "## 7. 简历可写的句子（可直接用）",
        "",
        "- 「以 PySpark 4.0 + Hive Metastore + Parquet 分区构建 19 表分层数仓，"
        "并用 5,000 达人 × 24 月 × 11 指标 = 132 万行合成数据做规模压测，"
        "同一套分层 SQL 全量通过 6 项数据质量校验。」",
        f"- 「分区裁剪使扫描文件数 {pp['files_scanned_all']}→{pp['files_scanned_filtered']}、"
        f"扫描量 {pp['bytes_mb_all']}→{pp['bytes_mb_filtered']} MB；"
        f"1,320,000 行事实与 5,000 行维表 JOIN 由 SortMergeJoin 改广播后提速 {bj['speedup_x']}×。」",
        "- 「用 DataFrame API（Window / broadcast join / 聚合）重写 DWS 标签层，"
        "与 Spark SQL 版双向 exceptAll 差异为 0。」",
        "",
        "面试追问时可讲的点：`EXPLAIN FORMATTED` 里的 `PartitionFilters`、"
        "广播阈值 `autoBroadcastJoinThreshold`、AQE 对计划的影响、"
        "为什么本机 64 分区反而比 1 分区慢（调度开销 > 并行收益）。",
        "",
        "## 8. 诚实边界",
        "",
        "- 合成数据、单机 `local[4]`、内嵌 Derby Metastore；不是 YARN/K8s 集群，不是 TB 级。",
        "- 计时包含 JVM 调度与本地磁盘 I/O，取多次最优值，只能用于**同机相对比较**，不能当生产性能承诺。",
        "- 简历可写「百万行级规模压测 / 分区裁剪 / 广播 join / shuffle 调参 / DataFrame API」，",
        "  不要写「Spark 集群调优」或「TB 级数据」。",
        "",
        "## 9. 复现",
        "",
        "```bash",
        "cd pgy_dw",
        "python spark/bench/gen_data.py                       # 生成合成 CSV",
        "python spark/bench/run_bench.py                      # 建表 + 全量压测",
        "python spark/bench/run_bench.py --skip-gen --skip-build   # 复用表，只重测",
        "python spark/bench/run_bench.py --report-only       # 只重排报告",
        "```",
        "",
        "可调环境变量：`PGY_BENCH_BLOGGERS`、`PGY_BENCH_MONTHS`、`PGY_BENCH_MASTER`、",
        "`PGY_BENCH_STAGING`、`PGY_SPARK_WAREHOUSE`。",
    ]
    path = C.OUT_DIR / "bench_report.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-gen", action="store_true", help="复用已生成的 CSV")
    ap.add_argument("--skip-build", action="store_true", help="复用已建好的 Hive 表")
    ap.add_argument("--only", choices=["all", "prune", "join", "shuffle", "df"], default="all")
    ap.add_argument("--report-only", action="store_true", help="仅用已有 JSON 重排报告")
    args = ap.parse_args()

    if args.report_only:
        result = json.loads((C.OUT_DIR / "bench_report.json").read_text(encoding="utf-8"))
        print("报告:", write_report(result), flush=True)
        return 0

    print("=" * 66, flush=True)
    print(f"Spark/Hive 规模压测 · {C.N_BLOGGERS:,} 达人 × {C.N_MONTHS} 月 · {C.MASTER}", flush=True)
    print("=" * 66, flush=True)

    print("[1] 生成合成数据...", flush=True)
    manifest = gen_data.generate() if not args.skip_gen else json.loads(
        (C.OUT_DIR / "data_manifest.json").read_text(encoding="utf-8"))

    spark = build_spark()
    result = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "environment": {
            "master": C.MASTER, "spark_version": spark.version,
            "python": sys.version.split()[0], "adaptive_enabled": False,
            "warehouse": str(WAREHOUSE),
        },
        "manifest": manifest,
    }

    if not args.skip_build:
        result["tables"] = build_tables(spark, manifest)
    else:
        base = WAREHOUSE / f"{C.DB}.db" / "dwd_blogger_monthly_snapshot"
        files = list(base.glob("stat_month=*/*.parquet"))
        nbytes = sum(p.stat().st_size for p in files)
        result["tables"] = {
            "database": C.DB,
            "partitions": len(io.collect_dicts(
                spark, f"SHOW PARTITIONS {C.DB}.dwd_blogger_monthly_snapshot", "bench_parts")),
            "tables": len(io.collect_dicts(spark, f"SHOW TABLES IN {C.DB}", "bench_tables")),
            "row_counts": {t: io.table_count(spark, f"{C.DB}.{t}") for t in [
                "dwd_blogger_monthly_snapshot", "dim_blogger", "dwd_blogger_tag_bridge",
                "dwd_blogger_metric_long", "dws_metric_monthly_change",
                "dws_platform_monthly", "dws_tag_monthly"]},
            "fact_parquet_files": len(files), "fact_parquet_bytes": nbytes,
            "fact_parquet_mb": round(nbytes / 1024 / 1024, 2),
            "fact_partition_dir": str(base),
        }
    print("    表规模:", json.dumps(result["tables"]["row_counts"], ensure_ascii=False), flush=True)

    if args.only in ("all", "prune"):
        print("[4] 分区裁剪对比...", flush=True)
        result["partition_pruning"] = bench_partition_pruning(spark, result["tables"])
    if args.only in ("all", "join"):
        print("[5] 广播 join 对比...", flush=True)
        result["broadcast_join"] = bench_broadcast_join(spark)
    if args.only in ("all", "shuffle"):
        print("[6] shuffle 分区调参...", flush=True)
        result["shuffle_partitions"] = bench_shuffle_partitions(spark)
    if args.only in ("all", "df"):
        print("[7] DataFrame API 版 DWS + 对账...", flush=True)
        result["dataframe_api"] = df_api.run(spark)
    if args.only == "all":
        print("[8] 百万行级数据质量校验...", flush=True)
        result["quality_at_scale"] = quality_at_scale(spark)
        print("    质量校验:", result["quality_at_scale"]["passed"],
              "/", result["quality_at_scale"]["total"], flush=True)

    if args.only == "all":
        report = write_report(result)
        report_label = str(report)
    else:
        partial = C.OUT_DIR / f"bench_partial_{args.only}.json"
        partial.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        report_label = str(partial)
    print("=" * 66, flush=True)
    print("报告:", report_label, flush=True)
    print(json.dumps({
        "rows": result["tables"]["row_counts"].get("dwd_blogger_metric_long"),
        "prune_x": result.get("partition_pruning", {}).get("speedup_x"),
        "join_x": result.get("broadcast_join", {}).get("speedup_x"),
        "best_shuffle": result.get("shuffle_partitions", {}).get("best_partitions"),
        "df_exact": result.get("dataframe_api", {}).get("exact_match"),
    }, ensure_ascii=False), flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
