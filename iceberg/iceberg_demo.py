"""Apache Iceberg 湖表实践：ACID 更新、快照时间旅行、Schema / 分区演进、元数据表。

对应 JD：图三要求 4「Paimon / Hudi / Iceberg」、图二要求 7「元数据」、图一职责 3「可扩展数仓体系」

与项目既有 Hive/Parquet 表的分工：
  - Hive 外表（现有 19 张）：分层结果、按月分区、INSERT OVERWRITE 幂等重跑；
  - Iceberg 表（本模块）：需要行级更新、历史版本回溯、Schema 平滑演进的分析层。
两条链路都落在真实 122 行 ADS 数据上，互不改写。
"""
from __future__ import annotations

import csv
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
for _lib in (r"C:\Temp\wk\sparklib", r"C:\Temp\wk\pylib"):
    if Path(_lib).is_dir() and _lib not in sys.path:
        sys.path.insert(0, _lib)

from spark.session import prepare_env  # noqa: E402

prepare_env()

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, LongType, StringType, StructField, StructType,
)

from spark import io_utils as io  # noqa: E402

JAR = Path(r"C:\Temp\wk\iceberglib\iceberg-spark-runtime-4.0_2.13-1.12.0.jar")
WAREHOUSE = Path(r"C:\Temp\pgy_iceberg_warehouse")
CMP_DIR = Path(r"C:\Temp\pgy_hive_compare")
OUT = ROOT / "output" / "iceberg"
TABLE = "lake.pgy.ads_blogger_value"
HIVE_DB = "pgy_dw_lake"

SCHEMA = StructType([
    StructField("stat_month", StringType(), True),
    StructField("red_id", StringType(), True),
    StructField("nickname", StringType(), True),
    StructField("tags_norm", StringType(), True),
    StructField("fans_count", LongType(), True),
    StructField("image_price", DoubleType(), True),
    StructField("cpe_30d", DoubleType(), True),
    StructField("engagement_rate_30d", DoubleType(), True),
    StructField("blogger_tier", StringType(), True),
    StructField("cpe_tier", StringType(), True),
])


def build_spark() -> SparkSession:
    # 注意：--jars 必须传 file:/// URI。直接写 C:/... 会被 Hadoop 解析成 scheme "C"，
    # 报 No FileSystem for scheme "C"（Windows 特有的坑）。
    jar_uri = "file:///" + JAR.as_posix()
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS",
                          f"--jars {jar_uri} pyspark-shell")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("pgy_dw_iceberg")
        .config("spark.jars", jar_uri)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", WAREHOUSE.as_uri())
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.hadoop.native.lib", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def timed(fn):
    t0 = time.perf_counter()
    out = fn()
    return round(time.perf_counter() - t0, 2), out


def build() -> dict:
    if not JAR.exists():
        raise FileNotFoundError(f"缺少 Iceberg runtime jar: {JAR}")
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE, ignore_errors=True)
    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.pgy")

    ads = (spark.read.schema(SCHEMA).option("header", "true")
           .csv((ROOT / "output" / "ads_blogger_value_wide.csv").as_posix()))
    ads.createOrReplaceTempView("v_ads")

    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(f"""
        CREATE TABLE {TABLE} (
          stat_month STRING, red_id STRING, nickname STRING, tags_norm STRING,
          fans_count BIGINT, image_price DOUBLE, cpe_30d DOUBLE,
          engagement_rate_30d DOUBLE, blogger_tier STRING, cpe_tier STRING
        ) USING iceberg PARTITIONED BY (stat_month, bucket(8, red_id))
    """)
    t_insert, _ = timed(lambda: spark.sql(f"INSERT INTO {TABLE} SELECT * FROM v_ads"))
    rows0 = io.table_count(spark, TABLE)

    # 1) 行级更新：MERGE INTO（Iceberg 按「文件」粒度重写，只动命中的文件）
    t_merge, _ = timed(lambda: spark.sql("""
        MERGE INTO lake.pgy.ads_blogger_value t
        USING (SELECT red_id, stat_month, '人工复核' AS cpe_tier
               FROM lake.pgy.ads_blogger_value ORDER BY red_id LIMIT 3) s
        ON t.red_id = s.red_id AND t.stat_month = s.stat_month
        WHEN MATCHED THEN UPDATE SET t.cpe_tier = s.cpe_tier
    """))

    snapshots = io.collect_dicts(spark, f"""
        SELECT snapshot_id, committed_at, operation,
               summary['added-records'] AS added_records,
               summary['deleted-records'] AS deleted_records,
               summary['added-data-files'] AS added_files,
               summary['deleted-data-files'] AS deleted_files
        FROM {TABLE}.snapshots ORDER BY committed_at
    """, "ice_snaps")

    # 2) 时间旅行：回到第一个快照核对行数与粉丝总数
    first_snap = snapshots[0]["snapshot_id"]
    travel = io.collect_dicts(spark, f"""
        SELECT 'v1(建表时)' AS version, COUNT(*) AS n,
               CAST(SUM(fans_count) AS BIGINT) AS total_fans,
               SUM(CASE WHEN cpe_tier='人工复核' THEN 1 ELSE 0 END) AS reviewed
        FROM {TABLE} VERSION AS OF {first_snap}
        UNION ALL
        SELECT 'v_now(当前)', COUNT(*), CAST(SUM(fans_count) AS BIGINT),
               SUM(CASE WHEN cpe_tier='人工复核' THEN 1 ELSE 0 END)
        FROM {TABLE}
    """, "ice_travel")

    # 3) Schema 演进：加列不重写历史数据
    schema_evo = {"added": False, "null_after_add": None}
    try:
        spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN data_quality_tag STRING")
        schema_evo["added"] = True
        schema_evo["null_after_add"] = io.scalar(
            spark, f"SELECT COUNT(*) FROM {TABLE} WHERE data_quality_tag IS NULL", "ice_evo")
    except Exception as exc:
        schema_evo["error"] = str(exc)[:200]

    # 4) 分区演进：追加隐藏分区字段（truncate red_id），旧数据不动
    partition_evo = {"added": False}
    try:
        spark.sql(f"ALTER TABLE {TABLE} ADD PARTITION FIELD truncate(4, red_id)")
        spec_cnt = int(io.scalar(spark, f"SELECT COUNT(*) FROM {TABLE}.partitions", "ice_parts"))
        partition_evo = {"added": True, "spec_rows": spec_cnt}
    except Exception as exc:
        partition_evo["error"] = str(exc)[:200]

    # 5) 元数据表：文件级明细
    files = io.collect_dicts(spark, f"""
        SELECT file_path, record_count, file_size_in_bytes,
               CAST(partition AS STRING) AS partition
        FROM {TABLE}.files ORDER BY record_count DESC
    """, "ice_files")

    # 6) 与 Hive/Parquet 对照：同样更新 3 行，Hive 需要整分区重写
    #    用固定 Temp 目录 + external table，避免 managed table 残留导致 LOCATION_ALREADY_EXISTS
    shutil.rmtree(CMP_DIR, ignore_errors=True)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.ads_value_parquet")
    spark.sql(f"""
        CREATE TABLE {HIVE_DB}.ads_value_parquet (
          red_id STRING, nickname STRING, tags_norm STRING,
          fans_count BIGINT, image_price DOUBLE, cpe_30d DOUBLE,
          engagement_rate_30d DOUBLE, blogger_tier STRING, cpe_tier STRING
        ) USING parquet PARTITIONED BY (stat_month STRING)
        LOCATION '{CMP_DIR.as_uri()}'
    """)
    spark.sql(f"""
        INSERT INTO {HIVE_DB}.ads_value_parquet PARTITION (stat_month)
        SELECT red_id, nickname, tags_norm, fans_count, image_price, cpe_30d,
               engagement_rate_30d, blogger_tier, cpe_tier, stat_month
        FROM v_ads
    """)
    t_hive, _ = timed(lambda: spark.sql(f"""
        INSERT OVERWRITE TABLE {HIVE_DB}.ads_value_parquet PARTITION (stat_month)
        SELECT red_id, nickname, tags_norm, fans_count, image_price, cpe_30d,
               engagement_rate_30d, blogger_tier,
               CASE WHEN red_id IN (SELECT red_id FROM v_ads ORDER BY red_id LIMIT 3)
                    THEN '人工复核' ELSE cpe_tier END AS cpe_tier,
               stat_month
        FROM v_ads
    """))
    hive_rows = io.table_count(spark, f"{HIVE_DB}.ads_value_parquet")

    # 导出（map / struct 列先转 string，CSV datasource 不支持嵌套类型）
    for name, sql in [
        ("snapshots.csv",
         f"SELECT snapshot_id, committed_at, operation, CAST(summary AS STRING) AS summary "
         f"FROM {TABLE}.snapshots ORDER BY committed_at"),
        ("time_travel.csv", f"""
            SELECT 'v1' AS version, COUNT(*) AS n FROM {TABLE} VERSION AS OF {first_snap}
            UNION ALL SELECT 'now', COUNT(*) FROM {TABLE}"""),
        ("files.csv",
         f"SELECT file_path, record_count, file_size_in_bytes, "
         f"CAST(partition AS STRING) AS partition FROM {TABLE}.files"),
    ]:
        rows = io.collect_dicts(spark, sql, "exp_" + name.replace(".", "_"))
        if rows:
            with (OUT / name).open("w", encoding="utf-8-sig", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                w.writeheader()
                w.writerows(rows)

    merge_snap = snapshots[-1]
    result = {
        "table": TABLE,
        "rows": rows0,
        "insert_seconds": t_insert,
        "merge_seconds": t_merge,
        "snapshots": len(snapshots),
        "snapshot_detail": snapshots,
        "merge_snapshot": merge_snap,
        "time_travel": travel,
        "schema_evolution": schema_evo,
        "partition_evolution": partition_evo,
        "data_files": len(files),
        "hive_overwrite_seconds": t_hive,
        "hive_rows_written": hive_rows,
        "iceberg_jar": JAR.name,
    }

    merge_added = merge_snap.get("added_records") if merge_snap else None
    md = [
        "# Apache Iceberg 湖表实践报告",
        "",
        f"- 运行环境：Spark 4.0.1 + `{JAR.name}`，Hadoop catalog，仓库 `{WAREHOUSE}`",
        f"- 表：`{TABLE}`，{rows0} 行，按 `stat_month` + `bucket(8, red_id)` 隐藏分区",
        "- 同一份数据另建一张 Hive/Parquet 表做对照，两者互不改写",
        "",
        "## 1. 快照（ACID 版本）",
        "",
        "| 快照 | 操作 | 新增记录 | 删除记录 | 新增文件 | 删除文件 |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for s in snapshots:
        md.append(f"| {s['snapshot_id']} | {s['operation']} | {s['added_records']} | "
                  f"{s['deleted_records']} | {s['added_files']} | {s['deleted_files']} |")
    md += [
        "",
        "## 2. 行级更新：MERGE 按文件粒度重写",
        "",
        f"用 `MERGE INTO` 把 3 行的 `cpe_tier` 改成「人工复核」：本次提交 "
        f"added-records={merge_added}、deleted-records={merge_snap.get('deleted_records')}，"
        f"耗时 {t_merge}s。",
        "",
        f"建表时按 `bucket(8, red_id)` 分桶，落成 **{len(files)}** 个数据文件，"
        f"MERGE 只重写了命中的文件（{merge_added}/{rows0} 行），不是整表。",
        "",
        "**必须讲清的细节**：Iceberg 的 MERGE 是**文件级重写**，不是行级原地更新。"
        "如果整张表只有一个数据文件，改 1 行也会重写整个文件——"
        "这正是生产上要做 `rewrite_data_files` 合并小文件、并合理设置分区/分桶的原因。",
        "",
        "对照：同样更新 3 行，Hive/Parquet 表用 `INSERT OVERWRITE ... PARTITION` 必须"
        f"重写分区内全部 **{hive_rows} 行**、耗时 {t_hive}s。",
        "",
        "> 122 行数据下两个耗时都在亚秒级，不构成性能结论；"
        "真正的差异是**写入语义**：Hive 整分区重写，Iceberg 是文件级增量 + 快照。",
        "",
        "## 3. 时间旅行",
        "",
        "| 版本 | 行数 | 粉丝总数 | 已复核行数 |",
        "|---|---:|---:|---:|",
    ]
    for t in travel:
        md.append(f"| {t['version']} | {t['n']} | {t['total_fans']} | {t['reviewed']} |")
    md += [
        "",
        f"- 用 `VERSION AS OF {first_snap}` 查到更新前的数据，说明历史快照可回溯；",
        "- 生产上可做误操作回滚、审计比对、按时间点复现报表。",
        "",
        "## 4. Schema 演进",
        "",
        f"- `ALTER TABLE ADD COLUMN data_quality_tag STRING`：{'成功' if schema_evo.get('added') else '失败'}",
        f"- 加列后历史数据该列为 NULL 的行数：{int(schema_evo.get('null_after_add') or 0)}"
        f"（等于 {rows0} 说明没有重写历史文件）",
        "",
        "## 5. 分区演进（隐藏分区）",
        "",
        f"- 追加 `truncate(4, red_id)` 分区字段：{'成功' if partition_evo.get('added') else '失败'}",
        "- Iceberg 的分区是「隐藏分区」，查询不需要写分区列，由引擎自动裁剪；"
        "新增分区字段不会移动旧数据，只会影响后续写入。",
        "",
        "## 6. 元数据表",
        "",
        f"- `{TABLE}.snapshots` / `.files` / `.history` / `.manifests` 可直接用 SQL 查询；",
        f"- 当前数据文件 **{len(files)}** 个，可在 `files.csv` 看到每个文件的行数与大小。",
        "",
        "## 7. 与 Hive/Parquet 的取舍",
        "",
        "| 维度 | Hive + Parquet（现有 19 张表） | Iceberg（本模块） |",
        "|---|---|---|",
        "| 写入语义 | 分区覆盖，整分区重写 | 文件级增量 + 快照 |",
        "| 行级更新 | 需重写分区 | `MERGE INTO` |",
        "| 历史版本 | 无（靠自己留快照表） | 原生快照 + 时间旅行 |",
        "| Schema 演进 | 加列需重写或改元数据 | `ADD COLUMN` 平滑加列 |",
        "| 小文件问题 | 需自己合并 | `rewrite_data_files` 可治理 |",
        "| 引擎依赖 | Hive Metastore | 需 Iceberg runtime |",
        "",
        "结论：**分层结果仍用 Hive/Parquet**（简单、生态广）；"
        "**需要更新与回溯的分析层用 Iceberg**。两者不是替代关系。",
        "",
        "## 8. 诚实边界",
        "",
        "- 单机 Hadoop catalog + 本地文件系统，**不是**对象存储 / Hive Metastore / REST catalog 的生产部署；",
        "- 未做并发写冲突、快照过期清理（`expire_snapshots`）、小文件合并（`rewrite_data_files`）；",
        "- 数据量 122 行，性能数字只用于说明写入语义差异，不能当基准测试。",
    ]
    (OUT / "iceberg_report.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    (OUT / "iceberg_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    spark.stop()
    return result


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
