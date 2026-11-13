"""P0-b：用 PySpark DataFrame API 重写 dws_tag_monthly 一层。

对照物是 spark/sql/03_dws.hql 里的最后一段 SQL。这里刻意不用任何 SQL 字符串，
全部用 DataFrame 算子表达，并把执行计划导出，证明「不是只会拼 SQL」。

用到的能力：
  - spark.table / select / join(on=[...])          读 Hive Metastore 表
  - functions.broadcast                           显式广播小表（消除大表 shuffle）
  - groupBy + agg（countDistinct / avg / round）   聚合
  - Window.partitionBy + row_number               窗口函数（衍生一张标签排名表）
  - queryExecution().executedPlan()                物理执行计划

Windows + Python 3.13 的 PySpark Python worker 不稳定，所以全程只做 JVM 侧惰性算子，
禁止 collect / toPandas / UDF；落表与计数都走 JVM。
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from spark.bench import config_bench as C

SQL_TABLE = f"{C.DB}.dws_tag_monthly"
DF_TABLE = f"{C.DB}.dws_tag_monthly_df"
RANK_TABLE = f"{C.DB}.dws_tag_rank_df"

COLUMNS = ["stat_month", "tag", "blogger_cnt",
           "avg_fans", "avg_image_price", "avg_cpe_30d"]


def build_tag_monthly(spark) -> DataFrame:
    """与 03_dws.hql 的 dws_tag_monthly 口径完全一致，只是换成 DataFrame API。"""
    bridge = spark.table(f"{C.DB}.dwd_blogger_tag_bridge")
    fact = spark.table(f"{C.DB}.dwd_blogger_monthly_snapshot").select(
        "stat_month", "red_id", "fans_count", "image_price", "cpe_30d"
    )
    # 事实表 12 万行 < 标签桥接表 ~24 万行，广播较小的一侧
    joined = bridge.join(F.broadcast(fact), on=["stat_month", "red_id"], how="inner")
    out = (
        joined.groupBy("stat_month", "tag")
        .agg(
            F.countDistinct("red_id").alias("blogger_cnt"),
            F.round(F.avg("fans_count"), 0).alias("avg_fans"),
            F.round(F.avg("image_price"), 2).alias("avg_image_price"),
            F.round(F.avg("cpe_30d"), 2).alias("avg_cpe_30d"),
        )
        .select(*COLUMNS)
    )
    return out


def build_tag_rank(base: DataFrame) -> DataFrame:
    """窗口函数：每月按覆盖博主数给标签排名。"""
    w = Window.partitionBy("stat_month").orderBy(F.desc("blogger_cnt"), F.asc("tag"))
    return base.withColumn("tag_rank", F.row_number().over(w))


def plan_text(df: DataFrame) -> str:
    """导出物理执行计划（JVM 侧字符串，不触发 Python worker）。"""
    try:
        return df._jdf.queryExecution().executedPlan().toString()
    except Exception:
        return df._jdf.queryExecution().simpleString()


def run(spark) -> dict:
    df = build_tag_monthly(spark)
    plan = plan_text(df)

    df.write.mode("overwrite").saveAsTable(DF_TABLE)
    rank = build_tag_rank(df)
    rank_plan = plan_text(rank)
    rank.write.mode("overwrite").saveAsTable(RANK_TABLE)

    # 对账：核心表逐行双向 exceptAll + 行数/聚合校验
    def normalized(table: str):
        cols = ", ".join(
            [f"CAST({c} AS STRING) AS {c}" for c in COLUMNS]
        )
        return spark.sql(f"SELECT {cols} FROM {table}")

    a, b = normalized(SQL_TABLE), normalized(DF_TABLE)
    diff_ab = a.exceptAll(b).count()
    diff_ba = b.exceptAll(a).count()

    counts = {}
    from spark import io_utils as io
    for name, table in (("sql", SQL_TABLE), ("dataframe", DF_TABLE)):
        counts[name] = io.collect_dicts(
            spark,
            f"SELECT CAST(COUNT(*) AS STRING) AS n, CAST(SUM(blogger_cnt) AS STRING) AS sum_cnt, "
            f"CAST(ROUND(SUM(avg_fans), 2) AS STRING) AS sum_fans, "
            f"CAST(ROUND(SUM(avg_image_price), 2) AS STRING) AS sum_price FROM {table}",
            f"dfchk_{name}",
        )[0]

    result = {
        "dataframe_api_table": DF_TABLE,
        "window_table": RANK_TABLE,
        "rows_sql": int(counts["sql"]["n"]),
        "rows_dataframe": int(counts["dataframe"]["n"]),
        "except_all_sql_minus_df": diff_ab,
        "except_all_df_minus_sql": diff_ba,
        "checksums": counts,
        "exact_match": diff_ab == 0 and diff_ba == 0 and counts["sql"] == counts["dataframe"],
        "uses_broadcast": "BroadcastHashJoin" in plan or "broadcast" in plan.lower(),
        "uses_window": "Window" in rank_plan or "window" in rank_plan.lower(),
        "plan_tag_monthly": plan,
        "plan_tag_rank": rank_plan,
    }
    return result
