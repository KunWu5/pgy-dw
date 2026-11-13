"""Spark Structured Streaming 实时管道：文件流 → 清洗 → 水位线窗口聚合 → 落 Parquet。

对应 JD：三份 JD 都要求的「实时数据开发经验」。
本机没有 Kafka/Flink（PyFlink 不支持 Windows），所以用 Spark 自带能力做**微批流处理**：
  - 文件流 source 模拟采集端持续落盘
  - watermark + 10 秒滚动窗口聚合
  - 有效流写窗口结果，脏数据单独写死信队列（实时质量分支）
  - 另起一条 rate → memory 流，演示「流表 + 增量查询」

全程只用 JVM 侧算子（无 Python UDF / foreachBatch / collect），
规避 Windows + Python 3.13 下 PySpark Python worker 崩溃的问题。
"""
from __future__ import annotations

import csv
import json
import shutil
import subprocess
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

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, LongType, StringType, StructField, StructType,
)

from spark import io_utils as io  # noqa: E402

WATCH = Path(r"C:\Temp\pgy_stream_in")
SINK = Path(r"C:\Temp\pgy_stream_sink")
CKPT = Path(r"C:\Temp\pgy_stream_ckpt")
EXPORT = Path(r"C:\Temp\pgy_stream_export")
OUT = ROOT / "output" / "streaming"

SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("red_id", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("fans_count", StringType(), True),
    StructField("image_price", StringType(), True),
    StructField("interaction_cnt", StringType(), True),
])


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("pgy_dw_streaming")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", "file:///C:/Temp/pgy_stream_warehouse/")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.hadoop.native.lib", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def export(df, name: str) -> int:
    tmp = EXPORT / name.replace(".csv", "")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(tmp))
    part = sorted(tmp.glob("part-*.csv"))[0]
    rows = list(csv.reader(part.open(encoding="utf-8", newline="")))
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / name).open("w", encoding="utf-8-sig", newline="") as f:
        csv.writer(f).writerows(rows)
    return max(len(rows) - 1, 0)


def run() -> dict:
    for d in (SINK, CKPT, EXPORT):
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)
    WATCH.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    raw = (spark.readStream.schema(SCHEMA).option("header", True)
           .option("maxFilesPerTrigger", 1).csv(str(WATCH)))

    parsed = (raw
              .withColumn("event_ts", F.to_timestamp("event_time"))
              .withColumn("tag", F.trim(F.col("tag")))
              .withColumn("fans", F.col("fans_count").cast("long"))
              .withColumn("price", F.col("image_price").cast("double"))
              .withColumn("interact", F.col("interaction_cnt").cast("long")))

    bad = parsed.filter(
        F.col("event_ts").isNull() | F.col("fans").isNull() | (F.col("fans") < 0)
        | F.col("tag").isNull() | (F.col("tag") == ""))
    good = parsed.filter(
        F.col("event_ts").isNotNull() & F.col("fans").isNotNull() & (F.col("fans") >= 0)
        & F.col("tag").isNotNull() & (F.col("tag") != ""))

    windowed = (good
                .withWatermark("event_ts", "1 minute")
                .groupBy(F.window("event_ts", "10 seconds"), "tag")
                .agg(F.count("*").alias("event_cnt"),
                     F.round(F.avg("fans"), 0).alias("avg_fans"),
                     F.round(F.avg("price"), 2).alias("avg_price")))

    q_win = (windowed.writeStream.format("parquet")
             .option("path", str(SINK / "tag_window"))
             .option("checkpointLocation", str(CKPT / "tag_window"))
             .trigger(processingTime="2 seconds").start())

    q_dead = (bad.select("event_time", "red_id", "tag", "fans_count")
              .writeStream.format("parquet")
              .option("path", str(SINK / "dead_letter"))
              .option("checkpointLocation", str(CKPT / "dead_letter"))
              .trigger(processingTime="2 seconds").start())

    rate = spark.readStream.format("rate").option("rowsPerSecond", 50).load()
    q_rate = (rate.writeStream.format("memory").queryName("rate_events")
              .trigger(processingTime="2 seconds").start())

    print("[stream] 3 条查询已启动，开始写入增量文件 ...", flush=True)
    subprocess.run([sys.executable, str(ROOT / "streaming" / "producer.py")],
                   check=False, cwd=str(ROOT))
    time.sleep(18)
    # memory sink 的表在 query 停止后会被清理，先查再停
    rate_cnt = int(io.scalar(spark, "SELECT COUNT(*) AS n FROM rate_events", "rate_cnt"))
    for q in (q_win, q_dead, q_rate):
        if q.isActive:
            q.stop()

    win_df = spark.read.parquet(str(SINK / "tag_window"))
    dead_df = spark.read.parquet(str(SINK / "dead_letter"))
    win_cnt = win_df.count()
    dead_cnt = dead_df.count()

    exported = {
        "stream_tag_window.csv": export(
            win_df.select(F.col("window.start").alias("win_start"),
                          F.col("window.end").alias("win_end"),
                          "tag", "event_cnt", "avg_fans", "avg_price"),
            "stream_tag_window.csv"),
        "stream_dead_letter.csv": export(dead_df, "stream_dead_letter.csv"),
    }
    result = {
        "mode": "Spark Structured Streaming（微批，2s 触发）",
        "source": "文件流 " + str(WATCH),
        "windows": "10 秒滚动窗口 + 1 分钟水位线",
        "window_rows": win_cnt,
        "dead_letter_rows": dead_cnt,
        "rate_stream_rows": rate_cnt,
        "exported": exported,
        "checkpoint": str(CKPT),
        "sink": str(SINK),
    }

    md = [
        "# Spark Structured Streaming 实时管道报告",
        "",
        f"- 运行模式：{result['mode']}",
        f"- Source：{result['source']}（producer 分 5 批写入 1,000 行，模拟采集端持续落盘）",
        f"- 处理：清洗 → 1 分钟水位线 → **{result['windows']}** 按标签聚合",
        f"- Sink：Parquet（窗口结果 {win_cnt} 行）+ 死信队列（{dead_cnt} 行）+ memory 流表（{rate_cnt} 行）",
        "- Checkpoint：" + str(CKPT) + "（保证失败重跑不重复消费）",
        "",
        "## 分两路的原因",
        "",
        "- **有效流**：粉丝数非法、标签为空、时间解析失败的记录直接过滤掉，不进入聚合；",
        "- **死信流**：脏数据单独落 `dead_letter`，对应「实时数据质量监控」——线上一般是告警 + 落表待修，不是直接丢弃。",
        "",
        "## 与批处理的差异（可讲的部分）",
        "",
        "1. 窗口 + 水位线解决乱序与迟到数据；批处理靠分区快照天然有序。",
        "2. Checkpoint 记录 offset 与状态，等价于批处理的「幂等重跑」，但粒度是微批。",
        "3. 内存流表（`rate_events`）可以直接用 SQL 查询增量结果，是流批一体的典型用法。",
        "",
        "## 诚实边界",
        "",
        "- 这是 **Spark 微批**，不是 Flink 的原生流；没有事件时间语义之外的低延迟保证，也没有 Kafka 做缓冲。",
        "- 未做状态后端调优、背压、Exactly-Once 端到端验证（Parquet sink 本身是幂等的，但没有两阶段提交）。",
        "- 文件流 source 用轮询实现，仅用于本机验证；生产一般用 Kafka。",
    ]
    (OUT / "stream_report.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    (OUT / "stream_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    spark.stop()
    return result


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
