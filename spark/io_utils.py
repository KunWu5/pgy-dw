"""Spark 侧 IO 工具：Excel 读取、DataFrame 构造、CSV 落盘、运行日志。"""
from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook

import config as cfg
from spark.session import OUT_DIR, WAREHOUSE

# Windows + Python 3.13 环境下 PySpark 的 Python worker 不稳定；
# driver 与 JVM 之间改用 CSV 文件中转（纯 JVM 侧的 spark.read.csv）。
STAGING = WAREHOUSE.parent / "pgy_spark_staging"


def write_staging_csv(rows: list[list], columns: list[str], name: str) -> Path:
    STAGING.mkdir(parents=True, exist_ok=True)
    path = STAGING / name
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])
    return path


def csv_to_view(spark, path: Path, view_name: str) -> None:
    (spark.read.option("header", "true").option("inferSchema", "false")
     .csv(str(path)).createOrReplaceTempView(view_name))


def _text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value).strip()


def excel_rows(path: Path, column_map: dict[str, str], months: tuple[str, ...]) -> list[list]:
    ws = load_workbook(path, read_only=True, data_only=True).active
    raw = list(ws.iter_rows(values_only=True))
    header = [_text(h) or "" for h in raw[0]]
    idx = {column_map[h]: i for i, h in enumerate(header) if h in column_map}
    has_month = "stat_month" in idx
    has_red_id = "red_id" in idx
    out = []
    for r in raw[1:]:
        if not r:
            continue
        if has_red_id and (idx["red_id"] >= len(r) or not _text(r[idx["red_id"]])):
            continue
        month = _text(r[idx["stat_month"]]) if has_month and idx["stat_month"] < len(r) else None
        if months and has_month and month and month not in months:
            continue
        out.append([_text(r[idx[c]]) if c in idx and idx[c] < len(r) else None
                    for c in column_map.values()])
    return out


def backup_insert_rows(sql_text: str, table: str) -> list[list[str]]:
    """复用 SQLite 版 ODS 的备份解析器，保证两个版本接入逻辑一致。"""
    from warehouse.extract import _clean, _split_sql_values

    rows = []
    for m in re.finditer(r"INSERT INTO `%s` VALUES (.*?);" % table, sql_text, re.S):
        for row in _split_sql_values(m.group(1)):
            rows.append([_clean(x) for x in row])
    return rows


def to_df(spark, rows: list[list], columns: list[str]):
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame([tuple(r) for r in rows], schema)


def _read_csv_rows(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def sql_to_csv(spark, sql: str, tag: str) -> Path:
    """SQL 结果由 JVM 侧直接写 CSV，再在 driver 端读文件，绕开 Python worker。"""
    tmp = STAGING / ("_q_" + tag)
    spark.sql(sql).coalesce(1).write.mode("overwrite") \
        .option("header", "true").csv(str(tmp))
    parts = sorted(tmp.glob("part-*.csv"))
    if not parts:
        raise RuntimeError(f"Spark 未产出结果文件: {tmp}")
    return parts[0]


def scalar(spark, sql: str, tag: str) -> float:
    rows = _read_csv_rows(sql_to_csv(spark, sql, tag))
    return float(rows[1][0]) if len(rows) > 1 and rows[1][0] not in ("", "NULL") else 0.0


def collect_dicts(spark, sql: str, tag: str) -> list[dict]:
    path = sql_to_csv(spark, sql, tag)
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(df, name: str) -> int:
    """导出单文件 CSV（供对账与查看），同样不经 Python worker。"""
    tmp = STAGING / ("_export_" + name.replace(".csv", ""))
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(tmp))
    part = sorted(tmp.glob("part-*.csv"))[0]
    rows = _read_csv_rows(part)
    path = OUT_DIR / name
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        csv.writer(f).writerows(rows)
    return max(len(rows) - 1, 0)


def log(stage: str, payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rec = {"stage": stage, "ts": datetime.now().isoformat(timespec="seconds"), **payload}
    with (OUT_DIR / "run_log.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print("      " + json.dumps(payload, ensure_ascii=False))


def table_count(spark, table: str) -> int:
    tag = "cnt_" + table.replace(".", "_")
    return int(scalar(spark, f"SELECT COUNT(*) AS n FROM {table}", tag))
