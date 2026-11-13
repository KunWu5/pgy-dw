"""规模压测参数。与原项目 pgy_dw 完全隔离：独立 DB、独立 staging、独立输出目录。

设计原则：
- 数据是**合成的**，只用于验证分层 SQL / 调优在百万行量级仍然成立；
- 不改动 pgy_dw 库里任何一张表，简历里的 122 / 310 / 27 等真实数字不受影响。
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPARK_DIR = ROOT / "spark"
BENCH_DIR = SPARK_DIR / "bench"
OUT_DIR = BENCH_DIR / "output"

# 纯英文路径：Windows + PySpark 下含中文的 warehouse 路径会被 URI 编码
STAGING = Path(os.environ.get("PGY_BENCH_STAGING", r"C:\Temp\pgy_bench_staging"))
WAREHOUSE = Path(os.environ.get("PGY_SPARK_WAREHOUSE", r"C:\Temp\pgy_spark_warehouse"))

DB = "pgy_dw_bench"
SOURCE_DB = "pgy_dw"

N_BLOGGERS = int(os.environ.get("PGY_BENCH_BLOGGERS", "5000"))
N_MONTHS = int(os.environ.get("PGY_BENCH_MONTHS", "24"))
N_METRICS = 11
SEED = 20261113
START_YEAR, START_MONTH = 2024, 1

MASTER = os.environ.get("PGY_BENCH_MASTER", "local[4]")

TAGS = [
    "美妆", "护肤", "穿搭", "母婴", "美食", "旅行", "数码", "家居", "健身", "宠物",
    "汽车", "教育", "游戏", "摄影", "音乐", "职场", "读书", "园艺", "咖啡", "露营",
]

# 事实表非分区列（顺序必须与 gen_data.py 的表头、建表 DDL 一致）
FACT_COLUMNS = [
    "red_id", "nickname", "tags_norm",
    "fans_count", "image_price", "video_price",
    "note_count_30d", "exposure_median_30d", "read_median_30d", "interaction_median_30d",
    "note_count_90d", "exposure_median_90d", "read_median_90d", "interaction_median_90d",
    "cpe_30d", "cpe_90d", "engagement_rate_30d", "price_per_10k_fans",
    "crawled_at", "source_file",
]
PARTITION_COLUMN = "stat_month"

DIM_COLUMNS = ["red_id", "nickname", "group_name", "status", "first_seen_month"]


def months() -> list[str]:
    out = []
    y, m = START_YEAR, START_MONTH
    for _ in range(N_MONTHS):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out
