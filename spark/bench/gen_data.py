"""生成合成规模数据（driver 端纯 Python，不启动 Spark、不经 Python worker）。

产出三份 CSV，全部落在纯英文路径的 staging 目录：
  bench_fact.csv : 5,000 达人 × 24 月 = 120,000 行事实表
  bench_dim.csv  : 5,000 行博主维表（供广播 join 压测）
  manifest.json  : 生成参数与行数，便于复现

数据是**合成的**：仅用于验证「同一套分层 SQL 在百万行量级仍成立」以及调优对比，
不参与原项目的任何结果，也不写进简历作为真实业务数据。
"""
from __future__ import annotations

import csv
import json
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.bench import config_bench as C  # noqa: E402


def generate() -> dict:
    C.STAGING.mkdir(parents=True, exist_ok=True)
    C.OUT_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(C.SEED)
    month_list = C.months()

    fact_path = C.STAGING / "bench_fact.csv"
    dim_path = C.STAGING / "bench_dim.csv"

    fact_rows = 0
    with fact_path.open("w", encoding="utf-8", newline="") as f, \
            dim_path.open("w", encoding="utf-8", newline="") as d:
        fw = csv.writer(f)
        dw = csv.writer(d)
        fw.writerow(C.FACT_COLUMNS + [C.PARTITION_COLUMN])
        dw.writerow(C.DIM_COLUMNS)

        for i in range(C.N_BLOGGERS):
            red_id = f"B{i:06d}"
            nickname = f"达人{i:05d}"
            tags = rng.sample(C.TAGS, rng.randint(1, 3))
            tags_norm = "|".join(tags)
            status = "active" if rng.random() < 0.85 else "inactive"
            group_name = rng.choice(["投放一组", "投放二组", "投放三组"])
            dw.writerow([red_id, nickname, group_name, status, month_list[0]])

            fans0 = rng.randint(10_000, 3_000_000)
            price0 = rng.randint(500, 80_000)
            vprice0 = int(price0 * rng.uniform(1.2, 2.4))
            notes0 = rng.randint(2, 60)
            exp0 = rng.randint(1_000, 400_000)
            read0 = int(exp0 * rng.uniform(0.25, 0.65))
            inter0 = int(read0 * rng.uniform(0.01, 0.12))
            phase = rng.uniform(0, 2 * math.pi)

            for t, month in enumerate(month_list):
                season = 1.0 + 0.12 * math.sin(phase + t / 3.0)
                drift = 1.0 + 0.012 * t
                jitter = rng.uniform(0.94, 1.06)
                k = season * drift * jitter
                fans = int(fans0 * (1 + 0.02 * t) * (0.97 + 0.06 * rng.random()))
                exp = int(exp0 * k)
                read = int(read0 * k)
                inter = int(inter0 * k)
                notes = max(1, int(notes0 * (0.9 + 0.2 * rng.random())))
                image_price = max(0, int(price0 * (1 + 0.01 * t) * (0.95 + 0.1 * rng.random())))
                video_price = max(0, int(vprice0 * (1 + 0.01 * t) * (0.95 + 0.1 * rng.random())))
                cpe30 = round(image_price / max(inter, 1), 4)
                cpe90 = round(video_price / max(inter * 3, 1), 4)
                engagement = round(inter / max(read, 1), 6)
                price_per_10k = round(image_price / max(fans / 10_000, 1), 4)
                fw.writerow([
                    red_id, nickname, tags_norm,
                    fans, image_price, video_price,
                    notes, exp, read, inter,
                    notes * 3, exp * 3, read * 3, inter * 3,
                    cpe30, cpe90, engagement, price_per_10k,
                    f"{month}-25 02:15:00", "bench_synthetic.csv", month,
                ])
                fact_rows += 1

    manifest = {
        "synthetic": True,
        "seed": C.SEED,
        "bloggers": C.N_BLOGGERS,
        "months": C.N_MONTHS,
        "metrics_per_row": C.N_METRICS,
        "fact_rows": fact_rows,
        "metric_long_rows_expected": fact_rows * C.N_METRICS,
        "dim_rows": C.N_BLOGGERS,
        "fact_csv": str(fact_path),
        "dim_csv": str(dim_path),
        "fact_csv_bytes": fact_path.stat().st_size,
    }
    (C.OUT_DIR / "data_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False))
    return manifest


if __name__ == "__main__":
    generate()
