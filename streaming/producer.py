"""模拟采集端：把达人事件持续落盘到监控目录（文件流 source）。

真实项目里这是采集系统写 Excel/CSV 的环节；这里复用 ADS 结果里的达人属性，
生成带事件时间的增量文件，供 Structured Streaming 消费。
"""
from __future__ import annotations

import csv
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

WATCH = Path(r"C:\Temp\pgy_stream_in")
BATCHES = 5
ROWS_PER_BATCH = 200
SEED = 20261113


def main() -> int:
    WATCH.mkdir(parents=True, exist_ok=True)
    for old in WATCH.glob("part_*.csv"):
        old.unlink()

    src = ROOT / "output" / "ads_blogger_value_wide.csv"
    with src.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("ADS 结果为空，无法生成流数据")
        return 1

    rng = random.Random(SEED)
    base_ts = 1763032800  # 2026-11-13 10:00:00 +08:00
    seq = 0
    for b in range(BATCHES):
        name = WATCH / f"part_{b:04d}.csv"
        with name.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["event_time", "red_id", "tag", "fans_count",
                        "image_price", "interaction_cnt"])
            for _ in range(ROWS_PER_BATCH):
                r = rows[seq % len(rows)]
                seq += 1
                tags = (r.get("tags_norm") or "").split("|")
                tag = tags[0] if tags and tags[0] else ""
                # 约 5% 脏数据：粉丝数为负或标签为空，供实时质量分支识别
                dirty = rng.random() < 0.05
                fans = -abs(int(float(r.get("fans_count") or 0))) if dirty else int(float(r.get("fans_count") or 0))
                tag = "" if rng.random() < 0.02 else tag
                ts = base_ts + int(seq * 0.5)
                w.writerow([time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
                            r.get("red_id"), tag, fans,
                            r.get("image_price") or 0,
                            r.get("interaction_median_30d") or 0])
        print(f"  写入 {name.name}: {ROWS_PER_BATCH} 行")
        time.sleep(1.2)
    print(f"共 {BATCHES * ROWS_PER_BATCH} 行，目录 {WATCH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
