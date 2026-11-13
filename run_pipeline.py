"""蒲公英达人离线数仓 · 一键运行入口。

用法：
    python run_pipeline.py                         # 默认 2026-08、2026-10 全量重跑
    python run_pipeline.py --months 2026-10       # 只跑指定月份
    python run_pipeline.py --source-dir <采集项目目录>

全量重建（DROP + CREATE）实现幂等：同一份输入反复执行，结果一致、不产生重复数据。
生产环境应改为按 stat_month 分区覆盖（Hive: INSERT OVERWRITE ... PARTITION）。
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config as cfg
from warehouse import extract as ex
from warehouse import quality as dq
from warehouse import transform as tf

EXPORT_TABLES = [
    "ads_blogger_value_wide",
    "ads_metric_alert",
    "ads_blogger_churn",
    "ads_dq_summary",
    "dws_metric_monthly_change",
]


def export_csv(con: sqlite3.Connection) -> dict[str, int]:
    cfg.OUT_DIR.mkdir(parents=True, exist_ok=True)
    counts = {}
    for table in EXPORT_TABLES:
        cur = con.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        path = cfg.OUT_DIR / f"{table}.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        counts[table] = len(rows)
    return counts


def main() -> int:
    ap = argparse.ArgumentParser(description="蒲公英达人离线数仓")
    ap.add_argument("--source-dir", default=None, help="原采集项目根目录")
    ap.add_argument("--months", nargs="*", default=list(cfg.DEFAULT_MONTHS), help="要接入的快照月份")
    ap.add_argument("--db", default=None, help="输出 SQLite 路径")
    args = ap.parse_args()

    if args.source_dir:
        cfg.SOURCE_DIR = Path(args.source_dir)
        cfg.EXPORTS_DIR = cfg.SOURCE_DIR / "exports"
        cfg.BACKUPS_DIR = cfg.SOURCE_DIR / "backups"
        cfg.BLOGGER_LIST = cfg.SOURCE_DIR / "data" / "blogger_list.xlsx"
    cfg.OUT_DIR.mkdir(parents=True, exist_ok=True)
    db = Path(args.db) if args.db else cfg.DB_PATH

    print("=" * 62)
    print("蒲公英达人离线数仓 · ODS → DIM/DWD → DWS → ADS")
    print("=" * 62)
    print(f"数据源 : {cfg.SOURCE_DIR}")
    print(f"月份   : {', '.join(args.months) or '全部'}")
    print(f"输出库 : {db}\n")

    con = sqlite3.connect(db)
    con.execute("PRAGMA foreign_keys = ON")

    print("[1/4] ODS 接入")
    src = ex.extract(con, tuple(args.months))
    for k, v in src["excel"].items():
        print(f"      {k}: {v} 行")
    print(f"      博主名单: {src['blogger_list']} 行")
    print(f"      MySQL 历史备份: {src['mysql_backups']['files']} 个文件 / "
          f"{src['mysql_backups']['runs']} 批次 / {src['mysql_backups']['snapshots']} 快照（仅 ODS 追溯）")

    print("[2/4] 分层建模")
    stats = tf.build(con)
    print(f"      dim_month={stats['dim_month']}  dim_metric={stats['dim_metric']}  "
          f"dim_blogger={stats['dim_blogger']}  SCD2={stats['dim_blogger_scd']}")
    print(f"      dwd 事实={stats['dwd_blogger_monthly_snapshot']}  "
          f"失败明细={stats['dwd_failed_record']}  标签桥接={stats['tag_bridge']}")
    print(f"      指标长表={stats['dwd_blogger_metric_long']}  "
          f"环比汇总={stats['dws_metric_monthly_change']}")
    print(f"      ads 宽表={stats['ads_blogger_value_wide']}  "
          f"异常提醒={stats['ads_metric_alert']}  增删追踪={stats['ads_blogger_churn']}")

    print("[3/4] 数据质量校验")
    checks = dq.run(con)
    failed = [c for c in checks if not c["passed"]]
    for c in checks:
        flag = "PASS" if c["passed"] else "FAIL"
        print(f"      [{flag}] {c['check_name']}: {c['actual']} (期望 {c['expected']})")
    print(f"      通过 {len(checks) - len(failed)}/{len(checks)}，报告 output/quality_report.json")

    print("[4/4] 导出 ADS 结果")
    for k, v in export_csv(con).items():
        print(f"      output/{k}.csv: {v} 行")

    print("\n完成。可直接打开 output/ 下的 CSV 查看结果。")
    con.close()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
