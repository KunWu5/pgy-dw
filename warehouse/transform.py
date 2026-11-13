"""分层构建：ODS -> DIM/DWD -> DWS -> ADS。

分层 SQL 独立放在 warehouse/sql/ 便于审查与迁移到 Hive / Spark SQL；
少量需要 Python 处理的逻辑（连续月份维表、指标维表、多值标签桥接表）在此补充。
"""
from __future__ import annotations

import sqlite3
from datetime import date

import config as cfg


def _month_range(first: str, last: str) -> list[str]:
    y, m = map(int, first.split("-"))
    ey, em = map(int, last.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def _build_dim_month(con: sqlite3.Connection) -> int:
    row = con.execute(
        "SELECT MIN(stat_month), MAX(stat_month) FROM ods_pgy_monthly_snapshot"
    ).fetchone()
    if not row or not row[0]:
        return 0
    months = _month_range(row[0], row[1])
    available = {
        r[0] for r in con.execute(
            "SELECT DISTINCT stat_month FROM ods_pgy_monthly_snapshot"
        ).fetchall()
    }
    rows = []
    for m in months:
        y, mm = map(int, m.split("-"))
        rows.append((
            m, y, (mm - 1) // 3 + 1, mm,
            1 if m in available else 0,
            1 if date(y, mm, 1).month in (3, 6, 9, 12) else 0,
        ))
    con.executemany(
        "INSERT INTO dim_month (stat_month, stat_year, stat_quarter, stat_month_no, "
        "has_snapshot, is_quarter_end) VALUES (?,?,?,?,?,?)",
        rows,
    )
    return len(rows)


def _build_dim_metric(con: sqlite3.Connection) -> int:
    rows = [
        (key, label, kind, unit, rule, cfg.ALERT_THRESHOLD_PERCENT)
        for key, label, kind, unit, rule in cfg.METRICS
    ]
    con.executemany(
        "INSERT INTO dim_metric (metric_key, metric_label, metric_type, unit, "
        "calc_rule, alert_threshold_pct) VALUES (?,?,?,?,?,?)",
        rows,
    )
    return len(rows)


def _normalize_tags(con: sqlite3.Connection) -> int:
    """标签排序去重后再做 SCD2，避免「护肤,美妆」与「美妆,护肤」被当成属性变化。"""
    rows = con.execute("SELECT rowid, tags_norm FROM dwd_blogger_monthly_snapshot").fetchall()
    updates = []
    for rid, tags in rows:
        norm = "|".join(sorted({t.strip() for t in (tags or "").split("|") if t.strip()}))
        updates.append((norm, rid))
    con.executemany("UPDATE dwd_blogger_monthly_snapshot SET tags_norm = ? WHERE rowid = ?", updates)
    return len(updates)


def _build_tag_bridge(con: sqlite3.Connection) -> int:
    """多值维度 -> 桥接表：一个博主可属于多个标签，避免事实表存多值字段。"""
    rows = []
    for stat_month, red_id, tags in con.execute(
        "SELECT stat_month, red_id, tags_norm FROM dwd_blogger_monthly_snapshot"
    ).fetchall():
        seen = set()
        for tag in (tags or "").split("|"):
            tag = tag.strip()
            if tag and tag not in seen:
                seen.add(tag)
                rows.append((stat_month, red_id, tag))
    con.executemany(
        "INSERT INTO dwd_blogger_tag_bridge (stat_month, red_id, tag) VALUES (?,?,?)", rows
    )
    return len(rows)


def build(con: sqlite3.Connection) -> dict:
    con.executescript((cfg.SQL_DIR / "02_dim_dwd.sql").read_text(encoding="utf-8"))
    con.commit()

    stats = {
        "tags_normalized": _normalize_tags(con),
        "dim_month": _build_dim_month(con),
        "dim_metric": _build_dim_metric(con),
    }
    con.commit()

    # SCD2 必须在标签规范化之后；桥接表必须在 03（标签汇总）之前
    con.executescript((cfg.SQL_DIR / "02b_scd.sql").read_text(encoding="utf-8"))
    con.commit()
    stats["tag_bridge"] = _build_tag_bridge(con)
    con.commit()

    con.executescript((cfg.SQL_DIR / "03_dws.sql").read_text(encoding="utf-8"))
    con.commit()
    con.executescript((cfg.SQL_DIR / "04_ads.sql").read_text(encoding="utf-8"))
    con.commit()

    for table in [
        "ods_pgy_monthly_snapshot", "dwd_blogger_monthly_snapshot",
        "dwd_failed_record", "dim_blogger", "dim_blogger_scd",
        "dwd_blogger_metric_long", "dws_metric_monthly_change",
        "ads_blogger_value_wide", "ads_metric_alert", "ads_blogger_churn",
    ]:
        stats[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return stats
