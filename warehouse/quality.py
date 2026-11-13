"""数据质量校验：把校验规则本身沉淀成一张 ADS 表 + 一份 JSON 报告。"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime

import config as cfg


def _scalar(con: sqlite3.Connection, sql: str) -> float:
    row = con.execute(sql).fetchone()
    return float(row[0] or 0) if row else 0.0


def run(con: sqlite3.Connection) -> list[dict]:
    checks: list[dict] = []

    def add(name: str, rule: str, actual: float, expected: str, passed: bool, detail: str = "") -> None:
        checks.append({
            "check_name": name,
            "rule": rule,
            "actual": actual,
            "expected": expected,
            "passed": 1 if passed else 0,
            "detail": detail,
        })

    ods_total = _scalar(con, "SELECT COUNT(*) FROM ods_pgy_monthly_snapshot")
    dwd_ok = _scalar(con, "SELECT COUNT(*) FROM dwd_blogger_monthly_snapshot")
    dwd_fail = _scalar(con, "SELECT COUNT(*) FROM dwd_failed_record")
    add("ODS 到 DWD 行数守恒", "ODS 行数 = DWD 成功行数 + 失败明细行数",
        ods_total, f"{dwd_ok + dwd_fail:.0f}", abs(ods_total - dwd_ok - dwd_fail) < 0.5,
        f"ODS={ods_total:.0f}, DWD成功={dwd_ok:.0f}, 失败={dwd_fail:.0f}")

    dup = _scalar(con, "SELECT COUNT(*) FROM (SELECT 1 FROM dwd_blogger_monthly_snapshot "
                       "GROUP BY stat_month, red_id HAVING COUNT(*) > 1)")
    add("DWD 主键唯一", "(stat_month, red_id) 无重复", dup, "0", dup == 0)

    nulls = _scalar(con, "SELECT COUNT(*) FROM dwd_blogger_monthly_snapshot "
                         "WHERE red_id IS NULL OR TRIM(red_id)='' OR stat_month IS NULL OR TRIM(stat_month)=''")
    add("关键字段非空", "red_id / stat_month 不为空", nulls, "0", nulls == 0)

    invalid = _scalar(con, "SELECT COUNT(*) FROM dwd_blogger_monthly_snapshot "
                           "WHERE fans_count <= 0 OR image_price < 0 OR video_price < 0 "
                           "OR interaction_median_30d < 0 OR exposure_median_30d < 0")
    add("数值值域", "粉丝数>0、报价/中位数指标>=0", invalid, "0", invalid == 0)

    metric_total = _scalar(con, "SELECT COUNT(*) FROM dwd_blogger_metric_long")
    metric_null = _scalar(con, "SELECT COUNT(*) FROM dwd_blogger_metric_long WHERE metric_value IS NULL")
    rate = (metric_total - metric_null) / metric_total * 100 if metric_total else 0
    add("11 项指标完整率", "非空指标占比 >= 95%", round(rate, 2), ">= 95", rate >= 95,
        f"总计 {metric_total:.0f} 条指标记录，空值 {metric_null:.0f}")

    month_rows = con.execute(
        "SELECT stat_month, COUNT(*), SUM(CASE WHEN crawl_status='success' THEN 1 ELSE 0 END) "
        "FROM ods_pgy_monthly_snapshot GROUP BY stat_month ORDER BY stat_month"
    ).fetchall()
    for m, total, ok in month_rows:
        r = ok / total * 100 if total else 0
        add(f"{m} 采集成功率", "月度采集成功率 >= 95%", round(r, 2), ">= 95", r >= 95,
            f"{ok:.0f}/{total:.0f}")

    alerts = _scalar(con, "SELECT COUNT(*) FROM ads_metric_alert")
    add("异常波动提醒", "记录 >=30% 波动的指标数（信息项）", alerts, "信息项", True,
        f"阈值 {cfg.ALERT_THRESHOLD_PERCENT:.0f}%")

    churn = _scalar(con, "SELECT COUNT(*) FROM ads_blogger_churn")
    add("名单增删追踪", "新增 / 流失博主记录数（信息项）", churn, "信息项", True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con.executemany(
        "INSERT INTO ads_dq_summary (check_name, rule, actual, expected, passed, detail, checked_at) "
        "VALUES (?,?,?,?,?,?,?)",
        [(c["check_name"], c["rule"], c["actual"], c["expected"], c["passed"], c["detail"], now)
         for c in checks],
    )
    con.commit()

    report = cfg.OUT_DIR / "quality_report.json"
    report.write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8")
    return checks
