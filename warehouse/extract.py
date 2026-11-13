"""ODS 接入层：把采集项目的 Excel 月快照 / 博主名单 / MySQL 备份原样落到 ODS 表。

原则：ODS 不做任何业务清洗，数值列保持 TEXT，保留来源文件、批次与接入时间，
      所有口径统一放到 DWD 完成，保证可追溯、可重跑。
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook

import config as cfg


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value).strip()


def init_ods(con: sqlite3.Connection) -> None:
    """执行 01_ods.sql，全量重建 ODS（重跑幂等）。"""
    con.executescript((cfg.SQL_DIR / "01_ods.sql").read_text(encoding="utf-8"))
    con.commit()


def _insert_ods_snapshot(con: sqlite3.Connection, rows: list[tuple]) -> int:
    cols = ",".join(cfg.ODS_COLUMNS + ["source_file", "ingest_batch", "ingested_at"])
    marks = ",".join("?" for _ in range(len(cfg.ODS_COLUMNS) + 3))
    con.executemany(f"INSERT INTO ods_pgy_monthly_snapshot ({cols}) VALUES ({marks})", rows)
    con.commit()
    return len(rows)


def load_excel_snapshots(con: sqlite3.Connection, months: tuple[str, ...]) -> dict:
    """读取 exports/YYYY-MM_pgy_result.xlsx，按月过滤。"""
    batch = datetime.now().strftime("batch_%Y%m%d_%H%M%S")
    ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    loaded: dict[str, int] = {}
    files = sorted(cfg.EXPORTS_DIR.glob("*_pgy_result.xlsx"))
    if not files:
        raise FileNotFoundError(f"未在 {cfg.EXPORTS_DIR} 找到 *_pgy_result.xlsx")

    for path in files:
        ws = load_workbook(path, read_only=True, data_only=True).active
        raw = list(ws.iter_rows(values_only=True))
        if not raw:
            continue
        header = [_text(h) or "" for h in raw[0]]
        idx = {cfg.COLUMN_MAP[h]: i for i, h in enumerate(header) if h in cfg.COLUMN_MAP}
        rows = []
        for r in raw[1:]:
            if not r or not _text(r[idx.get("red_id", 1)]):
                continue
            stat_month = _text(r[idx["stat_month"]]) if "stat_month" in idx else ""
            if months and stat_month and stat_month not in months:
                continue
            values = [_text(r[idx[c]]) if c in idx and idx[c] < len(r) else None for c in cfg.ODS_COLUMNS]
            rows.append(tuple(values + [path.name, batch, ingested_at]))
        n = _insert_ods_snapshot(con, rows)
        loaded[path.name] = n
    return loaded


def load_blogger_list(con: sqlite3.Connection) -> int:
    """读取正式博主名单（维表主数据来源）。"""
    if not cfg.BLOGGER_LIST.exists():
        return 0
    ws = load_workbook(cfg.BLOGGER_LIST, read_only=True, data_only=True).active
    raw = list(ws.iter_rows(values_only=True))
    header = [_text(h) or "" for h in raw[0]]
    idx = {h: i for i, h in enumerate(header)}
    rows = []
    for r in raw[1:]:
        if not r or not _text(r[idx.get("red_id", 0)]):
            continue
        rows.append((
            _text(r[idx["red_id"]]),
            _text(r[idx.get("nickname", 1)]) if idx.get("nickname", 1) < len(r) else None,
            _text(r[idx["group_name"]]) if "group_name" in idx and idx["group_name"] < len(r) else None,
            _text(r[idx["remark"]]) if "remark" in idx and idx["remark"] < len(r) else None,
        ))
    con.executemany(
        "INSERT INTO ods_blogger_list (red_id, nickname, group_name, remark) VALUES (?,?,?,?)", rows
    )
    con.commit()
    return len(rows)


def _split_sql_values(block: str) -> list[list[str]]:
    """解析 INSERT ... VALUES (...),(...); 中的元组，兼容单引号内的逗号与括号。"""
    rows: list[list[str]] = []
    cur: list[str] = []
    token: list[str] = []
    depth = 0
    in_quote = False
    i = 0
    while i < len(block):
        ch = block[i]
        if ch == "'":
            if in_quote and i + 1 < len(block) and block[i + 1] == "'":
                token.append("'")
                i += 2
                continue
            in_quote = not in_quote
            i += 1
            continue
        if not in_quote:
            if ch == "(":
                depth += 1
                if depth == 1:
                    cur, token = [], []
                i += 1
                continue
            if ch == ")":
                depth -= 1
                if depth == 0:
                    cur.append("".join(token).strip())
                    rows.append(cur)
                    i += 1
                    continue
            if ch == "," and depth == 1:
                cur.append("".join(token).strip())
                token = []
                i += 1
                continue
        token.append(ch)
        i += 1
    return rows


def _clean(value: str) -> str | None:
    v = value.strip()
    if v.upper() == "NULL" or v == "":
        return None
    return v


def load_mysql_backups(con: sqlite3.Connection) -> dict:
    """解析备份 SQL 中的 crawl_runs / blogger_snapshots，作为历史批次落到 ODS。

    这些是采集系统早期试跑批次（2026-07 / 2026-08），与正式 Excel 快照口径不同，
    只保留在 ODS 层供批次追溯，不并入 DWD 正式事实表。
    """
    result = {"runs": 0, "snapshots": 0, "files": 0}
    for path in sorted(cfg.BACKUPS_DIR.glob("*.sql")):
        try:
            sql = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if "INSERT INTO `blogger_snapshots`" not in sql:
            continue
        result["files"] += 1

        for m in re.finditer(r"INSERT INTO `crawl_runs` VALUES (.*?);", sql, re.S):
            for row in _split_sql_values(m.group(1)):
                if len(row) < 8:
                    continue
                v = [_clean(x) for x in row]
                con.execute(
                    "INSERT INTO ods_crawl_run (run_id, run_month, started_at, finished_at, "
                    "total_count, success_count, failed_count, status, source_file) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    tuple(v[:8]) + (path.name,),
                )
                result["runs"] += 1

        for m in re.finditer(r"INSERT INTO `blogger_snapshots` VALUES (.*?);", sql, re.S):
            for row in _split_sql_values(m.group(1)):
                if len(row) < 19:
                    continue
                v = [_clean(x) for x in row]
                con.execute(
                    "INSERT INTO ods_mysql_snapshot (snapshot_id, crawl_run_id, blogger_id, red_id, "
                    "stat_month, nickname, fans_count, image_price, video_price, tags_raw, "
                    "note_count_30d, exposure_median_30d, read_median_30d, interaction_median_30d, "
                    "note_count_90d, exposure_median_90d, read_median_90d, interaction_median_90d, "
                    "crawl_status, error_message, crawled_at, source_file) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    tuple(v[:21]) + (path.name,),
                )
                result["snapshots"] += 1
    con.commit()
    return result


def extract(con: sqlite3.Connection, months: tuple[str, ...]) -> dict:
    init_ods(con)
    return {
        "excel": load_excel_snapshots(con, months),
        "blogger_list": load_blogger_list(con),
        "mysql_backups": load_mysql_backups(con),
    }
