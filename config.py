"""蒲公英达人离线数仓 · 全局配置。

数据源默认指向原采集项目（只读）；可用环境变量 PGY_SOURCE_DIR 覆盖。
"""
from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SOURCE_DIR = Path(
    os.environ.get(
        "PGY_SOURCE_DIR",
        str(Path.home() / "Documents" / "Codex" / "2026-07-24"
            / "30-90-1-2-3-4" / "work" / "pgy_crawler"),
    )
)
EXPORTS_DIR = SOURCE_DIR / "exports"
BACKUPS_DIR = SOURCE_DIR / "backups"
BLOGGER_LIST = SOURCE_DIR / "data" / "blogger_list.xlsx"

SQL_DIR = BASE_DIR / "warehouse" / "sql"
OUT_DIR = BASE_DIR / "output"
DB_PATH = OUT_DIR / "pgy_dw.db"

ALERT_THRESHOLD_PERCENT = 30.0
DEFAULT_MONTHS = ("2026-08", "2026-10")

# 与采集项目 report.py 中 REPORT_METRICS 一一对应
METRICS: list[tuple[str, str, str, str, str]] = [
    ("fans_count", "粉丝数", "规模", "人", "蒲公英详情页粉丝数"),
    ("image_price", "图文报价", "商业", "元", "合作笔记图文报价"),
    ("video_price", "视频报价", "商业", "元", "合作笔记视频报价"),
    ("note_count_30d", "近30天合作笔记数", "产出", "篇", "合作笔记筛选，非日常笔记"),
    ("exposure_median_30d", "近30天曝光中位数", "传播", "次", "合作笔记曝光中位数"),
    ("read_median_30d", "近30天阅读中位数", "传播", "次", "合作笔记阅读中位数"),
    ("interaction_median_30d", "近30天互动中位数", "互动", "次", "合作笔记互动中位数"),
    ("note_count_90d", "近90天合作笔记数", "产出", "篇", "合作笔记筛选，非日常笔记"),
    ("exposure_median_90d", "近90天曝光中位数", "传播", "次", "合作笔记曝光中位数"),
    ("read_median_90d", "近90天阅读中位数", "传播", "次", "合作笔记阅读中位数"),
    ("interaction_median_90d", "近90天互动中位数", "互动", "次", "合作笔记互动中位数"),
]

# ODS 中文表头 -> 统一英文列名
COLUMN_MAP: dict[str, str] = {
    "采集月份": "stat_month",
    "小红书号": "red_id",
    "账户名": "nickname",
    "数据来源": "data_source",
    "是否真实采集": "is_real_collect",
    "备注": "remark",
    "粉丝数": "fans_count",
    "图文报价": "image_price",
    "视频报价": "video_price",
    "标签": "tags_raw",
    "近30天合作笔记数": "note_count_30d",
    "近30天曝光中位数": "exposure_median_30d",
    "近30天阅读中位数": "read_median_30d",
    "近30天互动中位数": "interaction_median_30d",
    "近90天合作笔记数": "note_count_90d",
    "近90天曝光中位数": "exposure_median_90d",
    "近90天阅读中位数": "read_median_90d",
    "近90天互动中位数": "interaction_median_90d",
    "采集状态": "crawl_status",
    "失败原因": "error_message",
    "采集时间": "crawled_at",
}
ODS_COLUMNS = list(COLUMN_MAP.values())
