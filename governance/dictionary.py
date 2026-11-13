"""数据标准 / 数据字典：从项目真实配置与分层 SQL 导出，不手写业务口径。

对应 JD：
- 图一职责 1/2：落地数据标准、数据字典（业务 / 技术 / 管理三类规范）、元数据、数据安全
- 图二要求 7：元数据、数据质量、权限与安全

产物：
  output/governance/data_dictionary.csv / .json   指标 + 字段 + 表三级字典
  output/governance/data_standards.md             三类数据规范 + 分级与脱敏策略
  output/governance/masking_policy.csv            字段安全分级与脱敏规则
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config as cfg  # noqa: E402
from governance import OUT  # noqa: E402

SQL_DIR = ROOT / "spark" / "sql"

DICT_COLUMNS = [
    "category", "standard_id", "name_cn", "name_en", "business_domain", "unit",
    "definition", "data_type", "source_object", "transform_rule", "quality_rule",
    "refresh_mode", "owner_role", "security_level",
]

# 表级元数据：粒度 / 主键 / 分区 / 更新策略属于「技术类数据规范」，是设计定义而非运行数据
TABLE_META = {
    "ods_pgy_monthly_snapshot": ("每月每次采集的达人原始快照", "stat_month+red_id+crawled_at", "无", "全量追加"),
    "ods_blogger_list": ("达人名单主数据", "red_id", "无", "全量覆盖"),
    "ods_crawl_run": ("采集批次记录", "run_id", "无", "全量追加"),
    "ods_mysql_snapshot": ("MySQL 备份中的历史快照", "snapshot_id", "无", "全量追加"),
    "dwd_blogger_monthly_snapshot": ("每月每位达人的成功采集明细", "stat_month+red_id", "stat_month", "按分区覆盖"),
    "dwd_failed_record": ("被隔离的采集失败明细", "stat_month+red_id", "无", "全量覆盖"),
    "dwd_crawl_run": ("批次级成功/失败汇总", "run_month", "无", "全量覆盖"),
    "dwd_blogger_tag_bridge": ("每月每位达人的单标签桥接", "stat_month+red_id+tag", "无", "全量覆盖"),
    "dwd_blogger_metric_long": ("每月每位达人每项指标的长表记录", "stat_month+red_id+metric_key", "无", "全量覆盖"),
    "dim_blogger": ("博主代理键维表", "blogger_sk", "无", "全量覆盖"),
    "dim_blogger_scd": ("博主属性 SCD Type 2 版本", "red_id+valid_from_month", "无", "全量覆盖"),
    "dim_month": ("连续月份维表（补稀疏月）", "stat_month", "无", "全量覆盖"),
    "dim_metric": ("11 项指标口径维表", "metric_key", "无", "全量覆盖"),
    "dws_metric_monthly_change": ("每月每位达人每指标的环比与异动", "stat_month+red_id+metric_key", "无", "全量覆盖"),
    "dws_platform_monthly": ("平台月度汇总", "stat_month", "无", "全量覆盖"),
    "dws_tag_monthly": ("标签月度汇总", "stat_month+tag", "无", "全量覆盖"),
    "dws_blogger_profile": ("达人标签与价值画像宽表", "stat_month+red_id", "无", "全量覆盖"),
    "ads_blogger_value_wide": ("最新月达人价值宽表", "stat_month+red_id", "无", "全量覆盖"),
    "ads_metric_alert": ("30% 阈值异动清单", "stat_month+red_id+metric_key", "无", "全量覆盖"),
    "ads_blogger_churn": ("名单新增/流失", "to_month+red_id+change_type", "无", "全量覆盖"),
    "dws_tag_monthly_df": ("DataFrame API 版标签汇总（对账用）", "stat_month+tag", "无", "全量覆盖"),
    "dws_tag_rank_df": ("标签月度排名（Window）", "stat_month+tag", "无", "全量覆盖"),
}

LAYER_BY_FILE = [
    ("01_", "ODS"), ("02", "DIM/DWD"), ("03_", "DWS"),
    ("04_", "ADS"), ("05_", "DWS(画像)"),
]

TABLE_RE = re.compile(
    r"(?:CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|DROP\s+TABLE(?:\s+IF\s+EXISTS)?)\s+"
    r"(?:pgy_dw|pgy_dw_bench)\.([a-zA-Z0-9_]+)", re.I)

TYPE_BY_METRIC = {
    "fans_count": "BIGINT", "image_price": "DOUBLE", "video_price": "DOUBLE",
    "note_count_30d": "INT", "exposure_median_30d": "BIGINT",
    "read_median_30d": "BIGINT", "interaction_median_30d": "BIGINT",
    "note_count_90d": "INT", "exposure_median_90d": "BIGINT",
    "read_median_90d": "BIGINT", "interaction_median_90d": "BIGINT",
    "cpe_30d": "DOUBLE", "cpe_90d": "DOUBLE",
    "engagement_rate_30d": "DOUBLE", "price_per_10k_fans": "DOUBLE",
}


def sql_tables() -> list[dict]:
    out, seen = [], set()
    for path in sorted(SQL_DIR.glob("*.hql")):
        layer = next((l for prefix, l in LAYER_BY_FILE if path.name.startswith(prefix)), "OTHER")
        for name in TABLE_RE.findall(path.read_text(encoding="utf-8")):
            if name in seen:
                continue
            seen.add(name)
            grain, pk, part, refresh = TABLE_META.get(
                name, ("按分层 SQL 定义", "见建表语句", "见建表语句", "全量覆盖"))
            out.append({
                "category": "table", "standard_id": "T-" + name.upper(),
                "name_cn": name, "name_en": name, "business_domain": layer, "unit": "表",
                "definition": grain, "data_type": "Parquet/Hive 表",
                "source_object": "spark/sql/" + path.name,
                "transform_rule": f"粒度={grain}; 主键={pk}; 分区={part}; 更新={refresh}",
                "quality_rule": "分层归属、行数守恒、主键唯一、分区完整性",
                "refresh_mode": refresh, "owner_role": "数据开发", "security_level": "内部",
            })
    # 两张表由 PySpark 代码直接落表（不在 HQL 里），单独补齐，来源如实标注
    for name, layer, src in [
        ("dim_metric", "DIM", "spark/run_stage.py::stage_dim_dwd"),
        ("ads_blogger_churn", "ADS", "spark/run_stage.py::stage_ads"),
    ]:
        if name in seen:
            continue
        grain, pk, part, refresh = TABLE_META[name]
        out.append({
            "category": "table", "standard_id": "T-" + name.upper(),
            "name_cn": name, "name_en": name, "business_domain": layer, "unit": "表",
            "definition": grain, "data_type": "Parquet/Hive 表",
            "source_object": src,
            "transform_rule": f"粒度={grain}; 主键={pk}; 分区={part}; 更新={refresh}",
            "quality_rule": "行数守恒、主键唯一、双引擎对账",
            "refresh_mode": refresh, "owner_role": "数据开发", "security_level": "内部",
        })
    return out


def indicator_rows() -> list[dict]:
    rows = []
    for key, label, domain, unit, definition in cfg.METRICS:
        rows.append({
            "category": "indicator", "standard_id": "I-" + key.upper(),
            "name_cn": label, "name_en": key, "business_domain": domain, "unit": unit,
            "definition": definition, "data_type": TYPE_BY_METRIC.get(key, "DOUBLE"),
            "source_object": "ods_pgy_monthly_snapshot -> dwd_blogger_monthly_snapshot",
            "transform_rule": "UNPIVOT 成长表；环比 = (本月值-上月值)/ABS(上月值)*100",
            "quality_rule": f"非空占比 >= 95%；|环比| >= {cfg.ALERT_THRESHOLD_PERCENT:.0f}% 标记异动",
            "refresh_mode": "每月 25 日采集后", "owner_role": "数据开发 + 业务投放",
            "security_level": "内部",
        })
    return rows


def field_rows() -> list[dict]:
    rows = []
    for cn, en in cfg.COLUMN_MAP.items():
        sensitive = en in ("red_id", "nickname")
        rows.append({
            "category": "field", "standard_id": "F-" + en.upper(),
            "name_cn": cn, "name_en": en, "business_domain": "ODS 接入",
            "unit": "-", "definition": f"原始表头「{cn}」统一映射为标准字段 {en}",
            "data_type": "STRING（ODS 保持原始类型）",
            "source_object": "exports/*_pgy_result.xlsx",
            "transform_rule": "COLUMN_MAP 显式映射；ODS 不做类型转换，DWD 才 CAST",
            "quality_rule": "red_id / stat_month 非空；表头变更需同步 COLUMN_MAP",
            "refresh_mode": "随采集批次", "owner_role": "数据开发",
            "security_level": "内部-个人信息（需脱敏）" if sensitive else "内部",
        })
    return rows


def masking_rows() -> list[dict]:
    return [
        {"field": "red_id", "level": "PII-标识", "rule": "对外报表/看板只展示哈希前 8 位或编号",
         "storage": "Hive 表明文（内部域）", "access": "数据开发 / 投放（内部）"},
        {"field": "nickname", "level": "PII-标识", "rule": "对外展示时截断或替换为达人编号",
         "storage": "Hive 表明文（内部域）", "access": "数据开发 / 投放（内部）"},
        {"field": "image_price / video_price", "level": "商业敏感", "rule": "聚合口径对外，明细口径不对外",
         "storage": "Hive 表明文（内部域）", "access": "投放决策相关角色"},
        {"field": "crawled_at / source_file", "level": "技术元数据", "rule": "保留用于血缘与批次追溯",
         "storage": "Hive 表明文", "access": "数据开发"},
        {"field": "登录态 / 凭据", "level": "密钥", "rule": "不入库、不进 Git，走运行时配置",
         "storage": "外部配置", "access": "采集任务运行时"},
    ]


def build() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    rows = indicator_rows() + field_rows() + sql_tables()

    with (OUT / "data_dictionary.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=DICT_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    (OUT / "data_dictionary.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    with (OUT / "masking_policy.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["field", "level", "rule", "storage", "access"])
        w.writeheader()
        w.writerows(masking_rows())

    counts = {c: sum(1 for r in rows if r["category"] == c)
              for c in ("indicator", "field", "table")}
    md = [
        "# 数据标准与数据字典",
        "",
        f"- 指标口径 **{counts['indicator']}** 项（源自 `config.py` 的 METRICS，与采集端 REPORT_METRICS 一一对应）",
        f"- 字段标准 **{counts['field']}** 项（ODS 中文表头 → 统一英文列名）",
        f"- 表级元数据 **{counts['table']}** 张（从 `spark/sql/*.hql` 的建表语句解析）",
        "- 阈值口径：环比绝对值 ≥ %.0f%% 判为异常波动" % cfg.ALERT_THRESHOLD_PERCENT,
        "",
        "## 业务类规范",
        "",
        "1. 指标口径由 `config.py` 单点定义，业务含义、单位、计算规则、异动阈值随代码版本管理，禁止在 SQL 里另写一份口径。",
        "2. 「近 30/90 天合作笔记数」只统计合作笔记，不含日常笔记；所有派生指标（CPE、互动率、万粉报价）以该口径为分母。",
        "3. 名单停用采用 `inactive` 软删除，保留历史版本；新增/流失以月为粒度比对，不覆盖历史。",
        "",
        "## 技术类规范",
        "",
        "1. 分层：ODS 原样接入（全 STRING）→ DWD 清洗去重与类型化 → DWS 汇总 → ADS 应用；跨层禁止反向引用。",
        "2. 命名：字段小写下划线；表前缀 `ods_/dwd_/dim_/dws_/ads_`；分区键统一 `stat_month`（yyyy-MM）。",
        "3. 存储：Parquet 列存；DWD 事实表按 `stat_month` 分区，用 `INSERT OVERWRITE ... PARTITION` 幂等重跑。",
        "4. 变更：新增指标必须同步 `METRICS`、`dim_metric` 维表、`UNPIVOT` 长表和质量校验规则四处。",
        "",
        "## 管理类规范",
        "",
        "1. 责任人角色：数据开发负责分层与调度，业务投放负责口径确认；口径变更需双方确认后改 `config.py`。",
        "2. 质量：每次运行产出质量校验报告，失败即阻断；采集失败记录单独落 `dwd_failed_record`，不污染事实表。",
        "3. 安全：按 `masking_policy.csv` 分级；登录态等凭据不入库、不进版本库；对外看板只出聚合口径。",
        "4. 追溯：表级血缘见 `lineage.json`，批次字段 `crawled_at / source_file / ingest_batch` 保留全链路。",
        "",
        "> 这是个人项目级的数据治理实践，规模与「企业级」不同，但标准、字典、质量、元数据、分级五类产物齐备。",
    ]
    (OUT / "data_standards.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    return {"dictionary_rows": len(rows), **counts,
            "outputs": ["data_dictionary.csv", "data_dictionary.json",
                        "data_standards.md", "masking_policy.csv"]}


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False))
