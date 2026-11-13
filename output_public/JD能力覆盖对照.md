# JD 能力覆盖对照（项目侧，不是简历措辞）

由 `python build_jd_extensions.py` 生成。所有「已做」项都有可运行代码与产物文件，
不依赖虚构描述；「部分 / 未做」如实标注。

## 本次构建结果

```json
{
  "governance": {
    "dictionary": {
      "dictionary_rows": 53,
      "indicator": 11,
      "field": 21,
      "table": 21,
      "outputs": [
        "data_dictionary.csv",
        "data_dictionary.json",
        "data_standards.md",
        "masking_policy.csv"
      ]
    },
    "lineage": {
      "nodes": 24,
      "edges": 28,
      "tasks": 5,
      "outputs": [
        "lineage.json",
        "lineage.html"
      ]
    },
    "quality": {
      "rules": 27,
      "runs": 3,
      "all_passed": true,
      "outputs": [
        "quality_rules.csv",
        "quality_history.csv",
        "dq_monitor.html"
      ]
    }
  },
  "profile": "skipped (--skip-spark)",
  "dashboard": {
    "dashboard": "output\\bi\\dashboard.html",
    "wide_rows": 122,
    "alert_rows": 310,
    "churn_rows": 27,
    "profile_rows": 122,
    "tag_rows": 85,
    "quality_runs": 3
  },
  "pandas": {
    "wide_rows": 122,
    "alert_rows": 310,
    "tier_groups": 2,
    "zscore_outliers": 9,
    "pandas": "3.1.1",
    "numpy": "2.5.4",
    "outputs": [
      "tier_summary.csv",
      "zscore_outliers.csv",
      "correlation.csv",
      "percentiles.csv",
      "pandas_report.md"
    ]
  },
  "olap": {
    "rows": 122,
    "duckdb_version": "2.0.0",
    "parquet": {
      "files": 1,
      "rows": 122
    },
    "scale": {
      "files": 44,
      "rows": 1320000,
      "groups": 11,
      "timing": {
        "runs_ms": [
          19.227,
          14.625,
          14.983
        ],
        "best_ms": 14.625,
        "median_ms": 14.983
      }
    },
    "result_match": true,
    "speedup_x": 0.8,
    "duckdb_best_ms": 3.727,
    "pandas_best_ms": 2.977,
    "outputs": [
      "ads_clean.csv",
      "olap_tier_agg.csv",
      "olap_window_top5.csv",
      "olap_scale_agg.csv",
      "duckdb_vs_pandas.csv",
      "olap_report.md"
    ]
  },
  "streaming": {
    "mode": "Spark Structured Streaming（微批，2s 触发）",
    "source": "文件流 C:\\Temp\\pgy_stream_in",
    "windows": "10 秒滚动窗口 + 1 分钟水位线",
    "window_rows": 392,
    "dead_letter_rows": 70,
    "rate_stream_rows": 1100,
    "exported": {
      "stream_tag_window.csv": 392,
      "stream_dead_letter.csv": 70
    },
    "checkpoint": "C:\\Temp\\pgy_stream_ckpt",
    "sink": "C:\\Temp\\pgy_stream_sink"
  },
  "iceberg": {
    "table": "lake.pgy.ads_blogger_value",
    "rows": 122,
    "insert_seconds": 2.98,
    "merge_seconds": 2.78,
    "snapshots": 2,
    "snapshot_detail": [
      {
        "snapshot_id": "106794598419492420",
        "committed_at": "2026-11-13T15:20:31.911+08:00",
        "operation": "append",
        "added_records": "122",
        "deleted_records": "",
        "added_files": "8",
        "deleted_files": ""
      },
      {
        "snapshot_id": "1198050979149091838",
        "committed_at": "2026-11-13T15:20:35.706+08:00",
        "operation": "overwrite",
        "added_records": "48",
        "deleted_records": "48",
        "added_files": "3",
        "deleted_files": "3"
      }
    ],
    "merge_snapshot": {
      "snapshot_id": "1198050979149091838",
      "committed_at": "2026-11-13T15:20:35.706+08:00",
      "operation": "overwrite",
      "added_records": "48",
      "deleted_records": "48",
      "added_files": "3",
      "deleted_files": "3"
    },
    "time_travel": [
      {
        "version": "v1(建表时)",
        "n": "122",
        "total_fans": "6779574",
        "reviewed": "0"
      },
      {
        "version": "v_now(当前)",
        "n": "122",
        "total_fans": "6779574",
        "reviewed": "3"
      }
    ],
    "schema_evolution": {
      "added": true,
      "null_after_add": 122.0
    },
    "partition_evolution": {
      "added": true,
      "spec_rows": 8
    },
    "data_files": 8,
    "hive_overwrite_seconds": 0.41,
    "hive_rows_written": 122,
    "iceberg_jar": "iceberg-spark-runtime-4.0_2.13-1.12.0.jar"
  }
}
```

## 原有结果完整性（未被污染）

| 产物 | 行数 |
|---|---:|
| ads_wide | 122 |
| ads_alert | 310 |
| ads_churn | 27 |
| spark_ads_wide | 122 |
| spark_ads_alert | 310 |
| spark_ads_churn | 27 |

## 能力覆盖表

| 能力项 | 对应 JD | 项目产物 | 状态 |
|---|---|---|---|
| 数据标准 / 数据字典（业务·技术·管理） | 图一职责 1/2 | governance/data_dictionary.csv、data_standards.md | 已做 |
| 元数据 / 表级血缘 | 图一职责 1、图二要求 7 | governance/lineage.json、lineage.html | 已做 |
| 数据质量规则 + 日常监控 | 三份 JD 均涉及 | governance/quality_rules.csv、dq_monitor.html | 已做 |
| 数据安全分级 / 脱敏策略 | 图一职责 1、图二要求 7 | governance/masking_policy.csv | 已做 |
| 标签体系 / 用户画像 | 图一职责 4 | dws_blogger_profile、ads_tag_profile | 已做 |
| 报表 / 可视化看板 | 图一职责 4、图二职责 3 | bi/dashboard.html（另有 Power BI 作品） | 已做 |
| Pandas / NumPy 数据处理 | 图一任职要求 | analysis/pandas_report.md 等 5 份产物 | 已做 |
| OLAP 引擎建模与查询 | 图二要求 5、图三要求 5 | olap/olap_report.md（DuckDB 列存） | 部分：嵌入式，非 Doris/ClickHouse |
| SQL / 引擎调优实测 | 图二要求 3、图三要求 4 | spark/bench/output/bench_report.md | 已做 |
| Spark 离线数仓 + 分层建模 | 三份 JD | 19 张既有 Hive 表 + 2 张画像表 | 已做 |
| 端到端采集→建模→计算→可视化 | 图一职责 | 采集系统 + 四层 SQL + 看板 | 已做 |
| Linux Shell 流水线脚本 | 图三要求 2 | scripts/run_pipeline.sh、start_airflow.sh | 轻量已做 |
| 工程质量与结果校验 | 图一职责 7、图二要求 7 | output_public/governance/quality_rules.csv、quality_history.csv | 已做：27 条规则 / 三层校验 |
| 实时数据开发（Spark Structured Streaming） | 三份 JD 均提及 | streaming/stream_report.md、output/streaming/*.csv | 已做：微批；Flink/Kafka 未做 |
| 大数据集群部署运维（Hadoop/YARN/Presto…） | 图三职责 3、要求 5 | — | 本机不可做 |
| 湖格式（Apache Iceberg） | 图三要求 4 | iceberg/iceberg_demo.py、output/iceberg/iceberg_report.md | 已做：MERGE / 时间旅行 / Schema 演进 |
| CDC / DataX | 图三要求 4 | — | 未做（P2） |
| Java | 图三要求 2 | — | 未做 |

## 边界

- 全部跑在 Windows + `local[1]/local[4]` 单机，内嵌 Derby Metastore；不是 Hadoop/YARN 集群。
- DuckDB 是嵌入式列存 OLAP，不等于 Doris/ClickHouse/StarRocks 部署运维。
- 132 万行是合成压测数据；218 行是真实快照。两者不可混为一谈。
- 数据安全分级是项目级策略文档，不是企业级权限系统（Ranger/Sentry）。

## 复现

```bash
python build_jd_extensions.py
bash scripts/run_pipeline.sh
bash scripts/start_airflow.sh test
```
