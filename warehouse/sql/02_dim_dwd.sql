-- 02_dim_dwd.sql  维度层 + 明细数据层
-- 口径统一在这里完成：类型转换、去重、失败记录隔离、派生指标、多值标签规范化

DROP TABLE IF EXISTS dim_blogger;
CREATE TABLE dim_blogger (
  blogger_sk INTEGER PRIMARY KEY,
  red_id TEXT NOT NULL UNIQUE,
  current_nickname TEXT,
  group_name TEXT,
  first_seen_month TEXT,
  last_seen_month TEXT,
  status TEXT
);

WITH base AS (
  SELECT red_id, MIN(stat_month) AS first_seen_month, MAX(stat_month) AS last_seen_month
  FROM ods_pgy_monthly_snapshot GROUP BY red_id
), latest AS (
  SELECT red_id, nickname,
         ROW_NUMBER() OVER (PARTITION BY red_id ORDER BY stat_month DESC, crawled_at DESC) AS rn
  FROM ods_pgy_monthly_snapshot
)
INSERT INTO dim_blogger (red_id, current_nickname, group_name, first_seen_month, last_seen_month, status)
SELECT b.red_id, l.nickname, g.group_name, b.first_seen_month, b.last_seen_month,
       CASE WHEN g.red_id IS NULL THEN 'inactive' ELSE 'active' END
FROM base b
LEFT JOIN latest l ON l.red_id = b.red_id AND l.rn = 1
LEFT JOIN ods_blogger_list g ON g.red_id = b.red_id;

-- 月份维表 / 指标维表：由 Python 补充数据（连续月份、11 项指标口径）
DROP TABLE IF EXISTS dim_month;
CREATE TABLE dim_month (
  stat_month TEXT PRIMARY KEY, stat_year INTEGER, stat_quarter INTEGER,
  stat_month_no INTEGER, has_snapshot INTEGER, is_quarter_end INTEGER
);

DROP TABLE IF EXISTS dim_metric;
CREATE TABLE dim_metric (
  metric_key TEXT PRIMARY KEY, metric_label TEXT, metric_type TEXT,
  unit TEXT, calc_rule TEXT, alert_threshold_pct REAL
);

-- DWD 明细事实：一次采集 = 一位博主一个月的快照
DROP TABLE IF EXISTS dwd_blogger_monthly_snapshot;
CREATE TABLE dwd_blogger_monthly_snapshot (
  stat_month TEXT NOT NULL, red_id TEXT NOT NULL, nickname TEXT, tags_norm TEXT,
  fans_count INTEGER, image_price REAL, video_price REAL,
  note_count_30d INTEGER, exposure_median_30d INTEGER, read_median_30d INTEGER, interaction_median_30d INTEGER,
  note_count_90d INTEGER, exposure_median_90d INTEGER, read_median_90d INTEGER, interaction_median_90d INTEGER,
  cpe_30d REAL, cpe_90d REAL, engagement_rate_30d REAL, price_per_10k_fans REAL,
  crawled_at TEXT, source_file TEXT, etl_time TEXT
);

WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY stat_month, red_id ORDER BY crawled_at DESC) AS rn
  FROM ods_pgy_monthly_snapshot
  WHERE red_id IS NOT NULL AND TRIM(red_id) <> ''
)
INSERT INTO dwd_blogger_monthly_snapshot (
  stat_month, red_id, nickname, tags_norm,
  fans_count, image_price, video_price,
  note_count_30d, exposure_median_30d, read_median_30d, interaction_median_30d,
  note_count_90d, exposure_median_90d, read_median_90d, interaction_median_90d,
  cpe_30d, cpe_90d, engagement_rate_30d, price_per_10k_fans,
  crawled_at, source_file, etl_time
)
SELECT
  stat_month, TRIM(red_id), nickname,
  TRIM(REPLACE(REPLACE(REPLACE(COALESCE(tags_raw, ''), ', ', '|'), ',', '|'), '，', '|'), '| '),
  CAST(NULLIF(fans_count, '') AS INTEGER),
  CAST(NULLIF(image_price, '') AS REAL),
  CAST(NULLIF(video_price, '') AS REAL),
  CAST(NULLIF(note_count_30d, '') AS INTEGER),
  CAST(NULLIF(exposure_median_30d, '') AS INTEGER),
  CAST(NULLIF(read_median_30d, '') AS INTEGER),
  CAST(NULLIF(interaction_median_30d, '') AS INTEGER),
  CAST(NULLIF(note_count_90d, '') AS INTEGER),
  CAST(NULLIF(exposure_median_90d, '') AS INTEGER),
  CAST(NULLIF(read_median_90d, '') AS INTEGER),
  CAST(NULLIF(interaction_median_90d, '') AS INTEGER),
  ROUND(CAST(NULLIF(image_price, '') AS REAL) / NULLIF(CAST(NULLIF(interaction_median_30d, '') AS REAL), 0), 2),
  ROUND(CAST(NULLIF(image_price, '') AS REAL) / NULLIF(CAST(NULLIF(interaction_median_90d, '') AS REAL), 0), 2),
  ROUND(CAST(NULLIF(interaction_median_30d, '') AS REAL) / NULLIF(CAST(NULLIF(exposure_median_30d, '') AS REAL), 0), 4),
  ROUND(CAST(NULLIF(image_price, '') AS REAL) / NULLIF(CAST(NULLIF(fans_count, '') AS REAL), 0) * 10000, 2),
  crawled_at, source_file, datetime('now')
FROM ranked
WHERE rn = 1 AND LOWER(COALESCE(crawl_status, '')) = 'success';

-- 失败明细不丢：单独留存，供数据质量与补采使用
DROP TABLE IF EXISTS dwd_failed_record;
CREATE TABLE dwd_failed_record AS
SELECT stat_month, red_id, nickname, crawl_status, error_message, crawled_at, source_file
FROM ods_pgy_monthly_snapshot
WHERE LOWER(COALESCE(crawl_status, '')) <> 'success';

-- 批次事实：按 ODS 快照汇总每月运行情况
DROP TABLE IF EXISTS dwd_crawl_run;
CREATE TABLE dwd_crawl_run AS
SELECT stat_month AS run_month,
       COUNT(*) AS total_count,
       SUM(CASE WHEN LOWER(COALESCE(crawl_status, '')) = 'success' THEN 1 ELSE 0 END) AS success_count,
       SUM(CASE WHEN LOWER(COALESCE(crawl_status, '')) <> 'success' THEN 1 ELSE 0 END) AS failed_count,
       ROUND(SUM(CASE WHEN LOWER(COALESCE(crawl_status, '')) = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct,
       MIN(ingest_batch) AS ingest_batch
FROM ods_pgy_monthly_snapshot GROUP BY stat_month;

-- 多值标签桥接表：由 Python 拆分 tags_norm 写入
DROP TABLE IF EXISTS dwd_blogger_tag_bridge;
CREATE TABLE dwd_blogger_tag_bridge (
  stat_month TEXT, red_id TEXT, tag TEXT
);
CREATE INDEX IF NOT EXISTS idx_tag_bridge ON dwd_blogger_tag_bridge(stat_month, tag);

-- 缓慢变化维 SCD2 见 02b_scd.sql：需先规范化 tags_norm，避免标签顺序变化误切版本
