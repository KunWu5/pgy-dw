-- 01_ods.sql  接入层：只做「原样落地」，不清洗、不改类型
-- 每次全量重跑先 DROP，保证幂等

DROP TABLE IF EXISTS ods_pgy_monthly_snapshot;
CREATE TABLE ods_pgy_monthly_snapshot (
  stat_month TEXT, red_id TEXT, nickname TEXT,
  data_source TEXT, is_real_collect TEXT, remark TEXT,
  fans_count TEXT, image_price TEXT, video_price TEXT, tags_raw TEXT,
  note_count_30d TEXT, exposure_median_30d TEXT, read_median_30d TEXT, interaction_median_30d TEXT,
  note_count_90d TEXT, exposure_median_90d TEXT, read_median_90d TEXT, interaction_median_90d TEXT,
  crawl_status TEXT, error_message TEXT, crawled_at TEXT,
  source_file TEXT NOT NULL,
  ingest_batch TEXT NOT NULL,
  ingested_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ods_snapshot_month ON ods_pgy_monthly_snapshot(stat_month, red_id);

DROP TABLE IF EXISTS ods_blogger_list;
CREATE TABLE ods_blogger_list (
  red_id TEXT, nickname TEXT, group_name TEXT, remark TEXT
);

DROP TABLE IF EXISTS ods_crawl_run;
CREATE TABLE ods_crawl_run (
  run_id TEXT, run_month TEXT, started_at TEXT, finished_at TEXT,
  total_count TEXT, success_count TEXT, failed_count TEXT, status TEXT,
  source_file TEXT
);

-- MySQL 早期试跑批次：口径与正式 Excel 快照不同，只留在 ODS 供批次追溯
DROP TABLE IF EXISTS ods_mysql_snapshot;
CREATE TABLE ods_mysql_snapshot (
  snapshot_id TEXT, crawl_run_id TEXT, blogger_id TEXT, red_id TEXT, stat_month TEXT,
  nickname TEXT, fans_count TEXT, image_price TEXT, video_price TEXT, tags_raw TEXT,
  note_count_30d TEXT, exposure_median_30d TEXT, read_median_30d TEXT, interaction_median_30d TEXT,
  note_count_90d TEXT, exposure_median_90d TEXT, read_median_90d TEXT, interaction_median_90d TEXT,
  crawl_status TEXT, error_message TEXT, crawled_at TEXT, source_file TEXT
);
