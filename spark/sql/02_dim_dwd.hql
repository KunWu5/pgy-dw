-- 02_dim_dwd.hql  维度表 DDL + DWD 分区表 DDL
-- Hive Metastore 表：DWD 以 stat_month 为分区，支持按分区覆盖（补数 / 幂等重跑）

-- DWD 表不 DROP：按 stat_month 静态分区覆盖即可实现幂等重跑与按月补数，
-- 单独补某个月时其他分区数据保持不变。
CREATE TABLE IF NOT EXISTS pgy_dw.dwd_blogger_monthly_snapshot (
  red_id STRING, nickname STRING, tags_norm STRING,
  fans_count BIGINT, image_price DOUBLE, video_price DOUBLE,
  note_count_30d INT, exposure_median_30d BIGINT, read_median_30d BIGINT, interaction_median_30d BIGINT,
  note_count_90d INT, exposure_median_90d BIGINT, read_median_90d BIGINT, interaction_median_90d BIGINT,
  cpe_30d DOUBLE, cpe_90d DOUBLE, engagement_rate_30d DOUBLE, price_per_10k_fans DOUBLE,
  crawled_at STRING, source_file STRING
) USING parquet PARTITIONED BY (stat_month STRING);

DROP TABLE IF EXISTS pgy_dw.dim_blogger;
CREATE TABLE pgy_dw.dim_blogger USING parquet AS
WITH base AS (
  SELECT red_id, MIN(stat_month) AS first_seen_month, MAX(stat_month) AS last_seen_month
  FROM pgy_dw.ods_pgy_monthly_snapshot GROUP BY red_id
), latest AS (
  SELECT red_id, nickname,
         ROW_NUMBER() OVER (PARTITION BY red_id ORDER BY stat_month DESC, crawled_at DESC) AS rn
  FROM pgy_dw.ods_pgy_monthly_snapshot
)
SELECT ROW_NUMBER() OVER (ORDER BY b.red_id) AS blogger_sk,
       b.red_id, l.nickname AS current_nickname, g.group_name,
       b.first_seen_month, b.last_seen_month,
       CASE WHEN g.red_id IS NULL THEN 'inactive' ELSE 'active' END AS status
FROM base b
LEFT JOIN latest l ON l.red_id = b.red_id AND l.rn = 1
LEFT JOIN pgy_dw.ods_blogger_list g ON g.red_id = b.red_id;

-- 连续月份维表：补出无快照的 2026-09，用于识别稀疏月份
DROP TABLE IF EXISTS pgy_dw.dim_month;
CREATE TABLE pgy_dw.dim_month USING parquet AS
SELECT date_format(m, 'yyyy-MM') AS stat_month,
       year(m) AS stat_year,
       quarter(m) AS stat_quarter,
       month(m) AS stat_month_no,
       CASE WHEN s.stat_month IS NOT NULL THEN 1 ELSE 0 END AS has_snapshot
FROM (
  SELECT explode(sequence(to_date('2026-08-01'), to_date('2026-10-01'), interval 1 month)) AS m
) t
LEFT JOIN (SELECT DISTINCT stat_month FROM pgy_dw.ods_pgy_monthly_snapshot) s
  ON s.stat_month = date_format(t.m, 'yyyy-MM');
