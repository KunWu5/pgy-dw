-- 03_dws.sql  轻度汇总层
-- 宽表转长表（UNPIVOT）后用 LAG 窗口函数统一算 11 项指标环比，替代原 Excel 报告逻辑

DROP TABLE IF EXISTS dwd_blogger_metric_long;
CREATE TABLE dwd_blogger_metric_long (
  stat_month TEXT, red_id TEXT, metric_key TEXT, metric_value REAL
);
INSERT INTO dwd_blogger_metric_long (stat_month, red_id, metric_key, metric_value)
SELECT stat_month, red_id, 'fans_count', CAST(fans_count AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'image_price', CAST(image_price AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'video_price', CAST(video_price AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'note_count_30d', CAST(note_count_30d AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'exposure_median_30d', CAST(exposure_median_30d AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'read_median_30d', CAST(read_median_30d AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'interaction_median_30d', CAST(interaction_median_30d AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'note_count_90d', CAST(note_count_90d AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'exposure_median_90d', CAST(exposure_median_90d AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'read_median_90d', CAST(read_median_90d AS REAL) FROM dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'interaction_median_90d', CAST(interaction_median_90d AS REAL) FROM dwd_blogger_monthly_snapshot;
CREATE INDEX IF NOT EXISTS idx_metric_long ON dwd_blogger_metric_long(red_id, metric_key, stat_month);

DROP TABLE IF EXISTS dws_metric_monthly_change;
CREATE TABLE dws_metric_monthly_change (
  stat_month TEXT, prev_month TEXT, red_id TEXT, metric_key TEXT,
  metric_value REAL, prev_value REAL, delta REAL, change_percent REAL,
  is_alert INTEGER, status TEXT
);

WITH base AS (
  SELECT l.stat_month, l.red_id, l.metric_key, l.metric_value,
         LAG(l.stat_month) OVER w AS prev_month,
         LAG(l.metric_value) OVER w AS prev_value
  FROM dwd_blogger_metric_long l
  WINDOW w AS (PARTITION BY l.red_id, l.metric_key ORDER BY l.stat_month)
)
INSERT INTO dws_metric_monthly_change (
  stat_month, prev_month, red_id, metric_key, metric_value, prev_value,
  delta, change_percent, is_alert, status
)
SELECT stat_month, prev_month, red_id, metric_key, metric_value, prev_value,
       CASE WHEN prev_value IS NULL THEN NULL ELSE metric_value - prev_value END,
       CASE WHEN prev_value IS NULL OR prev_value = 0 THEN NULL
            ELSE ROUND((metric_value - prev_value) / ABS(prev_value) * 100, 2) END,
       CASE WHEN prev_value IS NULL OR prev_value = 0 THEN 0
            WHEN ABS((metric_value - prev_value) / prev_value * 100) >= 30 THEN 1 ELSE 0 END,
       CASE WHEN prev_value IS NULL THEN '无上月快照'
            WHEN prev_value = 0 THEN '上月为0'
            WHEN ABS((metric_value - prev_value) / prev_value * 100) >= 30 THEN '异常波动'
            ELSE '正常' END
FROM base;

-- 平台整体月度汇总
DROP TABLE IF EXISTS dws_platform_monthly;
CREATE TABLE dws_platform_monthly AS
SELECT stat_month,
       COUNT(*) AS blogger_cnt,
       SUM(fans_count) AS total_fans,
       ROUND(AVG(image_price), 2) AS avg_image_price,
       ROUND(AVG(video_price), 2) AS avg_video_price,
       ROUND(AVG(cpe_30d), 2) AS avg_cpe_30d,
       ROUND(AVG(interaction_median_30d), 0) AS avg_interaction_30d,
       SUM(note_count_30d) AS total_notes_30d
FROM dwd_blogger_monthly_snapshot
GROUP BY stat_month;

-- 标签粒度汇总（多值维度通过桥接表聚合，不重复计数）
DROP TABLE IF EXISTS dws_tag_monthly;
CREATE TABLE dws_tag_monthly AS
SELECT b.stat_month, b.tag,
       COUNT(DISTINCT b.red_id) AS blogger_cnt,
       ROUND(AVG(d.fans_count), 0) AS avg_fans,
       ROUND(AVG(d.image_price), 2) AS avg_image_price,
       ROUND(AVG(d.interaction_median_30d), 0) AS avg_interaction_30d,
       ROUND(AVG(d.cpe_30d), 2) AS avg_cpe_30d
FROM dwd_blogger_tag_bridge b
JOIN dwd_blogger_monthly_snapshot d
  ON d.stat_month = b.stat_month AND d.red_id = b.red_id
GROUP BY b.stat_month, b.tag;
