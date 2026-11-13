-- 03_dws.hql  汇总层：宽表转长表 + 窗口函数环比

DROP TABLE IF EXISTS pgy_dw.dwd_blogger_metric_long;
CREATE TABLE pgy_dw.dwd_blogger_metric_long USING parquet AS
SELECT stat_month, red_id, 'fans_count' AS metric_key, CAST(fans_count AS DOUBLE) AS metric_value FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'image_price', CAST(image_price AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'video_price', CAST(video_price AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'note_count_30d', CAST(note_count_30d AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'exposure_median_30d', CAST(exposure_median_30d AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'read_median_30d', CAST(read_median_30d AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'interaction_median_30d', CAST(interaction_median_30d AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'note_count_90d', CAST(note_count_90d AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'exposure_median_90d', CAST(exposure_median_90d AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'read_median_90d', CAST(read_median_90d AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot
UNION ALL SELECT stat_month, red_id, 'interaction_median_90d', CAST(interaction_median_90d AS DOUBLE) FROM pgy_dw.dwd_blogger_monthly_snapshot;

DROP TABLE IF EXISTS pgy_dw.dws_metric_monthly_change;
CREATE TABLE pgy_dw.dws_metric_monthly_change USING parquet AS
SELECT stat_month, prev_month, red_id, metric_key, metric_value, prev_value,
       CASE WHEN prev_value IS NULL THEN NULL ELSE metric_value - prev_value END AS delta,
       CASE WHEN prev_value IS NULL OR prev_value = 0 THEN NULL
            ELSE ROUND((metric_value - prev_value) / ABS(prev_value) * 100, 2) END AS change_percent,
       CASE WHEN prev_value IS NULL OR prev_value = 0 THEN 0
            WHEN ABS((metric_value - prev_value) / prev_value * 100) >= 30 THEN 1 ELSE 0 END AS is_alert,
       CASE WHEN prev_value IS NULL THEN '无上月快照'
            WHEN prev_value = 0 THEN '上月为0'
            WHEN ABS((metric_value - prev_value) / prev_value * 100) >= 30 THEN '异常波动'
            ELSE '正常' END AS status
FROM (
  SELECT l.stat_month, l.red_id, l.metric_key, l.metric_value,
         LAG(l.stat_month) OVER (PARTITION BY l.red_id, l.metric_key ORDER BY l.stat_month) AS prev_month,
         LAG(l.metric_value) OVER (PARTITION BY l.red_id, l.metric_key ORDER BY l.stat_month) AS prev_value
  FROM pgy_dw.dwd_blogger_metric_long l
) b;

DROP TABLE IF EXISTS pgy_dw.dws_platform_monthly;
CREATE TABLE pgy_dw.dws_platform_monthly USING parquet AS
SELECT stat_month, COUNT(*) AS blogger_cnt, SUM(fans_count) AS total_fans,
       ROUND(AVG(image_price), 2) AS avg_image_price,
       ROUND(AVG(video_price), 2) AS avg_video_price,
       ROUND(AVG(cpe_30d), 2) AS avg_cpe_30d,
       SUM(note_count_30d) AS total_notes_30d
FROM pgy_dw.dwd_blogger_monthly_snapshot GROUP BY stat_month;

DROP TABLE IF EXISTS pgy_dw.dws_tag_monthly;
CREATE TABLE pgy_dw.dws_tag_monthly USING parquet AS
SELECT b.stat_month, b.tag, COUNT(DISTINCT b.red_id) AS blogger_cnt,
       ROUND(AVG(d.fans_count), 0) AS avg_fans,
       ROUND(AVG(d.image_price), 2) AS avg_image_price,
       ROUND(AVG(d.cpe_30d), 2) AS avg_cpe_30d
FROM pgy_dw.dwd_blogger_tag_bridge b
JOIN pgy_dw.dwd_blogger_monthly_snapshot d
  ON d.stat_month = b.stat_month AND d.red_id = b.red_id
GROUP BY b.stat_month, b.tag;
