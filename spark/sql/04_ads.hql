-- 04_ads.hql  应用层

DROP TABLE IF EXISTS pgy_dw.ads_blogger_value_wide;
CREATE TABLE pgy_dw.ads_blogger_value_wide USING parquet AS
SELECT d.stat_month, d.red_id, d.nickname, d.tags_norm,
       d.fans_count, d.image_price, d.video_price,
       d.note_count_30d, d.exposure_median_30d, d.read_median_30d, d.interaction_median_30d,
       d.note_count_90d, d.interaction_median_90d,
       d.cpe_30d, d.cpe_90d, d.engagement_rate_30d, d.price_per_10k_fans,
       cf.prev_value AS prev_fans_count, cf.change_percent AS fans_change_pct,
       cp.prev_value AS prev_image_price, cp.change_percent AS price_change_pct,
       ci.prev_value AS prev_interaction_30d, ci.change_percent AS interaction_change_pct,
       CASE WHEN d.fans_count >= 1000000 THEN '头部'
            WHEN d.fans_count >= 300000 THEN '腰部' ELSE '尾部' END AS blogger_tier,
       CASE t.value_tier WHEN 1 THEN '高性价比' WHEN 2 THEN '中等'
            WHEN 3 THEN '成本偏高' ELSE '无数据' END AS cpe_tier
FROM pgy_dw.dwd_blogger_monthly_snapshot d
LEFT JOIN pgy_dw.dws_metric_monthly_change cf
       ON cf.stat_month = d.stat_month AND cf.red_id = d.red_id AND cf.metric_key = 'fans_count'
LEFT JOIN pgy_dw.dws_metric_monthly_change cp
       ON cp.stat_month = d.stat_month AND cp.red_id = d.red_id AND cp.metric_key = 'image_price'
LEFT JOIN pgy_dw.dws_metric_monthly_change ci
       ON ci.stat_month = d.stat_month AND ci.red_id = d.red_id AND ci.metric_key = 'interaction_median_30d'
LEFT JOIN (
  SELECT red_id, NTILE(3) OVER (ORDER BY cpe_30d) AS value_tier
  FROM pgy_dw.dwd_blogger_monthly_snapshot
  WHERE stat_month = (SELECT MAX(stat_month) FROM pgy_dw.dwd_blogger_monthly_snapshot)
    AND cpe_30d IS NOT NULL AND cpe_30d > 0
) t ON t.red_id = d.red_id
WHERE d.stat_month = (SELECT MAX(stat_month) FROM pgy_dw.dwd_blogger_monthly_snapshot);

DROP TABLE IF EXISTS pgy_dw.ads_metric_alert;
CREATE TABLE pgy_dw.ads_metric_alert USING parquet AS
SELECT c.stat_month, c.prev_month, c.red_id, c.metric_key, m.metric_label, m.unit,
       c.metric_value, c.prev_value, c.delta, c.change_percent
FROM pgy_dw.dws_metric_monthly_change c
JOIN pgy_dw.dim_metric m ON m.metric_key = c.metric_key
WHERE c.is_alert = 1;
