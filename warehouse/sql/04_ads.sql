-- 04_ads.sql  应用层：直接给报表 / BI / 业务使用

-- 达人商业价值宽表（最新月）：当前值 + 上期值 + 环比 + 分层标签
DROP TABLE IF EXISTS ads_blogger_value_wide;
CREATE TABLE ads_blogger_value_wide AS
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
FROM dwd_blogger_monthly_snapshot d
LEFT JOIN dws_metric_monthly_change cf
       ON cf.stat_month = d.stat_month AND cf.red_id = d.red_id AND cf.metric_key = 'fans_count'
LEFT JOIN dws_metric_monthly_change cp
       ON cp.stat_month = d.stat_month AND cp.red_id = d.red_id AND cp.metric_key = 'image_price'
LEFT JOIN dws_metric_monthly_change ci
       ON ci.stat_month = d.stat_month AND ci.red_id = d.red_id AND ci.metric_key = 'interaction_median_30d'
LEFT JOIN (
  SELECT red_id, NTILE(3) OVER (ORDER BY cpe_30d) AS value_tier
  FROM dwd_blogger_monthly_snapshot
  WHERE stat_month = (SELECT MAX(stat_month) FROM dwd_blogger_monthly_snapshot)
    AND cpe_30d IS NOT NULL AND cpe_30d > 0
) t ON t.red_id = d.red_id
WHERE d.stat_month = (SELECT MAX(stat_month) FROM dwd_blogger_monthly_snapshot);

-- 异常波动清单：11 项指标中环比绝对值 >= 30% 的记录
DROP TABLE IF EXISTS ads_metric_alert;
CREATE TABLE ads_metric_alert AS
SELECT c.stat_month, c.prev_month, c.red_id, c.metric_key, m.metric_label, m.unit,
       c.metric_value, c.prev_value, c.delta, c.change_percent
FROM dws_metric_monthly_change c
JOIN dim_metric m ON m.metric_key = c.metric_key
WHERE c.is_alert = 1;

-- 名单增删追踪：新增 / 流失博主（对应原采集系统的 inactive 软删除语义）
DROP TABLE IF EXISTS ads_blogger_churn;
CREATE TABLE ads_blogger_churn (
  to_month TEXT, from_month TEXT, red_id TEXT, change_type TEXT
);
WITH months AS (
  SELECT DISTINCT stat_month FROM dwd_blogger_monthly_snapshot
), pairs AS (
  SELECT stat_month AS cur_month,
         LAG(stat_month) OVER (ORDER BY stat_month) AS prev_month
  FROM months
)
INSERT INTO ads_blogger_churn (to_month, from_month, red_id, change_type)
SELECT p.cur_month, p.prev_month, cur.red_id, 'new'
FROM pairs p
JOIN dwd_blogger_monthly_snapshot cur ON cur.stat_month = p.cur_month
WHERE p.prev_month IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM dwd_blogger_monthly_snapshot prev
    WHERE prev.stat_month = p.prev_month AND prev.red_id = cur.red_id
  )
UNION ALL
SELECT p.cur_month, p.prev_month, prev.red_id, 'lost'
FROM pairs p
JOIN dwd_blogger_monthly_snapshot prev ON prev.stat_month = p.prev_month
WHERE p.prev_month IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM dwd_blogger_monthly_snapshot cur
    WHERE cur.stat_month = p.cur_month AND cur.red_id = prev.red_id
  );

-- 数据质量结果表：由 warehouse/quality.py 写入
DROP TABLE IF EXISTS ads_dq_summary;
CREATE TABLE ads_dq_summary (
  check_name TEXT, rule TEXT, actual REAL, expected TEXT,
  passed INTEGER, detail TEXT, checked_at TEXT
);
