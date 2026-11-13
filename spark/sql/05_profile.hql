-- 05_profile.hql  标签体系与达人价值画像（对应 JD：标签 / 用户画像建设）
-- 只新增 2 张表，不改动已有 19 张表；基于真实 218 行快照，口径全部可追溯。
--
-- 性能边界：value_score 的 PERCENT_RANK 是全局窗口，Spark 会把数据收进单分区。
-- 这里只作用于最新月 122 行的 ADS 结果集，成本可忽略；若要放大到百万行，
-- 应改为 percentile_approx 分桶 + CASE 打分，避免全局 shuffle（与 bench 的 shuffle 调参结论一致）。

DROP TABLE IF EXISTS pgy_dw.dws_blogger_profile;
CREATE TABLE pgy_dw.dws_blogger_profile USING parquet AS
WITH tag_agg AS (
  SELECT stat_month, red_id, COUNT(DISTINCT tag) AS tag_cnt,
         concat_ws('|', slice(array_sort(collect_set(tag)), 1, 3)) AS top_tags
  FROM pgy_dw.dwd_blogger_tag_bridge
  GROUP BY stat_month, red_id
), primary_tag AS (
  SELECT stat_month, red_id, tag AS primary_tag FROM (
    SELECT stat_month, red_id, tag,
           ROW_NUMBER() OVER (PARTITION BY stat_month, red_id ORDER BY tag) AS rn
    FROM pgy_dw.dwd_blogger_tag_bridge
  ) x WHERE rn = 1
), activity AS (
  SELECT red_id, COUNT(DISTINCT stat_month) AS active_month_cnt,
         MIN(stat_month) AS first_seen_month, MAX(stat_month) AS last_seen_month
  FROM pgy_dw.dwd_blogger_monthly_snapshot GROUP BY red_id
), scored AS (
  SELECT a.stat_month, a.red_id, a.nickname, a.fans_count, a.image_price,
         a.cpe_30d, a.engagement_rate_30d, a.blogger_tier, a.cpe_tier,
         a.fans_change_pct, a.price_change_pct,
         COALESCE(t.tag_cnt, 0) AS tag_cnt, t.top_tags, p.primary_tag,
         COALESCE(v.active_month_cnt, 0) AS active_month_cnt,
         v.first_seen_month, v.last_seen_month, b.status AS list_status,
         ROUND((1 - PERCENT_RANK() OVER (ORDER BY COALESCE(a.cpe_30d, 9999))) * 50
               + PERCENT_RANK() OVER (ORDER BY COALESCE(a.engagement_rate_30d, 0)) * 30
               + PERCENT_RANK() OVER (ORDER BY COALESCE(a.fans_count, 0)) * 20, 1) AS value_score
  FROM pgy_dw.ads_blogger_value_wide a
  LEFT JOIN tag_agg t ON t.stat_month = a.stat_month AND t.red_id = a.red_id
  LEFT JOIN primary_tag p ON p.stat_month = a.stat_month AND p.red_id = a.red_id
  LEFT JOIN activity v ON v.red_id = a.red_id
  LEFT JOIN pgy_dw.dim_blogger b ON b.red_id = a.red_id
)
SELECT stat_month, red_id, nickname, primary_tag, top_tags, tag_cnt,
       blogger_tier, cpe_tier,
       CASE WHEN engagement_rate_30d >= 0.05 THEN '高互动'
            WHEN engagement_rate_30d >= 0.02 THEN '中互动' ELSE '低互动' END AS engagement_tier,
       active_month_cnt, first_seen_month, last_seen_month, list_status,
       fans_count, image_price, cpe_30d, engagement_rate_30d,
       fans_change_pct, price_change_pct, value_score,
       CASE WHEN value_score >= 70 THEN '高价值'
            WHEN value_score >= 40 THEN '潜力'
            ELSE '待观察' END AS profile_segment
FROM scored;

DROP TABLE IF EXISTS pgy_dw.ads_tag_profile;
CREATE TABLE pgy_dw.ads_tag_profile USING parquet AS
SELECT b.tag, COUNT(DISTINCT b.red_id) AS blogger_cnt,
       ROUND(AVG(p.value_score), 1) AS avg_value_score,
       SUM(CASE WHEN p.profile_segment = '高价值' THEN 1 ELSE 0 END) AS high_value_cnt,
       ROUND(AVG(d.cpe_30d), 2) AS avg_cpe_30d,
       ROUND(AVG(d.fans_count), 0) AS avg_fans
FROM pgy_dw.dwd_blogger_tag_bridge b
JOIN pgy_dw.dws_blogger_profile p
  ON p.stat_month = b.stat_month AND p.red_id = b.red_id
JOIN pgy_dw.dwd_blogger_monthly_snapshot d
  ON d.stat_month = b.stat_month AND d.red_id = b.red_id
GROUP BY b.tag
ORDER BY blogger_cnt DESC;
