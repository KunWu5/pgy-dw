-- 02c_post_load.hql  依赖 DWD 数据加载完成后构建的维度与明细表

-- SCD Type 2：标签已规范化，属性组合变化才切版本，用 LEAD 生成有效期
DROP TABLE IF EXISTS pgy_dw.dim_blogger_scd;
CREATE TABLE pgy_dw.dim_blogger_scd USING parquet AS
WITH grp AS (
  SELECT red_id, nickname, tags_norm, MIN(stat_month) AS valid_from_month
  FROM pgy_dw.dwd_blogger_monthly_snapshot
  GROUP BY red_id, nickname, tags_norm
), scd AS (
  SELECT *, LEAD(valid_from_month) OVER (PARTITION BY red_id ORDER BY valid_from_month) AS next_from
  FROM grp
)
SELECT red_id, nickname, tags_norm, valid_from_month,
       CASE WHEN next_from IS NULL THEN '9999-12'
            ELSE date_format(add_months(to_date(concat(next_from, '-01')), -1), 'yyyy-MM') END AS valid_to_month,
       CASE WHEN next_from IS NULL THEN 1 ELSE 0 END AS is_current
FROM scd;

-- 失败明细单独留存，不污染事实表
DROP TABLE IF EXISTS pgy_dw.dwd_failed_record;
CREATE TABLE pgy_dw.dwd_failed_record USING parquet AS
SELECT stat_month, red_id, nickname, crawl_status, error_message, crawled_at, source_file
FROM pgy_dw.ods_pgy_monthly_snapshot
WHERE LOWER(COALESCE(crawl_status, '')) <> 'success';

-- 批次事实：每月采集成功 / 失败汇总
DROP TABLE IF EXISTS pgy_dw.dwd_crawl_run;
CREATE TABLE pgy_dw.dwd_crawl_run USING parquet AS
SELECT stat_month AS run_month,
       COUNT(*) AS total_count,
       SUM(CASE WHEN LOWER(COALESCE(crawl_status, '')) = 'success' THEN 1 ELSE 0 END) AS success_count,
       SUM(CASE WHEN LOWER(COALESCE(crawl_status, '')) <> 'success' THEN 1 ELSE 0 END) AS failed_count,
       ROUND(SUM(CASE WHEN LOWER(COALESCE(crawl_status, '')) = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM pgy_dw.ods_pgy_monthly_snapshot
GROUP BY stat_month;

-- 多值标签桥接：explode 拉平后按 (月份, 博主, 标签) 去重
DROP TABLE IF EXISTS pgy_dw.dwd_blogger_tag_bridge;
CREATE TABLE pgy_dw.dwd_blogger_tag_bridge USING parquet AS
SELECT DISTINCT stat_month, red_id, TRIM(tag) AS tag
FROM pgy_dw.dwd_blogger_monthly_snapshot
LATERAL VIEW explode(split(tags_norm, '[|]')) e AS tag
WHERE TRIM(tag) <> '';
