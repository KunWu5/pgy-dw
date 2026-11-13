-- 02b_scd.sql  缓慢变化维 SCD Type 2
-- 依赖：DWD 已生成，且 tags_norm 已由 Python 规范化（排序去重），否则标签顺序变化会误切版本

DROP TABLE IF EXISTS dim_blogger_scd;
CREATE TABLE dim_blogger_scd (
  red_id TEXT, nickname TEXT, tags_norm TEXT,
  valid_from_month TEXT, valid_to_month TEXT, is_current INTEGER
);

WITH grp AS (
  SELECT red_id, nickname, tags_norm, MIN(stat_month) AS valid_from_month
  FROM dwd_blogger_monthly_snapshot GROUP BY red_id, nickname, tags_norm
), scd AS (
  SELECT *, LEAD(valid_from_month) OVER (PARTITION BY red_id ORDER BY valid_from_month) AS next_from
  FROM grp
)
INSERT INTO dim_blogger_scd (red_id, nickname, tags_norm, valid_from_month, valid_to_month, is_current)
SELECT red_id, nickname, tags_norm, valid_from_month,
       CASE WHEN next_from IS NULL THEN '9999-12'
            ELSE strftime('%Y-%m', date(next_from || '-01', '-1 month')) END,
       CASE WHEN next_from IS NULL THEN 1 ELSE 0 END
FROM scd;
