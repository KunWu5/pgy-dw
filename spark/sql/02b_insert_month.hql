-- 02b_insert_month.hql  按 stat_month 静态分区覆盖（Hive 语义）
-- ${MONTH} 由 run_stage.py 按月替换，重跑单月即可补数，不影响其他分区

INSERT OVERWRITE TABLE pgy_dw.dwd_blogger_monthly_snapshot PARTITION (stat_month='${MONTH}')
SELECT
  c.red_id, c.nickname, c.tags_norm,
  c.fans_count, c.image_price, c.video_price,
  c.note_count_30d, c.exposure_median_30d, c.read_median_30d, c.interaction_median_30d,
  c.note_count_90d, c.exposure_median_90d, c.read_median_90d, c.interaction_median_90d,
  ROUND(c.image_price / NULLIF(c.interaction_median_30d, 0), 2) AS cpe_30d,
  ROUND(c.image_price / NULLIF(c.interaction_median_90d, 0), 2) AS cpe_90d,
  ROUND(c.interaction_median_30d / NULLIF(c.exposure_median_30d, 0), 4) AS engagement_rate_30d,
  ROUND(c.image_price / NULLIF(c.fans_count, 0) * 10000, 2) AS price_per_10k_fans,
  c.crawled_at, c.source_file
FROM (
  SELECT
    TRIM(r.red_id) AS red_id, r.nickname,
    array_join(array_sort(array_distinct(split(
      regexp_replace(regexp_replace(
        replace(replace(replace(COALESCE(r.tags_raw, ''), ', ', '|'), ',', '|'), '，', '|'),
        '[|]+', '|'), '^[|]|[|]$', ''), '[|]'))), '|') AS tags_norm,
    CAST(NULLIF(r.fans_count, '') AS BIGINT) AS fans_count,
    CAST(NULLIF(r.image_price, '') AS DOUBLE) AS image_price,
    CAST(NULLIF(r.video_price, '') AS DOUBLE) AS video_price,
    CAST(NULLIF(r.note_count_30d, '') AS INT) AS note_count_30d,
    CAST(NULLIF(r.exposure_median_30d, '') AS BIGINT) AS exposure_median_30d,
    CAST(NULLIF(r.read_median_30d, '') AS BIGINT) AS read_median_30d,
    CAST(NULLIF(r.interaction_median_30d, '') AS BIGINT) AS interaction_median_30d,
    CAST(NULLIF(r.note_count_90d, '') AS INT) AS note_count_90d,
    CAST(NULLIF(r.exposure_median_90d, '') AS BIGINT) AS exposure_median_90d,
    CAST(NULLIF(r.read_median_90d, '') AS BIGINT) AS read_median_90d,
    CAST(NULLIF(r.interaction_median_90d, '') AS BIGINT) AS interaction_median_90d,
    r.crawled_at, r.source_file
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY red_id ORDER BY crawled_at DESC) AS rn
    FROM pgy_dw.ods_pgy_monthly_snapshot
    WHERE stat_month = '${MONTH}' AND red_id IS NOT NULL AND TRIM(red_id) <> ''
  ) r
  WHERE r.rn = 1 AND LOWER(COALESCE(r.crawl_status, '')) = 'success'
) c;
