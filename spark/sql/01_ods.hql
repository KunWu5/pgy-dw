-- 01_ods.hql  接入层：从 driver 端注册的临时视图落到 Hive Metastore 表（Parquet）
-- ODS 全部字段保持 STRING，不做类型转换

CREATE DATABASE IF NOT EXISTS pgy_dw;

DROP TABLE IF EXISTS pgy_dw.ods_pgy_monthly_snapshot;
CREATE TABLE pgy_dw.ods_pgy_monthly_snapshot USING parquet AS SELECT * FROM v_ods_snapshot;

DROP TABLE IF EXISTS pgy_dw.ods_blogger_list;
CREATE TABLE pgy_dw.ods_blogger_list USING parquet AS SELECT * FROM v_blogger_list;

DROP TABLE IF EXISTS pgy_dw.ods_crawl_run;
CREATE TABLE pgy_dw.ods_crawl_run USING parquet AS SELECT * FROM v_crawl_run;

DROP TABLE IF EXISTS pgy_dw.ods_mysql_snapshot;
CREATE TABLE pgy_dw.ods_mysql_snapshot USING parquet AS SELECT * FROM v_mysql_snapshot;
