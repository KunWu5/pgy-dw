# Apache Iceberg 湖表实践报告

- 运行环境：Spark 4.0.1 + `iceberg-spark-runtime-4.0_2.13-1.12.0.jar`，Hadoop catalog，仓库 `C:\Temp\pgy_iceberg_warehouse`
- 表：`lake.pgy.ads_blogger_value`，122 行，按 `stat_month` + `bucket(8, red_id)` 隐藏分区
- 同一份数据另建一张 Hive/Parquet 表做对照，两者互不改写

## 1. 快照（ACID 版本）

| 快照 | 操作 | 新增记录 | 删除记录 | 新增文件 | 删除文件 |
|---|---|---:|---:|---:|---:|
| 106794598419492420 | append | 122 |  | 8 |  |
| 1198050979149091838 | overwrite | 48 | 48 | 3 | 3 |

## 2. 行级更新：MERGE 按文件粒度重写

用 `MERGE INTO` 把 3 行的 `cpe_tier` 改成「人工复核」：本次提交 added-records=48、deleted-records=48，耗时 2.78s。

建表时按 `bucket(8, red_id)` 分桶，落成 **8** 个数据文件，MERGE 只重写了命中的文件（48/122 行），不是整表。

**必须讲清的细节**：Iceberg 的 MERGE 是**文件级重写**，不是行级原地更新。如果整张表只有一个数据文件，改 1 行也会重写整个文件——这正是生产上要做 `rewrite_data_files` 合并小文件、并合理设置分区/分桶的原因。

对照：同样更新 3 行，Hive/Parquet 表用 `INSERT OVERWRITE ... PARTITION` 必须重写分区内全部 **122 行**、耗时 0.41s。

> 122 行数据下两个耗时都在亚秒级，不构成性能结论；真正的差异是**写入语义**：Hive 整分区重写，Iceberg 是文件级增量 + 快照。

## 3. 时间旅行

| 版本 | 行数 | 粉丝总数 | 已复核行数 |
|---|---:|---:|---:|
| v1(建表时) | 122 | 6779574 | 0 |
| v_now(当前) | 122 | 6779574 | 3 |

- 用 `VERSION AS OF 106794598419492420` 查到更新前的数据，说明历史快照可回溯；
- 生产上可做误操作回滚、审计比对、按时间点复现报表。

## 4. Schema 演进

- `ALTER TABLE ADD COLUMN data_quality_tag STRING`：成功
- 加列后历史数据该列为 NULL 的行数：122（等于 122 说明没有重写历史文件）

## 5. 分区演进（隐藏分区）

- 追加 `truncate(4, red_id)` 分区字段：成功
- Iceberg 的分区是「隐藏分区」，查询不需要写分区列，由引擎自动裁剪；新增分区字段不会移动旧数据，只会影响后续写入。

## 6. 元数据表

- `lake.pgy.ads_blogger_value.snapshots` / `.files` / `.history` / `.manifests` 可直接用 SQL 查询；
- 当前数据文件 **8** 个，可在 `files.csv` 看到每个文件的行数与大小。

## 7. 与 Hive/Parquet 的取舍

| 维度 | Hive + Parquet（现有 19 张表） | Iceberg（本模块） |
|---|---|---|
| 写入语义 | 分区覆盖，整分区重写 | 文件级增量 + 快照 |
| 行级更新 | 需重写分区 | `MERGE INTO` |
| 历史版本 | 无（靠自己留快照表） | 原生快照 + 时间旅行 |
| Schema 演进 | 加列需重写或改元数据 | `ADD COLUMN` 平滑加列 |
| 小文件问题 | 需自己合并 | `rewrite_data_files` 可治理 |
| 引擎依赖 | Hive Metastore | 需 Iceberg runtime |

结论：**分层结果仍用 Hive/Parquet**（简单、生态广）；**需要更新与回溯的分析层用 Iceberg**。两者不是替代关系。

## 8. 诚实边界

- 单机 Hadoop catalog + 本地文件系统，**不是**对象存储 / Hive Metastore / REST catalog 的生产部署；
- 未做并发写冲突、快照过期清理（`expire_snapshots`）、小文件合并（`rewrite_data_files`）；
- 数据量 122 行，性能数字只用于说明写入语义差异，不能当基准测试。
