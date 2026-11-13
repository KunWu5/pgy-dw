# Spark / Hive 规模压测报告

- 生成时间：2026-11-13T13:48:24
- 环境：Python + PySpark 4.0.1，`local[4]` 单机，内嵌 Derby Hive Metastore，Parquet，AQE 关闭
- 数据：**合成数据**（seed=20261113），5,000 达人 × 24 个月 × 11 指标；只用于验证分层 SQL 与调优在百万行量级成立，不是真实业务数据
- 隔离：独立库 `pgy_dw_bench`，不改动 `pgy_dw` 里任何表；简历中的 122 / 310 / 27 等真实数字不受影响

## 1. 数据规模

| 项目 | 数值 |
|---|---:|
| 事实表行数 | 120,000 |
| 指标长表行数 | 1,320,000 |
| 标签桥接表行数 | 241,416 |
| 环比结果行数 | 1,320,000 |
| Hive 分区数 | 24 |
| 事实表 Parquet 文件数 / 体积 | 96 / 10.76 MB |
| 压测库表数 | 9 |

> 复用 `spark/sql/03_dws.hql` 原文，仅把 `pgy_dw.` 替换成 `pgy_dw_bench.`。

## 2. 分区裁剪

| 场景 | 扫描文件数 | 扫描字节(MB) | 最优耗时(s) |
|---|---:|---:|---:|
| 全表扫描 | 96 | 10.76 | 0.217 |
| 单月过滤 `2025-01` | 4 | 0.45 | 0.175 |

- 执行计划里的分区过滤：`PartitionFilters: [isnotnull(stat_month#57), (stat_month#57 = 2025-01)]`
- **扫描量减少 24.0× / 23.9×**（96→4 个文件，10.76→0.45 MB）；耗时比 1.24×。
  数据量只有 10 MB 且全部命中 OS 页缓存，所以耗时差距远小于 I/O 差距——这正是分区裁剪的意义：
  省的是磁盘/网络 I/O，集群与冷数据下差距会放大。

## 3. 广播 join vs SortMergeJoin

| 场景 | 最优耗时(s) | 采样(s) |
|---|---:|---|
| SortMergeJoin（1,320,000 × 5,000） | 1.033 | [1.073, 1.033] |
| BroadcastHashJoin（广播 5,000 行维表） | 0.407 | [0.407, 0.454] |

- 计划校验：SMJ=True，Broadcast=True；提速 **2.54×**

## 4. shuffle 分区数调参

| partitions | 最优耗时(s) | 采样 |
|---:|---:|---|
| 1 | 0.254 | [0.254, 0.283] |
| 4 | 0.289 | [0.291, 0.289] |
| 16 | 0.375 | [0.423, 0.375] |
| 64 | 0.748 | [0.756, 0.748] |

- 本机 `local[4]` 下最优 = **1**（0.254s），最差 = 64（0.748s），**盲目调大反而慢 2.94×**
- AQE 关闭，计划可复现。结论：1.32M 行的聚合在 4 核单机上属轻量负载，分区数从 1 增到 64 只增加任务调度与 shuffle 落盘开销；并行度要按「数据量 ÷ 可用 core」估，不是越大越好。

## 5. 百万行量级数据质量校验

| 规则 | 期望 | 实际 | 结果 |
|---|---:|---:|---|
| 指标长表行数守恒（事实表 × 11 项指标） | 1,320,000 | 1,320,000 | PASS |
| DWD 主键唯一（(stat_month, red_id) 无重复） | 0 | 0 | PASS |
| 关键字段非空（red_id / stat_month 不为空） | 0 | 0 | PASS |
| 数值值域（粉丝数>0、报价与中位数>=0） | 0 | 0 | PASS |
| 11 项指标完整率（metric_value 无空值） | 0 | 0 | PASS |
| Hive 分区完整性（分区数 = 24） | 24 | 24 | PASS |

- **6/6 通过**；同一套分层 SQL 从 218 行放大到 132 万行，行数守恒与主键唯一性仍成立。

## 6. P0-b：DataFrame API 版 DWS

- 产物表：`pgy_dw_bench.dws_tag_monthly_df`（与 SQL 版 `pgy_dw_bench.dws_tag_monthly` 同口径）、`pgy_dw_bench.dws_tag_rank_df`（Window 排名）
- 行数：SQL 版 480，DataFrame 版 480
- 双向 `exceptAll` 差异：0 / 0；精确一致 = **True**
- 执行计划含广播 join = True；含 Window = True
- 用到的算子：`spark.table` / `select` / `join` / `functions.broadcast` / `groupBy.agg`（`countDistinct`、`avg`、`round`）/ `Window.partitionBy + row_number`

## 7. 简历可写的句子（可直接用）

- 「以 PySpark 4.0 + Hive Metastore + Parquet 分区构建 19 表分层数仓，并用 5,000 达人 × 24 月 × 11 指标 = 132 万行合成数据做规模压测，同一套分层 SQL 全量通过 6 项数据质量校验。」
- 「分区裁剪使扫描文件数 96→4、扫描量 10.76→0.45 MB；1,320,000 行事实与 5,000 行维表 JOIN 由 SortMergeJoin 改广播后提速 2.54×。」
- 「用 DataFrame API（Window / broadcast join / 聚合）重写 DWS 标签层，与 Spark SQL 版双向 exceptAll 差异为 0。」

面试追问时可讲的点：`EXPLAIN FORMATTED` 里的 `PartitionFilters`、广播阈值 `autoBroadcastJoinThreshold`、AQE 对计划的影响、为什么本机 64 分区反而比 1 分区慢（调度开销 > 并行收益）。

## 8. 诚实边界

- 合成数据、单机 `local[4]`、内嵌 Derby Metastore；不是 YARN/K8s 集群，不是 TB 级。
- 计时包含 JVM 调度与本地磁盘 I/O，取多次最优值，只能用于**同机相对比较**，不能当生产性能承诺。
- 简历可写「百万行级规模压测 / 分区裁剪 / 广播 join / shuffle 调参 / DataFrame API」，
  不要写「Spark 集群调优」或「TB 级数据」。

## 9. 复现

```bash
cd pgy_dw
python spark/bench/gen_data.py                       # 生成合成 CSV
python spark/bench/run_bench.py                      # 建表 + 全量压测
python spark/bench/run_bench.py --skip-gen --skip-build   # 复用表，只重测
python spark/bench/run_bench.py --report-only       # 只重排报告
```

可调环境变量：`PGY_BENCH_BLOGGERS`、`PGY_BENCH_MONTHS`、`PGY_BENCH_MASTER`、
`PGY_BENCH_STAGING`、`PGY_SPARK_WAREHOUSE`。
