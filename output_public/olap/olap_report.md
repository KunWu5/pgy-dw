# DuckDB OLAP 层报告

- DuckDB 2.0.0；数据 122 行（来自 ADS 价值宽表 CSV）
- 物化列存表 `ads`；聚合查询、窗口排名、Parquet 直查均已执行

## 1. 聚合结果（DuckDB SQL）

| blogger_tier | blogger_cnt | avg_fans | avg_price | avg_cpe |
|---|---|---|---|---|
| 尾部 | 116 | 32,915 | 3,320.37 | 6.88 |
| 腰部 | 6 | 493,582 | 55,856.67 | 9.44 |

## 2. 窗口排名（每分层 TOP 5）

| red_id | nickname | blogger_tier | fans_count | tier_rank |
|---|---|---|---|---|
| r_4fb04a1d | -19doublew | 腰部 | 793385 | 1 |
| r_dc9782f3 | 美美沐 | 尾部 | 158058 | 4 |
| r_a7e49838 | 一达芙Yi | 腰部 | 773729 | 2 |
| r_91fdca62 | 婧威vivi | 尾部 | 289308 | 1 |
| r_5f31d231 | 开心一点啦 | 腰部 | 405407 | 3 |
| r_69bd8bdb | 蠢柚 | 腰部 | 364533 | 4 |
| r_ee4f843b | 安知夏Li | 尾部 | 148305 | 5 |
| r_ed7329ea | 叫我小q | 尾部 | 189339 | 2 |
| r_d8790877 | 一颗枣枣 | 腰部 | 312795 | 5 |
| r_9fb2d7c2 | 依依嘟嘟_11 | 尾部 | 169921 | 3 |

## 3. Parquet 直查

- Spark 写出的 Parquet 文件 1 个；DuckDB 直查行数 = 122
- 说明同一份 Parquet 可跨引擎读取，不需要重复导出。

## 4. DuckDB vs pandas 聚合耗时（5 次取最优）

| engine | best_ms | median_ms | runs_ms |
|---|---|---|---|
| DuckDB (列存 SQL) | 3.727 | 4.144 | [3.727, 4.604, 4.79, 4.144, 4.049] |
| pandas groupby | 2.977 | 3.244 | [4.463, 3.238, 3.244, 3.598, 2.977] |

- 结果一致性：avg_price 对账 一致；DuckDB 3.727 ms vs pandas 2.977 ms（比值 0.8×）。
- **122 行小数据下 DuckDB 并不比 pandas 快**：两者都受解释器与调用开销主导，
  这个数字只用于说明两条执行路径结果等价，不能当作 OLAP 性能结论。

## 5. 132 万行 Parquet 上的列存聚合

- 复用 `spark/bench` 压测数据：44 个 Parquet 文件、1,320,000 行，按 metric_key 聚合为 11 组。
- DuckDB 直查最优耗时 14.625 ms（3 次取最优），结果见 `olap_scale_agg.csv`。
- 这是「列存 + 向量化聚合」在真实规模下的验证，比 122 行的对比更有参考价值。

## 6. 诚实边界

- DuckDB 是嵌入式 OLAP 引擎，**不是** Doris / ClickHouse / StarRocks 这类分布式服务；
  没有分布式表、副本、分片、集群部署运维。
- 本项目证明的是 OLAP 建模、列存聚合、窗口 SQL、Parquet 互通与计时方法；
  若 JD 要求 Doris/ClickHouse 部署经验，仍需单独补。
