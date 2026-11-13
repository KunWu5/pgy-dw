# Spark Structured Streaming 实时管道报告

- 运行模式：Spark Structured Streaming（微批，2s 触发）
- Source：文件流 C:\Temp\pgy_stream_in（producer 分 5 批写入 1,000 行，模拟采集端持续落盘）
- 处理：清洗 → 1 分钟水位线 → **10 秒滚动窗口 + 1 分钟水位线** 按标签聚合
- Sink：Parquet（窗口结果 392 行）+ 死信队列（70 行）+ memory 流表（1100 行）
- Checkpoint：C:\Temp\pgy_stream_ckpt（保证失败重跑不重复消费）

## 分两路的原因

- **有效流**：粉丝数非法、标签为空、时间解析失败的记录直接过滤掉，不进入聚合；
- **死信流**：脏数据单独落 `dead_letter`，对应「实时数据质量监控」——线上一般是告警 + 落表待修，不是直接丢弃。

## 与批处理的差异（可讲的部分）

1. 窗口 + 水位线解决乱序与迟到数据；批处理靠分区快照天然有序。
2. Checkpoint 记录 offset 与状态，等价于批处理的「幂等重跑」，但粒度是微批。
3. 内存流表（`rate_events`）可以直接用 SQL 查询增量结果，是流批一体的典型用法。

## 诚实边界

- 这是 **Spark 微批**，不是 Flink 的原生流；没有事件时间语义之外的低延迟保证，也没有 Kafka 做缓冲。
- 未做状态后端调优、背压、Exactly-Once 端到端验证（Parquet sink 本身是幂等的，但没有两阶段提交）。
- 文件流 source 用轮询实现，仅用于本机验证；生产一般用 Kafka。
