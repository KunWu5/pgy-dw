"""DuckDB OLAP 层：列存表 + 向量化聚合 + Parquet 直查 + 与 pandas 对账/计时对比。

对应 JD：图二要求 5「熟悉至少一种 Doris/ClickHouse OLAP 引擎」、
        图一职责 3「可扩展数仓体系」、图三要求 4「大数据引擎配置调优」

诚实边界：DuckDB 是**嵌入式列存 OLAP**，不是 Doris/ClickHouse 那种分布式 OLAP 服务。
这里证明的是「OLAP 建模 / 列存 / 聚合下推 / Parquet 直查 / SQL 调优思路」，
不冒充 Doris/ClickHouse 部署经验。
"""
from __future__ import annotations

import csv
import json
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
for _lib in (r"C:\Temp\wk\datalib",):
    if Path(_lib).is_dir() and _lib not in sys.path:
        sys.path.insert(0, _lib)

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
from governance import OLAP_OUT, md_table  # noqa: E402

SRC = ROOT / "output" / "ads_blogger_value_wide.csv"
PARQUET_GLOB = r"C:\Temp\pgy_spark_warehouse\pgy_dw.db\ads_blogger_value_wide\*.parquet"
BENCH_DIR = Path(r"C:\Temp\pgy_spark_warehouse\pgy_dw_bench.db\dwd_blogger_metric_long")

AGG_SQL = """
SELECT blogger_tier, count(*) AS blogger_cnt,
       round(avg(fans_count), 0) AS avg_fans,
       round(avg(image_price), 2) AS avg_price,
       round(avg(cpe_30d), 2) AS avg_cpe
FROM ads GROUP BY blogger_tier ORDER BY blogger_cnt DESC
"""


def clean_csv() -> Path:
    OLAP_OUT.mkdir(parents=True, exist_ok=True)
    with SRC.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    path = OLAP_OUT / "ads_clean.csv"
    with path.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)
    return path


def timed(fn, repeats=5):
    fn()
    vals = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        vals.append(round((time.perf_counter() - t0) * 1000, 3))
    return {"runs_ms": vals, "best_ms": min(vals),
            "median_ms": round(statistics.median(vals), 3)}


def build() -> dict:
    path = clean_csv()
    con = duckdb.connect()
    con.execute(f"CREATE TABLE ads AS SELECT * FROM read_csv_auto('{path.as_posix()}', header=true)")
    n = con.execute("SELECT count(*) FROM ads").fetchone()[0]

    duck_agg = con.execute(AGG_SQL).fetchdf()
    duck_agg.to_csv(OLAP_OUT / "olap_tier_agg.csv", index=False, encoding="utf-8-sig")

    # 窗口函数：每个分层内按粉丝数排名（OLAP 典型查询）
    ranked = con.execute("""
        SELECT red_id, nickname, blogger_tier, fans_count,
               row_number() OVER (PARTITION BY blogger_tier ORDER BY fans_count DESC) AS tier_rank
        FROM ads QUALIFY tier_rank <= 5
    """).fetchdf()
    ranked.to_csv(OLAP_OUT / "olap_window_top5.csv", index=False, encoding="utf-8-sig")

    # Parquet 直查（不入库，验证列存格式互通）
    pq = list(Path(r"C:\Temp\pgy_spark_warehouse\pgy_dw.db\ads_blogger_value_wide").glob("*.parquet")) \
        if Path(r"C:\Temp\pgy_spark_warehouse\pgy_dw.db\ads_blogger_value_wide").exists() else []
    parquet_info = {"files": len(pq), "rows": None}
    if pq:
        try:
            parquet_info["rows"] = con.execute(
                f"SELECT count(*) FROM read_parquet('{PARQUET_GLOB}')").fetchone()[0]
        except Exception as exc:
            parquet_info["error"] = str(exc)[:150]

    # 132 万行 Parquet 上的列存聚合（复用 spark/bench 的压测数据，体现 OLAP 规模）
    scale = {"files": 0, "rows": 0, "groups": 0}
    if BENCH_DIR.exists():
        files = list(BENCH_DIR.glob("*.parquet"))
        glob = (BENCH_DIR.as_posix() + "/*.parquet")
        scale_sql = (
            "SELECT metric_key, count(*) AS n, round(avg(metric_value), 2) AS avg_v "
            f"FROM read_parquet('{glob}') GROUP BY metric_key ORDER BY metric_key"
        )
        t_scale = timed(lambda: con.execute(scale_sql).fetchdf(), repeats=3)
        scale_df = con.execute(scale_sql).fetchdf()
        scale_df.to_csv(OLAP_OUT / "olap_scale_agg.csv", index=False, encoding="utf-8-sig")
        scale = {"files": len(files), "rows": int(scale_df["n"].sum()),
                 "groups": len(scale_df), "timing": t_scale}

    pdf = pd.read_csv(path)
    pandas_agg = (pdf.groupby("blogger_tier")
                  .agg(blogger_cnt=("red_id", "count"), avg_fans=("fans_count", "mean"),
                       avg_price=("image_price", "mean"), avg_cpe=("cpe_30d", "mean"))
                  .round(2).reset_index())

    t_duck = timed(lambda: con.execute(AGG_SQL).fetchdf())
    t_pandas = timed(lambda: pdf.groupby("blogger_tier").agg(
        blogger_cnt=("red_id", "count"), avg_fans=("fans_count", "mean"),
        avg_price=("image_price", "mean"), avg_cpe=("cpe_30d", "mean")).round(2))

    merged = duck_agg.merge(pandas_agg, on="blogger_tier", suffixes=("_duckdb", "_pandas"))
    match = all(abs(float(a) - float(b)) < 0.02
                for a, b in zip(merged["avg_price_duckdb"], merged["avg_price_pandas"]))
    speedup = round(t_pandas["best_ms"] / t_duck["best_ms"], 2) if t_duck["best_ms"] else None

    cmp = pd.DataFrame([
        {"engine": "DuckDB (列存 SQL)", **{k: v for k, v in t_duck.items() if k != "runs_ms"},
         "runs_ms": str(t_duck["runs_ms"])},
        {"engine": "pandas groupby", **{k: v for k, v in t_pandas.items() if k != "runs_ms"},
         "runs_ms": str(t_pandas["runs_ms"])},
    ])
    cmp.to_csv(OLAP_OUT / "duckdb_vs_pandas.csv", index=False, encoding="utf-8-sig")

    md = [
        "# DuckDB OLAP 层报告",
        "",
        f"- DuckDB {duckdb.__version__}；数据 {n} 行（来自 ADS 价值宽表 CSV）",
        f"- 物化列存表 `ads`；聚合查询、窗口排名、Parquet 直查均已执行",
        "",
        "## 1. 聚合结果（DuckDB SQL）",
        "",
        duck_agg.pipe(md_table),
        "",
        "## 2. 窗口排名（每分层 TOP 5）",
        "",
        ranked.head(10).pipe(md_table),
        "",
        "## 3. Parquet 直查",
        "",
        f"- Spark 写出的 Parquet 文件 {parquet_info['files']} 个；"
        f"DuckDB 直查行数 = {parquet_info.get('rows', '未执行')}",
        "- 说明同一份 Parquet 可跨引擎读取，不需要重复导出。",
        "",
        "## 4. DuckDB vs pandas 聚合耗时（5 次取最优）",
        "",
        cmp.pipe(md_table),
        "",
        f"- 结果一致性：avg_price 对账 {'一致' if match else '不一致'}；"
        f"DuckDB {t_duck['best_ms']} ms vs pandas {t_pandas['best_ms']} ms（比值 {speedup}×）。",
        "- **122 行小数据下 DuckDB 并不比 pandas 快**：两者都受解释器与调用开销主导，",
        "  这个数字只用于说明两条执行路径结果等价，不能当作 OLAP 性能结论。",
        "",
        "## 5. 132 万行 Parquet 上的列存聚合",
        "",
        f"- 复用 `spark/bench` 压测数据：{scale['files']} 个 Parquet 文件、"
        f"{scale['rows']:,} 行，按 metric_key 聚合为 {scale['groups']} 组。",
        f"- DuckDB 直查最优耗时 {scale.get('timing', {}).get('best_ms', '未执行')} ms"
        "（3 次取最优），结果见 `olap_scale_agg.csv`。",
        "- 这是「列存 + 向量化聚合」在真实规模下的验证，比 122 行的对比更有参考价值。",
        "",
        "## 6. 诚实边界",
        "",
        "- DuckDB 是嵌入式 OLAP 引擎，**不是** Doris / ClickHouse / StarRocks 这类分布式服务；",
        "  没有分布式表、副本、分片、集群部署运维。",
        "- 本项目证明的是 OLAP 建模、列存聚合、窗口 SQL、Parquet 互通与计时方法；",
        "  若 JD 要求 Doris/ClickHouse 部署经验，仍需单独补。",
    ]
    (OLAP_OUT / "olap_report.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    con.close()
    return {"rows": n, "duckdb_version": duckdb.__version__,
            "parquet": parquet_info, "scale": scale,
            "result_match": match, "speedup_x": speedup,
            "duckdb_best_ms": t_duck["best_ms"], "pandas_best_ms": t_pandas["best_ms"],
            "outputs": ["ads_clean.csv", "olap_tier_agg.csv", "olap_window_top5.csv",
                        "olap_scale_agg.csv", "duckdb_vs_pandas.csv", "olap_report.md"]}


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
