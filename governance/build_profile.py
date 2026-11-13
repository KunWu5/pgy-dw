"""执行 05_profile.hql，构建标签体系 / 用户画像层并导出 CSV。

只新增 pgy_dw.dws_blogger_profile 与 pgy_dw.ads_tag_profile 两张表，
不 DROP、不改写原有 19 张表，122 / 310 / 27 等结果保持不变。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
for _lib in (r"C:\Temp\wk\sparklib", r"C:\Temp\wk\pylib"):
    if Path(_lib).is_dir() and _lib not in sys.path:
        sys.path.insert(0, _lib)

from spark.session import prepare_env  # noqa: E402

prepare_env()

from spark import io_utils as io  # noqa: E402
from spark.session import get_spark  # noqa: E402

SQL = ROOT / "spark" / "sql" / "05_profile.hql"


def run() -> dict:
    spark = get_spark("pgy_dw_profile")
    text = SQL.read_text(encoding="utf-8")
    n = 0
    for raw in text.split(";"):
        stmt = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if stmt:
            spark.sql(stmt)
            n += 1

    counts = {
        "profile_rows": io.table_count(spark, "pgy_dw.dws_blogger_profile"),
        "tag_profile_rows": io.table_count(spark, "pgy_dw.ads_tag_profile"),
    }
    exported = {}
    for table, name in [
        ("pgy_dw.dws_blogger_profile", "profile_blogger.csv"),
        ("pgy_dw.ads_tag_profile", "profile_tag.csv"),
    ]:
        exported[name] = io.write_csv(spark.sql(f"SELECT * FROM {table}"), name)

    segments = io.collect_dicts(
        spark,
        "SELECT profile_segment, COUNT(*) AS n FROM pgy_dw.dws_blogger_profile "
        "GROUP BY profile_segment ORDER BY n DESC", "profile_segments")
    top_tags = io.collect_dicts(
        spark, "SELECT tag, blogger_cnt, avg_value_score FROM pgy_dw.ads_tag_profile "
               "ORDER BY blogger_cnt DESC LIMIT 8", "profile_top_tags")
    spark.stop()
    return {"statements": n, **counts, "exported": exported,
            "segments": segments, "top_tags": top_tags}


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
