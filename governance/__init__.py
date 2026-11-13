"""数据治理产物目录。"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "governance"
BI_OUT = ROOT / "output" / "bi"
ANALYSIS_OUT = ROOT / "output" / "analysis"
OLAP_OUT = ROOT / "output" / "olap"


def md_table(df) -> str:
    """把 pandas DataFrame 转成 Markdown 表格，不依赖 tabulate。"""
    cols = [str(c) for c in df.columns]
    lines = ["| " + " | ".join(cols) + " |",
             "|" + "|".join("---" for _ in cols) + "|"]
    for row in df.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(
            "" if v is None else (f"{v:,.3f}".rstrip("0").rstrip(".")
                                  if isinstance(v, float) else str(v))
            for v in row) + " |")
    return "\n".join(lines)
