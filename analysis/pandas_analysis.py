"""Pandas / NumPy 分析：把 ADS 结果做成可复现的统计分析。

对应 JD：图一任职要求「掌握 Python，熟悉 Pandas/NumPy」、数据集处理项目经历

产物：output/analysis/ 下的报告与 4 份结果 CSV
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
for _lib in (r"C:\Temp\wk\datalib",):
    if Path(_lib).is_dir() and _lib not in sys.path:
        sys.path.insert(0, _lib)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from governance import ANALYSIS_OUT, md_table  # noqa: E402

NUMERIC = ["fans_count", "image_price", "video_price", "cpe_30d", "cpe_90d",
           "engagement_rate_30d", "price_per_10k_fans", "note_count_30d",
           "interaction_median_30d"]


def build() -> dict:
    ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)
    wide = pd.read_csv(ROOT / "output" / "ads_blogger_value_wide.csv", encoding="utf-8-sig")
    alerts = pd.read_csv(ROOT / "output" / "ads_metric_alert.csv", encoding="utf-8-sig")
    for c in NUMERIC:
        if c in wide.columns:
            wide[c] = pd.to_numeric(wide[c], errors="coerce")

    tier = (wide.groupby("blogger_tier")
            .agg(blogger_cnt=("red_id", "count"),
                 avg_fans=("fans_count", "mean"),
                 median_price=("image_price", "median"),
                 avg_cpe=("cpe_30d", "mean"),
                 avg_engagement=("engagement_rate_30d", "mean"))
            .round(2).reset_index())
    tier.to_csv(ANALYSIS_OUT / "tier_summary.csv", index=False, encoding="utf-8-sig")

    # NumPy Z-score 异常检测（|z| >= 3）
    outliers = []
    for col in ["fans_count", "image_price", "cpe_30d", "engagement_rate_30d"]:
        v = wide[col].to_numpy(dtype=float)
        ok = np.isfinite(v)
        if ok.sum() < 3:
            continue
        mu, sd = v[ok].mean(), v[ok].std(ddof=0)
        if sd == 0:
            continue
        z = np.where(ok, (v - mu) / sd, 0.0)
        for idx in np.flatnonzero(np.abs(z) >= 3):
            outliers.append({"red_id": wide.iloc[idx]["red_id"],
                             "nickname": wide.iloc[idx]["nickname"],
                             "metric": col, "value": round(float(v[idx]), 4),
                             "mean": round(float(mu), 4), "z_score": round(float(z[idx]), 2)})
    out_df = pd.DataFrame(outliers).sort_values("z_score", key=abs, ascending=False) if outliers \
        else pd.DataFrame(columns=["red_id", "nickname", "metric", "value", "mean", "z_score"])
    out_df.to_csv(ANALYSIS_OUT / "zscore_outliers.csv", index=False, encoding="utf-8-sig")

    corr = wide[NUMERIC].corr(numeric_only=True).round(3)
    corr.to_csv(ANALYSIS_OUT / "correlation.csv", encoding="utf-8-sig")

    pct = pd.DataFrame({
        "metric": ["fans_count", "image_price", "cpe_30d", "engagement_rate_30d"],
        "p25": [round(float(np.nanpercentile(wide[c].to_numpy(dtype=float), 25)), 3)
                for c in ["fans_count", "image_price", "cpe_30d", "engagement_rate_30d"]],
        "p50": [round(float(np.nanpercentile(wide[c].to_numpy(dtype=float), 50)), 3)
                for c in ["fans_count", "image_price", "cpe_30d", "engagement_rate_30d"]],
        "p75": [round(float(np.nanpercentile(wide[c].to_numpy(dtype=float), 75)), 3)
                for c in ["fans_count", "image_price", "cpe_30d", "engagement_rate_30d"]],
        "p95": [round(float(np.nanpercentile(wide[c].to_numpy(dtype=float), 95)), 3)
                for c in ["fans_count", "image_price", "cpe_30d", "engagement_rate_30d"]],
    })
    pct.to_csv(ANALYSIS_OUT / "percentiles.csv", index=False, encoding="utf-8-sig")

    alert_top = (alerts.groupby("metric_label").size()
                 .sort_values(ascending=False).head(8))

    md = [
        "# Pandas / NumPy 分析报告",
        "",
        f"- 数据：ADS 价值宽表 {len(wide)} 行 × {len(wide.columns)} 列；异动清单 {len(alerts)} 行",
        f"- 环境：pandas {pd.__version__} / numpy {np.__version__}",
        "- 复现：`python analysis/pandas_analysis.py`",
        "",
        "## 1. 达人分层汇总",
        "",
        tier.pipe(md_table),
        "",
        "## 2. Z-score 异常检测（|z| ≥ 3）",
        "",
        f"检出 **{len(out_df)}** 条极端值，示例：",
        "",
        out_df.head(8).pipe(md_table) if len(out_df) else "（无）",
        "",
        "## 3. 分位数（NumPy percentile）",
        "",
        pct.pipe(md_table),
        "",
        "## 4. 异动最多的指标",
        "",
        alert_top.to_string(),
        "",
        "## 5. 相关性（节选）",
        "",
        md_table(corr[["fans_count", "image_price", "cpe_30d", "engagement_rate_30d"]]),
        "",
        "> 分析基于 218 行真实快照产出的 ADS 结果，是探索性分析，不是业务结论。",
    ]
    (ANALYSIS_OUT / "pandas_report.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    return {"wide_rows": len(wide), "alert_rows": len(alerts),
            "tier_groups": len(tier), "zscore_outliers": len(out_df),
            "pandas": pd.__version__, "numpy": np.__version__,
            "outputs": ["tier_summary.csv", "zscore_outliers.csv",
                        "correlation.csv", "percentiles.csv", "pandas_report.md"]}


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
