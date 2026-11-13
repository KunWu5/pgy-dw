"""离线 BI 看板：把 ADS 结果渲染成零依赖 HTML（可离线打开，不依赖 CDN）。

对应 JD：图一职责 4（报表 / 可视化看板 / 标签 / 画像）、图二职责 3（报表 / BI 平台）
"""
from __future__ import annotations

import csv
import html
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from governance import BI_OUT, OUT as GOV_OUT  # noqa: E402

OUT_CSV = ROOT / "output"
SPARK_CSV = ROOT / "spark" / "output"


def read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def bars(items, color="#639922", unit=""):
    """简单横向条形图（纯 SVG）。"""
    if not items:
        return "<p class='muted'>暂无数据</p>"
    mx = max(num(v) for _, v in items) or 1
    out = ["<svg viewBox='0 0 620 %d' width='100%%' role='img'>" % (len(items) * 26 + 8)]
    for i, (label, value) in enumerate(items):
        y = i * 26 + 4
        w = max(2, int(num(value) / mx * 330))
        out.append(
            f"<text x='0' y='{y + 14}' font-size='12' fill='#444'>{html.escape(str(label)[:16])}</text>"
            f"<rect x='180' y='{y + 4}' width='{w}' height='12' rx='3' fill='{color}'/>"
            f"<text x='{190 + w}' y='{y + 14}' font-size='12' fill='#222'>{num(value):,.0f}{unit}</text>")
    out.append("</svg>")
    return "".join(out)


def build() -> dict:
    BI_OUT.mkdir(parents=True, exist_ok=True)
    wide = read(OUT_CSV / "ads_blogger_value_wide.csv")
    alerts = read(OUT_CSV / "ads_metric_alert.csv")
    churn = read(OUT_CSV / "ads_blogger_churn.csv")
    profile = read(SPARK_CSV / "profile_blogger.csv")
    tag_profile = read(SPARK_CSV / "profile_tag.csv")
    quality = read(GOV_OUT / "quality_history.csv")

    months = sorted({r.get("stat_month", "") for r in wide if r.get("stat_month")})
    tiers = Counter(r.get("blogger_tier", "未知") for r in wide)
    segments = Counter(r.get("profile_segment", "未知") for r in profile) if profile else Counter()
    alert_by_metric = Counter(r.get("metric_label", "未知") for r in alerts)
    top_fans = sorted(wide, key=lambda r: num(r.get("fans_count")), reverse=True)[:8]
    top_tags = [(r.get("tag"), r.get("blogger_cnt")) for r in tag_profile[:8]]

    parts = [
        "<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>蒲公英达人数据看板</title>",
        "<style>body{font:14px/1.6 -apple-system,'Segoe UI',sans-serif;color:#222;margin:28px auto;max-width:1080px;background:#fff}"
        "h1{font-size:21px;font-weight:500;margin:0 0 4px}h2{font-size:15px;font-weight:500;margin:28px 0 10px}"
        ".sub{color:#777;font-size:13px}.cards{display:flex;flex-wrap:wrap;gap:12px;margin-top:18px}"
        ".card{flex:1 1 150px;background:#f6f7f4;border-radius:10px;padding:14px}"
        ".card .lbl{font-size:12px;color:#666}.card .val{font-size:24px;font-weight:500;margin-top:4px}"
        "table{border-collapse:collapse;width:100%;font-size:13px}th,td{border-bottom:1px solid #e6e6e2;padding:6px 8px;text-align:left}"
        "th{background:#f6f7f4;font-weight:500}.grid{display:flex;gap:24px;flex-wrap:wrap}.grid>div{flex:1 1 420px}"
        ".muted{color:#888;font-size:13px}a{color:#185fa5}</style>",
        "<h1>小红书蒲公英达人 · 离线数仓看板</h1>",
        f"<div class='sub'>数据源：ODS→DWD→DWS→ADS 分层结果（{', '.join(months)}）｜"
        "离线生成，无外部依赖｜<a href='../governance/dq_monitor.html'>质量监控</a> · "
        "<a href='../governance/lineage.html'>表级血缘</a></div>",
        "<div class='cards'>",
        f"<div class='card'><div class='lbl'>本月达人</div><div class='val'>{len(wide):,}</div></div>",
        f"<div class='card'><div class='lbl'>指标异动</div><div class='val'>{len(alerts):,}</div></div>",
        f"<div class='card'><div class='lbl'>名单增删</div><div class='val'>{len(churn):,}</div></div>",
        f"<div class='card'><div class='lbl'>画像标签数</div><div class='val'>{len(tag_profile):,}</div></div>",
        f"<div class='card'><div class='lbl'>质量通过率</div><div class='val'>"
        f"{(sum(num(q.get('passed')) for q in quality) / max(sum(num(q.get('total')) for q in quality), 1) * 100):.0f}%</div></div>",
        "</div>",
        "<div class='grid'>",
        "<div><h2>达人分层分布</h2>" + bars(list(tiers.items()), "#639922") + "</div>",
        "<div><h2>价值画像分段</h2>" + (bars(list(segments.items()), "#378ADD") if segments
                                   else "<p class='muted'>运行 governance/profile.py 后生成</p>") + "</div>",
        "</div>",
        "<div class='grid'>",
        "<div><h2>异动最多的指标 TOP</h2>" + bars(alert_by_metric.most_common(8), "#BA7517") + "</div>",
        "<div><h2>标签覆盖达人 TOP</h2>" + (bars(top_tags, "#7F77DD") if top_tags
                                      else "<p class='muted'>运行 governance/profile.py 后生成</p>") + "</div>",
        "</div>",
        "<h2>粉丝量 TOP 8 达人</h2>",
        "<table><tr><th>达人</th><th>粉丝数</th><th>图文报价</th><th>CPE(30d)</th><th>粉丝分层</th><th>性价比</th></tr>",
    ]
    for r in top_fans:
        parts.append(
            f"<tr><td>{html.escape(r.get('nickname', '-'))}</td>"
            f"<td>{num(r.get('fans_count')):,.0f}</td><td>{num(r.get('image_price')):,.0f}</td>"
            f"<td>{num(r.get('cpe_30d')):,.2f}</td><td>{html.escape(r.get('blogger_tier', '-'))}</td>"
            f"<td>{html.escape(r.get('cpe_tier', '-'))}</td></tr>")
    parts += ["</table><h2>异动明细 TOP 10</h2>",
              "<table><tr><th>月份</th><th>达人</th><th>指标</th><th>本月</th><th>上月</th><th>环比%</th></tr>"]
    for r in alerts[:10]:
        parts.append(
            f"<tr><td>{html.escape(r.get('stat_month', '-'))}</td>"
            f"<td>{html.escape(r.get('red_id', '-'))}</td>"
            f"<td>{html.escape(r.get('metric_label', '-'))}</td>"
            f"<td>{num(r.get('metric_value')):,.0f}</td><td>{num(r.get('prev_value')):,.0f}</td>"
            f"<td>{num(r.get('change_percent')):,.2f}</td></tr>")
    parts.append("</table></html>")
    path = BI_OUT / "dashboard.html"
    path.write_text("\n".join(parts), encoding="utf-8")

    return {"dashboard": str(path.relative_to(ROOT)), "wide_rows": len(wide),
            "alert_rows": len(alerts), "churn_rows": len(churn),
            "profile_rows": len(profile), "tag_rows": len(tag_profile),
            "quality_runs": len(quality)}


if __name__ == "__main__":
    import json
    print(json.dumps(build(), ensure_ascii=False))
