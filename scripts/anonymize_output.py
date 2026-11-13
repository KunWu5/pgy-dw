"""把 output/ 与 spark/output/ 的产物脱敏成可公开的版本（output_public/）。

脱敏规则：
  red_id   → r_<sha256(salt + red_id)[:8]>   不可逆、保持唯一（同一个人前后一致）
  nickname → 达人001 / 达人002 …             按 anon id 排序编号，跨文件一致
  文本报告里的 red_id / nickname / 本地路径同样替换

用法：python scripts/anonymize_output.py
"""
from __future__ import annotations

import csv
import hashlib
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DST = ROOT / "output_public"
SALT = "pgy-dw-public-demo-v1"
PII_COLS = ("red_id", "nickname")
SRC_DIRS = [ROOT / "output", ROOT / "spark" / "output"]
SKIP_NAMES = {"ads_clean.csv", "pgy_dw.db"}
TEXT_SUFFIX = {".md", ".html", ".json", ".csv"}


def anon_id(rid: str) -> str:
    return "r_" + hashlib.sha256((SALT + rid).encode("utf-8")).hexdigest()[:8]


def collect() -> tuple[dict, dict]:
    """扫一遍所有 CSV，建立 red_id → anon、anon → 昵称编号 两张表。"""
    ids: set[str] = set()
    nick_of: dict[str, str] = {}
    for base in SRC_DIRS:
        if not base.exists():
            continue
        for path in base.rglob("*.csv"):
            if path.name in SKIP_NAMES:
                continue
            try:
                with path.open(encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames or "red_id" not in reader.fieldnames:
                        continue
                    for row in reader:
                        rid = (row.get("red_id") or "").strip()
                        if rid:
                            ids.add(rid)
                            if row.get("nickname") and rid not in nick_of:
                                nick_of[rid] = row["nickname"].strip()
            except Exception as exc:
                print(f"  跳过 {path}: {exc}")

    id_map = {rid: anon_id(rid) for rid in ids}
    anon_sorted = sorted(id_map.values())
    nick_map = {a: f"达人{i + 1:03d}" for i, a in enumerate(anon_sorted)}
    return id_map, {rid: nick_map[id_map[rid]] for rid in nick_of}


def scrub_text(text: str, id_map: dict, nick_map: dict) -> str:
    for rid, a in id_map.items():
        text = re.sub(r"(?<!\d)" + re.escape(rid) + r"(?!\d)", a, text)
    for nickname, label in nick_map.items():
        if len(nickname) >= 2:
            text = text.replace(nickname, label)
    text = re.sub(r"C:\\+Users\\+[^\\\"'<\s]+", r"C:\\Users\\<user>", text)
    text = re.sub(r"C:/Users/[^/\"'<\s]+", "C:/Users/<user>", text)
    return text


def build() -> dict:
    if DST.exists():
        shutil.rmtree(DST, ignore_errors=True)
    DST.mkdir(parents=True, exist_ok=True)

    id_map, nick_map = collect()
    print(f"  识别到 {len(id_map)} 个 red_id，{len(nick_map)} 个昵称")

    files = 0
    for base in SRC_DIRS:
        if not base.exists():
            continue
        prefix = Path("spark") if base.name == "output" and base.parent.name == "spark" else Path()
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix not in TEXT_SUFFIX:
                continue
            if path.name in SKIP_NAMES:
                continue
            out = DST / prefix / path.relative_to(base)
            out.parent.mkdir(parents=True, exist_ok=True)

            if path.suffix == ".csv":
                with path.open(encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    fields = reader.fieldnames or []
                    rows = list(reader)
                if "red_id" in fields:
                    for row in rows:
                        rid = (row.get("red_id") or "").strip()
                        if rid:
                            row["red_id"] = id_map.get(rid, anon_id(rid))
                        if "nickname" in fields and row.get("nickname"):
                            row["nickname"] = nick_map.get(rid, "达人")
                else:
                    # 非明细 CSV 里也可能出现 red_id 文本（如异常清单），统一过一遍
                    for row in rows:
                        for k, v in row.items():
                            if isinstance(v, str):
                                row[k] = scrub_text(v, id_map, nick_map)
                with out.open("w", encoding="utf-8-sig", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=fields)
                    w.writeheader()
                    w.writerows(rows)
            else:
                text = path.read_text(encoding="utf-8", errors="replace")
                out.write_text(scrub_text(text, id_map, nick_map), encoding="utf-8")
            files += 1

    return {"files": files, "red_ids": len(id_map), "nicknames": len(nick_map),
            "output": str(DST.relative_to(ROOT))}


if __name__ == "__main__":
    print(build())
