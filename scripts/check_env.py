"""环境自检：一次告诉你「这台机器上哪些模块能跑、缺什么」。

用法：python scripts/check_env.py

只依赖标准库，没有第三方依赖也能运行——这正是它的用途。
"""
from __future__ import annotations

import importlib.util
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def has(mod: str) -> str | None:
    try:
        spec = importlib.util.find_spec(mod)
    except (ImportError, ValueError):
        return None
    if not spec:
        return None
    try:
        m = __import__(mod)
        return getattr(m, "__version__", "installed")
    except Exception:
        return "installed"


def find_java() -> str | None:
    jh = os.environ.get("JAVA_HOME")
    if jh and (Path(jh) / "bin" / "java.exe").exists():
        return jh
    if jh and (Path(jh) / "bin" / "java").exists():
        return jh
    for base in (r"C:\Program Files\JetBrains", r"C:\Program Files\Java", r"C:\Program Files\Eclipse Adoptium"):
        p = Path(base)
        if p.exists():
            for cand in list(p.glob("*/jbr")) + list(p.glob("jdk*")):
                exe = cand / "bin" / ("java.exe" if os.name == "nt" else "java")
                if exe.exists():
                    return str(cand)
    found = shutil.which("java")
    return found


def main() -> int:
    rows: list[tuple[str, str, bool]] = []

    py = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    rows.append(("Python >= 3.10", py, sys.version_info >= (3, 10)))

    mods = {
        "openpyxl": "基础分层（run_pipeline.py）",
        "pyspark": "Spark 分层 / 画像 / 压测 / 实时 / Iceberg",
        "pandas": "Pandas 分析",
        "numpy": "Pandas 分析",
        "duckdb": "OLAP 查询",
    }
    versions = {}
    for m in mods:
        v = has(m)
        versions[m] = v
        rows.append((m, v or "未安装", bool(v)))

    java = find_java()
    rows.append(("JDK 17/21（跑 Spark 需要）", java or "未找到", bool(java)))

    source = None
    try:
        import config as cfg
        source = cfg.SOURCE_DIR
    except Exception as exc:
        rows.append(("config.py 可导入", str(exc)[:60], False))
    if source is not None:
        ok = Path(source).exists()
        rows.append(("采集源数据目录", str(source) if ok else f"不存在：{source}", ok))

    jar = Path(r"C:\Temp\wk\iceberglib")
    jars = list(jar.glob("iceberg-spark-runtime-*.jar")) if jar.exists() else []
    rows.append(("Iceberg runtime jar", jars[0].name if jars else "未找到（可选）", bool(jars)))

    print("=" * 72)
    print("环境自检")
    print("=" * 72)
    width = max(len(r[0]) for r in rows)
    for name, detail, ok in rows:
        mark = "OK  " if ok else "MISS"
        print(f"  [{mark}] {name:<{width}}  {detail}")

    modules = [
        ("基础分层 + 质量校验", ["openpyxl"], False),
        ("治理 / BI 看板 / 分析 / OLAP", ["pandas", "numpy", "duckdb"], False),
        ("Spark 分层（spark/run_stage.py）", ["pyspark"], True),
        ("标签画像（governance/build_profile.py）", ["pyspark"], True),
        ("132 万行压测（spark/bench/run_bench.py）", ["pyspark"], True),
        ("实时管道（streaming/stream_job.py）", ["pyspark"], True),
        ("Iceberg（iceberg/iceberg_demo.py）", ["pyspark"], True),
    ]
    print()
    print("模块可运行性")
    print("-" * 72)
    runnable, blocked = 0, []
    for label, need, need_java in modules:
        missing = [n for n in need if not versions.get(n)]
        if need_java and not java:
            missing.append("JDK")
        if label.startswith("Iceberg") and not jars:
            missing.append("iceberg jar")
        if missing:
            blocked.append((label, missing))
            print(f"  [SKIP] {label:<44} 缺：{', '.join(missing)}")
        else:
            runnable += 1
            print(f"  [RUN ] {label:<44} 就绪")

    print()
    if source is not None and not Path(source).exists():
        print("提示：基础分层需要采集系统的导出数据（exports/、backups/）。")
        print("      没有源数据时，可直接查看 output_public/ 下的脱敏结果与报告。")
    print(f"可运行 {runnable}/{len(modules)} 个模块；缺依赖时按 requirements.txt 安装即可。")
    return 0 if runnable == len(modules) else 1


if __name__ == "__main__":
    raise SystemExit(main())
