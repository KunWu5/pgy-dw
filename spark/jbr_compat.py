"""PyCharm JBR 兼容补丁（仅 Windows 本机需要）。

Spark 4.x 启动时强制加 `--add-modules=jdk.incubator.vector`，
而 JetBrains Runtime (JBR) 是精简镜像，不含这个 incubator 模块，导致 JVM 在
boot layer 直接退出（Module jdk.incubator.vector not found）。

本模块在检测到「JAVA_HOME 指向 JBR 且缺该模块」时，对本地 pyspark 的
spark-launcher jar 做一次**等长**常量替换：
    jdk.incubator.vector (20 bytes) -> java.base,java.prefs (20 bytes)
两者都是合法模块名且 JBR 均包含；等长替换不改变 class 文件长度，
常量池结构完全不变。执行前先备份 `<jar>.orig`，可随时还原。

标准 JDK 17 / 21 不需要此步骤，检测通过即跳过。

独立使用：
    python spark/jbr_compat.py --check
    python spark/jbr_compat.py --apply
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import zipfile
from pathlib import Path

OLD = b"jdk.incubator.vector"
NEW = b"java.base,java.prefs"
CLASS_ENTRY = "org/apache/spark/launcher/JavaModuleOptions.class"


def java_home() -> Path:
    return Path(os.environ.get("JAVA_HOME", ""))


def has_vector_module() -> tuple[bool, str]:
    exe = java_home() / "bin" / ("java.exe" if os.name == "nt" else "java")
    if not exe.exists():
        return False, f"java 不存在: {exe}"
    try:
        out = subprocess.run([str(exe), "--list-modules"], capture_output=True,
                             text=True, timeout=20).stdout
    except Exception as exc:  # pragma: no cover
        return False, str(exc)
    return "jdk.incubator.vector" in out, "ok"


def launcher_jar() -> Path | None:
    spec = importlib.util.find_spec("pyspark")
    if not spec or not spec.origin:
        return None
    jars = sorted((Path(spec.origin).parent / "jars").glob("spark-launcher_*.jar"))
    return jars[0] if jars else None


def is_patched(jar: Path) -> bool:
    with zipfile.ZipFile(jar) as z:
        return NEW in z.read(CLASS_ENTRY)


def apply_patch(jar: Path) -> bool:
    backup = jar.with_suffix(jar.suffix + ".orig")
    if not backup.exists():
        shutil.copy2(jar, backup)
    with zipfile.ZipFile(backup) as zin:
        items = [(info, zin.read(info.filename)) for info in zin.infolist()]
    assert len(OLD) == len(NEW), "必须等长替换"
    changed = False
    with zipfile.ZipFile(jar, "w", zipfile.ZIP_DEFLATED) as zout:
        for info, content in items:
            if info.filename == CLASS_ENTRY and OLD in content:
                content = content.replace(OLD, NEW)
                changed = True
            zout.writestr(info, content)
    return changed


def ensure_compatible() -> str:
    """在 SparkSession 启动前调用；返回状态说明。"""
    ok, detail = has_vector_module()
    if ok:
        return "标准 JDK，无需补丁"
    jar = launcher_jar()
    if jar is None:
        return f"JBR 缺 jdk.incubator.vector，但未找到 pyspark launcher jar（{detail}）"
    if is_patched(jar):
        return "JBR 兼容补丁已生效"
    changed = apply_patch(jar)
    return ("JBR 兼容补丁已应用（等长替换，已备份 .orig）" if changed
            else "JBR 缺模块且补丁未命中，请改用标准 JDK 17/21")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    ok, detail = has_vector_module()
    print("JAVA_HOME:", java_home())
    print("jdk.incubator.vector:", "存在" if ok else f"缺失（{detail}）")
    jar = launcher_jar()
    print("launcher jar:", jar)
    if jar:
        print("补丁状态:", "已应用" if is_patched(jar) else "未应用")
        if args.apply and not ok and not is_patched(jar):
            print("结果:", "已应用" if apply_patch(jar) else "未命中")
