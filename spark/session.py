"""SparkSession 工厂：启用 Hive 支持，使用本地内置 Derby Metastore。

要点：
- enableHiveSupport() 让 CREATE TABLE / INSERT OVERWRITE ... PARTITION 走 Hive Metastore，
  表结构会持久化到 metastore_db，Spark UI 与 Hive SQL 都能查到。
- JAVA_HOME 必须在 pyspark 启动 JVM 之前设置；本机为 PyCharm JBR 21。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
OUT_DIR = BASE / "spark" / "output"

# Hive Metastore 会按 URI 处理 warehouse 路径；含中文的用户目录会被百分号编码导致建库失败。
# 默认放到纯英文临时目录，可用 PGY_SPARK_WAREHOUSE 覆盖。
WAREHOUSE = Path(os.environ.get("PGY_SPARK_WAREHOUSE", r"C:\Temp\pgy_spark_warehouse"))

# Derby 默认把 metastore_db 建在当前工作目录 —— 手工运行时 cwd 是项目根目录，
# 被 Airflow/调度器拉起时 cwd 是临时目录，两边会各自看到一套"空"元数据：
# 表在 warehouse 里有目录、metastore 里却没有，Spark 就报 LOCATION_ALREADY_EXISTS，
# 而且 Derby 句柄占用会让临时目录删不掉（WinError 32）。
# 这里把 derby.system.home 钉死到项目根目录（早期手工运行生成的那套元数据就在这里），
# 任何 cwd 下都是同一套元数据；可 PGY_SPARK_METASTORE 覆盖。
# 注意：derby.system.home 是「包含 metastore_db 的目录」，指错会套出一层空的 metastore_db/。
METASTORE_HOME = Path(os.environ.get("PGY_SPARK_METASTORE", str(BASE)))

DEFAULT_JAVA_HOME = r"C:\Program Files\JetBrains\PyCharm 2025.3.3\jbr"


def prepare_env() -> None:
    """在 import pyspark 之前调用。"""
    if not os.environ.get("JAVA_HOME") or not Path(os.environ["JAVA_HOME"]).exists():
        os.environ["JAVA_HOME"] = DEFAULT_JAVA_HOME
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    METASTORE_HOME.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 用 JAVA_TOOL_OPTIONS 把 derby.system.home 传给 driver JVM：
    # 不依赖 spark-submit 参数、也不受 cwd 影响（PySpark 在 local 模式同样会带上）。
    derby_opt = "-Dderby.system.home=%s" % METASTORE_HOME
    current = os.environ.get("JAVA_TOOL_OPTIONS", "")
    if derby_opt not in current:
        os.environ["JAVA_TOOL_OPTIONS"] = (current + " " + derby_opt).strip()

    # Windows 上 Hive 建库需要 winutils.exe；Linux / Docker 环境无需此步。
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        for cand in (r"C:\Temp\wk\hadoop", str(BASE / "hadoop")):
            if (Path(cand) / "bin" / "winutils.exe").exists():
                hadoop_home = cand
                os.environ["HADOOP_HOME"] = cand
                break
    if hadoop_home:
        os.environ["PATH"] = str(Path(hadoop_home) / "bin") + os.pathsep + os.environ.get("PATH", "")

    # PyCharm JBR 精简运行时缺 jdk.incubator.vector，Spark 4.x 需要等长兼容补丁
    try:
        from spark import jbr_compat
        print("[env] " + jbr_compat.ensure_compatible())
    except Exception as exc:  # 兼容检测失败不阻断启动
        print("[env] JBR 兼容检测跳过:", exc)


def get_spark(app_name: str = "pgy_dw"):
    from pyspark.sql import SparkSession

    prepare_env()
    builder = (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.default.parallelism", "1")
        .config("spark.sql.warehouse.dir", WAREHOUSE.as_uri() + "/")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.hadoop.native.lib", "false")
    )
    if os.environ.get("HADOOP_HOME"):
        builder = builder.config("spark.hadoop.hadoop.home.dir", os.environ["HADOOP_HOME"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE IF NOT EXISTS pgy_dw")
    return spark
