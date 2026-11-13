"""Windows 本机启动 Airflow 的辅助脚本（实验性，官方不支持 Windows 原生部署）。

前置（在你要用的 Python 环境里执行一次）：
    pip install "apache-airflow==3.1.0" "apache-airflow-providers-standard"
    pip install -r requirements.txt        # pyspark / openpyxl

用法（在 pgy_dw 目录下）：
    python airflow/start_windows.py init          # 初始化元数据 DB
    python airflow/start_windows.py list          # 确认 DAG 被识别
    python airflow/start_windows.py test          # 不启 Scheduler，真跑一次 DAG
    python airflow/start_windows.py standalone    # 启动 Scheduler + Web UI
    python airflow/start_windows.py scheduler     # 只起调度器
    python airflow/start_windows.py api-server    # 只起 Web UI 服务

更稳妥的官方支持方式：在装好 Docker Desktop 的机器上执行
    cd airflow && cp .env.example .env && docker compose up --build
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

AIRFLOW_DIR = Path(__file__).resolve().parent
ROOT = AIRFLOW_DIR.parent
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", r"C:\Temp\airflow_home"))
DAGS_FOLDER = AIRFLOW_DIR / "dags"
DEFAULT_SOURCE = str(Path.home() / "Documents" / "Codex" / "2026-07-24"
                     / "30-90-1-2-3-4" / "work" / "pgy_crawler")

COMMANDS = {
    "init": ["db", "migrate"],
    "list": ["dags", "list"],
    "test": ["dags", "test", "pgy_dw_monthly", "2026-11-12"],
    "standalone": ["standalone"],
    "scheduler": ["scheduler"],
    "api-server": ["api-server"],
    "webserver": ["webserver"],
}


def build_env() -> dict:
    AIRFLOW_HOME.mkdir(parents=True, exist_ok=True)
    e = os.environ.copy()
    e["AIRFLOW_HOME"] = str(AIRFLOW_HOME)
    e["AIRFLOW__CORE__DAGS_FOLDER"] = str(DAGS_FOLDER)
    e["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    e["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
    e["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{AIRFLOW_HOME.as_posix()}/airflow.db"
    e["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"
    # Airflow 3 的 JWT 认证：issuer/secret 为空时 PyJWT 2.10 会抛
    # "Issuer (iss) must be a string"，登录接口直接 500。
    e.setdefault("AIRFLOW__API_AUTH__JWT_ISSUER", "airflow")
    e.setdefault("AIRFLOW__API_AUTH__JWT_SECRET", "pgy-dw-local-dev-only-change-me")
    e["PGY_PROJECT_ROOT"] = str(ROOT)
    e.setdefault("PGY_SOURCE_DIR", DEFAULT_SOURCE)
    e.setdefault("PGY_SPARK_WAREHOUSE", r"C:\Temp\pgy_spark_warehouse")
    e.setdefault("JAVA_HOME", r"C:\Program Files\JetBrains\PyCharm 2025.3.3\jbr")
    e.setdefault("HADOOP_HOME", r"C:\Temp\wk\hadoop")
    # Windows 无 fcntl，Airflow 3.x 的 simple_auth_manager 会因此加载失败；
    # _winstubs 提供仅用于本机调试的最小替身（生产请用 Linux/Docker）。
    stubs = AIRFLOW_DIR / "_winstubs"
    # 本机把 Airflow / PySpark 装到了 --target 目录（不污染全局环境），这里自动并入 PYTHONPATH。
    # 换机器时用 PGY_EXTRA_PATH 覆盖，例如：set PGY_EXTRA_PATH=D:\libs\airflowlib
    extras = [p for p in os.environ.get("PGY_EXTRA_PATH", "").split(os.pathsep) if p]
    for cand in (r"C:\Temp\wk\airflowlib", r"C:\Temp\wk\sparklib", r"C:\Temp\wk\pylib"):
        if Path(cand).is_dir() and cand not in extras:
            extras.append(cand)
    parts = [*extras, str(ROOT), str(AIRFLOW_DIR), str(stubs), e.get("PYTHONPATH", "")]
    e["PYTHONPATH"] = os.pathsep.join(p for p in parts if p)

    # Windows 文件名不能含 ':'，而 DagRun 的 run_id 形如
    # manual__2026-11-12T14:20:11.686208+00:00 —— 默认日志模板会直接拼进目录名，
    # 导致 os.mkdir 抛 WinError 123，任务在"排队->执行"这一步直接 failed。
    # 这里把 run_id 里的 +00:00 换成 Z、其余 ':' 换成 '-'。
    e.setdefault(
        "AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE",
        "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id | replace('+00:00','Z') | replace(':','-') }}"
        "/task_id={{ ti.task_id }}/attempt={{ try_number|default(ti.try_number) }}.log",
    )

    # 把当前解释器目录放到 PATH 最前：DAG 里的 BashOperator 用 `python run_stage.py`，
    # 若机器上 PATH 里先命中 Anaconda 3.9（本机就是），任务会因为 pyspark 不兼容直接失败。
    e["PATH"] = str(Path(sys.executable).parent) + os.pathsep + e.get("PATH", "")

    # Airflow 的 BashOperator 在 Windows 上仍会调用 bash.exe；把常见 Git Bash 目录并入 PATH
    bash_candidates = [
        r"C:\Program Files\Git\bin",
        r"C:\Program Files\Git\usr\bin",
        r"C:\Program Files (x86)\Git\bin",
    ]
    # WorkBuddy 自带的 PortableGit，版本号不固定，用 glob 找
    bash_candidates += [str(p) for p in
                        Path.home().glob(".workbuddy/binaries/PortableGit/versions/*/bin")]
    for candidate in bash_candidates:
        if (Path(candidate) / "bash.exe").exists():
            e["PATH"] = candidate + os.pathsep + e.get("PATH", "")
            break
    return e


def find_cli() -> list[str]:
    """优先用已安装的 airflow 命令；--target 安装没有 console script，故提供 CLI parser 兜底。"""
    if os.environ.get("AIRFLOW_CLI"):
        return [os.environ["AIRFLOW_CLI"]]
    exe = shutil.which("airflow")
    if exe:
        return [exe]
    for cand in (Path(sys.prefix) / "Scripts" / "airflow.exe", AIRFLOW_HOME / "bin" / "airflow.exe"):
        if cand.exists():
            return [str(cand)]
    code = (
        "import sys, airflow_win_patch; "  # 先补 Content-Type（见 _winstubs/airflow_win_patch.py）
        "from airflow.cli.cli_parser import get_parser; "
        "args = get_parser().parse_args(sys.argv[1:]); args.func(args)"
    )
    return [sys.executable, "-c", code]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=list(COMMANDS) + ["check"])
    ap.add_argument("--extra", nargs="*", default=[], help="追加给 airflow CLI 的参数")
    args = ap.parse_args()
    env = build_env()

    if args.command == "check":
        print("AIRFLOW_HOME :", AIRFLOW_HOME)
        print("DAGS_FOLDER  :", DAGS_FOLDER)
        print("CLI          :", " ".join(find_cli()))
        print("PGY_SOURCE_DIR:", env["PGY_SOURCE_DIR"])
        rc = subprocess.call(find_cli() + ["version"], env=env, cwd=str(ROOT))
        print("airflow version rc =", rc, "(0 表示可运行)")
        return rc

    cmd = find_cli() + COMMANDS[args.command] + args.extra
    print("执行:", " ".join(cmd))
    print("Web UI 默认地址: http://localhost:8080 （standalone 会打印初始管理员密码）")
    return subprocess.call(cmd, env=env, cwd=str(ROOT))


if __name__ == "__main__":
    raise SystemExit(main())
