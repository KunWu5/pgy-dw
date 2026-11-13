# 在 Windows 上本机跑 Airflow（无需 Docker）

Airflow 官方不支持 Windows 原生部署，但本机没有 Docker、WSL 也被策略禁用。
实测在 **Windows 11 + Python 3.13 + Airflow 3.1.0** 上可以跑通 scheduler +
api-server + Web UI（`dag-processor` / `triggerer` 仍不可用，见「已知限制」）。

**一句话结论**：UI 能开、能看 DAG 和运行历史、能触发 DagRun；
但**任务真正执行**要走 `dags test`（进程内执行）。由 scheduler/UI 拉起的任务
在 Windows 上必然失败，因为任务 supervisor 用的是 `os.fork`——这是硬限制，不是配置问题。

## 一次性安装

```powershell
# 1) Airflow 装到独立目录，不污染全局环境
python -m pip install "apache-airflow==3.1.0" "apache-airflow-providers-standard" `
  --target C:\Temp\wk\airflowlib -i https://mirrors.aliyun.com/pypi/simple/ `
  --prefer-binary

# 2) Starlette 必须降到 0.47.x：0.48 移除了 TemplateResponse(name, context) 旧签名，
#    Airflow 3.1.0 的 /index.html 首页会直接 500（TypeError: unhashable type: 'dict'）
python -m pip install "starlette==0.47.2" --target C:\Temp\wk\airflowlib --upgrade `
  -i https://mirrors.aliyun.com/pypi/simple/

# 3) 数仓作业依赖
python -m pip install -r requirements.txt
```

`start_windows.py` 会自动把 `C:\Temp\wk\airflowlib`、`sparklib`、`pylib` 并入
`PYTHONPATH`；换机器用 `PGY_EXTRA_PATH` 覆盖。

## 启动步骤

```powershell
cd <项目根目录>

python airflow/start_windows.py init     # 初始化元数据库（只需一次）
python airflow/start_windows.py list     # 确认 DAG 被识别
python airflow/start_windows.py test     # 用 Airflow 引擎真跑一遍 5 个任务（不启 UI）
python airflow/start_windows.py standalone   # 起 scheduler + api-server + Web UI
```

打开 <http://localhost:8080>，用户名 `admin`，密码在
`C:\Temp\airflow_home\simple_auth_manager_passwords.json.generated`。
DAG 默认是 **paused**，在 UI 里点一下开关才会被调度。

⚠️ 在 Windows 上：取消暂停 + Trigger 之后，DagRun 会正常从 queued 走到 running，
5 个 TaskInstance 也会被创建和排队，**但任务会在几秒内 failed**——原因见下方「已知限制」。
要在本机真正跑数，用：

```powershell
python airflow/start_windows.py test        # = airflow dags test，进程内执行，5/5 success
python airflow/local_runner.py              # 同一任务图，纯本机调度
```

UI 里可以看 DAG 结构、任务依赖、每次运行的起止时间和状态，用于演示和排查。

## 已修复的六个 Windows 兼容点

| 现象 | 原因 | 处理 |
|---|---|---|
| `ModuleNotFoundError: fcntl` | simple_auth_manager 用 flock 做文件锁 | `_winstubs/fcntl.py` 空实现 |
| `AttributeError: SIGALRM / SIGQUIT` | 超时与优雅退出用 Unix 信号 | `_winstubs/sitecustomize.py` 打补丁 |
| `PATCH task-instances/{id}/run` 返回 422 | 客户端只发 `content=`，没带 `Content-Type`，FastAPI 0.117 不再按 JSON 解析 | `_winstubs/airflow_win_patch.py` 补请求头 |
| `Popen(["airflow", ...])` WinError 2 | `--target` 安装没有 console script | 同上，改为 `python -c` 入口 |
| `No module named pwd / resource / grp` | `daemon`、子进程限流用 Unix 模块 | `_winstubs/` 三个最小替身 |
| 首页 500 `unhashable type: 'dict'` | Starlette 0.48 移除旧 `TemplateResponse` 签名 | 降到 starlette 0.47.2 |
| Trigger 后 DagRun 一直是 `queued` | 新 DAG 默认 `is_paused=True`，暂停时不会调度 | UI 打开暂停开关，或 `PATCH /api/v2/dags/{id} {"is_paused":false}` |
| 任务失败但日志文件 0 字节 | `run_id` 形如 `manual__2026-11-12T14:20:11.68+00:00`，`:` 是 Windows 非法字符，日志目录 mkdir 抛 `WinError 123` | `AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE` 把 `+00:00`→`Z`、`:`→`-`（已在 `start_windows.py` 内置） |
| `python run_stage.py` 报 pyspark 不兼容 | PATH 里 Anaconda 3.9 排在前面，`python` 命中的是 3.9 | `start_windows.py` 把当前解释器目录插到 PATH 最前 |

另有一个**跨目录的坑**（与 Windows 无关，但排查花了很久）：
Derby metastore 默认建在**当前工作目录**。手工运行时 cwd 是项目根，
被 Airflow 拉起时 cwd 是临时目录，两边各看到一套空元数据——表在 warehouse
里有目录、metastore 里没有，Spark 就报 `LOCATION_ALREADY_EXISTS`，同时 Derby
文件句柄会让临时目录删不掉（`WinError 32`）。
现在 `spark/session.py` 用 `JAVA_TOOL_OPTIONS=-Dderby.system.home=<项目根>` 把
`derby.system.home` 钉死（可 `PGY_SPARK_METASTORE` 覆盖），任何 cwd 都是同一套元数据。

## 已知限制（如实说明，别写进简历）

- **最硬的一条**：任务 supervisor（`airflow/sdk/execution_time/supervisor.py:476`）写死
  `pid = os.fork()`，Windows 没有 fork → `dag-processor`、`triggerer` 起不来，
  **LocalExecutor 拉起的任务也必然失败**（scheduler 日志里能看到 `AttributeError`，
  task log 是 0 字节）。所以 UI 上 Trigger 只能走到 running，跑不完。
  Windows 上真正能执行任务的只有进程内模式：`airflow dags test` / `airflow tasks test`。
- `dag-processor` 不可用还有一个副作用：改动 DAG 文件后，UI 里的 DAG 结构可能不会自动刷新。
- 本机是单机 LocalExecutor，不是集群；不要写「Airflow 集群 / 生产部署」。
- 真实的 Linux 生产环境仍然推荐 `docker compose up --build`（`airflow/` 目录下已备好）。

## 验证证据

- `airflow/output/airflow_dag_test_report.json`：`dags test` 的 5 个任务耗时、DagRun 状态、12 项质量校验、双引擎对账
- 元数据库 `C:\Temp\airflow_home\airflow.db`：`dags test` 产生的 `dag_run.state = success`，5 个 `task_instance` 全部 success
- `python airflow/validate_dag.py`：DAG 结构静态校验 16/16
- UI 侧证据：`/` 返回 200、`POST /auth/token` 201、`GET /api/v2/dags` 200 且能查到 `pgy_dw_monthly`、
  `POST /api/v2/dags/pgy_dw_monthly/dagRuns` 200（DagRun 进入 running）
