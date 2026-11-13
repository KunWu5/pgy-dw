"""Airflow 3.1.0 在 Windows 本机跑 ``dags test`` 的兼容补丁（仅调试用）。

问题：``airflow.sdk.api.client`` 调 Execution API 时是这样发的——

    self.client.patch(f"task-instances/{id}/run", content=body.model_dump_json())

只给了 ``content=``，**没有 Content-Type**。官方 constraints 锁定的 FastAPI 0.117.1
在缺少 JSON content-type 时不会把 body 当 JSON 解析，而是把整串字符串交给 pydantic，
于是返回 422：

    {"type":"model_attributes_type","loc":["body"],
     "msg":"Input should be a valid dictionary or object to extract fields from"}

在 Linux 容器里同样是这套版本，但 dockerd 环境下会走 httpx 的默认请求头路径，
所以只在 Windows 原生 + 本地 ASGI 传输（``http://in-process.invalid.``）下暴露。
这里统一给「有 content 但没 content-type」的请求补上 ``application/json``，
不改变任何执行语义，只是让服务端能正确反序列化请求体。
"""
from __future__ import annotations

_APPLIED = False

# 与 start_windows.find_cli() 里的兜底入口保持一致
CLI_BOOTSTRAP = (
    "import sys, airflow_win_patch; "
    "from airflow.cli.cli_parser import get_parser; "
    "args = get_parser().parse_args(sys.argv[1:]); args.func(args)"
)


def _patch_standalone() -> None:
    """``airflow standalone`` 用 ``Popen(["airflow", ...])`` 拉起 4 个子进程。

    用 ``pip install --target`` 安装时没有生成 Console Script（airflow.exe），
    子进程会因为 WinError 2 直接退出。这里换成当前解释器的 CLI 入口。
    """
    import subprocess
    import sys

    try:
        from airflow.cli.commands import standalone_command as sc
    except Exception as exc:  # noqa: BLE001
        print("[airflow_win_patch] standalone 补丁跳过:", exc, flush=True)
        return

    def run(self):  # type: ignore[no-untyped-def]
        self.process = subprocess.Popen(
            [sys.executable, "-c", CLI_BOOTSTRAP, *self.command],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.env,
        )
        for line in self.process.stdout:
            self.parent.output_queue.append((self.name, line))

    sc.SubCommand.run = run  # type: ignore[method-assign]
    print("[airflow_win_patch] standalone 子进程改为 python -m 入口", flush=True)


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return

    import httpx

    original = httpx.Client.build_request

    def build_request(self, method, url, **kwargs):  # type: ignore[no-untyped-def]
        headers = kwargs.get("headers")
        content = kwargs.get("content")
        # 只处理「显式给了 content 字节/字符串」这一种情况；
        # json= / data= 由 httpx 自己设置 content-type，不能覆盖。
        has_body = content is not None and kwargs.get("json") is None and kwargs.get("data") is None
        if has_body:
            if headers is None:
                kwargs["headers"] = {"Content-Type": "application/json"}
            else:
                lowered = {str(k).lower() for k in headers}
                if "content-type" not in lowered:
                    headers = dict(headers)
                    headers["Content-Type"] = "application/json"
                    kwargs["headers"] = headers
        return original(self, method, url, **kwargs)

    httpx.Client.build_request = build_request  # type: ignore[method-assign]
    _APPLIED = True
    print("[airflow_win_patch] 已补齐 Execution API 的 Content-Type 请求头", flush=True)

    # 只有 standalone 需要拉起 airflow 子进程，其它命令不引入额外导入
    import sys

    if "standalone" in sys.argv:
        _patch_standalone()


apply()
