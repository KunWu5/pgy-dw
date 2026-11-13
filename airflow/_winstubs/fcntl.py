"""Windows 本机调试用的 fcntl 替身（Airflow 3.x 官方不支持 Windows）。

airflow/api_fastapi/auth/managers/simple/simple_auth_manager.py 在生成
simple auth 密码文件时用 ``import fcntl`` + ``fcntl.flock`` 做文件锁。
Windows 没有 fcntl 模块，导致 db migrate / dags test 直接失败。

这里提供最小实现：常量 + no-op flock。本机单用户、单进程调试不会有并发写冲突；
**生产环境请用 Linux / Docker，不要使用这个替身**。
"""
from __future__ import annotations

LOCK_SH = 1
LOCK_EX = 2
LOCK_NB = 4
LOCK_UN = 8


def flock(fd, operation):  # noqa: ARG001
    """Windows 上不提供 advisory lock；调试时退化为无操作。"""
    return None


def fcntl(fd, cmd, arg=0):  # noqa: ARG001
    return 0


def ioctl(fd, request, arg=0, mutate_flag=True):  # noqa: ARG001
    return 0
