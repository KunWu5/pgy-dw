"""Windows 本机调试用的 pwd 替身（Airflow 3.x 官方不支持 Windows）。

``airflow api-server`` 会 import ``daemon``（python-daemon），后者在模块级
``import pwd``。Windows 没有这个 Unix 模块，于是 standalone 的 api-server
子进程直接 ModuleNotFoundError。

这里用当前登录用户构造一个最小 struct_passwd，满足 ``getpwuid`` / ``getpwnam``。
**仅调试用，生产请用 Linux / Docker。**
"""
from __future__ import annotations

import getpass
import os
from collections import namedtuple

struct_passwd = namedtuple(
    "struct_passwd", "pw_name pw_passwd pw_uid pw_gid pw_gecos pw_dir pw_shell"
)


def _current() -> struct_passwd:
    try:
        name = getpass.getuser()
    except Exception:  # noqa: BLE001
        name = os.environ.get("USERNAME", "airflow")
    return struct_passwd(
        name, "x", os.getuid() if hasattr(os, "getuid") else 1000,
        os.getgid() if hasattr(os, "getgid") else 1000,
        name, os.environ.get("USERPROFILE", os.path.expanduser("~")), "cmd.exe",
    )


def getpwuid(uid):  # noqa: ARG001
    return _current()


def getpwnam(name):  # noqa: ARG001
    return _current()
