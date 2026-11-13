"""Windows 本机调试用的 grp 替身（Airflow 3.x 官方不支持 Windows）。

部分依赖（python-daemon / 权限相关工具）会 ``import grp`` 读用户组。
Windows 无此模块，这里返回当前用户的一个伪组。
**仅调试用，生产请用 Linux / Docker。**
"""
from __future__ import annotations

from collections import namedtuple

struct_group = namedtuple("struct_group", "gr_name gr_passwd gr_gid gr_mem")

_GROUP = struct_group("Users", "x", 1000, [])


def getgrgid(gid):  # noqa: ARG001
    return _GROUP


def getgrnam(name):  # noqa: ARG001
    return _GROUP


def getgrall():
    return [_GROUP]
