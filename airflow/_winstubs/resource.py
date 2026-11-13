"""Windows 本机调试用的 resource 替身（Airflow 3.x 官方不支持 Windows）。

standalone 的 scheduler / dag-processor / triggerer / api-server 四个子进程都会
``import resource`` 来设置文件句柄上限（Unix 才有）。Windows 没有该模块，
子进程会 ModuleNotFoundError 直接退出。

这里提供常量 + 无害实现：limit 查询返回「很大」，设置操作直接忽略。
**仅调试用，生产请用 Linux / Docker。**
"""
from __future__ import annotations

RUSAGE_SELF = 0
RUSAGE_CHILDREN = -1

RLIMIT_CPU = 0
RLIMIT_FSIZE = 1
RLIMIT_DATA = 2
RLIMIT_STACK = 3
RLIMIT_CORE = 4
RLIMIT_RSS = 5
RLIMIT_NPROC = 6
RLIMIT_NOFILE = 7
RLIMIT_OFILE = RLIMIT_NOFILE
RLIMIT_MEMLOCK = 8
RLIMIT_AS = 9

_BIG = 1 << 31
RLIM_INFINITY = _BIG


def getrlimit(resource):  # noqa: ARG001
    return (_BIG, _BIG)


def setrlimit(resource, limits):  # noqa: ARG001
    return None


def getrusage(who):  # noqa: ARG001
    return {
        "ru_utime": 0.0, "ru_stime": 0.0, "ru_maxrss": 0,
        "ru_ixrss": 0, "ru_idrss": 0, "ru_isrss": 0,
        "ru_minflt": 0, "ru_majflt": 0, "ru_nswap": 0,
        "ru_inblock": 0, "ru_oublock": 0, "ru_msgsnd": 0,
        "ru_msgrcv": 0, "ru_nsignals": 0, "ru_nvcsw": 0, "ru_nivcsw": 0,
    }
