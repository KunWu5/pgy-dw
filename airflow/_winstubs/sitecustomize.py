"""Windows 本机调试用：补齐 Airflow 3.x 依赖的 Unix signal 接口。

airflow/utils/db.py 的 timeout_with_traceback 会执行
``signal.signal(signal.SIGALRM, handler)``，而 Windows 的 signal 模块没有
SIGALRM，直接抛 AttributeError。这里把 SIGALRM 的注册退化为无操作。

Python 启动时会自动 import sitecustomize（只要所在目录在 PYTHONPATH）。
仅在 _winstubs 加入 PYTHONPATH 的本机调试场景生效；生产环境 Linux/Docker
不会加载这个目录。
"""
from __future__ import annotations

import signal as _signal

if not hasattr(_signal, "SIGALRM"):
    _signal.SIGALRM = 14
    _original_signal = _signal.signal

    def _windows_safe_signal(sig, handler):
        if sig == _signal.SIGALRM:
            return None
        return _original_signal(sig, handler)

    _signal.signal = _windows_safe_signal
    _signal.alarm = lambda seconds: 0  # noqa: ARG005
    for _name, _value in (("ITIMER_REAL", 0), ("ITIMER_VIRTUAL", 1), ("ITIMER_PROF", 2)):
        if not hasattr(_signal, _name):
            setattr(_signal, _name, _value)
    _signal.setitimer = lambda which, seconds, interval=0.0: None  # noqa: ARG005
    _signal.getitimer = lambda which: (0.0, 0.0)  # noqa: ARG005

# standalone 的四个子进程会注册 SIGQUIT / SIGUSR1 等 Unix 信号做优雅退出。
# Windows 没有这些常量，注册时统一退化为无操作。
_EXTRA_SIGNALS = {
    "SIGQUIT": getattr(_signal, "SIGBREAK", 21),
    "SIGHUP": 1,
    "SIGUSR1": 10,
    "SIGUSR2": 12,
    "SIGCHLD": 17,
    "SIGPIPE": 13,
}
_missing = {k: v for k, v in _EXTRA_SIGNALS.items() if not hasattr(_signal, k)}
if _missing:
    for _name, _value in _missing.items():
        setattr(_signal, _name, _value)
    _base_signal = _signal.signal

    def _tolerant_signal(sig, handler):  # noqa: ARG001
        if sig in _missing.values():
            return None
        return _base_signal(sig, handler)

    _signal.signal = _tolerant_signal
