@echo off
rem =====================================================================
rem  pgy_dw - one click start of Airflow (Scheduler + Web UI) on Windows
rem  Usage: double click, then open http://localhost:8080
rem  Login: admin  /  password in
rem         C:\Temp\airflow_home\simple_auth_manager_passwords.json.generated
rem  This is an EXPERIMENTAL native-Windows setup. Linux/Docker is the
rem  officially supported way; see airflow\README_WINDOWS.md
rem =====================================================================
setlocal
cd /d "%~dp0.."

rem --- locate a Python 3.10+ interpreter (Anaconda 3.9 on PATH is too old) ---
set "PY="
set "CAND=%USERPROFILE%\.workbuddy\binaries\python\versions\3.13.12\python.exe"
if exist "%CAND%" set "PY=%CAND%"
if not defined PY (
  for /d %%D in ("%LOCALAPPDATA%\Programs\Python\Python3*") do (
    if exist "%%~fD\python.exe" if not defined PY set "PY=%%~fD\python.exe"
  )
)
if not defined PY (
  for /d %%D in ("C:\Python3*") do (
    if exist "%%~fD\python.exe" if not defined PY set "PY=%%~fD\python.exe"
  )
)
if not defined PY (
  echo [ERROR] Python 3.10+ not found.
  echo         Install Python 3.13 or set PY manually, e.g.
  echo         set PY=D:\Python313\python.exe ^&^& start_airflow.bat
  pause
  exit /b 1
)

echo [1/3] interpreter : %PY%
"%PY%" -c "import sys; assert sys.version_info[:2] >= (3,10), sys.version" || (echo [ERROR] need Python 3.10+ & pause & exit /b 1)

echo [2/3] checking Airflow libs ...
"%PY%" airflow\start_windows.py check || (echo [ERROR] Airflow not runnable, see airflow\README_WINDOWS.md & pause & exit /b 1)

echo [3/3] starting standalone (Scheduler + Web UI) ...
echo       open http://localhost:8080   (Ctrl+C to stop)
"%PY%" airflow\start_windows.py standalone
pause
