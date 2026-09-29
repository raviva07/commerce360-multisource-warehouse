@echo off
setlocal
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo Creating virtual environment...
  python -m venv .venv || goto :error
)
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip || goto :error
python -m pip install -r requirements.txt || goto :error
python run_local.py || goto :error
echo.
echo Project run completed successfully.
pause
exit /b 0
:error
echo.
echo Project run failed. Read QUICK_START.md and the error above.
pause
exit /b 1
