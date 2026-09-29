@echo off
setlocal
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo Run START_WINDOWS.bat first so the virtual environment and packages are installed.
  pause
  exit /b 1
)
call .venv\Scripts\activate.bat
python -m pytest -q
set CODE=%ERRORLEVEL%
echo.
if %CODE%==0 (echo Tests passed.) else (echo Tests failed.)
pause
exit /b %CODE%
