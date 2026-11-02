@echo off
cd /d "%~dp0"
if not exist venv\Scripts\python.exe (
  py -3 -m venv venv
  venv\Scripts\python.exe -m pip install -r requirements.txt
)
echo LIVE: bot will place REAL orders. Ctrl+C to cancel.
timeout /t 5
venv\Scripts\python.exe bot.py live
pause
