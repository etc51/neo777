@echo off
setlocal
cd /d "%~dp0.."
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" -m neo_trader.neobitcoin_research.local_control start
) else (
  python -m neo_trader.neobitcoin_research.local_control start
)
exit /b %errorlevel%
