@echo off
setlocal
cd /d "%~dp0.."
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" -m neo_trader.neobitcoin_research.local_control stop
) else (
  python -m neo_trader.neobitcoin_research.local_control stop
)
exit /b %errorlevel%
