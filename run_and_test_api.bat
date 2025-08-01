@echo off
echo Starting unified API server...
start /B python unified_api_correct.py
echo Waiting for server to start...
timeout /t 5 /nobreak > nul

echo.
echo Running backtest test...
curl -X POST http://localhost:8000/backtest -H "Content-Type: application/json" -d "{}"

echo.
echo Press any key to stop the server...
pause > nul
taskkill /F /IM python.exe /FI "WINDOWTITLE eq python*" 2>nul