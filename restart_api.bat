@echo off
echo Restarting API...
taskkill /F /IM python.exe 2>nul
timeout /t 2 /nobreak > nul
echo Starting fresh API...
python unified_api_correct.py