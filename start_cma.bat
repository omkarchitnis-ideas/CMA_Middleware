@echo off
title CMA Terminal API Gateway (Port 8555)
cd /d "%~dp0"

echo ========================================================
echo   CMA Terminal API Gateway
echo   Port: 8555
echo ========================================================
echo.

if exist "C:\Users\omchit\AppData\Local\Python\bin\python.exe" (
    "C:\Users\omchit\AppData\Local\Python\bin\python.exe" cma.py
) else (
    python cma.py
)
pause
