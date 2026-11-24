@echo off
rem Starts Quaero locally: FastAPI (127.0.0.1:8000) + React dev server (127.0.0.1:3000).
rem Wraps scripts\start-local.ps1. Close this window or press Ctrl+C to stop both servers.
setlocal
cd /d "%~dp0"

if not exist "frontend\node_modules" (
    echo Installing frontend dependencies ^(npm ci^)...
    pushd frontend
    call npm ci
    if errorlevel 1 (
        popd
        echo npm ci failed.
        pause
        exit /b 1
    )
    popd
)

rem Open the browser after the servers have had time to come up.
start "" /b cmd /c "timeout /t 12 /nobreak >nul & start "" http://127.0.0.1:3000"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\start-local.ps1"
echo.
echo Quaero stopped.
pause
