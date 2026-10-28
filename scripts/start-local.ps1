<#
.SYNOPSIS
    Launch Quaero Local Operations (FastAPI + React Frontend) on Windows loopback.

.DESCRIPTION
    Starts the Quaero FastAPI backend on 127.0.0.1:8000 and the Vite frontend
    server on 127.0.0.1:3000 without exposing either service on the LAN.
    Uses QUAERO_API_KEY from the environment, or generates an ephemeral
    cryptographically random session key in memory if not set.

.PARAMETER CheckOnly
    Validate Python, Node.js, dependencies, and configuration without starting servers.
#>
[CmdletBinding()]
param(
    [switch]$CheckOnly
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  QUAERO v0.8.0 - LOCAL OPERATIONS LAUNCHER (Loopback Only) " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# 1. Resolve Python interpreter
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (Test-Path $VenvPython) {
    $PythonExe = $VenvPython
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $PythonExe = (Get-Command python).Source
} else {
    Write-Error "Python interpreter not found. Create a virtual environment in .venv or install Python 3.11+."
    exit 1
}

# Verify Python dependencies
& $PythonExe -c "import fastapi, uvicorn, pandas, pyarrow, multipart" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Required Python packages are missing in '$PythonExe'. Run: pip install -r requirements.txt"
    exit 1
}

# 2. Verify Node.js / npm and frontend dependencies
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Error "npm is not installed or not on PATH. Install Node.js 20+ before launching Quaero."
    exit 1
}

$FrontendDir = Join-Path $RepoRoot "frontend"
$NodeModulesDir = Join-Path $FrontendDir "node_modules"
if (-not (Test-Path $NodeModulesDir)) {
    Write-Error "Frontend dependencies are not installed in 'frontend/node_modules'. Run: npm --prefix frontend install"
    exit 1
}

# 3. Configure loopback addresses and session API key (never hardcoded)
$ApiHost = "127.0.0.1"
$ApiPort = if ($env:QUAERO_API_PORT) { [int]$env:QUAERO_API_PORT } else { 8000 }
$WebHost = "127.0.0.1"
$WebPort = if ($env:QUAERO_WEB_PORT) { [int]$env:QUAERO_WEB_PORT } else { 3000 }

if ([string]::IsNullOrWhiteSpace($env:QUAERO_API_KEY)) {
    $randomBytes = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($randomBytes)
    $env:QUAERO_API_KEY = [System.BitConverter]::ToString($randomBytes).Replace("-", "").ToLowerInvariant()
    Write-Host "Auth     : QUAERO_API_KEY was not set; generated an ephemeral in-memory session key." -ForegroundColor Yellow
} else {
    Write-Host "Auth     : Using QUAERO_API_KEY from environment." -ForegroundColor Green
}

$env:QUAERO_PROJECT_ROOT = $RepoRoot
$env:QUAERO_API_URL = "http://$($ApiHost):$($ApiPort)"
$env:QUAERO_CORS_ORIGINS = "http://localhost:$($WebPort),http://127.0.0.1:$($WebPort)"

Write-Host "Python   : $PythonExe"
Write-Host "FastAPI  : http://$($ApiHost):$($ApiPort) (loopback only)"
Write-Host "Frontend : http://$($WebHost):$($WebPort) (loopback only)"

if ($CheckOnly) {
    Write-Host "OK: Pre-flight check passed." -ForegroundColor Green
    exit 0
}

$ApiProcess = $null
try {
    Write-Host "Starting Quaero FastAPI backend on $($ApiHost):$($ApiPort)..." -ForegroundColor Cyan
    $ApiArgs = @("-m", "uvicorn", "api.main:app", "--host", $ApiHost, "--port", "$ApiPort")
    $ApiProcess = Start-Process -FilePath $PythonExe -ArgumentList $ApiArgs -WorkingDirectory $RepoRoot -PassThru -NoNewWindow

    # Poll /health until ready or process exits
    $HealthUrl = "http://$($ApiHost):$($ApiPort)/health"
    $Ready = $false
    for ($i = 0; $i -lt 40; $i++) {
        if ($ApiProcess.HasExited) {
            throw "FastAPI process exited prematurely with code $($ApiProcess.ExitCode)."
        }
        try {
            $resp = Invoke-RestMethod -Uri $HealthUrl -Method Get -TimeoutSec 2 -ErrorAction Stop
            if ($resp.status -eq "ok") {
                $Ready = $true
                break
            }
        } catch {
            Start-Sleep -Milliseconds 300
        }
    }

    if (-not $Ready) {
        throw "FastAPI did not respond healthy at $HealthUrl within 12 seconds."
    }

    Write-Host "OK: FastAPI is online at http://$($ApiHost):$($ApiPort)" -ForegroundColor Green
    Write-Host "Starting Quaero React Frontend on http://$($WebHost):$($WebPort)..." -ForegroundColor Cyan
    Write-Host "Press Ctrl+C to stop both servers." -ForegroundColor Yellow

    & npm --prefix $FrontendDir run dev -- --host $WebHost --port $WebPort --strictPort
    if ($LASTEXITCODE -ne 0) {
        throw "Frontend dev server exited with code $LASTEXITCODE."
    }
} catch {
    Write-Error "Quaero startup failed: $($_.Exception.Message)"
    exit 1
} finally {
    if ($null -ne $ApiProcess -and -not $ApiProcess.HasExited) {
        Write-Host "Stopping Quaero FastAPI backend (PID $($ApiProcess.Id))..." -ForegroundColor Yellow
        Stop-Process -Id $ApiProcess.Id -Force -ErrorAction SilentlyContinue
    }
}
