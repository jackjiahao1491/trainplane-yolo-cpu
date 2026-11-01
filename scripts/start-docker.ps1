param([switch]$ApiOnly, [switch]$Build)
$ErrorActionPreference = 'Stop'
$TaskRoot = Split-Path $PSScriptRoot -Parent
$TaskCompose = Join-Path $TaskRoot 'compose.yaml'
if ($ApiOnly) {
    & docker compose -f $TaskCompose restart api
} else {
    if (!(Test-Path -PathType Leaf -LiteralPath (Join-Path $TaskRoot 'backend\yolov8n.pt'))) {
        throw 'Model file is missing. Run backend\.venv\Scripts\python.exe scripts\prepare_training.py --mirror first.'
    }
    $TaskArguments = @('compose', '-f', $TaskCompose, 'up', '-d', '--wait', '--wait-timeout', '180')
    if ($Build) { $TaskArguments += '--build' }
    $TaskArguments += @('redis', 'ray', 'api')
    & docker @TaskArguments
}
if ($LASTEXITCODE -ne 0) { throw 'Project Docker services did not start. Inspect docker compose logs api ray.' }
$TaskDeadline = (Get-Date).AddSeconds(90)
$TaskReady = $false
while ((Get-Date) -lt $TaskDeadline) {
    try {
        $TaskHealth = Invoke-RestMethod -Uri 'http://127.0.0.1:8000/api/health' -TimeoutSec 3
        if ($TaskHealth.status -eq 'ok') { $TaskReady = $true; break }
    } catch { }
    Start-Sleep -Seconds 1
}
if (!$TaskReady) { throw 'API dependencies did not become healthy. Inspect docker compose logs api ray redis.' }
if (!$ApiOnly) { & (Join-Path $PSScriptRoot 'start.ps1') -WebOnly }
Write-Host 'API (Docker Linux): http://127.0.0.1:8000/docs'
if (!$ApiOnly) { Write-Host 'Web (Windows Node): http://127.0.0.1:5173' }

