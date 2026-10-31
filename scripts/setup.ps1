$ErrorActionPreference = 'Stop'
$TaskRoot = Split-Path $PSScriptRoot -Parent
$TaskPython = Join-Path $TaskRoot 'backend\.venv\Scripts\python.exe'
$TaskCache = Join-Path $TaskRoot '.cache\pip'
if (!(Test-Path -LiteralPath $TaskPython)) {
    & python -m venv (Join-Path $TaskRoot 'backend\.venv')
    if ($LASTEXITCODE -ne 0) { throw 'Python venv creation failed' }
}
& $TaskPython -m pip --cache-dir $TaskCache install --timeout 120 -r (Join-Path $TaskRoot 'backend\requirements.lock.txt')
if ($LASTEXITCODE -ne 0) { throw 'Python dependency installation failed' }
Push-Location (Join-Path $TaskRoot 'frontend')
try {
    & npm.cmd ci
    if ($LASTEXITCODE -ne 0) { throw 'Frontend dependency installation failed' }
} finally { Pop-Location }
Write-Host 'Dependencies ready. Start Docker Desktop, then run scripts/start.ps1.'
