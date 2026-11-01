param([switch]$Native)
$ErrorActionPreference = 'Stop'
$TaskRoot = Split-Path $PSScriptRoot -Parent
$TaskPython = Join-Path $TaskRoot 'backend\.venv\Scripts\python.exe'
$TaskCache = Join-Path $TaskRoot '.cache\pip'
if (!(Test-Path -LiteralPath $TaskPython)) {
    & python -m venv (Join-Path $TaskRoot 'backend\.venv')
    if ($LASTEXITCODE -ne 0) { throw 'Python venv creation failed' }
}
if ($Native) {
    & $TaskPython -m pip --cache-dir $TaskCache install --timeout 120 -r (Join-Path $TaskRoot 'backend\requirements.lock.txt')
} else {
    # The training stack is installed in Docker; host Python only prepares public assets and runs acceptance scripts.
    & $TaskPython -m pip --cache-dir $TaskCache install --timeout 120 'requests==2.34.2' 'psutil==7.2.2'
}
if ($LASTEXITCODE -ne 0) { throw 'Python dependency installation failed' }
Push-Location (Join-Path $TaskRoot 'frontend')
try {
    & npm.cmd ci
    if ($LASTEXITCODE -ne 0) { throw 'Frontend dependency installation failed' }
} finally { Pop-Location }
Write-Host 'Dependencies ready. Prepare assets, then run scripts/start-docker.ps1 -Build.'
