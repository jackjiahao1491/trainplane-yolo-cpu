param([switch]$ApiOnly, [switch]$WebOnly, [switch]$StopRedis)
$ErrorActionPreference = 'Stop'
if ($ApiOnly -and $WebOnly) { throw 'ApiOnly and WebOnly cannot be combined.' }
$TaskRoot = Split-Path $PSScriptRoot -Parent
$TaskPython = Join-Path $TaskRoot 'backend\.venv\Scripts\python.exe'
$TaskManifest = Join-Path $TaskRoot '.runtime\processes.json'
if (Test-Path -LiteralPath $TaskManifest) {
    $TaskArguments = @((Join-Path $PSScriptRoot 'stop_owned.py'), $TaskManifest)
    if ($ApiOnly) { $TaskArguments += '--api-only' }
    if ($WebOnly) { $TaskArguments += '--web-only' }
    & $TaskPython @TaskArguments
    if ($LASTEXITCODE -ne 0) { throw 'Some owned processes could not be stopped; inspect output.' }
}
if ($StopRedis) {
    & docker compose -f (Join-Path $TaskRoot 'compose.yaml') stop redis
    if ($LASTEXITCODE -ne 0) { throw 'Project Redis could not be stopped.' }
}

