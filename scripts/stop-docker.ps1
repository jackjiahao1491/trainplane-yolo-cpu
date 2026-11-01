param([switch]$ApiOnly, [switch]$StopRedis)
$ErrorActionPreference = 'Stop'
$TaskRoot = Split-Path $PSScriptRoot -Parent
$TaskCompose = Join-Path $TaskRoot 'compose.yaml'
if ($ApiOnly) {
    & docker compose -f $TaskCompose stop api
} else {
    & (Join-Path $PSScriptRoot 'stop.ps1') -WebOnly
    $TaskServices = @('api', 'ray')
    if ($StopRedis) { $TaskServices += 'redis' }
    & docker compose -f $TaskCompose stop @TaskServices
}
if ($LASTEXITCODE -ne 0) { throw 'Some project Docker services could not stop.' }
# Preserve all volumes and ./backend/data. Never run global docker stop/prune.
