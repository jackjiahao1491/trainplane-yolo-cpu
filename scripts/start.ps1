param([switch]$ApiOnly, [switch]$WebOnly)
$ErrorActionPreference = 'Stop'
if ($ApiOnly -and $WebOnly) { throw 'ApiOnly and WebOnly cannot be combined.' }
$TaskRoot = Split-Path $PSScriptRoot -Parent
$TaskBackend = Join-Path $TaskRoot 'backend'
$TaskPython = Join-Path $TaskBackend '.venv\Scripts\python.exe'
$TaskRuntime = Join-Path $TaskRoot '.runtime'
$TaskManifest = Join-Path $TaskRuntime 'processes.json'
$TaskRayTemp = Join-Path ([System.IO.Path]::GetPathRoot($TaskRoot)) 'ray-trainplane'
if (!$WebOnly -and !(Test-Path -LiteralPath $TaskPython)) { throw 'Run the README dependency installation first: backend\.venv is missing.' }
New-Item -ItemType Directory -Force -Path $TaskRuntime | Out-Null
$TaskAllowed = @('REDIS_URL','RAY_ADDRESS','TOTAL_CPUS','HEARTBEAT_TIMEOUT','HEARTBEAT_STARTUP_GRACE','SCHEDULER_INTERVAL','TRAINPLANE_DATA_DIR')
$TaskEnvFile = Join-Path $TaskRoot '.env'
if (Test-Path -LiteralPath $TaskEnvFile) {
    foreach ($TaskLine in Get-Content -LiteralPath $TaskEnvFile) {
        if ($TaskLine -match '^\s*([A-Z_]+)\s*=(.*)$' -and $Matches[1] -in $TaskAllowed -and $Matches[2].Trim()) {
            [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2].Trim(), 'Process')
        }
    }
}
if (!$env:REDIS_URL) { $env:REDIS_URL = 'redis://127.0.0.1:6379/0' }
if (!$env:RAY_ADDRESS) { $env:RAY_ADDRESS = 'http://127.0.0.1:8265' }
if (!$env:TOTAL_CPUS) { $env:TOTAL_CPUS = '2' }
if (!$env:TRAINPLANE_DATA_DIR) { $env:TRAINPLANE_DATA_DIR = Join-Path $TaskBackend 'data' }
$env:PYTHONUNBUFFERED = '1'
$env:PYTHONUTF8 = '1'
$env:CUDA_VISIBLE_DEVICES = '-1'
if ($env:OS -eq 'Windows_NT') { $env:RAY_USE_OLD_GCS_CLIENT = '1' }
$TaskRecords = @()
function Read-TaskRecords($TaskValue) {
    foreach ($TaskRow in $TaskValue) {
        if ($TaskRow.name -and $TaskRow.pid -and $TaskRow.started_at) {
            # Rebuild plain objects: Windows PowerShell 5 otherwise retains array wrappers.
            $script:TaskRecords += [pscustomobject]@{name=[string]$TaskRow.name; pid=[int]$TaskRow.pid; started_at=[string]$TaskRow.started_at}
        } elseif ($TaskRow.value) {
            Read-TaskRecords $TaskRow.value
        } else { throw 'Invalid owned process record; refusing to overwrite the manifest.' }
    }
}
if (Test-Path -LiteralPath $TaskManifest) {
    $TaskDecoded = ConvertFrom-Json -InputObject (Get-Content -Raw -LiteralPath $TaskManifest)
    Read-TaskRecords $TaskDecoded
}
function Test-TaskHttp([string]$Url) {
    try { $null = Invoke-WebRequest -Uri $Url -TimeoutSec 2 -UseBasicParsing; return $true } catch { return $false }
}
function Start-TaskOwned([string]$Name, [string]$Executable, [string]$Arguments, [string]$Directory) {
    $TaskProcess = Start-Process -FilePath $Executable -ArgumentList $Arguments -WorkingDirectory $Directory -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $TaskRuntime "$Name.stdout.log") -RedirectStandardError (Join-Path $TaskRuntime "$Name.stderr.log")
    $script:TaskRecords = @($script:TaskRecords | Where-Object name -ne $Name) + @([pscustomobject]@{name=$Name; pid=$TaskProcess.Id; started_at=$TaskProcess.StartTime.ToUniversalTime().ToString('o')})
    ConvertTo-Json -InputObject @($script:TaskRecords) | Set-Content -Encoding utf8 -LiteralPath $TaskManifest
}
function Wait-TaskHttp([string]$Url, [int]$Seconds, [string]$Name) {
    $TaskDeadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $TaskDeadline) {
        if (Test-TaskHttp $Url) { return }
        Start-Sleep -Seconds 1
    }
    throw "$Name did not become ready. Inspect $TaskRuntime\$Name.stderr.log and $TaskRuntime\$Name.stdout.log"
}
if (!$ApiOnly -and !$WebOnly) {
    # Only the project's Compose service is started; do not alter other containers.
    & docker compose -f (Join-Path $TaskRoot 'compose.yaml') up -d --wait redis
    if ($LASTEXITCODE -ne 0) { throw 'Redis could not start. Start Docker Desktop (Linux containers), then retry.' }
    if (!(Test-TaskHttp "$env:RAY_ADDRESS/api/version")) {
        Start-TaskOwned 'ray' $TaskPython "-m ray.scripts.scripts start --head --block --num-cpus=$env:TOTAL_CPUS --num-gpus=0 --port=6380 --dashboard-host=127.0.0.1 --dashboard-port=8265 --object-store-memory=83886080 --disable-usage-stats --temp-dir=$TaskRayTemp" $TaskBackend
        Wait-TaskHttp "$env:RAY_ADDRESS/api/version" 90 'ray'
    }
}
if (!$WebOnly -and !(Test-TaskHttp 'http://127.0.0.1:8000/api/health')) {
    Start-TaskOwned 'api' $TaskPython '-m uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1' $TaskBackend
    Wait-TaskHttp 'http://127.0.0.1:8000/api/health' 60 'api'
}
if (!$ApiOnly -and !(Test-TaskHttp 'http://127.0.0.1:5173')) {
    $TaskNode = (Get-Command node.exe).Source
    $TaskNpm = Join-Path (Split-Path (Get-Command npm.cmd).Source -Parent) 'node_modules\npm\bin\npm-cli.js'
    Start-TaskOwned 'web' $TaskNode "`"$TaskNpm`" run dev -- --host 127.0.0.1 --port 5173 --strictPort" (Join-Path $TaskRoot 'frontend')
    Wait-TaskHttp 'http://127.0.0.1:5173' 30 'web'
}
if (!$WebOnly) { Write-Host 'API: http://127.0.0.1:8000/docs' }
if (!$ApiOnly) { Write-Host 'Web: http://127.0.0.1:5173' }
Write-Host "Logs and owned process IDs: $TaskRuntime"

