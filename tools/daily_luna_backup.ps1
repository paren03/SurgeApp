param(
    [string]$Source = "D:\SurgeApp",
    [string]$BackupRoot = "D:\Luna Backup"
)

$ErrorActionPreference = "Stop"

function Copy-DirectorySafe {
    param(
        [Parameter(Mandatory = $true)][string]$From,
        [Parameter(Mandatory = $true)][string]$To
    )

    if (-not (Test-Path -LiteralPath $From)) {
        return
    }

    New-Item -ItemType Directory -Force -Path $To | Out-Null
    robocopy $From $To /E /COPY:DAT /R:1 /W:1 /XD "__pycache__" ".git" "node_modules" ".aider_venv" | Out-Null
    if ($LASTEXITCODE -gt 7) {
        throw "robocopy failed for $From with code $LASTEXITCODE"
    }
    $global:LASTEXITCODE = 0
}

function Copy-FileSafe {
    param(
        [Parameter(Mandatory = $true)][string]$From,
        [Parameter(Mandatory = $true)][string]$To
    )

    if (-not (Test-Path -LiteralPath $From)) {
        return
    }

    $parent = Split-Path -Parent $To
    if ($parent) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    Copy-Item -LiteralPath $From -Destination $To -Force
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$createdAt = Get-Date
$createdAtIso = $createdAt.ToString("o")
$createdAtLocal = $createdAt.ToString("yyyy-MM-dd HH:mm:ss zzz")
$snapshot = Join-Path $BackupRoot "SurgeApp_snapshot_$stamp"
$logDir = Join-Path $BackupRoot "logs"
$logPath = Join-Path $logDir "daily_luna_backup.log"
$latestPath = Join-Path $BackupRoot "LATEST_RESTORE_POINT.json"
$indexJsonlPath = Join-Path $BackupRoot "restore_index.jsonl"
$indexCsvPath = Join-Path $BackupRoot "restore_index.csv"

New-Item -ItemType Directory -Force -Path $snapshot | Out-Null
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$directories = @(
    "luna_modules",
    "tests",
    "memory",
    "prompts",
    "tools",
    "branding",
    "mcp"
)

$files = @(
    "worker.py",
    "aider_bridge.py",
    "luna_guardian.py",
    "director_agent.py",
    "SurgeApp_Claude_Terminal.py",
    "LaunchLuna.pyw",
    "luna_start.pyw",
    "luna_start.bat",
    "Start_Aider_Bridge.bat",
    "Start_Luna_Guardian.bat",
    "Start_Luna_Apprentice.bat",
    "Start_SurgeApp.bat",
    "LUNA_BOOTSTRAP.md",
    "LUNA_BOOTSTRAP_V2.md",
    "CLAUDE.md",
    "package.json",
    "package-lock.json",
    "tsconfig.json"
)

try {
    foreach ($dir in $directories) {
        Copy-DirectorySafe -From (Join-Path $Source $dir) -To (Join-Path $snapshot $dir)
    }

    foreach ($file in $files) {
        Copy-FileSafe -From (Join-Path $Source $file) -To (Join-Path $snapshot $file)
    }

    $manifest = [ordered]@{
        created_at = $createdAtIso
        created_at_local = $createdAtLocal
        created_timezone = $createdAt.ToString("zzz")
        restore_label = "Luna restore point $createdAtLocal"
        source = $Source
        snapshot = $snapshot
        status = "ok"
        note = "Daily Luna recovery snapshot. Live files were copied only; no queues, memory, logs, backups, or staged edits were deleted."
        included_directories = $directories
        included_files = $files
        restore_hint = "Copy only the needed files back into D:\SurgeApp after making a fresh backup of the broken state."
    }
    $manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $snapshot "BACKUP_MANIFEST.json") -Encoding UTF8

    $readme = @"
# Luna Backup Snapshot

Created local: $createdAtLocal
Created ISO: $createdAtIso
Source: $Source
Snapshot: $snapshot

This is an automatic daily recovery copy for Luna core code, memory summaries, tests, launch files, tools, and configuration.

No live Luna files were deleted or moved.
Heavy runtime folders such as node_modules and .aider_venv are intentionally not copied.
"@
    $readme | Set-Content -LiteralPath (Join-Path $snapshot "README.txt") -Encoding UTF8

    $restoreRecord = [ordered]@{
        created_at = $createdAtIso
        created_at_local = $createdAtLocal
        created_timezone = $createdAt.ToString("zzz")
        snapshot = $snapshot
        source = $Source
        status = "ok"
    }
    $restoreRecord | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $latestPath -Encoding UTF8
    Add-Content -LiteralPath $indexJsonlPath -Value ($restoreRecord | ConvertTo-Json -Compress -Depth 4)
    if (-not (Test-Path -LiteralPath $indexCsvPath)) {
        "created_at,created_at_local,created_timezone,status,snapshot,source" | Set-Content -LiteralPath $indexCsvPath -Encoding UTF8
    }
    $csvLine = '"' + (($restoreRecord.created_at, $restoreRecord.created_at_local, $restoreRecord.created_timezone, $restoreRecord.status, $restoreRecord.snapshot, $restoreRecord.source) -join '","') + '"'
    Add-Content -LiteralPath $indexCsvPath -Value $csvLine

    Add-Content -LiteralPath $logPath -Value "[$createdAtLocal] OK $snapshot"
    Write-Output "BACKUP_OK=$snapshot"
    exit 0
}
catch {
    $message = "[$((Get-Date).ToString("s"))] FAILED $($_.Exception.Message)"
    Add-Content -LiteralPath $logPath -Value $message
    Write-Error $message
    exit 1
}
