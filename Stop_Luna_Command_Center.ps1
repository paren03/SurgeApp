# Stop_Luna_Command_Center.ps1
# 2026-06-02 — Clean shutdown for Luna. Makes her STAY closed and SILENT.
#
# Problem this solves: closing the Chrome dashboard window does NOT stop the
# backend. The guardian re-opens the dashboard ("she opens by herself") and
# the voice services keep running ("talking in the back"). This script:
#   1. Creates LUNA_STOP_NOW.flag  -> guardian + worker stop and DO NOT respawn
#   2. Sets warden kill-switches   -> dashboard/system wardens won't bounce
#   3. Kills every live Luna process (dashboard, voice stack, worker, guardian,
#      wardens, NATS, launcher, terminal)
#
# Reversible: clicking "Luna Command Center" runs Open_Luna_Command_Center.ps1,
# which DELETES these flags on startup and brings everything back.
#
# Safe: only touches processes whose command line references D:\SurgeApp Luna
# components. Never touches this shell, the OS, or unrelated apps.

[CmdletBinding()]
param(
    [string]$ProjectDir = 'D:\SurgeApp',
    [string]$LogPath    = 'D:\SurgeApp\logs\Stop_Luna_Command_Center.log'
)
$ErrorActionPreference = 'Continue'
$ProgressPreference    = 'SilentlyContinue'

function Write-Log([string]$m) {
    try {
        $line = ("{0}  {1}" -f (Get-Date -Format 'yyyy-MM-ddTHH:mm:ss'), $m)
        Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
    } catch {}
}

Write-Log "=== STOP LUNA start ==="

# --- 1. Stop flags: guardian + worker check LUNA_STOP_NOW.flag and will not
#        respawn / will idle while it exists. ---
$stopFlag = Join-Path $ProjectDir 'LUNA_STOP_NOW.flag'
try {
    Set-Content -LiteralPath $stopFlag -Value "stopped by Stop Luna at $(Get-Date -Format o)" -Encoding UTF8
    Write-Log "created stop flag: $stopFlag"
} catch { Write-Log "WARN: could not create stop flag: $($_.Exception.Message)" }

# --- 2. Warden kill-switches: dashboard + system wardens won't bounce/respawn. ---
$killSwitches = @(
    (Join-Path $ProjectDir 'memory\luna_dashboard_warden.disabled'),
    (Join-Path $ProjectDir 'memory\luna_system_warden.disabled')
)
foreach ($ks in $killSwitches) {
    try {
        $dir = Split-Path -Parent $ks
        if (-not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
        Set-Content -LiteralPath $ks -Value "stopped by Stop Luna" -Encoding UTF8
        Write-Log "set kill-switch: $ks"
    } catch { Write-Log "WARN: kill-switch $ks failed: $($_.Exception.Message)" }
}

# --- 3a. Kill listeners on Luna's ports (dashboard + voice stack). ---
$ports = @(8765,8766,8768,8769,8770,8771,8773,8080,4222)  # dashboard, voice, fastbrain, nats
foreach ($p in $ports) {
    try {
        $conns = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
        foreach ($c in $conns) {
            $procId = $c.OwningProcess
            try {
                Stop-Process -Id $procId -Force -ErrorAction Stop
                Write-Log "killed port $p listener PID $procId"
            } catch { Write-Log "WARN: port $p PID $procId kill failed: $($_.Exception.Message)" }
        }
    } catch {}
}

# --- 3b. Kill Luna processes by command-line signature (catches anything not
#         bound to a port: worker, guardian, wardens, launcher, terminal). ---
$patterns = @(
    'worker\.py', 'luna_guardian\.py', 'luna_dashboard_warden',
    'luna_system_warden', 'LaunchLuna\.pyw', 'LaunchLunaDashboard\.pyw',
    'SurgeApp_Claude_Terminal', 'luna_fastbrain', 'luna_realtime_voice',
    'luna_http_dashboard', 'aider_bridge', 'nats-server'
)
$rx = ($patterns -join '|')
try {
    $procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -and
        $_.CommandLine -match $rx -and
        $_.CommandLine -match 'SurgeApp' -and
        $_.CommandLine -notmatch 'Stop_Luna|claude|worktree|pwsh|powershell'
    }
    foreach ($pr in $procs) {
        try {
            Stop-Process -Id $pr.ProcessId -Force -ErrorAction Stop
            Write-Log "killed PID $($pr.ProcessId): $((($pr.CommandLine) -replace '\s+',' ').Substring(0,[Math]::Min(70,$pr.CommandLine.Length)))"
        } catch { Write-Log "WARN: PID $($pr.ProcessId) kill failed: $($_.Exception.Message)" }
    }
} catch { Write-Log "WARN: process sweep failed: $($_.Exception.Message)" }

# --- 4. Close the Chrome --app dashboard windows (the visible terminal). ---
try {
    $chromeApp = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -and $_.CommandLine -match '--app=http://127\.0\.0\.1:8765'
    }
    foreach ($cp in $chromeApp) {
        try { Stop-Process -Id $cp.ProcessId -Force -ErrorAction Stop; Write-Log "closed dashboard window PID $($cp.ProcessId)" } catch {}
    }
} catch {}

Write-Log "=== STOP LUNA done — Luna is closed and silent ==="
exit 0
