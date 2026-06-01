<#
  Install_Luna_Self_Improvement_Task.ps1

  Registers (OPT-IN) a recurring Windows Scheduled Task that re-runs Luna's
  self-verification sweep so smoke-test coverage stays at ~100% as new
  cognitive modules are added, and any newly-broken module gets re-flagged.

  SAFETY (this codebase has documented autonomous-cascade scar tissue):
    * DEFAULT = PreviewOnly. Nothing is installed unless you pass -Install.
    * LeastPrivilege (no admin needed). User-scope task only.
    * Runs pythonw.exe with a NO-print inline command (honors the hard
      "no print() under pythonw" rule — results go to files, not stdout).
    * The engine itself is bounded, reversible (only writes self_tests\),
      NEVER-raises, and obeys the kill-switch file:
        memory\kill_switches\luna_self_improvement.disabled
    * Does NOT touch the launcher chain, services, flags, or the vocab DB.

  USAGE:
    Preview (default):   powershell -File Install_Luna_Self_Improvement_Task.ps1
    Install nightly 3am: powershell -File Install_Luna_Self_Improvement_Task.ps1 -Install
    Remove:              powershell -File Install_Luna_Self_Improvement_Task.ps1 -Remove
#>
[CmdletBinding()]
param(
    [switch]$Install,
    [switch]$Remove,
    [string]$Time = "03:00"
)

$ErrorActionPreference = "Stop"
$TaskName = "LunaSelfImprovementSweepUser"
$Root     = "D:\SurgeApp"
$PyW      = Join-Path $Root ".aider_venv\Scripts\pythonw.exe"
$Inline   = "import sys; sys.path.insert(0, r'$Root'); " +
            "from luna_modules import luna_self_improvement as si; " +
            "si.run_overnight()"

Write-Host "=== Luna Self-Improvement Scheduled Task ==="
Write-Host "Task name : $TaskName"
Write-Host "Runtime   : $PyW"
Write-Host "Schedule  : daily @ $Time (user-scope, LeastPrivilege)"
Write-Host "Kill-switch: $Root\memory\kill_switches\luna_self_improvement.disabled"
Write-Host ""

if (-not (Test-Path $PyW)) {
    Write-Host "WARNING: pythonw.exe not found at $PyW" -ForegroundColor Yellow
}

if ($Remove) {
    try {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop
        Write-Host "REMOVED scheduled task '$TaskName'." -ForegroundColor Green
    } catch {
        Write-Host "No task named '$TaskName' to remove (or removal failed): $_" -ForegroundColor Yellow
    }
    return
}

if (-not $Install) {
    Write-Host "PREVIEW ONLY — nothing installed." -ForegroundColor Cyan
    Write-Host "Would register a daily task running:"
    Write-Host "  `"$PyW`" -c `"$Inline`""
    Write-Host ""
    Write-Host "Re-run with -Install to actually register it." -ForegroundColor Cyan
    return
}

# --- Actual install (only reached with -Install) ---
$action  = New-ScheduledTaskAction -Execute $PyW -Argument "-c `"$Inline`""
$trigger = New-ScheduledTaskTrigger -Daily -At $Time
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
            -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
            -ExecutionTimeLimit (New-TimeSpan -Hours 7)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal `
    -Description "Luna self-verification sweep (bounded, reversible, kill-switchable)." `
    -Force | Out-Null

Write-Host "INSTALLED daily self-verification sweep at $Time." -ForegroundColor Green
Write-Host "Disable anytime: create the kill-switch file, or run this script with -Remove."
