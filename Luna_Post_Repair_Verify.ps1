<#
Luna Post-Repair Verification
Read-only except for writing a report under D:\SurgeApp\logs.
Run after Claude repairs Luna to prove worker import, service health, and queue status.
#>

$ErrorActionPreference = "Continue"
$ProjectDir = "D:\SurgeApp"
$LogsDir = Join-Path $ProjectDir "logs"
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$ReportPath = Join-Path $LogsDir "luna_post_repair_verify_$Timestamp.txt"

function Add-Line {
    param([string]$Text = "")
    $Text | Tee-Object -FilePath $ReportPath -Append
}

function Section {
    param([string]$Name)
    Add-Line ""
    Add-Line "============================================================"
    Add-Line $Name
    Add-Line "============================================================"
}

function Status-Line {
    param(
        [string]$Status,
        [string]$Message
    )
    Add-Line ("[{0}] {1}" -f $Status, $Message)
}

function Get-RealPython {
    $Candidates = @(
        (Join-Path $ProjectDir ".aider_venv\Scripts\python.exe"),
        (Join-Path $ProjectDir ".venv\Scripts\python.exe"),
        "C:\Python311\python.exe"
    )
    foreach ($Candidate in $Candidates) {
        if (Test-Path $Candidate) {
            try {
                $Item = Get-Item $Candidate -ErrorAction Stop
                if ($Item.Length -gt 0 -and $Candidate.ToLowerInvariant() -notlike "*windowsapps*") {
                    return $Candidate
                }
            } catch {}
        }
    }
    return "python"
}

function Run-Cmd {
    param(
        [string]$Label,
        [string]$FilePath,
        [string[]]$Arguments,
        [int]$TimeoutSeconds = 30
    )
    Add-Line ""
    Add-Line ("> {0}" -f $Label)
    Add-Line ("  command: {0} {1}" -f $FilePath, ($Arguments -join " "))
    # Write any -c <code> argument to a temp file to avoid PS5/PS7 quoting differences.
    $tmpScript = $null
    $finalArgs = [System.Collections.Generic.List[string]]::new()
    $i = 0
    while ($i -lt $Arguments.Count) {
        if ($Arguments[$i] -eq '-c' -and ($i + 1) -lt $Arguments.Count) {
            $tmpScript = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), [System.IO.Path]::GetRandomFileName() + '.py')
            [System.IO.File]::WriteAllText($tmpScript, $Arguments[$i + 1], [System.Text.Encoding]::UTF8)
            $finalArgs.Add($tmpScript)
            $i += 2
        } else {
            $finalArgs.Add($Arguments[$i])
            $i++
        }
    }
    try {
        # Build argument string — compatible with .NET Framework 4.x (PS 5.1) and .NET Core (PS 7)
        # ProcessStartInfo.ArgumentList only exists in .NET 5+; use .Arguments (string) instead.
        $escapedArgs = $finalArgs | ForEach-Object {
            $a = $_
            if ($a -match '[\s"]') { '"' + ($a -replace '"', '\"') + '"' } else { $a }
        }
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = $FilePath
        $psi.Arguments = $escapedArgs -join ' '
        $psi.WorkingDirectory = $ProjectDir
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        $psi.UseShellExecute = $false
        $psi.CreateNoWindow = $true
        $proc = New-Object System.Diagnostics.Process
        $proc.StartInfo = $psi
        [void]$proc.Start()
        if (-not $proc.WaitForExit($TimeoutSeconds * 1000)) {
            try { $proc.Kill() } catch {}
            Add-Line "  EXIT: TIMEOUT"
            return @{ ok = $false; code = 124; stdout = ""; stderr = "timeout" }
        }
        $stdout = $proc.StandardOutput.ReadToEnd()
        $stderr = $proc.StandardError.ReadToEnd()
        Add-Line ("  EXIT: {0}" -f $proc.ExitCode)
        if ($stdout.Trim()) {
            Add-Line "  STDOUT:"
            ($stdout.TrimEnd() -split "`r?`n") | ForEach-Object { Add-Line ("    " + $_) }
        }
        if ($stderr.Trim()) {
            Add-Line "  STDERR:"
            ($stderr.TrimEnd() -split "`r?`n") | ForEach-Object { Add-Line ("    " + $_) }
        }
        return @{ ok = ($proc.ExitCode -eq 0); code = $proc.ExitCode; stdout = $stdout; stderr = $stderr }
    } catch {
        Add-Line ("  ERROR: {0}" -f $_.Exception.Message)
        return @{ ok = $false; code = 999; stdout = ""; stderr = $_.Exception.Message }
    } finally {
        if ($tmpScript -and (Test-Path $tmpScript)) {
            Remove-Item $tmpScript -Force -ErrorAction SilentlyContinue
        }
    }
}

function Count-JsonFiles {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return 0 }
    return @((Get-ChildItem -Path $Path -Filter "*.json" -File -ErrorAction SilentlyContinue)).Count
}

function Tail-File {
    param([string]$Path, [int]$Lines = 25)
    if (-not (Test-Path $Path)) {
        Add-Line "missing: $Path"
        return
    }
    try {
        Get-Content -Path $Path -Tail $Lines -ErrorAction Stop | ForEach-Object { Add-Line $_ }
    } catch {
        Add-Line ("tail error: {0}" -f $_.Exception.Message)
    }
}

New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null
if (Test-Path $ReportPath) { Remove-Item $ReportPath -Force }

Add-Line "Luna Post-Repair Verification"
Add-Line ("Generated: {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Add-Line ("Project:   {0}" -f $ProjectDir)
Add-Line ("Report:    {0}" -f $ReportPath)

$HardFailures = New-Object System.Collections.Generic.List[string]
$Warnings = New-Object System.Collections.Generic.List[string]

Section "1. Project and kill switch"
if (Test-Path $ProjectDir) {
    Status-Line "PASS" "Project folder exists: $ProjectDir"
} else {
    Status-Line "FAIL" "Project folder missing: $ProjectDir"
    [void]$HardFailures.Add("Project folder missing")
}
$KillSwitch = Join-Path $ProjectDir "LUNA_STOP_NOW.flag"
if (Test-Path $KillSwitch) {
    Status-Line "FAIL" "Kill switch is present: $KillSwitch"
    [void]$HardFailures.Add("Kill switch present")
} else {
    Status-Line "PASS" "Kill switch is not present."
}

Section "2. Python and import contract"
$PythonExe = Get-RealPython
Add-Line ("Python selected: {0}" -f $PythonExe)

$HygieneCheckCode = @'
import sys
sys.path.insert(0, r'D:\SurgeApp')
import luna_modules.luna_hygiene as h
required = [
    'HYGIENE_ASSIGN_BANNED_FRAGMENTS',
    'HYGIENE_BANNED_NAME_FRAGMENTS',
    'HYGIENE_IDENTIFIER_SUFFIX_BLOCKLIST',
    'HYGIENE_LOCAL_STRING_ASSIGN_MAX_LINES',
    'HYGIENE_NESTED_FUNCTION_MAX_LINES',
    'HygieneVisitor',
    'LEGACY_HYGIENE_WHITELIST',
    '_hygiene_check_assignment',
    '_hygiene_check_named_node',
    '_hygiene_check_nested_size',
    '_hygiene_extract_target_names',
    '_hygiene_forbidden_fragment',
    '_hygiene_forbidden_suffix',
    '_hygiene_string_literal_line_count',
]
missing = [name for name in required if not hasattr(h, name)]
if missing:
    raise SystemExit('MISSING_HYGIENE_SYMBOLS=' + ','.join(missing))
print('HYGIENE_IMPORT_CONTRACT_OK')
'@
$HygieneResult = Run-Cmd "Check luna_hygiene import contract" $PythonExe @("-c", $HygieneCheckCode) 30
if ($HygieneResult.ok) { Status-Line "PASS" "luna_hygiene exports required worker symbols." } else { Status-Line "FAIL" "luna_hygiene import contract failed."; [void]$HardFailures.Add("luna_hygiene import contract failed") }

$CoreFiles = @("worker.py", "aider_bridge.py", "luna_guardian.py", "director_agent.py", "SurgeApp_Claude_Terminal.py", "LaunchLuna.pyw", "luna_start.pyw")
foreach ($File in $CoreFiles) {
    $Path = Join-Path $ProjectDir $File
    if (Test-Path $Path) {
        $Result = Run-Cmd "py_compile $File" $PythonExe @("-m", "py_compile", $Path) 60
        if ($Result.ok) { Status-Line "PASS" "py_compile passed: $File" } else { Status-Line "FAIL" "py_compile failed: $File"; [void]$HardFailures.Add("py_compile failed: $File") }
    } else {
        Status-Line "WARN" "Core file not found: $File"
        [void]$Warnings.Add("Core file not found: $File")
    }
}

$WorkerImportCode = "import sys; sys.path.insert(0, r'D:\SurgeApp'); import worker; print('IMPORT_OK')"
$WorkerImport = Run-Cmd "Import worker.py" $PythonExe @("-c", $WorkerImportCode) 90
if ($WorkerImport.ok -and $WorkerImport.stdout -match "IMPORT_OK") {
    Status-Line "PASS" "worker.py imports cleanly."
} else {
    Status-Line "FAIL" "worker.py import failed."
    [void]$HardFailures.Add("worker.py import failed")
}

Section "3. Ollama local model endpoint"
try {
    $Response = Invoke-WebRequest -Uri "http://127.0.0.1:11434/api/tags" -UseBasicParsing -TimeoutSec 8
    Status-Line "PASS" ("Ollama /api/tags responded with HTTP {0}." -f [int]$Response.StatusCode)
    $Body = $Response.Content
    if ($Body.Length -gt 600) { $Body = $Body.Substring(0, 600) + "..." }
    Add-Line $Body
} catch {
    Status-Line "WARN" ("Ollama /api/tags did not respond: {0}" -f $_.Exception.Message)
    [void]$Warnings.Add("Ollama endpoint not reachable")
}

Section "4. Service processes and duplicate storm check"
$Rows = @()
try {
    $Rows = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match '^python' -and $_.CommandLine -match 'SurgeApp|Luna|aider|worker|guardian'
    } | Select-Object ProcessId, ParentProcessId, Name, CommandLine
    if (-not $Rows) { $Rows = @() }
} catch {
    Status-Line "WARN" ("Could not query process list: {0}" -f $_.Exception.Message)
    [void]$Warnings.Add("Could not query process list")
}

# Count-LogicalInstances: treat a parent/child pair (same script, one PID is parent of other)
# as ONE logical instance. This avoids false "duplicate storm" alarms from Windows Python
# launcher spawning a child interpreter for the same script.
function Count-LogicalInstances {
    param(
        [object[]]$MatchedRows
    )
    if ($MatchedRows.Count -le 1) { return $MatchedRows.Count }
    $pids    = @($MatchedRows | ForEach-Object { [int]$_.ProcessId })
    $parents = @($MatchedRows | ForEach-Object { [int]$_.ParentProcessId })
    # If exactly 2 processes and one is the parent of the other → 1 logical instance
    if ($MatchedRows.Count -eq 2) {
        if ($parents -contains $pids[0] -or $parents -contains $pids[1]) {
            return 1
        }
    }
    return $MatchedRows.Count
}

# Logical role definitions:
#   worker_main = worker.py WITHOUT --continues-update-start
#   worker_cu   = worker.py WITH  --continues-update-start
#   Others      = simple single-marker match

# worker_main
$WorkerMainRows  = @($Rows | Where-Object {
    ($_.CommandLine -replace '/', '\') -match [regex]::Escape('worker.py') -and
    ($_.CommandLine -notmatch '--continues-update-start')
})
$WorkerMainCount = Count-LogicalInstances $WorkerMainRows
Add-Line ""
Add-Line "Logical role: worker_main (worker.py, no --continues-update-start)"
if ($WorkerMainCount -eq 0) {
    Status-Line "WARN" "worker_main not running (count=0)"
    [void]$Warnings.Add("worker_main not running")
} elseif ($WorkerMainCount -gt 1) {
    Status-Line "FAIL" "worker_main duplicate storm (logical count=$WorkerMainCount)"
    [void]$HardFailures.Add("Duplicate storm: worker_main")
} else {
    Status-Line "PASS" "worker_main logical count=$WorkerMainCount (physical=$($WorkerMainRows.Count))"
}
foreach ($M in $WorkerMainRows | Select-Object -First 4) {
    Add-Line ("  pid={0} parent={1} cmd={2}" -f $M.ProcessId, $M.ParentProcessId, ($M.CommandLine -replace "`r`n"," "))
}

# worker_cu
$WorkerCuRows    = @($Rows | Where-Object {
    ($_.CommandLine -replace '/', '\') -match [regex]::Escape('worker.py') -and
    ($_.CommandLine -match '--continues-update-start')
})
$WorkerCuCount   = Count-LogicalInstances $WorkerCuRows
Add-Line ""
Add-Line "Logical role: worker_cu (worker.py --continues-update-start)"
if ($WorkerCuCount -eq 0) {
    Status-Line "INFO" "worker_cu not running (0 or 1 expected)"
} elseif ($WorkerCuCount -gt 1) {
    Status-Line "FAIL" "worker_cu duplicate storm (logical count=$WorkerCuCount)"
    [void]$HardFailures.Add("Duplicate storm: worker_cu")
} else {
    Status-Line "PASS" "worker_cu logical count=$WorkerCuCount (physical=$($WorkerCuRows.Count))"
}
foreach ($M in $WorkerCuRows | Select-Object -First 4) {
    Add-Line ("  pid={0} parent={1} cmd={2}" -f $M.ProcessId, $M.ParentProcessId, ($M.CommandLine -replace "`r`n"," "))
}

# aider_bridge (aider_bridge.py but not "python -m aider")
$BridgeRows  = @($Rows | Where-Object {
    ($_.CommandLine -replace '/', '\') -match [regex]::Escape('aider_bridge.py')
})
$BridgeCount = Count-LogicalInstances $BridgeRows
Add-Line ""
Add-Line "Logical role: aider_bridge (aider_bridge.py)"
if ($BridgeCount -eq 0) {
    Status-Line "WARN" "aider_bridge not running (count=0)"
    [void]$Warnings.Add("aider_bridge not running")
} elseif ($BridgeCount -gt 1) {
    Status-Line "FAIL" "aider_bridge duplicate storm (logical count=$BridgeCount)"
    [void]$HardFailures.Add("Duplicate storm: aider_bridge")
} else {
    Status-Line "PASS" "aider_bridge logical count=$BridgeCount (physical=$($BridgeRows.Count))"
}
foreach ($M in $BridgeRows | Select-Object -First 4) {
    Add-Line ("  pid={0} parent={1} cmd={2}" -f $M.ProcessId, $M.ParentProcessId, ($M.CommandLine -replace "`r`n"," "))
}

# Single-instance services (guardian, terminal, apprentice, tray)
$SingleRoles = @(
    @{Name="luna_guardian";  Marker="luna_guardian.py"},
    @{Name="terminal";       Marker="SurgeApp_Claude_Terminal.py"},
    @{Name="luna_apprentice";Marker="luna_apprentice.py"},
    @{Name="luna_tray";      Marker="luna_start.pyw"}
)
foreach ($Role in $SingleRoles) {
    $RoleRows  = @($Rows | Where-Object { ($_.CommandLine -replace '/', '\') -match [regex]::Escape($Role.Marker) })
    $RoleCount = Count-LogicalInstances $RoleRows
    Add-Line ""
    Add-Line ("Logical role: {0} ({1})" -f $Role.Name, $Role.Marker)
    if ($RoleCount -eq 0) {
        Status-Line "WARN" ("{0} not running" -f $Role.Name)
        [void]$Warnings.Add("{0} not running" -f $Role.Name)
    } elseif ($RoleCount -gt 1) {
        Status-Line "FAIL" ("{0} duplicate storm (logical count={1})" -f $Role.Name, $RoleCount)
        [void]$HardFailures.Add("Duplicate storm: {0}" -f $Role.Name)
    } else {
        Status-Line "PASS" ("{0} logical count={1} (physical={2})" -f $Role.Name, $RoleCount, $RoleRows.Count)
    }
    foreach ($M in $RoleRows | Select-Object -First 3) {
        Add-Line ("  pid={0} parent={1} cmd={2}" -f $M.ProcessId, $M.ParentProcessId, ($M.CommandLine -replace "`r`n"," "))
    }
}

Section "5. Locks and heartbeat freshness"
$LockFiles = @(
    "logs\luna_worker.lock.json",
    "logs\aider_bridge.pid",
    "logs\luna_terminal.pid.json",
    "memory\luna_guardian.lock.json",
    "logs\luna_worker_heartbeat.json"
)
foreach ($Rel in $LockFiles) {
    $Path = Join-Path $ProjectDir $Rel
    if (Test-Path $Path) {
        Status-Line "INFO" "Found $Rel"
        Tail-File $Path 12
    } else {
        Status-Line "WARN" "Missing $Rel"
        [void]$Warnings.Add("Missing $Rel")
    }
}

$HeartbeatPath = Join-Path $LogsDir "luna_worker_heartbeat.json"
if (Test-Path $HeartbeatPath) {
    try {
        $Heartbeat = Get-Content $HeartbeatPath -Raw | ConvertFrom-Json
        if ($Heartbeat.ts) {
            $HeartbeatTime = [datetime]::Parse($Heartbeat.ts)
            $AgeSeconds = [int]((Get-Date) - $HeartbeatTime).TotalSeconds
            Add-Line ("Heartbeat age seconds: {0}" -f $AgeSeconds)
            if ($AgeSeconds -le 180) {
                Status-Line "PASS" "Worker heartbeat is fresh."
            } else {
                Status-Line "WARN" "Worker heartbeat is stale."
                [void]$Warnings.Add("Worker heartbeat stale")
            }
        }
    } catch {
        Status-Line "WARN" ("Could not parse heartbeat: {0}" -f $_.Exception.Message)
        [void]$Warnings.Add("Could not parse heartbeat")
    }
}

Section "6. Queue counts"
$QueuePaths = @(
    "tasks\active", "tasks\done", "tasks\failed",
    "aider_jobs\active", "aider_jobs\done", "aider_jobs\failed", "aider_jobs\quarantine",
    "director_jobs\active", "director_jobs\done", "director_jobs\failed", "director_jobs\quarantine"
)
foreach ($Rel in $QueuePaths) {
    $Path = Join-Path $ProjectDir $Rel
    $Count = Count-JsonFiles $Path
    Add-Line ("{0,-32} {1}" -f $Rel, $Count)
}
$AiderActive = Count-JsonFiles (Join-Path $ProjectDir "aider_jobs\active")
if ($AiderActive -gt 0) {
    Status-Line "WARN" "Aider active jobs remain. Check if they are current or stale."
    [void]$Warnings.Add("Aider active jobs remain")
}

Section "7. Recent log tails"
Add-Line "--- luna_guardian.log tail ---"
Tail-File (Join-Path $LogsDir "luna_guardian.log") 30
Add-Line ""
Add-Line "--- luna_worker.log tail ---"
Tail-File (Join-Path $LogsDir "luna_worker.log") 30
Add-Line ""
Add-Line "--- aider_bridge.log tail ---"
Tail-File (Join-Path $LogsDir "aider_bridge.log") 40
Add-Line ""
Add-Line "--- luna_live_feed.jsonl tail ---"
Tail-File (Join-Path $LogsDir "luna_live_feed.jsonl") 30

Section "8. Summary"
if ($HardFailures.Count -eq 0) {
    Status-Line "PASS" "No hard failures found."
} else {
    Status-Line "FAIL" ("Hard failures: {0}" -f $HardFailures.Count)
    foreach ($Failure in $HardFailures) { Add-Line ("  - " + $Failure) }
}
if ($Warnings.Count -eq 0) {
    Status-Line "PASS" "No warnings found."
} else {
    Status-Line "WARN" ("Warnings: {0}" -f $Warnings.Count)
    foreach ($Warning in $Warnings) { Add-Line ("  - " + $Warning) }
}

Add-Line ""
Add-Line "Next read:"
Add-Line $ReportPath

if ($HardFailures.Count -eq 0) { exit 0 } else { exit 1 }
