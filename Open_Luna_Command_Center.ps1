# Open_Luna_Command_Center.ps1
# 2026-05-17 — One-shot user-facing launcher for the Luna Command Center.
#
# Chain (NEW):
#   Luna Command Center.lnk
#     -> wscript.exe Open_Luna_Command_Center.vbs   (silent wrapper)
#     -> powershell -File Open_Luna_Command_Center.ps1 (THIS FILE)
#         step 1: kill ALL pythonw listeners on port 8765 (handles orphans)
#         step 2: spawn fresh LaunchLuna.pyw via venv pythonw
#         step 3: poll /api/health until 200 OK or deadline
#         step 4: open default browser at http://127.0.0.1:8765
#
# Why a fresh bounce on every click:
#   The operator's edit cycle on luna_modules/*.py requires a server
#   restart for Python module changes to take effect. JS/CSS-only edits
#   would survive without a bounce, but we don't try to detect "what
#   changed" — every click guarantees a clean process listening with
#   today's code.
#
# Idempotent. Reversible (just kills and restarts; no install steps).

[CmdletBinding()]
param(
    [int]$Port                  = 8765,
    [string]$LauncherPyw        = 'D:\SurgeApp\LaunchLuna.pyw',
    [string]$VenvPythonw        = 'D:\SurgeApp\.aider_venv\Scripts\pythonw.exe',
    [int]$WaitFreeSeconds       = 10,
    [int]$HealthDeadlineSeconds = 180,  # 2026-05-20: bumped 60->180s. Dashboard cold-boot was observed to take ~120s on this host after the v4.5 voice-stack add (XTTS module + torch deps lazy-load on first import). 60s was right at the edge causing false-alarm "won't boot" warnings while the chain WAS actually working. 180s gives comfortable margin without making the launcher feel stuck.
    [string]$LandingUrl         = 'http://127.0.0.1:8765',
    [string]$LogPath            = 'D:\SurgeApp\logs\Open_Luna_Command_Center.log'
)

$ErrorActionPreference = 'Continue'
$ProgressPreference    = 'SilentlyContinue'

function Write-Log([string]$msg) {
    try {
        $line = ("{0}  {1}" -f (Get-Date -Format 'yyyy-MM-ddTHH:mm:ss'), $msg)
        $dir  = Split-Path -Parent $LogPath
        if ($dir -and -not (Test-Path -LiteralPath $dir)) {
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
        }
        Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
    } catch { }
}

function Get-DashboardListeners([int]$Port) {
    try {
        return Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    } catch { return @() }
}

function Test-DashboardHealthy([int]$Port) {
    try {
        $r = Invoke-WebRequest -Uri ("http://127.0.0.1:{0}/api/health" -f $Port) `
                               -UseBasicParsing -TimeoutSec 4 -ErrorAction SilentlyContinue
        return ($r.StatusCode -eq 200)
    } catch { return $false }
}

Write-Log "=== launcher start ==="

# 2026-06-02 Step 0: clear any "Stop Luna" flags so a fresh click always
# brings Luna fully back. Stop_Luna_Command_Center.ps1 sets these to make her
# stay closed + silent; opening the Command Center must undo them, otherwise
# the guardian/worker would idle and the wardens would stay disabled.
$stopFlags = @(
    'D:\SurgeApp\LUNA_STOP_NOW.flag',
    'D:\SurgeApp\memory\luna_dashboard_warden.disabled',
    'D:\SurgeApp\memory\luna_system_warden.disabled'
)
foreach ($sf in $stopFlags) {
    if (Test-Path -LiteralPath $sf) {
        try { Remove-Item -LiteralPath $sf -Force -ErrorAction Stop; Write-Log "cleared stop flag: $sf" }
        catch { Write-Log ("WARN: could not clear {0}: {1}" -f $sf, $_.Exception.Message) }
    }
}

# 2026-05-27 self-heal preflight (added after architectural fixes for the
# Guardian Safety Valve cascade). Runs BEFORE the dashboard bounce so the
# fresh dashboard starts on a clean process landscape:
#
#   Step 0a: Reap any orphan accumulator processes (semgrep zombies, hung
#            probe sweeps, etc). Bounded to 30s; never touches inviolate
#            services. Operator can disable via:
#              memory/kill_switches/luna_process_reaper.disabled
#   Step 0b: Kill the old luna_guardian.py so LaunchLuna.pyw respawns it
#            with current code (picks up the narrow-counting + self_heal
#            wiring + threshold-40 changes from 2026-05-27).
#
# Both steps are best-effort: if they fail, the dashboard bounce still
# proceeds. The audit trail at memory/luna_process_reaper_audit.jsonl
# captures every reap decision.

# Step 0a: orphan reap preflight
# Must run with cwd = project root so `-m luna_modules.luna_process_reaper`
# resolves (Python adds cwd to sys.path for -m). The .lnk sets the working
# dir to D:\SurgeApp, but we Push-Location explicitly so this works no
# matter how the PS1 is invoked (direct run, scheduled task, etc).
$VenvPython = 'D:\SurgeApp\.aider_venv\Scripts\python.exe'
if (Test-Path -LiteralPath $VenvPython) {
    Push-Location 'D:\SurgeApp'
    try {
        Write-Log "step 0a: running luna_process_reaper.self_heal preflight"
        $reapOutput = & $VenvPython -X utf8 -m luna_modules.luna_process_reaper heal 2>&1
        # heal returns JSON with killed_count + pre/post counts; log the tail
        $reapTail = ($reapOutput | Select-Object -Last 4) -join ' | '
        Write-Log ("step 0a result: {0}" -f $reapTail)
    } catch {
        Write-Log ("step 0a WARN: self_heal failed: {0}" -f $_.Exception.Message)
    } finally {
        Pop-Location
    }
} else {
    Write-Log ("step 0a SKIP: venv python missing at {0}" -f $VenvPython)
}

# Step 0b: bounce the Guardian so the new self-heal-wired code loads.
# Filter to the venv-pythonw guardian only (not the WindowsApps stub child
# which dies with its parent). taskkill /F so it doesn't linger.
try {
    Write-Log "step 0b: bouncing luna_guardian.py"
    $guardianProcs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine -like '*luna_guardian.py*' -and $_.CommandLine -like '*.aider_venv*' }
    if ($guardianProcs) {
        foreach ($g in $guardianProcs) {
            try {
                Write-Log ("step 0b: stopping guardian PID {0}" -f $g.ProcessId)
                taskkill /F /T /PID $g.ProcessId 2>&1 | Out-Null
            } catch {
                Write-Log ("step 0b WARN: stop PID {0} failed: {1}" -f $g.ProcessId, $_.Exception.Message)
            }
        }
        # LaunchLuna.pyw's start_if_missing will respawn the guardian
        # downstream in Step 2 below. Don't spawn it here — that would
        # race with the LaunchLuna respawn and create duplicate guardians.
    } else {
        Write-Log "step 0b: no live guardian found (LaunchLuna chain will spawn one)"
    }
} catch {
    Write-Log ("step 0b WARN: guardian bounce failed: {0}" -f $_.Exception.Message)
}

# Step 1: kill all listeners on the port (handles orphans).
$listeners = Get-DashboardListeners -Port $Port
if ($listeners) {
    $pids = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($pid_ in $pids) {
        try {
            $proc = Get-Process -Id $pid_ -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Log ("stopping PID {0} ({1})" -f $pid_, $proc.ProcessName)
                Stop-Process -Id $pid_ -Force -ErrorAction Stop
            }
        } catch {
            Write-Log ("WARN: could not stop PID {0}: {1}" -f $pid_, $_.Exception.Message)
        }
    }
    # Wait for port to free.
    $deadline = (Get-Date).AddSeconds($WaitFreeSeconds)
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-DashboardListeners -Port $Port)) { break }
        Start-Sleep -Milliseconds 350
    }
}

# Step 2: spawn fresh dashboard via pythonw (no console window).
if (-not (Test-Path -LiteralPath $VenvPythonw)) {
    Write-Log ("FATAL: venv pythonw missing at {0}" -f $VenvPythonw)
    # Try to open browser anyway in case another instance is up.
    Start-Process $LandingUrl
    exit 2
}
if (-not (Test-Path -LiteralPath $LauncherPyw)) {
    Write-Log ("FATAL: launcher missing at {0}" -f $LauncherPyw)
    Start-Process $LandingUrl
    exit 3
}

try {
    Write-Log "spawning fresh dashboard via $LauncherPyw"
    Start-Process -FilePath $VenvPythonw `
                  -ArgumentList @('"' + $LauncherPyw + '"') `
                  -WorkingDirectory 'D:\SurgeApp' `
                  -WindowStyle Hidden
} catch {
    Write-Log ("ERROR: spawn failed: {0}" -f $_.Exception.Message)
}

# Step 3: wait for health.
$deadline = (Get-Date).AddSeconds($HealthDeadlineSeconds)
$ok = $false
while ((Get-Date) -lt $deadline) {
    if (Test-DashboardHealthy -Port $Port) { $ok = $true; break }
    Start-Sleep -Milliseconds 700
}

if ($ok) {
    Write-Log ("OK: dashboard healthy on {0}" -f $LandingUrl)
} else {
    Write-Log ("WARN: dashboard did not respond on /api/health within {0}s; opening browser anyway" -f $HealthDeadlineSeconds)
}

# Step 4a (2026-05-30): autostart the Luna voice + brain stack so one
# click on the Command Center brings up speech bridge (8766), FastBrain
# llama-server (8080) + router (8771) + bridge (8773), and the FastTalk
# trio (8768/9/70). Idempotent (port-checks each service first) and
# kill-switchable via memory/kill_switches/voice_stack_autostart.disabled
# Always best-effort: a failure here NEVER blocks dashboard boot.
try {
    $voiceStack = 'D:\SurgeApp\Boot_Luna_Voice_Stack.ps1'
    if (Test-Path -LiteralPath $voiceStack) {
        Write-Log "step 4a: firing Boot_Luna_Voice_Stack.ps1"
        & $voiceStack | Out-Null
        Write-Log "step 4a: Boot_Luna_Voice_Stack.ps1 returned"
    } else {
        Write-Log "step 4a SKIP: Boot_Luna_Voice_Stack.ps1 missing"
    }
} catch {
    Write-Log ("step 4a WARN: voice stack autostart failed: {0}" -f $_.Exception.Message)
}

# Step 4b (2026-05-31): background prewarm of the voice + brain stack
# so the first user utterance after Command Center boot doesn't pay
# the XTTS / Whisper / LLM cold-load tax. Detached (does NOT block
# launcher exit). Opt out by creating
# memory\kill_switches\voice_stack_prewarm.disabled
try {
    $prewarmKillSwitch = 'D:\SurgeApp\memory\kill_switches\voice_stack_prewarm.disabled'
    $warmupBat = 'D:\SurgeApp\Warmup_Luna_FastTalk.bat'
    if (Test-Path -LiteralPath $prewarmKillSwitch) {
        Write-Log "step 4b SKIP: prewarm kill-switch present"
    } elseif (-not (Test-Path -LiteralPath $warmupBat)) {
        Write-Log "step 4b SKIP: Warmup_Luna_FastTalk.bat missing"
    } else {
        Write-Log "step 4b: firing Warmup_Luna_FastTalk.bat (detached)"
        Start-Process -FilePath "cmd.exe" -ArgumentList @('/c', $warmupBat) -WorkingDirectory "D:\SurgeApp" -WindowStyle Hidden | Out-Null
        Write-Log "step 4b: warmup spawned"
    }
} catch {
    Write-Log ("step 4b WARN: prewarm fire failed: {0}" -f $_.Exception.Message)
}

# Step 4: do NOT open browser here.
# 2026-05-17: LaunchLuna.pyw chain already opens a chromium app-window
# (honors $env:LUNA_BROWSER for picking Edge/Chrome/Brave). Opening from
# this launcher was producing a second window on top of LaunchLuna's,
# so we let the existing chain own the single browser-open.
# If LUNA_BROWSER is unset and you want this launcher to open the browser
# instead, set $OpenBrowser = $true above and uncomment the block below.
#
# try {
#     Start-Process $LandingUrl
#     Write-Log "opened browser at $LandingUrl"
# } catch {
#     Write-Log ("ERROR: browser open failed: {0}" -f $_.Exception.Message)
# }
Write-Log "browser-open skipped (deferred to LaunchLuna.pyw chain)"

Write-Log "=== launcher done ==="
exit ([int](-not $ok))
