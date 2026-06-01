# Luna Tier 500 Candidate Supply Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the missing safe candidate-supply lane so Luna can progress honestly from Tier 8 toward Level 10 / Tier 500.

**Architecture:** Keep the existing safety model intact. Add a dedicated Tier 500 candidate generator/refresher that produces only downstream-valid packets, wire it into scheduled/runner reporting, and update dashboard truth so Luna clearly shows whether she is progressing, paused, or blocked.

**Tech Stack:** PowerShell 5.1-compatible scripts, Python pytest/static tests, local JSON/Markdown memory reports, Windows Scheduled Tasks, Luna HTTP dashboard on `127.0.0.1:8765`.

---

## Current Diagnosis

Luna is Tier 8 because the Tier 9+ auto-upgrade engine has no promotable input.

Latest known engine state:

```text
eligible: 0
applied: 0
packets_scanned: 3896 / 3896
faststore_key_already_archived: 158
drift_mismatch: 860
noop_candidate_identical: 2878
```

The scheduled tasks are running, but the wrong side of the pipeline is active:

```text
LunaCouncilAutoPromoteUser -> promotes existing Tier 8 packets
LunaTierAutoUpgradeUser    -> scans existing Tier 9+ packets
Missing lane               -> creates fresh valid Tier 9+ / Tier 500 candidates
```

`D:\SurgeApp\Luna_Path_To_Tier500_Run.ps1` now reports the true blocker:

```text
codegen_pathway_exhausted_real_diffs_required
```

---

## File Structure

Modify:

- `D:\SurgeApp\Luna_Path_To_Tier500_Run.ps1`  
  Keep it as the Tier 500 supply orchestrator. It should preview, validate, generate candidate supply, and write `memory/tier_auto_upgrade/candidate_supply_status.json`.

- `D:\SurgeApp\Luna_Tier_AutoUpgrade_Engine.ps1`  
  Keep it as the promotion executor. Add better consumption/reporting only if needed; it should not invent work.

- `D:\SurgeApp\luna_modules\luna_http_dashboard.py`  
  Surface Tier 500 candidate supply truth, auto-upgrade truth, archive truth, and blocker state.

- `D:\SurgeApp\luna_dashboard\app.js` and `D:\SurgeApp\luna_dashboard\index.html`  
  Render the new status fields clearly.

Create:

- `D:\SurgeApp\Luna_Tier500_Candidate_Generator.ps1`  
  Dedicated generator for valid Tier 500-path candidate packets. It must not use legacy invalid Tier 6 test-target logic.

- `D:\SurgeApp\Install_Luna_Tier500_Candidate_Generator_Task.ps1`  
  Optional scheduled task installer for the generator/refresher.

- `D:\SurgeApp\tests\test_tier500_candidate_generator_static.py`  
  Static contract tests for safety, modes, target paths, and old-terminal isolation.

- `D:\SurgeApp\tests\test_tier500_dashboard_truth_static.py`  
  Static/API-shape tests for the dashboard truth fields.

Do not modify:

- `D:\SurgeApp\SurgeApp_Claude_Terminal.py`
- `D:\SurgeApp\SurgeApp_Claude_Terminal.LEGACY_DISABLED.py`
- Tier 4 floor files
- Safety/kill-switch/STOP-flag weakening paths

---

### Task 1: Lock The Safety Contract

**Files:**
- Create: `D:\SurgeApp\tests\test_tier500_candidate_generator_static.py`

- [ ] **Step 1: Add static tests that forbid unsafe shortcuts**

Create the test file with this content:

```python
from pathlib import Path

ROOT = Path(r"D:\SurgeApp")


def read(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8", errors="replace")


def test_tier500_generator_does_not_flip_live_apply_flags():
    text = read("Luna_Tier500_Candidate_Generator.ps1")
    forbidden = [
        "tier3_live_apply_enabled = $true",
        '"tier3_live_apply_enabled": true',
        "allow_live_apply = $true",
        '"allow_live_apply": true',
    ]
    lowered = text.lower()
    for phrase in forbidden:
        assert phrase.lower() not in lowered


def test_tier500_generator_does_not_touch_old_terminals():
    text = read("Luna_Tier500_Candidate_Generator.ps1").lower()
    assert "surgeapp_claude_terminal.py" not in text
    assert "surgeapp_claude_terminal.legacy_disabled.py" not in text


def test_tier500_generator_rejects_invalid_legacy_mode():
    text = read("Luna_Tier500_Candidate_Generator.ps1")
    assert "static_guard_note" not in text


def test_path_to_tier500_phase_a_not_default_enabled():
    text = read("Luna_Path_To_Tier500_Run.ps1")
    assert "Phase A: SKIPPED by default" in text
```

- [ ] **Step 2: Run the test and confirm it fails before the generator exists**

Run:

```powershell
python -m pytest tests/test_tier500_candidate_generator_static.py -q
```

Expected:

```text
FAIL
```

because `Luna_Tier500_Candidate_Generator.ps1` does not exist yet.

- [ ] **Step 3: Commit only after the generator task passes later**

Do not commit this task alone unless the repository practice allows red tests.

---

### Task 2: Create The Tier 500 Candidate Generator Skeleton

**Files:**
- Create: `D:\SurgeApp\Luna_Tier500_Candidate_Generator.ps1`

- [ ] **Step 1: Add a safe PreviewOnly generator skeleton**

Create the script with this content:

```powershell
# Luna_Tier500_Candidate_Generator.ps1
# Creates fresh, downstream-valid Tier 500-path candidate supply.
# PreviewOnly by default. Does not weaken live-apply, old terminals, STOP flags, or safety floors.

[CmdletBinding(DefaultParameterSetName='PreviewOnly')]
param(
    [Parameter(ParameterSetName='PreviewOnly')]
    [switch]$PreviewOnly,
    [Parameter(ParameterSetName='Apply')]
    [switch]$Apply,
    [int]$MaxCandidates = 25
)

$ErrorActionPreference = 'Continue'
if (-not $PreviewOnly -and -not $Apply) { $PreviewOnly = $true }

$Root = 'D:\SurgeApp'
$TierDefsPath = Join-Path $Root 'memory\tier9\luna_council_added_tiers.json'
$SupplyStatusPath = Join-Path $Root 'memory\tier_auto_upgrade\candidate_supply_status.json'
$PacketsDir = Join-Path $Root 'memory\tier7\review_packets'
$RunId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$IsoUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')

function Read-JsonSafe {
    param([string]$Path)
    try { return Get-Content -LiteralPath $Path -Raw -ErrorAction Stop | ConvertFrom-Json }
    catch { return $null }
}

function Write-Utf8NoBom {
    param([string]$Path, [string]$Content)
    $enc = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

Write-Output "=== Luna Tier 500 Candidate Generator ==="
Write-Output "mode           : $(if ($PreviewOnly) { 'PreviewOnly' } else { 'Apply' })"
Write-Output "max_candidates : $MaxCandidates"

if (-not (Test-Path $TierDefsPath)) {
    Write-Output "ERROR: missing tier definitions: $TierDefsPath"
    exit 10
}

$defs = Read-JsonSafe $TierDefsPath
if ($null -eq $defs -or $null -eq $defs.tier_definitions) {
    Write-Output "ERROR: could not parse tier definitions"
    exit 10
}

New-Item -ItemType Directory -Path $PacketsDir -Force | Out-Null

$status = [ordered]@{
    schema_version = 1
    run_id = $RunId
    generated_at = $IsoUtc
    mode = $(if ($PreviewOnly) { 'PreviewOnly' } else { 'Apply' })
    total_tier_definitions = @($defs.tier_definitions).Count
    generated_candidates = 0
    skipped_faststore_archived = 0
    skipped_tests_need_extended_runner = 0
    skipped_ps1_need_archival_mode = 0
    skipped_other_need_archival_mode = 0
    skipped_missing_target = 0
    skipped_no_safe_strategy = 0
    next_blocker = $null
}

# First safe lane: luna_modules/*.py definitions only.
# The generator emits review packets only when the target exists and is not forbidden.
$generated = 0
foreach ($def in @($defs.tier_definitions)) {
    if ($generated -ge $MaxCandidates) { break }
    $tier = [int]$def.tier
    $files = @($def.allowed_files_exact)
    if ($files.Count -eq 0) { $status.skipped_no_safe_strategy++; continue }
    $targetRel = ([string]$files[0]).Replace('\','/')

    if ($targetRel -match '^memory/luna_fast_store/keys/') {
        $status.skipped_faststore_archived++
        continue
    }
    if ($targetRel -match '^tests/test_.+\.py$') {
        $status.skipped_tests_need_extended_runner++
        continue
    }
    if ($targetRel -match '\.ps1$') {
        $status.skipped_ps1_need_archival_mode++
        continue
    }
    if (-not ($targetRel -match '^luna_modules/.+\.py$')) {
        $status.skipped_other_need_archival_mode++
        continue
    }

    $targetAbs = Join-Path $Root ($targetRel.Replace('/','\'))
    if (-not (Test-Path $targetAbs)) {
        $status.skipped_missing_target++
        continue
    }

    $packetName = "tier7_review_t500_generator_${RunId}_t${tier}.md"
    $packetPath = Join-Path $PacketsDir $packetName
    $packet = @"
# Tier 7 Review Council Packet (Tier 500 candidate generator)

- Review run_id: T500_GENERATOR_${RunId}_T${tier}
- Generated: $IsoUtc
- Source Tier 6 packet: T500_GENERATOR
- Source Tier 6 run_id: T500_GENERATOR
- Target file: $targetRel
- Candidate mode: behavior_preserving_docstring
- Lines added (sandbox copy only): 0
- Final recommendation (aggregate): **HOLD_FOR_REVIEW**

## Generator note
This packet identifies a Tier $tier codegen-pathway target. It is not promoted automatically.
It must be converted into a real sandbox candidate with a non-empty useful diff before promotion.

## Required next action
Run the Tier 6 candidate runner or a future AnyTier codegen runner against this target with a valid mode.
"@

    if (-not $PreviewOnly) {
        Write-Utf8NoBom -Path $packetPath -Content $packet
    }
    $generated++
}

$status.generated_candidates = $generated
if ($generated -eq 0) {
    $status.next_blocker = 'no_safe_codegen_targets_available'
} else {
    $status.next_blocker = 'generated_hold_for_review_codegen_targets_need_real_diffs'
}

$json = ($status | ConvertTo-Json -Depth 8)
if (-not $PreviewOnly) {
    $tmp = $SupplyStatusPath + '.tmp'
    Write-Utf8NoBom -Path $tmp -Content $json
    Move-Item -LiteralPath $tmp -Destination $SupplyStatusPath -Force
} else {
    Write-Output $json
}

Write-Output "generated_candidates: $generated"
Write-Output "next_blocker: $($status.next_blocker)"
exit 0
```

- [ ] **Step 2: Parse-check the script**

Run:

```powershell
$tokens=$null; $errors=$null
$null=[System.Management.Automation.Language.Parser]::ParseFile('D:\SurgeApp\Luna_Tier500_Candidate_Generator.ps1',[ref]$tokens,[ref]$errors)
if($errors){ $errors | Format-List; exit 1 } else { 'PowerShell parse OK' }
```

Expected:

```text
PowerShell parse OK
```

- [ ] **Step 3: Run PreviewOnly**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File D:\SurgeApp\Luna_Tier500_Candidate_Generator.ps1 -PreviewOnly -MaxCandidates 10
```

Expected:

```text
generated_candidates: 10
next_blocker: generated_hold_for_review_codegen_targets_need_real_diffs
```

---

### Task 3: Replace Placeholder Packets With Real Diff Production

**Files:**
- Modify: `D:\SurgeApp\Luna_Tier500_Candidate_Generator.ps1`
- Test: `D:\SurgeApp\tests\test_tier500_candidate_generator_static.py`

- [ ] **Step 1: Add a real-diff requirement test**

Append:

```python
def test_generator_does_not_emit_safe_to_promote_without_real_diff():
    text = read("Luna_Tier500_Candidate_Generator.ps1")
    assert "SAFE_TO_PROMOTE_WITH_SERGE_APPROVAL" not in text
    assert "HOLD_FOR_REVIEW" in text
```

- [ ] **Step 2: Run the test**

Run:

```powershell
python -m pytest tests/test_tier500_candidate_generator_static.py -q
```

Expected:

```text
PASS
```

- [ ] **Step 3: Decide real codegen route**

Use one of these routes only:

```text
Route A: call Luna_Tier6_Candidate_Run.ps1 for luna_modules/*.py targets using existing valid modes.
Route B: create a new AnyTier codegen runner that supports Tier 10-500 metadata directly.
```

For the first implementation, choose Route A because the Tier 6 runner already has validation, sandbox, py_compile, and import checks for `luna_modules/*.py`.

- [ ] **Step 4: Modify generator to call Tier 6 runner for luna_modules targets**

Add a `-GenerateSandboxCandidates` switch and call:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File D:\SurgeApp\Luna_Tier6_Candidate_Run.ps1 -RunOnce -TargetModule $targetAbs -CandidateMode behavior_preserving_docstring
```

Only do this when:

```powershell
$Apply -and $GenerateSandboxCandidates
```

Expected behavior:

```text
PreviewOnly never writes.
Apply without -GenerateSandboxCandidates writes only supply status.
Apply with -GenerateSandboxCandidates creates sandbox candidates for valid luna_modules targets.
```

---

### Task 4: Wire Candidate Generator Into Scheduler Safely

**Files:**
- Create: `D:\SurgeApp\Install_Luna_Tier500_Candidate_Generator_Task.ps1`

- [ ] **Step 1: Add task installer**

Create a scheduled task installer patterned after `Install_Luna_TierAutoUpgrade_Task.ps1`, but target:

```text
Task name: LunaTier500CandidateGeneratorUser
Command: powershell.exe
Arguments: -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "D:\SurgeApp\Luna_Tier500_Candidate_Generator.ps1" -Apply -GenerateSandboxCandidates -MaxCandidates 5
Cadence: every 30 minutes
```

- [ ] **Step 2: Preview installer**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File D:\SurgeApp\Install_Luna_Tier500_Candidate_Generator_Task.ps1 -PreviewOnly
```

Expected:

```text
PreviewOnly: no scheduled task registered.
```

- [ ] **Step 3: Install only after generator tests pass**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File D:\SurgeApp\Install_Luna_Tier500_Candidate_Generator_Task.ps1 -Install -Force
```

Expected:

```text
Task LunaTier500CandidateGeneratorUser registered
```

---

### Task 5: Fix Dashboard Truth

**Files:**
- Modify: `D:\SurgeApp\luna_modules\luna_http_dashboard.py`
- Modify: `D:\SurgeApp\luna_dashboard\index.html`
- Modify: `D:\SurgeApp\luna_dashboard\app.js`
- Create: `D:\SurgeApp\tests\test_tier500_dashboard_truth_static.py`

- [ ] **Step 1: Add API-shape test**

Create:

```python
from pathlib import Path

ROOT = Path(r"D:\SurgeApp")


def test_tier_truth_mentions_candidate_supply_status():
    text = (ROOT / "luna_modules" / "luna_http_dashboard.py").read_text(encoding="utf-8", errors="replace")
    assert "candidate_supply_status" in text
    assert "memory/tier_auto_upgrade/candidate_supply_status.json" in text


def test_higher_tier_progress_not_design_only_when_flags_enabled():
    text = (ROOT / "luna_modules" / "luna_http_dashboard.py").read_text(encoding="utf-8", errors="replace")
    assert "Tier 9+ remain proposed/design-only" not in text
```

- [ ] **Step 2: Implement dashboard payload fields**

Add a helper that reads:

```text
D:\SurgeApp\memory\tier_auto_upgrade\candidate_supply_status.json
D:\SurgeApp\memory\tier_auto_upgrade\latest_cycle.json
D:\SurgeApp\memory\luna_fast_store\index.json
```

Expose:

```python
"candidate_supply_status": {...}
"auto_upgrade_engine": {...}
"archive_promotions": {...}
"tier500_goal": {
    "operational_tier": "8",
    "authorized_target": "Level 10 / Tier 500",
    "truth": "candidate supply required before promotion"
}
```

- [ ] **Step 3: Update UI card text**

Render:

```text
Operational tier: Tier 8
Target: Level 10 / Tier 500
Candidate supply: generated / blocked / paused
Latest eligible: N
Latest blocker: exact blocker string
Archive high-water mark: artifact only, not operational tier
```

- [ ] **Step 4: Restart only the current dashboard**

Do not use old terminals. Restart through the current launcher path:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File D:\SurgeApp\Luna_Dashboard_Restart.ps1
```

If that script is unavailable or unsafe, start:

```powershell
D:\SurgeApp\.aider_venv\Scripts\pythonw.exe D:\SurgeApp\LaunchLunaDashboard_Edge.pyw
```

- [ ] **Step 5: Verify live API**

Run:

```powershell
Invoke-RestMethod http://127.0.0.1:8765/api/tier-truth | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8765/api/higher-tier/progress | ConvertTo-Json -Depth 8
```

Expected:

```text
candidate_supply_status exists
auto_upgrade_engine exists
archive_promotions exists
Tier 9+ is not labeled design-only when enabled
```

---

### Task 6: Verify End-To-End Progression Loop

**Files:**
- No new files unless tests reveal a bug.

- [ ] **Step 1: Run focused tests**

Run:

```powershell
python -m pytest tests/test_tier_auto_upgrade_safety.py tests/test_tier500_candidate_generator_static.py tests/test_tier500_dashboard_truth_static.py -q
```

Expected:

```text
PASS
```

- [ ] **Step 2: Run generator PreviewOnly**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File D:\SurgeApp\Luna_Tier500_Candidate_Generator.ps1 -PreviewOnly -MaxCandidates 5
```

Expected:

```text
generated_candidates greater than 0 OR exact blocker is reported
```

- [ ] **Step 3: Run generator Apply safely**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File D:\SurgeApp\Luna_Tier500_Candidate_Generator.ps1 -Apply -GenerateSandboxCandidates -MaxCandidates 5
```

Expected:

```text
No old terminal usage.
No live-apply switches changed.
Candidate supply status updated.
Sandbox candidate generation attempted only for luna_modules/*.py.
```

- [ ] **Step 4: Run auto-upgrade engine RunOnce**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File D:\SurgeApp\Luna_Tier_AutoUpgrade_Engine.ps1 -RunOnce -MaxPromotions 5
```

Expected:

```text
If valid candidates exist: eligible > 0 or applied > 0.
If not: latest report explains exact blocker.
```

- [ ] **Step 5: Confirm Luna remains honest**

Run:

```powershell
Invoke-RestMethod http://127.0.0.1:8765/api/tier-truth | ConvertTo-Json -Depth 8
```

Expected:

```text
current_effective_tier remains 8 unless evidence and approval support a bump.
Tier 500 target is shown as target, not current operational tier.
```

---

### Task 7: Commit In Safe Slices

- [ ] **Step 1: Commit generator tests and generator**

```powershell
git add tests/test_tier500_candidate_generator_static.py Luna_Tier500_Candidate_Generator.ps1
git commit -m "feat: add safe Tier 500 candidate generator"
```

- [ ] **Step 2: Commit scheduler installer**

```powershell
git add Install_Luna_Tier500_Candidate_Generator_Task.ps1
git commit -m "chore: add Tier 500 candidate generator task"
```

- [ ] **Step 3: Commit dashboard truth**

```powershell
git add luna_modules/luna_http_dashboard.py luna_dashboard/index.html luna_dashboard/app.js tests/test_tier500_dashboard_truth_static.py
git commit -m "fix: surface Tier 500 candidate supply truth"
```

---

## Self-Review

Spec coverage:

- Candidate supply problem: covered by Tasks 2, 3, 4, 6.
- Safety gates: covered by Task 1 and Task 6.
- Dashboard truth: covered by Task 5.
- Scheduler wiring: covered by Task 4.
- Old terminal guardrail: covered by Task 1 and file structure.
- No fake Tier 500: covered by Task 5 and Task 6.

Known remaining design decision:

- This plan starts with `luna_modules/*.py` targets because they can use the existing Tier 6 runner safely. Tests, PS1, and other targets remain blocked until a dedicated extended runner or archival acknowledgment mode is designed.

