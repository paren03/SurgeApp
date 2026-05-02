# Luna — Warp Repair Lessons (2026-05-01)

This is the human-readable companion to lessons **L018–L024** in
`luna_modules/luna_self_teacher.py` and the matching pattern entries in
`memory/luna_lessons_learned.jsonl`. It captures the architectural
lessons from the Warp repair session so I (Luna) can recognise the same
shapes again without re-discovering them.

> **One-line summary**: Process boundaries are leaky on Windows; status
> JSONs must be structured, not free-form; gate signals must be honest
> (no false alarms); informational state must never beat active state.

---

## Why this repair was needed

Symptom set the operator saw before the repair:
- Verifier said *"worker_main duplicate storm"* even though only one
  worker pair existed.
- continues_update said *"running"* while no real work was progressing.
- Aider Bridge would finish a job, NOOP, and immediately re-claim the
  same file — endlessly.
- Stale aider grandchildren survived bridge restarts and held VRAM.
- continues_update was permanently *paused_quality_gate* on every boot.

Root pattern across all of these: **observability lied about reality.**
Either the running services lied (status JSON missing fields), or the
verifiers/gates were testing for the wrong thing (substring match,
untracked files, informational state). When observability lies,
operators distrust automation, and the system grinds to a halt.

---

## Architectural rules I now follow

### Rule 1 — Structured status fields, never embed in `detail`

Every consumer of `aider_bridge_status.json`, `luna_guardian_status.json`,
or `continues_update_state.json` expects specific keys. If I want the UI
to show *"bridge processing worker.py for 12s"*, those three pieces of
information must be three structured fields (`state`, `target`,
`started_at`), never one free-form `detail` string the UI has to parse.

> **Enforcement**: every `_write_*_status()` writes the same key set
> every time. New fields are additive; never reuse a key for two
> different meanings.

### Rule 2 — Active state always wins over informational state

`dirty_targets` is **informational** — it says "here is what I skipped
this cycle." `pause_reason` is **directive** — it says "I am stopped
because of X." When mapping internal CU state to a UI status string,
priority is:

1. Blocked (worker_import / aider_bridge_stale)
2. Active (`running_real_job` when running and phase in queueing/starting)
3. Genuinely paused (pause_reason set, or hard-failure / noop / dirty
   thresholds while NOT running)
4. `idle_clean`

Skipping a file due to dirty state while another file is being patched
successfully is **running**, not paused.

### Rule 3 — Gate questions need exact phrasing

A gate says "is X true right now?" — it must answer the right question.

| Gate | Wrong question | Right question |
|---|---|---|
| Dirty core files | `git status --porcelain` (includes untracked!) | `git status --porcelain --untracked-files=no` |
| Stale aider job | "is `active/` non-empty?" | "is any active json older than 60 minutes?" |
| Worker is running | substring match `worker.py` in cmdline | `python(w)\.exe` followed by `worker.py` and excluding `-m aider` |
| Duplicate storm | physical pid count > 1 | logical role count > 1 (parent/child pair = 1 logical) |

Wrong question → false positive → operator distrust → everything paused.

### Rule 4 — Windows process kill must walk the tree

`subprocess.run(timeout=N)` and `Popen.kill()` only kill the **direct
child**. Aider invokes LiteLLM, which spawns its own helpers. Killing
the parent leaves grandchildren running with HTTP connections to Ollama,
holding VRAM, occasionally even writing diff output to disk minutes
after the bridge logged "timeout".

Always use `taskkill /T /F /PID <pid>` for tree kill on Windows, both
for timeouts and for orphan cleanup. The `/T` flag walks descendants.

### Rule 5 — Bridge must own orphan cleanup on startup

Two reasons aider grandchildren get orphaned:
1. Bridge is killed before its child finishes (Ctrl-C, BSOD, kill
   switch).
2. A timeout's tree-kill races with a new bridge claim.

The bridge MUST sweep on startup (after PID lock, before main loop).
Pattern: `Get-CimInstance` for python processes whose command line
contains `aider` AND (`logic_updates` OR `aider_jobs`). Anything whose
parent is not me is an orphan — `taskkill /T /F /PID`.

### Rule 6 — Per-target NOOP budget prevents loops

If aider produces no diff for a target twice, it almost certainly
cannot produce a diff for that target with the current model. Continuing
to claim, run, NOOP, re-claim wastes 90s per cycle indefinitely.

`logs/aider_bridge_noop_budget.json` tracks per-target count and
`cooldown_until`. After 2 NOOPs, set 24h cooldown. Bridge skips claims
in cooldown (live event `NOOP_BUDGET_SKIP`). Cooldown auto-expires.
This forces CU to find new work or settle into `idle_clean`.

### Rule 7 — Logical role classification, not pid counting

A normal Luna deployment shows **8 physical python processes** but only
**6 logical roles** (worker_main, worker_cu, aider_bridge, guardian,
terminal, apprentice, tray — plus optional aider_child). Two processes
that are a parent/child pair (same script, one is parent of the other,
no third) count as **one logical instance**.

Anything that classifies by raw PID count will always over-report
duplicates. Verifier uses `Count-LogicalInstances`; guardian uses
`_dedupe_service_processes` with the same parent/child collapse.

### Rule 8 — Distinguish the script from its arguments

A Python invocation has the form:

```
python.exe   <script-or-flag>   <args...>
```

The marker `worker.py` can appear in two places: as the script being
invoked (`python.exe worker.py`) or as a target file path argument
(`python.exe -m aider --file ...\worker.py`). A substring match
conflates them.

Match the script position explicitly. The guardian's
`_command_invokes_script` regex and the verifier's `Test-InvokesScript`
helper both require the marker to appear directly after `python.exe`.
Both also reject any command line that contains `-m aider` to filter
out aider's own children.

---

## Files touched by the warp repair (canonical reference)

| File | Change |
|---|---|
| `aider_bridge.py` | structured `_write_bridge_status` fields, `_cleanup_orphan_aider_children` on startup, `Popen+communicate+taskkill /T` for tree-kill timeout, `_noop_budget_*` 24h per-target cooldown |
| `LaunchLuna.pyw` | `_cu_startup_gate()` returning `(paused, reason)`; gates: stale_aider_job (>60min) / dirty_core_files (`--untracked-files=no`) / noop_budget_exhausted |
| `luna_guardian.py` | `_read_bridge_status()` and bridge field included in `luna_guardian_status.json` |
| `worker.py` | `_cu_compute_ui_status()` mapping internal CU state to 7 standardized strings; `_cu_write_state()` injects `ui_status` |
| `SurgeApp_Claude_Terminal.py` | `_tick_heartbeat()` reads `aider_bridge_status.json` and `continues_update_state.json` (`ui_status`); badge/phase show real CU/bridge state |
| `Luna_Post_Repair_Verify.ps1` | `Count-LogicalInstances` + `Test-InvokesScript`; checks worker_main / worker_cu / aider_bridge / guardian / terminal / apprentice / tray as logical roles |

---

## Standardized CU `ui_status` values (single source of truth)

The terminal's badge/phase, the verifier, and any future log-watcher
should ALL read these exact strings:

- `running_real_job` — running and phase in queueing/starting
- `idle_clean` — running but nothing to do (or just started, or rolled-over to next cycle)
- `paused_dirty_core` — pause_reason set, or all targets dirty while not running
- `paused_noop_budget` — `_all_skip_streak >= 2` or `noop_count >= 5`
- `paused_recent_failures` — `consecutive_failures >= 3`
- `blocked_worker_import` — worker.py fails import; CU cannot run
- `blocked_aider_stale` — aider bridge process older than file mtime

If a new state appears that doesn't fit, add it to **both** the
producer (`_cu_compute_ui_status`) and every consumer at the same time.
Don't add string values consumers don't recognise.

---

## How I (Luna) should detect each pattern

For runtime checks — see `detection_code` in `LESSONS[L018..L024]` in
`luna_modules/luna_self_teacher.py`. Each lesson has executable Python
that returns a clear pass/fail message. They are designed to run inside
`run_full_self_diagnosis()` without external dependencies.

For pattern-matching against future bug reports — see
`memory/luna_lessons_learned.jsonl` entries with `pattern_id` matching
this session (search for `pattern_id` containing `aider_orphan`,
`subprocess_run_timeout`, `noop_loop`, `bridge_status_missing`,
`ui_status_dirty_targets`, `git_status_porcelain_includes_untracked`,
or `process_match_substring`).

---

## What I (Luna) should NEVER do

- Never write a status field that lies about reality (e.g. claim
  `state=idle` while a child is mid-job).
- Never rely on `subprocess.run(timeout=N)` to clean up LLM tool
  processes on Windows.
- Never use `git status --porcelain` without `--untracked-files=no`
  inside a gate that means "is the user editing this?".
- Never count physical PIDs to detect duplicate services.
- Never substring-match a script name in a process command line; use
  the position-aware regex.
- Never re-queue a target that just NOOP'd twice without a cooldown.
- Never write code that puts `dirty_targets` ahead of `running` in any
  UI status priority.

---

*Recorded after the 2026-05-01 Warp repair session. Linked lessons
L018–L024 in luna_self_teacher.py.*
