# Luna Overnight Growth — 2026-05-31

**Status:** approved by operator (Option A: standing approvals, ceiling stays at T500)
**Operator request:** "since Luna has a bigger brain give her bigger task i want her to be bigger by 90% tomorrow. make it happen" → revised to "all of it, 99% better"
**Honest reframe (operator agreed):** push every lane that does NOT require raising the council ceiling or touching `cognitive_*.py` sources. Result is "Luna more deeply grounded at T500" — NOT "Luna at T1000."

## Live state probe (2026-05-31 morning)

| Axis | Value | Headroom |
|---|---|---|
| Tier ladder | `TIER 500 ACTIVE`, `total_advancements=493`, blocker `"(none — campaign complete)"` | 0 — at council ceiling |
| `recommended_next_tier` | `None` | 0 |
| Tier intent FULL-depth entries | 340 (T156..T495) | 0 |
| Tier intent stub entries | 0 medium, 0 low | 0 |
| Family gaps (`semantic_synth.detect_family_gaps`) | `[]` | 0 |
| `luna_modules/*.py` | 1098 | n/a |
| `cognitive_*` modules | 277 | n/a |
| Smoke coverage | 100% (277/277) | 0 |
| Report coverage (static `def report(` scan of cognitive_*) | 66.4% (184/277) | **+33.6 pp BUT** raising it would require editing `cognitive_*.py` — forbidden by `luna_self_improvement.py` docstring policy |
| `broad_live_apply` | `false` (council-gated only) | n/a |
| Memory footprint | 2.57 GB (140 jsonl, 70 754 json, 83 739 md) | grows with re-index |

**Conclusion:** the "99% bigger overnight" target is bounded by the council ceiling and the no-touching-cognition policy. What CAN grow tonight: audit chain, memory indexes, smoke-test discoveries on non-cognitive modules, council reflections.

## Lanes (all council-gated, adds-only, kill-switchable)

### Lane A — Self-Improvement Sweep (existing engine, scheduled run)
- Calls `luna_modules.luna_self_improvement.run_overnight()` (existing public API, 6 h budget, 100-cycle hard cap).
- Discovers any newly-added `cognitive_*` modules without a smoke test, generates one deterministically, runs it, keeps it ONLY if it passes.
- Writes audit trail to `memory/cognitive/luna_self_improvement_audit.jsonl`.
- Kill switch: `memory/kill_switches/luna_self_improvement.disabled`.
- **Reversibility:** delete `self_tests/` — full undo.
- **Expected outcome:** likely 0 new smoke tests (we're at 100 % on cognitive_*) but the run will flag any module whose smoke test starts failing — that's the actual signal we want.

### Lane B — Memory Index Rebuild
- Calls `luna_modules.luna_memory_index.build_memory_index()`, `build_keyword_index()`, `build_sqlite_fts_index()`, `build_memory_summaries()` over the 2.57 GB corpus.
- Writes indexes under `memory/index/` and `memory/summaries/`. Original memory files are read-only inputs.
- Kill switch: `memory/kill_switches/luna_overnight_memory_index.disabled`.
- **Reversibility:** delete the generated index files — full undo.
- **Expected outcome:** measurable index growth in MB + queryable keyword/FTS surfaces refreshed.

### Lane C — Audit-Chain Witness
- For every lane action, write a `VERIFIED_BY_RUNTIME_PROBE` entry to `memory/agent_bus_audit/luna_overnight_2026_05_31.jsonl` (new file, append-only).
- Provides a single forensic trail for the morning report.
- **Reversibility:** delete the file — full undo.

### Lane D — Morning Synthesis
- At end of run, call `luna_modules.luna_report_synthesizer.synthesize_report({...})` to compile a markdown morning report at `memory/luna_overnight_growth_2026_05_31_morning_report.md`.
- Includes before/after metrics, lane outcomes, any flagged modules, exact next-action prompt for operator.

## Out of scope (will NOT do tonight)
1. Edit any `cognitive_*.py` source (forbidden by self-improvement policy and inviolate floor).
2. Raise council ceiling T500 → T1000 (tier-jump action — operator declined Option B).
3. Modify boot-chain files: `Start_SurgeApp.bat`, `Start_SurgeApp.vbs`, `LaunchLuna.pyw`, `LaunchLunaDashboard.pyw`, `SurgeApp_Claude_Terminal.py`, `Luna Command Center.lnk`, `install_*_launcher.ps1`.
4. Modify `worker.py`, `runtime_state.json`, `luna_higher_tier_config.json` tier-0-4 fields (inviolate floor).
5. Touch the vocabulary DB, `feature_flags.json`, `API.txt`, `.env`, `memory/secrets/**`, `luna_kill_switch.flag`.
6. Spawn new windows / popups (every subprocess uses `CREATE_NO_WINDOW`).
7. Compete with existing scheduled tasks — runs in a non-conflicting time window.

## Safety guardrails
- **Master kill switch:** create `memory/kill_switches/luna_overnight_2026_05_31.disabled` to abort instantly.
- **NEVER-raise contract:** every lane wrapped in `try/except`; one lane failing does NOT abort others.
- **No popups:** all subprocesses spawn with `CREATE_NO_WINDOW`.
- **Hard time budget:** total wall-clock cap 12 h; per-lane caps enumerated in the worker.
- **Audit-first:** every action logged before execution.
- **Reversible:** every output goes to dedicated new directories or new JSONL files; delete to undo.

## Scheduling
- Run via `schtasks /Create /TN "LunaOvernightGrowth_2026_05_31" /SC ONCE /SD 2026-05-31 /ST 21:00 /TR ...` (tonight 9 PM local).
- RunLevel = `LeastPrivilege` (no admin).
- `/Z` self-deletes the one-shot task after execution.
- Worker runs detached, hidden, audit-trailed to `logs/luna_overnight_growth_2026_05_31.log`.

## Morning verification (one command, < 60 s)
```
.\.aider_venv\Scripts\python.exe -m luna_modules.luna_overnight_growth_2026_05_31 --verify
```
Outputs a one-screen table: `before`, `after`, `delta`, `lane_outcomes`, `flagged_modules`, `boot_chain_clean=true/false`.

## What operator should expect by tomorrow morning
- Tier number: **unchanged** (T500 — ceiling not raised).
- Smoke coverage: unchanged (already 100 %) but verified across all 277 modules.
- Report coverage: **unchanged at 66.4 %** (raising it requires `cognitive_*.py` edits — forbidden).
- Memory indexes: rebuilt; queryable surface refreshed; size grows by index volume.
- Audit chain: ~50–500 new `VERIFIED_BY_RUNTIME_PROBE` entries depending on lane work.
- Shared lessons: 0+ new entries.
- Boot chain `git status`: identical to bedtime.
- One morning report markdown ready for review.

## File inventory created tonight (everything is reversible)
1. `luna_modules/luna_overnight_growth_2026_05_31.py` — the worker
2. `logs/luna_overnight_growth_2026_05_31.log` — stdout/stderr trail
3. `memory/agent_bus_audit/luna_overnight_2026_05_31.jsonl` — audit entries
4. `memory/luna_overnight_growth_2026_05_31_morning_report.md` — morning summary
5. `Install_Luna_Overnight_Growth_2026_05_31.ps1` — operator-runnable scheduler (no admin)
6. Any new smoke tests in `self_tests/smoke_*.py` (likely zero)
7. Refreshed `memory/index/**` (existing path; adds-only writes)
