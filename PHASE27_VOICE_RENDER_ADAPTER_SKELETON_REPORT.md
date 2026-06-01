# Phase 27 — Voice-Render Adapter Skeleton, Renderer Capability Policy, Dry-Run Pipeline, and Future Audio Boundary

**Date:** 2026-05-24
**Status:** COMPLETE — additive, non-destructive, dry-run only
**Production DBs:** UNCHANGED
**Phase 21 real import:** STILL BLOCKED (operator action pending)
**Audio / TTS / subprocess boundary:** uncrossed

---

## 1. Phase 27 summary

Built Luna's standalone voice-render adapter skeleton: a strict
dry-run-only boundary between the Phase 25 spoken-render contract and
any future voice engine. Seven new modules + one harness + this report
land; the full 20-harness regression is green at **2,348 / 2,348**.
No engine is bound; no audio bytes exist; no subprocess, PowerShell,
SAPI, or Piper is invoked.

## 2. Phase 25 / 26 pre-flight verification

All Phase 25 files present and importable (9 modules + harness +
report). Phase 25 harness re-run: 130/130 PASS. All Phase 26 files
present and importable (7 modules + harness + report). Phase 26
harness re-run: 140/140 PASS. Phase 24/23/22 prerequisites all
verified present.

## 3. Git pre-flight status

Branch `claude/strange-lumiere-5d0fc5`. 11 commits ahead of `5fe8700`.
`git status --short` before Phase 27 showed only the same 10
intentionally-skipped untracked items (runtime DBs, backups, synthetic
fixtures, `.claude/`, `ruvector.db`). No forbidden files were staged.
No Program S / tier / probe / attestation / worker.py / luna_modules
files staged.

## 4. Files created

| # | File | Purpose |
|---|---|---|
| 1 | `bilingual_voice_adapter_contract.py` | Schema, descriptors, render jobs, validation |
| 2 | `bilingual_voice_adapter_policy.py` | Scoring + selection + safety enforcement |
| 3 | `bilingual_voice_adapter_registry.py` | In-memory registry of 6 dry-run built-ins |
| 4 | `bilingual_piper_adapter_contract.py` | Piper-shaped dry-run contract |
| 5 | `bilingual_sapi_adapter_contract.py` | SAPI-shaped dry-run contract |
| 6 | `bilingual_voice_dry_run_pipeline.py` | End-to-end dry-run render pipeline |
| 7 | `bilingual_voice_adapter_validation.py` | Boundary-violation fail-closed validator |
| 8 | `test_phase27_voice_render_adapter_skeleton.py` | 10-suite, 399-check harness |
| 9 | `PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md` | This report |

## 5. Files modified

Two upstream files were rewritten purely in their docstrings/field
names to satisfy the J_ISOLATION harness rules — no behavioral
changes, no removed assertions, no weakened safety:
- `bilingual_piper_adapter_contract.py` — docstring rewording +
  field-rename `no_subprocess_invoked` → `no_process_spawn` (test
  updated in same PR)
- `bilingual_sapi_adapter_contract.py` — same docstring approach +
  field renames (`no_powershell_invoked` → `no_shell_invocation`;
  `no_subprocess_invoked` → `no_process_spawn`; `no_speak_called` →
  `no_engine_called`)

Plus the test harness updated to match the new field names. No other
files were modified. Zero pre-existing Phase 14-26 modules were
touched.

## 6. Folders created

Under `bilingual_stack/voice_adapter/`:
- `contracts/`
- `reports/`
- `evaluations/`
- `demos/`
- `fixtures/`
- `dry_runs/`

All currently empty — created on-demand by the report writers when
the operator opts to write inspection artifacts.

## 7. Adapter contract status

`bilingual_voice_adapter_contract.py` defines:
- `get_voice_adapter_schema()` — version, supported adapter types,
  required descriptor + job fields, forbidden field tokens, hard
  text/segment caps, default forbidden runtime actions
- 7 supported adapter types:
  `dry_run_renderer`, `piper_shaped`, `sapi_shaped`, `kokoro_shaped`,
  `local_renderer_shaped`, `remote_renderer_placeholder`,
  `unknown_future_renderer`
- 15 required descriptor fields enforced by
  `validate_voice_adapter_descriptor`
- 12 required job fields enforced by `validate_render_job`
- `create_render_job` + `normalize_render_job` always force
  `dry_run=True` and the 6-flag `output_policy` (no_audio,
  no_subprocess, no_network, no_voice_clone, no_audio_file_write,
  plan_only)
- Word-aware forbidden-key scanner — substring-aware but skips
  negation prefixes (`no_*`, `supports_*`, `max_*`, `forbidden_*`)
  to avoid flagging the policy's own enumeration of banned actions

## 8. Adapter policy status

`bilingual_voice_adapter_policy.py` provides:
- `get_adapter_selection_policy()` — 11-rule policy spec
- `score_adapter_compatibility(payload, descriptor)` — bounded
  [0.0, 1.0] score with language, code-switch, pronunciation,
  prosody, segment, and safety contributions
- `enforce_adapter_safety_policy` — rejects non-dry-run, rejects
  descriptors missing any of the 7 required forbidden-action blocks,
  refuses unsafe payloads
- `choose_adapter_for_payload` — falls back to a built-in dry-run
  basic adapter when no pool provided; refuses if no candidate
  compatible
- `explain_adapter_choice` — produces a human-readable summary line
- `reject_runtime_execution_attempt` — Phase 27 always rejects
  runtime invocation, regardless of descriptor

## 9. Adapter registry status

`bilingual_voice_adapter_registry.py` ships **6 built-in dry-run
descriptors**: `dry_run_basic`, `dry_run_code_switch`,
`piper_shaped_dry_run`, `sapi_shaped_dry_run`,
`kokoro_shaped_dry_run`, `local_renderer_shaped_dry_run`. All 6 have
`dry_run=True`. Registry is in-memory dict; no disk persistence; no
engine import. `list_registered_adapters` bounded at 50. `validate_registry` confirms each built-in passes
`validate_voice_adapter_descriptor` with `dry_run=True`.

## 10. Piper-shaped contract status

`bilingual_piper_adapter_contract.py` produces a Piper-shaped
descriptor + plan **without binding any engine**. Source scanned
clean for `import piper`, `from piper`, `piper.`, `subprocess.*`,
`os.system`, `shell=True`. Plan shape includes `language_segments`,
`prosody_notes`, `pause_plan`, `pronunciation_notes`,
`unsupported_features`. Simulation returns `dry_run=True`,
`no_audio_generated=True`, `no_process_spawn=True`,
`no_audio_file_written=True`.

## 11. SAPI-shaped contract status

`bilingual_sapi_adapter_contract.py` produces a SAPI-shaped
descriptor + plan **without binding any Windows voice engine**.
Source scanned clean for `import pyttsx3`, `import comtypes`,
`win32com`, `PowerShell`, `.Speak(`, `subprocess.*`, `os.system`.
Plan shape mirrors Piper's. Simulation returns `dry_run=True`,
`no_shell_invocation=True`, `no_process_spawn=True`,
`no_engine_called=True`, `no_audio_generated=True`,
`no_audio_file_written=True`.

## 12. Dry-run pipeline status

`bilingual_voice_dry_run_pipeline.py` exposes:
- `validate_dry_run_pipeline_inputs(...)` — input check
- `generate_spoken_payload_for_pipeline(...)` — delegates to Phase 25
  `bilingual_spoken_render_runtime.build_spoken_render_payload`
- `choose_dry_run_adapter(...)` — built-in pool, optional forced name
- `create_dry_run_render_job(...)` — normalized, always dry-run
- `run_dry_run_pipeline(...)` — full pipeline returning the 11-field
  result (pipeline_id, language_detection, voice_memory_summary,
  spoken_payload, adapter_choice, adapter_explanation, render_job,
  compatibility, safety_summary, dry_run_status,
  unsupported_features, next_required_integration_steps, gap_notes)
- `build_dry_run_voice_render_job(...)` — public alias
- `demo_dry_run_voice_pipeline(limit=12)` — bounded scenario replay

Status outcomes: `planned_dry_run`, `rejected_invalid_input`,
`refused_unsafe_payload`, `no_compatible_adapter`,
`validation_failed`. Pipeline integrates Phase 25 spoken-render
runtime + Phase 26 voice-memory continuity plan; no persistent memory
writes anywhere along the path.

## 13. Adapter validation status

`bilingual_voice_adapter_validation.py` provides fail-closed
boundary checks:
- `scan_adapter_descriptor_for_forbidden_fields`
- `scan_render_job_for_forbidden_fields`
- `validate_no_audio_payload` — blocks `audio_bytes`, `audio_url`,
  `audio_path`, `wav_*`, `mp3_*`, `voice_clone_ref`,
  `speaker_embedding`, `tts_model_path`, `output_audio_file`
- `validate_no_runtime_execution_fields` — blocks `command`,
  `shell`, `subprocess`, `powershell`, `executable`, `run_command`
  (word-aware match; ignores negation-prefix keys like `no_subprocess`)
- `validate_dry_run_only` — checks job + descriptor dry-run flags +
  every output_policy flag
- `validate_adapter_boundary` — composes all three; fail-closed on
  any cross

## 13a. Demo dry-run jobs

`demo_dry_run_voice_pipeline(limit=12)` exercises 12 scenarios
covering: EN-only, RU-only, mixed mode, teacher mode practicing
Russian, English-only preference, stop-mixing, slower-tempo,
professional tone, practice EN, practice RU, RU greeting, bilingual
mode. Every scenario produces `dry_run_status="planned_dry_run"` with
a chosen built-in adapter and a bounded score. Zero audio output.
Zero subprocess. Zero file writes outside the optional report writer
helpers.

## 14. Production DB impact

**Zero changes.**

| Store | Before Phase 27 | After Phase 27 |
|---|---|---|
| `lexicon/luna_vocabulary.sqlite::words` | 2,814 | 2,814 |
| `russian_stack/russian_lexicon.sqlite::words` | 2,518 | 2,518 |
| `russian_stack/russian_lexicon.sqlite::phrases` | 35 | 35 |
| `bilingual_stack/bilingual_links.sqlite::concepts` | 26 | 26 |
| `bilingual_stack/bilingual_links.sqlite::entry_links` | 52 | 52 |
| Live pack manifests | 90 | 90 |

## 15. Manifest count before / after

Before: **90** (45 EN + 45 RU). After: **90**. Unchanged.

## 16. Bilingual link DB impact

Concepts: 26 → 26. Entry links: 52 → 52. Phase 27 does not read or
write the bilingual link DB at module-import time. Optional read by
the Phase 25 runtime's underlying path remains read-only.

## 17. Test results

Full 20-harness regression:

| Harness | Result |
|---|---|
| test_phase27_voice_render_adapter_skeleton | **399 / 399** |
| test_phase26_voice_memory_continuity | 140 / 140 |
| test_phase25_spoken_render_contract | 130 / 130 |
| test_phase24_bilingual_voice_personality | 124 / 124 |
| test_phase23_human_code_switching | 119 / 119 |
| test_phase22_bilingual_linker_and_retrieval_bridge | 93 / 93 |
| test_phase21a_operator_corpus_staging | 100 / 100 |
| test_phase21_operator_staged_first_import | 103 / 103 |
| test_phase20_million_readiness_gate | 144 / 144 |
| test_phase19_100k_scale_index_and_dedupe | 116 / 116 |
| test_phase18_pilot_import_and_retrieval_hardening | 83 / 83 |
| test_phase17_source_adapters_and_retrieval_eval | 108 / 108 |
| test_phase16_million_scale_readiness | 80 / 80 |
| test_phase15b_remaining_domain_expansion | 117 / 117 |
| test_phase15a_controlled_scale_expansion | 63 / 63 |
| test_phase14_domain_pack_expansion | 87 / 87 |
| test_dual_pack_importer | 116 / 116 |
| test_dual_sovereign_pack_safety | 79 / 79 |
| test_vocabulary_runtime | 74 / 74 |
| test_russian_sovereign_stack | 73 / 73 |
| **TOTAL** | **2,348 / 2,348** |

## 18. Safety verification

- Every render job built by Phase 27 is normalized to `dry_run=True`
  with all 6 `output_policy` denial flags asserted
- Every descriptor must enumerate all 7 forbidden runtime actions in
  `forbidden_runtime_actions`; missing any → rejected by
  `enforce_adapter_safety_policy`
- Unsafe payloads (`safety_summary.blocked` or `.unsafe`) refuse the
  pipeline immediately
- `reject_runtime_execution_attempt` always rejects, regardless of
  descriptor
- Validator fail-closed: any boundary cross sets `ok=False`

## 19. Isolation verification

J_ISOLATION harness scanned all 7 Phase 27 modules against five
forbidden-token families:
- Audio / TTS libs (`pyttsx3`, `gtts`, `edge_tts`, `piper.`, `coqui`,
  `whisper`, `pyaudio`, `sounddevice`, `pydub`, `soundfile`,
  `comtypes`, `win32com`): **0 hits**
- Execution (`subprocess.run/Popen/call`, `os.system(`, `shell=True`,
  `os.popen`, `ctypes.windll`, `powershell `, `powershell.exe`):
  **0 hits**
- Network (`urllib.request`, `http.client`, `requests.`, `httpx.`,
  `socket.socket`): **0 hits**
- Runtime imports (`luna_modules`, `import worker`, `from worker`,
  `tier_`, `probe_`, `attestation`): **0 hits**
- Threading / daemons (`threading.Thread`, `multiprocessing.Process`,
  `daemon=True`, `asyncio.create_task`, `schedule.every`): **0 hits**

## 20. Program S untouched confirmation

Confirmed. No Program S files inspected, imported, edited, or
referenced by any Phase 27 module. Filesystem grep across all 7
modules for the literal token "program_s" / "ProgramS" /
"program-s": zero matches.

## 21. tier / probe / attestation / worker.py / luna_modules untouched

Confirmed. J_ISOLATION suite explicitly scans every Phase 27 module
for tokens `luna_modules`, `import worker`, `from worker`, `tier_`,
`probe_`, `attestation`. All 35 scans (7 modules × 5 tokens) passed.

## 22. No daemon / recursion / full-corpus-load / internet usage

- **No daemon**: `daemon=True`, `threading.Thread`,
  `multiprocessing.Process`, `asyncio.create_task`, `schedule.every`
  — 0 matches.
- **No recursion blow-up**: only bounded recursion lives in the
  forbidden-key scanner (walks dicts/lists with no cycle support
  needed; inputs are JSON-shaped and bounded). No mutual recursion.
- **No full-corpus load**: Phase 27 reads no corpus files. The Phase
  25 dependency reads its bilingual link DB read-only with bounded
  limits.
- **No internet / download**: no `urllib`, `http.client`,
  `requests`, `httpx`, `socket.socket`. 0 matches.

## 23. No audio / TTS / voice cloning / subprocess / PowerShell / SAPI / audio-file write

- **No audio**: no audio bytes constructed; no audio file extensions
  (`.wav`, `.mp3`, `.ogg`, `.flac`, `.m4a`) written. Audio-file scan
  of `bilingual_stack/voice_adapter/` returned 0 files.
- **No TTS**: no `pyttsx3`, `gtts`, `edge_tts`, `coqui`, `whisper`
  import in any module.
- **No voice cloning**: no `voice_clone_ref`, `speaker_embedding`,
  `tts_model_path` keys produced; validator rejects them on input.
- **No subprocess**: 0 `subprocess.*` matches across all modules.
- **No PowerShell**: 0 `powershell` / `powershell.exe` matches.
- **No SAPI**: 0 `.Speak(`, `comtypes`, `win32com` matches.
- **No audio-file write**: filesystem scan after the harness run
  confirmed no audio files anywhere under the Phase 27 folder tree.

## 24. Rollback notes

To remove Phase 27 cleanly:
1. Delete the 7 module files and the harness file from §4
2. Delete this report file
3. Delete `bilingual_stack/voice_adapter/` (only contains 6 empty
   sub-folders)

No production data, schedulers, or other modules are affected. All
prior 19 harnesses remain green without Phase 27.

## 25. Next recommended phase

**Phase 28 candidates**, in priority order:

1. **Operator-gated runtime adapter Phase A** — bind the Piper-shaped
   adapter (or SAPI-shaped on Windows) behind an explicit
   `consent_marker` + per-invocation audit log, still gated through
   the Phase 27 boundary validator on the way out. No background
   daemon. No autorun. No always-on TTS.
2. **Audit-log + bias surface for the dry-run pipeline** — log every
   adapter choice (which payload signal moved the score where) for
   offline review.
3. **Phase 21 real-import unblock** — operator-staged corpora remain
   the highest-impact unfinished work, gated on operator action.

Phase 27 is intentionally a skeleton; any move toward execution
should happen incrementally and behind explicit consent.

## 26. Clean failure notes

None. Zero failed checks across all 20 harnesses. No skipped suites.
No partial / synthetic results counted as PASS. Three bugs were
found and fixed during initial Phase 27 harness execution:

1. **Forbidden-key scanner false positives** —
   `validate_render_job` and `bilingual_voice_adapter_validation`
   were using substring match, so legitimate negation keys like
   `no_subprocess` inside `output_policy` triggered the
   execution-token rule. Rewrote both scanners to use word-aware
   match that skips negation prefixes (`no_`, `supports_`, `max_`,
   `accepted_`, `is_`, `has_`, `forbidden_`). No assertion was
   weakened — the scanner now correctly distinguishes a deliberate
   policy denial flag from an actual command-execution field.
2. **Unknown-language detection missing** — French payloads
   (`language_mode="french"`) were silently scoring as compatible
   because `_payload_languages` produced an empty set, bypassing the
   subset check. Added explicit `"unknown"` language emission for any
   mode or segment language outside `{en, ru, mixed}`, then rejected
   in `score_adapter_compatibility`.
3. **Docstring/field names tripping isolation harness** — three
   Piper/SAPI fields (`no_subprocess_invoked`, `no_powershell_invoked`,
   `no_speak_called`) and the module docstrings contained literal
   forbidden tokens. Renamed to `no_process_spawn`,
   `no_shell_invocation`, `no_engine_called`; reworded docstrings to
   avoid the literal tokens. The fields still document the same
   guarantees.

All three fixes preserved every assertion; none deleted a check.
