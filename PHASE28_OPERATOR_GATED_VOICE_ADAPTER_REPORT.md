# Phase 28 — Operator-Gated Voice Adapter Runtime Phase A, Audit Log, Consent Boundary, and Dry-Run-to-Callable Adapter Bridge

**Date:** 2026-05-24
**Status:** COMPLETE — additive, non-destructive, dry-run only
**Production DBs:** UNCHANGED
**Audio / TTS / subprocess boundary:** uncrossed
**`approve=True` still does not execute** in Phase 28.

---

## 1. Phase 28 summary

Built the first operator-gated boundary between Phase 27 dry-run plans
and any future callable voice adapter. Seven new modules + one harness +
this report land; 21-harness regression is green at **2,731 / 2,731**.
The runtime accepts user text or an existing Phase 27 render job and
produces an audited dry-run call envelope; consent + capability +
execution-boundary + error-taxonomy + audit log are all enforced; even
`approve=True` cannot enable audio / TTS / subprocess / PowerShell /
SAPI / Piper / voice-clone in Phase 28.

## 2. Phase 27 pre-flight verification

All required Phase 27 files present (report + harness + 7 modules).
Phase 27 harness re-run: **399 / 399 PASS**.

## 3. Phase 26 / 25 pre-flight verification

All required Phase 26 files present (4 listed). Phase 26 re-run:
140 / 140 PASS. All required Phase 25 files present (4 listed).
Phase 25 re-run: 130 / 130 PASS.

## 4. Git pre-flight status

Branch `claude/strange-lumiere-5d0fc5`. 11 commits ahead of `5fe8700`.
`git status --short` shows the same intentionally-skipped runtime DBs /
backups / synthetic fixtures / `.claude/` / `ruvector.db` as untracked,
plus Phase 27 files & report still uncommitted from a prior session
(this Phase 28 work is intentionally not committed yet — same pattern).
No forbidden files staged. No Program S / tier / probe / attestation /
worker.py / luna_modules staged.

## 5. Files created

| # | File | Purpose |
|---|---|---|
| 1 | `bilingual_voice_operator_consent.py` | Consent request / decision schema + validators |
| 2 | `bilingual_voice_adapter_audit_log.py` | Bounded in-memory audit events + summary + opt-in disk write |
| 3 | `bilingual_voice_call_envelope.py` | Dry-run-only call envelope schema + validator + lifecycle |
| 4 | `bilingual_voice_execution_boundary.py` | Hard execution-intent guard (word-aware scanner) |
| 5 | `bilingual_voice_capability_negotiator.py` | Payload vs adapter capability negotiation + downgrade plan |
| 6 | `bilingual_voice_adapter_errors.py` | 16-code stable error / refusal taxonomy |
| 7 | `bilingual_voice_adapter_phase28_runtime.py` | Single standalone entrypoint composing the above |
| 8 | `test_phase28_operator_gated_voice_adapter.py` | 10-suite, 383-check harness |
| 9 | `PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md` | This report |

## 6. Files modified

Two of the just-created Phase 28 modules were rewritten in their
scanner internals only after their first harness run flagged real
bugs — these are not modifications to any prior-phase file:

- `bilingual_voice_call_envelope.py` — added `_SKIP_KEYS_FOR_SCAN` so
  the validator does not flag legitimate `output_placeholders` /
  `forbidden_actions` policy keys as forbidden tokens.
- `bilingual_voice_execution_boundary.py` — tightened `_key_matches`
  (rejects any key starting with a negation prefix even for composite
  tokens like `voice_clone`), tightened `_value_matches` (detects
  action-verb patterns like `run_subprocess` / `invoke_tts` but skips
  policy enumeration entries like `subprocess_execution`).

**Zero pre-existing Phase 14-27 modules were touched.** No assertions
were weakened anywhere.

## 7. Folders created

Under `bilingual_stack/voice_adapter_phase28/`:
- `contracts/`
- `audit_logs/`
- `reports/`
- `evaluations/`
- `fixtures/`
- `demos/`

All currently empty — created on-demand by the report-writer helpers
when the operator chooses to write inspection artifacts.

## 8. Operator consent status

`bilingual_voice_operator_consent.py`:
- `get_operator_consent_schema()` — version, supported actions (only
  `dry_run_prepare`), rejected actions (5: `execute_audio`,
  `run_tts`, `run_subprocess`, `write_audio`, `speak_now`)
- `create_consent_request` / `validate_consent_request` —
  rejects all 5 forbidden actions
- `create_consent_decision` / `validate_consent_decision` — requires
  `operator_id` when `approved=True`, `dry_run_only=True` always
- `require_phase28_dry_run_only` — Phase 28 always returns
  `dry_run_only=True`
- `explain_consent_boundary` — explicit human-readable summary

## 9. Audit log status

`bilingual_voice_adapter_audit_log.py`:
- 11 supported event types: `preflight`, `payload_validation`,
  `consent_request`, `consent_decision`, `boundary_guard`,
  `adapter_selection`, `compatibility_check`,
  `render_envelope_created`, `refusal`, `error`, `dry_run_complete`
- 7 supported status values (`ok`, `warn`, `error`, `refused`,
  `blocked`, `skipped`, `info`)
- In-memory list by default; bounded at **500** events
- `append_audit_event` enforces cap by trimming oldest
- Defensive transcript stripping: any `transcript` /
  `full_transcript` / `user_text_raw` / `assistant_text_raw` keys are
  removed from metadata at creation time and flagged as forbidden by
  the validator
- `write_audit_log` + `read_audit_log` are explicit-only; reads
  bounded to 500

## 10. Call envelope status

`bilingual_voice_call_envelope.py`:
- 14 required fields enforced by `validate_call_envelope`
- `dry_run` and `execution_blocked` always coerced to `True`
- 4 permitted actions: `validate`, `plan`, `simulate_acceptance`,
  `write_report`
- 9 forbidden actions: `generate_audio`, `invoke_tts`,
  `run_subprocess`, `call_powershell`, `call_sapi`, `call_piper`,
  `write_audio_file`, `clone_voice`, `network_call`
- 10 forbidden key tokens scanner (audio bytes / urls / paths /
  voice_clone_ref / speaker_embedding / tts_model_path /
  output_audio_file)
- `normalize_call_envelope` is fail-safe — given any input dict it
  emits an envelope with both safety flags forced to `True`
- Audio-bytes injection is rejected by the validator

## 11. Execution boundary status

`bilingual_voice_execution_boundary.py`:
- 17 forbidden action tokens: `generate_audio`, `synthesize`,
  `speak`, `tts`, `piper`, `sapi`, `powershell`, `subprocess`,
  `shell`, `os.system`, `audio_path`, `wav`, `mp3`, `voice_clone`,
  `speaker_embedding`, `network`, `download`
- Word-aware scanner with two false-positive defences:
  - **Key scan**: rejects any key starting with a negation prefix
    (`no_*` / `supports_*` / `max_*` / `forbidden_*`)
  - **Value scan**: ignores policy-enumeration strings like
    `subprocess_execution` / `audio_generation` / `network_call`;
    catches action-verb patterns `run_subprocess` / `invoke_tts` /
    `generate_audio` etc.
- `build_boundary_result` always sets `execution_blocked=True` and
  fails closed if any hit is found

## 12. Capability negotiator status

`bilingual_voice_capability_negotiator.py`:
- `extract_payload_requirements` — 12 derived fields (language mode,
  RU/EN requirements, code-switching, segments, prosody,
  pronunciation hints, emotional tone, safety redaction, text chars,
  segment count, dry-run requirement)
- `extract_adapter_capabilities` — mirrors descriptor + derives
  `supports_safety_redaction` from the presence of `audio_generation`
  AND `tts_invocation` in `forbidden_runtime_actions`
- `identify_unsupported_features` — produces typed
  `language:*` / `feature:*` / `limit:*` / `policy:*` tags
- `negotiate_capabilities` — hard-rejects any adapter that lacks
  safety-redaction support
- `propose_safe_downgrade_plan` — METADATA only; never strips safety
  metadata or language labels
- `score_negotiation_result` — bounded [0.0, 1.0]

## 13. Error taxonomy status

`bilingual_voice_adapter_errors.py` — **16 stable error codes**:
`PHASE28_EXECUTION_BLOCKED`, `CONSENT_MISSING`, `CONSENT_INVALID`,
`UNSAFE_PAYLOAD`, `UNSUPPORTED_LANGUAGE_MODE`,
`UNSUPPORTED_CODE_SWITCHING`, `UNSUPPORTED_PROSODY`,
`UNSUPPORTED_PRONUNCIATION_HINTS`, `ADAPTER_DRY_RUN_REQUIRED`,
`AUDIO_FIELD_FORBIDDEN`, `SUBPROCESS_FIELD_FORBIDDEN`,
`NETWORK_FIELD_FORBIDDEN`, `VOICE_CLONE_FIELD_FORBIDDEN`,
`PAYLOAD_INVALID`, `CAPABILITY_MISMATCH`, `UNKNOWN_ADAPTER`.

9 of these are auto-classed as `blocking` (force-overrides any
caller-supplied severity). Unknown codes fall back to
`PAYLOAD_INVALID` with severity `error`.

## 14. Phase 28 runtime status

`bilingual_voice_adapter_phase28_runtime.py` exposes:
- `prepare_operator_gated_voice_call(...)` — full pipeline from user
  text → Phase 27 dry-run pipeline → consent request/decision →
  execution-boundary check → capability negotiation → call envelope →
  audited dry-run-complete event
- `prepare_from_existing_render_job(...)` — same flow, starting from
  an already-built Phase 27 render job
- `validate_phase28_voice_call_envelope(...)` — passthrough validator
- `demo_phase28_operator_gated_calls(limit=12)` — 12-scenario replay
  covering EN, RU, mixed, teacher / professional modes, EN/RU
  practice, and approve=True / approve=False permutations
- `write_phase28_runtime_report(...)` — opt-in disk writer

Output structure: 12 fields per call (`phase28_id`,
`dry_run_pipeline_result`, `consent_request`, `consent_decision`,
`boundary_result`, `capability_negotiation`, `call_envelope`,
`audit_events`, `errors`, `status`, `next_allowed_actions`,
`forbidden_actions`, `gap_notes`).

`approve=True` flows still produce envelopes with
`execution_blocked=True` and `dry_run=True` — the consent decision is
recorded honestly, but the boundary cannot move in Phase 28.

## 15. Production DB impact

**Zero changes.**

| Store | Before Phase 28 | After Phase 28 |
|---|---|---|
| `lexicon/luna_vocabulary.sqlite::words` | 2,814 | 2,814 |
| `russian_stack/russian_lexicon.sqlite::words` | 2,518 | 2,518 |
| `russian_stack/russian_lexicon.sqlite::phrases` | 35 | 35 |
| `bilingual_stack/bilingual_links.sqlite::concepts` | 26 | 26 |
| `bilingual_stack/bilingual_links.sqlite::entry_links` | 52 | 52 |
| Live pack manifests | 90 | 90 |

## 16. Manifest count before / after

Before: **90**. After: **90**. Unchanged.

## 17. Bilingual link DB impact

Concepts 26 → 26. Entry links 52 → 52. Phase 28 does not read or
write the bilingual link DB.

## 18. Test results

Full **21-harness** regression:

| Harness | Result |
|---|---|
| test_phase28_operator_gated_voice_adapter | **383 / 383** |
| test_phase27_voice_render_adapter_skeleton | 399 / 399 |
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
| **TOTAL** | **2,731 / 2,731** |

## 19. Safety verification

- Every call envelope is normalized so `dry_run=True` and
  `execution_blocked=True` regardless of caller input
- All 5 forbidden consent actions (`execute_audio`, `run_tts`,
  `run_subprocess`, `write_audio`, `speak_now`) are rejected at
  request creation AND validation
- `approve=True` without `operator_id` is rejected by
  `validate_consent_decision`
- 9 forbidden envelope actions enumerated in every envelope
- Adapter missing safety-redaction support is rejected by
  capability negotiation
- Unsafe payloads (Phase 27 `refused_unsafe_payload` /
  `rejected_invalid_input`) refuse the envelope unconditionally
- Transcript-shaped keys in audit metadata are stripped at creation
  and rejected by validator

## 20. Isolation verification

J_ISOLATION harness scanned all 7 Phase 28 modules against five
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
- Threading / daemons (`threading.Thread`,
  `multiprocessing.Process`, `daemon=True`, `asyncio.create_task`,
  `schedule.every`): **0 hits**

## 21. Program S untouched confirmation

Confirmed. Zero matches for `program_s` / `ProgramS` / `program-s` /
`programs/` across all 7 Phase 28 modules.

## 22. tier / probe / attestation / worker.py / luna_modules untouched

Confirmed. J_ISOLATION suite scans each of the 7 Phase 28 modules
for tokens `luna_modules`, `import worker`, `from worker`, `tier_`,
`probe_`, `attestation`. **35 checks**, all PASS.

## 23. No daemon / recursion / full-corpus-load / internet

- **No daemon**: zero matches for `daemon=True`,
  `threading.Thread`, `multiprocessing.Process`,
  `asyncio.create_task`, `schedule.every`.
- **No recursion blow-up**: the only recursion in Phase 28 lives in
  `_scan_keys` / `_walk` of the boundary + envelope scanners; both
  carry a `visited` id-set to short-circuit cycles, and all inputs
  are JSON-shaped trees of bounded depth.
- **No full-corpus load**: Phase 28 reads no corpus files.
- **No internet / download**: zero matches for `urllib`,
  `http.client`, `requests`, `httpx`, `socket.socket`.

## 24. No audio / TTS / voice-clone / subprocess / PowerShell / SAPI / Piper / audio-file write

- **No audio file written**: post-harness scan of
  `bilingual_stack/voice_adapter_phase28/` returned **0** audio
  files (`.wav` / `.mp3` / `.ogg` / `.flac` / `.m4a`).
- **No TTS / voice clone**: no `pyttsx3`, `gtts`, `edge_tts`,
  `coqui`, `whisper` imports anywhere.
- **No subprocess / PowerShell / SAPI / Piper**: `subprocess.*` /
  `powershell` / `comtypes` / `win32com` / `.Speak(` / `import piper`
  all clean.
- **No execution invocations**: `os.system(`, `os.popen`,
  `ctypes.windll` clean. `shell=True` clean.
- Envelope validator + execution-boundary validator together fail
  closed on any of the above appearing as a key or as an action-verb
  value.

## 25. Rollback notes

To remove Phase 28 cleanly:
1. Delete the 7 module files + the harness file + this report
2. Delete `bilingual_stack/voice_adapter_phase28/` (only 6 empty
   sub-folders)

No production data, no schedulers, no other modules are affected.
The other 20 harnesses remain green without Phase 28.

## 26. Next recommended phase

**Phase 29 candidates**, in priority order:

1. **Phase B operator-gated runtime adapter (per-invocation consent +
   audit log + boundary validator on each call)** — the first phase
   that may bind a single adapter behind explicit per-invocation
   consent. Still no background daemon, no auto-spawn; the operator
   types the equivalent of "yes, do this one call". The Phase 28
   envelope becomes the request shape; the runtime adapter becomes
   the responder under a strictly scoped permission.
2. **Audit-log signing + tamper-evident chain** — sign each event
   with a key the operator owns; refuse audit reads if the chain
   breaks.
3. **Phase 21 real-import unblock** — still gated on operator-staged
   corpora in `corpus_sources/{english,russian}/incoming/`; remains
   the highest-impact gated work.

## 27. Clean failure notes

None. Two bugs were caught and fixed *during* the first Phase 28
harness run:

1. **Envelope scanner false positive on `audio_bytes_present`** —
   the envelope's own `output_placeholders.audio_bytes_present` key
   was being flagged. Fix: added `_SKIP_KEYS_FOR_SCAN` so the
   validator does not walk legitimate policy / placeholder keys.
2. **Execution boundary false positive on
   `forbidden_runtime_actions` enumeration** — strings like
   `subprocess_execution` (a policy enumeration entry, not a command
   request) were being flagged. Fix: tightened `_key_matches` so
   composite tokens like `voice_clone` are rejected when the key
   begins with a negation prefix (`no_voice_clone`), and tightened
   `_value_matches` so policy-enumeration values are skipped while
   action-verb patterns (`run_subprocess`, `invoke_tts`) are still
   caught. No assertion was weakened.

After both fixes: 383 / 383 PASS on Phase 28, 21-harness regression
**2,731 / 2,731 PASS**.
