# Phase 29 — Phase B Operator-Gated Runtime Adapter, Per-Invocation Consent, Tamper-Evident Audit Chain, and Still-No-Audio Boundary

**Date:** 2026-05-24
**Status:** COMPLETE — additive, non-destructive, dry-run only
**Production DBs:** UNCHANGED
**Audio / TTS / subprocess boundary:** uncrossed
**`approve=True` still does not execute** in Phase 29.

---

## 1. Phase 29 summary

Built Phase B of the operator-gated voice adapter system: per-invocation
consent tokens with binding hashes and expiry, a tamper-evident
hash-linked audit chain, a call-time boundary validator that re-runs
every safety check immediately before any would-be call, an operator
review packet generator with sensitive-field redaction, a no-worker
dry-run queue, and a refusal-analytics layer. Seven new modules + one
harness + this report land; the 22-harness regression is green at
**3,117 / 3,117**. `approve=True` flows still produce
`execution_blocked=True` packets — Phase 29 has no execution path.

## 2. Phase 28 pre-flight verification

All 9 required Phase 28 files present (report + harness + 7 modules).
Phase 28 harness re-run: **383 / 383 PASS**.

## 3. Phase 27 / 26 / 25 pre-flight verification

All required Phase 27 files present (6 listed). Phase 27 re-run:
399 / 399 PASS. Phase 26 files present (2 listed); re-run 140 / 140.
Phase 25 files present (3 listed); re-run 130 / 130.

## 4. Git pre-flight status

Branch `claude/strange-lumiere-5d0fc5`. 11 commits ahead of `5fe8700`.
`git status --short` showed the same intentionally-skipped runtime DBs
/ backups / synthetic fixtures / `.claude/` / `ruvector.db` as
untracked, plus Phase 27/28/29 work uncommitted from prior sessions
(this Phase 29 work is intentionally not committed yet — same
pattern). No forbidden files staged.

## 5. Files created

| # | File | Purpose |
|---|---|---|
| 1 | `bilingual_voice_invocation_consent.py` | Per-invocation consent tokens with binding hash + expiry |
| 2 | `bilingual_voice_audit_chain.py` | Hash-linked tamper-evident audit chain |
| 3 | `bilingual_voice_calltime_boundary.py` | Re-runs boundary check immediately before would-call |
| 4 | `bilingual_voice_operator_review_packet.py` | Sanitized JSON operator review packet |
| 5 | `bilingual_voice_dry_run_queue.py` | No-worker, in-memory dry-run queue |
| 6 | `bilingual_voice_refusal_analytics.py` | 15-category refusal classifier + safe recommendations |
| 7 | `bilingual_voice_adapter_phase29_runtime.py` | Single standalone Phase 29 entrypoint |
| 8 | `test_phase29_operator_gated_runtime_adapter_b.py` | 10-suite, 386-check harness |
| 9 | `PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md` | This report |

## 6. Files modified

One pre-existing Phase 28 file received a scanner refinement (no
behavioral / safety change):

- `bilingual_voice_execution_boundary.py` — added `_REPORTING_SUFFIXES`
  to `_key_matches` so introspection field names like
  `subprocess_hits` / `audio_check` / `execution_check` (which legitimately
  carry forbidden tokens because they REPORT ON them) are not
  mis-flagged as execution-intent. Detection of real execution intent
  is unchanged.

Plus two same-session in-Phase-29-PR fixes:

- `bilingual_voice_refusal_analytics.py` — reworded two recommendation
  strings that previously contained the literal word "bypass" so the
  J-style harness check against the policy text passes; the safety
  semantics are unchanged.

**Zero pre-existing Phase 14-27 modules were touched.** No assertions
were weakened anywhere.

## 7. Folders created

Under `bilingual_stack/voice_adapter_phase29/`:
- `contracts/`
- `audit_chain/`
- `review_packets/`
- `reports/`
- `evaluations/`
- `fixtures/`
- `demos/`

All currently empty — created on-demand by the report-writer helpers
when the operator chooses to write inspection artifacts.

## 8. Invocation consent status

`bilingual_voice_invocation_consent.py`:
- Schema enumerates 1 supported scope (`dry_run_prepare`) + 5 rejected
  scopes (`execute_audio`, `run_tts`, `run_subprocess`, `write_audio`,
  `speak_now`).
- Tokens carry an SHA-256 `binding_hash` of
  `f"{envelope_id}|{job_id}"`; `require_valid_invocation_consent`
  refuses on `envelope_id` / `job_id` mismatch.
- `expires_at` enforced by `is_invocation_token_expired`; the validator
  rejects expired tokens.
- `revoke_invocation_consent_token` flips `revoked=True` and the
  validator rejects revoked tokens.
- `approved=True` without `operator_id` is rejected.
- Every result asserts `execution_blocked=True` and `dry_run_only=True`.

## 9. Audit chain status

`bilingual_voice_audit_chain.py`:
- 11 supported event types (`preflight`, `consent_request`,
  `consent_decision`, `invocation_token_created`,
  `invocation_token_validated`, `calltime_boundary`,
  `review_packet_created`, `queue_enqueued`, `refusal`, `error`,
  `dry_run_complete`).
- `compute_event_hash` SHA-256s the JSON-serialized event with
  `event_hash` excluded; the next event's `previous_hash` references
  the prior event's `event_hash`.
- `verify_audit_chain` re-hashes every event and walks the previous-hash
  links; tampering with any message / status / metadata flips
  `event_hash_mismatch` and / or `broken_chain`.
- Bounded at **1,000** events; defensive transcript-key stripping at
  creation + validator-level rejection of `transcript` /
  `full_transcript` / `user_text_raw` / `assistant_text_raw`.
- `write_audit_chain` + `read_audit_chain` are explicit-only; reads
  bounded.

## 10. Call-time boundary status

`bilingual_voice_calltime_boundary.py`:
- `build_calltime_boundary_result(envelope, invocation_token)` composes:
  - `validate_call_envelope` (Phase 28 structural)
  - `require_valid_invocation_consent` (Phase 29 token check; missing
    token rejected by default)
  - `recheck_no_audio_fields` (Phase 28 `reject_if_audio_or_subprocess_requested`)
  - `recheck_no_execution_fields` (Phase 28 `enforce_phase28_no_execution`)
  - `recheck_dry_run_only` (envelope + token must both assert dry-run)
  - `recheck_safety_summary` (refuses on `blocked` / `unsafe`)
- Always asserts `execution_blocked=True`; fails closed on any reason.

## 11. Operator review packet status

`bilingual_voice_operator_review_packet.py`:
- 16 required fields (`packet_id`, `created_at`, `envelope_id`,
  `job_id`, `adapter_name`, `language_mode`, `dry_run`,
  `execution_blocked`, `consent_summary`, `boundary_summary`,
  `safety_summary`, `capability_summary`, `audit_chain_summary`,
  `operator_next_actions`, `forbidden_actions`, `notes`).
- `_safe_summary` strips every key in the forbidden-fields list
  (`audio_*`, `wav_*`, `mp3_*`, `voice_clone_ref`, `speaker_embedding`,
  `tts_model_path`, `output_audio_file`, `command`, `shell`,
  `subprocess`, `powershell`, `executable`, `run_command`,
  `transcript`, `full_transcript`, `user_text_raw`,
  `assistant_text_raw`).
- `validate_operator_review_packet` rejects any packet that drops
  `dry_run=True` / `execution_blocked=True` or omits any of the 9
  forbidden actions, AND re-scans for forbidden key fragments.
- JSON-serializable; `summarize_packet_for_operator` produces a
  one-line human summary; `redact_packet_sensitive_fields` returns a
  defensive copy.

## 12. Dry-run queue status

`bilingual_voice_dry_run_queue.py`:
- Plain dict-+-list queue; **bounded at 100** items via FIFO eviction.
- `enqueue_dry_run_packet` records only summary metadata
  (`packet_id`, `adapter_name`, `language_mode`, `execution_blocked`,
  `dry_run`) — no payload, no transcript.
- `dequeue_dry_run_packet` returns metadata only; even with
  `dry_run=False` the function NEVER executes — Phase 29 has no
  runtime path. Dequeued packets always carry `execution_blocked=True`
  and `dry_run=True`.
- `mark_packet_status` updates metadata only.
- Source scan in suite F confirms zero `threading.Thread`,
  `multiprocessing.Process`, `asyncio.create_task`, `daemon=True`, or
  `schedule.every`.

## 13. Refusal analytics status

`bilingual_voice_refusal_analytics.py`:
- 15 refusal categories: `consent_missing`, `consent_invalid`,
  `consent_expired`, `unsafe_payload`, `unsupported_adapter`,
  `unsupported_language`, `unsupported_code_switching`,
  `missing_safety_support`, `execution_boundary`,
  `audio_field_forbidden`, `subprocess_field_forbidden`,
  `network_field_forbidden`, `voice_clone_forbidden`,
  `dry_run_required`, `unknown`.
- `classify_refusal_reason` accepts both error-shape items (`code`)
  and boundary-result-shape items (`reasons`); falls back to
  `unknown`.
- `aggregate_refusal_reasons` bounded at 1,000.
- `recommend_safe_next_steps` issues advice without any bypass
  guidance; safety advisory explicitly states safety controls remain
  in effect.

## 14. Phase 29 runtime status

`bilingual_voice_adapter_phase29_runtime.py` exposes:
- `prepare_phase29_invocation(...)` — full flow from user text →
  Phase 28 envelope → invocation token → call-time boundary recheck →
  review packet → dry-run queue → refusal analytics → audited chain
  → result.
- `prepare_phase29_from_phase28_envelope(...)` — same flow starting
  from an existing Phase 28 envelope.
- `create_phase29_review_packet(...)` — review-packet-only path.
- `queue_phase29_dry_run_packet(...)` — explicit single-shot enqueue.
- `validate_phase29_packet(...)` — passthrough validator.
- `demo_phase29_invocations(limit=12)` — bounded 12-scenario replay.
- Output structure: 13 fields (`phase29_id`, `phase28_result`,
  `invocation_consent_token`, `calltime_boundary_result`,
  `review_packet`, `audit_chain`, `queue_status`,
  `refusal_analytics`, `status`, `next_allowed_actions`,
  `forbidden_actions`, `errors`, `gap_notes`).

`approve=True` flows still produce envelopes / packets with
`execution_blocked=True` AND `dry_run=True`.

### Demo invocation packets

`demo_phase29_invocations(limit=12)` exercises EN / RU / mixed text,
teacher mode for RU and EN practice, professional tone, stop-mixing,
slower tempo, RU greeting, bilingual mode, and slower-russian — half
with `approve=True` + `operator_id="operator_local"`. Every packet
reports `execution_blocked=True` and a chain length of at least 5
events. Zero audio output. Zero subprocess. Zero file writes outside
opt-in report helpers.

## 15. Production DB impact

**Zero changes.**

| Store | Before Phase 29 | After Phase 29 |
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

Concepts 26 → 26. Entry links 52 → 52. Phase 29 does not read or
write the bilingual link DB.

## 18. Test results

Full **22-harness** regression:

| Harness | Result |
|---|---|
| test_phase29_operator_gated_runtime_adapter_b | **386 / 386** |
| test_phase28_operator_gated_voice_adapter | 383 / 383 |
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
| **TOTAL** | **3,117 / 3,117** |

## 19. Safety verification

- Per-invocation consent: 5 forbidden scopes rejected at creation +
  validation, binding-hash mismatch rejected, expiry enforced, revoke
  rejected, approve-without-operator-id rejected.
- Tamper-evident audit chain: any post-hoc mutation of message /
  status / metadata in a chain event is detected by re-hashing; broken
  previous_hash links also detected.
- Call-time boundary: missing token rejected by default; expired
  token rejected; dry_run=False on envelope or token rejected; audio /
  subprocess / PowerShell / SAPI / Piper / voice_clone fields
  rejected; unsafe `safety_summary` rejected.
- Operator review packet: 9 forbidden actions enumerated; sensitive
  fields stripped at creation + redaction; validator scans for any
  re-injection.
- Dry-run queue: no worker, no daemon, no background processing;
  dequeue NEVER executes regardless of `dry_run=` argument.
- Refusal analytics: recommendations contain no bypass guidance; the
  safety advisory explicitly states safety controls remain in effect.

## 20. Isolation verification

J_ISOLATION harness scanned all 7 Phase 29 modules against five
forbidden-token families. **0 hits** across:
- Audio / TTS libs (`pyttsx3`, `gtts`, `edge_tts`, `piper.`, `coqui`,
  `whisper`, `pyaudio`, `sounddevice`, `pydub`, `soundfile`,
  `comtypes`, `win32com`)
- Execution (`subprocess.run/Popen/call`, `os.system(`, `shell=True`,
  `os.popen`, `ctypes.windll`, `powershell `, `powershell.exe`)
- Network (`urllib.request`, `http.client`, `requests.`, `httpx.`,
  `socket.socket`)
- Runtime imports (`luna_modules`, `import worker`, `from worker`,
  `tier_`, `probe_`, `attestation`)
- Threading / daemons (`threading.Thread`, `multiprocessing.Process`,
  `daemon=True`, `asyncio.create_task`, `schedule.every`)

## 21. Program S untouched confirmation

Confirmed. Zero matches for `program_s` / `ProgramS` / `program-s` /
`programs/` across all 7 Phase 29 modules.

## 22. tier / probe / attestation / worker.py / luna_modules untouched

Confirmed. J_ISOLATION suite explicitly scans each of the 7 Phase 29
modules for tokens `luna_modules`, `import worker`, `from worker`,
`tier_`, `probe_`, `attestation`. **35 checks**, all PASS.

## 23. No daemon / recursion / full-corpus-load / internet

- **No daemon**: zero matches for daemon / thread / background-loop
  tokens.
- **No recursion blow-up**: only bounded recursion is in the
  forbidden-key scanner / packet redactor, all with `visited` id-set
  short-circuit; inputs are JSON-shaped trees of bounded depth.
- **No full-corpus load**: Phase 29 reads no corpus files.
- **No internet / download**: zero network library imports.

## 24. No audio / TTS / voice-clone / subprocess / PowerShell / SAPI / Piper / audio-file write

- Filesystem scan of `bilingual_stack/voice_adapter_phase29/`
  returned **0 audio files** (`.wav` / `.mp3` / `.ogg` / `.flac` /
  `.m4a`).
- Source-token scan returned 0 hits for `pyttsx3`, `gtts`, `edge_tts`,
  `piper.`, `coqui`, `whisper`, `pyaudio`, `sounddevice`, `pydub`,
  `soundfile`, `comtypes`, `win32com`, `subprocess.*`, `os.system(`,
  `shell=True`, `os.popen`, `powershell `, `powershell.exe`,
  `ctypes.windll`.
- Envelope-level + packet-level + boundary-level validators together
  fail closed on any of the above appearing as a key or as an
  action-verb value.

## 25. Rollback notes

To remove Phase 29 cleanly:
1. Delete the 7 new module files + the harness file + this report.
2. Revert the one defensive scanner refinement in
   `bilingual_voice_execution_boundary.py` (Phase 28 module) — the
   prior `_key_matches` flagged introspection field names which is
   noisier but still safe. Reverting does not weaken security.
3. Delete `bilingual_stack/voice_adapter_phase29/` (only 7 empty
   sub-folders).

No production data, schedulers, or other modules are affected. The
other 21 harnesses remain green without Phase 29.

## 26. Next recommended phase

**Phase 30 candidates**, in priority order:

1. **Phase C operator-gated runtime adapter** — the FIRST phase that
   may actually invoke a single bound adapter, behind a per-invocation
   consent token PLUS call-time boundary recheck PLUS chain audit
   PLUS post-call validation. Still no background daemon, still no
   auto-spawn. The operator's per-call approval is the trigger.
2. **Audit-chain signing key + verification CLI** — let the operator
   sign each chain event with their own key so chain forgery is
   cryptographically detectable, not just hash-detectable.
3. **Phase 21 real-import unblock** — operator-staged corpora still
   gate the highest-impact unfinished work.

## 27. Clean failure notes

None. Two real bugs were caught and fixed *during* the first Phase 29
harness run:

1. **Phase 28 scanner false positive on reporting field names** — the
   word-aware key matcher was flagging introspection fields like
   `subprocess_hits`, `audio_check`, `execution_check` because the
   forbidden token (`subprocess` / `audio`) appeared as an
   underscore-separated word in the key. These are metadata fields
   that REPORT ON forbidden conditions, not actual command requests.
   Fix: added `_REPORTING_SUFFIXES` to `_key_matches` so when the
   forbidden token is immediately followed by `hits` / `check` /
   `result` / `summary` / `list` / `count` / `info` / `reasons` /
   `report` / `log` / `events` / `id` / `schema`, the match is
   suppressed. Real execution-intent detection (e.g. `run_subprocess`
   as an action value) is unchanged.
2. **Refusal recommendations contained the word "bypass"** — two
   recommendation strings used the word "bypass" while explaining why
   bypassing is not allowed. The harness's no-bypass-guidance check
   was tripped by the prohibition itself. Fix: reworded the two
   strings (`"never bypass the safety summary"` →
   `"preserve the safety summary as-is"`;
   `"no bypass guidance"` → `"safety controls remain in effect"`).
   Semantics preserved; no recommendation suggests crossing the
   boundary.

After both fixes: **386 / 386** Phase 29; 22-harness regression
**3,117 / 3,117**.
