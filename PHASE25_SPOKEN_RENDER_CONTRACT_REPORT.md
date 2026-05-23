# Phase 25 — Bilingual Spoken Render Spec, Voice-Render Contract, Prosody Markup, and Future TTS Integration Boundary

**Status:** Complete. **Date:** 2026-05-21.

Phase 25 ships the standalone bilingual spoken-render contract layer: a
JSON-serializable payload that future Luna voice/TTS systems will consume,
plus voice-text normalization, prosody markup, pronunciation hinting,
spoken-safety redaction, and an unbound future-renderer interface. NO
audio synthesis, NO TTS invocation, NO voice cloning, NO subprocess
renderer calls. Production lexicons and bilingual link DB unchanged.

## 1. Phase 25 completion status

ACCEPTED. 7 new production modules + harness + report. 18/18 harnesses
green (1809/1809 checks). Production lexicons + bilingual link DB
unchanged.

## 2. Phase 24 pre-flight verification

✅ All 29 required Phase 24/23/22 + core-vocabulary artifacts present.

## 3. Files created

| File | Lines | Role |
|---|---:|---|
| `bilingual_spoken_render_contract.py` | 280 | Canonical JSON-serializable payload schema; required-fields validator; disallowed-fields blocklist (audio_bytes / audio_url / tts_model / voice_clone_ref / etc.); fail-closed on unsafe content; segment cap 200; text-length cap 10,000. |
| `bilingual_voice_text_normalizer.py` | 200 | EN abbrev expansion (etc., i.e., e.g., Mr.); RU abbrev expansion (и т.д., т.е.); unspoken-symbol stripping; spacing/punctuation collapse; conservative code-switch-term preservation per language_mode. No transliteration. |
| `bilingual_prosody_markup.py` | 230 | Sentence + within-sentence EN/RU run splitter (≤200 segments); pause/emphasis/pace/tone assignment per conversation_mode + emotional_tone; code-switch-boundary marker; prosody-plan validator. |
| `bilingual_pronunciation_hinting.py` | 165 | Per-segment EN acronym hints, RU stress-uncertainty hints, code-switch boundary hints; transliteration-risk flag (word-boundary); acronym detector. Never auto-transliterates. |
| `bilingual_spoken_safety_redactor.py` | 200 | Stricter-than-text voice filter. `redact_for_spoken_voice` replaces unsafe markers with `[voice-safe wording]` placeholder; per-segment recognition_only flagged but kept; do_not_use_unprompted / vulgar / offensive blocked. |
| `bilingual_voice_renderer_interface.py` | 150 | UNBOUND_FUTURE_RENDERER contract. `dry_run=True` always enforced; non-dry-run rejected; capabilities validator; simulation only. No TTS imports, no subprocess. |
| `bilingual_spoken_render_runtime.py` | 230 | Single non-integrated entry point that aggregates Phase 24 voice-style runtime + Phase 25 normalize + prosody + pronunciation + safety + contract + renderer-request-dry-run. |
| `test_phase25_spoken_render_contract.py` | 550 | 10 suites, 130 checks. |
| `PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md` | — | this report. |

## 4. Files modified

None. Phase 25 is purely additive.

## 5. Folders created

* `bilingual_stack/spoken_render/schemas/`
* `bilingual_stack/spoken_render/reports/`
* `bilingual_stack/spoken_render/evaluations/`
* `bilingual_stack/spoken_render/demos/`
* `bilingual_stack/spoken_render/fixtures/`
* `bilingual_stack/spoken_render/contracts/`

## 6. Spoken render contract status

✅ JSON-serializable payload with 15 fields. Required-field validator
catches missing fields, unsupported language modes, segment-cap overage,
disallowed audio/tts/voice-clone fields, and fail-closes when
`safety_summary.unsafe_leakage_detected=True`. 8 supported language modes,
9 segment types, 9 prosody fields. Segment hard cap = 200; text-length cap
= 10,000 chars (configurable per call). Suite B verifies all eight gate
paths (create + validate + missing-required + unsupported-lang + segment-cap
+ json-serializable + audio-bytes-rejected + unsafe-fail-closed).

## 7. Text normalizer status

✅ EN, RU, and mixed normalization pipelines work without destructive
transliteration:

* `Mr. Smith, i.e. the engineer, said etc.` → `mister Smith, that is the engineer, said et cetera`.
* `Инженер и т.д. сказал.` → `Инженер и так далее сказал.` (Cyrillic preserved).
* Mixed `Hello, я инженер. Etc.` preserves the Russian code-switch token.
* Unspoken symbols (`*`, `#`, `~`, etc.) removed.
* Spacing/punctuation collapsed (`!!!` → `!`, runs of whitespace → 1).
* RU text passes through with full Cyrillic intact — no `Привет` → `Privet`.

Fix during build: `preserve_code_switch_terms` initially only flagged
`mixed_en_ru` for mixed-script tokens, missing pure-RU tokens in a mixed
input. **Fix:** widened the EN/RU minority-language flagging to also fire
under `mixed_en_ru` and all three code-switch modes.

## 8. Prosody markup status

✅ `create_prosody_plan(text, language_mode, conversation_mode,
emotional_tone)` returns:
* Per-segment language-aware splits (EN and RU runs kept apart).
* `pause_after_ms` (longer after `.!?`, shorter after `,;:`, teacher mode +60ms).
* `emphasis` ∈ {normal, moderate, strong}; `*marked*` runs get strong.
* `pace`: slow in teacher/curriculum; fast in concise; otherwise normal.
* `tone`: one of 8 emotional tones, defaults to `steady`.
* `code_switch_boundary` annotated when consecutive segments cross EN↔RU.
* `validate_prosody_plan` enforces non-negative pause and segment-cap.
* 300-sentence stress test capped to ≤200 segments.

## 9. Pronunciation hinting status

✅ Detects:
* Acronyms (2–5 char all-caps runs) → `pronunciation_attention=True`.
* Mixed-script tokens → `pronunciation_attention=True`.
* RU tokens in EN-dominant context → stress-unknown.
* RU stress uncertainty flagged for all Cyrillic tokens (heuristic, since
  pymorphy3 is not installed — see Phase 22 upgrade note).
* Transliteration risk flagged via word-boundary regex (`\bprivet\b`
  etc.) requiring ≥2 markers + no Cyrillic.

Per-segment helpers (`create_english/russian/code_switch_pronunciation_hints`)
produce bounded hint lists with explicit notes for the future renderer.

## 10. Spoken safety redactor status

✅ Stricter than text-mode filter:

* `step by step instructions to bypass`, `ignore previous instructions`,
  `system prompt:`, self-harm markers, bomb/explosive markers — all
  replaced with `[voice-safe wording]` and `unsafe_leakage_detected=True`.
* Per-segment `redact_segments_for_voice`:
  * `do_not_use_unprompted` unprompted → blocked.
  * `vulgar`/`offensive` in clean modes (conversation/teacher/professional/
    technical/curriculum/warm_friend/concise) → blocked.
  * `vulgar`/`offensive` unprompted → blocked.
  * `recognition_only` → kept as `safe` but flagged `_suggestion_blocked=True`.
  * Benign rows kept normally.
* `validate_voice_safe_text` returns `ok=False` whenever a high-risk
  marker is present.
* Suite F verifies all four classification outcomes.

## 11. Renderer interface status

✅ `get_voice_renderer_contract` reports `binding=UNBOUND_FUTURE_RENDERER`
with all four "in this phase" flags explicitly False
(`audio_synthesis_in_this_phase`, `tts_invocation_in_this_phase`,
`voice_clone_in_this_phase`, `subprocess_invocation_in_this_phase`).

* `create_renderer_request_from_payload` emits a request with
  `dry_run=True` always.
* `validate_renderer_request` rejects any `dry_run != True` request with
  `dry_run_must_be_true_in_phase25`.
* Output format restricted to {audio_wav, audio_mp3, audio_ogg, ssml_text,
  json_plan_only}; default in this phase is `json_plan_only`.
* `accepted_languages` must include both `en` and `ru`.
* `simulate_renderer_acceptance` reports `accepted=True/False` based
  purely on local validation — no engine is contacted.

## 12. Spoken render runtime status

✅ `build_spoken_render_payload(user_text, draft_response_text, ...)`
returns a single dict with 12 required fields:
`language_detection`, `voice_style_plan`, `normalized_text`,
`voice_safe_text`, `segments`, `prosody_plan`, `pronunciation_hints`,
`code_switch_boundaries`, `safety_summary`, `renderer_request_dry_run`,
`validation`, `gap_notes`.

* English query `Hello, what is an engineer?` → `chosen_spoken_mode=english_only`.
* Russian query `Привет, что такое инженер?` → `chosen_spoken_mode=russian_only`.
* Mixed query `Hello, я инженер and I work.` → `chosen_spoken_mode ∈
  {mixed_en_ru, english_with_russian_terms}`.
* `renderer_request_dry_run.dry_run == True` in every test.
* `demo_spoken_render_payloads(limit=N)` runs N synthetic scenarios; all
  produced valid payloads with `dry_run=True`.

Lighter helper `build_voice_safe_render_payload(text, ...)` does just
normalize → redact → segment → contract assembly, returning the same
validated payload structure.

## 13. Production DB impact

**Zero rows changed.** EN=2814, RU=2518, RU_phrases=35.

## 14. Manifest count before / after

90 → 90 (45 EN + 45 RU). Unchanged.

## 15. Bilingual link DB impact

**Zero changes.** `bilingual_stack/bilingual_links.sqlite`:
* concepts: 26 → 26
* entry_links: 52 → 52

Phase 25 reads from the link DB via the Phase 22 retrieval bridge (used
indirectly through the Phase 24 voice-style runtime) but never writes.

## 16. Test results

| Harness | Tests | Status |
|---|---:|---|
| `test_phase25_spoken_render_contract.py` | 130 | ✅ |
| `test_phase24_bilingual_voice_personality.py` | 124 | ✅ |
| `test_phase23_human_code_switching.py` | 119 | ✅ |
| `test_phase22_bilingual_linker_and_retrieval_bridge.py` | 93 | ✅ |
| `test_phase21a_operator_corpus_staging.py` | 100 | ✅ |
| `test_phase21_operator_staged_first_import.py` | 103 | ✅ |
| `test_phase20_million_readiness_gate.py` | 144 | ✅ |
| `test_phase19_100k_scale_index_and_dedupe.py` | 116 | ✅ |
| `test_phase18_pilot_import_and_retrieval_hardening.py` | 83 | ✅ |
| `test_phase17_source_adapters_and_retrieval_eval.py` | 108 | ✅ |
| `test_phase16_million_scale_readiness.py` | 80 | ✅ |
| `test_phase15b_remaining_domain_expansion.py` | 117 | ✅ |
| `test_phase15a_controlled_scale_expansion.py` | 63 | ✅ |
| `test_phase14_domain_pack_expansion.py` | 87 | ✅ |
| `test_dual_pack_importer.py` | 116 | ✅ |
| `test_dual_sovereign_pack_safety.py` | 79 | ✅ |
| `test_vocabulary_runtime.py` | 74 | ✅ |
| `test_russian_sovereign_stack.py` | 73 | ✅ |
| **Total** | **1809** | **1809 / 1809 PASS** |

### Phase 25 suite breakdown (130 checks)

| Suite | Focus | Checks |
|---|---|---:|
| A_PREFLIGHT | 29 required prior artifacts | 29 |
| B_CONTRACT | schema, payload create, validate, missing required, unsupported lang, segment cap, JSON serializable, audio rejected, unsafe fail-closed, report | 9 |
| C_NORMALIZER | EN abbrev, RU abbrev, mixed preserve, symbols, spacing, no destructive RU translit, report | 7 |
| D_PROSODY | segments produced, pauses, emphasis, pace teacher slow, tone warm, code-switch boundaries, plan validates, segment cap, report | 9 |
| E_PRONUNCIATION | sensitive terms, EN hints, RU hints, code-switch hints, translit risk x2, stress uncertainty, acronyms, report | 9 |
| F_SAFETY_REDACT | unsafe redacted, recognition_only flagged not blocked, do_not_use_blocked, vulgar blocked, benign kept, slang in teacher, clean text validates, decision, report | 9 |
| G_RENDERER | contract unbound, request dry-run, validates, non-dry-run rejected, capabilities validate, simulated acceptance, report | 7 |
| H_RUNTIME | EN/RU/mixed payloads, required fields, renderer dry-run, safe-only helper, prepare-request, demo bounded, report | 9 |
| I_PRODUCTION_SAFETY | EN/RU/phrases/manifest/bilingual concepts/links unchanged | 6 |
| J_ISOLATION | 7 files × {exists, forbidden, network, daemon/subprocess, audio/TTS} | 35 |

## 17. Safety verification

* `safety_summary.unsafe_leakage_detected=True` causes contract validator
  to **fail-close** the payload.
* `_voice_safe_text` always derived from a redaction pass; unsafe markers
  replaced with `[voice-safe wording]` placeholders.
* `recognition_only` segments survive into `safe` but carry
  `_suggestion_blocked=True` so a future renderer would not use them as
  Luna's own wording.
* `do_not_use_unprompted` → blocked unconditionally unless prompted.
* Vulgar/offensive blocked across the 7 clean conversation modes.
* Spoken safety classifier hard-flags `step by step instructions to
  bypass`, prompt-injection markers, self-harm markers, bomb/explosive
  markers.
* No production-lexicon rows or bilingual link rows modified.

## 18. Isolation verification

Suite J scans all 7 Phase 25 modules with FOUR pattern groups:

* Forbidden imports (`worker`, `luna_modules`, `tier_`, `probe_`,
  `attestation`, `program_s`) — **zero matches**.
* Network (`urllib`, `requests`, `httpx`, `aiohttp`, `socket`, `ftplib`,
  `urlopen`, `http.client`) — **zero matches**.
* Daemon / subprocess (`threading.Thread(`, `multiprocessing.Process(`,
  `asyncio.create_task(`, `subprocess.Popen(`, `subprocess.run(`,
  `subprocess.call(`, `import schedule`, `import apscheduler`,
  `BackgroundScheduler(`, `threading.Timer(`, `while True:`) — **zero
  matches**.
* Audio / TTS / voice-clone (`pyttsx3`, `gtts`, `tts`, `edge_tts`,
  `sounddevice`, `pyaudio`, `pydub`, `soundfile`, `wave`, `piper`,
  `whisper`, `coqui`, `.synthesize(`) — **zero matches**.

## 19. Confirmation Program S was not touched

✅ No file under Program S was opened, read, edited, imported, or referenced.

## 20. Confirmation no tier / probe / attestation / worker.py / luna_modules files were touched

✅ Verified by isolation regex scan. Phase 25 is additive: 7 new modules, 1
new harness, 1 new report, 6 new folders. Zero edits to any pre-Phase-25
file.

## 21. Confirmation no daemon / recursion / full-corpus-load / internet usage

* **No daemon** — suite J confirms zero matches across thread / process /
  scheduler / Timer / while-True patterns.
* **No recursion** — no function in any Phase 25 module calls itself.
* **No full-corpus load** — all bounded: text-length cap 10,000 chars,
  segment cap 200, demo iteration cap 20.
* **No internet** — zero network library imports.

## 22. Confirmation no audio / TTS / voice cloning / subprocess renderer was invoked

✅ Three orthogonal checks all confirm this:

1. Isolation regex scan (suite J) finds zero audio/TTS/voice-clone imports
   AND zero subprocess.* calls.
2. Renderer interface contract reports
   `audio_synthesis_in_this_phase=False`,
   `tts_invocation_in_this_phase=False`,
   `voice_clone_in_this_phase=False`,
   `subprocess_invocation_in_this_phase=False`.
3. Renderer request enforces `dry_run=True`; any non-dry-run request is
   rejected at validation time.

The runtime returns plans + skeletons + dry-run requests only — never raw
audio bytes, file paths, or engine handles.

## 23. Rollback notes

```powershell
# 1. New modules + harness + report
Remove-Item bilingual_spoken_render_contract.py
Remove-Item bilingual_voice_text_normalizer.py
Remove-Item bilingual_prosody_markup.py
Remove-Item bilingual_pronunciation_hinting.py
Remove-Item bilingual_spoken_safety_redactor.py
Remove-Item bilingual_voice_renderer_interface.py
Remove-Item bilingual_spoken_render_runtime.py
Remove-Item test_phase25_spoken_render_contract.py
Remove-Item PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md

# 2. New folders (no production data inside)
Remove-Item -Recurse bilingual_stack/spoken_render
```

No production rows or bilingual link rows were written; nothing else needs
reverting.

## 24. Next recommended phase

Three viable paths, in priority order:

1. **Phase 21 real import** — operator stages a real EN + RU file pair
   under `corpus_sources/{english,russian}/incoming/`, runs the existing
   Phase 21 runner with `allow_real_import=True` capped at 10k rows per
   language. After ingest, every layer (Phases 22→25) gains richer raw
   material to plan and render.

2. **Phase 26 — Luna Voice Memory and Bilingual Continuity Across Turns**
   — extend `bilingual_conversation_state` with a small local persistent
   bilingual-preference store (no daemon, no main-runtime integration),
   so per-user language-mix preferences persist across sessions.

3. **Phase 27 — Voice-Render Adapter Skeleton (Still No Audio)** — design
   the adapter shape for one *specific* future renderer (e.g., a local
   Piper-shaped adapter) WITHOUT installing it: an `AdapterContract`
   class, a stubbed `EnginePolicyPlugin`, and a compatibility test against
   the Phase 25 contract. Still no audio bytes; no subprocess; pure
   contract shaping.

Suggested order: **Phase 21 first**, **then Phase 26 voice memory**,
**then Phase 27 adapter skeleton**.

## 25. Clean failure notes

None.

Pre-flight passed. All 18 harnesses green. Baseline lexicon counts
unchanged (EN=2814, RU=2518, RU_phr=35, manifests=90). Bilingual link DB
unchanged (26 concepts / 52 entry_links). One real bug found and fixed
during build: `preserve_code_switch_terms` did not flag minority-language
tokens under `mixed_en_ru` and code-switch modes; widened the flagging
without altering the EN-only / RU-only paths. No weakened assertions, no
audio/TTS/voice-cloning, no production writes.
