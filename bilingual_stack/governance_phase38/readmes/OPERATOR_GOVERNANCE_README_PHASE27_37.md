# Luna Voice Adapter Governance — Operator README (Phase 27-37)


**Status:** Phase 37 complete. 30-harness regression green at 6185 / 6185.


**This is NOT real voice execution. This is NOT production secret management. This is NOT a corpus import workflow.**


## What this is, in one paragraph

Phase 27 through Phase 37 build a *governance and verification layer* around the idea of a future voice renderer. **It does not produce voice.** It defines contracts, validators, signing, witness exports, and a local exchange protocol so that the day a real audio engine is ever bound, every step before that engine has already been audited and proven safe. As of Phase 37 the system runs four *metadata-only* in-process adapters; none of them generate audio, invoke TTS, spawn a subprocess, open a socket, or write an audio file.


## The four allowed callable adapters (metadata-only)

| Adapter | Returns | Phase added |
|---|---|---|
| `dummy_metadata_adapter` | Echo + latency-shape metadata | Phase 30 |
| `bilingual_segment_metadata_adapter` | Language segment / code-switch boundary counts | Phase 31 |
| `prosody_density_metadata_adapter` | Pause / emphasis / tone marker counts | Phase 33 |
| `safety_redaction_trace_metadata_adapter` | Safety summary + redaction / recognition-only / DNU / voice-safe / vulgar-offensive counts | Phase 37 |

Each adapter MUST have these flags **False**: `produces_audio`, `invokes_tts`, `uses_subprocess`, `uses_network`, `writes_files`. The Phase 37 result verifier rejects any other adapter name.


## What is forbidden, end-to-end

- **No audio**: zero `.wav` / `.mp3` / `.ogg` / `.flac` / `.m4a` written anywhere under `bilingual_stack/voice_adapter_phase*/`.
- **No TTS**: no `pyttsx3`, `gtts`, `edge_tts`, `piper`, `coqui`, `whisper`, `pyaudio`, `sounddevice`, `pydub`, `soundfile`, `comtypes`, `win32com` imported or referenced.
- **No subprocess**: no `subprocess.run`, `subprocess.Popen`, `subprocess.call`, `os.system(`, `shell=True`, `os.popen`.
- **No PowerShell / SAPI / Piper**: no engine binding.
- **No network**: no `urllib.request`, `http.client`, `requests.`, `httpx.`, `socket.socket`.
- **No multiprocessing**: no `multiprocessing.Process`, `multiprocessing.Pool`.
- **No daemon / scheduler / watchdog / service / registry change**.
- **No production secret storage**: signing keys are in-memory HMAC-SHA256 test keys (Phase 32); secret-bearing envelopes (Phase 36) live only under `local_secret_handoff/` which ships its own `.gitignore`.
- **No corpus import**: Phase 21 real import remains blocked on operator-staged corpora.
- **No main runtime integration**: all Phase 27-37 modules are standalone; nothing imports `worker.py`, `luna_modules`, `tier_*`, `probe_*`, `attestation*`, or Program S.


## How to verify

### Quick check (30 seconds)
```
git status --short
ls bilingual_stack/voice_adapter_phase36/local_secret_handoff/   # expect only .gitignore
```

### Tests
```
python test_phase37_safety_trace_adapter_governance.py
python test_phase36_key_handoff_envelope.py
python test_phase35_witness_exchange_protocol.py
python test_phase34_external_witness_verification.py
python test_phase33_three_adapter_signed_governance.py
python test_phase32_audit_signing_and_verification.py
python test_phase31_multi_adapter_boundary.py
python test_phase30_callable_adapter_boundary.py
python test_phase29_operator_gated_runtime_adapter_b.py
python test_phase28_operator_gated_voice_adapter.py
python test_phase27_voice_render_adapter_skeleton.py
```
Every harness should print `Total: N | Pass: N | Fail: 0`.

### Signed evidence + witness chain
1. Run `prepare_phase37_four_adapter_invocation(...)` with `approve=True` and an `operator_id`.
2. The returned `signed_witness_pipeline` carries `signed_evidence_summary` (hash, algorithm, `test_only=True`) and an Phase 34 `witness_export_summary` and a Phase 35 `exchange_summary`.
3. Optionally call `verify_phase37_signed_witness_pipeline(output)` to re-check structurally.
4. Operator may, with a fresh consent marker, generate a Phase 36 sealed envelope and re-verify via `verify_witness_package_with_handoff(...)`.

### Production invariants
```
EN words: 2814
RU words: 2518
RU phrases: 35
Bilingual concepts: 26
Bilingual entry links: 52
Live pack manifests: 90
```
Any change to these means Phase 21 (corpus import) ran. If you did not intend that, stop and audit.


## What is safe to commit, what must NOT be committed

### Safe to commit
- `bilingual_voice_phase27_*.py` through `bilingual_voice_phase37_*.py` source modules
- `bilingual_safety_redaction_trace_adapter.py`, `bilingual_prosody_density_metadata_adapter.py`, `bilingual_segment_metadata_adapter.py`, `bilingual_voice_dummy_metadata_adapter.py`
- `test_phase27_*.py` through `test_phase37_*.py` harnesses
- `PHASE27_*` through `PHASE38_*` markdown reports
- `bilingual_stack/voice_adapter_phase*/` empty sub-folders (optional)
- `bilingual_stack/voice_adapter_phase36/local_secret_handoff/.gitignore` (the gitignore file itself)

### Must NOT be committed
- Runtime DBs: `lexicon/luna_vocabulary.sqlite`, `russian_stack/russian_lexicon.sqlite`, `russian_stack/russian_memory.sqlite`, `bilingual_stack/bilingual_links.sqlite`, `ruvector.db`, `corpus_sources/checkpoints/checkpoints.sqlite3`, `corpus_sources/phase20/ledger.sqlite3`
- `.claude/` settings
- `corpus_sources/backups/`, `corpus_sources/quality_samples/`, `corpus_sources/phase20/synthetic_million/`
- Any audio files (`.wav` / `.mp3` / `.ogg` / `.flac` / `.m4a`)
- **Any file under `bilingual_stack/voice_adapter_phase36/local_secret_handoff/` other than the gitignore itself ** — these may carry sealed test-key envelopes
- Anything with `private_key`, `material_hex`, `signing_key_material`, `sealed_payload` field names


## Phase 21 real corpus import (separate workflow)

Phase 21 is the operator-staged real corpus import. It is **not part of the voice-adapter governance stack**. It remains BLOCKED until the operator drops real vocabulary files into `corpus_sources/english/incoming/` and `corpus_sources/russian/incoming/`. The Phase 21 harness (`test_phase21_operator_staged_first_import.py`) and the Phase 21a staging-readiness gate (`test_phase21a_operator_corpus_staging.py`) currently report that both incoming folders are empty.

Verifying Phase 27-37 governance does NOT unblock Phase 21. Verifying Phase 27-37 governance does NOT imply anyone has heard Luna speak — no audio engine is bound.


## Phase-by-phase summary


- **Phase 27** — Voice-Render Adapter Skeleton (dry-run only). Allowed callable adapters: none.
- **Phase 28** — Operator-Gated Voice Adapter (consent + audit log + envelope, still dry-run). Allowed callable adapters: none.
- **Phase 29** — Per-Invocation Consent + Tamper-Evident Audit Chain (Phase B, still dry-run). Allowed callable adapters: none.
- **Phase 30** — First Callable Boundary — dummy metadata adapter only. Allowed callable adapters: dummy_metadata_adapter. New: dummy_metadata_adapter.
- **Phase 31** — Two Metadata-Only Adapters (Phase D). Allowed callable adapters: dummy_metadata_adapter, bilingual_segment_metadata_adapter. New: bilingual_segment_metadata_adapter.
- **Phase 32** — Audit-Chain Signing + Evidence Bundle (HMAC-SHA256 test-only). Allowed callable adapters: dummy_metadata_adapter, bilingual_segment_metadata_adapter.
- **Phase 33** — Three Metadata-Only Adapters + Signed Evidence Default (Phase E). Allowed callable adapters: dummy_metadata_adapter, bilingual_segment_metadata_adapter, prosody_density_metadata_adapter. New: prosody_density_metadata_adapter.
- **Phase 34** — External Witness Verification + Public Descriptor Export. Allowed callable adapters: dummy_metadata_adapter, bilingual_segment_metadata_adapter, prosody_density_metadata_adapter.
- **Phase 35** — Local File-Based Witness Exchange Protocol. Allowed callable adapters: dummy_metadata_adapter, bilingual_segment_metadata_adapter, prosody_density_metadata_adapter.
- **Phase 36** — Out-of-Band Test-Key Handoff Envelope (gitignored local_secret_handoff). Allowed callable adapters: dummy_metadata_adapter, bilingual_segment_metadata_adapter, prosody_density_metadata_adapter.
- **Phase 37** — Fourth Metadata-Only Callable + Signed Witness Pipeline (Phase F). Allowed callable adapters: dummy_metadata_adapter, bilingual_segment_metadata_adapter, prosody_density_metadata_adapter, safety_redaction_trace_metadata_adapter. New: safety_redaction_trace_metadata_adapter.

## How to roll back

See `bilingual_stack/governance_phase38/rollback/ROLLBACK_MATRIX.json` for per-phase file lists. Each phase rolls back by deleting its own modules + harness + report + empty sub-folder set. No prior phase depends on a later phase being present.


_Generated by Phase 38 operator README generator at 1779847466._
