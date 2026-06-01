# Main Brain → GPU (llama-cpp) — flip + proof (2026-05-31)

## What changed
Luna's main brain (`cognitive_sovereign_main_runtime.py`) can now run on the
**GPU via llama-cpp-python**, gated by the operator flag
`cognitive_main_gpu_llamacpp_enabled` (now **True**). gpt4all/CPU is kept as an
**automatic fallback** — any GPU load/gen failure silently drops to CPU, so the
flip can never brick her brain.

## Measured proof (real generate_main() reply, 8B hermes3)
| Path | Backend | Full reply |
|---|---|---|
| **GPU ON** | sovereign_llamacpp_gpu | **9.7 s** |
| GPU OFF (rollback) | sovereign_gpt4all_local_7b | 25.8 s |

* End-to-end **2.6× faster** (includes model load); **3.48× on pure generation**
  (17.4 vs 5.0 tok/s, measured in measure_gpu_8b.py).
* 8B full GPU offload uses ~4.8 GB VRAM into ~5.5 GB free — fits with headroom.
* Both paths produce real, in-character replies. Rollback verified.

## How it works (the edits)
- New flag `cognitive_main_gpu_llamacpp_enabled` (feature_flags + ALLOWED_FLAGS).
- `_gpu_llamacpp_enabled()` + constants `_GPU_NGL=-1`, `_GPU_N_CTX=2048`.
- `_ensure_model_and_session()`: GPU (llama-cpp) load first when the flag is on;
  ANY failure falls through to the existing gpt4all/CPU load. A loaded llama-cpp
  model needs no chat_session.
- `generate_main()`: when backend == llama_cpp, reply via `create_completion`
  (gpt4all streaming bypassed; the built prompt already carries context).
- result `backend` field reflects the real backend.

## Kill-switch / rollback
Flip `cognitive_main_gpu_llamacpp_enabled` → False (operator flag) = instant
return to gpt4all/CPU on the next model load. No code change needed.

## Honest caveats
- **Takes effect on the brain process's next start.** The currently-running Luna
  has the old code + (maybe) a gpt4all model already loaded in memory; it keeps
  using CPU until it restarts/reloads. Next boot = GPU automatically.
- **Cross-process VRAM:** if multiple Luna processes each load the 8B on GPU,
  only ~one fits in 8 GB; the rest hit the auto-fallback and run on CPU (safe,
  no crash). In practice the main brain is one process.
- **Cold GPU load** of the 8B is ~8–40 s (warm vs cold disk). Keep the brain
  warm; don't idle-unload it aggressively.
- gpt4all still prints harmless `0x7e` CUDA-DLL warnings during its availability
  probe — it's not used for the GPU path; llama-cpp drives the GPU.

## Proof scripts (one-off, reusable)
- `measure_gpu_proof.py` (1B, 5.1×), `measure_gpu_8b.py` (8B fit + 3.48×),
  `measure_main_brain_gpu.py` (real generate_main GPU vs CPU + rollback).

---

# Conversation-turn latency chain (2026-05-31 session)

Separate from the GPU flip above: a sequence of fixes to the live conversation
pipeline (`cognitive_conversation_runtime.handle_turn`). All numbers are
**warm-turn** (models already loaded) measured by `measure_turn_breakdown.py`.
Cold-vs-warm are never compared.

| Step | Fix | Warm turn |
|---|---|---|
| start | (warm path, post gpt4all-poison fix) | ~108.8 s |
| 1 | Bound BOTH voice-future reaps to 2.0 s (`_VOICE_REAP_TIMEOUT_S`) — reply returns immediately, audio plays async | 16.8 s |
| 2 | Compact JSON in `cognitive_research_memory_fabric._atomic_write` (was pretty-printing the whole store 16×/turn) | 10.6 s |
| 3 | Async post-reply reasoning (this step) | see honest note below |

## Step 3 — async post-reply reasoning (honest result)

**Change:** flag `cognitive_conversation_async_postreply_enabled` (default
False). When True, `cognitive_kernel_drive_engine.drive_turn(...)` runs
fire-and-forget on the thread pool AFTER the reply ships, instead of blocking
it. drive_turn updates KernelState for the NEXT turn and calls **no LLM**.

**What I can prove (clean, isolated — `measure_drive_turn_cost.py`):**
drive_turn's own cost = **warm median ~1.2 s (range ~1.0–1.6 s)**, cold ~1.55 s.
That is exactly the synchronous work removed from the reply's critical path.
The work is not skipped — it is deferred.

**What I could NOT prove (honest):** end-to-end warm-turn timing did not show a
clean drop. Two async-ON samples were 8.6 s (pipeline 7239 ms) and 10.1 s
(pipeline 8827 ms) vs the 10.6 s (pipeline 9089 ms) async-OFF baseline. The two
async-ON samples differ by ~1.6 s in pipeline overhead alone, and the 10.1 s run
logged an 838 s model load = the box was under heavy contention. **The
end-to-end measurement noise (~1.6 s) is as large as the effect**, so I do not
claim a clean "10.6 → 8.6 s". The defensible claim is the isolated one: ~1.2 s
of real work moved off the critical path.

**Why keep it:** it is mechanically incapable of making a turn slower (deferral
is fire-and-forget), it is flag-gated + reversible, and it removes ~1.2 s of
provable work from the path. Net: a real but modest win, honestly bounded.

**Tradeoff (operator-accepted):** when async ON, the per-turn kernel/drive audit
fields (`kernel_fusion`, `drive_snapshot`) are absent FOR THAT TURN — the
reasoning lands a turn later in state. Flip the flag False = synchronous full
reasoning with per-turn audit fields present. Instant kill-switch.

## Step 3 — decision (2026-06-01)
Built, measured, and **left OFF** (operator deferred the call to me). Rationale:
the ~1.2 s deferral is marginal on a still-~8 s turn (pipeline-dominated) and
unproven end-to-end, while the cost — kernel per-turn audit landing one turn
late — works against this system's synchronous-audit design. The lever stays
fully built: flip `cognitive_conversation_async_postreply_enabled` True to
re-enable. Revisit ON once the ~7–9 s pipeline overhead is cut and ~1.2 s is a
meaningful fraction of a fast turn.

## Step 3 — edits
- `cognitive_conversation_runtime.py`: `_async_postreply_enabled()` helper;
  drive block now `if _async_postreply_enabled() and drive_enabled:` →
  `pool.submit(kde.drive_turn, ...)` fire-and-forget; legacy unified_kernel
  path guarded with `and not _async_postreply_enabled()`.
- `cognitive_feature_flags.py`: flag default False.
- `cognitive_operator_controls.py`: flag added to `ALLOWED_FLAGS`.
- Proof script: `measure_drive_turn_cost.py`.

## Step 4 — research-fabric usage-write coalescing (2026-06-01) — BIG WIN

Profiled the warm turn (`profile_handle_turn_sections.py`, cProfile of the main
thread, 3 warm samples, median 8.49 s). The single dominant cost was NOT the
models and NOT drive_turn — it was:

  evidence_grounded_recall.recall -> research_memory_fabric.mark_card_used
  -> _save -> _atomic_write -> json.dump(WHOLE store)

`mark_card_used` rewrote the entire fabric to disk on every call, and it is the
shared hot path of evidence recall, the verifier (`_collect_support_from_recall`)
AND kernel-drive — **~16 whole-store JSON rewrites per turn** (~14.8 s cumulative
in the profiled call-tree). The 2026-05-31 compact-JSON change shrank each write
but not the 16× frequency; this is the real fix.

**Fix:** coalesce. `mark_card_used` now accumulates the usage bump
(`last_used_at_utc` / `usage_count` — low-value, delay-tolerant telemetry) in
memory and flushes to disk at most once per `USAGE_FLUSH_INTERVAL_S` (30 s),
applying ALL pending cards in ONE load+save. Thread-safe (lock), `atexit` flush
for durability, NEVER raises. Flag `cognitive_research_fabric_debounce_usage_
writes_enabled` (default **True**). Flip False = exact prior per-call-write.

**Measured proof (both runs voice-muted — Luna silent):**
| Measure | OFF (old) | ON (new) | Win |
|---|---|---|---|
| `recall()` median, isolated (`measure_fabric_recall_cost.py`) | 941.7 ms | 19.7 ms | **47.8×** |
| warm turn median, end-to-end delta (`measure_turn_silent_delta.py`) | 4.89 s | 2.54 s | **−2.36 s** |

The end-to-end delta isolates the fix (voice muted on BOTH sides). recall runs
multiple times/turn (evidence + verifier + drive), so the ~922 ms/call saving
compounds. Projected real (with-voice) turn: ~8.5 s − ~2.36 s ≈ **~6.1 s**
(projection — not re-measured with voice live, to keep Luna silent).

Why default ON (unlike Step 3): near-pure win. The only cost is usage telemetry
persisting a few seconds later; no per-turn audit content is lost, and it cannot
make a turn slower. Reversible via the flag.

## Step 4 — edits
- `cognitive_research_memory_fabric.py`: coalescing machinery (`_PENDING_USAGE`,
  `_USAGE_LOCK`, `_flush_pending_usage_locked`, `flush_usage`, `atexit` hook);
  `mark_card_used` debounced path + preserved legacy immediate-write path.
- `cognitive_feature_flags.py`: flag default True.
- `cognitive_operator_controls.py`: flag added to `ALLOWED_FLAGS`.
- Proof: `profile_handle_turn_sections.py`, `measure_fabric_recall_cost.py`,
  `measure_turn_silent_delta.py`.

## Honest caveat on the whole chain
After Step 4 the warm turn (voice-muted) is ~2.5 s and the dominant fabric-write
cost is gone. The remaining time is the two models (~1.3–2.0 s gen) plus smaller
synchronous passes (classification, dialogue pipeline, verifier compute, audit
append — note `_append_audit` still re-reads the whole ~1.6 MB ledger per turn,
a minor ~10 ms cost worth a later trim). With real voice, ~2 s of bounded
ack+main TTS reaps are added back. No silent truncation or hidden caps were
introduced.
