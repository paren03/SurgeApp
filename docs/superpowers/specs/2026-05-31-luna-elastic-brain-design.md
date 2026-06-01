# Luna Elastic Brain — Design Spec (2026-05-31)

> Goal: a "big" sovereign Luna that uses the hardware **as fully as needed but
> as little as possible** — scaling model size and GPU/CPU placement to each
> task, idle-powering-down by default. Big-on-demand, frugal-by-default.

## 1. Problem & constraints

- **Hardware:** 8 GB GPU (RTX 2080) + multi-core CPU + ~1 TB free on `D:`.
- **Tension to resolve:** operator wants "big Luna" AND "save energy + GPU."
  Resolution: an **elastic, tiered** brain — most work runs small/cheap; the
  heavy model only wakes for hard tasks, then releases the GPU.
- **Hard rules (carry over from the codebase doctrine):**
  - Sovereign / local-first. No cloud in the core (optional plug only).
  - Never modify the 1M/1M vocabulary DB.
  - All new public APIs NEVER raise.
  - Kill-switchable, bounded, reversible, audit-first.
  - No `print()` in any pythonw chain file.

## 2. Non-goals (YAGNI)

- NOT making Luna a frontier model. On 8 GB the honest ceiling is ~7–14B
  resident, 32B usable-but-slow, 70B batch-only. We say so plainly.
- NOT cloud escalation in this build (Grok/Claude). Designed as an optional
  future top rung, not implemented now.
- NOT training/fine-tuning models. We orchestrate existing GGUF weights.

## 3. Architecture overview

Four cooperating units, each independently testable:

```
query ──▶ (1) Difficulty Router ──▶ picks tier S/M/L/XL
                    │
                    ▼
        (2) Energy-Aware Model Manager ──▶ load/place/unload on GPU+CPU
                    │
                    ▼
        (3) Model tier runs (llama.cpp / existing fabric)
                    ▲
                    │ retrieved context
        (4) Knowledge Base (RAG) ◀── embed + retrieve from 1 TB store
```

## 4. Components

### 4.1 Model Library (config + on-disk weights)
Tiers (extends the existing `local_models/` set):

| Tier | Default model | Placement | Role |
|---|---|---|---|
| S (fast) | Llama-3.2-1B-Q4 | CPU / tiny GPU | acks, routing, trivial |
| M (daily) | hermes3-8B *(current main)* | full 8 GB GPU | everyday chat/reasoning |
| L (coder) | qwen2.5-coder-7B | GPU | code |
| XL (deep) | a 32B Q4 (~20 GB) | GPU+CPU split | hard reasoning, on demand |
| BATCH | optional 70B Q4 | CPU only | overnight/offline jobs |

- A JSON registry (`memory/elastic_brain/model_library.json`) describes each
  tier: file path, params, quant, est. VRAM, est. tokens/s, role tags.
- Tiers are **declarative** — adding/removing a model = editing the registry,
  no code change.
- **On-disk reality (honest):** S/M/L already exist in `local_models/`
  (Llama-3.2-1B, hermes3-8B, qwen2.5-coder-7B) — the elastic system works
  **today** with these. **XL (32B) and BATCH (70B) are NOT on disk yet** and
  require a one-time, operator-approved model download (~20 GB / ~40 GB). Per
  the safety rules a download is operator-gated, so the build ships fully
  functional on S/M/L and treats XL/BATCH as registry slots the operator fills
  when ready. No fake "we already have a 32B."

### 4.2 Energy-Aware Model Manager (`luna_modules/cognitive_elastic_model_manager.py`)
The heart of "uses GPU/CPU wisely". Public API (all NEVER raise):
- `acquire(tier) -> handle` — ensure that tier is loaded (load on demand);
  returns a handle to generate with.
- `release(tier)` / idle reaper — unload after `idle_unload_s` so the GPU
  returns to idle power.
- `_auto_gpu_layers(model)` — choose `n_gpu_layers` to fill ~8 GB, rest CPU.
- Policies:
  - **one-hot**: at most one heavy (M/L/XL) model resident; switching evicts.
  - **eco / balanced / performance** modes (flag-controlled). Eco prefers
    CPU+S; performance keeps M warm.
  - battery/thermal awareness (best-effort; bias to eco when on battery/hot).
- State + telemetry surfaced via `report()`.

### 4.3 Difficulty Router (extends existing HH `cognitive_hardware_aware_router`)
- `route(query, context) -> tier` — scores difficulty (length, task type from
  the existing classifier, code-ness, need-for-knowledge) and picks the
  **smallest tier that suffices**, honoring the current energy mode + live
  GPU/thermal state from the manager.
- Deterministic, bounded, audited (reuses the HH routing audit ledger).

### 4.4 Compressed Knowledge Vault + RAG (extends Program T knowledge ingestion)
The "1 TB feels like ~5 TB" component. A **content-addressed, compressed**
store with an index that acts as Luna's "links".

- **Layout** (`D:\SurgeApp\knowledge\`):
  - `vault.blobs` — knowledge chunked small; each chunk **zstd-compressed**
    (optionally with a trained zstd dictionary for high small-chunk ratio),
    content-addressed by hash → automatic **dedup**.
  - `vault.index` — maps logical key / vector-id → `(offset, length, codec)`.
    These index entries ARE the "links": an O(1) lookup, microseconds.
  - `vault.vectors` — embeddings (from the S-class embed model) → chunk ids,
    for semantic retrieval.
- **API (NEVER raise):** `ingest(path|text)`, `retrieve(query, k) -> snippets`,
  `get(link) -> text`, `stats()`.
- **Retrieval path:** embed query → vector search → chunk ids → index lookup →
  read small compressed blob → zstd-decompress. **Typically sub-millisecond per
  small chunk once warm** (NVMe read + µs-scale decompress); cold/large reads
  cost more — stated honestly, not "always <1ms".
- **Compression reality (honest):**
  - Text/json/md/logs/corpus → ~4–10:1 with zstd + dedup ⇒ **1 TB physical
    holds ≈ 5 TB+ of *knowledge***. This is the real "5T".
  - It does **NOT** apply to model weights (already quantized, must decompress
    to run) — those stay uncompressed in `local_models/`.
- The honest capability multiplier: a huge retrievable corpus makes a modest
  local model answer like a much bigger one. Retrieval > raw model size for
  factual work.

## 5. Data flow (one turn)
1. Query arrives → Difficulty Router scores it.
2. If it needs facts → RAG retrieves snippets into context.
3. Router picks tier; Energy Manager ensures that tier is resident (load if
   cold), placing layers across GPU/CPU.
4. Model generates; result returned.
5. Idle reaper later unloads the model → GPU powers down.

## 6. Energy policy (defaults)
- Default mode: **eco-balanced**. `idle_unload_s = 120`. One-hot heavy models.
- S tier may stay CPU-resident (cheap). M warms on first real query, not boot.
- XL/BATCH never auto-spawn; only on explicit request or scheduled offline job.

## 7. Safety / ops
- Every new module: NEVER-raise, `report()`, flag-gated master + pause switch,
  audit ledger, bounded time/memory.
- Kill-switch file disables elastic routing → falls back to today's single
  main brain (hermes3-8B). Fully reversible.
- Disk hygiene: rotate/cap the ~94 GB of `.log` bloat found in the footprint
  scan (separate, additive task; not on the hot path).

## 8. What's reused vs new
- **Reused:** model fabric, HH hardware-aware router + audit, Program S
  warm-state policy, Program T knowledge ingestion, conversation classifier,
  feature-flag + operator-control patterns.
- **New:** `cognitive_elastic_model_manager.py` (energy/GPU lifecycle), the
  model-library registry, the RAG store wiring, router difficulty→tier scoring,
  operator verbs + dashboard panel.

## 9. Testing / verification
- Per-module smoke tests (the self-improvement engine already covers this).
- Manager unit tests: load→generate→idle-unload cycle; one-hot eviction;
  auto-gpu-layers picks ≤ VRAM.
- Energy proof: measure GPU power/VRAM idle vs active (nvidia-smi) before/after
  to show idle-unload actually drops power — honest before/after numbers.
- Router proof: a query set → assert easy→S/M, hard→XL, code→L.
- Regression: existing harness suite stays green.

## 10. Phased build (high level; detailed plan via writing-plans)
1. Model-library registry + manager skeleton (load/unload/auto-layers), flag-off.
2. Idle reaper + one-hot + eco/balanced/perf modes; energy before/after proof.
3. Difficulty router difficulty→tier; wire to manager; routing proof.
4. RAG store + retrieve into context; knowledge proof.
5. Operator verbs + dashboard panel; kill-switch + rollback test.
6. Log-bloat hygiene (reclaim ~94 GB) + rotation.

## 11. Honest tradeoffs
- Cold-tier first hit pays a load delay (seconds for M/L; longer for XL) — the
  cost of powering down when idle. Acceptable for an assistant; tunable via
  `idle_unload_s` / performance mode.
- XL (32B) and BATCH (70B) are genuinely slow on 8 GB — positioned as
  "think hard" / offline, never the chat path.
- This raises Luna's local ceiling and makes her hardware-wise; it does not and
  cannot make her a frontier cloud model.
