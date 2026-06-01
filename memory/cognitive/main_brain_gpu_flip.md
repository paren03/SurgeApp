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
