"""VRAM safety guard — prevents GPU OOM crashes.

Query real-time VRAM usage via pynvml/nvidia-ml-py and gate any operation
that would exceed the safe budget. NEVER raises. Graceful fallback when
nvidia libraries are unavailable (non-NVIDIA host or no GPU).

Origin: 2026-06-02 hard GPU OOM crash. The 8B brain (4.8 GB weights +
4 GB KV@32K) plus XTTS voice clone (~2 GB) combined to ~11 GB on an 8 GB
RTX 2080 → kernel panic / hard reboot. This guard runs BEFORE loading any
model and picks the largest safe n_ctx that fits given current VRAM usage.

Public API (all NEVER raise):
  vram_free_mb()             -> int MB free (0 if unavailable)
  vram_used_mb()             -> int MB used
  vram_total_mb()            -> int MB total
  vram_report()              -> dict {total, used, free, gpu_name, ok}
  safe_n_ctx(requested, *, kv_bytes_per_token, headroom_mb) -> int
  fits(needed_mb, *, headroom_mb) -> bool
  assert_fits(needed_mb, label)   -> raises RuntimeError if not safe
"""
from __future__ import annotations

import os
from typing import Any, Dict

# KV cache bytes per token for Llama 3.1 8B (GQA-8, fp16)
# = 2 (K+V) × 32 (layers) × 8 (KV heads) × 128 (head_dim) × 2 (fp16 bytes)
LLAMA_31_8B_KV_BYTES_PER_TOKEN: int = 131_072  # 128 KB

# Headroom reserved for CUDA context, OS, and other GPU residents (MB).
# Keeps Luna below the hard limit even if other processes take a little.
DEFAULT_HEADROOM_MB: int = 800

# Hard safety ceiling — never allocate above this fraction of total VRAM.
VRAM_SAFETY_FRACTION: float = 0.88  # use at most 88 % of total VRAM

# Absolute minimum n_ctx we'll ever return — below this the brain is useless.
MIN_N_CTX: int = 512


def _nvml():
    """Lazy-import pynvml / nvidia-ml-py. Returns module or None."""
    for mod in ("pynvml", "nvidia_smi"):
        try:
            import importlib
            m = importlib.import_module(mod)
            m.nvmlInit()
            return m
        except Exception:  # noqa: BLE001
            continue
    return None


def vram_report() -> Dict[str, Any]:
    """Return {total_mb, used_mb, free_mb, gpu_name, ok}. NEVER raises."""
    nv = _nvml()
    if nv is None:
        return {"total_mb": 0, "used_mb": 0, "free_mb": 0,
                "gpu_name": "unavailable", "ok": False,
                "reason": "nvidia-ml-py not importable or no GPU"}
    try:
        h = nv.nvmlDeviceGetHandleByIndex(0)
        m = nv.nvmlDeviceGetMemoryInfo(h)
        raw_name = nv.nvmlDeviceGetName(h)
        name = raw_name.decode() if isinstance(raw_name, bytes) else str(raw_name)
        return {
            "total_mb": m.total // 1024 ** 2,
            "used_mb":  m.used  // 1024 ** 2,
            "free_mb":  m.free  // 1024 ** 2,
            "gpu_name": name,
            "ok": True,
        }
    except Exception as exc:  # noqa: BLE001
        return {"total_mb": 0, "used_mb": 0, "free_mb": 0,
                "gpu_name": "error", "ok": False,
                "reason": f"{type(exc).__name__}: {exc}"}


def vram_free_mb() -> int:
    """MB of free VRAM. Returns 0 if unavailable (caller falls back to CPU)."""
    return vram_report().get("free_mb", 0)


def vram_used_mb() -> int:
    return vram_report().get("used_mb", 0)


def vram_total_mb() -> int:
    return vram_report().get("total_mb", 0)


def fits(needed_mb: int, *, headroom_mb: int = DEFAULT_HEADROOM_MB) -> bool:
    """True if `needed_mb` would fit in current free VRAM with headroom."""
    free = vram_free_mb()
    if free == 0:
        return True  # no GPU info → assume fits (CPU path handles OOM)
    return (needed_mb + headroom_mb) <= free


def assert_fits(needed_mb: int, label: str = "operation") -> None:
    """Raise RuntimeError (not OOM) if the request won't fit safely."""
    free = vram_free_mb()
    if free == 0:
        return  # no info → pass through
    if not fits(needed_mb):
        raise RuntimeError(
            f"VRAM guard blocked {label}: needs {needed_mb} MB but only "
            f"{free} MB free (headroom {DEFAULT_HEADROOM_MB} MB reserved). "
            f"Reduce model size or n_ctx.")


def safe_n_ctx(
    requested: int,
    *,
    kv_bytes_per_token: int = LLAMA_31_8B_KV_BYTES_PER_TOKEN,
    model_vram_mb: int = 4800,   # measured: 8B Q4 Hermes3 ≈ 4.8 GB on GPU
    headroom_mb: int = 400,      # buffer for CUDA context + minor overhead
) -> int:
    """Return the largest n_ctx that safely fits given current VRAM.

    Simple, honest calculation:
        post_model_free = free_vram - model_weights - headroom
        max_tokens = post_model_free / kv_bytes_per_token

    Falls back to `requested` if VRAM info is unavailable (non-GPU host).

    Example (RTX 2080 8 GB, clean boot, ~2 GB OS overhead):
      free=6139 MB, model=4800 MB, headroom=400 MB
      post_model_free = 939 MB → max_tokens ≈ 7512 → safe_n_ctx(8192) = 7512

    Example (RTX 2080, XTTS also in VRAM ≈ 1.5 GB extra used):
      free≈4600 MB, post_model_free = 4600-4800-400 = -600 → returns MIN_N_CTX
      Brain will fall back to CPU. Safe. No crash.
    """
    rpt = vram_report()
    if not rpt.get("ok"):
        return requested   # no GPU info → trust the caller

    free_mb = rpt["free_mb"]
    post_model_free = free_mb - model_vram_mb - headroom_mb
    if post_model_free <= 0:
        return MIN_N_CTX   # model itself won't fit safely → CPU fallback

    kv_budget_bytes = post_model_free * 1024 * 1024
    max_tokens = kv_budget_bytes // kv_bytes_per_token
    return max(MIN_N_CTX, min(requested, int(max_tokens)))


def report() -> Dict[str, Any]:
    """Cockpit surface. NEVER raises."""
    rpt = vram_report()
    return {
        "available": rpt.get("ok", False),
        "gpu_name": rpt.get("gpu_name", "unavailable"),
        "total_mb": rpt.get("total_mb", 0),
        "used_mb": rpt.get("used_mb", 0),
        "free_mb": rpt.get("free_mb", 0),
        "safe_n_ctx_current": safe_n_ctx(32768) if rpt.get("ok") else "n/a",
        "headroom_mb": DEFAULT_HEADROOM_MB,
        "safety_fraction": VRAM_SAFETY_FRACTION,
    }
