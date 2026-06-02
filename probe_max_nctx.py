"""Find the largest n_ctx that fits on GPU for the 8B main brain.

Tries candidate values in ascending order. At each step: load the model with
full GPU offload, generate a short response (text only, NO voice), check that
backend=GPU. Reports the largest that worked, then sets cognitive_main_gpu_n_ctx
to that value.

Run:
  D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe D:\\SurgeApp\\probe_max_nctx.py
"""
import sys
import time

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_sovereign_main_runtime as smr   # noqa: E402
from luna_modules import cognitive_operator_controls as oc          # noqa: E402

# Staircase: 4096 (baseline) → 8192 → 16384 → 24576 → 32768
CANDIDATES = [4096, 8192, 16384, 24576, 32768]

CLF = {"category": "casual", "text_chars": 20}
PROMPT = "Say one word."


def try_ctx(n: int) -> dict:
    """Reset the singleton, set n_ctx, try a warm gen. Return result dict."""
    smr.reset_singleton()
    # Temporarily patch the flag so _gpu_n_ctx() returns our value.
    oc.set_flag("cognitive_main_gpu_n_ctx", n)
    t0 = time.perf_counter()
    try:
        r = smr.generate_main(
            incoming_text=PROMPT,
            classification=CLF,
            recent_turns=[],
            mode="professional",
            timeout_s=180.0,
        )
        dt = time.perf_counter() - t0
        ok = bool(r.get("ok")) and bool(r.get("text", "").strip())
        gpu = "gpu" in str(r.get("backend", "")).lower() or \
              "gpu" in str(smr.get_singleton().details().get("backend", "")).lower()
        return {
            "n_ctx": n,
            "ok": ok,
            "gpu": gpu,
            "backend": r.get("backend"),
            "load_s": round(smr.get_singleton().details().get(
                "load_elapsed_s", dt), 1),
            "gen_ms": smr.get_singleton().details().get("last_call_elapsed_ms"),
            "reply": (r.get("text") or "")[:40],
            "error": r.get("error"),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "n_ctx": n, "ok": False, "gpu": False,
            "error": f"{type(exc).__name__}: {exc}",
            "load_s": round(time.perf_counter() - t0, 1),
        }


def main() -> None:
    best = CANDIDATES[0]  # 4096 already proven
    print(f"Probing n_ctx candidates: {CANDIDATES}")
    print(f"GPU: RTX 2080 8 GB  |  Model: Hermes-3 8B (full offload)\n")

    for n in CANDIDATES:
        print(f"  trying n_ctx={n:6d} ...", end=" ", flush=True)
        res = try_ctx(n)
        status = "✓ GPU" if (res["ok"] and res["gpu"]) else \
                 ("✓ CPU" if res["ok"] else "✗ FAIL")
        print(f"{status}  load={res.get('load_s')}s  gen={res.get('gen_ms')}ms  "
              f"{repr(res.get('reply',''))}")
        if res["error"] and not res["ok"]:
            print(f"    error: {res['error'][:100]}")
        if res["ok"] and res["gpu"]:
            best = n
        elif res["ok"]:
            # Loaded but fell to CPU (VRAM exceeded) — don't try higher
            print(f"  ↳ fell to CPU at n_ctx={n} — stopping here")
            break
        else:
            print(f"  ↳ failed at n_ctx={n} — stopping here")
            break

    # Set the flag to the best confirmed GPU value
    oc.set_flag("cognitive_main_gpu_n_ctx", best)
    print(f"\n{'='*55}")
    print(f"  MAX GPU n_ctx = {best:,} tokens")
    print(f"  KV cache at this size ≈ {best * 128 // 1024} MB")
    print(f"  Flag cognitive_main_gpu_n_ctx set to {best}")
    print(f"  (Hermes-3 8B trained on 131,072 — using "
          f"{best/131072*100:.0f}% of its full range)")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
