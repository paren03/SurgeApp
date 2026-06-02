"""TRUE max-n_ctx probe: uses llama_cpp.Llama directly at each size.
Bypasses the broken set_flag(int) path that coerces to bool.
Verifies via llm.n_ctx() attribute — NOT the CUDA warning (which is always
emitted when n_ctx < n_ctx_train, regardless of actual n_ctx used).

Run (text only, NO voice, ~5-10 min):
  D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe D:\\SurgeApp\\probe_max_nctx_direct.py
"""
import gc
import json
import os
import sys
import time

sys.path.insert(0, r"D:\SurgeApp")

MODEL_PATH = r"D:\SurgeApp\local_models\hermes3-8b-llama3.1.gguf"
CANDIDATES = [8192, 16384, 24576, 32768]
FLAG_FILE   = r"D:\SurgeApp\memory\cognitive\feature_flags.json"

def set_flag_int(key: str, value: int) -> None:
    """Write integer directly to feature_flags.json (bypasses set_flag bool bug)."""
    try:
        with open(FLAG_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        data[key] = value
        with open(FLAG_FILE, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
    except Exception as exc:
        print(f"  flag write failed: {exc}")


def probe_one(n: int) -> dict:
    try:
        from llama_cpp import Llama  # noqa: PLC0415
    except ImportError:
        return {"ok": False, "error": "llama_cpp not importable"}
    t0 = time.perf_counter()
    try:
        llm = Llama(
            model_path=MODEL_PATH,
            n_gpu_layers=-1,   # full GPU offload
            n_ctx=n,
            n_threads=4,
            verbose=False,     # suppress CUDA warnings
        )
        actual_ctx = llm.n_ctx()
        # Quick gen to confirm it works
        out = llm.create_completion("Say the word: hello", max_tokens=6,
                                     temperature=0.0)
        reply = (out["choices"][0]["text"] if out and out.get("choices")
                 else "")
        load_s = round(time.perf_counter() - t0, 1)
        del llm
        gc.collect()
        return {
            "ok": True, "actual_ctx": actual_ctx, "load_s": load_s,
            "reply": reply.strip()[:30], "kv_mb": n * 128 // 1024,
        }
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:120]}",
                "load_s": round(time.perf_counter() - t0, 1)}


def main() -> None:
    if not os.path.isfile(MODEL_PATH):
        print(f"ERROR: model not found at {MODEL_PATH}")
        sys.exit(1)

    print(f"Probing REAL n_ctx: {CANDIDATES}")
    print(f"Model: {os.path.basename(MODEL_PATH)}")
    print(f"GPU: RTX 2080 8 GB | full offload (n_gpu_layers=-1)\n")

    best = 4096   # already proven
    for n in CANDIDATES:
        kv = n * 128 // 1024
        print(f"  n_ctx={n:6,d}  (KV≈{kv:4d}MB) ...", end=" ", flush=True)
        res = probe_one(n)
        if res["ok"]:
            print(f"✓ GPU  actual_ctx={res['actual_ctx']:,}  "
                  f"load={res['load_s']}s  reply={res['reply']!r}")
            if res["actual_ctx"] >= n:
                best = n
            else:
                print(f"  ↳ actual_ctx={res['actual_ctx']:,} < requested {n} — GPU OOM fallback")
                break
        else:
            print(f"✗ FAIL  {res.get('error','')}")
            break
        time.sleep(1)  # let CUDA release before next

    # Write the winner as an integer directly to the JSON
    set_flag_int("cognitive_main_gpu_n_ctx", best)

    # Verify
    try:
        with open(FLAG_FILE, "r", encoding="utf-8") as fh:
            stored = json.load(fh).get("cognitive_main_gpu_n_ctx")
        verified = stored == best
    except Exception:
        stored, verified = "?", False

    print(f"\n{'='*58}")
    print(f"  MAX CONFIRMED GPU n_ctx : {best:,} tokens")
    print(f"  KV cache at this size   : {best * 128 // 1024:,} MB")
    print(f"  Flag written (int, not bool): {stored}  ✓" if verified
          else f"  FLAG WRITE FAILED: stored={stored}")
    print(f"  Hermes-3 trained on 131,072 — using "
          f"{best/131072*100:.0f}% of its full range")
    print(f"{'='*58}")


if __name__ == "__main__":
    main()
