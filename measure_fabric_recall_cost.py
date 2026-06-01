"""Voice-silent proof of the mark_card_used write-coalescing fix.

The profiler showed the dominant warm-turn cost is:
  evidence_grounded_recall.recall -> research_memory_fabric.mark_card_used
  -> _save -> _atomic_write -> json.dump (whole store), ~16x/turn.

recall() triggers NO voice/TTS, so this isolates the fix with zero noise from
models or audio -- and Luna stays silent. We time recall() with the coalescing
flag ON (new, default) vs OFF (old per-call whole-store write).

Run:
  D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe D:\\SurgeApp\\measure_fabric_recall_cost.py
"""
import sys
import time

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_operator_controls as oc       # noqa: E402
from luna_modules import cognitive_evidence_grounded_recall as eg  # noqa: E402
from luna_modules import cognitive_research_memory_fabric as rmf  # noqa: E402

FLAG = "cognitive_research_fabric_debounce_usage_writes_enabled"
QUERY = "what should we focus on for the dashboard and latency work today"


def _time_recalls(n: int) -> list:
    out = []
    for _ in range(n):
        t0 = time.perf_counter()
        eg.recall(query=QUERY, top_k=3,
                  include_hybrid=False, include_layers=False)
        out.append((time.perf_counter() - t0) * 1000.0)
    return out


def main() -> None:
    # warm imports/singletons
    eg.recall(query=QUERY, top_k=3, include_hybrid=False, include_layers=False)
    rmf.flush_usage()

    # --- ON (new, default): coalesced writes ---
    oc.set_flag(FLAG, True)
    on = _time_recalls(6)
    rmf.flush_usage()

    # --- OFF (old): whole-store write per matched card ---
    oc.set_flag(FLAG, False)
    off = _time_recalls(6)

    # restore default ON
    oc.set_flag(FLAG, True)
    rmf.flush_usage()

    def med(xs):
        return sorted(xs)[len(xs) // 2]

    print("recall() ms per call (each internally marks matched cards used)")
    print("  OFF (old per-call whole-store write):")
    print("     " + ", ".join(f"{x:.1f}" for x in off))
    print(f"     median {med(off):.1f} ms")
    print("  ON  (coalesced, new default):")
    print("     " + ", ".join(f"{x:.1f}" for x in on))
    print(f"     median {med(on):.1f} ms")
    if med(on) > 0:
        print(f"\n  speedup (median): {med(off)/med(on):.1f}x  "
              f"({med(off)-med(on):.0f} ms saved per recall)")
    print("\n  recall is called multiple times per turn (evidence + verifier "
          "+ drive),\n  so per-turn savings = above x (calls/turn).")


if __name__ == "__main__":
    main()
