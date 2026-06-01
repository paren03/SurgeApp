"""Profile the warm conversation-turn PIPELINE overhead.

handle_turn + both brain calls run on the MAIN thread (only voice playback is
pooled), so cProfile of the main thread captures the full synchronous cost --
including the bounded voice-future reaps (they block the main thread up to 2s).

Two outputs:
  1. Honest wall-clock for 3 warm turns (NO profiler overhead) -> beats noise.
  2. cProfile ranking (cumulative + tottime) over 3 warm turns -> which
     functions/sections actually dominate. Profiler inflates absolute numbers
     but preserves relative ranking.

Run:
  D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe D:\\SurgeApp\\profile_handle_turn_sections.py
"""
import cProfile
import io
import pstats
import sys
import time

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_operator_controls as oc  # noqa: E402

PROMPTS = [
    "In one short sentence, how are you today?",
    "What should we focus on first this morning?",
    "Give me a quick status in one line.",
]


def main() -> None:
    # Warm up: load both models + warm every subsystem import/singleton.
    oc.luna_conversation_turn("Hello there")
    oc.luna_conversation_turn("Warm up once more please")

    # --- Honest wall-clock, no profiler overhead, 3 samples ---
    walls = []
    for p in PROMPTS:
        t0 = time.perf_counter()
        oc.luna_conversation_turn(p)
        walls.append(time.perf_counter() - t0)
    walls_sorted = sorted(walls)
    print("=== WALL-CLOCK (no profiler), 3 warm turns ===")
    print("  per turn : " + ", ".join(f"{w:.2f}s" for w in walls))
    print(f"  median   : {walls_sorted[1]:.2f}s")
    print(f"  min/max  : {walls_sorted[0]:.2f}s / {walls_sorted[-1]:.2f}s\n")

    # --- Profiled ranking, 3 warm turns aggregated ---
    pr = cProfile.Profile()
    pr.enable()
    for p in PROMPTS:
        oc.luna_conversation_turn(p)
    pr.disable()

    s = io.StringIO()
    pstats.Stats(pr, stream=s).strip_dirs().sort_stats(
        "cumulative").print_stats(40)
    print("=== TOP 40 BY CUMULATIVE TIME (call-tree cost) ===")
    print(s.getvalue())

    s2 = io.StringIO()
    pstats.Stats(pr, stream=s2).strip_dirs().sort_stats(
        "tottime").print_stats(40)
    print("=== TOP 40 BY TOTTIME (self/CPU cost) ===")
    print(s2.getvalue())

    # --- luna_modules-only cumulative view (maps functions -> sections) ---
    s3 = io.StringIO()
    pstats.Stats(pr, stream=s3).strip_dirs().sort_stats(
        "cumulative").print_stats("cognitive_|luna_", 30)
    print("=== TOP cognitive_/luna_ FRAMES BY CUMULATIVE ===")
    print(s3.getvalue())


if __name__ == "__main__":
    main()
