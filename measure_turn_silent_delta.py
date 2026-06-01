"""Silent end-to-end proof of the fabric write-coalescing fix.

Runs REAL warm conversation turns but with voice/TTS monkeypatched to a no-op,
so Luna stays SILENT. Voice is muted on BOTH sides, so the OFF-vs-ON delta
isolates the research-fabric coalescing fix's contribution to the full turn
(not just recall in isolation).

Run:
  D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe D:\\SurgeApp\\measure_turn_silent_delta.py
"""
import sys
import time

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_operator_controls as oc       # noqa: E402
from luna_modules import cognitive_conversation_runtime as crt   # noqa: E402

FLAG = "cognitive_research_fabric_debounce_usage_writes_enabled"
PROMPTS = [
    "In one short sentence, how are you today?",
    "What should we focus on first this morning?",
    "Give me a quick status in one line.",
]


def _mute_voice() -> None:
    """Replace voice playback with instant no-ops -> Luna stays silent and the
    bounded voice-reap waits are removed equally from both OFF and ON runs."""
    def _silent_ack(text):
        return {"ok": True, "text": text, "audible": False,
                "voice_backend": "muted_for_test", "elapsed_ms": 0}

    def _silent_main(text, *, want_premium=False):
        return {"ok": True, "text": text, "audible": False,
                "voice_backend": "muted_for_test", "elapsed_ms": 0}

    crt._speak_ack = _silent_ack
    crt._speak_main_reply = _silent_main


def _median(xs):
    return sorted(xs)[len(xs) // 2]


def _run(n_label: str):
    walls = []
    for p in PROMPTS:
        t0 = time.perf_counter()
        oc.luna_conversation_turn(p)
        walls.append(time.perf_counter() - t0)
    print(f"  {n_label}: " + ", ".join(f"{w:.2f}s" for w in walls)
          + f"   median {_median(walls):.2f}s")
    return _median(walls)


def main() -> None:
    _mute_voice()
    # warm up (load both models, warm subsystems) — silent
    oc.luna_conversation_turn("Hello there")
    oc.luna_conversation_turn("Warm once more")

    print("Silent warm turns (voice muted both sides). Wall-clock per turn:")
    oc.set_flag(FLAG, False)
    off = _run("OFF (old per-call fabric write)")
    oc.set_flag(FLAG, True)
    on = _run("ON  (coalesced, new default)  ")

    print(f"\n  median OFF {off:.2f}s -> ON {on:.2f}s   "
          f"= {off - on:.2f}s removed from the turn by the fabric fix")
    print("  (absolute totals exclude voice; the delta is the fix's "
          "contribution.)")


if __name__ == "__main__":
    main()
