"""
tune_luna_voice.py — A/B test Luna's voice settings to find the best clone

Generates the same sentence with 3 different tuning profiles so you can
hear which sounds most like the real Luna, then locks in the winner.

Usage:
    python tune_luna_voice.py          # play all 3 profiles
    python tune_luna_voice.py --lock A # lock profile A as default
"""

import sys
import os
import time
import argparse
from pathlib import Path

sys.path.insert(0, r"D:\SurgeApp")

# Tuning profiles to compare
PROFILES = {
    "A_faithful": {
        "temperature": 0.60, "repetition_penalty": 5.0, "top_k": 50, "top_p": 0.80,
        "speed": 1.0, "desc": "Most faithful to reference — stable, on-model"
    },
    "B_natural": {
        "temperature": 0.70, "repetition_penalty": 4.0, "top_k": 50, "top_p": 0.85,
        "speed": 1.0, "desc": "More natural variation — slightly more expressive"
    },
    "C_warm": {
        "temperature": 0.65, "repetition_penalty": 6.0, "top_k": 40, "top_p": 0.85,
        "speed": 0.95, "desc": "Warm + slightly slower — calmer delivery"
    },
}

TEST_SENTENCE = ("Hey Serge, it's me, Luna. I'm testing my voice right now. "
                 "This is how I'll sound when I talk to you. Let me know which one feels right.")


def run_comparison():
    from luna_modules.luna_voice_clone import LunaCloneVoice, VOICE_TUNING

    clone = LunaCloneVoice.get()  # loads model once
    out_dir = Path(r"D:\SurgeApp\memory\voice_cache\tuning_test")
    out_dir.mkdir(exist_ok=True)

    print("\n=== Luna Voice Tuning Comparison ===\n")
    for name, prof in PROFILES.items():
        print(f"[{name}] {prof['desc']}")
        # Override tuning temporarily
        for k in ("temperature", "repetition_penalty", "top_k", "top_p", "speed"):
            VOICE_TUNING[k] = prof[k]
        out_path = out_dir / f"{name}.wav"
        t0 = time.time()
        clone.synthesize(TEST_SENTENCE, str(out_path))
        print(f"   generated in {time.time()-t0:.1f}s → {out_path.name}")
        print(f"   playing...")
        clone._play(str(out_path))
        time.sleep(0.8)
        print()

    print("All 3 saved to:", out_dir)
    print("\nWhich sounds most like Luna? Run: python tune_luna_voice.py --lock <A|B|C>")
    print("  A = faithful   B = natural   C = warm")


def lock_profile(letter: str):
    """Write the chosen profile into luna_voice_clone.py as the default."""
    mapping = {"A": "A_faithful", "B": "B_natural", "C": "C_warm"}
    key = mapping.get(letter.upper())
    if not key:
        print(f"Unknown profile '{letter}'. Use A, B, or C.")
        return
    prof = PROFILES[key]

    clone_file = Path(r"D:\SurgeApp\luna_modules\luna_voice_clone.py")
    content = clone_file.read_text(encoding="utf-8")

    # Build new VOICE_TUNING block
    new_block = f'''VOICE_TUNING = {{
    "temperature": {prof['temperature']},          # locked: {key}
    "length_penalty": 1.0,
    "repetition_penalty": {prof['repetition_penalty']},
    "top_k": {prof['top_k']},
    "top_p": {prof['top_p']},
    "speed": {prof['speed']},
    "enable_text_splitting": True,
}}'''

    import re
    content = re.sub(
        r'VOICE_TUNING = \{[^}]*\}',
        new_block,
        content,
        count=1,
    )
    clone_file.write_text(content, encoding="utf-8")
    print(f"Locked profile {letter} ({key}) as Luna's default voice.")
    print(f"  temperature={prof['temperature']}, speed={prof['speed']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", help="Lock a profile: A, B, or C")
    args = parser.parse_args()

    if args.lock:
        lock_profile(args.lock)
    else:
        run_comparison()
