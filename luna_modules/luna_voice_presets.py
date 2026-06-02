"""
luna_voice_presets.py — Named voice presets for Luna

Keeps every tuned profile available so Luna can switch character on demand.
Default is "one" (smooth/natural). Switch any time:

    from luna_modules.luna_voice_presets import apply_preset
    apply_preset("two")   # expressive
    apply_preset("one")   # back to default

Or from CLI:
    python -m luna_modules.luna_voice_presets list
    python -m luna_modules.luna_voice_presets set two
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger("luna.voice_presets")

PRESET_FILE = Path(r"D:\SurgeApp\memory\voice_cache\luna_voice_presets.json")
PRESET_FILE.parent.mkdir(parents=True, exist_ok=True)

# All tuned profiles — kept permanently so Luna can use any of them later.
PRESETS = {
    "one": {
        "label": "Smooth & Natural (default, de-robotized + Luna EQ)",
        "temperature": 0.85, "length_penalty": 1.0, "repetition_penalty": 2.3,
        "top_k": 50, "top_p": 0.88, "speed": 1.0, "enable_text_splitting": True,
    },
    "two": {
        "label": "Expressive & Lively",
        "temperature": 0.85, "length_penalty": 1.0, "repetition_penalty": 2.0,
        "top_k": 60, "top_p": 0.92, "speed": 1.0, "enable_text_splitting": True,
    },
    "three": {
        "label": "Warm & Relaxed",
        "temperature": 0.80, "length_penalty": 1.0, "repetition_penalty": 3.0,
        "top_k": 50, "top_p": 0.90, "speed": 0.97, "enable_text_splitting": True,
    },
}

DEFAULT_PRESET = "one"


def _load_active() -> str:
    """Return the currently active preset name (persisted)."""
    if PRESET_FILE.exists():
        try:
            return json.loads(PRESET_FILE.read_text()).get("active", DEFAULT_PRESET)
        except Exception:
            pass
    return DEFAULT_PRESET


def _save_active(name: str):
    PRESET_FILE.write_text(json.dumps({"active": name}, indent=2))


def get_preset(name: str = None) -> dict:
    """Get a preset's tuning dict (defaults to the active one)."""
    name = (name or _load_active()).lower()
    return PRESETS.get(name, PRESETS[DEFAULT_PRESET])


def apply_preset(name: str):
    """Apply a preset to the live VOICE_TUNING used by luna_voice_clone."""
    name = name.lower()
    if name not in PRESETS:
        logger.warning(f"Unknown preset '{name}', using default")
        name = DEFAULT_PRESET
    preset = PRESETS[name]
    try:
        from luna_modules.luna_voice_clone import VOICE_TUNING
        for k in ("temperature", "length_penalty", "repetition_penalty",
                  "top_k", "top_p", "speed", "enable_text_splitting"):
            VOICE_TUNING[k] = preset[k]
        _save_active(name)
        logger.info(f"Applied voice preset: {name} ({preset['label']})")
    except Exception as e:
        logger.error(f"Could not apply preset: {e}")


def list_presets():
    active = _load_active()
    print(f"\nLuna voice presets (active: {active}):\n")
    for name, p in PRESETS.items():
        mark = " <-- ACTIVE" if name == active else ""
        print(f"  {name:6s} | {p['label']:28s} | temp={p['temperature']} rep={p['repetition_penalty']}{mark}")
    print()


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2 and sys.argv[1] == "set" and len(sys.argv) >= 3:
        apply_preset(sys.argv[2])
        print(f"Active preset set to: {sys.argv[2]}")
    else:
        list_presets()
