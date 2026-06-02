"""Operator-presence gate for Luna's VOICE.

Purpose (2026-06-02, per Serge): Luna must NOT talk out loud when the Luna
Command Center is closed. Background work (brain, self-improvement) keeps
running silently — only AUDIBLE speech is gated.

How it works (simple + cross-process via one timestamp file):
  * A real operator interaction (a conversation turn through the open
    Command Center) calls mark_present(). This writes the current epoch to
    memory/operator_present.flag.
  * Every server-side voice playback calls is_present() FIRST. If the last
    interaction was more than PRESENCE_MAX_AGE_S ago (Command Center closed
    or idle), playback is skipped — Luna stays silent.

This means:
  - You type/speak in the open terminal  -> turn marks present -> she speaks.
  - Command Center closed (no turns)      -> presence goes stale -> silence.
  - Autonomous/proactive/warmup speech    -> no recent turn     -> silence.

NEVER raises. Flag-gated (cognitive_voice_presence_gate_enabled, default ON).
Operator override: create memory/kill_switches/voice_presence_gate.disabled
to allow Luna to speak any time (old behaviour).
"""
from __future__ import annotations

import os
import time
from typing import Any, Dict

ROOT = r"D:\SurgeApp"
PRESENCE_FLAG = os.path.join(ROOT, "memory", "operator_present.flag")
KILL_SWITCH = os.path.join(ROOT, "memory", "kill_switches",
                           "voice_presence_gate.disabled")

# How long after the last interaction we still consider the operator "present".
# Generous so a normal pause mid-conversation never cuts her off, but short
# enough that a closed Command Center goes quiet within ~2 minutes.
PRESENCE_MAX_AGE_S = 150.0


def _gate_enabled() -> bool:
    """Gate is ON by default. Disabled by flag OR kill-switch file."""
    if os.path.isfile(KILL_SWITCH):
        return False
    try:
        from luna_modules import cognitive_feature_flags as ff
        v = ff.read_flags().get("cognitive_voice_presence_gate_enabled", True)
        return bool(v)
    except Exception:  # noqa: BLE001
        return True   # fail-safe: gate ON (quieter) if flags unreadable


def mark_present() -> None:
    """Record that the operator just interacted (open Command Center turn).
    NEVER raises."""
    try:
        os.makedirs(os.path.dirname(PRESENCE_FLAG), exist_ok=True)
        with open(PRESENCE_FLAG, "w", encoding="utf-8") as fh:
            fh.write(str(time.time()))
    except Exception:  # noqa: BLE001
        pass


def seconds_since_present() -> float:
    """Seconds since the last mark_present(). Large number if never/unknown."""
    try:
        with open(PRESENCE_FLAG, "r", encoding="utf-8") as fh:
            last = float((fh.read() or "0").strip())
        return max(0.0, time.time() - last)
    except Exception:  # noqa: BLE001
        return 1e9   # never marked -> treat as absent


def is_present(max_age_s: float = PRESENCE_MAX_AGE_S) -> bool:
    """True if the operator interacted recently (Command Center open + active).
    If the gate is disabled, always True (speech allowed). NEVER raises."""
    if not _gate_enabled():
        return True
    return seconds_since_present() <= float(max_age_s)


def may_speak_aloud() -> bool:
    """The one call voice-playback paths make before emitting sound.
    True = ok to play audio; False = stay silent (Command Center closed)."""
    return is_present()


def report() -> Dict[str, Any]:
    """Operator-readable status. NEVER raises."""
    return {
        "gate_enabled": _gate_enabled(),
        "seconds_since_present": round(seconds_since_present(), 1),
        "max_age_s": PRESENCE_MAX_AGE_S,
        "operator_present": is_present(),
        "kill_switch_present": os.path.isfile(KILL_SWITCH),
    }
