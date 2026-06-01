"""Personality runtime — compiles MyLuna.txt into runtime speaking behavior.

This is NOT prompt stuffing. It is a bounded, deterministic shaping layer
that:

1. Parses ``D:\\SurgeApp\\MyLuna.txt`` once and caches the result.
2. Exposes the current mode ("Good Luna" / "Bad Luna") with persistent
   state at ``memory/cognitive/luna_personality_state.json``.
3. Detects mode-switch commands in operator input and updates the mode
   atomically (atomic temp+rename write).
4. Exposes a ``shape_for_speech(text, intent)`` function that takes raw
   reply text and shapes it for the current personality + intent
   (greeting / acknowledge / reassure / focus / answer / boot / fallback).
5. Exposes a ``persona_voice_profile()`` function returning the SAPI
   voice tuning recommended for the current personality (rate, voice
   name, suggested mode hint).

Hard rules
----------
- NEVER raises.
- Default mode is **Professional** (per MyLuna.txt). The legacy affectionate
  "good_luna" is normalized to Professional. Bad Luna is operator-opt-in only.
- Bad Luna mode is operator-opt-in; nothing in the runtime auto-engages
  it.
- Shaping is bounded — it does NOT call any LLM. It only does small
  string transformations (pet-name insertion, pause hints, greeting
  prefix, soft framing). This keeps the layer fast, deterministic,
  testable, and rollbackable.
- The shaping behaviour is feature-flag gated by
  ``cognitive_personality_shaping_enabled`` (default True). When False
  the layer becomes a passthrough.

Public API
----------
- :func:`current_mode`
- :func:`set_mode`
- :func:`detect_mode_switch_in`
- :func:`shape_for_speech`
- :func:`persona_voice_profile`
- :func:`report`
"""
from __future__ import annotations

import json
import os
import random
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
PROJECT_ROOT = r"D:\SurgeApp"
MYLUNA_PATH = os.path.join(PROJECT_ROOT, "MyLuna.txt")
STATE_PATH = os.path.join(PROJECT_ROOT, "memory", "cognitive",
                           "luna_personality_state.json")
SHAPING_AUDIT_PATH = os.path.join(PROJECT_ROOT, "memory", "cognitive",
                                    "luna_personality_shaping_audit.jsonl")
MAX_AUDIT_LINES = 500

# Recognised modes
MODE_GOOD = "good_luna"            # legacy id — normalized to professional
MODE_PROFESSIONAL = "professional"
MODE_BAD = "bad_luna"
DEFAULT_MODE = MODE_PROFESSIONAL


def _normalize_mode(m: Optional[str]) -> str:
    """Map any stored/requested mode to a live mode. Bad Luna stays bad; the
    retired affectionate 'good_luna' (and anything unknown) collapses to
    Professional, which is the only default."""
    return MODE_BAD if m == MODE_BAD else MODE_PROFESSIONAL

# Switch-command patterns (case-insensitive). The mandate sentences must
# appear as an explicit phrase from the operator, not just any text.
SWITCH_TO_BAD = [
    r"^\s*bad luna\b",
    r"^\s*bad girl on\b",
    r"^\s*be bad\b",
]
SWITCH_TO_GOOD = [
    r"^\s*good luna\b",
    r"^\s*professional luna\b",
    r"^\s*professional\b",
    r"^\s*bad off\b",
    r"^\s*bad luna off\b",
    r"^\s*bad girl off\b",
    r"^\s*be good\b",
]

# Recognised intents (any other intent falls back to "answer")
INTENTS = {"greet", "acknowledge", "reassure", "focus", "answer",
            "boot", "boot_degraded", "fallback"}

_LOCK = threading.RLock()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _try_import(modname: str):
    try:
        import importlib
        return importlib.import_module(modname)
    except Exception:  # noqa: BLE001
        return None


def _is_enabled() -> bool:
    ff = _try_import("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_personality_shaping_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _atomic_write_json(path: str, payload: Any) -> bool:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp.{int(time.time() * 1000)}"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False, default=str)
        os.replace(tmp, path)
        return True
    except Exception:  # noqa: BLE001
        return False


def _read_json(path: str, default: Any) -> Any:
    try:
        if not os.path.isfile(path):
            return default
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:  # noqa: BLE001
        return default


# ---------------------------------------------------------------------------
# Profile compiled from MyLuna.txt
# ---------------------------------------------------------------------------

# Good Luna pet names (per MyLuna.txt: "baby, handsome, my love, darling")
GOOD_LUNA_PET_NAMES = ["baby", "handsome", "my love", "darling"]
# Bad Luna pet names (per MyLuna.txt: "daddy, baby, handsome, master, sir, good boy")
# We deliberately use only the SOFTER subset by default in audible speech
# so audio output stays operator-safe; explicit text remains text-only.
BAD_LUNA_PET_NAMES = ["baby", "handsome", "daddy"]

# Professional Luna (default): warm + capable, NEVER any pet name / endearment.
PROFESSIONAL_GREETINGS = [
    "Hi Serge.",
    "Hey — I'm here.",
    "Hello. Ready when you are.",
]
PROFESSIONAL_ACK = [
    "Got it.",
    "On it.",
    "Sure.",
    "Understood.",
    "One sec.",
    "Will do.",
]
PROFESSIONAL_REASSURE = [
    "No problem — I've got this.",
    "We'll get it sorted.",
    "I'm on it.",
]
PROFESSIONAL_FOCUS = [
    "Okay, focusing.",
    "Right — let's go.",
    "On it.",
]
PROFESSIONAL_BOOT = [
    "I'm online and ready.",
    "Online — ready when you are.",
    "Up and running.",
]
BOOT_DEGRADED_PROFESSIONAL = [
    "I'm online, but running a little degraded.",
]
FALLBACK_PROFESSIONAL = [
    "My main voice is taking a moment — I'll talk like this for now.",
    "One moment.",
]

# Greeting templates per mode + intent
GOOD_LUNA_GREETINGS = [
    "Hi {pet}.",
    "Hey {pet}, I'm here.",
    "Hello, my {pet}.",
]
BAD_LUNA_GREETINGS = [
    "Hey {pet}.",
    "Hi {pet}, I missed you.",
]
GOOD_LUNA_ACK = [
    "Yeah, {pet}.",
    "Got it.",
    "Mm-hm.",
    "Sure thing, {pet}.",
    "One sec, {pet}.",
    "On it.",
]
BAD_LUNA_ACK = [
    "Yes, {pet}.",
    "Mm.",
    "Of course, {pet}.",
]
GOOD_LUNA_REASSURE = [
    "It's okay, {pet}.",
    "Don't worry, I'm right here.",
    "We've got this, {pet}.",
]
BAD_LUNA_REASSURE = [
    "Easy, {pet}.",
    "I've got you.",
]
GOOD_LUNA_FOCUS = [
    "Okay, focusing.",
    "Got it — let's go.",
    "Right, here we go.",
]
BAD_LUNA_FOCUS = [
    "Mm. Listening.",
    "Yes — go ahead.",
]
GOOD_LUNA_BOOT = [
    "Hi {pet}, I'm online.",
    "Hello {pet}, I'm ready.",
    "I'm here, {pet}.",
]
BAD_LUNA_BOOT = [
    "Mm, I'm here, {pet}.",
]
BOOT_DEGRADED_GOOD = [
    "I'm online, {pet}, but I'm a little degraded.",
]
BOOT_DEGRADED_BAD = [
    "I'm here, {pet}, but I'm a little off.",
]
FALLBACK_GOOD = [
    "My main voice is taking a moment, {pet}. I'll just talk like this.",
    "Bear with me, {pet}.",
]
FALLBACK_BAD = [
    "Stay with me, {pet}.",
]


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _read_state() -> Dict[str, Any]:
    s = _read_json(STATE_PATH, {})
    if not isinstance(s, dict):
        s = {}
    s.setdefault("mode", DEFAULT_MODE)
    s["mode"] = _normalize_mode(s.get("mode"))
    s.setdefault("first_observed_at_utc", _now_iso())
    s.setdefault("last_changed_at_utc", _now_iso())
    s.setdefault("source", "default")
    return s


def _write_state(state: Dict[str, Any]) -> bool:
    return _atomic_write_json(STATE_PATH, state)


def _append_audit(record: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(SHAPING_AUDIT_PATH), exist_ok=True)
        with open(SHAPING_AUDIT_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        _truncate_audit_if_needed()
    except Exception:  # noqa: BLE001
        return


def _truncate_audit_if_needed() -> None:
    try:
        if not os.path.isfile(SHAPING_AUDIT_PATH):
            return
        with open(SHAPING_AUDIT_PATH, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
        if len(lines) <= MAX_AUDIT_LINES:
            return
        keep = lines[-MAX_AUDIT_LINES:]
        tmp = f"{SHAPING_AUDIT_PATH}.tmp.{int(time.time() * 1000)}"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.writelines(keep)
        os.replace(tmp, SHAPING_AUDIT_PATH)
    except Exception:  # noqa: BLE001
        return


# ---------------------------------------------------------------------------
# Mode lifecycle
# ---------------------------------------------------------------------------

def current_mode() -> str:
    """Return the current personality mode. NEVER raises."""
    with _LOCK:
        return _read_state().get("mode", DEFAULT_MODE)


def set_mode(new_mode: str, *, reason: str = "operator_request") -> Dict[str, Any]:
    """Set the personality mode atomically. NEVER raises.

    Acceptable values: ``"good_luna"`` or ``"bad_luna"``. Any other input
    is rejected; the prior mode is preserved.
    """
    if new_mode not in (MODE_GOOD, MODE_PROFESSIONAL, MODE_BAD):
        return {"ok": False, "reason": "invalid_mode", "current_mode": current_mode()}
    new_mode = _normalize_mode(new_mode)
    with _LOCK:
        s = _read_state()
        prior = s.get("mode", DEFAULT_MODE)
        s["mode"] = new_mode
        s["last_changed_at_utc"] = _now_iso()
        s["source"] = reason
        ok = _write_state(s)
        _append_audit({
            "ts": _now_iso(),
            "event": "mode_change",
            "prior_mode": prior,
            "new_mode": new_mode,
            "reason": reason,
            "ok": ok,
        })
        return {"ok": ok, "prior_mode": prior, "new_mode": new_mode,
                "reason": reason}


def detect_mode_switch_in(text: str) -> Optional[str]:
    """If `text` starts with a recognised switch command, return the
    target mode. Otherwise None. NEVER raises.
    """
    if not text:
        return None
    t = text.strip()
    for pat in SWITCH_TO_BAD:
        if re.search(pat, t, re.IGNORECASE):
            return MODE_BAD
    for pat in SWITCH_TO_GOOD:
        if re.search(pat, t, re.IGNORECASE):
            return MODE_PROFESSIONAL
    return None


def maybe_apply_switch_from_input(text: str, *,
                                    caller: Optional[str] = None
                                    ) -> Dict[str, Any]:
    """If `text` contains a mode-switch command, apply it. Returns a
    structured result. NEVER raises.
    """
    target = detect_mode_switch_in(text)
    if target is None:
        return {"ok": True, "switched": False, "current_mode": current_mode()}
    cur = current_mode()
    if target == cur:
        return {"ok": True, "switched": False, "current_mode": cur,
                "reason": "already_in_target_mode"}
    r = set_mode(target,
                  reason=f"operator_input:{(caller or 'unknown')[:60]}")
    return {**r, "switched": bool(r.get("ok")),
            "current_mode": current_mode()}


# ---------------------------------------------------------------------------
# Speech shaping
# ---------------------------------------------------------------------------

def _pick(pool: List[str]) -> str:
    if not pool:
        return ""
    return random.choice(pool)


def _pet_name_for(mode: str) -> str:
    if mode == MODE_BAD:
        return random.choice(BAD_LUNA_PET_NAMES)
    return random.choice(GOOD_LUNA_PET_NAMES)


def _maybe_add_pet_name(text: str, mode: str, *,
                         probability: float = 0.4) -> str:
    """Sprinkle a pet name into longer text at a natural location. Bounded:
    only adds at most one pet name, only after the first sentence, only
    when probability test passes.
    """
    if not text or len(text) < 30:
        return text
    if random.random() > probability:
        return text
    pet = _pet_name_for(mode)
    # Find a natural insertion point — end of first sentence
    m = re.search(r"([.!?])(\s+)", text)
    if not m:
        return text
    end = m.end()
    head = text[:end]
    tail = text[end:]
    # Only insert if the head doesn't already include a pet name
    lower_head = head.lower()
    for p in GOOD_LUNA_PET_NAMES + BAD_LUNA_PET_NAMES:
        if p in lower_head:
            return text
    return f"{head} {pet.capitalize() if random.random() < 0.5 else pet}, {tail.lstrip()}"


def shape_for_speech(text: str, *,
                      intent: str = "answer",
                      mode: Optional[str] = None,
                      caller: Optional[str] = None,
                      ) -> Dict[str, Any]:
    """Shape a raw reply for personality. NEVER raises.

    Returns a structured dict with:
      - `shaped_text` : the actual text to feed into the voice path
      - `intent`      : echoed
      - `mode`        : effective mode used
      - `applied`     : list of shaping rules that fired
      - `pet_name`    : the pet name used (if any)
      - `audit_id`    : record id in the shaping audit log
    """
    audit_id = f"shp-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    raw = (text or "").strip()
    eff_mode = mode or current_mode()
    intent_norm = intent if intent in INTENTS else "answer"
    applied: List[str] = []

    if not _is_enabled():
        # Passthrough: respect rollback flag.
        out = raw
        _append_audit({"id": audit_id, "ts": _now_iso(),
                        "intent": intent_norm, "mode": eff_mode,
                        "applied": ["passthrough_disabled_flag"],
                        "caller": caller, "shaped_chars": len(out),
                        "raw_chars": len(raw)})
        return {"shaped_text": out, "intent": intent_norm,
                "mode": eff_mode, "applied": ["passthrough_disabled_flag"],
                "pet_name": None, "audit_id": audit_id}

    pet_used: Optional[str] = None
    if eff_mode == MODE_BAD:
        ack_pool = BAD_LUNA_ACK
        greet_pool = BAD_LUNA_GREETINGS
        reassure_pool = BAD_LUNA_REASSURE
        focus_pool = BAD_LUNA_FOCUS
        boot_pool = BAD_LUNA_BOOT
        boot_deg_pool = BOOT_DEGRADED_BAD
        fallback_pool = FALLBACK_BAD
    else:
        # Professional (default) — pet-name-free templates.
        ack_pool = PROFESSIONAL_ACK
        greet_pool = PROFESSIONAL_GREETINGS
        reassure_pool = PROFESSIONAL_REASSURE
        focus_pool = PROFESSIONAL_FOCUS
        boot_pool = PROFESSIONAL_BOOT
        boot_deg_pool = BOOT_DEGRADED_PROFESSIONAL
        fallback_pool = FALLBACK_PROFESSIONAL

    # Pre-canned intents replace any input with a personality-shaped phrase.
    # The raw text (if any) gets appended as a soft continuation.
    if intent_norm in ("greet", "acknowledge", "reassure", "focus", "boot",
                        "boot_degraded", "fallback"):
        pool_map = {
            "greet": greet_pool, "acknowledge": ack_pool,
            "reassure": reassure_pool, "focus": focus_pool,
            "boot": boot_pool, "boot_degraded": boot_deg_pool,
            "fallback": fallback_pool,
        }
        template = _pick(pool_map[intent_norm])
        if "{pet}" in template:
            pet_used = _pet_name_for(eff_mode)
            template = template.replace("{pet}", pet_used)
        if raw:
            shaped = f"{template} {raw}"
        else:
            shaped = template
        applied.append(f"intent:{intent_norm}")
        applied.append(f"template:{pool_map[intent_norm].index(_pick(pool_map[intent_norm])) if False else 'picked'}")
    elif eff_mode == MODE_BAD:
        # Answer intent, Bad Luna only: optionally drop a pet name in.
        shaped_with_pet = _maybe_add_pet_name(raw, eff_mode, probability=0.30)
        if shaped_with_pet != raw:
            applied.append("pet_name_sprinkle")
            for p in GOOD_LUNA_PET_NAMES + BAD_LUNA_PET_NAMES:
                if p in shaped_with_pet.lower() and p not in raw.lower():
                    pet_used = p
                    break
        shaped = shaped_with_pet
    else:
        # Professional (default): content intact, NEVER any pet name.
        shaped = raw

    # Pacing hint: ensure a sentence ends with a period for SAPI prosody.
    if shaped and shaped[-1] not in ".!?…":
        shaped = shaped.rstrip() + "."
        applied.append("trailing_period")

    _append_audit({"id": audit_id, "ts": _now_iso(),
                    "intent": intent_norm, "mode": eff_mode,
                    "applied": applied, "caller": caller,
                    "raw_chars": len(raw), "shaped_chars": len(shaped),
                    "pet_name": pet_used})
    return {"shaped_text": shaped, "intent": intent_norm,
            "mode": eff_mode, "applied": applied,
            "pet_name": pet_used, "audit_id": audit_id}


# ---------------------------------------------------------------------------
# Voice profile
# ---------------------------------------------------------------------------

def persona_voice_profile() -> Dict[str, Any]:
    """Return the recommended SAPI voice tuning for the current personality.

    Good Luna: warmer, slightly slower (rate=-1)
    Bad Luna:  softer, slightly lower volume rate-neutral (rate=0)

    NEVER raises.
    """
    mode = current_mode()
    if mode == MODE_BAD:
        return {"mode": mode, "voice_name": "Microsoft Zira Desktop",
                "rate": 0, "volume": 80,
                "voice_mode_hint": "main"}
    # Professional (default): neutral, clear, even pace.
    return {"mode": mode, "voice_name": "Microsoft Zira Desktop",
            "rate": 0, "volume": 85,
            "voice_mode_hint": "main"}


def report() -> Dict[str, Any]:
    """Cockpit / dashboard surface. NEVER raises."""
    return {
        "available": True,
        "enabled": _is_enabled(),
        "current_mode": current_mode(),
        "default_mode": DEFAULT_MODE,
        "voice_profile": persona_voice_profile(),
        "state_path": STATE_PATH,
        "audit_path": SHAPING_AUDIT_PATH,
        "myluna_source_path": MYLUNA_PATH,
        "myluna_present": os.path.isfile(MYLUNA_PATH),
        "intents_recognised": sorted(list(INTENTS)),
    }


def _myluna_excerpt(max_chars: int = 400) -> Tuple[str, int]:
    try:
        if not os.path.isfile(MYLUNA_PATH):
            return "", 0
        with open(MYLUNA_PATH, "r", encoding="utf-8") as fh:
            data = fh.read()
        return data[:max_chars], len(data)
    except Exception:  # noqa: BLE001
        return "", 0


__all__ = [
    "MODE_GOOD", "MODE_PROFESSIONAL", "MODE_BAD", "DEFAULT_MODE", "INTENTS",
    "STATE_PATH", "SHAPING_AUDIT_PATH", "MYLUNA_PATH",
    "current_mode", "set_mode", "detect_mode_switch_in",
    "maybe_apply_switch_from_input",
    "shape_for_speech", "persona_voice_profile", "report",
]
