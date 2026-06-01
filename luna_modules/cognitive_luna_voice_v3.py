"""Luna Voice V3 — phone-call-speed conversational coordinator.

This is the top-level entry that combines:

  1. Personality shaping (``cognitive_personality_runtime``)
  2. Cached-phrase fast path     (``cognitive_voice_cached_phrase_adapter``)
  3. Persistent-SAPI warm path   (``cognitive_voice_persistent_sapi_adapter``)
  4. Per-call SAPI fallback path (already in ``cognitive_voice_runtime``)

The single public entry is :func:`speak_v3` which takes an intent + text
and delivers the best available voice path. It also detects and applies
operator mode-switch commands (``"bad luna"`` / ``"good luna"`` etc.)
without speaking the switch out loud.

Audit: every call writes one line to
``memory/cognitive/luna_voice_v3_audit.jsonl`` (FIFO-bounded).

NEVER raises.
"""
from __future__ import annotations

import importlib
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional

PROJECT_ROOT = r"D:\SurgeApp"
AUDIT_PATH = os.path.join(PROJECT_ROOT, "memory", "cognitive",
                           "luna_voice_v3_audit.jsonl")
MAX_AUDIT_LINES = 1000


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _try_import(modname: str):
    try:
        return importlib.import_module(modname)
    except Exception:  # noqa: BLE001
        return None


def _is_enabled() -> bool:
    ff = _try_import("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get("cognitive_luna_voice_v3_enabled", True))
    except Exception:  # noqa: BLE001
        return True


# Russian voice-clone reference (Serge's own RU recording). XTTS-v2 is
# multilingual, so the SAME model speaks Russian in his voice when given this
# reference + language "ru". The clone path routes Cyrillic text here.
_RU_SPEAKER_REF = r"D:\LunaVoiceWork\luna_clone_ru_reference\luna_ru_reference.wav"


def _clone_language_routing_enabled() -> bool:
    ff = _try_import("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_voice_clone_language_routing_enabled", True))
    except Exception:  # noqa: BLE001
        return True


def _looks_russian(text: str) -> bool:
    """True if `text` is predominantly Cyrillic. No regex — counts code points
    in the Cyrillic block (U+0400..U+04FF) against total letters."""
    if not text:
        return False
    cyr = alpha = 0
    for ch in text:
        if ch.isalpha():
            alpha += 1
            if "Ѐ" <= ch <= "ӿ":
                cyr += 1
    return alpha > 0 and (cyr / alpha) >= 0.3


def _append_audit(record: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
        with open(AUDIT_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        _truncate_audit_if_needed()
    except Exception:  # noqa: BLE001
        return


def _truncate_audit_if_needed() -> None:
    try:
        if not os.path.isfile(AUDIT_PATH):
            return
        with open(AUDIT_PATH, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
        if len(lines) <= MAX_AUDIT_LINES:
            return
        keep = lines[-MAX_AUDIT_LINES:]
        tmp = f"{AUDIT_PATH}.tmp.{int(time.time() * 1000)}"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.writelines(keep)
        os.replace(tmp, AUDIT_PATH)
    except Exception:  # noqa: BLE001
        return


# ---------------------------------------------------------------------------
# Intent / path policy
# ---------------------------------------------------------------------------

# For each intent, the order of adapter slots to try.
# V4 update: "premium" routes through v4_premium first (real-Luna sample
# clip playback when applicable, else profile-tuned SAPI), then falls
# back to V3 paths. Existing intents keep V3 behaviour for stability +
# phone-call-speed — premium voice is opt-in per call, not default.
INTENT_PATH_POLICY: Dict[str, List[str]] = {
    # Short, must be instant — prefer cache, fall to warm SAPI.
    "acknowledge": ["cached_phrase", "persistent_sapi", "sapi_powershell"],
    "greet":       ["cached_phrase", "persistent_sapi", "sapi_powershell"],
    "boot":        ["cached_phrase", "persistent_sapi", "sapi_powershell"],
    "boot_degraded": ["persistent_sapi", "sapi_powershell"],
    # Longer phrases — cached is usually not relevant; use warm SAPI.
    "answer":      ["persistent_sapi", "sapi_powershell"],
    "reassure":    ["persistent_sapi", "sapi_powershell", "cached_phrase"],
    "focus":       ["persistent_sapi", "sapi_powershell"],
    # Fallback: if even the warm path is sick, just try whatever sapi works
    "fallback":    ["sapi_powershell", "persistent_sapi"],
    # V4 premium intents — try real-Luna clip / profile-tuned SAPI first,
    # then standard fallback.
    "premium":         ["v4_premium", "persistent_sapi", "sapi_powershell"],
    "premium_greet":   ["v4_premium", "cached_phrase", "persistent_sapi",
                          "sapi_powershell"],
    "premium_audition": ["v4_premium"],  # operator audition — no fallback
    # V4.5 true-clone intent — XTTS-v2 first, then V4 (sample-clip) for
    # audition-style fallback, then V3 fast paths. The "clone" intent is
    # opt-in per call because synth latency is ~5-15 s on CPU.
    "clone":           ["xtts_clone", "v4_premium", "persistent_sapi",
                          "sapi_powershell"],
}
DEFAULT_INTENT_PATH = ["persistent_sapi", "sapi_powershell", "cached_phrase"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def warm_up() -> Dict[str, Any]:
    """Pre-spawn the persistent SAPI child + ensure cache is warm.

    Called once at boot (e.g., from presence_runtime.acknowledge_boot or
    operator on demand). NEVER raises.

    Returns timing of each warm step.
    """
    if not _is_enabled():
        return {"ok": False, "reason": "luna_voice_v3_disabled"}
    result: Dict[str, Any] = {"steps": []}
    t_all = time.time()

    # 1. Warm cached-phrase adapter manifest
    cp_mod = _try_import("luna_modules.cognitive_voice_cached_phrase_adapter")
    if cp_mod is not None:
        try:
            cp = cp_mod.get_singleton()
            t0 = time.time()
            r = cp.ensure_cache_warm()
            elapsed_ms = int((time.time() - t0) * 1000)
            result["steps"].append({"step": "cached_phrase_warm",
                                     "ok": bool(r.get("ok")),
                                     "rendered": r.get("rendered"),
                                     "skipped": r.get("skipped"),
                                     "failed": r.get("failed"),
                                     "elapsed_ms": elapsed_ms})
        except Exception as exc:  # noqa: BLE001
            result["steps"].append({"step": "cached_phrase_warm",
                                     "ok": False,
                                     "error": f"{type(exc).__name__}: {exc}"})
    else:
        result["steps"].append({"step": "cached_phrase_warm",
                                 "ok": False, "error": "module_missing"})

    # 2. Warm persistent-SAPI child
    psapi_mod = _try_import("luna_modules.cognitive_voice_persistent_sapi_adapter")
    if psapi_mod is not None:
        try:
            psapi = psapi_mod.get_singleton()
            t0 = time.time()
            r = psapi.warm_up()
            elapsed_ms = int((time.time() - t0) * 1000)
            result["steps"].append({"step": "persistent_sapi_warm",
                                     "ok": bool(r.get("ok")),
                                     "elapsed_ms": elapsed_ms,
                                     "child_alive": r.get("child_alive")})
        except Exception as exc:  # noqa: BLE001
            result["steps"].append({"step": "persistent_sapi_warm",
                                     "ok": False,
                                     "error": f"{type(exc).__name__}: {exc}"})
    else:
        result["steps"].append({"step": "persistent_sapi_warm",
                                 "ok": False, "error": "module_missing"})

    result["ok"] = all(s.get("ok") for s in result["steps"])
    result["elapsed_ms_total"] = int((time.time() - t_all) * 1000)
    _append_audit({"event": "warm_up", "ts": _now_iso(), **result})
    return result


def speak_v3(text: str, *,
              intent: str = "answer",
              caller: Optional[str] = None,
              mode: Optional[str] = None,
              allow_mode_switch_from_text: bool = True,
              ) -> Dict[str, Any]:
    """The phone-call-speed conversational entry. NEVER raises.

    Behaviour:
      1. If `allow_mode_switch_from_text` AND `text` starts with a
         recognised switch command, apply the mode switch silently and
         return (no audible output).
      2. Shape the text via personality runtime (intent-aware).
      3. Run the adapter chain per :data:`INTENT_PATH_POLICY` until one
         returns ok=True or all fail.
      4. Record timing + selection trace in the audit log.
    """
    utt_id = f"v3-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"
    captured = _now_iso()

    if not _is_enabled():
        rec = {"ok": False, "reason": "luna_voice_v3_disabled",
               "utt_id": utt_id, "captured_at_utc": captured}
        _append_audit({"event": "speak_v3", **rec, "caller": caller,
                        "intent": intent, "text_chars": len(text or "")})
        return rec

    # 1. Mode-switch detection (operator-only via input text)
    switched_to: Optional[str] = None
    if allow_mode_switch_from_text and text:
        pers = _try_import("luna_modules.cognitive_personality_runtime")
        if pers is not None:
            try:
                target = pers.detect_mode_switch_in(text)
                if target is not None:
                    r = pers.maybe_apply_switch_from_input(
                        text, caller=caller or "luna_voice_v3")
                    switched_to = r.get("current_mode")
                    rec = {"ok": True, "switched": True,
                           "switched_to": switched_to,
                           "utt_id": utt_id, "captured_at_utc": captured,
                           "audible": False,
                           "reason": "mode_switch_applied_silently"}
                    _append_audit({"event": "speak_v3", **rec,
                                    "caller": caller, "intent": intent,
                                    "text_chars": len(text)})
                    return rec
            except Exception:  # noqa: BLE001
                pass

    # 2. Personality shaping
    pers = _try_import("luna_modules.cognitive_personality_runtime")
    shaped_text = text or ""
    shaping_meta: Optional[Dict[str, Any]] = None
    if pers is not None:
        try:
            r = pers.shape_for_speech(text or "", intent=intent, mode=mode,
                                        caller=caller or "luna_voice_v3")
            shaped_text = r.get("shaped_text", text or "")
            shaping_meta = r
        except Exception:  # noqa: BLE001
            pass

    # 3. Voice profile (rate/volume/voice name suggestion)
    voice_profile: Dict[str, Any] = {}
    if pers is not None:
        try:
            voice_profile = pers.persona_voice_profile()
        except Exception:  # noqa: BLE001
            voice_profile = {}

    # 4. Build voice_mode hint for the adapter
    voice_mode_hint = voice_profile.get("voice_mode_hint")
    if intent == "acknowledge":
        voice_mode_hint = "ack"
    elif intent == "fallback":
        voice_mode_hint = "warm"
    elif intent == "premium_greet":
        # v4_premium adapter reads this as "play a short sample clip"
        voice_mode_hint = "premium_greet"
    elif intent == "premium_audition":
        voice_mode_hint = "premium_audition"
    elif intent == "premium":
        # premium with arbitrary text — let v4_premium pick its
        # profile-tuned SAPI path. Pass no special voice_mode so the
        # adapter routes to its synthesis branch.
        voice_mode_hint = "main"
    elif intent == "clone":
        # XTTS-v2 defaults to the prepared English Luna sample as the speaker
        # reference. For predominantly-Russian text, route to Serge's RUSSIAN
        # reference + language "ru" so she speaks Russian in her OWN cloned
        # voice (same multilingual model). Flag-gated; English stays default.
        voice_mode_hint = None
        if (_clone_language_routing_enabled()
                and _looks_russian(shaped_text)
                and os.path.isfile(_RU_SPEAKER_REF)):
            voice_mode_hint = f"speaker_ref:{_RU_SPEAKER_REF};language:ru"

    # 5. Adapter chain by intent
    vr_mod = _try_import("luna_modules.cognitive_voice_runtime")
    if vr_mod is None:
        rec = {"ok": False, "reason": "voice_runtime_missing",
               "utt_id": utt_id, "captured_at_utc": captured}
        _append_audit({"event": "speak_v3", **rec, "caller": caller,
                        "intent": intent})
        return rec

    rt = vr_mod.get_runtime()
    adapter_map = {c.name: c for c in rt.candidates()}
    policy = INTENT_PATH_POLICY.get(intent, DEFAULT_INTENT_PATH)
    attempted: List[Dict[str, Any]] = []
    final_outcome: Optional[Dict[str, Any]] = None

    for adapter_name in policy:
        cand = adapter_map.get(adapter_name)
        if cand is None or not cand.available:
            attempted.append({"adapter": adapter_name,
                               "reason": "unavailable"})
            continue
        try:
            adapter = rt._adapters.get(adapter_name) if hasattr(rt, "_adapters") else None
        except Exception:  # noqa: BLE001
            adapter = None
        if adapter is None:
            attempted.append({"adapter": adapter_name,
                               "reason": "adapter_instance_missing"})
            continue
        t0 = time.time()
        try:
            from pathlib import Path
            ok, voice_identity, err = adapter.synthesize(
                shaped_text,
                Path(os.path.join(PROJECT_ROOT, "memory", "voice_cache",
                                    f"{utt_id}.wav")),
                voice_mode=voice_mode_hint,
            )
            elapsed_ms = int((time.time() - t0) * 1000)
        except Exception as exc:  # noqa: BLE001
            attempted.append({"adapter": adapter_name,
                               "ok": False,
                               "elapsed_ms": int((time.time() - t0) * 1000),
                               "error": f"{type(exc).__name__}: {exc}"})
            continue
        attempted.append({"adapter": adapter_name,
                           "ok": bool(ok),
                           "elapsed_ms": elapsed_ms,
                           "voice_identity": voice_identity,
                           "error": err})
        if ok:
            final_outcome = {
                "ok": True,
                "audible": True,
                "backend": adapter_name,
                "voice_identity": voice_identity,
                "elapsed_ms": elapsed_ms,
                "fallback_used": (attempted[0]["adapter"] != adapter_name),
            }
            break

    if final_outcome is None:
        final_outcome = {"ok": False, "audible": False,
                          "reason": "no_adapter_succeeded"}

    rec = {
        "utt_id": utt_id,
        "captured_at_utc": captured,
        "intent": intent,
        "mode": (shaping_meta or {}).get("mode") if shaping_meta else (mode or "good_luna"),
        "voice_mode_hint": voice_mode_hint,
        "raw_text_chars": len(text or ""),
        "shaped_text_chars": len(shaped_text or ""),
        "shaping_applied": (shaping_meta or {}).get("applied") if shaping_meta else None,
        "pet_name": (shaping_meta or {}).get("pet_name") if shaping_meta else None,
        "attempted": attempted,
        "caller": caller,
        **final_outcome,
    }
    _append_audit({"event": "speak_v3", **rec})
    return rec


def play_identity_demo(tag: str = "identity_demo_1") -> Dict[str, Any]:
    """Operator surface: play one of the six provided sample WAVs as a
    voice-identity demonstration. NEVER raises.
    """
    if not _is_enabled():
        return {"ok": False, "reason": "luna_voice_v3_disabled"}
    cp_mod = _try_import("luna_modules.cognitive_voice_cached_phrase_adapter")
    if cp_mod is None:
        return {"ok": False, "reason": "cached_phrase_adapter_missing"}
    cp = cp_mod.get_singleton()
    t0 = time.time()
    r = cp.play_identity_demo(tag)
    r["elapsed_ms"] = int((time.time() - t0) * 1000)
    _append_audit({"event": "play_identity_demo", "ts": _now_iso(),
                    "tag": tag, **r})
    return r


def report() -> Dict[str, Any]:
    """Cockpit/dashboard surface — NEVER raises."""
    out: Dict[str, Any] = {
        "available": True,
        "enabled": _is_enabled(),
        "captured_at_utc": _now_iso(),
        "audit_path": AUDIT_PATH,
        "intent_path_policy": INTENT_PATH_POLICY,
    }
    pers = _try_import("luna_modules.cognitive_personality_runtime")
    out["personality"] = pers.report() if pers is not None else {"available": False}
    cp_mod = _try_import("luna_modules.cognitive_voice_cached_phrase_adapter")
    if cp_mod is not None:
        cp = cp_mod.get_singleton()
        out["cached_phrase"] = {
            "is_available": cp.is_available(),
            "details": cp.details(),
            "cached_phrases": cp.list_cached(),
            "identity_demos": cp.list_identity_demos(),
        }
    else:
        out["cached_phrase"] = {"available": False}
    psapi_mod = _try_import("luna_modules.cognitive_voice_persistent_sapi_adapter")
    if psapi_mod is not None:
        psapi = psapi_mod.get_singleton()
        out["persistent_sapi"] = {
            "is_available": psapi.is_available(),
            "details": psapi.details(),
        }
    else:
        out["persistent_sapi"] = {"available": False}
    return out


def shutdown_warm_children() -> Dict[str, Any]:
    """Rollback / explicit teardown — kill the persistent SAPI child if
    any. NEVER raises."""
    out: Dict[str, Any] = {"steps": []}
    psapi_mod = _try_import("luna_modules.cognitive_voice_persistent_sapi_adapter")
    if psapi_mod is not None:
        try:
            psapi = psapi_mod.get_singleton()
            r = psapi.shutdown()
            out["steps"].append({"step": "persistent_sapi_shutdown",
                                  **r})
        except Exception as exc:  # noqa: BLE001
            out["steps"].append({"step": "persistent_sapi_shutdown",
                                  "error": f"{type(exc).__name__}: {exc}"})
    out["ok"] = True
    return out


__all__ = [
    "AUDIT_PATH",
    "INTENT_PATH_POLICY",
    "warm_up",
    "speak_v3",
    "play_identity_demo",
    "report",
    "shutdown_warm_children",
]
