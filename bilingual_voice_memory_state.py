"""Phase 26 - Bilingual Voice Memory State.

Manages in-memory voice continuity state. Plain dict. No disk writes. No
background tracking.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_memory_schema as vms
import bilingual_voice_preference_extractor as vpe
import bilingual_language_mode_detector as lmd


def new_voice_session(session_id: Optional[str] = None) -> dict[str, Any]:
    return vms.create_empty_voice_memory_state(session_id)


def _push_bounded(lst: list[Any], item: Any) -> list[Any]:
    out = list(lst or [])
    out.append(item)
    return out[-vms.RECENT_LIST_HARD_CAP:]


def apply_user_language_preference(state: dict[str, Any],
                                    preference_text: str) -> dict[str, Any]:
    if not isinstance(state, dict):
        state = new_voice_session()
    prefs = vpe.extract_voice_memory_preferences(preference_text)
    update = vpe.normalize_preference_update(prefs)
    out = dict(state)
    for k, v in update.items():
        out[k] = v
    out["updated_at"] = time.time()
    return vms.clamp_voice_memory_state(out)


def apply_user_style_correction(state: dict[str, Any],
                                  correction_text: str) -> dict[str, Any]:
    # Defer the heavy logic to the correction-memory module to avoid a
    # circular import here; the state manager only records the raw text +
    # timestamp, the correction module classifies and dedupes on demand.
    if not isinstance(state, dict):
        state = new_voice_session()
    out = dict(state)
    rec = {"text": (correction_text or "")[:240],
           "ts": time.time()}
    out["recent_corrections"] = _push_bounded(
        out.get("recent_corrections") or [], rec)
    out["updated_at"] = time.time()
    return vms.clamp_voice_memory_state(out)


def update_voice_session(state: dict[str, Any],
                          user_text: str,
                          response_plan: Optional[dict[str, Any]] = None,
                          render_payload: Optional[dict[str, Any]] = None,
                          conversation_mode: str = "conversation"
                          ) -> dict[str, Any]:
    if not isinstance(state, dict):
        state = new_voice_session()
    out = dict(state)
    detected = lmd.classify_language_mode(user_text)
    out["last_detected_language_mode"] = detected["mode"]
    if response_plan and isinstance(response_plan, dict):
        out["last_chosen_response_mode"] = (
            response_plan.get("chosen_response_mode")
            or response_plan.get("chosen_spoken_mode"))
        if response_plan.get("turn_strategy") is not None:
            ts = (response_plan.get("turn_strategy") or {}).get("strategy")
            if ts:
                out["recent_turn_types"] = _push_bounded(
                    out.get("recent_turn_types") or [], ts)
    if render_payload and isinstance(render_payload, dict):
        out["last_spoken_render_mode"] = render_payload.get("language_mode")
        ss = render_payload.get("safety_summary") or {}
        seen = dict(out.get("safety_flags_seen") or {})
        if ss.get("unsafe_leakage_detected"):
            seen["unsafe_leakage_detected"] = int(
                seen.get("unsafe_leakage_detected", 0)) + 1
        for r in ss.get("risks") or []:
            head = str(r).split(":", 1)[0]
            seen[head] = int(seen.get(head, 0)) + 1
        out["safety_flags_seen"] = seen
    # Track recent code-switch density via transitions per sentence as
    # a rough proxy.
    tps = (detected.get("transitions") or {}).get(
        "transitions_per_sentence", 0)
    out["recent_code_switch_density"] = _push_bounded(
        out.get("recent_code_switch_density") or [], float(tps))
    out["recent_language_modes"] = _push_bounded(
        out.get("recent_language_modes") or [], detected["mode"])
    # Apply any preferences expressed in this turn.
    prefs = vpe.extract_voice_memory_preferences(user_text)
    pref_update = vpe.normalize_preference_update(prefs)
    for k, v in pref_update.items():
        out[k] = v
    out["updated_at"] = time.time()
    return vms.clamp_voice_memory_state(out)


def summarize_voice_session_state(state: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(state, dict):
        return {"ok": False, "reason": "state_not_dict"}
    return {
        "ok": True,
        "session_id": state.get("session_id"),
        "preferred_language_mode": state.get("preferred_language_mode"),
        "preferred_spoken_mode": state.get("preferred_spoken_mode"),
        "preferred_code_switch_density":
            state.get("preferred_code_switch_density"),
        "preferred_formality": state.get("preferred_formality"),
        "preferred_turn_style": state.get("preferred_turn_style"),
        "user_is_practicing_language":
            state.get("user_is_practicing_language"),
        "last_detected_language_mode":
            state.get("last_detected_language_mode"),
        "last_chosen_response_mode":
            state.get("last_chosen_response_mode"),
        "recent_language_modes":
            (state.get("recent_language_modes") or [])[-5:],
        "n_corrections": len(state.get("recent_corrections") or []),
        "memory_scope": state.get("memory_scope"),
        "updated_at": state.get("updated_at"),
    }


def reset_voice_session_state(state: dict[str, Any],
                              keep_preferences: bool = False
                              ) -> dict[str, Any]:
    if not isinstance(state, dict):
        return new_voice_session()
    fresh = new_voice_session(session_id=state.get("session_id"))
    if keep_preferences:
        for k in vms.SUPPORTED_PREFERENCE_KEYS:
            if state.get(k) is not None:
                fresh[k] = state.get(k)
    return fresh


def merge_session_updates(state: dict[str, Any],
                            updates: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(state, dict):
        state = new_voice_session()
    if not isinstance(updates, dict):
        return state
    out = dict(state)
    for k, v in updates.items():
        if k in ("session_id", "created_at"):
            continue
        out[k] = v
    out["updated_at"] = time.time()
    return vms.clamp_voice_memory_state(out)


def write_voice_memory_state_report(report: dict[str, Any],
                                     output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "new_voice_session",
    "update_voice_session",
    "apply_user_language_preference",
    "apply_user_style_correction",
    "summarize_voice_session_state",
    "reset_voice_session_state",
    "merge_session_updates",
    "write_voice_memory_state_report",
]
