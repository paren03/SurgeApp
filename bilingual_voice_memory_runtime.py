"""Phase 26 - Bilingual Voice Memory Runtime.

Single standalone continuity entry point. Not integrated. Session memory
is the default; persistence is dry-run unless explicitly called.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_memory_schema as vms
import bilingual_voice_memory_state as vmst
import bilingual_voice_preference_extractor as vpe
import bilingual_voice_correction_memory as vcm
import bilingual_voice_continuity_planner as vcp
import bilingual_language_mode_detector as lmd
import bilingual_voice_style_runtime as vsr
import bilingual_spoken_render_contract as src
import bilingual_spoken_render_runtime as rrt


_DEFAULT_LIMIT = 25
_HARD_LIMIT = 100


def _clamp(n: Optional[int]) -> int:
    if n is None:
        return _DEFAULT_LIMIT
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_LIMIT
    return max(1, min(v, _HARD_LIMIT))


def update_voice_memory_from_turn(state: dict[str, Any],
                                    user_text: str,
                                    response_plan: Optional[dict[str, Any]] = None,
                                    render_payload: Optional[dict[str, Any]] = None,
                                    conversation_mode: str = "conversation"
                                    ) -> dict[str, Any]:
    s = vmst.update_voice_session(state, user_text,
                                    response_plan=response_plan,
                                    render_payload=render_payload,
                                    conversation_mode=conversation_mode)
    s = vcm.apply_correction_to_state(s, user_text)
    return s


def get_voice_continuity_plan(user_text: str,
                               state: Optional[dict[str, Any]] = None,
                               conversation_mode: str = "conversation",
                               user_preference: Optional[str] = None,
                               render_payload: Optional[dict[str, Any]] = None,
                               limit: int = _DEFAULT_LIMIT,
                               link_db_path: Optional[str] = None
                               ) -> dict[str, Any]:
    cap = _clamp(limit)
    if not isinstance(state, dict):
        state = vmst.new_voice_session()
    prefs = vpe.extract_voice_memory_preferences(user_text)
    plan = vcp.plan_continuity_for_turn(
        user_text, state=state, conversation_mode=conversation_mode,
        user_preference=user_preference, limit=cap)
    actives = vcm.get_active_corrections(state)
    vs_plan = vsr.get_bilingual_voice_style_plan(
        user_text, conversation_state=state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
        limit=cap, link_db_path=link_db_path)
    # Memory-driven render adjustments (advisory, not applied)
    spoken_render_adjustments = {
        "chosen_spoken_mode": (plan["plan"].get("language") or {}
                                ).get("language_mode")
        or vs_plan.get("chosen_spoken_mode"),
        "code_switch_density": (plan["plan"].get("code_switch") or {}
                                  ).get("density"),
        "formality": (plan["plan"].get("formality") or {}).get("formality"),
        "turn_style": (plan["plan"].get("turn_style") or {}).get("turn_style"),
    }
    updated = update_voice_memory_from_turn(
        state, user_text, response_plan=vs_plan,
        render_payload=render_payload,
        conversation_mode=conversation_mode)
    return {
        "ok": True,
        "detected_language_mode":
            plan["plan"].get("detected_language_mode"),
        "extracted_preferences": prefs,
        "active_corrections": actives,
        "continuity_decision": plan["plan"],
        "voice_style_plan": {
            "detected_language_mode": vs_plan["detected_language_mode"],
            "chosen_spoken_mode": vs_plan["chosen_spoken_mode"],
            "spoken_register": vs_plan["spoken_register"],
            "code_switch_density": vs_plan["code_switch_density"],
        },
        "spoken_render_adjustments": spoken_render_adjustments,
        "updated_state": updated,
        "safety_summary": vs_plan.get("voice_safety_summary", {}),
        "continuity_notes": plan["plan"].get("notes") or [],
        "persistence_status": "session_only",
        "gap_notes": vs_plan.get("gap_notes"),
    }


def apply_voice_memory_to_render_payload(payload: dict[str, Any],
                                          state: dict[str, Any]
                                          ) -> dict[str, Any]:
    """Annotate a Phase 25 render payload with memory-derived hints in its
    metadata. Does NOT change segments or safety_summary structure."""
    if not isinstance(payload, dict):
        return {"ok": False, "reason": "payload_not_dict"}
    out = dict(payload)
    md = dict(out.get("metadata") or {})
    md["voice_memory_session_id"] = (state or {}).get("session_id")
    md["voice_memory_preferred_spoken_mode"] = (state or {}).get(
        "preferred_spoken_mode")
    md["voice_memory_preferred_formality"] = (state or {}).get(
        "preferred_formality")
    md["voice_memory_preferred_code_switch_density"] = (
        state or {}).get("preferred_code_switch_density")
    md["voice_memory_practice_language"] = (state or {}).get(
        "user_is_practicing_language")
    out["metadata"] = md
    val = src.validate_spoken_render_payload(out)
    return {"ok": val.get("ok", False),
            "payload": out, "validation": val}


def summarize_voice_continuity(state: dict[str, Any]) -> dict[str, Any]:
    s = vmst.summarize_voice_session_state(state)
    c = vcm.summarize_corrections(state)
    return {"ok": True, "state": s, "corrections": c}


def demo_voice_memory_scenarios(limit: int = 12) -> dict[str, Any]:
    cap = max(1, min(int(limit), 20))
    scenarios = [
        ("Hello, can you explain a lighthouse?", "conversation", None),
        ("Switch to Russian please.", "conversation", None),
        ("Расскажи мне про маяк.", "conversation", None),
        ("Mix English and Russian a bit.", "conversation", None),
        ("Be less formal.", "warm_friend", None),
        ("Stop mixing.", "conversation", None),
        ("Let's practice Russian.", "bilingual_practice", "russian"),
        ("Correct my Russian.", "bilingual_practice", "russian"),
        ("Shorter answers please.", "concise", None),
        ("Switch to English.", "conversation", None),
        ("More formal please.", "professional", None),
        ("More natural.", "conversation", None),
    ][:cap]
    state = vmst.new_voice_session()
    out: list[dict[str, Any]] = []
    for text, mode, pref in scenarios:
        plan = get_voice_continuity_plan(text, state=state,
                                           conversation_mode=mode,
                                           user_preference=pref,
                                           limit=5)
        state = plan["updated_state"]
        out.append({
            "user_text": text,
            "conversation_mode": mode,
            "detected": plan["detected_language_mode"],
            "spoken": plan["voice_style_plan"]["chosen_spoken_mode"],
            "memory_lang": state.get("preferred_language_mode"),
            "memory_density": state.get("preferred_code_switch_density"),
            "memory_formality": state.get("preferred_formality"),
            "n_corrections": len(state.get("recent_corrections") or []),
        })
    return {"ok": True, "count": len(out), "scenarios": out,
            "final_state_summary": vmst.summarize_voice_session_state(state)}


def write_voice_memory_runtime_report(report: dict[str, Any],
                                       output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_voice_continuity_plan",
    "update_voice_memory_from_turn",
    "apply_voice_memory_to_render_payload",
    "summarize_voice_continuity",
    "demo_voice_memory_scenarios",
    "write_voice_memory_runtime_report",
]
