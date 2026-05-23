"""Phase 24 - Bilingual Voice Style Runtime.

Single standalone entry point. NOT integrated into Luna main runtime. No
audio synthesis, no TTS, no voice cloning, no network.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_personality_profile as vpp
import bilingual_spoken_style_planner as ssp
import bilingual_personality_continuity_scorer as pcs
import bilingual_turn_taking_strategy as tts
import bilingual_voice_safety_filter as vsf
import bilingual_conversation_state as cstate
import bilingual_language_mode_detector as lmd


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


def get_bilingual_voice_style_plan(
    user_text: str,
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    is_user_prompted: bool = False,
    limit: int = _DEFAULT_LIMIT,
    link_db_path: Optional[str] = None,
) -> dict[str, Any]:
    cap = _clamp(limit)
    style_plan = ssp.plan_spoken_response_style(
        user_text, conversation_mode=conversation_mode,
        user_preference=user_preference,
        is_user_prompted=is_user_prompted, limit=cap,
        link_db_path=link_db_path)
    plan = style_plan["plan"]
    spoken_mode = plan["spoken_mode"]
    profile = vpp.get_spoken_style_profile(spoken_mode, conversation_mode)
    turn_plan = tts.plan_bilingual_turn(
        user_text, conversation_state=conversation_state,
        conversation_mode=conversation_mode)
    # Voice-safety pass over the bilingual context entries.
    ctx_entries = plan.get("bilingual_context", {}).get("entries", [])
    voice_safety = vsf.filter_voice_style_terms(
        ctx_entries, mode=conversation_mode,
        is_user_prompted=is_user_prompted)
    voice_safety_summary = {
        "spoken_safe_count": voice_safety["spoken_safe_count"],
        "suggestion_blocked_count": voice_safety["suggestion_blocked_count"],
        "blocked_count": voice_safety["blocked_count"],
    }
    # Continuity score is only meaningful when we have a draft - here we
    # score against the skeleton placeholder text to give the caller a
    # baseline structure.
    skeleton_text = " ".join(
        step.get("text", "") for step in
        plan.get("skeleton", {}).get("skeleton_steps", []))
    continuity = pcs.score_personality_continuity(
        skeleton_text, language_mode=spoken_mode,
        conversation_mode=conversation_mode)
    # Updated conversation state
    state_in = (conversation_state if isinstance(conversation_state, dict)
                else cstate.create_conversation_language_state())
    state_out = cstate.update_language_state(
        dict(state_in), user_text,
        {"mode": plan["detected_mode"],
         "ratio": lmd.estimate_language_ratio(user_text),
         "transitions": lmd.detect_code_switch_points(user_text),
         "preference": lmd.detect_user_language_preference(user_text)},
        plan["response_mode"])
    return {
        "ok": True,
        "detected_language_mode": plan["detected_mode"],
        "chosen_spoken_mode": spoken_mode,
        "code_switch_density":
            plan["code_switch_density"]["code_switch_density"],
        "spoken_register": plan["register"]["allowed_registers"],
        "sentence_length_guidance": plan["sentence_length"],
        "personality_profile": profile,
        "spoken_style_instructions": plan["spoken_style_instructions"],
        "turn_strategy": turn_plan["turn_strategy"],
        "voice_safety_summary": voice_safety_summary,
        "continuity_score": {"overall": continuity["overall_score"],
                              "verdict": continuity["verdict"]},
        "quality_notes": [
            "plan only - not final spoken wording",
            "no audio synthesis - no TTS - no voice cloning",
            "Phase 23 safety policy applied",
        ],
        "demo_response_skeleton": plan["skeleton"],
        "updated_conversation_state": state_out,
        "gap_notes": plan.get("gap_notes"),
    }


def get_voice_ready_guidance(user_text: str,
                             conversation_mode: str = "conversation",
                             user_preference: Optional[str] = None,
                             limit: int = _DEFAULT_LIMIT,
                             link_db_path: Optional[str] = None
                             ) -> dict[str, Any]:
    """Lighter helper that returns only the spoken-style instructions +
    skeleton (no continuity, no turn plan)."""
    cap = _clamp(limit)
    style_plan = ssp.plan_spoken_response_style(
        user_text, conversation_mode=conversation_mode,
        user_preference=user_preference, limit=cap,
        link_db_path=link_db_path)
    plan = style_plan["plan"]
    return {"ok": True,
            "detected_language_mode": plan["detected_mode"],
            "chosen_spoken_mode": plan["spoken_mode"],
            "spoken_style_instructions": plan["spoken_style_instructions"],
            "demo_response_skeleton": plan["skeleton"]}


def evaluate_voice_style_output(text: str,
                                language_mode: str = "mixed_en_ru",
                                conversation_mode: str = "conversation",
                                is_user_prompted: bool = False
                                ) -> dict[str, Any]:
    safe = vsf.check_voice_safe_register(
        text, language_mode=language_mode,
        conversation_mode=conversation_mode,
        is_user_prompted=is_user_prompted)
    leak = vsf.detect_spoken_unsafe_leakage(text)
    continuity = pcs.score_personality_continuity(
        text, language_mode=language_mode,
        conversation_mode=conversation_mode)
    return {"ok": True,
            "safe_register": safe,
            "unsafe_leakage": leak,
            "continuity": continuity,
            "verdict": ("pass" if continuity["overall_score"] >= 0.7
                                and safe["ok"] and leak["ok"]
                        else "warn" if continuity["overall_score"] >= 0.5
                                       and not leak["unsafe_leakage_detected"]
                        else "fail")}


def demo_bilingual_voice_style_scenarios(limit: int = 12) -> dict[str, Any]:
    cap = max(1, min(int(limit), 20))
    scenarios = [
        ("Hello, can you explain a lighthouse?", "conversation", None),
        ("Привет! Расскажи мне про маяк.", "conversation", None),
        ("Hello, я инженер. What's the Russian for ledger?",
         "conversation", None),
        ("Translate 'verse' to Russian please.", "translation_help", None),
        ("Let's practice Russian together.", "bilingual_practice", "russian"),
        ("Explain vectors precisely.", "teacher", None),
        ("idk", "conversation", None),
        ("wait, you said that wrong", "conversation", None),
        ("I feel tired today.", "warm_friend", None),
        ("Build me a function that adds two numbers.", "coding", None),
        ("Just chat with me casually.", "warm_friend", "mix"),
        ("Skazhi mne po-russki, kak dela.", "conversation", None),
    ][:cap]
    out: list[dict[str, Any]] = []
    for text, mode, pref in scenarios:
        p = get_bilingual_voice_style_plan(
            text, conversation_mode=mode, user_preference=pref,
            limit=5)
        out.append({"user_text": text,
                    "mode": mode,
                    "preference": pref,
                    "detected": p["detected_language_mode"],
                    "spoken": p["chosen_spoken_mode"],
                    "registers": p["spoken_register"],
                    "density": p["code_switch_density"],
                    "turn_strategy": p["turn_strategy"]["strategy"]})
    return {"ok": True, "count": len(out), "scenarios": out}


def write_voice_style_runtime_report(report: dict[str, Any],
                                     output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_bilingual_voice_style_plan",
    "get_voice_ready_guidance",
    "evaluate_voice_style_output",
    "demo_bilingual_voice_style_scenarios",
    "write_voice_style_runtime_report",
]
