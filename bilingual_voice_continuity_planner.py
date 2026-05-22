"""Phase 26 - Bilingual Voice Continuity Planner.

Resolves current-turn language/style decisions against prior voice-memory
state so Luna does not reset every turn. Latest explicit user instruction
always wins. Safety policy always wins over memory.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_language_mode_detector as lmd
import bilingual_code_switch_policy as csp
import bilingual_voice_preference_extractor as vpe
import bilingual_voice_correction_memory as vcm


def _avg(seq) -> float:
    seq = [float(x) for x in (seq or []) if isinstance(x, (int, float))]
    if not seq:
        return 0.0
    return sum(seq) / len(seq)


def resolve_language_mode_with_memory(user_text: str,
                                      state: dict[str, Any],
                                      detected_mode: str) -> dict[str, Any]:
    # 1. Latest explicit instruction this turn wins.
    pref = vpe.extract_language_preference(user_text)
    if pref.get("detected"):
        v = pref["value"]
        if v == "russian":
            return {"language_mode": "russian_only",
                    "reason": "latest_explicit_user_pref_russian",
                    "wins_over_memory": True}
        if v == "english":
            return {"language_mode": "english_only",
                    "reason": "latest_explicit_user_pref_english",
                    "wins_over_memory": True}
        if v == "mix":
            return {"language_mode": "mixed_en_ru",
                    "reason": "latest_explicit_user_pref_mix",
                    "wins_over_memory": True}
        if v == "no_mix_keep_one_language":
            # Fall back to mirroring current detected mode (no switch).
            return {"language_mode": detected_mode
                    if detected_mode in ("english_only", "russian_only")
                    else "english_only"
                    if (state or {}).get("preferred_language_mode")
                    == "english_only" else "russian_only"
                    if (state or {}).get("preferred_language_mode")
                    == "russian_only" else "english_only",
                    "reason": "no_mix_resolves_to_dominant_lang",
                    "wins_over_memory": True}
    # 2. Otherwise: prefer memory if set, else mirror detected.
    if isinstance(state, dict):
        plm = state.get("preferred_language_mode")
        if plm and plm != "auto":
            return {"language_mode": plm,
                    "reason": "memory_preferred_language_mode",
                    "wins_over_memory": False}
    return {"language_mode": detected_mode or "english_only",
            "reason": "mirror_detected_mode", "wins_over_memory": False}


def resolve_code_switch_density_with_memory(
    user_text: str, state: dict[str, Any],
    policy_decision: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    cs = vpe.extract_code_switch_preference(user_text)
    if cs.get("detected"):
        v = cs["value"]
        if v == "stop_mixing":
            return {"density": 0.0, "reason": "user_stop_mixing"}
        if v == "mix_more":
            return {"density": 0.55, "reason": "user_mix_more"}
    if isinstance(state, dict):
        d = state.get("preferred_code_switch_density")
        if isinstance(d, (int, float)):
            return {"density": max(0.0, min(1.0, float(d))),
                    "reason": "memory_preferred_density"}
    # Otherwise, use Phase 23 policy default if provided.
    if isinstance(policy_decision, dict) \
            and "density" in policy_decision:
        try:
            return {"density": float(policy_decision["density"]),
                    "reason": "policy_decision_default"}
        except Exception:
            pass
    # Otherwise, ewma over recent observations.
    if isinstance(state, dict):
        avg = _avg(state.get("recent_code_switch_density") or [])
        return {"density": round(min(1.0, max(0.0, avg / 4.0)), 3),
                "reason": "memory_ewma_default"}
    return {"density": 0.0, "reason": "no_memory_default_zero"}


def resolve_formality_with_memory(user_text: str,
                                  state: dict[str, Any]) -> dict[str, Any]:
    form = vpe.extract_formality_preference(user_text)
    if form.get("detected"):
        v = form["value"]
        if v == "less_formal":
            return {"formality": "casual", "reason": "user_less_formal"}
        if v == "more_formal":
            return {"formality": "professional",
                    "reason": "user_more_formal"}
    if isinstance(state, dict):
        f = state.get("preferred_formality")
        if f and f != "unknown":
            return {"formality": f, "reason": "memory_preferred_formality"}
    return {"formality": "standard", "reason": "default"}


def resolve_turn_style_with_memory(user_text: str,
                                    state: dict[str, Any]
                                    ) -> dict[str, Any]:
    t = vpe.extract_turn_style_preference(user_text)
    if t.get("detected"):
        return {"turn_style": t["value"], "reason": "user_turn_style"}
    if isinstance(state, dict):
        ts = state.get("preferred_turn_style")
        if ts and ts != "unknown":
            return {"turn_style": ts, "reason": "memory_turn_style"}
    return {"turn_style": "balanced", "reason": "default"}


def detect_continuity_conflict(user_text: str,
                                state: dict[str, Any]) -> dict[str, Any]:
    """Detect mismatch between current explicit user instruction and
    prior memory state. Latest user instruction wins; we just report."""
    if not isinstance(state, dict):
        return {"conflict": False, "reason": "no_state"}
    pref = vpe.extract_language_preference(user_text)
    conflicts: list[str] = []
    if pref.get("detected"):
        v = pref["value"]
        plm = state.get("preferred_language_mode")
        if v == "russian" and plm and plm not in (
                "russian_only", "auto"):
            conflicts.append("user_now_russian_vs_memory_other")
        if v == "english" and plm and plm not in (
                "english_only", "auto"):
            conflicts.append("user_now_english_vs_memory_other")
        if v == "mix" and plm in ("english_only", "russian_only"):
            conflicts.append("user_now_mix_vs_memory_single_lang")
        if v == "no_mix_keep_one_language" and \
                isinstance(state.get("preferred_code_switch_density"),
                           (int, float)) and \
                state["preferred_code_switch_density"] > 0.0:
            conflicts.append("user_now_no_mix_vs_memory_mix")
    return {"conflict": bool(conflicts),
            "conflicts": conflicts,
            "resolution": "latest_user_instruction_wins"
            if conflicts else "no_conflict"}


def generate_continuity_notes(state: dict[str, Any],
                              decision: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    if not isinstance(decision, dict):
        return notes
    lang = (decision.get("language") or {}).get("language_mode")
    if lang:
        notes.append(f"chose language_mode={lang} ("
                     f"{(decision.get('language') or {}).get('reason')})")
    dens = (decision.get("code_switch") or {}).get("density")
    if dens is not None:
        notes.append(f"chose code_switch_density={dens}")
    formality = (decision.get("formality") or {}).get("formality")
    if formality:
        notes.append(f"formality={formality}")
    ts = (decision.get("turn_style") or {}).get("turn_style")
    if ts:
        notes.append(f"turn_style={ts}")
    if isinstance(state, dict) and state.get("user_is_practicing_language") \
            and state["user_is_practicing_language"] != "none":
        notes.append(f"practice_language={state['user_is_practicing_language']}")
    actives = vcm.get_active_corrections(state)
    if actives:
        latest = actives[-1].get("type")
        if latest:
            notes.append(f"latest_active_correction={latest}")
    conflict = decision.get("conflict") or {}
    if conflict.get("conflict"):
        notes.append("note: latest user instruction overrode memory")
    return notes[:20]


def _teacher_overrides(decision: dict[str, Any],
                       conversation_mode: str) -> dict[str, Any]:
    """Teacher/professional modes can override excessive casual/slang and
    cap code-switch density."""
    if conversation_mode in ("teacher", "professional", "curriculum",
                              "technical"):
        cs = decision.get("code_switch") or {}
        if isinstance(cs.get("density"), (int, float)):
            cs["density"] = min(float(cs["density"]), 0.25)
            cs["reason"] = (cs.get("reason", "") + "+capped_by_clean_mode")
            decision["code_switch"] = cs
        form = decision.get("formality") or {}
        if form.get("formality") == "casual":
            form["formality"] = "standard"
            form["reason"] = form.get("reason", "") + "+overridden_by_clean_mode"
            decision["formality"] = form
    return decision


def _practice_overrides(decision: dict[str, Any],
                         state: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(state, dict):
        return decision
    pl = state.get("user_is_practicing_language")
    lang = decision.get("language") or {}
    if pl == "ru" and lang.get("language_mode") == "english_only":
        lang["language_mode"] = "russian_with_english_terms"
        lang["reason"] = lang.get("reason", "") + "+practice_ru_override"
        decision["language"] = lang
    elif pl == "en" and lang.get("language_mode") == "russian_only":
        lang["language_mode"] = "english_with_russian_terms"
        lang["reason"] = lang.get("reason", "") + "+practice_en_override"
        decision["language"] = lang
    return decision


def plan_continuity_for_turn(user_text: str,
                              state: Optional[dict[str, Any]] = None,
                              conversation_mode: str = "conversation",
                              user_preference: Optional[str] = None,
                              limit: int = 25) -> dict[str, Any]:
    detected = lmd.classify_language_mode(user_text)
    pol = csp.choose_response_language_mode(
        user_text, detected["mode"],
        user_preference=user_preference,
        context={"policy": conversation_mode})
    lang_decision = resolve_language_mode_with_memory(
        user_text, state or {}, detected["mode"])
    cs_decision = resolve_code_switch_density_with_memory(
        user_text, state or {})
    form_decision = resolve_formality_with_memory(user_text, state or {})
    turn_decision = resolve_turn_style_with_memory(user_text, state or {})
    conflict = detect_continuity_conflict(user_text, state or {})
    decision: dict[str, Any] = {
        "detected_language_mode": detected["mode"],
        "policy_response_mode": pol["response_mode"],
        "language": lang_decision,
        "code_switch": cs_decision,
        "formality": form_decision,
        "turn_style": turn_decision,
        "conflict": conflict,
        "conversation_mode": conversation_mode,
    }
    decision = _teacher_overrides(decision, conversation_mode)
    decision = _practice_overrides(decision, state or {})
    decision["notes"] = generate_continuity_notes(state or {}, decision)
    return {"ok": True, "plan": decision}


def write_continuity_plan_report(report: dict[str, Any],
                                  output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "plan_continuity_for_turn",
    "resolve_language_mode_with_memory",
    "resolve_code_switch_density_with_memory",
    "resolve_formality_with_memory",
    "resolve_turn_style_with_memory",
    "detect_continuity_conflict",
    "generate_continuity_notes",
    "write_continuity_plan_report",
]
