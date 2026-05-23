"""Phase 24 - Bilingual Spoken Style Planner.

Turns a semantic answer plan into voice-ready style instructions WITHOUT
generating audio or final wording. Uses Phase 23 detector/policy/runtime
to stay bounded and safety-aware.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_language_mode_detector as lmd
import bilingual_code_switch_policy as csp
import bilingual_human_switch_runtime as hsr
import bilingual_voice_personality_profile as vpp


_DEFAULT_LIMIT = 25
_HARD_LIMIT = 100


_LANG_MODE_TO_SPOKEN = {
    "english_only": "english_only",
    "russian_only": "russian_only",
    "english_with_russian_terms": "english_with_russian_terms",
    "russian_with_english_terms": "russian_with_english_terms",
    "mixed_en_ru": "mixed_en_ru",
    "code_switch_sentence_level": "mixed_en_ru",
    "code_switch_phrase_level": "mixed_en_ru",
    "code_switch_word_level": "english_with_russian_terms",
    "transliterated_russian": "russian_only",
    "unknown": "english_only",
}


def _clamp(n: Optional[int]) -> int:
    if n is None:
        return _DEFAULT_LIMIT
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_LIMIT
    return max(1, min(v, _HARD_LIMIT))


def choose_spoken_language_mode(user_text: str,
                                user_preference: Optional[str] = None,
                                conversation_mode: str = "conversation"
                                ) -> dict[str, Any]:
    detected = lmd.classify_language_mode(user_text)
    chosen = csp.choose_response_language_mode(
        user_text, detected["mode"], user_preference=user_preference,
        context={"policy": conversation_mode})
    spoken = _LANG_MODE_TO_SPOKEN.get(chosen["response_mode"], "english_only")
    return {"detected_mode": detected["mode"],
            "response_mode": chosen["response_mode"],
            "spoken_mode": spoken,
            "reason": chosen.get("reason")}


def choose_spoken_sentence_length(language_mode: str,
                                  conversation_mode: str) -> dict[str, Any]:
    profile = vpp.get_spoken_style_profile(language_mode, conversation_mode)
    sl = profile["sentence_length_chars"]
    return {"min_chars": int(sl["min"]),
            "max_chars": int(sl["max"]),
            "guidance": ("short_to_medium" if sl["max"] <= 160
                         else "medium")}


def choose_spoken_register(language_mode: str, conversation_mode: str,
                           is_user_prompted: bool = False
                           ) -> dict[str, Any]:
    profile = vpp.get_spoken_style_profile(language_mode, conversation_mode)
    allowed = list(profile["preferred_register"])
    if conversation_mode == "slang_allowed" and is_user_prompted:
        if "slang" not in allowed:
            allowed.append("slang")
    if conversation_mode in ("teacher", "professional", "technical",
                              "curriculum", "concise"):
        for r in ("vulgar", "offensive", "slang", "street"):
            if r in allowed:
                allowed.remove(r)
    return {"allowed_registers": allowed,
            "language_mode": language_mode,
            "conversation_mode": conversation_mode}


def choose_code_switch_density(language_mode: str,
                               conversation_mode: str,
                               user_preference: Optional[str] = None
                               ) -> dict[str, Any]:
    profile = vpp.get_spoken_style_profile(language_mode, conversation_mode)
    density = float(profile["code_switch_density"])
    if user_preference in ("english", "english_only"):
        density = 0.0
    elif user_preference in ("russian", "russian_only"):
        density = 0.0
    elif user_preference in ("mix", "mixed", "bilingual"):
        density = max(density, 0.45)
    return {"code_switch_density": round(density, 3),
            "language_mode": language_mode,
            "conversation_mode": conversation_mode,
            "user_preference": user_preference}


def generate_spoken_style_instructions(plan: dict[str, Any]) -> list[str]:
    if not isinstance(plan, dict):
        return ["plan_not_dict"]
    base = vpp.get_luna_bilingual_personality_profile()
    spoken_mode = plan.get("spoken_mode") or plan.get("response_mode") or \
        "english_only"
    conv = plan.get("conversation_mode") or "conversation"
    sl = plan.get("sentence_length", {})
    register = plan.get("register", {}).get("allowed_registers", [])
    density = plan.get("code_switch_density", {}).get("code_switch_density", 0)
    instructions = [
        f"Speak as: {base['core_identity']}",
        f"Target spoken mode: {spoken_mode}.",
        f"Sentence length: {sl.get('min_chars', 40)}-{sl.get('max_chars', 140)} chars.",
        f"Preferred registers: {', '.join(register) or 'standard'}.",
        f"Code-switch density: {density} (0.0 = no switch, 1.0 = heavy).",
        "Use active voice and clear, plain phrasing.",
        "Avoid filler hedges and fake academic phrasing.",
        ("Avoid translating idioms word-for-word; pick the natural form in "
         "the target language instead."),
    ]
    if conv in ("teacher", "curriculum"):
        instructions.append("Pause to check understanding when needed.")
    if conv == "concise":
        instructions.append("Keep responses under 2 sentences when possible.")
    if spoken_mode in ("mixed_en_ru", "english_with_russian_terms",
                        "russian_with_english_terms"):
        instructions.append("Switch only where the term genuinely fits the thought.")
        instructions.append("Repair phrase if a switch feels forced: "
                            "'Let me say that simpler...' / 'По-простому...'.")
    if spoken_mode == "russian_only":
        instructions.append("Use natural conversational Russian register.")
    if spoken_mode == "english_only":
        instructions.append("Use warm conversational English.")
    return instructions


def produce_voice_ready_response_skeleton(
    user_text: str,
    plan: dict[str, Any],
    limit: int = 10,
) -> dict[str, Any]:
    cap = max(1, min(int(limit), 25))
    spoken_mode = plan.get("spoken_mode") or "english_only"
    conv = plan.get("conversation_mode") or "conversation"
    en_open = "Sure — "
    ru_open = "Хорошо — "
    if conv == "warm_friend":
        en_open = "Of course, "
        ru_open = "Конечно, "
    if conv == "concise":
        en_open = ""
        ru_open = ""
    steps: list[dict[str, Any]] = []
    if spoken_mode == "english_only":
        steps.append({"slot": "open", "text": en_open})
        steps.append({"slot": "main", "text": "[main spoken answer in English]"})
        steps.append({"slot": "close", "text": "[short close in English if useful]"})
    elif spoken_mode == "russian_only":
        steps.append({"slot": "open", "text": ru_open})
        steps.append({"slot": "main", "text": "[основной ответ по-русски]"})
        steps.append({"slot": "close", "text": "[короткая концовка по-русски]"})
    elif spoken_mode == "english_with_russian_terms":
        steps.append({"slot": "open", "text": en_open})
        steps.append({"slot": "main",
                      "text": "[main English; insert 1-2 RU terms only "
                              "where they fit]"})
        steps.append({"slot": "close", "text": "[short close in English]"})
    elif spoken_mode == "russian_with_english_terms":
        steps.append({"slot": "open", "text": ru_open})
        steps.append({"slot": "main",
                      "text": "[основной русский; 1-2 английских термина "
                              "только где они уместны]"})
        steps.append({"slot": "close", "text": "[короткая концовка по-русски]"})
    else:  # mixed_en_ru
        steps.append({"slot": "open", "text": en_open + "/ " + ru_open})
        steps.append({"slot": "main",
                      "text": "[balanced mixed EN/RU spoken answer]"})
        steps.append({"slot": "close",
                      "text": "[short close mirroring user's style]"})
    return {"ok": True,
            "user_text_preview": (user_text or "")[:160],
            "spoken_mode": spoken_mode,
            "conversation_mode": conv,
            "skeleton_steps": steps[:cap],
            "note": "skeleton only - not final spoken wording"}


def plan_spoken_response_style(
    user_text: str,
    semantic_intent: str = "",
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    limit: int = _DEFAULT_LIMIT,
    is_user_prompted: bool = False,
    link_db_path: Optional[str] = None,
) -> dict[str, Any]:
    cap = _clamp(limit)
    mode_pick = choose_spoken_language_mode(
        user_text, user_preference=user_preference,
        conversation_mode=conversation_mode)
    spoken_mode = mode_pick["spoken_mode"]
    sl = choose_spoken_sentence_length(spoken_mode, conversation_mode)
    reg = choose_spoken_register(spoken_mode, conversation_mode,
                                  is_user_prompted=is_user_prompted)
    csd = choose_code_switch_density(spoken_mode, conversation_mode,
                                      user_preference=user_preference)
    style_profile = vpp.get_spoken_style_profile(spoken_mode,
                                                  conversation_mode)
    # Pull bilingual context for switch terms (bounded; respects safety)
    ctx = hsr.get_mixed_language_context(
        user_text, mode=conversation_mode, limit=cap,
        is_user_prompted=is_user_prompted, link_db_path=link_db_path)
    plan = {
        "detected_mode": mode_pick["detected_mode"],
        "response_mode": mode_pick["response_mode"],
        "spoken_mode": spoken_mode,
        "conversation_mode": conversation_mode,
        "semantic_intent": (semantic_intent or "")[:240],
        "sentence_length": sl,
        "register": reg,
        "code_switch_density": csd,
        "style_profile": style_profile,
        "bilingual_context": ctx.get("context", {"count": 0, "entries": []}),
        "safety_summary": ctx.get("safety_summary", {}),
        "gap_notes": ctx.get("gap_explanation"),
    }
    plan["spoken_style_instructions"] = generate_spoken_style_instructions(plan)
    plan["skeleton"] = produce_voice_ready_response_skeleton(
        user_text, plan, limit=cap)
    return {"ok": True, "plan": plan}


def write_spoken_style_plan_report(report: dict[str, Any],
                                   output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "plan_spoken_response_style",
    "choose_spoken_language_mode",
    "choose_spoken_sentence_length",
    "choose_spoken_register",
    "choose_code_switch_density",
    "generate_spoken_style_instructions",
    "produce_voice_ready_response_skeleton",
    "write_spoken_style_plan_report",
]
