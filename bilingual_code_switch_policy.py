"""Phase 23 - Bilingual Code-Switch Policy.

Decides when Luna should code-switch and at what granularity.
Read-only, bounded. No runtime integration.
"""

from __future__ import annotations

import json
from typing import Any, Optional


SWITCH_GRANULARITIES = ("none", "word", "term", "phrase", "sentence")

POLICY_MODES = (
    "conversation",
    "teacher",
    "technical",
    "coding",
    "curriculum",
    "professional",
    "warm_friend",
    "concise",
    "slang_allowed",
    "translation_help",
    "bilingual_practice",
)


# Per-policy-mode default behavior. Bounded, additive.
_POLICY_TABLE: dict[str, dict[str, Any]] = {
    "conversation":      {"max_granularity": "phrase",  "favor_user_style": True,  "allow_slang": False},
    "teacher":           {"max_granularity": "term",    "favor_user_style": True,  "allow_slang": False},
    "technical":         {"max_granularity": "term",    "favor_user_style": True,  "allow_slang": False},
    "coding":            {"max_granularity": "term",    "favor_user_style": True,  "allow_slang": False},
    "curriculum":        {"max_granularity": "term",    "favor_user_style": True,  "allow_slang": False},
    "professional":      {"max_granularity": "term",    "favor_user_style": True,  "allow_slang": False},
    "warm_friend":       {"max_granularity": "phrase",  "favor_user_style": True,  "allow_slang": False},
    "concise":           {"max_granularity": "term",    "favor_user_style": True,  "allow_slang": False},
    "slang_allowed":     {"max_granularity": "phrase",  "favor_user_style": True,  "allow_slang": True},
    "translation_help":  {"max_granularity": "phrase",  "favor_user_style": False, "allow_slang": False},
    "bilingual_practice":{"max_granularity": "sentence", "favor_user_style": False, "allow_slang": False},
}


def get_code_switch_policy(mode: str = "conversation") -> dict[str, Any]:
    pol = _POLICY_TABLE.get(mode) or _POLICY_TABLE["conversation"]
    return {"mode": mode if mode in _POLICY_TABLE else "conversation",
            **pol,
            "valid_modes": list(POLICY_MODES),
            "valid_granularities": list(SWITCH_GRANULARITIES)}


def should_code_switch(user_text: str,
                       detected_mode: str,
                       user_preference: Optional[str] = None,
                       context: Optional[dict[str, Any]] = None
                       ) -> dict[str, Any]:
    pol = (context or {}).get("policy") or "conversation"
    pol_data = get_code_switch_policy(pol)
    # Explicit user preference always wins
    if user_preference in ("mix", "mixed", "bilingual"):
        return {"switch": True, "reason": "user_preference_mix",
                "policy": pol_data["mode"]}
    if user_preference in ("en", "english", "english_only", "ru", "russian",
                            "russian_only"):
        return {"switch": False, "reason": f"user_preference_{user_preference}",
                "policy": pol_data["mode"]}
    if detected_mode in ("mixed_en_ru", "english_with_russian_terms",
                          "russian_with_english_terms",
                          "code_switch_sentence_level",
                          "code_switch_phrase_level",
                          "code_switch_word_level"):
        return {"switch": True, "reason": "user_already_mixed",
                "policy": pol_data["mode"]}
    if detected_mode == "english_only":
        return {"switch": False, "reason": "english_only_input",
                "policy": pol_data["mode"]}
    if detected_mode == "russian_only":
        return {"switch": False, "reason": "russian_only_input",
                "policy": pol_data["mode"]}
    return {"switch": False, "reason": "default_no_switch",
            "policy": pol_data["mode"]}


def choose_response_language_mode(user_text: str,
                                  detected_mode: str,
                                  user_preference: Optional[str] = None,
                                  context: Optional[dict[str, Any]] = None
                                  ) -> dict[str, Any]:
    if user_preference == "mix":
        return {"response_mode": "mixed_en_ru", "reason": "user_pref_mix"}
    if user_preference == "english":
        return {"response_mode": "english_only", "reason": "user_pref_en"}
    if user_preference == "russian":
        return {"response_mode": "russian_only", "reason": "user_pref_ru"}
    mirror_map = {
        "english_only": "english_only",
        "russian_only": "russian_only",
        "english_with_russian_terms": "english_with_russian_terms",
        "russian_with_english_terms": "russian_with_english_terms",
        "code_switch_sentence_level": "mixed_en_ru",
        "code_switch_phrase_level": "mixed_en_ru",
        "code_switch_word_level": "mixed_en_ru",
        "mixed_en_ru": "mixed_en_ru",
        "transliterated_russian": "russian_only",
        "unknown": "english_only",
    }
    return {"response_mode": mirror_map.get(detected_mode, "english_only"),
            "reason": "mirror_user"}


def choose_switch_granularity(user_text: str,
                              detected_mode: str,
                              user_preference: Optional[str] = None,
                              context: Optional[dict[str, Any]] = None
                              ) -> dict[str, Any]:
    pol = (context or {}).get("policy") or "conversation"
    pol_data = get_code_switch_policy(pol)
    max_gran = pol_data["max_granularity"]
    detected_to_gran = {
        "english_only": "none",
        "russian_only": "none",
        "english_with_russian_terms": "term",
        "russian_with_english_terms": "term",
        "code_switch_word_level": "word",
        "code_switch_phrase_level": "phrase",
        "code_switch_sentence_level": "sentence",
        "mixed_en_ru": "phrase",
        "transliterated_russian": "none",
        "unknown": "none",
    }
    proposed = detected_to_gran.get(detected_mode, "none")
    # Cap by policy
    order = SWITCH_GRANULARITIES
    cap_idx = order.index(max_gran)
    prop_idx = order.index(proposed)
    final = order[min(cap_idx, prop_idx)]
    return {"granularity": final, "proposed": proposed,
            "policy_max": max_gran, "policy": pol_data["mode"]}


def is_switch_allowed_for_entry(entry: dict[str, Any],
                                mode: str = "conversation",
                                is_user_prompted: bool = False
                                ) -> dict[str, Any]:
    """Check if a specific lexicon entry is safe to surface during a
    code-switch. Hard policy gate."""
    if not isinstance(entry, dict):
        return {"allowed": False, "reason": "entry_not_dict"}
    safety = set(_coerce_list(entry.get("safety_tags")
                              or entry.get("safety_tags_json") or []))
    register = set(_coerce_list(entry.get("register_tags")
                                or entry.get("register_tags_json") or []))
    pol = get_code_switch_policy(mode)
    if "do_not_use_unprompted" in safety and not is_user_prompted:
        return {"allowed": False, "reason": "do_not_use_unprompted"}
    if ({"vulgar", "offensive"} & (safety | register)) and not pol["allow_slang"]:
        return {"allowed": False, "reason": "vulgar_or_offensive_in_non_slang_mode"}
    if ({"vulgar", "offensive"} & (safety | register)) and not is_user_prompted:
        return {"allowed": False, "reason": "vulgar_or_offensive_unprompted"}
    if "recognition_only" in safety:
        return {"allowed": True, "reason": "recognition_only_recognized",
                "suggestion_blocked": True}
    return {"allowed": True, "reason": "ok"}


def filter_switch_candidates(entries: list[dict[str, Any]],
                             mode: str = "conversation",
                             is_user_prompted: bool = False
                             ) -> dict[str, Any]:
    safe: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for e in entries:
        decision = is_switch_allowed_for_entry(e, mode=mode,
                                                is_user_prompted=is_user_prompted)
        if decision["allowed"]:
            e2 = dict(e)
            if decision.get("suggestion_blocked"):
                e2["_suggestion_blocked"] = True
            safe.append(e2)
        else:
            blocked.append({"entry": e.get("word") or e.get("phrase")
                            or e.get("target_word"),
                            "reason": decision["reason"]})
    return {"ok": True, "safe": safe, "blocked": blocked,
            "safe_count": len(safe), "blocked_count": len(blocked)}


def explain_switch_decision(decision: dict[str, Any]) -> dict[str, Any]:
    parts: list[str] = []
    if decision.get("switch"):
        parts.append(f"Code-switching enabled because: {decision.get('reason')}")
    else:
        parts.append(f"Code-switching disabled because: {decision.get('reason')}")
    parts.append(f"Policy mode: {decision.get('policy', 'conversation')}")
    return {"explanation": "; ".join(parts), "input": decision}


def _coerce_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            d = json.loads(v)
            return [str(x) for x in d] if isinstance(d, list) else []
        except Exception:
            return []
    return []


__all__ = [
    "SWITCH_GRANULARITIES", "POLICY_MODES",
    "get_code_switch_policy",
    "should_code_switch",
    "choose_response_language_mode",
    "choose_switch_granularity",
    "is_switch_allowed_for_entry",
    "filter_switch_candidates",
    "explain_switch_decision",
]
