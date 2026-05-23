"""Phase 23 - Bilingual Conversation State.

Plain-dict utility to track local, non-persistent conversation language
preferences. No daemon. No persistent writes. No main runtime integration.
"""

from __future__ import annotations

import time
from typing import Any, Optional


_VALID_OUTPUT_MODES = (
    "english_only", "russian_only", "mixed_en_ru",
    "english_with_russian_terms", "russian_with_english_terms",
    "auto",
)


def create_conversation_language_state() -> dict[str, Any]:
    return {
        "preferred_output_mode": "auto",
        "last_user_language_mode": None,
        "last_response_language_mode": None,
        "english_ratio": 0.0,
        "russian_ratio": 0.0,
        "code_switch_frequency": 0.0,
        "user_requested_mix": False,
        "user_requested_english": False,
        "user_requested_russian": False,
        "turn_count": 0,
        "last_updated": time.time(),
    }


def _ewma(prev: float, new: float, alpha: float = 0.4) -> float:
    return round(alpha * float(new) + (1.0 - alpha) * float(prev), 4)


def update_language_state(state: dict[str, Any],
                          user_text: str,
                          detected_mode: dict[str, Any] | str,
                          chosen_response_mode: str) -> dict[str, Any]:
    if not isinstance(state, dict):
        state = create_conversation_language_state()
    detected_obj: dict[str, Any] = (detected_mode
                                    if isinstance(detected_mode, dict) else
                                    {"mode": detected_mode, "ratio": {},
                                     "transitions": {}, "preference": {}})
    state["last_user_language_mode"] = detected_obj.get("mode")
    state["last_response_language_mode"] = chosen_response_mode
    ratio = detected_obj.get("ratio") or {}
    state["english_ratio"] = _ewma(state.get("english_ratio") or 0.0,
                                    float(ratio.get("english_ratio") or 0.0))
    state["russian_ratio"] = _ewma(state.get("russian_ratio") or 0.0,
                                    float(ratio.get("russian_ratio") or 0.0))
    trans = detected_obj.get("transitions") or {}
    state["code_switch_frequency"] = _ewma(
        state.get("code_switch_frequency") or 0.0,
        float(trans.get("transitions_per_sentence") or 0.0))
    pref = detected_obj.get("preference") or {}
    state["user_requested_mix"] = bool(pref.get("requested_mix"))
    state["user_requested_english"] = bool(pref.get("requested_english"))
    state["user_requested_russian"] = bool(pref.get("requested_russian"))
    state["turn_count"] = int(state.get("turn_count") or 0) + 1
    state["last_updated"] = time.time()
    return state


def get_preferred_language_mix(state: dict[str, Any]) -> str:
    if not isinstance(state, dict):
        return "auto"
    return str(state.get("preferred_output_mode") or "auto")


def set_preferred_language_mix(state: dict[str, Any], mode: str
                               ) -> dict[str, Any]:
    if not isinstance(state, dict):
        state = create_conversation_language_state()
    if mode in _VALID_OUTPUT_MODES:
        state["preferred_output_mode"] = mode
        state["last_updated"] = time.time()
    return state


def reset_language_state(state: dict[str, Any]) -> dict[str, Any]:
    return create_conversation_language_state()


def summarize_language_state(state: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(state, dict):
        return {"ok": False, "reason": "state_not_dict"}
    return {"ok": True,
            "preferred_output_mode": state.get("preferred_output_mode"),
            "last_user_language_mode": state.get("last_user_language_mode"),
            "last_response_language_mode":
                state.get("last_response_language_mode"),
            "english_ratio": state.get("english_ratio"),
            "russian_ratio": state.get("russian_ratio"),
            "code_switch_frequency": state.get("code_switch_frequency"),
            "user_requested_mix": state.get("user_requested_mix"),
            "user_requested_english": state.get("user_requested_english"),
            "user_requested_russian": state.get("user_requested_russian"),
            "turn_count": state.get("turn_count"),
            "last_updated": state.get("last_updated")}


__all__ = [
    "create_conversation_language_state",
    "update_language_state",
    "get_preferred_language_mix",
    "set_preferred_language_mix",
    "reset_language_state",
    "summarize_language_state",
]
