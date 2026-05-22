"""Phase 26 - Bilingual Voice Memory Schema.

Defines bounded, JSON-serializable voice-memory and continuity data
structures. No persistent writes here - this module only declares the
shape, defaults, and validators.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


RECENT_LIST_HARD_CAP = 20


SUPPORTED_PREFERENCE_KEYS = (
    "preferred_language_mode",
    "preferred_spoken_mode",
    "preferred_code_switch_density",
    "preferred_formality",
    "preferred_turn_style",
    "user_is_practicing_language",
)


SUPPORTED_CONTINUITY_MODES = (
    "session_only",
    "explicit_local_with_consent",
)


SUPPORTED_LANGUAGE_MODES = (
    "english_only", "russian_only", "mixed_en_ru",
    "english_with_russian_terms", "russian_with_english_terms",
    "code_switch_sentence_level", "code_switch_phrase_level",
    "code_switch_word_level", "transliterated_russian", "unknown",
    "auto",
)


SUPPORTED_SPOKEN_MODES = (
    "english_only", "russian_only", "mixed_en_ru",
    "english_with_russian_terms", "russian_with_english_terms",
)


SUPPORTED_FORMALITY = (
    "casual", "warm", "standard", "professional", "teacher",
    "technical", "academic", "unknown",
)


SUPPORTED_TURN_STYLES = (
    "concise", "balanced", "explanatory", "warm_friend",
    "teacher", "translation_help", "bilingual_practice", "unknown",
)


SUPPORTED_PRACTICE_LANGUAGES = ("en", "ru", "mixed", "none")


# Default state fields that must NEVER be auto-populated with sensitive
# personal facts. Documented here so reviewers can audit at a glance.
PRIVACY_RULES = {
    "session_only_by_default": True,
    "persistence_requires_explicit_consent_marker": True,
    "no_full_transcripts_by_default": True,
    "forbidden_personal_attribute_buckets": [
        "medical", "political", "religious", "identity", "legal",
        "intimate", "biometric", "financial_identity", "location_history",
    ],
    "allowed_state_buckets": [
        "language_preference", "code_switch_preference",
        "formality_preference", "turn_style_preference",
        "practice_language_preference", "session_summary",
        "active_corrections", "safety_flags_seen_counts",
    ],
}


def get_memory_privacy_rules() -> dict[str, Any]:
    return dict(PRIVACY_RULES)


def get_supported_preference_keys() -> list[str]:
    return list(SUPPORTED_PREFERENCE_KEYS)


def get_supported_continuity_modes() -> list[str]:
    return list(SUPPORTED_CONTINUITY_MODES)


def _new_session_id() -> str:
    return f"vses_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def create_empty_voice_memory_state(session_id: Optional[str] = None
                                    ) -> dict[str, Any]:
    now = time.time()
    return {
        "session_id": session_id or _new_session_id(),
        "created_at": now,
        "updated_at": now,
        "preferred_language_mode": "auto",
        "preferred_spoken_mode": "auto",
        "preferred_code_switch_density": None,  # let policy decide
        "preferred_formality": "unknown",
        "preferred_turn_style": "unknown",
        "user_is_practicing_language": "none",
        "last_detected_language_mode": None,
        "last_chosen_response_mode": None,
        "last_spoken_render_mode": None,
        "recent_language_modes": [],
        "recent_code_switch_density": [],
        "recent_turn_types": [],
        "recent_corrections": [],
        "emotional_tone_trend": "steady",
        "personality_continuity_score": None,
        "safety_flags_seen": {},
        "memory_scope": "session_only",
        "consent_required_for_persistence": True,
        "notes": "",
    }


def get_voice_memory_schema() -> dict[str, Any]:
    return {
        "version": "phase26.v1",
        "fields": list(create_empty_voice_memory_state().keys()),
        "supported_preference_keys": list(SUPPORTED_PREFERENCE_KEYS),
        "supported_continuity_modes": list(SUPPORTED_CONTINUITY_MODES),
        "supported_language_modes": list(SUPPORTED_LANGUAGE_MODES),
        "supported_spoken_modes": list(SUPPORTED_SPOKEN_MODES),
        "supported_formality": list(SUPPORTED_FORMALITY),
        "supported_turn_styles": list(SUPPORTED_TURN_STYLES),
        "supported_practice_languages": list(SUPPORTED_PRACTICE_LANGUAGES),
        "recent_list_hard_cap": RECENT_LIST_HARD_CAP,
        "privacy_rules": PRIVACY_RULES,
        "notes": [
            "Session-only by default. Persistence requires explicit consent.",
            "Recent lists hard-clamped to 20.",
            "No full transcript storage; only preferences + summary.",
            "No sensitive personal-attribute storage.",
        ],
    }


def clamp_voice_memory_state(state: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(state, dict):
        return create_empty_voice_memory_state()
    out = dict(state)
    for key in ("recent_language_modes", "recent_code_switch_density",
                "recent_turn_types", "recent_corrections"):
        v = out.get(key) or []
        if not isinstance(v, list):
            v = []
        out[key] = v[-RECENT_LIST_HARD_CAP:]
    if out.get("memory_scope") not in SUPPORTED_CONTINUITY_MODES:
        out["memory_scope"] = "session_only"
    if out.get("preferred_language_mode") not in SUPPORTED_LANGUAGE_MODES:
        out["preferred_language_mode"] = "auto"
    if out.get("preferred_spoken_mode") not in SUPPORTED_SPOKEN_MODES \
            and out.get("preferred_spoken_mode") != "auto":
        out["preferred_spoken_mode"] = "auto"
    if out.get("preferred_formality") not in SUPPORTED_FORMALITY:
        out["preferred_formality"] = "unknown"
    if out.get("preferred_turn_style") not in SUPPORTED_TURN_STYLES:
        out["preferred_turn_style"] = "unknown"
    if out.get("user_is_practicing_language") not in SUPPORTED_PRACTICE_LANGUAGES:
        out["user_is_practicing_language"] = "none"
    sf = out.get("safety_flags_seen")
    if not isinstance(sf, dict):
        out["safety_flags_seen"] = {}
    return out


def validate_voice_memory_state(state: Any) -> dict[str, Any]:
    if not isinstance(state, dict):
        return {"ok": False, "reason": "state_not_dict"}
    required = ("session_id", "created_at", "updated_at", "memory_scope",
                "preferred_language_mode", "recent_language_modes")
    missing = [f for f in required if f not in state]
    if missing:
        return {"ok": False, "reason": "missing_required",
                "missing": missing}
    if state["memory_scope"] not in SUPPORTED_CONTINUITY_MODES:
        return {"ok": False,
                "reason": f"unsupported_memory_scope: {state['memory_scope']!r}"}
    # Forbidden personal-attribute keys absent
    for k in state.keys():
        kl = k.lower()
        for bucket in PRIVACY_RULES["forbidden_personal_attribute_buckets"]:
            if bucket in kl:
                return {"ok": False,
                        "reason": f"forbidden_personal_attribute_field: {k!r}"}
    # JSON-serializable
    try:
        json.dumps(state, ensure_ascii=False, default=str)
    except Exception as e:
        return {"ok": False, "reason": f"not_json_serializable: {e}"}
    # Recent list caps
    for key in ("recent_language_modes", "recent_code_switch_density",
                "recent_turn_types", "recent_corrections"):
        v = state.get(key) or []
        if not isinstance(v, list):
            return {"ok": False, "reason": f"{key}_not_list"}
        if len(v) > RECENT_LIST_HARD_CAP:
            return {"ok": False,
                    "reason": f"{key}_exceeds_cap",
                    "limit": RECENT_LIST_HARD_CAP,
                    "actual": len(v)}
    return {"ok": True}


def write_voice_memory_schema_report(report: dict[str, Any],
                                     output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "RECENT_LIST_HARD_CAP", "SUPPORTED_PREFERENCE_KEYS",
    "SUPPORTED_CONTINUITY_MODES", "SUPPORTED_LANGUAGE_MODES",
    "SUPPORTED_SPOKEN_MODES", "SUPPORTED_FORMALITY",
    "SUPPORTED_TURN_STYLES", "SUPPORTED_PRACTICE_LANGUAGES",
    "PRIVACY_RULES",
    "get_voice_memory_schema",
    "create_empty_voice_memory_state",
    "validate_voice_memory_state",
    "clamp_voice_memory_state",
    "get_supported_preference_keys",
    "get_supported_continuity_modes",
    "get_memory_privacy_rules",
    "write_voice_memory_schema_report",
]
