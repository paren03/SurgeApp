"""Shared coverage taxonomy + register/safety vocabularies.

Used by both English (Track A) and Russian (Track B) sovereign stacks to keep
pack manifests, ingestion validators, and policy enforcement aligned.

Pure module — no I/O, no side effects, no dependencies.
"""

from __future__ import annotations

from typing import Iterable

COVERAGE_CATEGORIES: tuple[str, ...] = (
    "core_vocabulary",
    "slang_street_talk",
    "idioms_phrases",
    "professions_jobs",
    "trades_construction",
    "science_math",
    "medicine_health",
    "law_government",
    "business_finance",
    "coding_technology",
    "poetry_literary",
    "philosophy_abstract",
    "art_music_culture",
    "history_geography",
    "psychology_education",
    "mechanics_transportation",
    "food_home_daily_life",
    "regional_dialect",
    "formal_informal_speech",
    "voice_personality",
    "recognition_only_sensitive",
)

REGISTER_TAGS: tuple[str, ...] = (
    "standard",
    "formal",
    "informal",
    "slang",
    "street",
    "vulgar",
    "offensive",
    "regional",
    "technical",
    "academic",
    "poetic",
    "philosophical",
    "professional",
    "construction",
    "coding",
    "medical",
    "legal",
    "business",
    "teacher",
    "voice_safe",
    "recognition_only",
    "do_not_use_unprompted",
)

SAFETY_TAGS: tuple[str, ...] = (
    "vulgar",
    "offensive",
    "recognition_only",
    "do_not_use_unprompted",
)

_COV_SET = frozenset(COVERAGE_CATEGORIES)
_REG_SET = frozenset(REGISTER_TAGS)
_SAF_SET = frozenset(SAFETY_TAGS)

_ALIASES_COVERAGE = {
    "core": "core_vocabulary",
    "slang": "slang_street_talk",
    "street": "slang_street_talk",
    "idiom": "idioms_phrases",
    "idioms": "idioms_phrases",
    "profession": "professions_jobs",
    "job": "professions_jobs",
    "trade": "trades_construction",
    "construction": "trades_construction",
    "science": "science_math",
    "math": "science_math",
    "medicine": "medicine_health",
    "health": "medicine_health",
    "law": "law_government",
    "government": "law_government",
    "business": "business_finance",
    "finance": "business_finance",
    "coding": "coding_technology",
    "tech": "coding_technology",
    "poetry": "poetry_literary",
    "literary": "poetry_literary",
    "philosophy": "philosophy_abstract",
    "art": "art_music_culture",
    "music": "art_music_culture",
    "culture": "art_music_culture",
    "history": "history_geography",
    "geography": "history_geography",
    "psychology": "psychology_education",
    "education": "psychology_education",
    "mechanics": "mechanics_transportation",
    "transport": "mechanics_transportation",
    "food": "food_home_daily_life",
    "home": "food_home_daily_life",
    "daily": "food_home_daily_life",
    "regional": "regional_dialect",
    "dialect": "regional_dialect",
    "voice": "voice_personality",
    "personality": "voice_personality",
    "recognition_only_sensitive_terms": "recognition_only_sensitive",
}

_ALIASES_REGISTER = {
    "neutral": "standard",
    "casual": "informal",
    "polite": "formal",
    "rude": "offensive",
    "swear": "vulgar",
    "curse": "vulgar",
    "trade": "construction",
    "dev": "coding",
    "doctor": "medical",
    "law": "legal",
    "biz": "business",
    "tutor": "teacher",
    "safe_for_voice": "voice_safe",
    "voice": "voice_safe",
    "noprompt": "do_not_use_unprompted",
    "no_prompt": "do_not_use_unprompted",
}


def _norm(value: str) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def normalize_coverage_category(value: str) -> str | None:
    v = _norm(value)
    if not v:
        return None
    if v in _COV_SET:
        return v
    return _ALIASES_COVERAGE.get(v)


def normalize_register_tag(value: str) -> str | None:
    v = _norm(value)
    if not v:
        return None
    if v in _REG_SET:
        return v
    return _ALIASES_REGISTER.get(v)


def normalize_safety_tag(value: str) -> str | None:
    v = _norm(value)
    if not v:
        return None
    return v if v in _SAF_SET else None


def validate_coverage_categories(values: Iterable[str]) -> dict[str, list[str]]:
    """Return {'accepted': [...canonical...], 'rejected': [...raw...]}."""
    accepted: list[str] = []
    rejected: list[str] = []
    seen: set[str] = set()
    for v in values or []:
        canon = normalize_coverage_category(v) if isinstance(v, str) else None
        if canon and canon not in seen:
            seen.add(canon)
            accepted.append(canon)
        else:
            rejected.append(str(v))
    return {"accepted": accepted, "rejected": rejected}


def validate_register_tags(values: Iterable[str]) -> dict[str, list[str]]:
    accepted: list[str] = []
    rejected: list[str] = []
    seen: set[str] = set()
    for v in values or []:
        canon = normalize_register_tag(v) if isinstance(v, str) else None
        if canon and canon not in seen:
            seen.add(canon)
            accepted.append(canon)
        else:
            rejected.append(str(v))
    return {"accepted": accepted, "rejected": rejected}


def validate_safety_tags(values: Iterable[str]) -> dict[str, list[str]]:
    accepted: list[str] = []
    rejected: list[str] = []
    seen: set[str] = set()
    for v in values or []:
        canon = normalize_safety_tag(v) if isinstance(v, str) else None
        if canon and canon not in seen:
            seen.add(canon)
            accepted.append(canon)
        else:
            rejected.append(str(v))
    return {"accepted": accepted, "rejected": rejected}


__all__ = [
    "COVERAGE_CATEGORIES",
    "REGISTER_TAGS",
    "SAFETY_TAGS",
    "normalize_coverage_category",
    "normalize_register_tag",
    "normalize_safety_tag",
    "validate_coverage_categories",
    "validate_register_tags",
    "validate_safety_tags",
]
