"""Russian Sovereign Language Stack — language detection / routing.

Standalone Luna subsystem. Does NOT touch Program S, Luna tiers/probes/
attestation, or worker orchestration. Gated by LUNA_RUSSIAN_STACK.
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

FEATURE_FLAG = "LUNA_RUSSIAN_STACK"

DEFAULT_LIMIT = 25
HARD_MAX_LIMIT = 200

_CYRILLIC_RE = re.compile(r"[Ѐ-ӿԀ-ԯ]")
_LATIN_RE = re.compile(r"[A-Za-z]")
_RUSSIAN_REQUEST_PATTERNS = (
    re.compile(r"\b(in|to|using)\s+russian\b", re.IGNORECASE),
    re.compile(r"\bпо[- ]русски\b", re.IGNORECASE),
    re.compile(r"\bна\s+русском\b", re.IGNORECASE),
    re.compile(r"\bspeak\s+russian\b", re.IGNORECASE),
    re.compile(r"\brespond\s+in\s+russian\b", re.IGNORECASE),
)

_VALID_MODES = (
    "conversation", "teacher", "technical", "coding",
    "curriculum", "professional", "warm_friend", "concise",
)


def _flag_enabled() -> bool:
    v = os.environ.get(FEATURE_FLAG, "")
    return v.strip() in ("1", "true", "yes", "on")


def _disabled_result(reason: str = "feature_flag_off") -> dict[str, Any]:
    return {"enabled": False, "reason": reason}


def _clamp(limit: Optional[int]) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    try:
        n = int(limit)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if n <= 0:
        return DEFAULT_LIMIT
    return min(n, HARD_MAX_LIMIT)


def detect_russian_text(text: str) -> dict[str, Any]:
    """Per-character Cyrillic ratio + flags. Pure-text analysis, no I/O."""
    if not isinstance(text, str) or not text:
        return {
            "is_russian": False, "has_cyrillic": False, "has_latin": False,
            "cyrillic_chars": 0, "latin_chars": 0, "total_chars": 0,
            "cyrillic_ratio": 0.0, "is_mixed": False,
        }
    sample = text[:4000]
    cyr = _CYRILLIC_RE.findall(sample)
    lat = _LATIN_RE.findall(sample)
    total = len(sample)
    ratio = len(cyr) / total if total else 0.0
    has_cyr = len(cyr) > 0
    has_lat = len(lat) > 0
    is_mixed = has_cyr and has_lat and 0.10 <= ratio <= 0.90
    is_russian = ratio >= 0.30
    return {
        "is_russian": is_russian,
        "has_cyrillic": has_cyr,
        "has_latin": has_lat,
        "cyrillic_chars": len(cyr),
        "latin_chars": len(lat),
        "total_chars": total,
        "cyrillic_ratio": round(ratio, 4),
        "is_mixed": is_mixed,
    }


def detect_language_mode(text: str) -> str:
    """Return one of: 'russian', 'mixed', 'english_with_russian_request', 'english', 'empty'."""
    if not isinstance(text, str) or not text.strip():
        return "empty"
    info = detect_russian_text(text)
    if info["is_russian"] and not info["is_mixed"]:
        return "russian"
    if info["is_mixed"]:
        return "mixed"
    for pat in _RUSSIAN_REQUEST_PATTERNS:
        if pat.search(text):
            return "english_with_russian_request"
    return "english"


def should_use_russian_stack(
    text: str,
    user_requested_language: Optional[str] = None,
) -> dict[str, Any]:
    """Bounded routing decision. Always returns a dict, never raises."""
    if not _flag_enabled():
        return {"use_russian": False, "reason": "feature_flag_off",
                "mode_detected": None}
    if isinstance(user_requested_language, str):
        if user_requested_language.strip().lower() in ("ru", "russian", "русский"):
            return {"use_russian": True, "reason": "user_requested",
                    "mode_detected": detect_language_mode(text or "")}
    mode = detect_language_mode(text or "")
    if mode in ("russian", "mixed", "english_with_russian_request"):
        return {"use_russian": True, "reason": mode, "mode_detected": mode}
    return {"use_russian": False, "reason": "no_russian_signal",
            "mode_detected": mode}


def route_russian_context(
    text: str,
    mode: str = "conversation",
    limit: int = DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Return bounded routing metadata. Never invokes downstream stores."""
    if not _flag_enabled():
        return _disabled_result()
    n = _clamp(limit)
    m = mode.strip().lower() if isinstance(mode, str) else "conversation"
    if m not in _VALID_MODES:
        m = "conversation"
    decision = should_use_russian_stack(text)
    info = detect_russian_text(text or "")
    tokens = re.findall(r"[Ѐ-ӿԀ-ԯ][Ѐ-ӿԀ-ԯ\-]{0,31}",
                        text or "")
    seen: set[str] = set()
    uniq: list[str] = []
    for t in tokens:
        tl = t.lower()
        if tl in seen:
            continue
        seen.add(tl)
        uniq.append(tl)
        if len(uniq) >= n:
            break
    return {
        "enabled": True,
        "mode": m,
        "limit": n,
        "routing_decision": decision,
        "detection": info,
        "sample_tokens": uniq,
        "token_count": len(uniq),
    }


__all__ = [
    "FEATURE_FLAG",
    "detect_russian_text",
    "detect_language_mode",
    "should_use_russian_stack",
    "route_russian_context",
]
