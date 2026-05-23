"""Russian Sovereign Language Stack — response quality scorer.

Bounded, advisory only. Never rewrites a response automatically. Returns
suggestions, scores in [0,1], and explanatory reasons. Honest about
heuristic uncertainty — does not claim certainty.
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

import russian_personality_layer as personality

FEATURE_FLAG = "LUNA_RUSSIAN_STACK"

DEFAULT_LIMIT = 5
HARD_MAX_LIMIT = 25

_CYR_RE = re.compile(r"[Ѐ-ӿԀ-ԯ]")
_LAT_WORD_RE = re.compile(r"[A-Za-z]{4,}")
_LONG_SENTENCE_WORDS = 28

_TOO_FORMAL = (
    "осуществить", "осуществл", "произвести", "произвед", "изготовить",
    "изготовл", "являются", "является", "данный",  "указанный", "вышеуказан",
    "нижеуказан", "ввиду", "в связи с тем что",
)

_TOO_ACADEMIC = (
    "вышеприведенн", "ниже",  "вышеперечисл", "коррелирует", "коррелир",
    "детерминирован", "эпистемологи", "имманентн",
)


def _flag_enabled() -> bool:
    return os.environ.get(FEATURE_FLAG, "").strip() in ("1", "true", "yes", "on")


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


def detect_translation_artifacts(text: str) -> dict[str, Any]:
    """Delegate to personality layer for the canonical artifact list."""
    arts = personality.avoid_translation_artifacts(text)
    return {"count": arts["found"], "artifacts": arts["artifacts"],
            "confidence": 0.55}


def score_native_feel(text: str) -> dict[str, Any]:
    """Heuristic native-feel score in [0,1]. Honest about uncertainty."""
    if not isinstance(text, str) or not text.strip():
        return {"score": 0.0, "reasons": ["empty"], "confidence": 0.0}
    sample = text[:4000]
    total = len(sample)
    cyr_chars = len(_CYR_RE.findall(sample))
    cyr_ratio = cyr_chars / total if total else 0.0
    latin_words = _LAT_WORD_RE.findall(sample)
    arts = personality.avoid_translation_artifacts(sample)

    score = 0.6
    reasons: list[str] = []
    if cyr_ratio < 0.50:
        score -= 0.25
        reasons.append(f"low_cyrillic_ratio:{round(cyr_ratio,2)}")
    if len(latin_words) > 3:
        score -= 0.10
        reasons.append(f"many_latin_words:{len(latin_words)}")
    if arts["found"] > 0:
        score -= min(0.10 * arts["found"], 0.40)
        reasons.append(f"translation_artifacts:{arts['found']}")
    if 0.80 <= cyr_ratio <= 1.0 and not latin_words:
        score += 0.15
        reasons.append("clean_cyrillic_text")
    score = max(0.0, min(1.0, score))
    return {"score": round(score, 3), "reasons": reasons[:10],
            "confidence": 0.35, "cyrillic_ratio": round(cyr_ratio, 3)}


def score_clarity_ru(text: str) -> dict[str, Any]:
    if not isinstance(text, str) or not text.strip():
        return {"score": 0.0, "reasons": ["empty"], "confidence": 0.0}
    sentences = [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]
    if not sentences:
        return {"score": 0.0, "reasons": ["no_sentences"], "confidence": 0.0}
    long_count = sum(1 for s in sentences
                     if len(re.findall(r"\S+", s)) > _LONG_SENTENCE_WORDS)
    long_ratio = long_count / len(sentences)
    reasons: list[str] = []
    score = 0.7
    if long_ratio > 0.5:
        score -= 0.30
        reasons.append(f"long_sentence_ratio:{round(long_ratio,2)}")
    elif long_ratio > 0.2:
        score -= 0.10
        reasons.append(f"some_long_sentences:{long_count}")
    avg_words = sum(len(re.findall(r"\S+", s)) for s in sentences) / len(sentences)
    if avg_words > 24:
        score -= 0.10
        reasons.append(f"high_avg_words:{round(avg_words,1)}")
    if 6 <= avg_words <= 18:
        score += 0.10
        reasons.append(f"clear_avg_words:{round(avg_words,1)}")
    score = max(0.0, min(1.0, score))
    return {"score": round(score, 3), "reasons": reasons[:10],
            "confidence": 0.35,
            "sentence_count": len(sentences),
            "avg_words_per_sentence": round(avg_words, 2)}


def score_register_fit(text: str, mode: str = "conversation") -> dict[str, Any]:
    if not isinstance(text, str) or not text.strip():
        return {"score": 0.0, "reasons": ["empty"], "confidence": 0.0, "mode": mode}
    rules = personality.get_russian_style_rules(mode)
    text_l = text.lower()
    reasons: list[str] = []
    too_formal_hits = sum(1 for tok in _TOO_FORMAL if tok in text_l)
    academic_hits = sum(1 for tok in _TOO_ACADEMIC if tok in text_l)
    max_formality = float(rules.get("max_formality", 0.5))

    score = 0.7
    if too_formal_hits and max_formality < 0.7:
        score -= 0.20
        reasons.append(f"too_formal_for_mode:{too_formal_hits}")
    if academic_hits and rules["mode"] not in ("teacher", "curriculum", "professional"):
        score -= 0.20
        reasons.append(f"too_academic_for_mode:{academic_hits}")
    if rules["mode"] == "concise":
        sentences = [s for s in re.split(r"[.!?]+", text) if s.strip()]
        if sentences and sum(len(re.findall(r"\S+", s)) for s in sentences) / len(sentences) > 14:
            score -= 0.15
            reasons.append("sentences_too_long_for_concise")
    score = max(0.0, min(1.0, score))
    return {"score": round(score, 3), "reasons": reasons[:10],
            "confidence": 0.30, "mode": rules["mode"]}


def suggest_russian_rewrites(
    text: str,
    mode: str = "conversation",
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """Return up to `limit` advisory rewrite suggestions. Never auto-rewrites."""
    if not isinstance(text, str) or not text.strip():
        return []
    n = _clamp(limit)
    out: list[dict[str, Any]] = []
    text_l = text.lower()
    rules = personality.get_russian_style_rules(mode)

    arts = personality.avoid_translation_artifacts(text)
    for a in arts["artifacts"][:n]:
        out.append({
            "type": "translation_artifact",
            "from_phrase": a["phrase"],
            "suggestion": f"Замени: {a['reason']}",
            "confidence": 0.55,
        })

    if len(out) < n:
        for tok in _TOO_FORMAL:
            if tok in text_l and len(out) < n:
                out.append({
                    "type": "register_too_formal",
                    "from_phrase": tok,
                    "suggestion": f"В режиме {rules['mode']} лучше выбрать простой эквивалент.",
                    "confidence": 0.40,
                })

    if len(out) < n:
        sentences = re.split(r"(?<=[.!?])\s+", text)
        for s in sentences:
            if len(re.findall(r"\S+", s)) > rules.get("max_words_per_sentence", 24):
                out.append({
                    "type": "long_sentence",
                    "from_phrase": s[:80] + ("…" if len(s) > 80 else ""),
                    "suggestion": "Разбей на 2 предложения.",
                    "confidence": 0.45,
                })
                if len(out) >= n:
                    break

    return out[:n]


def quality_check_ru(text: str, mode: str = "conversation") -> dict[str, Any]:
    """Bundled quality report. Bounded; no rewrites applied."""
    native = score_native_feel(text)
    clarity = score_clarity_ru(text)
    register = score_register_fit(text, mode=mode)
    arts = detect_translation_artifacts(text)
    weights = (0.45, 0.30, 0.25)
    overall = (native["score"] * weights[0]
               + clarity["score"] * weights[1]
               + register["score"] * weights[2])
    return {
        "mode": register.get("mode", mode),
        "scores": {
            "native_feel": native["score"],
            "clarity": clarity["score"],
            "register_fit": register["score"],
            "overall": round(overall, 3),
        },
        "reasons": {
            "native_feel": native["reasons"],
            "clarity": clarity["reasons"],
            "register_fit": register["reasons"],
        },
        "translation_artifacts": arts["artifacts"][:10],
        "suggested_rewrites": suggest_russian_rewrites(text, mode=mode, limit=5),
        "confidence": 0.35,
        "rewrites_applied": False,
    }


__all__ = [
    "FEATURE_FLAG",
    "detect_translation_artifacts",
    "score_native_feel",
    "score_clarity_ru",
    "score_register_fit",
    "suggest_russian_rewrites",
    "quality_check_ru",
]
