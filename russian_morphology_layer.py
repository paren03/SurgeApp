"""Russian Sovereign Language Stack — morphology hints.

Returns bounded morphology metadata. Uses pymorphy3 (or pymorphy2) when
installed; falls back to rule-based heuristics that explicitly mark uncertainty.

Install for richer analysis:
    pip install pymorphy3
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

FEATURE_FLAG = "LUNA_RUSSIAN_STACK"

DEFAULT_LIMIT = 10
HARD_MAX_LIMIT = 100

_CYR_WORD = re.compile(r"[Ѐ-ӿԀ-ԯ][Ѐ-ӿԀ-ԯ\-]*")
_VOWELS = set("аеёиоуыэюя")

_FEMININE_ENDINGS = ("а", "я", "ия", "ость", "ия")
_NEUTER_ENDINGS = ("о", "е", "ё", "мя")
_VERB_INF_ENDINGS = ("ть", "ться", "ти")
_ADJ_ENDINGS = ("ый", "ий", "ой", "ая", "яя", "ое", "ее", "ые", "ие")
_ADVERB_ENDINGS = ("о", "е")

_CASE_PREP_HINTS = {
    "в": ["accusative", "prepositional"],
    "на": ["accusative", "prepositional"],
    "под": ["accusative", "instrumental"],
    "над": ["instrumental"],
    "перед": ["instrumental"],
    "за": ["accusative", "instrumental"],
    "из": ["genitive"],
    "от": ["genitive"],
    "до": ["genitive"],
    "у": ["genitive"],
    "без": ["genitive"],
    "к": ["dative"],
    "по": ["dative"],
    "с": ["genitive", "instrumental"],
    "о": ["prepositional"],
    "об": ["prepositional"],
    "при": ["prepositional"],
}

_PLURAL_ENDINGS = ("ы", "и", "а", "я")

_MORPH = None
_MORPH_TRIED = False


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


def _get_morph():
    global _MORPH, _MORPH_TRIED
    if _MORPH_TRIED:
        return _MORPH
    _MORPH_TRIED = True
    try:
        import pymorphy3
        _MORPH = pymorphy3.MorphAnalyzer()
        return _MORPH
    except Exception:
        pass
    try:
        import pymorphy2
        _MORPH = pymorphy2.MorphAnalyzer()
        return _MORPH
    except Exception:
        _MORPH = None
        return None


def dependency_status() -> dict[str, Any]:
    morph = _get_morph()
    return {
        "pymorphy_available": morph is not None,
        "engine": type(morph).__module__ if morph else None,
        "install_hint": "pip install pymorphy3",
    }


def normalize_russian_word(word: str) -> str:
    """Lowercase, normalize ё→е (only as a softening fallback), strip punct."""
    if not isinstance(word, str):
        return ""
    w = word.strip().lower()
    return re.sub(r"[^\w\-]", "", w, flags=re.UNICODE)


def guess_lemma(word: str) -> dict[str, Any]:
    """Return {'lemma': str, 'confidence': float, 'source': 'pymorphy'|'heuristic'|'empty'}."""
    w = normalize_russian_word(word)
    if not w:
        return {"lemma": "", "confidence": 0.0, "source": "empty"}
    morph = _get_morph()
    if morph is not None:
        try:
            p = morph.parse(w)
            if p:
                best = p[0]
                return {
                    "lemma": best.normal_form,
                    "confidence": float(getattr(best, "score", 0.5)),
                    "source": "pymorphy",
                }
        except Exception:
            pass
    lemma = w
    for end in ("ться", "тся", "ого", "его", "ому", "ему", "ыми", "ими",
                "ами", "ями", "ах", "ях", "ов", "ев", "ей", "ям", "ам",
                "ой", "ей", "ы", "и", "у", "ю", "а", "я", "е"):
        if lemma.endswith(end) and len(lemma) > len(end) + 2:
            lemma = lemma[: -len(end)]
            break
    return {"lemma": lemma, "confidence": 0.25, "source": "heuristic"}


def detect_part_of_speech(word: str) -> dict[str, Any]:
    w = normalize_russian_word(word)
    if not w:
        return {"pos": "", "confidence": 0.0, "source": "empty"}
    morph = _get_morph()
    if morph is not None:
        try:
            p = morph.parse(w)
            if p:
                tag = str(p[0].tag.POS) if p[0].tag.POS else ""
                return {"pos": tag.lower(), "confidence": float(getattr(p[0], "score", 0.5)),
                        "source": "pymorphy"}
        except Exception:
            pass
    pos = "unknown"
    if w.endswith(_VERB_INF_ENDINGS):
        pos = "verb"
    elif w.endswith(_ADJ_ENDINGS) and len(w) > 4:
        pos = "adjective"
    elif w.endswith(("ость", "ние", "тие", "ство")):
        pos = "noun"
    elif w.endswith(_ADVERB_ENDINGS) and len(w) <= 6:
        pos = "adverb_or_noun"
    elif any(w.endswith(e) for e in ("а", "я", "о", "е", "ь")) or w[-1] not in _VOWELS:
        pos = "noun"
    return {"pos": pos, "confidence": 0.20, "source": "heuristic"}


def _has_pymorphy_tag(text: str, tag_name: str, values: list[str]) -> dict[str, Any]:
    morph = _get_morph()
    if morph is None:
        return {"hint": None, "confidence": 0.0, "source": "heuristic"}
    found: dict[str, int] = {}
    try:
        for tok in _CYR_WORD.findall(text or "")[:HARD_MAX_LIMIT]:
            p = morph.parse(tok.lower())
            if not p:
                continue
            tag = getattr(p[0].tag, tag_name, None)
            if tag is None:
                continue
            key = str(tag).lower()
            if key in values or any(v in key for v in values):
                found[key] = found.get(key, 0) + 1
    except Exception:
        return {"hint": None, "confidence": 0.0, "source": "heuristic"}
    if not found:
        return {"hint": None, "confidence": 0.0, "source": "pymorphy"}
    top = max(found.items(), key=lambda kv: kv[1])
    return {"hint": top[0], "counts": found, "confidence": 0.6, "source": "pymorphy"}


def detect_case_hint(phrase: str) -> dict[str, Any]:
    if not isinstance(phrase, str) or not phrase.strip():
        return {"hint": None, "confidence": 0.0, "source": "empty"}
    morph_result = _has_pymorphy_tag(
        phrase, "case",
        ["nomn", "gent", "datv", "accs", "ablt", "loct",
         "nominative", "genitive", "dative", "accusative", "instrumental", "prepositional"],
    )
    if morph_result.get("source") == "pymorphy" and morph_result.get("hint"):
        return morph_result
    tokens = [t.lower() for t in re.findall(r"[а-яё]+", phrase, flags=re.IGNORECASE)]
    for tok in tokens[:-1]:
        if tok in _CASE_PREP_HINTS:
            cases = _CASE_PREP_HINTS[tok]
            return {"hint": cases[0], "candidates": cases,
                    "trigger_preposition": tok, "confidence": 0.30,
                    "source": "heuristic"}
    return {"hint": None, "confidence": 0.0, "source": "heuristic"}


def detect_number_hint(phrase: str) -> dict[str, Any]:
    if not isinstance(phrase, str) or not phrase.strip():
        return {"hint": None, "confidence": 0.0, "source": "empty"}
    morph_result = _has_pymorphy_tag(
        phrase, "number",
        ["sing", "plur", "singular", "plural"],
    )
    if morph_result.get("source") == "pymorphy" and morph_result.get("hint"):
        return morph_result
    tokens = re.findall(r"[а-яё]+", phrase.lower(), flags=re.IGNORECASE)
    plural_hits = sum(1 for t in tokens if t.endswith(_PLURAL_ENDINGS) and len(t) > 3)
    if plural_hits >= 2:
        return {"hint": "plur", "confidence": 0.25, "source": "heuristic"}
    if tokens:
        return {"hint": "sing", "confidence": 0.20, "source": "heuristic"}
    return {"hint": None, "confidence": 0.0, "source": "heuristic"}


def detect_gender_hint(phrase: str) -> dict[str, Any]:
    if not isinstance(phrase, str) or not phrase.strip():
        return {"hint": None, "confidence": 0.0, "source": "empty"}
    morph_result = _has_pymorphy_tag(
        phrase, "gender",
        ["masc", "femn", "neut", "masculine", "feminine", "neuter"],
    )
    if morph_result.get("source") == "pymorphy" and morph_result.get("hint"):
        return morph_result
    tokens = re.findall(r"[а-яё]+", phrase.lower(), flags=re.IGNORECASE)
    if not tokens:
        return {"hint": None, "confidence": 0.0, "source": "heuristic"}
    t = tokens[0]
    if t.endswith(_FEMININE_ENDINGS):
        return {"hint": "femn", "confidence": 0.25, "source": "heuristic"}
    if t.endswith(_NEUTER_ENDINGS) and len(t) >= 3:
        return {"hint": "neut", "confidence": 0.25, "source": "heuristic"}
    if t[-1] not in _VOWELS:
        return {"hint": "masc", "confidence": 0.25, "source": "heuristic"}
    return {"hint": None, "confidence": 0.0, "source": "heuristic"}


def score_phrase_naturalness(phrase: str) -> dict[str, Any]:
    """Heuristic naturalness score in [0,1]. Never claims certainty."""
    if not isinstance(phrase, str) or not phrase.strip():
        return {"score": 0.0, "reasons": ["empty"], "confidence": 0.0}
    text = phrase.strip()
    tokens = re.findall(r"[а-яёА-ЯЁ]+", text)
    total = len(re.findall(r"\S+", text)) or 1
    cyr_ratio = (len(tokens) / total) if total else 0.0
    reasons: list[str] = []
    score = 0.5
    if cyr_ratio < 0.6:
        score -= 0.15
        reasons.append("low_cyrillic_ratio")
    if re.search(r"[a-zA-Z]{4,}", text):
        score -= 0.10
        reasons.append("latin_words_present")
    if re.search(r"\s{2,}", text):
        score -= 0.05
        reasons.append("double_whitespace")
    if re.search(r"(.)\1{3,}", text):
        score -= 0.05
        reasons.append("repeated_char_burst")
    avg_len = sum(len(t) for t in tokens) / max(len(tokens), 1)
    if avg_len > 12:
        score -= 0.10
        reasons.append("very_long_tokens")
    if 4 <= avg_len <= 9 and cyr_ratio > 0.85:
        score += 0.20
        reasons.append("typical_avg_word_length")
    score = max(0.0, min(1.0, score))
    return {"score": round(score, 3), "reasons": reasons[:10],
            "confidence": 0.30, "cyrillic_ratio": round(cyr_ratio, 3)}


def suggest_morphology_notes(text: str, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    if not isinstance(text, str) or not text.strip():
        return []
    n = _clamp(limit)
    notes: list[dict[str, Any]] = []
    tokens = _CYR_WORD.findall(text)
    seen: set[str] = set()
    for tok in tokens:
        t = tok.lower()
        if t in seen:
            continue
        seen.add(t)
        notes.append({
            "token": t,
            "lemma_guess": guess_lemma(t),
            "pos_guess": detect_part_of_speech(t),
        })
        if len(notes) >= n:
            break
    return notes


__all__ = [
    "FEATURE_FLAG",
    "dependency_status",
    "normalize_russian_word",
    "guess_lemma",
    "detect_part_of_speech",
    "detect_case_hint",
    "detect_number_hint",
    "detect_gender_hint",
    "score_phrase_naturalness",
    "suggest_morphology_notes",
]
