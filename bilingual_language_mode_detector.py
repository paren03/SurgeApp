"""Phase 23 - Bilingual Language Mode Detector.

Detects whether a prompt is English-only, Russian-only, mixed, transliterated,
or code-switched. Returns bounded metadata only. No daemon, no internet, no
external API.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional


LANGUAGE_MODES = (
    "english_only",
    "russian_only",
    "mixed_en_ru",
    "english_with_russian_terms",
    "russian_with_english_terms",
    "code_switch_sentence_level",
    "code_switch_phrase_level",
    "code_switch_word_level",
    "transliterated_russian",
    "unknown",
)


_CYR_RE = re.compile(r"[Ѐ-ӿԀ-ԯ]")
_LATIN_WORD_RE = re.compile(r"[A-Za-z][A-Za-z'\-]+")
_CYR_WORD_RE = re.compile(r"[Ѐ-ӿԀ-ԯ][Ѐ-ӿԀ-ԯ'\-]+")
_SENTENCE_SPLIT_RE = re.compile(r"[.!?\n]+")


_REQUEST_RU_PATTERNS = (
    r"\b(in russian|по\-?русски|на русском|in russian please|"
    r"answer in russian|reply in russian|respond in russian|"
    r"скажи по\-?русски|отвечай на русском)\b",
)
_REQUEST_EN_PATTERNS = (
    r"\b(in english|по\-?английски|на английском|in english please|"
    r"answer in english|reply in english|respond in english|"
    r"скажи по\-?английски|отвечай на английском)\b",
)
_REQUEST_MIX_PATTERNS = (
    r"\b(both english and russian|english and russian|mix english and russian|"
    r"bilingual|на двух языках|mixed|both languages|"
    r"переключайся между|with russian terms|with english terms)\b",
)


# Common transliteration markers (very rough — used only to soft-flag).
_TRANSLIT_MARKERS = (
    "privet", "spasibo", "do svidaniya", "kak dela", "khorosho",
    "ya", "ne", "menya", "tebya", "krasivo", "horosho",
)


def detect_script_mix(text: str) -> dict[str, Any]:
    s = text or ""
    n_latin_chars = sum(1 for c in s if c.isalpha() and c.isascii())
    n_cyr_chars = sum(1 for c in s if _CYR_RE.match(c))
    total = max(1, n_latin_chars + n_cyr_chars)
    return {"latin_chars": n_latin_chars,
            "cyrillic_chars": n_cyr_chars,
            "latin_ratio": round(n_latin_chars / total, 3),
            "cyrillic_ratio": round(n_cyr_chars / total, 3)}


def detect_language_segments(text: str) -> list[dict[str, Any]]:
    """Word-level segmentation with per-token language guess. Bounded."""
    s = text or ""
    out: list[dict[str, Any]] = []
    for m in re.finditer(r"\S+", s):
        tok = m.group(0)
        cyr = bool(_CYR_RE.search(tok))
        lat = any(c.isalpha() and c.isascii() for c in tok)
        if cyr and not lat:
            lang = "ru"
        elif lat and not cyr:
            lang = "en"
        elif cyr and lat:
            lang = "mixed_token"
        else:
            lang = "other"
        out.append({"token": tok, "start": m.start(),
                    "end": m.end(), "lang": lang})
        if len(out) >= 2000:
            break
    return out


def detect_code_switch_points(text: str) -> dict[str, Any]:
    """Count transitions between EN and RU word tokens."""
    segs = detect_language_segments(text)
    only_lang = [s for s in segs if s["lang"] in ("en", "ru")]
    transitions = 0
    last = None
    for s in only_lang:
        if last is not None and s["lang"] != last:
            transitions += 1
        last = s["lang"]
    sentence_count = max(1, len([x for x in _SENTENCE_SPLIT_RE.split(text or "")
                                  if x.strip()]))
    return {"transitions": transitions,
            "n_lang_tokens": len(only_lang),
            "n_sentences": sentence_count,
            "transitions_per_sentence":
                round(transitions / max(1, sentence_count), 3)}


def estimate_language_ratio(text: str) -> dict[str, Any]:
    en_words = _LATIN_WORD_RE.findall(text or "")
    ru_words = _CYR_WORD_RE.findall(text or "")
    total = max(1, len(en_words) + len(ru_words))
    return {"english_words": len(en_words),
            "russian_words": len(ru_words),
            "english_ratio": round(len(en_words) / total, 3),
            "russian_ratio": round(len(ru_words) / total, 3)}


def detect_transliteration_hint(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    # Word-boundary match so "ne" doesn't fire on "engineer", etc.
    hits = [t for t in _TRANSLIT_MARKERS
            if re.search(rf"\b{re.escape(t)}\b", s)]
    cyr = bool(_CYR_RE.search(text or ""))
    # Require at least 2 markers to soft-flag, OR 1 marker with no English
    # filler words elsewhere.
    looks_translit = (len(hits) >= 2) and not cyr
    return {"transliteration_likely": looks_translit, "markers_hit": hits}


def detect_user_language_preference(text: str,
                                    conversation_hint: Optional[str] = None
                                    ) -> dict[str, Any]:
    s = (text or "").lower()
    requested_ru = any(re.search(p, s) for p in _REQUEST_RU_PATTERNS)
    requested_en = any(re.search(p, s) for p in _REQUEST_EN_PATTERNS)
    requested_mix = any(re.search(p, s) for p in _REQUEST_MIX_PATTERNS)
    hint = (conversation_hint or "").lower().strip()
    if hint in ("ru", "russian"):
        requested_ru = True
    elif hint in ("en", "english"):
        requested_en = True
    elif hint in ("mix", "mixed", "bilingual"):
        requested_mix = True
    return {"requested_russian": requested_ru,
            "requested_english": requested_en,
            "requested_mix": requested_mix,
            "conversation_hint": conversation_hint}


def detect_requested_output_mode(text: str) -> dict[str, Any]:
    pref = detect_user_language_preference(text)
    if pref["requested_mix"]:
        return {"output_mode": "mixed_en_ru", "source": "explicit_request"}
    if pref["requested_russian"]:
        return {"output_mode": "russian_only", "source": "explicit_request"}
    if pref["requested_english"]:
        return {"output_mode": "english_only", "source": "explicit_request"}
    return {"output_mode": None, "source": "no_explicit_request"}


def classify_language_mode(text: str,
                           conversation_hint: Optional[str] = None
                           ) -> dict[str, Any]:
    """Roll up segment + ratio + transitions + request signals into a single
    LANGUAGE_MODES verdict."""
    s = text or ""
    if not s.strip():
        return {"mode": "unknown", "reason": "empty",
                "ratio": estimate_language_ratio(s),
                "transitions": detect_code_switch_points(s),
                "preference": detect_user_language_preference(s,
                                                              conversation_hint),
                "transliteration": detect_transliteration_hint(s)}
    ratio = estimate_language_ratio(s)
    trans = detect_code_switch_points(s)
    pref = detect_user_language_preference(s, conversation_hint)
    translit = detect_transliteration_hint(s)
    en_r = ratio["english_ratio"]
    ru_r = ratio["russian_ratio"]
    sentences = trans["n_sentences"]
    transitions = trans["transitions"]

    # Explicit user request overrides detection.
    if pref["requested_mix"]:
        mode = "mixed_en_ru"
    elif pref["requested_russian"] and ratio["russian_words"] >= 0:
        mode = "russian_only" if en_r == 0 else "russian_with_english_terms"
    elif pref["requested_english"] and ratio["english_words"] >= 0:
        mode = "english_only" if ru_r == 0 else "english_with_russian_terms"
    elif translit["transliteration_likely"]:
        mode = "transliterated_russian"
    elif en_r >= 0.95 and ru_r <= 0.05:
        mode = "english_only"
    elif ru_r >= 0.95 and en_r <= 0.05:
        mode = "russian_only"
    elif sentences >= 2 and transitions >= sentences - 1 \
            and en_r > 0 and ru_r > 0 and 0.35 <= en_r <= 0.65:
        # Multiple sentences, near-balanced, alternates → sentence-level switch.
        mode = "code_switch_sentence_level"
    elif en_r > ru_r and ru_r > 0.0:
        # English dominant with some Russian
        if ru_r <= 0.30 and sentences == 1:
            mode = "english_with_russian_terms"
        elif sentences >= 2 and transitions >= sentences:
            mode = "code_switch_sentence_level"
        elif ru_r >= 0.30:
            mode = "code_switch_phrase_level"
        else:
            mode = "english_with_russian_terms"
    elif ru_r > en_r and en_r > 0.0:
        if en_r <= 0.30 and sentences == 1:
            mode = "russian_with_english_terms"
        elif sentences >= 2 and transitions >= sentences:
            mode = "code_switch_sentence_level"
        elif en_r >= 0.30:
            mode = "code_switch_phrase_level"
        else:
            mode = "russian_with_english_terms"
    elif en_r > 0 and ru_r > 0:
        if sentences >= 2 and transitions >= sentences - 1:
            mode = "code_switch_sentence_level"
        elif transitions >= 2:
            mode = "code_switch_word_level"
        else:
            mode = "mixed_en_ru"
    else:
        mode = "unknown"
    return {"mode": mode,
            "ratio": ratio,
            "transitions": trans,
            "preference": pref,
            "transliteration": translit}


def write_language_mode_report(report: dict[str, Any],
                               output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "LANGUAGE_MODES",
    "detect_script_mix",
    "detect_language_segments",
    "detect_code_switch_points",
    "detect_user_language_preference",
    "classify_language_mode",
    "estimate_language_ratio",
    "detect_transliteration_hint",
    "detect_requested_output_mode",
    "write_language_mode_report",
]
