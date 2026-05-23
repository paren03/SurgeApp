"""Phase 25 - Bilingual Pronunciation Hinting.

Detects pronunciation-sensitive terms across EN / RU / mixed text and emits
hint annotations. NEVER auto-transliterates. Flags uncertainty so the
future renderer can decide.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional


_HARD_HINT_CAP = 200


_ACRONYM_RE = re.compile(r"\b[A-Z]{2,5}\b")
_CYR_TOKEN_RE = re.compile(r"[Ѐ-ӿԀ-ԯ][Ѐ-ӿԀ-ԯ'\-]+")
_LATIN_WORD_RE = re.compile(r"[A-Za-z][A-Za-z'\-]+")


_TRANSLIT_MARKERS = (
    "privet", "spasibo", "do svidaniya", "kak dela", "khorosho",
    "horosho", "krasivo",
)


def detect_pronunciation_sensitive_terms(text: str,
                                         language_mode: str
                                         ) -> list[dict[str, Any]]:
    s = text or ""
    out: list[dict[str, Any]] = []
    # Acronyms (any all-caps run 2-5 chars) need renderer attention.
    for m in _ACRONYM_RE.finditer(s):
        out.append({"token": m.group(0), "start": m.start(),
                    "end": m.end(), "kind": "acronym",
                    "pronunciation_attention": True,
                    "note": "acronym_letter_by_letter_or_word_form"})
        if len(out) >= _HARD_HINT_CAP:
            return out
    # Mixed tokens (contain both scripts).
    for m in re.finditer(r"\S+", s):
        tok = m.group(0)
        cyr = bool(_CYR_TOKEN_RE.search(tok))
        lat = bool(_LATIN_WORD_RE.search(tok))
        if cyr and lat:
            out.append({"token": tok, "start": m.start(),
                        "end": m.end(),
                        "kind": "mixed_script_token",
                        "pronunciation_attention": True,
                        "note": "mixed_script_renderer_choose"})
            if len(out) >= _HARD_HINT_CAP:
                return out
    # Russian terms in EN-dominant context: flag stress uncertainty.
    if language_mode in ("english_only", "english_with_russian_terms",
                          "code_switch_word_level",
                          "code_switch_phrase_level"):
        for m in _CYR_TOKEN_RE.finditer(s):
            out.append({"token": m.group(0), "start": m.start(),
                        "end": m.end(),
                        "kind": "ru_term_in_en_context",
                        "pronunciation_attention": True,
                        "note": "stress_unknown_renderer_default"})
            if len(out) >= _HARD_HINT_CAP:
                return out
    return out


def create_english_pronunciation_hints(segments: list[dict[str, Any]]
                                       ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in segments:
        if (s.get("language") or "") != "en":
            continue
        t = s.get("text") or ""
        if _ACRONYM_RE.search(t):
            out.append({"segment_id": s.get("segment_id"),
                        "kind": "acronym",
                        "pronunciation_attention": True,
                        "note": "acronyms_present"})
            if len(out) >= _HARD_HINT_CAP:
                return out
    return out


def create_russian_pronunciation_hints(segments: list[dict[str, Any]]
                                       ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in segments:
        if (s.get("language") or "") != "ru":
            continue
        out.append({"segment_id": s.get("segment_id"),
                    "kind": "ru_stress_uncertainty",
                    "pronunciation_attention": True,
                    "note": "russian_stress_renderer_default"})
        if len(out) >= _HARD_HINT_CAP:
            return out
    return out


def create_code_switch_pronunciation_hints(segments: list[dict[str, Any]]
                                           ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    last_lang = None
    for s in segments:
        cur = s.get("language") or "und"
        if last_lang is not None and last_lang in ("en", "ru") \
                and cur in ("en", "ru") and cur != last_lang:
            out.append({"segment_id": s.get("segment_id"),
                        "kind": "code_switch_boundary",
                        "pronunciation_attention": True,
                        "note": f"{last_lang}_to_{cur}_renderer_pivot"})
            if len(out) >= _HARD_HINT_CAP:
                return out
        last_lang = cur
    return out


def flag_transliteration_risk(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    hits = [t for t in _TRANSLIT_MARKERS
            if re.search(rf"\b{re.escape(t)}\b", s)]
    cyr = bool(_CYR_TOKEN_RE.search(text or ""))
    return {"ok": True,
            "transliteration_risk": (len(hits) >= 2) and not cyr,
            "markers_hit": hits[:10]}


def flag_russian_stress_uncertainty(text: str) -> dict[str, Any]:
    tokens = _CYR_TOKEN_RE.findall(text or "")
    return {"ok": True,
            "russian_token_count": len(tokens),
            "stress_uncertain_for_all_tokens": True if tokens else False,
            "note": "heuristic fallback: stress is renderer-dependent"}


def flag_acronym_pronunciation(text: str) -> dict[str, Any]:
    hits = _ACRONYM_RE.findall(text or "")
    return {"ok": True,
            "acronyms_detected": list({h for h in hits})[:50],
            "n_acronyms": len(hits)}


def write_pronunciation_hint_report(report: dict[str, Any],
                                    output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "detect_pronunciation_sensitive_terms",
    "create_english_pronunciation_hints",
    "create_russian_pronunciation_hints",
    "create_code_switch_pronunciation_hints",
    "flag_transliteration_risk",
    "flag_russian_stress_uncertainty",
    "flag_acronym_pronunciation",
    "write_pronunciation_hint_report",
]
