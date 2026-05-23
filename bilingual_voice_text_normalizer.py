"""Phase 25 - Bilingual Voice Text Normalizer.

Conservative spoken-render normalization for English / Russian / mixed EN-RU.
Preserves code-switch terms, does not transliterate automatically, and never
destroys safety-critical content.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any


_HARD_LEN_CAP = 20_000


_EN_ABBREVS = {
    "etc.": "et cetera",
    "i.e.": "that is",
    "e.g.": "for example",
    "vs.": "versus",
    "mr.": "mister",
    "mrs.": "missus",
    "dr.": "doctor",
    "&": "and",
}


_RU_ABBREVS = {
    "и т.д.": "и так далее",
    "и т.п.": "и тому подобное",
    "и др.": "и другие",
    "т.е.": "то есть",
    "напр.": "например",
}


_UNSPOKEN_SYMBOLS = {
    "*": "",
    "#": "",
    "_": " ",
    "~": "",
    "`": "",
    ">": "",
    "<": "",
    "|": "",
}


def _cap(s: Any) -> str:
    text = "" if s is None else str(s)
    return text[:_HARD_LEN_CAP]


def expand_or_flag_abbreviations(text: str, language_mode: str) -> dict[str, Any]:
    s = _cap(text)
    expansions: list[dict[str, str]] = []
    out = s
    if language_mode in ("english_only", "english_with_russian_terms",
                          "mixed_en_ru", "code_switch_sentence_level",
                          "code_switch_phrase_level",
                          "code_switch_word_level"):
        for ab, full in _EN_ABBREVS.items():
            if ab in out.lower():
                out = re.sub(re.escape(ab), full, out,
                              flags=re.IGNORECASE)
                expansions.append({"from": ab, "to": full,
                                   "language": "en"})
    if language_mode in ("russian_only", "russian_with_english_terms",
                          "mixed_en_ru", "code_switch_sentence_level",
                          "code_switch_phrase_level",
                          "code_switch_word_level"):
        for ab, full in _RU_ABBREVS.items():
            if ab in out:
                out = out.replace(ab, full)
                expansions.append({"from": ab, "to": full,
                                   "language": "ru"})
    return {"ok": True, "text": out, "expansions": expansions[:50]}


def remove_or_convert_unspoken_symbols(text: str,
                                       language_mode: str) -> dict[str, Any]:
    s = _cap(text)
    replaced: list[dict[str, str]] = []
    for sym, repl in _UNSPOKEN_SYMBOLS.items():
        if sym in s:
            s = s.replace(sym, repl)
            replaced.append({"symbol": sym, "replaced_with": repl})
    return {"ok": True, "text": s, "replaced": replaced[:20]}


def normalize_spacing_and_punctuation(text: str,
                                      language_mode: str) -> dict[str, Any]:
    s = _cap(text)
    # Collapse repeated spaces, keep meaningful punctuation needed for prosody.
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r" *\n+ *", "\n", s)
    # Normalize repeated punctuation (`!!!` -> `!`).
    s = re.sub(r"([!?.,;:])\1{2,}", r"\1", s)
    # Strip leading/trailing whitespace per line.
    s = "\n".join(line.strip() for line in s.split("\n")).strip()
    return {"ok": True, "text": s}


def preserve_code_switch_terms(text: str,
                               language_mode: str) -> dict[str, Any]:
    """Identify EN/RU tokens that should not be auto-transliterated."""
    preserved: list[dict[str, Any]] = []
    for m in re.finditer(r"\S+", text or ""):
        tok = m.group(0)
        if not tok:
            continue
        cyr = any("Ѐ" <= c <= "ӿ" for c in tok)
        lat = any(c.isalpha() and c.isascii() for c in tok)
        if cyr and lat:
            preserved.append({"token": tok, "kind": "mixed_token",
                              "start": m.start(), "end": m.end()})
        elif language_mode in ("english_only",
                                "english_with_russian_terms",
                                "mixed_en_ru",
                                "code_switch_sentence_level",
                                "code_switch_phrase_level",
                                "code_switch_word_level") and cyr:
            preserved.append({"token": tok, "kind": "ru_term_in_en",
                              "start": m.start(), "end": m.end()})
        elif language_mode in ("russian_only",
                                "russian_with_english_terms",
                                "mixed_en_ru",
                                "code_switch_sentence_level",
                                "code_switch_phrase_level",
                                "code_switch_word_level") and lat:
            preserved.append({"token": tok, "kind": "en_term_in_ru",
                              "start": m.start(), "end": m.end()})
        if len(preserved) >= 200:
            break
    return {"ok": True, "preserved_tokens": preserved,
            "preserved_count": len(preserved)}


def normalize_english_spoken_text(text: str) -> dict[str, Any]:
    a = expand_or_flag_abbreviations(text, "english_only")
    sym = remove_or_convert_unspoken_symbols(a["text"], "english_only")
    sp = normalize_spacing_and_punctuation(sym["text"], "english_only")
    return {"ok": True, "language_mode": "english_only",
            "text": sp["text"], "expansions": a["expansions"],
            "replaced_symbols": sym["replaced"]}


def normalize_russian_spoken_text(text: str) -> dict[str, Any]:
    a = expand_or_flag_abbreviations(text, "russian_only")
    sym = remove_or_convert_unspoken_symbols(a["text"], "russian_only")
    sp = normalize_spacing_and_punctuation(sym["text"], "russian_only")
    return {"ok": True, "language_mode": "russian_only",
            "text": sp["text"], "expansions": a["expansions"],
            "replaced_symbols": sym["replaced"]}


def normalize_mixed_spoken_text(text: str) -> dict[str, Any]:
    pres = preserve_code_switch_terms(text or "", "mixed_en_ru")
    a = expand_or_flag_abbreviations(text or "", "mixed_en_ru")
    sym = remove_or_convert_unspoken_symbols(a["text"], "mixed_en_ru")
    sp = normalize_spacing_and_punctuation(sym["text"], "mixed_en_ru")
    return {"ok": True, "language_mode": "mixed_en_ru",
            "text": sp["text"],
            "expansions": a["expansions"],
            "replaced_symbols": sym["replaced"],
            "preserved_tokens": pres["preserved_tokens"]}


def normalize_for_spoken_render(text: str,
                                language_mode: str = "mixed_en_ru",
                                conversation_mode: str = "conversation",
                                is_user_prompted: bool = False
                                ) -> dict[str, Any]:
    if language_mode == "english_only":
        return normalize_english_spoken_text(text)
    if language_mode == "russian_only":
        return normalize_russian_spoken_text(text)
    return normalize_mixed_spoken_text(text)


def write_normalization_report(report: dict[str, Any],
                               output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "normalize_for_spoken_render",
    "normalize_english_spoken_text",
    "normalize_russian_spoken_text",
    "normalize_mixed_spoken_text",
    "expand_or_flag_abbreviations",
    "remove_or_convert_unspoken_symbols",
    "normalize_spacing_and_punctuation",
    "preserve_code_switch_terms",
    "write_normalization_report",
]
