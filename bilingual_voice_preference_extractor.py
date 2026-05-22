"""Phase 26 - Bilingual Voice Preference Extractor.

Extracts EN / RU / mixed preference signals from user instructions. No
sensitive-attribute extraction. Returns confidence + evidence; never
writes memory itself.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any


_LANG_RU_PATTERNS = (
    r"\b(speak russian|answer in russian|reply in russian|"
    r"respond in russian|use russian|in russian please|"
    r"switch to russian)\b",
    r"\b(говори по\-?русски|отвечай на русском|на русском|"
    r"переключись на русский|давай по\-?русски)\b",
)


_LANG_EN_PATTERNS = (
    r"\b(speak english|answer in english|reply in english|"
    r"respond in english|use english|in english please|"
    r"switch to english)\b",
    r"\b(говори по\-?английски|отвечай на английском|на английском|"
    r"переключись на английский|давай по\-?английски)\b",
)


_LANG_MIX_PATTERNS = (
    r"\b(mix both|mix more|mix the languages|"
    r"mix english and russian|mix russian and english|"
    r"use both languages|bilingual mode|more mixing|"
    r"use russian words sometimes|use english words sometimes|"
    r"one word english one word russian)\b",
    r"\b(смешивай русский и английский|смешивай оба|смешивай больше|"
    r"оба языка|на двух языках)\b",
)


_LANG_NO_MIX_PATTERNS = (
    r"\b(don't mix languages|stop mixing|no mixing|"
    r"keep one language|stick to one language)\b",
    r"\b(не смешивай|не смешивай языки|перестань смешивать|"
    r"держись одного языка)\b",
)


_FORMALITY_LESS_PATTERNS = (
    r"\b(less formal|more casual|talk simpler|simpler please|"
    r"keep it casual|drop the formality)\b",
    r"\b(попроще|менее формально|разговорнее|более просто|"
    r"без официоза)\b",
)


_FORMALITY_MORE_PATTERNS = (
    r"\b(more formal|be professional|use professional language|"
    r"be more formal|business tone)\b",
    r"\b(более формально|официально|по\-деловому|"
    r"профессионально)\b",
)


_NATURAL_PATTERNS = (
    r"\b(more natural|sound natural|less robotic|"
    r"talk normally)\b",
    r"\b(естественнее|менее роботизированно|говори нормально|"
    r"будь живее)\b",
)


_NO_SLANG_PATTERNS = (
    r"\b(no slang|less slang|drop the slang|stop using slang)\b",
    r"\b(без сленга|меньше сленга)\b",
)


_PRACTICE_RU_PATTERNS = (
    r"\b(practice russian|let'?s practice russian|"
    r"i want to practice russian|teach me russian|"
    r"correct my russian)\b",
    r"\b(давай практиковать русский|учусь русскому|"
    r"исправляй мой русский)\b",
)


_PRACTICE_EN_PATTERNS = (
    r"\b(practice english|let'?s practice english|"
    r"i want to practice english|teach me english|"
    r"correct my english)\b",
    r"\b(давай практиковать английский|учусь английскому|"
    r"исправляй мой английский)\b",
)


_SHORT_ANSWER_PATTERNS = (
    r"\b(shorter answers|keep it short|be concise|"
    r"shorter please|too long|tldr)\b",
    r"\b(покороче|коротко|меньше слов|кратко)\b",
)


_LONG_ANSWER_PATTERNS = (
    r"\b(longer answer|more detail|explain more|deeper|"
    r"more depth|elaborate)\b",
    r"\b(подробнее|глубже|расскажи больше)\b",
)


_TURN_TEACHER_PATTERNS = (
    r"\b(teach me|tutor me|lesson mode|teacher mode)\b",
    r"\b(объясни мне|научи меня|урок|режим учителя)\b",
)


def _scan(text: str, patterns) -> list[str]:
    s = (text or "").lower()
    hits: list[str] = []
    for p in patterns:
        for m in re.finditer(p, s):
            hits.append(m.group(0))
    return hits


def _verdict(hits: list[str], value: str) -> dict[str, Any]:
    if hits:
        return {"detected": True, "value": value,
                "confidence": min(1.0, 0.6 + 0.1 * len(hits)),
                "evidence": hits[:5]}
    return {"detected": False, "value": None,
            "confidence": 0.0, "evidence": []}


def extract_language_preference(text: str) -> dict[str, Any]:
    if _scan(text, _LANG_NO_MIX_PATTERNS):
        return _verdict(_scan(text, _LANG_NO_MIX_PATTERNS),
                         "no_mix_keep_one_language")
    if _scan(text, _LANG_MIX_PATTERNS):
        return _verdict(_scan(text, _LANG_MIX_PATTERNS), "mix")
    if _scan(text, _LANG_RU_PATTERNS):
        return _verdict(_scan(text, _LANG_RU_PATTERNS), "russian")
    if _scan(text, _LANG_EN_PATTERNS):
        return _verdict(_scan(text, _LANG_EN_PATTERNS), "english")
    return _verdict([], None)


def extract_code_switch_preference(text: str) -> dict[str, Any]:
    if _scan(text, _LANG_NO_MIX_PATTERNS):
        return _verdict(_scan(text, _LANG_NO_MIX_PATTERNS), "stop_mixing")
    if _scan(text, _LANG_MIX_PATTERNS):
        return _verdict(_scan(text, _LANG_MIX_PATTERNS), "mix_more")
    return _verdict([], None)


def extract_formality_preference(text: str) -> dict[str, Any]:
    if _scan(text, _FORMALITY_LESS_PATTERNS):
        return _verdict(_scan(text, _FORMALITY_LESS_PATTERNS), "less_formal")
    if _scan(text, _FORMALITY_MORE_PATTERNS):
        return _verdict(_scan(text, _FORMALITY_MORE_PATTERNS), "more_formal")
    return _verdict([], None)


def extract_spoken_style_preference(text: str) -> dict[str, Any]:
    if _scan(text, _NATURAL_PATTERNS):
        return _verdict(_scan(text, _NATURAL_PATTERNS), "more_natural")
    if _scan(text, _NO_SLANG_PATTERNS):
        return _verdict(_scan(text, _NO_SLANG_PATTERNS), "less_slang")
    return _verdict([], None)


def extract_practice_language_preference(text: str) -> dict[str, Any]:
    if _scan(text, _PRACTICE_RU_PATTERNS):
        return _verdict(_scan(text, _PRACTICE_RU_PATTERNS), "ru")
    if _scan(text, _PRACTICE_EN_PATTERNS):
        return _verdict(_scan(text, _PRACTICE_EN_PATTERNS), "en")
    return _verdict([], None)


def extract_turn_style_preference(text: str) -> dict[str, Any]:
    if _scan(text, _SHORT_ANSWER_PATTERNS):
        return _verdict(_scan(text, _SHORT_ANSWER_PATTERNS), "concise")
    if _scan(text, _LONG_ANSWER_PATTERNS):
        return _verdict(_scan(text, _LONG_ANSWER_PATTERNS), "explanatory")
    if _scan(text, _TURN_TEACHER_PATTERNS):
        return _verdict(_scan(text, _TURN_TEACHER_PATTERNS), "teacher")
    return _verdict([], None)


def extract_voice_memory_preferences(text: str) -> dict[str, Any]:
    return {
        "language": extract_language_preference(text),
        "code_switch": extract_code_switch_preference(text),
        "formality": extract_formality_preference(text),
        "spoken_style": extract_spoken_style_preference(text),
        "practice_language": extract_practice_language_preference(text),
        "turn_style": extract_turn_style_preference(text),
    }


def normalize_preference_update(preferences: dict[str, Any]) -> dict[str, Any]:
    """Map preference verdicts into the voice-memory state field names."""
    update: dict[str, Any] = {}
    if not isinstance(preferences, dict):
        return update
    lang = (preferences.get("language") or {})
    if lang.get("detected"):
        v = lang["value"]
        if v == "russian":
            update["preferred_language_mode"] = "russian_only"
            update["preferred_spoken_mode"] = "russian_only"
        elif v == "english":
            update["preferred_language_mode"] = "english_only"
            update["preferred_spoken_mode"] = "english_only"
        elif v == "mix":
            update["preferred_language_mode"] = "mixed_en_ru"
            update["preferred_spoken_mode"] = "mixed_en_ru"
        elif v == "no_mix_keep_one_language":
            update["preferred_code_switch_density"] = 0.0
    cs = (preferences.get("code_switch") or {})
    if cs.get("detected"):
        if cs["value"] == "mix_more":
            update["preferred_code_switch_density"] = 0.55
        elif cs["value"] == "stop_mixing":
            update["preferred_code_switch_density"] = 0.0
    form = (preferences.get("formality") or {})
    if form.get("detected"):
        if form["value"] == "less_formal":
            update["preferred_formality"] = "casual"
        elif form["value"] == "more_formal":
            update["preferred_formality"] = "professional"
    style = (preferences.get("spoken_style") or {})
    if style.get("detected") and style["value"] == "less_slang":
        update["preferred_formality"] = update.get(
            "preferred_formality", "standard")
    practice = (preferences.get("practice_language") or {})
    if practice.get("detected"):
        update["user_is_practicing_language"] = practice["value"]
    turn = (preferences.get("turn_style") or {})
    if turn.get("detected"):
        update["preferred_turn_style"] = turn["value"]
    return update


def write_preference_extraction_report(report: dict[str, Any],
                                       output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "extract_language_preference",
    "extract_code_switch_preference",
    "extract_formality_preference",
    "extract_spoken_style_preference",
    "extract_practice_language_preference",
    "extract_turn_style_preference",
    "extract_voice_memory_preferences",
    "normalize_preference_update",
    "write_preference_extraction_report",
]
