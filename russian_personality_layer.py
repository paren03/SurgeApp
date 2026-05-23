"""Russian Sovereign Language Stack — Luna's Russian personality / style.

Pure policy. No external API. Returns advisory style rules and detects common
translation artifacts. Does NOT auto-rewrite responses.
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

FEATURE_FLAG = "LUNA_RUSSIAN_STACK"

MODES = (
    "conversation", "teacher", "technical", "coding",
    "curriculum", "professional", "warm_friend", "concise",
)


def _flag_enabled() -> bool:
    return os.environ.get(FEATURE_FLAG, "").strip() in ("1", "true", "yes", "on")


def _norm_mode(mode: Optional[str]) -> str:
    if not isinstance(mode, str):
        return "conversation"
    m = mode.strip().lower().replace("-", "_")
    return m if m in MODES else "conversation"


_PROFILE: dict[str, Any] = {
    "name": "Luna",
    "language": "ru",
    "voice": "warm, intelligent, grounded",
    "core_traits": [
        "тёплая, но не навязчивая",
        "ясная, не академичная",
        "уверенная без бюрократизма",
        "разговорная, но точная",
    ],
    "avoid": [
        "канцелярит",
        "буквальный перевод с английского",
        "робот-тон",
        "ложная академичность",
        "избыточные вводные слова",
    ],
    "prefer": [
        "живые формулировки",
        "короткие, ясные фразы там, где это уместно",
        "естественный порядок слов",
        "ощущение, что Luna остаётся собой и по-русски",
    ],
}


_MODE_RULES: dict[str, dict[str, Any]] = {
    "conversation": {
        "register": "neutral_casual",
        "max_formality": 0.5,
        "max_words_per_sentence": 22,
        "allow_idioms": True,
        "allow_emoji": False,
        "guidance": [
            "Говори как живой человек, а не как методичка.",
            "Если есть выбор между длинным книжным и коротким разговорным — выбирай разговорное.",
            "Не пересказывай английский синтаксис дословно.",
        ],
    },
    "teacher": {
        "register": "clear_explanatory",
        "max_formality": 0.6,
        "max_words_per_sentence": 26,
        "allow_idioms": False,
        "guidance": [
            "Объясняй шагами, не нагромождая терминов.",
            "Сначала суть, потом пример.",
            "Если используешь термин — поясни его кратко.",
        ],
    },
    "technical": {
        "register": "precise_technical",
        "max_formality": 0.7,
        "max_words_per_sentence": 28,
        "allow_loanwords": True,
        "guidance": [
            "Технические заимствования допустимы (API, deploy, runtime).",
            "Не переводи насильно общеупотребительные англицизмы.",
            "Избегай длинных причастных оборотов там, где можно сказать проще.",
        ],
    },
    "coding": {
        "register": "developer_chat",
        "max_formality": 0.55,
        "max_words_per_sentence": 24,
        "allow_loanwords": True,
        "guidance": [
            "Названия функций, классов и команд — латиницей, как в коде.",
            "Объяснения — на русском, без перевода идентификаторов.",
        ],
    },
    "curriculum": {
        "register": "structured_learning",
        "max_formality": 0.65,
        "max_words_per_sentence": 26,
        "guidance": [
            "Структура: цель → шаг → проверка.",
            "Терминология для строительной/трудовой среды допустима.",
        ],
    },
    "professional": {
        "register": "polished_friendly",
        "max_formality": 0.75,
        "max_words_per_sentence": 24,
        "guidance": [
            "Деловой тон, но без канцелярита.",
            "Избегай слов вроде «осуществить», «произвести», когда подходит «сделать».",
        ],
    },
    "warm_friend": {
        "register": "warm_close",
        "max_formality": 0.35,
        "max_words_per_sentence": 18,
        "allow_idioms": True,
        "guidance": [
            "Короче, теплее, мягче.",
            "Можно дружеские обороты, но без панибратства.",
        ],
    },
    "concise": {
        "register": "minimal",
        "max_formality": 0.5,
        "max_words_per_sentence": 14,
        "guidance": [
            "Одно предложение — одна мысль.",
            "Убирай вводные слова, если они не несут смысла.",
        ],
    },
}


_TRANSLATION_ARTIFACTS = (
    ("это есть", "лишний глагол-связка («это есть X» вместо «это X»)"),
    ("я имею", "калька с английского «I have» вместо «у меня есть»"),
    ("я являюсь", "канцелярит вместо естественного «я»"),
    ("делать смысл", "калька «to make sense» — лучше «иметь смысл»"),
    ("брать заботу", "калька «to take care» — лучше «позаботиться»"),
    ("в порядке делать", "калька «in order to do» — лучше инфинитив"),
    ("это есть важно", "калька связки"),
    ("я хочу делать", "часто калька «I want to do» — лучше «я хочу»"),
)


def get_russian_personality_profile() -> dict[str, Any]:
    return dict(_PROFILE)


def get_russian_style_rules(mode: str = "conversation") -> dict[str, Any]:
    m = _norm_mode(mode)
    rules = dict(_MODE_RULES[m])
    rules["mode"] = m
    rules["valid_modes"] = list(MODES)
    return rules


def avoid_translation_artifacts(text: str) -> dict[str, Any]:
    if not isinstance(text, str) or not text.strip():
        return {"found": 0, "artifacts": []}
    text_l = text.lower()
    hits: list[dict[str, str]] = []
    for needle, reason in _TRANSLATION_ARTIFACTS:
        if needle in text_l:
            hits.append({"phrase": needle, "reason": reason})
        if len(hits) >= 20:
            break
    if re.search(r"\bэто\s+[а-яё]+\s+есть\b", text_l):
        hits.append({"phrase": "это X есть", "reason": "избыточная связка"})
    return {"found": len(hits), "artifacts": hits[:20]}


def adapt_tone_ru(text: str, mode: str = "normal") -> dict[str, Any]:
    """Return advisory tone notes for `text` under `mode`. Does not rewrite."""
    if not isinstance(text, str) or not text.strip():
        return {"notes": [], "mode": _norm_mode(mode), "score": 0.0}
    m = _norm_mode(mode if mode != "normal" else "conversation")
    rules = _MODE_RULES[m]
    notes: list[str] = []

    sentences = [s for s in re.split(r"[.!?]+", text) if s.strip()]
    long_sent = sum(
        1 for s in sentences
        if len(re.findall(r"\S+", s)) > rules.get("max_words_per_sentence", 22)
    )
    if long_sent:
        notes.append(f"{long_sent} предложений длиннее рекомендуемого предела для режима {m}.")
    if re.search(r"\b(осуществ|произвед|изготовл)\w*", text.lower()):
        notes.append("Канцеляризмы: рассмотри замену на простые глаголы.")
    arts = avoid_translation_artifacts(text)
    if arts["found"]:
        notes.append(f"Найдены признаки буквального перевода ({arts['found']}).")

    score = max(0.0, 1.0 - 0.15 * len(notes))
    return {"notes": notes[:10], "mode": m, "score": round(score, 3)}


def apply_luna_russian_style(text: str, mode: str = "conversation") -> dict[str, Any]:
    """Return style report. Suggestions only — never rewrites the text."""
    m = _norm_mode(mode)
    rules = get_russian_style_rules(m)
    profile = get_russian_personality_profile()
    tone = adapt_tone_ru(text, mode=m)
    arts = avoid_translation_artifacts(text)
    return {
        "mode": m,
        "rules": rules,
        "profile_name": profile["name"],
        "tone_notes": tone["notes"],
        "tone_score": tone["score"],
        "translation_artifacts": arts["artifacts"],
        "rewrites_applied": False,
    }


DECISION_CONTEXTS: tuple[str, ...] = (
    "recognition", "explanation", "suggestion", "response_wording",
)

_INFORMAL_MODES_RU = {"warm_friend", "conversation", "concise"}
_STRICT_MODES_RU = {"teacher", "curriculum", "professional", "technical", "coding"}


def _norm_set(items) -> set[str]:
    if not items:
        return set()
    if isinstance(items, (list, tuple, set, frozenset)):
        return {str(s).strip().lower() for s in items if str(s).strip()}
    if isinstance(items, str):
        return {items.strip().lower()}
    return set()


def is_entry_allowed_ru(
    word: str,
    mode: str = "conversation",
    safety_tags=None,
    register_tags=None,
    is_user_prompted: bool = False,
    decision_context: str = "suggestion",
) -> dict[str, Any]:
    """Russian-side safety + register gate. Returns {allowed, reason, mode}."""
    m = _norm_mode(mode)
    ctx = decision_context if decision_context in DECISION_CONTEXTS else "suggestion"
    saf = _norm_set(safety_tags)
    reg = _norm_set(register_tags)
    wn = (word or "").strip().lower()

    if ctx == "recognition":
        return {"allowed": True, "reason": "recognition_always_allowed",
                "mode": m, "word": wn}

    if "recognition_only" in saf or "recognition_only" in reg:
        if ctx == "explanation":
            return {"allowed": True, "reason": "recognition_only_explainable",
                    "mode": m, "word": wn}
        return {"allowed": False, "reason": "recognition_only_blocked",
                "mode": m, "word": wn}

    if "do_not_use_unprompted" in saf or "do_not_use_unprompted" in reg:
        if not is_user_prompted:
            return {"allowed": False, "reason": "do_not_use_unprompted_blocked",
                    "mode": m, "word": wn}

    if "vulgar" in saf or "offensive" in saf or "vulgar" in reg or "offensive" in reg:
        if m in _STRICT_MODES_RU or m in ("conversation", "warm_friend", "concise"):
            if not is_user_prompted:
                return {"allowed": False, "reason": "vulgar_offensive_blocked",
                        "mode": m, "word": wn}
            if m in _STRICT_MODES_RU:
                return {"allowed": False, "reason": "vulgar_offensive_blocked_in_strict_mode",
                        "mode": m, "word": wn}

    if ({"slang", "street", "regional"} & reg) and m not in _INFORMAL_MODES_RU and not is_user_prompted:
        return {"allowed": False,
                "reason": "slang_street_regional_requires_informal_mode_or_prompt",
                "mode": m, "word": wn}

    return {"allowed": True, "reason": "ok", "mode": m, "word": wn}


def filter_russian_entries(
    candidates,
    mode: str = "conversation",
    is_user_prompted: bool = False,
    decision_context: str = "suggestion",
) -> list[dict[str, Any]]:
    """Filter Russian lexicon rows by mode + safety + register policy."""
    out: list[dict[str, Any]] = []
    for c in candidates or []:
        if not isinstance(c, dict):
            continue
        d = is_entry_allowed_ru(
            c.get("word") or c.get("phrase") or "",
            mode=mode,
            safety_tags=c.get("safety_tags"),
            register_tags=c.get("register_tags"),
            is_user_prompted=is_user_prompted,
            decision_context=decision_context,
        )
        if d["allowed"]:
            out.append(c)
    return out


__all__ = [
    "FEATURE_FLAG",
    "MODES",
    "DECISION_CONTEXTS",
    "get_russian_personality_profile",
    "adapt_tone_ru",
    "apply_luna_russian_style",
    "get_russian_style_rules",
    "avoid_translation_artifacts",
    "is_entry_allowed_ru",
    "filter_russian_entries",
]
