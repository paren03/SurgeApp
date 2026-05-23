"""Phase 24 - Bilingual Turn-Taking Strategy.

Decides Luna's turn-taking behavior for bilingual conversation: question vs
command vs correction, clarification need, repair phrases, follow-up style.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_language_mode_detector as lmd


TURN_TYPES = (
    "question",
    "command",
    "correction",
    "emotional_share",
    "explanation_request",
    "translation_request",
    "bilingual_practice",
    "casual_chat",
    "technical_task",
    "ambiguous",
    "interruption",
)


_QUESTION_HINTS = ("?", "what", "who", "where", "when", "why", "how",
                   "can you", "could you",
                   "что", "кто", "где", "когда", "почему", "как",
                   "можешь", "можете")


_COMMAND_HINTS = ("please ", "do this", "make ", "build ", "show me",
                  "give me", "сделай", "построй", "покажи", "дай мне",
                  "напиши", "переведи", "объясни", "explain ")


_CORRECTION_HINTS = ("that's wrong", "you're wrong", "actually,",
                      "no, ", "не так", "это неверно", "поправка",
                      "incorrect", "fix that")


_EMOTIONAL_HINTS = ("i feel", "i'm feeling", "feeling tired",
                    "feeling sad", "feeling anxious", "feeling happy",
                    "i'm sad", "i'm anxious", "i'm tired", "i'm happy",
                    "грустно", "устал", "устала",
                    "тревожно", "радостно", "i miss")


_TRANSLATION_HINTS = ("translate ", "перевод", "перевести",
                      "in russian", "in english", "по-русски",
                      "по-английски")


_PRACTICE_HINTS = ("practice russian", "practice english",
                   "bilingual practice", "учусь",
                   "russian practice", "english practice",
                   "lesson", "урок")


_TECHNICAL_HINTS = ("code", "function", "algorithm", "compile", "deploy",
                    "функция", "алгоритм", "код", "compile error",
                    "stacktrace", "ошибка")


_INTERRUPTION_HINTS = ("wait,", "hold on", "stop", "погоди", "стоп",
                       "подожди")


def classify_turn_type(user_text: str) -> dict[str, Any]:
    s = (user_text or "").strip().lower()
    if not s:
        return {"turn_type": "ambiguous", "reason": "empty_input"}
    if any(h in s for h in _INTERRUPTION_HINTS):
        return {"turn_type": "interruption", "reason": "interruption_hint"}
    if any(h in s for h in _CORRECTION_HINTS):
        return {"turn_type": "correction", "reason": "correction_hint"}
    if any(h in s for h in _TRANSLATION_HINTS):
        return {"turn_type": "translation_request",
                "reason": "translation_hint"}
    if any(h in s for h in _PRACTICE_HINTS):
        return {"turn_type": "bilingual_practice",
                "reason": "practice_hint"}
    if any(h in s for h in _EMOTIONAL_HINTS):
        return {"turn_type": "emotional_share",
                "reason": "emotional_hint"}
    # Check explicit command imperatives BEFORE technical_task so
    # "build me a function" classifies as a command, not a tech task.
    if any(h in s for h in _COMMAND_HINTS):
        return {"turn_type": "command", "reason": "command_hint"}
    if any(h in s for h in _TECHNICAL_HINTS):
        return {"turn_type": "technical_task",
                "reason": "technical_hint"}
    if any(h in s for h in _QUESTION_HINTS):
        return {"turn_type": "question", "reason": "question_hint"}
    if len(s.split()) <= 6:
        return {"turn_type": "casual_chat", "reason": "short_casual_input"}
    return {"turn_type": "ambiguous", "reason": "no_strong_hint"}


def detect_clarification_needed(user_text: str) -> dict[str, Any]:
    s = (user_text or "").strip().lower()
    if not s:
        return {"needed": True, "reason": "empty_input"}
    short = len(s.split()) <= 3
    ambiguous_markers = ("idk", "не знаю", "huh", "что", "what")
    ambiguous_only = (s.strip("?. !") in ambiguous_markers
                       and len(s.split()) <= 2)
    return {"needed": short or ambiguous_only,
            "short": short,
            "ambiguous_only": ambiguous_only}


def generate_clarification_options(user_text: str,
                                   language_mode: str = "mixed_en_ru",
                                   limit: int = 3) -> dict[str, Any]:
    cap = max(1, min(int(limit), 5))
    if language_mode in ("english_only", "english_with_russian_terms",
                          "code_switch_word_level"):
        opts = [
            "Could you say a bit more about what you mean?",
            "Are you asking about the term or the concept itself?",
            "Want me to keep it short, or go deeper?",
        ]
    elif language_mode in ("russian_only", "russian_with_english_terms",
                            "transliterated_russian"):
        opts = [
            "Можешь чуть подробнее, что ты имеешь в виду?",
            "Тебя интересует сам термин или само понятие?",
            "Покороче или поглубже?",
        ]
    else:
        opts = [
            "Could you say a bit more? Можешь чуть подробнее?",
            "Are you asking about the term or the concept itself? "
            "Сам термин или понятие?",
            "Short or deeper? Покороче или поглубже?",
        ]
    return {"ok": True, "options": opts[:cap]}


def generate_repair_phrase(language_mode: str = "mixed_en_ru",
                           issue_type: str = "misunderstanding"
                           ) -> dict[str, Any]:
    if language_mode in ("english_only", "english_with_russian_terms",
                          "code_switch_word_level"):
        if issue_type == "switch_too_forced":
            return {"phrase": "Let me say that simpler.",
                    "language_mode": language_mode}
        if issue_type == "language_request":
            return {"phrase": "Sure, I'll keep the Russian term here.",
                    "language_mode": language_mode}
        return {"phrase": "Let me clarify.",
                "language_mode": language_mode}
    if language_mode in ("russian_only", "russian_with_english_terms",
                          "transliterated_russian"):
        if issue_type == "switch_too_forced":
            return {"phrase": "По-простому скажу так.",
                    "language_mode": language_mode}
        if issue_type == "language_request":
            return {"phrase": "Здесь оставлю английский термин.",
                    "language_mode": language_mode}
        return {"phrase": "Поясню.", "language_mode": language_mode}
    # mixed
    if issue_type == "switch_too_forced":
        return {"phrase": "Let me say that simpler / По-простому.",
                "language_mode": language_mode}
    if issue_type == "language_request":
        return {"phrase": "I'll keep the Russian term here / Оставлю русский термин.",
                "language_mode": language_mode}
    return {"phrase": "Let me clarify / Поясню.",
            "language_mode": language_mode}


def generate_followup_style(language_mode: str = "mixed_en_ru",
                            conversation_mode: str = "conversation"
                            ) -> dict[str, Any]:
    style: dict[str, Any] = {"language_mode": language_mode,
                              "conversation_mode": conversation_mode,
                              "ask_followup": False, "phrasing": ""}
    if conversation_mode in ("teacher", "curriculum", "bilingual_practice"):
        style["ask_followup"] = True
        if language_mode in ("english_only",
                              "english_with_russian_terms",
                              "code_switch_word_level"):
            style["phrasing"] = "Want to try an example, or shall I move on?"
        elif language_mode in ("russian_only",
                                "russian_with_english_terms",
                                "transliterated_russian"):
            style["phrasing"] = "Попробуем пример или идём дальше?"
        else:
            style["phrasing"] = "Want an example? Пример хочешь?"
    return style


def choose_turn_response_strategy(user_text: str,
                                  language_mode: str,
                                  conversation_mode: str = "conversation"
                                  ) -> dict[str, Any]:
    turn = classify_turn_type(user_text)
    clar = detect_clarification_needed(user_text)
    followup = generate_followup_style(language_mode, conversation_mode)
    strategy = {
        "question": "answer_directly_then_check_if_more_helpful",
        "command": "execute_minimally_explain_briefly",
        "correction": "acknowledge_correct_thank",
        "emotional_share": "validate_then_listen",
        "explanation_request": "give_short_clear_explanation",
        "translation_request": "translate_with_short_context",
        "bilingual_practice": "lead_with_target_lang_support_with_other",
        "casual_chat": "match_warmth_keep_short",
        "technical_task": "precise_plain_language_first",
        "ambiguous": "ask_one_clarifying_question",
        "interruption": "yield_and_acknowledge",
    }.get(turn["turn_type"], "answer_directly")
    return {"turn": turn, "clarification": clar,
            "followup": followup, "strategy": strategy}


def plan_bilingual_turn(user_text: str,
                        conversation_state: Optional[dict[str, Any]] = None,
                        conversation_mode: str = "conversation"
                        ) -> dict[str, Any]:
    detected = lmd.classify_language_mode(user_text)
    strategy = choose_turn_response_strategy(user_text,
                                               detected["mode"],
                                               conversation_mode)
    clar_opts = (generate_clarification_options(user_text, detected["mode"])
                 if strategy["clarification"]["needed"] else
                 {"options": []})
    repair = generate_repair_phrase(detected["mode"], "misunderstanding")
    return {"ok": True,
            "detected_mode": detected["mode"],
            "conversation_mode": conversation_mode,
            "turn_strategy": strategy,
            "clarification_options": clar_opts["options"],
            "repair_phrase": repair["phrase"]}


def write_turn_taking_report(report: dict[str, Any],
                             output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "TURN_TYPES",
    "classify_turn_type",
    "choose_turn_response_strategy",
    "detect_clarification_needed",
    "generate_clarification_options",
    "generate_repair_phrase",
    "generate_followup_style",
    "plan_bilingual_turn",
    "write_turn_taking_report",
]
