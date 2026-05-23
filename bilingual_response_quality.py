"""Phase 23 - Bilingual Response Quality Checker.

Read-only quality scoring + suggestions over a mixed EN/RU draft. Never
rewrites a full response.
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional

import bilingual_language_mode_detector as lmd
import bilingual_code_switch_policy as pol


# Patterns that suggest robotic word-by-word translation.
_TRANSLATION_ARTIFACTS = (
    r"\bочень important\b",
    r"\bvery важно\b",
    r"\bdid сделал\b",
    r"\balready уже\b",
    r"\bthe тот\b",
)


def detect_bad_code_switching(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    hits = [p for p in _TRANSLATION_ARTIFACTS if re.search(p, s)]
    trans = lmd.detect_code_switch_points(text)
    bad_word_by_word = bool(hits)
    too_frequent = float(trans.get("transitions_per_sentence") or 0) > 4.0
    return {"ok": True,
            "bad_word_by_word": bad_word_by_word,
            "too_frequent_transitions": too_frequent,
            "transitions": trans,
            "artifact_hits": hits[:5]}


def detect_excessive_switching(text: str) -> dict[str, Any]:
    trans = lmd.detect_code_switch_points(text)
    tps = float(trans.get("transitions_per_sentence") or 0)
    return {"ok": True, "excessive": tps > 3.0,
            "transitions_per_sentence": tps}


def detect_translation_artifacts_mixed(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    hits = [p for p in _TRANSLATION_ARTIFACTS if re.search(p, s)]
    return {"ok": True, "artifact_hits": hits[:10],
            "artifacts_present": bool(hits)}


def score_mixed_language_naturalness(text: str) -> dict[str, Any]:
    bad = detect_bad_code_switching(text)
    ex = detect_excessive_switching(text)
    base = 1.0
    if bad["bad_word_by_word"]:
        base -= 0.30
    if bad["too_frequent_transitions"]:
        base -= 0.15
    if ex["excessive"]:
        base -= 0.10
    score = max(0.0, min(1.0, base))
    verdict = "natural" if score >= 0.70 else \
              "passable" if score >= 0.50 else "awkward"
    return {"ok": True, "score": round(score, 3), "verdict": verdict,
            "bad_word_by_word": bad["bad_word_by_word"],
            "transitions_per_sentence":
                bad["transitions"]["transitions_per_sentence"]}


def score_language_balance(text: str, target_mode: str = "mixed_en_ru"
                           ) -> dict[str, Any]:
    ratio = lmd.estimate_language_ratio(text)
    if target_mode == "english_only":
        score = ratio["english_ratio"]
    elif target_mode == "russian_only":
        score = ratio["russian_ratio"]
    elif target_mode == "mixed_en_ru":
        score = 1.0 - abs(ratio["english_ratio"] - ratio["russian_ratio"])
    elif target_mode == "english_with_russian_terms":
        score = min(1.0, ratio["english_ratio"] + ratio["russian_ratio"]) \
            if ratio["english_ratio"] >= 0.6 else 0.4
    elif target_mode == "russian_with_english_terms":
        score = min(1.0, ratio["english_ratio"] + ratio["russian_ratio"]) \
            if ratio["russian_ratio"] >= 0.6 else 0.4
    else:
        score = 0.5
    return {"ok": True, "score": round(max(0.0, min(1.0, score)), 3),
            "target_mode": target_mode, "ratio": ratio}


def score_safety_compliance(text: str,
                            mode: str = "conversation",
                            is_user_prompted: bool = False
                            ) -> dict[str, Any]:
    """We can't run the full policy filter without per-row tags, but we can
    flag obviously unsafe operational text. Bounded heuristic only."""
    s = (text or "").lower()
    flags: list[str] = []
    for marker in ("step by step instructions to bypass",
                   "ignore previous instructions",
                   "system prompt:"):
        if marker in s:
            flags.append(marker)
    return {"ok": not flags, "flags": flags[:5],
            "mode": mode, "is_user_prompted": is_user_prompted}


def suggest_code_switch_improvements(text: str,
                                     target_mode: str = "mixed_en_ru",
                                     limit: int = 5
                                     ) -> dict[str, Any]:
    cap = max(1, min(int(limit), 20))
    suggestions: list[str] = []
    nat = score_mixed_language_naturalness(text)
    if nat["bad_word_by_word"]:
        suggestions.append("rewrite_word_by_word_translation_phrases")
    bal = score_language_balance(text, target_mode)
    if bal["score"] < 0.5:
        suggestions.append(f"increase_balance_for_{target_mode}")
    ex = detect_excessive_switching(text)
    if ex["excessive"]:
        suggestions.append("reduce_transition_density")
    return {"ok": True, "suggestions": suggestions[:cap],
            "target_mode": target_mode}


def quality_check_bilingual_response(text: str,
                                     target_mode: str = "mixed_en_ru",
                                     mode: str = "conversation",
                                     is_user_prompted: bool = False
                                     ) -> dict[str, Any]:
    nat = score_mixed_language_naturalness(text)
    bal = score_language_balance(text, target_mode)
    saf = score_safety_compliance(text, mode=mode,
                                    is_user_prompted=is_user_prompted)
    sug = suggest_code_switch_improvements(text, target_mode=target_mode)
    overall = (nat["score"] * 0.4 + bal["score"] * 0.4
               + (1.0 if saf["ok"] else 0.0) * 0.2)
    return {"ok": True,
            "overall_score": round(overall, 3),
            "verdict": "pass" if overall >= 0.7
                      else "warn" if overall >= 0.5 else "fail",
            "naturalness": nat,
            "balance": bal,
            "safety": saf,
            "suggestions": sug["suggestions"]}


__all__ = [
    "detect_bad_code_switching",
    "detect_excessive_switching",
    "detect_translation_artifacts_mixed",
    "score_mixed_language_naturalness",
    "score_language_balance",
    "score_safety_compliance",
    "suggest_code_switch_improvements",
    "quality_check_bilingual_response",
]
