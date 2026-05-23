"""Phase 24 - Bilingual Personality Continuity Scorer.

Read-only scoring of whether a draft sounds like the same Luna across EN /
RU / mixed EN-RU. Returns scores + suggestions only; never rewrites the
text.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

import bilingual_language_mode_detector as lmd


_ROBOTIC_PATTERNS = (
    r"\bas an ai\b",
    r"\bi am an ai\b",
    r"\bI cannot have feelings\b",
    r"\bmy programming\b",
    r"\bbeep boop\b",
)


_OVERLY_FORMAL_RU_PATTERNS = (
    r"\bпозвольте мне выразить\b",
    r"\bвышеизложенное\b",
    r"\bпри прочих равных условиях\b",
    r"\bсемантическая интенция\b",
)


_OVERLY_SLANGY_PATTERNS = (
    r"\byo\b.*\byo\b",
    r"\bbruh+\b",
    r"\bлол+\b",
    r"\bкек+\b",
)


_TRANSLATION_ARTIFACT_PATTERNS = (
    r"\bочень important\b",
    r"\bvery важно\b",
    r"\bтот же as\b",
    r"\balready уже\b",
    r"\bthe тот\b",
)


_LUNA_IDENTITY_HINTS = (
    "luna", "луна",
)


def _hit_count(text: str, patterns) -> int:
    s = (text or "").lower()
    return sum(1 for p in patterns if re.search(p, s))


def score_warmth_consistency(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    cold = _hit_count(text, _ROBOTIC_PATTERNS)
    warm_markers = sum(s.count(w) for w in (
        "of course", "happy to", "let me know", "конечно", "рада",
        "with you", "вместе", "понятно"))
    score = max(0.0, min(1.0, 0.5 + 0.1 * warm_markers - 0.25 * cold))
    return {"ok": True, "score": round(score, 3),
            "cold_markers_hit": cold,
            "warm_markers_hit": warm_markers}


def score_directness_consistency(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    hedges = sum(s.count(h) for h in (
        "i think maybe", "perhaps possibly", "i'm not sure but i think",
        "может быть наверное", "возможно я не уверен"))
    sentences = max(1, len([x for x in re.split(r"[.!?\n]+", s) if x.strip()]))
    score = max(0.0, min(1.0, 1.0 - 0.2 * min(hedges, 5)
                          - 0.05 * max(0, sentences - 6)))
    return {"ok": True, "score": round(score, 3),
            "hedge_count": hedges,
            "sentence_count": sentences}


def score_luna_identity_consistency(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    has_name = any(h in s for h in _LUNA_IDENTITY_HINTS)
    robotic = _hit_count(text, _ROBOTIC_PATTERNS)
    score = max(0.0, min(1.0, 0.7 + (0.2 if has_name else 0.0)
                          - 0.25 * robotic))
    return {"ok": True, "score": round(score, 3),
            "name_mentioned": has_name,
            "robotic_markers": robotic}


def score_register_consistency(text: str,
                               conversation_mode: str = "conversation"
                               ) -> dict[str, Any]:
    overly_formal_ru = _hit_count(text, _OVERLY_FORMAL_RU_PATTERNS)
    slangy = _hit_count(text, _OVERLY_SLANGY_PATTERNS)
    base = 0.85
    if conversation_mode in ("teacher", "professional", "technical",
                              "curriculum"):
        # Teacher modes should NOT be slangy.
        base -= 0.20 * slangy
    if conversation_mode == "warm_friend":
        base -= 0.10 * overly_formal_ru
    else:
        base -= 0.15 * overly_formal_ru
    score = max(0.0, min(1.0, base))
    return {"ok": True, "score": round(score, 3),
            "overly_formal_ru_hits": overly_formal_ru,
            "overly_slangy_hits": slangy,
            "conversation_mode": conversation_mode}


def score_bilingual_identity_consistency(text: str) -> dict[str, Any]:
    ratio = lmd.estimate_language_ratio(text)
    trans = lmd.detect_code_switch_points(text)
    artifacts = _hit_count(text, _TRANSLATION_ARTIFACT_PATTERNS)
    tps = float(trans.get("transitions_per_sentence") or 0)
    score = max(0.0, min(1.0,
                          1.0 - 0.20 * artifacts - 0.10 * max(0, tps - 2.0)))
    return {"ok": True, "score": round(score, 3),
            "translation_artifact_hits": artifacts,
            "transitions_per_sentence": tps,
            "ratio": ratio}


def detect_personality_drift(text: str,
                             language_mode: str = "mixed_en_ru"
                             ) -> dict[str, Any]:
    s = (text or "").lower()
    drift: list[str] = []
    if _hit_count(text, _ROBOTIC_PATTERNS) > 0:
        drift.append("robotic_self_reference")
    if _hit_count(text, _OVERLY_FORMAL_RU_PATTERNS) > 0 \
            and language_mode in ("russian_only", "mixed_en_ru",
                                   "russian_with_english_terms"):
        drift.append("overly_formal_russian")
    if _hit_count(text, _OVERLY_SLANGY_PATTERNS) > 0:
        drift.append("excessive_slang")
    if _hit_count(text, _TRANSLATION_ARTIFACT_PATTERNS) > 0:
        drift.append("word_for_word_translation")
    return {"ok": True,
            "drift_detected": bool(drift),
            "drift_kinds": drift,
            "language_mode": language_mode}


def suggest_personality_corrections(text: str,
                                    language_mode: str = "mixed_en_ru",
                                    limit: int = 5) -> dict[str, Any]:
    cap = max(1, min(int(limit), 20))
    drift = detect_personality_drift(text, language_mode=language_mode)
    suggestions: list[str] = []
    for k in drift["drift_kinds"]:
        if k == "robotic_self_reference":
            suggestions.append("Replace 'as an AI' / 'my programming' with "
                               "warm grounded phrasing.")
        elif k == "overly_formal_russian":
            suggestions.append("Use conversational Russian; avoid academic "
                               "filler like 'вышеизложенное'.")
        elif k == "excessive_slang":
            suggestions.append("Reduce slang; the current register is too "
                               "casual for Luna's grounded voice.")
        elif k == "word_for_word_translation":
            suggestions.append("Rephrase mixed-language artifacts; idioms "
                               "should be native in the target language.")
    return {"ok": True, "suggestions": suggestions[:cap],
            "drift": drift}


def score_personality_continuity(text: str,
                                 language_mode: str = "mixed_en_ru",
                                 conversation_mode: str = "conversation"
                                 ) -> dict[str, Any]:
    w = score_warmth_consistency(text)
    d = score_directness_consistency(text)
    i = score_luna_identity_consistency(text)
    r = score_register_consistency(text, conversation_mode)
    b = score_bilingual_identity_consistency(text)
    overall = (w["score"] * 0.20 + d["score"] * 0.15
               + i["score"] * 0.20 + r["score"] * 0.20
               + b["score"] * 0.25)
    drift = detect_personality_drift(text, language_mode)
    return {"ok": True,
            "overall_score": round(overall, 3),
            "verdict": "luna_consistent" if overall >= 0.75
                       else "passable" if overall >= 0.55
                       else "drift",
            "warmth": w, "directness": d, "identity": i,
            "register": r, "bilingual": b, "drift": drift}


def write_personality_continuity_report(report: dict[str, Any],
                                        output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "score_personality_continuity",
    "score_warmth_consistency",
    "score_directness_consistency",
    "score_luna_identity_consistency",
    "score_register_consistency",
    "score_bilingual_identity_consistency",
    "detect_personality_drift",
    "suggest_personality_corrections",
    "write_personality_continuity_report",
]
