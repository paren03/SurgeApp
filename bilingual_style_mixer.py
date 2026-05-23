"""Phase 23 - Bilingual Style Mixer.

Style planner (NOT a generative model). Produces bounded code-switch plans,
templates, and term-substitution proposals. Uses the bilingual link bridge
where useful. Always returns plans, never claims final perfect writing.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_retrieval_bridge as brb
import bilingual_code_switch_policy as pol
import bilingual_language_mode_detector as lmd


_DEFAULT_LIMIT = 25
_HARD_LIMIT = 100


def _clamp(n: Optional[int]) -> int:
    if n is None:
        return _DEFAULT_LIMIT
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_LIMIT
    return max(1, min(v, _HARD_LIMIT))


def build_code_switch_plan(user_text: str,
                           target_mode: str = "auto",
                           conversation_mode: str = "conversation",
                           limit: int = _DEFAULT_LIMIT,
                           is_user_prompted: bool = False
                           ) -> dict[str, Any]:
    cap = _clamp(limit)
    detected = lmd.classify_language_mode(user_text)
    if target_mode == "auto":
        chosen = pol.choose_response_language_mode(
            user_text, detected["mode"])
        target_mode = chosen["response_mode"]
    gran = pol.choose_switch_granularity(
        user_text, detected["mode"],
        context={"policy": conversation_mode})
    sw = pol.should_code_switch(
        user_text, detected["mode"],
        context={"policy": conversation_mode})
    terms = select_switch_terms(user_text, target_mode, limit=cap,
                                 is_user_prompted=is_user_prompted)
    return {"ok": True,
            "user_text_preview": (user_text or "")[:240],
            "detected_mode": detected["mode"],
            "target_response_mode": target_mode,
            "policy_mode": conversation_mode,
            "should_code_switch": sw,
            "granularity": gran,
            "switch_terms": terms,
            "language_ratio": detected["ratio"],
            "transitions": detected["transitions"],
            "preference": detected["preference"]}


def select_switch_terms(user_text: str,
                        target_language_mix: str,
                        limit: int = 10,
                        is_user_prompted: bool = False,
                        conversation_mode: str = "conversation",
                        link_db_path: Optional[str] = None
                        ) -> dict[str, Any]:
    cap = _clamp(limit)
    if target_language_mix in ("english_only", "russian_only", "unknown"):
        return {"ok": True, "switch_terms": [], "limit": cap,
                "reason": "no_switch_for_target"}
    source_language = "ru" if any("Ѐ" <= c <= "ӿ" for c in (user_text or "")) else "en"
    target_language = "en" if source_language == "ru" else "ru"
    ctx = brb.get_bilingual_context(
        user_text, source_language=source_language,
        target_language=target_language,
        mode=conversation_mode, limit=cap,
        is_user_prompted=is_user_prompted, link_db_path=link_db_path)
    if not ctx.get("ok") or ctx["context"]["count"] == 0:
        return {"ok": True, "switch_terms": [], "limit": cap,
                "reason": "no_bilingual_links",
                "bridge_summary": ctx.get("safety_summary")}
    raw = ctx["context"]["entries"]
    filt = pol.filter_switch_candidates(raw, mode=conversation_mode,
                                         is_user_prompted=is_user_prompted)
    terms: list[dict[str, Any]] = []
    for e in filt["safe"]:
        terms.append({
            "source": e.get("source_word"),
            "target": e.get("target_word") or e.get("target_phrase"),
            "concept_id": e.get("concept_id"),
            "confidence": e.get("confidence"),
            "register_tags": e.get("register_tags") or [],
            "safety_tags": e.get("safety_tags") or [],
            "suggestion_blocked": bool(e.get("_suggestion_blocked")),
        })
        if len(terms) >= cap:
            break
    return {"ok": True, "switch_terms": terms[:cap], "limit": cap,
            "blocked_count": filt["blocked_count"]}


def apply_light_code_switch(text: str,
                            switch_terms: list[dict[str, Any]],
                            target_mode: str = "mixed_en_ru") -> dict[str, Any]:
    """Return a planning structure showing where switches would land.
    Does NOT claim to produce final writing."""
    plan_steps: list[dict[str, Any]] = []
    s = text or ""
    for t in (switch_terms or []):
        if not isinstance(t, dict):
            continue
        if t.get("suggestion_blocked"):
            continue
        src = (t.get("source") or "").strip()
        tgt = (t.get("target") or "").strip()
        if not src or not tgt:
            continue
        idx = s.lower().find(src.lower())
        if idx >= 0:
            plan_steps.append({
                "action": "substitute_term",
                "from": src, "to": tgt,
                "at_index": idx,
                "confidence": t.get("confidence"),
                "concept_id": t.get("concept_id"),
            })
    return {"ok": True, "target_mode": target_mode,
            "text_preview": s[:240],
            "n_proposed": len(plan_steps),
            "proposed": plan_steps[:25]}


def apply_sentence_level_switch(english_text: str,
                                russian_text: str,
                                pattern: str = "balanced") -> dict[str, Any]:
    """Plan alternation between EN and RU at sentence boundaries.
    Pattern ∈ {balanced, en_first, ru_first}."""
    if pattern not in ("balanced", "en_first", "ru_first"):
        pattern = "balanced"
    en_sents = [x.strip() for x in re.split(r"[.!?\n]+", english_text or "")
                if x.strip()]
    ru_sents = [x.strip() for x in re.split(r"[.!?\n]+", russian_text or "")
                if x.strip()]
    out: list[dict[str, Any]] = []
    if pattern == "en_first":
        seq = [("en", s) for s in en_sents] + [("ru", s) for s in ru_sents]
    elif pattern == "ru_first":
        seq = [("ru", s) for s in ru_sents] + [("en", s) for s in en_sents]
    else:
        # interleave
        seq = []
        for i in range(max(len(en_sents), len(ru_sents))):
            if i < len(en_sents):
                seq.append(("en", en_sents[i]))
            if i < len(ru_sents):
                seq.append(("ru", ru_sents[i]))
    for lang, s in seq[:50]:
        out.append({"lang": lang, "sentence": s})
    return {"ok": True, "pattern": pattern, "plan": out,
            "n_steps": len(out)}


def apply_phrase_level_switch(text: str,
                              phrase_pairs: list[tuple[str, str]],
                              target_mode: str = "mixed_en_ru"
                              ) -> dict[str, Any]:
    s = text or ""
    plan: list[dict[str, Any]] = []
    for pair in (phrase_pairs or [])[:50]:
        try:
            en, ru = pair
        except Exception:
            continue
        en = (en or "").strip()
        ru = (ru or "").strip()
        if not en or not ru:
            continue
        idx = s.lower().find(en.lower())
        if idx >= 0:
            plan.append({"action": "substitute_phrase",
                         "from": en, "to": ru, "at_index": idx})
    return {"ok": True, "target_mode": target_mode,
            "n_proposed": len(plan), "proposed": plan[:25]}


def preserve_user_mixed_style(user_text: str,
                              response_text: str,
                              limit: int = _DEFAULT_LIMIT) -> dict[str, Any]:
    """Compare user's en/ru ratio to response's and suggest light corrections
    so Luna's output mirrors the user's mix without robotic translation."""
    cap = _clamp(limit)
    u = lmd.estimate_language_ratio(user_text)
    r = lmd.estimate_language_ratio(response_text)
    suggestions: list[str] = []
    if u["english_ratio"] > 0.6 and r["english_ratio"] < 0.5:
        suggestions.append("response_too_russian_for_english_dominant_user")
    if u["russian_ratio"] > 0.6 and r["russian_ratio"] < 0.5:
        suggestions.append("response_too_english_for_russian_dominant_user")
    if u["english_ratio"] > 0.2 and u["russian_ratio"] > 0.2 \
            and (r["english_ratio"] < 0.1 or r["russian_ratio"] < 0.1):
        suggestions.append("response_should_mirror_user_mix")
    return {"ok": True, "user_ratio": u, "response_ratio": r,
            "suggestions": suggestions[:cap]}


_AWKWARD_PATTERNS = (
    # Common "robotic translation" tells
    r"\bпожалуйста, do\b",
    r"\bтот же as\b",
    r"\bочень important\b",
    r"\bvery важно\b",
)


def avoid_awkward_switching(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    hits = [p for p in _AWKWARD_PATTERNS if re.search(p, s)]
    return {"ok": True, "awkward_patterns": hits,
            "awkward_detected": bool(hits)}


def score_code_switch_naturalness(text: str,
                                  target_mode: str = "mixed_en_ru"
                                  ) -> dict[str, Any]:
    ratio = lmd.estimate_language_ratio(text)
    trans = lmd.detect_code_switch_points(text)
    awk = avoid_awkward_switching(text)
    # Naturalness heuristic: penalize too-many transitions per sentence and
    # awkward patterns. Reward balanced ratios in mixed_en_ru targets.
    transitions_per_sentence = float(trans.get("transitions_per_sentence") or 0)
    excess = max(0.0, transitions_per_sentence - 2.0)
    awkward_penalty = 0.15 if awk["awkward_detected"] else 0.0
    if target_mode == "mixed_en_ru":
        balance = 1.0 - abs(ratio["english_ratio"] - ratio["russian_ratio"])
    elif target_mode == "english_only":
        balance = ratio["english_ratio"]
    elif target_mode == "russian_only":
        balance = ratio["russian_ratio"]
    else:
        balance = 0.5
    score = max(0.0, min(1.0,
                          balance - 0.10 * excess - awkward_penalty))
    verdict = "natural" if score >= 0.65 else \
              "passable" if score >= 0.45 else "awkward"
    return {"ok": True, "target_mode": target_mode,
            "score": round(score, 3), "verdict": verdict,
            "ratio": ratio, "transitions": trans, "awkward": awk}


def write_style_mixer_report(report: dict[str, Any],
                             output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "build_code_switch_plan",
    "select_switch_terms",
    "apply_light_code_switch",
    "apply_sentence_level_switch",
    "apply_phrase_level_switch",
    "preserve_user_mixed_style",
    "avoid_awkward_switching",
    "score_code_switch_naturalness",
    "write_style_mixer_report",
]
