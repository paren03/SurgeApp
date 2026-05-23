"""Phase 23 - Bilingual Human Switch Runtime.

Single standalone entry point for future Luna integration. NOT wired into
Luna runtime yet. Every call is bounded, safety-policy-aware, and read-only
against the production lexicons.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_language_mode_detector as lmd
import bilingual_code_switch_policy as pol
import bilingual_style_mixer as mix
import bilingual_conversation_state as cstate
import bilingual_response_quality as rq
import bilingual_retrieval_bridge as brb


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


def choose_human_language_style(user_text: str,
                                conversation_state: Optional[dict[str, Any]] = None,
                                mode: str = "conversation",
                                user_preference: Optional[str] = None
                                ) -> dict[str, Any]:
    detected = lmd.classify_language_mode(user_text)
    chosen = pol.choose_response_language_mode(
        user_text, detected["mode"], user_preference=user_preference,
        context={"policy": mode})
    sw = pol.should_code_switch(
        user_text, detected["mode"], user_preference=user_preference,
        context={"policy": mode})
    gran = pol.choose_switch_granularity(
        user_text, detected["mode"], user_preference=user_preference,
        context={"policy": mode})
    return {"ok": True,
            "detected_language_mode": detected["mode"],
            "chosen_response_mode": chosen["response_mode"],
            "switch_granularity": gran["granularity"],
            "should_code_switch": sw["switch"],
            "policy_mode": mode,
            "language_ratio": detected["ratio"]}


def get_mixed_language_context(user_text: str,
                               mode: str = "conversation",
                               limit: int = _DEFAULT_LIMIT,
                               is_user_prompted: bool = False,
                               link_db_path: Optional[str] = None
                               ) -> dict[str, Any]:
    cap = _clamp(limit)
    source_language = "ru" if any("Ѐ" <= c <= "ӿ" for c in (user_text or "")) \
        else "en"
    target_language = "en" if source_language == "ru" else "ru"
    ctx = brb.get_bilingual_context(
        user_text, source_language=source_language,
        target_language=target_language,
        mode=mode, limit=cap, is_user_prompted=is_user_prompted,
        link_db_path=link_db_path)
    return ctx


def get_bilingual_response_plan(user_text: str,
                                conversation_state: Optional[dict[str, Any]] = None,
                                mode: str = "conversation",
                                user_preference: Optional[str] = None,
                                limit: int = _DEFAULT_LIMIT,
                                is_user_prompted: bool = False,
                                link_db_path: Optional[str] = None
                                ) -> dict[str, Any]:
    cap = _clamp(limit)
    style = choose_human_language_style(user_text,
                                          conversation_state=conversation_state,
                                          mode=mode,
                                          user_preference=user_preference)
    ctx = get_mixed_language_context(user_text, mode=mode, limit=cap,
                                      is_user_prompted=is_user_prompted,
                                      link_db_path=link_db_path)
    plan = mix.build_code_switch_plan(
        user_text,
        target_mode=style["chosen_response_mode"],
        conversation_mode=mode, limit=cap,
        is_user_prompted=is_user_prompted)
    # Updated state (caller may choose to keep it)
    state_in = conversation_state if isinstance(conversation_state, dict) \
        else cstate.create_conversation_language_state()
    detected_full = {"mode": style["detected_language_mode"],
                     "ratio": style["language_ratio"],
                     "transitions": plan.get("transitions", {}),
                     "preference": plan.get("preference", {})}
    state_out = cstate.update_language_state(
        dict(state_in), user_text, detected_full,
        style["chosen_response_mode"])
    gap_notes = ctx.get("gap_explanation")
    return {
        "ok": True,
        "detected_language_mode": style["detected_language_mode"],
        "chosen_response_mode": style["chosen_response_mode"],
        "switch_granularity": style["switch_granularity"],
        "should_code_switch": style["should_code_switch"],
        "language_ratio": style["language_ratio"],
        "bilingual_context": ctx.get("context", {"count": 0,
                                                  "entries": []}),
        "switch_terms": plan.get("switch_terms", {"switch_terms": []}),
        "style_plan": {
            "policy_mode": plan.get("policy_mode"),
            "target_response_mode": plan.get("target_response_mode"),
            "granularity": plan.get("granularity"),
        },
        "safety_summary": ctx.get("safety_summary", {}),
        "quality_notes": [
            "plan only - not final writing",
            "respects safety filter and policy mode",
        ],
        "updated_conversation_state": state_out,
        "gap_notes": gap_notes,
    }


def evaluate_bilingual_output(text: str,
                              target_mode: str = "mixed_en_ru",
                              mode: str = "conversation",
                              is_user_prompted: bool = False
                              ) -> dict[str, Any]:
    return rq.quality_check_bilingual_response(
        text, target_mode=target_mode, mode=mode,
        is_user_prompted=is_user_prompted)


def demo_code_switch_examples(limit: int = 10) -> dict[str, Any]:
    cap = _clamp(limit)
    examples = [
        ("Hello, my name is Anna. Я инженер.", "mixed_en_ru"),
        ("Привет, как дела? I'm doing fine.", "mixed_en_ru"),
        ("Tell me a story about the lighthouse.", "english_only"),
        ("Расскажи мне про маяк.", "russian_only"),
        ("Use the engineer concept but explain it in Russian please.",
         "russian_with_english_terms"),
        ("Translate 'verse' into Russian.", "translation_help"),
        ("Mix English and Russian for practice.", "bilingual_practice"),
        ("Skazhi mne po-russki, kak dela.", "transliterated_russian"),
        ("The инженер reviewed the schematic.",
         "english_with_russian_terms"),
        ("ledger -- бюджет", "mixed_en_ru"),
    ][:cap]
    out: list[dict[str, Any]] = []
    for text, expected in examples:
        detected = lmd.classify_language_mode(text)
        chosen = pol.choose_response_language_mode(text, detected["mode"])
        out.append({"text": text, "expected_label": expected,
                    "detected_mode": detected["mode"],
                    "chosen_response_mode": chosen["response_mode"]})
    return {"ok": True, "examples": out, "count": len(out)}


def write_bilingual_runtime_report(report: dict[str, Any],
                                   output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_bilingual_response_plan",
    "get_mixed_language_context",
    "choose_human_language_style",
    "evaluate_bilingual_output",
    "demo_code_switch_examples",
    "write_bilingual_runtime_report",
]
