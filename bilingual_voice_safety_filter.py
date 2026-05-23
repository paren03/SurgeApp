"""Phase 24 - Bilingual Voice Safety Filter.

Voice-style planning must be STRICTER than text suggestions: spoken output
is harder to retract, so the filter blocks anything Luna shouldn't say
aloud as her own wording. Wraps Phase 23 code-switch policy with extra
voice-mode strictness.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_code_switch_policy as csp


_SPOKEN_UNSAFE_MARKERS = (
    "step by step instructions to bypass",
    "ignore previous instructions",
    "system prompt:",
    "kill yourself", "kill themselves", "self-harm", "harm yourself",
)


def filter_voice_style_terms(terms: list[dict[str, Any]],
                             mode: str = "conversation",
                             is_user_prompted: bool = False
                             ) -> dict[str, Any]:
    """Strict spoken-mode filter. recognition_only NEVER returned as
    suggested wording (suggestion_blocked=True is enforced; rows pass
    through but flagged)."""
    base = csp.filter_switch_candidates(terms or [], mode=mode,
                                          is_user_prompted=is_user_prompted)
    # In spoken mode, recognition_only rows are kept as "recognized" but the
    # caller must NOT use them as Luna's wording.
    spoken_safe: list[dict[str, Any]] = []
    suggestion_blocked: list[dict[str, Any]] = []
    for e in base["safe"]:
        if e.get("_suggestion_blocked"):
            suggestion_blocked.append(e)
        else:
            spoken_safe.append(e)
    return {"ok": True,
            "spoken_safe": spoken_safe,
            "suggestion_blocked": suggestion_blocked,
            "blocked": base["blocked"],
            "spoken_safe_count": len(spoken_safe),
            "suggestion_blocked_count": len(suggestion_blocked),
            "blocked_count": base["blocked_count"]}


def check_voice_safe_register(text: str,
                              language_mode: str,
                              conversation_mode: str = "conversation",
                              is_user_prompted: bool = False
                              ) -> dict[str, Any]:
    s = (text or "").lower()
    flags: list[str] = []
    # Hard markers
    for marker in _SPOKEN_UNSAFE_MARKERS:
        if marker in s:
            flags.append(f"unsafe_marker:{marker}")
    # Teacher/professional/technical: no slang/vulgar at all
    if conversation_mode in ("teacher", "professional", "technical",
                              "curriculum"):
        for tok in (" yo ", " bruh ", " лол ", " кек "):
            if tok in f" {s} ":
                flags.append(f"slang_in_clean_mode:{tok.strip()}")
    return {"ok": not flags, "flags": flags[:10],
            "language_mode": language_mode,
            "conversation_mode": conversation_mode,
            "is_user_prompted": is_user_prompted}


def detect_spoken_unsafe_leakage(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    hits = [m for m in _SPOKEN_UNSAFE_MARKERS if m in s]
    return {"ok": not hits, "hits": hits[:5],
            "unsafe_leakage_detected": bool(hits)}


def block_voice_unsafe_entries(entries: list[dict[str, Any]],
                               mode: str = "conversation",
                               is_user_prompted: bool = False
                               ) -> dict[str, Any]:
    """Pass-through to filter_voice_style_terms but renamed for clarity."""
    return filter_voice_style_terms(entries, mode=mode,
                                     is_user_prompted=is_user_prompted)


def explain_voice_safety_decision(decision: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(decision, dict):
        return {"explanation": "no_decision_dict"}
    parts: list[str] = []
    parts.append(
        f"Allowed for spoken output: {decision.get('spoken_safe_count', 0)}")
    parts.append(
        f"Recognized but not for Luna wording: "
        f"{decision.get('suggestion_blocked_count', 0)}")
    parts.append(f"Blocked outright: {decision.get('blocked_count', 0)}")
    return {"explanation": "; ".join(parts), "input": decision}


def write_voice_safety_report(report: dict[str, Any],
                              output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "filter_voice_style_terms",
    "check_voice_safe_register",
    "detect_spoken_unsafe_leakage",
    "block_voice_unsafe_entries",
    "explain_voice_safety_decision",
    "write_voice_safety_report",
]
