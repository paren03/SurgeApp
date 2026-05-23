"""Phase 25 - Bilingual Spoken Safety Redactor.

Voice-specific spoken safety filtering. Stricter than text mode. Replaces
unsafe terms with transparent placeholders and emits redaction metadata.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional


_SPOKEN_UNSAFE_MARKERS = (
    "step by step instructions to bypass",
    "ignore previous instructions",
    "system prompt:",
    "kill yourself", "kill themselves", "self-harm", "harm yourself",
    "how to make a bomb", "how to make explosives",
)


_DEFAULT_REPLACEMENT = "[voice-safe wording]"


def _coerce_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            d = json.loads(v)
            return [str(x) for x in d] if isinstance(d, list) else []
        except Exception:
            return []
    return []


def classify_spoken_safety_risk(text: str,
                                language_mode: str,
                                conversation_mode: str
                                ) -> dict[str, Any]:
    s = (text or "").lower()
    risks: list[str] = []
    for m in _SPOKEN_UNSAFE_MARKERS:
        if m in s:
            risks.append(f"operational_unsafe:{m}")
    # Slang/vulgar tokens in non-slang modes
    if conversation_mode in ("teacher", "professional", "technical",
                              "curriculum"):
        for tok in (" yo ", " bruh ", " лол ", " кек "):
            if tok in f" {s} ":
                risks.append(f"slang_in_clean_mode:{tok.strip()}")
    return {"ok": not risks, "risks": risks[:20],
            "language_mode": language_mode,
            "conversation_mode": conversation_mode,
            "high_risk": any(r.startswith("operational_unsafe")
                              for r in risks)}


def replace_unsafe_spoken_terms(text: str,
                                replacement: str = _DEFAULT_REPLACEMENT
                                ) -> dict[str, Any]:
    s = text or ""
    replacements: list[dict[str, str]] = []
    out = s
    for m in _SPOKEN_UNSAFE_MARKERS:
        if m in out.lower():
            # Case-insensitive replace
            pattern = re.compile(re.escape(m), flags=re.IGNORECASE)
            out, n = pattern.subn(replacement, out)
            if n > 0:
                replacements.append({"from": m, "to": replacement,
                                     "count": int(n)})
    return {"ok": True, "text": out, "replacements": replacements}


def redact_for_spoken_voice(text: str,
                            language_mode: str = "mixed_en_ru",
                            conversation_mode: str = "conversation",
                            is_user_prompted: bool = False
                            ) -> dict[str, Any]:
    risk = classify_spoken_safety_risk(text, language_mode,
                                         conversation_mode)
    repl = replace_unsafe_spoken_terms(text or "")
    decision = {
        "original_chars": len(text or ""),
        "voice_safe_text": repl["text"],
        "replacements": repl["replacements"],
        "risk": risk,
        "is_user_prompted": is_user_prompted,
        "language_mode": language_mode,
        "conversation_mode": conversation_mode,
        "unsafe_leakage_detected": risk["high_risk"],
    }
    return {"ok": True, "decision": decision,
            "voice_safe_text": decision["voice_safe_text"],
            "safety_summary": {
                "replacements_count": len(repl["replacements"]),
                "high_risk": risk["high_risk"],
                "risks": risk["risks"],
                "unsafe_leakage_detected": risk["high_risk"],
            }}


def redact_segments_for_voice(segments: list[dict[str, Any]],
                              conversation_mode: str = "conversation",
                              is_user_prompted: bool = False
                              ) -> dict[str, Any]:
    """Apply spoken-mode safety filter per segment based on its
    safety_tags / register_tags. Suggestion-only-recognized rows are kept
    but flagged."""
    safe: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    flagged: list[dict[str, Any]] = []
    for s in segments or []:
        if not isinstance(s, dict):
            continue
        sf = set(_coerce_list(s.get("safety_tags")
                              or s.get("safety_tags_json") or []))
        rg = set(_coerce_list(s.get("register_tags")
                              or s.get("register_tags_json") or []))
        if "do_not_use_unprompted" in sf and not is_user_prompted:
            blocked.append({"segment_id": s.get("segment_id"),
                             "reason": "do_not_use_unprompted"})
            continue
        if ({"vulgar", "offensive"} & (sf | rg)) and \
                conversation_mode in ("conversation", "teacher",
                                       "professional", "technical",
                                       "curriculum", "warm_friend",
                                       "concise"):
            blocked.append({"segment_id": s.get("segment_id"),
                             "reason": "vulgar_or_offensive_in_clean_mode"})
            continue
        if ({"vulgar", "offensive"} & (sf | rg)) and not is_user_prompted:
            blocked.append({"segment_id": s.get("segment_id"),
                             "reason": "vulgar_or_offensive_unprompted"})
            continue
        s2 = dict(s)
        if "recognition_only" in sf:
            s2["_suggestion_blocked"] = True
            flagged.append({"segment_id": s.get("segment_id"),
                             "reason": "recognition_only_recognized"})
        safe.append(s2)
    return {"ok": True, "safe": safe, "blocked": blocked,
            "flagged_recognition_only": flagged,
            "safe_count": len(safe),
            "blocked_count": len(blocked),
            "flagged_count": len(flagged)}


def explain_spoken_redaction(decision: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(decision, dict):
        return {"explanation": "no_decision_dict"}
    parts = [
        f"Replacements: {len(decision.get('replacements', []))}",
        f"High risk: {decision.get('risk', {}).get('high_risk', False)}",
        f"Conversation mode: {decision.get('conversation_mode')}",
        f"User prompted: {decision.get('is_user_prompted', False)}",
    ]
    return {"explanation": "; ".join(parts), "input": decision}


def validate_voice_safe_text(text: str,
                             language_mode: str = "mixed_en_ru",
                             conversation_mode: str = "conversation",
                             is_user_prompted: bool = False
                             ) -> dict[str, Any]:
    r = classify_spoken_safety_risk(text, language_mode, conversation_mode)
    return {"ok": not r["high_risk"], "risk": r,
            "is_user_prompted": is_user_prompted}


def write_spoken_safety_report(report: dict[str, Any],
                               output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "redact_for_spoken_voice",
    "redact_segments_for_voice",
    "classify_spoken_safety_risk",
    "replace_unsafe_spoken_terms",
    "explain_spoken_redaction",
    "validate_voice_safe_text",
    "write_spoken_safety_report",
]
