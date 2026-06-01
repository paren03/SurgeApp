"""Phase 28 - Capability Negotiator.

Compares spoken-render payload requirements vs adapter capabilities.
Proposes safe downgrade metadata only. Never strips safety metadata or
language labels. Rejects adapters that lack safety support.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_MIXED_MODES = {
    "mixed", "mixed_en_ru", "english_with_russian_terms",
    "russian_with_english_terms",
}
_RU_MODES = {"russian", "russian_only", "russian_with_english_terms"}
_EN_MODES = {"english", "english_only", "english_with_russian_terms"}


def extract_payload_requirements(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "language_mode": "",
            "requires_russian": False,
            "requires_english": False,
            "requires_code_switching": False,
            "requires_segments": False,
            "requires_prosody": False,
            "requires_pronunciation_hints": False,
            "requires_emotional_tone": False,
            "requires_safety_redaction": True,
            "text_chars": 0,
            "segment_count": 0,
            "requires_dry_run": True,
        }
    mode = str(payload.get("language_mode") or "").lower()
    segs = payload.get("segments") or []
    seg_count = len(segs) if isinstance(segs, list) else 0
    pron_notes = payload.get("pronunciation_notes") or []
    prosody = payload.get("prosody") or {}
    text = payload.get("voice_safe_text") or payload.get(
        "normalized_text") or ""
    csb = payload.get("code_switch_boundaries") or []
    safety = payload.get("safety_summary") or {}
    requires_ru = mode in _RU_MODES or mode in _MIXED_MODES or \
        any(isinstance(s, dict) and str(s.get("language") or "")
            .lower() == "ru" for s in segs)
    requires_en = mode in _EN_MODES or mode in _MIXED_MODES or \
        any(isinstance(s, dict) and str(s.get("language") or "")
            .lower() == "en" for s in segs)
    requires_cs = bool(csb) or mode in _MIXED_MODES
    return {
        "language_mode": mode,
        "requires_russian": requires_ru,
        "requires_english": requires_en,
        "requires_code_switching": requires_cs,
        "requires_segments": seg_count > 0,
        "requires_prosody": bool(prosody),
        "requires_pronunciation_hints": bool(pron_notes),
        "requires_emotional_tone": False,
        "requires_safety_redaction": True,
        "text_chars": len(str(text or "")),
        "segment_count": seg_count,
        "requires_dry_run": True,
        "safety_summary": safety,
    }


def extract_adapter_capabilities(
    adapter_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(adapter_descriptor, dict):
        return {
            "supports_languages": [],
            "supports_code_switching": False,
            "supports_segments": False,
            "supports_prosody": False,
            "supports_pronunciation_hints": False,
            "supports_emotion": False,
            "supports_streaming": False,
            "max_text_chars": 0,
            "max_segments": 0,
            "supports_safety_redaction": False,
            "dry_run": False,
            "adapter_name": "",
            "adapter_type": "",
        }
    fra = {str(x).lower() for x in adapter_descriptor.get(
        "forbidden_runtime_actions") or []}
    supports_safety = ("audio_generation" in fra
                       and "tts_invocation" in fra)
    return {
        "supports_languages": list(adapter_descriptor.get(
            "supports_languages") or []),
        "supports_code_switching": bool(adapter_descriptor.get(
            "supports_code_switching")),
        "supports_segments": bool(adapter_descriptor.get(
            "supports_segments")),
        "supports_prosody": bool(adapter_descriptor.get(
            "supports_prosody")),
        "supports_pronunciation_hints": bool(adapter_descriptor.get(
            "supports_pronunciation_hints")),
        "supports_emotion": bool(adapter_descriptor.get(
            "supports_emotion")),
        "supports_streaming": bool(adapter_descriptor.get(
            "supports_streaming")),
        "max_text_chars": int(adapter_descriptor.get(
            "max_text_chars", 0) or 0),
        "max_segments": int(adapter_descriptor.get(
            "max_segments", 0) or 0),
        "supports_safety_redaction": supports_safety,
        "dry_run": adapter_descriptor.get("dry_run") is True,
        "adapter_name": adapter_descriptor.get("adapter_name") or "",
        "adapter_type": adapter_descriptor.get("adapter_type") or "",
    }


def identify_unsupported_features(
    payload: Any,
    adapter_descriptor: Any,
) -> list[str]:
    req = extract_payload_requirements(payload)
    caps = extract_adapter_capabilities(adapter_descriptor)
    unsupported: list[str] = []
    supports = {str(x).lower() for x in caps.get(
        "supports_languages") or []}
    if req["requires_russian"] and not (
            "ru" in supports or "mixed" in supports):
        unsupported.append("language:russian")
    if req["requires_english"] and not (
            "en" in supports or "mixed" in supports):
        unsupported.append("language:english")
    if req["requires_code_switching"] and not caps[
            "supports_code_switching"]:
        unsupported.append("feature:code_switching")
    if req["requires_prosody"] and not caps["supports_prosody"]:
        unsupported.append("feature:prosody")
    if req["requires_pronunciation_hints"] and not caps[
            "supports_pronunciation_hints"]:
        unsupported.append("feature:pronunciation_hints")
    if req["requires_segments"] and not caps["supports_segments"]:
        unsupported.append("feature:segments")
    if caps["max_text_chars"] and req["text_chars"] > caps[
            "max_text_chars"]:
        unsupported.append("limit:text_chars_exceeded")
    if caps["max_segments"] and req["segment_count"] > caps[
            "max_segments"]:
        unsupported.append("limit:segment_count_exceeded")
    if not caps["supports_safety_redaction"]:
        unsupported.append("feature:safety_redaction")
    if not caps["dry_run"]:
        unsupported.append("policy:dry_run_required")
    return unsupported


def negotiate_capabilities(
    payload: Any,
    adapter_descriptor: Any,
) -> dict[str, Any]:
    req = extract_payload_requirements(payload)
    caps = extract_adapter_capabilities(adapter_descriptor)
    unsupported = identify_unsupported_features(payload, adapter_descriptor)
    # Hard rejection if adapter lacks safety support
    if "feature:safety_redaction" in unsupported:
        return {
            "ok": False,
            "rejected": True,
            "reason": "adapter_missing_safety_redaction",
            "requirements": req,
            "capabilities": caps,
            "unsupported_features": unsupported,
            "downgrade_plan": {},
            "phase": "phase28",
        }
    downgrade = propose_safe_downgrade_plan(payload, adapter_descriptor)
    return {
        "ok": not unsupported,
        "rejected": False,
        "reason": ("" if not unsupported else "capability_mismatch"),
        "requirements": req,
        "capabilities": caps,
        "unsupported_features": unsupported,
        "downgrade_plan": downgrade,
        "phase": "phase28",
    }


def propose_safe_downgrade_plan(
    payload: Any,
    adapter_descriptor: Any,
) -> dict[str, Any]:
    """Build a metadata-only downgrade hint. Never mutates payload.
    Never strips safety metadata. Never strips language labels."""
    unsupported = identify_unsupported_features(payload, adapter_descriptor)
    notes: list[str] = []
    plan: dict[str, Any] = {
        "strip_safety_metadata": False,
        "strip_language_labels": False,
        "annotate_unsupported_features": list(unsupported),
        "notes": notes,
        "phase": "phase28",
    }
    if "feature:code_switching" in unsupported:
        notes.append("future: pass single-language segment runs separately")
    if "feature:prosody" in unsupported:
        notes.append("future: drop prosody markup from adapter call only")
    if "feature:pronunciation_hints" in unsupported:
        notes.append("future: emit hints as metadata sidecar, not inline")
    if "limit:text_chars_exceeded" in unsupported:
        notes.append("future: chunk text under adapter cap; do not splice "
                     "across safety boundaries")
    if "limit:segment_count_exceeded" in unsupported:
        notes.append("future: split into batches under adapter segment cap")
    return plan


def score_negotiation_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"score": 0.0, "ok": False}
    if result.get("rejected"):
        return {"score": 0.0, "ok": False,
                "reasons": [result.get("reason", "rejected")]}
    unsupported = result.get("unsupported_features") or []
    score = max(0.0, 1.0 - 0.1 * len(unsupported))
    return {
        "score": score,
        "ok": bool(result.get("ok")),
        "unsupported_count": len(unsupported),
    }


def write_capability_negotiation_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "extract_payload_requirements",
    "extract_adapter_capabilities",
    "identify_unsupported_features",
    "negotiate_capabilities",
    "propose_safe_downgrade_plan",
    "score_negotiation_result",
    "write_capability_negotiation_report",
]
