"""Phase 27 - Piper-shaped adapter contract (dry-run only).

Defines what a future neural-renderer-backed adapter would look like.
This module performs no engine binding, no process spawn, no audio
generation, and no file write. Mapping returns a PLAN only.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_adapter_contract as vac


_PIPER_VERSION = "phase27.piper_shaped.v1"


def get_piper_shaped_capabilities() -> dict[str, Any]:
    return {
        "version": _PIPER_VERSION,
        "supports_languages": ["en", "ru"],
        "supports_code_switching": False,
        "supports_segments": True,
        "supports_prosody": True,
        "supports_pronunciation_hints": True,
        "supports_emotion": False,
        "supports_streaming": True,
        "notes": [
            "Phase 27 shape only. No engine binding here.",
            "No process spawn, no wav generation, no audio file write.",
        ],
    }


def create_piper_shaped_descriptor(dry_run: bool = True) -> dict[str, Any]:
    caps = get_piper_shaped_capabilities()
    return vac.create_voice_adapter_descriptor(
        "piper_shaped_dry_run", "piper_shaped",
        capabilities={
            "supports_languages": caps["supports_languages"],
            "supports_code_switching": caps["supports_code_switching"],
            "supports_segments": caps["supports_segments"],
            "supports_prosody": caps["supports_prosody"],
            "supports_pronunciation_hints":
                caps["supports_pronunciation_hints"],
            "supports_emotion": caps["supports_emotion"],
            "supports_streaming": caps["supports_streaming"],
        },
        constraints={
            "notes": "phase27 piper-shaped dry-run; no engine bound",
        },
        dry_run=bool(dry_run) and True,
    )


def validate_piper_payload_compatibility(payload: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(payload, dict):
        return {"ok": False, "reasons": ["payload_not_dict"]}
    mode = str(payload.get("language_mode") or "").lower()
    if not mode:
        reasons.append("missing_language_mode")
    if "mixed" in mode and "en_ru" not in mode and \
            mode not in ("english_with_russian_terms",
                         "russian_with_english_terms"):
        # piper-shaped does not natively code-switch; mixed is a soft warning.
        reasons.append("mixed_mode_no_native_code_switch")
    segs = payload.get("segments") or []
    if not isinstance(segs, list):
        reasons.append("segments_not_list")
    elif len(segs) > 200:
        reasons.append("segments_exceed_cap")
    safety = payload.get("safety_summary") or {}
    if safety.get("blocked") or safety.get("unsafe"):
        reasons.append("payload_unsafe")
        return {"ok": False, "reasons": reasons}
    return {"ok": "mixed_mode_no_native_code_switch" not in reasons
            and not any(r for r in reasons if r != "mixed_mode_no_native_code_switch"),
            "reasons": reasons}


def map_payload_to_piper_plan(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "plan": {},
            "unsupported_features": ["payload_not_dict"],
            "dry_run": True,
        }
    segs = payload.get("segments") or []
    lang_segments: list[dict[str, Any]] = []
    prosody_notes: list[dict[str, Any]] = []
    pause_plan: list[dict[str, Any]] = []
    pronunciation_notes: list[dict[str, Any]] = []
    unsupported: list[str] = []
    for s in (segs if isinstance(segs, list) else []):
        if not isinstance(s, dict):
            continue
        lang = str(s.get("language") or "und").lower()
        if lang not in ("en", "ru"):
            unsupported.append(f"unsupported_segment_language:{lang}")
            continue
        lang_segments.append({
            "segment_id": s.get("segment_id"),
            "language": lang,
            "text": s.get("text", ""),
            "voice_id_hint": f"piper_{lang}_default",
        })
        if s.get("emphasis") and s.get("emphasis") != "normal":
            prosody_notes.append({
                "segment_id": s.get("segment_id"),
                "emphasis": s.get("emphasis"),
                "tone": s.get("tone", "steady"),
                "pace": s.get("pace", "normal"),
            })
        if s.get("pause_after_ms"):
            pause_plan.append({
                "after_segment_id": s.get("segment_id"),
                "pause_ms": int(s.get("pause_after_ms") or 0),
            })
        ph = s.get("pronunciation_hint") or ""
        if ph:
            pronunciation_notes.append({
                "segment_id": s.get("segment_id"),
                "hint": ph,
            })
    mode = str(payload.get("language_mode") or "").lower()
    if mode in ("mixed", "mixed_en_ru", "english_with_russian_terms",
                "russian_with_english_terms"):
        unsupported.append("native_code_switching")
    return {
        "ok": True,
        "plan": {
            "language_segments": lang_segments,
            "prosody_notes": prosody_notes,
            "pause_plan": pause_plan,
            "pronunciation_notes": pronunciation_notes,
        },
        "unsupported_features": unsupported,
        "dry_run": True,
        "adapter_type": "piper_shaped",
        "version": _PIPER_VERSION,
    }


def simulate_piper_acceptance(payload: dict[str, Any]) -> dict[str, Any]:
    compat = validate_piper_payload_compatibility(payload)
    plan = map_payload_to_piper_plan(payload)
    return {
        "accepted_in_simulation": compat["ok"] and plan["ok"],
        "compatibility_reasons": compat["reasons"],
        "plan_unsupported_features": plan.get("unsupported_features", []),
        "dry_run": True,
        "adapter_type": "piper_shaped",
        "no_audio_generated": True,
        "no_process_spawn": True,
        "no_audio_file_written": True,
    }


def write_piper_contract_report(
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
    "get_piper_shaped_capabilities",
    "create_piper_shaped_descriptor",
    "validate_piper_payload_compatibility",
    "map_payload_to_piper_plan",
    "simulate_piper_acceptance",
    "write_piper_contract_report",
]
