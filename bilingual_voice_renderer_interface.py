"""Phase 25 - Bilingual Voice Renderer Interface.

Defines a future renderer adapter interface WITHOUT implementing any TTS,
audio, or subprocess invocation. Dry-run only. No engine binding.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


REQUEST_REQUIRED_FIELDS = (
    "renderer_name",
    "render_payload",
    "accepted_languages",
    "supports_code_switching",
    "supports_prosody",
    "supports_pronunciation_hints",
    "supports_emotional_tone",
    "max_text_chars",
    "max_segments",
    "output_format_requested",
    "dry_run",
)


_VALID_OUTPUT_FORMATS = ("audio_wav", "audio_mp3", "audio_ogg", "ssml_text",
                         "json_plan_only")


def get_voice_renderer_contract() -> dict[str, Any]:
    return {
        "version": "phase25.renderer_contract.v1",
        "binding": "UNBOUND_FUTURE_RENDERER",
        "audio_synthesis_in_this_phase": False,
        "tts_invocation_in_this_phase": False,
        "voice_clone_in_this_phase": False,
        "subprocess_invocation_in_this_phase": False,
        "request_required_fields": list(REQUEST_REQUIRED_FIELDS),
        "valid_output_formats": list(_VALID_OUTPUT_FORMATS),
        "must_pass_payload_validation": True,
        "must_set_dry_run_true_in_phase25": True,
        "notes": [
            "Renderer is not invoked in Phase 25. dry_run is always True.",
            "Future adapters must implement their own engine binding.",
            "Phase 25 only validates that a payload is renderer-acceptable.",
        ],
    }


def create_renderer_request_from_payload(payload: dict[str, Any],
                                          renderer_name: str = "unbound_future_renderer"
                                          ) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"ok": False, "reason": "payload_not_dict"}
    return {
        "request_id": f"req_{int(time.time())}_{uuid.uuid4().hex[:8]}",
        "renderer_name": str(renderer_name),
        "render_payload": payload,
        "accepted_languages": ["en", "ru"],
        "supports_code_switching": True,
        "supports_prosody": True,
        "supports_pronunciation_hints": True,
        "supports_emotional_tone": True,
        "max_text_chars": 10_000,
        "max_segments": 200,
        "output_format_requested": "json_plan_only",
        "dry_run": True,
        "created_at": time.time(),
    }


def validate_renderer_request(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {"ok": False, "reason": "request_not_dict"}
    missing = [f for f in REQUEST_REQUIRED_FIELDS if f not in request]
    if missing:
        return {"ok": False, "reason": "missing_required",
                "missing": missing}
    if request.get("dry_run") is not True:
        return {"ok": False, "reason": "dry_run_must_be_true_in_phase25"}
    if request.get("output_format_requested") not in _VALID_OUTPUT_FORMATS:
        return {"ok": False,
                "reason": "invalid_output_format",
                "format": request.get("output_format_requested")}
    pl = request.get("render_payload") or {}
    if not isinstance(pl, dict):
        return {"ok": False, "reason": "render_payload_not_dict"}
    if "voice_safe_text" not in pl:
        return {"ok": False, "reason": "payload_missing_voice_safe_text"}
    if "segments" not in pl:
        return {"ok": False, "reason": "payload_missing_segments"}
    accepted = request.get("accepted_languages") or []
    if not isinstance(accepted, list) or "en" not in accepted \
            or "ru" not in accepted:
        return {"ok": False, "reason": "accepted_languages_must_include_en_and_ru"}
    if int(request.get("max_text_chars", 0)) <= 0:
        return {"ok": False, "reason": "invalid_max_text_chars"}
    if int(request.get("max_segments", 0)) <= 0:
        return {"ok": False, "reason": "invalid_max_segments"}
    return {"ok": True}


def validate_renderer_capabilities(capabilities: Any) -> dict[str, Any]:
    if not isinstance(capabilities, dict):
        return {"ok": False, "reason": "capabilities_not_dict"}
    required = ("supports_code_switching", "supports_prosody",
                "supports_pronunciation_hints", "supports_emotional_tone",
                "accepted_languages", "max_text_chars", "max_segments")
    missing = [f for f in required if f not in capabilities]
    if missing:
        return {"ok": False, "reason": "missing_required",
                "missing": missing}
    return {"ok": True}


def simulate_renderer_acceptance(payload: dict[str, Any]) -> dict[str, Any]:
    """Return whether a hypothetical future renderer with default
    capabilities WOULD accept this payload. Does NOT invoke a renderer."""
    request = create_renderer_request_from_payload(payload)
    val = validate_renderer_request(request)
    return {"ok": val["ok"], "validation": val,
            "request": request,
            "accepted": val["ok"],
            "note": "simulation_only_no_renderer_invoked"}


def write_renderer_interface_report(report: dict[str, Any],
                                    output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_voice_renderer_contract",
    "validate_renderer_request",
    "create_renderer_request_from_payload",
    "validate_renderer_capabilities",
    "simulate_renderer_acceptance",
    "write_renderer_interface_report",
]
