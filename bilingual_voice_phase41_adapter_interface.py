"""Phase 41 - Five-Adapter Interface.

Exactly five metadata-only adapters permitted:
- dummy_metadata_adapter
- bilingual_segment_metadata_adapter
- prosody_density_metadata_adapter
- safety_redaction_trace_metadata_adapter
- memory_continuity_audit_metadata_adapter
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase41.callable_interface.v1"


ALLOWED_ADAPTER_TYPES = (
    "dummy_metadata_adapter",
    "bilingual_segment_metadata_adapter",
    "prosody_density_metadata_adapter",
    "safety_redaction_trace_metadata_adapter",
    "memory_continuity_audit_metadata_adapter",
)


_REQUIRED_DESCRIPTOR_FIELDS = (
    "adapter_name", "adapter_type", "test_only",
    "produces_audio", "invokes_tts", "uses_subprocess",
    "uses_network", "writes_files", "phase",
)


_REQUIRED_REQUEST_FIELDS = (
    "request_id", "created_at", "phase29_packet_id",
    "envelope_id", "job_id", "adapter_descriptor",
    "invocation_token_id", "operator_id_hash", "approved",
    "dry_run", "test_only", "phase",
)


# Banned request-level keys that would leak raw transcript or
# sensitive personal facts.
_BANNED_REQUEST_KEYS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "raw_text", "transcript_text",
    "sensitive_facts", "personal_facts",
)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _hash_str(s: str) -> str:
    h = hashlib.sha256()
    h.update(str(s or "").encode("utf-8"))
    return h.hexdigest()


def get_phase41_callable_adapter_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "allowed_adapter_types": list(ALLOWED_ADAPTER_TYPES),
        "required_descriptor_fields":
            list(_REQUIRED_DESCRIPTOR_FIELDS),
        "required_request_fields":
            list(_REQUIRED_REQUEST_FIELDS),
        "banned_request_keys":
            list(_BANNED_REQUEST_KEYS),
        "test_only_required": True,
        "produces_audio_required_false": True,
        "invokes_tts_required_false": True,
        "uses_subprocess_required_false": True,
        "uses_network_required_false": True,
        "writes_files_required_false": True,
        "signed_evidence_required_by_default": True,
        "witness_export_required_by_default": True,
        "exchange_required_by_default": True,
        "replay_projection_required_by_default": True,
        "voice_memory_state_summary_allowed": True,
        "raw_transcript_forbidden": True,
        "notes": [
            "Phase 41 permits exactly five metadata-only adapters.",
            "Fifth adapter is memory_continuity_audit_metadata_adapter.",
            "All execution-shape flags must be False.",
            "Any real adapter type must be rejected.",
            "Request may carry a voice_memory_state summary; never a "
            "raw transcript.",
        ],
    }


def get_phase41_allowed_adapter_types() -> list[str]:
    return list(ALLOWED_ADAPTER_TYPES)


def create_phase41_adapter_descriptor(
    adapter_name: str,
    adapter_type: str,
    test_only: bool = True,
) -> dict[str, Any]:
    return {
        "adapter_name": str(adapter_name or ""),
        "adapter_type": str(adapter_type or ""),
        "test_only": bool(test_only) is True,
        "produces_audio": False,
        "invokes_tts": False,
        "uses_subprocess": False,
        "uses_network": False,
        "writes_files": False,
        "supports_languages": ["en", "ru", "mixed"],
        "supports_code_switching": True,
        "phase": _PHASE,
        "notes": ("phase41 metadata-only adapter descriptor; "
                  "in-process only"),
    }


def validate_phase41_adapter_descriptor(
    descriptor: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(descriptor, dict):
        return {"ok": False, "reasons": ["descriptor_not_dict"]}
    for f in _REQUIRED_DESCRIPTOR_FIELDS:
        if f not in descriptor:
            reasons.append(f"missing_field:{f}")
    at = str(descriptor.get("adapter_type") or "")
    if at not in ALLOWED_ADAPTER_TYPES:
        reasons.append(f"disallowed_adapter_type:{at}")
    if descriptor.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    for k in ("produces_audio", "invokes_tts", "uses_subprocess",
              "uses_network", "writes_files"):
        if descriptor.get(k) is not False:
            reasons.append(f"{k}_must_be_false")
    try:
        json.dumps(descriptor, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def _sanitize_voice_memory_state(
    state: Optional[dict[str, Any]],
) -> dict[str, Any]:
    """Accept only summary-shape keys; strip any raw transcript /
    sensitive-fact keys silently."""
    if not isinstance(state, dict):
        return {}
    allowed = {
        "preferred_language_mode", "preferred_spoken_mode",
        "code_switch_density", "correction_pattern_count",
        "recent_language_modes", "recent_correction_kinds",
        "continuity_confidence_score",
        "memory_scope", "persistence_status",
        "recent_drift_signal", "voice_style_continuity",
        "user_preference_drift",
        "session_memory_bounded",
        "recent_turn_count",
    }
    return {k: v for k, v in state.items() if k in allowed}


def create_phase41_adapter_request(
    phase29_packet: dict[str, Any],
    adapter_descriptor: dict[str, Any],
    invocation_token: dict[str, Any],
    voice_memory_state: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    pkt = phase29_packet if isinstance(phase29_packet,
                                         dict) else {}
    desc = adapter_descriptor if isinstance(adapter_descriptor,
                                              dict) else {}
    tok = invocation_token if isinstance(invocation_token,
                                          dict) else {}
    sp = pkt.get("spoken_render_payload") or {}
    return {
        "request_id": _new_id("p41req"),
        "created_at": time.time(),
        "phase29_packet_id": pkt.get("packet_id") or
            pkt.get("phase29_id") or "",
        "envelope_id": pkt.get("envelope_id") or "",
        "job_id": pkt.get("job_id") or "",
        "adapter_descriptor": dict(desc),
        "invocation_token_id": tok.get("token_id") or "",
        "operator_id_hash": _hash_str(tok.get("operator_id") or ""),
        "approved": bool(tok.get("approved")),
        "dry_run": True,
        "test_only": True,
        "language_mode": (pkt.get("language_mode") or
                          sp.get("language_mode") or ""),
        "segment_count": (
            len(sp.get("segments") or [])
            if isinstance(sp.get("segments"), list) else 0),
        "safety_summary": pkt.get("safety_summary") or {},
        "spoken_render_payload": sp,
        "voice_memory_state":
            _sanitize_voice_memory_state(voice_memory_state),
        "phase": _PHASE,
        "notes": "phase41 five-adapter request",
    }


def validate_phase41_adapter_request(
    request: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(request, dict):
        return {"ok": False, "reasons": ["request_not_dict"]}
    for f in _REQUIRED_REQUEST_FIELDS:
        if f not in request:
            reasons.append(f"missing_field:{f}")
    dv = validate_phase41_adapter_descriptor(
        request.get("adapter_descriptor"))
    if not dv["ok"]:
        reasons.append("descriptor_invalid:" +
                       ",".join(dv["reasons"]))
    if request.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    if request.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    if not request.get("invocation_token_id"):
        reasons.append("invocation_token_id_missing")
    if not request.get("operator_id_hash"):
        reasons.append("operator_id_hash_missing")
    # Reject banned raw-transcript keys at top level or in
    # voice_memory_state
    for k in _BANNED_REQUEST_KEYS:
        if k in request:
            reasons.append(f"banned_request_key:{k}")
    vms = request.get("voice_memory_state") or {}
    if isinstance(vms, dict):
        for k in _BANNED_REQUEST_KEYS:
            if k in vms:
                reasons.append(f"banned_vms_key:{k}")
    try:
        json.dumps(request, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase41_adapter_interface_report(
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
    "ALLOWED_ADAPTER_TYPES",
    "get_phase41_callable_adapter_schema",
    "get_phase41_allowed_adapter_types",
    "create_phase41_adapter_descriptor",
    "validate_phase41_adapter_descriptor",
    "create_phase41_adapter_request",
    "validate_phase41_adapter_request",
    "write_phase41_adapter_interface_report",
]
