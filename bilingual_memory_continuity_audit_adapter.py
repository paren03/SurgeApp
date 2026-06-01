"""Phase 41 - Memory-Continuity Audit Metadata Adapter.

Fifth Phase 41 callable. Inspects voice_memory_state summary +
spoken_render_payload metadata + safety_summary. Returns metadata
only -- never echoes raw transcript, never exposes sensitive
facts, never holds operator_id or signing material.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase41.memory_continuity_audit_adapter.v1"


_ADAPTER_TYPE = "memory_continuity_audit_metadata_adapter"


def _new_id() -> str:
    return f"mcas_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def get_memory_continuity_audit_adapter_descriptor(
) -> dict[str, Any]:
    return {
        "adapter_name": _ADAPTER_TYPE,
        "adapter_type": _ADAPTER_TYPE,
        "test_only": True,
        "produces_audio": False,
        "invokes_tts": False,
        "uses_subprocess": False,
        "uses_network": False,
        "writes_files": False,
        "supports_languages": ["en", "ru", "mixed"],
        "supports_code_switching": True,
        "phase": _PHASE,
        "notes": ("phase41 memory-continuity audit metadata "
                  "adapter; metadata-only; no engine bound; "
                  "no raw transcript; no sensitive facts"),
    }


def extract_voice_memory_summary_from_request(
    request: Any,
) -> dict[str, Any]:
    rq = request if isinstance(request, dict) else {}
    vms = rq.get("voice_memory_state") or {}
    spoken = rq.get("spoken_render_payload") or {}
    if not isinstance(vms, dict):
        vms = {}
    return {
        "present": bool(vms),
        "fields_present": sorted(list(vms.keys())),
        "preferred_language_mode":
            str(vms.get("preferred_language_mode")
                or spoken.get("language_mode") or ""),
        "preferred_spoken_mode":
            str(vms.get("preferred_spoken_mode") or ""),
        "code_switch_density": vms.get("code_switch_density"),
        "correction_pattern_count": int(
            vms.get("correction_pattern_count") or 0),
        "recent_language_modes":
            list(vms.get("recent_language_modes") or [])[:8],
        "recent_correction_kinds":
            list(vms.get("recent_correction_kinds") or [])[:8],
        "continuity_confidence_score":
            float(vms.get("continuity_confidence_score") or 0.0),
        "memory_scope":
            str(vms.get("memory_scope") or "session"),
        "persistence_status":
            str(vms.get("persistence_status") or "ephemeral"),
        "recent_drift_signal":
            bool(vms.get("recent_drift_signal")),
        "voice_style_continuity":
            str(vms.get("voice_style_continuity") or "stable"),
        "user_preference_drift":
            bool(vms.get("user_preference_drift")),
        "session_memory_bounded":
            bool(vms.get("session_memory_bounded", True)),
        "recent_turn_count": int(
            vms.get("recent_turn_count") or 0),
        "phase": _PHASE,
    }


def summarize_language_preference_stability(
    memory_summary: dict[str, Any],
) -> dict[str, Any]:
    plm = str(memory_summary.get(
        "preferred_language_mode") or "")
    recent = list(memory_summary.get(
        "recent_language_modes") or [])
    distinct = sorted({str(m) for m in recent if m})
    drift = bool(memory_summary.get("user_preference_drift")) \
        or (len(distinct) > 2)
    return {
        "preferred_language_mode": plm,
        "recent_distinct_count": len(distinct),
        "recent_distinct_modes": distinct,
        "drift_detected": drift,
        "phase": _PHASE,
    }


def summarize_code_switch_continuity(
    memory_summary: dict[str, Any],
) -> dict[str, Any]:
    csd = memory_summary.get("code_switch_density")
    if csd is None:
        density_bucket = "unknown"
    else:
        try:
            d = float(csd)
        except (TypeError, ValueError):
            d = 0.0
        if d <= 0.1:
            density_bucket = "low"
        elif d <= 0.3:
            density_bucket = "moderate"
        else:
            density_bucket = "high"
    return {
        "code_switch_density_value": csd,
        "code_switch_density_bucket": density_bucket,
        "phase": _PHASE,
    }


def summarize_correction_pattern_continuity(
    memory_summary: dict[str, Any],
) -> dict[str, Any]:
    n = int(memory_summary.get(
        "correction_pattern_count") or 0)
    kinds = list(memory_summary.get(
        "recent_correction_kinds") or [])
    distinct_kinds = sorted({str(k) for k in kinds if k})
    return {
        "correction_pattern_count": n,
        "recent_correction_kinds_count":
            len(distinct_kinds),
        "recent_correction_kinds": distinct_kinds,
        "phase": _PHASE,
    }


def summarize_privacy_and_scope(
    memory_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "memory_scope":
            str(memory_summary.get("memory_scope") or "session"),
        "persistence_status":
            str(memory_summary.get("persistence_status")
                or "ephemeral"),
        "session_memory_bounded":
            bool(memory_summary.get(
                "session_memory_bounded", True)),
        "voice_style_continuity":
            str(memory_summary.get(
                "voice_style_continuity") or "stable"),
        "phase": _PHASE,
    }


def _continuity_confidence(
    memory_summary: dict[str, Any],
    pref_stab: dict[str, Any],
    correction_summary: dict[str, Any],
) -> float:
    base = float(memory_summary.get(
        "continuity_confidence_score") or 0.0)
    if base < 0.0:
        base = 0.0
    if base > 1.0:
        base = 1.0
    score = base if base else 0.7
    if pref_stab.get("drift_detected"):
        score -= 0.15
    if int(correction_summary.get(
            "correction_pattern_count") or 0) > 5:
        score -= 0.1
    if memory_summary.get("recent_drift_signal"):
        score -= 0.15
    if score < 0.0:
        score = 0.0
    if score > 1.0:
        score = 1.0
    return round(score, 4)


def call_memory_continuity_audit_adapter(
    request: Any,
) -> dict[str, Any]:
    if not isinstance(request, dict):
        return _refusal("request_not_dict")
    if request.get("approved") is not True:
        return _refusal("not_approved")
    desc = request.get("adapter_descriptor") or {}
    if not isinstance(desc, dict) or \
            desc.get("adapter_type") != _ADAPTER_TYPE:
        return _refusal("wrong_adapter_type")
    mem = extract_voice_memory_summary_from_request(request)
    pref_stab = summarize_language_preference_stability(mem)
    cs_cont = summarize_code_switch_continuity(mem)
    corr_summary = summarize_correction_pattern_continuity(mem)
    privacy = summarize_privacy_and_scope(mem)
    confidence = _continuity_confidence(
        mem, pref_stab, corr_summary)
    return {
        "result_id": _new_id(),
        "created_at": time.time(),
        "adapter_name": _ADAPTER_TYPE,
        "adapter_type": _ADAPTER_TYPE,
        "status": "ok",
        "dry_run": True,
        "test_only": True,
        "produced_audio": False,
        "invoked_tts": False,
        "used_subprocess": False,
        "used_network": False,
        "wrote_files": False,
        "received_language_mode":
            str(request.get("language_mode") or ""),
        "received_segment_count": int(
            request.get("segment_count") or 0),
        "memory_summary_present": bool(mem.get("present")),
        "preferred_language_mode":
            pref_stab.get("preferred_language_mode"),
        "preferred_spoken_mode":
            mem.get("preferred_spoken_mode"),
        "code_switch_density_summary": cs_cont,
        "correction_pattern_count":
            corr_summary.get("correction_pattern_count"),
        "correction_pattern_summary": corr_summary,
        "language_preference_summary": pref_stab,
        "continuity_confidence_score": confidence,
        "privacy_scope_status": privacy,
        "raw_transcript_absent": True,
        "sensitive_fact_absent": True,
        "persistence_status": privacy.get(
            "persistence_status"),
        "metadata_summary": {
            "memory_fields_present":
                mem.get("fields_present"),
            "memory_scope": mem.get("memory_scope"),
            "voice_style_continuity":
                mem.get("voice_style_continuity"),
            "session_memory_bounded":
                mem.get("session_memory_bounded"),
        },
        "phase": _PHASE,
        "notes": [
            "Metadata-only result.",
            "No raw transcript echoed.",
            "No sensitive facts exposed.",
            "No audio engine bound.",
            "No subprocess / network / file write.",
        ],
    }


def _refusal(reason: str) -> dict[str, Any]:
    return {
        "result_id": _new_id(),
        "created_at": time.time(),
        "adapter_name": _ADAPTER_TYPE,
        "adapter_type": _ADAPTER_TYPE,
        "status": "refused",
        "reason": str(reason),
        "dry_run": True,
        "test_only": True,
        "produced_audio": False,
        "invoked_tts": False,
        "used_subprocess": False,
        "used_network": False,
        "wrote_files": False,
        "raw_transcript_absent": True,
        "sensitive_fact_absent": True,
        "phase": _PHASE,
    }


_REQUIRED_RESULT_FIELDS = (
    "result_id", "created_at", "adapter_name",
    "adapter_type", "status", "dry_run", "test_only",
    "produced_audio", "invoked_tts",
    "used_subprocess", "used_network", "wrote_files",
    "raw_transcript_absent", "sensitive_fact_absent",
    "phase",
)


_BANNED_RESULT_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def validate_memory_continuity_audit_result(
    result: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    for f in _REQUIRED_RESULT_FIELDS:
        if f not in result:
            reasons.append(f"missing_field:{f}")
    if result.get("adapter_type") != _ADAPTER_TYPE:
        reasons.append("adapter_type_mismatch")
    for k in ("produced_audio", "invoked_tts",
              "used_subprocess", "used_network",
              "wrote_files"):
        if result.get(k) is not False:
            reasons.append(f"{k}_must_be_false")
    if result.get("raw_transcript_absent") is not True:
        reasons.append("raw_transcript_absent_must_be_true")
    if result.get("sensitive_fact_absent") is not True:
        reasons.append("sensitive_fact_absent_must_be_true")
    for k in _BANNED_RESULT_FIELDS:
        if k in result and result.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_result_field:{k}")
    try:
        json.dumps(result, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def write_memory_continuity_audit_adapter_report(
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
    "get_memory_continuity_audit_adapter_descriptor",
    "call_memory_continuity_audit_adapter",
    "extract_voice_memory_summary_from_request",
    "summarize_language_preference_stability",
    "summarize_code_switch_continuity",
    "summarize_correction_pattern_continuity",
    "summarize_privacy_and_scope",
    "validate_memory_continuity_audit_result",
    "write_memory_continuity_audit_adapter_report",
]
