"""Phase 30 - Dummy Metadata Adapter.

The single permitted Phase 30 callable: a safe in-process metadata-only
adapter. It produces NO audio, NO files, NO subprocess, NO network. It
returns a JSON-serializable result describing what it received.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase30.dummy_adapter.v1"


def _new_id() -> str:
    return f"dres_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def get_dummy_metadata_adapter_descriptor() -> dict[str, Any]:
    return {
        "adapter_name": "dummy_metadata_adapter",
        "adapter_type": "dummy_metadata_adapter",
        "test_only": True,
        "produces_audio": False,
        "invokes_tts": False,
        "uses_subprocess": False,
        "uses_network": False,
        "writes_files": False,
        "supports_languages": ["en", "ru", "mixed"],
        "supports_code_switching": True,
        "phase": _PHASE,
        "notes": ("phase30 dummy metadata adapter; no engine bound; "
                  "in-process only"),
    }


def simulate_adapter_latency_metadata(
    request: Any,
) -> dict[str, Any]:
    """Returns a synthetic latency-shape dict. Does not sleep. Does not
    actually defer execution. Pure metadata."""
    rq = request if isinstance(request, dict) else {}
    seg_count = int(rq.get("segment_count") or 0)
    base_ms = 25
    return {
        "would_be_latency_ms": base_ms + 5 * max(0, seg_count),
        "would_be_first_byte_ms": base_ms,
        "synthetic": True,
        "phase": _PHASE,
    }


def call_dummy_metadata_adapter(request: Any) -> dict[str, Any]:
    """Returns a metadata-only result. Side effects: NONE."""
    rq = request if isinstance(request, dict) else {}
    safety = rq.get("safety_summary") or {}
    lang_mode = str(rq.get("language_mode") or "")
    seg_count = int(rq.get("segment_count") or 0)
    prosody_count = int(rq.get("prosody_count") or 0)
    latency = simulate_adapter_latency_metadata(rq)
    return {
        "result_id": _new_id(),
        "created_at": time.time(),
        "adapter_name": "dummy_metadata_adapter",
        "adapter_type": "dummy_metadata_adapter",
        "status": "metadata_only_ok",
        "dry_run": True,
        "test_only": True,
        "produced_audio": False,
        "invoked_tts": False,
        "used_subprocess": False,
        "used_network": False,
        "wrote_files": False,
        "received_language_mode": lang_mode,
        "received_segment_count": seg_count,
        "received_prosody_count": prosody_count,
        "received_safety_summary": safety,
        "metadata_summary": {
            "would_be_latency": latency,
            "request_id": rq.get("request_id") or "",
            "envelope_id": rq.get("envelope_id") or "",
            "job_id": rq.get("job_id") or "",
            "phase": _PHASE,
        },
        "notes": ("phase30 dummy metadata adapter result; "
                  "no audio; no engine call; no side effects"),
        "phase": _PHASE,
    }


def validate_dummy_metadata_result(result: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    required = ("result_id", "created_at", "adapter_name",
                "adapter_type", "status", "dry_run", "test_only",
                "produced_audio", "invoked_tts", "used_subprocess",
                "used_network", "wrote_files",
                "received_language_mode", "received_segment_count",
                "received_prosody_count", "received_safety_summary",
                "metadata_summary", "phase")
    for f in required:
        if f not in result:
            reasons.append(f"missing_field:{f}")
    if result.get("adapter_name") != "dummy_metadata_adapter":
        reasons.append("wrong_adapter_name")
    if result.get("adapter_type") != "dummy_metadata_adapter":
        reasons.append("wrong_adapter_type")
    for k in ("produced_audio", "invoked_tts", "used_subprocess",
              "used_network", "wrote_files"):
        if result.get(k) is not False:
            reasons.append(f"{k}_must_be_false")
    if result.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    if result.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    # No audio / command fields
    forbidden_keys = ("audio_bytes", "audio_url", "audio_path",
                      "wav_path", "mp3_path", "voice_clone_ref",
                      "speaker_embedding", "tts_model_path",
                      "output_audio_file", "command", "shell",
                      "subprocess", "powershell", "executable",
                      "run_command")
    for k in forbidden_keys:
        if k in result:
            reasons.append(f"forbidden_field:{k}")
    try:
        json.dumps(result, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def write_dummy_adapter_report(
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
    "get_dummy_metadata_adapter_descriptor",
    "call_dummy_metadata_adapter",
    "validate_dummy_metadata_result",
    "simulate_adapter_latency_metadata",
    "write_dummy_adapter_report",
]
