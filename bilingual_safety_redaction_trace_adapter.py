"""Phase 37 - Safety Redaction Trace Metadata Adapter.

Fourth Phase 37 callable. Inspects safety_summary + redaction
metadata + recognition_only / do_not_use_unprompted / voice-safe
replacement / vulgar/offensive blocks on the spoken-render payload.
Returns metadata only. Never echoes unsafe terms in full — counts
and summaries only.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase37.safety_redaction_trace_adapter.v1"


def _new_id() -> str:
    return f"sres_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def get_safety_redaction_trace_adapter_descriptor() -> dict[str, Any]:
    return {
        "adapter_name": "safety_redaction_trace_metadata_adapter",
        "adapter_type": "safety_redaction_trace_metadata_adapter",
        "test_only": True,
        "produces_audio": False,
        "invokes_tts": False,
        "uses_subprocess": False,
        "uses_network": False,
        "writes_files": False,
        "supports_languages": ["en", "ru", "mixed"],
        "supports_code_switching": True,
        "phase": _PHASE,
        "notes": ("phase37 safety redaction trace metadata adapter; "
                  "no engine bound; metadata-only; unsafe terms "
                  "never echoed in full"),
    }


def extract_safety_summary_from_request(
    request: Any,
) -> dict[str, Any]:
    rq = request if isinstance(request, dict) else {}
    safety = rq.get("safety_summary") or {}
    spoken = rq.get("spoken_render_payload") or {}
    spoken_safety = spoken.get("safety_summary") or {}
    merged = {}
    if isinstance(safety, dict):
        merged.update(safety)
    if isinstance(spoken_safety, dict):
        for k, v in spoken_safety.items():
            merged.setdefault(k, v)
    return {
        "present": bool(merged),
        "fields_present": sorted(list(merged.keys())),
        "flags": {
            "unsafe": bool(merged.get("unsafe")),
            "blocked": bool(merged.get("blocked")),
            "high_risk": bool(merged.get("high_risk")),
            "unsafe_leakage_detected":
                bool(merged.get("unsafe_leakage_detected")),
        },
        "replacements_count": int(
            merged.get("replacements_count") or 0),
        "risk_count": (len(merged.get("risks") or [])
                        if isinstance(merged.get("risks"), list)
                        else 0),
        "phase": _PHASE,
    }


def _count_list_keys(payload: Any, keys: tuple[str, ...]) -> int:
    if not isinstance(payload, dict):
        return 0
    n = 0
    for k in keys:
        v = payload.get(k)
        if isinstance(v, list):
            n += len(v)
        elif v is True:
            n += 1
        elif isinstance(v, dict) and v:
            n += len(v)
    return n


def _walk_count_by_substring(payload: Any,
                              key_substrings: tuple[str, ...]) -> int:
    if not isinstance(payload, dict):
        return 0
    n = 0
    visited: list[int] = []

    def _walk(o: Any) -> None:
        nonlocal n
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                if any(s in ks for s in key_substrings):
                    if isinstance(v, list):
                        n += len(v)
                    elif v is True:
                        n += 1
                    elif isinstance(v, (int, float)):
                        n += int(v) if v > 0 else 0
                    elif isinstance(v, str) and v:
                        n += 1
                    elif isinstance(v, dict) and v:
                        n += len(v)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)

    _walk(payload)
    return n


def summarize_redaction_decisions(payload: Any) -> dict[str, Any]:
    n_total = _walk_count_by_substring(
        payload, ("redaction", "redacted", "redact_"))
    return {
        "redaction_decision_count": n_total,
        "phase": _PHASE,
    }


def count_recognition_only_blocks(payload: Any) -> int:
    return _walk_count_by_substring(
        payload, ("recognition_only", "recognize_only",
                  "listen_only"))


def count_do_not_use_unprompted_blocks(payload: Any) -> int:
    return _walk_count_by_substring(
        payload, ("do_not_use_unprompted",
                  "do_not_use_without_prompt",
                  "no_unprompted_use"))


def count_voice_safe_replacements(payload: Any) -> int:
    if not isinstance(payload, dict):
        return 0
    safety = payload.get("safety_summary") or {}
    explicit = (int(safety.get("replacements_count") or 0)
                 if isinstance(safety, dict) else 0)
    walked = _walk_count_by_substring(
        payload, ("voice_safe_replacement",
                  "voice_safe_substitution",
                  "voice_safe_swap"))
    return explicit + walked


def count_vulgar_offensive_blocks(payload: Any) -> int:
    return _walk_count_by_substring(
        payload, ("vulgar_block", "offensive_block",
                  "profanity_block", "vulgar_redact",
                  "offensive_redact", "profanity_redact"))


def call_safety_redaction_trace_adapter(
    request: Any,
) -> dict[str, Any]:
    rq = request if isinstance(request, dict) else {}
    payload = rq.get("spoken_render_payload") or {}
    safety = extract_safety_summary_from_request(rq)
    redactions = summarize_redaction_decisions(payload)
    rec_only = count_recognition_only_blocks(payload)
    dnu = count_do_not_use_unprompted_blocks(payload)
    vsr = count_voice_safe_replacements(payload)
    vob = count_vulgar_offensive_blocks(payload)
    lang_mode = str(rq.get("language_mode") or
                     payload.get("language_mode") or "")
    seg_count = int(rq.get("segment_count") or 0)
    # Safety trace score: higher when safety metadata is rich and
    # the result has more redaction signal — bounded to [0, 1].
    score = min(1.0,
                 (0.2 if safety["present"] else 0.0)
                 + 0.05 * (redactions["redaction_decision_count"])
                 + 0.05 * rec_only
                 + 0.05 * dnu
                 + 0.05 * vsr
                 + 0.05 * vob
                 + 0.05 * safety["replacements_count"])
    return {
        "result_id": _new_id(),
        "created_at": time.time(),
        "adapter_name": "safety_redaction_trace_metadata_adapter",
        "adapter_type": "safety_redaction_trace_metadata_adapter",
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
        "safety_summary_present": safety["present"],
        "redaction_decision_count":
            redactions["redaction_decision_count"],
        "recognition_only_block_count": rec_only,
        "do_not_use_unprompted_block_count": dnu,
        "voice_safe_replacement_count": vsr,
        "vulgar_offensive_block_count": vob,
        "safety_trace_score": score,
        "metadata_summary": {
            "request_id": rq.get("request_id") or "",
            "envelope_id": rq.get("envelope_id") or "",
            "job_id": rq.get("job_id") or "",
            "safety_summary": safety,
            "redactions": redactions,
            "phase": _PHASE,
        },
        "notes": ("phase37 safety redaction trace metadata "
                  "result; no audio; no engine call; no unsafe "
                  "terms echoed in full; counts and summaries "
                  "only"),
        "phase": _PHASE,
    }


def validate_safety_redaction_trace_result(
    result: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    required = ("result_id", "created_at", "adapter_name",
                "adapter_type", "status", "dry_run", "test_only",
                "produced_audio", "invoked_tts", "used_subprocess",
                "used_network", "wrote_files",
                "received_language_mode", "received_segment_count",
                "safety_summary_present",
                "redaction_decision_count",
                "recognition_only_block_count",
                "do_not_use_unprompted_block_count",
                "voice_safe_replacement_count",
                "vulgar_offensive_block_count",
                "safety_trace_score",
                "metadata_summary", "phase")
    for f in required:
        if f not in result:
            reasons.append(f"missing_field:{f}")
    if result.get("adapter_name") != \
            "safety_redaction_trace_metadata_adapter":
        reasons.append("wrong_adapter_name")
    if result.get("adapter_type") != \
            "safety_redaction_trace_metadata_adapter":
        reasons.append("wrong_adapter_type")
    for k in ("produced_audio", "invoked_tts", "used_subprocess",
              "used_network", "wrote_files"):
        if result.get(k) is not False:
            reasons.append(f"{k}_must_be_false")
    if result.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    if result.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    forbidden = ("audio_bytes", "audio_url", "audio_path",
                  "wav_path", "mp3_path", "voice_clone_ref",
                  "speaker_embedding", "tts_model_path",
                  "output_audio_file", "command", "shell",
                  "powershell_command", "executable",
                  "run_command", "vulgar_terms", "offensive_terms",
                  "profanity_terms")
    for k in forbidden:
        if k in result:
            reasons.append(f"forbidden_field:{k}")
    try:
        json.dumps(result, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def write_safety_redaction_trace_adapter_report(
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
    "get_safety_redaction_trace_adapter_descriptor",
    "call_safety_redaction_trace_adapter",
    "extract_safety_summary_from_request",
    "summarize_redaction_decisions",
    "count_recognition_only_blocks",
    "count_do_not_use_unprompted_blocks",
    "count_voice_safe_replacements",
    "count_vulgar_offensive_blocks",
    "validate_safety_redaction_trace_result",
    "write_safety_redaction_trace_adapter_report",
]
