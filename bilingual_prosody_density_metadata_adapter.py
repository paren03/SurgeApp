"""Phase 33 - Prosody Density Metadata Adapter.

Third Phase 33 callable. Analyzes pause / emphasis / tone density on
the spoken-render payload. Returns metadata only. No audio, no engine.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase33.prosody_density_adapter.v1"


def _new_id() -> str:
    return f"pdres_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def get_prosody_density_metadata_adapter_descriptor() -> dict[str, Any]:
    return {
        "adapter_name": "prosody_density_metadata_adapter",
        "adapter_type": "prosody_density_metadata_adapter",
        "test_only": True,
        "produces_audio": False,
        "invokes_tts": False,
        "uses_subprocess": False,
        "uses_network": False,
        "writes_files": False,
        "supports_languages": ["en", "ru", "mixed"],
        "supports_code_switching": True,
        "phase": _PHASE,
        "notes": ("phase33 prosody density metadata adapter; "
                  "no engine bound; metadata-only"),
    }


def _count_keys(prosody: Any, key_substrings: tuple[str, ...]) -> int:
    if not isinstance(prosody, dict):
        return 0
    n = 0
    for k, v in prosody.items():
        ks = str(k).lower()
        if any(s in ks for s in key_substrings):
            if isinstance(v, (list, tuple)):
                n += len(v)
            elif v is True:
                n += 1
            elif isinstance(v, (int, float)):
                n += 1 if v else 0
            elif isinstance(v, str) and v:
                n += 1
    return n


def summarize_pause_density(payload: Any) -> dict[str, Any]:
    pros = (payload or {}).get("prosody") or {} \
        if isinstance(payload, dict) else {}
    count = _count_keys(pros, ("pause", "break", "rest"))
    return {"pause_marker_count": count, "phase": _PHASE}


def summarize_emphasis_density(payload: Any) -> dict[str, Any]:
    pros = (payload or {}).get("prosody") or {} \
        if isinstance(payload, dict) else {}
    count = _count_keys(pros, ("emphasis", "stress", "accent"))
    return {"emphasis_marker_count": count, "phase": _PHASE}


def summarize_tone_density(payload: Any) -> dict[str, Any]:
    pros = (payload or {}).get("prosody") or {} \
        if isinstance(payload, dict) else {}
    count = _count_keys(pros, ("tone", "pitch", "intonation"))
    return {"tone_marker_count": count, "phase": _PHASE}


def summarize_code_switch_prosody_load(
    payload: Any,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"code_switch_boundary_count": 0,
                "prosody_load_estimate": 0.0, "phase": _PHASE}
    csb = payload.get("code_switch_boundaries") or []
    csb_count = len(csb) if isinstance(csb, list) else 0
    pros = payload.get("prosody") or {}
    pros_total = sum(
        (len(v) if isinstance(v, (list, tuple))
         else (1 if v else 0))
        for v in (pros.values() if isinstance(pros, dict) else []))
    return {
        "code_switch_boundary_count": csb_count,
        "prosody_load_estimate": min(1.0,
                                       0.1 * csb_count +
                                       0.05 * pros_total),
        "phase": _PHASE,
    }


def analyze_prosody_density_from_request(
    request: Any,
) -> dict[str, Any]:
    rq = request if isinstance(request, dict) else {}
    payload = rq.get("spoken_render_payload") or {}
    pause = summarize_pause_density(payload)
    emp = summarize_emphasis_density(payload)
    tone = summarize_tone_density(payload)
    cs = summarize_code_switch_prosody_load(payload)
    seg_count = (len(payload.get("segments") or [])
                  if isinstance(payload.get("segments"), list)
                  else 0)
    pause_n = pause["pause_marker_count"]
    emp_n = emp["emphasis_marker_count"]
    tone_n = tone["tone_marker_count"]
    cs_n = cs["code_switch_boundary_count"]
    density_score = min(1.0,
                         0.15 * pause_n + 0.15 * emp_n +
                         0.10 * tone_n + 0.08 * cs_n)
    complexity_score = min(1.0,
                            density_score +
                            (0.05 if seg_count > 4 else 0.0))
    return {
        "pause": pause, "emphasis": emp, "tone": tone,
        "code_switch": cs,
        "pause_marker_count": pause_n,
        "emphasis_marker_count": emp_n,
        "tone_marker_count": tone_n,
        "code_switch_boundary_count": cs_n,
        "prosody_density_score": density_score,
        "spoken_complexity_score": complexity_score,
        "phase": _PHASE,
    }


def call_prosody_density_metadata_adapter(request: Any) -> dict[str, Any]:
    """Returns metadata-only result. Side effects: NONE."""
    rq = request if isinstance(request, dict) else {}
    analysis = analyze_prosody_density_from_request(rq)
    lang_mode = str(rq.get("language_mode") or
                     (rq.get("spoken_render_payload") or {})
                     .get("language_mode") or "")
    seg_count = int(rq.get("segment_count") or 0)
    return {
        "result_id": _new_id(),
        "created_at": time.time(),
        "adapter_name": "prosody_density_metadata_adapter",
        "adapter_type": "prosody_density_metadata_adapter",
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
        "pause_marker_count": analysis["pause_marker_count"],
        "emphasis_marker_count": analysis["emphasis_marker_count"],
        "tone_marker_count": analysis["tone_marker_count"],
        "code_switch_boundary_count":
            analysis["code_switch_boundary_count"],
        "prosody_density_score": analysis["prosody_density_score"],
        "spoken_complexity_score":
            analysis["spoken_complexity_score"],
        "metadata_summary": {
            "request_id": rq.get("request_id") or "",
            "envelope_id": rq.get("envelope_id") or "",
            "job_id": rq.get("job_id") or "",
            "analysis": analysis,
            "phase": _PHASE,
        },
        "notes": ("phase33 prosody density metadata result; "
                  "no audio; no engine call; no side effects"),
        "phase": _PHASE,
    }


def validate_prosody_density_metadata_result(
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
                "pause_marker_count", "emphasis_marker_count",
                "tone_marker_count", "code_switch_boundary_count",
                "prosody_density_score", "spoken_complexity_score",
                "metadata_summary", "phase")
    for f in required:
        if f not in result:
            reasons.append(f"missing_field:{f}")
    if result.get("adapter_name") != \
            "prosody_density_metadata_adapter":
        reasons.append("wrong_adapter_name")
    if result.get("adapter_type") != \
            "prosody_density_metadata_adapter":
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
                  "run_command")
    for k in forbidden:
        if k in result:
            reasons.append(f"forbidden_field:{k}")
    try:
        json.dumps(result, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def write_prosody_density_adapter_report(
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
    "get_prosody_density_metadata_adapter_descriptor",
    "call_prosody_density_metadata_adapter",
    "analyze_prosody_density_from_request",
    "summarize_pause_density",
    "summarize_emphasis_density",
    "summarize_tone_density",
    "summarize_code_switch_prosody_load",
    "validate_prosody_density_metadata_result",
    "write_prosody_density_adapter_report",
]
