"""Phase 31 - Bilingual Segment Metadata Adapter.

The second Phase 31 callable: analyzes language segments and
code-switch boundaries on the spoken-render payload, returns
metadata-only JSON. No audio, no engine, no subprocess, no network,
no file IO.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase31.bilingual_segment_adapter.v1"


def _new_id() -> str:
    return f"bsres_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def get_bilingual_segment_metadata_adapter_descriptor() -> dict[str, Any]:
    return {
        "adapter_name": "bilingual_segment_metadata_adapter",
        "adapter_type": "bilingual_segment_metadata_adapter",
        "test_only": True,
        "produces_audio": False,
        "invokes_tts": False,
        "uses_subprocess": False,
        "uses_network": False,
        "writes_files": False,
        "supports_languages": ["en", "ru", "mixed"],
        "supports_code_switching": True,
        "phase": _PHASE,
        "notes": ("phase31 bilingual segment metadata adapter; "
                  "no engine bound; metadata-only"),
    }


def analyze_language_segments_from_request(
    request: Any,
) -> dict[str, Any]:
    rq = request if isinstance(request, dict) else {}
    spoken = rq.get("spoken_render_payload") or {}
    segs = spoken.get("segments") or []
    if not isinstance(segs, list):
        segs = []
    counts: dict[str, int] = {}
    sample: list[dict[str, Any]] = []
    for s in segs[:200]:
        if not isinstance(s, dict):
            continue
        lang = str(s.get("language") or "").lower() or "unknown"
        counts[lang] = counts.get(lang, 0) + 1
        if len(sample) < 5:
            sample.append({
                "segment_id": s.get("segment_id") or "",
                "language": lang,
                "char_count": len(str(s.get("text") or "")),
            })
    return {
        "total_segments": len(segs),
        "language_counts": counts,
        "sample": sample,
        "phase": _PHASE,
    }


def summarize_segment_distribution(
    segments: Any,
) -> dict[str, Any]:
    if not isinstance(segments, list):
        return {"total": 0, "language_counts": {}}
    counts: dict[str, int] = {}
    for s in segments[:500]:
        if not isinstance(s, dict):
            continue
        lang = str(s.get("language") or "").lower() or "unknown"
        counts[lang] = counts.get(lang, 0) + 1
    return {
        "total": len(segments),
        "language_counts": counts,
        "phase": _PHASE,
    }


def summarize_code_switch_boundaries(
    segments: Any,
) -> dict[str, Any]:
    if not isinstance(segments, list):
        return {"boundary_count": 0, "phase": _PHASE}
    last_lang = None
    boundaries = 0
    for s in segments[:500]:
        if not isinstance(s, dict):
            continue
        lang = str(s.get("language") or "").lower() or "unknown"
        if last_lang is not None and lang != last_lang:
            boundaries += 1
        last_lang = lang
    return {"boundary_count": boundaries, "phase": _PHASE}


def call_bilingual_segment_metadata_adapter(request: Any) -> dict[str, Any]:
    """Returns a metadata-only analysis result. No side effects."""
    rq = request if isinstance(request, dict) else {}
    spoken = rq.get("spoken_render_payload") or {}
    segs = spoken.get("segments") or []
    if not isinstance(segs, list):
        segs = []
    seg_summary = summarize_segment_distribution(segs)
    cs_summary = summarize_code_switch_boundaries(segs)
    prosody = spoken.get("prosody") or {}
    prosody_count = (len(prosody) if isinstance(prosody, dict)
                      else 0)
    pron = spoken.get("pronunciation_notes") or []
    pron_count = (len(pron) if isinstance(pron, list) else 0)
    safety = rq.get("safety_summary") or {}
    safety_flag_count = sum(1 for k in ("unsafe", "blocked",
                                          "high_risk")
                              if safety.get(k))
    seg_count = int(rq.get("segment_count") or len(segs))
    lang_mode = str(rq.get("language_mode") or
                     spoken.get("language_mode") or "")
    return {
        "result_id": _new_id(),
        "created_at": time.time(),
        "adapter_name": "bilingual_segment_metadata_adapter",
        "adapter_type": "bilingual_segment_metadata_adapter",
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
        "language_segment_counts": seg_summary["language_counts"],
        "code_switch_boundary_count": cs_summary["boundary_count"],
        "prosody_marker_count": prosody_count,
        "pronunciation_hint_count": pron_count,
        "safety_flag_count": safety_flag_count,
        "metadata_summary": {
            "request_id": rq.get("request_id") or "",
            "envelope_id": rq.get("envelope_id") or "",
            "job_id": rq.get("job_id") or "",
            "segment_distribution": seg_summary,
            "code_switch_summary": cs_summary,
            "phase": _PHASE,
        },
        "notes": ("phase31 bilingual segment metadata result; "
                  "no audio; no engine call; no side effects"),
        "phase": _PHASE,
    }


def validate_bilingual_segment_metadata_result(
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
                "language_segment_counts",
                "code_switch_boundary_count",
                "prosody_marker_count", "pronunciation_hint_count",
                "safety_flag_count", "metadata_summary", "phase")
    for f in required:
        if f not in result:
            reasons.append(f"missing_field:{f}")
    if result.get("adapter_name") != \
            "bilingual_segment_metadata_adapter":
        reasons.append("wrong_adapter_name")
    if result.get("adapter_type") != \
            "bilingual_segment_metadata_adapter":
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


def write_bilingual_segment_adapter_report(
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
    "get_bilingual_segment_metadata_adapter_descriptor",
    "call_bilingual_segment_metadata_adapter",
    "analyze_language_segments_from_request",
    "summarize_segment_distribution",
    "summarize_code_switch_boundaries",
    "validate_bilingual_segment_metadata_result",
    "write_bilingual_segment_adapter_report",
]
