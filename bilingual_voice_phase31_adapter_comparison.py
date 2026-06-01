"""Phase 31 - Adapter Comparison.

Compare metadata-only adapter results without invoking any renderer.
No execution. No audio. Metadata-shape only.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase31.adapter_comparison.v1"


def compare_adapter_descriptors(
    descriptors: Any,
) -> dict[str, Any]:
    if not isinstance(descriptors, list):
        return {"ok": False, "reasons": ["descriptors_not_list"]}
    summary: list[dict[str, Any]] = []
    for d in descriptors[:10]:
        if not isinstance(d, dict):
            continue
        summary.append({
            "adapter_name": d.get("adapter_name"),
            "adapter_type": d.get("adapter_type"),
            "test_only": bool(d.get("test_only")),
            "produces_audio": bool(d.get("produces_audio")),
            "invokes_tts": bool(d.get("invokes_tts")),
            "uses_subprocess": bool(d.get("uses_subprocess")),
            "uses_network": bool(d.get("uses_network")),
            "writes_files": bool(d.get("writes_files")),
            "supports_languages":
                list(d.get("supports_languages") or []),
            "supports_code_switching":
                bool(d.get("supports_code_switching")),
        })
    return {
        "ok": True,
        "count": len(summary),
        "descriptors": summary,
        "phase": _PHASE,
    }


def compare_metadata_results(
    result_a: Any,
    result_b: Any,
) -> dict[str, Any]:
    if not isinstance(result_a, dict) or not isinstance(result_b,
                                                          dict):
        return {"ok": False, "reasons": ["bad_inputs"]}
    diffs: list[str] = []
    common_keys = ("produced_audio", "invoked_tts", "used_subprocess",
                   "used_network", "wrote_files", "dry_run",
                   "test_only", "received_language_mode",
                   "received_segment_count")
    for k in common_keys:
        if result_a.get(k) != result_b.get(k):
            diffs.append(f"{k}:a={result_a.get(k)},b={result_b.get(k)}")
    boundary_equal = all(result_a.get(k) is False and
                         result_b.get(k) is False
                         for k in ("produced_audio", "invoked_tts",
                                    "used_subprocess",
                                    "used_network", "wrote_files"))
    return {
        "ok": True,
        "boundary_equal": boundary_equal,
        "differences": diffs,
        "a_adapter": result_a.get("adapter_name"),
        "b_adapter": result_b.get("adapter_name"),
        "phase": _PHASE,
    }


def score_result_usefulness(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"score": 0.0, "ok": False}
    score = 0.0
    fields_seen = 0
    informative = ("received_language_mode", "received_segment_count",
                   "language_segment_counts",
                   "code_switch_boundary_count",
                   "prosody_marker_count",
                   "pronunciation_hint_count",
                   "safety_flag_count")
    for k in informative:
        if k in result:
            fields_seen += 1
            v = result.get(k)
            if isinstance(v, (int, float)) and v > 0:
                score += 0.1
            elif isinstance(v, str) and v:
                score += 0.05
            elif isinstance(v, dict) and v:
                score += 0.05
    score += 0.05 * fields_seen
    return {
        "score": min(1.0, score),
        "informative_fields_present": fields_seen,
        "phase": _PHASE,
        "ok": True,
    }


def identify_adapter_result_gaps(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"gaps": ["result_not_dict"]}
    gaps: list[str] = []
    for k in ("received_language_mode", "received_segment_count"):
        if k not in result:
            gaps.append(f"missing:{k}")
    if "language_segment_counts" not in result:
        gaps.append("missing_optional:language_segment_counts")
    if "code_switch_boundary_count" not in result:
        gaps.append("missing_optional:code_switch_boundary_count")
    return {
        "ok": not [g for g in gaps if not g.startswith(
            "missing_optional:")],
        "gaps": gaps,
        "phase": _PHASE,
        "safety_advisory": ("Comparison output does not propose "
                             "crossing the dry-run boundary."),
    }


def write_adapter_comparison_report(
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
    "compare_adapter_descriptors",
    "compare_metadata_results",
    "score_result_usefulness",
    "identify_adapter_result_gaps",
    "write_adapter_comparison_report",
]
