"""Phase 42 - Replay Matrix.

Creates a Phase 40-compatible replay matrix from Phase 41 replay
projections embedded in Phase 42 trace results. Does NOT modify
Phase 40. Does NOT re-invoke adapters.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase42.replay_matrix.v1"


_REQUIRED_MATRIX_FIELDS = (
    "matrix_id", "created_at", "phase",
    "scenario_count", "projection_count",
    "per_scenario_projection_status",
    "trace_hash_summaries",
    "adapter_selection_summaries",
    "governance_result_summaries",
    "baseline_summaries",
    "phase21_status_summaries",
    "compatibility_status",
)


_BANNED_MATRIX_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def create_phase42_replay_matrix(
    trace_results: list[Any],
) -> dict[str, Any]:
    per_status: list[dict[str, Any]] = []
    trace_hashes: list[dict[str, Any]] = []
    sel_summaries: list[dict[str, Any]] = []
    gov_summaries: list[dict[str, Any]] = []
    baseline_summaries: list[dict[str, Any]] = []
    phase21_summaries: list[dict[str, Any]] = []
    projection_count = 0
    for r in trace_results or []:
        if not isinstance(r, dict):
            continue
        sid = r.get("scenario_id")
        present = bool(r.get("replay_projection_present"))
        proj_sum = r.get("replay_projection_summary") or {}
        per_status.append({
            "scenario_id": sid,
            "status": r.get("status"),
            "projection_present": present,
        })
        if present:
            projection_count += 1
            trace_hashes.append({
                "scenario_id": sid,
                "trace_hash": proj_sum.get("trace_hash"),
            })
            sel_summaries.append({
                "scenario_id": sid,
                "selected_adapter_name":
                    proj_sum.get("selected_adapter_name"),
            })
            gov_summaries.append({
                "scenario_id": sid,
                "result_verification_ok":
                    bool(r.get(
                        "result_verification_ok")),
                "governance_recheck_ok":
                    bool(r.get(
                        "governance_recheck_ok")),
                "signed_evidence_validates":
                    bool(r.get(
                        "signed_evidence_validates")),
                "witness_export_status":
                    r.get("witness_export_status"),
                "exchange_status":
                    r.get("exchange_status"),
            })
            baseline_summaries.append({
                "scenario_id": sid,
                "baseline_expected": {
                    "english_words": 2814,
                    "russian_words": 2518,
                    "russian_phrases": 35,
                    "bilingual_concepts": 26,
                    "bilingual_entry_links": 52,
                    "live_pack_manifests": 90,
                },
            })
            phase21_summaries.append({
                "scenario_id": sid,
                "status_text":
                    proj_sum.get("phase21_status_text")
                    or "BLOCKED",
            })
    compat_status = "ok" if projection_count > 0 else \
        "no_successful_traces"
    return {
        "matrix_id": f"p42mat_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "scenario_count": len(trace_results or []),
        "projection_count": projection_count,
        "per_scenario_projection_status": per_status,
        "trace_hash_summaries": trace_hashes,
        "adapter_selection_summaries": sel_summaries,
        "governance_result_summaries": gov_summaries,
        "baseline_summaries": baseline_summaries,
        "phase21_status_summaries": phase21_summaries,
        "compatibility_status": compat_status,
        "notes": [
            "Matrix is read-only.",
            "No adapter re-invocation.",
            "No Phase 40 modification.",
        ],
    }


def validate_phase42_replay_matrix(
    matrix: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(matrix, dict):
        return {"ok": False,
                "reasons": ["matrix_not_dict"]}
    for f in _REQUIRED_MATRIX_FIELDS:
        if f not in matrix:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_MATRIX_FIELDS:
        if k in matrix and matrix.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase42_replay_projections(
    matrix: Any,
) -> dict[str, Any]:
    if not isinstance(matrix, dict):
        return {"ok": False,
                "reasons": ["matrix_not_dict"]}
    reasons: list[str] = []
    th = matrix.get("trace_hash_summaries") or []
    for entry in th:
        if not isinstance(entry, dict):
            continue
        h = entry.get("trace_hash")
        if not isinstance(h, str) or len(h) < 64:
            reasons.append(
                f"bad_trace_hash:{entry.get('scenario_id')}")
    gov = matrix.get("governance_result_summaries") or []
    for entry in gov:
        if not isinstance(entry, dict):
            continue
        if entry.get("result_verification_ok") is not True:
            reasons.append(
                f"result_verification_failed:"
                f"{entry.get('scenario_id')}")
        if entry.get("governance_recheck_ok") is not True:
            reasons.append(
                f"governance_recheck_failed:"
                f"{entry.get('scenario_id')}")
        if entry.get("signed_evidence_validates") is not True:
            reasons.append(
                f"signed_evidence_invalid:"
                f"{entry.get('scenario_id')}")
        if entry.get("witness_export_status") != "ok":
            reasons.append(
                f"witness_export_not_ok:"
                f"{entry.get('scenario_id')}")
        if entry.get("exchange_status") not in (
                "ok", "witness_failed"):
            reasons.append(
                f"exchange_not_ok:"
                f"{entry.get('scenario_id')}")
    p21 = matrix.get("phase21_status_summaries") or []
    for entry in p21:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("status_text") or "") not in (
                "BLOCKED", "STAGED_AWAITING_OPERATOR"):
            reasons.append(
                f"phase21_status_unexpected:"
                f"{entry.get('scenario_id')}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def summarize_phase42_replay_matrix(
    matrix: Any,
) -> dict[str, Any]:
    if not isinstance(matrix, dict):
        return {"ok": False, "summary": "no_matrix"}
    return {
        "ok": matrix.get("compatibility_status") == "ok",
        "summary": (
            f"phase42 replay matrix: scenarios="
            f"{matrix.get('scenario_count')} "
            f"projections={matrix.get('projection_count')} "
            f"compat={matrix.get('compatibility_status')}"),
        "matrix_id": matrix.get("matrix_id"),
        "phase": _PHASE,
    }


def write_phase42_replay_matrix(
    matrix: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(matrix)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase42_replay_matrix",
    "validate_phase42_replay_matrix",
    "verify_phase42_replay_projections",
    "summarize_phase42_replay_matrix",
    "write_phase42_replay_matrix",
]
