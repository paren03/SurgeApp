"""Phase 41 - Result Verifier.

Independent verifier of Phase 41 adapter results, selection
receipts, invocation receipts, memory-privacy boundary, and
governance compliance.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_phase41_adapter_interface as p41i
import bilingual_voice_phase36_secret_boundary as sb


_PHASE = "phase41.result_verifier.v1"


_BANNED_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def _check_metadata_only(r: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for k in ("produced_audio", "invoked_tts",
              "used_subprocess", "used_network",
              "wrote_files"):
        if r.get(k) is True:
            reasons.append(f"{k}_must_be_false")
    return reasons


def _check_banned_fields(r: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for k in _BANNED_FIELDS:
        if k in r and r.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    return reasons


def verify_phase41_adapter_result(
    result: Any,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False,
                "reasons": ["result_not_dict"]}
    reasons: list[str] = []
    at = str(result.get("adapter_type") or "")
    if at and at not in p41i.ALLOWED_ADAPTER_TYPES:
        reasons.append(f"unknown_adapter:{at}")
    reasons.extend(_check_metadata_only(result))
    reasons.extend(_check_banned_fields(result))
    # Secret leakage scan
    leaks = sb.scan_object_for_secret_fields(result)
    for h in leaks:
        reasons.append(f"secret_leak:{h}")
    try:
        json.dumps(result, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase41_memory_privacy_result(
    result: Any,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False,
                "reasons": ["result_not_dict"]}
    reasons: list[str] = []
    if result.get("adapter_type") == \
            "memory_continuity_audit_metadata_adapter":
        if result.get("raw_transcript_absent") is not True:
            reasons.append(
                "raw_transcript_absent_must_be_true")
        if result.get("sensitive_fact_absent") is not True:
            reasons.append(
                "sensitive_fact_absent_must_be_true")
    reasons.extend(_check_banned_fields(result))
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase41_selection_receipt(
    receipt: Any,
    result: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return {"ok": False,
                "reasons": ["receipt_not_dict"]}
    if not isinstance(result, dict):
        reasons.append("result_not_dict")
    at = str((result or {}).get("adapter_type") or "")
    if at and at not in p41i.ALLOWED_ADAPTER_TYPES:
        reasons.append(f"unknown_adapter:{at}")
    if "candidate_adapters" not in receipt:
        reasons.append("missing_candidate_adapters")
    # Banned fields scan
    reasons.extend(_check_banned_fields(receipt))
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase41_invocation_receipt(
    receipt: Any,
    result: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return {"ok": False,
                "reasons": ["receipt_not_dict"]}
    if not isinstance(result, dict):
        reasons.append("result_not_dict")
    reasons.extend(_check_banned_fields(receipt))
    # Operator id only as hash
    if "operator_id" in receipt and receipt.get(
            "operator_id") not in (None, ""):
        reasons.append("raw_operator_id_present")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase41_result_against_governance(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False,
                "reasons": ["output_not_dict"]}
    reasons: list[str] = []
    status = str(output.get("status") or "")
    pipe = output.get("signed_witness_pipeline") or {}
    proj = output.get("replay_projection") or {}
    has_signed_ev = bool(
        (pipe.get("signed_evidence_summary") or {})
        .get("evidence_validates"))
    has_witness = (pipe.get("witness_export_summary") or {}
                    ).get("status") == "ok"
    has_exchange = (pipe.get("exchange_summary") or {}
                     ).get("status") in (
                         "ok", "witness_failed")
    has_proj = bool(proj)
    if status == "ok":
        if not has_signed_ev:
            reasons.append(
                "signed_evidence_required_for_ok")
        if not has_witness:
            reasons.append(
                "witness_export_required_for_ok")
        if not has_exchange:
            reasons.append(
                "exchange_required_for_ok")
        if not has_proj:
            reasons.append(
                "replay_projection_required_for_ok")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase41_complete_output(
    output: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(output, dict):
        return {"ok": False,
                "reasons": ["output_not_dict"]}
    result = output.get("selected_adapter_result") or {}
    sel_r = output.get("selection_receipt") or {}
    inv_r = output.get("invocation_receipt") or {}
    rv1 = verify_phase41_adapter_result(result)
    if not rv1["ok"]:
        reasons.extend(
            ["result:" + r for r in rv1["reasons"]])
    rv2 = verify_phase41_memory_privacy_result(result)
    if not rv2["ok"]:
        reasons.extend(
            ["mem_priv:" + r for r in rv2["reasons"]])
    rv3 = verify_phase41_selection_receipt(sel_r, result)
    if not rv3["ok"]:
        reasons.extend(
            ["sel:" + r for r in rv3["reasons"]])
    rv4 = verify_phase41_invocation_receipt(inv_r, result)
    if not rv4["ok"]:
        reasons.extend(
            ["inv:" + r for r in rv4["reasons"]])
    rv5 = verify_phase41_result_against_governance(output)
    if not rv5["ok"]:
        reasons.extend(
            ["gov:" + r for r in rv5["reasons"]])
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def write_phase41_result_verification_report(
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
    "verify_phase41_adapter_result",
    "verify_phase41_memory_privacy_result",
    "verify_phase41_selection_receipt",
    "verify_phase41_invocation_receipt",
    "verify_phase41_result_against_governance",
    "verify_phase41_complete_output",
    "write_phase41_result_verification_report",
]
