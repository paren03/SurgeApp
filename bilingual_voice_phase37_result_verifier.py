"""Phase 37 - Result Verifier."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_phase37_adapter_interface as p37i
import bilingual_voice_receipt_verifier as rv
import bilingual_voice_phase37_signed_witness_pipeline as p37p
import bilingual_voice_phase36_secret_boundary as sb


_PHASE = "phase37.result_verifier.v1"


_ALLOWED_ADAPTERS = set(p37i.ALLOWED_ADAPTER_TYPES)


def verify_phase37_adapter_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    reasons: list[str] = []
    for k in ("produced_audio", "invoked_tts", "used_subprocess",
              "used_network", "wrote_files"):
        if result.get(k) is True:
            reasons.append(f"{k}_true")
    name = str(result.get("adapter_name") or "")
    if name and name not in _ALLOWED_ADAPTERS:
        reasons.append(f"unknown_adapter:{name}")
    if result.get("dry_run") is not True:
        reasons.append("dry_run_not_true")
    if result.get("test_only") is not True:
        reasons.append("test_only_not_true")
    forbidden = ("audio_bytes", "audio_url", "audio_path",
                  "wav_path", "mp3_path", "voice_clone_ref",
                  "speaker_embedding", "tts_model_path",
                  "output_audio_file", "command", "shell",
                  "powershell_command", "executable",
                  "run_command")
    for k in forbidden:
        if k in result:
            reasons.append(f"forbidden_field:{k}")
    hits = sb.scan_object_for_secret_fields(result)
    if hits:
        reasons.append("secret_leak:" + ",".join(sorted(set(hits))))
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


_RECEIPT_FORBIDDEN_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
)


def _common_receipt_checks(receipt: Any) -> list[str]:
    """Phase 37 inline replacement for Phase 32 receipt validator.
    Phase 32's validator hard-codes only 2 allowed adapters; Phase 37
    permits all 4, so we run the same boundary/secret/JSON checks
    locally and check adapter-name against the Phase 37 allowlist."""
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return ["receipt_not_dict"]
    # Execution-flag boundary
    for k in ("audio_generated", "tts_invoked", "subprocess_used",
              "network_used", "files_written"):
        if receipt.get(k) is True:
            reasons.append(f"{k}_true")
    # Raw operator_id leak / operator_id_hash required for
    # invocation receipts (those start with recv_)
    if "operator_id" in receipt:
        reasons.append("raw_operator_id_present")
    if str(receipt.get("receipt_id") or "").startswith("recv_") \
            and not receipt.get("operator_id_hash"):
        reasons.append("operator_id_hash_missing")
    # Forbidden audio / command keys
    for k in _RECEIPT_FORBIDDEN_FIELDS:
        if k in receipt:
            reasons.append(f"forbidden_field:{k}")
    # dry_run / test_only
    if receipt.get("dry_run") is not True:
        reasons.append("dry_run_not_true")
    if receipt.get("test_only") is not True:
        reasons.append("test_only_not_true")
    # JSON-serializable
    try:
        json.dumps(receipt, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return reasons


def verify_phase37_selection_receipt(
    receipt: Any,
    result: Any,
) -> dict[str, Any]:
    reasons = _common_receipt_checks(receipt)
    if isinstance(receipt, dict) and isinstance(result, dict):
        sel_name = str(receipt.get("selected_adapter_name") or "")
        res_name = str(result.get("adapter_name") or "")
        if sel_name and res_name and sel_name != res_name:
            reasons.append("selected_vs_result_adapter_mismatch")
        if sel_name and sel_name not in _ALLOWED_ADAPTERS:
            reasons.append(f"unknown_selected_adapter:{sel_name}")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_phase37_invocation_receipt(
    receipt: Any,
    result: Any,
) -> dict[str, Any]:
    reasons = _common_receipt_checks(receipt)
    if isinstance(receipt, dict) and isinstance(result, dict):
        rname = str(receipt.get("adapter_name") or "")
        res_name = str(result.get("adapter_name") or "")
        if rname and res_name and rname != res_name:
            reasons.append("receipt_vs_result_adapter_mismatch")
        if rname and rname not in _ALLOWED_ADAPTERS:
            reasons.append(f"unknown_adapter:{rname}")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_phase37_result_against_pipeline(
    result: Any,
    pipeline_output: Any,
) -> dict[str, Any]:
    if not isinstance(result, dict) or \
            not isinstance(pipeline_output, dict):
        return {"ok": False, "reasons": ["bad_inputs"]}
    reasons: list[str] = []
    pv = p37p.verify_phase37_signed_witness_pipeline(
        pipeline_output)
    if not pv["ok"]:
        reasons.extend([f"pipeline:{r}"
                         for r in pv.get("reasons", [])])
    hits = sb.scan_object_for_secret_fields(pipeline_output)
    real = [h for h in hits if h != "consent_marker_hash"]
    if real:
        reasons.append("pipeline_secret_leak:" +
                       ",".join(sorted(set(real))))
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_phase37_complete_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    reasons: list[str] = []
    status = str(output.get("status") or "")
    result = output.get("selected_adapter_result") or {}
    if status == "ok":
        rc = verify_phase37_adapter_result(result)
        if not rc["ok"]:
            reasons.extend([f"result:{r}"
                             for r in rc["reasons"]])
        inv_r = output.get("invocation_receipt") or {}
        sel_r = output.get("selection_receipt") or {}
        if inv_r:
            ic = verify_phase37_invocation_receipt(inv_r, result)
            if not ic["ok"]:
                reasons.extend([f"invocation:{r}"
                                 for r in ic["reasons"]])
        if sel_r:
            sc = verify_phase37_selection_receipt(sel_r, result)
            if not sc["ok"]:
                reasons.extend([f"selection:{r}"
                                 for r in sc["reasons"]])
        pipe = output.get("signed_witness_pipeline") or {}
        if not pipe:
            reasons.append("signed_witness_pipeline_missing_for_ok")
        else:
            pc = verify_phase37_result_against_pipeline(
                result, pipe)
            if not pc["ok"]:
                reasons.extend([f"pipeline:{r}"
                                 for r in pc["reasons"]])
    return {
        "ok": not reasons,
        "reasons": reasons,
        "status": status,
        "phase": _PHASE,
    }


def write_phase37_result_verification_report(
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
    "verify_phase37_adapter_result",
    "verify_phase37_selection_receipt",
    "verify_phase37_invocation_receipt",
    "verify_phase37_result_against_pipeline",
    "verify_phase37_complete_output",
    "write_phase37_result_verification_report",
]
