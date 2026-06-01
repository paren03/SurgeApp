"""Phase 33 - Result Verifier.

Combines Phase 33 adapter result + receipt + signed-evidence checks.
Fails closed on any execution flag, missing evidence on success, or
unknown adapter.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_phase33_adapter_interface as p33i
import bilingual_voice_receipt_verifier as rv
import bilingual_voice_phase33_signed_evidence as p33s


_PHASE = "phase33.result_verifier.v1"


_ALLOWED_ADAPTERS = set(p33i.ALLOWED_ADAPTER_TYPES)


def verify_phase33_adapter_result(result: Any) -> dict[str, Any]:
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
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_phase33_selection_receipt(
    receipt: Any,
    result: Any,
) -> dict[str, Any]:
    base = rv.verify_selection_receipt(receipt)
    reasons = list(base.get("reasons", []))
    if isinstance(receipt, dict) and isinstance(result, dict):
        sel_name = str(receipt.get("selected_adapter_name") or "")
        res_name = str(result.get("adapter_name") or "")
        if sel_name and res_name and sel_name != res_name:
            reasons.append("selected_vs_result_adapter_mismatch")
        if sel_name and sel_name not in _ALLOWED_ADAPTERS:
            reasons.append(f"unknown_selected_adapter:{sel_name}")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_phase33_invocation_receipt(
    receipt: Any,
    result: Any,
) -> dict[str, Any]:
    base = rv.verify_invocation_receipt(receipt)
    reasons = list(base.get("reasons", []))
    if isinstance(receipt, dict) and isinstance(result, dict):
        rname = str(receipt.get("adapter_name") or "")
        res_name = str(result.get("adapter_name") or "")
        if rname and res_name and rname != res_name:
            reasons.append("receipt_vs_result_adapter_mismatch")
        if rname and rname not in _ALLOWED_ADAPTERS:
            reasons.append(f"unknown_adapter:{rname}")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_phase33_result_against_evidence(
    result: Any,
    evidence: Any,
) -> dict[str, Any]:
    if not isinstance(result, dict) or not isinstance(evidence, dict):
        return {"ok": False, "reasons": ["bad_inputs"]}
    reasons: list[str] = []
    val = p33s.validate_phase33_signed_evidence(evidence)
    if not val["ok"]:
        reasons.extend(val["reasons"])
    bundle = evidence.get("evidence_bundle") or {}
    ar_summary = bundle.get("adapter_result_summary") or {}
    res_name = str(result.get("adapter_name") or "")
    ev_name = str(ar_summary.get("adapter_name") or "")
    if res_name and ev_name and res_name != ev_name:
        reasons.append("evidence_vs_result_adapter_mismatch")
    res_id = str(result.get("result_id") or "")
    ev_id = str(ar_summary.get("result_id") or "")
    if res_id and ev_id and res_id != ev_id:
        reasons.append("evidence_vs_result_id_mismatch")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_phase33_complete_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    reasons: list[str] = []
    status = str(output.get("status") or "")
    result = output.get("selected_adapter_result") or {}
    if status == "ok":
        rv_check = verify_phase33_adapter_result(result)
        if not rv_check["ok"]:
            reasons.extend([f"result:{r}"
                             for r in rv_check["reasons"]])
        inv_r = output.get("invocation_receipt") or {}
        sel_r = output.get("selection_receipt") or {}
        if inv_r:
            ic = verify_phase33_invocation_receipt(inv_r, result)
            if not ic["ok"]:
                reasons.extend([f"invocation:{r}"
                                 for r in ic["reasons"]])
        if sel_r:
            sc = verify_phase33_selection_receipt(sel_r, result)
            if not sc["ok"]:
                reasons.extend([f"selection:{r}"
                                 for r in sc["reasons"]])
        ev = output.get("signed_evidence") or {}
        if not ev:
            reasons.append("signed_evidence_missing_for_ok")
        else:
            ec = verify_phase33_result_against_evidence(result, ev)
            if not ec["ok"]:
                reasons.extend([f"evidence:{r}"
                                 for r in ec["reasons"]])
    return {
        "ok": not reasons,
        "reasons": reasons,
        "status": status,
        "phase": _PHASE,
    }


def write_phase33_result_verification_report(
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
    "verify_phase33_adapter_result",
    "verify_phase33_selection_receipt",
    "verify_phase33_invocation_receipt",
    "verify_phase33_result_against_evidence",
    "verify_phase33_complete_output",
    "write_phase33_result_verification_report",
]
