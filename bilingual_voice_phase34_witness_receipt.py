"""Phase 34 - Witness Verification Receipt.

Records the outcome of an offline witness verification run.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase34.witness_receipt.v1"


_REQUIRED_FIELDS = (
    "receipt_id", "created_at", "package_id", "verifier_id",
    "verification_status", "checks_passed", "checks_failed",
    "checks_warned", "evidence_hash", "manifest_hash",
    "boundary_preserved", "secrets_absent", "audio_absent",
    "execution_absent", "notes", "phase",
)


_FORBIDDEN_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
    "operator_id", "private_key", "secret",
    "signing_key_material", "material_hex",
)


def _new_id() -> str:
    return f"wrec_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _hash_dict(obj: Any) -> str:
    if obj is None:
        return ""
    try:
        body = json.dumps(obj, sort_keys=True, default=str,
                          ensure_ascii=False)
    except Exception:  # noqa: BLE001
        return ""
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_witness_verification_receipt(
    verification_result: dict[str, Any],
    package_id: str,
    verifier_id: str = "local_phase34",
) -> dict[str, Any]:
    vr = verification_result if isinstance(verification_result,
                                            dict) else {}
    checks = vr.get("checks") or {}
    se_check = checks.get("signed_evidence") or {}
    im_check = checks.get("integrity_manifest") or {}
    bs_check = checks.get("boundary_summary") or {}
    sec_check = checks.get("no_secret_leakage") or {}
    return {
        "receipt_id": _new_id(),
        "created_at": time.time(),
        "package_id": str(package_id or ""),
        "verifier_id": str(verifier_id or "local_phase34"),
        "verification_status": str(vr.get("status") or "unknown"),
        "checks_passed": list(vr.get("checks_passed") or []),
        "checks_failed": list(vr.get("checks_failed") or []),
        "checks_warned": list(vr.get("checks_warned") or []),
        "evidence_hash": _hash_dict(se_check),
        "manifest_hash": _hash_dict(im_check),
        "boundary_preserved": bs_check.get("ok") is True,
        "secrets_absent": sec_check.get("ok") is True,
        "audio_absent": True,
        "execution_absent": True,
        "notes": ("phase34 witness verification receipt; "
                  "local offline verification"),
        "phase": _PHASE,
    }


def validate_witness_verification_receipt(
    receipt: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return {"ok": False, "reasons": ["receipt_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in receipt:
            reasons.append(f"missing_field:{f}")
    for k in _FORBIDDEN_FIELDS:
        if k in receipt:
            reasons.append(f"forbidden_field:{k}")
    if receipt.get("audio_absent") is not True:
        reasons.append("audio_absent_must_be_true")
    if receipt.get("execution_absent") is not True:
        reasons.append("execution_absent_must_be_true")
    try:
        json.dumps(receipt, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_witness_verification_receipt(
    receipt: Any,
) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        return {"ok": False, "summary": "no_receipt"}
    return {
        "ok": True,
        "summary": (
            f"phase34 witness receipt: status="
            f"{receipt.get('verification_status')} "
            f"boundary_preserved="
            f"{bool(receipt.get('boundary_preserved'))} "
            f"secrets_absent={bool(receipt.get('secrets_absent'))}"),
        "receipt_id": receipt.get("receipt_id"),
        "phase": _PHASE,
    }


def write_witness_verification_receipt(
    receipt: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(receipt)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_witness_receipt_report(
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
    "create_witness_verification_receipt",
    "validate_witness_verification_receipt",
    "summarize_witness_verification_receipt",
    "write_witness_verification_receipt",
    "write_witness_receipt_report",
]
