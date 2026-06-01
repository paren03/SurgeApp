"""Phase 32 - Receipt Verifier.

Verifies invocation receipts (Phase 30), selection receipts (Phase 31),
and adapter results. Fails closed on audio/TTS/subprocess/network/files
flags, raw operator_id leak, missing operator hash, or unknown adapter.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase32.receipt_verifier.v1"


_ALLOWED_METADATA_ONLY_ADAPTERS = (
    "dummy_metadata_adapter",
    "bilingual_segment_metadata_adapter",
)


_FORBIDDEN_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
)


def verify_receipt_boundary_flags(receipt: Any) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        return {"ok": False, "reasons": ["receipt_not_dict"]}
    reasons: list[str] = []
    # Phase 30 invocation receipt flags
    for k in ("audio_generated", "tts_invoked", "subprocess_used",
              "network_used", "files_written"):
        v = receipt.get(k)
        if v is True:
            reasons.append(f"{k}_true")
    return {"ok": not reasons, "reasons": reasons}


def verify_no_raw_operator_id(receipt: Any) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        return {"ok": False, "reasons": ["receipt_not_dict"]}
    reasons: list[str] = []
    if "operator_id" in receipt:
        reasons.append("raw_operator_id_present")
    # Phase 30 invocation receipt requires operator_id_hash
    if receipt.get("receipt_id", "").startswith("recv_") and not \
            receipt.get("operator_id_hash"):
        reasons.append("operator_id_hash_missing")
    return {"ok": not reasons, "reasons": reasons}


def verify_receipt_json_serializable(receipt: Any) -> dict[str, Any]:
    try:
        json.dumps(receipt, default=str)
        return {"ok": True, "reasons": []}
    except Exception as e:  # noqa: BLE001
        return {"ok": False,
                "reasons": [f"not_json_serializable:{type(e).__name__}"]}


def _no_forbidden_fields(obj: Any) -> dict[str, Any]:
    if not isinstance(obj, dict):
        return {"ok": False, "reasons": ["not_dict"]}
    hits = [k for k in _FORBIDDEN_FIELDS if k in obj]
    return {"ok": not hits,
            "reasons": ([f"forbidden_field:{k}" for k in hits])}


def verify_invocation_receipt(receipt: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return {"ok": False, "reasons": ["receipt_not_dict"]}
    bf = verify_receipt_boundary_flags(receipt)
    if not bf["ok"]:
        reasons.extend(bf["reasons"])
    rop = verify_no_raw_operator_id(receipt)
    if not rop["ok"]:
        reasons.extend(rop["reasons"])
    js = verify_receipt_json_serializable(receipt)
    if not js["ok"]:
        reasons.extend(js["reasons"])
    nf = _no_forbidden_fields(receipt)
    if not nf["ok"]:
        reasons.extend(nf["reasons"])
    adapter_name = str(receipt.get("adapter_name") or "")
    if adapter_name and adapter_name not in \
            _ALLOWED_METADATA_ONLY_ADAPTERS:
        reasons.append(f"unknown_adapter:{adapter_name}")
    if receipt.get("dry_run") is not True:
        reasons.append("dry_run_not_true")
    if receipt.get("test_only") is not True:
        reasons.append("test_only_not_true")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_selection_receipt(receipt: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return {"ok": False, "reasons": ["receipt_not_dict"]}
    bf = verify_receipt_boundary_flags(receipt)
    if not bf["ok"]:
        reasons.extend(bf["reasons"])
    rop = verify_no_raw_operator_id(receipt)
    if not rop["ok"]:
        reasons.extend(rop["reasons"])
    js = verify_receipt_json_serializable(receipt)
    if not js["ok"]:
        reasons.extend(js["reasons"])
    nf = _no_forbidden_fields(receipt)
    if not nf["ok"]:
        reasons.extend(nf["reasons"])
    sel_name = str(receipt.get("selected_adapter_name") or "")
    if sel_name and sel_name not in _ALLOWED_METADATA_ONLY_ADAPTERS:
        reasons.append(f"unknown_selected_adapter:{sel_name}")
    if receipt.get("dry_run") is not True:
        reasons.append("dry_run_not_true")
    if receipt.get("test_only") is not True:
        reasons.append("test_only_not_true")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_adapter_result_against_receipt(
    adapter_result: Any,
    receipt: Any,
) -> dict[str, Any]:
    if not isinstance(adapter_result, dict) or \
            not isinstance(receipt, dict):
        return {"ok": False, "reasons": ["bad_inputs"]}
    reasons: list[str] = []
    for k in ("produced_audio", "invoked_tts", "used_subprocess",
              "used_network", "wrote_files"):
        if adapter_result.get(k) is True:
            reasons.append(f"result_{k}_true")
    rid = receipt.get("result_id")
    if rid and adapter_result.get("result_id") and \
            rid != adapter_result.get("result_id"):
        reasons.append("result_id_mismatch")
    rname = (receipt.get("adapter_name") or
              receipt.get("selected_adapter_name") or "")
    aname = adapter_result.get("adapter_name") or ""
    if rname and aname and rname != aname:
        reasons.append("adapter_name_mismatch")
    if aname and aname not in _ALLOWED_METADATA_ONLY_ADAPTERS:
        reasons.append(f"unknown_adapter:{aname}")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def summarize_receipt_verification(
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(results, list):
        results = []
    total = len(results)
    ok = sum(1 for r in results if isinstance(r, dict)
              and r.get("ok"))
    failed = total - ok
    reasons: list[str] = []
    for r in results:
        if isinstance(r, dict) and not r.get("ok"):
            reasons.extend(r.get("reasons") or [])
    return {
        "total": total, "ok": ok, "failed": failed,
        "top_reasons": reasons[:10], "phase": _PHASE,
    }


def write_receipt_verification_report(
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
    "verify_invocation_receipt",
    "verify_selection_receipt",
    "verify_adapter_result_against_receipt",
    "verify_receipt_boundary_flags",
    "verify_no_raw_operator_id",
    "verify_receipt_json_serializable",
    "summarize_receipt_verification",
    "write_receipt_verification_report",
]
