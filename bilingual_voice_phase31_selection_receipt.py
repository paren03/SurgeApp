"""Phase 31 - Selection Receipt.

Records why a particular metadata-only adapter was chosen.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase31.selection_receipt.v1"


_REQUIRED_FIELDS = (
    "receipt_id", "created_at", "selected_adapter_name",
    "selected_adapter_type", "candidate_adapters",
    "selection_reason", "score_summary", "request_id",
    "result_id", "dry_run", "test_only",
    "execution_boundary_preserved", "audio_generated",
    "tts_invoked", "subprocess_used", "network_used",
    "files_written", "audit_chain_hash", "notes", "phase",
)


_FORBIDDEN_RECEIPT_KEYS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
    "operator_id",
)


def _new_id() -> str:
    return f"selrec_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _hash_chain(chain: list[dict[str, Any]]) -> str:
    if not isinstance(chain, list) or not chain:
        return ""
    body = json.dumps(chain, sort_keys=True, default=str,
                      ensure_ascii=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_selection_receipt(
    request: dict[str, Any],
    selection_choice: dict[str, Any],
    adapter_result: Optional[dict[str, Any]] = None,
    comparison: Optional[dict[str, Any]] = None,
    audit_chain: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    rq = request if isinstance(request, dict) else {}
    sc = selection_choice if isinstance(selection_choice, dict) else {}
    chosen = sc.get("chosen") or {}
    rs = adapter_result if isinstance(adapter_result, dict) else {}
    return {
        "receipt_id": _new_id(),
        "created_at": time.time(),
        "selected_adapter_name": chosen.get("adapter_name") or "",
        "selected_adapter_type": chosen.get("adapter_type") or "",
        "candidate_adapters":
            list(sc.get("candidate_adapters") or []),
        "selection_reason": sc.get("reason") or "",
        "score_summary": dict(sc.get("score_summary") or {}),
        "request_id": rq.get("request_id") or "",
        "result_id": rs.get("result_id") or "",
        "dry_run": True,
        "test_only": True,
        "execution_boundary_preserved": (
            (rs.get("produced_audio") in (False, None)) and
            (rs.get("invoked_tts") in (False, None)) and
            (rs.get("used_subprocess") in (False, None)) and
            (rs.get("used_network") in (False, None)) and
            (rs.get("wrote_files") in (False, None))),
        "audio_generated": False,
        "tts_invoked": False,
        "subprocess_used": False,
        "network_used": False,
        "files_written": False,
        "audit_chain_hash": _hash_chain(list(audit_chain or [])),
        "comparison_summary": (comparison or {}),
        "notes": ("phase31 selection receipt; execution boundary "
                  "preserved; no audio"),
        "phase": _PHASE,
    }


def validate_selection_receipt(receipt: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return {"ok": False, "reasons": ["receipt_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in receipt:
            reasons.append(f"missing_field:{f}")
    if receipt.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    if receipt.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    for k in ("audio_generated", "tts_invoked", "subprocess_used",
              "network_used", "files_written"):
        if receipt.get(k) is not False:
            reasons.append(f"{k}_must_be_false")
    for k in _FORBIDDEN_RECEIPT_KEYS:
        if k in receipt:
            reasons.append(f"forbidden_field:{k}")
    try:
        json.dumps(receipt, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_selection_receipt(receipt: Any) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        return {"ok": False, "summary": "no_receipt"}
    return {
        "ok": True,
        "summary": (
            f"phase31 selection: adapter="
            f"{receipt.get('selected_adapter_name') or 'none'} "
            f"reason={receipt.get('selection_reason') or 'none'} "
            f"execution_boundary_preserved="
            f"{bool(receipt.get('execution_boundary_preserved'))}"),
        "receipt_id": receipt.get("receipt_id"),
        "phase": _PHASE,
    }


def write_selection_receipt(
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


def write_selection_receipt_report(
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
    "create_selection_receipt",
    "validate_selection_receipt",
    "summarize_selection_receipt",
    "write_selection_receipt",
    "write_selection_receipt_report",
]
