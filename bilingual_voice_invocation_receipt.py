"""Phase 30 - Invocation Receipt.

Generates an inspectable receipt for a Phase 30 dummy adapter call.
operator_id is never stored raw; only its SHA-256 hash. No transcript,
no audio, no commands.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase30.receipt.v1"


_REQUIRED_FIELDS = (
    "receipt_id", "created_at", "adapter_name", "adapter_type",
    "request_id", "result_id", "operator_id_hash", "dry_run",
    "test_only", "execution_boundary_preserved",
    "audio_generated", "tts_invoked", "subprocess_used",
    "network_used", "files_written", "pre_call_status",
    "post_call_status", "audit_chain_hash", "notes", "phase",
)


_FORBIDDEN_RECEIPT_KEYS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "subprocess", "powershell", "executable",
    "run_command", "transcript", "full_transcript",
    "user_text_raw", "assistant_text_raw", "operator_id",
)


def _new_id() -> str:
    return f"recv_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _hash_operator_id(s: str) -> str:
    h = hashlib.sha256()
    h.update(str(s or "").encode("utf-8"))
    return h.hexdigest()


def _hash_chain(chain: list[dict[str, Any]]) -> str:
    if not isinstance(chain, list) or not chain:
        return ""
    body = json.dumps(chain, sort_keys=True, default=str,
                      ensure_ascii=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_invocation_receipt(
    request: dict[str, Any],
    result: dict[str, Any],
    pre_call_validation: dict[str, Any],
    post_call_validation: dict[str, Any],
    audit_chain: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    rq = request if isinstance(request, dict) else {}
    rs = result if isinstance(result, dict) else {}
    pre = pre_call_validation if isinstance(pre_call_validation,
                                             dict) else {}
    post = post_call_validation if isinstance(post_call_validation,
                                                dict) else {}
    desc = rq.get("adapter_descriptor") or {}
    # Operator id may live raw on token if passed in, else use the hash
    op_hash = rq.get("operator_id_hash") or ""
    return {
        "receipt_id": _new_id(),
        "created_at": time.time(),
        "adapter_name": desc.get("adapter_name") or
            rs.get("adapter_name") or "",
        "adapter_type": desc.get("adapter_type") or
            rs.get("adapter_type") or "",
        "request_id": rq.get("request_id") or "",
        "result_id": rs.get("result_id") or "",
        "operator_id_hash": op_hash,
        "dry_run": True,
        "test_only": True,
        "execution_boundary_preserved": (
            bool(pre.get("ok")) and bool(post.get("ok"))),
        "audio_generated": False,
        "tts_invoked": False,
        "subprocess_used": False,
        "network_used": False,
        "files_written": False,
        "pre_call_status": ("ok" if pre.get("ok") else "blocked"),
        "post_call_status": ("ok" if post.get("ok") else "blocked"),
        "audit_chain_hash": _hash_chain(list(audit_chain or [])),
        "notes": ("phase30 invocation receipt; execution boundary "
                  "preserved; no audio; no engine call"),
        "phase": _PHASE,
    }


def validate_invocation_receipt(receipt: Any) -> dict[str, Any]:
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


def summarize_invocation_receipt(receipt: Any) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        return {"ok": False, "summary": "no_receipt"}
    return {
        "ok": True,
        "summary": (
            f"phase30 receipt: adapter="
            f"{receipt.get('adapter_name') or 'unknown'} "
            f"pre={receipt.get('pre_call_status')} "
            f"post={receipt.get('post_call_status')} "
            f"execution_boundary_preserved="
            f"{bool(receipt.get('execution_boundary_preserved'))}"),
        "receipt_id": receipt.get("receipt_id"),
        "execution_boundary_preserved":
            bool(receipt.get("execution_boundary_preserved")),
        "phase": _PHASE,
    }


def write_invocation_receipt(
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


def write_invocation_receipt_report(
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
    "create_invocation_receipt",
    "validate_invocation_receipt",
    "summarize_invocation_receipt",
    "write_invocation_receipt",
    "write_invocation_receipt_report",
]
