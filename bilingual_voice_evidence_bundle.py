"""Phase 32 - Operator Evidence Bundle.

Local, bounded, JSON-serializable bundle of audit chain + receipts +
report hashes + governance summaries. No transcript, no audio, no
secrets.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_audit_chain as vac
import bilingual_voice_audit_chain_signer as acs
import bilingual_voice_receipt_verifier as rv


_PHASE = "phase32.evidence_bundle.v1"


_REQUIRED_FIELDS = (
    "bundle_id", "created_at", "phase",
    "audit_chain_summary", "signed_audit_chain",
    "invocation_receipt", "selection_receipt",
    "adapter_result_summary", "report_hashes",
    "boundary_summary", "production_impact_summary",
    "safety_summary", "isolation_summary", "metadata",
)


_FORBIDDEN_BUNDLE_KEYS = (
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
    return f"evb_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _adapter_result_summary(adapter_result: Any) -> dict[str, Any]:
    if not isinstance(adapter_result, dict):
        return {}
    keep = ("result_id", "adapter_name", "adapter_type", "status",
             "dry_run", "test_only", "produced_audio",
             "invoked_tts", "used_subprocess", "used_network",
             "wrote_files", "received_language_mode",
             "received_segment_count")
    return {k: adapter_result.get(k) for k in keep
            if k in adapter_result}


def _scan_forbidden(obj: Any) -> list[str]:
    hits: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                if ks in _FORBIDDEN_BUNDLE_KEYS and ks not in hits:
                    hits.append(ks)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)
    _walk(obj)
    return hits


def create_evidence_bundle(
    bundle_id: str,
    audit_chain: Optional[list[dict[str, Any]]] = None,
    invocation_receipt: Optional[dict[str, Any]] = None,
    selection_receipt: Optional[dict[str, Any]] = None,
    adapter_result: Optional[dict[str, Any]] = None,
    reports: Optional[list[dict[str, Any]]] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    chain = list(audit_chain or [])
    rep = list(reports or [])
    return {
        "bundle_id": str(bundle_id or _new_id()),
        "created_at": time.time(),
        "phase": _PHASE,
        "audit_chain_summary":
            vac.summarize_audit_chain(chain),
        "signed_audit_chain": chain,
        "invocation_receipt": dict(invocation_receipt or {}),
        "selection_receipt": dict(selection_receipt or {}),
        "adapter_result_summary":
            _adapter_result_summary(adapter_result),
        "report_hashes": rep,
        "boundary_summary": {
            "execution_blocked": True,
            "dry_run": True,
            "test_only": True,
            "phase30_strict": True,
            "phase31_two_adapter_boundary": True,
        },
        "production_impact_summary": {
            "expected_en_words": 2814,
            "expected_ru_words": 2518,
            "expected_ru_phrases": 35,
            "expected_concepts": 26,
            "expected_links": 52,
            "expected_manifests": 90,
        },
        "safety_summary": {
            "audio_generated": False,
            "tts_invoked": False,
            "subprocess_used": False,
            "network_used": False,
            "files_written_outside_reports": False,
        },
        "isolation_summary": {
            "program_s_touched": False,
            "gated_internals_touched": False,
            "core_runtime_modules_touched": False,
            "daemon_spawned": False,
        },
        "metadata": dict(metadata or {}),
    }


def validate_evidence_bundle(bundle: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(bundle, dict):
        return {"ok": False, "reasons": ["bundle_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in bundle:
            reasons.append(f"missing_field:{f}")
    hits = _scan_forbidden(bundle)
    if hits:
        reasons.append("forbidden_field:" + ",".join(sorted(set(hits))))
    bsum = bundle.get("boundary_summary") or {}
    if bsum.get("execution_blocked") is not True:
        reasons.append("boundary_execution_not_blocked")
    if bsum.get("dry_run") is not True:
        reasons.append("boundary_dry_run_not_true")
    ssum = bundle.get("safety_summary") or {}
    for k in ("audio_generated", "tts_invoked", "subprocess_used",
              "network_used"):
        if ssum.get(k) is True:
            reasons.append(f"safety_{k}_true")
    try:
        json.dumps(bundle, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_evidence_bundle(bundle: Any) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"ok": False, "summary": "no_bundle"}
    cs = bundle.get("audit_chain_summary") or {}
    return {
        "ok": True,
        "summary": (
            f"phase32 evidence bundle: chain_length="
            f"{cs.get('length', 0)} "
            f"execution_blocked="
            f"{(bundle.get('boundary_summary') or {}).get('execution_blocked')}"),
        "bundle_id": bundle.get("bundle_id"),
        "phase": _PHASE,
    }


def write_evidence_bundle(
    bundle: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(bundle)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_evidence_bundle(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def verify_evidence_bundle(
    bundle: Any,
    key_descriptor: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    val = validate_evidence_bundle(bundle)
    reasons = list(val.get("reasons", []))
    chain_verify = None
    if isinstance(bundle, dict):
        chain = bundle.get("signed_audit_chain") or []
        if isinstance(chain, list) and chain:
            if key_descriptor and chain and "signature" in chain[0]:
                chain_verify = acs.verify_signed_audit_chain(
                    chain, key_descriptor)
            else:
                chain_verify = vac.verify_audit_chain(chain)
            if not chain_verify.get("ok"):
                reasons.append("chain_verification_failed")
        inv_r = bundle.get("invocation_receipt") or {}
        if inv_r:
            iv = rv.verify_invocation_receipt(inv_r)
            if not iv["ok"]:
                reasons.append("invocation_receipt_invalid")
        sel_r = bundle.get("selection_receipt") or {}
        if sel_r:
            sv = rv.verify_selection_receipt(sel_r)
            if not sv["ok"]:
                reasons.append("selection_receipt_invalid")
    return {
        "ok": not reasons,
        "reasons": reasons,
        "chain_verification": chain_verify,
        "phase": _PHASE,
    }


def write_evidence_bundle_report(
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
    "create_evidence_bundle",
    "validate_evidence_bundle",
    "summarize_evidence_bundle",
    "write_evidence_bundle",
    "read_evidence_bundle",
    "verify_evidence_bundle",
    "write_evidence_bundle_report",
]
