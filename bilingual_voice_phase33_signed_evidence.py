"""Phase 33 - Signed Evidence by Default.

Wraps Phase 32 evidence bundle + signed audit chain into a single
JSON-serializable Phase 33 evidence object. Default key is HMAC-SHA256
test-only.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_audit_signing_policy as asp
import bilingual_voice_audit_chain_signer as acs
import bilingual_voice_evidence_bundle as veb


_PHASE = "phase33.signed_evidence.v1"


_REQUIRED_FIELDS = (
    "evidence_id", "created_at", "phase",
    "signed_audit_chain", "audit_chain_summary",
    "evidence_bundle", "boundary_summary",
    "signing_metadata",
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
    return f"sigev_{int(time.time())}_{uuid.uuid4().hex[:10]}"


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
                if ks in _FORBIDDEN_FIELDS and ks not in hits:
                    hits.append(ks)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)
    _walk(obj)
    return hits


def _default_key() -> dict[str, Any]:
    return asp.create_test_signing_key("phase33_test_key")


def create_phase33_signed_evidence(
    invocation_output: dict[str, Any],
    key_descriptor: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    out = invocation_output if isinstance(invocation_output,
                                           dict) else {}
    key = key_descriptor if isinstance(key_descriptor, dict) else \
        _default_key()
    kv = asp.validate_signing_key_descriptor(key)
    if not kv["ok"]:
        return {
            "ok": False, "evidence_id": "",
            "reasons": ["key_invalid:" + ",".join(kv["reasons"])],
            "phase": _PHASE,
        }
    chain = out.get("audit_chain") or []
    inv_r = out.get("invocation_receipt") or {}
    sel_r = out.get("selection_receipt") or {}
    adapter_result = out.get("selected_adapter_result") or {}
    sign_res = acs.sign_audit_chain(chain, key) \
        if isinstance(chain, list) else {"ok": False,
                                           "signed_chain": []}
    signed_chain = sign_res.get("signed_chain", []) \
        if sign_res.get("ok") else []
    bundle = veb.create_evidence_bundle(
        bundle_id=f"phase33_bundle_{int(time.time())}",
        audit_chain=signed_chain,
        invocation_receipt=inv_r,
        selection_receipt=sel_r,
        adapter_result=adapter_result,
        reports=[])
    return {
        "evidence_id": _new_id(),
        "created_at": time.time(),
        "phase": _PHASE,
        "signed_audit_chain": signed_chain,
        "audit_chain_summary":
            bundle.get("audit_chain_summary") or {},
        "evidence_bundle": bundle,
        "boundary_summary": {
            "execution_blocked": True,
            "dry_run": True,
            "test_only": True,
            "signed_by_default": True,
        },
        "signing_metadata": {
            "algorithm": "HMAC-SHA256",
            "key_id": key.get("key_id"),
            "key_label": key.get("label"),
            "test_only": True,
        },
        "notes": ("phase33 signed evidence; default key is test-only; "
                  "no production secrets"),
    }


def validate_phase33_signed_evidence(
    evidence: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(evidence, dict):
        return {"ok": False, "reasons": ["evidence_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in evidence:
            reasons.append(f"missing_field:{f}")
    hits = _scan_forbidden(evidence)
    if hits:
        reasons.append("forbidden_field:" +
                       ",".join(sorted(set(hits))))
    bsum = evidence.get("boundary_summary") or {}
    if bsum.get("execution_blocked") is not True:
        reasons.append("execution_not_blocked")
    if bsum.get("dry_run") is not True:
        reasons.append("dry_run_not_true")
    sm = evidence.get("signing_metadata") or {}
    if sm.get("test_only") is not True:
        reasons.append("signing_metadata_not_test_only")
    if str(sm.get("algorithm") or "") != "HMAC-SHA256":
        reasons.append("signing_metadata_wrong_algorithm")
    try:
        json.dumps(evidence, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase33_signed_evidence(
    evidence: Any,
    key_descriptor: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    val = validate_phase33_signed_evidence(evidence)
    reasons = list(val.get("reasons", []))
    chain_verify = None
    if isinstance(evidence, dict):
        chain = evidence.get("signed_audit_chain") or []
        if not isinstance(chain, list):
            reasons.append("signed_chain_not_list")
        elif key_descriptor:
            chain_verify = acs.verify_signed_audit_chain(
                chain, key_descriptor)
            if not chain_verify.get("ok"):
                reasons.append("chain_verification_failed:" +
                               ",".join(chain_verify.get(
                                   "reasons", [])))
    return {
        "ok": not reasons,
        "reasons": reasons,
        "chain_verification": chain_verify,
        "phase": _PHASE,
    }


def summarize_phase33_signed_evidence(
    evidence: Any,
) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        return {"ok": False, "summary": "no_evidence"}
    cs = evidence.get("audit_chain_summary") or {}
    sm = evidence.get("signing_metadata") or {}
    return {
        "ok": True,
        "summary": (
            f"phase33 signed evidence: chain_length="
            f"{cs.get('length', 0)} "
            f"algorithm={sm.get('algorithm')} "
            f"test_only={sm.get('test_only')}"),
        "evidence_id": evidence.get("evidence_id"),
        "phase": _PHASE,
    }


def write_phase33_signed_evidence(
    evidence: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(evidence)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase33_signed_evidence_report(
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
    "create_phase33_signed_evidence",
    "validate_phase33_signed_evidence",
    "verify_phase33_signed_evidence",
    "write_phase33_signed_evidence",
    "summarize_phase33_signed_evidence",
    "write_phase33_signed_evidence_report",
]
