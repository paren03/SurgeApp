"""Phase 32 - Audit Chain Signer / Verifier.

Signs Phase 29 audit-chain events with HMAC-SHA256 (test-only). Each
signed event carries `signature_metadata` + `signature` (hex digest)
covering the canonical JSON of the event excluding the signature
itself. Chain hash verification still runs before signature
verification.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_audit_signing_policy as asp
import bilingual_voice_audit_chain as vac


_PHASE = "phase32.audit_signer.v1"
_HARD_CAP = 1000


_SIG_EXCLUDED = ("signature", "signature_metadata")


def _strip_for_signing(event: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in event.items() if k not in _SIG_EXCLUDED}


def sign_audit_chain_event(
    event: Any,
    key_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(event, dict):
        return {"ok": False, "reasons": ["event_not_dict"]}
    kv = asp.validate_signing_key_descriptor(key_descriptor)
    if not kv["ok"]:
        return {"ok": False,
                "reasons": ["key_invalid:" + ",".join(kv["reasons"])]}
    payload = _strip_for_signing(event)
    sig = asp.sign_payload(payload, key_descriptor)
    meta = asp.create_signature_metadata(
        algorithm="HMAC-SHA256",
        key_id=str(key_descriptor.get("key_id") or "test"))
    out = dict(event)
    out["signature_metadata"] = meta
    out["signature"] = sig
    return {"ok": True, "signed_event": out, "phase": _PHASE}


def verify_signed_audit_chain_event(
    signed_event: Any,
    key_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(signed_event, dict):
        return {"ok": False, "reasons": ["event_not_dict"]}
    if "signature" not in signed_event:
        return {"ok": False, "reasons": ["signature_missing"]}
    if "signature_metadata" not in signed_event:
        return {"ok": False, "reasons": ["signature_metadata_missing"]}
    sig_meta_val = asp.validate_signature_metadata(
        signed_event.get("signature_metadata"))
    if not sig_meta_val["ok"]:
        return {"ok": False,
                "reasons": ["signature_metadata_invalid:" +
                             ",".join(sig_meta_val["reasons"])]}
    # Chain hash must match first (this catches event-content tampering)
    ev_val = vac.validate_audit_chain_event(
        _strip_for_signing(signed_event))
    if not ev_val["ok"]:
        return {"ok": False,
                "reasons": ["event_hash_mismatch:" +
                             ",".join(ev_val["reasons"])]}
    kv = asp.validate_signing_key_descriptor(key_descriptor)
    if not kv["ok"]:
        return {"ok": False,
                "reasons": ["key_invalid:" + ",".join(kv["reasons"])]}
    payload = _strip_for_signing(signed_event)
    expected = asp.sign_payload(payload, key_descriptor)
    if expected != signed_event["signature"]:
        return {"ok": False, "reasons": ["signature_mismatch"]}
    return {"ok": True, "reasons": [], "phase": _PHASE}


def sign_audit_chain(
    chain: Any,
    key_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(chain, list):
        return {"ok": False, "reasons": ["chain_not_list"]}
    cap = min(len(chain), _HARD_CAP)
    out: list[dict[str, Any]] = []
    for ev in chain[:cap]:
        r = sign_audit_chain_event(ev, key_descriptor)
        if not r["ok"]:
            return {"ok": False,
                    "reasons": ["sign_failed:" +
                                 ",".join(r.get("reasons", []))],
                    "partial": out}
        out.append(r["signed_event"])
    return {
        "ok": True,
        "signed_chain": out,
        "length": len(out),
        "phase": _PHASE,
    }


def verify_signed_audit_chain(
    signed_chain: Any,
    key_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(signed_chain, list):
        return {"ok": False, "reasons": ["chain_not_list"], "length": 0}
    reasons: list[str] = []
    prev = ""
    for i, ev in enumerate(signed_chain):
        r = verify_signed_audit_chain_event(ev, key_descriptor)
        if not r["ok"]:
            reasons.append(f"event_{i}_invalid:" +
                           ",".join(r.get("reasons", [])))
            continue
        if ev.get("previous_hash") != prev:
            reasons.append(f"event_{i}_broken_chain")
        prev = ev.get("event_hash") or ""
    return {
        "ok": not reasons,
        "reasons": reasons,
        "length": len(signed_chain),
        "phase": _PHASE,
    }


def detect_signed_chain_tampering(
    signed_chain: Any,
    key_descriptor: Any,
) -> dict[str, Any]:
    v = verify_signed_audit_chain(signed_chain, key_descriptor)
    return {
        "tampered": not v["ok"],
        "reasons": v.get("reasons", []),
        "phase": _PHASE,
    }


def write_signed_audit_chain(
    signed_chain: list[dict[str, Any]],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "phase": _PHASE,
        "written_at": time.time(),
        "length": len(signed_chain),
        "signed_chain": list(signed_chain or [])[-_HARD_CAP:],
    }
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_signed_audit_chain(
    path: str,
    limit: int = _HARD_CAP,
) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    cap = max(1, min(int(limit or 1), _HARD_CAP))
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    chain = body.get("signed_chain") if isinstance(body, dict) else None
    if not isinstance(chain, list):
        return []
    return chain[:cap]


def write_audit_chain_signer_report(
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
    "sign_audit_chain_event",
    "verify_signed_audit_chain_event",
    "sign_audit_chain",
    "verify_signed_audit_chain",
    "detect_signed_chain_tampering",
    "write_signed_audit_chain",
    "read_signed_audit_chain",
    "write_audit_chain_signer_report",
]
