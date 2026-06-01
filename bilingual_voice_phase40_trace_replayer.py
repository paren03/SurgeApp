"""Phase 40 - Trace Replayer.

Re-derives the Phase 39 receipt hash chain and trace root hash
from the loaded stage receipts and compares against the stored
trace. Does NOT rerun adapters. Does NOT create new consent.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_phase37_adapter_interface as p37i


_PHASE = "phase40.trace_replayer.v1"


_BANNED_RECEIPT_FIELDS = (
    "produced_audio", "invoked_tts",
    "used_subprocess", "used_network", "wrote_files",
)


_SECRET_FIELDS = (
    "signing_key_material", "private_key",
    "material_hex", "sealed_payload",
    "raw_secret", "secret_material",
)


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _extract_receipt_object(r: Any) -> dict[str, Any]:
    """A loaded receipt from the loader is a wrapper dict with
    an 'object' key carrying the actual receipt body. Tolerate
    raw-receipt input too."""
    if isinstance(r, dict):
        if "object" in r and isinstance(r["object"], dict):
            return r["object"]
        return r
    return {}


def _strip_loader_metadata(receipt: dict[str, Any]) -> dict[str, Any]:
    """Phase 39 trace assembler hashes the receipt dict directly
    BEFORE the loader's per-write 'written_at' is added. We must
    match the in-memory form."""
    body = dict(receipt)
    body.pop("written_at", None)
    return body


def rederive_receipt_hash_chain(
    stage_receipts: list[Any],
) -> list[str]:
    out: list[str] = []
    for r in stage_receipts or []:
        body = _strip_loader_metadata(
            _extract_receipt_object(r))
        out.append(_stable_hash(body))
    return out


def rederive_trace_root_hash(
    receipt_hash_chain: list[str],
) -> str:
    return _stable_hash(list(receipt_hash_chain or []))


def _check_receipt_safety(
    receipt: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    name = receipt.get("selected_adapter_name")
    if name and name not in p37i.ALLOWED_ADAPTER_TYPES:
        reasons.append(f"disallowed_adapter:{name}")
    for k in _BANNED_RECEIPT_FIELDS:
        if receipt.get(k) is True:
            reasons.append(f"runtime_flag:{k}")
    for k in _SECRET_FIELDS:
        if k in receipt:
            reasons.append(f"secret_field:{k}")
    if "operator_id" in receipt and receipt.get(
            "operator_id") not in (None, ""):
        reasons.append("raw_operator_id")
    return reasons


def detect_trace_tampering(
    artifacts: Any,
) -> dict[str, Any]:
    if not isinstance(artifacts, dict):
        return {"ok": False,
                "reasons": ["artifacts_not_dict"],
                "phase": _PHASE}
    loaded = artifacts.get("loaded") or {}
    trace_w = loaded.get("rehearsal_trace") or {}
    stored_trace = trace_w.get("object") \
        if isinstance(trace_w, dict) else None
    if not isinstance(stored_trace, dict):
        return {"ok": False,
                "reasons": ["stored_trace_missing"],
                "phase": _PHASE}
    receipts = artifacts.get("stage_receipts") or []
    bodies = [_strip_loader_metadata(
        _extract_receipt_object(r)) for r in receipts]
    rederived_chain = [_stable_hash(b) for b in bodies]
    rederived_root = _stable_hash(rederived_chain)
    stored_chain = stored_trace.get(
        "receipt_hash_chain") or []
    stored_root = stored_trace.get("trace_root_hash") or ""
    # If we have direct receipts in the trace, also verify
    # their hashes match
    inline_receipts = stored_trace.get("receipts") or []
    inline_chain = [_stable_hash(r) for r in inline_receipts
                     if isinstance(r, dict)]
    reasons: list[str] = []
    chain_matches_loaded = (rederived_chain == stored_chain)
    chain_matches_inline = (inline_chain == stored_chain)
    # Strict policy: if loaded receipts exist, they MUST match
    # the stored chain. Fallback to inline-matching is only
    # allowed when no loaded receipts are present.
    if bodies:
        if not chain_matches_loaded:
            reasons.append("hash_chain_drift")
    else:
        if not chain_matches_inline:
            reasons.append("hash_chain_drift")
    expected_root_from_stored = _stable_hash(stored_chain)
    if stored_root != expected_root_from_stored:
        reasons.append("trace_root_hash_drift")
    if (rederived_root != stored_root
            and not chain_matches_inline):
        reasons.append("trace_root_hash_rederive_drift")
    # Safety checks per receipt
    bad: list[dict[str, Any]] = []
    for b in bodies:
        safety = _check_receipt_safety(b)
        if safety:
            bad.append({
                "scenario_id": b.get("scenario_id"),
                "reasons": safety,
            })
    if bad:
        reasons.append("receipt_safety_violation")
    return {
        "ok": not reasons,
        "reasons": reasons,
        "rederived_root_hash": rederived_root,
        "stored_root_hash": stored_root,
        "chain_matches_loaded_receipts":
            chain_matches_loaded,
        "chain_matches_inline_receipts":
            chain_matches_inline,
        "bad_receipts": bad,
        "phase": _PHASE,
    }


def compare_replayed_trace_to_stored(
    replayed: dict[str, Any],
    stored_trace: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(replayed, dict) or not isinstance(
            stored_trace, dict):
        return {"ok": False,
                "reasons": ["non_dict_input"],
                "phase": _PHASE}
    reasons: list[str] = []
    if (replayed.get("rederived_root_hash")
            != stored_trace.get("trace_root_hash")):
        reasons.append("trace_root_hash_mismatch")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def replay_phase39_trace(
    artifacts: Any,
) -> dict[str, Any]:
    if not isinstance(artifacts, dict):
        return {"ok": False, "phase": _PHASE,
                "reason": "artifacts_not_dict"}
    loaded = artifacts.get("loaded") or {}
    trace_w = loaded.get("rehearsal_trace") or {}
    stored_trace = trace_w.get("object") \
        if isinstance(trace_w, dict) else None
    receipts = artifacts.get("stage_receipts") or []
    if not isinstance(stored_trace, dict):
        return {"ok": False, "phase": _PHASE,
                "reason": "stored_trace_missing"}
    chain = rederive_receipt_hash_chain(receipts)
    root = rederive_trace_root_hash(chain)
    tamper = detect_trace_tampering(artifacts)
    expected_count = stored_trace.get("receipt_count")
    chain_count_ok = (expected_count is None
                       or len(chain) == int(expected_count)
                       or len(stored_trace.get("receipts") or [])
                       == int(expected_count))
    return {
        "replay_id": f"trplay_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "rederived_receipt_count": len(chain),
        "stored_receipt_count": expected_count,
        "chain_count_ok": chain_count_ok,
        "rederived_receipt_hash_chain": chain,
        "rederived_root_hash": root,
        "stored_root_hash":
            stored_trace.get("trace_root_hash"),
        "tampering_check": tamper,
        "ok": (tamper.get("ok") is True
               and chain_count_ok),
    }


def write_phase40_trace_replay_report(
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
    "rederive_receipt_hash_chain",
    "rederive_trace_root_hash",
    "compare_replayed_trace_to_stored",
    "detect_trace_tampering",
    "replay_phase39_trace",
    "write_phase40_trace_replay_report",
]
