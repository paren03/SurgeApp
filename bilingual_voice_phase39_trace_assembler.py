"""Phase 39 - End-to-End Trace Assembler.

Joins per-scenario receipts into a single end-to-end rehearsal
trace. Never carries signing material; never carries the raw
operator_id; never carries the spoken render payload.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase39.trace_assembler.v1"


_REQUIRED_TRACE_FIELDS = (
    "trace_id", "created_at", "phase",
    "contract_id", "consent_id",
    "receipt_count", "receipts",
    "per_stage_coverage",
    "ok_receipt_count", "refused_receipt_count",
    "kill_switch_blocked_count",
    "adapter_distribution",
    "rehearsal_dry_run_only",
)


_EXPECTED_STAGE_KEYS = (
    "phase29_per_invocation_consent",
    "phase30_callable_boundary",
    "phase31_two_adapter_selection",
    "phase32_audit_chain_signing",
    "phase33_three_adapter_signed_evidence",
    "phase34_witness_export",
    "phase35_local_exchange",
    "phase36_optional_handoff",
    "phase37_signed_witness_pipeline",
)


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def assemble_rehearsal_trace(
    contract: dict[str, Any],
    consent: dict[str, Any],
    receipts: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(receipts, list):
        receipts = []
    rcpts = [dict(r) for r in receipts if isinstance(r, dict)]
    ok_count = sum(1 for r in rcpts
                    if r.get("status") in
                    ("ok", "ok_with_warnings"))
    refused = sum(1 for r in rcpts
                    if "refused" in
                    str(r.get("status") or ""))
    ks_blocked = sum(1 for r in rcpts
                      if r.get("kill_switch_blocked") is True)
    adapter_dist: dict[str, int] = {}
    for r in rcpts:
        name = r.get("selected_adapter_name") or "none"
        adapter_dist[name] = adapter_dist.get(name, 0) + 1
    coverage: dict[str, dict[str, int]] = {}
    for k in _EXPECTED_STAGE_KEYS:
        coverage[k] = {"present": 0, "absent": 0}
    for r in rcpts:
        sp = r.get("stages_present") or {}
        for k in _EXPECTED_STAGE_KEYS:
            if sp.get(k) is True:
                coverage[k]["present"] += 1
            else:
                coverage[k]["absent"] += 1
    chain_hashes = [_stable_hash(r) for r in rcpts]
    return {
        "trace_id": f"rtrace_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "contract_id": (contract or {}).get(
            "contract_id", ""),
        "consent_id": (consent or {}).get("consent_id", ""),
        "receipt_count": len(rcpts),
        "receipts": rcpts,
        "ok_receipt_count": ok_count,
        "refused_receipt_count": refused,
        "kill_switch_blocked_count": ks_blocked,
        "adapter_distribution": adapter_dist,
        "per_stage_coverage": coverage,
        "receipt_hash_chain": chain_hashes,
        "trace_root_hash": _stable_hash(chain_hashes),
        "rehearsal_dry_run_only": True,
        "notes": [
            "Trace carries scenario receipts only -- no raw "
            "spoken payload, no signing material, no raw "
            "operator_id.",
            "receipt_hash_chain is content-addressed for "
            "tamper-evidence at trace level.",
        ],
    }


def validate_rehearsal_trace(
    trace: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(trace, dict):
        return {"ok": False, "reasons": ["trace_not_dict"]}
    for f in _REQUIRED_TRACE_FIELDS:
        if f not in trace:
            reasons.append(f"missing_field:{f}")
    if trace.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    rcpts = trace.get("receipts") or []
    if not isinstance(rcpts, list):
        reasons.append("receipts_not_list")
    elif trace.get("receipt_count") != len(rcpts):
        reasons.append("receipt_count_mismatch")
    # Hash chain integrity
    expected = [_stable_hash(r) for r in rcpts
                 if isinstance(r, dict)]
    if trace.get("receipt_hash_chain") != expected:
        reasons.append("receipt_hash_chain_mismatch")
    expected_root = _stable_hash(expected)
    if trace.get("trace_root_hash") != expected_root:
        reasons.append("trace_root_hash_mismatch")
    # No secret leakage in trace top-level
    for k in ("signing_key_material", "private_key",
              "material_hex", "sealed_payload",
              "operator_id"):
        if k in trace:
            reasons.append(f"banned_field:{k}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_rehearsal_trace(
    trace: Any,
) -> dict[str, Any]:
    if not isinstance(trace, dict):
        return {"ok": False, "summary": "no_trace"}
    return {
        "ok": True,
        "summary": (
            f"phase39 trace: receipts="
            f"{trace.get('receipt_count')} "
            f"ok={trace.get('ok_receipt_count')} "
            f"refused={trace.get('refused_receipt_count')} "
            f"kill_switch_blocked="
            f"{trace.get('kill_switch_blocked_count')}"),
        "trace_id": trace.get("trace_id"),
        "phase": _PHASE,
    }


def write_rehearsal_trace(
    trace: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(trace)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "assemble_rehearsal_trace",
    "validate_rehearsal_trace",
    "summarize_rehearsal_trace",
    "write_rehearsal_trace",
]
