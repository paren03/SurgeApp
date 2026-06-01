"""Phase 29 - Tamper-Evident Audit Chain.

Hash-linked event chain for dry-run adapter governance events. Each
event records previous_hash + event_hash so post-hoc tampering can be
detected by re-hashing.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase29.audit_chain.v1"


_SUPPORTED_EVENT_TYPES = (
    "preflight",
    "consent_request",
    "consent_decision",
    "invocation_token_created",
    "invocation_token_validated",
    "calltime_boundary",
    "review_packet_created",
    "queue_enqueued",
    "refusal",
    "error",
    "dry_run_complete",
)

_SUPPORTED_STATUS = (
    "ok", "warn", "error", "refused", "blocked", "skipped", "info",
)

_HARD_CAP = 1000
_DEFAULT_READ_LIMIT = 1000


_HASH_EXCLUDED_KEYS = ("event_hash",)


def _new_event_id() -> str:
    return f"chain_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _strip_for_hash(event: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in event.items() if k not in _HASH_EXCLUDED_KEYS}


def compute_event_hash(event: Any) -> str:
    if not isinstance(event, dict):
        return ""
    body = _strip_for_hash(event)
    s = json.dumps(body, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def create_audit_chain_event(
    event_type: str,
    status: str,
    message: str = "",
    metadata: Optional[dict[str, Any]] = None,
    previous_hash: str = "",
) -> dict[str, Any]:
    et = str(event_type or "").lower()
    if et not in _SUPPORTED_EVENT_TYPES:
        et = "info"
    st = str(status or "").lower()
    if st not in _SUPPORTED_STATUS:
        st = "info"
    md = dict(metadata or {})
    for k in ("transcript", "full_transcript", "user_text_raw",
              "assistant_text_raw"):
        md.pop(k, None)
    event: dict[str, Any] = {
        "event_id": _new_event_id(),
        "created_at": time.time(),
        "event_type": et,
        "status": st,
        "message": str(message or "")[:512],
        "metadata": md,
        "previous_hash": str(previous_hash or ""),
        "phase": _PHASE,
    }
    event["event_hash"] = compute_event_hash(event)
    return event


def validate_audit_chain_event(event: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(event, dict):
        return {"ok": False, "reasons": ["event_not_dict"]}
    for f in ("event_id", "created_at", "event_type", "status",
              "message", "metadata", "previous_hash", "event_hash",
              "phase"):
        if f not in event:
            reasons.append(f"missing_field:{f}")
    if event.get("event_type") not in _SUPPORTED_EVENT_TYPES:
        reasons.append("unsupported_event_type")
    if event.get("status") not in _SUPPORTED_STATUS:
        reasons.append("unsupported_status")
    if not isinstance(event.get("metadata"), dict):
        reasons.append("metadata_not_dict")
    else:
        for k in ("transcript", "full_transcript", "user_text_raw",
                  "assistant_text_raw"):
            if k in event["metadata"]:
                reasons.append(f"forbidden_metadata_key:{k}")
    computed = compute_event_hash(event)
    if computed != event.get("event_hash"):
        reasons.append("event_hash_mismatch")
    return {"ok": not reasons, "reasons": reasons}


def append_chain_event(
    chain: list[dict[str, Any]],
    event: dict[str, Any],
    limit: int = _HARD_CAP,
) -> list[dict[str, Any]]:
    if not isinstance(chain, list):
        chain = []
    cap = max(1, min(int(limit or 1), _HARD_CAP))
    val = validate_audit_chain_event(event)
    if not val["ok"]:
        return chain
    # If previous_hash empty but chain non-empty, repair it
    if chain and not event.get("previous_hash"):
        event = dict(event)
        event["previous_hash"] = chain[-1].get("event_hash") or ""
        event["event_hash"] = compute_event_hash(event)
    chain.append(event)
    if len(chain) > cap:
        chain = chain[-cap:]
    return chain


def verify_audit_chain(chain: Any) -> dict[str, Any]:
    if not isinstance(chain, list):
        return {"ok": False, "reasons": ["chain_not_list"], "length": 0}
    reasons: list[str] = []
    prev = ""
    for i, ev in enumerate(chain):
        v = validate_audit_chain_event(ev)
        if not v["ok"]:
            reasons.append(f"event_{i}_invalid:" + ",".join(v["reasons"]))
            continue
        if ev.get("previous_hash") != prev:
            reasons.append(f"event_{i}_broken_chain")
        prev = ev.get("event_hash") or ""
    return {
        "ok": not reasons,
        "reasons": reasons,
        "length": len(chain),
        "phase": _PHASE,
    }


def summarize_audit_chain(chain: Any) -> dict[str, Any]:
    if not isinstance(chain, list):
        chain = []
    by_type: dict[str, int] = {}
    by_status: dict[str, int] = {}
    first_ts = None
    last_ts = None
    for e in chain:
        if not isinstance(e, dict):
            continue
        by_type[str(e.get("event_type") or "")] = \
            by_type.get(str(e.get("event_type") or ""), 0) + 1
        by_status[str(e.get("status") or "")] = \
            by_status.get(str(e.get("status") or ""), 0) + 1
        ts = e.get("created_at")
        if isinstance(ts, (int, float)):
            first_ts = ts if first_ts is None else min(first_ts, ts)
            last_ts = ts if last_ts is None else max(last_ts, ts)
    verified = verify_audit_chain(chain)
    return {
        "length": len(chain),
        "by_event_type": by_type,
        "by_status": by_status,
        "first_at": first_ts,
        "last_at": last_ts,
        "verified_ok": verified["ok"],
        "verification_reasons": verified.get("reasons", []),
        "cap": _HARD_CAP,
        "phase": _PHASE,
    }


def write_audit_chain(
    chain: list[dict[str, Any]],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "phase": _PHASE,
        "written_at": time.time(),
        "summary": summarize_audit_chain(chain),
        "chain": list(chain or [])[-_HARD_CAP:],
    }
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_audit_chain(
    path: str,
    limit: int = _DEFAULT_READ_LIMIT,
) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    cap = max(1, min(int(limit or 1), _HARD_CAP))
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    chain = body.get("chain") if isinstance(body, dict) else None
    if not isinstance(chain, list):
        return []
    return chain[:cap]


def write_audit_chain_report(
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
    "create_audit_chain_event",
    "compute_event_hash",
    "validate_audit_chain_event",
    "append_chain_event",
    "verify_audit_chain",
    "summarize_audit_chain",
    "write_audit_chain",
    "read_audit_chain",
    "write_audit_chain_report",
]
