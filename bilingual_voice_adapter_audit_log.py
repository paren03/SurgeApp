"""Phase 28 - Voice Adapter Audit Log.

Bounded, in-memory by default. File writes only when explicitly called.
No transcript persistence by default.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_SUPPORTED_EVENT_TYPES = (
    "preflight",
    "payload_validation",
    "consent_request",
    "consent_decision",
    "boundary_guard",
    "adapter_selection",
    "compatibility_check",
    "render_envelope_created",
    "refusal",
    "error",
    "dry_run_complete",
)

_SUPPORTED_STATUS = (
    "ok", "warn", "error", "refused", "blocked", "skipped", "info",
)

_HARD_CAP = 500
_DEFAULT_READ_LIMIT = 500


def _new_event_id() -> str:
    return f"audit_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def create_audit_event(
    event_type: str,
    status: str,
    message: str = "",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    et = str(event_type or "").lower()
    if et not in _SUPPORTED_EVENT_TYPES:
        et = "info"
    st = str(status or "").lower()
    if st not in _SUPPORTED_STATUS:
        st = "info"
    md = dict(metadata or {})
    # Defensive: strip raw transcript-style keys by default
    for k in ("transcript", "full_transcript", "user_text_raw",
              "assistant_text_raw"):
        md.pop(k, None)
    return {
        "event_id": _new_event_id(),
        "event_type": et,
        "status": st,
        "message": str(message or "")[:512],
        "metadata": md,
        "created_at": time.time(),
        "phase": "phase28",
    }


def validate_audit_event(event: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(event, dict):
        return {"ok": False, "reasons": ["event_not_dict"]}
    for f in ("event_id", "event_type", "status", "message",
              "metadata", "created_at", "phase"):
        if f not in event:
            reasons.append(f"missing_field:{f}")
    if event.get("event_type") not in _SUPPORTED_EVENT_TYPES:
        reasons.append("unsupported_event_type")
    if event.get("status") not in _SUPPORTED_STATUS:
        reasons.append("unsupported_status")
    md = event.get("metadata") or {}
    if not isinstance(md, dict):
        reasons.append("metadata_not_dict")
    else:
        for k in ("transcript", "full_transcript", "user_text_raw",
                  "assistant_text_raw"):
            if k in md:
                reasons.append(f"forbidden_metadata_key:{k}")
    return {"ok": not reasons, "reasons": reasons}


def append_audit_event(
    events: list[dict[str, Any]],
    event: dict[str, Any],
    limit: int = _HARD_CAP,
) -> list[dict[str, Any]]:
    if not isinstance(events, list):
        events = []
    cap = max(1, min(int(limit or 1), _HARD_CAP))
    if validate_audit_event(event)["ok"]:
        events.append(event)
    if len(events) > cap:
        events = events[-cap:]
    return events


def summarize_audit_events(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(events, list):
        events = []
    by_type: dict[str, int] = {}
    by_status: dict[str, int] = {}
    first_ts = None
    last_ts = None
    for e in events:
        if not isinstance(e, dict):
            continue
        t = str(e.get("event_type") or "")
        s = str(e.get("status") or "")
        by_type[t] = by_type.get(t, 0) + 1
        by_status[s] = by_status.get(s, 0) + 1
        ts = e.get("created_at")
        if isinstance(ts, (int, float)):
            first_ts = ts if first_ts is None else min(first_ts, ts)
            last_ts = ts if last_ts is None else max(last_ts, ts)
    return {
        "count": len(events),
        "by_event_type": by_type,
        "by_status": by_status,
        "first_at": first_ts,
        "last_at": last_ts,
        "phase": "phase28",
        "cap": _HARD_CAP,
    }


def write_audit_log(
    events: list[dict[str, Any]],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "phase": "phase28",
        "written_at": time.time(),
        "summary": summarize_audit_events(events),
        "events": list(events or [])[-_HARD_CAP:],
    }
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_audit_log(
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
    events = body.get("events") if isinstance(body, dict) else None
    if not isinstance(events, list):
        return []
    return events[:cap]


def write_adapter_audit_report(
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
    "create_audit_event",
    "validate_audit_event",
    "append_audit_event",
    "summarize_audit_events",
    "write_audit_log",
    "read_audit_log",
    "write_adapter_audit_report",
]
