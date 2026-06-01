"""Phase 29 - Dry-Run Queue (no worker, no daemon, no background).

Plain Python list + dict queue object. dequeue is dry-run by default
and must never execute anything. Status changes are metadata only.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase29.dry_run_queue.v1"

_HARD_CAP = 100


def _new_id() -> str:
    return f"q_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def create_dry_run_queue() -> dict[str, Any]:
    return {
        "queue_id": _new_id(),
        "created_at": time.time(),
        "items": [],
        "status_index": {},
        "cap": _HARD_CAP,
        "phase": _PHASE,
        "notes": ("phase29 dry-run queue; no worker; no daemon; "
                  "dequeue never executes"),
    }


def enqueue_dry_run_packet(
    queue: dict[str, Any],
    packet: dict[str, Any],
    limit: int = _HARD_CAP,
) -> dict[str, Any]:
    if not isinstance(queue, dict) or not isinstance(queue.get("items"),
                                                     list):
        queue = create_dry_run_queue()
    if not isinstance(packet, dict):
        return queue
    if not packet.get("packet_id"):
        # Cannot enqueue items without an id
        return queue
    cap = max(1, min(int(limit or 1), _HARD_CAP))
    queue["items"].append({
        "packet_id": packet["packet_id"],
        "enqueued_at": time.time(),
        "status": "queued_dry_run",
        "summary": {
            "adapter_name": packet.get("adapter_name"),
            "language_mode": packet.get("language_mode"),
            "execution_blocked": True,
            "dry_run": True,
        },
    })
    queue["status_index"][packet["packet_id"]] = "queued_dry_run"
    if len(queue["items"]) > cap:
        # Evict from the front
        removed = queue["items"][:-cap]
        queue["items"] = queue["items"][-cap:]
        for r in removed:
            queue["status_index"].pop(r.get("packet_id"), None)
    return queue


def list_dry_run_queue(
    queue: Any,
    limit: int = _HARD_CAP,
) -> list[dict[str, Any]]:
    if not isinstance(queue, dict):
        return []
    items = queue.get("items") or []
    cap = max(1, min(int(limit or 1), _HARD_CAP))
    return list(items)[:cap]


def dequeue_dry_run_packet(
    queue: dict[str, Any],
    dry_run: bool = True,
) -> Optional[dict[str, Any]]:
    """Returns the front packet metadata. Even with dry_run=False, this
    function never executes anything — Phase 29 has no runtime path."""
    if not isinstance(queue, dict):
        return None
    items = queue.get("items") or []
    if not items:
        return None
    head = items.pop(0)
    queue["items"] = items
    pid = head.get("packet_id")
    if pid in queue.get("status_index", {}):
        queue["status_index"][pid] = "dequeued_dry_run"
    head["status"] = "dequeued_dry_run"
    head["dequeued_at"] = time.time()
    head["dry_run"] = True
    head["execution_blocked"] = True
    return head


def mark_packet_status(
    queue: dict[str, Any],
    packet_id: str,
    status: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    if not isinstance(queue, dict):
        return {"ok": False, "reasons": ["queue_not_dict"]}
    found = False
    for item in queue.get("items") or []:
        if item.get("packet_id") == packet_id:
            item["status"] = str(status or "unknown")
            item["status_at"] = time.time()
            found = True
            break
    if found:
        queue.setdefault("status_index", {})[packet_id] = str(
            status or "unknown")
    return {
        "ok": found,
        "reasons": ([] if found else ["packet_id_not_found"]),
        "phase": _PHASE,
    }


def summarize_dry_run_queue(queue: Any) -> dict[str, Any]:
    if not isinstance(queue, dict):
        return {"length": 0, "phase": _PHASE}
    items = queue.get("items") or []
    by_status: dict[str, int] = {}
    for it in items:
        s = str(it.get("status") or "")
        by_status[s] = by_status.get(s, 0) + 1
    return {
        "queue_id": queue.get("queue_id"),
        "length": len(items),
        "by_status": by_status,
        "cap": _HARD_CAP,
        "phase": _PHASE,
    }


def write_dry_run_queue_report(
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
    "create_dry_run_queue",
    "enqueue_dry_run_packet",
    "list_dry_run_queue",
    "dequeue_dry_run_packet",
    "mark_packet_status",
    "summarize_dry_run_queue",
    "write_dry_run_queue_report",
]
