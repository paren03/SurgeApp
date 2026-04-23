"""Approval queue helpers.

Extracted from ``worker.py`` (step 11 of modularity refactor).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Tuple

from luna_modules.luna_io import safe_read_json, write_json_atomic
from luna_modules.luna_logging import now_iso
from luna_modules.luna_paths import LUNA_APPROVAL_QUEUE_PATH
from luna_modules.luna_routing import normalize_prompt_text

# ``speak`` lives in ``worker.py`` (it depends on shared heartbeat state).
# We resolve it lazily via a module-level callable that ``worker.py`` sets
# immediately after importing this module, so the rest of the call path
# remains synchronous and the behaviour unchanged.
_speak = None


def set_speak_callback(func) -> None:
    global _speak
    _speak = func


def _call_speak(message: str, mood: str = "awake") -> None:
    if _speak is not None:
        try:
            _speak(message, mood=mood)
        except Exception:
            pass


def task_requires_approval(task: Dict[str, Any]) -> Tuple[bool, str]:
    prompt = normalize_prompt_text(task.get("prompt", ""))
    target = str(task.get("target_file") or "")
    lower_target = target.lower()
    if any(marker in lower_target for marker in [r"c:\windows", "system32", "appdata\\local\\microsoft\\windows", "program files"]):
        return True, "Target path is outside Luna core or appears system-sensitive."
    if any(term in prompt for term in ["registry", "regedit", "delete system", "format drive", "powershell rm -recurse", "taskkill /f /im explorer.exe"]):
        return True, "Requested action is high-risk and needs confirmation."
    if task.get("requires_approval") is True:
        return True, "Task explicitly requires approval."
    return False, ""


def count_pending_approvals() -> int:
    queue = safe_read_json(LUNA_APPROVAL_QUEUE_PATH, default={"pending": []})
    pending = queue.get("pending", [])
    return len([item for item in pending if item.get("status") == "PENDING_APPROVAL"])


def enqueue_approval(task: Dict[str, Any], reason: str) -> str:
    queue = safe_read_json(LUNA_APPROVAL_QUEUE_PATH, default={"pending": [], "history": []})
    approval_id = task.get("approval_id") or f"approval_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    entry = {
        "approval_id": approval_id,
        "task_id": task.get("id", ""),
        "prompt": task.get("prompt", ""),
        "target_file": task.get("target_file", ""),
        "reason": reason,
        "status": "PENDING_APPROVAL",
        "created_at": now_iso(),
    }
    queue.setdefault("pending", [])
    queue["pending"] = [item for item in queue["pending"] if item.get("approval_id") != approval_id]
    queue["pending"].append(entry)
    write_json_atomic(LUNA_APPROVAL_QUEUE_PATH, queue)
    _call_speak(f"I found a higher-risk action and I’m waiting for your yes or no. Approval ID: {approval_id}", mood="cautious")
    return approval_id


def process_approval_response(task: Dict[str, Any]) -> str:
    approval_id = str(task.get("approval_id") or "").strip()
    decision = normalize_prompt_text(str(task.get("decision") or task.get("prompt") or ""))
    approved = decision in {"y", "yes", "approve", "approved"}
    denied = decision in {"n", "no", "deny", "denied"}
    if not approval_id:
        return "[LUNA APPROVAL RESPONSE]\nstatus : FAILED\nreason : missing approval_id\n"
    queue = safe_read_json(LUNA_APPROVAL_QUEUE_PATH, default={"pending": [], "history": []})
    pending = queue.get("pending", [])
    match = None
    remaining = []
    for item in pending:
        if item.get("approval_id") == approval_id and match is None:
            match = item
        else:
            remaining.append(item)
    if not match:
        return f"[LUNA APPROVAL RESPONSE]\nstatus : FAILED\nreason : approval_id not found: {approval_id}\n"
    match["status"] = "APPROVED" if approved else ("DENIED" if denied else "INVALID")
    match["resolved_at"] = now_iso()
    queue["pending"] = remaining
    queue.setdefault("history", []).append(match)
    write_json_atomic(LUNA_APPROVAL_QUEUE_PATH, queue)
    if approved:
        _call_speak(f"Thanks. I recorded approval {approval_id} and I’m ready for the next supervised action.", mood="grateful")
        return f"[LUNA APPROVAL RESPONSE]\nstatus : APPROVED\napproval_id : {approval_id}\n"
    if denied:
        _call_speak(f"Understood. I cancelled approval {approval_id} and kept everything unchanged.", mood="steady")
        return f"[LUNA APPROVAL RESPONSE]\nstatus : DENIED\napproval_id : {approval_id}\n"
    return f"[LUNA APPROVAL RESPONSE]\nstatus : INVALID\napproval_id : {approval_id}\nreason : reply must be yes or no\n"
