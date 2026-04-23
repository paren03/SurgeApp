"""Task lifecycle primitives for the Luna worker.

Extracted from ``worker.py`` (step 7 of modularity refactor).

Functions with forward dependencies on not-yet-extracted domain runners
(``_run_standard_mode_action``, ``_handle_standard_task_mode``,
``process_task``, ``_finish_quit_request``) remain in ``worker.py``
until those runners are modularised in later steps.
"""

from __future__ import annotations

import os
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from luna_modules.luna_approvals import count_pending_approvals, enqueue_approval
from luna_modules.luna_heartbeat import HEARTBEAT_STATE, set_heartbeat
from luna_modules.luna_io import (
    safe_read_json,
    safe_write_text,
    write_json_atomic,
)
from luna_modules.luna_logging import _diag, ensure_layout, log, now_iso
from luna_modules.luna_paths import (
    ACTIVE_DIR,
    BACKUPS_DIR,
    DONE_DIR,
    FAILED_DIR,
    LUNA_PENDING_APPROVAL,
    LUNA_TASK_MEMORY_PATH,
    LUNA_SESSION_MEMORY_PATH,
    PROJECT_DIR,
    SOLUTIONS_DIR,
)
from luna_modules.luna_routing import normalize_task_type, resolve_worker_mode
from luna_modules.luna_verification import attach_verification, verification_ok
from luna_modules.luna_paths import (
    LUNA_EXECUTION_FAILURE,
    LUNA_IMPROVEMENT_FAILURE,
)


# ── Memory helpers ────────────────────────────────────────────────────────────

def append_task_memory(task_name: str, result: str, success: bool, category: str = "task") -> None:
    data = safe_read_json(LUNA_TASK_MEMORY_PATH, default={})
    key = "completed" if success else "failures"
    data.setdefault(key, []).append({
        "task": task_name,
        "result": result[:2000],
        "timestamp": now_iso(),
        "category": category,
    })
    data["last_updated"] = now_iso()
    write_json_atomic(LUNA_TASK_MEMORY_PATH, data)


def update_session_summary(task_name: str, mood: str = "awake") -> None:
    data = safe_read_json(LUNA_SESSION_MEMORY_PATH, default={})
    data["session_date"] = datetime.now().strftime("%Y-%m-%d")
    data["working_summary"] = f"Last worker task completed: {task_name}"
    data["active_focus"] = [
        "Always-on Luna core",
        "Verification harness",
        "Approval-gated self-healing",
        "Mission supervision",
    ]
    data["luna_state"] = mood
    data["last_worker_message"] = HEARTBEAT_STATE.get("last_message", "")
    write_json_atomic(LUNA_SESSION_MEMORY_PATH, data)


# ── Task identity and path helpers ────────────────────────────────────────────

def _task_identity(task_path: Path, task: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    task = task if isinstance(task, dict) else safe_read_json(task_path, default={})
    task_id = task.get("id", task_path.stem.replace(".working", ""))
    target_file = task.get("target_file") or str(PROJECT_DIR / "worker.py")
    user_input = (task.get("prompt") or "").strip()
    return {
        "task": task,
        "task_id": task_id,
        "target_file": target_file,
        "user_input": user_input,
        "solution_path": SOLUTIONS_DIR / f"{task_id}.txt",
    }


def build_final_task_name(task_path: Path) -> str:
    return task_path.name.replace(".working.json", ".json")


def build_solution_header(mode_label: str, task_id: str, target_file: str) -> str:
    return "# LUNA QUALITY REPORT\n" + f"# mode={mode_label}  task={task_id}  target={target_file}\n\n"


def build_runtime_exception_report(task_id: str, target_file: str, exc: Exception) -> str:
    return (
        "[LUNA EXECUTION FAILURE]\n"
        f"task_id : {task_id}\n"
        f"target  : {target_file}\n"
        f"error   : {type(exc).__name__}\n"
        f"reason  : {exc}\n"
        f"trace   :\n{traceback.format_exc()}\n"
    )


def run_mode_safely(task_id: str, target_file: str, fn):
    try:
        return fn(), True
    except Exception as exc:
        return build_runtime_exception_report(task_id, target_file, exc), False


def build_backup_path(target_path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    unique = str(time.time_ns())[-6:]
    return BACKUPS_DIR / f"{target_path.stem}_{stamp}_{os.getpid()}_{unique}{target_path.suffix}.bak"


# ── Runtime state helpers ─────────────────────────────────────────────────────

def update_task_runtime(
    task_path: Path,
    state: str,
    phase: str = "",
    progress: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    payload = safe_read_json(task_path, default={})
    if not payload:
        return
    payload["status"] = state
    payload["state"] = state
    if phase:
        payload["phase"] = phase
    if progress is not None:
        payload["progress"] = max(0, min(100, int(progress)))
    payload["updated_at"] = now_iso()
    payload["worker_pid"] = os.getpid()
    if extra:
        payload.update(extra)
    write_json_atomic(task_path, payload)
    set_heartbeat(state=state, task_id=str(payload.get("id", "")), phase=phase or str(payload.get("phase", "")))


# ── Task finish helpers ───────────────────────────────────────────────────────

def _finish_task(task_path: Path, solution_path: Path, header: str, body: str, success: bool) -> None:
    safe_write_text(solution_path, header + body)
    update_task_runtime(task_path, "done" if success else "failed", "complete" if success else "failed", 100, {"finished_at": now_iso()})
    dest = DONE_DIR / build_final_task_name(task_path) if success else FAILED_DIR / build_final_task_name(task_path)
    try:
        os.replace(str(task_path), str(dest))
    except Exception as exc:
        log(f"[LUNA] file move issue: {exc}")


def _finish_kill_switch_block(task_path: Path) -> bool:
    report = "[LUNA EXECUTION BLOCKED]\nKill switch is active. Clear LUNA_STOP_NOW.flag to resume.\n"
    ctx = _task_identity(task_path)
    _finish_task(
        task_path,
        ctx["solution_path"],
        build_solution_header("blocked", ctx["task_id"], ctx["target_file"]),
        report,
        False,
    )
    return True


def _resolve_task_mode(task: Dict[str, Any]) -> Tuple[str, str, str]:
    if normalize_task_type(task.get("task_type", "")) == "approval_response":
        return "approval-response", "approval_response", "approval_response"
    return resolve_worker_mode(task)


def _finish_empty_prompt(task_path: Path, ctx: Dict[str, Any]) -> bool:
    body = build_runtime_exception_report(
        ctx["task_id"],
        ctx["target_file"],
        ValueError("empty or missing prompt in task payload"),
    )
    _finish_task(task_path, ctx["solution_path"], build_solution_header("failed", ctx["task_id"], ctx["target_file"]), body, False)
    log(f"[LUNA] task failed: empty prompt {task_path.name}")
    return True


def _finish_pending_approval(task_path: Path, ctx: Dict[str, Any], approval_reason: str) -> bool:
    approval_id = enqueue_approval(ctx["task"], approval_reason)
    pending_report = (
        f"{LUNA_PENDING_APPROVAL}\n"
        f"task_id     : {ctx['task_id']}\n"
        f"approval_id : {approval_id}\n"
        f"target      : {ctx['target_file']}\n"
        f"reason      : {approval_reason}\n"
        "reply       : use /approve <approval_id> yes or /approve <approval_id> no in the terminal.\n"
    )
    _finish_task(
        task_path,
        ctx["solution_path"],
        build_solution_header("pending-approval", ctx["task_id"], ctx["target_file"]),
        pending_report,
        False,
    )
    return True


def _finish_invalid_target(task_path: Path, ctx: Dict[str, Any], target_reason: str) -> bool:
    invalid_report = (
        "[LUNA EXECUTION BLOCKED]\n"
        "Unsafe task target.\n"
        f"task_id : {ctx['task_id']}\n"
        f"prompt  : {ctx['user_input']!r}\n"
        f"target  : {ctx['target_file']}\n"
        f"reason  : {target_reason}\n"
    )
    _finish_task(
        task_path,
        ctx["solution_path"],
        build_solution_header("blocked", ctx["task_id"], ctx["target_file"]),
        invalid_report,
        False,
    )
    return True


def _complete_task_mode(
    task_path: Path,
    ctx: Dict[str, Any],
    mode_label: str,
    report: str,
    success: bool,
    category: str,
    mood: str,
    verification: Optional[Dict[str, Any]] = None,
) -> bool:
    body = attach_verification(report, verification) if verification is not None else report
    _finish_task(
        task_path,
        ctx["solution_path"],
        build_solution_header(mode_label, ctx["task_id"], ctx["target_file"]),
        body,
        success,
    )
    append_task_memory(ctx["user_input"], report, success, category=category)
    update_session_summary(ctx["user_input"], mood)
    return True


def _evaluate_standard_mode_success(mode_label: str, report: str, runtime_ok: bool, verification: Optional[Dict[str, Any]]) -> bool:
    if mode_label in {"chat", "chat-response"}:
        return runtime_ok and bool(str(report or "").strip())
    if mode_label == "approval-response":
        return "FAILED" not in report
    if mode_label in {"system-action", "mcp-adoption", "upgrade-proposal"}:
        return runtime_ok and "FAILED" not in report
    if mode_label == "self-fix":
        return runtime_ok and verification_ok(verification or {}) and "[LUNA SELF-FIX FAILURE]" not in report
    if mode_label == "guided-loop":
        return runtime_ok and verification_ok(verification or {}) and LUNA_EXECUTION_FAILURE not in report
    if mode_label == "improvement":
        return runtime_ok and verification_ok(verification or {}) and LUNA_IMPROVEMENT_FAILURE not in report
    if mode_label in {"mission-kernel", "meta-decision", "self-upgrade"}:
        return runtime_ok and LUNA_EXECUTION_FAILURE not in report and "MISSION BLOCKED" not in report
    if mode_label == "acquisition-request":
        return runtime_ok and LUNA_EXECUTION_FAILURE not in report and "status : FAILED" not in report
    return False


def _finish_blocked_mode(task_path: Path, ctx: Dict[str, Any]) -> bool:
    block_report = (
        "[LUNA EXECUTION BLOCKED]\n"
        "Unsupported command.\n"
        f"task_id : {ctx['task_id']}\n"
        f"prompt  : {ctx['user_input']!r}\n"
        "Allowed modes: chat, chat-response, self-fix, guided-loop, improvement, mission-kernel, system-action, mcp-adoption.\n"
    )
    _finish_task(
        task_path,
        ctx["solution_path"],
        build_solution_header("blocked", ctx["task_id"], ctx["target_file"]),
        block_report,
        False,
    )
    append_task_memory(ctx["user_input"], block_report, False, category="blocked")
    update_session_summary(ctx["user_input"], "steady")
    return True


# ── Task claim and orphan recovery ────────────────────────────────────────────

def claim_task(task_path: Path) -> Optional[Path]:
    if task_path.name.endswith(".working.json"):
        return None
    claimed = task_path.with_name(task_path.stem + ".working.json")
    try:
        os.replace(str(task_path), str(claimed))
        return claimed
    except Exception:
        return None


def recover_orphaned_tasks() -> int:
    recovered = 0
    ensure_layout()
    for working_path in sorted(ACTIVE_DIR.glob("*.working.json")):
        payload = safe_read_json(working_path, default={})
        task_id = str(payload.get("id", working_path.stem.replace(".working", "")))
        payload["status"] = "active"
        payload["state"] = "active"
        payload["phase"] = "recovered"
        payload["progress"] = min(10, int(payload.get("progress", 0) or 0))
        payload["updated_at"] = now_iso()
        recovered_path = working_path.with_name(build_final_task_name(working_path))
        write_json_atomic(working_path, payload)
        try:
            os.replace(str(working_path), str(recovered_path))
            recovered += 1
            log(f"[LUNA] recovered orphaned task: {task_id}")
        except Exception as exc:
            _diag(f"recover_orphaned_tasks failed for {working_path}: {exc}")
    return recovered
