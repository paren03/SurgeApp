"""Aider Bridge result discipline for Luna Autonomy Control v1."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _duration(started_at: float | int | None, finished_at: float | int | None) -> float:
    try:
        if started_at is None or finished_at is None:
            return 0.0
        return round(max(0.0, float(finished_at) - float(started_at)), 3)
    except Exception:
        return 0.0


def build_aider_completion_record(
    *,
    task_id: str,
    target_file: str,
    diff_text: str,
    diff_path: str,
    log_path: str,
    verification_passed: bool,
    applied: bool,
    failure_reason: str,
    analysis_only: bool,
    model_used: str,
    started_at: float | int | None,
    finished_at: float | int | None,
    quarantined_reason: str = "",
) -> Dict[str, Any]:
    """Build the required final job record without mutating queues."""
    diff_exists = bool(str(diff_text or "").strip() or str(diff_path or "").strip())
    failure = str(failure_reason or "").strip()
    quarantined = str(quarantined_reason or "").strip()
    status = "done"
    done_reason = "real_diff"
    noop_reason = ""
    live_feed_event = "DONE"
    counts_as_successful_upgrade = True

    if quarantined:
        status = "quarantined"
        done_reason = ""
        live_feed_event = "QUARANTINED"
        counts_as_successful_upgrade = False
        failure = quarantined
    elif failure or not verification_passed:
        status = "failed"
        done_reason = ""
        live_feed_event = "FAILED"
        counts_as_successful_upgrade = False
        if not failure:
            failure = "verification_failed"
    elif not diff_exists and analysis_only:
        status = "done"
        done_reason = "analysis_only"
        counts_as_successful_upgrade = False
    elif not diff_exists:
        status = "noop"
        done_reason = ""
        noop_reason = "no_diff"
        live_feed_event = "NOOP"
        counts_as_successful_upgrade = False

    return {
        "ts": _now_iso(),
        "task_id": str(task_id or ""),
        "status": status,
        "done_reason": done_reason,
        "diff_exists": diff_exists,
        "diff_path": str(diff_path or ""),
        "log_path": str(log_path or ""),
        "target_file": str(target_file or ""),
        "verification_passed": bool(verification_passed),
        "applied": bool(applied),
        "failure_reason": failure,
        "noop_reason": noop_reason,
        "analysis_only": bool(analysis_only),
        "model_used": str(model_used or ""),
        "duration_seconds": _duration(started_at, finished_at),
        "counts_as_successful_upgrade": counts_as_successful_upgrade,
        "live_feed_event": live_feed_event,
    }


def build_aider_report(
    record: Dict[str, Any],
    *,
    prompt: str,
    diff_text: str,
    stdout: str,
    stderr: str,
) -> str:
    """Build the human-readable Aider report with structured metadata."""
    status = str(record.get("status") or "")
    explanation = ""
    if status == "noop" and record.get("noop_reason") == "no_diff":
        explanation = (
            "\n## NOOP Explanation\n"
            "Empty diff is not a successful upgrade. The job produced no code changes "
            "and was not marked analysis_only=true.\n"
        )
    return (
        "# AIDER BRIDGE REPORT\n"
        f"# task_id={record.get('task_id', '')}  target={record.get('target_file', '')}  "
        f"status={status}\n\n"
        "## Result Metadata\n"
        f"```json\n{json.dumps(record, indent=2, ensure_ascii=True)}\n```\n\n"
        f"## Prompt\n{prompt}\n\n"
        f"## Diff\n```diff\n{diff_text or '(no changes detected)'}\n```\n"
        f"{explanation}\n"
        "## Verification\n"
        f"verification_passed={record.get('verification_passed')}  "
        f"applied={record.get('applied')}  "
        f"failure_reason={record.get('failure_reason', '')}  "
        f"noop_reason={record.get('noop_reason', '')}\n\n"
        f"## Aider stdout\n{str(stdout or '')[:4000]}\n\n"
        f"## Aider stderr\n{str(stderr or '')[:2000]}\n"
    )
