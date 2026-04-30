"""Read-only Inspector snapshot for Luna autonomy work."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace") or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _read_jsonl_tail(path: Path, limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]:
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
            except Exception:
                rows.append({"event": "unparseable_log_line", "raw": line[:300]})
    except Exception:
        pass
    return rows


def _json_files(folder: Path, limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not folder.exists():
        return rows
    for path in sorted(folder.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
        payload = _read_json(path)
        payload.setdefault("path", str(path))
        rows.append(payload)
    return rows


def _diff_files(folder: Path, limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not folder.exists():
        return rows
    for path in sorted(folder.rglob("*.diff"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
        rows.append({
            "path": str(path),
            "bytes": path.stat().st_size,
            "preview": path.read_text(encoding="utf-8", errors="replace")[:1200],
        })
    return rows


def _summary_files(memory_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for name in ("nightly_updates.md", "nightly_updates.jsonl", "luna_autonomy_quality_gate.json"):
        path = memory_dir / name
        if not path.exists():
            continue
        rows.append({
            "path": str(path),
            "bytes": path.stat().st_size,
            "preview": path.read_text(encoding="utf-8", errors="replace")[-1600:],
        })
    return rows


def _continues_update_status(memory_dir: Path) -> Dict[str, Any]:
    """Return the CU status the Inspector needs even when no job is active."""
    state = _read_json(memory_dir / "continues_update_state.json")
    stop_flag = memory_dir / "continues_update.stop"
    running = bool(state.get("running"))
    last_cycle_at = str(state.get("last_cycle_at") or "")
    started_at = str(state.get("started_at") or "")
    return {
        "running": running,
        "display_status": "running_sleeping" if running else "stopped",
        "started_at": started_at,
        "last_cycle_at": last_cycle_at,
        "last_task_id": state.get("last_task_id") or "",
        "last_status": state.get("last_status") or "",
        "cycles": state.get("cycles", 0),
        "consecutive_failures": state.get("consecutive_failures", 0),
        "noop_count": state.get("noop_count", 0),
        "stop_flag_present": stop_flag.exists(),
    }


def build_inspector_autonomy_snapshot(project_dir: str | Path, limit: int = 25) -> Dict[str, Any]:
    """Return plans, jobs, logs, diffs, verification, failures, and summaries."""
    root = Path(project_dir)
    director_dir = root / "director_jobs"
    aider_dir = root / "aider_jobs"
    memory_dir = root / "memory"
    logs_dir = root / "logs"
    jobs = (
        _json_files(aider_dir / "active", limit)
        + _json_files(aider_dir / "done", limit)
        + _json_files(aider_dir / "failed", limit)
    )[:limit]
    failures = [job for job in jobs if str(job.get("status") or job.get("state") or "").lower() in {"failed", "noop", "quarantined"}]
    verification = [
        {
            "task_id": job.get("task_id") or job.get("id") or Path(str(job.get("path", ""))).stem,
            "status": job.get("status"),
            "verification_passed": job.get("verification_passed") or job.get("verify_passed"),
            "diff_exists": job.get("diff_exists"),
            "failure_reason": job.get("failure_reason") or job.get("error"),
            "noop_reason": job.get("noop_reason"),
        }
        for job in jobs
    ]
    cu_status = _continues_update_status(memory_dir)
    return {
        "ts": _now_iso(),
        "continues_update": cu_status,
        "running": {
            "continues_update": cu_status,
            "active_aider_jobs": _json_files(aider_dir / "active", limit),
            "active_director_jobs": _json_files(director_dir / "active", limit),
        },
        "plans": (
            _json_files(director_dir / "active", limit)
            + _json_files(director_dir / "done", limit)
            + _json_files(director_dir / "failed", limit)
            + _json_files(director_dir / "quarantine", limit)
        )[:limit],
        "jobs": jobs,
        "logs": _read_jsonl_tail(logs_dir / "luna_live_feed.jsonl", limit),
        "diffs": _diff_files(root / "logic_updates", limit),
        "verification": verification,
        "failures": failures,
        "summaries": _summary_files(memory_dir),
    }
