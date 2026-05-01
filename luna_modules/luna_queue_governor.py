"""
luna_queue_governor.py — Queue Governor

Prevents mass-job chaos by enforcing per-cycle budgets, classifying
prompt families, and quarantining (never deleting) overrun items.

All functions: stdlib only, no print(), UTF-8 safe logging.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_POLICY = {
    "max_active_aider_jobs": 1,
    "max_jobs_per_cycle": 12,
    "max_failed_per_cycle": 5,
    "max_noop_per_cycle": 5,
    "max_same_file_jobs_per_cycle": 3,
    "max_same_prompt_family_per_cycle": 20,
    "done_requires_diff": True,
    "quarantine_instead_of_delete": True,
}

PROMPT_FAMILY_KEYWORDS = {
    "refactor":   ["refactor", "clean", "cleanup", "reorganize", "simplify"],
    "bugfix":     ["fix", "bug", "error", "crash", "exception", "fail"],
    "feature":    ["add", "implement", "create", "build", "new"],
    "test":       ["test", "unittest", "pytest", "spec"],
    "docs":       ["doc", "docstring", "comment", "readme"],
    "analysis":   ["analyze", "analyse", "inspect", "review", "audit", "check"],
    "dependency": ["install", "pip", "package", "dep", "require"],
    "config":     ["config", "setting", "env", "environment", "configure"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _live_feed(project_dir: Path, event: str, msg: str, details: Optional[dict] = None) -> None:
    log_path = project_dir / "logs" / "luna_live_feed.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": _now_iso(),
        "role": "operating_layer",
        "source": "luna_queue_governor",
        "event": event,
        "message": msg,
        "details": details or {},
    }
    try:
        with open(log_path, "a", encoding="utf-8", errors="replace") as f:
            f.write(json.dumps(entry, ensure_ascii=True) + "\n")
    except Exception:
        pass


def _load_json(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8", errors="replace") as f:
        json.dump(data, f, indent=2, ensure_ascii=True)
    tmp.replace(path)


def _write_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", errors="replace") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_queue_policy(project_dir: Path) -> dict:
    """Return the current queue policy (creates default if missing)."""
    policy_path = project_dir / "memory" / "queue_governor_policy.json"
    if policy_path.exists():
        stored = _load_json(policy_path)
        if stored:
            return stored
    # Write default
    _write_json(policy_path, DEFAULT_POLICY)
    return dict(DEFAULT_POLICY)


def inspect_queues(project_dir: Path) -> dict:
    """
    Scan known queue directories and return counts.
    Returns: {queues: {name: count}, total_pending: int, status: "ok"}
    """
    queue_dirs = {
        "tasks/active":    project_dir / "tasks" / "active",
        "tasks/queued":    project_dir / "tasks" / "queued",
        "tasks/failed":    project_dir / "tasks" / "failed",
        "aider_jobs":      project_dir / "aider_jobs",
        "director_jobs":   project_dir / "director_jobs",
        "solutions":       project_dir / "solutions",
        "logic_updates":   project_dir / "logic_updates",
        "uploads":         project_dir / "uploads",
    }
    counts = {}
    for name, qpath in queue_dirs.items():
        if qpath.exists():
            try:
                counts[name] = len(list(qpath.iterdir()))
            except Exception:
                counts[name] = -1
        else:
            counts[name] = 0

    total_pending = counts.get("tasks/active", 0) + counts.get("tasks/queued", 0)
    return {
        "status": "ok",
        "queues": counts,
        "total_pending": total_pending,
        "ts": _now_iso(),
    }


def classify_prompt_family(prompt: str) -> str:
    """Classify a prompt string into a known family."""
    lower = prompt.lower()
    for family, keywords in PROMPT_FAMILY_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return family
    return "general"


def can_start_job(project_dir: Path, job: dict) -> dict:
    """
    Check whether a new job may start given current cycle state.

    job keys:
      - prompt (str)
      - target_file (str, optional)
      - analysis_only (bool, optional)

    Returns: {allowed: bool, reason: str, policy: dict, state: dict}
    """
    policy = load_queue_policy(project_dir)
    state = _load_cycle_state(project_dir)

    prompt = job.get("prompt", "")
    target_file = job.get("target_file", "")
    family = classify_prompt_family(prompt)

    # Check active aider jobs
    active_aider = state.get("active_aider_jobs", 0)
    if active_aider >= policy["max_active_aider_jobs"]:
        reason = f"active_aider_jobs={active_aider} >= limit={policy['max_active_aider_jobs']}"
        _live_feed(project_dir, "QUEUE_GOVERNOR_PAUSE", reason, {"job": job})
        return {"allowed": False, "reason": reason, "policy": policy, "state": state}

    # Check total jobs this cycle
    total_jobs = state.get("jobs_this_cycle", 0)
    if total_jobs >= policy["max_jobs_per_cycle"]:
        reason = f"jobs_this_cycle={total_jobs} >= limit={policy['max_jobs_per_cycle']}"
        _live_feed(project_dir, "QUEUE_GOVERNOR_PAUSE", reason, {"job": job})
        return {"allowed": False, "reason": reason, "policy": policy, "state": state}

    # Check failed count
    failed_count = state.get("failed_this_cycle", 0)
    if failed_count >= policy["max_failed_per_cycle"]:
        reason = f"failed_this_cycle={failed_count} >= limit={policy['max_failed_per_cycle']}"
        _live_feed(project_dir, "QUEUE_GOVERNOR_PAUSE", reason, {"job": job})
        return {"allowed": False, "reason": reason, "policy": policy, "state": state}

    # Check noop count
    noop_count = state.get("noop_this_cycle", 0)
    if noop_count >= policy["max_noop_per_cycle"]:
        reason = f"noop_this_cycle={noop_count} >= limit={policy['max_noop_per_cycle']}"
        _live_feed(project_dir, "QUEUE_GOVERNOR_PAUSE", reason, {"job": job})
        return {"allowed": False, "reason": reason, "policy": policy, "state": state}

    # Check same-file saturation
    if target_file:
        file_jobs = state.get("file_job_counts", {}).get(target_file, 0)
        if file_jobs >= policy["max_same_file_jobs_per_cycle"]:
            reason = f"file '{target_file}' already has {file_jobs} jobs this cycle >= limit={policy['max_same_file_jobs_per_cycle']}"
            _live_feed(project_dir, "QUEUE_GOVERNOR_PAUSE", reason, {"job": job})
            return {"allowed": False, "reason": reason, "policy": policy, "state": state}

    # Check same-family saturation
    family_jobs = state.get("family_job_counts", {}).get(family, 0)
    if family_jobs >= policy["max_same_prompt_family_per_cycle"]:
        reason = f"prompt family '{family}' already has {family_jobs} jobs >= limit={policy['max_same_prompt_family_per_cycle']}"
        _live_feed(project_dir, "QUEUE_GOVERNOR_PAUSE", reason, {"job": job})
        return {"allowed": False, "reason": reason, "policy": policy, "state": state}

    _live_feed(project_dir, "QUEUE_GOVERNOR_ALLOW", f"Job allowed (family={family})", {"job": job})
    return {"allowed": True, "reason": "ok", "policy": policy, "state": state, "family": family}


def record_job_outcome(project_dir: Path, outcome: dict) -> dict:
    """
    Record a completed job outcome into the cycle state.

    outcome keys:
      - status: "done" | "failed" | "noop" | "quarantined"
      - target_file (str, optional)
      - prompt (str, optional)
      - had_diff (bool, optional)
      - analysis_only (bool, optional)
    """
    state = _load_cycle_state(project_dir)
    status = outcome.get("status", "done")
    target_file = outcome.get("target_file", "")
    prompt = outcome.get("prompt", "")
    family = classify_prompt_family(prompt)

    state.setdefault("jobs_this_cycle", 0)
    state.setdefault("failed_this_cycle", 0)
    state.setdefault("noop_this_cycle", 0)
    state.setdefault("active_aider_jobs", 0)
    state.setdefault("file_job_counts", {})
    state.setdefault("family_job_counts", {})

    state["jobs_this_cycle"] += 1

    if status == "failed":
        state["failed_this_cycle"] += 1
    elif status == "noop":
        state["noop_this_cycle"] += 1

    # Decrement active if it was active
    if state["active_aider_jobs"] > 0:
        state["active_aider_jobs"] -= 1

    if target_file:
        state["file_job_counts"][target_file] = state["file_job_counts"].get(target_file, 0) + 1

    state["family_job_counts"][family] = state["family_job_counts"].get(family, 0) + 1
    state["last_updated"] = _now_iso()

    _save_cycle_state(project_dir, state)

    ledger_path = project_dir / "memory" / "cycle_summaries.jsonl"
    _write_jsonl(ledger_path, {**outcome, "family": family, "ts": _now_iso()})

    return {"status": "recorded", "state": state}


def should_pause_cycle(project_dir: Path) -> dict:
    """Return whether the current cycle should pause (budgets exhausted)."""
    policy = load_queue_policy(project_dir)
    state = _load_cycle_state(project_dir)

    reasons = []
    if state.get("jobs_this_cycle", 0) >= policy["max_jobs_per_cycle"]:
        reasons.append("max_jobs_per_cycle exhausted")
    if state.get("failed_this_cycle", 0) >= policy["max_failed_per_cycle"]:
        reasons.append("max_failed_per_cycle exhausted")
    if state.get("noop_this_cycle", 0) >= policy["max_noop_per_cycle"]:
        reasons.append("max_noop_per_cycle exhausted")

    should_pause = len(reasons) > 0
    if should_pause:
        _live_feed(project_dir, "QUEUE_GOVERNOR_PAUSE", "Cycle pause triggered", {"reasons": reasons})
    return {"should_pause": should_pause, "reasons": reasons, "state": state}


def write_cycle_summary(project_dir: Path, summary: dict) -> dict:
    """Append a manual cycle summary to cycle_summaries.jsonl."""
    path = project_dir / "memory" / "cycle_summaries.jsonl"
    record = {**summary, "ts": _now_iso(), "source": "write_cycle_summary"}
    _write_jsonl(path, record)
    return {"status": "written", "path": str(path)}


def reset_cycle_state(project_dir: Path) -> dict:
    """Reset cycle counters for a new cycle (called at cycle start)."""
    fresh = {
        "cycle_start": _now_iso(),
        "jobs_this_cycle": 0,
        "failed_this_cycle": 0,
        "noop_this_cycle": 0,
        "active_aider_jobs": 0,
        "file_job_counts": {},
        "family_job_counts": {},
        "last_updated": _now_iso(),
    }
    _save_cycle_state(project_dir, fresh)
    return {"status": "reset", "state": fresh}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_cycle_state(project_dir: Path) -> dict:
    path = project_dir / "memory" / "queue_governor_state.json"
    data = _load_json(path)
    if not data:
        return {
            "cycle_start": _now_iso(),
            "jobs_this_cycle": 0,
            "failed_this_cycle": 0,
            "noop_this_cycle": 0,
            "active_aider_jobs": 0,
            "file_job_counts": {},
            "family_job_counts": {},
            "last_updated": _now_iso(),
        }
    return data


def _save_cycle_state(project_dir: Path, state: dict) -> None:
    path = project_dir / "memory" / "queue_governor_state.json"
    _write_json(path, state)
