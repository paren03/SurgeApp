"""Luna Autonomous Architect - self-improvement engine that never needs human input.

Scans Luna's own source files using Python AST to find SPECIFIC, CONCRETE issues
(exact function name + line number). Generates precise aider instructions that
guarantee a real diff. Submits jobs to the aider bridge, tracks what has been
fixed, and cycles forever.

Runs as a background thread inside worker.py. No user or Claude intervention needed.
"""
from __future__ import annotations

import ast as pyast
import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_PROJECT_DIR = Path(os.environ.get("LUNA_PROJECT_DIR", r"D:\SurgeApp"))
_AIDER_ACTIVE = _PROJECT_DIR / "aider_jobs" / "active"
_AIDER_DONE   = _PROJECT_DIR / "aider_jobs" / "done"
_AIDER_FAILED = _PROJECT_DIR / "aider_jobs" / "failed"
_AIDER_QUARANTINE = _PROJECT_DIR / "aider_jobs" / "quarantine"
_MEMORY_DIR   = _PROJECT_DIR / "memory"
_LOGS_DIR     = _PROJECT_DIR / "logs"
_FEED_PATH    = _LOGS_DIR / "luna_live_feed.jsonl"
_STATE_PATH   = _MEMORY_DIR / "architect_state.json"
_DONE_PATH    = _MEMORY_DIR / "architect_done_issues.json"
_NIGHTLY_PATH = _MEMORY_DIR / "nightly_updates.md"

# Files the architect is allowed to improve (never touches worker.py directly
# - too large; it goes through the normal self-upgrade pipeline instead).
_SCAN_TARGETS: List[str] = [
    "aider_bridge.py",
    "luna_guardian.py",
    "luna_apprentice.py",
    "luna_modules/luna_routing.py",
    "luna_modules/luna_tasks.py",
    "luna_modules/luna_heartbeat.py",
    "luna_modules/luna_io.py",
    "luna_modules/luna_logging.py",
    "luna_modules/luna_approvals.py",
    "luna_modules/luna_verification.py",
    "luna_modules/luna_paths.py",
    "luna_start.pyw",
]

# How long to wait for an aider job to finish (seconds)
_JOB_TIMEOUT = 210.0
# Pause between cycles (seconds) - 8 min gives Ollama time to breathe
_CYCLE_INTERVAL = 480.0
# Max hard failures before the architect pauses for a long rest
_MAX_FAILURES = 4
# Long rest duration after exhausting failure budget (seconds)
_FAILURE_REST = 1800.0
_LOW_VALUE_ISSUE_TYPES = {"no_docstring", "no_comments"}
_HIGH_IMPACT_ISSUE_TYPES = {"open_no_encoding"}


# ---------------------------------------------------------------------------
# Logging / feed helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _feed(event: str, msg: str, detail: str = "") -> None:
    try:
        row: Dict[str, Any] = {
            "ts": datetime.now().strftime("%H:%M:%S"),
            "event": event,
            "icon": "[ARCH]  ",
            "msg": str(msg)[:240],
            "source": "luna_architect",
        }
        if detail:
            row["detail"] = str(detail)[:400]
        _FEED_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _FEED_PATH.open("a", encoding="utf-8", errors="replace") as f:
            json.dump(row, f, ensure_ascii=True)
            f.write("\n")
    except Exception:
        pass


def _log_nightly(entry: Dict[str, Any]) -> None:
    try:
        _NIGHTLY_PATH.parent.mkdir(parents=True, exist_ok=True)
        block = (
            f"## [ARCHITECT] {entry.get('issue_type','?')} in {entry.get('target_file','?')} "
            f"-- {entry.get('ts', _now_iso())}\n"
            f"- function : {entry.get('fn_name','?')} (line {entry.get('lineno','?')})\n"
            f"- status   : {entry.get('status','?')}\n"
            f"- applied  : {entry.get('applied', False)}\n"
            f"- task_id  : {entry.get('task_id','')}\n\n"
        )
        with _NIGHTLY_PATH.open("a", encoding="utf-8", errors="replace") as f:
            f.write(block)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Completed-issue registry (persist across restarts)
# ---------------------------------------------------------------------------

def _load_done() -> set:
    try:
        if _DONE_PATH.exists():
            data = json.loads(_DONE_PATH.read_text(encoding="utf-8", errors="replace") or "[]")
            if isinstance(data, list):
                return set(data)
    except Exception:
        pass
    return set()


def _save_done(done: set) -> None:
    try:
        _DONE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _DONE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(sorted(done), indent=2, ensure_ascii=True), encoding="utf-8")
        tmp.replace(_DONE_PATH)
    except Exception:
        pass


def _issue_key(issue_type: str, rel_path: str, fn_name: str, lineno: int) -> str:
    return f"{issue_type}::{rel_path}::{fn_name}::{lineno}"


# ---------------------------------------------------------------------------
# AST scanner - finds SPECIFIC issues with exact locations
# ---------------------------------------------------------------------------

IssueList = List[Tuple[str, str, str, int]]  # (issue_type, rel_path, fn_name, lineno)


def scan_file(rel_path: str) -> IssueList:
    """Scan one file and return a list of (issue_type, rel_path, fn_name, lineno)."""
    issues: IssueList = []
    fp = _PROJECT_DIR / rel_path
    if not fp.exists():
        return issues
    try:
        src = fp.read_text(encoding="utf-8", errors="replace")
        lines = src.splitlines()
        tree = pyast.parse(src)
    except Exception:
        return issues

    for node in pyast.walk(tree):
        if not isinstance(node, pyast.FunctionDef):
            continue
        fn = node.name
        lineno = node.lineno

        # Deliberately ignore low-value cosmetic issues. The autonomy quality
        # gate forbids endless one-line docstring/comment work from counting as
        # real upgrades.
        _ = fn
        _ = lineno

    # Issue: open() without encoding
    for i, ln in enumerate(lines):
        stripped = ln.strip()
        if (
            "open(" in stripped
            and "encoding" not in stripped
            and not stripped.startswith("#")
            and not stripped.startswith('"""')
        ):
            issues.append(("open_no_encoding", rel_path, f"open_at_line_{i+1}", i + 1))
            lines[i] = lines[i].replace("open(", "open(encoding='utf-8', errors='replace', ", 1)

    return issues


def is_meaningful_issue(issue_type: str) -> bool:
    """Return True when Architect may spend an autonomous Aider job on this issue."""
    return issue_type in _HIGH_IMPACT_ISSUE_TYPES and issue_type not in _LOW_VALUE_ISSUE_TYPES


def scan_all_targets(done: set) -> IssueList:
    """Scan all target files and return issues not yet completed."""
    pending: IssueList = []
    for rel in _SCAN_TARGETS:
        for issue in scan_file(rel):
            if not is_meaningful_issue(issue[0]):
                continue
            key = _issue_key(*issue)
            skip_reason = _recent_architect_skip_reason(issue)
            if skip_reason:
                done.add(key)
                _save_done(done)
                if skip_reason == "no_diff":
                    _feed("ARCH_ALREADY_COMPLIANT", f"Skipped recent no-diff issue: {key}")
                else:
                    _feed("ARCH_DEFERRED_RECENT_QUARANTINE", f"Deferred recently quarantined issue: {key}")
                continue
            if key not in done:
                pending.append(issue)
    return pending


def _recent_architect_results(limit: int = 200) -> List[Path]:
    """Return recent Architect result files without deleting or moving anything."""
    paths: List[Path] = []
    for folder in (_AIDER_FAILED, _AIDER_DONE, _AIDER_QUARANTINE):
        try:
            if folder.exists():
                paths.extend(p for p in folder.rglob("*.json") if p.is_file())
        except Exception:
            continue
    paths.sort(key=lambda path: path.stat().st_mtime if path.exists() else 0.0, reverse=True)
    return paths[:limit]


def _was_recent_noop(issue: Tuple[str, str, str, int]) -> bool:
    """Return True if a recent Architect job proved this issue produced no diff."""
    return _recent_architect_skip_reason(issue) == "no_diff"


def _recent_architect_skip_reason(issue: Tuple[str, str, str, int]) -> str:
    """Return why a recent Architect result should suppress immediate retries."""
    issue_type, rel_path, _fn_name, lineno = issue
    line_marker = f"line {lineno}"
    for result_path in _recent_architect_results():
        try:
            data = json.loads(result_path.read_text(encoding="utf-8", errors="replace") or "{}")
        except Exception:
            continue
        if data.get("origin") != "luna_architect":
            continue
        targets = [str(item).replace("\\", "/") for item in data.get("target_files") or []]
        if rel_path.replace("\\", "/") not in targets:
            continue
        status = str(data.get("status") or data.get("state") or "").lower()
        noop_reason = str(data.get("noop_reason") or data.get("failure_reason") or "").lower()
        instructions = str(data.get("instructions") or "").lower()
        if issue_type.lower() not in instructions and line_marker.lower() not in instructions:
            continue
        if status == "noop" or "no_diff" in noop_reason:
            return "no_diff"
        if status == "quarantined" or "target_has_staged_or_unstaged_edits" in noop_reason:
            return "quarantined"
    return ""


# ---------------------------------------------------------------------------
# Precise instruction generator
# ---------------------------------------------------------------------------

def _describe_function(rel_path: str, fn_name: str, lineno: int) -> str:
    """Read the first few lines of a function to give aider context."""
    try:
        src = (_PROJECT_DIR / rel_path).read_text(encoding="utf-8", errors="replace")
        lines = src.splitlines()
        snippet = "\n".join(lines[max(0, lineno - 1): lineno + 4])
        return snippet[:300]
    except Exception:
        return ""


def generate_instruction(issue_type: str, rel_path: str, fn_name: str, lineno: int) -> str:
    """Generate a precise aider instruction for a specific issue."""
    if issue_type == "no_docstring":
        return ""
    if issue_type == "no_comments":
        return ""
    if issue_type == "open_no_encoding":
        return (
            f"At line {lineno} there is an `open()` call that is missing "
            f"`encoding='utf-8'`. Add `encoding='utf-8', errors='replace'` "
            f"as arguments to that specific `open()` call. "
            f"Change ONLY that one `open()` call. Do not touch anything else."
        )
    return ""


# ---------------------------------------------------------------------------
# Aider job submission and polling
# ---------------------------------------------------------------------------

def _submit_job(instruction: str, rel_path: str) -> str:
    """Write a task JSON to aider_jobs/active/ and return the task_id."""
    task_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_arch_" + uuid.uuid4().hex[:6]
    payload = {
        "task_id": task_id,
        "id": task_id,
        "task_type": "aider_patch",
        "timestamp": _now_iso(),
        "session_id": "luna_architect",
        "target_files": [rel_path],
        "instructions": instruction,
        "apply_on_pass": False,
        "origin": "luna_architect",
        "expected_diff_type": "safety_fix",
        "analysis_only": False,
    }
    _AIDER_ACTIVE.mkdir(parents=True, exist_ok=True)
    fp = _AIDER_ACTIVE / f"{task_id}.json"
    tmp = _AIDER_ACTIVE / f"{task_id}.tmp"
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    tmp.replace(fp)
    return task_id


def _wait_for_job(task_id: str) -> Tuple[str, bool]:
    """Poll terminal Aider result dirs. Returns (status, applied)."""
    deadline = time.monotonic() + _JOB_TIMEOUT
    while time.monotonic() < deadline:
        for status, folder in (("done", _AIDER_DONE), ("failed", _AIDER_FAILED), ("quarantined", _AIDER_QUARANTINE)):
            p = folder / f"{task_id}.json"
            if p.exists():
                try:
                    data = json.loads(p.read_text(encoding="utf-8", errors="replace") or "{}")
                    result_status = str(data.get("status") or data.get("state") or "").lower()
                    noop_reason = str(data.get("noop_reason") or data.get("failure_reason") or "").lower()
                    if result_status == "quarantined":
                        return "quarantined", False
                    if result_status == "noop" or "no_diff" in noop_reason:
                        return "empty_diff", False
                    applied = bool(data.get("applied") or data.get("status") == "done")
                    # Check solution file for real diff
                    sol = _PROJECT_DIR / "solutions" / f"{task_id}.txt"
                    if sol.exists():
                        sol_txt = sol.read_text(encoding="utf-8", errors="replace")
                        if "no changes" in sol_txt.lower() or "(no changes" in sol_txt:
                            return "empty_diff", False
                    return status, applied
                except Exception:
                    return status, False
        time.sleep(2.0)
    return "timeout", False


# ---------------------------------------------------------------------------
# Kill-switch check
# ---------------------------------------------------------------------------

def _should_stop() -> bool:
    return (
        (_PROJECT_DIR / "LUNA_STOP_NOW.flag").exists()
        or (_LOGS_DIR / "SHUTDOWN.flag").exists()
        or (_MEMORY_DIR / "architect.stop").exists()
    )


# ---------------------------------------------------------------------------
# Main architect loop
# ---------------------------------------------------------------------------

def architect_loop() -> None:
    """Run forever: scan -> pick issue -> fix -> log -> repeat.

    This is the autonomous self-improvement engine. It requires no human input
    and no Claude intervention. It will keep improving Luna's codebase as long
    as issues exist, then wait for new ones to appear (e.g. after Luna writes
    new code via self-upgrade).
    """
    _feed("ARCH_START", "Luna Architect online - autonomous self-improvement active")
    done: set = _load_done()
    consecutive_failures = 0
    cycle = 0

    while not _should_stop():
        try:
            # Scan for pending issues
            pending = scan_all_targets(done)

            if not pending:
                _feed("ARCH_IDLE", "All known issues resolved - waiting for new code")
                # Reset done registry so re-scan finds newly added code
                done = set()
                _save_done(done)
                time.sleep(_CYCLE_INTERVAL * 2)
                continue

            if consecutive_failures >= _MAX_FAILURES:
                _feed("ARCH_REST",
                      f"Hit {consecutive_failures} consecutive failures - resting {_FAILURE_REST}s")
                consecutive_failures = 0
                time.sleep(_FAILURE_REST)
                continue

            # Pick the next issue (first pending, prioritise open_no_encoding > no_docstring > no_comments)
            priority_order = {"open_no_encoding": 0, "no_docstring": 1, "no_comments": 2}
            pending.sort(key=lambda x: priority_order.get(x[0], 9))
            issue_type, rel_path, fn_name, lineno = pending[0]
            key = _issue_key(issue_type, rel_path, fn_name, lineno)

            cycle += 1
            instruction = generate_instruction(issue_type, rel_path, fn_name, lineno)
            if not instruction:
                done.add(key)
                _save_done(done)
                continue

            _feed("ARCH_CYCLE",
                  f"Cycle {cycle}: [{issue_type}] {rel_path}:{lineno} -> {fn_name}",
                  detail=instruction[:200])

            task_id = _submit_job(instruction, rel_path)
            status, applied = _wait_for_job(task_id)

            entry = {
                "ts": _now_iso(),
                "cycle": cycle,
                "issue_type": issue_type,
                "target_file": rel_path,
                "fn_name": fn_name,
                "lineno": lineno,
                "task_id": task_id,
                "status": status,
                "applied": applied,
            }
            _log_nightly(entry)

            if status == "empty_diff":
                # Issue may already be fixed by a previous cycle - mark done
                _feed("ARCH_SKIP",
                      f"Cycle {cycle}: empty diff for {fn_name} - marking done, moving on")
                done.add(key)
                _save_done(done)
                # No failure count increment for empty diffs

            elif status == "done" and applied:
                _feed("ARCH_APPLIED",
                      f"Cycle {cycle}: REAL CHANGE applied to {rel_path}:{fn_name}")
                done.add(key)
                _save_done(done)
                consecutive_failures = 0

            elif status == "done":
                # Done but not applied (staged only)
                _feed("ARCH_STAGED",
                      f"Cycle {cycle}: staged (not applied) for {fn_name}")
                done.add(key)
                _save_done(done)

            elif status == "quarantined":
                _feed("ARCH_DEFERRED",
                      f"Cycle {cycle}: quarantined {fn_name}; waiting for dirty target to clear")
                done.add(key)
                _save_done(done)

            else:
                # Failed or timed out
                consecutive_failures += 1
                _feed("ARCH_FAIL",
                      f"Cycle {cycle}: {status} on {fn_name} "
                      f"(failures={consecutive_failures}/{_MAX_FAILURES})")

            # Update state file so the terminal can display architect status
            try:
                _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
                state = {
                    "running": True,
                    "cycle": cycle,
                    "issues_resolved": len(done),
                    "issues_pending": len(pending) - 1,
                    "last_issue": f"{issue_type}::{rel_path}::{fn_name}",
                    "last_status": status,
                    "consecutive_failures": consecutive_failures,
                    "updated_at": _now_iso(),
                }
                tmp = _STATE_PATH.with_suffix(".tmp")
                tmp.write_text(json.dumps(state, indent=2, ensure_ascii=True), encoding="utf-8")
                tmp.replace(_STATE_PATH)
            except Exception:
                pass

            # Cooldown between cycles
            slept = 0.0
            while slept < _CYCLE_INTERVAL and not _should_stop():
                time.sleep(min(5.0, _CYCLE_INTERVAL - slept))
                slept += 5.0

        except Exception as exc:
            _feed("ARCH_ERROR", f"Architect loop recovered from error: {exc}")
            time.sleep(30.0)

    _feed("ARCH_STOP", "Luna Architect stopped")
    try:
        state = json.loads(_STATE_PATH.read_text(encoding="utf-8", errors="replace") or "{}")
        state["running"] = False
        state["stopped_at"] = _now_iso()
        _STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=True), encoding="utf-8")
    except Exception:
        pass
