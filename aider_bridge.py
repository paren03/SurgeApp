"""Aider Bridge — safe task watcher for aider_patch jobs.

Watches aider_jobs/active/ for JSON tasks with task_type=aider_patch.
For each task:
  1. Copies the target file to logic_updates/<task_id>/
  2. Runs Aider on the COPY (never the original directly)
  3. Generates a unified diff
  4. Verifies with py_compile + import check
  5. If APPLY_ON_PASS=true and verification passes, replaces original
  6. Writes result to solutions/<task_id>.txt and moves task to done/failed

Emits live_feed events at every stage: CLAIM, RUN_AIDER_START, RUN_AIDER_END,
DIFF_SAVED, VERIFY_COMPILE, VERIFY_IMPORT, APPLY, DONE/FAILED.

Python preference: D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe (skips MS Store alias stubs).
UTF-8 IO with errors='replace'; JSON ensure_ascii=True; no stdout under pythonw.
"""
from __future__ import annotations

import json
import os
import py_compile
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime
from difflib import unified_diff
from pathlib import Path
from typing import Any, Dict, Tuple

from luna_modules.luna_aider_result_policy import (
    build_aider_completion_record,
    build_aider_report,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_DIR   = Path(os.environ.get("LUNA_PROJECT_DIR", r"D:\SurgeApp"))
ACTIVE_DIR    = PROJECT_DIR / "aider_jobs" / "active"   # UI writes here
DONE_DIR      = PROJECT_DIR / "aider_jobs" / "done"     # UI reads here
FAILED_DIR    = PROJECT_DIR / "aider_jobs" / "failed"   # UI reads here
QUARANTINE_DIR = PROJECT_DIR / "aider_jobs" / "quarantine"
SOLUTIONS_DIR = PROJECT_DIR / "solutions"
SOLUTION_LOGS_DIR = SOLUTIONS_DIR / "logs"
SOLUTION_DIFFS_DIR = SOLUTIONS_DIR / "diffs"
LOGIC_DIR     = PROJECT_DIR / "logic_updates"
LOGS_DIR      = PROJECT_DIR / "logs"
LIVE_FEED_PATH = LOGS_DIR / "luna_live_feed.jsonl"
OLLAMA_API_BASE = os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_TAGS_URL = f"{OLLAMA_API_BASE}/api/tags"
MAX_UNSCOPED_TARGET_BYTES = int(os.environ.get("LUNA_AIDER_MAX_UNSCOPED_BYTES", "120000"))
MAX_FAILED_PER_CYCLE = 5
MAX_NOOP_PER_CYCLE = 5
MAX_JOBS_PER_CYCLE = 12

# ── Aider python (prefer .aider_venv; reject 0-byte MS Store stubs) ────────────
def _is_safe_aider_python(candidate: str) -> bool:
    low = str(candidate or "").lower()
    if "windowsapps" in low:
        return False
    try:
        path = Path(candidate)
        return path.exists() and path.stat().st_size > 0
    except Exception:
        return False


def _aider_python() -> str:
    candidates = [
        str(PROJECT_DIR / ".aider_venv" / "Scripts" / "python.exe"),
        str(PROJECT_DIR / ".aider_venv" / "Scripts" / "pythonw.exe"),
    ]
    local_app = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
    candidates += [
        str(Path(local_app) / "Programs" / "Python" / "Python311" / "python.exe"),
        r"C:\Python311\python.exe",
    ]
    for c in candidates:
        if _is_safe_aider_python(c):
            return c
    return sys.executable if _is_safe_aider_python(sys.executable) else str(PROJECT_DIR / ".aider_venv" / "Scripts" / "python.exe")

AIDER_PYTHON  = _aider_python()

# ── Aider config ───────────────────────────────────────────────────────────────
# Read from env var so launcher can pin the model permanently at boot.
AIDER_MODEL   = os.environ.get(
    "LUNA_INSTRUCTOR_MODEL", "ollama_chat/qwen2.5-coder:7b-instruct"
)
AIDER_FLAGS   = [
    "--no-pretty",
    "--no-stream",
    "--map-tokens", "0",
    "--map-refresh", "manual",
    "--max-chat-history-tokens", "512",
    "--no-detect-urls",
    "--no-restore-chat-history",
    "--no-gitignore",
    "--yes-always",
    "--no-auto-commits",
    "--no-show-model-warnings",
    "--edit-format", "diff",
    # diff (search/replace blocks) is REQUIRED for qwen2.5-coder:7b.
    # Default "whole" makes aider return entire file (~4500 tokens) and times
    # out at 240s. "diff" outputs only changed lines (~20-50 tokens) and
    # completes in ~25-50s. Without this flag, jobs WIPE files to 0 bytes.
]

APPLY_ON_PASS  = os.environ.get("APPLY_ON_PASS", "false").lower() == "true"
POLL_INTERVAL  = 3.0   # seconds between active dir scans
AIDER_TIMEOUT  = 240   # seconds — sufficient for diff format
BRIDGE_PID_PATH = LOGS_DIR / "aider_bridge.pid"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [AIDER-BRIDGE] {msg}"
    try:
        (LOGS_DIR / "aider_bridge.log").open("a", encoding="utf-8", errors="replace").write(line + "\n")
    except Exception:
        pass


def _live_feed(event: str, msg: str, task_id: str = "", detail: str = "") -> None:
    """Emit a structured event to luna_live_feed.jsonl for the UI to mirror."""
    try:
        row: Dict[str, Any] = {
            "ts":     datetime.now().strftime("%H:%M:%S"),
            "event":  event,
            "icon":   "[AIDER] ",
            "msg":    str(msg or "")[:240],
            "source": "aider_bridge",
        }
        if task_id:
            row["task_id"] = str(task_id)
        if detail:
            row["detail"] = str(detail)[:600]
        LIVE_FEED_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LIVE_FEED_PATH.open("a", encoding="utf-8", errors="replace") as f:
            json.dump(row, f, ensure_ascii=True)
            f.write("\n")
    except Exception:
        pass


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(f".{uuid.uuid4().hex[:8]}.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8", errors="replace")
    tmp.replace(path)


def _job_log_path(task_id: str) -> Path:
    return SOLUTION_LOGS_DIR / f"{task_id}.log"


def _job_diff_path(task_id: str) -> Path:
    return SOLUTION_DIFFS_DIR / f"{task_id}.diff"


def _append_job_log(task_id: str, message: str) -> None:
    try:
        SOLUTION_LOGS_DIR.mkdir(parents=True, exist_ok=True)
        with _job_log_path(task_id).open("a", encoding="utf-8", errors="replace") as f:
            f.write(f"[{_now_iso()}] {message}\n")
    except Exception:
        pass


def _aider_subprocess_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["OLLAMA_API_BASE"] = OLLAMA_API_BASE
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    return env


def _ollama_available(timeout_seconds: float = 5.0) -> Tuple[bool, str]:
    try:
        with urllib.request.urlopen(OLLAMA_TAGS_URL, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", 200) or 200)
            if 200 <= status < 300:
                return True, OLLAMA_TAGS_URL
            return False, f"http_status_{status}"
    except Exception as exc:
        return False, str(exc)


def _prompt_has_blocked_url(prompt: str) -> bool:
    lowered = str(prompt or "").lower()
    blocked = (
        "http://127.0.0.1:11434",
        "https://127.0.0.1:11434",
        "http://localhost:11434",
        "https://localhost:11434",
    )
    return any(item in lowered for item in blocked)


def _target_scope_allowed(target_path: Path, task: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        size = target_path.stat().st_size
    except Exception:
        return False, "target_stat_failed"
    if size <= 0:
        return False, "empty_target_file"
    has_scope = bool(
        task.get("function_scope")
        or task.get("excerpt_mode")
        or task.get("excerpt")
        or (task.get("start_line") and task.get("end_line"))
    )
    if size > MAX_UNSCOPED_TARGET_BYTES and not has_scope:
        return False, "oversized_target_requires_scope"
    return True, ""


def _target_has_local_edits(target_path: Path) -> bool:
    """Return True when git already has staged/unstaged edits for target_path."""
    try:
        rel = target_path
        if target_path.is_absolute():
            rel = target_path.resolve().relative_to(PROJECT_DIR.resolve())
        result = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=no", "--", str(rel)],
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        return result.returncode == 0 and bool((result.stdout or "").strip())
    except Exception:
        return False


def _read_task(path: Path) -> dict:
    """Read a task from a JSON file and return it as a dictionary."""

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _update_task(path: Path, **updates: object) -> None:
    """Update a task file with the given updates."""
    try:
        data = _read_task(path)
        data.update(updates)
        _write_json(path, data)
    except Exception:
        pass


def _finish(
    task_path: Path,
    task_id: str,
    report: str,
    success: bool,
    result_record: Dict[str, Any] | None = None,
) -> None:
    sol_path = SOLUTIONS_DIR / f"{task_id}.txt"
    try:
        sol_path.write_text(report, encoding="utf-8")
    except Exception as exc:
        _log(f"solution write failed: {exc}")

    final_status = str((result_record or {}).get("status") or ("done" if success else "failed"))
    if final_status == "done":
        dest_dir = DONE_DIR
    elif final_status == "quarantined":
        dest_dir = QUARANTINE_DIR
    else:
        dest_dir = FAILED_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{task_id}.json"
    try:
        updates: Dict[str, Any] = {
            "status": final_status,
            "state": final_status,
            "phase": "complete",
            "progress": 100,
            "finished_at": _now_iso(),
        }
        if result_record:
            updates.update(result_record)
        _update_task(task_path, **updates)
        os.replace(str(task_path), str(dest))
    except Exception as exc:
        _log(f"task finalize failed: {exc}")


def _py_verify(path: Path) -> tuple[bool, str]:
    """Verify Python syntax with py_compile.

    Defensive: catches PyCompileError, FileNotFoundError, and any other
    exception so a missing/quarantined staged copy never crashes the watcher.
    """
    if path.suffix.lower() != ".py":
        return True, ""
    if not path.exists():
        return False, f"staged file vanished before verify: {path}"
    try:
        py_compile.compile(str(path), doraise=True)
        return True, ""
    except py_compile.PyCompileError as exc:
        return False, str(exc)
    except Exception as exc:
        return False, f"verify error: {exc}"


def _make_diff(original: Path, patched: Path) -> str:
    try:
        a = original.read_text(encoding="utf-8", errors="ignore").splitlines(keepends=True)
        b = patched.read_text(encoding="utf-8", errors="ignore").splitlines(keepends=True)
        return "".join(unified_diff(a, b, fromfile=f"a/{original.name}", tofile=f"b/{patched.name}"))
    except Exception:
        return ""


def _finish_quarantined(
    task_path: Path,
    task_id: str,
    target: str,
    prompt: str,
    reason: str,
    started_at: float,
    analysis_only: bool = False,
) -> None:
    log_path = str(_job_log_path(task_id))
    _append_job_log(task_id, f"QUARANTINED {reason}")
    _live_feed("QUARANTINED", f"Stage 5/5: QUARANTINED — {reason}", task_id=task_id, detail=target)
    record = build_aider_completion_record(
        task_id=task_id,
        target_file=target,
        diff_text="",
        diff_path="",
        log_path=log_path,
        verification_passed=False,
        applied=False,
        failure_reason=reason,
        analysis_only=analysis_only,
        model_used=AIDER_MODEL,
        started_at=started_at,
        finished_at=time.time(),
        quarantined_reason=reason,
    )
    _finish(task_path, task_id, build_aider_report(record, prompt=prompt, diff_text="", stdout="", stderr=""), False, record)


# ── Core pipeline ──────────────────────────────────────────────────────────────

def run_aider_patch(task_path: Path) -> None:
    started_at = time.time()
    task = _read_task(task_path)
    task_id   = str(task.get("task_id") or task.get("id") or task_path.stem)
    prompt    = str(task.get("prompt") or task.get("instructions") or "").strip()
    # support both singular and list form
    _tf = task.get("target_file") or ""
    _tfs = task.get("target_files") or []
    if not _tf and _tfs:
        _tf = str(_tfs[0]) if isinstance(_tfs, list) else str(_tfs)
    target    = str(_tf).strip()
    # resolve relative path against PROJECT_DIR
    if target and not Path(target).is_absolute():
        target = str(PROJECT_DIR / target)
    apply     = bool(task.get("apply_on_pass", APPLY_ON_PASS))
    analysis_only = bool(task.get("analysis_only") is True)
    log_path = str(_job_log_path(task_id))

    # Stage 1/5 — CLAIM
    _log(f"Processing aider_patch task={task_id} target={target!r}")
    _append_job_log(task_id, f"CLAIM target={target!r}")
    _live_feed("CLAIM", f"Stage 1/5: Claimed job", task_id=task_id,
               detail=f"target={target}")
    _update_task(task_path, status="running", phase="aider_patch", progress=10)

    if not prompt:
        _live_feed("FAILED", "Stage 5/5: Failed — no prompt", task_id=task_id)
        record = build_aider_completion_record(
            task_id=task_id,
            target_file=target,
            diff_text="",
            diff_path="",
            log_path=log_path,
            verification_passed=False,
            applied=False,
            failure_reason="no_prompt",
            analysis_only=analysis_only,
            model_used=AIDER_MODEL,
            started_at=started_at,
            finished_at=time.time(),
        )
        _finish(task_path, task_id, build_aider_report(record, prompt=prompt, diff_text="", stdout="", stderr=""), False, record)
        return
    if _prompt_has_blocked_url(prompt):
        _finish_quarantined(task_path, task_id, target, prompt, "blocked_local_service_url", started_at, analysis_only)
        return
    if not target or not Path(target).exists():
        _finish_quarantined(task_path, task_id, target, prompt, "target_not_found", started_at, analysis_only)
        return

    target_path = Path(target)
    scope_ok, scope_reason = _target_scope_allowed(target_path, task)
    if not scope_ok:
        _finish_quarantined(task_path, task_id, target, prompt, scope_reason, started_at, analysis_only)
        return
    if _target_has_local_edits(target_path):
        _finish_quarantined(
            task_path,
            task_id,
            target,
            prompt,
            "target_has_staged_or_unstaged_edits",
            started_at,
            analysis_only,
        )
        return
    if not _is_safe_aider_python(AIDER_PYTHON):
        _finish_quarantined(task_path, task_id, target, prompt, "unsafe_aider_python", started_at, analysis_only)
        return
    ollama_ok, ollama_detail = _ollama_available()
    _live_feed(
        "OLLAMA_CHECK",
        f"Ollama {'available' if ollama_ok else 'unavailable'}",
        task_id=task_id,
        detail=ollama_detail,
    )
    _append_job_log(task_id, f"OLLAMA_CHECK ok={ollama_ok} detail={ollama_detail}")
    if not ollama_ok:
        record = build_aider_completion_record(
            task_id=task_id,
            target_file=target,
            diff_text="",
            diff_path="",
            log_path=log_path,
            verification_passed=False,
            applied=False,
            failure_reason="ollama_unavailable",
            analysis_only=analysis_only,
            model_used=AIDER_MODEL,
            started_at=started_at,
            finished_at=time.time(),
        )
        _finish(task_path, task_id, build_aider_report(record, prompt=prompt, diff_text="", stdout="", stderr=ollama_detail), False, record)
        return

    # 1. Copy target to safe workspace (unique per bridge PID to avoid collision)
    workspace = LOGIC_DIR / f"{task_id}_{os.getpid()}"
    workspace.mkdir(parents=True, exist_ok=True)
    copy_path = workspace / target_path.name
    shutil.copy2(target_path, copy_path)
    if not copy_path.exists() or copy_path.stat().st_size <= 0:
        _finish_quarantined(task_path, task_id, target, prompt, "empty_staged_copy", started_at, analysis_only)
        return
    _log(f"Copied {target_path.name} → {copy_path}")
    _append_job_log(task_id, f"Copied {target_path} -> {copy_path}")
    _update_task(task_path, progress=20)

    # Stage 2/5 — RUN_AIDER_START
    cmd = [
        AIDER_PYTHON, "-m", "aider",
        "--model", AIDER_MODEL,
        *AIDER_FLAGS,
        "--file", str(copy_path),
        "--message", prompt,
    ]
    _log(f"Running: {' '.join(cmd[:6])} ...")
    _append_job_log(task_id, f"RUN_AIDER_START python={AIDER_PYTHON} model={AIDER_MODEL}")
    _live_feed("RUN_AIDER_START", "Stage 2/5: Aider running", task_id=task_id,
               detail=f"python={AIDER_PYTHON}")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(workspace),
            capture_output=True,
            text=True,
            timeout=AIDER_TIMEOUT,
            creationflags=0x08000000,  # CREATE_NO_WINDOW
            env=_aider_subprocess_env(),
        )
        aider_stdout = result.stdout or ""
        aider_stderr = result.stderr or ""
        aider_rc = result.returncode
        _append_job_log(task_id, f"RUN_AIDER_END rc={aider_rc}")
        if aider_stdout:
            _append_job_log(task_id, "STDOUT\n" + aider_stdout[:4000])
        if aider_stderr:
            _append_job_log(task_id, "STDERR\n" + aider_stderr[:4000])
    except subprocess.TimeoutExpired:
        _live_feed("FAILED", f"Stage 5/5: Aider timed out ({AIDER_TIMEOUT}s)", task_id=task_id)
        _log(f"Aider timed out after {AIDER_TIMEOUT}s")
        record = build_aider_completion_record(
            task_id=task_id,
            target_file=target,
            diff_text="",
            diff_path="",
            log_path=log_path,
            verification_passed=False,
            applied=False,
            failure_reason="aider_timeout",
            analysis_only=analysis_only,
            model_used=AIDER_MODEL,
            started_at=started_at,
            finished_at=time.time(),
        )
        _finish(task_path, task_id, build_aider_report(record, prompt=prompt, diff_text="", stdout="", stderr=""), False, record)
        return
    except FileNotFoundError:
        _log("Aider not found — is it installed in this Python env?")
        record = build_aider_completion_record(
            task_id=task_id,
            target_file=target,
            diff_text="",
            diff_path="",
            log_path=log_path,
            verification_passed=False,
            applied=False,
            failure_reason="aider_executable_not_found",
            analysis_only=analysis_only,
            model_used=AIDER_MODEL,
            started_at=started_at,
            finished_at=time.time(),
        )
        _finish(task_path, task_id, build_aider_report(record, prompt=prompt, diff_text="", stdout="", stderr=""), False, record)
        return

    # Stage 2/5 — RUN_AIDER_END
    _log(f"Aider finished rc={aider_rc}")
    _live_feed("RUN_AIDER_END", f"Stage 2/5: Aider finished rc={aider_rc}", task_id=task_id)
    _update_task(task_path, progress=60)

    # Stage 3/5 — DIFF_SAVED
    diff = _make_diff(target_path, copy_path)
    diff_path = ""
    if diff:
        SOLUTION_DIFFS_DIR.mkdir(parents=True, exist_ok=True)
        diff_file = _job_diff_path(task_id)
        try:
            diff_file.write_text(diff, encoding="utf-8", errors="replace")
            diff_path = str(diff_file)
        except Exception:
            pass
    _live_feed("DIFF_SAVED", f"Stage 3/5: Diff {'saved' if diff else 'empty'}", task_id=task_id,
               detail=diff_path or "(no changes)")

    # Stage 4/5 — VERIFY_COMPILE
    # If aider made no changes (empty diff), record a NOOP unless the task was
    # explicitly marked analysis_only=true.
    if not diff:
        ok, err = True, "no changes; verify skipped"
    else:
        ok, err = _py_verify(copy_path)
    _log(f"Syntax check: {'PASS' if ok else 'FAIL'} {err}")
    _append_job_log(task_id, f"VERIFY_COMPILE ok={ok} detail={err}")
    _live_feed("VERIFY_COMPILE", f"Stage 4/5: Compile {'PASS' if ok else 'FAIL'}", task_id=task_id,
               detail=err or "")
    _live_feed("VERIFY_IMPORT", "Stage 4/5: Import check skipped", task_id=task_id,
               detail="stage_only copy verification; import is required before applying worker.py")
    _update_task(task_path, progress=80)

    # Stage 5a — APPLY (optional)
    applied = False
    if ok and apply and diff:
        # ── SAFETY GUARD ───────────────────────────────────────────────────────
        # Prevent the "wipe-to-0-bytes" disaster we saw on 2026-04-29.
        # Aider sometimes writes a near-empty file when the model fails the
        # SEARCH/REPLACE format. Reject anything below safety thresholds.
        try:
            orig_size = target_path.stat().st_size
            new_size  = copy_path.stat().st_size
        except Exception:
            orig_size = new_size = 0
        # Hard rules:
        #  - new file MUST be at least 100 bytes (no wiping)
        #  - new file MUST be at least 30% of original (no truncation)
        #  - new file must not be MORE than 250% of original (no runaway)
        size_ok = (
            new_size >= 100
            and (orig_size == 0 or new_size >= orig_size * 0.30)
            and (orig_size == 0 or new_size <= orig_size * 2.5)
        )
        if not size_ok:
            ok = False
            err = (
                f"BLOCKED by size guard: original={orig_size}B  staged={new_size}B "
                f"(must be 100B-250% of original and >=30% of original)"
            )
            _log(f"REFUSED apply: {err}")
            _live_feed("APPLY_BLOCKED", "Stage 5/5: Apply REJECTED — size guard",
                       task_id=task_id, detail=err)
        else:
            _live_feed("APPLY", "Stage 5/5: Applying patch to live file", task_id=task_id)
            try:
                # Make a backup BEFORE replacing — so we can recover if anything
                # else goes wrong downstream.
                backup_dir = PROJECT_DIR / "backups" / "aider_apply"
                backup_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = backup_dir / f"{target_path.name}.{ts}.bak"
                shutil.copy2(target_path, backup_path)
                shutil.copy2(copy_path, target_path)
                applied = True
                _log(f"Applied patch to {target_path}  backup={backup_path.name}")
            except Exception as exc:
                _log(f"Apply failed: {exc}")
                ok = False
                err = str(exc)

    # Stage 5b — DONE / FAILED / NOOP
    record = build_aider_completion_record(
        task_id=task_id,
        target_file=target,
        diff_text=diff,
        diff_path=diff_path,
        log_path=log_path,
        verification_passed=ok,
        applied=applied,
        failure_reason=err if not ok else "",
        analysis_only=analysis_only,
        model_used=AIDER_MODEL,
        started_at=started_at,
        finished_at=time.time(),
    )
    status_line = str(record["status"]).upper()
    _live_feed(
        record["live_feed_event"],
        f"Stage 5/5: {status_line}",
        task_id=task_id,
        detail=record.get("failure_reason") or record.get("noop_reason") or f"diff_path={diff_path}",
    )
    report = build_aider_report(record, prompt=prompt, diff_text=diff, stdout=aider_stdout, stderr=aider_stderr)
    _finish(task_path, task_id, report, record["status"] == "done", record)
    _log(f"Done task={task_id} status={status_line}")


# ── Main watch loop ────────────────────────────────────────────────────────────

def _process_command_line(pid: int) -> str:
    """Return the command line for pid, or an empty string if the process is gone."""
    if pid <= 0:
        return ""
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"(Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\").CommandLine",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            creationflags=0x08000000,
        )
        if result.returncode == 0:
            return (result.stdout or "").strip()
    except Exception:
        pass
    return ""


def _pid_alive(pid: int, marker: str = "") -> bool:
    """Reliable PID check that can require the process command line to match Luna."""
    if pid <= 0:
        return False
    if marker:
        return marker.lower() in _process_command_line(pid).lower()
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            creationflags=0x08000000,
        )
        return str(pid) in (result.stdout or "")
    except Exception:
        pass
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _bridge_pid_blocks_start(existing_pid: int, my_pid: int, parent_pid: int) -> bool:
    if existing_pid <= 0 or existing_pid in {my_pid, parent_pid}:
        return False
    return _pid_alive(existing_pid, "aider_bridge.py")


def main() -> None:
    for d in [
        ACTIVE_DIR,
        DONE_DIR,
        FAILED_DIR,
        QUARANTINE_DIR,
        SOLUTIONS_DIR,
        SOLUTION_LOGS_DIR,
        SOLUTION_DIFFS_DIR,
        LOGIC_DIR,
        LOGS_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)

    if "--verify-smoke" in sys.argv:
        print("aider bridge smoke ok")
        return

    # Single-instance lock — bail if another bridge is already running
    my_pid = os.getpid()
    try:
        if BRIDGE_PID_PATH.exists():
            try:
                existing = int(BRIDGE_PID_PATH.read_text(encoding="utf-8").strip())
                if _bridge_pid_blocks_start(existing, my_pid, os.getppid()):
                    _log(f"Bridge already running at PID {existing}. Exiting.")
                    return
                if existing != my_pid:
                    _log(f"Clearing stale bridge PID {existing}.")
            except Exception:
                pass
        BRIDGE_PID_PATH.write_text(str(my_pid), encoding="utf-8")
    except Exception:
        pass

    import atexit

    def _release_pid_lock() -> None:
        try:
            if BRIDGE_PID_PATH.exists() and BRIDGE_PID_PATH.read_text(encoding="utf-8").strip() == str(my_pid):
                BRIDGE_PID_PATH.unlink(missing_ok=True)
        except Exception:
            pass

    atexit.register(_release_pid_lock)

    _log("Aider Bridge started. Watching aider_jobs/active/ for aider_patch tasks.")
    _log(f"Model: {AIDER_MODEL}  timeout={AIDER_TIMEOUT}s  APPLY_ON_PASS={APPLY_ON_PASS}")

    seen: set[str] = set()

    while True:
        try:
            jobs_seen = 0
            failed_seen = 0
            noop_seen = 0
            for task_file in sorted(ACTIVE_DIR.glob("*.json")):
                if jobs_seen >= MAX_JOBS_PER_CYCLE or failed_seen >= MAX_FAILED_PER_CYCLE or noop_seen >= MAX_NOOP_PER_CYCLE:
                    _live_feed(
                        "AIDER_BUDGET_PAUSED",
                        "Aider Bridge paused this scan after hitting job/failure/noop budget",
                        detail=f"jobs={jobs_seen} failed={failed_seen} noop={noop_seen}",
                    )
                    break
                if task_file.name in seen:
                    continue
                task = _read_task(task_file)
                ttype = str(task.get("task_type") or "").lower()
                if ttype != "aider_patch":
                    continue
                seen.add(task_file.name)
                try:
                    run_aider_patch(task_file)
                    jobs_seen += 1
                    task_id = str(task.get("task_id") or task.get("id") or task_file.stem)
                    result_payload: Dict[str, Any] = {}
                    for result_dir in (DONE_DIR, FAILED_DIR, QUARANTINE_DIR):
                        result_file = result_dir / f"{task_id}.json"
                        if result_file.exists():
                            result_payload = _read_task(result_file)
                            break
                    status = str(result_payload.get("status") or result_payload.get("state") or "").lower()
                    if status in {"failed", "quarantined"}:
                        failed_seen += 1
                    elif status == "noop":
                        noop_seen += 1
                except Exception as exc:
                    _log(f"Unexpected error on {task_file.name}: {exc}")
                    _finish(
                        task_file,
                        task_file.stem,
                        f"[AIDER-BRIDGE]\nUnexpected error: {exc}",
                        False,
                    )
        except Exception as exc:
            _log(f"Watch loop error: {exc}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
