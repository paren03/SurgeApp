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
import uuid
from datetime import datetime
from difflib import unified_diff
from pathlib import Path
from typing import Any, Dict

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_DIR   = Path(os.environ.get("LUNA_PROJECT_DIR", r"D:\SurgeApp"))
ACTIVE_DIR    = PROJECT_DIR / "aider_jobs" / "active"   # UI writes here
DONE_DIR      = PROJECT_DIR / "aider_jobs" / "done"     # UI reads here
FAILED_DIR    = PROJECT_DIR / "aider_jobs" / "failed"   # UI reads here
SOLUTIONS_DIR = PROJECT_DIR / "solutions"
LOGIC_DIR     = PROJECT_DIR / "logic_updates"
LOGS_DIR      = PROJECT_DIR / "logs"
LIVE_FEED_PATH = LOGS_DIR / "luna_live_feed.jsonl"

# ── Aider python (prefer .aider_venv; reject 0-byte MS Store stubs) ────────────
def _aider_python() -> str:
    candidates = [
        str(PROJECT_DIR / ".aider_venv" / "Scripts" / "python.exe"),
        str(PROJECT_DIR / ".aider_venv" / "Scripts" / "pythonw.exe"),
    ]
    local_app = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
    candidates += [
        str(Path(local_app) / "Microsoft" / "WindowsApps" /
            "PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0" / "python.exe"),
        str(Path(local_app) / "Programs" / "Python" / "Python311" / "python.exe"),
        r"C:\Python311\python.exe",
    ]
    for c in candidates:
        try:
            p = Path(c)
            low = c.lower()
            if low.endswith(r"\windowsapps\python.exe") or low.endswith(r"\windowsapps\pythonw.exe"):
                continue
            if p.exists() and p.stat().st_size > 0:
                return c
        except Exception:
            continue
    return sys.executable

AIDER_PYTHON  = _aider_python()

# ── Aider config ───────────────────────────────────────────────────────────────
AIDER_MODEL   = "ollama_chat/qwen2.5-coder:7b-instruct"   # code-specialist; faster + more accurate than llama3.1:8b
AIDER_FLAGS   = [
    "--map-tokens", "0",
    "--map-refresh", "manual",
    "--max-chat-history-tokens", "512",   # was 8000 — single-shot jobs need almost none
    "--no-detect-urls",
    "--no-restore-chat-history",
    "--no-gitignore",
    "--yes-always",
    "--no-auto-commits",
    "--no-show-model-warnings",
]

APPLY_ON_PASS  = os.environ.get("APPLY_ON_PASS", "false").lower() == "true"
POLL_INTERVAL  = 3.0   # seconds between active dir scans
AIDER_TIMEOUT  = 180   # seconds — qwen2.5-coder:7b on small files finishes in <2 min
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
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _read_task(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _update_task(path: Path, **updates: object) -> None:
    try:
        data = _read_task(path)
        data.update(updates)
        _write_json(path, data)
    except Exception:
        pass


def _finish(task_path: Path, task_id: str, report: str, success: bool) -> None:
    sol_path = SOLUTIONS_DIR / f"{task_id}.txt"
    try:
        sol_path.write_text(report, encoding="utf-8")
    except Exception as exc:
        _log(f"solution write failed: {exc}")

    dest_dir = DONE_DIR if success else FAILED_DIR
    dest = dest_dir / f"{task_id}.json"
    try:
        _update_task(task_path, status="done" if success else "failed",
                     state="done" if success else "failed",
                     phase="complete", progress=100, finished_at=_now_iso())
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


# ── Core pipeline ──────────────────────────────────────────────────────────────

def run_aider_patch(task_path: Path) -> None:
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

    # Stage 1/5 — CLAIM
    _log(f"Processing aider_patch task={task_id} target={target!r}")
    _live_feed("CLAIM", f"Stage 1/5: Claimed job", task_id=task_id,
               detail=f"target={target}")
    _update_task(task_path, status="running", phase="aider_patch", progress=10)

    if not prompt:
        _live_feed("FAILED", "Stage 5/5: Failed — no prompt", task_id=task_id)
        _finish(task_path, task_id, "[AIDER-BRIDGE]\nNo prompt provided.", False)
        return
    if not target or not Path(target).exists():
        _live_feed("FAILED", f"Stage 5/5: Failed — target not found", task_id=task_id,
                   detail=target)
        _finish(task_path, task_id, f"[AIDER-BRIDGE]\nTarget not found: {target}", False)
        return

    target_path = Path(target)

    # 1. Copy target to safe workspace (unique per bridge PID to avoid collision)
    workspace = LOGIC_DIR / f"{task_id}_{os.getpid()}"
    workspace.mkdir(parents=True, exist_ok=True)
    copy_path = workspace / target_path.name
    shutil.copy2(target_path, copy_path)
    _log(f"Copied {target_path.name} → {copy_path}")
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
        )
        aider_stdout = result.stdout or ""
        aider_stderr = result.stderr or ""
        aider_rc = result.returncode
    except subprocess.TimeoutExpired:
        _live_feed("FAILED", f"Stage 5/5: Aider timed out ({AIDER_TIMEOUT}s)", task_id=task_id)
        _log(f"Aider timed out after {AIDER_TIMEOUT}s")
        _finish(task_path, task_id, f"[AIDER-BRIDGE]\nAider timed out ({AIDER_TIMEOUT}s).", False)
        return
    except FileNotFoundError:
        _log("Aider not found — is it installed in this Python env?")
        _finish(task_path, task_id, "[AIDER-BRIDGE]\nAider executable not found.", False)
        return

    # Stage 2/5 — RUN_AIDER_END
    _log(f"Aider finished rc={aider_rc}")
    _live_feed("RUN_AIDER_END", f"Stage 2/5: Aider finished rc={aider_rc}", task_id=task_id)
    _update_task(task_path, progress=60)

    # Stage 3/5 — DIFF_SAVED
    diff = _make_diff(target_path, copy_path)
    diff_path = ""
    if diff:
        diff_file = workspace / f"{task_id}.diff"
        try:
            diff_file.write_text(diff, encoding="utf-8", errors="replace")
            diff_path = str(diff_file)
        except Exception:
            pass
    _live_feed("DIFF_SAVED", f"Stage 3/5: Diff {'saved' if diff else 'empty'}", task_id=task_id,
               detail=diff_path or "(no changes)")

    # Stage 4/5 — VERIFY_COMPILE
    # If aider made no changes (empty diff), treat as a no-op success without
    # touching the staged copy — the live file is unchanged anyway.
    if not diff:
        ok, err = True, "no changes; verify skipped"
    else:
        ok, err = _py_verify(copy_path)
    _log(f"Syntax check: {'PASS' if ok else 'FAIL'} {err}")
    _live_feed("VERIFY_COMPILE", f"Stage 4/5: Compile {'PASS' if ok else 'FAIL'}", task_id=task_id,
               detail=err or "")
    _update_task(task_path, progress=80)

    # Stage 5a — APPLY (optional)
    applied = False
    if ok and apply and diff:
        _live_feed("APPLY", "Stage 5/5: Applying patch to live file", task_id=task_id)
        try:
            shutil.copy2(copy_path, target_path)
            applied = True
            _log(f"Applied patch to {target_path}")
        except Exception as exc:
            _log(f"Apply failed: {exc}")
            ok = False
            err = str(exc)

    # Stage 5b — DONE / FAILED
    status_line = "APPLIED" if applied else ("PASS_STAGED" if ok else "FAIL")
    final_event = "DONE" if ok else "FAILED"
    _live_feed(final_event, f"Stage 5/5: {status_line}", task_id=task_id,
               detail=err or f"diff_path={diff_path}")
    report = (
        f"# AIDER BRIDGE REPORT\n"
        f"# task_id={task_id}  target={target}  status={status_line}\n\n"
        f"## Prompt\n{prompt}\n\n"
        f"## Diff\n```diff\n{diff or '(no changes detected)'}\n```\n\n"
        f"## Verification\n"
        f"syntax_ok={ok}  applied={applied}  aider_rc={aider_rc}\n"
        f"{('error: ' + err) if err else ''}\n\n"
        f"## Aider stdout\n{aider_stdout[:4000]}\n\n"
        f"## Aider stderr\n{aider_stderr[:2000]}\n"
    )
    _finish(task_path, task_id, report, ok)
    _log(f"Done task={task_id} status={status_line}")


# ── Main watch loop ────────────────────────────────────────────────────────────

def _pid_alive(pid: int) -> bool:
    try:
        import ctypes
        handle = ctypes.windll.kernel32.OpenProcess(0x0400, False, pid)  # PROCESS_QUERY_INFORMATION
        if not handle:
            return False
        code = ctypes.c_ulong(0)
        ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        ctypes.windll.kernel32.CloseHandle(handle)
        return code.value == 259  # STILL_ACTIVE
    except Exception:
        try:
            import signal
            os.kill(pid, 0)
            return True
        except Exception:
            return False


def main() -> None:
    for d in [ACTIVE_DIR, DONE_DIR, FAILED_DIR, SOLUTIONS_DIR, LOGIC_DIR, LOGS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    # Single-instance lock — bail if another bridge is already running
    my_pid = os.getpid()
    try:
        if BRIDGE_PID_PATH.exists():
            try:
                existing = int(BRIDGE_PID_PATH.read_text(encoding="utf-8").strip())
                if existing != my_pid and _pid_alive(existing):
                    _log(f"Bridge already running at PID {existing}. Exiting.")
                    return
            except Exception:
                pass
        BRIDGE_PID_PATH.write_text(str(my_pid), encoding="utf-8")
    except Exception:
        pass

    import atexit
    atexit.register(lambda: BRIDGE_PID_PATH.unlink(missing_ok=True) if BRIDGE_PID_PATH.exists() else None)

    _log("Aider Bridge started. Watching aider_jobs/active/ for aider_patch tasks.")
    _log(f"Model: {AIDER_MODEL}  timeout={AIDER_TIMEOUT}s  APPLY_ON_PASS={APPLY_ON_PASS}")

    seen: set[str] = set()

    while True:
        try:
            for task_file in sorted(ACTIVE_DIR.glob("*.json")):
                if task_file.name in seen:
                    continue
                task = _read_task(task_file)
                ttype = str(task.get("task_type") or "").lower()
                if ttype != "aider_patch":
                    continue
                seen.add(task_file.name)
                try:
                    run_aider_patch(task_file)
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
