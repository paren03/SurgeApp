"""LaunchLuna.pyw - One-click launcher for Luna Command Center.

Shortcut target: pythonw3.11.exe "D:\SurgeApp\LaunchLuna.pyw"

This script replaces the VBS -> BAT chain with a single Python launcher that:
  1. Ensures all required directories exist
  2. Archives stale worker/service locks if the old PID is dead
  3. Starts Ollama (if not already serving)
  4. Starts all background services (StartIfMissing)
  5. Opens the SurgeApp_Claude_Terminal visible window

Double-clicking the desktop icon is safe -- each service only starts
once even if clicked multiple times.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import json
from pathlib import Path
from typing import Any

ROOT = Path(r"D:\SurgeApp")
LOGS = ROOT / "logs"
MEMORY = ROOT / "memory"
NO_WIN = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
ICON_PATH = ROOT / "Luna_Command_Center.ico"
if not ICON_PATH.exists():
    ICON_PATH = ROOT / "Luna.ico"
KILL_SWITCH = ROOT / "LUNA_STOP_NOW.flag"
STARTUP_STATUS = LOGS / "luna_one_click_startup.json"

# ── Environment ────────────────────────────────────────────────────────────────
ENV = {
    **os.environ,
    "LUNA_PROJECT_DIR": str(ROOT),
    "LUNA_ICON_PATH": str(ICON_PATH),
    "OLLAMA_API_BASE": os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434"),
    "LUNA_INSTRUCTOR_MODEL": os.environ.get(
        "LUNA_INSTRUCTOR_MODEL", "ollama_chat/qwen2.5-coder:7b-instruct"
    ),
}

# ── Python executables ─────────────────────────────────────────────────────────
# Use the real Python binary (not the 0-byte WindowsApps stub).
# The stub can't be used as a shortcut target reliably.
REAL_PYTHONW = Path(
    r"C:\Program Files\WindowsApps"
    r"\PythonSoftwareFoundation.Python.3.11_3.11.2544.0_x64__qbz5n2kfra8p0"
    r"\pythonw3.11.exe"
)
# Fallback: use whichever interpreter is running this script
_self_exe = Path(sys.executable)
_self_w = _self_exe.parent / "pythonw.exe"
PYEXE = str(REAL_PYTHONW) if REAL_PYTHONW.exists() else (
    str(_self_w) if _self_w.exists() and _self_w.stat().st_size > 0 else str(_self_exe)
)

AIDER_PY = str(ROOT / ".aider_venv" / "Scripts" / "python.exe")
WORKER_PY = str(ROOT / ".aider_venv" / "Scripts" / "pythonw.exe")
if not Path(WORKER_PY).exists():
    WORKER_PY = AIDER_PY

SERVICE_LOCKS = {
    "luna_apprentice.py": LOGS / "luna_apprentice.pid.json",
    "worker.py": LOGS / "luna_worker.lock.json",
    "aider_bridge.py": LOGS / "aider_bridge.pid",
    "luna_start.pyw": LOGS / "luna_tray.pid.json",
    "luna_guardian.py": MEMORY / "luna_guardian.lock.json",
    "SurgeApp_Claude_Terminal.py": LOGS / "luna_terminal.pid.json",
    "--continues-update-start": MEMORY / "cu_loop.lock.json",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def ensure_dirs() -> None:
    """Create required directory tree if missing."""
    for sub in [
        "logs", "memory",
        "tasks/active", "tasks/done", "tasks/failed",
        "solutions", "logic_updates", "backups",
        "aider_jobs/active", "aider_jobs/done", "aider_jobs/failed",
    ]:
        (ROOT / sub).mkdir(parents=True, exist_ok=True)


def _read_pid_lock(lock: Path) -> int:
    try:
        if not lock.exists():
            return 0
        text = lock.read_text(encoding="utf-8", errors="replace").strip()
        if not text:
            return 0
        if text.startswith("{"):
            data: Any = json.loads(text)
            return int(data.get("pid", 0) or 0) if isinstance(data, dict) else 0
        return int(text)
    except Exception:
        return 0


def _process_command_line_for_pid(pid: int) -> str:
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
            creationflags=NO_WIN,
        )
        if result.returncode == 0:
            return (result.stdout or "").strip()
    except Exception:
        pass
    return ""


def _pid_alive(pid: int, marker: str = "") -> bool:
    if pid <= 0:
        return False
    if marker:
        return marker.lower() in _process_command_line_for_pid(pid).lower()
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            creationflags=NO_WIN,
        )
        return str(pid) in (result.stdout or "")
    except Exception:
        return False


def _lock_marker(lock: Path) -> str:
    try:
        for service, service_lock in SERVICE_LOCKS.items():
            if service_lock == lock:
                return service
    except Exception:
        pass
    return ""


def _lock_alive(lock: Path) -> bool:
    return _pid_alive(_read_pid_lock(lock), _lock_marker(lock))


def _archive_path(path: Path, reason: str) -> Path | None:
    """Preserve stale runtime files instead of deleting them."""
    try:
        if not path.exists():
            return None
        archive_dir = MEMORY / "disabled_flags" / "one_click_launcher"
        archive_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        dest = archive_dir / f"{path.name}.{reason}.{stamp}"
        path.replace(dest)
        return dest
    except Exception:
        return None


def _write_pid_lock(lock: Path, pid: int, service: str) -> None:
    try:
        lock.parent.mkdir(parents=True, exist_ok=True)
        if lock.suffix == ".pid":
            lock.write_text(str(pid), encoding="utf-8")
        else:
            lock.write_text(
                json.dumps({"pid": pid, "service": service, "started_at": time.strftime("%Y-%m-%dT%H:%M:%S")}, indent=2),
                encoding="utf-8",
            )
    except Exception:
        pass


def clear_stale_lock() -> None:
    """Archive stale PID locks so the launcher can safely start missing services."""
    for lock in SERVICE_LOCKS.values():
        if not lock.exists():
            continue
        try:
            if not _lock_alive(lock):
                _archive_path(lock, "stale")
        except Exception:
            pass


def _process_command_lines() -> list[str]:
    """Return process command lines using PowerShell CIM, falling back safely."""
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "Get-CimInstance Win32_Process | "
                "Where-Object { $_.Name -match '^python' -and $_.CommandLine -match 'SurgeApp|Luna|aider|worker|guardian' } | "
                "ForEach-Object { $_.CommandLine }",
            ],
            capture_output=True, text=True, timeout=10,
            creationflags=NO_WIN,
        )
        return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    except Exception:
        return []


def is_running(script_name: str, *, exclude: str = "") -> bool:
    """Return True if a process with script_name in its command line exists."""
    marker = script_name.lower()
    excluded = exclude.lower()
    for command in _process_command_lines():
        lower = command.lower()
        if marker in lower and (not excluded or excluded not in lower):
            return True
    return False


def start_hidden(exe: str, script: str, extra_args: list[str] | None = None) -> int:
    """Launch exe with script as arg, hidden window, no console."""
    args = [exe, script]
    if extra_args:
        args.extend(extra_args)
    proc = subprocess.Popen(
        args,
        cwd=str(ROOT),
        env=ENV,
        creationflags=NO_WIN,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    return int(proc.pid)


def start_if_missing(script_rel: str, exe: str | None = None,
                     extra_args: list[str] | None = None) -> bool:
    """Start script only if not already running. Returns True if started."""
    script_path = ROOT / script_rel
    if not script_path.exists():
        return False
    command_marker = script_rel
    if extra_args:
        command_marker = " ".join([script_rel, *extra_args])
    if is_running(command_marker):
        return False
    lock = SERVICE_LOCKS.get(script_rel)
    if lock and _lock_alive(lock):
        return False
    pid = start_hidden(exe or PYEXE, str(script_path), extra_args)
    if lock and script_rel != "aider_bridge.py":
        _write_pid_lock(lock, pid, script_rel)
    return True


def ensure_ollama() -> None:
    """Start Ollama if it is not responding."""
    import urllib.request
    base = ENV.get("OLLAMA_API_BASE", "http://127.0.0.1:11434")
    try:
        urllib.request.urlopen(base + "/api/tags", timeout=2)
        return  # already up
    except Exception:
        pass
    # Try to start it
    try:
        subprocess.Popen(
            ["ollama", "serve"],
            creationflags=NO_WIN,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return
    # Wait up to 15 s
    for _ in range(15):
        try:
            urllib.request.urlopen(base + "/api/tags", timeout=2)
            return
        except Exception:
            time.sleep(1)


def open_terminal() -> None:
    """Open the Luna terminal window (visible, not hidden)."""
    script = ROOT / "SurgeApp_Claude_Terminal.py"
    if not script.exists():
        return
    if is_running("SurgeApp_Claude_Terminal.py"):
        return
    terminal_lock = SERVICE_LOCKS["SurgeApp_Claude_Terminal.py"]
    if _lock_alive(terminal_lock):
        return
    # Use Popen without CREATE_NO_WINDOW so Qt can show its window normally
    proc = subprocess.Popen(
        [PYEXE, str(script)],
        cwd=str(ROOT),
        env=ENV,
        stdin=subprocess.DEVNULL,
        # stdout/stderr intentionally not redirected so Qt can own the display
    )
    _write_pid_lock(terminal_lock, int(proc.pid), "SurgeApp_Claude_Terminal.py")


def write_startup_status(events: list[dict[str, Any]]) -> None:
    try:
        LOGS.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "launcher": str(ROOT / "LaunchLuna.pyw"),
            "desktop_shortcut": r"D:\OneDrive\Desktop\Luna Command Center.lnk",
            "kill_switch_present": KILL_SWITCH.exists(),
            "events": events,
        }
        STARTUP_STATUS.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    except Exception:
        pass


def _cu_quality_gate_paused() -> bool:
    """Return True when CU should not be auto-restarted by one-click launch."""
    try:
        state_path = MEMORY / "continues_update_state.json"
        if not state_path.exists():
            return False
        state = json.loads(state_path.read_text(encoding="utf-8", errors="replace") or "{}")
        last_status = str(state.get("last_status") or "").lower()
        noop_count = int(state.get("noop_count", 0) or 0)
        return last_status == "noop" and noop_count >= 5
    except Exception:
        return False


def _worker_import_ready() -> tuple[bool, str]:
    worker_path = ROOT / "worker.py"
    if not worker_path.exists():
        return False, "worker_missing"
    try:
        result = subprocess.run(
            [AIDER_PY, "-c", "import worker; print('IMPORT_OK')"],
            cwd=str(ROOT),
            env=ENV,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            creationflags=NO_WIN,
        )
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if result.returncode == 0 and "IMPORT_OK" in stdout:
            return True, "IMPORT_OK"
        return False, stderr or stdout or f"rc={result.returncode}"
    except Exception as exc:
        return False, str(exc)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    events: list[dict[str, Any]] = []
    ensure_dirs()
    clear_stale_lock()
    if KILL_SWITCH.exists():
        # Auto-expire stale kill switches (older than 24 h) so a forgotten
        # manual shutdown can't silently block Luna forever.
        try:
            age_s = time.time() - KILL_SWITCH.stat().st_mtime
        except Exception:
            age_s = 0
        if age_s > 86400:
            _archive_path(KILL_SWITCH, "auto_expired")
            events.append({"service": "autonomy", "action": "kill_switch_auto_expired",
                           "age_hours": round(age_s / 3600, 1)})
        else:
            events.append({"service": "autonomy", "action": "blocked", "reason": "kill_switch_present"})
            # Write status first so the terminal can show the blocked state.
            write_startup_status(events)
            open_terminal()
            return
    # Show the monitor first; slower background services can warm up behind it.
    open_terminal()
    events.append({"service": "SurgeApp_Claude_Terminal.py", "action": "opened_early_or_already_running"})
    write_startup_status(events)

    ensure_ollama()
    events.append({"service": "ollama", "action": "ensured"})

    # Background services (hidden, start only if not already running). Guardian
    # starts last so it does not race the launcher and duplicate services.
    events.append({"service": "luna_apprentice.py", "action": "started" if start_if_missing("luna_apprentice.py") else "already_running_or_missing"})
    events.append({"service": "worker.py", "action": "started" if start_if_missing("worker.py", exe=WORKER_PY) else "already_running_or_missing"})

    # Aider bridge uses .aider_venv Python (has aider installed)
    aider_py_path = ROOT / ".aider_venv" / "Scripts" / "python.exe"
    if aider_py_path.exists():
        aider_started = start_if_missing("aider_bridge.py", exe=str(aider_py_path))
    else:
        aider_started = start_if_missing("aider_bridge.py")
    events.append({"service": "aider_bridge.py", "action": "started" if aider_started else "already_running_or_missing"})

    # Tray icon (luna_start.pyw --tray-only)
    events.append({"service": "luna_start.pyw --tray-only", "action": "started" if start_if_missing("luna_start.pyw", extra_args=["--tray-only"]) else "already_running_or_missing"})

    worker_import_ok, worker_import_detail = _worker_import_ready()
    events.append({
        "service": "worker_import",
        "action": "ok" if worker_import_ok else "blocked",
        "detail": worker_import_detail[:300],
    })

    # Continues-update loop (unique flag so it's not confused with worker.py)
    cu_lock = SERVICE_LOCKS["--continues-update-start"]
    if _cu_quality_gate_paused():
        _archive_path(cu_lock, "quality_gate_paused")
        events.append({
            "service": "continues_update",
            "action": "paused_quality_gate",
            "reason": "noop_budget_exhausted",
        })
    elif not worker_import_ok:
        _archive_path(cu_lock, "worker_import_blocked")
        events.append({
            "service": "continues_update",
            "action": "blocked_worker_import",
            "reason": worker_import_detail[:300],
        })
    elif (ROOT / "worker.py").exists() and not _lock_alive(cu_lock) and not is_running("--continues-update-start"):
        _archive_path(MEMORY / "continues_update.stop", "one_click_resume")
        pid = start_hidden(WORKER_PY, str(ROOT / "worker.py"), ["--continues-update-start"])
        events.append({"service": "continues_update", "action": "started", "pid": pid})
    else:
        events.append({"service": "continues_update", "action": "already_running_or_missing"})

    events.append({"service": "luna_guardian.py", "action": "started" if start_if_missing("luna_guardian.py") else "already_running_or_missing"})

    write_startup_status(events)


if __name__ == "__main__":
    main()
