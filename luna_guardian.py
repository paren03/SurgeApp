"""Luna Guardian - service health and duplicate-process prevention.

This file was restored after the on-disk guardian became empty. The guardian is
intentionally conservative: it does not delete queues, logs, memory, backups, or
staged edits. It only starts missing core services and records what it does.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Dict, List


PROJECT_DIR = Path(os.environ.get("LUNA_PROJECT_DIR", r"D:\SurgeApp"))
LOGS_DIR = PROJECT_DIR / "logs"
MEMORY_DIR = PROJECT_DIR / "memory"
LIVE_FEED_PATH = LOGS_DIR / "luna_live_feed.jsonl"
GUARDIAN_LOCK_PATH = MEMORY_DIR / "luna_guardian.lock.json"
GUARDIAN_STATUS_PATH = MEMORY_DIR / "luna_guardian_status.json"
KILL_SWITCH_PATH = PROJECT_DIR / "LUNA_STOP_NOW.flag"
NO_WIN = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0

OLLAMA_EXE = Path(os.environ.get(
    "OLLAMA_EXE",
    r"C:\Users\paren\AppData\Local\Programs\Ollama\ollama.exe",
))
OLLAMA_API_BASE = os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434").rstrip("/")

# Mutable state for Ollama recovery — avoids global keyword in functions.
_OLLAMA: Dict[str, object] = {"fail_count": 0, "cooldown_until": 0.0}

PYTHONW = Path(
    r"C:\Program Files\WindowsApps"
    r"\PythonSoftwareFoundation.Python.3.11_3.11.2544.0_x64__qbz5n2kfra8p0"
    r"\pythonw3.11.exe"
)
AIDER_PYTHON = PROJECT_DIR / ".aider_venv" / "Scripts" / "python.exe"
WORKER_PYTHONW = PROJECT_DIR / ".aider_venv" / "Scripts" / "pythonw.exe"

SERVICE_SCRIPTS = {
    "worker": "worker.py",
    "aider_bridge": "aider_bridge.py",
    # Terminal is opened by LaunchLuna.pyw only — guardian must not respawn it
    # because each respawn creates an unwanted visible popup window.
}

SERVICE_LOCKS = {
    "worker": LOGS_DIR / "luna_worker.lock.json",
    "aider_bridge": LOGS_DIR / "aider_bridge.pid",
    "terminal": LOGS_DIR / "luna_terminal.pid.json",
}


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _log(message: str) -> None:
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [GUARDIAN] {message}"
        with (LOGS_DIR / "luna_guardian.log").open("a", encoding="utf-8", errors="replace") as handle:
            handle.write(line + "\n")
    except Exception:
        pass


def _feed(event: str, message: str, detail: str = "") -> None:
    try:
        row = {
            "ts": datetime.now().strftime("%H:%M:%S"),
            "role": "guardian",
            "event": event,
            "msg": message,
            "detail": detail,
            "source": "luna_guardian",
        }
        LIVE_FEED_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LIVE_FEED_PATH.open("a", encoding="utf-8", errors="replace") as handle:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")
    except Exception:
        pass


def _process_rows() -> List[Dict[str, str]]:
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "Get-CimInstance Win32_Process | "
                "Where-Object { $_.Name -match '^python' -and $_.CommandLine -match 'SurgeApp|Luna|aider|worker|guardian' } | "
                "Select-Object ProcessId,ParentProcessId,Name,CommandLine | ConvertTo-Json -Compress",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            creationflags=NO_WIN,
        )
    except Exception:
        return []
    try:
        parsed = json.loads(result.stdout or "[]")
    except Exception:
        return []
    if isinstance(parsed, dict):
        parsed = [parsed]
    rows: List[Dict[str, str]] = []
    for item in parsed if isinstance(parsed, list) else []:
        if isinstance(item, dict):
            rows.append({
                "command": str(item.get("CommandLine") or ""),
                "name": str(item.get("Name") or ""),
                "parent_pid": str(item.get("ParentProcessId") or ""),
                "pid": str(item.get("ProcessId") or ""),
            })
    return rows


def _matching_processes(script_marker: str, *, exclude: str = "") -> List[Dict[str, str]]:
    marker = script_marker.lower()
    excluded = exclude.lower()
    return [
        row for row in _process_rows()
        if _command_invokes_script(row.get("command") or "", marker)
        and (not excluded or excluded not in (row.get("command") or "").lower())
        and str(os.getpid()) != str(row.get("pid") or "")
    ]


def _command_invokes_script(command: str, script_marker: str) -> bool:
    """Return True only when command launches the service script itself."""
    marker = re.escape(str(script_marker or "").lower())
    if not marker:
        return False
    normalized = str(command or "").lower().replace("/", "\\")
    pattern = (
        r'(?:^|\s)"?[^"\s]*python[\w.]*\.exe"?\s+'
        r'"?[^"\s]*\\?' + marker + r'(?:"|\s|$)'
    )
    return bool(re.search(pattern, normalized))


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
        return _command_invokes_script(_process_command_line_for_pid(pid), marker)
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


def _read_pid_lock(lock: Path) -> int:
    try:
        if not lock.exists():
            return 0
        text = lock.read_text(encoding="utf-8", errors="replace").strip()
        if not text:
            return 0
        if text.startswith("{"):
            data = json.loads(text)
            return int(data.get("pid", 0) or 0) if isinstance(data, dict) else 0
        return int(text)
    except Exception:
        return 0


def _lock_alive(lock: Path) -> bool:
    marker = ""
    try:
        for service, service_lock in SERVICE_LOCKS.items():
            if service_lock == lock:
                marker = SERVICE_SCRIPTS.get(service, "").split()[0]
                break
    except Exception:
        marker = ""
    return _pid_alive(_read_pid_lock(lock), marker)


def _write_pid_lock(lock: Path, pid: int, service: str) -> None:
    try:
        lock.parent.mkdir(parents=True, exist_ok=True)
        if lock.suffix == ".pid":
            lock.write_text(str(pid), encoding="utf-8")
        else:
            lock.write_text(
                json.dumps({"pid": pid, "service": service, "started_at": _now_iso()}, indent=2),
                encoding="utf-8",
            )
    except Exception:
        pass


def _terminate_pid(pid: int, reason: str) -> bool:
    if pid <= 0 or pid == os.getpid():
        return False
    try:
        result = subprocess.run(
            ["taskkill", "/F", "/PID", str(pid)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
            creationflags=NO_WIN,
        )
        ok = result.returncode == 0
        _log(f"terminated duplicate pid={pid} reason={reason} ok={ok}")
        if ok:
            _feed("GUARDIAN_DUPLICATE_STOPPED", "stopped duplicate service process", f"pid={pid} reason={reason}")
        return ok
    except Exception as exc:
        _log(f"failed to terminate duplicate pid={pid}: {exc}")
        return False


def _dedupe_service_processes(service_name: str, script_spec: str) -> List[int]:
    marker = script_spec.split()[0]
    exclude = "--continues-update-start" if service_name == "worker" else "luna_guardian.py"
    rows = _matching_processes(marker, exclude=exclude)
    if len(rows) <= 1:
        return []
    lock = SERVICE_LOCKS.get(service_name)
    lock_pid = _read_pid_lock(lock) if lock else 0
    row_pids: List[int] = []
    for row in rows:
        try:
            pid = int(row.get("pid") or 0)
        except Exception:
            pid = 0
        if pid > 0 and pid not in row_pids:
            row_pids.append(pid)
    if len(row_pids) <= 1:
        return []
    parent_by_pid: Dict[int, int] = {}
    for row in rows:
        try:
            pid = int(row.get("pid") or 0)
            parent_by_pid[pid] = int(row.get("parent_pid") or 0)
        except Exception as exc:
            _log(f"swallowed: {exc}")
    child_pids = [pid for pid, parent_pid in parent_by_pid.items() if parent_pid in row_pids]
    # Some Windows Python launchers spawn a child interpreter that still shows the
    # same script in its command line. Treat that parent/child pair as one service.
    if len(row_pids) == 2 and len(child_pids) == 1:
        if lock:
            _write_pid_lock(lock, child_pids[0], service_name)
        return []
    keep_pid = lock_pid if lock_pid in row_pids and _pid_alive(lock_pid, marker) else row_pids[0]
    stopped: List[int] = []
    for pid in row_pids:
        if pid == keep_pid:
            continue
        if _terminate_pid(pid, f"duplicate_{service_name}"):
            stopped.append(pid)
    if lock and keep_pid > 0:
        _write_pid_lock(lock, keep_pid, service_name)
    return stopped


def _service_pid(service_name: str, script_spec: str) -> int:
    marker = script_spec.split()[0]
    lock = SERVICE_LOCKS.get(service_name)
    if lock:
        pid = _read_pid_lock(lock)
        if _pid_alive(pid, marker):
            return pid
    exclude = "--continues-update-start" if service_name == "worker" else "luna_guardian.py"
    running = _matching_processes(marker, exclude=exclude)
    if running:
        try:
            return int(running[0].get("pid") or 0)
        except Exception:
            return 0
    return 0


def _write_status(results: Dict[str, bool]) -> None:
    services: Dict[str, Dict[str, object]] = {}
    for service_name, script_spec in SERVICE_SCRIPTS.items():
        pid = _service_pid(service_name, script_spec)
        services[service_name] = {
            "running": bool(pid),
            "pid": pid,
            "started_this_tick": bool(results.get(service_name)),
            "script": script_spec.split()[0],
        }
    all_running = all(item["running"] for item in services.values()) if services else False
    ollama_alive = _ollama_port_alive(timeout=2.0)
    payload = {
        "ts": _now_iso(),
        "guardian_pid": os.getpid(),
        "kill_switch_present": KILL_SWITCH_PATH.exists(),
        "status": "services_healthy" if (all_running and ollama_alive) else "service_attention_needed",
        "services": services,
        "ollama": {
            "port_alive": ollama_alive,
            "restarted_this_tick": bool(results.get("ollama")),
            "fail_count": int(_OLLAMA["fail_count"]),  # type: ignore[arg-type]
        },
    }
    try:
        GUARDIAN_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        GUARDIAN_STATUS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


def acquire_guardian_lock() -> bool:
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    if GUARDIAN_LOCK_PATH.exists():
        try:
            existing = json.loads(GUARDIAN_LOCK_PATH.read_text(encoding="utf-8", errors="replace") or "{}")
            pid = int(existing.get("pid", 0) or 0)
            if pid and pid != os.getpid() and _pid_alive(pid, "luna_guardian.py"):
                _log(f"another guardian already running pid={pid}; exiting")
                return False
        except Exception:
            pass
    GUARDIAN_LOCK_PATH.write_text(
        json.dumps({"pid": os.getpid(), "started_at": _now_iso()}, indent=2),
        encoding="utf-8",
    )
    return True


def _pythonw_for(service_name: str) -> str:
    if service_name == "aider_bridge" and AIDER_PYTHON.exists():
        return str(AIDER_PYTHON)
    if service_name == "worker":
        if WORKER_PYTHONW.exists():
            return str(WORKER_PYTHONW)
        if AIDER_PYTHON.exists():
            return str(AIDER_PYTHON)
    return str(PYTHONW if PYTHONW.exists() else sys.executable)


def _start_service(service_name: str, script_spec: str) -> bool:
    marker = script_spec.split()[0]
    _dedupe_service_processes(service_name, script_spec)
    lock = SERVICE_LOCKS.get(service_name)
    if lock and _lock_alive(lock):
        _log(f"{service_name} already running pid={_read_pid_lock(lock)}")
        return False
    exclude = "--continues-update-start" if service_name == "worker" else "luna_guardian.py"
    running = _matching_processes(marker, exclude=exclude)
    if running:
        pid = int(running[0].get("pid") or 0)
        if lock and pid > 0:
            _write_pid_lock(lock, pid, service_name)
        _log(f"{service_name} already running via process scan pid={pid}")
        return False
    script_path = PROJECT_DIR / marker
    if not script_path.exists() or script_path.stat().st_size == 0:
        _log(f"{service_name} missing or empty: {script_path}")
        _feed("GUARDIAN_SERVICE_MISSING", f"{service_name} missing or empty", str(script_path))
        return False
    args = [_pythonw_for(service_name), str(script_path)]
    if "--tray-only" in script_spec:
        args.append("--tray-only")
    proc = subprocess.Popen(
        args,
        cwd=str(PROJECT_DIR),
        creationflags=NO_WIN,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    if lock and service_name != "aider_bridge":
        _write_pid_lock(lock, int(proc.pid), service_name)
    _log(f"launched {service_name}: {' '.join(args)}")
    _feed("GUARDIAN_SERVICE_STARTED", f"started {service_name}", " ".join(args))
    return True


def _ollama_port_alive(timeout: float = 3.0) -> bool:
    try:
        with urllib.request.urlopen(f"{OLLAMA_API_BASE}/api/tags", timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def _recover_ollama() -> bool:
    """Start `ollama serve` if the port is down for two consecutive ticks.

    Returns True if a restart was attempted this tick.
    Enforces a 120-second cooldown between restarts to prevent thrash.
    """
    if _ollama_port_alive():
        _OLLAMA["fail_count"] = 0
        return False

    _OLLAMA["fail_count"] = int(_OLLAMA["fail_count"]) + 1  # type: ignore[arg-type]
    if int(_OLLAMA["fail_count"]) < 2:
        # Wait one more tick before acting — avoids false positives during slow start
        _log("Ollama port 11434 did not respond (strike 1 of 2); waiting")
        return False

    now = time.time()
    if now < float(_OLLAMA["cooldown_until"]):  # type: ignore[arg-type]
        _log("Ollama down but still in restart cooldown; waiting")
        return False

    if not OLLAMA_EXE.exists():
        _log(f"ollama.exe not found at {OLLAMA_EXE}; cannot recover")
        _feed("OLLAMA_MISSING", "ollama.exe not found", str(OLLAMA_EXE))
        return False

    # If the process exists but the port is dead, killing and re-launching may
    # cause data loss.  Log the anomaly and leave it for the user to resolve.
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq ollama.exe", "/NH"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=5, creationflags=NO_WIN,
        )
        if "ollama.exe" in (result.stdout or "").lower():
            _log("ollama.exe running but port unresponsive — skipping forced restart")
            _feed("OLLAMA_UNRESPONSIVE", "ollama process alive but port dead; manual check needed", "")
            _OLLAMA["cooldown_until"] = now + 60
            return False
    except Exception:
        pass

    _log(f"Ollama port 11434 down (strike {_OLLAMA['fail_count']}); launching ollama serve")
    _feed("OLLAMA_RECOVER", "Ollama port down; launching ollama serve", str(OLLAMA_EXE))
    try:
        subprocess.Popen(
            [str(OLLAMA_EXE), "serve"],
            cwd=str(PROJECT_DIR),
            creationflags=NO_WIN,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        _OLLAMA["fail_count"] = 0
        _OLLAMA["cooldown_until"] = now + 120
        return True
    except Exception as exc:
        _log(f"Failed to launch ollama serve: {exc}")
        _feed("OLLAMA_RECOVER_FAIL", "ollama serve launch failed", str(exc))
        _OLLAMA["cooldown_until"] = now + 60
        return False


def check_services_once() -> Dict[str, bool]:
    results: Dict[str, bool] = {}
    if KILL_SWITCH_PATH.exists():
        _log("kill switch present; not starting services")
        _feed("GUARDIAN_PAUSED", "kill switch present; not starting services", str(KILL_SWITCH_PATH))
        _write_status(results)
        return results
    for service_name, script_spec in SERVICE_SCRIPTS.items():
        results[service_name] = _start_service(service_name, script_spec)
    results["ollama"] = _recover_ollama()
    _write_status(results)
    return results


def main() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    if not acquire_guardian_lock():
        return
    _log("Guardian started")
    _feed("GUARDIAN_START", "Guardian started")
    while not KILL_SWITCH_PATH.exists():
        check_services_once()
        time.sleep(30)
    _log("Guardian stopped by kill switch")
    _feed("GUARDIAN_STOP", "Guardian stopped by kill switch", str(KILL_SWITCH_PATH))


if __name__ == "__main__":
    main()
