"""Luna Guardian — process watchdog for SurgeApp services.

Responsibilities:
  - Single-instance daemon (lock: logs/luna_guardian.lock.json)
  - Owns and auto-restarts aider_bridge.py (checks every BRIDGE_CHECK_EVERY ticks × TICK_SLEEP s)
  - Emits live_feed events so the terminal can show guardian health

Run: pythonw D:\\SurgeApp\\luna_guardian.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_DIR     = Path(os.environ.get("LUNA_PROJECT_DIR", r"D:\SurgeApp"))
LOGS_DIR        = PROJECT_DIR / "logs"
MEMORY_DIR      = PROJECT_DIR / "memory"
LIVE_FEED_PATH  = LOGS_DIR / "luna_live_feed.jsonl"
GUARDIAN_LOG    = LOGS_DIR / "luna_guardian.log"
LOCK_PATH       = MEMORY_DIR / "luna_guardian.lock.json"

BRIDGE_PATH     = PROJECT_DIR / "aider_bridge.py"
BRIDGE_PID_PATH = LOGS_DIR / "aider_bridge.pid"
AIDER_VENV_PY   = PROJECT_DIR / ".aider_venv" / "Scripts" / "pythonw.exe"

# ── Timing ─────────────────────────────────────────────────────────────────────
TICK_SLEEP        = 5.0   # seconds per guardian tick
BRIDGE_CHECK_EVERY = 6    # check bridge every 6 ticks = 30 s

# ── Helpers ────────────────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [GUARDIAN] {msg}\n"
    try:
        GUARDIAN_LOG.parent.mkdir(parents=True, exist_ok=True)
        with GUARDIAN_LOG.open("a", encoding="utf-8", errors="replace") as f:
            f.write(line)
    except Exception:
        pass


def _live_feed(event: str, msg: str, detail: str = "") -> None:
    try:
        row: Dict[str, Any] = {
            "ts":     datetime.now().strftime("%H:%M:%S"),
            "event":  event,
            "icon":   "[GRD]   ",
            "msg":    str(msg or "")[:240],
            "source": "luna_guardian",
        }
        if detail:
            row["detail"] = str(detail)[:400]
        LIVE_FEED_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LIVE_FEED_PATH.open("a", encoding="utf-8", errors="replace") as f:
            json.dump(row, f, ensure_ascii=True)
            f.write("\n")
    except Exception:
        pass


def _pid_alive(pid: int) -> bool:
    """Return True if the given PID is still running on Windows (or Unix)."""
    if pid <= 0:
        return False
    # Simplest reliable check: use psutil-free approach via tasklist
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, timeout=5,
            creationflags=0x08000000,
        )
        return str(pid) in (result.stdout or "")
    except Exception:
        pass
    # Fallback: ctypes with correct 64-bit HANDLE type
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.OpenProcess.restype = ctypes.c_void_p  # must be void_p not c_long!
        handle = kernel32.OpenProcess(0x0400, False, pid)
        if not handle:
            return False
        code = ctypes.c_ulong(0)
        kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        kernel32.CloseHandle(handle)
        return code.value == 259  # STILL_ACTIVE
    except Exception:
        pass
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _read_pid_file(path: Path) -> int:
    """Read a plain-text PID file. Returns 0 on any error."""
    try:
        if path.exists():
            return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        pass
    return 0


# ── Single-instance lock ───────────────────────────────────────────────────────

def _acquire_lock() -> bool:
    """Write our PID to the guardian lock. Returns False if another instance is alive."""
    my_pid = os.getpid()
    try:
        LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        if LOCK_PATH.exists():
            try:
                data = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
                existing_pid = int(data.get("pid") or 0)
                if existing_pid and existing_pid != my_pid and _pid_alive(existing_pid):
                    return False  # another guardian is already running
            except Exception:
                pass  # corrupt lock — overwrite it
        LOCK_PATH.write_text(
            json.dumps({"pid": my_pid, "started": datetime.now().isoformat(timespec="seconds")}),
            encoding="utf-8",
        )
        return True
    except Exception:
        return True  # can't write lock but carry on


def _release_lock() -> None:
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


# ── Bridge management (Popen-based — no PID file races) ───────────────────────

_bridge_proc: "Optional[subprocess.Popen[bytes]]" = None  # type: ignore[type-arg]


def bridge_running() -> bool:
    """Return True if the bridge process guardian owns is still alive."""
    global _bridge_proc
    if _bridge_proc is None:
        return False
    try:
        if _bridge_proc.poll() is None:
            return True   # still running
        _bridge_proc = None
        return False
    except Exception:
        _bridge_proc = None
        return False


def launch_bridge() -> None:
    """Start aider_bridge.py using the .aider_venv Python. Stores the Popen handle."""
    global _bridge_proc
    if not BRIDGE_PATH.exists():
        _log("bridge path not found — skipping launch")
        return

    py_exe = str(AIDER_VENV_PY) if AIDER_VENV_PY.exists() else sys.executable
    try:
        _bridge_proc = subprocess.Popen(
            [py_exe, str(BRIDGE_PATH)],
            cwd=str(PROJECT_DIR),
            creationflags=0x08000000,  # CREATE_NO_WINDOW
            close_fds=True,
        )
        _log(f"Launched aider_bridge.py PID={_bridge_proc.pid} via {py_exe}")
        _live_feed("BRIDGE_START", "Guardian launched aider_bridge.py",
                   detail=f"pid={_bridge_proc.pid}")
    except Exception as exc:
        _log(f"Failed to launch bridge: {exc}")
        _bridge_proc = None


# ── Guardian iteration (called every tick) ─────────────────────────────────────

def guardian_iteration(state: Dict[str, Any]) -> None:
    bridge_tick = int(state.get("_bridge_tick", 0) or 0) + 1
    state["_bridge_tick"] = bridge_tick

    if bridge_tick >= BRIDGE_CHECK_EVERY:
        state["_bridge_tick"] = 0
        try:
            if not bridge_running():
                _log("Bridge not running — restarting…")
                launch_bridge()
            # else: bridge alive — no action, no log spam
        except Exception as exc:
            _log(f"Bridge check error: {exc}")


def tray_running() -> bool:
    """Stub — no tray icon in this deployment."""
    return True


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> int:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)

    if not _acquire_lock():
        # Another guardian is already alive — just exit quietly
        return 0

    import atexit
    atexit.register(_release_lock)

    _log("Guardian started.")
    _live_feed("GUARDIAN_START", "Luna Guardian started", detail=f"pid={os.getpid()}")

    # Launch bridge immediately (guardian owns the single bridge instance)
    launch_bridge()

    state: Dict[str, Any] = {}
    while True:
        try:
            guardian_iteration(state)
        except Exception as exc:
            _log(f"Iteration error: {exc}")
        time.sleep(TICK_SLEEP)


if __name__ == "__main__":
    raise SystemExit(main())
