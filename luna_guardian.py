"""Luna Guardian — process watchdog for SurgeApp services.

Responsibilities:
  - Single-instance daemon (lock: memory/luna_guardian.lock.json)
  - Owns and auto-restarts aider_bridge.py (checks every BRIDGE_CHECK_EVERY ticks × TICK_SLEEP s)
  - Keeps the Aider model (Qwen) warm in VRAM — prevents 30-60 s cold-load penalty
  - Emits live_feed events so the terminal can show guardian health

Run: pythonw D:\\SurgeApp\\luna_guardian.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

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

# ── Ollama keep-warm ────────────────────────────────────────────────────────────
OLLAMA_BASE        = os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434")
# Model that aider_bridge uses — must stay warm so jobs start instantly (no 30-60 s reload)
AIDER_MODEL        = os.environ.get("LUNA_AIDER_MODEL", "qwen2.5-coder:7b-instruct")
# How long Ollama should keep the model loaded after each ping (5 minutes rolling window)
KEEP_ALIVE_WINDOW  = "5m"
# Ping every N ticks: 24 × 5 s = 120 s = 2 min (well inside the 5 min window)
WARM_CHECK_EVERY   = 24

# ── Timing ─────────────────────────────────────────────────────────────────────
TICK_SLEEP         = 5.0   # seconds per guardian tick
BRIDGE_CHECK_EVERY = 6     # check bridge every 6 ticks = 30 s

# ── Helpers ────────────────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    """Append a timestamped line to the guardian log file."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [GUARDIAN] {msg}\n"
    try:
        GUARDIAN_LOG.parent.mkdir(parents=True, exist_ok=True)
        with GUARDIAN_LOG.open("a", encoding="utf-8", errors="replace") as f:
            f.write(line)
    except Exception:
        pass


def _live_feed(event: str, msg: str, detail: str = "") -> None:
    """Emit one JSON line to the live feed so the terminal Inspector can show it."""
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
    """Return True if the given PID is still running (tasklist-based — avoids ctypes 64-bit HANDLE truncation)."""
    if pid <= 0:
        return False
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, timeout=5,
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
    """Remove the guardian lock file on exit."""
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


# ── Bridge management (Popen-based — no PID file races) ───────────────────────

_bridge_proc: Optional[subprocess.Popen] = None  # type: ignore[type-arg]


def bridge_running() -> bool:
    """Return True if a bridge is alive — checks both our Popen handle and the PID file.

    The venv pythonw launcher may exit after spawning its Windows interpreter child,
    so we fall back to the PID file (written by the actual interpreter) as ground truth.
    """
    global _bridge_proc
    # 1. Popen check — covers the case where we own the launcher
    popen_alive = False
    if _bridge_proc is not None:
        try:
            popen_alive = _bridge_proc.poll() is None
        except Exception:
            popen_alive = False
        if not popen_alive:
            _bridge_proc = None

    # 2. PID file check — written by the actual Python interpreter doing the work
    pid_file_alive = False
    pid_from_file = _read_pid_file(BRIDGE_PID_PATH)
    if pid_from_file:
        pid_file_alive = _pid_alive(pid_from_file)

    return popen_alive or pid_file_alive


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


# ── Ollama model keep-warm ─────────────────────────────────────────────────────

def _ollama_ps() -> list:
    """Return list of currently loaded models from Ollama /api/ps."""
    try:
        req = urllib.request.Request(
            f"{OLLAMA_BASE}/api/ps",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=4) as resp:
            data = json.loads(resp.read())
            return data.get("models") or []
    except Exception:
        return []


def _warm_aider_model() -> bool:
    """Ping Ollama to keep the aider model loaded in VRAM.

    Uses keep_alive to extend the TTL by KEEP_ALIVE_WINDOW each call.
    Returns True if the model is now warm, False on error.
    """
    payload = json.dumps({
        "model":      AIDER_MODEL,
        "prompt":     "",          # empty prompt — just loads/keeps alive the model
        "keep_alive": KEEP_ALIVE_WINDOW,
        "stream":     False,
    }).encode()
    try:
        req = urllib.request.Request(
            f"{OLLAMA_BASE}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=90) as resp:
            resp.read()  # drain
        return True
    except Exception as exc:
        _log(f"Model warm-ping failed: {exc}")
        return False


def _check_and_warm_model(state: Dict[str, Any]) -> None:
    """Check if the aider model is loaded; warm it if not. Emit live_feed events."""
    loaded_models = _ollama_ps()
    loaded_names  = [m.get("name", "") for m in loaded_models]
    is_warm       = any(AIDER_MODEL in name for name in loaded_names)

    if is_warm:
        # Already warm — just extend the keep_alive TTL silently
        _warm_aider_model()
        state["_model_warm"] = True
    else:
        # Cold — need to load (takes ~30-60 s on first call)
        _log(f"Model {AIDER_MODEL!r} is cold — warming now…")
        _live_feed("MODEL_WARM_START", f"Warming {AIDER_MODEL}",
                   detail="model was cold — loading into VRAM")
        ok = _warm_aider_model()
        if ok:
            vram_mb = 0
            for m in _ollama_ps():
                if AIDER_MODEL in m.get("name", ""):
                    vram_mb = m.get("size_vram", 0) // 1024 // 1024
            _log(f"Model warm — {vram_mb} MB in VRAM")
            _live_feed("MODEL_WARM_DONE", f"{AIDER_MODEL} loaded",
                       detail=f"{vram_mb} MB VRAM")
            state["_model_warm"] = True
        else:
            state["_model_warm"] = False


# ── Guardian iteration (called every tick) ─────────────────────────────────────

def guardian_iteration(state: Dict[str, Any]) -> None:
    """Run one guardian tick: check bridge health and keep Ollama model warm."""
    # ── Bridge health check (every 30 s) ──────────────────────────────────────
    bridge_tick = int(state.get("_bridge_tick", 0) or 0) + 1
    state["_bridge_tick"] = bridge_tick
    if bridge_tick >= BRIDGE_CHECK_EVERY:
        state["_bridge_tick"] = 0
        try:
            if not bridge_running():
                # Cooldown: don't restart more than once per 60 s to avoid loops
                last_restart = float(state.get("_last_bridge_restart", 0) or 0)
                if time.time() - last_restart >= 60.0:
                    _log("Bridge not running — restarting…")
                    _live_feed("BRIDGE_RESTART", "Bridge dead — restarting")
                    launch_bridge()
                    state["_last_bridge_restart"] = time.time()
        except Exception as exc:
            _log(f"Bridge check error: {exc}")

    # ── Ollama keep-warm (every 2 min) ────────────────────────────────────────
    warm_tick = int(state.get("_warm_tick", 0) or 0) + 1
    state["_warm_tick"] = warm_tick
    if warm_tick >= WARM_CHECK_EVERY:
        state["_warm_tick"] = 0
        try:
            _check_and_warm_model(state)
        except Exception as exc:
            _log(f"Model warm check error: {exc}")


def tray_running() -> bool:
    """Stub — no tray icon in this deployment."""
    return True


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> int:
    """Guardian entry point — acquire lock, launch bridge, warm model, then loop."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)

    if not _acquire_lock():
        return 0  # another guardian is alive — exit quietly

    import atexit
    atexit.register(_release_lock)

    _log("Guardian started.")
    _live_feed("GUARDIAN_START", "Luna Guardian started", detail=f"pid={os.getpid()}")

    # Launch bridge immediately — guardian owns the single bridge instance
    launch_bridge()

    # Warm the aider model immediately so the first CU cycle doesn't pay cold-load penalty
    _log(f"Warming aider model {AIDER_MODEL!r} on startup…")
    try:
        _check_and_warm_model({})
    except Exception as exc:
        _log(f"Startup warm failed (non-fatal): {exc}")

    state: Dict[str, Any] = {}
    while True:
        try:
            guardian_iteration(state)
        except Exception as exc:
            _log(f"Iteration error: {exc}")
        time.sleep(TICK_SLEEP)


if __name__ == "__main__":
    raise SystemExit(main())
