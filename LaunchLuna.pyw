r"""LaunchLuna.pyw - One-click launcher for Luna Command Center.

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
import re
import subprocess
import sys
import threading
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

# Voice venv executables (separate from .aider_venv — has XTTS, torch,
# faster-whisper, sounddevice and all voice-pipeline deps).
VOICE_PY = str(ROOT / ".voice_venv" / "Scripts" / "python.exe")
VOICE_PYW = str(ROOT / ".voice_venv" / "Scripts" / "pythonw.exe")
if not Path(VOICE_PYW).exists():
    VOICE_PYW = VOICE_PY

SERVICE_LOCKS = {
    "luna_apprentice.py": LOGS / "luna_apprentice.pid.json",
    "worker.py": LOGS / "luna_worker.lock.json",
    "aider_bridge.py": LOGS / "aider_bridge.pid",
    "luna_start.pyw": LOGS / "luna_tray.pid.json",
    "luna_guardian.py": MEMORY / "luna_guardian.lock.json",
    "SurgeApp_Claude_Terminal.py": LOGS / "luna_terminal.pid.json",
    "--continues-update-start": MEMORY / "cu_loop.lock.json",
    # FastTalk voice pipeline (bilingual RU/EN clone voices)
    "luna_fast_stt_service.py": LOGS / "luna_fast_stt.pid.json",
    "luna_fast_tts_service.py": LOGS / "luna_fast_tts.pid.json",
    "luna_fasttalk_controller.py": LOGS / "luna_fasttalk_ctl.pid.json",
    "luna_fastbrain_router.py": LOGS / "luna_fastbrain.pid.json",
    "luna_fastbrain_llm_server.py": LOGS / "luna_fastbrain_llm.pid.json",
    "luna_fastbrain_bridge.py": LOGS / "luna_fastbridge.pid.json",
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
    """Return the command line for a specific PID.

    2026-05-16 popup root-cause fix (Codex H6): replaced the
    subprocess powershell.exe / Get-CimInstance call with a native
    psutil call. powershell.exe on Windows 11 allocates a conhost.exe
    even with CREATE_NO_WINDOW, producing the visible terminal flash
    the operator saw on every click. psutil is in-process, no spawn,
    no flash. Falls back to empty string on any failure so callers
    that treat "" as "PID dead" still work.
    """
    if pid <= 0:
        return ""
    try:
        import psutil  # local import: keeps this file fast on cold start
        p = psutil.Process(pid)
        return " ".join(p.cmdline() or [])
    except Exception:  # noqa: BLE001
        # NoSuchProcess, AccessDenied, ImportError -> empty.
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


# 2026-06-01 boot-speed fix: the process-table scan below (psutil cmdline
# access) costs ~1-3s and is_running() calls it ONCE PER SERVICE at boot
# (~15x) -> ~30s of boot was just re-scanning. Cache the scan for a short TTL
# so the boot burst shares one scan. Correctness is preserved because
# start_if_missing() ALSO guards double-spawn via the per-service PID lock
# (_lock_alive), so a slightly stale process view cannot double-spawn.
_pcl_cache: list[str] | None = None
_pcl_cache_ts: float = 0.0
_PCL_TTL_S = 10.0


def _process_command_lines() -> list[str]:
    """Return command lines for all python-ish processes that look
    like Luna components. Cached for _PCL_TTL_S seconds (boot-burst speedup).

    2026-05-16 popup root-cause fix (Codex H6): replaced the
    subprocess powershell.exe / Get-CimInstance call with native
    psutil enumeration. The PowerShell version was spawning a
    conhost.exe popup on every click (Win11 allocates conhost even
    with CREATE_NO_WINDOW). psutil walks the process table
    in-process - no spawn, no flash.

    Returns a list of command-line strings, one per matching process.
    Returns empty list on any failure (psutil unavailable, etc.).
    Filter mirrors the original PowerShell where clause: python-ish
    name + cmdline matches one of SurgeApp/Luna/aider/worker/guardian.
    """
    global _pcl_cache, _pcl_cache_ts
    now = time.time()
    if _pcl_cache is not None and (now - _pcl_cache_ts) < _PCL_TTL_S:
        return _pcl_cache
    try:
        import psutil
    except ImportError:
        return []
    out: list[str] = []
    needle_re = re.compile(r"surgeapp|luna|aider|worker|guardian",
                           re.IGNORECASE)
    name_re = re.compile(r"^python", re.IGNORECASE)
    for p in psutil.process_iter(["name", "cmdline"]):
        try:
            nm = p.info["name"] or ""
            if not name_re.match(nm):
                continue
            cmd = " ".join(p.info["cmdline"] or [])
            if cmd and needle_re.search(cmd):
                out.append(cmd)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    _pcl_cache = out
    _pcl_cache_ts = now
    return out


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
    if lock:
        # 2026-05-16 Codex deep-scan C8 fix: previous code had
        # 'script_rel != "aider_bridge.py"' here, which permanently
        # left aider_bridge.pid stale after the initial creation -
        # any tool that read the lock got an outdated PID. Write
        # the fresh PID for EVERY service uniformly. If the aider
        # bridge needs special lock semantics (e.g. shared with the
        # bridge's own startup self-write), it can read+merge
        # rather than relying on this layer skipping it entirely.
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


_CU_GATE_CORE_FILES = [
    "worker.py", "aider_bridge.py", "luna_guardian.py",
    "LaunchLuna.pyw", "luna_start.pyw",
    "SurgeApp_Claude_Terminal.py", "director_agent.py",
]


def _cu_startup_gate() -> tuple[bool, str]:
    """Return (paused, reason) when CU should not be auto-started.

    Phase 4D polish: high-priority `continues_update.resume_once` check —
    the user's explicit one-shot resume bypasses persisted-state pause
    decisions (but NOT the stop flag, which is the explicit kill).

    Phase 3 stabilization: gate respects continues_update.stop flag, the
    loop's persisted ui_status (paused_*/blocked_*), and a 30-min stale
    aider-job threshold.

    Reasons in priority order:
      paused_user_stop, paused_dirty_core, paused_noop_budget,
      paused_recent_failures, blocked_worker_import, blocked_aider_stale,
      stale_aider_job, dirty_core_files, noop_budget_exhausted, bridge_processing_stuck
    """
    # 1. Explicit user/stabilization stop flag wins over everything
    if (MEMORY / "continues_update.stop").exists():
        return True, "paused_user_stop"

    # 2. Phase 4D: explicit one-shot resume — if the user wrote
    #    memory/continues_update.resume_once, allow CU to start exactly once
    #    even when persisted ui_status says paused_*/blocked_*. The wrapper's
    #    own quiet-poll loop will consume the flag the first time it hits
    #    backoff; LaunchLuna does NOT consume it here so the wrapper still
    #    gets to honour it during long-pause polling. The launcher only logs
    #    that the bypass was honoured.
    if (MEMORY / "continues_update.resume_once").exists():
        return False, ""  # bypass — resume_once is consumed inside the wrapper

    # 3. CU loop's own persisted ui_status — if it explicitly says paused/blocked,
    #    respect it. The wrapper backoff is ALREADY waiting; LaunchLuna should
    #    not race-restart and override that.
    try:
        state_path = MEMORY / "continues_update_state.json"
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8", errors="replace") or "{}")
            ui_status = str(state.get("ui_status") or "")
            if ui_status.startswith("paused_") or ui_status.startswith("blocked_"):
                return True, ui_status
    except Exception:
        pass

    # 3. Stale active aider jobs (>30 min). Bridge will quarantine on its next
    #    startup; gate just refuses to start CU on top of unresolved stale work.
    active_dir = ROOT / "aider_jobs" / "active"
    if active_dir.exists():
        for jf in active_dir.glob("*.json"):
            try:
                if (time.time() - jf.stat().st_mtime) > 1800:
                    return True, "stale_aider_job"
            except Exception:
                pass

    # 4. Aider bridge stuck in processing >30 min (bridge heartbeat died)
    try:
        bs_path = ROOT / "logs" / "aider_bridge_status.json"
        if bs_path.exists():
            bs = json.loads(bs_path.read_text(encoding="utf-8", errors="replace") or "{}")
            if str(bs.get("state") or "") == "processing":
                last_evt = str(bs.get("last_event_at") or bs.get("started_at") or "")
                if last_evt:
                    try:
                        from datetime import datetime as _dt
                        age = (_dt.now() - _dt.fromisoformat(last_evt)).total_seconds()
                        if age > 1800:
                            return True, "bridge_processing_stuck"
                    except Exception:
                        pass
    except Exception:
        pass

    # 5. Dirty core files (tracked-modified only — untracked files don't count)
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=no", "--"] + _CU_GATE_CORE_FILES,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
            creationflags=NO_WIN,
        )
        if result.returncode == 0 and result.stdout.strip():
            return True, "dirty_core_files"
    except Exception:
        pass

    # 6. Noop / all-skip budget exhaustion
    try:
        state_path = MEMORY / "continues_update_state.json"
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8", errors="replace") or "{}")
            noop_count = int(state.get("noop_count") or 0)
            all_skip_streak = int(state.get("_all_skip_streak") or 0)
            if noop_count >= 5 or all_skip_streak >= 3:
                return True, "noop_budget_exhausted"
    except Exception:
        pass

    return False, ""


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

def _acquire_launcher_singleton() -> tuple[bool, str]:
    """Return (acquired, reason). True = we are the only LaunchLuna.pyw
    in flight; safe to proceed. False = another chain is already running;
    we should exit silently to avoid spawning duplicate terminals.

    2026-05-16 multi-click idempotency fix per Serge ("if I click too
    many times it opens multiple terminals"). Mechanism: a Windows
    named-mutex (kernel object) created at module entry.

      * On the FIRST click of a quiet system: CreateMutexW returns a
        handle with GetLastError() == 0. We continue normally.
      * On the SECOND/THIRD click while the first chain is still in
        flight: CreateMutexW returns the SAME handle with
        GetLastError() == ERROR_ALREADY_EXISTS (183). We exit
        immediately, no spawn.

    The mutex is held for the lifetime of THIS process. When
    LaunchLuna.pyw exits, Windows auto-releases it. The next click
    after that gets a clean acquire.

    The chosen name 'Global\\LunaCommandCenterLauncher_v1' is a
    user-session-scoped semaphore (not Global\\ system-wide) so two
    different Windows users on the same machine can each have their
    own launcher chain. Pythonw runs without elevation; non-Global\\
    names work in the user session.

    Returns (True, reason) on acquire; (False, reason) on busy. On
    any ctypes failure, returns (True, fallback_reason) so a broken
    helper never strands the launcher.
    """
    if os.name != "nt":
        return True, "non_windows_skip"
    try:
        import ctypes
        from ctypes import wintypes
        kernel32 = ctypes.windll.kernel32
        kernel32.CreateMutexW.argtypes = [
            wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR,
        ]
        kernel32.CreateMutexW.restype = wintypes.HANDLE
        kernel32.GetLastError.restype = wintypes.DWORD
        ERROR_ALREADY_EXISTS = 183
        # Local\\ scope so we don't need Local System privileges.
        # The 'v1' suffix lets us bump if the protocol ever changes.
        name = "Local\\LunaCommandCenterLauncher_v1"
        handle = kernel32.CreateMutexW(None, True, name)
        err = kernel32.GetLastError()
        if handle == 0:
            # Creation failed entirely (rare). Fall through so the
            # operator's click isn't lost.
            return True, f"mutex_create_failed_errno_{err}"
        if err == ERROR_ALREADY_EXISTS:
            # Another LaunchLuna.pyw is already in flight. Release our
            # weak claim on this handle and bail. (We don't hold the
            # handle because we're not the owner; the OS keeps the
            # mutex alive via the original creator's handle.)
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle(handle)
            return False, "another_launcher_in_flight"
        # We acquired the mutex as the creator. PIN the handle to a
        # module-level global so Python doesn't GC it before our
        # process exits. On process exit, Windows auto-releases.
        globals()["_LAUNCHER_MUTEX_HANDLE"] = handle
        return True, "mutex_acquired"
    except Exception as exc:  # noqa: BLE001
        # Defensive: never let a singleton check itself fail the chain.
        return True, f"mutex_check_errored: {type(exc).__name__}"


def main() -> None:
    # 2026-05-16 entry-point singleton check. Without this, rapid
    # double/triple-clicks on Luna Command Center.lnk fire multiple
    # wscript -> cmd -> pythonw LaunchLuna.pyw chains in parallel.
    # The existing in-chain checks (is_running, _lock_alive, the
    # SurgeApp_Claude_Terminal early-focus) handle the END of each
    # chain reasonably, but the chains still race during boot and
    # can each spawn their own Chrome --app= window. The kernel-
    # level mutex below is the only reliable way to coalesce
    # rapid-click chains on Windows.
    acquired, reason = _acquire_launcher_singleton()
    if not acquired:
        # Another LaunchLuna.pyw is already in flight. The operator's
        # click is "served" by that one; we exit immediately. No
        # log noise: this is the normal case under rapid clicks.
        return

    events: list[dict[str, Any]] = []
    ensure_dirs()
    clear_stale_lock()
    # Boot-timing instrumentation: NEVER raises; append-only log line.
    try:
        from luna_modules import luna_boot_timing as _bt
        _bt.mark("LaunchLuna.main.start")
    except Exception:
        pass
    events.append({
        "service": "launcher_singleton",
        "action": "acquired",
        "reason": reason,
    })
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
            # 2026-05-17 cascade-mitigation: previously this branch called
            # open_terminal() before returning so the operator could see
            # the blocked state. That itself spawned the chain. Per the
            # 7-fix plan: when LUNA_STOP_NOW.flag is present we MUST exit
            # immediately with code 0 and spawn NOTHING. The status JSON
            # is still written so a tray/CLI inspector can see the reason.
            events.append({"service": "autonomy", "action": "blocked",
                           "reason": "kill_switch_present",
                           "exit_path": "immediate_no_spawn"})
            write_startup_status(events)
            return
    # 2026-05-17 cascade-mitigation (Serge 7-fix plan, Fix 7): safety
    # valve. If > 20 Luna-flavored processes are already alive, refuse
    # to spawn the terminal + services. Last line of defense if all the
    # other layers' protections somehow fail to prevent a cascade --
    # LaunchLuna.pyw will not amplify what it observes.
    try:
        import psutil as _psutil
        _luna_count = sum(
            1 for _p in _psutil.process_iter(['cmdline'])
            if any(
                'surgeapp' in str(_c).lower() or 'luna' in str(_c).lower()
                for _c in (_p.info.get('cmdline') or [])
            )
        )
        # 2026-05-31 threshold recalibration (Serge "Luna won't boot"):
        # the broad cmdline match ('luna'/'surgeapp') counts legitimate
        # always-on processes — WinSW services (~6), the FastTalk voice
        # pipeline added since (6: stt/tts/controller/fastbrain/llm/bridge),
        # core services (~6), dashboard+terminal, plus any active dev/Claude
        # session running under D:\SurgeApp. A healthy system is now ~26,
        # which TRIPPED the old threshold of 20 and blocked boot entirely
        # (observed: luna_count=26 > 20 -> spawned NOTHING). True runaway
        # cascades historically hit 90-110+ python processes, so 60 cleanly
        # separates "healthy + dev session" (<=~40) from "runaway" (90+)
        # while keeping the last-line cascade defense intact.
        _SAFETY_VALVE_THRESHOLD = 60
        if _luna_count > _SAFETY_VALVE_THRESHOLD:
            events.append({"service": "autonomy", "action": "blocked",
                           "reason": "safety_valve_too_many_luna_processes",
                           "luna_count": _luna_count,
                           "threshold": _SAFETY_VALVE_THRESHOLD})
            write_startup_status(events)
            return
    except Exception:  # noqa: BLE001
        pass
    # Show the monitor first; slower background services can warm up behind it.
    try:
        from luna_modules import luna_boot_timing as _bt
        _bt.mark("LaunchLuna.before_services")
    except Exception:
        pass
    # 2026-06-01: Skip the native Python terminal (SurgeApp_Claude_Terminal.py)
    # — operator wants ONE window, not two. Start LaunchLunaDashboard.pyw
    # directly here instead; it opens Chrome in --app= mode at 127.0.0.1:8765
    # (the one Luna terminal the operator wants). SurgeApp_Claude_Terminal.py
    # was the OLD native Qt window; the web dashboard is the primary interface.
    # open_terminal()  # DISABLED — opens unwanted second window
    events.append({"service": "SurgeApp_Claude_Terminal.py", "action": "skipped_operator_wants_one_window"})
    _dashboard_started = start_if_missing("LaunchLunaDashboard.pyw")
    events.append({"service": "LaunchLunaDashboard.pyw",
                   "action": "started" if _dashboard_started else "already_running"})
    write_startup_status(events)

    # 2026-05-31: Ollama removed — all in-house, no external LLM deps.
    # ensure_ollama()  # DISABLED
    events.append({"service": "ollama", "action": "skipped_in_house_only"})

    # Background services (hidden, start only if not already running). Guardian
    # starts last so it does not race the launcher and duplicate services.
    events.append({"service": "luna_apprentice.py", "action": "started" if start_if_missing("luna_apprentice.py") else "already_running_or_missing"})
    events.append({"service": "worker.py", "action": "started" if start_if_missing("worker.py", exe=WORKER_PY) else "already_running_or_missing"})

    # 2026-06-01 Luna System Warden daemon — watches worker.py + luna_guardian.py
    # for heartbeat-stale / CPU-thrash / IO-thrash patterns and auto-restarts
    # them with safety caps (10 min cooldown, 4/24h cap, soft auto-disable for
    # 6h). Standalone tiny daemon (independent of worker so a wedged worker
    # cannot disable the watcher). Has its own cmdline-singleton check so
    # double-spawn from rapid clicks is safe. Spec at
    # memory/spec_luna_system_warden_2026_06_01.md.
    events.append({"service": "luna_system_warden_daemon.py",
                    "action": "started" if start_if_missing(
                        r"luna_modules\luna_system_warden_daemon.py")
                    else "already_running_or_missing"})

    # Aider bridge uses .aider_venv Python (has aider installed).
    # 2026-05-16 conhost-popup fix per Codex audit #6: prefer pythonw.exe
    # from the aider venv so the spawned bridge does NOT allocate a
    # conhost window. python.exe is the console subsystem; pythonw.exe
    # is /SUBSYSTEM:WINDOWS. Falls back to python.exe only if pythonw
    # is missing from the venv (Aider can be installed against either,
    # but pythonw is the default modern install).
    aider_pyw_path = ROOT / ".aider_venv" / "Scripts" / "pythonw.exe"
    aider_py_path  = ROOT / ".aider_venv" / "Scripts" / "python.exe"
    if aider_pyw_path.exists():
        aider_started = start_if_missing(
            "aider_bridge.py", exe=str(aider_pyw_path))
    elif aider_py_path.exists():
        # Fallback: console-subsystem python with explicit CREATE_NO_WINDOW
        # is handled inside start_hidden (NO_WIN flag). Still spawns a
        # hidden conhost but at least nothing flashes.
        aider_started = start_if_missing(
            "aider_bridge.py", exe=str(aider_py_path))
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
    _resume_once_present = (MEMORY / "continues_update.resume_once").exists()
    _cu_gate_paused, _cu_gate_reason = _cu_startup_gate()
    if _cu_gate_paused:
        _archive_path(cu_lock, f"gate_{_cu_gate_reason}")
        events.append({
            "service": "continues_update",
            "action": "paused_gate",
            "reason": _cu_gate_reason,
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
        # Phase 4D: distinguish a resume_once-honoured launch from a normal
        # startup so operators can see the gate bypass in the log.
        if _resume_once_present:
            events.append({
                "service": "continues_update",
                "action": "resume_once_allowed",
                "reason": "memory/continues_update.resume_once present — gate bypass honoured",
                "pid": pid,
            })
        else:
            events.append({"service": "continues_update", "action": "started", "pid": pid})
    else:
        events.append({"service": "continues_update", "action": "already_running_or_missing"})

    events.append({"service": "luna_guardian.py", "action": "started" if start_if_missing("luna_guardian.py") else "already_running_or_missing"})

    # ── FastTalk voice pipeline (bilingual RU/EN Luna clone voices) ─────────
    # These 6 services give Luna her voice. Spawned via .voice_venv so they
    # get XTTS, torch, faster-whisper, and sounddevice. Stagger by 300 ms
    # so each service binds its port before the next one starts.
    # Services: STT(8768) TTS(8769) Controller(8770)
    #           FastBrain(8771) LLM(8772) Bridge(8773)
    if Path(VOICE_PYW).exists():
        _voice_env = {**ENV, "KMP_DUPLICATE_LIB_OK": "TRUE"}
        _voice_scripts = [
            "luna_fast_stt_service.py",
            "luna_fast_tts_service.py",
            "luna_fasttalk_controller.py",
            "luna_fastbrain_router.py",
            # luna_fastbrain_llm_server.py REMOVED — no Ollama/Llama,
            # all in-house. FastBrain uses vocab DB (2M words) +
            # conversational templates instead.
            "luna_fastbrain_bridge.py",
        ]
        for _vs in _voice_scripts:
            _vs_path = ROOT / _vs
            if not _vs_path.exists():
                events.append({"service": _vs, "action": "missing"})
                continue
            if is_running(_vs):
                events.append({"service": _vs, "action": "already_running"})
                continue
            _vs_lock = SERVICE_LOCKS.get(_vs)
            if _vs_lock and _lock_alive(_vs_lock):
                events.append({"service": _vs, "action": "already_running"})
                continue
            try:
                _vs_proc = subprocess.Popen(
                    [VOICE_PYW, str(_vs_path)],
                    cwd=str(ROOT),
                    env=_voice_env,
                    creationflags=NO_WIN,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    close_fds=True,
                )
                if _vs_lock:
                    _write_pid_lock(_vs_lock, int(_vs_proc.pid), _vs)
                events.append({"service": _vs, "action": "started",
                               "pid": int(_vs_proc.pid)})
            except Exception as _ve:
                events.append({"service": _vs, "action": "start_failed",
                               "error": str(_ve)[:200]})
            time.sleep(0.3)  # stagger so ports don't collide

        # Background warmup thread: poll health, configure bilingual mode,
        # then prewarm STT+TTS models so the first voice call is instant.
        def _voice_warmup() -> None:
            import urllib.request as _ur
            _ports = {"stt": 8768, "tts": 8769, "ctl": 8770,
                      "fb": 8771, "bridge": 8773}
            # 8772 (LLM server) removed — in-house only, no Llama
            _deadline = time.time() + 120
            # Wait for all services to respond on /health
            while time.time() < _deadline:
                _all_up = True
                for _name, _port in _ports.items():
                    try:
                        _ur.urlopen(f"http://127.0.0.1:{_port}/health",
                                    timeout=2)
                    except Exception:
                        _all_up = False
                        break
                if _all_up:
                    break
                time.sleep(2)
            # Configure: set STT to bilingual, controller to FastBrain route
            for _cfg_url, _cfg_body in [
                ("http://127.0.0.1:8768/set_mode",
                 '{"mode": "fast_multi"}'),
                ("http://127.0.0.1:8770/set_route_mode",
                 '{"mode": "fast_luna"}'),
            ]:
                try:
                    _req = _ur.Request(
                        _cfg_url, data=_cfg_body.encode("utf-8"),
                        headers={"Content-Type": "application/json"})
                    _ur.urlopen(_req, timeout=5)
                except Exception:
                    pass
            # Prewarm STT (loads Whisper model) and TTS (loads XTTS clone)
            for _wu_url in [
                "http://127.0.0.1:8768/warmup",
                "http://127.0.0.1:8769/prewarm",
            ]:
                try:
                    _req = _ur.Request(
                        _wu_url, data=b'{}',
                        headers={"Content-Type": "application/json"})
                    _ur.urlopen(_req, timeout=600)
                except Exception:
                    pass

        threading.Thread(target=_voice_warmup, name="voice-warmup",
                         daemon=True).start()
        events.append({"service": "voice_pipeline",
                       "action": "warmup_thread_started"})
    else:
        events.append({"service": "voice_pipeline",
                       "action": "skipped_no_voice_venv",
                       "path": VOICE_PYW})

    try:
        from luna_modules import luna_boot_timing as _bt
        _bt.mark("LaunchLuna.after_services")
    except Exception:
        pass

    write_startup_status(events)


if __name__ == "__main__":
    main()
