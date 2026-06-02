"""Launch the Luna read-only HTTP dashboard in chromeless app-window mode.

Double-clicking this .pyw boots the dashboard server on http://127.0.0.1:8765
and opens it inside Chrome / Edge / Brave with ``--app=`` so there is no
URL bar, no tab strip, no extension toolbar — Luna fills the whole window
like a native command-center app. If no Chromium browser is installed,
falls back to the system default browser.

No console window; no shell calls into project files; no package installs;
no live execution. The launched browser process is the user's own browser
in app-window mode — exactly what they would see if they pressed
``Install Luna`` on a PWA, only built from a stdlib helper instead.

Phase UI-1A — Luna Futuristic HTTP Dashboard Foundation.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import webbrowser
from pathlib import Path

# Ensure project root is importable when launched by double-click.
_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# The desktop shortcut can be associated with the WindowsApps pythonw.exe,
# which does not see the repo venv packages. Add the venv site-packages early
# so local TTS dependencies such as pyttsx3 are visible before importing the
# HTTP dashboard module.
_VENV_SITE_PACKAGES = (
    _PROJECT_ROOT / ".aider_venv" / "Lib" / "site-packages"
)
if _VENV_SITE_PACKAGES.exists() and str(_VENV_SITE_PACKAGES) not in sys.path:
    sys.path.insert(1, str(_VENV_SITE_PACKAGES))

# 2026-05-13 API vault boot hook. Loads operator-provided API keys
# from D:\SurgeApp\API.txt + D:\SurgeApp\API - Copy.txt into os.environ
# BEFORE any module that might read those env vars imports. This is the
# single canonical operator-key source. Never logs key values; never
# overrides an env var that the operator's shell already set.
try:
    from luna_modules import luna_api_vault as _vault
    _vault_status = _vault.populate_env(force=False)
    # Audit-safe info line — names only, no values.
    try:
        import logging as _logging
        _logging.getLogger("luna.api_vault").info(
            "api_vault: %d keys loaded into env (%d skipped already-set); names=%s",
            _vault_status["set_count"], _vault_status["skipped_existing"],
            _vault_status["available_names"],
        )
    except Exception:  # noqa: BLE001
        pass
except Exception:  # noqa: BLE001
    # Vault failure must not break dashboard boot. Council and other
    # consumers will simply see empty env vars (safe-degraded path).
    pass

from luna_modules import luna_http_dashboard as hd

# 2026-05-13 in-process orchestrator: every 30s refreshes audit /
# progression / stuck; every 120s runs self-recovery; keeps a heartbeat
# breadcrumb alive. Turns SurgeApp from snapshot mode to live mode.
try:
    from luna_modules import luna_master_status as _ms
    _ms.start_orchestrator()
except Exception:  # noqa: BLE001
    # Orchestrator failure must NOT break the dashboard boot. The
    # /api/master-status endpoint still computes on-demand if the
    # scheduler is absent.
    pass

# Write the canonical runtime-ownership map once at boot.
try:
    from luna_modules import luna_runtime_ownership as _ro
    _ro.write_runtime_ownership_map()
except Exception:  # noqa: BLE001
    pass


# Common Chromium-family install locations on Windows. Each entry is a
# (binary path, friendly name) pair. We probe in priority order; first hit
# wins. The user's installed browser stays the source of truth — this
# script only opens it.
#
# Priority order changed 2026-05-08 per Serge directive: Edge first so
# Luna runs in its own browser separate from Chrome's everyday session.
# Override per-launch via env var:
#   LUNA_BROWSER=edge   -> only Edge candidates probed
#   LUNA_BROWSER=chrome -> only Chrome candidates probed
#   LUNA_BROWSER=brave  -> only Brave candidates probed
#   (unset)             -> Edge -> Chrome -> Brave fallback chain
def _candidate_browsers() -> list[tuple[Path, str]]:
    pf = os.environ.get("ProgramFiles", r"C:\Program Files")
    pf86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
    local = os.environ.get("LocalAppData", str(Path.home() / "AppData" / "Local"))
    edge_paths = [
        (Path(pf) / "Microsoft" / "Edge" / "Application" / "msedge.exe", "Edge"),
        (Path(pf86) / "Microsoft" / "Edge" / "Application" / "msedge.exe", "Edge"),
        (Path(local) / "Microsoft" / "Edge" / "Application" / "msedge.exe", "Edge"),
    ]
    chrome_paths = [
        (Path(pf) / "Google" / "Chrome" / "Application" / "chrome.exe", "Chrome"),
        (Path(pf86) / "Google" / "Chrome" / "Application" / "chrome.exe", "Chrome"),
        (Path(local) / "Google" / "Chrome" / "Application" / "chrome.exe", "Chrome"),
    ]
    brave_paths = [
        (Path(pf) / "BraveSoftware" / "Brave-Browser" / "Application" / "brave.exe", "Brave"),
        (Path(pf86) / "BraveSoftware" / "Brave-Browser" / "Application" / "brave.exe", "Brave"),
    ]
    pref = (os.environ.get("LUNA_BROWSER") or "").strip().lower()
    if pref == "edge":   return edge_paths
    if pref == "chrome": return chrome_paths
    if pref == "brave":  return brave_paths
    # Default fallback chain: Edge first, then Chrome, then Brave.
    return edge_paths + chrome_paths + brave_paths


def _find_chromium() -> tuple[Path, str] | None:
    for p, name in _candidate_browsers():
        if p.exists() and p.is_file():
            return p, name
    return None


def _system_dpi_scale() -> float:
    """Return the Windows display scale factor (1.0 = 100% scaling).

    Reads HKCU\\Control Panel\\Desktop\\WindowMetrics::AppliedDPI which
    Windows sets to 96 (100%), 120 (125%), 144 (150%), 192 (200%), etc.
    Returns 1.0 on non-Windows or any failure — safe default that means
    'do not pass a force-device-scale-factor flag'.
    """
    if os.name != "nt":
        return 1.0
    try:
        import winreg
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Control Panel\Desktop\WindowMetrics",
        ) as key:
            applied_dpi, _ = winreg.QueryValueEx(key, "AppliedDPI")
        if applied_dpi and applied_dpi > 0:
            return float(applied_dpi) / 96.0
    except Exception:  # noqa: BLE001
        pass
    return 1.0


def _open_in_app_mode(url: str) -> None:
    """Open ``url`` in a Chromium app window if possible; otherwise fall back."""
    # 2026-06-02: headless respawn guard. When the warden / terminal-updater
    # restarts the dashboard for HEALTH reasons it sets LUNA_DASHBOARD_NO_BROWSER
    # so we keep the HTTP server alive WITHOUT popping a Command Center window.
    # The operator's desktop-shortcut click does NOT set this, so a real click
    # still opens the window normally. This stops Luna "popping up on her own".
    import os as _os
    if _os.environ.get("LUNA_DASHBOARD_NO_BROWSER", "").strip() in ("1", "true", "True"):
        try:
            _fatal_log("LaunchLunaDashboard.pyw: LUNA_DASHBOARD_NO_BROWSER set "
                       "— server kept alive, browser window suppressed "
                       "(background health respawn, no operator click).")
        except Exception:  # noqa: BLE001
            pass
        return
    time.sleep(0.5)  # tiny delay so the server is accepting before we ask
    chrome = _find_chromium()
    if chrome is None:
        try:
            webbrowser.open(url, new=2, autoraise=True)
        except Exception:  # noqa: BLE001
            pass
        return

    binary, _name = chrome
    # We deliberately use os.spawnv with P_NOWAIT — no shell, no PATH lookup,
    # exact absolute binary, fixed argv. This is the same as letting the user
    # double-click their browser and visiting the URL, just chromeless.
    #
    # NOTE on --user-data-dir: previous versions of this script passed
    # --user-data-dir=<isolated dir> for sandboxing. That flag DOES NOT WORK
    # when an existing Chrome browser is already running on the same machine:
    # Chrome's IPC relay hijacks the new spawn, the spawned process exits with
    # STATUS_BREAKPOINT (0x80000003), and no chromeless window appears. The
    # operator was experiencing exactly this whenever Luna was launched after
    # opening regular Chrome. Removing --user-data-dir lets the chromeless
    # --app= window open inside the user's existing Chrome process, with no
    # IPC conflict. This is functionally identical to what a PWA "Install
    # Luna Dashboard" would produce in their existing Chrome profile, and is
    # safe because the dashboard runs entirely on 127.0.0.1:8765 (no third-
    # party origins to leak to).
    # Chrome flag minimum set. Verified working 2026-05-08.
    #
    # DROPPED FLAGS (caused chrome.exe to exit with STATUS_BREAKPOINT
    # 0x80000003 in current Chrome builds):
    #   --use-fake-ui-for-media-stream
    #   --auto-accept-camera-and-microphone-capture
    #   --user-data-dir=<isolated profile>
    # The first two were tightened by Chrome's policy and now kill the
    # process at startup. The third caused IPC-relay conflicts when
    # another Chrome was already running. None are required for the
    # dashboard to work. Operator clicks "Allow" once on the mic icon
    # for 127.0.0.1:8765 and Chrome remembers it for that origin
    # forever (same UX as any first-time mic permission grant).
    args = [
        str(binary),
        f"--app={url}",
        "--window-size=1480,940",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    # DPI handling: we used to pass `--force-device-scale-factor=(1/scale)`
    # here to keep the dashboard at design size on >100% Windows scaling.
    # That flag is REMOVED 2026-05-08 because Chromium's IPC singleton
    # routes new spawns through the running browser process, so the flag
    # bled into the operator's regular Chrome session and shrunk every
    # window to 80% (1.0/1.25). Symptom: "my Chrome is so small".
    # Without the flag the dashboard inherits Windows' native DPI scale,
    # which is what regular web browsing expects anyway. If the dashboard
    # renders too large on high-PPI displays, the right fix is a CSS-side
    # adjustment in luna_dashboard/style.css (e.g. a zoom override on the
    # luna-shell root scoped to media (min-resolution: 1.25dppx)) - NOT a
    # process-wide command-line flag that leaks to the whole browser.
    # _scale = _system_dpi_scale()  # kept for reference; intentionally unused
    # subprocess.Popen with no creationflags. Chrome detaches itself
    # from the parent's console/process-group correctly. DETACHED_PROCESS
    # was removed because it breaks Chrome's GUI initialization on
    # Windows. The chromeless --app= window survives launcher exit
    # regardless of how the launcher was invoked.
    try:
        import subprocess
        # 2026-05-16 conhost-flash safety net per Codex audit #12:
        # Chrome is a GUI app and normally doesn't flash a console, but
        # on some Windows builds the spawn briefly creates a helper
        # conhost.exe. Adding CREATE_NO_WINDOW (0x08000000) explicitly
        # tells the OS to suppress any console allocation. Harmless on
        # builds that never would have flashed anyway. DETACHED_PROCESS
        # is still avoided because it breaks Chrome's GUI init.
        _NO_WIN_FLAG = 0x08000000 if os.name == "nt" else 0
        subprocess.Popen(
            args,
            close_fds=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=_NO_WIN_FLAG,
        )
    except (OSError, Exception):  # noqa: BLE001
        # Last-resort: ordinary default-browser open.
        try:
            webbrowser.open(url, new=2, autoraise=True)
        except Exception:  # noqa: BLE001
            pass


def _worker_already_running() -> bool:
    """Best-effort check: is a ``worker.py`` background instance alive?

    Uses the worker's own lockfile + Win32 OpenProcess (no subprocess, no
    conhost flicker). Per the Warp fix memo (memory/warp_fix_playbook.txt):
    wmic.exe and tasklist.exe both allocate a conhost.exe even with
    CREATE_NO_WINDOW, and on Windows 11 that flashes a black window. The
    correct pattern is ctypes OpenProcess + GetExitCodeProcess (STILL_ACTIVE).
    Worker.py writes ``logs/luna_worker.lock.json`` with its pid; we trust
    that single file. On read failure we return False - duplicate workers
    are harmless because worker.py itself enforces a singleton at startup.
    """
    lock = _PROJECT_ROOT / "logs" / "luna_worker.lock.json"
    if not lock.exists():
        return False
    try:
        import json
        data = json.loads(lock.read_text(encoding="utf-8", errors="replace") or "{}")
        pid = int(data.get("pid") or 0)
    except Exception:  # noqa: BLE001
        return False
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    try:
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong(0)
            ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
            return bool(ok) and exit_code.value == STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    except (OSError, AttributeError):
        return False


def _ensure_worker_running() -> None:
    """Start ``worker.py`` hidden if not already running.

    Mirrors the current "start worker if missing" behavior so a user
    can launch *just* the dashboard and still get a live chat round-trip
    with Luna.
    """
    worker_py = _PROJECT_ROOT / "worker.py"
    if not worker_py.exists():
        return
    if _worker_already_running():
        return
    # Pick a REAL pythonw.exe with PySide6, NOT the WindowsApps alias stub
    # under C:\Users\<user>\AppData\Local\Microsoft\WindowsApps\. The alias
    # stub triggers Microsoft Store popups on hidden launches (per the
    # May 2026 warp_fix memo). Real installer paths first; alias stub
    # ONLY as a last-resort fallback.
    candidates = [
        Path(r"C:\Program Files\Python311\pythonw.exe"),
        Path(r"C:\Program Files\Python312\pythonw.exe"),
        Path(sys.executable).with_name("pythonw.exe"),
        Path(sys.executable),
        # Last-resort fallback only — alias stubs can pop the Store.
        Path(os.environ.get("LocalAppData", str(Path.home() / "AppData" / "Local")))
            / "Microsoft" / "WindowsApps"
            / "PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0" / "pythonw.exe",
    ]
    py: Path | None = None
    for c in candidates:
        try:
            if c.exists() and c.is_file() and c.stat().st_size > 0:
                # Skip the WindowsApps alias stub if a real path won earlier.
                if "WindowsApps" in str(c) and py is None:
                    py = c  # accept only if nothing better was found
                    continue
                if "WindowsApps" not in str(c):
                    py = c
                    break
        except OSError:
            continue
    if py is None:
        return
    # 2026-05-12 second pythonw fix per Serge: redirect stdout/stderr to
    # a real log file, NOT DEVNULL. Reproduced symptom: DEVNULL'd streams
    # caused the worker (and earlier the dashboard) to die silently when
    # any library write (BaseHTTPServer log_message, print(), etc.)
    # touched the broken descriptor. Real file handles keep the process
    # alive and give us a forensic trail.
    NO_WIN = 0x08000000  # CREATE_NO_WINDOW
    import subprocess
    stdout_fh = None
    stderr_fh = None
    try:
        log_dir = _PROJECT_ROOT / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        worker_log = log_dir / "luna_worker_service.log"
        stdout_fh = worker_log.open("ab", buffering=0)
        stderr_fh = worker_log.open("ab", buffering=0)
        subprocess.Popen(
            [str(py), str(worker_py)],
            cwd=str(_PROJECT_ROOT),
            creationflags=NO_WIN,
            close_fds=True,
            stdin=subprocess.DEVNULL,
            stdout=stdout_fh,
            stderr=stderr_fh,
        )
    except OSError:
        # Worker absent is non-fatal: chat will queue tasks but they'll
        # only be answered once the user starts the full stack.
        pass
    except Exception:
        # Any other failure (log file write blocked, etc.) -> swallow so
        # the dashboard still starts.
        pass
    finally:
        # Close the parent's references; the child inherits the OS handles.
        for fh in (stdout_fh, stderr_fh):
            try:
                if fh is not None:
                    fh.close()
            except Exception:
                pass


def _ensure_runtime_watchdog_running() -> None:
    """Spawn the same bounded watchdog window used by the scheduled task.

    The watchdog itself remains bounded; the persistent continuity layer is
    ``LunaRuntimeWatchdogUser``. The desktop launcher starts one window too
    so a manual launch immediately gets watchdog coverage without waiting
    for the next scheduled fire.

    Idempotency: a sentinel lockfile at
    ``logs/luna_runtime_watchdog.lock.json`` records the live PID;
    if that PID is still alive, this is a no-op.

    Opt-out: set ``LUNA_DISABLE_WATCHDOG=1`` in the environment to
    skip auto-spawning the watchdog (useful for debug sessions where
    the operator wants full manual control of the runtime).

    Stdout/stderr go to ``logs/luna_runtime_watchdog.log`` (NEVER
    DEVNULL — same silent-crash trap class the worker and dashboard
    spawns already avoid).
    """
    if (os.environ.get("LUNA_DISABLE_WATCHDOG") or "").strip() in {"1", "true", "yes"}:
        return
    import subprocess
    log_dir   = _PROJECT_ROOT / "logs"
    lock_path = log_dir / "luna_runtime_watchdog.lock.json"
    log_path  = log_dir / "luna_runtime_watchdog.log"
    runner    = _PROJECT_ROOT / "Luna_Runtime_Watchdog_Window.ps1"
    if not runner.exists():
        return

    # Idempotency: if a prior watchdog recorded its PID and it's still
    # running, don't spawn a duplicate.
    try:
        if lock_path.exists():
            import json as _json
            data = _json.loads(lock_path.read_text(encoding="utf-8"))
            prior_pid = int(data.get("pid", 0) or 0)
            if prior_pid > 0:
                try:
                    import ctypes
                    PROCESS_QUERY_LIMITED = 0x1000
                    h = ctypes.windll.kernel32.OpenProcess(
                        PROCESS_QUERY_LIMITED, False, prior_pid)
                    if h:
                        ctypes.windll.kernel32.CloseHandle(h)
                        return  # still alive — don't spawn duplicate
                except Exception:  # noqa: BLE001
                    pass
    except (OSError, ValueError, Exception):  # noqa: BLE001
        pass

    powershell = "powershell.exe"
    NO_WIN = 0x08000000  # CREATE_NO_WINDOW
    stdout_fh = None
    stderr_fh = None
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_fh = log_path.open("ab", buffering=0)
        stderr_fh = log_path.open("ab", buffering=0)
        proc = subprocess.Popen(
            [
                powershell,
                "-NoProfile",
                "-ExecutionPolicy", "Bypass",
                "-File", str(runner),
                "-RunOnce",
                "-MaxTicks", "240",
                "-SleepSeconds", "5",
            ],
            cwd=str(_PROJECT_ROOT),
            creationflags=NO_WIN,
            close_fds=False,
            stdin=subprocess.DEVNULL,
            stdout=stdout_fh,
            stderr=stderr_fh,
        )
        try:
            import json as _json
            from datetime import datetime as _dt, timezone as _tz
            lock_path.write_text(_json.dumps({
                "pid": int(proc.pid),
                "service": "luna_runtime_watchdog",
                "started_at": _dt.now(tz=_tz.utc).isoformat(),
            }, indent=2), encoding="utf-8")
        except OSError:
            pass
    except OSError:
        pass
    except Exception:  # noqa: BLE001
        pass
    finally:
        for fh in (stdout_fh, stderr_fh):
            try:
                if fh is not None:
                    fh.close()
            except Exception:
                pass


def _ensure_repair_executor_running() -> None:
    """Spawn ``python -m luna_modules.repair_task_executor --watch`` as a
    sibling service of worker.py. Idempotent: a sentinel lockfile at
    ``logs/luna_repair_executor.lock.json`` records the live PID; if
    that PID is still alive, this is a no-op.

    Stdout/stderr go to ``logs/luna_repair_executor.log`` (NEVER DEVNULL —
    same silent-crash trap class the worker spawn fixed earlier).
    """
    import subprocess
    log_dir = _PROJECT_ROOT / "logs"
    lock_path = log_dir / "luna_repair_executor.lock.json"
    log_path = log_dir / "luna_repair_executor.log"

    # Idempotency: if a prior executor recorded its PID and it's still
    # running, don't spawn a duplicate.
    try:
        if lock_path.exists():
            import json as _json
            data = _json.loads(lock_path.read_text(encoding="utf-8"))
            prior_pid = int(data.get("pid", 0) or 0)
            if prior_pid > 0:
                # Best-effort liveness probe.
                try:
                    import ctypes
                    PROCESS_QUERY_LIMITED = 0x1000
                    h = ctypes.windll.kernel32.OpenProcess(
                        PROCESS_QUERY_LIMITED, False, prior_pid)
                    if h:
                        ctypes.windll.kernel32.CloseHandle(h)
                        return  # still alive — don't spawn duplicate
                except Exception:  # noqa: BLE001
                    pass
    except (OSError, ValueError, Exception):  # noqa: BLE001
        pass

    # Pick a python interpreter — prefer the same one running this pyw.
    py = sys.executable
    if not py or not Path(py).exists():
        return

    NO_WIN = 0x08000000  # CREATE_NO_WINDOW
    stdout_fh = None
    stderr_fh = None
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_fh = log_path.open("ab", buffering=0)
        stderr_fh = log_path.open("ab", buffering=0)
        proc = subprocess.Popen(
            [str(py), "-m", "luna_modules.repair_task_executor",
             "--watch", "--keepalive", "--max", "10000", "--sleep", "5"],
            cwd=str(_PROJECT_ROOT),
            creationflags=NO_WIN,
            close_fds=True,
            stdin=subprocess.DEVNULL,
            stdout=stdout_fh,
            stderr=stderr_fh,
        )
        # Record the PID so the idempotent check above can see it next time.
        try:
            import json as _json
            from datetime import datetime as _dt, timezone as _tz
            lock_path.write_text(_json.dumps({
                "pid": int(proc.pid),
                "service": "luna_repair_task_executor",
                "started_at": _dt.now(tz=_tz.utc).isoformat(),
            }, indent=2), encoding="utf-8")
        except OSError:
            pass
    except OSError:
        pass
    except Exception:  # noqa: BLE001
        pass
    finally:
        for fh in (stdout_fh, stderr_fh):
            try:
                if fh is not None:
                    fh.close()
            except Exception:
                pass


def _dashboard_already_serving(host: str, port: int, timeout_s: float = 0.5) -> bool:
    """True if something is already listening on host:port AND responding
    to GET /api/health 2xx. 2026-05-14 routed through the shared helper
    ``luna_launcher_health.dashboard_healthy`` so every link in the
    launcher chain (LaunchLuna.pyw, SurgeApp_Claude_Terminal.py, and
    this file) shares ONE definition of "already serving"."""
    try:
        from luna_modules import luna_launcher_health as _lh
        return _lh.dashboard_healthy(host=host, port=port,
                                      timeout_s=max(timeout_s, 1.0))
    except Exception:    # noqa: BLE001
        # Conservative fallback to the legacy bare TCP + health probe
        # so a missing helper never strands the launcher.
        import socket
        import urllib.request
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout_s)
            sock.connect((host, port))
            sock.close()
        except OSError:
            return False
        finally:
            try:
                if sock is not None:
                    sock.close()
            except OSError:
                pass
        try:
            req = urllib.request.Request(f"http://{host}:{port}/api/health")
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return 200 <= resp.status < 300
        except Exception:  # noqa: BLE001
            return False


def _fatal_log(message: str) -> None:
    """Append a fatal-exit record to logs/luna_command_center.log. Never raises."""
    try:
        from pathlib import Path as _Path
        import datetime as _dt
        logs_dir = _Path(__file__).resolve().parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        stamp = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
        with (logs_dir / "luna_command_center.log").open("a", encoding="utf-8", errors="replace") as fh:
            fh.write(f"[{stamp}] {message}\n")
    except Exception:  # noqa: BLE001
        pass


def _winsw_lunadashboard_running() -> bool:
    """2026-05-17 multi-bind root fix: if the WinSW LunaDashboard
    service is RUNNING, the desktop launcher MUST NOT bind 8765
    too. Windows SO_REUSEADDR allows multi-bind, and each extra
    binder splits traffic, slowing /api/health, which the watchdog
    misreads as 'dashboard down' and spawns more launchers - vicious
    cycle observed on 2026-05-17 with 34+ stacked launchers. The
    desktop click should focus the existing Luna window (if any)
    and exit cleanly when WinSW owns the service.
    """
    try:
        import subprocess as _sp
        proc = _sp.run(
            ["sc.exe", "query", "LunaDashboard"],
            capture_output=True, text=True,
            timeout=2.5, creationflags=0x08000000)
        if proc.returncode != 0:
            return False
        for line in (proc.stdout or "").splitlines():
            if "STATE" in line and (
                "RUNNING" in line or "4  RUNNING" in line):
                return True
        return False
    except Exception:  # noqa: BLE001
        return False


def main() -> int:
    # 2026-05-17 cascade-mitigation MASTER kill switch (Serge 7-fix plan,
    # Fix 1c). Honored by every launcher / supervisor entry point. If the
    # flag exists we exit immediately with code 0 and spawn NOTHING. This
    # is the only reliable way to break a runaway cascade across all the
    # supervision layers (guardian + watchdog + this launcher + winSW).
    # Auto-expire is handled by LaunchLuna.pyw - we just respect the file
    # if it is present.
    _kill_switch = Path(r"D:\SurgeApp\LUNA_STOP_NOW.flag")
    if _kill_switch.exists():
        try:
            _fatal_log("LaunchLunaDashboard.pyw: LUNA_STOP_NOW.flag is "
                       "present at D:\\SurgeApp\\LUNA_STOP_NOW.flag; "
                       "exiting immediately with code 0 and spawning "
                       "NOTHING (cascade mitigation).")
        except Exception:  # noqa: BLE001
            pass
        return 0
    # 2026-05-17 multi-bind root fix: if WinSW LunaDashboard service
    # is RUNNING, focus the existing Luna window (if any) and exit
    # without spawning a new user-space dashboard. The WinSW service
    # already owns port 8765; a second binder would cause SO_REUSEADDR
    # contention.
    if _winsw_lunadashboard_running():
        _fatal_log("LaunchLunaDashboard.pyw: WinSW LunaDashboard "
                   "service is RUNNING; deferring to it instead of "
                   "spawning a user-space dashboard.")
        # Focus existing Luna window if any (so the operator sees
        # feedback from their click).
        try:
            from luna_modules import luna_window_focus as _wf
            try:
                _wf.focus_luna_command_center()
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            pass
        return 0
    # 2026-05-14 multiple-command-center root fix (REVISED 2026-05-15
    # after live operator report "Luna won't boot from desktop"):
    #
    # The first version of this guard was too aggressive in two ways:
    #
    # 1. It exited whenever ANOTHER ``LaunchLunaDashboard.pyw`` process
    #    was alive, even if that other process was stuck in its early
    #    boot and had not yet bound port 8765. Result: the operator's
    #    desktop click would log "singleton exit — 1 other instance(s)
    #    alive" and silently do nothing. The fix below now requires
    #    BOTH "another launcher alive" AND "port 8765 already serving
    #    a healthy dashboard" before refusing to start. A stuck early-
    #    boot launcher is treated as a no-op and the new instance
    #    takes over.
    #
    # 2. It respected the operator-quit flag, which the operator sets
    #    when intentionally closing Luna so the watchdog stops
    #    auto-restarting it. Blocking an EXPLICIT desktop click on
    #    that flag is wrong — clicking the shortcut IS the operator's
    #    unquit signal. The flag is now ONLY consulted by the
    #    watchdog's restart actions (see luna_runtime_watchdog.
    #    watchdog_tick). Explicit invocations always proceed AND
    #    auto-clear the flag below.
    try:
        from luna_modules import luna_singleton as _sing
        from luna_modules import luna_launcher_health as _lh
        _other = _sing.is_another_instance_running(["LaunchLunaDashboard.pyw"])
        if _other.get("another_running"):
            # Only short-circuit if the other instance ACTUALLY brought
            # the dashboard up (port bound + /api/health 200). A stuck
            # other instance does not count.
            # 2026-05-16: widened timeout from 1.5s -> 8.0s after
            # discovering 4+ launcher zombies bound to port 8765
            # simultaneously. Root cause: /api/health composite payload
            # takes 6-7s under panel-polling load; the 1.5s timeout
            # falsely reported "dashboard not healthy" and the new
            # launcher took over via SO_REUSEADDR multi-bind instead of
            # cleanly exiting. 8s gives the composite path full headroom.
            _healthy_ok, _hr = _lh.wait_for_dashboard_healthy(
                host=hd.DEFAULT_HOST, port=hd.DEFAULT_PORT,
                total_wait_s=8.0, probe_interval_s=1.0)
            if _healthy_ok:
                _fatal_log("LaunchLunaDashboard.pyw: singleton skip — "
                            f"another launcher is alive AND dashboard is "
                            f"healthy at {hd.DEFAULT_HOST}:{hd.DEFAULT_PORT} "
                            f"(pids={[m['pid'] for m in _other['instances']]}); "
                            "opening browser at existing instance and exiting")
                # 2026-05-15 desktop-click visibility fix REVISED:
                # The first version of this branch unconditionally
                # spawned _open_in_app_mode + focus_luna_command_center
                # and exited. That works WHEN a Luna --app window
                # actually exists for Chromium's IPC singleton to
                # focus. It does NOT work when the user closed every
                # Luna window: Chromium's IPC sends the --app= request
                # to the running browser process, which has no Luna
                # window for that URL, and (depending on browser
                # version) may silently open the URL as a regular
                # background tab. The operator's click produces
                # nothing.
                #
                # Fix: check whether a Luna window currently exists
                # BEFORE deciding to skip. If yes -> spawn the focus
                # path. If no -> the singleton-alive sibling is
                # stuck/window-less; fall through to the normal-boot
                # path so a fresh --app window opens.
                _wf_module = None
                try:
                    from luna_modules import luna_window_focus as _wf_module
                except Exception:    # noqa: BLE001
                    _wf_module = None
                existing_luna_count = 0
                if _wf_module is not None:
                    try:
                        wins = _wf_module._find_luna_windows()  # noqa: SLF001
                        existing_luna_count = len(wins)
                    except Exception:    # noqa: BLE001
                        existing_luna_count = 0
                if existing_luna_count > 0:
                    try:
                        url = (f"http://{hd.DEFAULT_HOST}:{hd.DEFAULT_PORT}"
                                "/launcher-splash.html?next=%2F")
                        threading.Thread(target=_open_in_app_mode,
                                          args=(url,), daemon=True).start()
                        time.sleep(1.5)
                    except Exception:    # noqa: BLE001
                        pass
                    try:
                        if _wf_module is not None:
                            _wf_module.focus_luna_command_center()
                    except Exception:    # noqa: BLE001
                        pass
                    return 0
                # No Luna window exists. The singleton sibling is
                # alive but window-less. Fall through to the normal
                # boot path so a fresh --app window opens.
                _fatal_log(
                    "LaunchLunaDashboard.pyw: singleton sibling alive "
                    "but ZERO Luna windows exist; falling through to "
                    "normal boot so a fresh --app window opens"
                )
            # Other launcher alive but dashboard NOT healthy → it's
            # stuck. Log and proceed; this new instance will take over.
            _fatal_log("LaunchLunaDashboard.pyw: another launcher is "
                        f"alive but dashboard is NOT healthy at "
                        f"{hd.DEFAULT_HOST}:{hd.DEFAULT_PORT} "
                        f"(pids={[m['pid'] for m in _other['instances']]}); "
                        "this new instance will take over")
    except Exception:    # noqa: BLE001
        # Best-effort: if the helper isn't importable, fall through to
        # the legacy idempotency check below rather than strand Luna.
        pass
    # Auto-clear the operator-quit flag. An explicit launcher invocation
    # IS the operator's unquit signal — the flag was only meant to
    # suppress watchdog auto-restarts, not block desktop clicks.
    try:
        from luna_modules import luna_runtime_watchdog as _w
        if _w.operator_quit_active().get("active"):
            try:
                _w.OPERATOR_QUIT_FLAG_PATH.unlink()
                _fatal_log("LaunchLunaDashboard.pyw: explicit invocation "
                            "auto-cleared operator-quit flag (the click "
                            "is the unquit signal)")
            except FileNotFoundError:
                pass
            except OSError as exc:
                _fatal_log(f"LaunchLunaDashboard.pyw: could not clear "
                            f"operator-quit flag ({type(exc).__name__}: {exc})")
    except Exception:    # noqa: BLE001
        pass
    # 2026-05-13 Ctrl+F5 hard-refusal-of-connection fix: every step of
    # boot is wrapped, and the inner serve_forever is itself a
    # self-restarting loop (see luna_http_dashboard.py). The only way
    # this process exits is KeyboardInterrupt or 30+ consecutive bind
    # failures (logged loudly so the supervisor sees them).
    try:
        # 2026-05-17 cascade-mitigation (Serge 7-fix plan, Fix 4):
        # the three _ensure_*_running() calls that previously lived here
        # are DISABLED. They were the cascade-multiplier root: this
        # dashboard launcher used to spawn worker.py + repair_task_executor
        # + ANOTHER copy of the runtime watchdog every time it started.
        # Combined with the watchdog trying to restart the dashboard,
        # this created exponential respawn loops.
        #
        # New layer responsibilities:
        #   * worker.py            -> spawned by LaunchLuna.pyw only
        #   * aider_bridge.py      -> managed by luna_guardian.py only
        #   * repair_task_executor -> manually started if needed
        #   * runtime watchdog     -> WinSW LunaWatchdog service only
        #
        # This launcher's job is now exactly one thing: serve HTTP on
        # port 8765. No child processes.
        #
        # _ensure_worker_running()           # DISABLED (Fix 4)
        # _ensure_repair_executor_running()  # DISABLED (Fix 4)
        # _ensure_runtime_watchdog_running() # DISABLED (Fix 4)
        url = f"http://{hd.DEFAULT_HOST}:{hd.DEFAULT_PORT}/launcher-splash.html?next=%2F"

        # Idempotent boot: if the dashboard is already serving from a prior
        # launcher invocation (or from Luna_Dashboard_Restart.ps1, or from the
        # boot self-heal), do NOT try to bind a second copy. Just open the
        # browser at the existing instance and exit cleanly.
        # 2026-05-16: switched from single-shot _dashboard_already_serving
        # (1.0s minimum timeout) to wait_for_dashboard_healthy with
        # 10s total budget. The composite /api/health path takes 6-7s
        # under panel-polling load and a single 1s probe gave false-
        # negative "dashboard not serving" -> new launcher binds port
        # 8765 alongside the old one (SO_REUSEADDR multi-bind) -> port
        # 8765 ends up owned by 3-4 competing pythonw processes. The
        # 10s waiter sees the slow-but-alive dashboard correctly.
        try:
            from luna_modules import luna_launcher_health as _lh_chk
            _is_serving, _ = _lh_chk.wait_for_dashboard_healthy(
                host=hd.DEFAULT_HOST, port=hd.DEFAULT_PORT,
                total_wait_s=10.0, probe_interval_s=1.0)
        except Exception:  # noqa: BLE001
            _is_serving = _dashboard_already_serving(
                hd.DEFAULT_HOST, hd.DEFAULT_PORT)
        if _is_serving:
            # 2026-05-16 multi-window fix per Serge ("multiple terminals
            # open every click"): when the dashboard is already alive AND
            # a Luna window already exists, JUST focus it. Do NOT spawn
            # another Chromium --app= window. The previous behaviour
            # spawned an --app window unconditionally and then focused;
            # Chromium opens a NEW window on each --app= invocation when
            # the per-URL singleton fails (different Chrome versions
            # differ), so repeated clicks were stacking 2-4 Luna --app
            # windows on the operator's desktop.
            import time
            _wf_mod = None
            _existing_luna_windows = 0
            try:
                from luna_modules import luna_window_focus as _wf_mod
                try:
                    _wins = _wf_mod._find_luna_windows()  # noqa: SLF001
                    _existing_luna_windows = len(_wins or [])
                except Exception:  # noqa: BLE001
                    _existing_luna_windows = 0
            except Exception:  # noqa: BLE001
                _wf_mod = None
            if _existing_luna_windows > 0:
                _fatal_log(
                    "LaunchLunaDashboard.pyw: dashboard already serving "
                    f"AND {_existing_luna_windows} Luna window(s) already "
                    "exist; focusing existing instead of opening a new "
                    "--app window (multi-click idempotency)"
                )
                try:
                    if _wf_mod is not None:
                        _wf_mod.focus_luna_command_center()
                except Exception:  # noqa: BLE001
                    pass
                return 0
            # No Luna window exists yet — open one and then focus.
            threading.Thread(target=_open_in_app_mode,
                              args=(url,), daemon=True).start()
            time.sleep(1.5)
            try:
                if _wf_mod is not None:
                    _wf_mod.focus_luna_command_center()
            except Exception:    # noqa: BLE001
                pass
            return 0

        threading.Thread(target=_open_in_app_mode, args=(url,), daemon=True).start()
        # Boot-timing instrumentation: NEVER raises; one append-only log line.
        try:
            from luna_modules import luna_boot_timing as _bt
            _bt.mark("LaunchLunaDashboard.before_serve_forever")
        except Exception:
            pass
        hd.serve_forever(host=hd.DEFAULT_HOST, port=hd.DEFAULT_PORT)
        return 0
    except KeyboardInterrupt:
        return 0
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        try:
            import traceback as _tb
            _fatal_log(
                "LaunchLunaDashboard fatal: "
                f"{type(exc).__name__}: {exc}\n{_tb.format_exc()}"
            )
        except Exception:  # noqa: BLE001
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
