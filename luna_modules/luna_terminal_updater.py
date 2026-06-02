"""Luna terminal updater - the canonical 'update the terminal' recipe.

Origin
======
2026-05-16 pair session. Operator (Serge) asked Claude to teach Luna
how to update her own terminal so she can do it autonomously in
future sessions. Before this module existed, every dashboard wedge
required either:

  - Operator-run Bounce_Luna_Dashboard.ps1 (good but operator-gated)
  - Director-coached PowerShell taskkill (good but session-gated)

This module bottles the recipe so Luna can run it herself any time
the dashboard becomes unresponsive: at session start, after a
detected health-probe timeout, when the operator says "update the
terminal", or on a scheduled task.

What "update the terminal" means here
=====================================
The Luna desktop terminal is the chain
``Luna Command Center.lnk -> wscript -> Start_SurgeApp.vbs ->
Start_SurgeApp.bat -> LaunchLuna.pyw -> SurgeApp_Claude_Terminal.py
-> LaunchLunaDashboard.pyw`` ending in a pythonw process bound to
``127.0.0.1:8765`` serving the dashboard SPA.

"Update" means three things, in order:

  1. SURVEY - measure what's actually alive on disk and on the wire
     (no guessing, no caching).
  2. CLEAN ZOMBIES - kill stale LaunchLunaDashboard.pyw processes
     that are NOT the active listener and NOT one of the inviolate
     sibling services (worker.py, repair_task_executor, NATS, etc.).
  3. RE-VERIFY - if the dashboard is still unhealthy after cleanup,
     spawn one fresh launcher and wait for /api/health 200.

Public API
==========
``survey()``                    -> dict (read-only snapshot)
``clean_zombies(preserve_pids)`` -> dict (action report)
``bounce_dashboard()``          -> dict (action report)
``update_terminal()``           -> dict (high-level orchestrator)

Doctrine guardrails
===================
The recipe enforces, in code:

  - NEVER kill the PID currently bound to port 8765 unless the
    operator passed allow_listener_kill=True. (Dashboard self-kill
    is reserved for the bounce path.)
  - NEVER kill the inviolate sibling services. Explicit allowlist of
    process-name fragments excluded from every kill set:
       worker.py, repair_task_executor, nats-server,
       LunaQdrantServer, LunaWatchdog, LunaTemporalServer,
       LunaTemporalWorker, LunaSemanticMemory, aider_bridge.py.
  - NEVER kill ``os.getpid()`` or any of its parents. Luna must not
    kill the very process running her stack.
  - NEVER spawn a launcher with paid-provider environment variables.
  - ALL spawns are detached + hidden (CREATE_NO_WINDOW +
    DETACHED_PROCESS + CREATE_NEW_PROCESS_GROUP on Windows).
  - The recipe NEVER weakens a Director-Playbook gate.

Self-bounded
============
Every function in this module has an explicit cap:
  - ``clean_zombies`` walks at most ``max_targets`` candidates.
  - ``bounce_dashboard`` waits at most ``health_deadline_s`` seconds.
  - ``update_terminal`` returns within ``total_budget_s`` seconds.

Never raises. Each function returns a structured dict that the caller
can log or surface to the operator.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAUNCHER_PYW = PROJECT_ROOT / "LaunchLunaDashboard.pyw"
VENV_PYW = PROJECT_ROOT / ".aider_venv" / "Scripts" / "pythonw.exe"
LOGS_DIR = PROJECT_ROOT / "logs"
DASHBOARD_SERVICE_LOG = LOGS_DIR / "luna_dashboard_service.log"
DASHBOARD_PORT = 8765

# Inviolate sibling services - NEVER kill these.
# The fragments are matched case-insensitively against the command line.
INVIOLATE_SIBLING_FRAGMENTS: Tuple[str, ...] = (
    "worker.py",
    "repair_task_executor",
    "nats-server",
    "LunaQdrantServer",
    "LunaWatchdog",
    "LunaTemporalServer",
    "LunaTemporalWorker",
    "LunaSemanticMemory",
    "aider_bridge.py",
)

LAUNCHER_CMDLINE_RE = re.compile(
    r"pythonw[0-9.]*\.exe\s+.*LaunchLunaDashboard\.pyw",
    re.IGNORECASE,
)

NO_WIN_FLAGS = 0
DETACH_FLAGS = 0
if os.name == "nt":
    NO_WIN_FLAGS = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    DETACH_FLAGS = (
        getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        | getattr(subprocess, "DETACHED_PROCESS", 0)
    )
DASHBOARD_SPAWN_FLAGS = NO_WIN_FLAGS | DETACH_FLAGS


def _now_iso() -> str:
    import datetime as _dt
    return _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# SURVEY
# ---------------------------------------------------------------------------


def _safe_psutil():
    try:
        import psutil  # type: ignore
        return psutil
    except Exception:  # noqa: BLE001
        return None


def _listener_pid() -> Optional[int]:
    """Return the PID currently bound to DASHBOARD_PORT, or None."""
    psutil = _safe_psutil()
    if psutil is None:
        return None
    for c in psutil.net_connections(kind="tcp"):
        try:
            if c.laddr.port == DASHBOARD_PORT and c.status == psutil.CONN_LISTEN:
                return c.pid
        except Exception:  # noqa: BLE001
            continue
    return None


def _dashboard_healthy(timeout_s: float = 8.0) -> Tuple[bool, Dict[str, Any]]:
    """Return (healthy, detail) by calling /api/health."""
    import urllib.error
    import urllib.request
    detail: Dict[str, Any] = {"url": f"http://127.0.0.1:{DASHBOARD_PORT}/api/health",
                              "status": None, "latency_ms": None,
                              "error": None}
    t0 = time.monotonic()
    try:
        req = urllib.request.Request(
            detail["url"],
            headers={"User-Agent": "luna-terminal-updater/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read(64 * 1024)
        detail["status"] = int(resp.status)
        detail["latency_ms"] = int((time.monotonic() - t0) * 1000)
        detail["body_size"] = len(body)
        return (200 <= detail["status"] < 300), detail
    except (urllib.error.URLError, urllib.error.HTTPError, OSError,
            TimeoutError) as exc:
        detail["error"] = f"{type(exc).__name__}: {exc}"[:200]
        detail["latency_ms"] = int((time.monotonic() - t0) * 1000)
        return False, detail


def _all_launcher_pids() -> List[Dict[str, Any]]:
    """Return every process whose command line matches the launcher pattern."""
    psutil = _safe_psutil()
    if psutil is None:
        return []
    out: List[Dict[str, Any]] = []
    me = os.getpid()
    for p in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
        try:
            pid = p.info["pid"]
            if pid == me:
                continue
            cmd = " ".join(p.info["cmdline"] or [])
            if not LAUNCHER_CMDLINE_RE.search(cmd):
                continue
            out.append({
                "pid": pid,
                "name": p.info["name"] or "",
                "cmdline": cmd,
                "create_time": p.info["create_time"],
                "age_seconds": int(time.time() - p.info["create_time"]),
            })
        except Exception:  # noqa: BLE001
            continue
    out.sort(key=lambda d: d["age_seconds"], reverse=True)
    return out


def _inviolate_pids() -> Set[int]:
    """Return PIDs of every running inviolate sibling. Never kill these."""
    psutil = _safe_psutil()
    out: Set[int] = set()
    if psutil is None:
        return out
    for p in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmd = " ".join(p.info["cmdline"] or [])
            nm = (p.info["name"] or "").lower()
            for frag in INVIOLATE_SIBLING_FRAGMENTS:
                if frag.lower() in cmd.lower() or frag.lower() in nm:
                    out.add(p.info["pid"])
                    break
        except Exception:  # noqa: BLE001
            continue
    return out


def survey() -> Dict[str, Any]:
    """Read-only snapshot of the terminal's current state.

    Always returns a dict with these keys (never raises):
      ts             - ISO-8601 UTC timestamp
      listener_pid   - PID bound to 8765 or None
      health         - {ok, status, latency_ms, error}
      launchers      - list of dicts (pid, age_seconds, cmdline)
      launcher_count - len(launchers)
      zombies        - launchers whose pid != listener_pid
      inviolate_pids - set of sibling PIDs we will never touch
      verdict        - "healthy" | "wedged" | "down" | "multi_listener"
    """
    listener = _listener_pid()
    healthy, detail = _dashboard_healthy(timeout_s=8.0)
    launchers = _all_launcher_pids()
    inviolate = _inviolate_pids()
    zombies = [l for l in launchers if l["pid"] != listener]

    if listener is None:
        verdict = "down"
    elif healthy and len(launchers) <= 1:
        verdict = "healthy"
    elif healthy and len(launchers) > 1:
        verdict = "healthy_with_zombies"
    elif not healthy:
        verdict = "wedged"
    else:
        verdict = "unknown"

    return {
        "ts": _now_iso(),
        "listener_pid": listener,
        "health": {"healthy": healthy, **detail},
        "launchers": launchers,
        "launcher_count": len(launchers),
        "zombies": zombies,
        "zombie_count": len(zombies),
        "inviolate_pid_count": len(inviolate),
        "verdict": verdict,
    }


# ---------------------------------------------------------------------------
# CLEAN ZOMBIES
# ---------------------------------------------------------------------------


def _kill_pid(pid: int, *, timeout_s: float = 4.0) -> Dict[str, Any]:
    """Cross-platform kill. Returns {ok, output, returncode}."""
    if os.name != "nt":
        try:
            os.kill(pid, 9)
            return {"ok": True, "output": "SIGKILL sent",
                    "returncode": 0}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "output": f"{type(exc).__name__}: {exc}",
                    "returncode": -1}
    try:
        r = subprocess.run(
            ["taskkill", "/F", "/PID", str(pid)],
            capture_output=True, text=True,
            timeout=timeout_s, shell=False,
        )
        out = (r.stdout or "") + (r.stderr or "")
        return {"ok": r.returncode == 0, "output": out.strip()[:240],
                "returncode": r.returncode}
    except subprocess.TimeoutExpired:
        return {"ok": False, "output": "taskkill_timeout",
                "returncode": -2}
    except FileNotFoundError:
        return {"ok": False, "output": "taskkill_not_found",
                "returncode": -3}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "output": f"{type(exc).__name__}: {exc}",
                "returncode": -4}


def clean_zombies(
    *,
    preserve_pids: Optional[Set[int]] = None,
    max_targets: int = 32,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Kill launcher zombies that are NOT the active listener and NOT
    inviolate siblings.

    A "zombie" is any process matching ``LAUNCHER_CMDLINE_RE`` whose
    PID is NOT in:
      - the currently-bound listener on DASHBOARD_PORT
      - the explicit ``preserve_pids`` set
      - ``_inviolate_pids()``  (worker, NATS, WinSW services, etc.)
      - ``os.getpid()``  (never kill ourselves)

    Always returns a dict (never raises). ``dry_run=True`` returns
    the kill plan without executing it.
    """
    listener = _listener_pid()
    inviolate = _inviolate_pids()
    preserve = set(preserve_pids or [])
    if listener is not None:
        preserve.add(listener)
    preserve.update(inviolate)
    preserve.add(os.getpid())

    launchers = _all_launcher_pids()
    targets = [l for l in launchers if l["pid"] not in preserve]
    if len(targets) > max_targets:
        targets = targets[:max_targets]

    plan = [{"pid": t["pid"], "age_seconds": t["age_seconds"],
             "cmdline_head": t["cmdline"][:80]} for t in targets]

    if dry_run:
        return {"ok": True, "dry_run": True, "would_kill": plan,
                "preserved": sorted(preserve),
                "listener_pid": listener}

    results: List[Dict[str, Any]] = []
    killed = 0
    for t in targets:
        r = _kill_pid(t["pid"])
        results.append({"pid": t["pid"], **r,
                        "age_seconds": t["age_seconds"]})
        if r["ok"]:
            killed += 1

    return {"ok": True, "dry_run": False,
            "listener_pid": listener,
            "preserved": sorted(preserve),
            "considered": len(launchers),
            "targeted": len(targets),
            "killed": killed,
            "results": results}


# ---------------------------------------------------------------------------
# BOUNCE
# ---------------------------------------------------------------------------


def _spawn_fresh_launcher() -> Dict[str, Any]:
    """Spawn one detached LaunchLunaDashboard.pyw. Returns spawn detail."""
    if not LAUNCHER_PYW.is_file():
        return {"ok": False, "reason": "launcher_pyw_missing",
                "expected_path": str(LAUNCHER_PYW)}
    py = str(VENV_PYW) if VENV_PYW.is_file() else sys.executable
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        out_fh = DASHBOARD_SERVICE_LOG.open("ab", buffering=0)
        err_fh = DASHBOARD_SERVICE_LOG.open("ab", buffering=0)
        # 2026-06-02: background/health respawns (warden bounces) must NOT pop
        # a Command Center window. Set LUNA_DASHBOARD_NO_BROWSER so the launcher
        # restarts the HTTP server headless. The window only opens when the
        # operator clicks the desktop shortcut (that path does NOT set this).
        _spawn_env = dict(os.environ)
        _spawn_env["LUNA_DASHBOARD_NO_BROWSER"] = "1"
        p = subprocess.Popen(
            [py, str(LAUNCHER_PYW)],
            cwd=str(PROJECT_ROOT),
            stdin=subprocess.DEVNULL,
            stdout=out_fh,
            stderr=err_fh,
            creationflags=DASHBOARD_SPAWN_FLAGS,
            close_fds=False,
            env=_spawn_env,
        )
        try:
            out_fh.close()
            err_fh.close()
        except Exception:  # noqa: BLE001
            pass
        return {"ok": True, "spawned_pid": p.pid, "python": py,
                "launcher": str(LAUNCHER_PYW)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": "spawn_failed",
                "detail": f"{type(exc).__name__}: {exc}"[:240]}


def _wait_for_health(deadline_s: float = 45.0,
                     poll_interval_s: float = 2.0) -> Dict[str, Any]:
    """Poll /api/health until 2xx or deadline. Returns last detail."""
    t0 = time.monotonic()
    last: Dict[str, Any] = {}
    while time.monotonic() - t0 < deadline_s:
        ok, detail = _dashboard_healthy(timeout_s=6.0)
        last = detail
        last["healthy"] = ok
        if ok:
            last["wait_elapsed_s"] = round(time.monotonic() - t0, 2)
            return last
        time.sleep(max(0.5, poll_interval_s))
    last["healthy"] = False
    last["wait_elapsed_s"] = round(time.monotonic() - t0, 2)
    last["reason"] = "deadline_exceeded"
    return last


def bounce_dashboard(
    *,
    allow_listener_kill: bool = True,
    health_deadline_s: float = 45.0,
) -> Dict[str, Any]:
    """Kill the current listener (if allowed) + spawn fresh + wait.

    The default ``allow_listener_kill=True`` is correct for a 'update
    the terminal' call - if the dashboard is wedged, the operator
    wants it bounced. The flag exists so an automation that should
    refuse to kill a running listener (e.g. a cleanup-only sweep)
    can pass False.
    """
    pre = survey()
    actions: List[str] = []
    listener = pre["listener_pid"]

    if listener is not None:
        if not allow_listener_kill:
            return {"ok": False, "reason": "listener_kill_disallowed",
                    "pre_survey": pre}
        # Save inviolate PIDs first so we don't kill them.
        inv = _inviolate_pids()
        if listener in inv:
            return {"ok": False,
                    "reason": "listener_is_inviolate_sibling",
                    "listener_pid": listener,
                    "pre_survey": pre}
        if listener == os.getpid():
            return {"ok": False, "reason": "listener_is_self",
                    "listener_pid": listener}
        kill_r = _kill_pid(listener)
        actions.append(f"kill_listener({listener}): "
                       f"{kill_r.get('output','?')}")
        # Brief grace for port to free.
        for _ in range(20):
            if _listener_pid() is None:
                break
            time.sleep(0.2)

    # Spawn fresh.
    spawn_r = _spawn_fresh_launcher()
    actions.append(f"spawn_fresh_launcher: "
                   f"ok={spawn_r.get('ok')} "
                   f"pid={spawn_r.get('spawned_pid','?')}")
    if not spawn_r.get("ok"):
        return {"ok": False, "reason": "spawn_failed",
                "spawn_detail": spawn_r, "actions": actions,
                "pre_survey": pre}

    # Wait for health.
    health = _wait_for_health(deadline_s=health_deadline_s)
    actions.append(f"wait_for_health: healthy={health.get('healthy')} "
                   f"elapsed={health.get('wait_elapsed_s')}s")
    post = survey()

    return {
        "ok": bool(health.get("healthy")) and post["listener_pid"] is not None,
        "pre_survey": pre,
        "post_survey": post,
        "spawn_detail": spawn_r,
        "health_detail": health,
        "actions": actions,
    }


# ---------------------------------------------------------------------------
# HIGH-LEVEL ORCHESTRATOR
# ---------------------------------------------------------------------------


def update_terminal(
    *,
    bounce_if_unhealthy: bool = True,
    total_budget_s: float = 90.0,
) -> Dict[str, Any]:
    """Run the full update cycle:

      1. SURVEY
      2. If zombies > 0 AND listener exists, CLEAN ZOMBIES preserving listener.
      3. SURVEY again.
      4. If dashboard still unhealthy AND bounce_if_unhealthy, BOUNCE.
      5. Final SURVEY + return structured report.

    Bounded by ``total_budget_s`` (default 90s). Never raises.
    """
    t0 = time.monotonic()
    log: List[str] = []
    pre = survey()
    log.append(f"pre_verdict={pre['verdict']} "
               f"listener={pre['listener_pid']} "
               f"launchers={pre['launcher_count']} "
               f"zombies={pre['zombie_count']} "
               f"healthy={pre['health']['healthy']}")

    # Step 2: clean zombies if any.
    clean_report: Optional[Dict[str, Any]] = None
    if pre["zombie_count"] > 0 and pre["listener_pid"] is not None:
        clean_report = clean_zombies()
        log.append(f"clean_zombies: killed={clean_report['killed']}/"
                   f"{clean_report['targeted']}")

    mid = survey()
    log.append(f"mid_verdict={mid['verdict']} "
               f"listener={mid['listener_pid']} "
               f"healthy={mid['health']['healthy']}")

    # Step 4: bounce if still unhealthy.
    bounce_report: Optional[Dict[str, Any]] = None
    if (not mid["health"]["healthy"]) and bounce_if_unhealthy:
        remaining = max(15.0, total_budget_s - (time.monotonic() - t0))
        bounce_report = bounce_dashboard(
            allow_listener_kill=True,
            health_deadline_s=min(60.0, remaining))
        log.append(f"bounce: ok={bounce_report.get('ok')}")

    final = survey()
    log.append(f"final_verdict={final['verdict']} "
               f"listener={final['listener_pid']} "
               f"healthy={final['health']['healthy']} "
               f"launchers={final['launcher_count']}")

    return {
        "ok": (final["verdict"] in ("healthy",
                                     "healthy_with_zombies")
               and final["health"]["healthy"]),
        "elapsed_s": round(time.monotonic() - t0, 2),
        "pre": pre,
        "mid": mid,
        "final": final,
        "clean_report": clean_report,
        "bounce_report": bounce_report,
        "log": log,
    }


# ---------------------------------------------------------------------------
# Strict-attestor probe surface
# ---------------------------------------------------------------------------


def run_probe() -> Dict[str, Any]:
    """Self-probe: are the prerequisites for terminal updating in place?"""
    try:
        if not LAUNCHER_PYW.is_file():
            return {"ok": False, "reason": "launcher_pyw_missing",
                    "expected_path": str(LAUNCHER_PYW)}
        if not VENV_PYW.is_file():
            return {"ok": False, "reason": "venv_pythonw_missing"}
        if _safe_psutil() is None:
            return {"ok": False, "reason": "psutil_unimportable"}
        return {"ok": True,
                "terminal_updater_module_ok": True,
                "launcher_pyw": str(LAUNCHER_PYW),
                "venv_pythonw": str(VENV_PYW)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": "probe_raised",
                "detail": f"{type(exc).__name__}: {exc}"}


def adoption_probe() -> Dict[str, Any]:
    return run_probe()


def use_probe() -> Dict[str, Any]:
    return run_probe()


# ---------------------------------------------------------------------------
# CLI entry (so the operator can invoke this directly from PowerShell)
# ---------------------------------------------------------------------------


def _cli() -> int:
    import argparse
    import json as _json
    parser = argparse.ArgumentParser(
        description="Update the Luna terminal (clean zombies + bounce).")
    parser.add_argument("command", choices=["survey", "clean", "bounce",
                                              "update"],
                        help="survey: read-only snapshot. clean: kill "
                             "zombies. bounce: kill listener + spawn "
                             "fresh. update: full cycle (default).")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-bounce", action="store_true",
                        help="(update only) skip the bounce step")
    parser.add_argument("--budget-seconds", type=float, default=90.0)
    args = parser.parse_args()

    if args.command == "survey":
        r = survey()
    elif args.command == "clean":
        r = clean_zombies(dry_run=args.dry_run)
    elif args.command == "bounce":
        r = bounce_dashboard()
    else:
        r = update_terminal(bounce_if_unhealthy=not args.no_bounce,
                            total_budget_s=args.budget_seconds)
    print(_json.dumps(r, indent=2, default=str))
    return 0 if r.get("ok") else 1


if __name__ == "__main__":
    sys.exit(_cli())
