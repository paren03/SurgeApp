"""Luna dashboard warden - prevents the LaunchLunaDashboard.pyw memory leak.

Origin
======
2026-05-31 pair session. After the operator's desktop slowed to a crawl
because LaunchLunaDashboard.pyw had leaked 3.4 GB of RAM over ~15 hours
and its /api/health endpoint had wedged, the operator asked:

    "right a code in Luna to fix this in the future add it to her
     wokers and make sure it does not happen again. Create a worker
     that over sees this"

This module is that worker. It rides on top of the existing
``luna_terminal_updater`` recipe: that module is the BOUNCE engine,
this module is the THRESHOLD engine that decides when to call it.

Design
======
The warden is a PERIODIC TICK, not a long-running thread. The host
(worker.py main loop, or the CLI, or a scheduled task) calls
``tick()`` and the warden:

  1. Reads the kill-switch (state file). If disabled, returns early.
  2. Surveys the listener PID's RAM + /api/health latency + CLOSE_WAIT
     pile on the dashboard port.
  3. Decides whether to bounce, using these rules:
       - listener RAM > RAM_BOUNCE_MB                       -> bounce
       - consecutive health failures >= FAIL_THRESHOLD      -> bounce
       - CLOSE_WAIT sockets >= CLOSE_WAIT_BOUNCE_THRESHOLD  -> bounce
       - consecutive slow responses (>= LATENCY_SLOW_MS)
             >= SLOW_FAIL_THRESHOLD                         -> bounce
     subject to:
       - cooldown of MIN_INTERVAL_BETWEEN_BOUNCES_S
       - 24-hour cap of MAX_BOUNCES_PER_24H (soft — auto-disable for
         AUTO_DISABLE_COOLDOWN_S then auto-re-enable)
       - listener is NOT an inviolate sibling
  4. If a bounce is warranted, calls ``luna_terminal_updater.bounce_dashboard()``.
  5. Records every observation + every bounce to a JSONL audit log.

Doctrine guardrails
===================
This module ENFORCES, in code:

  - NEVER raises. Every public function returns a structured dict.
    Worker tick loops must never be broken by a warden failure.
  - NEVER bounces an inviolate sibling.
  - NEVER bounces more than ``MAX_BOUNCES_PER_24H`` in 24 hours.
    On hitting the cap, the warden AUTO-DISABLES itself and logs
    why - that pattern implies something is fundamentally broken
    and the operator should investigate, not have the warden flap.
    Auto-disable is SOFT: after AUTO_DISABLE_COOLDOWN_S (default 6h)
    the warden auto-re-enables itself, so the dashboard is never
    permanently unprotected. Operator-initiated disable is preserved.
  - Respects an operator-set kill switch
    (``memory/luna_dashboard_warden.disabled`` file presence = OFF).
  - Bounce cooldown of ``MIN_INTERVAL_BETWEEN_BOUNCES_S`` prevents
    flapping when the dashboard repeatedly fails right after a fresh
    spawn.

Public API
==========
``tick()``           -> dict     ; main entry, call this from worker loop
``status()``         -> dict     ; read-only snapshot for dashboards
``survey()``         -> dict     ; raw measurements (RAM + health)
``should_bounce()``  -> (bool, str) ; pure decision, no side effects
``bounce_now(reason)`` -> dict   ; force a bounce (CLI / operator)
``is_disabled()``    -> bool     ; kill-switch check
``disable(reason)``  -> dict     ; create the kill-switch file
``enable()``         -> dict     ; remove the kill-switch file
``run_probe()``      -> dict     ; strict-attestor self-test

CLI
===
``python -m luna_modules.luna_dashboard_warden {tick|status|survey|
    bounce|disable|enable|reset}``
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MEMORY_DIR = PROJECT_ROOT / "memory"
LOGS_DIR = PROJECT_ROOT / "logs"

STATE_PATH = MEMORY_DIR / "luna_dashboard_warden_state.json"
KILL_SWITCH_PATH = MEMORY_DIR / "luna_dashboard_warden.disabled"
AUDIT_LOG_PATH = LOGS_DIR / "luna_dashboard_warden.jsonl"

# --- Thresholds. Raised 2026-06-02 to account for XTTS + 8B brain loading ---
# Original 1000 MB was set before the voice-clone (XTTS ~1.5 GB in-process)
# and cognitive models were added. The dashboard legitimately uses 1.1–2.5 GB
# when XTTS + brain models are resident. Bouncing at 1 GB was killing the
# dashboard the moment voice became active, causing "dashboard offline" errors.
# New threshold: 3000 MB gives headroom for all loaded models; genuine leaks
# (RAM growing without bound) would exceed this and still get caught.
RAM_WARN_MB = 1500           # log a warning, no action
RAM_BOUNCE_MB = 3000         # auto-bounce (was 1000 — too low for XTTS+brain)
HEALTH_TIMEOUT_S = 4.0       # /api/health request timeout
FAIL_THRESHOLD = 2           # consecutive health failures before bounce
MIN_INTERVAL_BETWEEN_BOUNCES_S = 600    # 10 minutes
MAX_BOUNCES_PER_24H = 6
TICK_BUDGET_S = 30.0         # the tick itself must finish within this

# --- 2026-06-01 additions: degraded-state triggers. ---
# Origin: 2026-05-31 23:08 UTC operator hit "dashboard offline - message
# not delivered" while the warden was auto-disabled (had hit 6/24h cap at
# 18:40Z). Even when the warden IS active, RAM + complete-health-failure
# triggers MISS the degraded-but-listening wedge: socket-pile + slow
# /api/health responses. These two triggers + soft auto-disable cooldown
# close that gap.
DASHBOARD_PORT = 8765
CLOSE_WAIT_WARN = 5          # log warn at this many CLOSE_WAIT sockets
CLOSE_WAIT_BOUNCE_THRESHOLD = 10   # auto-bounce above this
LATENCY_SLOW_MS = 3000       # /api/health >= this counts as "slow"
SLOW_FAIL_THRESHOLD = 3      # consecutive slow responses before bounce
AUTO_DISABLE_COOLDOWN_S = 6 * 3600   # soft auto-disable: re-enable after 6h

# --- internal --------------------------------------------------------------


def _now_iso() -> str:
    import datetime as _dt
    return _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _now_epoch() -> float:
    return time.time()


def _safe_psutil():
    try:
        import psutil  # type: ignore
        return psutil
    except Exception:  # noqa: BLE001
        return None


def _read_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.is_file():
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        pass
    return {
        "version": 2,
        "first_seen_ts": _now_iso(),
        "tick_count": 0,
        "consecutive_health_failures": 0,
        "consecutive_slow_responses": 0,
        "last_tick_ts": None,
        "last_tick_verdict": None,
        "last_listener_pid": None,
        "last_listener_ram_mb": None,
        "last_health_ms": None,
        "last_health_ok": None,
        "last_close_wait_count": None,
        "last_bounce_ts": None,
        "last_bounce_reason": None,
        "last_bounce_ok": None,
        "bounce_history": [],          # list[{ts, reason, ok}], capped
        "auto_disabled": False,
        "auto_disabled_reason": None,
        "auto_disabled_ts_epoch": None,
        "auto_reenabled_history": [],  # list[{ts, after_cooldown_s}], capped
    }


def _write_state(state: Dict[str, Any]) -> None:
    try:
        MEMORY_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, indent=2, default=str),
                       encoding="utf-8")
        tmp.replace(STATE_PATH)
    except Exception:  # noqa: BLE001
        pass


def _audit(record: Dict[str, Any]) -> None:
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"ts": _now_iso(), **record}, default=str)
        with AUDIT_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:  # noqa: BLE001
        pass


def _bounces_within(state: Dict[str, Any], seconds: float) -> int:
    cutoff = _now_epoch() - seconds
    count = 0
    for h in state.get("bounce_history", []):
        try:
            ts = float(h.get("ts_epoch", 0))
            if ts >= cutoff:
                count += 1
        except Exception:  # noqa: BLE001
            continue
    return count


# --- measurements ----------------------------------------------------------


def _listener_pid() -> Optional[int]:
    """Return PID bound to the dashboard port (delegates to terminal_updater)."""
    try:
        from luna_modules import luna_terminal_updater as _tu
        return _tu._listener_pid()  # noqa: SLF001 (intentional)
    except Exception:  # noqa: BLE001
        return None


def _process_ram_mb(pid: Optional[int]) -> Optional[float]:
    if pid is None:
        return None
    psutil = _safe_psutil()
    if psutil is None:
        return None
    try:
        p = psutil.Process(pid)
        return round(p.memory_info().rss / (1024 * 1024), 1)
    except Exception:  # noqa: BLE001
        return None


def _socket_pile_count(port: int = DASHBOARD_PORT) -> Optional[int]:
    """Count CLOSE_WAIT sockets on the dashboard port. None on failure.

    CLOSE_WAIT means the remote (browser/curl) sent FIN but the server
    hasn't called close() yet. A pile of these is the leading indicator
    that the listener's handler loop is stuck — it can't drain finished
    connections fast enough. The listener will appear "up" (TCP bound,
    /api/health may still respond if barely) right up until it
    fully wedges. Catching this early prevents the silent-degrade
    state the operator hit on 2026-05-31 at 18:08:44.
    """
    psutil = _safe_psutil()
    if psutil is None:
        return None
    try:
        count = 0
        for c in psutil.net_connections(kind="tcp"):
            try:
                laddr = c.laddr
                if not laddr:
                    continue
                # laddr can be a 2-tuple or namedtuple with .port
                lport = getattr(laddr, "port", None) or (
                    laddr[1] if len(laddr) > 1 else None)
                if lport != port:
                    continue
                status = getattr(c, "status", "") or ""
                if status.upper() == "CLOSE_WAIT":
                    count += 1
            except Exception:  # noqa: BLE001
                continue
        return count
    except Exception:  # noqa: BLE001
        return None


def _health_probe(timeout_s: float = HEALTH_TIMEOUT_S
                  ) -> Tuple[bool, Optional[int], Optional[str]]:
    """Return (healthy, latency_ms, error_str).

    2026-05-31 root-cause fix: the dashboard's /api/health route can return
    HTTP 200 OK with body.ok=false when the legacy code path hit a
    BuilderPoolSaturated condition. The luna_terminal_updater helper only
    checked HTTP status — fooling the warden into thinking a wedged
    dashboard was healthy. We now ALSO parse the JSON body and only
    declare the dashboard healthy when BOTH 2xx HTTP status AND body.ok
    is truthy. Falls back to the legacy helper's check if JSON parsing
    fails (defense in depth — never raises into the warden tick).
    """
    import json as _json
    import urllib.request as _ur
    import urllib.error as _ue

    url = "http://127.0.0.1:8765/api/health"
    t0 = time.monotonic()
    latency_ms: Optional[int] = None
    try:
        req = _ur.Request(url, headers={"User-Agent": "luna-warden/1.0"})
        with _ur.urlopen(req, timeout=timeout_s) as resp:
            status = int(resp.status)
            body_bytes = resp.read(64 * 1024)
        latency_ms = int((time.monotonic() - t0) * 1000)
        if not (200 <= status < 300):
            return (False, latency_ms, f"http_{status}")
        # Parse body — if body.ok is explicitly False, the dashboard is
        # degraded (BuilderPoolSaturated etc.) even though TCP/HTTP work.
        try:
            body = _json.loads(body_bytes.decode("utf-8", errors="replace"))
        except Exception:  # noqa: BLE001
            # Body unparseable — treat as healthy if HTTP was 2xx (the
            # endpoint may have legitimately returned non-JSON during a
            # transitional state; don't flap on parse errors alone).
            return (True, latency_ms, None)
        if isinstance(body, dict):
            body_ok = body.get("ok")
            if body_ok is False:
                src = body.get("source_status") or body.get("error_kind") or "degraded"
                return (False, latency_ms, f"body_ok_false:{src}")
        return (True, latency_ms, None)
    except (_ue.URLError, _ue.HTTPError, OSError, TimeoutError) as exc:
        latency_ms = int((time.monotonic() - t0) * 1000)
        return (False, latency_ms, f"{type(exc).__name__}: {str(exc)[:120]}")
    except Exception as exc:  # noqa: BLE001
        return (False, latency_ms, f"{type(exc).__name__}: {str(exc)[:120]}")


def survey() -> Dict[str, Any]:
    """Read-only snapshot of dashboard health + RAM + socket-pile.

    Never raises. ``close_wait_count`` and ``health_slow`` are 2026-06-01
    additions; old callers can ignore the new keys.
    """
    try:
        pid = _listener_pid()
        ram_mb = _process_ram_mb(pid)
        ok, latency_ms, err = _health_probe()
        close_wait = _socket_pile_count()
        # "Slow" = HTTP 200 returned but latency above LATENCY_SLOW_MS.
        # Distinct from health_ok=False (which means TCP/HTTP failed
        # entirely). Slow means the server is alive but degraded.
        slow = bool(ok and latency_ms is not None
                    and latency_ms >= LATENCY_SLOW_MS)
        return {
            "ts": _now_iso(),
            "listener_pid": pid,
            "listener_ram_mb": ram_mb,
            "health_ok": ok,
            "health_latency_ms": latency_ms,
            "health_error": err,
            "health_slow": slow,
            "close_wait_count": close_wait,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ts": _now_iso(),
            "error": f"{type(exc).__name__}: {exc}",
        }


# --- decision --------------------------------------------------------------


def _is_inviolate_listener(pid: Optional[int]) -> bool:
    if pid is None:
        return False
    try:
        from luna_modules import luna_terminal_updater as _tu
        return pid in _tu._inviolate_pids()  # noqa: SLF001
    except Exception:  # noqa: BLE001
        return False


def should_bounce(
    state: Optional[Dict[str, Any]] = None,
    measurement: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """Pure decision. Returns (decision, reason).

    Never raises. ``state`` and ``measurement`` are accepted to keep
    this testable in isolation - if omitted they're read live.
    """
    try:
        st = state if state is not None else _read_state()
        m = measurement if measurement is not None else survey()
    except Exception as exc:  # noqa: BLE001
        return False, f"survey_failed: {exc}"

    if st.get("auto_disabled"):
        return False, ("auto_disabled: "
                       + str(st.get("auto_disabled_reason") or "?"))

    # Cooldown after the most recent bounce.
    last_bounce_epoch = 0.0
    for h in reversed(st.get("bounce_history", []) or []):
        try:
            last_bounce_epoch = float(h.get("ts_epoch", 0))
            break
        except Exception:  # noqa: BLE001
            continue
    if last_bounce_epoch > 0:
        since = _now_epoch() - last_bounce_epoch
        if since < MIN_INTERVAL_BETWEEN_BOUNCES_S:
            return False, (f"cooldown: only {int(since)}s since last bounce "
                           f"(min {MIN_INTERVAL_BETWEEN_BOUNCES_S}s)")

    # 24h cap.
    recent = _bounces_within(st, 24 * 3600)
    if recent >= MAX_BOUNCES_PER_24H:
        return False, (f"24h_cap: already bounced {recent} times in 24h "
                       f"(max {MAX_BOUNCES_PER_24H})")

    pid = m.get("listener_pid")
    ram_mb = m.get("listener_ram_mb")
    health_ok = bool(m.get("health_ok"))

    # Never touch an inviolate listener (defense in depth).
    if _is_inviolate_listener(pid):
        return False, f"listener_inviolate: pid={pid}"

    # No listener -> no bounce (a bounce would only re-spawn; the
    # operator-installed launcher chain should handle "down" state).
    if pid is None:
        return False, "no_listener"

    # Trigger: RAM growth.
    if ram_mb is not None and ram_mb >= RAM_BOUNCE_MB:
        return True, f"ram_exceeded: {ram_mb} MB >= {RAM_BOUNCE_MB} MB"

    # Trigger: repeated health failure.
    fails = int(st.get("consecutive_health_failures", 0) or 0)
    if not health_ok and fails >= FAIL_THRESHOLD:
        return True, (f"health_failed: {fails} consecutive failures "
                      f">= {FAIL_THRESHOLD}")

    # Trigger: CLOSE_WAIT socket pile (2026-06-01). A pile is a pile —
    # one observation is enough. The kernel doesn't accumulate this
    # state transiently; if we see N stuck CLOSE_WAITs, the listener's
    # handler loop is stuck. Bounce immediately.
    close_wait = m.get("close_wait_count")
    if (close_wait is not None
            and close_wait >= CLOSE_WAIT_BOUNCE_THRESHOLD):
        return True, (f"socket_pile: {close_wait} CLOSE_WAIT sockets "
                      f"on port {DASHBOARD_PORT} "
                      f">= {CLOSE_WAIT_BOUNCE_THRESHOLD}")

    # Trigger: sustained slow /api/health (2026-06-01). HTTP 200 but
    # latency above LATENCY_SLOW_MS for SLOW_FAIL_THRESHOLD ticks in
    # a row. Catches the "server is alive but the dashboard takes
    # 7 seconds to answer a health check" state.
    slow_count = int(st.get("consecutive_slow_responses", 0) or 0)
    if slow_count >= SLOW_FAIL_THRESHOLD:
        return True, (f"health_slow: {slow_count} consecutive slow "
                      f"responses (>= {LATENCY_SLOW_MS}ms) "
                      f">= {SLOW_FAIL_THRESHOLD}")

    return False, (f"healthy: ram={ram_mb}MB latency_ms="
                   f"{m.get('health_latency_ms')} ok={health_ok} "
                   f"close_wait={close_wait} slow_streak={slow_count}")


# --- action ----------------------------------------------------------------


def _do_bounce(reason: str) -> Dict[str, Any]:
    """Call the canonical bounce recipe. Always returns a dict."""
    try:
        from luna_modules import luna_terminal_updater as _tu
        r = _tu.bounce_dashboard(allow_listener_kill=True,
                                 health_deadline_s=45.0)
        return {
            "ok": bool(r.get("ok")),
            "reason_requested": reason,
            "elapsed_s": r.get("health_detail", {}).get("wait_elapsed_s"),
            "bounce_actions": r.get("actions"),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason_requested": reason,
            "error": f"{type(exc).__name__}: {exc}",
        }


def bounce_now(reason: str = "operator_request") -> Dict[str, Any]:
    """Force a bounce regardless of thresholds (still subject to cooldown
    if reason starts with 'forced_' you can bypass cooldown - use sparingly).
    """
    state = _read_state()
    force = reason.startswith("forced_")
    if not force:
        recent = _bounces_within(state, 24 * 3600)
        if recent >= MAX_BOUNCES_PER_24H:
            return {"ok": False,
                    "skipped": "24h_cap",
                    "recent_bounce_count_24h": recent}
    r = _do_bounce(reason)
    _record_bounce(state, reason, r.get("ok", False))
    _audit({"event": "bounce", "reason": reason, "forced": force, **r})
    return r


def _record_bounce(state: Dict[str, Any], reason: str, ok: bool) -> None:
    history = list(state.get("bounce_history") or [])
    history.append({
        "ts": _now_iso(),
        "ts_epoch": _now_epoch(),
        "reason": reason,
        "ok": ok,
    })
    history = history[-50:]  # cap
    state["bounce_history"] = history
    state["last_bounce_ts"] = history[-1]["ts"]
    state["last_bounce_reason"] = reason
    state["last_bounce_ok"] = ok
    # Reset consecutive failure counter after any bounce attempt.
    state["consecutive_health_failures"] = 0
    # Auto-disable if we hit the 24h cap RIGHT NOW after this bounce.
    if _bounces_within(state, 24 * 3600) >= MAX_BOUNCES_PER_24H:
        state["auto_disabled"] = True
        state["auto_disabled_reason"] = (
            f"hit MAX_BOUNCES_PER_24H={MAX_BOUNCES_PER_24H} "
            f"at {_now_iso()}")
        state["auto_disabled_ts_epoch"] = _now_epoch()
        try:
            KILL_SWITCH_PATH.write_text(
                f"AUTO-DISABLED by warden at {_now_iso()}: "
                + state["auto_disabled_reason"]
                + f"\n(will auto-re-enable after "
                + f"{AUTO_DISABLE_COOLDOWN_S}s cooldown)\n",
                encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass
        _audit({"event": "auto_disabled",
                "reason": state["auto_disabled_reason"],
                "cooldown_s": AUTO_DISABLE_COOLDOWN_S})
    _write_state(state)


# --- kill switch -----------------------------------------------------------


def _maybe_auto_reenable() -> Dict[str, Any]:
    """Soft auto-disable: if the warden auto-disabled itself AND
    AUTO_DISABLE_COOLDOWN_S has elapsed since, re-enable automatically.

    This prevents the warden from permanently abandoning the dashboard
    after a flap-day. The dashboard is the user-facing surface; going
    silently unprotected for >6 hours is the failure mode operator
    Serge hit on 2026-05-31 (auto-disabled 18:40Z, dashboard wedged by
    23:08Z). Operator-initiated disable (kill switch with no auto_disabled
    flag) is preserved — only auto-disable is auto-recovered.

    Returns a dict describing what happened (safe for callers to ignore).
    """
    try:
        state = _read_state()
        if not state.get("auto_disabled"):
            return {"ok": True, "reenabled": False,
                    "reason": "not_auto_disabled"}
        ts_epoch = state.get("auto_disabled_ts_epoch")
        if ts_epoch is None:
            # Old state file without ts — assume cooldown elapsed.
            elapsed = AUTO_DISABLE_COOLDOWN_S + 1
        else:
            try:
                elapsed = _now_epoch() - float(ts_epoch)
            except Exception:  # noqa: BLE001
                elapsed = AUTO_DISABLE_COOLDOWN_S + 1
        if elapsed < AUTO_DISABLE_COOLDOWN_S:
            return {"ok": True, "reenabled": False,
                    "reason": "cooldown_active",
                    "elapsed_s": int(elapsed),
                    "cooldown_s": AUTO_DISABLE_COOLDOWN_S}
        # Re-enable: clear flag + remove the kill switch the AUTO path
        # wrote. Bounce history is preserved — the 24h cap will naturally
        # roll forward as old bounces age out.
        state["auto_disabled"] = False
        state["auto_disabled_reason"] = None
        state["auto_disabled_ts_epoch"] = None
        history = list(state.get("auto_reenabled_history") or [])
        history.append({"ts": _now_iso(),
                        "after_cooldown_s": int(elapsed)})
        state["auto_reenabled_history"] = history[-20:]
        _write_state(state)
        try:
            if KILL_SWITCH_PATH.exists():
                txt = KILL_SWITCH_PATH.read_text(encoding="utf-8",
                                                 errors="replace")
                # Only remove if WE auto-wrote it — leave operator's
                # manual disable untouched.
                if "AUTO-DISABLED by warden" in txt:
                    KILL_SWITCH_PATH.unlink()
        except Exception:  # noqa: BLE001
            pass
        _audit({"event": "auto_reenabled",
                "after_cooldown_s": int(elapsed),
                "cooldown_s": AUTO_DISABLE_COOLDOWN_S})
        return {"ok": True, "reenabled": True,
                "after_cooldown_s": int(elapsed)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False,
                "reenabled": False,
                "error": f"{type(exc).__name__}: {exc}"}


def is_disabled() -> bool:
    """True if the operator (or auto-disable) has turned the warden off.

    Calls _maybe_auto_reenable() first so the soft cooldown can
    auto-recover the warden without operator action.
    """
    try:
        _maybe_auto_reenable()
        return KILL_SWITCH_PATH.exists()
    except Exception:  # noqa: BLE001
        return False


def disable(reason: str = "operator_request") -> Dict[str, Any]:
    try:
        MEMORY_DIR.mkdir(parents=True, exist_ok=True)
        KILL_SWITCH_PATH.write_text(
            f"DISABLED at {_now_iso()}: {reason}\n", encoding="utf-8")
        _audit({"event": "disable", "reason": reason})
        return {"ok": True, "disabled": True, "path": str(KILL_SWITCH_PATH)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def enable() -> Dict[str, Any]:
    try:
        if KILL_SWITCH_PATH.exists():
            KILL_SWITCH_PATH.unlink()
        # Clear the auto_disabled flag in state too.
        state = _read_state()
        if state.get("auto_disabled"):
            state["auto_disabled"] = False
            state["auto_disabled_reason"] = None
            _write_state(state)
        _audit({"event": "enable"})
        return {"ok": True, "disabled": False}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# --- main tick (called by worker) ------------------------------------------


def tick() -> Dict[str, Any]:
    """Run one warden cycle. Called periodically by the worker loop.

    Always returns a dict (never raises). The dict has these keys:
      ts               - ISO timestamp
      verdict          - "disabled" | "ok" | "warned" | "bounced" |
                         "bounce_skipped" | "bounce_failed" | "error"
      reason           - explanation string
      measurement      - the survey() output
      bounce_result    - present iff a bounce was attempted
      tick_count       - cumulative
    """
    t0 = time.monotonic()
    try:
        if is_disabled():
            return {
                "ts": _now_iso(),
                "verdict": "disabled",
                "reason": "kill_switch_present",
                "kill_switch_path": str(KILL_SWITCH_PATH),
            }

        state = _read_state()
        m = survey()

        # Update measurement-derived state.
        state["tick_count"] = int(state.get("tick_count", 0) or 0) + 1
        state["last_tick_ts"] = m.get("ts")
        state["last_listener_pid"] = m.get("listener_pid")
        state["last_listener_ram_mb"] = m.get("listener_ram_mb")
        state["last_health_ms"] = m.get("health_latency_ms")
        state["last_health_ok"] = m.get("health_ok")
        state["last_close_wait_count"] = m.get("close_wait_count")

        if m.get("health_ok"):
            state["consecutive_health_failures"] = 0
        else:
            state["consecutive_health_failures"] = int(
                state.get("consecutive_health_failures", 0) or 0) + 1

        # Slow-response streak: bumps when 200 OK + slow; resets when
        # latency drops below LATENCY_SLOW_MS OR when health_ok=False
        # (a full failure is counted in consecutive_health_failures
        # instead — we don't want to double-count one slow tick as both).
        if m.get("health_slow"):
            state["consecutive_slow_responses"] = int(
                state.get("consecutive_slow_responses", 0) or 0) + 1
        elif m.get("health_ok"):
            state["consecutive_slow_responses"] = 0
        # else: failure case — leave the slow counter alone, the failure
        # counter is what's responsible for triggering.

        # Decision.
        do_bounce, reason = should_bounce(state, m)

        # Warning (no action) for elevated RAM.
        ram_mb = m.get("listener_ram_mb")
        warned = False
        if (ram_mb is not None and ram_mb >= RAM_WARN_MB
                and ram_mb < RAM_BOUNCE_MB):
            warned = True
            _audit({"event": "warn_ram",
                    "listener_pid": m.get("listener_pid"),
                    "ram_mb": ram_mb,
                    "threshold_mb": RAM_WARN_MB})

        # Warning (no action) for CLOSE_WAIT socket pile that hasn't
        # crossed the bounce threshold yet.
        cw = m.get("close_wait_count")
        if (cw is not None and cw >= CLOSE_WAIT_WARN
                and cw < CLOSE_WAIT_BOUNCE_THRESHOLD):
            warned = True
            _audit({"event": "warn_close_wait",
                    "listener_pid": m.get("listener_pid"),
                    "close_wait_count": cw,
                    "warn_threshold": CLOSE_WAIT_WARN,
                    "bounce_threshold": CLOSE_WAIT_BOUNCE_THRESHOLD})

        # Warning (no action) for a slow but successful /api/health.
        if m.get("health_slow"):
            warned = True
            _audit({"event": "warn_health_slow",
                    "listener_pid": m.get("listener_pid"),
                    "latency_ms": m.get("health_latency_ms"),
                    "slow_threshold_ms": LATENCY_SLOW_MS,
                    "consecutive_slow_responses":
                        state.get("consecutive_slow_responses")})

        bounce_result: Optional[Dict[str, Any]] = None
        verdict: str
        if do_bounce:
            bounce_result = _do_bounce(reason)
            _record_bounce(state, reason, bounce_result.get("ok", False))
            _audit({"event": "bounce_triggered",
                    "reason": reason,
                    "ok": bounce_result.get("ok"),
                    "listener_pid_before": m.get("listener_pid"),
                    "ram_mb_before": ram_mb,
                    "details": bounce_result})
            verdict = ("bounced"
                       if bounce_result.get("ok") else "bounce_failed")
        else:
            verdict = "warned" if warned else "ok"

        state["last_tick_verdict"] = verdict
        _write_state(state)

        return {
            "ts": _now_iso(),
            "verdict": verdict,
            "reason": reason,
            "measurement": m,
            "bounce_result": bounce_result,
            "tick_count": state["tick_count"],
            "consecutive_health_failures":
                state["consecutive_health_failures"],
            "consecutive_slow_responses":
                state.get("consecutive_slow_responses"),
            "elapsed_s": round(time.monotonic() - t0, 3),
        }
    except Exception as exc:  # noqa: BLE001
        _audit({"event": "tick_error",
                "error": f"{type(exc).__name__}: {exc}"})
        return {
            "ts": _now_iso(),
            "verdict": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_s": round(time.monotonic() - t0, 3),
        }


def status() -> Dict[str, Any]:
    """Read-only status snapshot for dashboards. Never raises."""
    try:
        # is_disabled() will fire _maybe_auto_reenable() on read, so
        # callers see the freshest state.
        disabled = is_disabled()
        state = _read_state()
        return {
            "ts": _now_iso(),
            "disabled": disabled,
            "auto_disabled": state.get("auto_disabled"),
            "auto_disabled_reason": state.get("auto_disabled_reason"),
            "auto_disabled_ts_epoch":
                state.get("auto_disabled_ts_epoch"),
            "tick_count": state.get("tick_count"),
            "last_tick_ts": state.get("last_tick_ts"),
            "last_tick_verdict": state.get("last_tick_verdict"),
            "last_listener_pid": state.get("last_listener_pid"),
            "last_listener_ram_mb": state.get("last_listener_ram_mb"),
            "last_health_ms": state.get("last_health_ms"),
            "last_health_ok": state.get("last_health_ok"),
            "last_close_wait_count": state.get("last_close_wait_count"),
            "consecutive_health_failures":
                state.get("consecutive_health_failures"),
            "consecutive_slow_responses":
                state.get("consecutive_slow_responses"),
            "last_bounce_ts": state.get("last_bounce_ts"),
            "last_bounce_reason": state.get("last_bounce_reason"),
            "last_bounce_ok": state.get("last_bounce_ok"),
            "bounces_last_24h": _bounces_within(state, 24 * 3600),
            "max_bounces_per_24h": MAX_BOUNCES_PER_24H,
            "auto_reenabled_count":
                len(state.get("auto_reenabled_history") or []),
            "auto_reenabled_history":
                (state.get("auto_reenabled_history") or [])[-3:],
            "thresholds": {
                "ram_warn_mb": RAM_WARN_MB,
                "ram_bounce_mb": RAM_BOUNCE_MB,
                "health_timeout_s": HEALTH_TIMEOUT_S,
                "fail_threshold": FAIL_THRESHOLD,
                "min_interval_between_bounces_s":
                    MIN_INTERVAL_BETWEEN_BOUNCES_S,
                "close_wait_warn": CLOSE_WAIT_WARN,
                "close_wait_bounce_threshold":
                    CLOSE_WAIT_BOUNCE_THRESHOLD,
                "latency_slow_ms": LATENCY_SLOW_MS,
                "slow_fail_threshold": SLOW_FAIL_THRESHOLD,
                "auto_disable_cooldown_s": AUTO_DISABLE_COOLDOWN_S,
            },
        }
    except Exception as exc:  # noqa: BLE001
        return {"ts": _now_iso(),
                "error": f"{type(exc).__name__}: {exc}"}


# --- probe surface ---------------------------------------------------------


def run_probe() -> Dict[str, Any]:
    """Self-probe: are prerequisites in place?"""
    try:
        from luna_modules import luna_terminal_updater  # noqa: F401
        if _safe_psutil() is None:
            return {"ok": False, "reason": "psutil_unimportable"}
        if not MEMORY_DIR.exists():
            return {"ok": False, "reason": "memory_dir_missing",
                    "expected": str(MEMORY_DIR)}
        # A no-op tick attempt - if survey/state read break, fail here.
        _ = _read_state()
        _ = survey()
        return {"ok": True,
                "warden_module_ok": True,
                "state_path": str(STATE_PATH),
                "audit_log_path": str(AUDIT_LOG_PATH),
                "kill_switch_path": str(KILL_SWITCH_PATH),
                "thresholds": {
                    "ram_bounce_mb": RAM_BOUNCE_MB,
                    "fail_threshold": FAIL_THRESHOLD,
                    "min_interval_between_bounces_s":
                        MIN_INTERVAL_BETWEEN_BOUNCES_S,
                    "max_bounces_per_24h": MAX_BOUNCES_PER_24H,
                    "close_wait_bounce_threshold":
                        CLOSE_WAIT_BOUNCE_THRESHOLD,
                    "latency_slow_ms": LATENCY_SLOW_MS,
                    "slow_fail_threshold": SLOW_FAIL_THRESHOLD,
                    "auto_disable_cooldown_s":
                        AUTO_DISABLE_COOLDOWN_S,
                }}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": "probe_raised",
                "detail": f"{type(exc).__name__}: {exc}"}


def adoption_probe() -> Dict[str, Any]:
    return run_probe()


def use_probe() -> Dict[str, Any]:
    return run_probe()


# --- CLI -------------------------------------------------------------------


def _cli() -> int:
    import argparse
    parser = argparse.ArgumentParser(
        description="Luna dashboard warden - prevents the dashboard "
                    "memory leak by auto-bouncing on thresholds.")
    parser.add_argument(
        "command",
        choices=["tick", "status", "survey", "bounce",
                 "disable", "enable", "reset", "probe"],
        help=("tick: run one warden cycle. "
              "status: read-only state snapshot. "
              "survey: live RAM+health measurement. "
              "bounce: force an immediate bounce (subject to 24h cap). "
              "disable/enable: toggle the kill switch. "
              "reset: clear bounce history (operator use). "
              "probe: self-test."))
    parser.add_argument("--reason", default="operator_request")
    args = parser.parse_args()

    if args.command == "tick":
        r = tick()
    elif args.command == "status":
        r = status()
    elif args.command == "survey":
        r = survey()
    elif args.command == "bounce":
        r = bounce_now(reason=args.reason)
    elif args.command == "disable":
        r = disable(reason=args.reason)
    elif args.command == "enable":
        r = enable()
    elif args.command == "reset":
        state = _read_state()
        state["bounce_history"] = []
        state["consecutive_health_failures"] = 0
        state["auto_disabled"] = False
        state["auto_disabled_reason"] = None
        _write_state(state)
        r = {"ok": True, "reset": True}
    else:
        r = run_probe()

    print(json.dumps(r, indent=2, default=str))
    return 0 if r.get("ok", True) is not False else 1


if __name__ == "__main__":
    sys.exit(_cli())
