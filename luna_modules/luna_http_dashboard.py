"""Luna HTTP Dashboard (Phase UI-1A — local command-center foundation).

A local-only HTTP server that exposes a futuristic 2090-style
dashboard for Luna at http://127.0.0.1:8765. Stdlib-only; binds to 127.0.0.1;
serves whitelisted static files from ``luna_dashboard/`` and a small set of
read-only JSON/JSONL/MD sources from ``memory/`` and ``logs/``.

Hard safety guarantees enforced in this module:
  * Bind only to 127.0.0.1 (never 0.0.0.0).
  * Reject every method except GET, HEAD, and narrow local-only POST actions.
  * No arbitrary shell execution or eval anywhere in the request path.
  * No arbitrary file writes from request handling.
  * Static file serving is whitelisted (no path traversal, no arbitrary reads).
  * Live-feed tail is bounded (default 100 lines).
  * Runtime safety state remains authoritative — this module never flips it.

CLI:
    python -m luna_modules.luna_http_dashboard [--host 127.0.0.1]
                                                [--port 8765]
                                                [--self-test]

Phase UI-1A — Luna Futuristic HTTP Dashboard Foundation.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable

# 2026-05-26 time-based payload cache — prevents concurrent slow-builder
# stampede and makes the dashboard responsive under rapid panel polling.
# 2026-06-01 LEAK FIX: added _MAX_CACHE_ENTRIES + LRU eviction. Previously
# the cache was bounded only by distinct cache keys; if a URL handler ever
# let a user-controlled string become a key (as /api/terminal-truth/<panel>
# did pre-fix), the cache grew without bound. Now hard-capped: oldest
# entries evicted when the cache crosses _MAX_CACHE_ENTRIES. The handler-
# level allowlist fix is the real fix; this is defense in depth against
# future regressions.
_CACHE_TTL_S = 300.0
_MAX_CACHE_ENTRIES = 512
_payload_cache: dict[str, tuple[Any, float]] = {}
_payload_cache_lock = threading.Lock()


def _evict_payload_cache_if_full() -> None:
    """Drop the oldest entries (by stored timestamp) when the cache crosses
    the cap. Called under _payload_cache_lock. Returns how many we dropped."""
    if len(_payload_cache) <= _MAX_CACHE_ENTRIES:
        return
    target = max(1, _MAX_CACHE_ENTRIES // 2)  # halve it
    keep_n = _MAX_CACHE_ENTRIES - target
    by_age = sorted(_payload_cache.items(), key=lambda kv: kv[1][1])
    for key, _ in by_age[:-keep_n]:
        _payload_cache.pop(key, None)
        # Drop the matching per-key build lock so it doesn't leak either.
        _key_build_locks.pop(key, None)

# 2026-05-31 builder-thread leak containment. _safe_build runs each payload
# builder in a daemon worker thread and orphans it on timeout (kept running,
# daemonic). Unbounded, those orphans accumulated to 1381 threads / 8.5 GB on
# a live dashboard process (PID 45420) over ~5h of polling slow/hung
# endpoints. Cap concurrent builder threads with a semaphore so orphans can
# never grow without limit — when saturated, _safe_build returns an instant
# degraded payload instead of spawning (and leaking) another thread. The cap
# is well above the request-handler cap (128) for normal fast builders, so
# healthy operation never hits it; it only bites when builders truly hang.
_MAX_BUILDER_THREADS = 96
_builder_slots = threading.BoundedSemaphore(_MAX_BUILDER_THREADS)

# 2026-05-31 stale-while-revalidate + single-flight upgrade. The previous
# _cached_build released the lock and ran fn() on EVERY stale/cold miss with
# NO single-flight guard, so at TTL expiry every concurrent request rebuilt
# the expensive payload at once (stampede) AND blocked on it — the latency
# spikes + handler pile-up behind the 503s. Now:
#   * FRESH -> return cached instantly.
#   * STALE -> return the stale value INSTANTLY and refresh once in the
#              background (serve-stale-while-revalidate); no request ever
#              blocks on a rebuild after the first warm.
#   * COLD  -> single-flight synchronous build (one builder; concurrent
#              first-hit callers wait on it instead of stampeding).
# Background refresh is single-flight per key, so stuck refreshers are bounded
# to the number of distinct cache keys (a handful), never unbounded.
_refresh_inflight: set[str] = set()
_key_build_locks: dict[str, threading.Lock] = {}

# 2026-05-31 /api/health fast-path. The legacy /api/health was wrapped in
# _safe_build (semaphore + 120s timeout). When the builder pool saturated
# (the documented BuilderPoolSaturated path), /api/health returned 200 OK but
# body.ok=false — fooling external monitors that only checked HTTP status,
# and producing wall-clock latency in the seconds range when the dashboard
# was loaded. /api/health is a LIVENESS probe; it must work even when every
# slow builder is hung. Bypass _safe_build entirely. The local cache below
# absorbs polling bursts so the ~17 file stat() calls happen at most
# once per _HEALTH_FAST_CACHE_TTL_S even at 1000 req/s.
_HEALTH_FAST_CACHE_TTL_S = 5.0
_health_fast_cache: dict[str, Any] = {"ts": 0.0, "sources": None}
_health_fast_lock = threading.Lock()


def _build_health_payload_fast() -> dict[str, Any]:
    """Cheap /api/health builder. NEVER goes through _safe_build.

    Target: returns in <50ms even under load. The only I/O is N file
    exists() checks (~17 entries) cached for _HEALTH_FAST_CACHE_TTL_S.
    Returns the same response shape as the legacy build_health_payload
    so frontends and the launcher don't see a contract change.
    """
    now = time.time()
    with _health_fast_lock:
        cached_sources = _health_fast_cache.get("sources")
        cached_ts = _health_fast_cache.get("ts", 0.0)
    if cached_sources is None or (now - cached_ts) > _HEALTH_FAST_CACHE_TTL_S:
        try:
            sources_present = {
                key: READONLY_SOURCES[key].exists() for key in READONLY_SOURCES
            }
        except Exception:  # noqa: BLE001 — health must never raise
            sources_present = cached_sources or {}
        with _health_fast_lock:
            _health_fast_cache["sources"] = sources_present
            _health_fast_cache["ts"] = now
    else:
        sources_present = cached_sources
    return {
        "ok": True,
        "endpoint": "/api/health",
        "source_status": "fresh",
        "elapsed_ms": 0,
        "phase": PHASE_ID,
        "phase_name": PHASE_NAME,
        "generated_at": _now_iso(),
        "host": DEFAULT_HOST,
        "advisory_only": ADVISORY_ONLY,
        "code_execution_state": "LOCKED",
        "guardian_live_enforcement": "DISABLED",
        "sources_present": sources_present,
    }


# 2026-05-31 working-set trim. Python's pymalloc keeps freed memory in arena
# pools and rarely returns it to the OS. On this polling dashboard the live
# Python heap is small but RSS grows to multi-GB because pages stay committed.
# Periodically gc.collect() then ask Windows to trim the working set. NEVER
# raises. Non-Windows: gc.collect() only (still helpful).
def _trim_working_set_periodically(interval_s: float = 60.0) -> None:
    import gc
    kernel32_setws = None
    proc_handle = None
    c_size_t = None
    try:
        import ctypes as _ctypes
        from ctypes import wintypes as _wintypes
        _k32 = _ctypes.windll.kernel32
        _set = _k32.SetProcessWorkingSetSize
        _set.argtypes = [_wintypes.HANDLE, _ctypes.c_size_t, _ctypes.c_size_t]
        _set.restype = _wintypes.BOOL
        _get = _k32.GetCurrentProcess
        _get.restype = _wintypes.HANDLE
        kernel32_setws = _set
        proc_handle = _get()
        c_size_t = _ctypes.c_size_t
    except Exception:  # noqa: BLE001 — non-Windows or restricted
        kernel32_setws = None
        proc_handle = None
        c_size_t = None
    while True:
        try:
            time.sleep(interval_s)
            gc.collect()
            if (kernel32_setws is not None
                    and proc_handle is not None
                    and c_size_t is not None):
                # (-1, -1) = "trim immediately, no minimum commitment".
                # OS will page out unused memory and shrink working set.
                try:
                    kernel32_setws(proc_handle,
                                   c_size_t(-1), c_size_t(-1))
                except Exception:  # noqa: BLE001
                    pass
        except Exception:  # noqa: BLE001
            pass


def _start_working_set_trim_daemon() -> None:
    """Start the periodic working-set trim daemon. Idempotent; safe to call
    from boot. Daemonic — process exit kills it cleanly."""
    try:
        # Only start once.
        for t in threading.enumerate():
            if t.name == "luna-dashboard-trim-ws":
                return
        threading.Thread(
            target=_trim_working_set_periodically,
            args=(60.0,),
            name="luna-dashboard-trim-ws",
            daemon=True,
        ).start()
    except Exception:  # noqa: BLE001
        pass


def _key_build_lock(key: str) -> threading.Lock:
    with _payload_cache_lock:
        lk = _key_build_locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _key_build_locks[key] = lk
        return lk


def _spawn_cache_refresh(fn, key: str) -> None:
    """Single-flight background refresh: at most one refresh per key at a
    time. NEVER raises; a failed refresh just leaves the stale value in place."""
    with _payload_cache_lock:
        if key in _refresh_inflight:
            return
        _refresh_inflight.add(key)

    def _refresh() -> None:
        try:
            fresh = fn()
            with _payload_cache_lock:
                _payload_cache[key] = (fresh, time.time())
                _evict_payload_cache_if_full()
        except Exception:    # noqa: BLE001
            pass
        finally:
            with _payload_cache_lock:
                _refresh_inflight.discard(key)

    try:
        threading.Thread(target=_refresh, name=f"cache-refresh:{key}",
                         daemon=True).start()
    except Exception:    # noqa: BLE001
        with _payload_cache_lock:
            _refresh_inflight.discard(key)


def _cached_build(fn, key: str) -> Any:
    now = time.time()
    with _payload_cache_lock:
        entry = _payload_cache.get(key)
    if entry is not None:
        val, ts = entry
        if now - ts < _CACHE_TTL_S:
            return val                      # FRESH
        _spawn_cache_refresh(fn, key)       # STALE: refresh in background
        return val                          # ...and serve stale instantly
    # COLD miss (no cached value yet) — single-flight synchronous build so a
    # burst of first-hit requests doesn't all build the payload at once.
    lk = _key_build_lock(key)
    with lk:
        with _payload_cache_lock:
            entry = _payload_cache.get(key)
        if entry is not None:
            return entry[0]                 # built by another thread while we waited
        val = fn()
        with _payload_cache_lock:
            _payload_cache[key] = (val, time.time())
            _evict_payload_cache_if_full()
        # 2026-05-31 cold builds allocate large intermediate dicts; force GC
        # so RSS doesn't balloon from temporary objects retained in arena
        # pools. Bounded — only runs on actual cold misses (one per key per
        # _CACHE_TTL_S window per process).
        try:
            import gc as _gc
            _gc.collect(generation=1)
        except Exception:  # noqa: BLE001
            pass
        return val


# Phase 6B runtime-state helper. Defensive import: if the helper is missing
# or fails to import for any reason, the dashboard falls back to a locked
# safety block via the inline fallback in build_status_payload below. The
# dashboard must NEVER serve an unlocked surface accidentally.
try:
    from luna_modules import luna_phase6b_runtime_state as _phase6b_runtime
except Exception:  # noqa: BLE001
    _phase6b_runtime = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Phase / safety constants
# ---------------------------------------------------------------------------
PHASE_ID = "UI-1A"
PHASE_NAME = "Luna Futuristic HTTP Dashboard Foundation"
ADVISORY_ONLY = True
CODE_EXECUTION_LOCKED = True
GUARDIAN_LIVE_ENFORCEMENT = False
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
LIVE_FEED_MAX_LINES = 100

# ---------------------------------------------------------------------------
# Path layout
# ---------------------------------------------------------------------------
_THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = _THIS_FILE.parent.parent
DASHBOARD_DIR = PROJECT_ROOT / "luna_dashboard"
ASSETS_DIR = DASHBOARD_DIR / "assets"
MEMORY_DIR = PROJECT_ROOT / "memory"
LOGS_DIR = PROJECT_ROOT / "logs"
ARCHIVE_DIR = PROJECT_ROOT / "Luna New UpGrades"

# Whitelisted static files inside DASHBOARD_DIR. Any GET to a static path
# must resolve to one of these names (no traversal, no arbitrary serving).
STATIC_FILES: dict[str, str] = {
    "/": "index.html",
    "/index.html": "index.html",
    # 2026-05-16 Codex deep-scan H14 fix: index.html references
    # /manifest.webmanifest but the file is actually manifest.json.
    # Created manifest.webmanifest as a copy + allowlist both extensions
    # (modern HTML5 spec prefers .webmanifest extension, but legacy
    # references to .json still work).
    "/manifest.json": "manifest.json",
    "/manifest.webmanifest": "manifest.webmanifest",
    "/style.css": "style.css",
    "/app.js": "app.js",
    "/cyberguy-console.js": "cyberguy-console.js",
    "/livemap-retract.js": "livemap-retract.js",
    "/launcher-splash.html": "launcher-splash.html",
    "/launcher-splash.css": "launcher-splash.css",
    "/launcher-splash.js": "launcher-splash.js",
    "/live-map-4k-preview.html": "live-map-4k-preview.html",
    "/live-map-4k-preview.css": "live-map-4k-preview.css",
    "/live-map-4k-preview.js": "live-map-4k-preview.js",
    # 2026-05-16 Luna Sovereign Map — Mission Control upgrade. Standalone
    # page that polls /api/probe-health, /api/terminal-truth, /api/agent-bus
    # and renders a real-data orbital telemetry display (200+ tier stars on
    # 10 level orbits, comet trails per agent-bus event, contracting T500
    # horizon, heartbeat rings per backend service). Can be opened directly
    # at http://127.0.0.1:8765/mission_control_sovereign.html or embedded
    # as an iframe in the existing Mission Control panel.
    "/mission_control_sovereign.html": "mission_control_sovereign.html",
    "/assets/luna_logo.svg": "assets/luna_logo.svg",
    "/assets/luna_icon.png": "assets/luna_icon.png",
    "/assets/luna_icon.ico": "assets/luna_icon.ico",
    # Vendored animation libraries — Three.js / tsParticles / Anime.js.
    # Allowlisted by exact path so no traversal is possible. Files live
    # under D:\SurgeApp\luna_dashboard\vendor\.
    "/vendor/three.min.js":              "vendor/three.min.js",
    "/vendor/tsparticles.bundle.min.js": "vendor/tsparticles.bundle.min.js",
    "/vendor/anime.min.js":              "vendor/anime.min.js",
}

# Whitelisted read-only data sources surfaced through the API. These are the
# ONLY filesystem locations the server is permitted to read.
READONLY_SOURCES: dict[str, Path] = {
    "morning_brief_json": MEMORY_DIR / "luna_morning_decision_brief.json",
    "morning_brief_md": MEMORY_DIR / "luna_morning_decision_brief.md",
    "decision_card_digest": MEMORY_DIR / "luna_decision_card_digest.json",
    "advisory_soak_report": MEMORY_DIR / "luna_advisory_soak_report.json",
    "soak_verdict_report": MEMORY_DIR / "luna_soak_verdict_report.json",
    "schema_review_report": MEMORY_DIR / "luna_schema_review_report.json",
    "capability_scorecard": MEMORY_DIR / "luna_capability_scorecard.json",
    "resource_status": MEMORY_DIR / "luna_resource_status.json",
    "guardian_readiness": MEMORY_DIR / "luna_guardian_readiness_report.json",
    "guardian_status": MEMORY_DIR / "luna_guardian_status.json",
    "worker_heartbeat": LOGS_DIR / "luna_worker_heartbeat.json",
    "aider_bridge_status": LOGS_DIR / "aider_bridge_status.json",
    "live_feed": LOGS_DIR / "luna_live_feed.jsonl",
    "evidence_gate": MEMORY_DIR / "luna_self_upgrade_evidence_gate.json",
    "always_on_heartbeat": MEMORY_DIR / "always_on" / "luna_always_on_heartbeat.json",
    "always_on_latest_status": MEMORY_DIR / "always_on" / "luna_always_on_latest_status.md",
    "self_patch_attempts": MEMORY_DIR / "luna_self_patch_attempts.jsonl",
    "current_activity": MEMORY_DIR / "luna_current_activity.json",
}

CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
    ".json": "application/json; charset=utf-8",
    ".webmanifest": "application/manifest+json; charset=utf-8",
}

SOAK_COMMAND = (
    r"D:\SurgeApp\.aider_venv\Scripts\python.exe -m luna_modules.luna_decision_brief"
    " --soak --cycles 144 --sleep-seconds 600 --write-soak"
)


# ---------------------------------------------------------------------------
# Read-only data helpers
# ---------------------------------------------------------------------------
def _safe_read_json(path: Path) -> dict[str, Any] | None:
    """Return parsed JSON dict from ``path`` or None if missing/invalid.

    Never raises. Used only for whitelisted READONLY_SOURCES.
    """
    try:
        if not path.exists() or not path.is_file():
            return None
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
        if isinstance(data, dict):
            return data
        return {"_raw": data}
    except (OSError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Bounded readers for hot dashboard paths.
#
# /api/mission-control reads ~7 JSON files + tails one JSONL on every poll.
# When a writer (repair_task_executor, adoption/use attestor, housekeeping
# sweep, rebuild campaign step) is mid-atomic-replace on any of those files,
# a plain ``Path.read_text`` can block for seconds on Windows. To keep the
# dashboard responsive we cap each individual read and the total wall budget
# for the handler, returning a partial payload with explicit ``stale_sources``
# rather than hanging.
# ---------------------------------------------------------------------------
MISSION_CONTROL_READ_TIMEOUT_MS = 500
MISSION_CONTROL_TOTAL_BUDGET_MS = 1500

# 2026-05-14 universal API wall-clock budget. Applied uniformly by
# ``LunaDashboardServer._safe_build`` to every payload builder so no
# endpoint can hang the dashboard. 5 s is comfortably above the slowest
# legitimate cold-cache build (tier-truth at ~3 s after the
# canonical_truth recursion fix) and well under any normal HTTP client
# timeout.
SAFE_BUILD_TIMEOUT_S = 120.0
TERMINAL_TRUTH_SAFE_BUILD_TIMEOUT_S = 120.0


class _ReadBudget:
    """Tracks total wall time spent in bounded reads for one handler call."""

    __slots__ = ("started_at", "total_budget_ms", "stale_sources")

    def __init__(self, total_budget_ms: int = MISSION_CONTROL_TOTAL_BUDGET_MS) -> None:
        self.started_at = time.monotonic()
        self.total_budget_ms = total_budget_ms
        self.stale_sources: dict[str, str] = {}

    def elapsed_ms(self) -> int:
        return int((time.monotonic() - self.started_at) * 1000)

    def remaining_ms(self) -> int:
        return max(0, self.total_budget_ms - self.elapsed_ms())

    def mark_stale(self, source: str, reason: str) -> None:
        # One short reason per source; last writer wins.
        self.stale_sources[source] = reason


def _read_with_timeout(fn, timeout_ms: int) -> tuple[Any, bool]:
    """Run ``fn()`` in a daemon worker thread; return (value, timed_out).

    Used to bound individual filesystem reads when a writer may briefly hold
    an exclusive lock. ``fn`` must be a no-arg callable; exceptions inside
    ``fn`` are swallowed (return None, False) so the dashboard handler can
    never crash on a bad source.
    """
    timeout_s = max(0.0, timeout_ms / 1000.0)
    box: list[Any] = [None]

    def _runner() -> None:
        try:
            box[0] = fn()
        except Exception:  # noqa: BLE001 — never raise into the handler
            box[0] = None

    th = threading.Thread(target=_runner, daemon=True)
    th.start()
    th.join(timeout_s)
    if th.is_alive():
        return None, True
    return box[0], False


def _bounded_read_json(
    source_name: str,
    path: Path,
    budget: _ReadBudget,
    per_read_timeout_ms: int = MISSION_CONTROL_READ_TIMEOUT_MS,
) -> dict[str, Any] | None:
    """Bounded variant of :func:`_safe_read_json`.

    Respects both the per-read deadline and the handler's total budget.
    On timeout, records ``source_name`` into ``budget.stale_sources`` and
    returns ``None`` so callers can fall back to defaults.
    """
    if budget.remaining_ms() <= 0:
        budget.mark_stale(source_name, "total_budget_exhausted")
        return None
    timeout_ms = min(per_read_timeout_ms, budget.remaining_ms())
    value, timed_out = _read_with_timeout(lambda: _safe_read_json(path), timeout_ms)
    if timed_out:
        budget.mark_stale(source_name, "lock_timeout_" + str(timeout_ms) + "ms")
        return None
    if isinstance(value, dict):
        return value
    return None


def _bounded_tail_jsonl(
    source_name: str,
    path: Path,
    budget: _ReadBudget,
    max_records: int = LIVE_FEED_MAX_LINES,
    per_read_timeout_ms: int = MISSION_CONTROL_READ_TIMEOUT_MS,
) -> list[dict[str, Any]]:
    """Bounded variant of :func:`_safe_tail_jsonl` / :func:`_read_jsonl_records`."""
    if budget.remaining_ms() <= 0:
        budget.mark_stale(source_name, "total_budget_exhausted")
        return []
    timeout_ms = min(per_read_timeout_ms, budget.remaining_ms())
    value, timed_out = _read_with_timeout(
        lambda: _safe_tail_jsonl(path, limit=max_records), timeout_ms
    )
    if timed_out:
        budget.mark_stale(source_name, "lock_timeout_" + str(timeout_ms) + "ms")
        return []
    if isinstance(value, list):
        return value
    return []


def _source_age_seconds(path: Path) -> int | None:
    """Best-effort mtime freshness for live dashboard cards."""
    try:
        if not path.exists() or not path.is_file():
            return None
        return max(0, int(time.time() - path.stat().st_mtime))
    except OSError:
        return None


def _safe_tail_jsonl(path: Path, limit: int = LIVE_FEED_MAX_LINES) -> list[dict[str, Any]]:
    """Return up to ``limit`` last JSONL records from ``path``. Bounded + safe."""
    if limit <= 0:
        return []
    if limit > LIVE_FEED_MAX_LINES:
        limit = LIVE_FEED_MAX_LINES
    try:
        if not path.exists() or not path.is_file():
            return []
        # For modest log sizes (typical Luna live feed), reading + slicing is
        # fine. For very large files, we read in binary chunks from the end.
        size = path.stat().st_size
        if size <= 256 * 1024:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        else:
            # Read last ~256KB which comfortably covers > 100 lines.
            with path.open("rb") as fh:
                fh.seek(-256 * 1024, os.SEEK_END)
                blob = fh.read()
            text = blob.decode("utf-8", errors="replace")
            # Drop the (likely partial) first line.
            lines = text.split("\n", 1)[1].splitlines() if "\n" in text else []
        out: list[dict[str, Any]] = []
        for raw in lines[-limit:]:
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
                if isinstance(rec, dict):
                    out.append(rec)
                else:
                    out.append({"_raw": rec})
            except json.JSONDecodeError:
                out.append({"_raw": raw[:500]})
        return out
    except OSError:
        return []


def _archive_listing() -> list[dict[str, Any]]:
    """Return a read-only directory listing of the archive folder.

    Never recurses. Never reads file contents.
    """
    out: list[dict[str, Any]] = []
    try:
        if not ARCHIVE_DIR.exists() or not ARCHIVE_DIR.is_dir():
            return out
        for entry in sorted(ARCHIVE_DIR.iterdir(), key=lambda p: p.name.lower()):
            try:
                stat = entry.stat()
            except OSError:
                continue
            out.append({
                "name": entry.name,
                "size_bytes": stat.st_size if entry.is_file() else 0,
                "is_dir": entry.is_dir(),
                "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            })
    except OSError:
        pass
    return out


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 2026-05-13 ROOT-CAUSE FIX — Canonical current-truth contract.
#
# Every primary panel reads ``/api/canonical-truth`` first; every panel-
# specific payload embeds ``canonical_truth_summary`` so the JS render
# layer can cross-check. When the helper itself fails, we attach a
# stub with ``unavailable=true`` so the frontend can detect drift.
# ---------------------------------------------------------------------------
# Re-entrancy guard (2026-05-14 root-cause fix for /api/mission-control,
# /api/agent-bus, /api/tier-truth timeouts).
#
# Diagnosed call chain that produced the hang:
#   build_mission_control_payload()
#     -> _attach_canonical_truth_summary()
#         -> _canonical_truth_summary_safe()
#             -> luna_canonical_truth.canonical_truth_summary()
#                 -> luna_canonical_truth.build_canonical_current_truth()
#                     -> luna_canonical_truth._mission_control_section()
#                         -> build_mission_control_payload()       (re-entry)
#
# Each recursion level performed ~7 bounded I/O reads (~3.5 s of wall
# time), so a few stack levels exhausted any sane client-side request
# budget. The thread-local guard below short-circuits the second and
# deeper entries, returning a labeled DEGRADED stub so the outer panel
# payload still carries the canonical_truth_summary key (panel JS
# expects it) but the recursion terminates immediately.
_canonical_truth_guard = threading.local()


def _canonical_truth_summary_safe() -> dict[str, Any]:
    """Best-effort canonical truth summary for embedding. Never raises
    and never recurses into an already-in-flight canonical-truth build."""
    if getattr(_canonical_truth_guard, "in_flight", False):
        return {
            "current_rebuild_tier":           None,
            "highest_honestly_verified_tier": None,
            "current_blocker_summary":        None,
            "next_action_actor":              None,
            "current_phase":                  None,
            "generated_at":                   _now_iso(),
            "primary_truth_source":           "luna_canonical_truth.canonical_truth_summary",
            "degraded":                       True,
            "source_status":                  "reentrant_skip",
            "reason":                         (
                "canonical_truth_summary already in flight on this thread; "
                "embedded as degraded stub to prevent recursive panel build"
            ),
        }
    _canonical_truth_guard.in_flight = True
    try:
        from luna_modules import luna_canonical_truth as _ct
        return _ct.canonical_truth_summary()
    except Exception as exc:  # noqa: BLE001
        return {
            "current_rebuild_tier":           None,
            "highest_honestly_verified_tier": None,
            "current_blocker_summary":        None,
            "next_action_actor":              None,
            "current_phase":                  None,
            "generated_at":                   _now_iso(),
            "primary_truth_source":           "luna_canonical_truth.canonical_truth_summary",
            "unavailable":                    True,
            "degraded":                       True,
            "source_status":                  "summary_builder_error",
            "error":                          f"{type(exc).__name__}",
            "reason":                         str(exc)[:200],
        }
    finally:
        _canonical_truth_guard.in_flight = False


def _attach_canonical_truth_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """Embed canonical_truth_summary at top of any panel payload.

    Idempotent — if the caller already set the field it is preserved.
    Re-entrancy guarded — see ``_canonical_truth_guard`` above.
    """
    if not isinstance(payload, dict):
        return payload
    if "canonical_truth_summary" not in payload:
        payload["canonical_truth_summary"] = _canonical_truth_summary_safe()
    return payload


# ---------------------------------------------------------------------------
# API payload builders
# ---------------------------------------------------------------------------
def _ecosystem_verifier_state() -> str:
    """Lowercase state word for the ecosystem object (consumed by
    app.js's setEco vocabulary)."""
    try:
        from luna_modules import luna_verifier_status as _vs
        return _vs.ecosystem_state_str()
    except Exception:  # noqa: BLE001
        return "unknown"


def _ecosystem_verifier_dict() -> dict[str, Any]:
    """Full canonical verifier dict for the ecosystem.verifier block."""
    try:
        from luna_modules import luna_verifier_status as _vs
        env = _vs.compute_verifier_status()
        return {
            "state":   _vs.ecosystem_state_str(),
            "status":  env["status"],
            "label":   env["label"],
            "detail":  env["label"],
            "source":  env["source"],
            "healthy": env["healthy"],
            "canonical_source": "luna_verifier_status",
        }
    except Exception as exc:  # noqa: BLE001
        return {"state": "unknown", "status": "UNKNOWN",
                "label": "Verifier · UNKNOWN",
                "detail": f"verifier_status unavailable: {type(exc).__name__}",
                "source": "fallback",
                "healthy": False,
                "canonical_source": "fallback"}


def _build_canonical_verifier_block(readiness: Any) -> dict[str, Any]:
    """Return the canonical verifier dict that build_status_payload's
    `verifier` field uses. Single source: luna_verifier_status.
    Falls back gracefully (never to bare 'unknown' if a signal exists)."""
    try:
        from luna_modules import luna_verifier_status as _vs
        env = _vs.compute_verifier_status()
        return {
            "summary":             _vs.summary_for_top_strip(),
            "status":              env["status"],
            "label":               env["label"],
            "source":              env["source"],
            "last_update":         env["last_update"],
            "healthy":             env["healthy"],
            "reason_if_unknown":   env["reason_if_unknown"],
            "evidence_refs":       env["evidence_refs"],
            "canonical_source":    "luna_verifier_status",
        }
    except Exception as exc:  # noqa: BLE001
        # Fail-soft fallback: if the canonical module breaks, still
        # avoid blank UNKNOWN by surfacing the readiness summary.
        rsum = None
        if isinstance(readiness, dict):
            rsum = readiness.get("verifier_summary")
        return {
            "summary":           rsum or "ADVISORY",
            "status":            "UNKNOWN",
            "label":             "Verifier · UNKNOWN",
            "source":            "fallback_after_error",
            "last_update":       None,
            "healthy":           False,
            "reason_if_unknown": f"verifier_status module unavailable: {type(exc).__name__}",
            "evidence_refs":     [],
            "canonical_source":  "fallback",
        }


def build_status_payload() -> dict[str, Any]:
    """Top command-bar payload — Luna / worker / guardian / aider / soak state."""
    heartbeat = _safe_read_json(READONLY_SOURCES["worker_heartbeat"]) or {}
    aider = _safe_read_json(READONLY_SOURCES["aider_bridge_status"]) or {}
    guardian = _safe_read_json(READONLY_SOURCES["guardian_status"]) or {}
    verdict = _safe_read_json(READONLY_SOURCES["soak_verdict_report"]) or {}
    readiness = _safe_read_json(READONLY_SOURCES["guardian_readiness"]) or {}

    services = guardian.get("services", {}) if isinstance(guardian, dict) else {}
    worker_running = bool(services.get("worker", {}).get("running")) if isinstance(services, dict) else False
    aider_running = bool(services.get("aider_bridge", {}).get("running")) if isinstance(services, dict) else False

    return {
        "phase": PHASE_ID,
        "phase_name": PHASE_NAME,
        "generated_at": _now_iso(),
        "luna": {
            "state": heartbeat.get("state", "unknown"),
            "phase": heartbeat.get("phase", "unknown"),
            "mood": heartbeat.get("mood", ""),
            "last_message": heartbeat.get("last_message", ""),
            "ts": heartbeat.get("ts", ""),
            "alive": bool(heartbeat.get("alive", False)),
        },
        "worker": {
            "running": worker_running,
            "pid": services.get("worker", {}).get("pid") if isinstance(services, dict) else None,
            "queue_depth": heartbeat.get("queue_depth", 0),
            "active_count": heartbeat.get("active_count", 0),
            "approval_pending": heartbeat.get("approval_pending", 0),
        },
        "guardian": {
            "running": True if guardian.get("guardian_pid") else False,
            "pid": guardian.get("guardian_pid"),
            "status": guardian.get("status", "unknown"),
            "kill_switch_present": bool(guardian.get("kill_switch_present", False)),
            "live_enforcement": False,
        },
        "aider_bridge": {
            "running": aider_running,
            "pid": aider.get("pid"),
            "state": aider.get("state", "unknown"),
            "stage": aider.get("stage", ""),
        },
        # 2026-05-13 canonical verifier source — luna_verifier_status
        # is the SINGLE source every payload + panel consumes. Avoids
        # the prior "Verifier unknown" vs "Verifier live" split bug.
        "verifier": _build_canonical_verifier_block(readiness),
        "soak": {
            "verdict": verdict.get("verdict", "UNKNOWN"),
            "observed_cycles": verdict.get("observed_cycles", 0),
            "required_cycles": verdict.get("required_cycles", 144),
            "stable_recommendation": verdict.get("stable_recommendation", ""),
            "checklist_24h_satisfied": bool(verdict.get("checklist_item_24h_soak_satisfied", False)),
            "last_update": verdict.get("generated_at", ""),
        },
        # The safety block is the source-of-truth display for Phase 6B
        # state. It is read from memory/luna_phase6b_runtime_state.json via
        # luna_modules.luna_phase6b_runtime_state. Default contents of that
        # file are locked/advisory. If the helper or file is missing or
        # invalid, we fall back to the locked literal block below — the
        # dashboard NEVER serves an unlocked surface by accident.
        "safety": (
            _phase6b_runtime.build_dashboard_safety_payload(
                _phase6b_runtime.load_runtime_state()
            )
            if _phase6b_runtime is not None
            else {
                "code_execution_state": "LOCKED",
                "guardian_live_enforcement": "DISABLED",
                "dry_run_active": True,
                "guardian_mode_label": "DRY-RUN (advisory only)",
                "advisory_only": True,
                "safe_to_execute_now": False,
                "safe_to_apply_real_project": False,
                "guardian_enforcing_live": False,
                "live_enforcement_enabled": False,
                "live_enforcement_ready": False,
            }
        ),
    }


def build_decision_brief_payload() -> dict[str, Any]:
    brief = _safe_read_json(READONLY_SOURCES["morning_brief_json"]) or {}
    digest = _safe_read_json(READONLY_SOURCES["decision_card_digest"]) or {}
    live_feed_path = READONLY_SOURCES["live_feed"]
    live_feed_items = _safe_tail_jsonl(live_feed_path, limit=8)
    brief_age = _source_age_seconds(READONLY_SOURCES["morning_brief_json"])
    feed_age = _source_age_seconds(live_feed_path)
    higher_tier_context = build_higher_tier_progress_payload()
    stale_threshold = 600
    source_name = (
        "logs/luna_live_feed.jsonl"
        if live_feed_items and (brief_age is None or (feed_age is not None and feed_age < brief_age))
        else "memory/luna_morning_decision_brief.json"
    )
    return {
        "available": bool(brief),
        "generated_at": brief.get("generated_at", ""),
        "advisory_only": bool(brief.get("advisory_only", True)),
        "overall_recommendation": brief.get("overall_recommendation", "unknown"),
        "counts": brief.get("counts", {}),
        "top_items": brief.get("top_items", [])[:8],
        "live_feed_items": live_feed_items,
        "serge_summary": brief.get("serge_summary", ""),
        "next_safe_action": brief.get("next_safe_action", ""),
        "decision_card_digest": digest if isinstance(digest, dict) else {},
        "decision_queue_source": source_name,
        "source_age_seconds": brief_age,
        "live_feed_age_seconds": feed_age,
        "is_stale": bool(brief_age is not None and brief_age > stale_threshold and not live_feed_items),
        "higher_tier_endpoint": "/api/higher-tier/progress",
        "higher_tier_context": higher_tier_context if isinstance(higher_tier_context, dict) else {},
        "sources_read": [
            "memory/luna_morning_decision_brief.json",
            "memory/luna_decision_card_digest.json",
            "logs/luna_live_feed.jsonl",
            "/api/higher-tier/progress",
        ],
    }


def build_soak_payload() -> dict[str, Any]:
    """Surface the soak status to /api/soak.

    Source-of-truth precedence (smallest safe reporting fix):
      1. If ``luna_advisory_soak_report.json`` shows a *completed* soak
         (cycles ≥ required_cycles AND no failures AND no warnings),
         prefer it.  Stamp the verdict as ``ADVISORY_PASS`` — explicitly
         neutral; this does NOT imply autonomy or real-project apply.
      2. Otherwise fall back to ``luna_soak_verdict_report.json`` as
         before, preserving every prior payload field unchanged.

    Governance flags (advisory_only / safe_to_execute_now /
    safe_to_apply_real_project / guardian_enforcing_live) are not
    sourced from soak data and remain invariants of this module.
    """
    verdict = _safe_read_json(READONLY_SOURCES["soak_verdict_report"]) or {}
    advisory = _safe_read_json(READONLY_SOURCES["advisory_soak_report"]) or {}

    # Did the advisory report capture a completed clean run?
    adv_cycles_obs = int(advisory.get("cycles") or advisory.get("observed_cycles") or 0)
    adv_cycles_req = int(advisory.get("required_cycles") or 144)
    adv_failures   = advisory.get("failures") or []
    adv_warnings   = advisory.get("warnings") or []
    adv_complete   = (
        bool(advisory)
        and adv_cycles_obs >= adv_cycles_req
        and not adv_failures
        and not adv_warnings
    )

    if adv_complete:
        # Use advisory report as primary source; stamp a neutral verdict.
        return {
            "available": True,
            "verdict": "ADVISORY_PASS",
            "observed_cycles": adv_cycles_obs,
            "required_cycles": adv_cycles_req,
            "required_duration_seconds": int(advisory.get("required_duration_seconds")
                                             or verdict.get("required_duration_seconds")
                                             or 86400),
            "observed_duration_seconds": int(advisory.get("duration_seconds")
                                             or advisory.get("observed_duration_seconds")
                                             or verdict.get("observed_duration_seconds")
                                             or 0),
            "stable_recommendation": str(advisory.get("stable_recommendation")
                                         or verdict.get("stable_recommendation") or ""),
            "failures": adv_failures,
            "warnings": adv_warnings,
            "checklist_24h_satisfied": True,
            "advisory_only": True,
            "last_update": str(advisory.get("finished_at")
                               or advisory.get("generated_at")
                               or verdict.get("generated_at") or ""),
            "soak_command": SOAK_COMMAND,
            "serge_summary": str(advisory.get("serge_summary")
                                 or verdict.get("serge_summary") or ""),
            "recommended_next_action": str(advisory.get("recommended_next_action")
                                           or verdict.get("recommended_next_action") or ""),
            "soak_id": str(advisory.get("soak_id") or ""),
            "source": "advisory_soak_report",
        }

    # Fall-back: original behaviour, every key preserved.
    return {
        "available": bool(verdict),
        "verdict": verdict.get("verdict", "UNKNOWN"),
        "observed_cycles": verdict.get("observed_cycles", 0),
        "required_cycles": verdict.get("required_cycles", 144),
        "required_duration_seconds": verdict.get("required_duration_seconds", 86400),
        "observed_duration_seconds": verdict.get("observed_duration_seconds", 0),
        "stable_recommendation": verdict.get("stable_recommendation", ""),
        "failures": verdict.get("failures", []),
        "warnings": verdict.get("warnings", []),
        "checklist_24h_satisfied": bool(verdict.get("checklist_item_24h_soak_satisfied", False)),
        "advisory_only": True,
        "last_update": verdict.get("generated_at", ""),
        "soak_command": SOAK_COMMAND,
        "serge_summary": verdict.get("serge_summary", ""),
        "recommended_next_action": verdict.get("recommended_next_action", ""),
        "source": "soak_verdict_report",
    }


def build_scorecard_payload() -> dict[str, Any]:
    sc = _safe_read_json(READONLY_SOURCES["capability_scorecard"]) or {}
    dims = sc.get("dimensions", []) if isinstance(sc, dict) else []
    trimmed_dims = [
        {
            "name": d.get("name", "?"),
            "score": d.get("score", 0),
            "weight": d.get("weight", 0),
            "status": d.get("status", "unknown"),
            "recommended_next_action": d.get("recommended_next_action", ""),
        }
        for d in dims if isinstance(d, dict)
    ]
    return {
        "available": bool(sc),
        "generated_at": sc.get("generated_at", ""),
        "overall_score": sc.get("overall_score", 0),
        "overall_status": sc.get("overall_status", "unknown"),
        "readiness_level": sc.get("readiness_level", "unknown"),
        "critical_blockers": sc.get("critical_blockers", []),
        "dimensions": trimmed_dims,
    }


def build_resources_payload() -> dict[str, Any]:
    rs = _safe_read_json(READONLY_SOURCES["resource_status"]) or {}
    resource_path = READONLY_SOURCES["resource_status"]
    live_feed_path = READONLY_SOURCES["live_feed"]
    age = _source_age_seconds(resource_path)
    live_age = _source_age_seconds(live_feed_path)
    stale_threshold = 30
    return {
        "available": bool(rs),
        "generated_at": rs.get("generated_at", ""),
        "resource_source": "memory/luna_resource_status.json",
        "source_age_seconds": age,
        "live_feed_source": "logs/luna_live_feed.jsonl",
        "live_feed_age_seconds": live_age,
        "is_stale": bool(age is None or age > stale_threshold),
        "resource_mode": rs.get("resource_mode", "unknown"),
        "host": rs.get("host", ""),
        "platform": rs.get("platform", ""),
        "disk": rs.get("disk", {}),
        "memory": rs.get("memory", {}),
        "cpu": rs.get("cpu", {}),
        "gpu": rs.get("gpu", {}),
        "ollama": rs.get("ollama", {}),
        "warnings": rs.get("warnings", []),
    }


def build_live_feed_payload(limit: int = LIVE_FEED_MAX_LINES) -> dict[str, Any]:
    if limit <= 0:
        limit = LIVE_FEED_MAX_LINES
    if limit > LIVE_FEED_MAX_LINES:
        limit = LIVE_FEED_MAX_LINES
    records = _safe_tail_jsonl(READONLY_SOURCES["live_feed"], limit=limit)
    return {
        "available": bool(records),
        "limit": limit,
        "count": len(records),
        "records": records,
    }


def build_archive_payload() -> dict[str, Any]:
    items = _archive_listing()
    return {
        "archive_path": str(ARCHIVE_DIR),
        "count": len(items),
        "items": items,
    }


def build_activity_payload(window_seconds: int = 1800, buckets: int = 60) -> dict[str, Any]:
    """Return a time-bucketed activity histogram of the live feed.

    Buckets the most recent ``window_seconds`` of live-feed events into
    ``buckets`` equal slices and counts events per bucket per role/source.
    Used by the front-end oscilloscope and event-frequency chart.
    The dashboard derives this client-side too, but providing a clean
    server payload keeps the JS small and the contract explicit.
    """
    if buckets <= 0:
        buckets = 1
    if buckets > 240:
        buckets = 240
    if window_seconds <= 0:
        window_seconds = 60
    if window_seconds > 24 * 3600:
        window_seconds = 24 * 3600

    records = _safe_tail_jsonl(READONLY_SOURCES["live_feed"], limit=LIVE_FEED_MAX_LINES)
    now = time.time()
    bucket_size = max(1.0, window_seconds / buckets)
    counts = [0] * buckets
    by_role: dict[str, int] = {}
    last_role = ""
    last_event = ""
    last_ts = ""

    for rec in records:
        if not isinstance(rec, dict):
            continue
        role = str(rec.get("role") or rec.get("source") or "unknown")[:24]
        by_role[role] = by_role.get(role, 0) + 1
        last_role = role
        last_event = str(rec.get("event") or "")
        last_ts = str(rec.get("ts") or "")
        # Bucketing: live_feed.jsonl uses HH:MM:SS strings, not full ISO,
        # so we approximate by spacing events evenly across the window. The
        # client sees the order as a "recent activity" signal — exact wall
        # time isn't required for the visualization.
    n = len(records)
    if n > 0:
        for i in range(n):
            # Map the i-th most-recent record to a bucket from "now" backward.
            offset_seconds = (n - 1 - i) * (window_seconds / max(1, n))
            bidx = int((window_seconds - offset_seconds) / bucket_size)
            if bidx < 0:
                bidx = 0
            if bidx >= buckets:
                bidx = buckets - 1
            counts[bidx] += 1

    # Top roles, sorted, capped at 6 — for the role frequency strip.
    top_roles = sorted(by_role.items(), key=lambda kv: kv[1], reverse=True)[:6]

    return {
        "generated_at": _now_iso(),
        "window_seconds": window_seconds,
        "buckets": buckets,
        "bucket_size_seconds": bucket_size,
        "counts": counts,
        "total_events": n,
        "by_role": [{"role": r, "count": c} for r, c in top_roles],
        "last_role": last_role,
        "last_event": last_event,
        "last_ts": last_ts,
        "now_epoch": now,
    }


def build_supermax_payload() -> dict[str, Any]:
    """Surface the Luna self-upgrade evidence gate, always-on supervisor
    heartbeat, and the most recent self-patch attempts. Read-only."""
    gate = _safe_read_json(READONLY_SOURCES["evidence_gate"]) or {}
    heartbeat = _safe_read_json(READONLY_SOURCES["always_on_heartbeat"]) or {}

    recent_attempts: list[dict[str, Any]] = []
    log_path = READONLY_SOURCES["self_patch_attempts"]
    try:
        if log_path.exists():
            text = log_path.read_text(encoding="utf-8-sig", errors="replace")
            lines = [ln for ln in text.splitlines() if ln.strip()]
            for ln in lines[-12:]:
                try:
                    recent_attempts.append(json.loads(ln))
                except Exception:
                    pass
    except Exception:
        pass

    latest_status_md = ""
    status_path = READONLY_SOURCES["always_on_latest_status"]
    try:
        if status_path.exists():
            latest_status_md = status_path.read_text(encoding="utf-8-sig", errors="replace")
    except Exception:
        pass

    rules = gate.get("promotion_rules", {}) if isinstance(gate, dict) else {}
    threshold_t2 = int(rules.get("tier_2_promotion_threshold_t0t1_successes", 10))
    sum_t0t1 = int(gate.get("tier0_success_count", 0)) + int(gate.get("tier1_success_count", 0))
    rb_failures = int(gate.get("rollback_failure_count", 0))
    progress_to_tier_2 = min(1.0, sum_t0t1 / float(max(threshold_t2, 1)))

    return _attach_canonical_truth_summary({
        "ok": True,
        "generated_at": _now_iso(),
        "evidence_gate": gate,
        "computed": {
            "t0t1_successes": sum_t0t1,
            "tier_2_threshold": threshold_t2,
            "progress_to_tier_2": progress_to_tier_2,
            "rollback_failures": rb_failures,
            "tier_2_eligible": (sum_t0t1 >= threshold_t2 and rb_failures == 0),
        },
        "always_on_heartbeat": heartbeat,
        "latest_status_md": latest_status_md,
        "recent_attempts": recent_attempts,
    })


def _read_jsonl_records(path: Path, max_records: int = 200) -> list[dict[str, Any]]:
    """Read the last N parsed JSON-lines records from `path`. Tolerant of
    BOM, trailing whitespace, malformed lines."""
    records: list[dict[str, Any]] = []
    if not path.exists():
        return records
    try:
        text = path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return records
    lines = [ln for ln in text.splitlines() if ln.strip()]
    for ln in lines[-max_records:]:
        try:
            obj = json.loads(ln)
            if isinstance(obj, dict):
                records.append(obj)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    return records


def _read_cycle_reports(dir_path: Path, max_files: int = 8) -> list[dict[str, Any]]:
    """Return the N most recent JSON cycle reports from `dir_path`."""
    out: list[dict[str, Any]] = []
    if not dir_path.exists() or not dir_path.is_dir():
        return out
    files = sorted(
        [p for p in dir_path.glob("luna_always_on_cycle_*.json")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )[:max_files]
    for p in files:
        try:
            obj = json.loads(p.read_text(encoding="utf-8-sig", errors="replace"))
            if isinstance(obj, dict):
                obj["_report_path"] = "memory/always_on/" + p.name
                out.append(obj)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
    return out


def build_self_upgrade_progress_payload() -> dict[str, Any]:
    """Public read-only API for the dashboard self-upgrade UI.

    Returns a structured snapshot covering:
      - current_tier
      - tier2_eligible / tier2_approved / tier2_approval_time
      - counts (per-tier successes, failures, rollback failures)
      - recent_patches (newest first, structured rows the UI can render)
      - last_update + sources_read
    """
    gate = _safe_read_json(READONLY_SOURCES["evidence_gate"]) or {}
    rules = gate.get("promotion_rules", {}) if isinstance(gate, dict) else {}
    threshold_t2 = int(rules.get("tier_2_promotion_threshold_t0t1_successes", 10))
    sum_t0t1 = int(gate.get("tier0_success_count", 0)) + int(gate.get("tier1_success_count", 0))
    rb_failures = int(gate.get("rollback_failure_count", 0))

    tier2_eligible_flag = bool(gate.get("tier2_eligible", False))
    if not tier2_eligible_flag:
        # Tolerate older gate JSON: compute eligibility from counters.
        tier2_eligible_flag = (sum_t0t1 >= threshold_t2 and rb_failures == 0)

    # Read the patch attempts log and turn it into UI-friendly rows.
    attempts = _read_jsonl_records(READONLY_SOURCES["self_patch_attempts"], max_records=400)
    cycles = _read_cycle_reports(READONLY_SOURCES["always_on_heartbeat"].parent, max_files=8)

    # Map cycle_id -> verdict, and per-task records, so we can surface
    # supervisor-level context for each patch attempt.
    cycle_verdict_by_id: dict[str, str] = {}
    task_records_by_cycle: dict[str, list[dict[str, Any]]] = {}
    for c in cycles:
        cid = str(c.get("cycle_id", ""))
        if cid:
            cycle_verdict_by_id[cid] = str(c.get("verdict", ""))
            tasks = c.get("per_task")
            if isinstance(tasks, list):
                task_records_by_cycle[cid] = tasks

    recent_rows: list[dict[str, Any]] = []
    for rec in reversed(attempts):  # newest first
        result = str(rec.get("result", ""))
        tier   = rec.get("tier")
        target = str(rec.get("target", ""))
        ts     = str(rec.get("ts", ""))
        run_id = str(rec.get("run_id", ""))
        backup = str(rec.get("backup_path", ""))
        verify = str(rec.get("verify_detail", ""))

        is_ok       = (result == "APPLIED_AND_VERIFIED")
        is_rolled   = (result == "VERIFY_FAIL_ROLLED_BACK")
        is_rb_fail  = (result == "VERIFY_FAIL_ROLLBACK_FAILED")
        is_refused  = result.startswith("REFUSED_")
        status_kind = (
            "ok" if is_ok else
            "rolled_back" if is_rolled else
            "rollback_failed" if is_rb_fail else
            "refused" if is_refused else
            "fail"
        )

        # Try to attach a title from the target filename.
        try:
            title = target.replace("\\", "/").rsplit("/", 1)[-1]
        except Exception:
            title = target
        rollback_status = (
            "n/a (success)" if is_ok else
            "rolled back" if is_rolled else
            "ROLLBACK FAILED — needs Serge" if is_rb_fail else
            "refused before write" if is_refused else
            "unknown"
        )

        # Dashboard-friendly status label. Some "REFUSED_*" + "ROLLED_BACK"
        # outcomes are not really failures — they're Luna's safety guards
        # working correctly. Relabel those so Recent Patches doesn't fill
        # up with red entries that are actually fine:
        #   REFUSED_TIER1_NOT_ADDITIVE + verify_detail "file already exists"
        #     => the test file is already on disk, target state is reached
        #     => "Already in place" (green), counted as ok
        #   REFUSED_SCOPE_*  => "Out of scope" (neutral)
        #   VERIFY_FAIL_ROLLED_BACK + pytest_exit=1
        #     => the test ran but failed; needs a human to fix the test
        #     => "Test failed · rolled back" (amber)
        # The wrapper writes "file already exists" into `reason` for
        # refused records and into `verify_detail` for verify failures.
        # Check both fields.
        verify_lower = (verify or "").lower()
        reason_lower = str(rec.get("reason") or "").lower()
        combined_detail = (verify_lower + " " + reason_lower).strip()
        already_in_place = (
            result == "REFUSED_TIER1_NOT_ADDITIVE"
            and ("already exists" in combined_detail or "file already exists" in combined_detail)
        )
        if is_ok:
            status_label = "Completed + Verified"
        elif already_in_place:
            status_label = "Already in place"
        elif is_rolled:
            if "pytest_exit" in verify_lower:
                status_label = "Test failed · rolled back"
            else:
                status_label = "Rolled back"
        elif is_rb_fail:
            status_label = "Rollback failed"
        elif is_refused:
            # Generic refused (e.g., scope, gate, runtime). Neutral, not red.
            human = result.replace("REFUSED_", "").replace("_", " ").lower()
            status_label = "Refused · " + human if human else "Refused"
        elif result:
            status_label = result.replace("_", " ").title()
        else:
            status_label = "Unknown"

        # Promote "already in place" to OK kind so the green badge appears
        # and the counter shows it as a success rather than a failure.
        if already_in_place:
            status_kind = "ok"

        # "Already in place" counts as completed + verified (the file was
        # already at the desired state, which is the whole point).
        flag_completed = bool(is_ok) or bool(already_in_place)
        recent_rows.append({
            "timestamp": ts,
            "task_id": run_id,
            "title": title,
            "tier": tier,
            "status": result or "UNKNOWN",
            "status_kind": status_kind,
            "status_label": status_label,
            "applied": flag_completed,
            "verified": flag_completed,
            "completed": flag_completed,
            "files_changed": [target] if target else [],
            "files_changed_count": 1 if target else 0,
            "verifier": verify or ("passed" if is_ok else ""),
            "rollback": rollback_status,
            "report_path": "memory/luna_self_patch_attempts.jsonl",
            "duration_seconds": None,
            "backup_path": backup or None,
            "reason": rec.get("reason") if is_refused else None,
        })
        if len(recent_rows) >= 20:
            break

    sources_read: list[str] = []
    if READONLY_SOURCES["evidence_gate"].exists():
        sources_read.append("memory/luna_self_upgrade_evidence_gate.json")
    if READONLY_SOURCES["self_patch_attempts"].exists():
        sources_read.append("memory/luna_self_patch_attempts.jsonl")
    if READONLY_SOURCES["always_on_heartbeat"].exists():
        sources_read.append("memory/always_on/luna_always_on_heartbeat.json")
    if READONLY_SOURCES["always_on_latest_status"].exists():
        sources_read.append("memory/always_on/luna_always_on_latest_status.md")
    latest_attempt_md = MEMORY_DIR / "luna_latest_self_patch_attempt.md"
    if latest_attempt_md.exists():
        sources_read.append("memory/luna_latest_self_patch_attempt.md")

    last_update = str(gate.get("last_updated") or _now_iso())

    # Display-only enrichment from the higher-tier config so the existing
    # /api/self-upgrade/progress consumer can also see "we are beyond Tier 2"
    # without needing to add a separate fetch. The authoritative
    # higher-tier surface is /api/higher-tier/progress; this is a small
    # mirror so old UIs do not get stuck on "TIER 2 ACTIVE" forever.
    higher_cfg_for_self_upgrade = _safe_read_json(MEMORY_DIR / "luna_higher_tier_config.json") or {}
    current_effective_tier_str = str(higher_cfg_for_self_upgrade.get("current_effective_tier") or "")

    return {
        "ok": True,
        "generated_at": _now_iso(),
        "last_update": last_update,
        "current_tier": int(gate.get("current_allowed_tier", 1)),
        "current_allowed_tier": int(gate.get("current_allowed_tier", 1)),
        "current_effective_tier": current_effective_tier_str,
        "tier2_eligible": tier2_eligible_flag,
        "tier2_approved": bool(gate.get("tier2_approved", False)),
        "tier2_approval_time": gate.get("tier2_approval_time"),
        "tier2_approved_by": gate.get("tier2_approved_by"),
        "counts": {
            "t0_docs":           int(gate.get("tier0_success_count", 0)),
            "t1_tests":          int(gate.get("tier1_success_count", 0)),
            "t2_helpers":        int(gate.get("tier2_success_count", 0)),
            "t3":                int(gate.get("tier3_success_count", 0)),
            "failed":            int(gate.get("failed_self_patch_count", 0)),
            "rollback_success":  int(gate.get("rollback_success_count", 0)),
            "rollback_failures": rb_failures,
        },
        "tier_2_threshold": threshold_t2,
        "progress_to_tier_2": min(1.0, sum_t0t1 / float(max(threshold_t2, 1))),
        "recent_patches": recent_rows,
        "recent_cycles": [
            {
                "cycle_id": c.get("cycle_id"),
                "verdict":  c.get("verdict"),
                "started":  c.get("started"),
                "ended":    c.get("ended"),
                "attempted": c.get("tasks_attempted"),
                "succeeded": c.get("tasks_succeeded"),
                "failed":    c.get("tasks_failed"),
                "skipped":   c.get("tasks_skipped"),
                "report_path": c.get("_report_path"),
            }
            for c in cycles[:5]
        ],
        "sources_read": sources_read,
    }


# ---------------------------------------------------------------------------
# Higher-Tier Progress surface (display-only).
#
# Reads the higher-tier config + the latest tier_progression cycle report +
# the Tier 7 scoreboard + (optionally) the Tier 8 readiness report, plus the
# LunaTierProgressionEngine scheduled-task status, and returns a single
# JSON payload the dashboard can render without re-implementing parsers.
#
# This function never modifies any file. It never approves any tier. It
# only READS whitelisted state and returns a snapshot. The authoritative
# state remains the per-tier scripts and config files.
# ---------------------------------------------------------------------------
_TASK_NAME_RE = re.compile(r"^[A-Za-z0-9_\\\-]{1,64}$")


def _scheduled_task_status(task_name: str) -> dict[str, Any]:
    """Best-effort schtasks query. Read-only. Never throws to caller."""
    if not _TASK_NAME_RE.match(task_name or ""):
        return {"task_name": task_name, "queryable": False, "error": "invalid_task_name"}
    info: dict[str, Any] = {"task_name": task_name, "queryable": False}
    try:
        # creationflags hides conhost on Win11 per Warp memo - this code
        # path fires on every /api/higher-tier/progress request.
        _NO_WIN = 0x08000000 if os.name == "nt" else 0
        proc = subprocess.run(
            ["schtasks", "/Query", "/TN", task_name, "/V", "/FO", "LIST"],
            capture_output=True, text=True, timeout=4, check=False,
            creationflags=_NO_WIN,
        )
        if proc.returncode != 0:
            return {
                "task_name": task_name,
                "queryable": False,
                "error": (proc.stderr or "").strip()[:200] or "schtasks_returned_nonzero",
            }
        info["queryable"] = True
        for ln in proc.stdout.splitlines():
            ln = ln.rstrip()
            for prefix, label in (
                ("Status:", "status"),
                ("Last Result:", "last_result"),
                ("Last Run Time:", "last_run_time"),
                ("Next Run Time:", "next_run_time"),
                ("Scheduled Task State:", "state"),
                ("Logon Mode:", "logon_mode"),
            ):
                if ln.startswith(prefix):
                    info[label] = ln.split(":", 1)[1].strip()
                    break
        info["enabled"] = (str(info.get("state") or "")).lower() == "enabled"
        return info
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"task_name": task_name, "queryable": False, "error": f"{type(exc).__name__}"}


def build_higher_tier_progress_payload() -> dict[str, Any]:
    """Cached wrapper (2026-05-31). The raw builder shells out to schtasks
    (up to 2×, each capped but seconds-slow on this host) and was the SINGLE
    slow endpoint — /api/decision-brief measured >25s purely because it calls
    this. Serve it stale-while-revalidate so requests never block on the
    schtasks probe; the cost is paid once per TTL in a background refresh."""
    return _cached_build(_build_higher_tier_progress_payload_raw,
                         "higher_tier_progress")


def _build_higher_tier_progress_payload_raw() -> dict[str, Any]:
    """Display-only payload for the Tier 6+ progression surface.

    Reads (and only reads):
      - memory/luna_higher_tier_config.json
      - memory/tier_progression/luna_tier_progression_latest.md
      - memory/tier_progression/reports/ (most recent JSON if present)
      - memory/tier7/luna_tier7_scoreboard.json
      - memory/tier8/luna_tier8_readiness_report.json (if present)
      - logs/luna_live_feed.jsonl (mtime + last record)
    Plus a best-effort scheduled-task status for LunaTierProgressionEngine.

    Never modifies any file. Never approves any tier. Never enables any
    live-apply flag. Never invokes the kill-switch. Never reads outside
    the whitelisted memory/ + logs/ tree.
    """
    cfg_path = MEMORY_DIR / "luna_higher_tier_config.json"
    cfg = _safe_read_json(cfg_path) or {}

    # Latest progression report (parse a few key fields from the MD).
    tp_dir = MEMORY_DIR / "tier_progression"
    latest_md_path = tp_dir / "luna_tier_progression_latest.md"
    latest_md = ""
    try:
        if latest_md_path.exists():
            latest_md = latest_md_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        latest_md = ""

    def _grep(pattern: str) -> str:
        m = re.search(pattern, latest_md)
        return m.group(1).strip() if m else ""

    latest_decision = _grep(r"decision=([\w]+)")
    latest_passed_str = _grep(r"Actions passed:\s*(\d+)")
    latest_failed_str = _grep(r"Actions failed:\s*(\d+)")
    latest_highest = _grep(r"Highest eligible tier:\s*(\S+)")
    latest_cycle_id = _grep(r"Cycle ID:\s*(\S+)")
    latest_loop_id = _grep(r"Loop ID:\s*(\S+)")

    # Most recent tier_progression JSON report (if any) to show generated_at.
    latest_report_iso = None
    latest_report_path = None
    try:
        rdir = tp_dir / "reports"
        if rdir.exists() and rdir.is_dir():
            files = sorted(
                [p for p in rdir.glob("tier_progression_*.json")],
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )[:1]
            if files:
                obj = _safe_read_json(files[0]) or {}
                latest_report_iso = obj.get("generated_at")
                latest_report_path = "memory/tier_progression/reports/" + files[0].name
    except OSError:
        pass

    # Tier 7 scoreboard.
    sb7 = _safe_read_json(MEMORY_DIR / "tier7" / "luna_tier7_scoreboard.json") or {}
    tier7_by_role = sb7.get("by_role") if isinstance(sb7, dict) else None

    # Tier 8 readiness (optional).
    sb8 = _safe_read_json(MEMORY_DIR / "tier8" / "luna_tier8_readiness_report.json") or {}

    # Scheduled-task status (best-effort). Try the actually-installed
    # user-scope task name FIRST; fall back to the legacy name only if
    # the user-scope task is not present. Without this, this endpoint
    # reports queryable=true / enabled=false on the legacy name and
    # Mission Control mislabels Luna as "progression engine offline".
    task = {}
    for _name in ("LunaTierProgressionEngineUser", "LunaTierProgressionEngine"):
        _t = _scheduled_task_status(_name) or {}
        # Prefer the task that's actually enabled + queryable.
        if _t.get("queryable") and str(_t.get("state", "")).lower() == "enabled":
            task = _t
            break
        if not task:
            task = _t  # remember the first queryable result as fallback

    # Live feed freshness.
    live_feed_path = LOGS_DIR / "luna_live_feed.jsonl"
    live_feed_age_seconds = None
    last_event_iso = None
    try:
        if live_feed_path.exists():
            mtime = live_feed_path.stat().st_mtime
            live_feed_age_seconds = max(0, int(time.time() - mtime))
            tail = _safe_tail_jsonl(live_feed_path, limit=1)
            if tail:
                last_event_iso = tail[-1].get("iso_utc") or tail[-1].get("ts")
    except OSError:
        pass

    # Resolve current label.
    current = str(cfg.get("current_effective_tier") or "5L")
    tier6_en = bool(cfg.get("tier6_enabled"))
    tier7_en = bool(cfg.get("tier7_enabled"))
    tier8_en = bool(cfg.get("tier8_enabled"))
    tier9_en = bool(cfg.get("tier9_enabled"))
    tier10_en = bool(cfg.get("tier10_enabled"))
    tierx_en = bool(cfg.get("tier_x_apex_enabled"))

    # tier3_live_apply_enabled / allow_live_apply live in the tier5l config.
    cfg5 = _safe_read_json(MEMORY_DIR / "luna_tier5l_config.json") or {}
    live_apply_state = {
        "tier3_live_apply_enabled": bool(cfg5.get("tier3_live_apply_enabled", False)),
        "allow_live_apply": bool(cfg5.get("allow_live_apply", False)),
    }

    # Headline: use Level/Tier format for numeric tiers, legacy label for named rungs.
    try:
        _ct = int(current)
        headline = _fmt_level_tier(_ct) + " ACTIVE"
    except (ValueError, TypeError):
        headline = "TIER " + current + " ACTIVE"
    if tier8_en:
        subline = "Limited Live Helper Promotion (operator-driven; auto-apply OFF)"
    elif tier7_en:
        subline = "Multi-Agent Review Council active"
    elif tier6_en:
        subline = "Autonomous Sandbox Improvement Engine active"
    else:
        subline = "Sandbox / Lab Layer active"

    next_gate = ""
    if not tier8_en and tier7_en:
        # Use the Tier 8 readiness report's blockers if present.
        blockers = list(sb8.get("blockers", []))[:3] if isinstance(sb8, dict) else []
        if blockers:
            next_gate = "Tier 8 (Limited Live Helper Promotion) — blockers: " + "; ".join(blockers)
        else:
            next_gate = "Tier 8 (Limited Live Helper Promotion) — council-gated; restore drill required"
    elif not tier7_en and tier6_en:
        next_gate = "Tier 7 (Multi-Agent Review Council) — council-gated; awaiting runtime verification"
    elif not tier6_en:
        next_gate = "Tier 6 (Autonomous Sandbox Improvement Engine) — council-gated; awaiting runtime verification"
    elif tier9_en or tier10_en or tierx_en:
        # 2026-05-09 honesty fix: Tier 9/10/X were authorized on 2026-05-08
        # via explicit_serge_authorization. The stale "Tier 9+ remain
        # proposed/design-only" wording made the council-gated auto-promote
        # engine look disabled. Reflect actual operational state instead.
        next_gate = (
            "Tier 9+ expansion enabled (council-gated auto-promote ON); "
            "candidate supply lane is the next blocker — see "
            "/api/tier-truth.candidate_supply_status for honest counts."
        )
    else:
        next_gate = "Tier 9+ pending council authorization"

    sources_read: list[str] = []
    if cfg_path.exists():
        sources_read.append("memory/luna_higher_tier_config.json")
    if latest_md_path.exists():
        sources_read.append("memory/tier_progression/luna_tier_progression_latest.md")
    if (MEMORY_DIR / "tier7" / "luna_tier7_scoreboard.json").exists():
        sources_read.append("memory/tier7/luna_tier7_scoreboard.json")
    if (MEMORY_DIR / "tier8" / "luna_tier8_readiness_report.json").exists():
        sources_read.append("memory/tier8/luna_tier8_readiness_report.json")
    if live_feed_path.exists():
        sources_read.append("logs/luna_live_feed.jsonl")

    return {
        "ok": True,
        "generated_at": _now_iso(),
        "current_effective_tier": current,
        "headline": headline,
        "subline": subline,
        "next_gate": next_gate,
        "tier_flags": {
            "tier6_enabled": tier6_en,
            "tier7_enabled": tier7_en,
            "tier8_enabled": tier8_en,
            "tier9_enabled": tier9_en,
            "tier10_enabled": tier10_en,
            "tier_x_apex_enabled": tierx_en,
        },
        "live_apply_state": live_apply_state,
        "latest_progression": {
            "loop_id": latest_loop_id,
            "cycle_id": latest_cycle_id,
            "decision": latest_decision,
            "passed": int(latest_passed_str) if latest_passed_str.isdigit() else 0,
            "failed": int(latest_failed_str) if latest_failed_str.isdigit() else 0,
            "highest_eligible_tier": latest_highest,
            "report_path": "memory/tier_progression/luna_tier_progression_latest.md",
            "report_json_path": latest_report_path,
            "report_generated_at": latest_report_iso,
        },
        "tier7_scoreboard": {
            "total_reviews": int(sb7.get("total_reviews", 0)) if isinstance(sb7, dict) else 0,
            "approved_packets": int(sb7.get("approved_packets", 0)) if isinstance(sb7, dict) else 0,
            "hold_for_review_packets": int(sb7.get("hold_for_review_packets", 0)) if isinstance(sb7, dict) else 0,
            "do_not_promote_packets": int(sb7.get("do_not_promote_packets", 0)) if isinstance(sb7, dict) else 0,
            "rollback_failures": int(sb7.get("rollback_failures", 0)) if isinstance(sb7, dict) else 0,
            "live_apply_enabled": bool(sb7.get("live_apply_enabled", False)) if isinstance(sb7, dict) else False,
            "by_role": tier7_by_role if isinstance(tier7_by_role, dict) else None,
        },
        "tier8_readiness": {
            "tier8_eligible": bool(sb8.get("tier8_eligible", False)) if isinstance(sb8, dict) else False,
            "blockers": list(sb8.get("blockers", []))[:5] if isinstance(sb8, dict) else [],
            "next_required_actions": list(sb8.get("next_required_actions", []))[:5] if isinstance(sb8, dict) else [],
            "report_path": "memory/tier8/luna_tier8_readiness_report.md",
        },
        "scheduled_task": task,
        "live_feed": {
            "last_event_iso": last_event_iso,
            "age_seconds": live_feed_age_seconds,
            "stale_threshold_seconds": 600,
            "is_stale": (live_feed_age_seconds is not None and live_feed_age_seconds > 600),
        },
        # 2026-05-09 honesty fix: parallel surface to /api/tier-truth so any
        # consumer of /api/higher-tier/progress (including the legacy
        # higher-tier-card render path) can show "auto-upgrade: 0 eligible /
        # 0 applied (drained)" + "Archive promotions: N" instead of the
        # stale "Highest eligible tier: 6" claim from this engine. The
        # legacy "highest_eligible_tier" field above is preserved unchanged
        # so any older consumer keeps working.
        "auto_upgrade_engine": _build_tier_auto_upgrade_snapshot(),
        "archive_promotions":  _build_fast_store_archive_snapshot(),
        "council_added_tiers_truth": _build_council_added_tiers_truth(),
        "auto_promote_state":  _build_auto_promote_state(),
        "candidate_supply_status": _build_candidate_supply_status(),
        "nonstop_orchestrator": _build_nonstop_orchestrator_status(),
        "lane_router":         _build_lane_router_status(),
        "sources_read": sources_read,
        "canonical_truth_summary": _canonical_truth_summary_safe(),
    }


# ---------------------------------------------------------------------------
# /api/tier-truth — synthesized live tier state for the Evolution Command Center
# UI. Always derives the displayable label from luna_higher_tier_config.json
# rather than the legacy gate (which only knows about Tier 1/2). Read-only.
# ---------------------------------------------------------------------------

# All tier rungs we render in the ladder, in promotion order.
_TIER_LADDER: tuple[str, ...] = ("5L", "6", "7", "8", "9", "10", "X")

# Friendly per-tier titles. Source-of-truth for the hero/subtitle.
_TIER_TITLES: dict[str, dict[str, str]] = {
    "5L": {"label": "Sandbox / Lab Layer",                 "category": "advisory"},
    "6":  {"label": "Autonomous Sandbox Improvement",      "category": "sandbox"},
    "7":  {"label": "Multi-Agent Review Council",          "category": "review"},
    "8":  {"label": "Limited Live Helper Promotion",       "category": "live-helper"},
    "9":  {"label": "Assisted Module Promotion",           "category": "module"},
    "10": {"label": "Sovereign Orchestration Layer",       "category": "sovereign"},
    "X":  {"label": "Apex / Reserved",                     "category": "apex"},
}


def _fmt_level_tier(tier_num: int) -> str:
    """Format a global tier number as 'Level X Tier Y' where Y is 1-50 within the level."""
    if tier_num <= 0:
        return "Tier 0"
    level = (tier_num - 1) // 50 + 1
    tier_in_level = ((tier_num - 1) % 50) + 1
    return "Level %d Tier %d" % (level, tier_in_level)


def _normalize_tier_key(value) -> str:
    """Coerce '8', 8, 'tier8', 'Tier-8', 'X', 'apex', '12', '237' etc. to a
    ladder key. The dashboard's visible ladder still has 7 fixed rungs
    (5L, 6, 7, 8, 9, 10, X), but Luna's real current_effective_tier can
    be any integer from 1 to 500 in the extended framework. Returning a
    raw integer string for tiers > 10 lets the rest of the pipeline
    surface "TIER 12 ACTIVE" instead of silently downgrading to 5L.

    2026-05-09 fix per playbook §22 (same bug class as §15 tier-ceiling
    enumeration in PowerShell). Detector for THIS bug class would need
    to scan Python too - currently Luna_Detect_SelfDefeating_TierChecks.ps1
    only checks .ps1 files."""
    if value is None:
        return "5L"
    s = str(value).strip().lower().replace("tier", "").replace("-", "").replace("_", "").strip()
    if s in {"x", "apex"}:
        return "X"
    if s in {"5l", "5"}:
        return "5L"
    if s in _TIER_LADDER:
        return s
    if s in {"6", "7", "8", "9", "10"}:
        return s
    # Accept any positive integer 1..500 (and beyond) in the extended
    # framework. Don't fall through to "5L" - that's the bug that hid
    # Luna's real tier behind a legacy label for the entire afternoon.
    try:
        n = int(s)
        if 1 <= n <= 9999:
            return str(n)
    except (ValueError, TypeError):
        pass
    return "5L"


def build_opencode_status_payload() -> dict[str, Any]:
    """Read-only OpenCode bridge status, derived from the existing detect
    report. Never executes opencode; never mutates state."""
    detect_path = MEMORY_DIR / "opencode" / "luna_opencode_detect_report.json"
    cfg_path    = MEMORY_DIR / "opencode" / "luna_opencode_bridge_config.json"

    detect = _safe_read_json(detect_path) or {}
    cfg    = _safe_read_json(cfg_path) or {}

    cli      = (detect.get("cli") or {}) if isinstance(detect, dict) else {}
    desktop  = (detect.get("desktop") or {}) if isinstance(detect, dict) else {}
    sidecar  = (detect.get("sidecar") or {}) if isinstance(detect, dict) else {}

    cli_found     = bool(cli.get("found"))
    desktop_found = bool(desktop.get("found"))
    sidecar_found = bool(sidecar.get("found"))
    bridge_enabled = bool(cfg.get("enabled", False))

    # Pick the most-meaningful state label.
    if bridge_enabled and cli_found:
        state = "ONLINE"
    elif sidecar_found:
        state = "SIDECAR"
    elif desktop_found:
        state = "DESKTOP"
    elif cli_found:
        state = "CLI"
    else:
        state = "OFFLINE"

    sources_read: list[str] = []
    if detect_path.exists():
        sources_read.append("memory/opencode/luna_opencode_detect_report.json")
    if cfg_path.exists():
        sources_read.append("memory/opencode/luna_opencode_bridge_config.json")

    return {
        "ok": True,
        "generated_at": _now_iso(),
        "state": state,
        "bridge_enabled": bridge_enabled,
        "cli_found":      cli_found,
        "cli_path":       cli.get("path"),
        "cli_version":    cli.get("version"),
        "desktop_found":  desktop_found,
        "desktop_exe":    desktop.get("exe"),
        "sidecar_found":  sidecar_found,
        "detect_generated_at": detect.get("generated_at") if isinstance(detect, dict) else None,
        "sources_read": sources_read,
    }


def _build_continuous_supervisor_snapshot() -> dict[str, Any]:
    """Read-only snapshot of the Luna_Continuous_Supervisor.ps1 state.

    Returns:
      {
        "enabled":                bool,    # config flag (false = supervisor sleeping)
        "config_cadence_seconds": int,
        "log_age_seconds":        int|None, # how recent is the last log line
        "log_recent_event":       str,      # tail of the last log line
        "alive":                  bool,     # heuristic: log <= 120s old
      }
    Never mutates state. Never reads secret material.
    """
    cfg_path = MEMORY_DIR / "luna_continuous_supervisor_config.json"
    log_path = LOGS_DIR / "luna_continuous_supervisor.log"
    out: dict[str, Any] = {
        "enabled":               False,
        "config_cadence_seconds": None,
        "log_age_seconds":       None,
        "log_recent_event":      "",
        "alive":                 False,
    }
    cfg = _safe_read_json(cfg_path) or {}
    if isinstance(cfg, dict):
        out["enabled"]                = bool(cfg.get("enabled", False))
        out["config_cadence_seconds"] = cfg.get("cadence_seconds")
        out["max_minutes_per_cycle"]  = cfg.get("max_minutes_per_cycle")
        out["max_actions_per_cycle"]  = cfg.get("max_actions_per_cycle")
        out["external_watchdog_grace_seconds"] = cfg.get("external_watchdog_grace_seconds")
    try:
        if log_path.exists():
            mt = log_path.stat().st_mtime
            out["log_age_seconds"] = max(0, int(time.time() - mt))
            # Alive threshold = 30 min. A real cycle can run up to
            # MaxMinutesPerCycle=45 without writing to the log (the
            # supervisor only logs on cycle start + cycle end).
            # 120s would falsely call long-but-healthy cycles dead.
            out["alive"] = out["log_age_seconds"] <= 1800
            # Tail: last non-empty line, trimmed to 240 chars.
            data = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            for line in reversed(data):
                line = line.strip()
                if line:
                    out["log_recent_event"] = line[:240]
                    break
    except OSError:
        pass
    return out


def _build_tier_auto_upgrade_snapshot() -> dict[str, Any]:
    """Read-only snapshot of Luna_Tier_AutoUpgrade_Engine.ps1's latest cycle.

    Prefers the engine-written `memory/tier_auto_upgrade/latest_cycle.json`
    (added 2026-05-09) which carries skip_reasons + packets_scanned. Falls
    back to parsing the latest cycle markdown if the JSON file does not
    yet exist (older engine builds).

    Surfaces the auto-upgrade engine's truth so the dashboard can answer
    "why is eligible 0?" honestly. Per the standing 2026-05-09 honesty rule.
    """
    base = MEMORY_DIR / "tier_auto_upgrade" / "cycle_reports"
    latest_json_path = MEMORY_DIR / "tier_auto_upgrade" / "latest_cycle.json"
    out: dict[str, Any] = {
        "ok": False,
        "run_id": None,
        "generated_at": None,
        "eligible": None,
        "applied": None,
        "failed": None,
        "rolled_back": None,
        "rollback_failed": None,
        "cycle_elapsed_seconds": None,
        "packets_scanned": None,
        "packets_total": None,
        "skip_reasons": {},
        "report_path": None,
        "status_label": "unknown",
        "blocker_explanation": None,
    }

    # ---- Preferred path: read engine-written latest_cycle.json ----
    if latest_json_path.exists():
        try:
            j = _safe_read_json(latest_json_path) or {}
            if isinstance(j, dict):
                out["ok"] = True
                out["run_id"]                = j.get("run_id")
                out["generated_at"]          = j.get("generated_at")
                out["eligible"]              = j.get("eligible")
                out["applied"]               = j.get("applied")
                out["failed"]                = j.get("failed")
                out["rolled_back"]           = j.get("rolled_back")
                out["rollback_failed"]       = j.get("rollback_failed")
                out["cycle_elapsed_seconds"] = j.get("elapsed_seconds")
                out["packets_scanned"]       = j.get("packets_scanned")
                out["packets_total"]         = j.get("packets_total")
                if isinstance(j.get("skip_reasons"), dict):
                    # Drop zero-valued reasons so the UI focuses on what
                    # actually caused skips.
                    out["skip_reasons"] = {
                        k: int(v) for k, v in j["skip_reasons"].items()
                        if isinstance(v, (int, float)) and int(v) > 0
                    }
                out["report_path"] = j.get("report_path")
        except Exception:
            pass

    # ---- Fallback: parse latest cycle markdown ----
    if not out["ok"]:
        if not base.is_dir():
            out["status_label"] = "no_engine_dir"
            return out
        try:
            files = sorted(
                base.glob("cycle_*.md"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
        except OSError:
            files = []
        if not files:
            out["status_label"] = "no_cycles_yet"
            return out
        latest = files[0]
        try:
            text = latest.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return out

        def _g(pat: str) -> str:
            m = re.search(pat, text)
            return m.group(1).strip() if m else ""

        out["ok"] = True
        out["run_id"] = _g(r"Run ID:\s*(\S+)") or None
        out["generated_at"] = _g(r"Generated:\s*(\S+)") or None
        elig = _g(r"Eligible candidates:\s*(\d+)")
        appl = _g(r"Applied:\s*(\d+)")
        fail = _g(r"Failed:\s*(\d+)")
        rolb = _g(r"Rolled back:\s*(\d+)")
        rolf = _g(r"Rollback failed:\s*(\d+)")
        elap = _g(r"Cycle elapsed:\s*(\d+)s")
        out["eligible"] = int(elig) if elig.isdigit() else None
        out["applied"] = int(appl) if appl.isdigit() else None
        out["failed"] = int(fail) if fail.isdigit() else None
        out["rolled_back"] = int(rolb) if rolb.isdigit() else None
        out["rollback_failed"] = int(rolf) if rolf.isdigit() else None
        out["cycle_elapsed_seconds"] = int(elap) if elap.isdigit() else None
        out["report_path"] = (
            "memory/tier_auto_upgrade/cycle_reports/" + latest.name
        )

    # ---- Honest status_label + blocker_explanation ----
    if (out["rollback_failed"] or 0) > 0:
        out["status_label"] = "blocked_rollback_failed"
        out["blocker_explanation"] = (
            "Rollback failed on a previous promotion. Engine halts until "
            "memory/tier_auto_upgrade/BLOCKED_NEEDS_SERGE.md is removed "
            "and operator confirms restore from "
            "memory/tier9/promotion_backups/."
        )
    elif (out["applied"] or 0) > 0:
        out["status_label"] = "advancing"
        out["blocker_explanation"] = None
    elif out["eligible"] == 0:
        out["status_label"] = "drained"
        # Compose an honest "why" from the skip-reason buckets so the
        # dashboard never reads "stuck" when the queue is just drained.
        sr = out["skip_reasons"] or {}
        if sr:
            top = sorted(sr.items(), key=lambda kv: kv[1], reverse=True)[:3]
            top_str = "; ".join(f"{k}={v}" for k, v in top)
            out["blocker_explanation"] = (
                "Queue drained — every Tier 7 SAFE packet was rejected by a "
                "honest gate. Top skip reasons: " + top_str + ". "
                "To resume: produce NEW SAFE packets that target FastStore "
                "keys not yet in the index, OR generate codegen-pathway "
                "candidates with non-empty diffs and matching live SHA."
            )
        else:
            out["blocker_explanation"] = (
                "Queue drained — engine ran but found no Tier 7 packets to "
                "evaluate. To resume: run the Tier 7 review council to "
                "produce new SAFE_TO_PROMOTE packets."
            )
    else:
        out["status_label"] = "running"
        out["blocker_explanation"] = None
    return out


def _build_council_added_tiers_truth() -> dict[str, Any]:
    """Detect the source-of-truth mismatch between the two
    council_added_tiers files (per failure mode 9, 2026-05-09).

      - memory/luna_council_added_tiers.json  (the *config pointer*; in
        practice an empty placeholder with `"tiers": []`)
      - memory/tier9/luna_council_added_tiers.json  (what the engine
        actually reads; the populated 1.3 MB file with tier_definitions)

    The two are NOT autonomously editable to "merge" them — both are
    inviolate-floor-adjacent or directly inside it. The honest fix is to
    surface the divergence so the dashboard says clearly which file is
    authoritative.
    """
    config_pointer = MEMORY_DIR / "luna_council_added_tiers.json"
    engine_source = MEMORY_DIR / "tier9" / "luna_council_added_tiers.json"

    out: dict[str, Any] = {
        "config_pointer_path":      "memory/luna_council_added_tiers.json",
        "config_pointer_exists":    config_pointer.exists(),
        "config_pointer_size":      None,
        "config_pointer_tiers":     None,
        "engine_source_path":       "memory/tier9/luna_council_added_tiers.json",
        "engine_source_exists":     engine_source.exists(),
        "engine_source_size":       None,
        "engine_source_tiers":      None,
        "authoritative_path":       "memory/tier9/luna_council_added_tiers.json",
        "divergence_detected":      False,
        "divergence_explanation":   None,
    }

    def _read_tiers(p: Path) -> tuple[int | None, int | None]:
        if not p.exists():
            return None, None
        try:
            sz = p.stat().st_size
        except OSError:
            sz = None
        d = _safe_read_json(p) or {}
        if not isinstance(d, dict):
            return sz, None
        td = d.get("tier_definitions") or d.get("tiers") or []
        if not isinstance(td, list):
            return sz, None
        return sz, len(td)

    out["config_pointer_size"], out["config_pointer_tiers"] = _read_tiers(config_pointer)
    out["engine_source_size"],  out["engine_source_tiers"]  = _read_tiers(engine_source)

    # 2026-05-11 Serge requested the "source warning" be stopped (the
    # warning is technically correct but it was noisy + cosmetic; the
    # engine was already reading the right authoritative file). The two
    # files share an unfortunate filename but are DIFFERENT artifacts
    # with different schemas:
    #   - memory/luna_council_added_tiers.json: policy/governance file
    #     with `writer_constraints` and an always-empty `tiers: []`
    #     array. It was never meant to carry tier_definitions; it
    #     documents WHO may write them and HOW.
    #   - memory/tier9/luna_council_added_tiers.json: the actual data
    #     file with `tier_definitions: [...]` (the 491-entry ladder).
    #
    # When the config-pointer file is the policy-only artifact (detected
    # by presence of `writer_constraints` field + empty `tiers` array),
    # report it as a policy file rather than as a stale data divergence.
    def _is_policy_only_file(p: Path) -> bool:
        if not p.exists():
            return False
        d = _safe_read_json(p) or {}
        if not isinstance(d, dict):
            return False
        has_writer_constraints = isinstance(d.get("writer_constraints"), list)
        tiers_field = d.get("tiers")
        tiers_is_empty_list = isinstance(tiers_field, list) and len(tiers_field) == 0
        no_tier_definitions = "tier_definitions" not in d
        return has_writer_constraints and tiers_is_empty_list and no_tier_definitions

    config_pointer_is_policy_only = _is_policy_only_file(config_pointer)
    out["config_pointer_role"] = (
        "policy_file" if config_pointer_is_policy_only else "data_file"
    )

    if config_pointer_is_policy_only:
        # Not a divergence -- the policy file is intentionally schema-distinct
        # from the data file. Surface it as informational so the dashboard
        # stops nagging, but keep the authoritative path visible.
        out["divergence_detected"] = False
        out["divergence_explanation"] = None
        out["informational_note"] = (
            f"memory/luna_council_added_tiers.json is the council policy/"
            f"governance file (schema: writer_constraints + empty tiers array). "
            f"Tier definition data lives at memory/tier9/"
            f"luna_council_added_tiers.json ({out['engine_source_tiers']} "
            f"entries). The engine reads the data file directly; the policy "
            f"file is read-only governance reference."
        )
    elif (
        out["config_pointer_exists"]
        and out["engine_source_exists"]
        and (out["config_pointer_tiers"] or 0) != (out["engine_source_tiers"] or 0)
    ):
        # True divergence: same schema, different data. Surface loudly.
        out["divergence_detected"] = True
        out["divergence_explanation"] = (
            f"luna_higher_tier_config.json::council_added_tiers_pointer "
            f"points at memory/luna_council_added_tiers.json "
            f"(tier_definitions={out['config_pointer_tiers']}) but "
            f"Luna_Tier_AutoUpgrade_Engine.ps1 reads "
            f"memory/tier9/luna_council_added_tiers.json "
            f"(tier_definitions={out['engine_source_tiers']}). The engine's "
            f"file is authoritative — it carries the populated tier 10..500 "
            f"definitions. The config pointer is a stale placeholder. Both "
            f"files are inviolate-adjacent so the divergence is surfaced "
            f"here rather than silently merged."
        )
    return out


def _build_lane_router_status() -> dict[str, Any]:
    """Read-only snapshot of the Tier 500 lane router's latest decision.

    The lane router (Luna_Tier500_Lane_Router.ps1) runs PreviewOnly inside
    every orchestrator cycle and reports the selected lane + the next
    safest lane + exact blocker. This helper surfaces that decision on
    /api/tier-truth so the dashboard can show "current lane / next lane
    / next blocker" without parsing the orchestrator's nested report.
    """
    p = MEMORY_DIR / "tier_auto_upgrade" / "tier500_lane_router_latest.json"
    out: dict[str, Any] = {
        "ok":              False,
        "run_id":          None,
        "generated_at":    None,
        "selected_lane":   None,
        "selected_reason": None,
        "next_lane":       None,
        "next_reason":     None,
        "next_blocker":    None,
        "next_action":     None,
        "skipped_lanes":   [],
        "lane_readiness":  {},
        "counters":        {},
    }
    d = _safe_read_json(p)
    if isinstance(d, dict):
        out["ok"] = True
        out["run_id"]          = d.get("run_id")
        out["generated_at"]    = d.get("generated_at")
        out["selected_lane"]   = d.get("selected_lane")
        out["selected_reason"] = d.get("selected_reason")
        out["next_lane"]       = d.get("next_lane")
        out["next_reason"]     = d.get("next_reason")
        out["next_blocker"]    = d.get("next_blocker")
        out["next_action"]     = d.get("next_action")
        out["skipped_lanes"]   = list(d.get("skipped_lanes") or [])
        out["lane_readiness"]  = dict(d.get("lane_readiness") or {})
        out["counters"]        = dict(d.get("counters") or {})
    return out


def _build_nonstop_orchestrator_status() -> dict[str, Any]:
    """Read-only snapshot of the Tier 500 nonstop orchestrator's latest cycle.

    Reads `memory/tier_auto_upgrade/nonstop_orchestrator_latest.json` written
    by Luna_Tier500_Nonstop_Orchestrator.ps1 on every fire. This is the
    AUTHORITATIVE source for "is Luna progressing right now?" because the
    orchestrator owns the strict execution order (gen -> council -> upgrade
    -> truth-sync) and avoids the ISO-tie ambiguity that the multi-lane
    freshness synthesis still has when individual tasks fire concurrently.
    """
    p = MEMORY_DIR / "tier_auto_upgrade" / "nonstop_orchestrator_latest.json"
    out: dict[str, Any] = {
        "ok": False,
        "installed_and_running": False,
        "run_id": None,
        "generated_at": None,
        "cycle_elapsed_seconds": None,
        "state": None,
        "next_blocker": None,
        "next_action": None,
        "current_effective_tier_at_cycle": None,
        "lanes": {},
        "promoted_targets": [],
        "drill_age_hours": None,
        "isolation_age_hours": None,
        "report_path": "memory/tier_auto_upgrade/nonstop_orchestrator_latest.md",
    }
    d = _safe_read_json(p)
    if isinstance(d, dict):
        out["ok"] = True
        out["installed_and_running"] = True
        out["run_id"] = d.get("run_id")
        out["generated_at"] = d.get("generated_at")
        out["cycle_elapsed_seconds"] = d.get("cycle_elapsed_seconds")
        out["state"] = d.get("state")
        out["next_blocker"] = d.get("next_blocker")
        out["next_action"] = d.get("next_action")
        out["current_effective_tier_at_cycle"] = d.get("current_effective_tier")
        out["drill_age_hours"] = d.get("drill_age_hours")
        out["isolation_age_hours"] = d.get("isolation_age_hours")
        lanes = d.get("lanes") or {}
        # 2026-05-10: tier_advancement.outcome is one of:
        #   advanced / cooldown_active / evidence_gate_failed /
        #   blocked_safety / skipped_cycle_budget /
        #   no_advancement_due_this_cycle.
        # The orchestrator guarantees it is never null. Surface it on
        # the dashboard alongside the four classic lanes so the API
        # truth matches the orchestrator's truth.
        ta = lanes.get("tier_advancement") or {}
        if not isinstance(ta, dict):
            ta = {}
        if not ta.get("outcome"):
            ta = {"attempted": False, "outcome": "no_advancement_due_this_cycle",
                  "detail": "orchestrator did not record a tier_advancement block this cycle"}
        out["lanes"] = {
            "generator":        lanes.get("generator")        or {},
            "council":          lanes.get("council")          or {},
            "auto_upgrade":     lanes.get("auto_upgrade")     or {},
            "truth_sync":       lanes.get("truth_sync")       or {},
            "lane_router":      lanes.get("lane_router")      or {},
            "tests_lane_proof": lanes.get("tests_lane_proof") or {"attempted": False, "outcome": "no_advancement_due_this_cycle"},
            "tier_advancement": ta,
        }
        # Top-level convenience fields for the dashboard headline.
        out["tier_advancement_outcome"] = ta.get("outcome")
        out["tier_advancement_detail"]  = ta.get("detail")
        out["tier_advancement_new_cet"] = ta.get("new_current_effective_tier")
        au = (lanes.get("auto_upgrade") or {})
        # 2026-05-10 serialization defense: PowerShell's ConvertTo-Json may
        # emit a single-element promoted_targets as a scalar STRING (after
        # Select-Object -Unique unwraps the array). Naive list(value) on a
        # string iterates as a char array. The orchestrator was patched to
        # force array semantics with @(...), but we defend on the read
        # side too so a future regression cannot break the dashboard list.
        pt = au.get("promoted_targets") or []
        if isinstance(pt, str):
            pt = [pt]  # treat scalar string as single-element list
        elif not isinstance(pt, list):
            try:
                pt = list(pt)
            except TypeError:
                pt = [pt]
        out["promoted_targets"] = pt
    return out


def _build_candidate_supply_status() -> dict[str, Any]:
    """Read-only summary of where the Tier 9+ candidate supply lane stands.

    Reads `memory/tier_auto_upgrade/candidate_supply_status.json` written by
    Luna_Path_To_Tier500_Run.ps1 on every preview/apply. Falls back to an
    on-the-fly compute if the file is missing.

    Surfaces the truthful breakdown of the 491 council-authorized tier
    definitions so the dashboard never has to guess "why is eligible 0?":
      - archived: FastStore-keyed and already in the index (success state)
      - faststore_unarchived: Phase B can emit synthetic packets
      - luna_modules / tests / ps1 / other_real: need real codegen
      - empty_allowlist: malformed tier definition
    Per the standing 2026-05-09 honesty rule.
    """
    p = MEMORY_DIR / "tier_auto_upgrade" / "candidate_supply_status.json"
    out: dict[str, Any] = {
        "ok": False,
        "generated_at": None,
        "total_tier_definitions": None,
        "archived": None,
        "faststore_unarchived": None,
        "luna_modules_pending": None,
        "tests_pending": None,
        "ps1_pending": None,
        "other_real_pending": None,
        "empty_allowlist": None,
        "archive_completion_pct": None,
        "phase_a_skipped": None,
        "phase_b_emitted_last_run": None,
        # 2026-05-09 addendum: surface the new generator's run-specific
        # counters so the dashboard can show "0 eligible / 2 sandbox runs
        # PASSED awaiting council review" instead of just "drained".
        "generated_candidates_last_run": None,
        "sandbox_runs_attempted": None,
        "sandbox_runs_passed": None,
        "sandbox_runs_failed": None,
        "sandbox_runs_noop": None,
        "written_by": None,
        "next_blocker": None,
        "honest_explanation": None,
    }

    if p.exists():
        d = _safe_read_json(p)
        if isinstance(d, dict):
            out["ok"] = True
            out["generated_at"]            = d.get("generated_at")
            out["total_tier_definitions"]  = d.get("total_tier_definitions")
            out["archived"]                = d.get("archived")
            out["faststore_unarchived"]    = d.get("faststore_unarchived")
            out["luna_modules_pending"]    = d.get("luna_modules_pending")
            out["tests_pending"]           = d.get("tests_pending")
            out["ps1_pending"]             = d.get("ps1_pending")
            out["other_real_pending"]      = d.get("other_real_pending")
            out["empty_allowlist"]         = d.get("empty_allowlist")
            out["archive_completion_pct"]  = d.get("archive_completion_pct")
            out["phase_a_skipped"]         = d.get("phase_a_skipped")
            out["phase_b_emitted_last_run"] = d.get("phase_b_emitted_this_run")
            # New generator fields — gracefully missing on older runs.
            out["generated_candidates_last_run"] = d.get("generated_candidates")
            out["sandbox_runs_attempted"] = d.get("sandbox_runs_attempted")
            out["sandbox_runs_passed"]    = d.get("sandbox_runs_passed")
            out["sandbox_runs_failed"]    = d.get("sandbox_runs_failed")
            out["sandbox_runs_noop"]      = d.get("sandbox_runs_noop")
            out["written_by"]             = d.get("written_by")
            out["next_blocker"]            = d.get("next_blocker")
            # Prefer the generator's honest_explanation when present
            # (it has the freshest insight into why supply is what it is).
            if d.get("honest_explanation"):
                out["honest_explanation"]   = d.get("honest_explanation")

    # 2026-05-09 freshness synthesis: prefer the nonstop orchestrator's
    # authoritative state when present (it owns the strict execution
    # order and avoids ISO-tie races). Fall back to per-lane ISO compare
    # only when the orchestrator hasn't run yet. The clearer lane-state
    # taxonomy is documented in
    # LUNA_FINAL_NONSTOP_TIER500_COMMAND_FOR_CLAUDE_2026_05_09.txt item 4.
    try:
        # Authoritative source: nonstop orchestrator, when fresh.
        nonstop_path = MEMORY_DIR / "tier_auto_upgrade" / "nonstop_orchestrator_latest.json"
        nonstop = _safe_read_json(nonstop_path) or {}

        sb7_path = MEMORY_DIR / "tier7" / "luna_tier7_scoreboard.json"
        latest_cycle_path = MEMORY_DIR / "tier_auto_upgrade" / "latest_cycle.json"
        sb7 = _safe_read_json(sb7_path) or {}
        lc = _safe_read_json(latest_cycle_path) or {}
        gen_iso = out.get("generated_at") or ""
        cnc_iso = sb7.get("last_updated") if isinstance(sb7, dict) else ""
        eng_iso = lc.get("generated_at") if isinstance(lc, dict) else ""
        ns_iso = nonstop.get("generated_at") if isinstance(nonstop, dict) else ""
        lanes = [
            ("generator", gen_iso),
            ("council", cnc_iso),
            ("auto_upgrade", eng_iso),
            ("nonstop_orchestrator", ns_iso),
        ]
        lanes_with_iso = [(n, i) for (n, i) in lanes if i]
        latest_lane_name, latest_lane_iso = (
            max(lanes_with_iso, key=lambda kv: kv[1]) if lanes_with_iso else ("none", None)
        )
        out["latest_lane_run"] = {
            "name":              latest_lane_name,
            "iso_utc":           latest_lane_iso,
            "generator_iso":     gen_iso or None,
            "council_iso":       cnc_iso or None,
            "auto_upgrade_iso":  eng_iso or None,
            "nonstop_orchestrator_iso": ns_iso or None,
            "council_total_reviews":   sb7.get("total_reviews"),
            "council_approved":        sb7.get("approved_packets"),
            "auto_upgrade_eligible":   lc.get("eligible"),
            "auto_upgrade_applied":    lc.get("applied"),
            "auto_upgrade_failed":     lc.get("failed"),
        }
        # If the orchestrator ran most recently, its `state` field is the
        # ground truth for the whole conveyor. 2026-05-10 priority fix
        # per master command item 1: prefer the orchestrator's own
        # next_blocker text verbatim when it's a hard blocked_* state,
        # so /api/tier-truth surfaces the SAME exact string the
        # orchestrator wrote (e.g. "codegen_pathway_exhausted_real_diffs_required").
        if latest_lane_name == "nonstop_orchestrator" and isinstance(nonstop, dict):
            ns_state = str(nonstop.get("state") or "")
            ns_blocker = nonstop.get("next_blocker")
            au_lane = (nonstop.get("lanes") or {}).get("auto_upgrade") or {}
            applied_in_cycle = int(au_lane.get("applied") or 0)
            # Hard-blocker states: use the orchestrator's own next_blocker
            # text verbatim (it's the most descriptive; matches the .md report).
            if ns_state in {
                "blocked_codegen_exhausted",
                "blocked_safety",
                "blocked_manual_approval",     # legacy: kept for record schema
                "blocked_council_quorum",      # normal-workflow replacement
                "blocked_inviolate_floor",     # true exception
                "blocked_rollback_failed",
            }:
                out["next_blocker"] = ns_blocker or ns_state
                out["primary_blocker"] = out["next_blocker"]
                out["primary_blocker_source"] = "nonstop_orchestrator"
            elif ns_state == "advancing":
                out["next_blocker"] = "auto_upgrade_applied_" + str(applied_in_cycle)
            elif ns_state == "drained_waiting_for_next_generation":
                out["next_blocker"] = "auto_upgrade_drained_after_latest_cycle"
            elif ns_state == "eligible_pending":
                out["next_blocker"] = "auto_upgrade_eligible_awaiting_apply_capacity"
            elif ns_blocker:
                out["next_blocker"] = ns_blocker
        elif latest_lane_name == "auto_upgrade" and isinstance(lc.get("applied"), int):
            if lc["applied"] > 0:
                out["next_blocker"] = "auto_upgrade_advancing_applied_" + str(lc["applied"])
            elif (lc.get("eligible") or 0) > 0:
                out["next_blocker"] = "auto_upgrade_eligible_" + str(lc["eligible"]) + "_awaiting_apply_capacity"
            elif (lc.get("eligible") or 0) == 0:
                out["next_blocker"] = "auto_upgrade_drained_council_caught_up_no_safe_packets_remain"
        elif latest_lane_name == "council" and isinstance(sb7.get("approved_packets"), int):
            # Council most recent: SAFE packets are queued for the engine.
            out["next_blocker"] = "safe_packets_created_awaiting_auto_upgrade"
    except Exception:
        # Never let the freshness-synthesis pass take down /api/tier-truth.
        pass

    # 2026-05-10 final command: lane-readiness vocabulary for tests/PS1/
    # other-file unsupported target classes. Each lane is currently
    # NEEDS_IMPLEMENTATION (no Apply-mode runner exists; Tier 6 candidate
    # runner refuses these target classes by ValidateSet/path-prefix).
    # When a lane scaffolds a PreviewOnly script + tests, the matching
    # state below should change to *_PREVIEW_READY. When the lane
    # ships an Apply-mode runner with full tests, state becomes
    # *_LIVE. Per the user's command file 2026-05-10 items 8-10.
    out["lane_readiness"] = {
        "luna_modules_lane":      "LIVE",  # Tier 6 runner accepts these
        "tests_lane":             "NEEDS_IMPLEMENTATION",
        "ps1_lane":               "NEEDS_IMPLEMENTATION",
        "other_file_lane":        "NEEDS_IMPLEMENTATION",
        "real_codegen_producer":  "DISABLED",  # Aider/OpenCode hook, off by default
    }
    router_readiness = _build_lane_router_status().get("lane_readiness")
    if isinstance(router_readiness, dict):
        out["lane_readiness"].update(router_readiness)
    # 2026-05-10: surface the DO_NOT_PROMOTE classification report so the
    # dashboard can show counts of source_repair-needed targets.
    dnp_path = MEMORY_DIR / "tier_auto_upgrade" / "tier500_do_not_promote_classification.json"
    dnp = _safe_read_json(dnp_path) or {}
    if isinstance(dnp, dict) and dnp.get("scanned_failures"):
        out["denied_classification"] = {
            "scanned_failures":             int(dnp.get("scanned_failures") or 0),
            "distinct_targets_with_failures": int(dnp.get("distinct_targets_with_failures") or 0),
            "by_primary_classification":    dict(dnp.get("by_primary_classification") or {}),
            "report_path": "memory/tier_auto_upgrade/tier500_do_not_promote_classification.md",
        }

    # Honest explanation derived from the buckets.
    blocker = out.get("next_blocker")
    if blocker == "codegen_pathway_exhausted_real_diffs_required":
        codegen_pending = (
            (out.get("luna_modules_pending") or 0) +
            (out.get("tests_pending") or 0) +
            (out.get("ps1_pending") or 0) +
            (out.get("other_real_pending") or 0)
        )
        out["honest_explanation"] = (
            f"Phase B (FastStore-keyed synthetic packets) is exhausted — every "
            f"FastStore-keyed council tier definition is already in the archive index. "
            f"The remaining {codegen_pending} council tier definitions target real files "
            f"(luna_modules / tests / ps1 / other) that require Tier 6 codegen with "
            f"non-empty diffs. The current Tier 6 runner only produces comment-block "
            f"prepends, and once a live source already carries a `# tier6-candidate` "
            f"header from an earlier promotion the runner returns no-op. So the supply "
            f"lane is genuinely stalled awaiting real codegen output — NOT a generator "
            f"or scheduler bug. Operator action required: either (a) run a real codegen "
            f"session that produces meaningful diffs, or (b) redesign the remaining tier "
            f"definitions, or (c) extend the AnyTier promote runner with a "
            f"council-acknowledgment-archival mode for non-FastStore real files."
        )
    elif blocker == "phase_b_can_emit_more_run_apply":
        out["honest_explanation"] = (
            f"Phase B can still emit {out.get('faststore_unarchived')} synthetic "
            f"packets for unarchived FastStore-keyed tier definitions. Run "
            f"Luna_Path_To_Tier500_Run.ps1 -Apply to populate them; the auto-upgrade "
            f"engine will then pick them up on its next cycle."
        )
    elif not out["ok"]:
        out["honest_explanation"] = (
            "candidate_supply_status.json missing — run Luna_Path_To_Tier500_Run.ps1 "
            "(PreviewOnly is fine) to populate the supply truth."
        )
    return out


def _build_auto_promote_state() -> dict[str, Any]:
    """Honest distinction between *council-gated auto-promote* (Luna's
    own progression mechanism — currently ENABLED) and *broad live
    apply* (an inviolate-floor switch — correctly DISABLED).

    The dashboard previously rendered both as "auto/live apply OFF /
    operator-driven" which made the council-gated auto-promote look
    disabled when it has been firing every 15 min via
    LunaTierAutoUpgradeUser. Per 2026-05-09 honesty fix.
    """
    cfg = _safe_read_json(MEMORY_DIR / "luna_higher_tier_config.json") or {}
    cfg5 = _safe_read_json(MEMORY_DIR / "luna_tier5l_config.json") or {}

    promotion_rules = cfg.get("promotion_rules") if isinstance(cfg, dict) else {}
    if not isinstance(promotion_rules, dict):
        promotion_rules = {}

    return {
        # Council-gated auto-promote: this IS Luna's progression mechanism.
        # Enabled = council unanimous can promote within inviolate floor.
        "council_gated_auto_promote_enabled": bool(promotion_rules.get("auto_promote", False)),
        "council_vote_threshold": str(promotion_rules.get("auto_promote_authorized_by", "")) or None,
        "council_scope": str(promotion_rules.get("auto_promote_only_within", "")) or None,
        # Broad live apply: must stay false. Inviolate floor.
        "broad_live_apply_enabled":   bool(cfg5.get("allow_live_apply", False)),
        "tier3_live_apply_enabled":   bool(cfg5.get("tier3_live_apply_enabled", False)),
        # Combined headline label for the dashboard so it does not have
        # to re-derive the truth from the raw flags.
        "headline":  (
            "council-gated auto-promote ON · broad live-apply OFF"
            if promotion_rules.get("auto_promote") and not cfg5.get("allow_live_apply")
            else (
                "auto-promote OFF · broad live-apply OFF"
                if not promotion_rules.get("auto_promote")
                else "BROAD LIVE APPLY ON — INVIOLATE FLOOR VIOLATION"
            )
        ),
        # Honest sub-headline lines.
        "lines": [
            "council-gated auto-promote: " + ("ENABLED" if promotion_rules.get("auto_promote") else "DISABLED"),
            "council vote threshold: " + str(promotion_rules.get("auto_promote_authorized_by", "—")),
            "broad live-apply: " + ("ON (FLOOR VIOLATION)" if cfg5.get("allow_live_apply") else "OFF (correctly disabled per inviolate floor)"),
            "tier3 live apply: " + ("ON (FLOOR VIOLATION)" if cfg5.get("tier3_live_apply_enabled") else "OFF (correctly disabled per inviolate floor)"),
        ],
    }


def _build_fast_store_archive_snapshot() -> dict[str, Any]:
    """Read-only summary of memory/luna_fast_store/index.json.

    Returns the count of tier_*_artifact entries plus the highest tier
    number seen so the dashboard can show "Archive promotions: N · highest
    exercised: tier M" without conflating it with current_effective_tier.
    Per the standing honesty rule: archive operations are real persistent
    state but NOT proof of higher-tier capability. The dashboard MUST
    label this separately from the operational tier.
    """
    p = MEMORY_DIR / "luna_fast_store" / "index.json"
    out: dict[str, Any] = {
        "ok": False,
        "total_entries": 0,
        "tier_artifact_count": 0,
        "highest_tier_seen": None,
        "lowest_tier_seen": None,
        "index_path": "memory/luna_fast_store/index.json",
    }
    data = _safe_read_json(p)
    if not isinstance(data, dict):
        return out
    entries = data.get("entries") or {}
    if not isinstance(entries, dict):
        return out
    out["ok"] = True
    out["total_entries"] = len(entries)
    pat = re.compile(r"^memory/luna_fast_store/keys/tier_(\d+)_artifact$")
    tiers: list[int] = []
    for k in entries.keys():
        try:
            normalized = str(k).replace("\\", "/").lower()
        except Exception:
            continue
        m = pat.match(normalized)
        if m:
            try:
                tiers.append(int(m.group(1)))
            except ValueError:
                continue
    out["tier_artifact_count"] = len(tiers)
    if tiers:
        out["highest_tier_seen"] = max(tiers)
        out["lowest_tier_seen"] = min(tiers)
    return out


def _build_tier_truth_payload_ensure_cache():
    """Idempotent: ensures the cache attribute exists for tier-truth's
    slow filesystem walks. Called at top of build_tier_truth_payload.

    Performance fix 2026-05-09 (post-honesty): the per-call walks for
    tier5l sandbox count + tier8 auto-promote .py count took 4-7 seconds
    each because the dashboard polls /api/tier-truth at 1Hz. Cache TTL
    30s — first call after expiry takes the full hit, next 29 take ~10ms.
    """
    if not hasattr(build_tier_truth_payload, "_counter_cache"):
        build_tier_truth_payload._counter_cache = {
            "ts": None,
            "tier5l_sandbox_runs": 0,
            "tier8_promotions": 0,
            "tier8_recent_run_iso": None,
        }


def build_tier_truth_payload() -> dict[str, Any]:
    _build_tier_truth_payload_ensure_cache()
    """Single synthesized 'true current tier' surface for the dashboard UI.

    Combines luna_higher_tier_config.json (current_effective_tier + per-tier
    enabled flags) with tier6/7/8 scoreboards, the latest tier_progression
    cycle, the legacy self-upgrade gate, and the OpenCode detect report.

    Per ladder rung we emit a 'state' field the UI uses to color the rung:
      - completed   tier is enabled and a higher tier is enabled
      - current     tier is the current_effective_tier
      - next        next rung the gate is being evaluated against
      - eligible    the next-rung scoreboard says eligible == true
      - blocked     next rung exists but blockers list is non-empty
      - future      not yet enabled and not the next gate
      - apex        tier == "X" (always reserved)
    """
    cfg_path = MEMORY_DIR / "luna_higher_tier_config.json"
    cfg = _safe_read_json(cfg_path) or {}

    # Compute the current rung. Prefer the higher-tier config; fall back to
    # the legacy gate's current_allowed_tier (1/2) only as a last resort.
    current_raw = cfg.get("current_effective_tier")
    if not current_raw:
        gate_path = MEMORY_DIR / "luna_self_upgrade_evidence_gate.json"
        gate = _safe_read_json(gate_path) or {}
        current_raw = gate.get("current_allowed_tier") or "5L"
    current_key = _normalize_tier_key(current_raw)

    flags = {
        "5L": True,  # 5L is always the floor.
        "6":  bool(cfg.get("tier6_enabled")),
        "7":  bool(cfg.get("tier7_enabled")),
        "8":  bool(cfg.get("tier8_enabled")),
        "9":  bool(cfg.get("tier9_enabled")),
        "10": bool(cfg.get("tier10_enabled")),
        "X":  bool(cfg.get("tier_x_apex_enabled")),
    }

    # Read scoreboards for context (ok if missing).
    sb6 = _safe_read_json(MEMORY_DIR / "tier6" / "luna_tier6_scoreboard.json") or {}
    sb7 = _safe_read_json(MEMORY_DIR / "tier7" / "luna_tier7_scoreboard.json") or {}
    sb8 = _safe_read_json(MEMORY_DIR / "tier8" / "luna_tier8_readiness_report.json") or {}

    blockers_t8 = list(sb8.get("blockers") or []) if isinstance(sb8, dict) else []
    next_actions_t8 = list(sb8.get("next_required_actions") or []) if isinstance(sb8, dict) else []

    # Identify the "next gate" (first disabled rung after the current one).
    # 2026-05-09 fix per playbook §22: when current_key is a numeric tier
    # > 10 (e.g. "12"), it's not in the visible ladder. Treat it as
    # "past tier 10" for cur_idx purposes so the next-gate scan looks
    # at X, not 5L→6→7. Without this fix, every poll re-pointed
    # next_gate_key at the lowest disabled rung, which produced the
    # confusing "Tier 6 in 00:12" UP NEXT for a system already at 12.
    if current_key in _TIER_LADDER:
        cur_idx = _TIER_LADDER.index(current_key)
    else:
        try:
            n = int(current_key)
            cur_idx = _TIER_LADDER.index("10") if n > 10 else 0
        except (ValueError, TypeError):
            cur_idx = 0
    next_gate_key: str | None = None
    for k in _TIER_LADDER[cur_idx + 1:]:
        if not flags.get(k, False):
            next_gate_key = k
            break

    # Build per-rung ladder states. 2026-05-09 fix per playbook §22:
    # when current_key is a numeric tier > 10 (extended framework), mark
    # ALL visible rungs (5L..10) as "completed" since Luna has objectively
    # crossed them. Without this, the ladder showed Tier 5L as "current"
    # while Luna was actually at Tier 12, which is what made the supermax
    # panel hero say "TIER 5L Sandbox / Lab Layer".
    is_extended_tier = False
    try:
        is_extended_tier = int(current_key) > 10
    except (ValueError, TypeError):
        pass
    ladder = []
    for k in _TIER_LADDER:
        idx = _TIER_LADDER.index(k)
        title = _TIER_TITLES[k]["label"]
        state: str
        if k == "X":
            state = "apex"
        elif is_extended_tier:
            # Luna is past Tier 10 - every visible rung is done.
            state = "completed"
        elif k == current_key:
            state = "current"
        elif idx < cur_idx and flags.get(k, False):
            state = "completed"
        elif k == next_gate_key:
            # Eligible vs blocked depends on which tier we're gating into.
            if k == "8":
                eligible = bool(sb8.get("tier8_eligible")) if isinstance(sb8, dict) else False
                state = "blocked" if (blockers_t8 and not eligible) else ("eligible" if eligible else "next")
            else:
                state = "next"
        else:
            state = "future"
        ladder.append({
            "key":   k,
            "label": title,
            "state": state,
        })

    # Headline + subline. 2026-05-09 fix per playbook §22: when
    # current_key is a numeric tier > 10 (extended framework), derive
    # a level-themed subline from the framework's per-level capability
    # summary instead of falling back to "Sandbox / Lab Layer". The
    # fallback was making Tier 12 / 47 / 188 all look like Tier 5L.
    # 2026-05-10: headline now shows "LEVEL X · TIER Y ACTIVE" where
    # Y is the tier-within-level (1-50), not the global tier number.
    headline = "TIER " + current_key + " ACTIVE"
    title_block = _TIER_TITLES.get(current_key)
    if title_block is None:
        # Extended-framework tier (>10). Derive level-based theme.
        try:
            n_tier = int(current_key)
        except (ValueError, TypeError):
            n_tier = 0
        if n_tier > 10:
            level = max(1, (n_tier - 1) // 50 + 1)
            tier_in_level = ((n_tier - 1) % 50) + 1
            headline = "LEVEL " + str(level) + " · TIER " + str(tier_in_level) + " ACTIVE"
            level_themes = {
                1:  "Foundation (codegen pipeline)",
                2:  "Refactor & Expansion",
                3:  "Autonomous Capability Authoring",
                4:  "Self-Evolving",
                5:  "Compressed Archive Era",
                6:  "Cross-Tier Learning",
                7:  "Self-Healing",
                8:  "Autonomous Research",
                9:  "Mission-Driven",
                10: "Singularity Asymptote",
            }
            subline = level_themes.get(level, f"Level {level} · extended framework")
        else:
            subline = _TIER_TITLES["5L"]["label"]
    else:
        subline = title_block["label"]

    # Council card data (Tier 7).
    by_role = sb7.get("by_role") if isinstance(sb7, dict) else None
    council = {
        "total_reviews":       int(sb7.get("total_reviews", 0))         if isinstance(sb7, dict) else 0,
        "approved":            int(sb7.get("approved_packets", 0))      if isinstance(sb7, dict) else 0,
        "hold_for_review":     int(sb7.get("hold_for_review_packets", 0)) if isinstance(sb7, dict) else 0,
        "do_not_promote":      int(sb7.get("do_not_promote_packets", 0)) if isinstance(sb7, dict) else 0,
        "rollback_failures":   int(sb7.get("rollback_failures", 0))     if isinstance(sb7, dict) else 0,
        "by_role":             by_role if isinstance(by_role, dict) else None,
    }

    # Tier 6 sandbox metrics.
    sandbox = {
        "candidates_total":    int(sb6.get("total_tier6_candidates", 0)) if isinstance(sb6, dict) else 0,
        "candidates_passed":   int(sb6.get("passed", 0))                  if isinstance(sb6, dict) else 0,
        "candidates_failed":   int(sb6.get("failed", 0))                  if isinstance(sb6, dict) else 0,
        "promotion_packets":   int(sb6.get("promotion_packets_created", 0)) if isinstance(sb6, dict) else 0,
        "rollback_failures":   int(sb6.get("rollback_failures", 0))      if isinstance(sb6, dict) else 0,
    }
    tier5l_dir = MEMORY_DIR / "tier5l"
    tier8_dir = MEMORY_DIR / "tier8"
    sandboxes_dir = MEMORY_DIR / "tier6" / "sandboxes"

    # Performance fix 2026-05-09 (post-honesty): the tier5l + tier8 counters
    # below walk the filesystem on EVERY /api/tier-truth call (which the
    # dashboard polls every 1 sec). With 5147+ sandbox dirs and 115+ tier8
    # auto-promote run dirs (with recursive glob for .py), this was 4-7
    # seconds per call. Now cached for 30 seconds with mtime-based
    # invalidation. Subsequent reads inside the TTL skip the filesystem
    # walks entirely — the actual work happens at most every 30 sec.
    now_perf = time.time()
    cache = build_tier_truth_payload._counter_cache  # type: ignore[attr-defined]
    cache_ttl = 30.0  # seconds
    cache_ok = (
        cache.get("ts") is not None
        and (now_perf - cache["ts"]) < cache_ttl
    )

    if cache_ok:
        tier5l_sandbox_runs = cache["tier5l_sandbox_runs"]
        tier8_promotions = cache["tier8_promotions"]
        tier8_recent_run_iso = cache["tier8_recent_run_iso"]
    else:
        # Recompute (the slow paths)
        try:
            if sandboxes_dir.exists():
                tier5l_sandbox_runs = sum(1 for d in sandboxes_dir.iterdir() if d.is_dir())
            else:
                tier5l_sandbox_runs = 0
        except OSError:
            tier5l_sandbox_runs = 0

        auto_promote_dir = MEMORY_DIR / "tier8" / "auto_promote_backups"
        tier8_promotions = 0
        tier8_recent_run_iso = None
        try:
            if auto_promote_dir.exists():
                run_dirs = [d for d in auto_promote_dir.iterdir() if d.is_dir()]
                tier8_promotions = sum(1 for d in run_dirs for _ in d.rglob("*.py"))
                if run_dirs:
                    newest = max(run_dirs, key=lambda d: d.stat().st_mtime)
                    tier8_recent_run_iso = datetime.utcfromtimestamp(
                        newest.stat().st_mtime
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
        except OSError:
            pass
        # Legacy fallback (also cached)
        if tier8_promotions == 0:
            try:
                if tier8_dir.exists():
                    tier8_promotions = sum(
                        1 for _ in tier8_dir.glob("*promotion*.json") if _.is_file()
                    )
            except OSError:
                pass

        cache["ts"] = now_perf
        cache["tier5l_sandbox_runs"] = tier5l_sandbox_runs
        cache["tier8_promotions"] = tier8_promotions
        cache["tier8_recent_run_iso"] = tier8_recent_run_iso

    # Tier 7 review staleness — round 24 per Serge. The auto-promote
    # task fires every 15 min and exits 0 but the scoreboard hasn't
    # been written in 8.85h. Surface the staleness in the dashboard so
    # operators can SEE the upstream bug instead of trusting a silent
    # static number. Also let the front-end render "(stale Xh)".
    tier7_scoreboard_path = MEMORY_DIR / "tier7" / "luna_tier7_scoreboard.json"
    tier7_reviews_age_seconds = None
    try:
        if tier7_scoreboard_path.exists():
            tier7_reviews_age_seconds = max(0, int(time.time() - tier7_scoreboard_path.stat().st_mtime))
    except OSError:
        tier7_reviews_age_seconds = None

    # Tier 9 / 10 honest labels. Standing approval moved ordinary safe
    # tier advancement from Serge-per-tier approval to the council's
    # evidence gate. Keep safety/rollback/forbidden-file exceptions
    # Serge-only, but do not label normal Tier 9+ progress as waiting on
    # Serge after the council has already approved 8->9 and 9->10.
    tier9_label  = "council-authorized · completed" if flags.get("9") else "council evidence gate pending"
    tier10_label = "current · council-controlled" if flags.get("10") else "council evidence gate pending"

    progress_counters = {
        "tier5l_sandbox_runs":        tier5l_sandbox_runs,
        "tier6_candidates":           sandbox["candidates_total"],
        "tier7_reviews":              council["total_reviews"],
        "tier7_reviews_age_seconds":  tier7_reviews_age_seconds,   # for "(stale Xh)" rendering
        "tier8_promotions":           tier8_promotions,
        "tier9_gate_status":          tier9_label,
        "tier10_apex_roadmap":        tier10_label,
    }

    # Latest progression cycle (parse a few fields from the markdown).
    tp_dir = MEMORY_DIR / "tier_progression"
    latest_md_path = tp_dir / "luna_tier_progression_latest.md"
    latest_md = ""
    try:
        if latest_md_path.exists():
            latest_md = latest_md_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        latest_md = ""

    def _grep(pattern: str) -> str:
        m = re.search(pattern, latest_md)
        return m.group(1).strip() if m else ""

    latest = {
        "decision":  _grep(r"decision=([\w]+)") or _grep(r"Decision:\s*(\S+)"),
        "passed":    _grep(r"Actions passed:\s*(\d+)") or "0",
        "failed":    _grep(r"Actions failed:\s*(\d+)") or "0",
        "cycle_id":  _grep(r"Cycle ID:\s*(\S+)"),
        "loop_id":   _grep(r"Loop ID:\s*(\S+)"),
        "highest_eligible": _grep(r"Highest eligible tier:\s*(\S+)"),
        "report_path": "memory/tier_progression/luna_tier_progression_latest.md",
    }
    try:
        latest["passed"] = int(latest["passed"])
        latest["failed"] = int(latest["failed"])
    except Exception:
        pass

    # Scheduled-task status (best-effort). Try the actually-installed
    # user-scope task name FIRST; fall back to the legacy name only if
    # the user-scope task is not present. Without this, the dashboard
    # queries `LunaTierProgressionEngine` (no User suffix), gets back
    # `queryable=false`, and renders "Progression disabled" even though
    # the real task fires every 15 minutes.
    task: dict[str, Any] = {}
    if "_scheduled_task_status" in globals():
        for _name in ("LunaTierProgressionEngineUser",
                      "LunaTierProgressionEngine"):
            _t = _scheduled_task_status(_name)
            if _t and _t.get("queryable"):
                task = _t
                break
        else:
            task = _t or {}

    # Live feed freshness (re-used to mark "fresh"/"stale" on the UI).
    live_feed_path = LOGS_DIR / "luna_live_feed.jsonl"
    live_feed_age_seconds = None
    last_event_iso = None
    try:
        if live_feed_path.exists():
            mtime = live_feed_path.stat().st_mtime
            live_feed_age_seconds = max(0, int(time.time() - mtime))
            tail = _safe_tail_jsonl(live_feed_path, limit=1) if "_safe_tail_jsonl" in globals() else []
            if tail:
                last_event_iso = tail[-1].get("iso_utc") or tail[-1].get("ts")
    except OSError:
        pass

    # Worker ecosystem (small status pills).
    worker_hb = _safe_read_json(LOGS_DIR / "luna_worker_heartbeat.json") or {}
    aider_st  = _safe_read_json(LOGS_DIR / "aider_bridge_status.json") or {}
    guardian  = _safe_read_json(MEMORY_DIR / "luna_guardian_status.json") or {}
    ao_hb     = _safe_read_json(MEMORY_DIR / "always_on" / "luna_always_on_heartbeat.json") or {}

    ecosystem = {
        "luna":              "active" if worker_hb.get("alive") else "offline",
        "worker_state":      str(worker_hb.get("state", "unknown")),
        "guardian":          str(guardian.get("status", "unknown")),
        "verifier":          _ecosystem_verifier_state(),  # canonical (luna_verifier_status)
        "aider":             "online" if aider_st.get("running") else ("idle" if str(aider_st.get("state","")).lower() == "idle" else "offline"),
        "always_on_state":   str(ao_hb.get("state", "unknown")),
        "always_on_verdict": str(ao_hb.get("verdict", "")),
    }

    # OpenCode (re-use the dedicated builder).
    opencode = build_opencode_status_payload()

    # Blocker card (active blocker, prefer Tier 8 readiness whenever Tier 8
    # is the current tier or the next gate, since the readiness blockers
    # are what prevent further live-apply work even after the gate flipped).
    blocker = None
    if blockers_t8 and (next_gate_key == "8" or current_key == "8"):
        # Pick a clear title from the first blocker line.
        first_line = str(blockers_t8[0]) if blockers_t8 else ""
        if "restore drill" in first_line.lower():
            title = "Tier 8 restore drill required"
        elif "rollback" in first_line.lower():
            title = "Tier 8 rollback failure recorded"
        else:
            title = "Tier 8 readiness blocker"
        blocker = {
            "tier":     "8",
            "title":    title,
            "lines":    blockers_t8[:5],
            "actions":  next_actions_t8[:5],
            "report_path": "memory/tier8/luna_tier8_readiness_report.md",
        }

    # Active-state text for the hero card. Replaces misleading IDLE/sleeping.
    if str(ao_hb.get("state", "")).lower() == "cycle_starting":
        active_text = "Running progression cycle now"
    elif blocker:
        active_text = "Blocked at Tier " + blocker["tier"] + " gate · " + blocker["title"]
    elif task and isinstance(task, dict) and task.get("next_run_time"):
        active_text = "Progression active · next run " + str(task.get("next_run_time"))
    elif current_key in {"6", "7", "8"} and flags.get(current_key):
        active_text = "Tier " + current_key + " active · waiting for next bounded cycle"
    elif str(worker_hb.get("state", "")).lower() == "idle":
        active_text = "Worker idle · always-on supervisor between cycles"
    else:
        active_text = "Waiting for next work order"

    sources_read: list[str] = []
    if cfg_path.exists(): sources_read.append("memory/luna_higher_tier_config.json")
    if (MEMORY_DIR / "tier6" / "luna_tier6_scoreboard.json").exists():
        sources_read.append("memory/tier6/luna_tier6_scoreboard.json")
    if (MEMORY_DIR / "tier7" / "luna_tier7_scoreboard.json").exists():
        sources_read.append("memory/tier7/luna_tier7_scoreboard.json")
    if (MEMORY_DIR / "tier8" / "luna_tier8_readiness_report.json").exists():
        sources_read.append("memory/tier8/luna_tier8_readiness_report.json")
    if latest_md_path.exists():
        sources_read.append("memory/tier_progression/luna_tier_progression_latest.md")

    # ------------------------------------------------------------------
    # Live-truth sub-blocks for the dashboard's Evolution panel.
    # next_gate carries an explicit progress label so the JS no longer
    # has to compose it from sandboxes / councils — and so legacy
    # supermax counters can never leak Tier 2 wording into the Tier 9
    # next-gate card.
    # ------------------------------------------------------------------
    next_gate_label = ""
    next_gate_progress_current: int | None = None
    next_gate_progress_required: int | None = None
    next_gate_progress_text = ""
    if next_gate_key == "8":
        cw = council.get("approved") or 0
        next_gate_progress_current  = int(cw)
        next_gate_progress_required = 10
        next_gate_progress_text = f"{min(int(cw), 10)} / 10 council passes"
        next_gate_label = "Tier 8 (Limited Live Helper Promotion)"
    elif next_gate_key == "7":
        cp = sandbox.get("candidates_passed") or 0
        next_gate_progress_current  = int(cp)
        next_gate_progress_required = 10
        next_gate_progress_text = f"{min(int(cp), 10)} / 10 Tier 6 candidates"
        next_gate_label = "Tier 7 (Multi-Agent Review Council)"
    elif next_gate_key == "9":
        # Tier 9+ advancement is council-authorized by standing Serge
        # approval and must use the evidence gate; do NOT show a
        # Tier 2-style numeric eligibility label here.
        next_gate_progress_current  = 0
        next_gate_progress_required = 0
        next_gate_progress_text = "council evidence gate pending"
        next_gate_label = "Tier 9 (Assisted Module Promotion)"
    elif next_gate_key == "10":
        next_gate_progress_text = "roadmap locked"
        next_gate_label = "Tier 10 (Apex Roadmap)"
    elif next_gate_key == "X":
        next_gate_progress_text = "apex · permanent reserved"
        next_gate_label = "Tier X (Apex)"
    elif next_gate_key == "6":
        next_gate_progress_text = "council-gated · awaiting runtime verification"
        next_gate_label = "Tier 6 (Autonomous Sandbox Improvement Engine)"
    next_gate_block = {
        "tier":             next_gate_key,
        "label":            next_gate_label,
        "progress_current": next_gate_progress_current,
        "progress_required": next_gate_progress_required,
        "progress_text":    next_gate_progress_text,
        "is_design_only":   next_gate_key in {"9", "10", "X"},
    }

    # ------------------------------------------------------------------
    # OpenCode worker-log freshness. The OpenCode pill must show "active"
    # (not "offline") whenever a real worker run produced
    # INGESTED_FROM_REAL_OPENCODE_OUTPUT recently — even if the bridge
    # config has opencode_enabled=false (which is the safety default).
    # ------------------------------------------------------------------
    oc_worker_log = MEMORY_DIR / "opencode" / "luna_opencode_worker_log.jsonl"
    oc_recent_real = False
    oc_log_age_seconds: int | None = None
    oc_last_verdict: str | None = None
    try:
        if oc_worker_log.exists():
            mt = oc_worker_log.stat().st_mtime
            oc_log_age_seconds = max(0, int(time.time() - mt))
            tail = _safe_tail_jsonl(oc_worker_log, limit=5) if "_safe_tail_jsonl" in globals() else []
            for line in reversed(tail):
                v = (line or {}).get("verdict")
                if v:
                    oc_last_verdict = v
                    break
            for line in reversed(tail):
                if (line or {}).get("verdict") == "INGESTED_FROM_REAL_OPENCODE_OUTPUT":
                    oc_recent_real = (oc_log_age_seconds is not None
                                      and oc_log_age_seconds <= 3600)
                    break
    except OSError:
        pass

    opencode_status_label = "offline"
    if oc_recent_real:
        opencode_status_label = "active"
    elif opencode.get("cli_found") and opencode.get("opencode_run_ready"):
        opencode_status_label = "ready"
    elif opencode.get("cli_found"):
        opencode_status_label = "degraded"
    elif opencode.get("desktop_found"):
        opencode_status_label = "desktop_only"

    # ------------------------------------------------------------------
    # Worker ecosystem live-truth block. Each pill carries an explicit
    # status (live | active | ready | idle | warn | offline) AND the
    # human-readable detail line so the dashboard does not have to
    # compose state from raw fields. This kills the "Progression
    # disabled" / "OpenCode offline" mislabelling once and for all.
    # ------------------------------------------------------------------
    progression_state = "offline"
    progression_detail = "task not detected"
    if task and task.get("queryable"):
        if str(task.get("state", "")).lower() == "enabled":
            progression_state = "active"
            progression_detail = (
                f"{task.get('task_name', 'progression')} enabled · "
                f"next run {task.get('next_run_time', '?')}"
            )
        elif str(task.get("state", "")).lower() == "disabled":
            progression_state = "offline"
            progression_detail = "scheduled task disabled"
        else:
            progression_state = "idle"
            progression_detail = f"task state: {task.get('state', 'unknown')}"

    # Latest progression report freshness (independent confirmation).
    progression_report_age = None
    if latest_md_path.exists():
        try:
            progression_report_age = max(0, int(time.time() - latest_md_path.stat().st_mtime))
        except OSError:
            progression_report_age = None
    if progression_report_age is not None and progression_report_age <= 1800:
        # Report fresh in last 30 min: definitely active.
        progression_state = "active"
        if not progression_detail:
            progression_detail = f"latest cycle {progression_report_age}s ago"

    guardian_state = "warn"
    guardian_detail = str(guardian.get("status", "unknown"))
    if guardian_detail.lower().startswith("services_healthy"):
        guardian_state = "live"
    elif guardian_detail.lower() in ("ok", "healthy", "ready"):
        guardian_state = "live"

    aider_state_str = str(ecosystem.get("aider", "offline")).lower()
    aider_state = "live" if aider_state_str == "online" else (
        "idle" if aider_state_str == "idle" else "offline"
    )

    worker_ecosystem = {
        "luna":         {"state": "live" if ecosystem.get("luna") == "active" else "offline",
                         "detail": str(ecosystem.get("worker_state", "unknown"))},
        "progression":  {"state": progression_state, "detail": progression_detail},
        "guardian":     {"state": guardian_state,    "detail": guardian_detail},
        "verifier":     _ecosystem_verifier_dict(),  # canonical (luna_verifier_status)
        "aider":        {"state": aider_state,       "detail": aider_state_str},
        "opencode":     {"state": opencode_status_label,
                         "detail": (f"last verdict {oc_last_verdict} {oc_log_age_seconds}s ago"
                                    if oc_last_verdict and oc_log_age_seconds is not None
                                    else "no recent worker log")},
        "dashboard":    {"state": "live", "detail": "/api/health 200"},
    }

    # ------------------------------------------------------------------
    # Source mismatch warnings. The legacy self-upgrade gate still
    # reports tier_2_eligible when current_effective_tier is 6/7/8 —
    # that's the source of the "Tier 2 ELIGIBLE" leak. We surface the
    # mismatch as a small warning rather than rendering both labels.
    # ------------------------------------------------------------------
    mismatch: list[str] = []
    try:
        gate = _safe_read_json(MEMORY_DIR / "luna_self_upgrade_evidence_gate.json") or {}
        if gate.get("tier_2_eligible") and current_key not in {"1", "2"}:
            mismatch.append(
                f"legacy gate reports tier_2_eligible while current effective tier is {current_key}; "
                "rendering legacy as historical"
            )
    except Exception:
        pass
    if next_gate_key in {"9", "10", "X"} and progression_state == "active":
        # Not a mismatch - just a clarifying note for the UI.
        pass

    nonstop_orchestrator = _build_nonstop_orchestrator_status()
    tier_runtime_capability = _build_tier_runtime_capability_status()
    runtime_headline = tier_runtime_capability.get("tier_label_for_headline")
    if runtime_headline:
        headline = str(runtime_headline)
    operational_capability_tier = tier_runtime_capability.get("operational_capability_tier")
    proven_tiers = tier_runtime_capability.get("proven_tiers") or []
    tier_runtime_status = "unknown"
    try:
        current_tier_int = int(current_key)
    except (TypeError, ValueError):
        current_tier_int = 0
    if current_tier_int in proven_tiers:
        tier_runtime_status = "proven"
    elif operational_capability_tier is not None and current_tier_int > int(operational_capability_tier):
        tier_runtime_status = "runtime_proof_pending"
        level = max(1, (current_tier_int - 1) // 50 + 1)
        tier_in_level = ((current_tier_int - 1) % 50) + 1
        proven_level = max(1, (int(operational_capability_tier) - 1) // 50 + 1)
        proven_tier_in_level = ((int(operational_capability_tier) - 1) % 50) + 1
        subline = "Counter Level %d Tier %d" % (level, tier_in_level)

    # 2026-05-13 canonical-truth injection per Serge audit:
    # The single source of truth for "what tier should the UI render?" is
    # luna_modules.drift_repair_authority.reconcile_tier_state. Surface its
    # canonical_* fields + truth_verdict at the TOP of the /api/tier-truth
    # payload so consumers never need to re-derive truth from raw fields.
    # The raw current_effective_tier / current_tier_label / headline / subline
    # fields remain for backward compat but are now flagged deprecated.
    try:
        from luna_modules import drift_repair_authority as _dra
        _reconciled = _dra.load_and_reconcile() or {}
    except Exception:  # noqa: BLE001
        _reconciled = {}
    # 2026-05-13 canonical announcement formatter — single source for the
    # visible announcement strings (headline/subline/blocker/next action).
    # Replaces ad-hoc "TIER X ACTIVE" / "Counter Level Y Tier Z" templates.
    try:
        from luna_modules import canonical_announcement_formatter as _caf
        from luna_modules import terminal_manager_tier_review as _tmr
        _review_state = _tmr.get_state() if hasattr(_tmr, "get_state") else None
        _announcements = _caf.format_all(_reconciled, _review_state) or {}
    except Exception:  # noqa: BLE001
        _announcements = {}
    payload: dict[str, Any] = {
        "ok": True,
        "generated_at": _now_iso(),
        # ---- CANONICAL truth (UI MUST consume these) ---------------------
        "truth_verdict":             _reconciled.get("truth_verdict"),
        "canonical_ui_status":       _reconciled.get("canonical_ui_status"),
        "canonical_operating_tier":  _reconciled.get("canonical_operating_tier"),
        "canonical_displayed_tier":  _reconciled.get("canonical_displayed_tier"),
        "canonical_terminal_used_tier": _reconciled.get("canonical_terminal_used_tier"),
        "canonical_next_gate":       _reconciled.get("canonical_next_gate"),
        "canonical_tier_500_status": _reconciled.get("canonical_tier_500_status"),
        "may_claim_active":          _reconciled.get("may_claim_active"),
        "blocker_reason":            _reconciled.get("blocker_reason"),
        "next_action":               _reconciled.get("next_action"),
        "repair_decision":           _reconciled.get("repair_decision"),
        "drift":                     _reconciled.get("drift"),
        # ---- CANONICAL announcement strings (UI MUST consume these) -----
        "canonical_headline":            _announcements.get("canonical_headline"),
        "canonical_subline":             _announcements.get("canonical_subline"),
        "canonical_blocker_summary":     _announcements.get("canonical_blocker_summary"),
        "canonical_next_action_text":    _announcements.get("canonical_next_action_text"),
        # ---- DEPRECATED raw fields (left for backward compat only) ------
        # UI consumers: prefer truth_verdict / canonical_ui_status above.
        # These fields can leak the counter high-water during drift; do NOT
        # render them as "current tier" without consulting truth_verdict.
        "deprecated_raw_fields_warning": (
            "UI consumers should prefer truth_verdict / canonical_*. The "
            "fields current_effective_tier and current_tier_label may "
            "reflect the counter high-water, not the operating tier."
        ),
        "current_effective_tier": current_key,
        "current_tier_label":     subline,
        "headline":               headline,
        "subline":                subline,
        "operational_capability_tier": operational_capability_tier,
        "tier_runtime_status":    tier_runtime_status,
        "tier45_runtime_status":  tier_runtime_capability.get("tier45_runtime_status"),
        "tier_runtime_label":     headline,
        "active_text":            active_text,
        "ladder":                 ladder,
        "tier_flags":             flags,
        "next_gate_key":          next_gate_key,
        "next_gate_label":        _TIER_TITLES.get(next_gate_key, {}).get("label") if next_gate_key else None,
        "council":                council,
        "sandbox":                sandbox,
        "progress_counters":      progress_counters,
        "ecosystem":              ecosystem,
        "worker_ecosystem":       worker_ecosystem,
        "next_gate":              next_gate_block,
        "opencode":               {k: v for k, v in opencode.items() if k != "ok"},
        "opencode_status_label":  opencode_status_label,
        "opencode_recent_real_output": oc_recent_real,
        "opencode_last_verdict":  oc_last_verdict,
        "opencode_log_age_seconds": oc_log_age_seconds,
        "latest_progression":     latest,
        "blocker":                blocker,
        "scheduled_task":         task,
        "progression_report_age_seconds": progression_report_age,
        "live_feed": {
            "last_event_iso":  last_event_iso,
            "age_seconds":     live_feed_age_seconds,
            "is_stale":        (live_feed_age_seconds is not None and live_feed_age_seconds > 600),
        },
        "source_mismatch_warnings": mismatch,
        # Continuous Supervisor live status — surfaces whether the
        # in-process loop that fires cycles at sub-minute cadence is
        # alive, what cadence it's running, and how recently it logged
        # activity. Read-only snapshot; never mutates anything.
        "continuous_supervisor":   _build_continuous_supervisor_snapshot(),
        # 2026-05-09 honesty fix: surface the newer auto-upgrade engine's
        # truth and the FastStore archive count separately from
        # current_effective_tier. The legacy progression engine's
        # "highest_eligible_tier" is no longer the dashboard's source of
        # truth for "what's the next promotion path" — auto_upgrade_engine
        # is. archive_promotions is a count of stored work (real, persistent)
        # that is explicitly NOT a higher-tier-capability claim.
        "auto_upgrade_engine":    _build_tier_auto_upgrade_snapshot(),
        "archive_promotions":     _build_fast_store_archive_snapshot(),
        # 2026-05-09 honesty fix per failure mode 9: surface the source-of-
        # truth divergence between memory/luna_council_added_tiers.json
        # (config pointer; empty placeholder) and
        # memory/tier9/luna_council_added_tiers.json (engine reads; populated).
        "council_added_tiers_truth": _build_council_added_tiers_truth(),
        # 2026-05-09 honesty fix per failure mode 12: distinguish the
        # council-gated auto-promote (Luna's progression mechanism, ON)
        # from broad live-apply (inviolate-floor switch, OFF). Earlier
        # the dashboard conflated both into "auto/live apply OFF" which
        # made the council-gated engine look disabled.
        "auto_promote_state":     _build_auto_promote_state(),
        # 2026-05-09 addendum honesty fix: surface the Tier 9+ candidate
        # supply truth (where the 491 council tier_definitions stand) so
        # the dashboard never has to guess "why is eligible 0?".
        "candidate_supply_status": _build_candidate_supply_status(),
        # 2026-05-09 final command: authoritative nonstop conveyor state.
        # Owns the strict execution order; replaces the 3 racing tasks.
        "nonstop_orchestrator":   nonstop_orchestrator,
        "tier_advancement_outcome": nonstop_orchestrator.get("tier_advancement_outcome"),
        "tier_advancement_detail": nonstop_orchestrator.get("tier_advancement_detail"),
        "tier_advancement_new_cet": nonstop_orchestrator.get("tier_advancement_new_cet"),
        # 2026-05-10 final command: lane router decision (read-only).
        # Tells the dashboard which lane is currently active and which
        # lane is next when this one exhausts.
        "lane_router":            _build_lane_router_status(),
        # 2026-05-10 Tier 45 honesty fix per Serge: distinguish the
        # tier counter (current_effective_tier) from operationally
        # proven capability. A counter at 45 with no runtime proof is
        # NOT the same as Tier 45 active; the dashboard must say so.
        "tier_runtime_capability": tier_runtime_capability,
        "sources_read":           sources_read,
        # 2026-05-11 Serge directive: unified Tier Truth Router payload.
        # The dashboard frontend (Mission Control headline, intent router
        # status replies, status panels) should prefer reading this block
        # over the legacy ladder fields above. Legacy ladder remains for
        # backward compat with panels not yet migrated. luna_tier_truth
        # is read-only and falls back to a stub on any internal error so
        # this never breaks the rest of the payload.
        "unified_tier_truth": _build_unified_tier_truth_block(),
    }
    # 2026-05-12 Dashboard Tier Display Unification: lift canonical
    # truth fields to the top level so every front-end panel can read
    # them without drilling into unified_tier_truth. The frontend
    # helper getCanonicalTierDisplay(tt) reads these.
    try:
        _utt = payload.get("unified_tier_truth") or {}
        for _k in (
            "current_operating_tier",
            "current_operationally_proven_tier",
            "current_adopted_tier",
            "highest_proposed_tier",
            "highest_artifact_tier",
            "highest_generated_tier",
            "counter_high_water_mark",
            "lifecycle_state",
            "stale_tier_labels",
            "unproven_tier_claims",
            "operating_tier_display",
        ):
            if _k in _utt and _k not in payload:
                payload[_k] = _utt[_k]
    except Exception:
        pass
    return payload


def _build_unified_tier_truth_block() -> dict[str, Any]:
    """Wrapper around luna_modules.luna_tier_truth.get_tier_truth() that
    NEVER raises -- on import error or computation error we return a
    stub so the dashboard payload stays valid."""
    try:
        from luna_modules.luna_tier_truth import get_tier_truth as _get_tier_truth
        return _get_tier_truth()
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "current_display_tier": "TIER ? (unified router unavailable)",
            "current_operational_tier": None,
        }


def build_upgrade_adoption_payload() -> dict[str, Any]:
    """Return Luna Upgrade Adoption Engine status. Read-only.
    NEVER includes API keys or private memory. Safe for /api/upgrade-adoption."""
    try:
        from luna_modules.luna_upgrade_adoption import get_adoption_status
        s = get_adoption_status()
        if not isinstance(s, dict):
            s = {"ok": False, "error": "get_adoption_status returned non-dict"}
        return s
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "total_upgrades_known": 0,
            "active_count": 0,
            "blocked_count": 0,
        }


def build_cyberguy_console_payload() -> dict[str, Any]:
    """The full 9-section CyberGuy Console payload. Read-only. Already
    secret-redacted at every internal boundary."""
    try:
        from luna_modules.luna_cyberguy_guardian import get_cyberguy_console_payload, safe_redact
        p = get_cyberguy_console_payload()
        return p if isinstance(p, dict) else {"ok": False, "error": "guardian_returned_non_dict"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200],
                "overall_severity": "WATCH", "sections": {}}


def build_cyberguy_threat_db_payload() -> dict[str, Any]:
    """Surface the local threat intel DB summary (CISA KEV + NVD)."""
    try:
        from luna_modules.luna_cyberguy_threat_intel import get_threat_intel_status
        s = get_threat_intel_status()
        return s if isinstance(s, dict) else {"ok": False, "error": "intel_returned_non_dict"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200],
                "total_entries": 0, "uses_external_models": False}


def build_cyberguy_actions_payload() -> dict[str, Any]:
    """Surface action audit + pending actions + quarantine state."""
    try:
        from luna_modules.luna_cyberguy_actions import get_actions_status
        s = get_actions_status()
        return s if isinstance(s, dict) else {"ok": False, "error": "actions_returned_non_dict"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200],
                "auto_execute": False}


def _cyberguy_scan_handle(handler: BaseHTTPRequestHandler, mode: str) -> None:
    """POST /api/cyberguy/scan or /api/cyberguy/scan-preview.
    Spawns Luna_CyberGuy_SecurityScan.ps1 detached; returns task_id."""
    if not _check_loopback(handler):
        return
    try:
        import subprocess as _sp
        script = PROJECT_ROOT / "Luna_CyberGuy_SecurityScan.ps1"
        if not script.exists():
            _send_json(handler, HTTPStatus.NOT_FOUND,
                       {"ok": False, "error": "scan_script_missing"})
            return
        args = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                "-WindowStyle", "Hidden", "-File", str(script)]
        if mode == "scan":
            args += ["-Scan", "-WriteReport", "-NoExternalNetwork"]
        else:
            args += ["-PreviewOnly"]
        kw = _hidden_popen_kwargs() if "_hidden_popen_kwargs" in globals() else {}
        proc = _sp.Popen(args, cwd=str(PROJECT_ROOT),
                         stdin=_sp.DEVNULL, stdout=_sp.DEVNULL, stderr=_sp.DEVNULL,
                         **kw)
        _send_json(handler, HTTPStatus.OK,
                   {"ok": True, "mode": mode, "scan_pid": proc.pid,
                    "note": "scan dispatched; poll /api/cyberguy/status for result"})
    except Exception as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": "%s: %s" % (type(exc).__name__, str(exc)[:200])})


def _cyberguy_acknowledge_handle(handler: BaseHTTPRequestHandler) -> None:
    """POST /api/cyberguy/acknowledge -- operator acks an alert.
    Records the ack in the action audit log."""
    if not _check_loopback(handler):
        return
    try:
        data = _read_post_json(handler) or {}
        alert_id = str((data or {}).get("alert_id") or "")[:120]
        from luna_modules.luna_cyberguy_actions import write_action_audit
        write_action_audit({
            "action_kind": "acknowledge_alert",
            "target": alert_id,
            "status": "EXECUTED",
            "note": "operator acknowledged alert",
        })
        _send_json(handler, HTTPStatus.OK,
                   {"ok": True, "alert_id": alert_id, "acknowledged": True})
    except Exception as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": "%s: %s" % (type(exc).__name__, str(exc)[:200])})


def _cyberguy_request_action_handle(handler: BaseHTTPRequestHandler) -> None:
    """POST /api/cyberguy/request-action -- propose a quarantine/delete/restore.
    Body: {action: 'quarantine'|'delete'|'restore', target: '<path or qid>', reason?: '...'}
    Returns action_id for the two-step confirmation flow."""
    if not _check_loopback(handler):
        return
    try:
        data = _read_post_json(handler) or {}
        action = str((data or {}).get("action") or "").strip().lower()
        target = str((data or {}).get("target") or "")
        reason = str((data or {}).get("reason") or "")[:200]
        if not action or not target:
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": "missing action or target"})
            return
        from luna_modules.luna_cyberguy_actions import (
            request_quarantine, request_delete, request_restore,
        )
        if action == "quarantine":
            r = request_quarantine(target, reason)
        elif action == "delete":
            r = request_delete(target, reason)
        elif action == "restore":
            r = request_restore(target, reason)
        else:
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": "invalid action; want quarantine|delete|restore"})
            return
        _send_json(handler, HTTPStatus.OK, r)
    except Exception as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": "%s: %s" % (type(exc).__name__, str(exc)[:200])})


def _cyberguy_panel_action_handle(handler: BaseHTTPRequestHandler,
                                   *, action: str) -> None:
    """POST /api/cyberguy/action/{restore,archive,delete}

    Body: {item_id: str, operator?: str, reason?: str, confirm?: bool}

    Calls the operator-friendly thin layer in
    luna_cyberguy_panel_actions which wraps the safe two-step gate.
    Delete requires confirm=true (or the gate refuses).
    Every action appends one structured line to
    memory/cyberguy/panel_action_log.jsonl.
    """
    if not _check_loopback(handler):
        return
    try:
        data = _read_post_json(handler) or {}
        item_id  = str((data or {}).get("item_id") or "").strip()
        operator = str((data or {}).get("operator") or "operator")[:64]
        reason   = str((data or {}).get("reason") or "")[:300]
        confirm  = bool((data or {}).get("confirm"))
        if not item_id:
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": "missing item_id"})
            return
        from luna_modules import luna_cyberguy_panel_actions as _cpa
        if action == "restore":
            result = _cpa.panel_restore(item_id, operator=operator, reason=reason)
        elif action == "archive":
            result = _cpa.panel_archive(item_id, operator=operator, reason=reason)
        elif action == "delete":
            result = _cpa.panel_delete(item_id, confirm=confirm,
                                        operator=operator, reason=reason)
        else:
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": f"invalid action {action!r}"})
            return
        _send_json(handler, HTTPStatus.OK, result)
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:200]}"})


def build_cyberguy_panel_status_payload() -> dict[str, Any]:
    """GET /api/cyberguy/panel-status — caught items + archived items
    + recent panel actions. Read-only; authoritative."""
    try:
        from luna_modules import luna_cyberguy_panel_actions as _cpa
        return _cpa.panel_status()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def _cyberguy_confirm_action_handle(handler: BaseHTTPRequestHandler) -> None:
    """POST /api/cyberguy/confirm-action
    Body: {action_id, confirmation_phrase?, execute?}
    If execute=True (and confirmation succeeds), runs the action;
    otherwise just marks it CONFIRMED."""
    if not _check_loopback(handler):
        return
    try:
        data = _read_post_json(handler) or {}
        action_id = str((data or {}).get("action_id") or "")[:120]
        phrase = str((data or {}).get("confirmation_phrase") or "")
        execute = bool((data or {}).get("execute"))
        from luna_modules.luna_cyberguy_actions import (
            confirm_action, execute_confirmed_action,
        )
        result = confirm_action(action_id, phrase)
        if result.get("ok") and execute:
            result = execute_confirmed_action(action_id)
        _send_json(handler, HTTPStatus.OK, result)
    except Exception as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": "%s: %s" % (type(exc).__name__, str(exc)[:200])})


def build_tier_graduation_payload() -> dict[str, Any]:
    """Return Luna Tier Graduation status (doctrine v1).

    Reads only the graduation governor + proof registry; never contains
    secrets or personal memory. Suitable for /api/tier-graduation.
    """
    out: dict[str, Any] = {
        "ok": True,
        "iso_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "current_operating_tier": None,
        "current_effective_tier": None,
        "lifecycle_state": None,
        "approval_type": None,
        "serge_approval_required": None,
        "council_approval_status": None,
        "operational_proof_complete": False,
        "graduation_allowed": False,
        "next_tier_allowed": False,
        "next_tier_blocker_reason": None,
        "next_tier_id_if_allowed": None,
        "proof_checklist": {},
        "state_counts": {},
        "highest_artifact_tier": None,
        "highest_proposed_tier": None,
        "doctrine": "memory/tier_truth/tier_graduation_doctrine.md",
        "adopted_tiers": [],
    }
    try:
        from luna_modules import luna_tier_graduation as _g
        status = _g.write_tier_graduation_report() or {}
        out["current_operating_tier"] = status.get("current_operating_tier")
        out["current_effective_tier"] = status.get("current_effective_tier")
        out["next_tier_allowed"] = status.get("next_tier_proposal_allowed", False)
        out["next_tier_blocker_reason"] = status.get("next_tier_blocker_reason")
        out["next_tier_id_if_allowed"] = status.get("next_tier_id_if_allowed")
        out["state_counts"] = status.get("state_counts", {})
        proof = status.get("proof") or {}
        out["lifecycle_state"] = proof.get("lifecycle_state")
        out["operational_proof_complete"] = proof.get("proof_complete", False)
        out["proof_checklist"] = proof.get("proof_checklist", {})
        cur_rec = _g.get_tier_lifecycle(out["current_operating_tier"]) if out["current_operating_tier"] is not None else {}
        out["approval_type"] = cur_rec.get("approval_type")
        out["serge_approval_required"] = bool(cur_rec.get("serge_required"))
        out["council_approval_status"] = (
            "council_unanimous" if cur_rec.get("approval_type") == "council_policy"
            else cur_rec.get("approval_type")
        )
        out["graduation_allowed"] = bool(cur_rec.get("graduation_allowed"))
        # Adopted tier numbers
        try:
            from pathlib import Path as _P
            reg_path = _P(__file__).resolve().parent.parent / "memory" / "tier_truth" / "tier_proof_registry.json"
            if reg_path.exists():
                with reg_path.open("r", encoding="utf-8") as fh:
                    reg = json.load(fh)
                out["adopted_tiers"] = sorted({
                    int(t.get("tier")) for t in (reg.get("tiers") or [])
                    if isinstance(t, dict) and t.get("tier") is not None
                    and t.get("lifecycle_state") in ("ADOPTED", "OPERATING", "OPERATIONAL_PROVEN", "GRADUATED")
                })
                out["highest_proposed_tier"] = max(
                    [int(t.get("tier")) for t in (reg.get("tiers") or [])
                     if isinstance(t, dict) and t.get("tier") is not None],
                    default=None,
                )
        except Exception:
            pass
        # Best-effort: highest artifact tier from memory/tier{N}/ dirs
        try:
            mem_dir = PROJECT_ROOT / "memory"
            tiers = []
            for child in mem_dir.iterdir():
                if child.is_dir():
                    m = re.match(r"^tier(\d+)$", child.name)
                    if m:
                        tiers.append(int(m.group(1)))
            if tiers:
                out["highest_artifact_tier"] = max(tiers)
        except Exception:
            pass
    except Exception as e:
        out["ok"] = False
        out["error"] = f"governor_unavailable: {type(e).__name__}"

    # 2026-05-13 Terminal Accuracy Pass: cross-check the graduation
    # governor's lifecycle_state against the canonical truth_verdict.
    # If they disagree (governor says PROVEN while canonical says
    # UNDER_AUDIT), force the displayed lifecycle to an explicit
    # UNDER_AUDIT_OVERRIDE so the operator never sees a contradictory
    # PROVEN label. Also fill in `next_tier_blocker_reason` so the
    # panel never shows a blank em-dash.
    try:
        from luna_modules import luna_operator_truth_surface as _ots
        from luna_modules import canonical_truth_authority as _cta
        truth = _cta.get_canonical_truth()
        aligned = _ots._build_tier_graduation_aligned(truth)
        out["truth_verdict"]           = truth.get("truth_verdict")
        out["truth_aligned"]           = aligned["truth_aligned"]
        out["displayed_lifecycle"]     = aligned["displayed_lifecycle"]
        # If contradictory, force the visible field rendering:
        if not aligned["truth_aligned"]:
            out["lifecycle_state"]     = aligned["displayed_lifecycle"]
            out["next_tier_allowed"]   = False
            out["graduation_allowed"]  = False
            out["operational_proof_complete"] = False
        # Fill blank fields when canonical layer can explain them.
        if not out.get("next_tier_blocker_reason"):
            out["next_tier_blocker_reason"] = aligned["next_tier_blocker_reason"]
        if out.get("approval_type") in (None, ""):
            out["approval_type"] = ("repair_task_executor_completion"
                                     if not aligned["truth_aligned"]
                                     else "council_policy")
        if out.get("council_approval_status") in (None, ""):
            out["council_approval_status"] = "n/a_while_under_audit"
    except Exception as _e:  # noqa: BLE001
        out.setdefault("truth_aligned", False)
        out.setdefault("alignment_error", f"{type(_e).__name__}")
    # 2026-05-13 canonical contract: populate the operator fields from
    # canonical so they are NEVER blank when canonical knows the answer.
    _attach_canonical_truth_summary(out)
    try:
        from luna_modules import luna_canonical_truth as _ct
        _truth = _ct.build_canonical_current_truth()
        _rf = _truth.get("rebuild_frontier") or {}
        _blk = _truth.get("current_blocker") or {}
        _nxt = _truth.get("next_action") or {}
        # Always populate fields from canonical when we have them.
        out["current_rebuild_tier"]           = _rf.get("current_rebuild_tier")
        out["highest_honestly_verified_tier"] = _rf.get("highest_honestly_verified_tier")
        out["next_tier_candidate"]            = _rf.get("next_tier_candidate")
        out["canonical_blocker"]              = _blk
        out["canonical_actor"]                = (_blk or {}).get("actor")
        out["canonical_required_artifact"]    = (_blk or {}).get("required_artifact")
        out["canonical_next_action"]          = _nxt
        # Routine progression is council-gated + runtime-verified.
        out["approval_mode"]      = "council-gated + runtime-verified"
        out["serge_approval_required"] = False
    except Exception:  # noqa: BLE001
        pass
    return out


def build_tier_adoption_payload() -> dict[str, Any]:
    """Return Tier Adoption Governor status (Tier 160 Self-Repair Doctrine).

    Reads only governor + live-chat-brain status; never contains API
    keys or private memory. Suitable for /api/tier-adoption.
    """
    out: dict[str, Any] = {
        "ok": True,
        "iso_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "current_live_tier": None,
        "highest_generated_tier": None,
        "highest_adopted_tier": None,
        "highest_displayed_tier": None,
        "highest_terminal_used_tier": None,
        "drift": False,
        "drift_signals": [],
        "live_chat_brain_active": False,
        "canned_fallback_blocked": True,
        "latest_adopted_tier": None,
        "latest_adopted_at": None,
        "next_action": "monitor",
        "doctrine": "memory/tier160/luna_tier160_self_repair_doctrine.md",
    }
    try:
        from luna_modules import luna_tier_adoption_governor as _gov
        try:
            status = _gov.get_tier_adoption_status() or {}
            for k in (
                "current_live_tier", "highest_generated_tier",
                "highest_adopted_tier", "highest_displayed_tier",
                "highest_terminal_used_tier", "drift", "drift_signals",
                "live_chat_brain_active", "canned_fallback_blocked",
                "latest_adopted_tier", "latest_adopted_at", "next_action",
            ):
                if k in status:
                    out[k] = status[k]
        except Exception as e:
            out["ok"] = False
            out["error"] = f"governor.get_tier_adoption_status: {type(e).__name__}"
    except Exception as e:
        out["ok"] = False
        out["error"] = f"governor import failed: {type(e).__name__}"
    # 2026-05-13 canonical contract: surface rebuild frontier FIRST and
    # explicitly label the claimed historical fields as secondary.
    _attach_canonical_truth_summary(out)
    try:
        from luna_modules import luna_canonical_truth as _ct
        _truth = _ct.build_canonical_current_truth()
        out["rebuild_frontier_primary"] = _truth.get("rebuild_frontier")
        out["claimed_operating_tier_historical"] = _truth.get(
            "claimed_operating_tier_historical"
        )
        out["primary_authority_note"] = (
            "rebuild_frontier is PRIMARY truth. "
            "highest_generated_tier / highest_adopted_tier (160/500/499) "
            "are historical/secondary context only."
        )
    except Exception:  # noqa: BLE001
        pass
    return out


def build_live_chat_brain_status_payload() -> dict[str, Any]:
    """Return Live Chat Brain status (Tier 160 Self-Repair Doctrine).

    Surfaces which subsystems the brain can reach, which categories are
    routed, and whether canned-fallback is blocked. Never contains
    secrets or private memory. Suitable for /api/live-chat-brain/status.
    """
    out: dict[str, Any] = {
        "ok": True,
        "iso_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "active": False,
        "terminal_tier": None,
        "categories": [],
        "subsystems": {},
        "canned_fallback_blocked": True,
        "banned_canned_phrases": [],
    }
    try:
        from luna_modules import luna_live_chat_brain as _brain
        s = _brain.live_chat_brain_status() or {}
        for k in ("active", "terminal_tier", "categories", "subsystems",
                  "canned_fallback_blocked", "banned_canned"):
            if k in s:
                key = "banned_canned_phrases" if k == "banned_canned" else k
                out[key] = s[k]
        out["active"] = bool(s.get("active", True))
    except Exception as e:
        out["ok"] = False
        out["error"] = f"live_chat_brain unavailable: {type(e).__name__}"
    return out


def build_agent_bus_payload() -> dict[str, Any]:
    """Luna Agent Bus surface for the dashboard's Agent Communication
    panel (2026-05-12). Returns ONLY verified visible messages plus
    counts. Rejected / NEEDS_REVIEW messages stay in the bus log for
    auditing and are NEVER surfaced through this endpoint.

    Reads only ``luna_agent_bus``; never includes secrets (the bus
    redacts secret-shape content at publish time, and any message
    that contained one is auto-marked NEEDS_REVIEW which we filter
    out here).
    """
    out: dict[str, Any] = {
        "ok": True,
        "iso_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "policy_path": "memory/agent_bus/agent_bus_policy.md",
        "counts": {"verified": 0, "hypotheses": 0, "blockers": 0, "rejected": 0},
        "by_agent": {},
        "latest_visible": [],
        "latest_blockers": [],
        "latest_hypotheses": [],
    }
    try:
        from luna_modules import luna_agent_bus as _bus
        snap = _bus.summarize_for_core_brain(limit=12) or {}
        out["counts"] = snap.get("counts") or out["counts"]
        out["by_agent"] = snap.get("by_agent") or {}
        # 2026-05-13 sentinel-hiding: dev/test publication sentinels
        # (SOVEREIGN_PUB_TEST_SENTINEL_*, SOVEREIGN_PUB_WIRING_TEST_*)
        # were leaking into the visible operator panels. Filter them
        # out of the visible mirror here; they still live in the
        # audit log for forensics.
        # Also mark messages older than 600 s as `historical=True` so
        # the Live Map can render them in a separate "Past" lane and
        # never as "current blocker".
        import time as _time
        from datetime import datetime as _dt, timezone as _tz
        _STALE_AGE_S = 600.0
        _sentinel_prefixes = ("SOVEREIGN_PUB_TEST_SENTINEL_",
                              "SOVEREIGN_PUB_WIRING_TEST_",
                              "RUNTIME_TEST_SENTINEL_",
                              "ECOSYSTEM_AUDIT_PROBE_",
                              "POST_RESTART_PROBE")

        def _is_historical(rec: Any) -> bool:
            try:
                ts = (rec or {}).get("timestamp")
                if not ts:
                    return False
                # Tolerate ISO with or without 'Z'.
                t = _dt.fromisoformat(str(ts).replace("Z", "+00:00"))
                age = (_dt.now(tz=_tz.utc) - t).total_seconds()
                return age > _STALE_AGE_S
            except Exception:  # noqa: BLE001
                return False
        def _hide_sentinel(rec: Any) -> bool:
            try:
                s = str((rec or {}).get("summary") or "")
                return any(p in s for p in _sentinel_prefixes)
            except Exception:  # noqa: BLE001
                return False
        def _filter(records: Any) -> list:
            if not isinstance(records, list):
                return []
            out_recs = []
            for r in records:
                if _hide_sentinel(r):
                    continue
                if _is_historical(r):
                    rec = dict(r)
                    rec["historical"] = True
                    out_recs.append(rec)
                else:
                    out_recs.append(r)
            return out_recs
        out["latest_visible"]    = _filter(snap.get("latest_verified") or [])
        out["latest_blockers"]   = _filter(snap.get("latest_blockers") or [])
        out["latest_hypotheses"] = _filter(snap.get("latest_hypotheses") or [])
        # Active blockers must reflect CURRENT state only (not historical
        # events kept around for context). Operators see "Active blockers"
        # as the live work-list; historical/closed ones move to the
        # "Past" lane via the historical=True flag.
        out["current_blockers"] = [r for r in out["latest_blockers"]
                                    if not r.get("historical")]
        # Per-agent strip: drop any agent whose last visible message is a sentinel.
        ba = dict(out["by_agent"] or {})
        for role in list(ba.keys()):
            last = ba.get(role) or {}
            if any(p in str(last.get("last_summary") or "") for p in _sentinel_prefixes):
                ba.pop(role, None)
        out["by_agent"] = ba
        out["sentinels_filtered"] = True
    except Exception as exc:  # noqa: BLE001
        out["ok"] = False
        out["error_kind"] = type(exc).__name__
        out["error_detail"] = str(exc)[:200]
    # 2026-05-13 canonical contract: cross-checked Active Blockers must
    # be reconciled against canonical current_blocker so the panel never
    # renders stale "BLOCKED_NEEDS_RUNTIME_PROOF" while canonical says
    # otherwise.
    _attach_canonical_truth_summary(out)
    try:
        from luna_modules import luna_canonical_truth as _ct
        _truth = _ct.build_canonical_current_truth()
        _blk   = _truth.get("current_blocker") or {}
        out["canonical_active_blocker"] = {
            "summary":           _blk.get("summary"),
            "tier":              _blk.get("tier"),
            "actor":             _blk.get("actor"),
            "required_artifact": _blk.get("required_artifact"),
            "verdict":           _blk.get("verdict"),
        }
        # Filter any visible blocker that contradicts canonical Tier 1
        # truth — if canonical says Tier 1 is COMPLETE, the legacy
        # "Tier 1 BLOCKED_NEEDS_RUNTIME_PROOF" entry must be flagged.
        _rf = _truth.get("rebuild_frontier") or {}
        _t1_verdict = (_rf.get("tier_1_current_verdict") or "").upper()
        if _t1_verdict == "COMPLETE":
            _cb = []
            for rec in out.get("current_blockers") or []:
                summary = str((rec or {}).get("summary") or "")
                if "TIER 1" in summary.upper() and "BLOCKED" in summary.upper():
                    rec = dict(rec)
                    rec["contradicts_canonical"] = True
                    rec["canonical_says"] = "Tier 1 is COMPLETE on disk; legacy event ignored"
                _cb.append(rec)
            out["current_blockers"] = _cb
    except Exception:  # noqa: BLE001
        pass
    return out


def build_backfill_status_payload() -> dict[str, Any]:
    """Read-only summary of the prior-tier backfill audit + the last
    council coordination round. NEVER triggers a network call.
    Authoritative for the AUDIT — advisory for the recommendations."""
    try:
        from luna_modules import luna_tier_backfill_auditor       as _aud
        from luna_modules import luna_council_backfill_coordinator as _coord
        env = _aud.audit_all_tiers()
        sessions     = _coord.read_recent_sessions(limit=10)
        coordination = _coord.read_recent_coordination(limit=10)
        return {
            "ok":             True,
            "advisory_only":  False,
            "authoritative":  True,
            "audit_summary": {
                "canonical_operating_tier": env.get("canonical_operating_tier"),
                "canonical_truth_verdict":  env.get("canonical_truth_verdict"),
                "counts":                   env.get("counts"),
                "incomplete_count":         env.get("incomplete_count"),
                "first_incomplete":         env.get("first_incomplete"),
                "top_priority":             env.get("top_priority"),
            },
            "recent_council_sessions":     sessions,
            "recent_coordination_records": coordination,
            "note": ("Audit is authoritative. Recent council sessions and "
                     "coordination records are advisory; Luna's local "
                     "decision is recorded in coordination_records."),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def build_stuck_status_payload() -> dict[str, Any]:
    """Read-only stuck-detection report. Authoritative."""
    try:
        from luna_modules import luna_stuck_detector as _sd
        return _sd.detect(persist_history=False)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def build_progression_proof_payload() -> dict[str, Any]:
    """Read-only progression-proof envelope. Authoritative."""
    try:
        from luna_modules import luna_progression_proof as _pp
        return _pp.write_progression_proof()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def build_verifier_status_payload() -> dict[str, Any]:
    """Canonical /api/verifier-status — single source for every panel."""
    try:
        from luna_modules import luna_verifier_status as _vs
        return _vs.compute_verifier_status()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def build_first_tier_milestone_payload() -> dict[str, Any]:
    """Operator-visible first-tier milestone."""
    try:
        from luna_modules import luna_first_tier_watch as _ftw
        return _ftw.check_first_tier()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def build_probe_health_payload() -> dict[str, Any]:
    """Latest snapshot from luna_probe_health_monitor.

    Reads memory/core_brain/probe_health_latest.json (produced by the
    scheduled task ``LunaProbeHealthSweepUser`` invoking
    ``Luna_Probe_Health_Sweep.ps1`` every N minutes). Authoritative
    for per-tier probe status across T1..T200. Returns a stable
    shape even when the snapshot is missing/corrupt so the panel
    can always render *something*.
    """
    try:
        from luna_modules import luna_probe_health_monitor as _mon
        snap = _mon.load_latest_snapshot()
        active = _mon.list_active_alerts(snap)
        if not snap:
            return {
                "ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": "snapshot_missing",
                "error_detail": (
                    "probe_health_latest.json not on disk. Install the "
                    "LunaProbeHealthSweepUser scheduled task via "
                    "Install_Luna_Probe_Health_Sweep_Task.ps1."),
                "active_failures": [],
                "ok_count": 0, "fail_count": 0, "ok_pct": 0.0,
            }
        return {
            "ok": True,
            "advisory_only": False,
            "authoritative": True,
            "schema_version": snap.get("schema_version", 1),
            "started_at": snap.get("started_at"),
            "finished_at": snap.get("finished_at"),
            "elapsed_ms": snap.get("elapsed_ms"),
            "tier_range": snap.get("tier_range"),
            "discovered_count": snap.get("discovered_count"),
            "ok_count": snap.get("ok_count"),
            "fail_count": snap.get("fail_count"),
            "ok_pct": round(snap.get("ok_pct", 0.0), 2),
            "alerts_this_sweep": snap.get("alerts_this_sweep", []),
            "active_failures": active,
            "active_failure_count": len(active),
            "bus_status": snap.get("bus_status"),
            "missing_tiers": snap.get("missing_tiers", []),
            "slow_probe_tiers": snap.get("slow_probe_tiers", []),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def build_cognitive_scheduler_payload() -> dict[str, Any]:
    """Phase 10 cognitive-scheduler surface (read-only).

    Returns the latest persisted SchedulerRun, current lockfile state,
    and the tail of the bounded scheduler-run log. The dashboard NEVER
    triggers a cycle; the scheduler is invoked only by the canonical
    LunaProbeHealthSweepUser scheduled task.
    """
    try:
        from luna_modules import cognitive_scheduler as _sched
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"cognitive_scheduler import failed: "
                            f"{str(exc)[:160]}",
        }
    return {
        "ok": True,
        "advisory_only": True,
        "authoritative": True,
        "scheduler_enabled": _sched.is_enabled(),
        "kill_switch_active": _sched.is_kill_switch_active(),
        "current_lock_state": _sched.current_lock_state(),
        "latest_scheduler_run": _sched.load_latest(),
        "recent_runs_tail": _sched.tail_run_log(8),
        "lock_path": _sched.LOCK_PATH,
        "stale_lock_seconds_threshold": _sched.STALE_LOCK_SECONDS,
    }


def build_cognitive_strategy_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 9 strategy-adjustments surface (read-only by default).

    Default: returns the latest strategy-adjustments snapshot persisted
    at ``memory/cognitive/latest_strategy_adjustments.json`` plus the
    current consumer-active flag state.

    ``fresh=True``: triggers a fresh adapt_latest() pass over the
    synthesized memories currently on disk. Analysis + persist only;
    never mutates upstream cognitive state.

    Honours BOTH Phase 9 flags:
    - ``cognitive_strategy_adaptation_enabled`` controls EMISSION
    - ``cognitive_strategy_active`` controls APPLICATION (read by Phase 4)
    """
    try:
        from luna_modules import strategy_adapter as _strategy
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"strategy_adapter import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _strategy.adapt_latest(
            {"trigger": "dashboard_fresh_request"})
        latest = _strategy.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_strategy_adjustments": latest,
            "consumer_active": _strategy.is_active(),
            "active_rule_count": len(_strategy.get_active_rules()),
        }
    latest = _strategy.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_strategy_adjustments": latest,
        "consumer_active": _strategy.is_active(),
        "active_rule_count": len(_strategy.get_active_rules()),
        "note": ("No latest_strategy_adjustments.json on disk yet. The "
                 "probe-sweep hook chain writes it after each "
                 "LunaProbeHealthSweepUser fire (Phase 9 stage). Use "
                 "?fresh=true to trigger one now over the current "
                 "synthesized memories.")
                 if latest is None else None,
    }


def build_cognitive_memory_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 8 synthesized-memory surface (read-only by default).

    Default: returns the latest per-cycle memory snapshot persisted at
    ``memory/cognitive/latest_synthesized_memory.json`` plus a peek at
    the bounded LRU store stats.

    ``fresh=True``: triggers a fresh synthesize_latest() pass over the
    reflection results currently on disk (also touches the LRU store).
    Analysis + store-only; never mutates upstream cognitive state.

    Honours the ``cognitive_memory_synthesis_enabled`` flag.
    """
    try:
        from luna_modules import memory_synthesis_engine as _mem
        from luna_modules import reflection_memory as _store_m
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"memory module import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _mem.synthesize_latest(
            {"trigger": "dashboard_fresh_request"})
        latest = _mem.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_synthesized_memory": latest,
            "store_stats": _store_m.stats(),
        }
    latest = _mem.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_synthesized_memory": latest,
        "store_stats": _store_m.stats(),
        "note": ("No latest_synthesized_memory.json on disk yet. The "
                 "probe-sweep hook chain writes it after each "
                 "LunaProbeHealthSweepUser fire. Use ?fresh=true to "
                 "trigger one now over the current reflection results.")
                 if latest is None else None,
    }


def build_cognitive_reflection_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 7 reflection surface (read-only by default).

    Default: returns the latest reflection-results snapshot persisted at
    ``memory/cognitive/latest_reflection_results.json``.

    ``fresh=True``: triggers a fresh reflect_latest() pass over the
    execution-results currently on disk. Analysis-only; never mutates
    upstream cognitive state.

    Honours the ``cognitive_reflection_enabled`` flag.
    """
    try:
        from luna_modules import reflection_engine as _refl
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"reflection_engine import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _refl.reflect_latest(
            {"trigger": "dashboard_fresh_request"})
        latest = _refl.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_reflection_results": latest,
        }
    latest = _refl.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_reflection_results": latest,
        "note": ("No latest_reflection_results.json on disk yet. The "
                 "probe-sweep hook chain writes it after each "
                 "LunaProbeHealthSweepUser fire. Use ?fresh=true to "
                 "trigger one now over the current execution results.")
                 if latest is None else None,
    }


def build_cognitive_execution_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 6 execution-results surface (read-only by default).

    Default: returns the latest execution-results snapshot persisted at
    ``memory/cognitive/latest_execution_results.json``.

    ``fresh=True``: triggers a fresh execute_latest() pass over the
    plans currently on disk. The executor refuses any plan that is not
    fully read-only + dry_run at preflight.

    Honours the ``cognitive_execution_enabled`` flag.
    """
    try:
        from luna_modules import execution_coordinator as _exec
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"execution_coordinator import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _exec.execute_latest(
            {"trigger": "dashboard_fresh_request"})
        latest = _exec.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_execution_results": latest,
        }
    latest = _exec.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_execution_results": latest,
        "note": ("No latest_execution_results.json on disk yet. The "
                 "probe-sweep hook chain writes it after each "
                 "LunaProbeHealthSweepUser fire. Use ?fresh=true to "
                 "trigger one now over the current plans snapshot.")
                 if latest is None else None,
    }


def build_cognitive_plans_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 5 plans surface (read-only by default).

    Default: returns the latest plans snapshot persisted at
    ``memory/cognitive/latest_plans.json`` (written by the probe-sweep
    hook chain).

    ``fresh=True``: triggers a fresh plan_latest() pass over the
    prioritized-goals snapshot currently on disk. Same code path the
    probe-sweep hook uses. Plans are pure description -- no external
    calls, no action.

    Honours the ``cognitive_planning_enabled`` flag.
    """
    try:
        from luna_modules import planning_engine as _plan
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"planning_engine import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _plan.plan_latest(
            {"trigger": "dashboard_fresh_request"})
        latest = _plan.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_plans": latest,
        }
    latest = _plan.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_plans": latest,
        "note": ("No latest_plans.json on disk yet. The probe-sweep hook "
                 "chain writes it after each LunaProbeHealthSweepUser "
                 "fire. Use ?fresh=true to trigger one now over the "
                 "current prioritized-goals snapshot.")
                 if latest is None else None,
    }


def build_cognitive_priorities_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 4 prioritized-goals surface (read-only by default).

    Default: returns the latest prioritization snapshot persisted at
    ``memory/cognitive/latest_prioritized_goals.json`` (written by the
    probe-sweep hook chain).

    ``fresh=True``: triggers a fresh prioritize_latest() pass over the
    candidate-goals currently on disk. Same code path the probe-sweep
    hook uses. Pure projection -- no external calls, no action.

    Honours the ``cognitive_prioritization_enabled`` flag.
    """
    try:
        from luna_modules import goal_prioritizer as _prio
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"goal_prioritizer import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _prio.prioritize_latest(
            {"trigger": "dashboard_fresh_request"})
        latest = _prio.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_prioritized_goals": latest,
        }
    latest = _prio.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_prioritized_goals": latest,
        "note": ("No latest_prioritized_goals.json on disk yet. The "
                 "probe-sweep hook chain writes it after each "
                 "LunaProbeHealthSweepUser fire. Use ?fresh=true to "
                 "trigger one now over the current candidate-goals.")
                 if latest is None else None,
    }


def build_cognitive_goals_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 3 candidate-goals surface (read-only by default).

    Default: returns the latest candidate-goals snapshot persisted at
    ``memory/cognitive/latest_candidate_goals.json`` (written by the
    probe-sweep hook chain).

    ``fresh=True``: triggers a fresh generate_latest() pass over the
    interpretation currently on disk. Same code path the probe-sweep
    hook uses. Still pure projection -- no external calls, no action.

    Honours the ``cognitive_goal_generation_enabled`` flag.
    """
    try:
        from luna_modules import goal_generation_engine as _goals
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"goal_generation_engine import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _goals.generate_latest(
            {"trigger": "dashboard_fresh_request"})
        latest = _goals.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_candidate_goals": latest,
        }
    latest = _goals.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_candidate_goals": latest,
        "note": ("No latest_candidate_goals.json on disk yet. The "
                 "probe-sweep hook chain writes it after each "
                 "LunaProbeHealthSweepUser fire. Use ?fresh=true to "
                 "trigger one now over the current interpretation.")
                 if latest is None else None,
    }


def build_cognitive_interpretation_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 2 interpretation surface (read-only by default).

    Default: returns the latest interpretation persisted at
    ``memory/cognitive/latest_interpretation.json`` (written by the
    probe-sweep hook chain). Lightweight: just a JSON read.

    ``fresh=True``: triggers a fresh interpret_latest() pass over the
    observation currently on disk. Same code path the probe-sweep hook
    uses. Still pure projection -- no external calls, no action.

    Honours the ``cognitive_interpretation_enabled`` flag: when disabled,
    ``fresh=True`` returns the disabled stub; the read-only path still
    returns whatever is on disk.
    """
    try:
        from luna_modules import interpretation_engine as _interp
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"interpretation_engine import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _interp.interpret_latest(
            {"trigger": "dashboard_fresh_request"})
        latest = _interp.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_interpretation": latest,
        }
    latest = _interp.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_interpretation": latest,
        "note": ("No latest_interpretation.json on disk yet. The "
                 "probe-sweep hook chain writes it after each "
                 "LunaProbeHealthSweepUser fire. Use ?fresh=true to "
                 "trigger one now over the current observation.")
                 if latest is None else None,
    }


def build_cognitive_observation_payload(fresh: bool = False) -> dict[str, Any]:
    """Phase 1 observation surface (read-only by default).

    Default: returns the latest observation persisted at
    ``memory/cognitive/latest_observation.json`` (written by the
    probe-sweep hook). Lightweight: just a JSON read.

    ``fresh=True``: triggers a fresh observation through
    :func:`observation_engine.observe_and_persist`. Same code path the
    probe-sweep hook calls. Still read-only -- collectors only read disk.

    Honours the ``cognitive_observation_enabled`` flag: when disabled,
    ``fresh=True`` returns the disabled stub from observe_and_persist;
    the read-only path still works (returns whatever is on disk).

    Rollback: set ``cognitive_observation_enabled`` to false. To remove
    the endpoint entirely, revert this function and the routing block.
    """
    try:
        from luna_modules import observation_engine as _obs
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": f"observation_engine import failed: "
                            f"{str(exc)[:160]}",
            "mode": "fresh" if fresh else "read_only",
        }
    if fresh:
        triggered = _obs.observe_and_persist({"trigger": "dashboard_fresh_request"})
        latest = _obs.load_latest()
        return {
            "ok": bool(triggered.get("ok")) or bool(latest),
            "advisory_only": True,
            "authoritative": True,
            "mode": "fresh",
            "trigger_result": triggered,
            "latest_observation": latest,
        }
    latest = _obs.load_latest()
    return {
        "ok": latest is not None,
        "advisory_only": True,
        "authoritative": True,
        "mode": "read_only",
        "latest_observation": latest,
        "note": ("No latest_observation.json on disk yet. The probe-sweep "
                 "hook writes it after each LunaProbeHealthSweepUser fire. "
                 "Use ?fresh=true to trigger one now.")
                 if latest is None else None,
    }


def build_cognitive_status_payload() -> dict[str, Any]:
    """Phase 0 cognitive foundation status surface (read-only).

    Surfaces the state of the cognitive foundation modules
    (``cognitive_state``, ``cognitive_contracts``, ``cognitive_event_log``,
    ``cognitive_feature_flags``, ``cognitive_core``). The cognitive path
    is OFF by default. This endpoint NEVER triggers any cognition; it
    only reads flag file + module-level introspection.

    Rollback: delete or zero out
    ``D:\\SurgeApp\\memory\\cognitive\\feature_flags.json``.
    """
    try:
        from luna_modules import cognitive_core as _cog
        snap = _cog.status()
        # Wrap in dashboard-standard envelope so the panel can render
        # consistently with /api/probe-health, /api/rebuild-campaign, etc.
        return {
            "ok": bool(snap.get("ok")),
            "advisory_only": True,
            "authoritative": True,
            "schema_version": snap.get("schema_version", 1),
            "phase": snap.get("phase"),
            "foundation_version": snap.get("foundation_version"),
            "is_enabled": snap.get("is_enabled"),
            "is_logging_enabled": snap.get("is_logging_enabled"),
            "modules_loaded": snap.get("modules_loaded", []),
            "contracts_registered": snap.get("contracts_registered", []),
            "artifact_types": snap.get("artifact_types", []),
            "flags": snap.get("flags", {}),
            "log_file": snap.get("log_file"),
            "flag_file": snap.get("flag_file"),
            "python_executable": snap.get("python_executable"),
            "note": snap.get("note"),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "advisory_only": True, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
        }




def build_terminal_truth_payload() -> dict[str, Any]:
    return _cached_build(_build_terminal_truth_payload_raw, "terminal_truth")


def build_terminal_truth_panel_payload(panel: str) -> dict[str, Any]:
    def _fn():
        return _build_terminal_truth_panel_payload_raw(panel)
    return _cached_build(_fn, f"terminal_truth_panel:{panel}")


def build_master_status_payload() -> dict[str, Any]:
    return _cached_build(_build_master_status_payload_raw, "master_status")


def build_mission_control_payload() -> dict[str, Any]:
    return _cached_build(_build_mission_control_payload_raw, "mission_control")


def build_rebuild_campaign_payload() -> dict[str, Any]:
    return _cached_build(_build_rebuild_campaign_payload_raw, "rebuild_campaign")


def _build_rebuild_campaign_payload_raw() -> dict[str, Any]:
    """Read-only Tier 1..500 rebuild campaign surface.

    2026-05-13 doctrine: every tier from 1..500 must earn honest
    status. Returns the campaign frontier + ledger summary + queued
    task pointer + post-500 maintenance status."""
    try:
        from luna_modules import luna_tier_rebuild_campaign as _rc
        from luna_modules import luna_tier_post_500_maintenance as _maint
        status = _rc.campaign_status()
        status["post_500_maintenance"] = _maint.revalidation_sweep()
        return _attach_canonical_truth_summary(status)
    except Exception as exc:  # noqa: BLE001
        return _attach_canonical_truth_summary({
            "ok": False, "advisory_only": False, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
        })


def build_canonical_truth_payload() -> dict[str, Any]:
    """The ONE canonical current-truth payload — primary truth surface.

    Every primary panel in the dashboard MUST consume this FIRST.
    Panel-specific endpoints may elaborate but never override these
    fields. Built by :mod:`luna_modules.luna_canonical_truth`.
    """
    try:
        from luna_modules import luna_canonical_truth as _ct
        payload = _ct.build_canonical_current_truth()
        # Self-reference for symmetry — the canonical payload contains
        # everything in its own summary already.
        if isinstance(payload, dict):
            payload["canonical_truth_summary"] = _ct.canonical_truth_summary(payload)
        return payload
    except Exception as exc:  # noqa: BLE001
        return {
            "ok":             False,
            "advisory_only":  False,
            "authoritative":  True,
            "error_kind":     type(exc).__name__,
            "error_detail":   str(exc)[:200],
            "generated_at":   _now_iso(),
        }


def _build_terminal_truth_payload_raw() -> dict[str, Any]:
    """The HARD-CUTOVER canonical snapshot for the terminal.

    See ``luna_modules.luna_terminal_truth`` docstring for the contract.
    Every primary panel renders its primary fields from THIS payload.
    """
    try:
        from luna_modules import luna_terminal_truth as _tt
        return _tt.build_terminal_truth()
    except Exception as exc:  # noqa: BLE001
        return {
            "ok":             False,
            "schema_version": 1,
            "generated_at":   _now_iso(),
            "error_kind":     type(exc).__name__,
            "error_detail":   str(exc)[:200],
            "terminal_truth": None,
            "primary_truth_source": "luna_terminal_truth.build_terminal_truth",
        }


def _build_terminal_truth_panel_payload_raw(panel: str) -> dict[str, Any]:
    """Per-panel slice of the canonical terminal-truth snapshot."""
    try:
        from luna_modules import luna_terminal_truth as _tt
        if panel not in _tt.PRIMARY_PANELS:
            return {
                "ok": False,
                "schema_version": 1,
                "generated_at": _now_iso(),
                "error_kind": "UnknownPanel",
                "error_detail": f"panel '{panel}' is not a primary panel",
                "panel": panel,
                "truth": None,
                "health": None,
                "meta": None,
            }
        return _tt.build_terminal_truth_panel_slice(panel)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "schema_version": 1,
            "generated_at": _now_iso(),
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "panel": panel,
            "truth": None,
            "health": None,
            "meta": None,
        }


def _build_master_status_payload_raw() -> dict[str, Any]:
    """ONE consolidated operator surface. Answers every audit-checklist
    question from a single endpoint.

    2026-05-13 final overhaul: surfaces operator_truth + cost_control +
    backfill + stuck + progression + provider_health + crash_breadcrumb
    + self_recovery + watchdog + runtime_ownership + housekeeping + a
    one-line headline block. Read-only, offline.
    """
    try:
        from luna_modules import luna_master_status as _ms
        return _attach_canonical_truth_summary(_ms.build_master_status())
    except Exception as exc:  # noqa: BLE001
        return _attach_canonical_truth_summary({
            "ok": False, "advisory_only": False, "authoritative": True,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
        })


def build_housekeeping_payload() -> dict[str, Any]:
    """Read-only operator-visible housekeeping surface.

    2026-05-13 autonomous housekeeping system: returns the latest sweep
    summary + policy mode + counters. Authoritative for the local
    housekeeping state.
    """
    try:
        from luna_modules import luna_housekeeping as _hk
        block = _hk.operator_panel_block()
        block["ok"] = True
        return block
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def build_cost_control_payload() -> dict[str, Any]:
    """Read-only operator surface for the strict cost-control policy.

    2026-05-13 free-first routing: surfaces the policy snapshot, the
    recent paid-escalation log, recent denials, and monthly running
    spend estimate. Authoritative for OBSERVATIONS; advisory for the
    estimates."""
    try:
        from luna_modules import luna_cost_routing_policy as _crp
        return _crp.cost_control_status()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "advisory_only": False, "authoritative": True,
                "error_kind": type(exc).__name__,
                "error_detail": str(exc)[:200]}


def build_operator_truth_payload() -> dict[str, Any]:
    """Single canonical operator-truth payload.

    2026-05-13 Terminal Accuracy Pass: every visible operator-facing
    panel reads through this surface. Cross-checks tier_graduation
    against truth_verdict so the operator NEVER sees OPERATIONAL_PROVEN
    while the canonical layer says UNDER_AUDIT. Read-only, offline."""
    try:
        from luna_modules import luna_operator_truth_surface as _ots
        return _attach_canonical_truth_summary(_ots.build_operator_truth_payload())
    except Exception as exc:  # noqa: BLE001
        return _attach_canonical_truth_summary({
            "ok":             False,
            "advisory_only":  False,
            "authoritative":  True,
            "error_kind":     type(exc).__name__,
            "error_detail":   str(exc)[:200],
        })


def build_council_status_payload() -> dict[str, Any]:
    """Read-only operator surface for the Luna Council Advisor.

    2026-05-13 Council subsystem (advisory-only, non-authoritative):
    surfaces the LAST advisory session + health from memory/llm_council/.
    NEVER reaches the network. NEVER influences tier truth, repair tasks,
    or any operator-facing authoritative display. Every field returned
    here carries advisory_only=True, authoritative=False so a misread by
    any downstream consumer cannot accidentally elevate council output
    into authoritative truth.
    """
    try:
        from luna_modules import luna_llm_council_bridge   as _bridge
        from luna_modules import luna_llm_council_storage  as _store
        from luna_modules import luna_llm_council_reporter as _rep
        from luna_modules import luna_council_advisor      as _advisor
        from luna_modules import luna_council_consumer_trail as _trail
        # Offline-only path. We do NOT call _bridge.run_council here;
        # the read endpoint must never dispatch to a remote provider.
        last_session = _store.latest_session()
        last_summary = _rep.summarize_for_dashboard(last_session) if last_session else None
        # Per-integration-topic last advisory so the operator card can
        # show which build workflows have recently consulted the council.
        per_topic    = _advisor.last_advisory_per_topic(limit_scanned=50)
        # Phase 3: real-consumer trail summary. Read-only — every field
        # carries advisory_only=True and the dashboard renders it as
        # "Luna locally decided X with advisor input Y".
        last_per_consumer = _trail.last_per_consumer()
        recent_trail      = _trail.latest_entries(limit=10)
        accept_stats      = _trail.acceptance_stats(limit=200)
        last_consumer_advice = recent_trail[0] if recent_trail else None
        h = _bridge.health()
        return {
            "ok":                          True,
            "advisory_only":               True,
            "authoritative":               False,
            "health":                      h,
            "last_advisory":               last_summary,
            "last_advisory_per_topic":     per_topic,
            "integration_topics":          list(_advisor.ADVISORY_TOPICS),
            # Phase 3 additions:
            "last_consumer_advice":        last_consumer_advice,
            "last_advice_per_consumer":    last_per_consumer,
            "recent_consumer_trail":       recent_trail,
            "consumer_acceptance_stats":   accept_stats,
            "note": (
                "Council Advisor is advisory only. Luna alone owns tier "
                "truth, repair completion, adoption/use records, and "
                "operator-facing displays. Consumer trail entries show "
                "Luna's LOCAL decisions after receiving advisory input."
            ),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok":                          False,
            "advisory_only":               True,
            "authoritative":               False,
            "error_kind":                  type(exc).__name__,
            "error_detail":                str(exc)[:200],
            "last_advisory":               None,
            "last_advisory_per_topic":     None,
            "last_consumer_advice":        None,
            "last_advice_per_consumer":    None,
            "recent_consumer_trail":       [],
            "consumer_acceptance_stats":   None,
        }


def build_cyberguy_status_payload() -> dict[str, Any]:
    """Return Luna CyberGuy defensive security status. Read-only.
    NEVER includes API key values. Suitable for /api/cyberguy/status."""
    try:
        from luna_modules.luna_cyberguy_guardian import get_cyberguy_status, safe_redact
        s = get_cyberguy_status()
        if not isinstance(s, dict):
            s = {"ok": False, "error": "get_cyberguy_status returned non-dict"}
        # Defense in depth: redact any long string fields
        for k, v in list(s.items()):
            if isinstance(v, str) and len(v) > 20:
                s[k] = safe_redact(v)
        return s
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "overall_severity": "WATCH",
            "secrets_redaction_active": True,
            "external_network_scan_active": False,
            "auto_repair_active": False,
        }


def build_cyberguy_report_payload() -> dict[str, Any]:
    """Return the full CyberGuy posture report. Already secret-redacted."""
    try:
        from luna_modules.luna_cyberguy_guardian import get_cyberguy_full_report
        return get_cyberguy_full_report()
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "overall_severity": "WATCH",
        }


def build_privacy_status_payload() -> dict[str, Any]:
    """Return Luna External Model Privacy Boundary status.

    2026-05-11 Serge directive: surface 'External model mode: Coding-only,
    Primary provider: Gemini, Private memory: Local-only, Last external
    model call: timestamp/provider/purpose, Secrets protected: yes'.

    NEVER includes API key values."""
    try:
        from luna_modules.luna_external_prompt_guard import privacy_boundary_status
        s = privacy_boundary_status()
        if not isinstance(s, dict):
            s = {"ok": False, "error": "privacy_boundary_status returned non-dict"}
        # Defense in depth: redact anything that looks like a key
        try:
            from luna_modules.luna_memory_os import redact_secrets as _rs
            for k, v in list(s.items()):
                if isinstance(v, str) and len(v) > 20:
                    s[k] = _rs(v)
        except Exception:
            pass
        return s
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "external_model_mode": "Coding-only",
            "primary_provider": "Gemini",
            "personal_memory_local_only": True,
            "secrets_redaction_active": True,
        }


def build_memory_status_payload() -> dict[str, Any]:
    """Return Luna Memory OS status. Counts + flags only.
    NEVER includes key values or raw memory content.

    2026-05-11 Serge directive: 'show memory status -- raw chat archive
    count, long-term memory count, preferences loaded yes/no,
    personality profile loaded yes/no, latest daily summary'."""
    try:
        from luna_modules.luna_memory_os import memory_os_status
        s = memory_os_status()
        if not isinstance(s, dict):
            s = {"ok": False, "error": "memory_os_status returned non-dict"}
        # Defense: scrub anything that looks like a key value
        from luna_modules.luna_memory_os import redact_secrets as _rs
        for k, v in list(s.items()):
            if isinstance(v, str) and len(v) > 20:
                s[k] = _rs(v)
        return s
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "memory_os_module": "luna_modules.luna_memory_os",
        }


def build_memory_search_payload(q: str = "", limit: int = 10) -> dict[str, Any]:
    """Return top-N relevance-ranked memory snippets for query `q`.

    NEVER includes API keys. Each snippet is passed through
    luna_memory_os.redact_secrets both at search-time and here at
    response-serialization time (defense in depth)."""
    try:
        if not q or not q.strip():
            return {
                "ok": False,
                "error": "missing or empty 'q' parameter",
                "results": [],
                "query": q,
            }
        from luna_modules.luna_memory_os import search_memory, redact_secrets
        results = search_memory(q, max_results=int(limit))
        # Defense in depth: re-redact and bound snippet length
        safe = []
        for r in (results or []):
            if not isinstance(r, dict):
                continue
            text = redact_secrets(str(r.get("text") or ""))[:480]
            safe.append({
                "source": str(r.get("source") or "unknown")[:64],
                "ts":     r.get("ts"),
                "score":  r.get("score"),
                "text":   text,
            })
        return {
            "ok": True,
            "query": str(q)[:200],
            "limit": int(limit),
            "result_count": len(safe),
            "results": safe,
            "secret_redaction_applied": True,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "query": q,
            "results": [],
        }


def build_model_hierarchy_payload() -> dict[str, Any]:
    """Return Luna's current model hierarchy. Safe view -- API keys are
    NEVER included in the response body. Reads memory/luna_model_hierarchy.json
    as the source of truth.

    Public contract (2026-05-11 Serge directive):
        primary:     Gemini (top of hierarchy, default for every task)
        secondary:   Local/Ollama (cheap always-on background)
        optional:    Grok (backup if configured)
        claude_anthropic_active:  false
    """
    import json as _json
    try:
        hpath = MEMORY_DIR / "luna_model_hierarchy.json"
        if not hpath.exists():
            return {
                "ok": False,
                "error": "luna_model_hierarchy.json not found",
                "primary": "Gemini",
                "fallback": "Local/Ollama",
                "optional": "Grok",
                "claude_anthropic_active": False,
            }
        raw = hpath.read_text(encoding="utf-8-sig", errors="replace")
        d = _json.loads(raw) if raw.strip() else {}
        if not isinstance(d, dict):
            d = {}
        # Build a safe response that strips any key-source paths from the
        # detail entries (those are paths, not keys, but be defensive).
        hierarchy_detail_safe = []
        for entry in (d.get("hierarchy_detail") or []):
            if not isinstance(entry, dict):
                continue
            safe = {
                "rank": entry.get("rank"),
                "name": entry.get("name"),
                "role": entry.get("role"),
                "endpoint": entry.get("endpoint"),
                "default_pro_model": entry.get("default_pro_model"),
                "default_flash_model": entry.get("default_flash_model"),
                "default_model_suggestion": entry.get("default_model_suggestion"),
                "current_default_model": entry.get("current_default_model"),
                "status_note": entry.get("status_note"),
                "notes": entry.get("notes"),
            }
            # NOTE: key_source path is intentionally OMITTED here; the
            # raw JSON file is operator-readable but the API response
            # never surfaces it.
            hierarchy_detail_safe.append({k: v for k, v in safe.items() if v is not None})

        return {
            "ok": True,
            "primary": "Gemini",
            "primary_model_source": d.get("primary_model_source", "Gemini"),
            "primary_default_model_pro": d.get("primary_default_model_pro", "gemini-2.5-pro"),
            "primary_default_model_flash": d.get("primary_default_model_flash", "gemini-2.5-flash"),
            "fallback": "Local/Ollama",
            "optional": "Grok",
            "hierarchy": d.get("hierarchy") or [
                "Gemini",
                "Local/Ollama",
                "Grok",
                "Other only if Serge specifically asks",
            ],
            "hierarchy_detail": hierarchy_detail_safe,
            "removed_from_active_hierarchy": d.get("removed_from_active_hierarchy") or [
                "Claude",
                "Anthropic",
            ],
            "claude_anthropic_active": False,
            "rules": d.get("rules") or [],
            "intent_router_responses": d.get("intent_router_responses") or {},
            "do_not_print_keys": True,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc)[:200],
            "primary": "Gemini",
            "fallback": "Local/Ollama",
            "optional": "Grok",
            "claude_anthropic_active": False,
        }


def _build_tier_runtime_capability_status() -> dict[str, Any]:
    """Read the runtime-capability proof history and report the highest
    tier with a 'proven' entry.

    Distinguishes:
      * current_effective_tier         - the council-authorized tier counter
      * operational_capability_tier    - highest tier with real runtime proof
      * tier45_runtime_status          - pending / proven / failed for Tier 45
    """
    proof_path = MEMORY_DIR / "tier_auto_upgrade" / "tier_runtime_proof_history.json"
    cfg_path = MEMORY_DIR / "luna_higher_tier_config.json"
    cfg = _safe_read_json(cfg_path) or {}
    cet_str = str(cfg.get("current_effective_tier") or "0")
    try:
        cet = int(cet_str)
    except (TypeError, ValueError):
        cet = 0
    proofs = _safe_read_json(proof_path) or {}
    proof_list = proofs.get("proofs") or []
    proven_tiers: list[int] = []
    proven_by_tier: dict[int, dict[str, Any]] = {}
    for entry in proof_list:
        if not isinstance(entry, dict):
            continue
        if entry.get("proof_status") != "proven":
            continue
        try:
            t = int(entry.get("tier") or 0)
        except (TypeError, ValueError):
            continue
        if t > 0:
            proven_tiers.append(t)
            proven_by_tier[t] = entry
    operational_capability_tier = max(proven_tiers) if proven_tiers else 9
    # Tier 45 status: 'proven' if a Tier 45 proof exists, else
    # 'pending' when CET >= 45, else 'not_yet_authorized'.
    if 45 in proven_by_tier:
        tier45_status = "proven"
    elif cet >= 45:
        tier45_status = "pending"
    else:
        tier45_status = "not_yet_authorized"
    tier45_target = "luna_modules/luna_routing.py"
    tier45_blocker = None
    tier45_entry = proven_by_tier.get(45)
    if tier45_status == "pending":
        tier45_blocker = (
            "Tier 45 counter authorized but TIER45_RUNTIME_PROOF.md / "
            "tier_runtime_proof_history.json has no proven entry yet."
        )
    return {
        "current_effective_tier":         cet,
        "operational_capability_tier":    operational_capability_tier,
        "tier45_runtime_status":          tier45_status,
        "tier45_runtime_target":          tier45_target,
        "tier45_blocker":                 tier45_blocker,
        "tier45_runtime_proof_path":      "memory/tier_auto_upgrade/TIER45_RUNTIME_PROOF.md" if tier45_status == "proven" else None,
        "tier45_proof_record":            tier45_entry if tier45_entry else None,
        "proven_tiers":                   sorted(proven_tiers),
        "tier_label_for_headline":        _build_tier_label_for_headline(
            cet, operational_capability_tier, proven_by_tier
        ),
    }


def _build_tier_label_for_headline(
    cet: int,
    operational_capability_tier: int,
    proven_by_tier: dict[int, Any],
) -> str:
    """Build the Mission Control / Evolution Command Center headline.

    2026-05-12 honesty fix per Serge: never claim "Level 10 Tier 50
    ACTIVE" while the operating tier (per the proof registry) is
    lower than the counter. The pre-fix code formatted ``cet`` (the
    counter, e.g. 500) and labelled it "active" / "authorized" — which
    is exactly what Serge captured on his dashboard. Now:

      * if the proof registry's ``current_operating_tier`` differs
        from the counter, the headline shows the OPERATING tier with
        an "under audit" qualifier, and notes the counter as a side
        fact.
      * if operating tier equals counter and is in ``proven_by_tier``,
        we keep the legacy "active - runtime proof passed" wording.
      * otherwise we label it "authorized - runtime proof pending"
        / "active" exactly as before.
    """
    op_tier = None
    try:
        from pathlib import Path as _P
        import json as _json
        reg_path = MEMORY_DIR / "tier_truth" / "tier_proof_registry.json"
        if reg_path.exists():
            data = _json.loads(reg_path.read_text(encoding="utf-8"))
            v = data.get("current_operating_tier")
            if v is not None:
                op_tier = int(v)
    except Exception:
        op_tier = None
    if op_tier is not None and op_tier != cet:
        # Drift — the counter is higher than the proven operating tier.
        # Render the OPERATING tier as the headline; never claim the
        # counter is active.
        return (
            _fmt_level_tier(op_tier) + " operating - counter "
            + _fmt_level_tier(cet) + " under audit"
        )
    if cet in proven_by_tier:
        return _fmt_level_tier(cet) + " active - runtime proof passed"
    if cet > operational_capability_tier:
        return _fmt_level_tier(cet) + " authorized - runtime proof pending"
    return _fmt_level_tier(cet) + " active"


# ---------------------------------------------------------------------------
# Tier 2 approval handler (POST). Local-only by design (server only listens on
# 127.0.0.1). Refuses unless tier2 is currently eligible.
# ---------------------------------------------------------------------------
APPROVAL_EVENTS_PATH = MEMORY_DIR / "luna_self_upgrade_approval_events.jsonl"


def _approve_tier2_handle(handler: BaseHTTPRequestHandler) -> None:
    # Local-loopback safety: refuse anything that isn't the loopback adapter.
    client_host = ""
    try:
        client_host = (handler.client_address or ("",))[0]
    except Exception:
        client_host = ""
    if client_host not in {"127.0.0.1", "::1", "localhost"}:
        _send_json(handler, HTTPStatus.FORBIDDEN, {"ok": False, "error": "loopback only"})
        return

    try:
        length = int(handler.headers.get("Content-Length") or "0")
    except (TypeError, ValueError):
        length = 0
    if length <= 0 or length > 4096:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid body size"})
        return
    raw = handler.rfile.read(length)
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid json"})
        return
    if not isinstance(data, dict):
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "expected object"})
        return

    # Strict, explicit fields. No defaults. No magic.
    if data.get("approve") is not True:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "missing approve=true"})
        return
    if str(data.get("action") or "") != "APPROVE_TIER2":
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "missing action=APPROVE_TIER2"})
        return
    approved_by = str(data.get("approved_by") or "").strip()
    if not approved_by:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "missing approved_by"})
        return

    gate_path = READONLY_SOURCES["evidence_gate"]
    gate = _safe_read_json(gate_path)
    if not isinstance(gate, dict):
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": "evidence gate unreadable"})
        return

    rules = gate.get("promotion_rules", {}) if isinstance(gate.get("promotion_rules"), dict) else {}
    threshold_t2 = int(rules.get("tier_2_promotion_threshold_t0t1_successes", 10))
    sum_t0t1 = int(gate.get("tier0_success_count", 0)) + int(gate.get("tier1_success_count", 0))
    rb_failures = int(gate.get("rollback_failure_count", 0))
    eligible = bool(gate.get("tier2_eligible")) or (sum_t0t1 >= threshold_t2 and rb_failures == 0)
    if not eligible:
        _send_json(handler, HTTPStatus.CONFLICT, {
            "ok": False,
            "error": "tier 2 not eligible",
            "needed_t0t1_successes": threshold_t2,
            "have_t0t1_successes": sum_t0t1,
            "rollback_failures": rb_failures,
        })
        return

    now = _now_iso()
    gate["tier2_eligible"] = True
    gate["tier2_approved"] = True
    gate["tier2_approval_time"] = now
    gate["tier2_approved_by"] = approved_by
    gate["current_allowed_tier"] = max(int(gate.get("current_allowed_tier", 1)), 2)
    gate["last_promotion_decision"] = {
        "promoted_to": 2,
        "ts": now,
        "reason": f"approved by {approved_by} via dashboard/API",
    }
    gate["last_updated"] = now

    # Atomic write: temp-and-rename, BOM-free UTF-8.
    try:
        gate_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = gate_path.with_suffix(gate_path.suffix + ".tmp")
        tmp.write_text(json.dumps(gate, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, gate_path)
    except OSError as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": f"write failed: {exc.__class__.__name__}"})
        return

    # Append a durable approval-events record.
    try:
        APPROVAL_EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        event = {
            "ts": now,
            "action": "APPROVE_TIER2",
            "approved_by": approved_by,
            "client": client_host,
            "t0t1_successes_at_approval": sum_t0t1,
            "rollback_failures_at_approval": rb_failures,
        }
        with APPROVAL_EVENTS_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except OSError:
        # Approval already persisted in gate; failing to append the event log
        # is non-fatal (best-effort audit trail).
        pass

    _send_json(handler, HTTPStatus.OK, {
        "ok": True,
        "tier2_approved": True,
        "tier2_approval_time": now,
        "tier2_approved_by": approved_by,
        "current_allowed_tier": int(gate.get("current_allowed_tier", 2)),
        "ack": f"Tier 2 approved by {approved_by}",
    })


# ---------------------------------------------------------------------------
# Self-upgrade run-cycle handler (POST). Local-only, fixed command, bounded
# TierMax 2. The existing supervisor script keeps the runtime/verifier/gate
# safety controls authoritative.
# ---------------------------------------------------------------------------
def _run_self_upgrade_cycle_handle(handler: BaseHTTPRequestHandler) -> None:
    client_host = ""
    try:
        client_host = (handler.client_address or ("",))[0]
    except Exception:
        client_host = ""
    if client_host not in {"127.0.0.1", "::1", "localhost"}:
        _send_json(handler, HTTPStatus.FORBIDDEN, {"ok": False, "error": "loopback only"})
        return

    try:
        length = int(handler.headers.get("Content-Length") or "0")
    except (TypeError, ValueError):
        length = 0
    if length < 0 or length > 2048:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid body size"})
        return
    data: dict[str, Any] = {}
    if length:
        raw = handler.rfile.read(length)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid json"})
            return
        if not isinstance(parsed, dict):
            _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "expected object"})
            return
        data = parsed
    action = str(data.get("action") or "")
    if action not in {"", "RUN_ONE_WORK_CYCLE", "START_SPRINT_MODE", "RUN_ONE_SPRINT_CYCLE"}:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid action"})
        return

    supervisor = PROJECT_ROOT / "Luna_AlwaysOn_Supervisor.ps1"
    if not supervisor.exists():
        _send_json(handler, HTTPStatus.NOT_FOUND, {"ok": False, "error": "supervisor script missing"})
        return

    sprint_mode = bool(data.get("sprint")) or action in {"START_SPRINT_MODE", "RUN_ONE_SPRINT_CYCLE"}
    loop_mode = action == "START_SPRINT_MODE"

    sup_args: list[str] = []
    if loop_mode:
        sup_args.append("-SupervisorLoop")
    else:
        sup_args.append("-RunOnce")
    if sprint_mode:
        sup_args.append("-SprintMode")
        sup_args.extend(["-EnableTier2IfGatePasses", "-CleanStaleDashboard"])
    else:
        sup_args.extend([
            "-TierMax", "2",
            "-EnableTier2IfGatePasses",
            "-MaxTasksPerCycle", "5",
            "-MaxMinutesPerCycle", "45",
            "-CleanStaleDashboard",
        ])
    pid, err = _launch_via_wscript(supervisor, sup_args)
    if err is not None:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {
            "ok": False,
            "error": err,
        })
        return

    _send_json(handler, HTTPStatus.ACCEPTED, {
        "ok": True,
        "started": True,
        "pid": pid,
        "tier_max": 2,
        "max_tasks": 10 if sprint_mode else 5,
        "max_minutes": 30 if sprint_mode else 45,
        "cycle_interval_minutes": 1 if sprint_mode else None,
        "sprint_mode": sprint_mode,
        "loop_mode": loop_mode,
        "message": (
            "Sprint Mode supervisor loop started" if loop_mode
            else ("Sprint Mode bounded cycle started" if sprint_mode
                  else "bounded supervisor cycle started")
        ),
    })


# ---------------------------------------------------------------------------
# Supervisor control endpoints (run-once, start-sprint, stop-sprint, status)
# All local-only, all guarded by client_host check + body validation.
# ---------------------------------------------------------------------------
SUPERVISOR_LOCK_PATH = MEMORY_DIR / "always_on" / "luna_always_on.lock"
SUPERVISOR_HEARTBEAT_PATH = MEMORY_DIR / "always_on" / "luna_always_on_heartbeat.json"
SPRINT_STOP_FLAG_PATH = MEMORY_DIR / "always_on" / "luna_sprint_stop.flag"


def _check_loopback(handler: BaseHTTPRequestHandler) -> bool:
    try:
        client_host = (handler.client_address or ("",))[0]
    except Exception:
        client_host = ""
    if client_host in {"127.0.0.1", "::1", "localhost"}:
        return True
    _send_json(handler, HTTPStatus.FORBIDDEN, {"ok": False, "error": "loopback only"})
    return False


def _read_post_json(handler: BaseHTTPRequestHandler, max_bytes: int = 2048) -> dict[str, Any] | None:
    try:
        length = int(handler.headers.get("Content-Length") or "0")
    except (TypeError, ValueError):
        length = 0
    if length < 0 or length > max_bytes:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid body size"})
        return None
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid json"})
        return None
    if not isinstance(parsed, dict):
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "expected object"})
        return None
    return parsed


def _supervisor_pid_from_lock() -> int | None:
    """Return the PID inside the always-on lock if the process is alive, else None."""
    if not SUPERVISOR_LOCK_PATH.exists():
        return None
    try:
        data = json.loads(SUPERVISOR_LOCK_PATH.read_text(encoding="utf-8-sig", errors="replace"))
    except (OSError, json.JSONDecodeError):
        return None
    pid = data.get("pid") if isinstance(data, dict) else None
    if not isinstance(pid, int):
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            return None
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return pid
        except OSError:
            return None
    # Windows: use a Win32 API call (OpenProcess) instead of shelling out
    # to tasklist.exe. tasklist.exe is a console app that ALWAYS allocates
    # a conhost.exe, and on Windows 11 conhost flashes briefly even with
    # CREATE_NO_WINDOW. The OpenProcess approach is in-process — no
    # subprocess, no conhost, no flash. This is THE flicker fix the user
    # asked about; see memory/warp_fix_playbook.txt for the full history.
    try:
        import ctypes
        # PROCESS_QUERY_LIMITED_INFORMATION = 0x1000 (cheapest access right
        # that confirms the PID is alive without needing privileges).
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return None
        # Check exit code: STILL_ACTIVE (259) means running.
        exit_code = ctypes.c_ulong(0)
        ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
        kernel32.CloseHandle(handle)
        if not ok:
            return None
        if exit_code.value == 259:  # STILL_ACTIVE
            return pid
        return None
    except (OSError, AttributeError):
        return None


def _hidden_popen_kwargs() -> dict[str, Any]:
    """Return Popen kwargs that fully hide a Windows powershell child.
    Uses CREATE_NO_WINDOW (no console host) + STARTUPINFO with SW_HIDE
    (belt-and-suspenders: even if a console is briefly allocated,
    the window stays hidden). DETACHED_PROCESS is intentionally NOT
    combined with CREATE_NO_WINDOW because the two flags conflict
    on some Windows builds and crash the child immediately."""
    kwargs: dict[str, Any] = {"close_fds": True}
    if os.name != "nt":
        return kwargs
    kwargs["creationflags"] = 0x08000000  # CREATE_NO_WINDOW
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = 0  # SW_HIDE
    kwargs["startupinfo"] = si
    return kwargs


_HIDDEN_LAUNCHER = PROJECT_ROOT / "Luna_Hidden_Launcher.vbs"


def _launch_via_wscript(script_path: Path, args: list[str]) -> tuple[int | None, str | None]:
    """Launch a .ps1 invisibly via wscript.exe + VBS shim. Most reliable
    Windows hide pattern: WScript.Shell.Run cmd, 0, False never flashes
    a conhost.exe (unlike subprocess.Popen with CREATE_NO_WINDOW which
    still flashes briefly on some Windows builds). Returns the wscript
    pid; the wscript exits immediately after spawning the powershell
    child, so this pid is short-lived. The actual supervisor pid lives
    in the always-on lock file."""
    if os.name != "nt" or not _HIDDEN_LAUNCHER.exists():
        # Fallback: direct subprocess.Popen with the hide flags. Still
        # safer than nothing on non-Windows or if the VBS got removed.
        cmd = [
            "powershell", "-NoProfile", "-NonInteractive",
            "-WindowStyle", "Hidden", "-ExecutionPolicy", "Bypass",
            "-File", str(script_path),
        ] + args
        try:
            proc = subprocess.Popen(
                cmd, cwd=str(PROJECT_ROOT),
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, **_hidden_popen_kwargs(),
            )
            return proc.pid, None
        except OSError as exc:
            return None, f"start failed: {exc.__class__.__name__}"
    cmd = ["wscript.exe", str(_HIDDEN_LAUNCHER), str(script_path)] + args
    try:
        proc = subprocess.Popen(
            cmd, cwd=str(PROJECT_ROOT),
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, **_hidden_popen_kwargs(),
        )
        return proc.pid, None
    except OSError as exc:
        return None, f"start failed: {exc.__class__.__name__}"


def _start_supervisor(args: list[str]) -> tuple[int | None, str | None]:
    supervisor = PROJECT_ROOT / "Luna_AlwaysOn_Supervisor.ps1"
    if not supervisor.exists():
        return None, "supervisor script missing"
    return _launch_via_wscript(supervisor, args)


def _supervisor_run_once_handle(handler: BaseHTTPRequestHandler) -> None:
    if not _check_loopback(handler):
        return
    data = _read_post_json(handler)
    if data is None:
        return
    if _supervisor_pid_from_lock() is not None:
        _send_json(handler, HTTPStatus.CONFLICT, {
            "ok": False, "error": "supervisor already running",
            "running_pid": _supervisor_pid_from_lock(),
            "message": "another supervisor cycle holds the lock; wait for it to finish",
        })
        return
    sprint = bool(data.get("sprint"))
    args = ["-RunOnce", "-EnableTier2IfGatePasses", "-CleanStaleDashboard"]
    if sprint:
        args.insert(0, "-SprintMode")
    else:
        args.extend(["-TierMax", "2", "-MaxTasksPerCycle", "5", "-MaxMinutesPerCycle", "45"])
    pid, err = _start_supervisor(args)
    if err is not None:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": err})
        return
    _send_json(handler, HTTPStatus.ACCEPTED, {
        "ok": True, "started": True, "pid": pid,
        "sprint_mode": sprint, "loop_mode": False,
        "message": "bounded supervisor cycle started",
    })


def _supervisor_start_sprint_handle(handler: BaseHTTPRequestHandler) -> None:
    if not _check_loopback(handler):
        return
    if _read_post_json(handler) is None:
        return
    if _supervisor_pid_from_lock() is not None:
        _send_json(handler, HTTPStatus.CONFLICT, {
            "ok": False, "error": "supervisor already running",
            "running_pid": _supervisor_pid_from_lock(),
            "message": "supervisor lock is held; stop the current run before starting Sprint Mode",
        })
        return
    # Clear any prior stop flag so a fresh sprint isn't immediately killed.
    try:
        if SPRINT_STOP_FLAG_PATH.exists():
            SPRINT_STOP_FLAG_PATH.unlink()
    except OSError:
        pass
    args = ["-SupervisorLoop", "-SprintMode", "-EnableTier2IfGatePasses", "-CleanStaleDashboard"]
    pid, err = _start_supervisor(args)
    if err is not None:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": err})
        return
    _send_json(handler, HTTPStatus.ACCEPTED, {
        "ok": True, "started": True, "pid": pid,
        "sprint_mode": True, "loop_mode": True,
        "message": "Sprint Mode supervisor loop started (1-min cycles, 24h-bounded)",
    })


def _supervisor_stop_sprint_handle(handler: BaseHTTPRequestHandler) -> None:
    """Soft-stop: drop a stop flag the supervisor checks between cycles.
    The current cycle is allowed to finish; the loop exits cleanly afterward.
    Never kills worker.py or any other process."""
    if not _check_loopback(handler):
        return
    if _read_post_json(handler) is None:
        return
    try:
        SPRINT_STOP_FLAG_PATH.parent.mkdir(parents=True, exist_ok=True)
        SPRINT_STOP_FLAG_PATH.write_text(_now_iso(), encoding="utf-8")
    except OSError as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": f"flag write failed: {exc.__class__.__name__}"})
        return
    _send_json(handler, HTTPStatus.OK, {
        "ok": True, "stopped": True,
        "flag_path": str(SPRINT_STOP_FLAG_PATH.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "message": "stop flag set; supervisor will exit after the current cycle",
    })


def _supervisor_status_payload() -> dict[str, Any]:
    pid = _supervisor_pid_from_lock()
    hb = _safe_read_json(SUPERVISOR_HEARTBEAT_PATH) or {}
    sprint = bool(hb.get("sprint_mode"))
    last_cycle_id = hb.get("cycle_id") or None
    last_cycle_path = None
    if last_cycle_id:
        candidate = (MEMORY_DIR / "always_on" / f"luna_always_on_cycle_{last_cycle_id}.json")
        if candidate.exists():
            last_cycle_path = str(candidate.relative_to(PROJECT_ROOT)).replace("\\", "/")
    install_present = SUPERVISOR_HEARTBEAT_PATH.exists() or SUPERVISOR_LOCK_PATH.parent.exists()
    return {
        "ok": True,
        "generated_at": _now_iso(),
        "installed": bool(install_present),
        "running": pid is not None,
        "running_pid": pid,
        "is_sprint_mode": sprint,
        "last_cycle_id": last_cycle_id,
        "last_cycle_state": hb.get("state"),
        "last_cycle_verdict": hb.get("verdict"),
        "next_cycle_at": hb.get("next_cycle_at"),
        "next_cycle_in_seconds": hb.get("next_cycle_in_seconds"),
        "cycles_completed": hb.get("cycles_completed"),
        "stop_flag_present": SPRINT_STOP_FLAG_PATH.exists(),
        "last_report_path": last_cycle_path,
        "lock_path": str(SUPERVISOR_LOCK_PATH.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }


def _supervisor_status_handle(handler: BaseHTTPRequestHandler) -> None:
    payload = _supervisor_status_payload()
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.end_headers()
    handler.wfile.write(body)


# ---------------------------------------------------------------------------
# Kill-switch endpoint (POST). Local-only, requires explicit confirm phrase
# in the body. Calls Luna_Live_KillSwitch.ps1 — which is the only authority
# allowed to flip the runtime state. The server must NEVER fire it without
# an explicit, exact confirmation token.
# ---------------------------------------------------------------------------
KILL_SWITCH_CONFIRM_PHRASE = "RUN KILL SWITCH"


def _kill_switch_handle(handler: BaseHTTPRequestHandler) -> None:
    if not _check_loopback(handler):
        return
    data = _read_post_json(handler)
    if data is None:
        return
    if str(data.get("action") or "") != "RUN_KILL_SWITCH":
        _send_json(handler, HTTPStatus.BAD_REQUEST,
                   {"ok": False, "error": "missing action=RUN_KILL_SWITCH"})
        return
    confirm = str(data.get("confirm") or "").strip().upper()
    if confirm != KILL_SWITCH_CONFIRM_PHRASE:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {
            "ok": False,
            "error": f'confirm phrase must be exactly "{KILL_SWITCH_CONFIRM_PHRASE}"',
        })
        return
    script = PROJECT_ROOT / "Luna_Live_KillSwitch.ps1"
    if not script.exists():
        _send_json(handler, HTTPStatus.NOT_FOUND, {"ok": False, "error": "kill-switch script missing"})
        return
    pid, err = _launch_via_wscript(script, [])
    if err is not None:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": err})
        return
    _send_json(handler, HTTPStatus.ACCEPTED, {
        "ok": True, "started": True, "pid": pid,
        "message": "kill-switch invoked; system reverting to advisory state",
    })


# ---------------------------------------------------------------------------
# Voice — /api/voice/* endpoints (loopback-only).
#
# All endpoints route through luna_modules.luna_voice. The engine itself
# already sanitises text (no secrets, no big code blocks, max chars).
# These handlers add nothing to that pipeline; they ONLY toggle config /
# read status / enqueue a short test line. The browser dashboard never
# sees the api key value of any premium provider.
# ---------------------------------------------------------------------------
try:
    from luna_modules import luna_voice as _luna_voice_mod  # type: ignore
    _LUNA_VOICE_MOD_OK = True
except Exception:  # noqa: BLE001
    _luna_voice_mod = None  # type: ignore[assignment]
    _LUNA_VOICE_MOD_OK = False


def _voice_status_handle(handler: BaseHTTPRequestHandler) -> None:
    if not _check_loopback(handler):
        return
    if not _LUNA_VOICE_MOD_OK or _luna_voice_mod is None:
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE,
                   {"ok": False, "error": "luna_voice module unavailable"})
        return
    try:
        snap = _luna_voice_mod.voice_status_for_dashboard(root=str(PROJECT_ROOT))
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": f"voice status error: {type(exc).__name__}"})
        return
    _send_json(handler, HTTPStatus.OK, {"ok": True, "status": snap})


def _voice_toggle_handle(handler: BaseHTTPRequestHandler) -> None:
    """POST {"on": true|false}. Persists voice_enabled in the config."""
    if not _check_loopback(handler):
        return
    if not _LUNA_VOICE_MOD_OK or _luna_voice_mod is None:
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE,
                   {"ok": False, "error": "luna_voice module unavailable"})
        return
    data = _read_post_json(handler)
    if data is None:
        return
    on = bool(data.get("on"))
    try:
        eng = _luna_voice_mod.get_engine(root=str(PROJECT_ROOT))
        eng.set_enabled(on)
        snap = _luna_voice_mod.voice_status_for_dashboard(root=str(PROJECT_ROOT))
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": f"toggle error: {type(exc).__name__}"})
        return
    _send_json(handler, HTTPStatus.OK,
               {"ok": True, "voice_enabled": on, "status": snap})


def _voice_stop_handle(handler: BaseHTTPRequestHandler) -> None:
    if not _check_loopback(handler):
        return
    if not _LUNA_VOICE_MOD_OK or _luna_voice_mod is None:
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE,
                   {"ok": False, "error": "luna_voice module unavailable"})
        return
    try:
        eng = _luna_voice_mod.get_engine(root=str(PROJECT_ROOT))
        eng.stop()
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": f"stop error: {type(exc).__name__}"})
        return
    _send_json(handler, HTTPStatus.OK, {"ok": True, "stopped": True})


# Default test line baked in. Operator may override via POST body.
_VOICE_DEFAULT_TEST_LINE = (
    "Hey Serge. Luna is online. I'm here, I'm focused, and I'm ready to move."
)


def _voice_test_handle(handler: BaseHTTPRequestHandler) -> None:
    if not _check_loopback(handler):
        return
    if not _LUNA_VOICE_MOD_OK or _luna_voice_mod is None:
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE,
                   {"ok": False, "error": "luna_voice module unavailable"})
        return
    data = _read_post_json(handler)
    if data is None:
        return
    raw_text = (data.get("text") or "").strip()
    text = raw_text or _VOICE_DEFAULT_TEST_LINE
    # Belt-and-braces: trim before queuing (the engine's sanitiser also
    # truncates long input; this is just a defense in depth).
    if len(text) > 600:
        text = text[:600]
    # Program D — route through Luna-owned cognitive_voice_runtime FIRST.
    # If the new runtime has at least one backend available, it owns the
    # speech. Falls back to the legacy engine ONLY if the cognitive runtime
    # has no working backends (or its module fails to import). This makes
    # every "/api/voice/test" call observable in cognitive_voice_audit.jsonl
    # and routes Luna's voice through her own runtime by default.
    used_cognitive_runtime = False
    cognitive_result_payload: dict = {}
    try:
        from luna_modules import cognitive_voice_runtime as _cvr  # type: ignore
        cvr_runtime = _cvr.get_runtime()
        if cvr_runtime.primary_backend() != "none":
            result = cvr_runtime.speak(text, caller="/api/voice/test")
            used_cognitive_runtime = True
            cognitive_result_payload = {
                "backend": result.backend,
                "voice_identity": result.voice_identity,
                "latency_ms": result.latency_ms,
                "ok": bool(result.ok),
                "wav_path": result.wav_path,
                "utt_id": result.utt_id,
            }
            ok = bool(result.ok)
    except Exception:
        used_cognitive_runtime = False
    if not used_cognitive_runtime:
        try:
            eng = _luna_voice_mod.get_engine(root=str(PROJECT_ROOT))
            ok = eng.speak_async(text, category="system")
        except Exception as exc:  # noqa: BLE001
            _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                       {"ok": False, "error": f"test error: {type(exc).__name__}"})
            return
    _send_json(handler, HTTPStatus.OK,
               {"ok": True, "queued": bool(ok), "text_preview": text[:80],
                "owner": "cognitive_voice_runtime" if used_cognitive_runtime else "legacy_engine",
                "cognitive_result": cognitive_result_payload if used_cognitive_runtime else None})


_DECISION_VERDICTS_LOG = LOGS_DIR / "luna_decision_verdicts.jsonl"
_VALID_DECISION_ACTIONS = {"approve", "wait", "do_not_approve"}


def _decision_verdict_handle(handler: BaseHTTPRequestHandler) -> None:
    """Operator's verdict on a council-recommended decision card.

    Body: {"decision_id": "<id|goal|path>", "action": "approve"|"wait"|"do_not_approve",
           "note": "<optional free-text>"}

    Appends a JSONL record to logs/luna_decision_verdicts.jsonl. Read-only
    record — never mutates worker.py / runtime_state / kill switch /
    Tier 4 floor. Safe + reversible. Loopback only.
    """
    if not _check_loopback(handler):
        return
    data = _read_post_json(handler)
    if data is None:
        return
    decision_id = str(data.get("decision_id") or "").strip()[:200]
    action = str(data.get("action") or "").strip().lower()
    note = str(data.get("note") or "").strip()[:600]
    if not decision_id:
        _send_json(handler, HTTPStatus.BAD_REQUEST,
                   {"ok": False, "error": "missing decision_id"})
        return
    if action not in _VALID_DECISION_ACTIONS:
        _send_json(handler, HTTPStatus.BAD_REQUEST,
                   {"ok": False, "error": f"invalid action; allowed: {sorted(_VALID_DECISION_ACTIONS)}"})
        return
    record = {
        "ts":          _now_iso(),
        "decision_id": decision_id,
        "action":      action,
        "note":        note,
        "actor":       "Serge",
        "source":      "dashboard",
    }
    try:
        _DECISION_VERDICTS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with _DECISION_VERDICTS_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": f"write failed: {exc.__class__.__name__}"})
        return
    _send_json(handler, HTTPStatus.OK, {"ok": True, "saved": record})


_VOICE_CONVERSE_MAX_BYTES = 5 * 1024 * 1024  # 5 MB per utterance is plenty


def _voice_converse_handle(handler: BaseHTTPRequestHandler) -> None:
    """Realtime voice conversation endpoint.

    Receives a single audio blob (multipart/form-data with `audio`
    field, OR raw audio body when Content-Type is audio/*) and returns:
      { ok, transcript, reply_text, audio_b64, audio_mime, model_used,
        stt_s, llm_s, total_s }

    Pipeline: faster-whisper (STT) -> Ollama (Luna brain) ->
    luna_voice_engine (Kokoro/edge-tts/pyttsx3) for TTS. Never speaks
    or returns secrets — both transcript and reply are scrubbed
    against the voice engine's secret pattern set.
    """
    if not _check_loopback(handler):
        return
    try:
        from luna_modules import luna_realtime_voice as _lrv  # type: ignore
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE,
                   {"ok": False, "error": f"realtime voice unavailable: {type(exc).__name__}"})
        return
    try:
        length = int(handler.headers.get("Content-Length") or "0")
    except (TypeError, ValueError):
        length = 0
    if length <= 0 or length > _VOICE_CONVERSE_MAX_BYTES:
        _send_json(handler, HTTPStatus.BAD_REQUEST,
                   {"ok": False, "error": "invalid audio body size"})
        return

    raw = handler.rfile.read(length)
    content_type = handler.headers.get("Content-Type") or ""
    session_id = handler.headers.get("X-Luna-Session") or "default"
    model_override = handler.headers.get("X-Luna-Model") or None

    # Parse multipart/form-data OR treat the body as a raw audio blob.
    audio_bytes: bytes = b""
    audio_mime: str = "audio/webm"
    if content_type.startswith("multipart/form-data"):
        try:
            audio_bytes, audio_mime = _parse_multipart_audio(raw, content_type)
        except Exception as exc:  # noqa: BLE001
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": f"multipart parse failed: {type(exc).__name__}"})
            return
    elif content_type.startswith("audio/"):
        audio_bytes = raw
        audio_mime = content_type.split(";", 1)[0].strip()
    else:
        # Last resort: treat the body as raw bytes; assume webm (browser
        # MediaRecorder default).
        audio_bytes = raw
        audio_mime = "audio/webm"

    if not audio_bytes or len(audio_bytes) < 200:
        _send_json(handler, HTTPStatus.BAD_REQUEST,
                   {"ok": False, "error": "audio blob too small"})
        return

    try:
        result = _lrv.converse(audio_bytes, audio_mime,
                               session_id=session_id, model=model_override)
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": f"converse error: {type(exc).__name__}"})
        return
    _send_json(handler, HTTPStatus.OK, result)


def _voice_converse_stream_handle(handler: BaseHTTPRequestHandler) -> None:
    """V2 streaming voice endpoint (round 21, 2026-05-09 per Serge).

    Same input as /api/voice/converse, but the response is a
    Server-Sent Events stream so the browser can start playing each
    sentence's audio the instant Luna's TTS produces it - while the
    LLM is still generating later sentences. Net perceived latency
    drops from 5-13 s to ~2-4 s on typical replies.

    Wire format: text/event-stream, each event is a single
    `data: <json>\\n\\n` line with one of:
        {"type": "transcript", "text": "..."}
        {"type": "token", "content": "..."}
        {"type": "sentence_audio", "text": "...", "audio_b64": "...", "mime": "audio/wav"}
        {"type": "done", "full_reply": "...", "total_latency_s": <float>, ...}
        {"type": "error", "stage": "...", "error": "..."}

    Backward-compat: existing /api/voice/converse still works
    unchanged. Browsers can opt in to v2 by hitting this endpoint.
    """
    if not _check_loopback(handler):
        return
    try:
        from luna_modules import luna_realtime_voice as _lrv  # type: ignore
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE,
                   {"ok": False, "error": f"realtime voice v2 unavailable: {type(exc).__name__}"})
        return
    if not hasattr(_lrv, "converse_stream_sse"):
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE,
                   {"ok": False, "error": "luna_realtime_voice missing converse_stream_sse"})
        return

    try:
        length = int(handler.headers.get("Content-Length") or "0")
    except (TypeError, ValueError):
        length = 0
    if length <= 0 or length > _VOICE_CONVERSE_MAX_BYTES:
        _send_json(handler, HTTPStatus.BAD_REQUEST,
                   {"ok": False, "error": "invalid audio body size"})
        return

    raw = handler.rfile.read(length)
    content_type = handler.headers.get("Content-Type") or ""
    session_id = handler.headers.get("X-Luna-Session") or "default"
    model_override = handler.headers.get("X-Luna-Model") or None

    audio_bytes: bytes = b""
    audio_mime: str = "audio/webm"
    if content_type.startswith("multipart/form-data"):
        try:
            audio_bytes, audio_mime = _parse_multipart_audio(raw, content_type)
        except Exception as exc:  # noqa: BLE001
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": f"multipart parse failed: {type(exc).__name__}"})
            return
    elif content_type.startswith("audio/"):
        audio_bytes = raw
        audio_mime = content_type.split(";", 1)[0].strip()
    else:
        audio_bytes = raw
        audio_mime = "audio/webm"

    if not audio_bytes or len(audio_bytes) < 200:
        _send_json(handler, HTTPStatus.BAD_REQUEST,
                   {"ok": False, "error": "audio blob too small"})
        return

    # Send SSE headers. We must NOT use _send_json (which sets
    # Content-Length and JSON content-type). Stream raw chunks.
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
    handler.send_header("Cache-Control", "no-cache, no-transform")
    handler.send_header("Connection", "keep-alive")
    handler.send_header("X-Accel-Buffering", "no")  # disable proxy buffering
    handler.end_headers()
    try:
        for chunk_bytes in _lrv.converse_stream_sse(
                audio_bytes, audio_mime,
                session_id=session_id, model=model_override):
            try:
                handler.wfile.write(chunk_bytes)
                handler.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                # Client closed the stream early (e.g., user cancelled).
                # That's fine, just stop emitting.
                return
    except Exception as exc:  # noqa: BLE001
        # Try to send a final error event before bailing.
        try:
            err_line = f'data: {{"type":"error","stage":"server","error":"{type(exc).__name__}: {exc}"}}\n\n'
            handler.wfile.write(err_line.encode("utf-8"))
            handler.wfile.flush()
        except Exception:
            pass


def _parse_multipart_audio(raw: bytes, content_type: str) -> tuple[bytes, str]:
    """Minimal multipart/form-data parser scoped to the audio field.
    Returns (audio_bytes, audio_mime). Raises on malformed input."""
    # Extract boundary.
    import email
    msg = email.message_from_bytes(
        b"Content-Type: " + content_type.encode("ascii", "ignore") + b"\r\n\r\n" + raw
    )
    if not msg.is_multipart():
        raise ValueError("not multipart")
    for part in msg.walk():
        cd = part.get("Content-Disposition", "") or ""
        if "name=\"audio\"" in cd or "name='audio'" in cd or "name=audio" in cd:
            payload = part.get_payload(decode=True)
            if payload:
                return bytes(payload), part.get_content_type() or "audio/webm"
    raise ValueError("audio part missing")


def _voice_preset_handle(handler: BaseHTTPRequestHandler) -> None:
    """POST {"preset": "luna_ara_like_energy" | "luna_sovereign" | ...}."""
    if not _check_loopback(handler):
        return
    if not _LUNA_VOICE_MOD_OK or _luna_voice_mod is None:
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE,
                   {"ok": False, "error": "luna_voice module unavailable"})
        return
    data = _read_post_json(handler)
    if data is None:
        return
    name = str(data.get("preset") or "").strip()
    if not name:
        _send_json(handler, HTTPStatus.BAD_REQUEST,
                   {"ok": False, "error": "missing preset"})
        return
    try:
        eng = _luna_voice_mod.get_engine(root=str(PROJECT_ROOT))
        snap = eng.set_voice_preset(name)
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR,
                   {"ok": False, "error": f"preset error: {type(exc).__name__}"})
        return
    _send_json(handler, HTTPStatus.OK, {"ok": True, "status": snap})


# ---------------------------------------------------------------------------
# Mission Control — live "who is doing what right now"
# ---------------------------------------------------------------------------
SOURCE_TO_ACTOR: dict[str, str] = {
    "luna_architect":          "ARCHITECT",
    "aider_bridge":            "AIDER",
    "luna_qa_verifier":        "VERIFICATION",
    "luna_guardian":           "GUARDIAN",
    "luna_mission_engine":     "MISSION ENGINE",
    "continues_update":        "WORKER",
    "cu_self_fix":             "SELF-FIX",
    "luna_failure_doctor":     "FAILURE DOCTOR",
    "luna_queue_governor":     "QUEUE GOVERNOR",
    "luna_self_knowledge":     "SELF-KNOWLEDGE",
    "luna_toolchain":          "TOOLCHAIN",
    "luna_tool_registry":      "TOOL REGISTRY",
    "codex":                   "CODEX",
    "director_agent":          "DIRECTOR",
    "luna_director":           "DIRECTOR",
}

GUARDIAN_STATUS_PATH = MEMORY_DIR / "luna_guardian_status.json"


def _parse_iso_local(ts: str | None):
    """Tolerant ISO-8601 parser. Returns a NAIVE LOCAL datetime so that
    subtraction with ``datetime.now()`` produces the right elapsed age.
    Handles 'Z' suffix, '+HH:MM' offsets, and fractional seconds without
    clobbering the timezone marker."""
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None
    # Normalize 'Z' -> '+00:00' so fromisoformat can parse it on all 3.x.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Strip fractional seconds without removing a trailing tz offset.
    # The regex matches a dot followed by digits, then either a tz offset
    # ('+HH:MM' / '-HH:MM') or end-of-string.
    s = re.sub(r"\.(\d+)(?=([+-]\d{2}:?\d{2}$|$))", "", s)
    try:
        t = datetime.fromisoformat(s)
    except Exception:
        return None
    if t.tzinfo is not None:
        try:
            t = t.astimezone().replace(tzinfo=None)
        except Exception:
            t = t.replace(tzinfo=None)
    return t


def _live_feed_event_is_recent(rec: dict[str, Any], now_dt: datetime, max_age_seconds: int = 120) -> bool:
    """Live-feed records only carry HH:MM:SS - no date. Consider them
    recent only when the time-of-day is within ``max_age_seconds`` of
    ``now``'s time-of-day on the same calendar wall clock."""
    ts = str(rec.get("ts", "")).strip()
    m = re.match(r"^(\d{1,2}):(\d{2}):(\d{2})$", ts)
    if not m:
        return False
    try:
        rec_seconds = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
    except Exception:
        return False
    now_seconds = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
    diff = now_seconds - rec_seconds
    if diff < 0:
        # Either the event is from "the future" (clock skew) or it
        # crossed midnight - both edge cases. Be conservative: not recent.
        return False
    return diff <= max_age_seconds


def _stage_from_event(event: str) -> str:
    if not event:
        return "thinking"
    if event.startswith("ARCH_"):
        return "Architecting · " + event
    if event.startswith("APPLY"):
        return "Applying patch"
    if event.startswith("BRIDGE_"):
        return event.replace("_", " ").title()
    if event.startswith("CU_"):
        return event.replace("_", " ").title()
    if event in {"CONTEXT_OVERFLOW"}:
        return "Context overflow"
    if event in {"CLAIM"}:
        return "Claiming task"
    if event in {"QUARANTINED"}:
        return "Quarantined"
    return event.replace("_", " ").title()


def _build_mission_control_payload_raw() -> dict[str, Any]:
    """Live actor/stage/task surface for the dashboard Mission Control.

    Truth order:
      1. memory/luna_current_activity.json (when fresh, < 5 min) — written
         by Luna_LiveEvent_Write.ps1 from supervisor / cycle / self-upgrade
         scripts. This is the single source of truth for "what is Luna
         doing right now". When present and fresh, it wins.
      2. Fallback: heartbeats + live feed inference, as before.

    Even when (1) wins, the payload still surfaces idle_reason /
    blocked_reason so the dashboard can never show ambiguous IDLE.
    """
    now_dt = datetime.now()

    # Bounded reads: cap per-source at MISSION_CONTROL_READ_TIMEOUT_MS and
    # total handler at MISSION_CONTROL_TOTAL_BUDGET_MS. If a writer is mid
    # atomic-replace on any source the dashboard returns a partial payload
    # with ``read_budget.stale_sources`` populated instead of hanging.
    _read_budget = _ReadBudget()
    worker   = _bounded_read_json("worker_heartbeat",     READONLY_SOURCES["worker_heartbeat"],     _read_budget) or {}
    aider    = _bounded_read_json("aider_bridge_status",  READONLY_SOURCES["aider_bridge_status"],  _read_budget) or {}
    guardian = _bounded_read_json("guardian_status",      GUARDIAN_STATUS_PATH,                     _read_budget) or {}
    ao_hb    = _bounded_read_json("always_on_heartbeat",  READONLY_SOURCES["always_on_heartbeat"],  _read_budget) or {}
    gate     = _bounded_read_json("evidence_gate",        READONLY_SOURCES["evidence_gate"],        _read_budget) or {}
    activity = _bounded_read_json("current_activity",     READONLY_SOURCES["current_activity"],     _read_budget) or {}

    # Activity file age (used to decide whether it can override heartbeat-
    # based inference, and whether the dashboard should show "stale").
    activity_age_s: float | None = None
    activity_updated = activity.get("updated_at") if isinstance(activity, dict) else None
    t_act = _parse_iso_local(activity_updated)
    if t_act is not None:
        activity_age_s = max(0.0, (now_dt - t_act).total_seconds())

    feed_records = _bounded_tail_jsonl(
        "live_feed",
        READONLY_SOURCES["live_feed"],
        _read_budget,
        max_records=80,
    )
    latest_feed = feed_records[-1] if feed_records else None
    prev_feed   = feed_records[-2] if len(feed_records) >= 2 else None

    # Pick the freshest ISO timestamp we can find.
    iso_candidates = [
        ("worker_heartbeat",     worker.get("ts")),
        ("aider_bridge",         aider.get("last_event_at")),
        ("guardian_status",      guardian.get("ts")),
        ("always_on_heartbeat",  ao_hb.get("ts")),
        ("evidence_gate",        gate.get("last_updated")),
    ]
    last_iso = None
    last_age_s: float | None = None
    last_iso_source = None
    for src_name, ts in iso_candidates:
        t = _parse_iso_local(ts)
        if t is None:
            continue
        age = (now_dt - t).total_seconds()
        if age < 0:
            age = 0
        if last_age_s is None or age < last_age_s:
            last_age_s = age
            last_iso = ts
            last_iso_source = src_name

    # Determine current actor / stage in priority order.
    actor = "IDLE"
    stage = "Waiting for next work order"
    title = ""
    status = ""
    started: str | None = None
    elapsed: int | None = None
    is_active = False

    aider_state = str(aider.get("state", "")).lower()
    aider_running = aider_state in {"running", "claim", "applying", "active"}

    worker_state = str(worker.get("state", "")).lower()
    worker_phase = str(worker.get("phase", "") or "")
    worker_alive = bool(worker.get("alive"))

    ao_state = str(ao_hb.get("state", "")).lower()

    if aider_running:
        actor = "AIDER"
        a_stage = str(aider.get("stage") or "running").upper()
        stage  = a_stage
        title  = str(aider.get("target") or aider.get("task_id") or "")
        status = str(aider.get("detail") or aider.get("state") or "running")
        started = aider.get("started_at") or None
        try:
            elapsed = int(aider.get("elapsed_seconds") or 0) or None
        except Exception:
            elapsed = None
        is_active = True
    elif ao_state == "cycle_starting":
        actor = "SUPERVISOR"
        stage = "Always-On cycle"
        title = "luna_always_on_supervisor"
        status = "applying tier-bounded patches"
        started = ao_hb.get("ts") or None
        is_active = True
    elif worker_state == "running" and worker_alive:
        # Try to interpret the phase: "mission:<idx>:<step>:<mode>",
        # "task", "self_fix", "self-improvement", etc.
        phase_lower = worker_phase.lower()
        if "verify" in phase_lower:
            actor = "VERIFICATION"
            stage = "Verifying"
        elif "self_fix" in phase_lower or "self-fix" in phase_lower:
            actor = "SELF-FIX"
            stage = "Self-Fix Mode"
        elif "mission" in phase_lower:
            actor = "MISSION ENGINE"
            stage = "Mission · " + worker_phase
        elif "plan" in phase_lower:
            actor = "ARCHITECT"
            stage = "Planning"
        elif "task" in phase_lower:
            actor = "WORKER"
            stage = "Executing task"
        else:
            actor = "WORKER"
            stage = worker_phase or "running"
        title = str(worker.get("task_id") or "")
        status = str(worker.get("last_message") or "")
        started = worker.get("started_at") or None
        is_active = True
    elif worker_state == "verifying":
        actor = "VERIFICATION"
        stage = "Verifying " + (worker.get("task_id") or "").rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
        title = str(worker.get("task_id") or "")
        status = str(worker.get("last_message") or "")
        started = worker.get("started_at") or None
        is_active = True
    elif latest_feed and _live_feed_event_is_recent(latest_feed, now_dt, max_age_seconds=120):
        # Live feed events only have HH:MM:SS (no date), so we only treat
        # them as "currently active" when their time-of-day is within the
        # last couple minutes of now's time-of-day. Older events shouldn't
        # cause Mission Control to lie that "X is working" hours later.
        src = str(latest_feed.get("source") or "")
        ev  = str(latest_feed.get("event") or "")
        actor = SOURCE_TO_ACTOR.get(src, "ACTIVE")
        stage = _stage_from_event(ev)
        title = str(latest_feed.get("task_id") or latest_feed.get("msg") or "")[:120]
        status = str(latest_feed.get("msg") or "")
        is_active = True
    elif ao_state == "cycle_complete":
        actor = "IDLE"
        stage = "Last cycle: " + str(ao_hb.get("verdict") or "OK")
        title = "always-on supervisor (waiting for next cycle)"
        status = (
            "succeeded " + str(ao_hb.get("succeeded", 0)) +
            " of " + str(ao_hb.get("attempted", 0))
        )
        started = ao_hb.get("ts") or None
        is_active = False

    # Compute elapsed when we have a started_at and not already set.
    # NOTE: worker.started_at is process-start, not per-task — only use it
    # when worker is actively running, otherwise it is misleading.
    if elapsed is None and started and is_active:
        t = _parse_iso_local(started)
        if t is not None:
            try:
                elapsed = max(0, int((now_dt - t).total_seconds()))
            except Exception:
                elapsed = None

    # Stale: actively claimed but heartbeat is old, OR one of the bounded
    # reads above hit a writer-held lock and was returned as stale. Either
    # condition surfaces explicitly so the panel never hangs.
    is_stale = False
    stale_reason: str | None = None
    if is_active and last_age_s is not None and last_age_s > 10:
        is_stale = True
        stale_reason = f"no heartbeat for {int(last_age_s)}s ({last_iso_source or 'unknown source'})"
    if _read_budget.stale_sources:
        is_stale = True
        _src_summary = ",".join(sorted(_read_budget.stale_sources))
        _lt_reason = f"read_lock_timeout: {_src_summary}"
        stale_reason = (stale_reason + " | " + _lt_reason) if stale_reason else _lt_reason

    # Handoff inference from live_feed source order.
    last_handoff_from: str | None = None
    last_handoff_to: str | None = None
    if prev_feed and latest_feed:
        prev_actor = SOURCE_TO_ACTOR.get(str(prev_feed.get("source") or ""))
        cur_actor  = SOURCE_TO_ACTOR.get(str(latest_feed.get("source") or ""))
        if prev_actor and cur_actor and prev_actor != cur_actor:
            last_handoff_from = prev_actor
            last_handoff_to   = cur_actor

    # last_result from the most recent self-patch attempt.
    last_result = None
    attempts_tail = _bounded_tail_jsonl(
        "self_patch_attempts",
        READONLY_SOURCES["self_patch_attempts"],
        _read_budget,
        max_records=1,
    )
    if attempts_tail:
        rec = attempts_tail[-1]
        last_result = {
            "result": rec.get("result"),
            "target": rec.get("target"),
            "tier":   rec.get("tier"),
            "ts":     rec.get("ts"),
        }

    # progress_percent: only meaningful when actively grinding toward Tier 2
    # promotion. Once approved, we stop reporting it (avoid 100%-forever).
    progress_percent: float | None = None
    if isinstance(gate, dict) and not bool(gate.get("tier2_approved")):
        rules = gate.get("promotion_rules", {}) if isinstance(gate.get("promotion_rules"), dict) else {}
        thr = int(rules.get("tier_2_promotion_threshold_t0t1_successes", 10))
        sumt = int(gate.get("tier0_success_count", 0)) + int(gate.get("tier1_success_count", 0))
        if thr > 0:
            progress_percent = round(min(1.0, sumt / float(thr)) * 100.0, 1)

    # ------------------------------------------------------------------
    # Activity-file override: when memory/luna_current_activity.json is
    # fresh, it is the authoritative source and replaces the heuristic
    # actor/stage above. The heartbeat / live-feed inference still
    # populates source_summary so the dashboard can show context.
    # ------------------------------------------------------------------
    is_idle = False
    is_blocked = False
    is_complete = False
    idle_reason: str | None = None
    blocked_reason: str | None = None
    next_action: str | None = None
    activity_report_path: str | None = None
    source_used = "heartbeats+livefeed"
    next_cycle_at: str | None = None
    next_cycle_in_seconds: int | None = None
    sprint_mode: bool = False
    cycles_completed: int | None = None

    activity_fresh = (
        isinstance(activity, dict)
        and activity_age_s is not None
        and activity_age_s <= 300.0  # 5 min freshness window
    )
    if activity_fresh:
        source_used = "current_activity.json"
        actor = str(activity.get("current_actor") or actor or "IDLE")
        stage = str(activity.get("current_stage") or stage or "")
        title = str(activity.get("current_task_title") or title or "")
        status_act = str(activity.get("current_status_text") or "")
        if status_act:
            status = status_act
        is_active = bool(activity.get("is_active"))
        is_idle = bool(activity.get("is_idle"))
        is_blocked = bool(activity.get("is_blocked"))
        is_complete = bool(activity.get("is_complete"))
        idle_reason = activity.get("idle_reason") or None
        blocked_reason = activity.get("blocked_reason") or None
        next_action = activity.get("next_action") or None
        activity_report_path = activity.get("report_path") or None
        next_cycle_at = activity.get("next_cycle_at") or None
        # Override progress when the writer set one explicitly.
        try:
            ap = activity.get("progress_percent")
            if ap is not None:
                progress_percent = float(ap)
        except (TypeError, ValueError):
            pass
        # Hand-off and elapsed override.
        if activity.get("last_handoff_from") and activity.get("last_handoff_to"):
            last_handoff_from = activity.get("last_handoff_from")
            last_handoff_to = activity.get("last_handoff_to")
        try:
            es = activity.get("elapsed_seconds")
            if es is not None:
                elapsed = int(es)
        except (TypeError, ValueError):
            pass
        if activity.get("started_at"):
            started = activity.get("started_at")

    # Compute cycle countdown from next_cycle_at (UTC ISO). When the
    # activity file's next_cycle_at is older than the wall clock OR the
    # activity file is stale, fall back to the LIVE Next Run Time from
    # the LunaTierProgressionEngineUser scheduled task (which is the
    # actual cycle dispatcher). This kills the "stuck on yesterday's
    # date" countdown bug after the engine cadence change.
    if next_cycle_at:
        t_next = _parse_iso_local(next_cycle_at)
        if t_next is not None:
            delta = (t_next - now_dt).total_seconds()
            # If the recorded "next" is already in the past by more than
            # 5 minutes, the activity file is stale — discard.
            if delta < -300:
                next_cycle_at = None
                t_next = None
            else:
                next_cycle_in_seconds = max(0, int(delta))
    # When the scheduled task is AtLogOn-only (no PT1M/PT3M Repetition),
    # the supervisor (Luna_Continuous_Supervisor.ps1) is the actual
    # cycle dispatcher and Next Run Time is N/A. In that case derive
    # the countdown from the supervisor's config + log activity.
    # Also: if the most-recent supervisor log line is "cycle #N start"
    # without a matching "ok"/"fail", flip is_active=True so Mission
    # Control shows "Progression cycle running now" instead of WAITING.
    cycle_running_for_seconds: int | None = None
    if not next_cycle_at:
        try:
            sup_snap = _build_continuous_supervisor_snapshot()
            recent = str(sup_snap.get("log_recent_event") or "")
            if " cycle #" in recent and (" start" in recent) and (" ok" not in recent) and (" fail" not in recent):
                # Supervisor is mid-cycle. Don't show a countdown — show
                # "running now" via is_active. Compute elapsed seconds
                # by parsing the cycle-start timestamp in the log line:
                #   "[2026-05-07T15:48:34.0527952-07:00] cycle #1 start"
                is_active = True
                if not stage:
                    stage = "Progression cycle running now"
                try:
                    import re as _re
                    m = _re.match(r"\[([^\]]+)\]", recent)
                    if m:
                        ts = m.group(1)
                        # Strip subsecond + parse with offset.
                        ts2 = _re.sub(r"\.(\d+)", "", ts)
                        from datetime import datetime as _dt
                        _start = _dt.fromisoformat(ts2)
                        if _start.tzinfo is not None:
                            _start_naive = _start.astimezone().replace(tzinfo=None)
                        else:
                            _start_naive = _start
                        elapsed_s = int((now_dt - _start_naive).total_seconds())
                        if elapsed_s >= 0:
                            cycle_running_for_seconds = elapsed_s
                except Exception:
                    pass
            if sup_snap.get("enabled") and sup_snap.get("config_cadence_seconds"):
                cadence = int(sup_snap["config_cadence_seconds"])
                # Estimate seconds until next cycle: cadence minus
                # (time since last log line). When alive=False (long
                # cycle silent), default to cadence and let the JS
                # render "—:—" via is_active=true.
                age = sup_snap.get("log_age_seconds")
                if sup_snap.get("alive") and isinstance(age, int):
                    remaining = max(0, cadence - age)
                    next_cycle_in_seconds = remaining
                    from datetime import timedelta as _td
                    next_cycle_at = (now_dt + _td(seconds=remaining)).strftime(
                        "%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    if not next_cycle_at and "_scheduled_task_status" in globals():
        try:
            for _name in ("LunaTierProgressionEngineUser",
                          "LunaTierProgressionEngine"):
                _t = _scheduled_task_status(_name) or {}
                _nrt = str(_t.get("next_run_time") or "").strip()
                if not _nrt:
                    continue
                # next_run_time from schtasks comes back in local-time
                # format (e.g. "5/7/2026 3:34:00 PM") in en-US locale.
                # Try a few fmts; last-resort: skip silently.
                _parsed = None
                for _fmt in ("%m/%d/%Y %I:%M:%S %p",
                             "%Y-%m-%dT%H:%M:%S",
                             "%Y-%m-%d %H:%M:%S"):
                    try:
                        from datetime import datetime as _dt
                        _parsed = _dt.strptime(_nrt, _fmt)
                        break
                    except (ValueError, TypeError):
                        continue
                if _parsed is None:
                    continue
                # _parsed is a naive LOCAL datetime (schtasks returns
                # local-time strings). now_dt is also naive local. So
                # the seconds delta is a clean local-local subtraction.
                # For the JSON-serialised next_cycle_at we convert to
                # UTC so the JS Date() parser handles it without ambiguity.
                from datetime import timezone as _tz
                # Local naive -> attach the system tz -> convert to UTC.
                try:
                    _local_aware = _parsed.astimezone()
                    _utc_aware   = _local_aware.astimezone(_tz.utc)
                    next_cycle_at = _utc_aware.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    # Fallback: just emit the local-naive ISO without tz
                    # so JS at least sees something parseable.
                    next_cycle_at = _parsed.strftime("%Y-%m-%dT%H:%M:%S")
                next_cycle_in_seconds = max(0, int((_parsed - now_dt).total_seconds()))
                break
        except Exception:
            pass

    # Pull sprint-mode + cycles_completed from the always-on heartbeat
    # (most authoritative source for those two fields).
    if isinstance(ao_hb, dict):
        if ao_hb.get("sprint_mode") is True:
            sprint_mode = True
        try:
            cc = ao_hb.get("cycles_completed")
            if cc is not None:
                cycles_completed = int(cc)
        except (TypeError, ValueError):
            cycles_completed = None
        # Heartbeat may also carry next_cycle_at when the activity file
        # is stale (heartbeat is written more often than the activity).
        # SAME stale-discard rule applies here: if the heartbeat's
        # recorded next-cycle is more than 5 min in the past, drop it
        # so the supervisor / schtasks fallbacks downstream can compute
        # a live value instead of pinning a stuck one.
        if not next_cycle_at and ao_hb.get("next_cycle_at"):
            cand = str(ao_hb.get("next_cycle_at"))
            t_next = _parse_iso_local(cand)
            if t_next is not None:
                delta = (t_next - now_dt).total_seconds()
                if delta >= -300:
                    next_cycle_at = cand
                    next_cycle_in_seconds = max(0, int(delta))

    # If activity file is missing or stale, derive idle/blocked reasons
    # heuristically so the dashboard never shows bare IDLE without context.
    if not activity_fresh:
        if not is_active:
            if not READONLY_SOURCES["always_on_heartbeat"].exists():
                idle_reason = "supervisor_not_installed"
            elif ao_state == "cycle_complete":
                idle_reason = "waiting_for_next_scheduled_cycle"
            elif ao_state == "sleeping":
                idle_reason = "waiting_for_next_scheduled_cycle"
            elif ao_state == "idle_no_tasks":
                idle_reason = "queue_empty"
            else:
                idle_reason = "waiting_for_manual_start"
            is_idle = True

    # 2026-05-13 Terminal Accuracy Pass: emit three explicitly-labeled
    # fields — `mc_latest_event`, `mc_current_state`, `mc_next_scheduled_action`
    # — so the operator UI never has to mash phases together. These
    # sit alongside the legacy fields below for back-compat; the
    # /api/operator-truth surface and this panel both read them.
    _mc_latest_event = (
        f"{last_handoff_from} -> {last_handoff_to}" if last_handoff_from or last_handoff_to
        else (str(latest_feed.get("event")) if latest_feed and latest_feed.get("event") else None)
    )
    if is_complete:
        _mc_current_state = "COMPLETED"
    elif is_blocked:
        _mc_current_state = f"BLOCKED: {blocked_reason or '(reason unknown)'}"
    elif is_active:
        _mc_current_state = f"ACTIVE: {actor} {stage}"
    elif is_idle:
        _mc_current_state = f"IDLE: {idle_reason or 'waiting_for_next_work'}"
    elif is_stale:
        _mc_current_state = f"STALE: {stale_reason or 'no_recent_heartbeat'}"
    else:
        _mc_current_state = "UNKNOWN"
    _mc_next_scheduled_action = next_action or (
        f"next cycle in {next_cycle_in_seconds}s"
        if next_cycle_in_seconds is not None else None
    )

    return _attach_canonical_truth_summary({
        "ok": True,
        "generated_at": _now_iso(),
        # ---- 2026-05-13 explicitly-labeled mission control fields ----
        "mc_latest_event":          _mc_latest_event,
        "mc_current_state":         _mc_current_state,
        "mc_next_scheduled_action": _mc_next_scheduled_action,
        "mc_label_contract": (
            "These three mc_* fields are the operator-visible truth for "
            "Mission Control. They are NEVER concatenated into one line."
        ),
        # ---- legacy fields (back-compat; panel migrators read mc_* above) ----
        "current_actor": actor,
        "current_stage": stage,
        "current_task_title": title,
        "current_status_text": status,
        "started_at": started,
        "elapsed_seconds": elapsed,
        "eta_seconds": None,
        "progress_percent": progress_percent,
        "heartbeat_age_seconds": int(last_age_s) if last_age_s is not None else None,
        "last_update": last_iso,
        "activity_age_seconds": int(activity_age_s) if activity_age_s is not None else None,
        "activity_updated_at": activity_updated,
        "last_handoff_from": last_handoff_from,
        "last_handoff_to": last_handoff_to,
        "last_result": last_result,
        "is_active": is_active,
        "is_stale": is_stale,
        "is_idle": is_idle,
        "is_blocked": is_blocked,
        "is_complete": is_complete,
        "stale_reason": stale_reason,
        "idle_reason": idle_reason,
        "blocked_reason": blocked_reason,
        "next_action": next_action,
        "report_path": activity_report_path,
        "next_cycle_at": next_cycle_at,
        "next_cycle_in_seconds": next_cycle_in_seconds,
        "cycle_running_for_seconds": cycle_running_for_seconds,
        "sprint_mode": sprint_mode,
        "cycles_completed": cycles_completed,
        "source_used": source_used,
        "source_summary": {
            "worker_state":  worker.get("state"),
            "worker_phase":  worker.get("phase"),
            "worker_pid":    worker.get("pid"),
            "worker_alive":  worker.get("alive"),
            "aider_state":   aider.get("state"),
            "aider_pid":     aider.get("pid"),
            "guardian_status": guardian.get("status"),
            "always_on_state": ao_hb.get("state"),
            "always_on_installed": READONLY_SOURCES["always_on_heartbeat"].exists(),
            "live_feed_latest_source": latest_feed.get("source") if latest_feed else None,
            "live_feed_latest_event":  latest_feed.get("event")  if latest_feed else None,
            "last_iso_source": last_iso_source,
            "current_activity_present": bool(activity_fresh),
            "current_activity_age_seconds": int(activity_age_s) if activity_age_s is not None else None,
        },
        # ---- 2026-05-13 Mission Control bounded-read budget ----
        # Surfaces total handler wall time + any sources whose read hit a
        # writer-held lock. The UI uses `read_budget.is_stale` to render a
        # "partial" indicator instead of treating timeouts as authoritative.
        "read_budget": {
            "total_budget_ms":  MISSION_CONTROL_TOTAL_BUDGET_MS,
            "per_read_timeout_ms": MISSION_CONTROL_READ_TIMEOUT_MS,
            "elapsed_ms":       _read_budget.elapsed_ms(),
            "stale_sources":    dict(_read_budget.stale_sources),
            "is_stale":         bool(_read_budget.stale_sources),
        },
    })


def build_health_payload() -> dict[str, Any]:
    """Tiny health endpoint — useful for tests and the hero pulse animation."""
    sources_present = {
        key: READONLY_SOURCES[key].exists() for key in READONLY_SOURCES
    }
    return {
        "ok": True,
        "phase": PHASE_ID,
        "phase_name": PHASE_NAME,
        "generated_at": _now_iso(),
        "host": DEFAULT_HOST,
        "advisory_only": ADVISORY_ONLY,
        "code_execution_state": "LOCKED",
        "guardian_live_enforcement": "DISABLED",
        "sources_present": sources_present,
    }


def _build_process_health_payload() -> dict[str, Any]:
    """Surface luna_process_reaper.review() to the dashboard.

    2026-05-27: Created so Luna (and the operator) can SEE the categorized
    process landscape. The payload includes:
      - narrow vs broad Luna process counts (architectural fix context)
      - reapable orphan list with reasons (parent_dead / stale_accumulator)
      - by-category breakdown for visual review
      - recent audit-trail tail (last 20 reaper actions)
      - thresholds (so dashboard can show headroom)

    On any error, returns a minimal {"ok": False, "error": ...} payload
    so the dashboard never crashes from this endpoint.
    """
    try:
        from luna_modules import luna_process_reaper as _reaper
        r = _reaper.review()
        # Add a top-level ok flag so the dashboard can branch cleanly
        r["ok"] = True
        return r
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "now": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }


def _dashboard_latest_report_payload() -> dict[str, Any]:
    """Return the freshest dashboard/progression report path without reading secrets."""
    candidates: list[Path] = []
    for folder in (
        MEMORY_DIR / "tier_progression",
        MEMORY_DIR / "tier_progression" / "reports",
        MEMORY_DIR / "tier8",
        MEMORY_DIR / "tier7",
        MEMORY_DIR / "tier6",
        LOGS_DIR,
    ):
        try:
            if folder.exists() and folder.is_dir():
                candidates.extend([
                    p for p in folder.glob("*")
                    if p.is_file() and p.suffix.lower() in {".md", ".json", ".jsonl", ".txt"}
                ])
        except OSError:
            continue
    latest: Path | None = None
    try:
        latest = max(candidates, key=lambda p: p.stat().st_mtime) if candidates else None
    except OSError:
        latest = None
    rel = ""
    mtime = None
    if latest is not None:
        try:
            rel = str(latest.relative_to(PROJECT_ROOT)).replace("\\", "/")
            mtime = datetime.fromtimestamp(latest.stat().st_mtime).astimezone().isoformat()
        except OSError:
            rel = str(latest)
    return {
        "ok": True,
        "report_path": rel,
        "report_modified_local": mtime,
        "generated_at": _now_iso(),
    }


def _hidden_powershell_log_command(script_path: Path, args: list[str], log_path: Path) -> tuple[int | None, str | None]:
    """Run a fixed local script hidden, teeing output to a fixed log.

    This is deliberately narrow: no request-supplied command text, no
    elevation, no visible console, and stdout/stderr are still redirected
    away from the dashboard process.
    """
    if not script_path.exists() or not script_path.is_file():
        return None, "script missing"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    arg_text = " ".join(args)
    command = (
        "& { & '" + str(script_path).replace("'", "''") + "' " + arg_text +
        " *>&1 | Tee-Object -FilePath '" + str(log_path).replace("'", "''") + "' }"
    )
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-NonInteractive",
        "-WindowStyle",
        "Hidden",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        command,
    ]
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            **_hidden_popen_kwargs(),
        )
        return proc.pid, None
    except OSError as exc:
        return None, f"start failed: {exc.__class__.__name__}"


def _dashboard_health_check_handle(handler: BaseHTTPRequestHandler) -> None:
    if not _check_loopback(handler):
        return
    _read_post_json(handler, max_bytes=512)
    log_path = LOGS_DIR / "dashboard_health_check_latest.txt"
    pid, err = _hidden_powershell_log_command(
        PROJECT_ROOT / "Luna_Dashboard_ProcessCheck.ps1",
        [],
        log_path,
    )
    if err:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": err})
        return
    _send_json(handler, HTTPStatus.ACCEPTED, {
        "ok": True,
        "pid": pid,
        "log_path": str(log_path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "message": "hidden dashboard health check started",
    })


def _dashboard_boot_selfheal_handle(handler: BaseHTTPRequestHandler) -> None:
    if not _check_loopback(handler):
        return
    _read_post_json(handler, max_bytes=512)
    log_path = LOGS_DIR / "dashboard_boot_selfheal_latest.txt"
    pid, err = _hidden_powershell_log_command(
        PROJECT_ROOT / "Luna_Boot_SelfHeal.ps1",
        ["-Repair", "-CleanStaleLaunchers", "-VerifyProgressionTask"],
        log_path,
    )
    if err:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": err})
        return
    _send_json(handler, HTTPStatus.ACCEPTED, {
        "ok": True,
        "pid": pid,
        "log_path": str(log_path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "message": "hidden boot self-heal started",
    })


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------
ALLOWED_METHODS = {"GET", "HEAD"}
_PATH_RE = re.compile(r"^/[A-Za-z0-9._/\-]*$")

# 2026-05-13 Ctrl+F5 hard-refusal-of-connection fix.
# Tuple of "client went away mid-write" exceptions. Every wire-touching
# operation in the handler is wrapped against this so a torn-down socket
# during Ctrl+F5 cannot propagate up through socketserver and trip the
# stdlib's default handle_error (which writes to sys.stderr=None under
# pythonw — a secondary AttributeError that historically silently killed
# the entire server process).
_SOCKET_DIED = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError)


def _safe_log_dashboard_error(message: str) -> None:
    """Append a message to logs/luna_command_center.log. Never raises.

    Used by handle_error / finish / log_request paths that under pythonw
    would otherwise crash trying to write to sys.stderr=None.
    """
    try:
        log_path = PROJECT_ROOT / "logs" / "luna_command_center.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8", errors="replace") as fh:
            fh.write(f"[{_now_iso()}] luna_http_dashboard: {message}\n")
    except Exception:  # noqa: BLE001
        pass


class LunaDashboardHandler(BaseHTTPRequestHandler):
    """Read-only HTTP handler. GET/HEAD only. No writes. No shell."""

    server_version = "LunaDashboard/UI-1A"

    # Suppress default stderr access logs; the dashboard is a quiet local
    # helper, and Serge sees state through the live feed.
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    # 2026-05-13 Ctrl+F5 fix: both log_request and log_error also funnel
    # through log_message in the stdlib, but be belt-and-braces: silence
    # any direct call too. Under pythonw, sys.stderr is None and the
    # stdlib's default would crash here on the first non-200 response.
    def log_request(self, code="-", size="-") -> None:  # noqa: A002
        return

    def log_error(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    def finish(self) -> None:
        """Defensive close: a Ctrl+F5'd socket raises during
        ``StreamRequestHandler.finish()`` when it flushes ``wfile``. Swallow
        the socket-died family so the per-request thread exits cleanly
        instead of propagating to ``handle_error``.

        2026-06-02 socket-pile fix: explicitly shutdown + close the kernel
        socket after super().finish() runs. The stdlib path leaves the
        underlying ``self.connection`` reachable from the thread frame
        until GC eventually releases it — on Windows that's slow enough
        that 90 sockets pile in CLOSE_WAIT within 90 sec under heavy
        browser polling. Explicit shutdown(SHUT_RDWR) sends FIN in both
        directions immediately, transitioning CLOSE_WAIT -> LAST_ACK ->
        CLOSED at kernel speed. Combined with the existing
        Connection: close + close_connection=True on every response,
        this should kill the socket-pile bounces.
        """
        try:
            super().finish()
        except _SOCKET_DIED:
            pass
        except Exception:  # noqa: BLE001
            pass
        # Force kernel-level close — independent of GC timing.
        try:
            import socket as _socket
            try:
                self.connection.shutdown(_socket.SHUT_RDWR)
            except (_SOCKET_DIED, OSError, AttributeError):
                # Socket already half-closed / already shut / no connection.
                pass
            try:
                self.connection.close()
            except (_SOCKET_DIED, OSError, AttributeError):
                pass
        except Exception:  # noqa: BLE001 — finish() must never raise
            pass

    def handle_one_request(self) -> None:
        """Wrap the stdlib request loop with explicit socket-died catches.

        The stdlib version catches ``socket.timeout`` but lets
        ``BrokenPipeError`` propagate up. Under Ctrl+F5 the browser
        aborts every in-flight fetch — the resulting BrokenPipe storms
        used to take the entire server down (no traceback recorded
        because ``handle_error`` itself crashed on stderr=None).
        """
        try:
            super().handle_one_request()
        except _SOCKET_DIED:
            try:
                self.close_connection = True
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            try:
                self.close_connection = True
            except Exception:  # noqa: BLE001
                pass

    # ---- method gating --------------------------------------------------
    def _reject_unsupported(self) -> None:
        self.send_error(HTTPStatus.METHOD_NOT_ALLOWED, "Method not allowed (read-only)")

    def do_POST(self) -> None:  # noqa: N802
        # Narrow exception to the read-only contract: the Command Console
        # chat lane is allowed to drop tasks into tasks/active/, and the
        # Self-Upgrade approval lane is allowed to flip tier2_approved in
        # the evidence gate JSON, and the run-cycle lane may start the
        # fixed bounded supervisor command. Everything else still 405s.
        try:
            parsed = urllib.parse.urlsplit(self.path or "/")
            raw_path = parsed.path or "/"
            if raw_path == "/api/conversation/turn":
                # Luna Conversation Runtime V1 — canonical live chat path.
                # Classifies turn -> dynamic micro-ack via gpt4all_local
                # in parallel with main reasoning -> intent-aware voice
                # routing through V3/V4/V4.5. Replaces /api/chat/send for
                # normal interactive chat. Legacy /api/chat/send still
                # exists (worker.py task-queue path) for batched / vision
                # workloads.
                _conversation_turn_handle(self)
                return
            if raw_path == "/api/chat/send":
                _chat_handle_send(self)
                return
            if raw_path == "/api/chat/upload":
                _chat_handle_upload(self)
                return
            if raw_path == "/api/vision/describe":
                # Stage 2 - Luna Vision Link. Loopback-only Ollama call;
                # path-jailed to TASKS_UPLOADS_DIR; never crashes the
                # router (per-handler try wraps the call below).
                _vision_describe_handle(self)
                return
            if raw_path == "/api/self-upgrade/approve-tier2":
                _approve_tier2_handle(self)
                return
            if raw_path == "/api/self-upgrade/run-cycle":
                _run_self_upgrade_cycle_handle(self)
                return
            if raw_path == "/api/supervisor/run-once":
                _supervisor_run_once_handle(self)
                return
            if raw_path == "/api/supervisor/start-sprint":
                _supervisor_start_sprint_handle(self)
                return
            if raw_path == "/api/supervisor/stop-sprint":
                _supervisor_stop_sprint_handle(self)
                return
            if raw_path == "/api/kill-switch/run":
                _kill_switch_handle(self)
                return
            if raw_path == "/api/voice/toggle":
                _voice_toggle_handle(self)
                return
            if raw_path == "/api/voice/stop":
                _voice_stop_handle(self)
                return
            if raw_path == "/api/voice/test":
                _voice_test_handle(self)
                return
            if raw_path == "/api/voice/preset":
                _voice_preset_handle(self)
                return
            if raw_path == "/api/voice/converse":
                _voice_converse_handle(self)
                return
            if raw_path == "/api/voice/v2/stream":
                # V2 streaming pipeline (round 21) - SSE response with
                # token + sentence_audio events as Luna generates.
                _voice_converse_stream_handle(self)
                return
            # 2026-05-12 CyberGuy Security Console POST endpoints
            if raw_path == "/api/cyberguy/scan-preview":
                _cyberguy_scan_handle(self, mode="preview")
                return
            if raw_path == "/api/cyberguy/scan":
                _cyberguy_scan_handle(self, mode="scan")
                return
            if raw_path == "/api/cyberguy/acknowledge":
                _cyberguy_acknowledge_handle(self)
                return
            if raw_path == "/api/cyberguy/request-action":
                _cyberguy_request_action_handle(self)
                return
            if raw_path == "/api/cyberguy/confirm-action":
                _cyberguy_confirm_action_handle(self)
                return
            # 2026-05-13 panel-friendly Cyberguy action endpoints.
            if raw_path == "/api/cyberguy/action/restore":
                _cyberguy_panel_action_handle(self, action="restore")
                return
            if raw_path == "/api/cyberguy/action/archive":
                _cyberguy_panel_action_handle(self, action="archive")
                return
            if raw_path == "/api/cyberguy/action/delete":
                _cyberguy_panel_action_handle(self, action="delete")
                return
            if raw_path == "/api/decision/verdict":
                _decision_verdict_handle(self)
                return
            if raw_path == "/api/dashboard/health-check":
                _dashboard_health_check_handle(self)
                return
            if raw_path == "/api/dashboard/boot-selfheal":
                _dashboard_boot_selfheal_handle(self)
                return
        except Exception as exc:  # noqa: BLE001
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"post error: {type(exc).__name__}")
            return
        self._reject_unsupported()

    def do_PUT(self) -> None:  # noqa: N802
        self._reject_unsupported()

    def do_PATCH(self) -> None:  # noqa: N802
        self._reject_unsupported()

    def do_DELETE(self) -> None:  # noqa: N802
        self._reject_unsupported()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._reject_unsupported()

    # ---- routing --------------------------------------------------------
    def do_HEAD(self) -> None:  # noqa: N802
        self._handle(write_body=False)

    def do_GET(self) -> None:  # noqa: N802
        self._handle(write_body=True)

    def _handle(self, write_body: bool) -> None:
        try:
            parsed = urllib.parse.urlsplit(self.path or "/")
            raw_path = parsed.path or "/"
            # Reject anything that isn't a clean ASCII web path (kills %2e%2e
            # and other path-traversal variants before any filesystem touch).
            if not _PATH_RE.match(raw_path):
                self.send_error(HTTPStatus.BAD_REQUEST, "invalid path")
                return
            if ".." in raw_path:
                self.send_error(HTTPStatus.BAD_REQUEST, "path traversal rejected")
                return
            if raw_path.startswith("/api/"):
                self._serve_api(raw_path, parsed.query, write_body=write_body)
                return
            self._serve_static(raw_path, write_body=write_body)
        except Exception as exc:  # noqa: BLE001
            # Last-ditch defensive handling — never propagate raw stack traces.
            try:
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"internal error: {type(exc).__name__}")
            except Exception:  # noqa: BLE001
                pass

    # ---- static file serving (whitelist only) ---------------------------
    def _serve_static(self, path: str, write_body: bool) -> None:
        rel = STATIC_FILES.get(path)
        if rel is None:
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return
        target = (DASHBOARD_DIR / rel).resolve()
        try:
            target.relative_to(DASHBOARD_DIR.resolve())
        except ValueError:
            self.send_error(HTTPStatus.FORBIDDEN, "outside dashboard root")
            return
        if not target.exists() or not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "asset missing")
            return
        try:
            data = target.read_bytes()
        except OSError:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "read error")
            return
        ctype = CONTENT_TYPES.get(target.suffix.lower(), "application/octet-stream")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Connection", "close")
        self.close_connection = True
        try:
            self.end_headers()
        except _SOCKET_DIED:
            # Client (Ctrl+F5) aborted before headers flushed. Mark the
            # connection dead so we don't keep-alive into a doomed socket.
            try:
                self.close_connection = True
            except Exception:  # noqa: BLE001
                pass
            return
        if write_body:
            # 2026-05-13 Ctrl+F5 hardening: a raw self.wfile.write here used
            # to raise BrokenPipeError when the browser cancels in-flight
            # fetches on a hard refresh. The outer _handle catches Exception
            # already, but we want this single call to be a tight, named
            # swallow so the request thread exits cleanly without bouncing
            # through the broad-Exception path (which also tries send_error
            # against the same dead socket).
            try:
                self.wfile.write(data)
            except _SOCKET_DIED:
                try:
                    self.close_connection = True
                except Exception:  # noqa: BLE001
                    pass
                return

    # ---- api routing ----------------------------------------------------
    def _safe_build(self, fn, endpoint_name: str, *args, **kwargs) -> dict[str, Any]:
        # Wrap every payload builder in its own guard so one broken file or
        # one transient race in a single endpoint cannot 500 the whole route.
        # The frontend gets ok=false + error_kind + endpoint and can fall back
        # to /api/higher-tier/progress when the older panels go offline. We
        # NEVER fake activity here - on failure we say so explicitly so the
        # operator can see the real error in the per-panel source label.
        #
        # 2026-05-14 universal wall-clock guard: each builder runs in a
        # daemon worker thread with a hard SAFE_BUILD_TIMEOUT_S budget.
        # If the builder exceeds that budget (the historical
        # /api/mission-control + /api/agent-bus + /api/tier-truth
        # recursive-hang failure mode), we return a labeled DEGRADED
        # payload instead of letting the HTTP request hang indefinitely.
        # The orphan thread continues but is daemonic so it cannot
        # block process shutdown.
        timeout_s = float(
            TERMINAL_TRUTH_SAFE_BUILD_TIMEOUT_S
            if endpoint_name in ("/api/terminal-truth", "/api/master-status")
            else SAFE_BUILD_TIMEOUT_S
        )
        box: list[Any] = [None, None]   # [payload, exception]

        # 2026-05-31 thread-leak fix: a builder that exceeds its join timeout
        # is orphaned (left running, daemonic). On a long-lived server those
        # orphans accumulated WITHOUT BOUND while polling slow/hung endpoints
        # (observed live: 1381 threads / 8.5 GB on PID 45420). Acquire a slot
        # before spawning; the worker releases it only when it ACTUALLY
        # finishes, so a still-running orphan keeps holding its slot — which
        # caps the live thread count. If no slot is free, skip the spawn and
        # return an instant degraded payload (the leak grew precisely by
        # spawning one more thread each time).
        _slot_acquired = _builder_slots.acquire(blocking=False)

        def _runner() -> None:
            try:
                box[0] = fn(*args, **kwargs)
            except Exception as exc:    # noqa: BLE001
                box[1] = exc
            finally:
                try:
                    _builder_slots.release()
                except Exception:    # noqa: BLE001
                    pass

        # 2026-05-14 OTel instrumentation: per-endpoint span + counter.
        # Soft-imported so a missing luna_otel never breaks a request.
        try:
            from luna_modules import luna_otel as _ot
            _ot_span_cm = _ot.start_span(
                f"dashboard.endpoint{endpoint_name}",
                {"endpoint": endpoint_name, "timeout_s": timeout_s},
                kind="SERVER",
            )
            _ot_span = _ot_span_cm.__enter__()
        except Exception:    # noqa: BLE001
            _ot = None
            _ot_span_cm = None
            _ot_span = None

        t_start = time.monotonic()
        result: dict[str, Any]
        if not _slot_acquired:
            # Builder pool saturated by slow/stuck builders. Do NOT spawn
            # another thread — that is exactly how the leak grew to 1381
            # threads. Return an instant labeled degraded payload; slots free
            # up as orphaned builders eventually finish.
            elapsed_ms = int((time.monotonic() - t_start) * 1000)
            result = {
                "ok":            False,
                "endpoint":      endpoint_name,
                "source_status": "builder_pool_saturated",
                "degraded":      True,
                "error_kind":    "BuilderPoolSaturated",
                "error_detail":  (f"builder thread pool ({_MAX_BUILDER_THREADS}) "
                                   f"saturated by slow/stuck builders; skipped "
                                   f"spawn to prevent a thread leak"),
                "elapsed_ms":    elapsed_ms,
                "generated_at":  _now_iso(),
                "fallback_hint": (
                    "upstream builders are slow/hung; this request returned "
                    "instantly instead of leaking a thread. Check /api/health "
                    "and the dashboard log for upstream-source slowness."
                ),
            }
        else:
            th = threading.Thread(target=_runner,
                                   name=f"safe_build:{endpoint_name}",
                                   daemon=True)
            th.start()
            th.join(timeout_s)
            elapsed_ms = int((time.monotonic() - t_start) * 1000)
            if th.is_alive():
                result = {
                    "ok":            False,
                    "endpoint":      endpoint_name,
                    "source_status": "timeout",
                    "degraded":      True,
                    "error_kind":    "BuilderTimeout",
                    "error_detail":  (f"builder exceeded {timeout_s:.1f}s "
                                       f"wall-clock budget (elapsed {elapsed_ms} ms)"),
                    "elapsed_ms":    elapsed_ms,
                    "timeout_s":     timeout_s,
                    "generated_at":  _now_iso(),
                    "fallback_hint": (
                        "this endpoint hit its wall-clock budget; the request "
                        "did not hang because every builder is bounded. "
                        "Check /api/health and the dashboard log for "
                        "upstream-source slowness."
                    ),
                }
            else:
                exc = box[1]
                if exc is not None:
                    result = {
                        "ok":            False,
                        "endpoint":      endpoint_name,
                        "source_status": "endpoint_offline",
                        "degraded":      True,
                        "error_kind":    type(exc).__name__,
                        "error_detail":  str(exc)[:200],
                        "elapsed_ms":    elapsed_ms,
                        "generated_at":  _now_iso(),
                        "fallback_hint": (
                            "this endpoint failed; check /api/higher-tier/progress "
                            "and /api/live-feed for ground-truth Luna freshness"
                        ),
                    }
                else:
                    payload = box[0]
                    if isinstance(payload, dict):
                        payload.setdefault("ok", True)
                        payload.setdefault("endpoint", endpoint_name)
                        payload.setdefault("source_status", "fresh")
                        payload.setdefault("elapsed_ms", elapsed_ms)
                    result = payload    # type: ignore[assignment]
        # OTel finalize: attach attributes to the span + emit metrics.
        try:
            if _ot is not None and _ot_span is not None:
                _ot_span["attributes"].update({
                    "endpoint":            endpoint_name,
                    "elapsed_ms":          elapsed_ms,
                    "source_status":       (result.get("source_status")
                                              if isinstance(result, dict) else "unknown"),
                    "ok":                  (bool(result.get("ok"))
                                              if isinstance(result, dict) else False),
                    "degraded":            (bool(result.get("degraded"))
                                              if isinstance(result, dict) else False),
                })
                _ot.record_histogram(
                    "dashboard.endpoint.latency_ms",
                    float(elapsed_ms),
                    {"endpoint": endpoint_name,
                     "source_status": (result.get("source_status")
                                         if isinstance(result, dict) else "unknown")})
                if isinstance(result, dict) and result.get("source_status") in (
                        "timeout", "endpoint_offline"):
                    _ot.inc_counter("dashboard.endpoint.degraded",
                                     1.0,
                                     {"endpoint": endpoint_name,
                                      "source_status": result["source_status"]})
                _ot_span_cm.__exit__(None, None, None)
        except Exception:    # noqa: BLE001
            try:
                if _ot_span_cm is not None:
                    _ot_span_cm.__exit__(None, None, None)
            except Exception:    # noqa: BLE001
                pass
        return result

    def _serve_api(self, path: str, query: str, write_body: bool) -> None:
        params = urllib.parse.parse_qs(query) if query else {}
        try:
            if path == "/api/status":
                payload = self._safe_build(build_status_payload, path)
            elif path == "/api/decision-brief":
                payload = self._safe_build(build_decision_brief_payload, path)
            elif path == "/api/soak":
                payload = self._safe_build(build_soak_payload, path)
            elif path == "/api/scorecard":
                payload = self._safe_build(build_scorecard_payload, path)
            elif path == "/api/resources":
                payload = self._safe_build(build_resources_payload, path)
            elif path == "/api/live-feed":
                limit_raw = params.get("limit", [str(LIVE_FEED_MAX_LINES)])[0]
                try:
                    limit = int(limit_raw)
                except (TypeError, ValueError):
                    limit = LIVE_FEED_MAX_LINES
                payload = self._safe_build(build_live_feed_payload, path, limit=limit)
            elif path == "/api/archive":
                payload = self._safe_build(build_archive_payload, path)
            elif path == "/api/activity":
                w_raw = params.get("window", ["1800"])[0]
                b_raw = params.get("buckets", ["60"])[0]
                try:
                    window = int(w_raw)
                except (TypeError, ValueError):
                    window = 1800
                try:
                    buckets = int(b_raw)
                except (TypeError, ValueError):
                    buckets = 60
                payload = self._safe_build(build_activity_payload, path,
                                           window_seconds=window, buckets=buckets)
            elif path == "/api/health":
                # 2026-05-31 FAST PATH (root-cause fix): do NOT route /api/health
                # through _safe_build. /api/health is a LIVENESS probe and must
                # work even when the builder pool is saturated (the BuilderPoolSaturated
                # path returned 200 OK with body.ok=false which silently fooled
                # external monitors). The fast path uses a 5s local cache for the
                # ~17 file exists() checks. Target <50ms always.
                payload = _build_health_payload_fast()
            elif path == "/api/process-health":
                # 2026-05-27: Luna's self-review endpoint. Returns
                # categorized process landscape so the dashboard (or
                # operator) can SEE what's running and whether any
                # accumulator class is leaking. Mirrors:
                #   python -m luna_modules.luna_process_reaper review
                payload = self._safe_build(_build_process_health_payload, path)
            elif path == "/api/dashboard/latest-report":
                payload = self._safe_build(_dashboard_latest_report_payload, path)
            elif path == "/api/chat/response":
                task_id = (params.get("task", [""])[0] or "").strip()
                payload = self._safe_build(build_chat_response_payload, path, task_id)
            elif path == "/api/files/roots":
                payload = self._safe_build(build_files_roots_payload, path)
            elif path == "/api/files/list":
                req_path = (params.get("path", [""])[0] or "").strip()
                payload = self._safe_build(build_files_list_payload, path, req_path)
            elif path == "/api/supermax":
                payload = self._safe_build(build_supermax_payload, path)
            elif path == "/api/self-upgrade/progress":
                payload = self._safe_build(build_self_upgrade_progress_payload, path)
            elif path == "/api/canonical-truth":
                # 2026-05-13 ROOT-CAUSE FIX — the ONE canonical current
                # operator-facing truth payload. Every primary panel in
                # luna_dashboard/app.js MUST consume this first and use
                # LunaCanonicalTruthClient to fan its values to per-panel
                # render contracts before falling back to panel-specific
                # endpoint elaboration.
                payload = self._safe_build(build_canonical_truth_payload, path)
            elif path == "/api/terminal-truth":
                # 2026-05-13 HARD CUTOVER — the SINGLE source of truth
                # consumed by every primary panel render path. Built by
                # luna_modules.luna_terminal_truth.build_terminal_truth.
                # Legacy endpoints are LEGACY_DETAIL_ONLY for non-primary
                # auxiliary data; any primary panel reading them for
                # current truth fails a static test.
                payload = self._safe_build(build_terminal_truth_payload, path)
            elif path.startswith("/api/terminal-truth/"):
                # 2026-05-13 per-panel slice for narrow re-renders.
                # 2026-06-01 LEAK FIX: validate panel against the canonical
                # PRIMARY_PANELS allowlist BEFORE letting it become a cache
                # key. Unbounded panel strings from URL polluted
                # _payload_cache + _key_build_locks with one permanent
                # entry per unique URL — a slow but real RAM leak that
                # contributed to the dashboard hitting 1000MB and forcing
                # warden bounces ~6x/day.
                panel = path[len("/api/terminal-truth/"):].strip("/").lower()
                try:
                    from luna_modules.luna_terminal_truth import PRIMARY_PANELS
                    _valid_panels = frozenset(PRIMARY_PANELS)
                except Exception:  # noqa: BLE001 — defensive
                    _valid_panels = frozenset()
                if panel and panel in _valid_panels:
                    payload = self._safe_build(
                        build_terminal_truth_panel_payload, path, panel
                    )
                else:
                    # 404 — never goes near the cache.
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    body = json.dumps({
                        "ok": False,
                        "error": "unknown_panel",
                        "panel": panel,
                        "valid_panels": sorted(_valid_panels),
                    }).encode("utf-8")
                    self.wfile.write(body)
                    return
            elif path == "/api/mission-control":
                payload = self._safe_build(build_mission_control_payload, path)
            elif path == "/api/supervisor/status":
                payload = self._safe_build(_supervisor_status_payload, path)
            elif path == "/api/higher-tier/progress":
                # Display-only Tier 6+ progress surface. Read-only.
                payload = self._safe_build(build_higher_tier_progress_payload, path)
            elif path == "/api/tier-truth":
                # Single synthesized "what is Luna's actual current tier"
                # surface that drives the Evolution Command Center UI.
                # 2026-05-11: payload now includes unified_tier_truth block
                # from luna_modules.luna_tier_truth.get_tier_truth().
                payload = self._safe_build(build_tier_truth_payload, path)
            elif path == "/api/model-hierarchy":
                # 2026-05-11 Serge directive: surface the active model
                # hierarchy (Gemini-first). NEVER includes API keys.
                payload = self._safe_build(build_model_hierarchy_payload, path)
            elif path == "/api/memory/status":
                # 2026-05-11 Luna Memory OS: counts + last-compression
                # timestamp + redaction-active flag. NEVER includes
                # keys or raw memory content.
                payload = self._safe_build(build_memory_status_payload, path)
            elif path == "/api/privacy/status":
                # 2026-05-11 External Model Privacy Boundary: surface the
                # boundary state. NEVER includes API keys or secrets.
                payload = self._safe_build(build_privacy_status_payload, path)
            elif path == "/api/upgrade-adoption":
                # 2026-05-11 Upgrade Adoption Engine: surface which
                # upgrades are ACTIVE, BLOCKED, etc. NEVER includes
                # API keys or private memory.
                payload = self._safe_build(build_upgrade_adoption_payload, path)
            elif path == "/api/tier-adoption":
                # 2026-05-12 Tier 160 Self-Repair Doctrine: surface the
                # tier-adoption governor + live-chat-brain status. Reads
                # only governor + brain status; never contains secrets.
                payload = self._safe_build(build_tier_adoption_payload, path)
            elif path == "/api/tier-graduation":
                # 2026-05-12 Tier Graduation Doctrine v1: surface the
                # lifecycle state, proof checklist, approval type,
                # next-tier gate, and separated tier-truth readings.
                # Reads only the graduation governor; never contains
                # secrets or personal memory.
                payload = self._safe_build(build_tier_graduation_payload, path)
            elif path == "/api/live-chat-brain/status":
                # 2026-05-12 Live Chat Brain health surface (subsystems
                # loaded, categories routed, canned-fallback blocked).
                payload = self._safe_build(build_live_chat_brain_status_payload, path)
            elif path == "/api/agent-bus":
                # 2026-05-12 Luna Live Map Agent Communication layer:
                # surface verified inter-agent messages + counts +
                # per-agent latest. Reads ONLY luna_agent_bus; never
                # contains secrets or rejected NEEDS_REVIEW messages
                # (those are auditing-only and stay in the bus log).
                payload = self._safe_build(build_agent_bus_payload, path)
            elif path == "/api/cyberguy/panel-status":
                # 2026-05-13 panel-friendly Cyberguy surface — caught
                # items + archived items + recent panel actions.
                payload = self._safe_build(build_cyberguy_panel_status_payload, path)
            elif path == "/api/verifier-status":
                # 2026-05-13 canonical verifier — eliminates the
                # "Verifier unknown" vs "Verifier live" UI split.
                payload = self._safe_build(build_verifier_status_payload, path)
            elif path == "/api/first-tier-milestone":
                # 2026-05-13 first-tier landing watcher.
                payload = self._safe_build(build_first_tier_milestone_payload, path)
            elif path == "/api/probe-health":
                # 2026-05-16 scheduled probe-health re-runner surface.
                # Reads memory/core_brain/probe_health_latest.json which
                # is refreshed every N minutes by the scheduled task
                # LunaProbeHealthSweepUser (see
                # Install_Luna_Probe_Health_Sweep_Task.ps1). Panels show
                # per-tier ok/fail across T1..T200 and any OK->FAIL
                # alerts. Authoritative for the live probe surface.
                payload = self._safe_build(build_probe_health_payload, path)
            elif path == "/api/cognitive":
                # 2026-05-18 Phase 0 cognitive foundation status surface.
                # Read-only. Reads only flag file + foundation module
                # state; NEVER drives behavior. The cognitive path is
                # OFF by default (cognitive_feature_flags.DEFAULTS).
                # Rollback: delete D:\SurgeApp\memory\cognitive\feature_flags.json.
                payload = self._safe_build(build_cognitive_status_payload, path)
            elif path == "/api/cognitive/observation":
                # 2026-05-18 Phase 1 observation surface.
                # Default behaviour: read-only -- returns the latest
                # persisted observation (memory/cognitive/latest_observation.json)
                # written by the probe-sweep hook. With ?fresh=true the
                # endpoint triggers a fresh observation through
                # observation_engine.observe_and_persist() (read-only
                # collectors; same code path the probe-sweep hook uses).
                # Rollback: set cognitive_observation_enabled=false in
                # memory/cognitive/feature_flags.json.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_observation_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/interpretation":
                # 2026-05-18 Phase 2 interpretation surface.
                # Default behaviour: read-only -- returns the latest
                # persisted interpretation (memory/cognitive/
                # latest_interpretation.json) written by the probe-sweep
                # hook chain (observation -> interpretation). With
                # ?fresh=true the endpoint triggers a fresh
                # interpret_latest() pass over whatever observation is
                # currently on disk (same code path the hook uses).
                # Pure projection: no external calls, no action.
                # Rollback: set cognitive_interpretation_enabled=false
                # in memory/cognitive/feature_flags.json.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_interpretation_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/goals":
                # 2026-05-18 Phase 3 candidate-goals surface.
                # Default behaviour: read-only -- returns the latest
                # persisted candidate-goals snapshot (memory/cognitive/
                # latest_candidate_goals.json) written by the probe-sweep
                # hook chain (observation -> interpretation -> goals).
                # With ?fresh=true the endpoint triggers a fresh
                # goal_generation_engine.generate_latest() pass over
                # whatever interpretation is currently on disk (same
                # code path the hook uses). Pure projection: no external
                # calls, no action.
                # Rollback: set cognitive_goal_generation_enabled=false
                # in memory/cognitive/feature_flags.json.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_goals_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/priorities":
                # 2026-05-18 Phase 4 prioritized-goals surface.
                # Default behaviour: read-only -- returns the latest
                # persisted prioritization (memory/cognitive/
                # latest_prioritized_goals.json) written by the probe-
                # sweep hook chain (observation -> interpretation ->
                # goals -> priorities). With ?fresh=true the endpoint
                # triggers a fresh prioritize_latest() pass over
                # whatever candidate-goals snapshot is currently on
                # disk. Pure projection (weighted scoring + history
                # update only); no external calls; no action.
                # Rollback: set cognitive_prioritization_enabled=false
                # in memory/cognitive/feature_flags.json.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_priorities_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/plans":
                # 2026-05-18 Phase 5 plans surface.
                # Default behaviour: read-only -- returns the latest
                # persisted plans (memory/cognitive/latest_plans.json)
                # written by the probe-sweep hook chain (observation ->
                # interpretation -> goals -> priorities -> plans). With
                # ?fresh=true the endpoint triggers a fresh
                # planning_engine.plan_latest() pass over the priorities
                # currently on disk. Plans are read-only description
                # artifacts -- the planner NEVER executes anything.
                # Rollback: set cognitive_planning_enabled=false in
                # memory/cognitive/feature_flags.json.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_plans_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/execution":
                # 2026-05-18 Phase 6 execution-results surface.
                # Default behaviour: read-only -- returns the latest
                # persisted execution results (memory/cognitive/
                # latest_execution_results.json) written by the probe-
                # sweep hook chain. With ?fresh=true the endpoint
                # triggers a fresh execute_latest() pass over the
                # plans currently on disk. The executor REFUSES at
                # preflight any plan not fully read-only + dry_run.
                # Rollback: set cognitive_execution_enabled=false in
                # memory/cognitive/feature_flags.json.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_execution_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/reflection":
                # 2026-05-18 Phase 7 reflection surface.
                # Default behaviour: read-only -- returns the latest
                # persisted reflection results (memory/cognitive/
                # latest_reflection_results.json). With ?fresh=true the
                # endpoint triggers a fresh reflect_latest() pass over
                # the execution results currently on disk. Analysis-only;
                # never mutates other cognitive state.
                # Rollback: set cognitive_reflection_enabled=false in
                # memory/cognitive/feature_flags.json.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_reflection_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/memory":
                # 2026-05-19 Phase 8 synthesized-memory surface.
                # Default behaviour: read-only -- returns the latest
                # per-cycle memory snapshot (memory/cognitive/
                # latest_synthesized_memory.json) plus the bounded LRU
                # store stats. With ?fresh=true the endpoint triggers a
                # fresh synthesize_latest() pass over the reflection
                # results currently on disk (also touches the LRU
                # store). Analysis-only; never mutates other cognitive
                # state.
                # Rollback: set cognitive_memory_synthesis_enabled=false
                # in memory/cognitive/feature_flags.json.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_memory_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/strategy":
                # 2026-05-19 Phase 9 strategy-adjustments surface.
                # Default behaviour: read-only -- returns the latest
                # strategy-adjustments snapshot (memory/cognitive/
                # latest_strategy_adjustments.json). With ?fresh=true
                # the endpoint triggers a fresh adapt_latest() pass over
                # the synthesized memories currently on disk. The
                # adjustments file is consumed by Phase 4 on the next
                # cycle; the consumer flag (cognitive_strategy_active)
                # controls application. Bounded influence only.
                # Rollback: set cognitive_strategy_adaptation_enabled
                # or cognitive_strategy_active to false.
                fresh_q = (params.get("fresh", ["false"])[0]
                           or "false").strip().lower()
                fresh = fresh_q in ("1", "true", "yes", "y", "on")
                payload = self._safe_build(
                    build_cognitive_strategy_payload, path, fresh=fresh
                )
            elif path == "/api/cognitive/scheduler":
                # 2026-05-19 Phase 10 cognitive-scheduler surface.
                # Read-only: returns the latest SchedulerRun snapshot,
                # current lockfile state, and the tail of the bounded
                # run log. NEVER triggers a cycle from the dashboard;
                # the scheduler is fired only by the canonical
                # LunaProbeHealthSweepUser scheduled task. Operator can
                # use this endpoint to confirm overlap protection +
                # audit trail are working.
                payload = self._safe_build(
                    build_cognitive_scheduler_payload, path
                )
            elif path == "/api/rebuild-campaign":
                # 2026-05-13 Tier 1..500 honest rebuild campaign.
                # Authoritative for ledger + frontier; advisory layer
                # may recommend tuneup vs repair vs redo.
                payload = self._safe_build(build_rebuild_campaign_payload, path)
            elif path == "/api/master-status":
                # 2026-05-13 ONE consolidated operator surface — answers
                # every audit-checklist question from a single URL.
                payload = self._safe_build(build_master_status_payload, path)
            elif path == "/api/housekeeping":
                # 2026-05-13 autonomous housekeeping surface — last sweep
                # counters, policy mode, blockers. Read-only; the runtime
                # actor is luna_housekeeping.maybe_run_sweep().
                payload = self._safe_build(build_housekeeping_payload, path)
            elif path == "/api/cost-control":
                # 2026-05-13 strict cost-control routing surface.
                # Read-only; authoritative for the policy snapshot.
                payload = self._safe_build(build_cost_control_payload, path)
            elif path == "/api/backfill-status":
                # 2026-05-13 Backfill + Council coordination surface.
                # Read-only. Authoritative for the audit; advisory for
                # recent council sessions.
                payload = self._safe_build(build_backfill_status_payload, path)
            elif path == "/api/stuck-status":
                # 2026-05-13 Stuck detector. Read-only. Authoritative.
                payload = self._safe_build(build_stuck_status_payload, path)
            elif path == "/api/progression-proof":
                # 2026-05-13 Progression proof artifact. Read-only.
                # Authoritative — only records what's observed on disk.
                payload = self._safe_build(build_progression_proof_payload, path)
            elif path == "/api/operator-truth":
                # 2026-05-13 Terminal Accuracy Pass: the SINGLE canonical
                # operator-truth payload. Every visible panel must read
                # this. Cross-checks tier_graduation lifecycle_state
                # against canonical truth_verdict so OPERATIONAL_PROVEN
                # cannot leak while UNDER_AUDIT.
                payload = self._safe_build(build_operator_truth_payload, path)
            elif path == "/api/council/status":
                # 2026-05-13 Luna Council Advisor (advisory-only): READ-ONLY
                # operator surface for the last council session + health.
                # NEVER reaches the network; NEVER influences authoritative
                # tier/repair/adoption/use state.
                payload = self._safe_build(build_council_status_payload, path)
            elif path == "/api/cyberguy/status":
                payload = self._safe_build(build_cyberguy_status_payload, path)
            elif path == "/api/cyberguy/report":
                payload = self._safe_build(build_cyberguy_report_payload, path)
            elif path == "/api/cyberguy/console":
                # 2026-05-12 CyberGuy Security Console: full 9-section
                # payload for the dashboard's premium SOC-style overlay.
                payload = self._safe_build(build_cyberguy_console_payload, path)
            elif path == "/api/cyberguy/threat-db":
                payload = self._safe_build(build_cyberguy_threat_db_payload, path)
            elif path == "/api/cyberguy/actions":
                payload = self._safe_build(build_cyberguy_actions_payload, path)
            elif path == "/api/memory/search":
                # 2026-05-11 Luna Memory OS: top-N relevance-ranked
                # snippets. All snippets pre-redacted via
                # luna_memory_os.redact_secrets at search time AND at
                # response-serialization time (defense in depth).
                q_raw   = (params.get("q",   [""])[0] or "").strip()
                lim_raw = (params.get("limit", ["10"])[0] or "10").strip()
                try:
                    lim = max(1, min(50, int(lim_raw)))
                except (TypeError, ValueError):
                    lim = 10
                payload = self._safe_build(build_memory_search_payload, path,
                                           q=q_raw, limit=lim)
            elif path == "/api/opencode/status":
                payload = self._safe_build(build_opencode_status_payload, path)
            elif path == "/api/voice/status":
                # Public read-only voice status for the dashboard's voice
                # row. Never includes the api key value of any provider.
                # Contract keys passed through from luna_voice:
                # provider_active, missing_dependency, install_hint,
                # human_voice_available, "premium human voice not configured".
                if _LUNA_VOICE_MOD_OK and _luna_voice_mod is not None:
                    payload = self._safe_build(
                        _luna_voice_mod.voice_status_for_dashboard, path,
                        root=str(PROJECT_ROOT),
                    )
                else:
                    payload = {
                        "ok": False,
                        "error": "luna_voice module unavailable",
                        "is_premium_voice": False,
                        "fallback_notice": (
                            "Local fallback voice active — premium human "
                            "voice not configured."
                        ),
                    }
            elif path == "/api/voice/realtime-status":
                # Reports whether the realtime voice conversation
                # pipeline (faster-whisper + Ollama + Kokoro) is wired.
                # Read-only; never includes provider keys.
                try:
                    from luna_modules import luna_realtime_voice as _lrv  # type: ignore
                    payload = {"ok": True, **_lrv.availability()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"realtime voice module unavailable: "
                                 f"{type(exc).__name__}",
                        "ready": False,
                    }
            elif path == "/api/cognitive/voice":
                # Program D — Luna-owned voice runtime snapshot. Reports
                # which backend is primary, the last utterance result, the
                # availability of Piper / direct SAPI / legacy / edge, and
                # whether the runtime is disabled via the rollback flag.
                try:
                    from luna_modules import cognitive_voice_runtime as _cvr  # type: ignore
                    snap = _cvr.get_runtime().snapshot()
                    payload = {"ok": True, **snap.as_dict()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"cognitive_voice_runtime unavailable: {type(exc).__name__}: {exc}",
                        "runtime_enabled": False,
                        "primary_backend": "none",
                        "backends": [],
                    }
            elif path == "/api/cognitive/perception":
                # Program D — Luna-owned multimodal perception snapshot.
                # Reports availability of the screenshot/document/workspace
                # channels and the last result per channel.
                try:
                    from luna_modules import cognitive_perception_runtime as _cpr  # type: ignore
                    snap = _cpr.get_runtime().snapshot()
                    payload = {"ok": True, **snap.as_dict()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"cognitive_perception_runtime unavailable: {type(exc).__name__}: {exc}",
                        "runtime_enabled": False,
                        "channels": {},
                    }
            elif path == "/api/cognitive/presence":
                # Presence Layer — Luna's "I am here" surface. Returns:
                #   - current posture (alive / alive_degraded / limited / asleep / disabled)
                #   - subsystem availability (voice / perception / daily-operator / brain / self-model)
                #   - last boot acknowledgement
                #   - last speech outcome
                #   - speech rate-limit state
                #   - presence policy
                # All read-only and cheap.
                try:
                    from luna_modules import cognitive_presence_runtime as _cprt  # type: ignore
                    payload = {"ok": True, **_cprt.presence_report()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"cognitive_presence_runtime unavailable: {type(exc).__name__}: {exc}",
                        "posture": {"posture": "unknown"},
                    }
            elif path == "/api/cognitive/luna_status":
                # Reality Check — single canonical "what's Luna's state right now"
                # endpoint. Returns the contradiction-free matrix: every
                # subsystem with status (accepted / degraded / blocked / unknown),
                # every harness baseline with grade, and an overall_status
                # (green / green_with_degraded / yellow / red). This is the
                # one URL the operator can hit to answer "are you OK?".
                try:
                    from luna_modules import cognitive_reality_check as _crc  # type: ignore
                    payload = {"ok": True, **_crc.gather_state()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"cognitive_reality_check unavailable: {type(exc).__name__}: {exc}",
                        "overall_status": "unknown",
                    }
            elif path == "/api/cognitive/luna_continuity":
                # Conversational continuity — what Luna treats as current
                # context for the next speak/respond. Cheap, structured,
                # caps text bodies. Useful for any caller that wants to
                # ride Luna's recent context.
                try:
                    from luna_modules import cognitive_presence_runtime as _cprt2  # type: ignore
                    payload = {"ok": True, **_cprt2.compose_response_context()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"presence continuity unavailable: {type(exc).__name__}: {exc}",
                        "posture": "unknown",
                    }
            elif path == "/api/cognitive/luna_voice_v3":
                # Luna Voice V3 — coordinator report (personality + cached
                # phrase manifest + persistent SAPI warm-state + intent
                # policy). Operator-readable.
                try:
                    from luna_modules import cognitive_luna_voice_v3 as _v3  # type: ignore
                    payload = {"ok": True, **_v3.report()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"luna_voice_v3 unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/personality":
                # MyLuna.txt-derived personality runtime: current mode,
                # voice profile, recognised intents.
                try:
                    from luna_modules import cognitive_personality_runtime as _per  # type: ignore
                    payload = {"ok": True, **_per.report()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"personality runtime unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/luna_voice_v4":
                # V4 premium voice profile + adapter readiness + sample
                # prep summary. Read-only.
                try:
                    from luna_modules import cognitive_voice_v4_premium_adapter as _v4  # type: ignore
                    from luna_modules import cognitive_voice_sample_prep as _vprep  # type: ignore
                    adapter = _v4.get_singleton()
                    payload = {
                        "ok": True,
                        "available": adapter.is_available(),
                        "voice_profile": _vprep.report(),
                        "details": adapter.details(),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"v4_premium unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/luna_voice_v4_5":
                # V4.5 TRUE clone via XTTS-v2 — adapter readiness, model
                # load state, last synth timing. Read-only.
                try:
                    from luna_modules import cognitive_voice_xtts_adapter as _xt  # type: ignore
                    adapter = _xt.get_singleton()
                    payload = {
                        "ok": True,
                        "available": adapter.is_available(),
                        "details": adapter.details(),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"xtts unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/conversation":
                # Luna Conversation Runtime V1 — read-only report:
                #   - enabled / premium_voice_allowed flags
                #   - hot in-memory state (recent turns, counters,
                #     legacy_path_quarantine)
                #   - micro-ack adapter readiness
                #   - classifier categories
                try:
                    from luna_modules import cognitive_conversation_runtime as _cr  # type: ignore
                    payload = {"ok": True, **_cr.report()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"conversation runtime unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/luna_warming":
                # Boot-time warm-up state: which components are ready,
                # per-component elapsed_ms + error if any. Used by the
                # UI to show "Luna is warming up… 3/4 ready."
                try:
                    from luna_modules import cognitive_luna_warming as _w  # type: ignore
                    payload = {"ok": True, **_w.report()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"warming unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/luna_phrase_library":
                # Canonical Luna phrase library status — library size +
                # rendered count + per-entry manifest. Read-only.
                try:
                    from luna_modules import cognitive_voice_phrase_renderer as _r  # type: ignore
                    payload = {"ok": True, **_r.report()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"phrase library unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/sovereign":
                # Luna SOVEREIGN Conversation Runtime V2 — dual-model
                # dynamic local path. Surfaces ack + main runtime state
                # plus the legacy_path_quarantine counters that prove
                # Ollama / cache / brain_runtime have NOT been hit in
                # the live hot path. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_sovereign_status(
                        reason="dashboard_api_sovereign")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"sovereign unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/deep_memory":
                # Program M — Deep Memory (hierarchical layers + ranker).
                # Returns the assembler report (weights, char budgets,
                # doctrine). Read-only. To inspect a real per-turn
                # context pack, call ``cognitive_deep_memory.assemble_pack``
                # directly with the operator's text.
                try:
                    from luna_modules import cognitive_deep_memory as _dm  # type: ignore
                    payload = {"ok": True, **_dm.report()}
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"deep_memory unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/executive":
                # Program Q — Executive cortex. Aggregates state /
                # arbiter / interruption / mission_control /
                # proactivity reports + last focus explanation.
                # Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_executive_status(
                        reason="dashboard_api_executive")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"executive unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/adaptation":
                # Program R — Local adaptation. Aggregates trace
                # store + distillation engine + governor + registry
                # reports + recent governor verdicts. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_adaptation_status(
                        reason="dashboard_api_adaptation")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"adaptation unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/fabric":
                # Program S — Realtime acceleration + model fabric.
                # Aggregates fabric / warm-state / streaming /
                # telemetry reports. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_fabric_status(
                        reason="dashboard_api_fabric")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"fabric unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/knowledge":
                # Program T — Sovereign knowledge engine. Aggregates
                # ingestion / fabric / governor / synthesis / recall
                # reports. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_knowledge_status(
                        reason="dashboard_api_knowledge")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"knowledge unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/deliberation":
                # Program U — Sovereign simulation engine.
                # Aggregates simulation / scorer / counterfactual /
                # preemption / decision-engine reports plus the
                # most recent simulations. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_deliberation_status(
                        reason="dashboard_api_deliberation")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"deliberation unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/reflection":
                # Program V — Reflective metacognition + verifier.
                # Aggregates reflective-state / contradiction /
                # calibrator / verifier / epistemic-discipline
                # reports plus the most recent reflections.
                # Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_reflection_status(
                        reason="dashboard_api_reflection")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"reflection unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/dialogue":
                # Program W — Sovereign dialogue mastery +
                # relationship continuity. Aggregates dialogue
                # state / intent / continuity / style adapter /
                # strategy reports plus recent dialogue states.
                # Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_dialogue_status(
                        reason="dashboard_api_dialogue")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"dialogue unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/capability_foundry":
                # Program X — Sovereign capability foundry.
                # Aggregates gap detector / spec / synthesis /
                # validation / governor / registry reports plus
                # recent registry rows and sandbox file listing.
                # Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_capability_foundry_status(
                        reason="dashboard_api_capability_foundry")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"capability_foundry unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/kernel":
                # Programs Y + Z — Sovereign unified cognitive
                # kernel + drive-mode. Aggregates state_bus /
                # lifecycle / router / doctrine / kernel reports
                # (Y) and stage_handlers / drive_engine /
                # budget_governor reports (Z) + last 8 KernelState
                # records + last 8 turn audit rows showing drive
                # vs legacy mode. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    status_y = _oc.luna_kernel_status(
                        reason="dashboard_api_kernel")
                    recent_y = _oc.luna_kernel_recent(
                        limit=8,
                        reason="dashboard_api_kernel")
                    status_z = _oc.luna_kernel_drive_status(
                        reason="dashboard_api_kernel")
                    payload = {
                        "ok": True,
                        "components": {
                            **(status_y.get("components") or {}),
                            **(status_z.get("components") or {}),
                        },
                        "recent_rows": recent_y.get("rows"),
                        "recent_count":
                            recent_y.get("count"),
                        "drive_mode_components":
                            status_z.get("components"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"kernel unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/goals":
                # Program AA — Long-horizon goals. Aggregates
                # state / progress / drift / advisor + lists
                # active goals + recent goal events + drift
                # flags + surfacing budget posture. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_goal_state as _gs  # type: ignore
                    status = _oc.luna_goal_status(
                        reason="dashboard_api_goals")
                    active = _gs.list_goals(
                        status="active", limit=32)
                    recent = _gs.latest_recent(limit=8)
                    payload = {
                        "ok": True,
                        "components":
                            status.get("components"),
                        "active_goals": active,
                        "recent_goals": recent,
                        "active_count": len(active),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"goals unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/outcomes":
                # Program BB — Self-evaluation + outcome
                # scoring. Aggregates state / scoring /
                # attribution / goal_eval / governor + lists
                # recent scored turns + promotion/caution
                # counts + bridge records. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_outcome_score_state as _oss  # type: ignore
                    from luna_modules import cognitive_outcome_adaptation_bridge as _br  # type: ignore
                    status = _oc.luna_outcome_status(
                        reason="dashboard_api_outcomes")
                    recent = _oss.latest_recent(limit=8)
                    rep = _oss.report()
                    bridge_rep = _br.report()
                    bridge_recent = _br.latest_recent(
                        limit=8)
                    payload = {
                        "ok": True,
                        "components":
                            status.get("components"),
                        "recent_outcomes": recent,
                        "summary_by_label":
                            rep.get("by_label"),
                        "summary_by_goal_effect":
                            rep.get("by_goal_effect"),
                        "promotion_count":
                            rep.get("promotion_count"),
                        "avoid_pattern_count":
                            rep.get("avoid_pattern_count"),
                        "bridge_report": bridge_rep,
                        "bridge_recent_records":
                            bridge_recent,
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"outcomes unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/pattern_consumer":
                # Program EE — Pattern advisor consumer.
                # Aggregates 6 EE component reports + recent
                # hints + counts by adapter + recent audit
                # events. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_pattern_consumer_state as _pcs  # type: ignore
                    from luna_modules import cognitive_pattern_consumer_audit as _pca  # type: ignore
                    status = (_oc
                                .luna_pattern_consumer_status(
                                    reason=(
                                        "dashboard_api_"
                                        "pattern_consumer")))
                    recent_hints = _pcs.latest_recent(
                        limit=12)
                    recent_audit = _pca.recent(limit=16)
                    rep = _pcs.report()
                    payload = {
                        "ok": True,
                        "components":
                            status.get("components"),
                        "recent_hints": recent_hints,
                        "recent_audit_events":
                            recent_audit,
                        "by_adapter":
                            rep.get("by_adapter"),
                        "by_state":
                            rep.get("by_state"),
                        "total_hints":
                            rep.get("total_hints"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"pattern_consumer unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/execution_packing":
                # Program KK — Execution packing + prompt
                # assembly. Aggregates 5 KK component reports
                # + latest packed window + recent audit rows +
                # counts.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_execution_packed_state as _eps  # type: ignore
                    from luna_modules import cognitive_execution_packing_audit as _epa  # type: ignore
                    status_kk = (_oc
                                    .luna_execution_packing_status(
                                        reason=(
                                            "dashboard_api_"
                                            "execution_packing")))
                    window = _eps.latest_window()
                    recent_audit = _epa.recent(limit=32)
                    counts = _epa.counts_by_event(
                        limit=500)
                    payload = {
                        "ok": True,
                        "components":
                            status_kk.get("components"),
                        "latest_window": window,
                        "recent_audit_events":
                            recent_audit,
                        "counts_by_event": counts,
                        "runtime_use_enabled":
                            status_kk.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_kk.get("paused"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"execution_packing unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/task_plans":
                # Program LL — Sovereign task decomposition +
                # multi-step plan stitching. Aggregates 5 LL
                # component reports + active plan list +
                # active-plan progress snapshots.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_task_plan_state as _tps  # type: ignore
                    from luna_modules import cognitive_plan_progress_tracker as _ppt  # type: ignore
                    status_ll = (_oc
                                    .luna_task_planning_status(
                                        reason=(
                                            "dashboard_api_"
                                            "task_plans")))
                    plans = _tps.list_plans(limit=12)
                    active_progress = (
                        _ppt.snapshot_all_active(limit=12))
                    payload = {
                        "ok": True,
                        "components":
                            status_ll.get("components"),
                        "plans": plans,
                        "active_progress":
                            active_progress,
                        "runtime_use_enabled":
                            status_ll.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_ll.get("paused"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"task_plans unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/step_execution":
                # Program MM — Sovereign step-execution
                # orchestrator. Aggregates 5 MM component reports +
                # active executions + recent audit + counts +
                # top refusal reasons.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_step_execution_state as _ses  # type: ignore
                    from luna_modules import cognitive_step_execution_audit as _sea  # type: ignore
                    status_mm = (_oc
                                    .luna_step_execution_status(
                                        reason=(
                                            "dashboard_api_"
                                            "step_execution")))
                    active = _ses.list_active_executions(
                        limit=24)
                    recent_audit = _sea.recent(limit=32)
                    counts = _sea.counts_by_event(
                        limit=500)
                    refusal_top = (
                        _sea.top_refusal_reasons(
                            limit=500))
                    payload = {
                        "ok": True,
                        "components":
                            status_mm.get("components"),
                        "active_executions": active,
                        "recent_audit_events":
                            recent_audit,
                        "counts_by_event": counts,
                        "top_refusal_reasons":
                            refusal_top,
                        "runtime_use_enabled":
                            status_mm.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_mm.get("paused"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"step_execution unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/step_actions":
                # Program NN — Sovereign step action table +
                # bounded tool dispatch. Aggregates NN component
                # reports + recent action events + counts by
                # event/kind + top refusal reasons + active LL
                # step-action posture from operator wrapper.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_action_dispatch_audit as _sad  # type: ignore
                    status_nn = (_oc
                                    .luna_step_action_status(
                                        reason=(
                                            "dashboard_api_"
                                            "step_actions")))
                    recent_audit = _sad.recent(limit=32)
                    counts = _sad.counts_by_event(
                        limit=500)
                    by_kind = (
                        _sad.counts_by_action_kind(
                            limit=500))
                    refusal_top = (
                        _sad.top_refusal_reasons(
                            limit=500))
                    payload = {
                        "ok": True,
                        "components":
                            status_nn.get("components"),
                        "recent_audit_events":
                            recent_audit,
                        "counts_by_event": counts,
                        "counts_by_action_kind":
                            by_kind,
                        "top_refusal_reasons":
                            refusal_top,
                        "runtime_use_enabled":
                            status_nn.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_nn.get("paused"),
                        "feedback_to_mm_enabled":
                            status_nn.get(
                                "feedback_to_mm_enabled"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"step_actions unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/policy_shaping":
                # Program OO — Outcome-to-action learning +
                # bounded execution policy shaping. Aggregates OO
                # component reports + recent shaping events +
                # active overrides + counts + top refusal
                # reasons.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_policy_shaping_audit as _psa  # type: ignore
                    from luna_modules import cognitive_action_policy_shaper as _aps  # type: ignore
                    status_oo = (_oc
                                    .luna_policy_shaping_status(
                                        reason=(
                                            "dashboard_api_"
                                            "policy_shaping")))
                    recent_audit = _psa.recent(limit=32)
                    counts = _psa.counts_by_event(
                        limit=500)
                    by_target = _psa.counts_by_target(
                        limit=500)
                    refusal_top = (
                        _psa.top_refusal_reasons(
                            limit=500))
                    overrides = _aps.get_overrides()
                    payload = {
                        "ok": True,
                        "components":
                            status_oo.get("components"),
                        "recent_audit_events":
                            recent_audit,
                        "counts_by_event": counts,
                        "counts_by_target": by_target,
                        "top_refusal_reasons":
                            refusal_top,
                        "overrides": overrides,
                        "runtime_use_enabled":
                            status_oo.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_oo.get("paused"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"policy_shaping unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/execution_memory":
                # Program PP — Long-horizon execution memory +
                # strategy consolidation. Aggregates PP component
                # reports + promoted strategies + recent events +
                # counts + top refusal reasons + advisor recall.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_execution_memory_audit as _ema  # type: ignore
                    from luna_modules import cognitive_execution_memory_state as _ems  # type: ignore
                    from luna_modules import cognitive_strategy_advisor as _adv  # type: ignore
                    status_pp = (_oc
                                    .luna_execution_memory_status(
                                        reason=(
                                            "dashboard_api_"
                                            "execution_memory")))
                    recent_audit = _ema.recent(limit=32)
                    counts = _ema.counts_by_event(
                        limit=500)
                    by_kind = _ema.counts_by_kind(
                        limit=500)
                    refusal_top = (
                        _ema.top_refusal_reasons(
                            limit=500))
                    promoted = _ems.list_strategies(
                        strategy_state="promoted",
                        limit=24)
                    advisor = _adv.recommend_strategy(
                        plan_kind=None, limit=5)
                    payload = {
                        "ok": True,
                        "components":
                            status_pp.get("components"),
                        "promoted_strategies": promoted,
                        "recent_audit_events":
                            recent_audit,
                        "counts_by_event": counts,
                        "counts_by_kind": by_kind,
                        "top_refusal_reasons":
                            refusal_top,
                        "advisor_recommendation":
                            advisor,
                        "runtime_use_enabled":
                            status_pp.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_pp.get("paused"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"execution_memory unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/working_memory":
                # Program JJ — Working-memory + attention
                # budgeting. Aggregates 5 JJ component reports +
                # active slots + recent audit rows + counts.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_working_memory_state as _wms  # type: ignore
                    from luna_modules import cognitive_working_memory_audit as _wma  # type: ignore
                    status_jj = (_oc
                                    .luna_working_memory_status(
                                        reason=(
                                            "dashboard_api_"
                                            "working_memory")))
                    active_slots = _wms.list_active_slots(
                        limit=24)
                    recent_audit = _wma.recent(limit=32)
                    counts = _wma.counts_by_event(
                        limit=500)
                    rep = _wms.report()
                    payload = {
                        "ok": True,
                        "components":
                            status_jj.get("components"),
                        "active_slots": active_slots,
                        "active_by_kind":
                            (rep or {}).get(
                                "active_by_kind"),
                        "active_slot_count":
                            (rep or {}).get(
                                "active_slot_count"),
                        "demoted_recently_count":
                            (rep or {}).get(
                                "demoted_recently_count"),
                        "recent_audit_events":
                            recent_audit,
                        "counts_by_event": counts,
                        "runtime_use_enabled":
                            status_jj.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_jj.get("paused"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"working_memory unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/context_compression":
                # Program II — Context compression + cross-session
                # recall. Aggregates 5 II component reports +
                # recent compressed units + counts by state/kind +
                # current paused posture. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_context_compression_state as _ccs  # type: ignore
                    status_ii = (_oc
                                    .luna_context_compression_status(
                                        reason=(
                                            "dashboard_api_"
                                            "context_compression")))
                    recent_units = _ccs.list_units(limit=24)
                    rep = _ccs.report()
                    payload = {
                        "ok": True,
                        "components":
                            status_ii.get("components"),
                        "recent_units": recent_units,
                        "by_state":
                            (rep or {}).get("by_state"),
                        "by_kind":
                            (rep or {}).get("by_kind"),
                        "total_units":
                            (rep or {}).get(
                                "total_units"),
                        "max_units":
                            (rep or {}).get("max_units"),
                        "runtime_use_enabled":
                            status_ii.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_ii.get("paused"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"context_compression unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/model_selection":
                # Program HH — Model selection + quality-tier
                # orchestration. Aggregates 4 HH component reports
                # + tier registry snapshot + recent routing
                # decisions + counts by decision + current
                # paused posture. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_tier_selection_audit as _tsa  # type: ignore
                    from luna_modules import cognitive_quality_tier_registry as _qtr  # type: ignore
                    status_hh = (_oc
                                    .luna_model_selection_status(
                                        reason=(
                                            "dashboard_api_"
                                            "model_selection")))
                    recent_decisions = _tsa.recent(limit=32)
                    counts = _tsa.counts_by_decision(
                        limit=500)
                    tier_snapshot = _qtr.list_tiers()
                    payload = {
                        "ok": True,
                        "components":
                            status_hh.get("components"),
                        "tier_registry_snapshot":
                            tier_snapshot,
                        "recent_decisions":
                            recent_decisions,
                        "counts_by_decision": counts,
                        "runtime_use_enabled":
                            status_hh.get(
                                "runtime_use_enabled"),
                        "paused":
                            status_hh.get("paused"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"model_selection unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/meta_policy":
                # Program GG — Meta-policy learning + threshold
                # refinement. Aggregates 5 GG component reports
                # + recent proposals + recent audit rows +
                # counts by event + current mutable-knob values
                # + auto-apply posture. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_meta_policy_audit as _mpa  # type: ignore
                    from luna_modules import cognitive_meta_policy_proposal_state as _mpps  # type: ignore
                    status_gg = (_oc
                                    .luna_meta_policy_status(
                                        reason=(
                                            "dashboard_api_"
                                            "meta_policy")))
                    recent_props = _mpps.list_proposals(
                        limit=16)
                    recent_audit = _mpa.recent(limit=32)
                    counts = _mpa.counts_by_event(limit=500)
                    payload = {
                        "ok": True,
                        "components":
                            status_gg.get("components"),
                        "mutable_knob_values":
                            status_gg.get(
                                "mutable_knob_values"),
                        "auto_apply_enabled":
                            status_gg.get(
                                "auto_apply_enabled"),
                        "recent_proposals": recent_props,
                        "recent_audit_events":
                            recent_audit,
                        "counts_by_event": counts,
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"meta_policy unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/pattern_consumption":
                # Program FF — Live pattern consumption.
                # Aggregates the 5 FF component reports + recent
                # audit rows + governor 24h status. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_pattern_consumption_governor as _ffg  # type: ignore
                    from luna_modules import cognitive_pattern_consumption_audit as _ffa  # type: ignore
                    status_ff = (_oc
                                    .luna_pattern_consumption_status(
                                        reason=(
                                            "dashboard_api_"
                                            "pattern_consumption")))
                    gov_status = _ffg.status()
                    recent_audit = _ffa.recent(limit=24)
                    payload = {
                        "ok": True,
                        "components":
                            status_ff.get("components"),
                        "governor_status": gov_status,
                        "recent_audit_events":
                            recent_audit,
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"pattern_consumption unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/patterns":
                # Program DD — Multi-turn pattern mining.
                # Aggregates 5 DD component reports + recent
                # patterns + counts by kind + scan window
                # summary. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_pattern_state as _ps  # type: ignore
                    from luna_modules import cognitive_pattern_miner as _pm  # type: ignore
                    status = _oc.luna_pattern_status(
                        reason="dashboard_api_patterns")
                    recent_rows = _ps.latest_recent(
                        limit=12)
                    rep = _ps.report()
                    # Lightweight scan summary (read-only mine
                    # would touch caches but stay bounded). We
                    # do NOT auto-promote here — that requires
                    # the operator wrapper or reflect-cadence.
                    mine_lite = _pm.mine_once() \
                        if hasattr(_pm, "mine_once") \
                        else {"ok": False}
                    payload = {
                        "ok": True,
                        "components":
                            status.get("components"),
                        "recent_patterns": recent_rows,
                        "by_kind": rep.get("by_kind"),
                        "by_state":
                            rep.get("by_state"),
                        "total_patterns":
                            rep.get("total_patterns"),
                        "scan_window_summary":
                            mine_lite.get(
                                "scan_window_summary"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"patterns unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/bridge_consumer":
                # Program CC — R-side bridge consumer +
                # bridge-derived evidence. Aggregates 5 CC
                # component reports + recent derived records +
                # recent audit events. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    from luna_modules import cognitive_bridge_derived_evidence as _bde  # type: ignore
                    from luna_modules import cognitive_bridge_consumer_audit as _baud  # type: ignore
                    status = _oc.luna_bridge_consumer_status(
                        reason=(
                            "dashboard_api_bridge_consumer"))
                    recent_derived = _bde.latest_recent(
                        limit=8)
                    recent_audit = _baud.recent(limit=16)
                    rep = _bde.report()
                    payload = {
                        "ok": True,
                        "components":
                            status.get("components"),
                        "recent_derived_records":
                            recent_derived,
                        "recent_audit_events":
                            recent_audit,
                        "by_state":
                            rep.get("by_state"),
                        "by_category":
                            rep.get("by_category"),
                        "total_records":
                            rep.get("total_records"),
                    }
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"bridge_consumer unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/perception":
                # Program P — Multimodal perception. Aggregates the
                # 4 perception surfaces (screen, document, audio,
                # world_model). Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_perception_status(
                        reason="dashboard_api_perception")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"perception unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/workflow":
                # Program O — Bounded Workflow Operator. Aggregates
                # state/planner/executor/recovery reports + lists the
                # most recent workflows. Read-only.
                try:
                    from luna_modules import cognitive_operator_controls as _oc  # type: ignore
                    payload = _oc.luna_workflow_status(
                        reason="dashboard_api_workflow")
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "ok": False,
                        "error": f"workflow unavailable: {type(exc).__name__}: {exc}",
                    }
            elif path == "/api/cognitive/learning":
                # Program N — Learned Continuity stack. Aggregates the
                # 5 learning surfaces (outcome memory, skill traces,
                # failure replay, preferences, consolidations) into
                # one operator-readable endpoint. Read-only.
                payload = {"ok": True}
                for key, modname in (
                    ("outcome_memory",
                     "luna_modules.cognitive_outcome_memory"),
                    ("skill_traces",
                     "luna_modules.cognitive_skill_traces"),
                    ("failure_replay",
                     "luna_modules.cognitive_failure_replay"),
                    ("preferences",
                     "luna_modules.cognitive_preference_learner"),
                    ("consolidations",
                     "luna_modules.cognitive_memory_consolidator"),
                ):
                    try:
                        mod = __import__(modname, fromlist=["report"])
                        payload[key] = mod.report()
                    except Exception as exc:  # noqa: BLE001
                        payload[key] = {
                            "available": False,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
            else:
                self.send_error(HTTPStatus.NOT_FOUND, "unknown api endpoint")
                return
        except Exception as exc:  # noqa: BLE001
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"payload error: {type(exc).__name__}")
            return
        # 2026-05-13 HARD CUTOVER — annotate legacy primary endpoints so
        # the frontend contract tests can detect any primary panel still
        # pulling current truth from them. Primary truth comes from
        # /api/terminal-truth ONLY; these stay alive for auxiliary detail.
        _LEGACY_DETAIL_PATHS = {
            "/api/mission-control",
            "/api/agent-bus",
            "/api/tier-adoption",
            "/api/tier-graduation",
            "/api/higher-tier/progress",
            "/api/tier-truth",
            "/api/operator-truth",
            "/api/master-status",
            "/api/supermax",
            "/api/canonical-truth",
        }
        if isinstance(payload, dict) and path in _LEGACY_DETAIL_PATHS:
            payload.setdefault("LEGACY_FOR_DETAIL_ONLY", True)
            payload.setdefault(
                "primary_truth_route",
                "/api/terminal-truth"
            )
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Connection", "close")
        self.close_connection = True
        try:
            self.end_headers()
        except _SOCKET_DIED:
            try:
                self.close_connection = True
            except Exception:  # noqa: BLE001
                pass
            return
        if write_body:
            # 2026-05-13 Ctrl+F5 hardening: raw write used to raise
            # BrokenPipeError on hard refresh and propagate via send_error
            # retry into the broad-Exception path. Local tight swallow
            # keeps the request thread clean and the server alive.
            try:
                self.wfile.write(body)
            except _SOCKET_DIED:
                try:
                    self.close_connection = True
                except Exception:  # noqa: BLE001
                    pass
                return


SOLUTIONS_DIR = PROJECT_ROOT / "solutions"
_TASK_ID_RE = re.compile(r"^task_\d{8}_\d{6}_[A-Za-z0-9]{3,16}$")


# ---------------------------------------------------------------------------
# File Explorer (read-only file browsing under whitelisted roots)
# ---------------------------------------------------------------------------
def _allowed_fs_roots() -> list[Path]:
    """Roots the dashboard's File Explorer is permitted to enumerate.

    Local-only dashboard, so user-home + the project drive is fine. We
    deliberately do NOT include the system drive (C:\\) at the root level
    to avoid surfacing system files casually.
    """
    home = Path.home()
    roots: list[Path] = []
    candidates = [home, Path("D:\\")]
    for c in candidates:
        try:
            if c.exists():
                roots.append(c.resolve())
        except OSError:
            continue
    return roots


def _is_under_allowed_root(p: Path) -> bool:
    try:
        rp = p.resolve()
    except OSError:
        return False
    for root in _allowed_fs_roots():
        try:
            rp.relative_to(root)
            return True
        except ValueError:
            continue
        if rp == root:
            return True
    return False


def build_files_roots_payload() -> dict[str, Any]:
    """Return the predefined Quick-access nav tree for the File Explorer."""
    home = Path.home()
    roots: list[dict[str, Any]] = []

    def _add(group: str, name: str, path: Path, icon: str) -> None:
        try:
            if path.exists():
                roots.append({"name": name, "path": str(path), "icon": icon, "group": group})
        except OSError:
            pass

    _add("pinned", "Desktop",     home / "Desktop",     "desktop")
    _add("pinned", "Documents",   home / "Documents",   "doc")
    _add("pinned", "Downloads",   home / "Downloads",   "download")
    _add("pinned", "Pictures",    home / "Pictures",    "image")
    _add("pinned", "Videos",      home / "Videos",      "video")
    _add("pinned", "Music",       home / "Music",       "audio")

    _add("project", "SurgeApp",        PROJECT_ROOT,                  "luna")
    _add("project", "tasks/active",    PROJECT_ROOT / "tasks" / "active",  "folder")
    _add("project", "tasks/done",      PROJECT_ROOT / "tasks" / "done",    "folder")
    _add("project", "memory",          PROJECT_ROOT / "memory",       "folder")
    _add("project", "logs",            PROJECT_ROOT / "logs",         "folder")
    _add("project", "solutions",       PROJECT_ROOT / "solutions",    "folder")

    _add("pc", "User home", home,        "drive")
    _add("pc", "D:\\",      Path("D:\\"), "drive")

    return {"ok": True, "roots": roots}


def _norm_drive_letter(p: str) -> str:
    """``d:`` → ``D:\\``, ``D:/foo`` → ``D:\\foo`` (best-effort path normaliser)."""
    p = (p or "").strip().strip('"').strip("'")
    if not p:
        return p
    # Bare drive letter
    if len(p) == 2 and p[1] == ":":
        return p[0].upper() + ":\\"
    return p


def build_files_list_payload(raw_path: str) -> dict[str, Any]:
    """List one directory. Locked to whitelisted roots; no path traversal."""
    raw_path = _norm_drive_letter(raw_path)
    if not raw_path:
        target = PROJECT_ROOT
    else:
        try:
            target = Path(raw_path)
        except (OSError, ValueError):
            return {"ok": False, "error": "bad path"}
    try:
        target = target.resolve()
    except OSError:
        return {"ok": False, "error": "bad path"}
    if not _is_under_allowed_root(target):
        return {"ok": False, "error": "path not allowed"}
    if not target.exists():
        return {"ok": False, "error": "not found"}
    if not target.is_dir():
        return {"ok": False, "error": "not a directory"}

    entries: list[dict[str, Any]] = []
    try:
        items = list(target.iterdir())
    except OSError as exc:
        return {"ok": False, "error": f"read error: {exc.__class__.__name__}"}

    for p in items:
        try:
            st = p.stat()
        except OSError:
            continue
        try:
            is_dir = p.is_dir()
        except OSError:
            is_dir = False
        try:
            hidden = p.name.startswith(".")
            attrs = getattr(st, "st_file_attributes", 0)
            if attrs:
                # FILE_ATTRIBUTE_HIDDEN = 0x2, FILE_ATTRIBUTE_SYSTEM = 0x4
                if attrs & 0x2 or attrs & 0x4:
                    hidden = True
        except Exception:  # noqa: BLE001
            hidden = False
        ext = "" if is_dir else (p.suffix.lstrip(".").lower())
        entries.append({
            "name": p.name,
            "path": str(p),
            "is_dir": is_dir,
            "type": ext if not is_dir else "",
            "size": 0 if is_dir else int(st.st_size or 0),
            "mtime": float(st.st_mtime or 0),
            "hidden": hidden,
        })

    parent = None
    try:
        if target.parent != target and _is_under_allowed_root(target.parent):
            parent = str(target.parent)
    except (OSError, ValueError):
        pass

    return {
        "ok": True,
        "path": str(target),
        "parent": parent,
        "entries": entries,
        "count": len(entries),
    }


def _read_chat_task_runtime(task_id: str) -> dict[str, Any]:
    """Return visible runtime details for a command-console task."""
    if not _TASK_ID_RE.match(task_id or ""):
        return {}
    candidates = [
        TASKS_ACTIVE_DIR / (task_id + ".json"),
        TASKS_ACTIVE_DIR / (task_id + ".working.json"),
        TASKS_DIR / "done" / (task_id + ".json"),
        TASKS_DIR / "done" / (task_id + ".working.json"),
        TASKS_DIR / "failed" / (task_id + ".json"),
        TASKS_DIR / "failed" / (task_id + ".working.json"),
    ]
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
            resolved.relative_to(TASKS_DIR.resolve())
            if not resolved.exists() or not resolved.is_file():
                continue
            payload = json.loads(resolved.read_text(encoding="utf-8", errors="replace") or "{}")
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        steps = payload.get("visible_steps")
        if not isinstance(steps, list):
            steps = []
        safe_steps = []
        for step in steps[-12:]:
            if isinstance(step, dict):
                safe_steps.append(
                    {
                        "ts": str(step.get("ts") or "")[:40],
                        "phase": str(step.get("phase") or "")[:80],
                        "progress": int(step.get("progress") or 0),
                        "text": str(step.get("text") or "")[:240],
                    }
                )
        return {
            "task_status": str(payload.get("status") or payload.get("state") or "")[:40],
            "phase": str(payload.get("phase") or "")[:80],
            "progress": int(payload.get("progress") or 0),
            "visible_steps": safe_steps,
        }
    return {}


def build_chat_response_payload(task_id: str) -> dict[str, Any]:
    """Return Luna's textual reply for ``task_id`` if the worker has written it.

    Reads ``solutions/<task_id>.txt`` (whitelisted by strict id format) and
    strips the leading ``# LUNA QUALITY REPORT`` header so the UI sees only
    the reply body. Returns ``ok=False, ready=False`` if the reply hasn't
    landed yet — the UI polls.
    """
    if not _TASK_ID_RE.match(task_id or ""):
        return {"ok": False, "ready": False, "error": "invalid task id"}
    runtime = _read_chat_task_runtime(task_id)
    candidate = (SOLUTIONS_DIR / (task_id + ".txt")).resolve()
    try:
        candidate.relative_to(SOLUTIONS_DIR.resolve())
    except ValueError:
        return {"ok": False, "ready": False, "error": "path escape rejected"}
    if not candidate.exists() or not candidate.is_file():
        return {"ok": True, "ready": False, **runtime}
    try:
        text = candidate.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {"ok": True, "ready": False, **runtime}
    # Strip the leading "# LUNA QUALITY REPORT" header (first contiguous block of "#" lines).
    lines = text.splitlines()
    body_start = 0
    for i, ln in enumerate(lines):
        if ln.startswith("#"):
            body_start = i + 1
        else:
            if body_start == 0:
                break
            if ln.strip() == "":
                body_start = i + 1
                continue
            break
    reply = "\n".join(lines[body_start:]).strip()
    return {"ok": True, "ready": True, "task_id": task_id, "reply": reply[:64000], **runtime}


# ---------------------------------------------------------------------------
# Chat write-lane (narrow exception to read-only)
# ---------------------------------------------------------------------------
# The Command Console drops user input into tasks/active/ — Luna's existing
# intake path. Hard limits: small JSON body, single file ≤ MAX_UPLOAD_BYTES,
# session id sanitised, no path traversal, no shell, no eval.

TASKS_DIR = PROJECT_ROOT / "tasks"
TASKS_ACTIVE_DIR = TASKS_DIR / "active"
TASKS_UPLOADS_DIR = TASKS_ACTIVE_DIR / "uploads"
MAX_CHAT_BODY_BYTES = 64 * 1024            # 64 KB per send
MAX_UPLOAD_BYTES = 8 * 1024 * 1024         # 8 MB per file
ALLOWED_PERM_MODES = {"ask", "bypass", "readonly", "sandbox", "council"}
_SESSION_RE = re.compile(r"^chat_[A-Za-z0-9_]{6,64}$")
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._\-]")


def _send_json(handler: BaseHTTPRequestHandler, status: HTTPStatus, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.send_header("X-Frame-Options", "DENY")
    handler.send_header("Referrer-Policy", "no-referrer")
    handler.send_header("Connection", "close")
    try:
        handler.close_connection = True
    except Exception:  # noqa: BLE001
        pass
    handler.end_headers()
    try:
        handler.wfile.write(body)
    except Exception:  # noqa: BLE001
        pass


def _safe_session_id(raw: str) -> str:
    raw = (raw or "").strip()
    if not _SESSION_RE.match(raw):
        # Coerce to a deterministic clean id rather than reject — UI may pass blank.
        return "chat_anon_" + datetime.now().strftime("%Y%m%d%H%M%S")
    return raw


def _safe_filename(raw: str) -> str:
    name = (raw or "file").strip().replace("\\", "/").rsplit("/", 1)[-1]
    name = _SAFE_NAME_RE.sub("_", name)[:120]
    if not name:
        name = "file"
    return name


def _conversation_turn_handle(handler: BaseHTTPRequestHandler) -> None:
    """Canonical live-chat handler for Luna Conversation Runtime V1.

    Body: ``{"message": "...", "session": "...",
              "want_premium_voice": true, "allow_audible": true}``

    Returns the full turn record from cognitive_conversation_runtime:
    classification, ack (text/dynamic/backend/latency/audible),
    main_reply (text/brain_backend/voice_backend/latency), route_summary.
    """
    if not _check_loopback(handler):
        return
    try:
        try:
            length = int(handler.headers.get("Content-Length") or "0")
        except (TypeError, ValueError):
            length = 0
        if length <= 0 or length > MAX_CHAT_BODY_BYTES:
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": "invalid body size"})
            return
        raw = handler.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": "invalid json"})
            return
        if not isinstance(data, dict):
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": "expected object"})
            return
        message = str(data.get("message") or "").strip()
        session_id = _safe_session_id(str(data.get("session") or "default"))
        want_premium = bool(data.get("want_premium_voice", True))
        allow_audible = bool(data.get("allow_audible", True))
        if not message:
            _send_json(handler, HTTPStatus.BAD_REQUEST,
                       {"ok": False, "error": "empty message"})
            return
        try:
            from luna_modules import cognitive_conversation_runtime as _cr  # type: ignore
            result = _cr.handle_turn(
                message,
                session_id=session_id,
                caller="dashboard:/api/conversation/turn",
                want_premium_voice=want_premium,
                allow_audible=allow_audible,
            )
            _send_json(handler, HTTPStatus.OK, result)
        except Exception as exc:  # noqa: BLE001
            _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {
                "ok": False,
                "error": f"conversation runtime failure: {type(exc).__name__}: {exc}",
            })
    except Exception as exc:  # noqa: BLE001
        try:
            _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {
                "ok": False, "error": f"handler raised: {type(exc).__name__}: {exc}",
            })
        except Exception:  # noqa: BLE001
            pass


def _chat_handle_send(handler: BaseHTTPRequestHandler) -> None:
    trace_id = None
    _chat_span_cm = None
    try:
        from luna_modules import luna_otel as _chat_ot
        _chat_span_cm = _chat_ot.start_span(
            "dashboard.chat.send",
            {"route": "/api/chat/send"},
            kind="SERVER",
        )
        _chat_span = _chat_span_cm.__enter__()
        trace_id = _chat_span.get("trace_id")
    except Exception:  # noqa: BLE001
        _chat_span_cm = None
        trace_id = None

    # Parse JSON body with a strict size cap.
    try:
        try:
            length = int(handler.headers.get("Content-Length") or "0")
        except (TypeError, ValueError):
            length = 0
        if length <= 0 or length > MAX_CHAT_BODY_BYTES:
            _send_json(handler, HTTPStatus.BAD_REQUEST, {
                "ok": False, "error": "invalid body size", "trace_id": trace_id
            })
            return
        raw = handler.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            _send_json(handler, HTTPStatus.BAD_REQUEST, {
                "ok": False, "error": "invalid json", "trace_id": trace_id
            })
            return
        if not isinstance(data, dict):
            _send_json(handler, HTTPStatus.BAD_REQUEST, {
                "ok": False, "error": "expected object", "trace_id": trace_id
            })
            return

        session_id = _safe_session_id(str(data.get("session") or ""))
        message = str(data.get("message") or "").strip()
        perm = str(data.get("perm_mode") or "ask").strip()
        if perm not in ALLOWED_PERM_MODES:
            perm = "ask"
        attachments_in = data.get("attachments") or []
        if not isinstance(attachments_in, list):
            attachments_in = []
        if not message and not attachments_in:
            _send_json(handler, HTTPStatus.BAD_REQUEST, {
                "ok": False, "error": "empty message", "trace_id": trace_id
            })
            return
        # Attachment-only drops: synthesise a default prompt so worker.py
        # never receives an empty prompt (which would fail with
        # "ValueError: empty or missing prompt in task payload").
        if not message and attachments_in:
            names = [str((a or {}).get("name") or "")
                     for a in attachments_in if isinstance(a, dict)]
            names = [n for n in names if n][:5]
            if names:
                message = "Please review the attached file(s): " + ", ".join(names)
            else:
                message = "Please review the attached files."

        # Allocate a task id up front so the fast path can reference it.
        now = datetime.now()
        task_id = "task_" + now.strftime("%Y%m%d_%H%M%S") + "_" + os.urandom(3).hex()

    # ==================================================================
    # Core Brain FAST PATH (per 2026-05-12 chat-latency fix)
    # ==================================================================
    # Read-only audit-guarded questions (tier-status, agent-bus
    # communication, "what lessons have been learned") answer
    # synchronously via luna_core_brain.answer_fast — no worker queue, no
    # LLM, no solutions/ disk write. Typical latency: <100 ms.
    #
    # Strictly limited to:
    #   * pure text messages (no attachments — those need real worker
    #     processing including vision)
    #   * questions for which luna_core_brain has a dedicated handler
    #     (is_tier_question OR is_agent_bus_question)
    #
    # Any failure in the fast path silently falls through to the
    # normal queue, so a broken brain cannot block chat — it just
    # makes chat slow again.
        if message and not attachments_in:
            try:
                from luna_modules import luna_core_brain as _brain
                if _brain.is_tier_question(message) or _brain.is_agent_bus_question(message):
                    fast = _brain.answer_fast(message)
                    if isinstance(fast, dict) and fast.get("handled") and fast.get("answer"):
                        _send_json(handler, HTTPStatus.OK, {
                            "ok": True,
                            "task_id": task_id,
                            "ack": f"Answered via Core Brain fast path · perm={perm}",
                            "perm_mode": perm,
                            "fast_path": True,
                            "answer": str(fast.get("answer") or ""),
                            "category": fast.get("category"),
                            "route": fast.get("route"),
                            "proof_chain_status": fast.get("proof_chain_status"),
                            "may_claim_active": fast.get("may_claim_active"),
                            "sources": list(fast.get("sources") or []),
                            "trace_id": trace_id,
                        })
                        return
            except Exception:  # noqa: BLE001
                # Fast path is best-effort; any failure falls through to the
                # normal worker queue below. Never block chat on the brain.
                pass

        # =====================================================================
        # LIVE CONVERSATION FAST PATH (2026-06-02)
        # =====================================================================
        # Route all conversational messages directly to the sovereign 8B GPU
        # brain via cognitive_operator_controls.luna_conversation_turn(). This
        # returns a proper Luna reply and triggers server-side voice synthesis
        # (XTTS clone) so the operator can both see AND hear the response.
        # The browser's fast_path=True / answer= path renders the text
        # immediately without polling the worker queue at all.
        #
        # Attachments still go to the worker (vision processing). Any failure
        # silently falls through to the slow path so chat never hard-breaks.
        if message and not attachments_in:
            try:
                from luna_modules import cognitive_operator_controls as _conv_oc  # type: ignore
                _conv_result = _conv_oc.luna_conversation_turn(message)
                # handle_turn returns {"main_reply": {"text": "..."}, ...}
                # OR {"text": "..."} depending on the operator wrapper depth.
                _main = _conv_result.get("main_reply") or {}
                _conv_text = (
                    str(_main.get("text") or "")
                    or str(_conv_result.get("text") or "")
                ).strip()
                if _conv_text:
                    _send_json(handler, HTTPStatus.OK, {
                        "ok": True,
                        "task_id": task_id,
                        "ack": "Luna replied",
                        "perm_mode": perm,
                        "fast_path": True,
                        "answer": _conv_text,
                        "trace_id": trace_id,
                    })
                    return
            except Exception:  # noqa: BLE001
                # Live path failed → fall through to slow worker queue
                pass

        # Slow path: queue a task file for worker.py to pick up.
        payload = {
            "id": task_id,
            "task_id": task_id,
            "prompt": message[:8000],
            "attachments": [
                {
                    "name": _safe_filename(str(a.get("name") or "")),
                    "size": int(a.get("size") or 0) if isinstance(a.get("size"), (int, float)) else 0,
                    "stored": str(a.get("stored") or ""),
                }
                for a in attachments_in
                if isinstance(a, dict)
            ][:20],
            "created_at": now.isoformat(timespec="seconds"),
            "supervisor": "Luna",
            "luna_profile": str((PROJECT_ROOT / "MyLuna.txt").resolve()),
            "source": "command_console",
            "chat_session": session_id,
            "perm_mode": perm,
            "trace_id": trace_id,
        }
        try:
            TASKS_ACTIVE_DIR.mkdir(parents=True, exist_ok=True)
            out = TASKS_ACTIVE_DIR / (task_id + ".json")
            # Resolve and verify the write target stays inside tasks/active/.
            resolved = out.resolve()
            try:
                resolved.relative_to(TASKS_ACTIVE_DIR.resolve())
            except ValueError:
                _send_json(handler, HTTPStatus.FORBIDDEN, {
                    "ok": False, "error": "path escape rejected", "trace_id": trace_id
                })
                return
            resolved.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        except OSError as exc:
            _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {
                "ok": False, "error": f"write failed: {exc.__class__.__name__}", "trace_id": trace_id
            })
            return

        _send_json(handler, HTTPStatus.OK, {
            "ok": True,
            "task_id": task_id,
            "ack": f"Queued · perm={perm}",
            "perm_mode": perm,
            "trace_id": trace_id,
        })
    finally:
        if _chat_span_cm is not None:
            try:
                _chat_span_cm.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass


def _chat_handle_upload(handler: BaseHTTPRequestHandler) -> None:
    # Multipart parsing using stdlib (no external deps). Strict size cap.
    try:
        length = int(handler.headers.get("Content-Length") or "0")
    except (TypeError, ValueError):
        length = 0
    if length <= 0 or length > MAX_UPLOAD_BYTES + 16 * 1024:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid upload size"})
        return
    ctype = handler.headers.get("Content-Type") or ""
    if "multipart/form-data" not in ctype:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "expected multipart/form-data"})
        return

    # Use email-style parser via cgi.FieldStorage for stdlib robustness.
    import cgi  # local import — only the chat lane needs it
    env = {
        "REQUEST_METHOD": "POST",
        "CONTENT_TYPE": ctype,
        "CONTENT_LENGTH": str(length),
    }
    try:
        form = cgi.FieldStorage(fp=handler.rfile, headers=handler.headers, environ=env, keep_blank_values=True)
    except Exception as exc:  # noqa: BLE001
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": f"multipart parse error: {exc.__class__.__name__}"})
        return

    session_id = _safe_session_id(form.getvalue("session", ""))
    item = form["file"] if "file" in form else None
    if item is None or not getattr(item, "filename", ""):
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "no file"})
        return
    fname = _safe_filename(item.filename)
    data = item.file.read(MAX_UPLOAD_BYTES + 1) if item.file else b""
    if len(data) > MAX_UPLOAD_BYTES:
        _send_json(handler, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"ok": False, "error": "file too large"})
        return

    try:
        sess_dir = TASKS_UPLOADS_DIR / session_id
        sess_dir.mkdir(parents=True, exist_ok=True)
        # collision-safe write: prefix with timestamp+rand
        prefix = datetime.now().strftime("%H%M%S") + "_" + os.urandom(2).hex() + "_"
        out = sess_dir / (prefix + fname)
        resolved = out.resolve()
        try:
            resolved.relative_to(TASKS_UPLOADS_DIR.resolve())
        except ValueError:
            _send_json(handler, HTTPStatus.FORBIDDEN, {"ok": False, "error": "path escape rejected"})
            return
        resolved.write_bytes(data)
    except OSError as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": f"store failed: {exc.__class__.__name__}"})
        return

    _send_json(handler, HTTPStatus.OK, {
        "ok": True,
        "name": fname,
        "size": len(data),
        "path": str(resolved),
    })


# ---------------------------------------------------------------------------
# Stage 2 - Luna Vision Link: snapshot description via local Ollama
# ---------------------------------------------------------------------------
# All processing is loopback-only. The endpoint POST /api/vision/describe
# takes a JSON body {session, file_path} where file_path is the absolute
# path returned by /api/chat/upload. Path-jailed to TASKS_UPLOADS_DIR.
# Selects the first vision-capable model on the local Ollama and calls
# /api/generate with the image base64-encoded. No external network calls.
# No model auto-pull. No new global state. Failures degrade gracefully so
# the chat lane (and Stage 1 webcam preview / Live Talk) keep working.

VISION_CONFIG_PATH      = PROJECT_ROOT / "memory" / "luna_vision_config.json"
VISION_OLLAMA_BASE      = os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434")
VISION_DEFAULT_TIMEOUT  = 30
VISION_TAGS_TIMEOUT     = 3.0
VISION_MODEL_PRIORITY   = (
    # Order: real vision models first, then permissive fallbacks. We never
    # auto-pull; if none of these exist on the local Ollama, the endpoint
    # responds with a clear "not installed" message.
    "llava:13b",
    "llava:latest",
    "llava",
    "llama3.2-vision:latest",
    "llama3.2-vision",
    "bakllava:latest",
    "bakllava",
    "moondream:latest",
    "moondream",
    "qwen2.5-vl:7b",
    "qwen2.5-vl",
    "gemma3:4b",   # Gemma 3 may accept images depending on the local Ollama tag.
    "gemma3",
)


def _read_vision_config() -> dict[str, Any]:
    """Read memory/luna_vision_config.json with safe defaults.

    Defaults: enabled=true, timeout=30s, max_bytes=8MB,
    prompt_prefix="Describe what you see in this image..."
    Operator can flip enabled=false to disable the endpoint entirely
    (returns 503) without code changes.
    """
    cfg: dict[str, Any] = {
        "enabled": True,
        "timeout_s": VISION_DEFAULT_TIMEOUT,
        "max_bytes": MAX_UPLOAD_BYTES,
        "prompt_prefix": "Describe what you see in this image. Be concise (one or two sentences).",
        "ollama_base": VISION_OLLAMA_BASE,
    }
    try:
        if VISION_CONFIG_PATH.exists():
            text = VISION_CONFIG_PATH.read_text(encoding="utf-8", errors="ignore")
            if text.strip():
                loaded = json.loads(text)
                if isinstance(loaded, dict):
                    cfg.update({k: v for k, v in loaded.items() if k in cfg})
    except Exception:  # noqa: BLE001
        pass
    return cfg


def _list_ollama_models(base: str, timeout_s: float = VISION_TAGS_TIMEOUT) -> list[str]:
    """Probe the local Ollama instance for installed model names.

    Loopback-only (the base URL is rejected if it is not 127.0.0.1 /
    localhost). Returns an empty list on any failure - callers must
    treat that as "no vision available" and fall back gracefully.
    """
    if not (base.startswith("http://127.0.0.1") or base.startswith("http://localhost")):
        return []
    try:
        url = base.rstrip("/") + "/api/tags"
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            if resp.status != 200:
                return []
            payload = json.loads(resp.read().decode("utf-8", errors="ignore") or "{}")
        out: list[str] = []
        for m in (payload.get("models") or []):
            name = str(m.get("name") or "").strip()
            if name:
                out.append(name)
        return out
    except Exception:  # noqa: BLE001
        return []


def _pick_vision_model(installed: list[str]) -> str | None:
    """Choose the best vision model from the installed list.

    Priority order is in VISION_MODEL_PRIORITY. Match either an exact
    name or a same-family prefix (e.g. 'llava' matches 'llava:7b').
    Returns None when no candidate is present so the caller can return
    a clean 'not installed' response.
    """
    if not installed:
        return None
    installed_set = set(installed)
    # Exact match first.
    for cand in VISION_MODEL_PRIORITY:
        if cand in installed_set:
            return cand
    # Family prefix match.
    families = {c.split(":")[0] for c in VISION_MODEL_PRIORITY}
    for name in installed:
        family = name.split(":")[0]
        if family in families:
            return name
    return None


def _call_ollama_vision(
    base: str,
    model: str,
    image_bytes: bytes,
    prompt: str,
    timeout_s: float,
) -> tuple[bool, str, str]:
    """Call Ollama /api/generate with a base64-encoded image.

    Returns (ok, text, error). ok=False with an error message describes
    common failure modes including "model does not accept image input"
    (which is what gemma3:4b would say if the local tag is the text-only
    build). All HTTP traffic is loopback. No external network call.
    """
    if not (base.startswith("http://127.0.0.1") or base.startswith("http://localhost")):
        return False, "", "ollama base must be loopback"
    try:
        b64 = base64.b64encode(image_bytes).decode("ascii")
    except Exception as exc:  # noqa: BLE001
        return False, "", f"base64 encode failed: {type(exc).__name__}"
    body = {
        "model": model,
        "prompt": prompt,
        "images": [b64],
        "stream": False,
    }
    try:
        url = base.rstrip("/") + "/api/generate"
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        payload = json.loads(raw or "{}")
        text = str(payload.get("response") or "").strip()
        if not text:
            # Some Ollama builds will refuse images on a text-only model
            # by returning a zero-length response with an error block.
            ollama_err = str(payload.get("error") or "").strip()
            if ollama_err:
                return False, "", f"ollama: {ollama_err[:240]}"
            return False, "", "vision model returned empty response (likely text-only model)"
        return True, text, ""
    except urllib.error.HTTPError as exc:
        body_excerpt = ""
        try:
            body_excerpt = exc.read().decode("utf-8", errors="ignore")[:240]
        except Exception:  # noqa: BLE001
            pass
        return False, "", f"http {exc.code}: {body_excerpt or exc.reason}"
    except urllib.error.URLError as exc:
        return False, "", f"ollama unreachable: {exc.reason}"
    except Exception as exc:  # noqa: BLE001
        return False, "", f"{type(exc).__name__}: {str(exc)[:200]}"


def _vision_describe_handle(handler: BaseHTTPRequestHandler) -> None:
    """POST /api/vision/describe - describe a previously-uploaded image.

    Body: {session, file_path}. file_path must be the absolute path
    returned by /api/chat/upload. The endpoint path-jails it to
    TASKS_UPLOADS_DIR so a malicious caller cannot read arbitrary disk.

    Failure modes (all return ok=false with a clear, operator-actionable
    message; never crash, never raise):
      - vision disabled in config         -> 503 + reason
      - body invalid                      -> 400
      - file_path outside upload dir      -> 403
      - file too large                    -> 413
      - no vision model installed         -> 200 ok=false + recommended_install
      - vision model rejected the image   -> 200 ok=false +
        \"Vision model not available or does not accept image input.\"
    """
    cfg = _read_vision_config()
    if not bool(cfg.get("enabled", True)):
        _send_json(handler, HTTPStatus.SERVICE_UNAVAILABLE, {
            "ok": False,
            "error": "vision disabled in memory/luna_vision_config.json",
        })
        return

    try:
        length = int(handler.headers.get("Content-Length") or "0")
    except (TypeError, ValueError):
        length = 0
    if length <= 0 or length > 64 * 1024:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid body size"})
        return
    try:
        raw = handler.rfile.read(length).decode("utf-8", errors="ignore")
        body = json.loads(raw or "{}")
    except Exception:  # noqa: BLE001
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid json"})
        return
    if not isinstance(body, dict):
        _send_json(handler, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "expected object"})
        return

    session_id = _safe_session_id(str(body.get("session") or ""))
    file_path  = str(body.get("file_path") or "").strip()
    if not session_id or not file_path:
        _send_json(handler, HTTPStatus.BAD_REQUEST, {
            "ok": False,
            "error": "missing session or file_path",
        })
        return

    # Path-jail: the file must live under TASKS_UPLOADS_DIR and the
    # session segment must match the body's session id (defense in depth).
    try:
        target = Path(file_path).resolve()
        target.relative_to(TASKS_UPLOADS_DIR.resolve())
    except (ValueError, OSError):
        _send_json(handler, HTTPStatus.FORBIDDEN, {"ok": False, "error": "path escape rejected"})
        return
    if not target.exists() or not target.is_file():
        _send_json(handler, HTTPStatus.NOT_FOUND, {"ok": False, "error": "file not found"})
        return
    try:
        size = target.stat().st_size
    except OSError:
        size = 0
    max_b = int(cfg.get("max_bytes", MAX_UPLOAD_BYTES))
    if size <= 0 or size > max_b:
        _send_json(handler, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {
            "ok": False,
            "error": f"file size {size} bytes outside acceptable range (0, {max_b}]",
        })
        return

    base = str(cfg.get("ollama_base") or VISION_OLLAMA_BASE)
    installed = _list_ollama_models(base)
    model = _pick_vision_model(installed)
    if not model:
        _send_json(handler, HTTPStatus.OK, {
            "ok": False,
            "error": "no vision model installed locally",
            "recommended_install": "ollama pull llava",
            "installed_models": installed,
        })
        return

    try:
        image_bytes = target.read_bytes()
    except OSError as exc:
        _send_json(handler, HTTPStatus.INTERNAL_SERVER_ERROR, {
            "ok": False,
            "error": f"read failed: {type(exc).__name__}",
        })
        return

    timeout_s = float(cfg.get("timeout_s", VISION_DEFAULT_TIMEOUT))
    prompt = str(cfg.get("prompt_prefix") or "Describe what you see in this image.")
    user_extra = str(body.get("prompt") or "").strip()
    if user_extra:
        prompt = prompt + " " + user_extra[:600]

    started = time.monotonic()
    ok, text, err = _call_ollama_vision(base, model, image_bytes, prompt, timeout_s)
    latency_ms = int((time.monotonic() - started) * 1000)

    if not ok:
        # Surface the canonical operator-facing message when the model
        # is present but does not accept image input (e.g. text-only
        # gemma3:4b tag).
        canonical = err
        low = err.lower() if err else ""
        if (
            "image" in low or "vision" in low or "modal" in low
            or "empty response" in low or "text-only" in low
            or "does not accept" in low
        ):
            canonical = "Vision model not available or does not accept image input."
        _send_json(handler, HTTPStatus.OK, {
            "ok": False,
            "error": canonical,
            "ollama_error": err,
            "model": model,
            "latency_ms": latency_ms,
            "recommended_install": "ollama pull llava",
        })
        return

    _send_json(handler, HTTPStatus.OK, {
        "ok": True,
        "description": text,
        "model": model,
        "latency_ms": latency_ms,
        "byte_size": size,
    })


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------
class LunaDashboardServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that refuses non-loopback binds.

    2026-05-13 Ctrl+F5 fix: overrides ``handle_error`` so per-request
    exceptions never reach stdlib's default — which writes a traceback
    to ``sys.stderr``. Under pythonw that's None and the error handler
    itself raises AttributeError, leaving the entire server process to
    die silently. We route the traceback to ``luna_command_center.log``.
    """

    daemon_threads = True
    allow_reuse_address = True
    # Bounded concurrency cap: ThreadingHTTPServer spawns one daemon thread
    # per request with NO limit. Under aggressive Command-Center-UI polling
    # with slow per-request handlers, handler threads accumulate without bound
    # -> the observed runaway (1700+ blocked threads, ~17 GB RAM). Cap
    # concurrent handlers and shed excess with 503 instead of leaking threads.
    # 2026-05-31: raised 32 -> 128. At boot the browser fires a burst of
    # panel requests while the background master_status warm is still running;
    # at 32 slots that burst exhausted the semaphore and shed the launcher's
    # cheap /api/health liveness probe with 503, delaying "healthy" detection
    # by the whole warm window. 128 gives the boot burst headroom while still
    # bounding handler-thread growth far below any runaway (historically 90+
    # PROCESSES, never hundreds of concurrent dashboard requests).
    _MAX_CONCURRENT_REQUESTS = 128

    def __init__(self, host: str, port: int) -> None:
        # Boot-timing instrumentation: NEVER raises; one append-only log line.
        try:
            from luna_modules import luna_boot_timing as _bt
            _bt.mark("LunaDashboardServer.__init__.start",
                     detail={"host": host, "port": port})
        except Exception:
            pass
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError(f"refusing non-loopback bind: {host!r}")
        # Force IPv4 loopback to keep behavior identical across hosts.
        self.address_family = socket.AF_INET
        self._req_sem = threading.BoundedSemaphore(
            self._MAX_CONCURRENT_REQUESTS)
        super().__init__((host, port), LunaDashboardHandler)
        try:
            from luna_modules import luna_boot_timing as _bt
            _bt.mark("LunaDashboardServer.__init__.end")
        except Exception:
            pass

    def process_request(self, request, client_address) -> None:
        # Bound concurrent handler threads; shed load on overflow so a flood
        # of requests can never spawn an unbounded number of threads.
        # NEVER raises. Under normal load the cap is never hit (no behaviour
        # change); it only sheds under runaway-level flooding.
        if not self._req_sem.acquire(blocking=False):
            try:
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"Content-Length: 0\r\nConnection: close\r\n\r\n")
            except Exception:  # noqa: BLE001
                pass
            try:
                self.shutdown_request(request)
            except Exception:  # noqa: BLE001
                pass
            return
        super().process_request(request, client_address)

    def process_request_thread(self, request, client_address) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            try:
                self._req_sem.release()
            except Exception:  # noqa: BLE001
                pass

    def handle_error(self, request: Any, client_address: Any) -> None:
        """Route per-request exception tracebacks to file, never stderr."""
        try:
            import traceback as _tb
            tb = _tb.format_exc()
        except Exception:  # noqa: BLE001
            tb = "(traceback unavailable)"
        _safe_log_dashboard_error(
            f"handler error for {client_address!r}: {tb}"
        )


def create_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> LunaDashboardServer:
    """Create (but do not start) a Luna dashboard server bound to loopback."""
    return LunaDashboardServer(host, port)


def serve_forever(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    """Run the dashboard server until KeyboardInterrupt.

    2026-05-13 Ctrl+F5 hard-refusal-of-connection fix: this is a
    self-restarting loop. If ``server.serve_forever()`` ever exits via
    an unhandled exception (the historical pythonw silent-crash trap),
    we log the traceback to ``luna_command_center.log`` and immediately
    rebuild a fresh server bound to the same port. The only path that
    actually exits is KeyboardInterrupt.

    Combined with the handler-level ``finish``/``handle_one_request``
    overrides and the server-level ``handle_error`` override, a Ctrl+F5
    storm cannot take the dashboard down — every layer absorbs the
    BrokenPipe family before it propagates.
    """
    # Boot-timing instrumentation: NEVER raises; one append-only log line.
    try:
        from luna_modules import luna_boot_timing as _bt
        _bt.mark("luna_http_dashboard.serve_forever.entry",
                 detail={"host": host, "port": port})
    except Exception:
        pass
    banner = f"[Luna Dashboard {PHASE_ID}] http://{host}:{port}/  (read-only)"
    # Banner — pythonw-safe (sys.stdout may be None).
    try:
        if sys.stdout is not None:
            print(banner, flush=True)
    except Exception:  # noqa: BLE001
        pass
    _safe_log_dashboard_error(banner)

    consecutive_crashes = 0
    while True:
        try:
            server = create_server(host, port)
        except Exception as exc:  # noqa: BLE001
            _safe_log_dashboard_error(
                f"create_server failed for {host}:{port}: "
                f"{type(exc).__name__}: {exc}"
            )
            # If we can't bind the port, sleep and retry. Don't propagate
            # — LaunchLunaDashboard.pyw would die otherwise.
            time.sleep(2.0)
            consecutive_crashes += 1
            if consecutive_crashes > 30:
                # ~60 s of unrecoverable bind failure — bail out so
                # outer launcher can investigate. Log loudly first.
                _safe_log_dashboard_error(
                    "serve_forever: 30 consecutive create_server failures; bailing out"
                )
                return
            continue
        # 2026-05-26 FIX: presence + warming moved to background thread
        # so server.serve_forever() starts immediately. The warming
        # import can trigger slow CUDA DLL loading that blocks for
        # 30+ seconds; we must not delay request acceptance.
        def _boot_background():
            # 2026-05-26: delay warming 60s so server starts accepting requests first.
            time.sleep(60)
            try:
                from luna_modules import cognitive_presence_runtime as _cprt
                _ack = _cprt.acknowledge_boot(
                    reason="dashboard_serve_forever",
                    caller="luna_http_dashboard.serve_forever")
                _safe_log_dashboard_error(
                    f"[presence] boot ack posture={_ack.get('posture')} "
                    f"spoke={_ack.get('spoke')} session={_ack.get('session_id')}")
            except Exception as _exc:
                try:
                    _safe_log_dashboard_error(
                        f"[presence] acknowledge_boot failed: "
                        f"{type(_exc).__name__}: {_exc}")
                except Exception:
                    pass
            # 2026-05-26: warming SKIPPED because warm_all workers consume
            # 1000%+ CPU and starve the dashboard request handler threads.
            # The warming is best-effort per original design; skipping is safe.
            _safe_log_dashboard_error("[warming] SKIPPED to prevent CPU starvation")
        threading.Thread(target=_boot_background, name="luna-boot-bg", daemon=True).start()

        # 2026-05-26 pre-warm slow caches in the main thread BEFORE
        # server.serve_forever() starts.  The _safe_build daemon thread
        # context causes master-status to hang or exceed 120s; building
        # in the main thread while the server is idle takes ~43s and
        # populates the 15s payload cache so the first request is fast.
        # 2026-05-31 BOOT-SPEED FIX (measured): building master_status in the
        # MAIN thread before serve_forever() blocked request acceptance for
        # ~43-120s (measured 122.8s to first /api/health 200). The socket is
        # bound by create_server() above, so TCP connects succeed but NO HTTP
        # response is produced until this finishes — the operator sees a dead
        # dashboard for ~2 minutes. /api/health uses the cheap
        # build_health_payload (NOT master_status), so it can answer in ~2s
        # the moment serve_forever() runs. We move the master_status prewarm
        # into a background daemon thread: serve_forever() starts immediately,
        # /api/health is fast, and the heavier master-status panel cache warms
        # behind the live server (first panel request triggers an on-demand
        # build if it races the warm — same path request handlers already use).
        def _prewarm_master_status_bg():
            try:
                _safe_log_dashboard_error(
                    '[prewarm] building master_status cache in background...')
                t0 = time.monotonic()
                build_master_status_payload()
                _safe_log_dashboard_error(
                    f'[prewarm] master_status cache ready '
                    f'({time.monotonic() - t0:.1f}s)')
            except Exception as _pw_exc:
                _safe_log_dashboard_error(
                    f'[prewarm] master_status cache failed: '
                    f'{type(_pw_exc).__name__}: {_pw_exc}')
            # 2026-05-31 also prewarm the higher-tier progress cache — its
            # schtasks probe was the single slow endpoint (>25s on
            # /api/decision-brief). Warming it here pays that cost once at
            # boot so no request ever blocks on it.
            try:
                t1 = time.monotonic()
                build_higher_tier_progress_payload()
                _safe_log_dashboard_error(
                    f'[prewarm] higher_tier_progress cache ready '
                    f'({time.monotonic() - t1:.1f}s)')
            except Exception as _pw_exc2:
                _safe_log_dashboard_error(
                    f'[prewarm] higher_tier_progress cache failed: '
                    f'{type(_pw_exc2).__name__}: {_pw_exc2}')
        threading.Thread(
            target=_prewarm_master_status_bg,
            name='luna-prewarm-mstatus', daemon=True).start()

        # 2026-05-31 start the working-set trim daemon. Python's pymalloc
        # keeps freed memory in arena pools — on this polling server the
        # observed RSS grew to multi-GB while the live heap stayed small.
        # The daemon calls gc.collect() + Windows SetProcessWorkingSetSize
        # every 60s to return unused pages to the OS. Idempotent (no-op if
        # already started). Defensive — never raises.
        try:
            _start_working_set_trim_daemon()
        except Exception:  # noqa: BLE001
            pass

        try:
            server.serve_forever()
            # Clean exit from server.serve_forever() (someone called
            # server.shutdown()). Treat as intentional shutdown.
            try:
                server.server_close()
            except Exception:  # noqa: BLE001
                pass
            return
        except KeyboardInterrupt:
            try:
                server.server_close()
            except Exception:  # noqa: BLE001
                pass
            return
        except Exception as exc:  # noqa: BLE001
            # serve_forever() raised — this is what historically killed
            # the process on Ctrl+F5. Log it, close the broken server,
            # and immediately rebuild a fresh one. NEVER re-raise.
            try:
                import traceback as _tb
                _safe_log_dashboard_error(
                    f"serve_forever crashed (will restart): "
                    f"{type(exc).__name__}: {exc}\n{_tb.format_exc()}"
                )
            except Exception:  # noqa: BLE001
                pass
            try:
                server.server_close()
            except Exception:  # noqa: BLE001
                pass
            consecutive_crashes += 1
            if consecutive_crashes > 50:
                # A genuine hard-crash storm — log loudly and stop so
                # the supervisor can diagnose. 50 in a row is far past
                # any plausible Ctrl+F5 burst.
                _safe_log_dashboard_error(
                    "serve_forever: 50 consecutive crashes; bailing out for supervisor"
                )
                return
            time.sleep(0.25)  # tiny backoff, then rebuild
        else:
            # serve_forever returned cleanly — reset the crash counter.
            consecutive_crashes = 0


# ---------------------------------------------------------------------------
# Self-test (keeps CLI useful without starting a long-running server)
# ---------------------------------------------------------------------------
def run_self_test() -> int:
    """Boot the server on an ephemeral port, hit endpoints, then shut down."""
    import urllib.request

    # Pick an ephemeral free loopback port to avoid colliding with a running
    # dashboard.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((DEFAULT_HOST, 0))
        port = s.getsockname()[1]

    server = create_server(DEFAULT_HOST, port)
    thread = threading.Thread(target=server.serve_forever, name="luna-dashboard-self-test", daemon=True)
    thread.start()
    base = f"http://{DEFAULT_HOST}:{port}"
    failures: list[str] = []
    try:
        time.sleep(0.05)
        # 1. /api/health returns ok=True
        with urllib.request.urlopen(f"{base}/api/health", timeout=3) as resp:
            if resp.status != 200:
                failures.append(f"health status {resp.status}")
            health = json.loads(resp.read().decode("utf-8"))
            if not health.get("ok") or health.get("phase") != PHASE_ID:
                failures.append("health payload bad")
        # 2. POST is rejected
        req = urllib.request.Request(f"{base}/api/health", method="POST", data=b"{}")
        try:
            urllib.request.urlopen(req, timeout=3)
            failures.append("POST was not rejected")
        except urllib.error.HTTPError as e:
            if e.code != 405:
                failures.append(f"POST got {e.code}, expected 405")
        # 3. Path traversal rejected
        try:
            urllib.request.urlopen(f"{base}/../etc/passwd", timeout=3)
            failures.append("path traversal allowed")
        except urllib.error.HTTPError as e:
            if e.code not in (400, 404, 403):
                failures.append(f"traversal got {e.code}")
        # 4. /api/status is JSON
        with urllib.request.urlopen(f"{base}/api/status", timeout=3) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("safety", {}).get("code_execution_state") != "LOCKED":
                failures.append("status not LOCKED")
            if data.get("safety", {}).get("guardian_live_enforcement") != "DISABLED":
                failures.append("guardian not DISABLED")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    if failures:
        for f in failures:
            print(f"SELF-TEST FAIL: {f}", file=sys.stderr)
        return 1
    print(f"[Luna Dashboard {PHASE_ID}] self-test OK on port {port}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=f"Luna Dashboard ({PHASE_ID}) — read-only local HTTP UI",
    )
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help="bind host (must be 127.0.0.1, localhost, or ::1)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"bind port (default {DEFAULT_PORT})")
    parser.add_argument("--self-test", action="store_true",
                        help="boot an ephemeral server, smoke-test endpoints, then exit")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.self_test:
        return run_self_test()

    serve_forever(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
