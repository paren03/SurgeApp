"""Luna HTTP Dashboard (Phase UI-1A — read-only foundation).

A local-only, read-only HTTP server that exposes a futuristic 2090-style
dashboard for Luna at http://127.0.0.1:8765. Stdlib-only; binds to 127.0.0.1;
serves whitelisted static files from ``luna_dashboard/`` and a small set of
read-only JSON/JSONL/MD sources from ``memory/`` and ``logs/``.

Hard safety guarantees enforced in this module:
  * Bind only to 127.0.0.1 (never 0.0.0.0).
  * Reject every method except GET and HEAD with HTTP 405.
  * No shell execution, subprocess, or eval anywhere in the request path.
  * No file writes from request handling.
  * Static file serving is whitelisted (no path traversal, no arbitrary reads).
  * Live-feed tail is bounded (default 100 lines).
  * advisory_only / safe_to_execute_now / safe_to_apply_real_project /
    guardian_enforcing_live remain False — this module never flips them.

CLI:
    python -m luna_modules.luna_http_dashboard [--host 127.0.0.1]
                                                [--port 8765]
                                                [--self-test]

Phase UI-1A — Luna Futuristic HTTP Dashboard Foundation.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable

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
    "/style.css": "style.css",
    "/app.js": "app.js",
    "/assets/luna_logo.svg": "assets/luna_logo.svg",
    "/assets/luna_icon.png": "assets/luna_icon.png",
    "/assets/luna_icon.ico": "assets/luna_icon.ico",
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
}

CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
    ".json": "application/json; charset=utf-8",
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
# API payload builders
# ---------------------------------------------------------------------------
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
        "verifier": {
            "summary": readiness.get("verifier_summary", "unknown") if isinstance(readiness, dict) else "unknown",
        },
        "soak": {
            "verdict": verdict.get("verdict", "UNKNOWN"),
            "observed_cycles": verdict.get("observed_cycles", 0),
            "required_cycles": verdict.get("required_cycles", 144),
            "stable_recommendation": verdict.get("stable_recommendation", ""),
            "checklist_24h_satisfied": bool(verdict.get("checklist_item_24h_soak_satisfied", False)),
            "last_update": verdict.get("generated_at", ""),
        },
        "safety": {
            "code_execution_state": "LOCKED",
            "guardian_live_enforcement": "DISABLED",
            "advisory_only": True,
            "safe_to_execute_now": False,
            "safe_to_apply_real_project": False,
            "guardian_enforcing_live": False,
        },
    }


def build_decision_brief_payload() -> dict[str, Any]:
    brief = _safe_read_json(READONLY_SOURCES["morning_brief_json"]) or {}
    digest = _safe_read_json(READONLY_SOURCES["decision_card_digest"]) or {}
    return {
        "available": bool(brief),
        "generated_at": brief.get("generated_at", ""),
        "advisory_only": bool(brief.get("advisory_only", True)),
        "overall_recommendation": brief.get("overall_recommendation", "unknown"),
        "counts": brief.get("counts", {}),
        "top_items": brief.get("top_items", [])[:8],
        "serge_summary": brief.get("serge_summary", ""),
        "next_safe_action": brief.get("next_safe_action", ""),
        "decision_card_digest": digest if isinstance(digest, dict) else {},
    }


def build_soak_payload() -> dict[str, Any]:
    verdict = _safe_read_json(READONLY_SOURCES["soak_verdict_report"]) or {}
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
    return {
        "available": bool(rs),
        "generated_at": rs.get("generated_at", ""),
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


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------
ALLOWED_METHODS = {"GET", "HEAD"}
_PATH_RE = re.compile(r"^/[A-Za-z0-9._/\-]*$")


class LunaDashboardHandler(BaseHTTPRequestHandler):
    """Read-only HTTP handler. GET/HEAD only. No writes. No shell."""

    server_version = "LunaDashboard/UI-1A"

    # Suppress default stderr access logs; the dashboard is a quiet local
    # helper, and Serge sees state through the live feed.
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    # ---- method gating --------------------------------------------------
    def _reject_unsupported(self) -> None:
        self.send_error(HTTPStatus.METHOD_NOT_ALLOWED, "Method not allowed (read-only)")

    def do_POST(self) -> None:  # noqa: N802
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
        self.end_headers()
        if write_body:
            self.wfile.write(data)

    # ---- api routing ----------------------------------------------------
    def _serve_api(self, path: str, query: str, write_body: bool) -> None:
        params = urllib.parse.parse_qs(query) if query else {}
        try:
            if path == "/api/status":
                payload = build_status_payload()
            elif path == "/api/decision-brief":
                payload = build_decision_brief_payload()
            elif path == "/api/soak":
                payload = build_soak_payload()
            elif path == "/api/scorecard":
                payload = build_scorecard_payload()
            elif path == "/api/resources":
                payload = build_resources_payload()
            elif path == "/api/live-feed":
                limit_raw = params.get("limit", [str(LIVE_FEED_MAX_LINES)])[0]
                try:
                    limit = int(limit_raw)
                except (TypeError, ValueError):
                    limit = LIVE_FEED_MAX_LINES
                payload = build_live_feed_payload(limit=limit)
            elif path == "/api/archive":
                payload = build_archive_payload()
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
                payload = build_activity_payload(window_seconds=window, buckets=buckets)
            elif path == "/api/health":
                payload = build_health_payload()
            else:
                self.send_error(HTTPStatus.NOT_FOUND, "unknown api endpoint")
                return
        except Exception as exc:  # noqa: BLE001
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"payload error: {type(exc).__name__}")
            return
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        if write_body:
            self.wfile.write(body)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------
class LunaDashboardServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that refuses non-loopback binds."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, host: str, port: int) -> None:
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError(f"refusing non-loopback bind: {host!r}")
        # Force IPv4 loopback to keep behavior identical across hosts.
        self.address_family = socket.AF_INET
        super().__init__((host, port), LunaDashboardHandler)


def create_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> LunaDashboardServer:
    """Create (but do not start) a Luna dashboard server bound to loopback."""
    return LunaDashboardServer(host, port)


def serve_forever(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    server = create_server(host, port)
    print(
        f"[Luna Dashboard {PHASE_ID}] http://{host}:{port}/  (read-only)",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


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
