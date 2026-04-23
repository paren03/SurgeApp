"""Logging, layout, and telemetry fallback shim.

Extracted verbatim from ``worker.py`` (step 3 of modularity refactor).
Provides ``now_iso``, ``_diag``, ``ensure_layout``, ``log`` plus the
telemetry callable shims that ``speak`` in ``worker.py`` continues to
use.  No behavior change.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from luna_modules.luna_paths import (
    ACQUISITIONS_DIR,
    ACTIVE_DIR,
    ARCHIVE_LOGS_DIR,
    BACKUPS_DIR,
    DIAGNOSTIC_PREFIX,
    DONE_DIR,
    FAILED_DIR,
    LOGIC_UPDATES_DIR,
    LOGS_DIR,
    LUNA_MODULES_DIR,
    LUNA_MODULES_INIT_PATH,
    MCP_DIR,
    MEMORY_DIR,
    SOLUTIONS_DIR,
    TASKS_DIR,
    TEMP_TEST_ZONE_DIR,
    WORKER_LOG_PATH,
)

try:
    from luna_modules.luna_telemetry import (
        emit_diag as telemetry_emit_diag,
        emit_log as telemetry_emit_log,
        emit_speak as telemetry_emit_speak,
    )
except Exception:
    def telemetry_emit_diag(message, diagnostic_prefix, worker_log_path, layout_cb=None):
        line = f"{diagnostic_prefix} {message}"
        try:
            sys.stderr.write(line + "\n")
        except Exception:
            pass
        try:
            if layout_cb is not None:
                layout_cb()
            else:
                Path(worker_log_path).parent.mkdir(parents=True, exist_ok=True)
            with open(worker_log_path, "a", encoding="utf-8") as handle:
                handle.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {line}\n")
        except Exception:
            pass

    def telemetry_emit_log(message, worker_log_path, layout_cb=None):
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        print(line, flush=True)
        try:
            if layout_cb is not None:
                layout_cb()
            else:
                Path(worker_log_path).parent.mkdir(parents=True, exist_ok=True)
            with open(worker_log_path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        except Exception:
            pass

    def telemetry_emit_speak(message, mood, autonomy_messages, heartbeat_cb, log_cb):
        autonomy_messages.append(message)
        heartbeat_cb(mood=mood, last_message=message)
        log_cb(f"[LUNA] {message}")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_layout() -> None:
    for folder in (
        TASKS_DIR,
        ACTIVE_DIR,
        DONE_DIR,
        FAILED_DIR,
        SOLUTIONS_DIR,
        LOGS_DIR,
        MEMORY_DIR,
        BACKUPS_DIR,
        LOGIC_UPDATES_DIR,
        TEMP_TEST_ZONE_DIR,
        ARCHIVE_LOGS_DIR,
        ACQUISITIONS_DIR,
        MCP_DIR,
        LUNA_MODULES_DIR,
    ):
        folder.mkdir(parents=True, exist_ok=True)
    try:
        if not LUNA_MODULES_INIT_PATH.exists():
            LUNA_MODULES_INIT_PATH.write_text('"""Luna fractal modules."""\n', encoding="utf-8")
    except Exception:
        pass


def _diag(message: str) -> None:
    telemetry_emit_diag(message, DIAGNOSTIC_PREFIX, WORKER_LOG_PATH, ensure_layout)


def log(message: str) -> None:
    telemetry_emit_log(message, WORKER_LOG_PATH, ensure_layout)
