"""Heartbeat, thread health, and worker lock primitives.

Extracted from ``worker.py`` (step 4 of modularity refactor).
Only the cleanly decoupled primitives live here. Functions with
mutable module-local ``global`` state (``WARM_RESET_COUNT``,
``STOP_REQUESTED``, ``HEARTBEAT_FAILURE_COUNT``,
``LAST_HEARTBEAT_WRITE_MONO``) stay in ``worker.py`` for now.
"""

from __future__ import annotations

import os
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict

from luna_modules.luna_io import safe_read_json, write_json_atomic
from luna_modules.luna_logging import _diag, now_iso
from luna_modules.luna_paths import WORKER_LOCK_PATH, WORKER_STALE_SECONDS

# Shared heartbeat / thread-health / autonomy-message state. These are
# imported by ``worker.py`` and accessed there; because they are mutable
# objects (dict/Lock/deque), rebinding the names via ``from ... import``
# preserves the single shared instance.
HEARTBEAT_STATE: Dict[str, Any] = {
    "state": "booting",
    "task_id": "",
    "phase": "boot",
    "detail": "",
    "mood": "waking up",
    "last_message": "",
}
HEARTBEAT_LOCK = threading.Lock()

THREAD_HEALTH: Dict[str, Dict[str, Any]] = {}
THREAD_HEALTH_LOCK = threading.Lock()

AUTONOMY_MESSAGES: Deque[str] = deque(maxlen=10)


def heartbeat_age_seconds(heartbeat: Dict[str, Any]) -> int:
    ts = str((heartbeat or {}).get("ts", "")).strip()
    if not ts:
        return 10**9
    try:
        return max(0, int((datetime.now() - datetime.fromisoformat(ts)).total_seconds()))
    except Exception:
        return 10**9


def register_thread_heartbeat(name: str, status: str = "ok", detail: str = "") -> None:
    with THREAD_HEALTH_LOCK:
        THREAD_HEALTH[name] = {
            "ts": now_iso(),
            "mono": time.monotonic(),
            "status": status,
            "detail": detail,
            "alive": True,
        }


def thread_health_snapshot() -> Dict[str, Any]:
    with THREAD_HEALTH_LOCK:
        snapshot = {key: dict(value) for key, value in THREAD_HEALTH.items()}
    for value in snapshot.values():
        value.pop("mono", None)
    return snapshot


def set_heartbeat(**updates: Any) -> None:
    with HEARTBEAT_LOCK:
        HEARTBEAT_STATE.update(updates)


def start_background_thread(target, name: str) -> threading.Thread:
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def _pid_is_alive(pid: int) -> bool:
    if not pid or pid == os.getpid():
        return True
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True
    return True


def acquire_worker_lock() -> bool:
    existing = safe_read_json(WORKER_LOCK_PATH, default={})
    if existing:
        pid = existing.get("pid")
        ts = existing.get("ts")
        if pid and ts:
            try:
                age = (datetime.now() - datetime.fromisoformat(ts)).total_seconds()
            except Exception:
                age = WORKER_STALE_SECONDS + 1
            if not _pid_is_alive(int(pid)):
                _diag(f"stale worker lock cleared for dead pid={pid}")
            elif age <= WORKER_STALE_SECONDS and pid != os.getpid():
                return False
    write_json_atomic(WORKER_LOCK_PATH, {"pid": os.getpid(), "ts": now_iso()})
    return True


def refresh_worker_lock() -> None:
    write_json_atomic(WORKER_LOCK_PATH, {"pid": os.getpid(), "ts": now_iso()})


def release_worker_lock() -> None:
    existing = safe_read_json(WORKER_LOCK_PATH, default={})
    if existing.get("pid") == os.getpid():
        try:
            WORKER_LOCK_PATH.unlink(missing_ok=True)
        except Exception:
            pass
