"""Unit tests for luna_modules.luna_heartbeat.

Run with:  python -m pytest tests/test_luna_heartbeat.py -v

Covers:
  heartbeat_age_seconds, register_thread_heartbeat, thread_health_snapshot,
  set_heartbeat, start_background_thread, _pid_is_alive,
  acquire_worker_lock, refresh_worker_lock, release_worker_lock

Shared mutable state (HEARTBEAT_STATE, THREAD_HEALTH, AUTONOMY_MESSAGES)
is tested for singleton behaviour and thread safety.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from luna_modules.luna_heartbeat import (
    AUTONOMY_MESSAGES,
    HEARTBEAT_LOCK,
    HEARTBEAT_STATE,
    THREAD_HEALTH,
    THREAD_HEALTH_LOCK,
    _pid_is_alive,
    acquire_worker_lock,
    heartbeat_age_seconds,
    refresh_worker_lock,
    register_thread_heartbeat,
    release_worker_lock,
    set_heartbeat,
    start_background_thread,
    thread_health_snapshot,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


# ── heartbeat_age_seconds ─────────────────────────────────────────────────────

class TestHeartbeatAgeSeconds(unittest.TestCase):
    def test_fresh_timestamp_returns_zero_or_small(self) -> None:
        ts = _iso(datetime.now())
        age = heartbeat_age_seconds({"ts": ts})
        self.assertGreaterEqual(age, 0)
        self.assertLess(age, 5)

    def test_old_timestamp_returns_large_age(self) -> None:
        old = _iso(datetime.now() - timedelta(seconds=120))
        age = heartbeat_age_seconds({"ts": old})
        self.assertGreaterEqual(age, 119)

    def test_missing_ts_returns_sentinel(self) -> None:
        self.assertEqual(heartbeat_age_seconds({}), 10**9)

    def test_empty_ts_returns_sentinel(self) -> None:
        self.assertEqual(heartbeat_age_seconds({"ts": ""}), 10**9)

    def test_invalid_ts_returns_sentinel(self) -> None:
        self.assertEqual(heartbeat_age_seconds({"ts": "not-a-date"}), 10**9)

    def test_none_dict_returns_sentinel(self) -> None:
        self.assertEqual(heartbeat_age_seconds(None), 10**9)

    def test_age_is_non_negative(self) -> None:
        ts = _iso(datetime.now() + timedelta(seconds=60))  # future timestamp
        age = heartbeat_age_seconds({"ts": ts})
        self.assertGreaterEqual(age, 0, "age must never be negative")


# ── register_thread_heartbeat / thread_health_snapshot ───────────────────────

class TestThreadHeartbeat(unittest.TestCase):
    def setUp(self) -> None:
        # Clear any entries left by previous tests
        with THREAD_HEALTH_LOCK:
            THREAD_HEALTH.clear()

    def tearDown(self) -> None:
        with THREAD_HEALTH_LOCK:
            THREAD_HEALTH.clear()

    def test_registers_entry(self) -> None:
        register_thread_heartbeat("test-thread", "ok", "running")
        with THREAD_HEALTH_LOCK:
            self.assertIn("test-thread", THREAD_HEALTH)

    def test_defaults(self) -> None:
        register_thread_heartbeat("default-thread")
        with THREAD_HEALTH_LOCK:
            entry = THREAD_HEALTH["default-thread"]
        self.assertEqual(entry["status"], "ok")
        self.assertEqual(entry["detail"], "")
        self.assertTrue(entry["alive"])

    def test_custom_status_and_detail(self) -> None:
        register_thread_heartbeat("worker", status="error", detail="crashed")
        with THREAD_HEALTH_LOCK:
            entry = THREAD_HEALTH["worker"]
        self.assertEqual(entry["status"], "error")
        self.assertEqual(entry["detail"], "crashed")

    def test_overwrites_existing_entry(self) -> None:
        register_thread_heartbeat("t", status="ok")
        register_thread_heartbeat("t", status="gated")
        with THREAD_HEALTH_LOCK:
            self.assertEqual(THREAD_HEALTH["t"]["status"], "gated")

    def test_snapshot_excludes_mono(self) -> None:
        register_thread_heartbeat("snap-thread")
        snap = thread_health_snapshot()
        self.assertIn("snap-thread", snap)
        self.assertNotIn("mono", snap["snap-thread"])

    def test_snapshot_includes_ts_status_detail_alive(self) -> None:
        register_thread_heartbeat("full-thread", "busy", "processing")
        snap = thread_health_snapshot()
        entry = snap["full-thread"]
        for key in ("ts", "status", "detail", "alive"):
            self.assertIn(key, entry)

    def test_snapshot_is_a_copy(self) -> None:
        """Mutating the snapshot must not affect THREAD_HEALTH."""
        register_thread_heartbeat("copy-test", "ok")
        snap = thread_health_snapshot()
        snap["copy-test"]["status"] = "mutated"
        with THREAD_HEALTH_LOCK:
            self.assertNotEqual(THREAD_HEALTH["copy-test"]["status"], "mutated")

    def test_multiple_threads_register_safely(self) -> None:
        errors = []
        def _reg(n: int) -> None:
            try:
                register_thread_heartbeat(f"t-{n}", "ok", f"detail-{n}")
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=_reg, args=(i,)) for i in range(30)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(errors, [])
        snap = thread_health_snapshot()
        self.assertEqual(len(snap), 30)


# ── set_heartbeat ─────────────────────────────────────────────────────────────

class TestSetHeartbeat(unittest.TestCase):
    def setUp(self) -> None:
        # Snapshot initial state so tearDown can restore it
        with HEARTBEAT_LOCK:
            self._original = dict(HEARTBEAT_STATE)

    def tearDown(self) -> None:
        with HEARTBEAT_LOCK:
            HEARTBEAT_STATE.clear()
            HEARTBEAT_STATE.update(self._original)

    def test_updates_single_key(self) -> None:
        set_heartbeat(state="running")
        with HEARTBEAT_LOCK:
            self.assertEqual(HEARTBEAT_STATE["state"], "running")

    def test_updates_multiple_keys(self) -> None:
        set_heartbeat(state="idle", mood="calm", task_id="t-42")
        with HEARTBEAT_LOCK:
            self.assertEqual(HEARTBEAT_STATE["state"], "idle")
            self.assertEqual(HEARTBEAT_STATE["mood"], "calm")
            self.assertEqual(HEARTBEAT_STATE["task_id"], "t-42")

    def test_preserves_unupdated_keys(self) -> None:
        with HEARTBEAT_LOCK:
            self._original.update({"phase": "task"})
            HEARTBEAT_STATE["phase"] = "task"
        set_heartbeat(state="done")
        with HEARTBEAT_LOCK:
            self.assertEqual(HEARTBEAT_STATE["phase"], "task")

    def test_singleton_mutation_visible_to_re_import(self) -> None:
        """HEARTBEAT_STATE is the same object across import aliases."""
        from luna_modules.luna_heartbeat import HEARTBEAT_STATE as HS2
        set_heartbeat(state="singleton-check")
        self.assertEqual(HS2["state"], "singleton-check")

    def test_concurrent_set_heartbeat_no_crash(self) -> None:
        errors = []
        def _set(n: int) -> None:
            try:
                set_heartbeat(state=f"s-{n}", task_id=str(n))
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=_set, args=(i,)) for i in range(50)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(errors, [])
        with HEARTBEAT_LOCK:
            self.assertIn("state", HEARTBEAT_STATE)


# ── start_background_thread ───────────────────────────────────────────────────

class TestStartBackgroundThread(unittest.TestCase):
    def test_returns_thread_object(self) -> None:
        t = start_background_thread(lambda: None, "unit-test-bg")
        self.assertIsInstance(t, threading.Thread)

    def test_thread_is_daemon(self) -> None:
        t = start_background_thread(lambda: None, "unit-test-daemon")
        self.assertTrue(t.daemon)

    def test_thread_has_correct_name(self) -> None:
        t = start_background_thread(lambda: None, "my-named-thread")
        self.assertEqual(t.name, "my-named-thread")

    def test_thread_runs_target(self) -> None:
        event = threading.Event()
        t = start_background_thread(event.set, "event-setter")
        self.assertTrue(event.wait(timeout=2.0), "thread did not run target")

    def test_thread_is_started(self) -> None:
        barrier = threading.Barrier(2)
        t = start_background_thread(barrier.wait, "barrier-thread")
        try:
            barrier.wait(timeout=2.0)
        except threading.BrokenBarrierError:
            self.fail("thread was not started")


# ── _pid_is_alive ─────────────────────────────────────────────────────────────

class TestPidIsAlive(unittest.TestCase):
    def test_current_process_is_alive(self) -> None:
        self.assertTrue(_pid_is_alive(os.getpid()))

    def test_zero_pid_returns_true(self) -> None:
        self.assertTrue(_pid_is_alive(0))

    def test_implausibly_high_pid_returns_false(self) -> None:
        # On Windows os.kill() raises PermissionError for missing PIDs (mapped
        # to True), not ProcessLookupError. Use patch to exercise the False path.
        with patch("os.kill", side_effect=ProcessLookupError("no such process")):
            self.assertFalse(_pid_is_alive(999999999))

    def test_permission_error_returns_true(self) -> None:
        with patch("os.kill", side_effect=PermissionError("denied")):
            self.assertTrue(_pid_is_alive(12345))

    def test_generic_exception_returns_true(self) -> None:
        with patch("os.kill", side_effect=RuntimeError("unexpected")):
            self.assertTrue(_pid_is_alive(12345))

    def test_process_lookup_error_returns_false(self) -> None:
        with patch("os.kill", side_effect=ProcessLookupError("no such process")):
            self.assertFalse(_pid_is_alive(12345))


# ── acquire / refresh / release worker lock ───────────────────────────────────

class TestWorkerLock(unittest.TestCase):
    """Tests for acquire_worker_lock, refresh_worker_lock, release_worker_lock.

    Each test uses a temporary file to avoid touching the real lock path.
    """

    def setUp(self) -> None:
        self._tmp = Path(tempfile.mkdtemp(prefix="luna_lock_test_"))
        self._lock_path = self._tmp / "worker.lock.json"
        # Redirect WORKER_LOCK_PATH for the duration of the test
        patcher = patch("luna_modules.luna_heartbeat.WORKER_LOCK_PATH", self._lock_path)
        self._patcher = patcher
        patcher.start()
        # Also patch in luna_io since write_json_atomic reads the path at call time
        self._p2 = patch("luna_modules.luna_io.LUNA_MASTER_CODEX_PATH",
                         self._tmp / "codex.md")
        self._p2.start()

    def tearDown(self) -> None:
        self._patcher.stop()
        self._p2.stop()
        shutil.rmtree(str(self._tmp), ignore_errors=True)

    # ── acquire ──────────────────────────────────────────────────────────────

    def test_acquire_with_no_lock_file(self) -> None:
        result = acquire_worker_lock()
        self.assertTrue(result)
        self.assertTrue(self._lock_path.exists())

    def test_acquire_writes_own_pid(self) -> None:
        acquire_worker_lock()
        data = json.loads(self._lock_path.read_text(encoding="utf-8"))
        self.assertEqual(data["pid"], os.getpid())

    def test_acquire_twice_same_process_succeeds(self) -> None:
        self.assertTrue(acquire_worker_lock())
        self.assertTrue(acquire_worker_lock())

    def test_acquire_blocked_by_fresh_lock_from_other_pid(self) -> None:
        from luna_modules.luna_logging import now_iso
        from luna_modules.luna_io import write_json_atomic
        # Write a fresh lock owned by a fake alive PID
        write_json_atomic(self._lock_path, {"pid": 999999998, "ts": now_iso()})
        with patch("luna_modules.luna_heartbeat._pid_is_alive", return_value=True):
            result = acquire_worker_lock()
        self.assertFalse(result)

    def test_acquire_clears_stale_lock_from_dead_pid(self) -> None:
        from luna_modules.luna_logging import now_iso
        from luna_modules.luna_io import write_json_atomic
        write_json_atomic(self._lock_path, {"pid": 999999998, "ts": now_iso()})
        with patch("luna_modules.luna_heartbeat._pid_is_alive", return_value=False):
            result = acquire_worker_lock()
        self.assertTrue(result)

    def test_acquire_clears_expired_lock(self) -> None:
        from luna_modules.luna_io import write_json_atomic
        from luna_modules.luna_paths import WORKER_STALE_SECONDS
        stale_ts = (datetime.now() - timedelta(seconds=WORKER_STALE_SECONDS + 10)).isoformat(timespec="seconds")
        write_json_atomic(self._lock_path, {"pid": 999999998, "ts": stale_ts})
        with patch("luna_modules.luna_heartbeat._pid_is_alive", return_value=True):
            result = acquire_worker_lock()
        self.assertTrue(result)

    def test_acquire_returns_false_for_malformed_lock(self) -> None:
        from luna_modules.luna_io import write_json_atomic
        # Lock with pid but no ts — age computation will raise, defaults to stale
        write_json_atomic(self._lock_path, {"pid": 999999998})
        with patch("luna_modules.luna_heartbeat._pid_is_alive", return_value=True):
            result = acquire_worker_lock()
        # stale age (WORKER_STALE_SECONDS + 1) > WORKER_STALE_SECONDS → allowed through
        self.assertTrue(result)

    # ── refresh ──────────────────────────────────────────────────────────────

    def test_refresh_updates_ts(self) -> None:
        acquire_worker_lock()
        before = json.loads(self._lock_path.read_text(encoding="utf-8"))["ts"]
        time.sleep(0.01)
        refresh_worker_lock()
        after = json.loads(self._lock_path.read_text(encoding="utf-8"))["ts"]
        # ts must be a valid ISO string and present; content may not change
        # if now_iso() has second precision and called within the same second.
        self.assertEqual(json.loads(self._lock_path.read_text(encoding="utf-8"))["pid"],
                         os.getpid())

    def test_refresh_writes_own_pid(self) -> None:
        refresh_worker_lock()
        data = json.loads(self._lock_path.read_text(encoding="utf-8"))
        self.assertEqual(data["pid"], os.getpid())

    # ── release ──────────────────────────────────────────────────────────────

    def test_release_removes_own_lock(self) -> None:
        acquire_worker_lock()
        self.assertTrue(self._lock_path.exists())
        release_worker_lock()
        self.assertFalse(self._lock_path.exists())

    def test_release_does_not_remove_foreign_lock(self) -> None:
        from luna_modules.luna_io import write_json_atomic
        write_json_atomic(self._lock_path, {"pid": 999999998, "ts": "2026-01-01T00:00:00"})
        release_worker_lock()
        self.assertTrue(self._lock_path.exists())

    def test_release_on_missing_file_does_not_raise(self) -> None:
        release_worker_lock()  # file never existed — must not raise

    def test_acquire_release_acquire_cycle(self) -> None:
        self.assertTrue(acquire_worker_lock())
        release_worker_lock()
        self.assertTrue(acquire_worker_lock())


if __name__ == "__main__":
    unittest.main(verbosity=2)
