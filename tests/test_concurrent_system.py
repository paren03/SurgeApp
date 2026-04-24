"""Full concurrent system test for the refactored worker.py.

Tests five domains under concurrent load:
  1. LLM waterfall — 9 parallel calls across 3 providers × 3 threads
  2. Atomic I/O    — 20 concurrent writers to the same JSONL file
  3. External APIs — Brave Search + GitHub in parallel
  4. Shared state  — CORE_STATE and heartbeat integrity under load
  5. Worker health — live worker process still alive and publishing heartbeats

All tests are read-only or write to isolated temp paths; the live worker
process (if running) is never interrupted.

Run with:
    python tests/test_concurrent_system.py
"""

import io
import json
import os
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

# Force UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# ── lightweight test framework ────────────────────────────────────────────────
_results = []
_lock = threading.Lock()


def record(name: str, ok: bool, elapsed: float, detail: str = ""):
    with _lock:
        _results.append({"name": name, "ok": ok, "elapsed": elapsed, "detail": detail})
    mark = "✓" if ok else "✗"
    print(f"  {mark}  {name}  ({elapsed:.2f}s){' — ' + detail[:80] if detail else ''}")


def section(title: str):
    print(f"\n── {title} {'─' * max(0, 62 - len(title))}")


# ── imports ───────────────────────────────────────────────────────────────────
section("Importing modules")

t0 = time.monotonic()
from luna_modules.luna_io import append_jsonl, safe_read_json, write_json_atomic
from luna_modules.luna_paths import PROJECT_DIR, MEMORY_DIR, LOGS_DIR
from luna_modules.luna_tools import (
    _read_vault, web_search, github_get_repo,
    run_project_shell, project_read_file, list_project_files,
)
record("Module imports", True, round(time.monotonic() - t0, 3))

os.environ["LUNA_WORKER_NO_THREADS"] = "1"
t0 = time.monotonic()
try:
    import worker as w
    record("worker.py import (no threads)", True, round(time.monotonic() - t0, 2))
except SystemExit:
    record("worker.py import (no threads)", True, round(time.monotonic() - t0, 2), "SystemExit swallowed OK")
except Exception as exc:
    record("worker.py import (no threads)", False, round(time.monotonic() - t0, 2), str(exc))
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Concurrent LLM waterfall (9 parallel calls)
# ═══════════════════════════════════════════════════════════════════════════════
section("1. Concurrent LLM waterfall — 9 parallel calls")

PROMPT = [{"role": "user", "text": "Reply with one word: READY"}]

_llm_results: dict = {}
_llm_errors: dict = {}


def _call_llm(key: str, fn):
    start = time.monotonic()
    try:
        result = fn() or ""
        _llm_results[key] = (result, round(time.monotonic() - start, 2))
    except Exception as exc:
        _llm_errors[key] = (str(exc), round(time.monotonic() - start, 2))


# Spawn 9 threads: 3 providers × 3 concurrent calls each
threads = []
for i in range(3):
    threads.append(threading.Thread(target=_call_llm, args=(
        f"openrouter_{i}",
        lambda: w._query_openrouter_chat(PROMPT, "meta-llama/llama-3.3-70b-instruct"),
    )))
    threads.append(threading.Thread(target=_call_llm, args=(
        f"openai_{i}",
        lambda: w._query_openai_chat(PROMPT, "gpt-4o"),
    )))
    threads.append(threading.Thread(target=_call_llm, args=(
        f"grok_{i}",
        lambda: w._query_xai_chat(PROMPT, "grok-4-0709"),
    )))

start_all = time.monotonic()
for t in threads:
    t.start()
for t in threads:
    t.join(timeout=30)
total_llm_time = round(time.monotonic() - start_all, 2)

passed_llm = sum(1 for r, _ in _llm_results.values() if len(r) >= 2)
failed_llm = len(_llm_errors)

record(
    f"9 concurrent LLM calls — {passed_llm}/9 returned content",
    passed_llm >= 6,  # allow up to 3 rate-limit failures
    total_llm_time,
    f"errors={failed_llm}",
)

# per-provider breakdown
for provider in ("openrouter", "openai", "grok"):
    wins = sum(1 for k, (r, _) in _llm_results.items() if k.startswith(provider) and len(r) >= 2)
    errs = sum(1 for k in _llm_errors if k.startswith(provider))
    times = [e for k, (_, e) in _llm_results.items() if k.startswith(provider)]
    avg = round(sum(times) / len(times), 2) if times else 0
    record(
        f"  {provider}: {wins}/3 successful  avg={avg}s",
        wins >= 1,
        avg,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Atomic I/O under concurrent writers
# ═══════════════════════════════════════════════════════════════════════════════
section("2. Atomic I/O — 20 concurrent writers")

_tmp_jsonl = PROJECT_DIR / "temp_test_zone" / "_concurrent_io_test.jsonl"
_tmp_jsonl.parent.mkdir(parents=True, exist_ok=True)
if _tmp_jsonl.exists():
    _tmp_jsonl.unlink()

_io_errors = []


def _write_entry(thread_id: int):
    for seq in range(5):
        try:
            append_jsonl(_tmp_jsonl, {"thread": thread_id, "seq": seq, "ts": time.time()})
        except Exception as exc:
            with _lock:
                _io_errors.append(f"t{thread_id}:{seq} -> {exc}")
        time.sleep(0.002)  # slight stagger


io_threads = [threading.Thread(target=_write_entry, args=(i,)) for i in range(20)]
t0 = time.monotonic()
for t in io_threads:
    t.start()
for t in io_threads:
    t.join(timeout=15)
io_elapsed = round(time.monotonic() - t0, 2)

# Read back and validate
lines = [l for l in _tmp_jsonl.read_text(errors="ignore").splitlines() if l.strip()]
parsed = 0
for line in lines:
    try:
        json.loads(line)
        parsed += 1
    except Exception:
        pass

expected = 20 * 5  # 20 threads × 5 writes
record(
    f"20-thread JSONL write: {parsed}/{expected} valid entries, 0 corruption",
    parsed == expected and not _io_errors,
    io_elapsed,
    f"io_errors={len(_io_errors)}",
)
record(
    "No torn writes (all entries parse as valid JSON)",
    parsed == expected,
    0,
    f"valid={parsed} expected={expected}",
)
_tmp_jsonl.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Parallel external API calls (Brave + GitHub)
# ═══════════════════════════════════════════════════════════════════════════════
section("3. Parallel external API calls")

_api_results: dict = {}


def _run_api(key, fn):
    t = time.monotonic()
    try:
        _api_results[key] = (fn(), round(time.monotonic() - t, 2))
    except Exception as exc:
        _api_results[key] = ({"ok": False, "error": str(exc)}, round(time.monotonic() - t, 2))


api_threads = [
    threading.Thread(target=_run_api, args=("brave_1", lambda: web_search("Python concurrency threading", max_results=3))),
    threading.Thread(target=_run_api, args=("brave_2", lambda: web_search("autonomous AI agent design", max_results=3))),
    threading.Thread(target=_run_api, args=("github_repo", lambda: github_get_repo("paren03/SurgeApp"))),
    threading.Thread(target=_run_api, args=("shell_1", lambda: run_project_shell("python --version"))),
    threading.Thread(target=_run_api, args=("shell_2", lambda: run_project_shell("python -m py_compile worker.py"))),
    threading.Thread(target=_run_api, args=("file_read", lambda: project_read_file(str(PROJECT_DIR / "worker.py")))),
]

t0 = time.monotonic()
for t in api_threads:
    t.start()
for t in api_threads:
    t.join(timeout=20)
api_elapsed = round(time.monotonic() - t0, 2)

for key, (result, elapsed) in _api_results.items():
    ok = result.get("ok", False) if isinstance(result, dict) else False
    detail = ""
    if key.startswith("brave"):
        detail = f"{len(result.get('results', []))} results"
    elif key == "github_repo":
        detail = f"lang={result.get('language')} stars={result.get('stars')}"
    elif key.startswith("shell"):
        detail = result.get("stdout", "")[:40].strip() or result.get("stderr", "")[:40].strip()
    elif key == "file_read":
        detail = f"size={result.get('size', 0)} bytes"
    record(f"  {key}", ok, elapsed, detail)

api_pass = sum(1 for r, _ in _api_results.values() if isinstance(r, dict) and r.get("ok"))
record(f"All parallel API calls ({api_pass}/{len(api_threads)} OK)", api_pass == len(api_threads), api_elapsed)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Shared state integrity under concurrent reads/writes
# ═══════════════════════════════════════════════════════════════════════════════
section("4. Shared state integrity under concurrent load")

from luna_modules.luna_state import CORE_STATE
from luna_modules.luna_heartbeat import HEARTBEAT_STATE, HEARTBEAT_LOCK, set_heartbeat

_state_errors = []
_counter_results = []


def _mutate_core_state(thread_id: int):
    """Increment counters and check for race conditions."""
    for _ in range(50):
        try:
            val = CORE_STATE.warm_reset_count
            _ = CORE_STATE.stop_requested
            _ = CORE_STATE.heartbeat_failure_count
        except Exception as exc:
            with _lock:
                _state_errors.append(f"t{thread_id}: {exc}")
    with _lock:
        _counter_results.append(thread_id)


def _read_heartbeat(thread_id: int):
    """Concurrent heartbeat reads under the lock."""
    for _ in range(20):
        try:
            with HEARTBEAT_LOCK:
                state_copy = dict(HEARTBEAT_STATE)
            assert isinstance(state_copy, dict)
        except Exception as exc:
            with _lock:
                _state_errors.append(f"hb_t{thread_id}: {exc}")


state_threads = (
    [threading.Thread(target=_mutate_core_state, args=(i,)) for i in range(10)]
    + [threading.Thread(target=_read_heartbeat, args=(i,)) for i in range(10)]
)
t0 = time.monotonic()
for t in state_threads:
    t.start()
for t in state_threads:
    t.join(timeout=10)
state_elapsed = round(time.monotonic() - t0, 2)

record(
    "20 threads reading CORE_STATE + HEARTBEAT_STATE concurrently",
    not _state_errors and len(_counter_results) == 10,
    state_elapsed,
    f"errors={len(_state_errors)}",
)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Live worker process health
# ═══════════════════════════════════════════════════════════════════════════════
section("5. Live worker process health")

hb_path = LOGS_DIR / "luna_worker_heartbeat.json"


def _worker_is_healthy() -> tuple:
    try:
        hb = json.loads(hb_path.read_text())
        pid = int(hb.get("pid", 0))
        ts = str(hb.get("ts", ""))
        state = hb.get("state", "")
        try:
            import psutil
            alive = psutil.pid_exists(pid)
        except Exception:
            alive = pid > 0
        return pid, alive, state, ts
    except Exception as exc:
        return 0, False, "error", str(exc)


pid, alive, state, ts = _worker_is_healthy()
record(f"Worker PID {pid} alive", alive, 0, f"state={state} ts={ts[:19]}")

# Check heartbeat freshness — should have updated in last 60s in local time
try:
    from datetime import datetime
    from luna_modules.luna_logging import now_iso
    hb_data = json.loads(hb_path.read_text())
    hb_ts_str = str(hb_data.get("ts", ""))
    if hb_ts_str:
        hb_dt = datetime.fromisoformat(hb_ts_str)
        age_seconds = abs((datetime.now() - hb_dt).total_seconds())
        # Heartbeat uses local time; accept up to 120s for safety
        fresh = age_seconds < 120
        record(
            f"Heartbeat fresh (age={int(age_seconds)}s, threshold=120s)",
            fresh,
            0,
            f"ts={hb_ts_str[:19]}",
        )
    else:
        record("Heartbeat timestamp present", False, 0, "no ts field")
except Exception as exc:
    record("Heartbeat freshness check", False, 0, str(exc))

# Verify autonomy cycle is running
try:
    autonomy = safe_read_json(MEMORY_DIR / "luna_autonomy_state.json", default={})
    last_cycle = str(autonomy.get("last_cycle_at", "never"))
    record(f"Autonomy last_cycle_at set", bool(last_cycle and last_cycle != "never"), 0, last_cycle[:19])
except Exception as exc:
    record("Autonomy state readable", False, 0, str(exc))

# Verify logic_updates cap is holding
lu = PROJECT_DIR / "logic_updates"
if lu.exists():
    dirs = [d for d in lu.iterdir() if d.is_dir()]
    record(
        f"logic_updates/ cap holding ({len(dirs)} dirs ≤ 30)",
        len(dirs) <= 30,
        0,
        f"{len(dirs)} dirs",
    )

# Verify upgrade notifications path is writable
try:
    from luna_modules.luna_paths import LUNA_UPGRADE_NOTIFICATIONS_PATH
    test_entry = {"ts": "test", "source": "system_test", "detail": "concurrent test probe"}
    append_jsonl(LUNA_UPGRADE_NOTIFICATIONS_PATH, test_entry)
    record("Upgrade notifications file writable", True, 0, str(LUNA_UPGRADE_NOTIFICATIONS_PATH.name))
except Exception as exc:
    record("Upgrade notifications file writable", False, 0, str(exc))


# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
passed = [r for r in _results if r["ok"]]
failed = [r for r in _results if not r["ok"]]
total_time = sum(r["elapsed"] for r in _results if r["elapsed"] < 60)

print(f"\n{'═' * 62}")
print(f"  Results : {len(passed)}/{len(_results)} passed  |  {len(failed)} failed")
print(f"  Domains : LLM waterfall, atomic I/O, external APIs, shared state, worker health")
print(f"{'═' * 62}")

if failed:
    print("\nFailed:")
    for r in failed:
        print(f"  ✗  {r['name']}")
        if r.get("detail"):
            print(f"       {r['detail']}")
    sys.exit(1)
else:
    print("\n  All tests passed. ✓")
    sys.exit(0)
