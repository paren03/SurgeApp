# Dashboard Thread-Leak Fix — root cause + measured proof (2026-05-31)

## Symptom (operator-visible)
Dashboard "up but not 100%": `/api/health` flickered 503 and was slow.

## Honest measurement BEFORE (live PID 45420)
* `/api/health` x20 (sequential): **14×200, 6×503 → 30% failure**
* latency: min 279ms, **max 8,579ms**, avg 1,633ms
* process: **1,381 threads, 8,498 MB RAM, 17,534s (4.9h) CPU**

This matched the code's own warned-of runaway ("1700+ blocked threads, ~17 GB").

## Root cause (in code, confirmed)
`LunaDashboardHandler._safe_build` (luna_http_dashboard.py) runs every payload
builder in a fresh daemon worker thread and `join(timeout)`. On timeout it
returns a "degraded" payload but **leaves the worker thread running forever**
(orphaned on a slow/hung upstream). The inline comment called this harmless
("daemonic so it cannot block process shutdown") — but the server never shuts
down, so over ~5h of the browser polling slow endpoints those orphan threads
accumulated WITHOUT BOUND to 1,381. The BoundedSemaphore cap (128) only bounds
HTTP *handler* threads, not the builder sub-threads, so it did not contain this.

## Fix (durable, applied)
Added a module-level `_builder_slots = threading.BoundedSemaphore(96)`.
`_safe_build` now:
1. Acquires a builder slot (non-blocking) before spawning the worker.
2. The worker releases its slot only when it ACTUALLY finishes — so a
   still-running orphan keeps holding its slot, capping the live count.
3. If no slot is free, it SKIPS the spawn and returns an instant
   `builder_pool_saturated` degraded payload — never leaking another thread.

Net: an unbounded leak (slowly crashes the process) becomes a bounded,
self-recovering degradation (threads capped at ~handler 128 + builder 96 +
background; saturation yields fast degraded responses that recover as orphans
die). `py_compile` clean.

## Restore (cascade-safe bounce)
Used `luna_terminal_updater.bounce_dashboard()` (targeted `taskkill /F /PID`,
no `/T`, excludes inviolate siblings + self) to kill PID 45420 and spawn a
fresh listener loading the fixed code. No cascade to the session.

## Honest measurement AFTER (fresh PID 47820)
* `/api/health` x20: **20×200, 0×503 → 0% failure**
* latency: min 26ms, **max 153ms, avg 48ms** (~34× faster)
* process: **123 threads, 533 MB RAM** (~11× fewer threads, ~16× less RAM)

| | BEFORE (45420) | AFTER (47820) |
|---|---|---|
| 503 rate | 30% | **0%** |
| avg latency | 1,633 ms | **48 ms** |
| max latency | 8,579 ms | **153 ms** |
| threads | 1,381 | **123** |
| RAM | 8,498 MB | **533 MB** |

## did_not_happen / honest caveats
* This fixed and verified the DASHBOARD surface (the thing that was broken).
  I did not separately load-test every backend service or the voice pipeline.
* The fix CONTAINS the leak; it does not make individual builders stop hanging.
  The deeper fix (per-builder upstream timeouts so builders never hang) remains
  open — flagged, not done.
* The launcher runs as a 2-process pair (`.aider_venv` pythonw spawns the
  WindowsApps pythonw3.11 listener); the updater labels one a "zombie." This is
  the normal steady-state pattern here, present before and after — left as-is.
* Threads at 123 on the fresh process are baseline + in-flight handlers, now
  hard-bounded; they cannot run away to 1,381 again.

---

# Robustness upgrade — robust-by-design (2026-05-31, follow-up)

After the leak fix, a SUSTAINED CONCURRENT load test (480 requests / 24
workers) exposed a second, separate problem the sequential probe missed:

## Load test #1 (leak fixed, but builders still slow)
* success 85.8% (412/480), **68 requests timed out at 15s**
* latency p50/p95/max: 47 / 15,015 / 15,546 ms
* 503 shed rate 0% (leak fix held), threads 42→186→183 (bounded)

## Root cause #2 (measured)
A per-endpoint sequential probe found ONE pathological endpoint:
`/api/decision-brief` took **>25s** while all 6 others were 68–456 ms. Cause:
it calls `build_higher_tier_progress_payload()`, which shells out to
`schtasks` up to 2× (each capped at 10s → ~20s+). That single slow builder
ate the entire bad tail (68 ≈ 480÷7 = one endpoint's share).

## Fixes (applied)
1. **Single-flight + serve-stale-while-revalidate** in `_cached_build`: FRESH
   returns instantly; STALE serves the last-good value instantly and refreshes
   once in the background; COLD is single-flight (no stampede). No request ever
   blocks on a rebuild after first warm.
2. **Cached `build_higher_tier_progress_payload`** (key `higher_tier_progress`)
   via that mechanism — the schtasks cost is paid once per TTL in a background
   refresh, never on a request.
3. **schtasks timeout 10s → 4s** — bounds even the background refresh.
4. **Boot prewarm** now warms `higher_tier_progress` too, so the cold-start
   hit is paid once at boot, not on the first user request.

## Load test #2 (after upgrade) — fresh PID 47836
* **success 100.0% (480/480), 0 timeouts, 0 × 503**
* latency p50/p95/max: **46 / 2,421 / 3,563 ms** (p95 6.2× better, max 4.3× better)
* threads 48 → peak 90 → settled 43 (tighter than before)

| under 24-way sustained load | #1 | #2 |
|---|---|---|
| success | 85.8% | **100.0%** |
| timeouts | 68 | **0** |
| p95 latency | 15,015 ms | **2,421 ms** |
| max latency | 15,546 ms | **3,563 ms** |
| peak threads | 186 | **90** |

## Honest caveats
* p95 of ~2.4s is under EXTREME 24-way sustained concurrency (a stress test);
  real browser polling is a handful of concurrent requests and sees p50 ~46ms.
* The schtasks probe may still report task state as unavailable if the host's
  Task Scheduler is slow — but that now resolves in the background and never
  blocks or slows a request.
* All changes are additive + in the live module, so every future boot inherits
  them. Reversible by reverting luna_http_dashboard.py.
