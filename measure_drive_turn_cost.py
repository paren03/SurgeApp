"""Isolated cost of the post-reply kernel/drive reasoning.

The async-postreply change (`cognitive_conversation_async_postreply_enabled`)
moves `cognitive_kernel_drive_engine.drive_turn(...)` OFF the reply's critical
path (fire-and-forget). drive_turn calls NO LLM -- it composes existing
subsystems -- so its cost is deterministic CPU/pipeline work, not noisy model
inference. Timing it directly is the honest proof of exactly how much the
change removes from the synchronous reply path.

Run: D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe D:\\SurgeApp\\measure_drive_turn_cost.py
"""
import sys
import time

sys.path.insert(0, "D:/SurgeApp")


def _ms() -> float:
    return time.perf_counter() * 1000.0


def main() -> None:
    from luna_modules import cognitive_kernel_drive_engine as kde
    try:
        from luna_modules import cognitive_conversation_classifier as clf
    except Exception:
        clf = None

    user_text = "what should we get done today?"
    if clf is not None:
        try:
            classification = clf.classify(user_text)
        except Exception:
            classification = {"category": "task", "confidence": 0.9}
    else:
        classification = {"category": "task", "confidence": 0.9}

    main_reply = {
        "text": "Let's knock out the latency work first, then the dashboard.",
        "backend": "sovereign_local_dynamic_no_ollama_no_canned",
        "ok": True,
    }

    print("kernel_drive enabled :", getattr(kde, "_enabled", lambda: "n/a")())

    samples = []
    for i in range(5):
        t0 = _ms()
        out = kde.drive_turn(
            user_text=user_text,
            classification=classification,
            mode="good",
            recent_turns=[],
            main_reply=main_reply,
            turn_id=f"drive_probe_{i}",
            caller="drive_turn_isolation_probe",
        )
        dt = _ms() - t0
        samples.append(dt)
        print(f"  call {i}: {dt:8.1f} ms   ok={out.get('ok')} "
              f"reason={out.get('reason', '-')}")

    warm = samples[1:]  # drop call 0 (cold imports)
    warm_sorted = sorted(warm)
    median = warm_sorted[len(warm_sorted) // 2]
    print(f"\ncold call (0)        : {samples[0]:8.1f} ms")
    print(f"warm calls (1-4)     : "
          + ", ".join(f"{x:.1f}" for x in warm) + " ms")
    print(f"warm median          : {median:8.1f} ms  "
          f"<-- removed from the synchronous reply path when async ON")


if __name__ == "__main__":
    main()
