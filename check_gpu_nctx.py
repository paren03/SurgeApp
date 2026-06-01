"""Honest GPU-fit check for n_ctx=4096: load the 8B main brain and confirm it
stays on GPU (not CPU fallback). Generates TEXT only — no voice."""
import sys
import time

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_sovereign_main_runtime as smr  # noqa: E402

print("configured n_ctx:", smr._gpu_n_ctx())
CLF = {"category": "casual", "text_chars": 24}
# Gen #1 = COLD (loads model + warms CUDA). Untimed-as-real.
t0 = time.perf_counter()
r0 = smr.generate_main(incoming_text="Say hi.", classification=CLF,
                       recent_turns=[], mode="good_luna", timeout_s=120.0)
print(f"COLD gen: ok={r0.get('ok')} backend={r0.get('backend')} "
      f"s={round(time.perf_counter()-t0,1)} reply={(r0.get('text') or '')[:40]!r}")
# Gen #2 = WARM (model already loaded). This is the real test.
t = time.perf_counter()
r = smr.generate_main(
    incoming_text="In one short sentence, how are you today?",
    classification=CLF, recent_turns=[], mode="good_luna", timeout_s=60.0)
dt = time.perf_counter() - t
print("WARM ok:", r.get("ok"), "| backend:", r.get("backend"),
      "| gen s:", round(dt, 1))
try:
    d = smr.get_singleton().details()
    print("details backend:", d.get("backend"),
          "| load_elapsed_s:", d.get("load_elapsed_s"),
          "| last_gen_ms:", d.get("last_call_elapsed_ms"))
except Exception as exc:  # noqa: BLE001
    print("details err:", exc)
print("reply:", (r.get("text") or "")[:60])
