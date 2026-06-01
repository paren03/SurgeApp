"""Silent proof of the _append_audit amortized-truncation fix (no voice)."""
import os
import sys
import tempfile
import time

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_conversation_runtime as crt   # noqa: E402
from luna_modules import cognitive_operator_controls as oc       # noqa: E402

FLAG = "cognitive_conversation_audit_lazy_truncate_enabled"


def main() -> None:
    # Build a ~1.6 MB temp ledger (1000 fat lines) and point the module at it.
    crt.AUDIT_PATH = os.path.join(tempfile.gettempdir(),
                                  "luna_audit_trim_probe.jsonl")
    fat = '{"x":"' + ("y" * 1500) + '"}\n'
    with open(crt.AUDIT_PATH, "w", encoding="utf-8") as fh:
        for _ in range(1000):
            fh.write(fat)
    mb = os.path.getsize(crt.AUDIT_PATH) / 1024 / 1024
    print(f"probe ledger: {mb:.2f} MB, 1000 lines")

    rec = {"event": "probe", "data": "z" * 200}

    def per_append(n: int) -> float:
        t0 = time.perf_counter()
        for _ in range(n):
            crt._append_audit(rec)
        return (time.perf_counter() - t0) / n * 1000.0

    oc.set_flag(FLAG, False)
    crt._AUDIT_APPENDS_SINCE_TRUNCATE[0] = 0
    off = per_append(60)
    oc.set_flag(FLAG, True)
    crt._AUDIT_APPENDS_SINCE_TRUNCATE[0] = 0
    on = per_append(60)

    print(f"OFF (truncate-check every append): {off:.2f} ms/append")
    print(f"ON  (amortized every {crt._AUDIT_TRUNCATE_EVERY}):       "
          f"{on:.2f} ms/append")
    if on > 0:
        print(f"speedup: {off / on:.1f}x  ({off - on:.2f} ms saved/turn)")
    try:
        os.remove(crt.AUDIT_PATH)
    except Exception:
        pass


if __name__ == "__main__":
    main()
