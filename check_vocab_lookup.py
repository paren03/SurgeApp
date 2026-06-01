"""Silent test of the bilingual vocab lookup (read-only, no voice/models)."""
import sys
import time

sys.path.insert(0, r"D:\SurgeApp")
from luna_modules import cognitive_bilingual_vocab_lookup as v  # noqa: E402

QUERIES = [
    "Tell me about freedom and knowledge and truth",
    "Расскажи про свободу истину и знание",
]
for q in QUERIES:
    t = time.perf_counter()
    hits = v.lookup(q)
    dt = (time.perf_counter() - t) * 1000.0
    print(f"{dt:6.1f} ms  hits={len(hits)}  -> {[h['word'] for h in hits]}")

print("\n--- prompt block (EN sample) ---")
print(v.as_prompt_block("Tell me about freedom and knowledge"))
print("\n--- read-only proof: attempt a write ---")
import sqlite3  # noqa: E402
try:
    rw = sqlite3.connect(
        "file:D:/SurgeApp/bilingual_links.sqlite?mode=ro", uri=True, timeout=5)
    rw.execute("CREATE TABLE _probe_should_fail(x)")
    print("WRITE SUCCEEDED — NOT read-only!")
except Exception as exc:
    print(f"write blocked (good): {type(exc).__name__}: {exc}")
