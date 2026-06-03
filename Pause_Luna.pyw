# Pause_Luna.pyw — turn always-on Luna OFF (graceful). Runs hidden via pythonw.
# Creating this flag makes a running Luna shut down within ~3s, and stops her
# from auto-starting at login until the flag is removed (Resume_Luna).
from pathlib import Path

ks = Path(r"D:\SurgeApp\memory\kill_switches")
ks.mkdir(parents=True, exist_ok=True)
(ks / "jarvis.disabled").write_text("paused by operator\n", encoding="utf-8")
