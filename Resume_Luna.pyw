# Resume_Luna.pyw — turn always-on Luna ON and start her now. Hidden via pythonw.
# Removes the pause flag, then launches the voice assistant (windowless).
import subprocess
from pathlib import Path

flag = Path(r"D:\SurgeApp\memory\kill_switches\jarvis.disabled")
try:
    if flag.exists():
        flag.unlink()
except Exception:
    pass

pyw = r"D:\SurgeApp\.aider_venv\Scripts\pythonw.exe"
subprocess.Popen(
    [pyw, r"D:\SurgeApp\luna_jarvis.py"],
    cwd=r"D:\SurgeApp",
    creationflags=0x08000000,   # CREATE_NO_WINDOW — never flash a console
)
