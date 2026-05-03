"""Launch the Luna read-only HTTP dashboard in chromeless app-window mode.

Double-clicking this .pyw boots the dashboard server on http://127.0.0.1:8765
and opens it inside Chrome / Edge / Brave with ``--app=`` so there is no
URL bar, no tab strip, no extension toolbar — Luna fills the whole window
like a native command-center app. If no Chromium browser is installed,
falls back to the system default browser.

No console window; no shell calls into project files; no package installs;
no live execution. The launched browser process is the user's own browser
in app-window mode — exactly what they would see if they pressed
``Install Luna`` on a PWA, only built from a stdlib helper instead.

Phase UI-1A — Luna Futuristic HTTP Dashboard Foundation.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import webbrowser
from pathlib import Path

# Ensure project root is importable when launched by double-click.
_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from luna_modules import luna_http_dashboard as hd


# Common Chromium-family install locations on Windows. Each entry is a
# (binary path, friendly name) pair. We probe in priority order; first hit
# wins. The user's installed browser stays the source of truth — this
# script only opens it.
def _candidate_browsers() -> list[tuple[Path, str]]:
    pf = os.environ.get("ProgramFiles", r"C:\Program Files")
    pf86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
    local = os.environ.get("LocalAppData", str(Path.home() / "AppData" / "Local"))
    return [
        (Path(pf) / "Google" / "Chrome" / "Application" / "chrome.exe", "Chrome"),
        (Path(pf86) / "Google" / "Chrome" / "Application" / "chrome.exe", "Chrome"),
        (Path(local) / "Google" / "Chrome" / "Application" / "chrome.exe", "Chrome"),
        (Path(pf) / "Microsoft" / "Edge" / "Application" / "msedge.exe", "Edge"),
        (Path(pf86) / "Microsoft" / "Edge" / "Application" / "msedge.exe", "Edge"),
        (Path(pf) / "BraveSoftware" / "Brave-Browser" / "Application" / "brave.exe", "Brave"),
        (Path(pf86) / "BraveSoftware" / "Brave-Browser" / "Application" / "brave.exe", "Brave"),
    ]


def _find_chromium() -> tuple[Path, str] | None:
    for p, name in _candidate_browsers():
        if p.exists() and p.is_file():
            return p, name
    return None


def _open_in_app_mode(url: str) -> None:
    """Open ``url`` in a Chromium app window if possible; otherwise fall back."""
    time.sleep(0.5)  # tiny delay so the server is accepting before we ask
    chrome = _find_chromium()
    if chrome is None:
        try:
            webbrowser.open(url, new=2, autoraise=True)
        except Exception:  # noqa: BLE001
            pass
        return

    binary, _name = chrome
    # We deliberately use os.spawnv with P_NOWAIT — no shell, no PATH lookup,
    # exact absolute binary, fixed argv. This is the same as letting the user
    # double-click their browser and visiting the URL, just chromeless.
    args = [
        str(binary),
        f"--app={url}",
        "--window-size=1480,940",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-features=Translate",
        # Keep state out of the user's normal profile; Luna runs sandboxed:
        f"--user-data-dir={Path.home() / '.luna_dashboard_app_profile'}",
    ]
    try:
        os.spawnv(os.P_NOWAIT, str(binary), args)
    except OSError:
        # Last-resort: ordinary default-browser open.
        try:
            webbrowser.open(url, new=2, autoraise=True)
        except Exception:  # noqa: BLE001
            pass


def main() -> int:
    url = f"http://{hd.DEFAULT_HOST}:{hd.DEFAULT_PORT}/"
    threading.Thread(target=_open_in_app_mode, args=(url,), daemon=True).start()
    hd.serve_forever(host=hd.DEFAULT_HOST, port=hd.DEFAULT_PORT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
