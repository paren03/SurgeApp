"""Launch the Luna read-only HTTP dashboard.

Double-clicking this .pyw boots the dashboard at http://127.0.0.1:8765 in
the background and opens it in the default browser. No console window;
no shell calls; no package installs; no live execution.

Phase UI-1A — Luna Futuristic HTTP Dashboard Foundation.
"""
from __future__ import annotations

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


def _open_browser_when_ready(url: str) -> None:
    # Tiny delay so the server's accept loop is up before the browser asks.
    time.sleep(0.5)
    try:
        webbrowser.open(url, new=2, autoraise=True)
    except Exception:  # noqa: BLE001
        pass


def main() -> int:
    url = f"http://{hd.DEFAULT_HOST}:{hd.DEFAULT_PORT}/"
    threading.Thread(target=_open_browser_when_ready, args=(url,), daemon=True).start()
    hd.serve_forever(host=hd.DEFAULT_HOST, port=hd.DEFAULT_PORT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
