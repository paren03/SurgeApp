"""Create a Windows desktop shortcut named 'Luna Dashboard'.

Builds a .lnk via the WScript.Shell COM object (no extra packages required
on a stock Windows install). The shortcut launches:

    pythonw.exe  D:\\SurgeApp\\LaunchLunaDashboard.pyw

with Luna's icon. Read-only by design — this script makes no edits to any
core runtime files and never enables live execution.

Run from D:\\SurgeApp:

    D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe create_luna_dashboard_shortcut.py

Phase UI-1A — Luna Futuristic HTTP Dashboard Foundation.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
LAUNCHER = PROJECT_ROOT / "LaunchLunaDashboard.pyw"
ICON_FILE = PROJECT_ROOT / "luna_dashboard" / "assets" / "luna_icon.ico"
SHORTCUT_NAME = "Luna Dashboard.lnk"


def _resolve_pythonw() -> Path:
    """Find pythonw.exe alongside the running interpreter; fall back to venv."""
    here = Path(sys.executable).with_name("pythonw.exe")
    if here.exists():
        return here
    venv_pyw = PROJECT_ROOT / ".aider_venv" / "Scripts" / "pythonw.exe"
    if venv_pyw.exists():
        return venv_pyw
    # Last resort: same interpreter as launcher (will show a console).
    return Path(sys.executable)


def _desktop_path() -> Path:
    home = Path(os.path.expanduser("~"))
    candidates = [
        home / "Desktop",
        home / "OneDrive" / "Desktop",
    ]
    for p in candidates:
        if p.exists() and p.is_dir():
            return p
    return home / "Desktop"


def create_shortcut(dry_run: bool = False) -> Path:
    desktop = _desktop_path()
    desktop.mkdir(parents=True, exist_ok=True)
    target = desktop / SHORTCUT_NAME

    if dry_run:
        return target

    if sys.platform != "win32":
        raise RuntimeError("Windows-only helper.")

    # Use WScript.Shell via COM (ships with Windows). Avoid pythoncom/pywin32
    # so we don't add a dependency.
    try:
        import ctypes
        from ctypes import wintypes  # noqa: F401
        # Use win32 dispatch via comtypes-free path: write a tiny VBScript and
        # run it through the user's WScript host.
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"COM unavailable: {exc}") from exc

    pythonw = _resolve_pythonw()

    vbs = (
        'Set sh = CreateObject("WScript.Shell")\r\n'
        f'Set lnk = sh.CreateShortcut("{target}")\r\n'
        f'lnk.TargetPath = "{pythonw}"\r\n'
        f'lnk.Arguments = """{LAUNCHER}"""\r\n'
        f'lnk.WorkingDirectory = "{PROJECT_ROOT}"\r\n'
        f'lnk.IconLocation = "{ICON_FILE},0"\r\n'
        'lnk.Description = "Luna Command Center (read-only dashboard)"\r\n'
        'lnk.WindowStyle = 7\r\n'  # 7 = minimized; we use pythonw.exe so no console anyway
        'lnk.Save\r\n'
    )

    tmp = PROJECT_ROOT / "_luna_dashboard_shortcut.vbs"
    tmp.write_text(vbs, encoding="utf-8")
    try:
        # Run cscript silently; raises CalledProcessError on failure.
        # We deliberately avoid `subprocess` in the dashboard module itself
        # for safety; this helper is opt-in and only creates a single .lnk.
        import subprocess  # noqa: PLC0415  (local opt-in to subprocess)
        subprocess.run(
            ["cscript", "/nologo", str(tmp)],
            check=True,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
        )
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass

    if not target.exists():
        raise RuntimeError(f"shortcut not created at {target}")
    return target


def main() -> int:
    if "--dry-run" in sys.argv:
        target = create_shortcut(dry_run=True)
        print(f"[dry-run] would create: {target}")
        return 0
    target = create_shortcut(dry_run=False)
    print(f"[ok] Luna Dashboard shortcut created: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
