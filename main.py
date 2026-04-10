import sys
import os
import shutil
import hashlib
import secrets
import threading
import time as _time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import xxhash
import send2trash

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QTreeView, QFrame, QProgressBar, QSplitter,
    QStatusBar, QHeaderView, QMessageBox, QAbstractItemView,
    QLineEdit, QStackedWidget, QMenu, QFileDialog, QInputDialog,
    QDialog, QDialogButtonBox, QTextEdit
)
from PyQt6.QtGui import (
    QStandardItemModel, QStandardItem, QColor, QFont, QPalette,
    QAction, QShortcut, QKeySequence
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QDir, QSortFilterProxyModel, QModelIndex, QTimer
)

try:
    from PyQt6.QtWidgets import QFileSystemModel
except ImportError:
    from PyQt6.QtGui import QFileSystemModel  # type: ignore

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------
DARK_BG  = "#0d0d0d"
PANEL_BG = "#141414"
CARD_BG  = "#1a1a1a"
ACCENT   = "#e63946"
TEXT     = "#e8e8e8"
SUB      = "#888888"
BORDER   = "#2a2a2a"

SS = f"""
QMainWindow, QWidget {{
    background: {DARK_BG};
    color: {TEXT};
    font-family: 'Segoe UI', sans-serif;
    font-size: 13px;
}}
QTreeView {{
    background: {CARD_BG};
    alternate-background-color: {PANEL_BG};
    border: 1px solid {BORDER};
    selection-background-color: {ACCENT};
    selection-color: #fff;
    outline: 0;
}}
QTreeView::item {{ padding: 3px 6px; min-height: 22px; }}
QTreeView::item:hover {{ background: #252525; }}
QHeaderView::section {{
    background: {PANEL_BG}; color: {SUB}; border: none;
    border-bottom: 1px solid {BORDER}; padding: 5px 8px; font-size: 11px;
}}
QLineEdit {{
    background: {CARD_BG}; color: {TEXT}; border: 1px solid {BORDER};
    border-radius: 6px; padding: 5px 10px;
    selection-background-color: {ACCENT};
}}
QLineEdit:focus {{ border-color: {ACCENT}; }}
QPushButton {{
    color: {TEXT}; background: {PANEL_BG}; border: 1px solid {BORDER};
    border-radius: 6px; padding: 5px 14px;
}}
QPushButton:hover {{ background: #222; border-color: #444; }}
QPushButton:disabled {{ color: #3a3a3a; border-color: {BORDER}; }}
QProgressBar {{
    background: {CARD_BG}; border: none; border-radius: 2px;
}}
QProgressBar::chunk {{ background: {ACCENT}; border-radius: 2px; }}
QStatusBar {{
    background: {PANEL_BG}; color: {SUB}; border-top: 1px solid {BORDER};
    font-size: 11px; padding: 0 8px;
}}
QMenu {{
    background: {CARD_BG}; color: {TEXT}; border: 1px solid {BORDER};
    border-radius: 6px; padding: 4px;
}}
QMenu::item {{ padding: 7px 22px; border-radius: 4px; }}
QMenu::item:selected {{ background: {ACCENT}; color: #fff; }}
QMenu::separator {{ height: 1px; background: {BORDER}; margin: 3px 6px; }}
QSplitter::handle {{ background: {BORDER}; }}
QScrollBar:vertical {{
    background: {DARK_BG}; width: 7px; border-radius: 3px; margin: 0;
}}
QScrollBar::handle:vertical {{
    background: #2e2e2e; border-radius: 3px; min-height: 20px;
}}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
QScrollBar:horizontal {{
    background: {DARK_BG}; height: 7px; border-radius: 3px; margin: 0;
}}
QScrollBar::handle:horizontal {{
    background: #2e2e2e; border-radius: 3px; min-width: 20px;
}}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0; }}
QMessageBox {{ background: {CARD_BG}; }}
QMessageBox QLabel {{ color: {TEXT}; }}
QDialog {{ background: {CARD_BG}; }}
QTextEdit {{
    background: {DARK_BG}; color: {TEXT};
    border: 1px solid {BORDER}; border-radius: 6px;
}}
"""

BTN_ACCENT  = (f"QPushButton{{background:{ACCENT};color:#fff;border:none;"
               f"border-radius:6px;padding:7px 20px;font-weight:bold;letter-spacing:1px;}}"
               f"QPushButton:hover{{background:#ff4d5a;}}"
               f"QPushButton:disabled{{background:#2a2a2a;color:#555;border:none;}}")
BTN_OUTLINE = (f"QPushButton{{background:transparent;color:{ACCENT};"
               f"border:1px solid {ACCENT};border-radius:6px;padding:5px 14px;}}"
               f"QPushButton:hover{{background:rgba(230,57,70,0.10);}}"
               f"QPushButton:disabled{{color:#444;border-color:#333;}}")
BTN_MODE_ON = (f"QPushButton{{background:{ACCENT};color:#fff;border:none;"
               f"border-radius:6px;padding:5px 16px;font-weight:600;}}")
BTN_MODE_OFF= (f"QPushButton{{background:transparent;color:{SUB};"
               f"border:1px solid {BORDER};border-radius:6px;padding:5px 16px;}}"
               f"QPushButton:hover{{color:{TEXT};border-color:#444;}}")
BTN_NAV     = (f"QPushButton{{background:transparent;border:none;color:{SUB};"
               f"font-size:14px;border-radius:5px;padding:4px 8px;}}"
               f"QPushButton:hover{{color:{TEXT};background:#1e1e1e;}}"
               f"QPushButton:disabled{{color:#2e2e2e;}}")
BTN_CMD     = (f"QPushButton{{background:transparent;border:none;color:{TEXT};"
               f"border-radius:5px;padding:5px 10px;font-size:12px;min-width:36px;}}"
               f"QPushButton:hover{{background:#222;}}"
               f"QPushButton:pressed{{background:#2a2a2a;}}"
               f"QPushButton:disabled{{color:#3a3a3a;}}")


# ---------------------------------------------------------------------------
# Worker: 3-phase parallel duplicate scanner
# ---------------------------------------------------------------------------
class ScanWorker(QThread):
    progress = pyqtSignal(int, int)
    phase    = pyqtSignal(str)
    found    = pyqtSignal(dict)
    error    = pyqtSignal(str)
    finished = pyqtSignal()

    _THREADS = min(32, max(4, (os.cpu_count() or 2) * 2))
    _PARTIAL = 65_536
    _CHUNK   = 1 << 20

    def __init__(self, folder: str):
        super().__init__()
        self.folder = folder
        self._stop  = threading.Event()
        self._pause = threading.Event()   # set = paused, clear = running

    def abort(self):
        self._stop.set()
        self._pause.clear()   # unblock if paused so thread can exit

    def pause(self):
        self._pause.set()

    def resume(self):
        self._pause.clear()

    def run(self):
        try:
            self._scan()
        except Exception as exc:
            self.error.emit(str(exc))
        finally:
            self.finished.emit()

    def _scan(self):
        stop  = self._stop
        pause = self._pause

        def _check_pause():
            """Block here while paused; return True if stop was requested."""
            while pause.is_set():
                if stop.is_set():
                    return True
                threading.Event().wait(0.1)
            return stop.is_set()

        # Phase 1: index by size
        self.phase.emit("Phase 1/3 — Indexing files...")
        size_map: dict[int, list[str]] = defaultdict(list)
        stack = [self.folder]
        while stack and not stop.is_set():
            cur = stack.pop()
            try:
                with os.scandir(cur) as it:
                    for e in it:
                        if stop.is_set():
                            return
                        try:
                            if e.is_dir(follow_symlinks=False):
                                stack.append(e.path)
                            elif e.is_file(follow_symlinks=False):
                                sz = e.stat(follow_symlinks=False).st_size
                                if sz > 0:
                                    size_map[sz].append(e.path)
                        except OSError:
                            pass
            except (OSError, PermissionError):
                pass

        flat = [p for grp in size_map.values() if len(grp) >= 2 for p in grp]
        if not flat or stop.is_set():
            self.found.emit({})
            return

        total = len(flat)
        self.phase.emit(f"Phase 2/3 — Quick scan  ({total:,} candidates)...")
        self.progress.emit(0, total)

        # Phase 2: partial hash (64 KB)
        partial_map: dict[int, list[str]] = defaultdict(list)
        done = 0
        pb   = self._PARTIAL

        def _quick(path):
            if stop.is_set():
                return None, None
            try:
                h = xxhash.xxh3_128()
                with open(path, "rb") as f:
                    h.update(f.read(pb))
                return path, h.intdigest()
            except OSError:
                return None, None

        with ThreadPoolExecutor(max_workers=self._THREADS) as pool:
            for path, digest in pool.map(_quick, flat, chunksize=128):
                if _check_pause(): return
                done += 1
                self.progress.emit(done, total)
                if path and digest is not None:
                    partial_map[digest].append(path)

        deep = [p for grp in partial_map.values() if len(grp) >= 2 for p in grp]
        if not deep or stop.is_set():
            self.found.emit({})
            return

        total2 = len(deep)
        self.phase.emit(f"Phase 3/3 — Deep scan  ({total2:,} files)...")
        self.progress.emit(0, total2)

        # Phase 3: full hash (1 MB chunks)
        hash_map: dict[int, list[str]] = defaultdict(list)
        done2 = 0
        chunk = self._CHUNK

        def _full(path):
            if stop.is_set():
                return None, None
            try:
                h = xxhash.xxh3_128()
                with open(path, "rb") as f:
                    while True:
                        buf = f.read(chunk)
                        if not buf:
                            break
                        h.update(buf)
                return path, h.intdigest()
            except OSError:
                return None, None

        with ThreadPoolExecutor(max_workers=self._THREADS) as pool:
            for path, digest in pool.map(_full, deep, chunksize=32):
                if _check_pause():
                    return
                done2 += 1
                self.progress.emit(done2, total2)
                if path and digest is not None:
                    hash_map[digest].append(path)

        dupes = {str(d): paths for d, paths in hash_map.items() if len(paths) >= 2}
        self.found.emit(dupes)


# ---------------------------------------------------------------------------
# Worker: DoD 5220.22-M 3-pass secure delete (files + folders + drives)
# ---------------------------------------------------------------------------
class ShredWorker(QThread):
    progress = pyqtSignal(int, int)
    shredded = pyqtSignal(str)
    error    = pyqtSignal(str)
    log      = pyqtSignal(str)
    status   = pyqtSignal(str)    # current file name being shredded
    finished = pyqtSignal()

    def __init__(self, targets: list[str]):
        super().__init__()
        self.targets = targets      # may include files, folders, or drive roots
        self._stop_event  = threading.Event()
        self._pause_event = threading.Event()

    def pause(self):
        self._pause_event.set()

    def resume(self):
        self._pause_event.clear()

    def stop(self):
        self._stop_event.set()
        self._pause_event.clear()   # unblock if paused so thread can exit

    def _check_pause_stop(self) -> bool:
        """Block while paused. Returns True if stop was requested."""
        while self._pause_event.is_set():
            if self._stop_event.is_set():
                return True
            threading.Event().wait(0.1)
        return self._stop_event.is_set()

    def run(self):
        import stat as _stat
        import subprocess
        import uuid
        import time

        targets = [_resolve_drop_path(t) or os.path.normpath(t) for t in self.targets]

        # ── Step 0: Pause OneDrive so it cannot re-sync during shredding ──
        # OneDrive instantly re-downloads deleted files — we stop it first,
        # do all the work, then restart it when we're done.
        onedrive_was_running = self._pause_onedrive()
        if onedrive_was_running:
            self.log.emit("OneDrive paused for shredding...")
            time.sleep(1)   # give it a moment to fully stop

        try:
            all_files: list[str] = []
            top_dirs:  list[str] = []

            for target in targets:
                if os.path.isfile(target):
                    # Force-hydrate single OneDrive placeholder by reading it
                    self._hydrate_file(target)
                    all_files.append(target)
                elif os.path.isdir(target):
                    top_dirs.append(target)
                    self.log.emit(f"Scanning: {target}")

                    # Force-hydrate all placeholder files inside the folder
                    # by reading each one — this triggers OneDrive download
                    try:
                        subprocess.run(
                            ["attrib", "-h", "-r", "-s", "/s", "/d", target],
                            capture_output=True, timeout=60
                        )
                    except Exception:
                        pass

                    found = 0
                    try:
                        for root, dirs, files in os.walk(target, topdown=False):
                            for name in files:
                                fp = os.path.join(root, name)
                                self._hydrate_file(fp)
                                all_files.append(fp)
                                found += 1
                    except Exception as e:
                        self.log.emit(f"  Walk error: {e}")

                    self.log.emit(f"  Found {found} file(s).")
                else:
                    self.error.emit(f"NOT FOUND: {target}")
                    self.log.emit(f"NOT FOUND: {target}")

            if not all_files and not top_dirs:
                self.log.emit("Nothing to shred.")
                return

            # ── Phase 1: 7-pass overwrite every file ──────────────────────
            total      = max(len(all_files), 1)
            shred_ok   = 0
            shred_fail = 0

            for i, path in enumerate(all_files, 1):
                # Honour pause/stop before each file
                if self._check_pause_stop():
                    self.log.emit("Shred stopped by user.")
                    break

                self.progress.emit(i, total)
                fname = os.path.basename(path)
                self.log.emit(f"Shredding ({i}/{total}): {fname}")
                self.status.emit(fname)   # live activity display

                try:
                    os.chmod(path, _stat.S_IWRITE | _stat.S_IREAD)
                except Exception:
                    pass

                unlock_info = self._force_unlock_handles(path)
                if unlock_info:
                    self.log.emit(f"  {unlock_info}")

                ok, msg = self._dod_shred(path)

                if not os.path.exists(path):
                    # File is gone — success regardless of ok flag
                    self.shredded.emit(path)
                    extra = f"  ({msg})" if msg else ""
                    self.log.emit(f"  Destroyed{extra}: {path}")
                    shred_ok += 1
                elif "PENDING_REBOOT" in (msg or ""):
                    # Data zeroed; OS will delete on next boot
                    self.shredded.emit(path)
                    self.log.emit(f"  Data zeroed — deletion pending reboot: {os.path.basename(path)}")
                    shred_ok += 1
                else:
                    self.error.emit(f"DELETE FAILED: {os.path.basename(path)}: {msg}")
                    self.log.emit(f"  FAILED: {path} | {msg}")
                    shred_fail += 1

            # ── Phase 2: remove folder trees ──────────────────────────────
            deleted_dirs: list[str] = []
            for top in top_dirs:
                if not os.path.exists(top):
                    deleted_dirs.append(top)
                    self.log.emit(f"  Folder already gone: {top}")
                    continue
                try:
                    # chmod the entire tree so rmtree can access everything
                    for root, dirs, files in os.walk(top):
                        for name in dirs + files:
                            try:
                                os.chmod(os.path.join(root, name),
                                         _stat.S_IWRITE | _stat.S_IREAD)
                            except Exception:
                                pass

                    # Unlock the folder itself (Restart Manager)
                    self._force_unlock_handles(top)

                    # Rename the folder before deleting — this breaks
                    # OneDrive's internal tracking so it can't re-create it
                    parent   = os.path.dirname(top)
                    tmp_name = os.path.join(parent, f"_surge_{uuid.uuid4().hex[:8]}")
                    try:
                        os.rename(top, tmp_name)
                        delete_target = tmp_name
                        self.log.emit(f"  Renamed to temp: {tmp_name}")
                    except Exception:
                        delete_target = top   # rename failed, try direct delete

                    shutil.rmtree(delete_target, onerror=self._rmtree_err)

                    if not os.path.exists(delete_target) and not os.path.exists(top):
                        deleted_dirs.append(top)
                        self.log.emit(f"  Folder destroyed: {top}")
                    else:
                        self.error.emit(
                            f"Folder still present after delete: {os.path.basename(top)}"
                        )
                        self.log.emit(f"  STILL EXISTS: {top}")
                except Exception as exc:
                    self.error.emit(
                        f"Folder removal error: {os.path.basename(top)}: {exc}"
                    )
                    self.log.emit(f"  Folder removal error: {top} | {exc}")

            # ── Phase 3: notify Windows Explorer immediately ───────────────
            _shell_notify_deleted(deleted_dirs + all_files)

            self.log.emit(f"\nDone.  {shred_ok} file(s) destroyed,  {shred_fail} failed.")

        finally:
            # Always restart OneDrive — even if shredding threw an exception
            if onedrive_was_running:
                self._resume_onedrive()
                self.log.emit("OneDrive restarted.")

    @staticmethod
    def _rmtree_err(func, path, exc_info):
        """onerror callback for shutil.rmtree — chmod and retry."""
        import stat
        try:
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
            func(path)
        except Exception:
            pass

    @staticmethod
    def _hydrate_file(path: str):
        """
        Force OneDrive to download a cloud-only placeholder by opening
        and reading the first byte. Windows triggers the download on first
        file access — after this the file is physically on disk.
        """
        try:
            with open(path, "rb") as f:
                f.read(1)
        except Exception:
            pass

    @staticmethod
    def _pause_onedrive() -> bool:
        """
        Gracefully shut down OneDrive so it cannot re-sync files during
        shredding. Returns True if OneDrive was running.
        """
        import subprocess
        onedrive = os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            "Microsoft", "OneDrive", "OneDrive.exe"
        )
        # Check if running
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq OneDrive.exe", "/NH"],
            capture_output=True, text=True
        )
        if "OneDrive.exe" not in result.stdout:
            return False
        # Shut it down gracefully
        try:
            subprocess.run([onedrive, "/shutdown"],
                           capture_output=True, timeout=10)
        except Exception:
            # Fallback: taskkill
            subprocess.run(
                ["taskkill", "/F", "/IM", "OneDrive.exe"],
                capture_output=True
            )
        return True

    @staticmethod
    def _resume_onedrive():
        """Restart OneDrive after shredding is complete."""
        import subprocess
        onedrive = os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            "Microsoft", "OneDrive", "OneDrive.exe"
        )
        try:
            if os.path.exists(onedrive):
                subprocess.Popen([onedrive])
        except Exception:
            pass

    # 7-pass overwrite schedule (enhanced DoD 5220.22-M):
    #   Pass 1: 0x00   Pass 2: 0xFF   Pass 3: random
    #   Pass 4: 0x00   Pass 5: 0xFF   Pass 6: random
    #   Pass 7: 0x00 (final verify-clean)
    _PASSES = [b"\x00", b"\xff", None, b"\x00", b"\xff", None, b"\x00"]

    @staticmethod
    def _force_unlock_handles(path: str) -> str:
        """
        Use Windows Restart Manager API to find and close every process
        that has an open handle on *path*, then terminate any that refuse
        to close.  This is the same mechanism as Eraser's
        ForceUnlockLockedFiles=1 setting.

        Returns a human-readable summary of what was done (empty = nothing
        needed unlocking).
        """
        import ctypes
        import ctypes.wintypes as wt

        try:
            rstrtmgr = ctypes.windll.RstrtMgr
        except OSError:
            return ""   # RstrtMgr.dll not available (Wine/older OS)

        # ── constants ────────────────────────────────────────────────────────
        RM_REBOOT_REASON_NONE = 0
        RmForceShutdown       = 1          # dwActionFlags for RmShutdown

        # ── structs ──────────────────────────────────────────────────────────
        class RM_UNIQUE_PROCESS(ctypes.Structure):
            _fields_ = [
                ("dwProcessId",    wt.DWORD),
                ("ProcessStartTime", wt.FILETIME),
            ]

        class RM_PROCESS_INFO(ctypes.Structure):
            _fields_ = [
                ("Process",            RM_UNIQUE_PROCESS),
                ("strAppName",         ctypes.c_wchar * 256),
                ("strServiceShortName",ctypes.c_wchar * 64),
                ("ApplicationType",    ctypes.c_int),
                ("AppStatus",          wt.ULONG),
                ("TSSessionId",        wt.DWORD),
                ("bRestartable",       wt.BOOL),
            ]

        # ── open a Restart Manager session ───────────────────────────────────
        session_handle = wt.DWORD(0)
        session_key    = ctypes.create_unicode_buffer(CCH_RM_SESSION_KEY := 32 + 1)
        try:
            CCH_RM_SESSION_KEY  # silence "referenced before assignment"
        except Exception:
            pass
        CCH_RM_SESSION_KEY = 33
        session_key = ctypes.create_unicode_buffer(CCH_RM_SESSION_KEY)

        rc = rstrtmgr.RmStartSession(
            ctypes.byref(session_handle), 0, session_key
        )
        if rc != 0:
            return ""

        msgs = []
        try:
            # Register the file (or the files in a folder) with the session
            files = (ctypes.c_wchar_p * 1)(path)
            rc = rstrtmgr.RmRegisterResources(
                session_handle, 1, files,
                0, None,   # no processes
                0, None    # no service names
            )
            if rc != 0:
                return ""

            # Query which processes are using it
            n_needed    = wt.UINT(0)
            n_info      = wt.UINT(4)
            proc_info   = (RM_PROCESS_INFO * 4)()
            reboot_reasons = wt.DWORD(0)

            while True:
                rc = rstrtmgr.RmGetList(
                    session_handle,
                    ctypes.byref(n_needed),
                    ctypes.byref(n_info),
                    proc_info,
                    ctypes.byref(reboot_reasons)
                )
                if rc == 0:
                    break
                if rc == 234:   # ERROR_MORE_DATA
                    n_info    = wt.UINT(n_needed.value)
                    proc_info = (RM_PROCESS_INFO * n_needed.value)()
                    continue
                break   # unexpected error

            count = n_info.value
            if count == 0:
                return ""

            for i in range(count):
                msgs.append(proc_info[i].strAppName or
                            f"PID {proc_info[i].Process.dwProcessId}")

            # Ask Restart Manager to shut down those apps; force-kill if needed
            rstrtmgr.RmShutdown(
                session_handle,
                RmForceShutdown,
                None    # no progress callback
            )
        finally:
            rstrtmgr.RmEndSession(session_handle)

        return "Unlocked from: " + ", ".join(msgs) if msgs else ""

    @staticmethod
    def _dod_shred(path: str) -> tuple[bool, str]:
        import stat, time

        try:
            # Strip read-only / hidden / system attributes
            current = os.stat(path).st_mode
            if not (current & stat.S_IWRITE):
                os.chmod(path, stat.S_IWRITE | stat.S_IREAD)

            size = os.path.getsize(path)
            if size == 0:
                os.remove(path)
                return True, ""

            # ── 7-pass DoD 5220.22-M overwrite ───────────────────────────
            with open(path, "r+b") as fh:
                for fill in ShredWorker._PASSES:
                    fh.seek(0)
                    written = 0
                    while written < size:
                        n = min(65536, size - written)
                        fh.write(secrets.token_bytes(n) if fill is None else fill * n)
                        written += n
                    fh.flush()
                    os.fsync(fh.fileno())
            # File handle is fully closed here.

            # ── Delete with unlock-retry loop ─────────────────────────────
            # Root cause of "shred overwrites but file remains":
            # Antivirus (Bitdefender, Defender) and OneDrive re-open files
            # immediately after a write to scan/sync them.  By the time we
            # call os.remove() the file is locked again → WinError 32.
            #
            # Fix: call Restart Manager AGAIN after the overwrite, then retry
            # os.remove() up to 4 times with increasing delays.  This covers:
            #   - AV scanners that hold for <1 s after a write
            #   - OneDrive that wakes up when file contents change
            #   - Word / other editors that have the file open
            # ── Layer 1: Restart Manager unlock + retry (covers AV / OneDrive) ──
            last_err = None
            for attempt, delay in enumerate((0, 0.25, 0.5, 1.0, 2.0)):
                if delay:
                    time.sleep(delay)
                ShredWorker._force_unlock_handles(path)
                try:
                    os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
                    os.remove(path)
                    return True, ("" if attempt == 0 else f"deleted after {attempt} retries")
                except PermissionError as e:
                    last_err = e
                except FileNotFoundError:
                    return True, ""

            # ── Layer 2: FILE_FLAG_DELETE_ON_CLOSE via NtSetInformationFile ──
            # Ask the NT kernel directly to mark the file for deletion.
            # This bypasses the Win32 sharing-mode check that makes
            # CreateFileW fail when a locker doesn't have FILE_SHARE_DELETE.
            try:
                import ctypes, ctypes.wintypes as _wt
                ntdll   = ctypes.windll.ntdll
                kernel32 = ctypes.windll.kernel32

                # Open a handle with DELETE access + full share flags
                DELETE         = 0x00010000
                FILE_SHARE_ALL = 0x00000007   # READ|WRITE|DELETE
                OPEN_EXISTING  = 3
                FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
                INVALID_HANDLE = ctypes.c_void_p(-1).value

                h = kernel32.CreateFileW(
                    path, DELETE, FILE_SHARE_ALL,
                    None, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, None
                )
                if h != INVALID_HANDLE and h != 0:
                    # FileDispositionInformation = 13, DeleteFile = TRUE
                    class _FILE_DISPOSITION(ctypes.Structure):
                        _fields_ = [("DeleteFile", _wt.BOOL)]

                    class _IO_STATUS_BLOCK(ctypes.Structure):
                        _fields_ = [("Status", ctypes.c_ulong),
                                    ("Information", ctypes.POINTER(ctypes.c_ulong))]

                    disp = _FILE_DISPOSITION(True)
                    iosb = _IO_STATUS_BLOCK()
                    ntdll.NtSetInformationFile(
                        h, ctypes.byref(iosb),
                        ctypes.byref(disp), ctypes.sizeof(disp),
                        13   # FileDispositionInformation
                    )
                    kernel32.CloseHandle(h)   # deletion fires on last handle close
                    time.sleep(0.15)
                    if not os.path.exists(path):
                        return True, "deleted via NtSetInformationFile"
            except Exception:
                pass

            # ── Layer 3: MoveFileEx — schedule deletion at next boot ──────
            # Last resort: zero data is already gone; mark the directory entry
            # for removal on next Windows startup (requires no special rights).
            try:
                import ctypes
                MOVEFILE_DELAY_UNTIL_REBOOT = 0x00000004
                if ctypes.windll.kernel32.MoveFileExW(path, None, MOVEFILE_DELAY_UNTIL_REBOOT):
                    return False, "PENDING_REBOOT — data zeroed, file removed on next restart"
            except Exception:
                pass

            return False, f"Could not delete (file locked): {last_err}"

        except PermissionError as exc:
            # Overwrite itself failed (couldn't even open for writing)
            try:
                os.chmod(path, 0o777)
                os.remove(path)
                return True, "overwrite skipped — file force-deleted"
            except Exception as exc2:
                return False, f"Permission denied: {exc2}"
        except Exception as exc:
            return False, str(exc)


# ---------------------------------------------------------------------------
# Shred Log Dialog
# ---------------------------------------------------------------------------
class ShredLogDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Shred Log")
        self.setMinimumSize(640, 400)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 16)
        lay.setSpacing(10)

        self.text = QTextEdit()
        self.text.setReadOnly(True)
        self.text.setFont(QFont("Consolas", 10))
        lay.addWidget(self.text)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        btns.rejected.connect(self.accept)
        lay.addWidget(btns)

    def append(self, line: str):
        self.text.append(line)


# ---------------------------------------------------------------------------
# Main Window
# ---------------------------------------------------------------------------
class SurgeWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("SURGE  —  File Manager & Secure Shredder")
        self.setMinimumSize(1200, 760)
        self.setAcceptDrops(True)

        self._history: list[str]       = []
        self._hist_idx                 = -1
        self._duplicates: dict         = {}
        self._scan_folder              = ""
        self._clipboard_paths: list[str] = []
        self._clipboard_op             = ""     # "copy" | "cut"
        self._scan_worker: ScanWorker | None  = None
        self._shred_worker: ShredWorker | None = None

        self._build_ui()
        self.setStyleSheet(SS)
        self._setup_shortcuts()
        self._navigate_to("", push=False)

    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # Elevated drag-and-drop fix (UIPI bypass)
    # -----------------------------------------------------------------------

    def showEvent(self, event):
        """Called once the window has a real HWND — safe to patch the filter."""
        super().showEvent(event)
        self._allow_elevated_drop()

    def _allow_elevated_drop(self):
        """
        When Surge runs as Administrator, Windows UIPI blocks drag messages
        from non-elevated Explorer. We call ChangeWindowMessageFilterEx on
        the three messages that make drag-and-drop work:
          WM_DROPFILES  (0x0233)  — classic ShellExecute drop
          WM_COPYDATA   (0x004A)  — OLE data transfer
          0x0049        (WM_COPYGLOBALDATA) — used by OLE drag internally
        MSGFLT_ALLOW = 1
        """
        try:
            import ctypes
            import ctypes.wintypes

            MSGFLT_ALLOW      = 1
            WM_DROPFILES      = 0x0233
            WM_COPYDATA       = 0x004A
            WM_COPYGLOBALDATA = 0x0049

            hwnd   = int(self.winId())
            user32 = ctypes.windll.user32

            for msg in (WM_DROPFILES, WM_COPYDATA, WM_COPYGLOBALDATA):
                user32.ChangeWindowMessageFilterEx(
                    ctypes.wintypes.HWND(hwnd),
                    ctypes.c_uint(msg),
                    ctypes.c_uint(MSGFLT_ALLOW),
                    ctypes.c_void_p(None)
                )

            # Tell the shell this HWND physically accepts dropped files
            ctypes.windll.shell32.DragAcceptFiles(
                ctypes.wintypes.HWND(hwnd), True
            )
        except Exception:
            pass   # Non-Windows or non-admin — silently skip

    # -----------------------------------------------------------------------
    # UI construction
    # -----------------------------------------------------------------------

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        vbox = QVBoxLayout(root)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.setSpacing(0)

        vbox.addWidget(self._build_header())

        # Command bar (Explorer-only)
        self.cmd_bar = self._build_command_bar()
        vbox.addWidget(self.cmd_bar)

        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(3)
        self.progress_bar.setVisible(False)
        vbox.addWidget(self.progress_bar)

        self.stack = QStackedWidget()
        self.stack.addWidget(self._build_explorer_page())
        self.stack.addWidget(self._build_duplicates_page())
        vbox.addWidget(self.stack, stretch=1)

        vbox.addWidget(self._build_bottom_bar())

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")

    # ── Header ──────────────────────────────────────────────────────────────

    def _build_header(self) -> QFrame:
        hdr = QFrame()
        hdr.setFixedHeight(52)
        hdr.setStyleSheet(
            f"QFrame{{background:{PANEL_BG};border-bottom:1px solid {BORDER};}}"
        )
        lay = QHBoxLayout(hdr)
        lay.setContentsMargins(14, 0, 14, 0)
        lay.setSpacing(6)

        logo = QLabel("SURGE")
        logo.setStyleSheet(
            f"color:{ACCENT};font-size:17px;font-weight:900;"
            f"letter-spacing:5px;background:transparent;border:none;"
        )
        lay.addWidget(logo)

        div = QFrame()
        div.setFrameShape(QFrame.Shape.VLine)
        div.setFixedSize(1, 24)
        div.setStyleSheet(f"background:{BORDER};")
        lay.addWidget(div)
        lay.addSpacing(2)

        self.back_btn = self._mk_nav_btn("◀")
        self.fwd_btn  = self._mk_nav_btn("▶")
        self.up_btn   = self._mk_nav_btn("▲")
        self.back_btn.clicked.connect(self._go_back)
        self.fwd_btn.clicked.connect(self._go_forward)
        self.up_btn.clicked.connect(self._go_up)
        for b in (self.back_btn, self.fwd_btn, self.up_btn):
            b.setEnabled(False)
            lay.addWidget(b)

        lay.addSpacing(4)

        self.addr_bar = QLineEdit()
        self.addr_bar.setPlaceholderText("Type a path and press Enter...")
        self.addr_bar.setFixedHeight(30)
        self.addr_bar.returnPressed.connect(
            lambda: self._navigate_to(self.addr_bar.text().strip())
        )
        lay.addWidget(self.addr_bar, stretch=1)

        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search current folder...")
        self.search_box.setFixedHeight(30)
        self.search_box.setFixedWidth(200)
        self.search_box.textChanged.connect(self._on_search_changed)
        lay.addWidget(self.search_box)
        lay.addSpacing(8)

        self.btn_explorer = QPushButton("Explorer")
        self.btn_explorer.setFixedHeight(30)
        self.btn_explorer.clicked.connect(lambda: self._set_mode(0))

        self.btn_dupes = QPushButton("Duplicates")
        self.btn_dupes.setFixedHeight(30)
        self.btn_dupes.clicked.connect(lambda: self._set_mode(1))

        lay.addWidget(self.btn_explorer)
        lay.addWidget(self.btn_dupes)
        self._refresh_mode_buttons(0)
        return hdr

    # ── Command bar (Windows Explorer style) ────────────────────────────────

    def _build_command_bar(self) -> QFrame:
        bar = QFrame()
        bar.setFixedHeight(44)
        bar.setStyleSheet(
            f"QFrame{{background:{PANEL_BG};border-bottom:1px solid {BORDER};}}"
        )
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(10, 0, 10, 0)
        lay.setSpacing(2)

        # ── New ──────────────────────────────────────────────
        new_menu = QMenu(self)
        new_menu.addAction("📁  New Folder",    self._new_folder)
        new_menu.addAction("📄  New Text File", self._new_text_file)

        new_btn = QPushButton("＋ New  ▾")
        new_btn.setStyleSheet(BTN_CMD)
        new_btn.setMenu(new_menu)
        lay.addWidget(new_btn)

        lay.addWidget(self._cmd_sep())

        # ── Clipboard ────────────────────────────────────────
        self.cut_btn   = self._cmd_btn("✂  Cut",   self._cut_items)
        self.copy_btn  = self._cmd_btn("⎘  Copy",  self._copy_items)
        self.paste_btn = self._cmd_btn("⏙  Paste", self._paste_items)
        self.cut_btn.setEnabled(False)
        self.copy_btn.setEnabled(False)
        self.paste_btn.setEnabled(False)
        for b in (self.cut_btn, self.copy_btn, self.paste_btn):
            lay.addWidget(b)

        lay.addWidget(self._cmd_sep())

        # ── File ops ─────────────────────────────────────────
        self.rename_btn = self._cmd_btn("✎  Rename",  self._rename_item)
        self.delete_btn = self._cmd_btn("🗑  Delete",  self._delete_to_trash)
        self.shred_cmd_btn = self._cmd_btn("⚡  Shred",  self._confirm_shred)
        self.rename_btn.setEnabled(False)
        self.delete_btn.setEnabled(False)
        self.shred_cmd_btn.setEnabled(False)
        self.shred_cmd_btn.setStyleSheet(
            BTN_CMD.replace(f"color:{TEXT}", f"color:{ACCENT}")
        )
        self.wipe_drive_btn = self._cmd_btn("💾  Wipe Drive...", self._wipe_drive_dialog)
        self.wipe_drive_btn.setStyleSheet(
            BTN_CMD.replace(f"color:{TEXT}", f"color:{ACCENT}")
        )
        for b in (self.rename_btn, self.delete_btn, self.shred_cmd_btn, self.wipe_drive_btn):
            lay.addWidget(b)

        lay.addWidget(self._cmd_sep())

        # ── View controls ────────────────────────────────────
        sort_menu = QMenu(self)
        for i, label in enumerate(["Name", "Size", "Type", "Date Modified"]):
            a = QAction(label, self)
            a.triggered.connect(lambda _, c=i: self._sort_by(c, Qt.SortOrder.AscendingOrder))
            sort_menu.addAction(a)
        sort_menu.addSeparator()
        for i, label in enumerate(["Name ↓", "Size ↓", "Type ↓", "Date ↓"]):
            a = QAction(label, self)
            a.triggered.connect(lambda _, c=i: self._sort_by(c, Qt.SortOrder.DescendingOrder))
            sort_menu.addAction(a)

        sort_btn = QPushButton("⇅  Sort  ▾")
        sort_btn.setStyleSheet(BTN_CMD)
        sort_btn.setMenu(sort_menu)

        view_menu = QMenu(self)
        view_menu.addAction("Details",       lambda: self._set_view_mode("details"))
        view_menu.addAction("Compact List",  lambda: self._set_view_mode("compact"))
        view_btn = QPushButton("⊞  View  ▾")
        view_btn.setStyleSheet(BTN_CMD)
        view_btn.setMenu(view_menu)

        filter_menu = QMenu(self)
        filter_menu.addAction("All Files",    lambda: self._apply_filter(""))
        filter_menu.addSeparator()
        for ext, label in [("*.jpg *.jpeg *.png *.gif *.bmp *.webp", "Images"),
                           ("*.mp4 *.mkv *.avi *.mov *.wmv",         "Videos"),
                           ("*.mp3 *.wav *.flac *.aac *.ogg",        "Audio"),
                           ("*.pdf *.doc *.docx *.xls *.xlsx *.ppt", "Documents"),
                           ("*.zip *.rar *.7z *.tar *.gz",           "Archives"),
                           ("*.exe *.msi *.bat *.cmd *.ps1",         "Executables")]:
            a = QAction(label, self)
            a.triggered.connect(lambda _, e=ext: self._apply_filter(e))
            filter_menu.addAction(a)
        filter_btn = QPushButton("⊟  Filter  ▾")
        filter_btn.setStyleSheet(BTN_CMD)
        filter_btn.setMenu(filter_menu)

        refresh_btn = self._cmd_btn("↺  Refresh", self._refresh)

        for w in (sort_btn, view_btn, filter_btn,
                  self._cmd_sep(), refresh_btn):
            lay.addWidget(w)

        lay.addStretch()

        # Properties button (far right)
        prop_btn = self._cmd_btn("⋯  Properties", self._show_properties)
        lay.addWidget(prop_btn)
        self.prop_btn = prop_btn
        self.prop_btn.setEnabled(False)

        return bar

    # ── Explorer page ────────────────────────────────────────────────────────

    def _build_explorer_page(self) -> QWidget:
        page = QWidget()
        lay = QHBoxLayout(page)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setHandleWidth(1)
        splitter.setStyleSheet(f"QSplitter::handle{{background:{BORDER};}}")

        # Left: folder tree
        self.nav_model = QFileSystemModel()
        self.nav_model.setRootPath("")
        self.nav_model.setFilter(
            QDir.Filter.Drives | QDir.Filter.AllDirs | QDir.Filter.NoDotAndDotDot
        )
        self.folder_tree = QTreeView()
        self.folder_tree.setModel(self.nav_model)
        self.folder_tree.setRootIndex(self.nav_model.index(""))
        for col in (1, 2, 3):
            self.folder_tree.hideColumn(col)
        self.folder_tree.header().hide()
        self.folder_tree.setIndentation(14)
        self.folder_tree.setMinimumWidth(190)
        self.folder_tree.setMaximumWidth(300)
        self.folder_tree.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.folder_tree.setStyleSheet(
            f"QTreeView{{border:none;border-right:1px solid {BORDER};border-radius:0;}}"
        )
        self.folder_tree.clicked.connect(self._on_nav_tree_clicked)
        self.folder_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.folder_tree.customContextMenuRequested.connect(self._show_nav_ctx_menu)

        # Right: file view
        self.file_model = QFileSystemModel()
        self.file_model.setRootPath("")
        self.file_model.setFilter(
            QDir.Filter.AllEntries | QDir.Filter.NoDotAndDotDot | QDir.Filter.System
        )
        self.search_proxy = QSortFilterProxyModel()
        self.search_proxy.setSourceModel(self.file_model)
        self.search_proxy.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.search_proxy.setFilterKeyColumn(0)

        self.file_view = QTreeView()
        self.file_view.setModel(self.search_proxy)
        self.file_view.setRootIsDecorated(False)
        self.file_view.setItemsExpandable(False)
        self.file_view.setAlternatingRowColors(True)
        self.file_view.setSortingEnabled(True)
        self.file_view.sortByColumn(0, Qt.SortOrder.AscendingOrder)
        self.file_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.file_view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        hdr = self.file_view.header()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for c in (1, 2, 3):
            hdr.setSectionResizeMode(c, QHeaderView.ResizeMode.ResizeToContents)
        self.file_view.setStyleSheet(
            f"QTreeView{{border:none;border-radius:0;}}"
        )
        self.file_view.doubleClicked.connect(self._on_file_double_clicked)
        self.file_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.file_view.customContextMenuRequested.connect(self._show_file_ctx_menu)
        self.file_view.selectionModel().selectionChanged.connect(
            lambda: self._update_command_state()
        )

        splitter.addWidget(self.folder_tree)
        splitter.addWidget(self.file_view)
        splitter.setSizes([220, 980])
        lay.addWidget(splitter)
        return page

    # ── Duplicates page ──────────────────────────────────────────────────────

    def _build_duplicates_page(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setContentsMargins(20, 18, 20, 14)
        lay.setSpacing(14)

        self.drop_zone = QFrame()
        self.drop_zone.setFixedHeight(110)
        self.drop_zone.setStyleSheet(
            f"QFrame{{border:2px dashed {BORDER};border-radius:10px;background:{PANEL_BG};}}"
        )
        dz = QHBoxLayout(self.drop_zone)
        dz.setAlignment(Qt.AlignmentFlag.AlignCenter)
        dz.setSpacing(16)

        arrow = QLabel("⇩")
        arrow.setStyleSheet(
            f"font-size:26px;color:{SUB};background:transparent;border:none;"
        )
        arrow.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)

        self.dz_label = QLabel("Drop a folder here  —  or")
        self.dz_label.setStyleSheet(
            f"color:{SUB};background:transparent;border:none;font-size:13px;"
        )
        self.dz_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)

        browse_btn = QPushButton("Browse...")
        browse_btn.setFixedHeight(32)
        browse_btn.clicked.connect(self._browse_scan_folder)

        self.scan_btn = QPushButton("SCAN")
        self.scan_btn.setFixedHeight(32)
        self.scan_btn.setStyleSheet(BTN_OUTLINE)
        self.scan_btn.setEnabled(False)
        self.scan_btn.clicked.connect(self._start_scan)

        dz.addWidget(arrow)
        dz.addWidget(self.dz_label)
        dz.addWidget(browse_btn)
        dz.addWidget(self.scan_btn)
        lay.addWidget(self.drop_zone)

        sr = QHBoxLayout()
        sr.setSpacing(10)
        self.stat_groups = self._stat_card("0",   "duplicate groups")
        self.stat_files  = self._stat_card("0",   "files")
        self.stat_wasted = self._stat_card("0 B", "wasted space")
        for s in (self.stat_groups, self.stat_files, self.stat_wasted):
            sr.addWidget(s)
        sr.addStretch()
        lay.addLayout(sr)

        self.dup_model = QStandardItemModel()
        self.dup_model.setHorizontalHeaderLabels(["File / Path", "Size", "Folder"])

        # FIX: connect itemChanged so every checkbox toggle refreshes the
        # SHRED button and selection counter without needing to click anything.
        self.dup_model.itemChanged.connect(self._on_dup_item_changed)

        self.dup_tree = QTreeView()
        self.dup_tree.setModel(self.dup_model)
        self.dup_tree.setAlternatingRowColors(True)
        # FIX: NoSelection — we use checkboxes, not row highlight, to pick files.
        # Row-highlight selection was the crash path: selecting items via
        # selectionModel() on a tree with None children caused segfaults.
        self.dup_tree.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self.dup_tree.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        dh = self.dup_tree.header()
        dh.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        dh.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        dh.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.dup_tree.setIndentation(18)
        self.dup_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.dup_tree.customContextMenuRequested.connect(self._show_dup_ctx_menu)
        lay.addWidget(self.dup_tree, stretch=1)
        return page

    # ── Bottom bar ───────────────────────────────────────────────────────────

    def _build_bottom_bar(self) -> QFrame:
        bar = QFrame()
        bar.setFixedHeight(50)
        bar.setStyleSheet(
            f"QFrame{{background:{PANEL_BG};border-top:1px solid {BORDER};}}"
        )
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(16, 0, 16, 0)
        lay.setSpacing(8)

        # ── Left: selection label ─────────────────────────────────────────
        self.lbl_sel = QLabel("Nothing selected")
        self.lbl_sel.setStyleSheet(
            f"color:{SUB};background:transparent;border:none;"
        )
        lay.addWidget(self.lbl_sel)

        # ── Centre: live activity display (visible while op is running) ───
        # Shows  ⏱ 00:12  |  ⚡ filename.txt
        self.lbl_activity = QLabel("")
        self.lbl_activity.setStyleSheet(
            f"color:{ACCENT};background:transparent;border:none;"
            f"font-family:'Consolas';font-size:11px;"
        )
        self.lbl_activity.setVisible(False)
        lay.addWidget(self.lbl_activity)

        lay.addStretch()

        # ── Duplicate selection buttons (visible in Duplicates mode only) ─
        self.desel_all_btn = QPushButton("Deselect All")
        self.desel_all_btn.setStyleSheet(BTN_CMD)
        self.desel_all_btn.setFixedHeight(30)
        self.desel_all_btn.setEnabled(False)
        self.desel_all_btn.setVisible(False)
        self.desel_all_btn.setToolTip("Uncheck every file.")
        self.desel_all_btn.clicked.connect(self._deselect_all_duplicates)

        self.sel_dupes_btn = QPushButton("Select Duplicates")
        self.sel_dupes_btn.setStyleSheet(BTN_OUTLINE)
        self.sel_dupes_btn.setFixedHeight(30)
        self.sel_dupes_btn.setEnabled(False)
        self.sel_dupes_btn.setVisible(False)
        self.sel_dupes_btn.setToolTip(
            "Check only the duplicate copies — the first file in each group "
            "stays unchecked as the 'original'."
        )
        self.sel_dupes_btn.clicked.connect(self._select_duplicates_only)

        self.sel_all_btn = QPushButton("Select All")
        self.sel_all_btn.setStyleSheet(BTN_OUTLINE)
        self.sel_all_btn.setFixedHeight(30)
        self.sel_all_btn.setEnabled(False)
        self.sel_all_btn.setVisible(False)
        self.sel_all_btn.setToolTip("Check every file in every duplicate group.")
        self.sel_all_btn.clicked.connect(self._select_all_duplicates)

        for w in (self.desel_all_btn, self.sel_dupes_btn, self.sel_all_btn):
            lay.addWidget(w)

        # ── Separator ─────────────────────────────────────────────────────
        self._bot_sep = self._cmd_sep()
        self._bot_sep.setVisible(False)
        lay.addWidget(self._bot_sep)

        # ── Pause / Stop (visible only while an operation is running) ─────
        self.pause_btn = QPushButton("⏸  Pause")
        self.pause_btn.setFixedHeight(30)
        self.pause_btn.setStyleSheet(BTN_CMD)
        self.pause_btn.setVisible(False)
        self.pause_btn.setToolTip("Pause the current scan or shred.")
        self.pause_btn.clicked.connect(self._toggle_pause)

        self.stop_btn = QPushButton("⏹  Stop")
        self.stop_btn.setFixedHeight(30)
        self.stop_btn.setStyleSheet(
            BTN_CMD.replace(f"color:{TEXT}", "color:#ff6b6b")
        )
        self.stop_btn.setVisible(False)
        self.stop_btn.setToolTip("Stop the current scan or shred immediately.")
        self.stop_btn.clicked.connect(self._stop_operation)

        lay.addWidget(self.pause_btn)
        lay.addWidget(self.stop_btn)

        # ── Separator ─────────────────────────────────────────────────────
        self._bot_sep2 = self._cmd_sep()
        self._bot_sep2.setVisible(False)
        lay.addWidget(self._bot_sep2)

        # ── SHRED button ──────────────────────────────────────────────────
        self.shred_btn = QPushButton("  SHRED")
        self.shred_btn.setStyleSheet(BTN_ACCENT)
        self.shred_btn.setFixedHeight(34)
        self.shred_btn.setEnabled(False)
        self.shred_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.shred_btn.clicked.connect(self._confirm_shred)
        lay.addWidget(self.shred_btn)

        # ── Elapsed-time QTimer (updates lbl_activity every second) ───────
        self._op_timer      = QTimer(self)
        self._op_timer_secs = 0
        self._op_cur_file   = ""
        self._op_timer.setInterval(1000)
        self._op_timer.timeout.connect(self._tick_activity)

        return bar

    # ── Helper widget builders ───────────────────────────────────────────────

    def _mk_nav_btn(self, text: str) -> QPushButton:
        b = QPushButton(text)
        b.setFixedSize(28, 28)
        b.setStyleSheet(BTN_NAV)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        return b

    def _cmd_btn(self, label: str, slot) -> QPushButton:
        b = QPushButton(label)
        b.setStyleSheet(BTN_CMD)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.clicked.connect(slot)
        return b

    def _cmd_sep(self) -> QFrame:
        s = QFrame()
        s.setFrameShape(QFrame.Shape.VLine)
        s.setFixedSize(1, 22)
        s.setStyleSheet(f"background:{BORDER};")
        return s

    def _stat_card(self, val: str, cap: str) -> QFrame:
        w = QFrame()
        w.setFixedWidth(148)
        w.setStyleSheet(
            f"QFrame{{background:{CARD_BG};border:1px solid {BORDER};border-radius:8px;}}"
        )
        lay = QVBoxLayout(w)
        lay.setContentsMargins(12, 8, 12, 8)
        lay.setSpacing(2)
        v = QLabel(val)
        v.setStyleSheet(
            f"color:{ACCENT};font-size:20px;font-weight:bold;border:none;background:transparent;"
        )
        c = QLabel(cap)
        c.setStyleSheet(
            f"color:{SUB};font-size:10px;border:none;background:transparent;"
        )
        lay.addWidget(v)
        lay.addWidget(c)
        w._val = v
        return w

    def _refresh_mode_buttons(self, active: int):
        self.btn_explorer.setStyleSheet(BTN_MODE_ON if active == 0 else BTN_MODE_OFF)
        self.btn_dupes.setStyleSheet(BTN_MODE_ON if active == 1 else BTN_MODE_OFF)

    # ── Keyboard shortcuts ───────────────────────────────────────────────────

    def _setup_shortcuts(self):
        QShortcut(QKeySequence.StandardKey.Copy,   self).activated.connect(self._copy_items)
        QShortcut(QKeySequence.StandardKey.Cut,    self).activated.connect(self._cut_items)
        QShortcut(QKeySequence.StandardKey.Paste,  self).activated.connect(self._paste_items)
        QShortcut(QKeySequence("F2"),              self).activated.connect(self._rename_item)
        QShortcut(QKeySequence("Delete"),          self).activated.connect(self._delete_to_trash)
        QShortcut(QKeySequence("F5"),              self).activated.connect(self._refresh)
        QShortcut(QKeySequence("Ctrl+Shift+N"),    self).activated.connect(self._new_folder)
        QShortcut(QKeySequence("Alt+Left"),        self).activated.connect(self._go_back)
        QShortcut(QKeySequence("Alt+Right"),       self).activated.connect(self._go_forward)
        QShortcut(QKeySequence("Alt+Up"),          self).activated.connect(self._go_up)
        QShortcut(QKeySequence("Ctrl+L"),          self).activated.connect(self.addr_bar.setFocus)

    # -----------------------------------------------------------------------
    # Mode switching
    # -----------------------------------------------------------------------

    def _set_mode(self, idx: int):
        self.stack.setCurrentIndex(idx)
        self._refresh_mode_buttons(idx)
        is_exp = (idx == 0)
        self.cmd_bar.setVisible(is_exp)
        for w in (self.back_btn, self.fwd_btn, self.up_btn,
                  self.addr_bar, self.search_box):
            w.setVisible(is_exp)
        # Selection buttons only shown in Duplicates mode
        for w in (self.sel_dupes_btn, self.sel_all_btn, self.desel_all_btn):
            w.setVisible(not is_exp)
        self._update_command_state()

    # -----------------------------------------------------------------------
    # Pause / Stop / Activity timer
    # -----------------------------------------------------------------------

    def _start_activity(self, label: str = ""):
        """Show the Pause/Stop buttons and start the elapsed timer."""
        self._op_timer_secs = 0
        self._op_cur_file   = label
        self.lbl_activity.setText(f"⏱ 00:00  |  {label}")
        self.lbl_activity.setVisible(True)
        self._bot_sep.setVisible(True)
        self._bot_sep2.setVisible(True)
        self.pause_btn.setText("⏸  Pause")
        self.pause_btn.setVisible(True)
        self.stop_btn.setVisible(True)
        self._op_timer.start()

    def _stop_activity(self):
        """Hide Pause/Stop buttons and stop the elapsed timer."""
        self._op_timer.stop()
        self.pause_btn.setVisible(False)
        self.stop_btn.setVisible(False)
        self._bot_sep.setVisible(False)
        self._bot_sep2.setVisible(False)
        self.lbl_activity.setVisible(False)

    def _tick_activity(self):
        """Called every second to update the elapsed clock."""
        self._op_timer_secs += 1
        m, s = divmod(self._op_timer_secs, 60)
        elapsed = f"{m:02d}:{s:02d}"
        name = self._op_cur_file
        if name:
            self.lbl_activity.setText(f"⏱ {elapsed}  |  ⚡ {name}")
        else:
            self.lbl_activity.setText(f"⏱ {elapsed}")

    def _toggle_pause(self):
        """Pause or resume whichever worker is currently running."""
        scan_running  = self._scan_worker  and self._scan_worker.isRunning()
        shred_running = self._shred_worker and self._shred_worker.isRunning()

        if not scan_running and not shred_running:
            return

        # Detect current paused state
        paused = False
        if scan_running  and self._scan_worker._pause.is_set():
            paused = True
        if shred_running and self._shred_worker._pause_event.is_set():
            paused = True

        if paused:
            # Resume
            if scan_running:  self._scan_worker.resume()
            if shred_running: self._shred_worker.resume()
            self.pause_btn.setText("⏸  Pause")
            self._op_timer.start()
            self.status_bar.showMessage("Resumed.")
        else:
            # Pause
            if scan_running:  self._scan_worker.pause()
            if shred_running: self._shred_worker.pause()
            self.pause_btn.setText("▶  Resume")
            self._op_timer.stop()
            m, s = divmod(self._op_timer_secs, 60)
            self.lbl_activity.setText(f"⏸ {m:02d}:{s:02d}  |  PAUSED")
            self.status_bar.showMessage("Paused — click Resume to continue.")

    def _stop_operation(self):
        """Stop whichever worker is running."""
        if self._scan_worker and self._scan_worker.isRunning():
            self._scan_worker.abort()
            self.status_bar.showMessage("Scan stopped.")
        if self._shred_worker and self._shred_worker.isRunning():
            self._shred_worker.stop()
            self.status_bar.showMessage("Shred stopped.")

    # -----------------------------------------------------------------------
    # Drag & Drop
    # -----------------------------------------------------------------------

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event):
        urls = event.mimeData().urls()
        if not urls:
            return

        # Normalise paths: toLocalFile() on Windows may return forward-slash
        # paths, AND may strip .lnk from shortcut files. Resolve both.
        raw = [u.toLocalFile() for u in urls if u.toLocalFile()]
        paths = [_resolve_drop_path(p) for p in raw if p]
        paths = [p for p in paths if p]   # drop any still-None
        if not paths:
            return

        event.acceptProposedAction()
        self._reset_drop_zone_style()

        # Duplicates tab: a dropped folder goes straight to the scanner
        if self.stack.currentIndex() == 1:
            first = paths[0]
            if os.path.isdir(first):
                self._set_scan_folder(first)
                return

        # Defer so the Qt drag machinery fully finishes before we open a dialog
        QTimer.singleShot(100, lambda: self._confirm_shred_targets(paths))

    def dragLeaveEvent(self, event):
        self._reset_drop_zone_style()

    def _reset_drop_zone_style(self):
        self.drop_zone.setStyleSheet(
            f"QFrame{{border:2px dashed {BORDER};border-radius:10px;background:{PANEL_BG};}}"
        )

    # -----------------------------------------------------------------------
    # Navigation
    # -----------------------------------------------------------------------

    def _navigate_to(self, path: str, push: bool = True):
        if path and not os.path.exists(path):
            self.status_bar.showMessage(f"Path not found: {path}")
            return
        path = str(Path(path)) if path else ""

        if push:
            self._history = self._history[:self._hist_idx + 1]
            self._history.append(path)
            self._hist_idx = len(self._history) - 1

        if path:
            src_idx   = self.file_model.setRootPath(path)
            proxy_idx = self.search_proxy.mapFromSource(src_idx)
        else:
            self.file_model.setRootPath("")
            proxy_idx = QModelIndex()
        self.file_view.setRootIndex(proxy_idx)

        if path:
            nav_idx = self.nav_model.index(path)
            if nav_idx.isValid():
                self.folder_tree.setCurrentIndex(nav_idx)
                self.folder_tree.scrollTo(nav_idx)

        self.addr_bar.setText(path)
        self.back_btn.setEnabled(self._hist_idx > 0)
        self.fwd_btn.setEnabled(self._hist_idx < len(self._history) - 1)
        parent = Path(path).parent if path else None
        self.up_btn.setEnabled(bool(path) and parent and str(parent) != path)
        self.status_bar.showMessage(path or "This PC")
        self.search_box.clear()
        self._update_command_state()

    def _go_back(self):
        if self._hist_idx > 0:
            self._hist_idx -= 1
            self._navigate_to(self._history[self._hist_idx], push=False)

    def _go_forward(self):
        if self._hist_idx < len(self._history) - 1:
            self._hist_idx += 1
            self._navigate_to(self._history[self._hist_idx], push=False)

    def _go_up(self):
        path = self.addr_bar.text().strip()
        if path:
            parent = str(Path(path).parent)
            if parent != path:
                self._navigate_to(parent)

    def _on_nav_tree_clicked(self, index):
        self._navigate_to(self.nav_model.filePath(index))

    def _on_file_double_clicked(self, proxy_idx):
        src  = self.search_proxy.mapToSource(proxy_idx)
        path = self.file_model.filePath(src)
        if self.file_model.isDir(src):
            self._navigate_to(path)
        else:
            try:
                os.startfile(path)
            except Exception as exc:
                self.status_bar.showMessage(f"Cannot open: {exc}")

    def _on_search_changed(self, text: str):
        self.search_proxy.setFilterFixedString(text)

    def _refresh(self):
        path = self.addr_bar.text().strip()
        self.file_model.setRootPath("")      # force refresh
        self._navigate_to(path, push=False)

    # -----------------------------------------------------------------------
    # Selection helpers
    # -----------------------------------------------------------------------

    def _get_selected_entries(self) -> list[str]:
        """All selected paths (files AND directories) in the file view."""
        paths, seen = [], set()
        for idx in self.file_view.selectedIndexes():
            if idx.column() != 0:
                continue
            src = self.search_proxy.mapToSource(idx)
            p   = self.file_model.filePath(src)
            if p not in seen:
                seen.add(p)
                paths.append(p)
        return paths

    def _get_selected_files_only(self) -> list[str]:
        return [p for p in self._get_selected_entries() if os.path.isfile(p)]

    def _get_single_entry(self) -> str | None:
        entries = self._get_selected_entries()
        return entries[0] if len(entries) == 1 else None

    def _get_dup_selected_paths(self) -> list[str]:
        """
        FIX: collect checked file paths from checkboxes instead of from the
        selection model.  The old approach (selectedIndexes()) was unreliable
        because:
          - The tree was in NoSelection mode after our fix, so nothing was
            ever "selected" in the Qt sense.
          - Even with ExtendedSelection, itemFromIndex() could return None
            for group-header rows, causing an AttributeError / crash.
        """
        paths: list[str] = []
        seen:  set[str]  = set()
        for row in range(self.dup_model.rowCount()):
            grp = self.dup_model.item(row, 0)
            if grp is None:
                continue
            for cr in range(grp.rowCount()):
                child = grp.child(cr, 0)
                if child is None:
                    continue
                if child.checkState() != Qt.CheckState.Checked:
                    continue
                p = child.data(Qt.ItemDataRole.UserRole)
                if p and p not in seen:
                    seen.add(p)
                    paths.append(p)
        return paths

    # -----------------------------------------------------------------------
    # Command bar state update
    # -----------------------------------------------------------------------

    def _update_command_state(self):
        in_explorer = (self.stack.currentIndex() == 0)

        if in_explorer:
            entries   = self._get_selected_entries()
            files     = [p for p in entries if os.path.isfile(p)]
            has_sel   = bool(entries)
            has_files = bool(files)
            one_sel   = len(entries) == 1

            self.cut_btn.setEnabled(has_sel)
            self.copy_btn.setEnabled(has_sel)
            self.paste_btn.setEnabled(bool(self._clipboard_paths))
            self.rename_btn.setEnabled(one_sel)
            self.delete_btn.setEnabled(has_sel)
            self.shred_cmd_btn.setEnabled(has_sel)    # files + folders + drives
            self.prop_btn.setEnabled(one_sel)

            self.shred_btn.setEnabled(has_sel)
            n = len(entries)
            self.lbl_sel.setText(f"{n} item(s) selected" if n else "Nothing selected")
        else:
            dupes = self._get_dup_selected_paths()
            n     = len(dupes)
            self.shred_btn.setEnabled(n > 0)
            self.lbl_sel.setText(f"{n} file(s) selected" if n else "Nothing selected")

    # -----------------------------------------------------------------------
    # Command bar operations
    # -----------------------------------------------------------------------

    # ── New ──────────────────────────────────────────────────────────────────

    def _new_folder(self):
        cwd = self.addr_bar.text().strip()
        if not cwd or not os.path.isdir(cwd):
            self.status_bar.showMessage("Navigate to a folder first.")
            return
        name, ok = QInputDialog.getText(self, "New Folder", "Folder name:", text="New Folder")
        if ok and name.strip():
            dest = os.path.join(cwd, name.strip())
            try:
                os.makedirs(dest, exist_ok=True)
                self.status_bar.showMessage(f"Created: {dest}")
            except OSError as exc:
                QMessageBox.warning(self, "Error", str(exc))

    def _new_text_file(self):
        cwd = self.addr_bar.text().strip()
        if not cwd or not os.path.isdir(cwd):
            self.status_bar.showMessage("Navigate to a folder first.")
            return
        name, ok = QInputDialog.getText(self, "New Text File", "File name:", text="New File.txt")
        if ok and name.strip():
            dest = os.path.join(cwd, name.strip())
            try:
                with open(dest, "w") as f:
                    f.write("")
                self.status_bar.showMessage(f"Created: {dest}")
            except OSError as exc:
                QMessageBox.warning(self, "Error", str(exc))

    # ── Cut / Copy / Paste ───────────────────────────────────────────────────

    def _cut_items(self):
        entries = self._get_selected_entries()
        if entries:
            self._clipboard_paths = entries
            self._clipboard_op    = "cut"
            self.paste_btn.setEnabled(True)
            self.status_bar.showMessage(f"Cut {len(entries)} item(s)  (Ctrl+V to paste)")

    def _copy_items(self):
        entries = self._get_selected_entries()
        if entries:
            self._clipboard_paths = entries
            self._clipboard_op    = "copy"
            self.paste_btn.setEnabled(True)
            # Also copy paths to system clipboard
            QApplication.clipboard().setText("\n".join(entries))
            self.status_bar.showMessage(f"Copied {len(entries)} item(s)  (Ctrl+V to paste)")

    def _paste_items(self):
        if not self._clipboard_paths:
            return
        cwd = self.addr_bar.text().strip()
        if not cwd or not os.path.isdir(cwd):
            self.status_bar.showMessage("Navigate to a destination folder first.")
            return
        errors = []
        for src in self._clipboard_paths:
            name = os.path.basename(src)
            dst  = os.path.join(cwd, name)
            # Avoid overwriting same path
            if os.path.abspath(src) == os.path.abspath(dst):
                base, ext = os.path.splitext(name)
                dst = os.path.join(cwd, f"{base} — Copy{ext}")
            try:
                if self._clipboard_op == "cut":
                    shutil.move(src, dst)
                else:
                    if os.path.isdir(src):
                        shutil.copytree(src, dst)
                    else:
                        shutil.copy2(src, dst)
            except Exception as exc:
                errors.append(f"{name}: {exc}")

        if self._clipboard_op == "cut":
            self._clipboard_paths = []
            self._clipboard_op    = ""
            self.paste_btn.setEnabled(False)

        if errors:
            QMessageBox.warning(self, "Paste errors",
                                "\n".join(errors[:10]))
        else:
            self.status_bar.showMessage("Paste complete.")

    # ── Rename ───────────────────────────────────────────────────────────────

    def _rename_item(self):
        entry = self._get_single_entry()
        if not entry:
            return
        old   = os.path.basename(entry)
        name, ok = QInputDialog.getText(self, "Rename", "New name:", text=old)
        if ok and name.strip() and name.strip() != old:
            new_path = os.path.join(os.path.dirname(entry), name.strip())
            try:
                os.rename(entry, new_path)
                self.status_bar.showMessage(f"Renamed to {name.strip()}")
            except OSError as exc:
                QMessageBox.warning(self, "Rename error", str(exc))

    # ── Delete to Recycle Bin ────────────────────────────────────────────────

    def _delete_to_trash(self):
        entries = self._get_selected_entries()
        if not entries:
            return
        reply = QMessageBox.question(
            self, "Delete",
            f"Move {len(entries)} item(s) to the Recycle Bin?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
        )
        if reply == QMessageBox.StandardButton.Yes:
            errors = []
            for p in entries:
                try:
                    send2trash.send2trash(p)
                except Exception as exc:
                    errors.append(f"{os.path.basename(p)}: {exc}")
            if errors:
                QMessageBox.warning(self, "Delete errors", "\n".join(errors[:10]))
            else:
                self.status_bar.showMessage(f"Moved {len(entries)} item(s) to Recycle Bin.")

    # ── Sort / View / Filter ─────────────────────────────────────────────────

    def _sort_by(self, column: int, order: Qt.SortOrder):
        self.file_view.sortByColumn(column, order)

    def _set_view_mode(self, mode: str):
        if mode == "compact":
            self.file_view.setIndentation(0)
            self.file_view.header().setSectionResizeMode(
                0, QHeaderView.ResizeMode.ResizeToContents
            )
        else:
            self.file_view.setIndentation(0)
            self.file_view.header().setSectionResizeMode(
                0, QHeaderView.ResizeMode.Stretch
            )

    def _apply_filter(self, pattern: str):
        if pattern:
            self.file_model.setNameFilters(pattern.split())
            self.file_model.setNameFilterDisables(False)
        else:
            self.file_model.setNameFilters([])

    # ── Properties ───────────────────────────────────────────────────────────

    def _show_properties(self):
        entry = self._get_single_entry()
        if not entry:
            return
        try:
            stat = os.stat(entry)
            import datetime
            mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
            ctime = datetime.datetime.fromtimestamp(stat.st_ctime)
            is_dir = os.path.isdir(entry)
            if is_dir:
                total_size = sum(
                    os.path.getsize(os.path.join(r, f))
                    for r, _, files in os.walk(entry) for f in files
                )
                size_str = _human_size(total_size)
            else:
                size_str = _human_size(stat.st_size)

            info = (
                f"Name:      {os.path.basename(entry)}\n"
                f"Type:      {'Folder' if is_dir else 'File'}\n"
                f"Path:      {entry}\n"
                f"Size:      {size_str}\n"
                f"Modified:  {mtime:%Y-%m-%d  %H:%M:%S}\n"
                f"Created:   {ctime:%Y-%m-%d  %H:%M:%S}\n"
            )
        except OSError as exc:
            info = str(exc)

        QMessageBox.information(self, f"Properties — {os.path.basename(entry)}", info)

    # -----------------------------------------------------------------------
    # Context menus
    # -----------------------------------------------------------------------
    # Nav-tree context menu (left panel — drives & folders)
    # -----------------------------------------------------------------------

    def _show_nav_ctx_menu(self, pos):
        idx = self.folder_tree.indexAt(pos)
        if not idx.isValid():
            return
        path  = self.nav_model.filePath(idx)
        p     = Path(path)
        is_drive = (p == p.parent)

        menu = QMenu(self)
        menu.addAction("Open", lambda: self._navigate_to(path))
        menu.addAction("Scan for Duplicates", lambda: self._scan_from_explorer(path))
        menu.addSeparator()
        menu.addAction("Delete to Recycle Bin", lambda: self._trash_path(path))
        menu.addSeparator()

        shred_label = "Wipe Drive..." if is_drive else "Shred Folder..."
        shred_a = QAction(shred_label, self)
        # QAction has no setForeground — removed to prevent crash
        shred_a.triggered.connect(lambda: self._confirm_shred_targets([path]))
        menu.addAction(shred_a)

        menu.exec(self.folder_tree.viewport().mapToGlobal(pos))

    # ── Wipe Drive dialog (dedicated drive picker) ───────────────────────────

    def _wipe_drive_dialog(self):
        """Show all available drives and let the user pick one to wipe."""
        drives = [d.mountpoint for d in self._list_drives()]

        # Fallback: ask the OS for drive letters on Windows
        if not drives:
            import string
            drives = [f"{l}:\\" for l in string.ascii_uppercase
                      if os.path.exists(f"{l}:\\")]

        if not drives:
            QMessageBox.information(self, "No Drives", "No drives detected.")
            return

        drive, ok = QInputDialog.getItem(
            self, "Wipe Drive",
            "Select a drive to completely wipe (DoD 5220.22-M):",
            drives, 0, False
        )
        if ok and drive:
            self._confirm_shred_targets([drive])

    @staticmethod
    def _list_drives():
        """Return a list of objects with .mountpoint for each drive."""
        try:
            import psutil
            return psutil.disk_partitions(all=False)
        except ImportError:
            pass
        return []

    def _trash_path(self, path: str):
        """Send a single path (file or folder) to the Recycle Bin."""
        if not os.path.exists(path):
            return
        reply = QMessageBox.question(
            self, "Delete",
            f"Move  '{os.path.basename(path)}'  to the Recycle Bin?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                send2trash.send2trash(path)
                self.status_bar.showMessage(f"Moved to Recycle Bin: {path}")
            except Exception as exc:
                QMessageBox.warning(self, "Delete error", str(exc))

    # -----------------------------------------------------------------------

    def _show_file_ctx_menu(self, pos):
        indexes = [i for i in self.file_view.selectedIndexes() if i.column() == 0]
        if not indexes:
            return

        entries: list[tuple[str, bool]] = []
        seen: set[str] = set()
        for idx in indexes:
            src  = self.search_proxy.mapToSource(idx)
            path = self.file_model.filePath(src)
            if path not in seen:
                seen.add(path)
                entries.append((path, self.file_model.isDir(src)))

        menu = QMenu(self)

        if len(entries) == 1:
            path, is_dir = entries[0]
            if is_dir:
                menu.addAction("Open",                lambda: self._navigate_to(path))
                menu.addAction("Scan for Duplicates", lambda p=path: self._scan_from_explorer(p))
            else:
                menu.addAction("Open", lambda p=path: os.startfile(p))
            menu.addSeparator()
            menu.addAction("Cut",    self._cut_items)
            menu.addAction("Copy",   self._copy_items)
            menu.addSeparator()
            menu.addAction("Rename", self._rename_item)
            menu.addAction("Copy Path", lambda p=path: QApplication.clipboard().setText(p))
            menu.addSeparator()

        all_paths = [p for p, _ in entries]
        menu.addAction(f"Delete  ({len(all_paths)} item(s))", self._delete_to_trash)

        shred_a = QAction(f"Shred  ({len(all_paths)} item(s))...", self)
        # QAction has no setForeground — removed to prevent crash
        shred_a.triggered.connect(lambda: self._confirm_shred_targets(all_paths))
        menu.addAction(shred_a)

        if not menu.isEmpty():
            menu.exec(self.file_view.viewport().mapToGlobal(pos))

    def _show_dup_ctx_menu(self, pos):
        # FIX: build the menu from the clicked item (not from selection model,
        # which no longer drives anything).  Also added Check/Uncheck actions
        # as a convenience so right-clicking a file lets you toggle it.
        idx = self.dup_tree.indexAt(pos)
        if not idx.isValid():
            return
        item = self.dup_model.itemFromIndex(idx)
        if item is None:
            return
        clicked_path = item.data(Qt.ItemDataRole.UserRole) if idx.column() == 0 else None
        # Only file rows have a file path (group headers have a hash string)
        is_file_row = (clicked_path and os.path.isfile(clicked_path))

        menu = QMenu(self)
        if is_file_row:
            menu.addAction("Show in Explorer",
                           lambda p=clicked_path: self._reveal_in_explorer(p))
            menu.addAction("Copy Path",
                           lambda p=clicked_path: QApplication.clipboard().setText(p))
            menu.addSeparator()
            if item.checkState() == Qt.CheckState.Checked:
                menu.addAction("Uncheck", lambda i=item: i.setCheckState(Qt.CheckState.Unchecked))
            else:
                menu.addAction("Check",   lambda i=item: i.setCheckState(Qt.CheckState.Checked))
            menu.addSeparator()

        checked = self._get_dup_selected_paths()
        if checked:
            sa = QAction(f"Shred {len(checked)} checked file(s)...", self)
            sa.triggered.connect(lambda: self._confirm_shred_targets(checked))
            menu.addAction(sa)

        if not menu.isEmpty():
            menu.exec(self.dup_tree.viewport().mapToGlobal(pos))

    # -----------------------------------------------------------------------
    # Duplicate scanner
    # -----------------------------------------------------------------------

    def _browse_scan_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select folder to scan")
        if folder:
            self._set_scan_folder(folder)

    def _set_scan_folder(self, path: str):
        self._scan_folder = path
        self.dz_label.setText(f"Ready:  {os.path.basename(path) or path}")
        self.drop_zone.setStyleSheet(
            f"QFrame{{border:2px dashed {ACCENT};border-radius:10px;background:{PANEL_BG};}}"
        )
        self.scan_btn.setEnabled(True)
        self.status_bar.showMessage(f"Folder: {path}")

    def _scan_from_explorer(self, path: str):
        self._set_mode(1)
        self._set_scan_folder(path)
        self._start_scan()

    def _start_scan(self):
        if not self._scan_folder:
            return
        if self._scan_worker and self._scan_worker.isRunning():
            self._scan_worker.abort()
        self._clear_dup_results()
        self.scan_btn.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setVisible(True)
        self.status_bar.showMessage(f"Scanning  {self._scan_folder} ...")
        self._start_activity(os.path.basename(self._scan_folder) or self._scan_folder)

        self._scan_worker = ScanWorker(self._scan_folder)
        self._scan_worker.phase.connect(self.status_bar.showMessage)
        self._scan_worker.phase.connect(lambda p: self._set_activity_label(p))
        self._scan_worker.progress.connect(self._on_scan_progress)
        self._scan_worker.found.connect(self._on_scan_found)
        self._scan_worker.error.connect(
            lambda e: self.status_bar.showMessage(f"Scan error: {e}")
        )
        self._scan_worker.finished.connect(self._on_scan_finished)
        self._scan_worker.start()

    def _on_scan_progress(self, cur: int, total: int):
        self.progress_bar.setRange(0, max(total, 1))
        self.progress_bar.setValue(cur)

    def _on_scan_found(self, dupes: dict):
        self._duplicates = dupes
        self._populate_dup_tree(dupes)
        groups = len(dupes)
        files  = sum(len(v) for v in dupes.values())
        wasted = sum(
            os.path.getsize(v[0]) * (len(v) - 1)
            for v in dupes.values() if v and os.path.isfile(v[0])
        )
        self.stat_groups._val.setText(str(groups))
        self.stat_files._val.setText(str(files))
        self.stat_wasted._val.setText(_human_size(wasted))
        has = groups > 0
        self.sel_dupes_btn.setEnabled(has)
        self.sel_all_btn.setEnabled(has)
        self.desel_all_btn.setEnabled(has)

    def _on_scan_finished(self):
        self.progress_bar.setVisible(False)
        self.scan_btn.setEnabled(True)
        self._stop_activity()
        n = len(self._duplicates)
        self.status_bar.showMessage(
            f"Scan complete — {n} duplicate group(s) found." if n
            else "Scan complete — no duplicates found."
        )

    def _clear_dup_results(self):
        self.dup_model.removeRows(0, self.dup_model.rowCount())
        self._duplicates = {}
        for card in (self.stat_groups, self.stat_files):
            card._val.setText("0")
        self.stat_wasted._val.setText("0 B")
        self.sel_dupes_btn.setEnabled(False)
        self.sel_all_btn.setEnabled(False)
        self.desel_all_btn.setEnabled(False)

    def _populate_dup_tree(self, dupes: dict):
        # FIX: block itemChanged signals while building the tree so each
        # checkbox creation does not fire _on_dup_item_changed hundreds of
        # times and trigger premature UI updates / potential recursion.
        self.dup_model.blockSignals(True)
        try:
            self.dup_model.removeRows(0, self.dup_model.rowCount())
            for h, paths in dupes.items():
                try:
                    sz = os.path.getsize(paths[0])
                except OSError:
                    sz = 0

                # ── Group header row (not checkable) ──────────────────────
                grp = QStandardItem(
                    f"  {len(paths)} identical files  ·  {_human_size(sz)} each"
                )
                grp.setForeground(QColor(ACCENT))
                grp.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
                grp.setEditable(False)
                # Store hash on the group so context menu can retrieve it
                grp.setData(h, Qt.ItemDataRole.UserRole)

                sz_item = QStandardItem(_human_size(sz))
                sz_item.setForeground(QColor(SUB))
                sz_item.setEditable(False)

                hash_item = QStandardItem(h[:22] + "…")
                hash_item.setForeground(QColor(SUB))
                hash_item.setEditable(False)

                self.dup_model.appendRow([grp, sz_item, hash_item])

                # ── File child rows (each has a checkbox) ─────────────────
                # FIX: added Qt.ItemFlag.ItemIsUserCheckable + Unchecked
                # state to every file row so the user can tick exactly which
                # files to shred.  Previously there were no checkboxes at
                # all, making the "Select duplicates" button visually do
                # nothing useful.
                for path in paths:
                    n_item = QStandardItem("  " + os.path.basename(path))
                    n_item.setData(path, Qt.ItemDataRole.UserRole)
                    n_item.setToolTip(path)
                    n_item.setEditable(False)
                    # Add checkbox
                    n_item.setFlags(
                        Qt.ItemFlag.ItemIsEnabled |
                        Qt.ItemFlag.ItemIsUserCheckable
                    )
                    n_item.setCheckState(Qt.CheckState.Unchecked)

                    try:
                        fsz = os.path.getsize(path)
                    except OSError:
                        fsz = 0
                    fsz_item = QStandardItem(_human_size(fsz))
                    fsz_item.setForeground(QColor(SUB))
                    fsz_item.setEditable(False)

                    dir_item = QStandardItem(os.path.dirname(path))
                    dir_item.setForeground(QColor(SUB))
                    dir_item.setEditable(False)

                    grp.appendRow([n_item, fsz_item, dir_item])
        finally:
            self.dup_model.blockSignals(False)

        self.dup_tree.expandAll()
        self._update_command_state()

    # -----------------------------------------------------------------------
    # Duplicate checkbox helpers
    # -----------------------------------------------------------------------

    def _on_dup_item_changed(self, item: QStandardItem):
        """
        FIX: called whenever a checkbox is toggled by the user.
        Refreshes the selection counter and SHRED button state immediately
        so the UI stays in sync without any extra click.
        """
        self._update_command_state()

    def _set_all_dup_checks(self, state: Qt.CheckState):
        """Internal: set every file-row checkbox to *state*."""
        self.dup_model.blockSignals(True)
        try:
            for row in range(self.dup_model.rowCount()):
                grp = self.dup_model.item(row, 0)
                if grp is None:
                    continue
                for cr in range(grp.rowCount()):
                    child = grp.child(cr, 0)
                    if child is not None:
                        child.setCheckState(state)
        finally:
            self.dup_model.blockSignals(False)
        self._update_command_state()

    def _select_duplicates_only(self):
        """
        FIX (was _select_duplicates — did nothing): checks only the
        *duplicate* copies in each group (index 1, 2, … of each group),
        leaving the first file (index 0, treated as 'original') unchecked.

        Previously this method called selectionModel().select() which:
          1. Did not check any checkboxes (wrong API for checkbox-based trees)
          2. Could crash with a segfault when grp.child() returned None
        """
        self.dup_model.blockSignals(True)
        try:
            for row in range(self.dup_model.rowCount()):
                grp = self.dup_model.item(row, 0)
                if grp is None:
                    continue
                for cr in range(grp.rowCount()):
                    child = grp.child(cr, 0)
                    if child is None:
                        continue
                    # cr == 0 → keep the "original"; cr > 0 → check as duplicate
                    child.setCheckState(
                        Qt.CheckState.Unchecked if cr == 0
                        else Qt.CheckState.Checked
                    )
        finally:
            self.dup_model.blockSignals(False)
        self._update_command_state()

    def _select_all_duplicates(self):
        """Check every file in every duplicate group (originals + dupes)."""
        self._set_all_dup_checks(Qt.CheckState.Checked)

    def _deselect_all_duplicates(self):
        """Uncheck every file."""
        self._set_all_dup_checks(Qt.CheckState.Unchecked)

    # -----------------------------------------------------------------------
    # Shredder  (files · folders · drives)
    # -----------------------------------------------------------------------

    def _confirm_shred(self):
        if self.stack.currentIndex() == 0:
            targets = self._get_selected_entries()
        else:
            targets = self._get_dup_selected_paths()
        self._confirm_shred_targets(targets)

    def _confirm_shred_targets(self, targets: list[str]):
        if not targets:
            return

        # Count what we're about to destroy
        file_count = 0
        dir_count  = 0
        drive_count = 0
        for t in targets:
            if os.path.isfile(t):
                file_count += 1
            elif os.path.isdir(t):
                # Detect drive roots (e.g. C:\)
                p = Path(t)
                if p == p.parent:
                    drive_count += 1
                else:
                    dir_count += 1

        summary_parts = []
        if file_count:  summary_parts.append(f"{file_count} file(s)")
        if dir_count:   summary_parts.append(f"{dir_count} folder(s)")
        if drive_count: summary_parts.append(f"{drive_count} drive(s)")
        summary = ", ".join(summary_parts)

        # Extra confirmation for drives
        if drive_count:
            dlg = QDialog(self)
            dlg.setWindowTitle("DANGER — Drive Shred")
            dlg.setMinimumWidth(480)
            lay = QVBoxLayout(dlg)
            lay.setContentsMargins(20, 20, 20, 20)
            lay.setSpacing(12)
            warn = QLabel(
                f"<b style='color:{ACCENT};font-size:15px;'>"
                f"You are about to shred an ENTIRE DRIVE.</b><br><br>"
                f"This will permanently destroy EVERY FILE on the selected drive(s).<br>"
                f"This action is <b>completely irreversible</b>.<br><br>"
                f"Type  <b>I UNDERSTAND</b>  below to confirm:"
            )
            warn.setWordWrap(True)
            warn.setStyleSheet(f"color:{TEXT};background:transparent;")
            lay.addWidget(warn)
            confirm_box = QLineEdit()
            confirm_box.setPlaceholderText("Type: I UNDERSTAND")
            confirm_box.setFixedHeight(34)
            lay.addWidget(confirm_box)
            btns = QDialogButtonBox(
                QDialogButtonBox.StandardButton.Ok |
                QDialogButtonBox.StandardButton.Cancel
            )
            btns.accepted.connect(dlg.accept)
            btns.rejected.connect(dlg.reject)
            lay.addWidget(btns)
            if dlg.exec() != QDialog.DialogCode.Accepted:
                return
            if confirm_box.text().strip().upper() != "I UNDERSTAND":
                QMessageBox.warning(self, "Cancelled",
                                    "Confirmation text did not match. Shred cancelled.")
                return

        # ── Custom confirmation dialog — file list always visible ──────────
        dlg = QDialog(self)
        dlg.setWindowTitle("Confirm Secure Shred")
        dlg.setMinimumWidth(520)
        dlg.setMinimumHeight(340)
        lay = QVBoxLayout(dlg)
        lay.setContentsMargins(20, 20, 20, 16)
        lay.setSpacing(12)

        # Header
        hdr = QLabel(
            f"<b style='color:{ACCENT};font-size:14px;'>"
            f"Permanently destroy {summary}?</b>"
        )
        hdr.setWordWrap(True)
        hdr.setStyleSheet("background:transparent;")
        lay.addWidget(hdr)

        # File list — always fully visible, never hidden
        MAX_SHOW = 200
        list_lines = []
        for t in targets:
            norm = os.path.normpath(t)
            name = os.path.basename(norm) or norm
            if os.path.isfile(norm):
                try:
                    sz = _human_size(os.path.getsize(norm))
                except OSError:
                    sz = "?"
                list_lines.append(f"{name}  ({sz})\n  {norm}")
            elif os.path.isdir(norm):
                list_lines.append(f"{name}/  [folder]\n  {norm}")
            else:
                list_lines.append(f"{name}  [not found]\n  {norm}")

        if len(list_lines) > MAX_SHOW:
            list_lines = list_lines[:MAX_SHOW]
            list_lines.append(f"... and {len(targets) - MAX_SHOW} more items")

        file_box = QTextEdit()
        file_box.setReadOnly(True)
        file_box.setFont(QFont("Consolas", 10))
        file_box.setPlainText("\n\n".join(list_lines))
        file_box.setMinimumHeight(140)
        lay.addWidget(file_box)

        # Warning text
        warn = QLabel(
            "Each file will be overwritten <b>7 times</b> "
            "(0x00 \u2192 0xFF \u2192 random \u2192 0x00 \u2192 0xFF \u2192 random \u2192 0x00) "
            "then permanently deleted.<br>"
            "<b>This action CANNOT be undone.</b>"
        )
        warn.setWordWrap(True)
        warn.setStyleSheet(f"color:{SUB};background:transparent;font-size:11px;")
        lay.addWidget(warn)

        # Buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setFixedHeight(34)
        cancel_btn.clicked.connect(dlg.reject)

        shred_btn = QPushButton("  SHRED  ")
        shred_btn.setFixedHeight(34)
        shred_btn.setStyleSheet(BTN_ACCENT)
        shred_btn.clicked.connect(dlg.accept)

        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(shred_btn)
        lay.addLayout(btn_row)

        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._run_shred(targets)

    def _run_shred(self, targets: list[str]):
        self.shred_btn.setEnabled(False)
        self.shred_cmd_btn.setEnabled(False)
        self.scan_btn.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setVisible(True)
        label = os.path.basename(targets[0]) if targets else "files"
        self._start_activity(label)

        self._shred_log = ShredLogDialog(self)
        self._shred_log.show()

        self._shred_worker = ShredWorker(targets)
        self._shred_worker.progress.connect(
            lambda c, t: (
                self.progress_bar.setRange(0, t),
                self.progress_bar.setValue(c),
                self.status_bar.showMessage(f"Shredding  {c} / {t} ...")
            )
        )
        self._shred_worker.status.connect(self._set_activity_label)
        self._shred_errors: list[str] = []
        self._shredded_paths: list[str] = []
        self._shred_worker.shredded.connect(lambda p: self._shredded_paths.append(p))
        self._shred_worker.shredded.connect(self._on_file_shredded)
        self._shred_worker.log.connect(self._shred_log.append)
        self._shred_worker.error.connect(self._on_shred_error)
        self._shred_worker.finished.connect(self._on_shred_finished)
        self._shred_worker.start()

    def _set_activity_label(self, text: str):
        """Update just the file/phase name in the activity display."""
        self._op_cur_file = text
        m, s = divmod(self._op_timer_secs, 60)
        self.lbl_activity.setText(f"⏱ {m:02d}:{s:02d}  |  ⚡ {text}")

    def _on_shred_error(self, msg: str):
        self._shred_errors.append(msg)
        self.status_bar.showMessage(f"Error: {msg}")

    def _on_file_shredded(self, path: str):
        # FIX: added None-guards throughout.  Previously grp or child could
        # be None (e.g. after a row was already removed mid-loop) causing an
        # AttributeError that crashed the whole callback and left the UI in a
        # broken state.
        for row in range(self.dup_model.rowCount()):
            grp = self.dup_model.item(row, 0)
            if grp is None:
                continue
            for cr in range(grp.rowCount()):
                child = grp.child(cr, 0)
                if child is None:
                    continue
                if child.data(Qt.ItemDataRole.UserRole) == path:
                    grp.removeRow(cr)
                    return

    def _on_shred_finished(self):
        self.progress_bar.setVisible(False)
        self.scan_btn.setEnabled(True)
        self._stop_activity()
        for row in range(self.dup_model.rowCount() - 1, -1, -1):
            grp = self.dup_model.item(row, 0)
            if grp is None or grp.rowCount() <= 1:
                self.dup_model.removeRow(row)

        errors = getattr(self, "_shred_errors", [])
        if errors:
            self.status_bar.showMessage(f"Shred finished with {len(errors)} error(s).")
            QMessageBox.warning(
                self, "Shred Errors",
                f"{len(errors)} item(s) could not be shredded:\n\n" +
                "\n".join(errors[:20])
            )
        else:
            self.status_bar.showMessage("Shred complete — all files destroyed.")

        self._shred_errors = []
        self._update_command_state()
        self._refresh()
        # Notify Windows Explorer so deleted icons disappear immediately
        shredded = getattr(self, "_shredded_paths", [])
        if shredded:
            _shell_notify_deleted(shredded)
        self._shredded_paths = []

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _reveal_in_explorer(self, path: str):
        self._set_mode(0)
        self._navigate_to(os.path.dirname(path))


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _shell_notify_deleted(paths: list[str]):
    """
    Tell Windows Explorer that paths have been deleted so icons disappear
    immediately without requiring an F5 refresh.
    Uses SHChangeNotify with SHCNE_DELETE / SHCNE_RMDIR.
    """
    try:
        import ctypes
        SHCNE_DELETE  = 0x00000002
        SHCNE_RMDIR   = 0x00000010
        SHCNF_PATHW   = 0x0005
        SHCNE_ALLEVENTS = 0x7FFFFFFF
        shell32 = ctypes.windll.shell32

        for p in paths:
            p_w = ctypes.c_wchar_p(p)
            # Notify for the deleted item itself
            shell32.SHChangeNotify(SHCNE_RMDIR | SHCNE_DELETE,
                                   SHCNF_PATHW, p_w, None)
            # Notify the parent folder so Explorer redraws it
            parent = os.path.dirname(p)
            if parent:
                shell32.SHChangeNotify(SHCNE_RMDIR,
                                       SHCNF_PATHW,
                                       ctypes.c_wchar_p(parent), None)

        # Flush all pending notifications
        shell32.SHChangeNotify(SHCNE_ALLEVENTS, 0, None, None)
    except Exception:
        pass   # Non-Windows — silently skip


def _resolve_drop_path(raw: str) -> str | None:
    """
    Resolve a path coming from a Qt drag-and-drop event.
    Windows strips the .lnk extension from shortcut files when building
    the MIME data, so 'D:\\Desktop\\Surge' is actually 'D:\\Desktop\\Surge.lnk'.
    Try the path as-is first, then with .lnk appended.
    Returns the normalised, existing path or None if still not found.
    """
    p = os.path.normpath(raw)
    if os.path.exists(p):
        return p
    lnk = p + ".lnk"
    if os.path.exists(lnk):
        return lnk
    return None


def _human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{int(n)} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Surge")

    pal = QPalette()
    pal.setColor(QPalette.ColorRole.Window,          QColor(DARK_BG))
    pal.setColor(QPalette.ColorRole.WindowText,      QColor(TEXT))
    pal.setColor(QPalette.ColorRole.Base,            QColor(CARD_BG))
    pal.setColor(QPalette.ColorRole.AlternateBase,   QColor(PANEL_BG))
    pal.setColor(QPalette.ColorRole.Text,            QColor(TEXT))
    pal.setColor(QPalette.ColorRole.Button,          QColor(PANEL_BG))
    pal.setColor(QPalette.ColorRole.ButtonText,      QColor(TEXT))
    pal.setColor(QPalette.ColorRole.Highlight,       QColor(ACCENT))
    pal.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
    app.setPalette(pal)

    win = SurgeWindow()
    win.show()

    # CLI: Surge.exe --shred "C:\path\to\file1" "C:\path\to\file2" ...
    # Launched from Windows Explorer right-click context menu
    args = app.arguments()[1:]   # skip argv[0]
    if args and args[0] == "--shred":
        targets = [p for p in args[1:] if os.path.exists(p)]
        if targets:
            QTimer.singleShot(200, lambda: win._confirm_shred_targets(targets))

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
