# File: D:\SurgeApp\SurgeApp_Claude_Terminal.py
# Purpose: Claude-style Luna Command Center UI
# - Right panel is collapsible (slide open/close) and contains: Live Work (Terminal), Files (editable), Diff, Tasks, Plan
# - Main chat stays clean: only You/Luna (no session/control noise, no quality report headers)
# - Left panel = Sessions list (named, editable, persistent). Clicking a session restores its chat history.
# - Drag & drop + 📎 attachment button; attachments copied into D:\SurgeApp\uploads\ and referenced in task payload.
# Drop-in replacement. No manual edits required.

from __future__ import annotations

import difflib
import html
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from PySide6.QtCore import QPoint, QRect, QThread, QTimer, Qt, Signal, Slot
    from PySide6.QtGui import QColor, QPainter, QPainterPath, QPen, QTextCursor, QDragEnterEvent, QDropEvent
    from PySide6.QtWidgets import (
        QApplication,
        QFileDialog,
        QFrame,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QListWidget,
        QListWidgetItem,
        QMenu,
        QPushButton,
        QSplitter,
        QTabWidget,
        QTextBrowser,
        QPlainTextEdit,
        QVBoxLayout,
        QSizeGrip,
        QWidget,
    )
    PYSIDE_AVAILABLE = True
except Exception:
    PYSIDE_AVAILABLE = False


# =============================================================================
# Paths
# =============================================================================
DEFAULT_PROJECT_DIR = r"D:\SurgeApp"
PROJECT_DIR = Path(os.environ.get("LUNA_PROJECT_DIR", DEFAULT_PROJECT_DIR))

TASKS_DIR = PROJECT_DIR / "tasks"
ACTIVE_DIR = TASKS_DIR / "active"
DONE_DIR = TASKS_DIR / "done"
FAILED_DIR = TASKS_DIR / "failed"
SOLUTIONS_DIR = PROJECT_DIR / "solutions"
LOGS_DIR = PROJECT_DIR / "logs"
MEMORY_DIR = PROJECT_DIR / "memory"

WORKER_PATH = PROJECT_DIR / "worker.py"
WORKER_HEARTBEAT_PATH = LOGS_DIR / "luna_worker_heartbeat.json"
WORKER_LOCK_PATH = LOGS_DIR / "luna_worker.lock.json"
LIVE_FEED_PATH = LOGS_DIR / "luna_live_feed.jsonl"

HISTORY_PATH = LOGS_DIR / "luna_command_history.jsonl"
SESSIONS_PATH = LOGS_DIR / "luna_sessions.json"
KILL_SWITCH_PATH = PROJECT_DIR / "LUNA_STOP_NOW.flag"

UPLOADS_DIR = PROJECT_DIR / "uploads"  # attachments are copied here (path-jail friendly)

AIDER_JOBS_DIR = PROJECT_DIR / "aider_jobs"
AIDER_ACTIVE_DIR = AIDER_JOBS_DIR / "active"
AIDER_DONE_DIR = AIDER_JOBS_DIR / "done"
AIDER_FAILED_DIR = AIDER_JOBS_DIR / "failed"

# Windows: hide spawned console windows for subprocesses we launch
CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0

_STOP_WORDS = {
    "stop", "pause", "halt", "hold on", "hold up", "wait", "freeze",
    "stop luna", "pause luna", "stop working", "pause working",
    "/stop", "/pause", "/halt",
}
_RESUME_WORDS = {
    "resume", "continue", "go", "unpause", "restart", "start again",
    "/resume", "/continue", "/unpause",
}


# =============================================================================
# Utilities
# =============================================================================
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_layout() -> None:
    for d in [TASKS_DIR, ACTIVE_DIR, DONE_DIR, FAILED_DIR, SOLUTIONS_DIR, LOGS_DIR, MEMORY_DIR, UPLOADS_DIR, AIDER_JOBS_DIR, AIDER_ACTIVE_DIR, AIDER_DONE_DIR, AIDER_FAILED_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def _safe_read_json(path: Path, default: Any = None) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8", errors="ignore") or "null")
    except Exception:
        return default
    return default


def _safe_write_json(path: Path, obj: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def write_history(entry: Dict[str, Any]) -> None:
    ensure_layout()
    entry["timestamp"] = _now_iso()
    try:
        with open(HISTORY_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _read_history_lines(limit: int = 1500) -> List[Dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    try:
        raw = HISTORY_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for line in raw[-limit:]:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def _sanitize_luna_text(text: str) -> str:
    """
    Remove noisy quality-report headers like:
      # LUNA QUALITY REPORT
      # mode=...
      # task=...
      # target=...
    Keep only the actual response body.
    """
    s = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not s.strip():
        return ""

    # Remove bracketed report blocks if present
    s = re.sub(r"^\s*\[LUNA\s+QUALITY\s+REPORT\].*?\n", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*\[LUNA\s+QUALITY\s+REPORT.*?\]\s*\n", "", s, flags=re.IGNORECASE)

    lines = s.splitlines()
    cleaned: List[str] = []
    dropped = 0
    for i, ln in enumerate(lines):
        t = ln.strip()
        if i < 30:
            if t.startswith("#"):
                # Drop known meta lines
                if re.search(r"luna\s+quality\s+report", t, re.IGNORECASE):
                    dropped += 1
                    continue
                if re.match(r"^#\s*(mode|task|target|ts|timestamp|quality|report)\s*[:=]", t, re.IGNORECASE):
                    dropped += 1
                    continue
                if "mode=" in t.lower() or "task=" in t.lower() or "target=" in t.lower():
                    dropped += 1
                    continue
            # Also drop empty banner separators right after meta
            if dropped and (not t):
                dropped += 1
                continue
        cleaned.append(ln)

    # Trim leading empties
    while cleaned and not cleaned[0].strip():
        cleaned.pop(0)

    out = "\n".join(cleaned).strip()
    return out


def _load_sessions() -> List[Dict[str, Any]]:
    data = _safe_read_json(SESSIONS_PATH, default=None)
    if isinstance(data, dict) and isinstance(data.get("sessions"), list):
        sessions = [s for s in data["sessions"] if isinstance(s, dict)]
        return sessions
    if isinstance(data, list):
        return [s for s in data if isinstance(s, dict)]
    return []


def _save_sessions(sessions: List[Dict[str, Any]]) -> None:
    _safe_write_json(SESSIONS_PATH, {"sessions": sessions, "ts": _now_iso()})


def _ensure_default_session(sessions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if sessions:
        return sessions, sessions[0]
    sid = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    s = {"id": sid, "name": "Session 1", "created_at": _now_iso(), "last_used": _now_iso()}
    sessions = [s]
    _save_sessions(sessions)
    return sessions, s


def _session_messages(session_id: str, limit: int = 600) -> List[Tuple[str, str]]:
    """
    Returns list of (role, text) for the session from history.
    role in {"user","luna"}.
    """
    rows = _read_history_lines(limit=3000)
    msgs: List[Tuple[str, str]] = []
    for r in rows:
        if str(r.get("session_id") or "") != str(session_id):
            continue
        ev = str(r.get("event") or "")
        if ev == "user_input":
            text = str(r.get("raw") or "").strip()
            if text:
                msgs.append(("user", text))
        elif ev == "luna_response":
            text = str(r.get("text") or "").strip()
            if text:
                msgs.append(("luna", _sanitize_luna_text(text)))
    return msgs[-limit:]


def submit_worker_task(prompt_text: str, session_id: str, attachments: List[Path]) -> str:
    """
    Worker-compatible task payload. Attachments are included as relative paths under PROJECT_DIR.
    """
    ensure_layout()
    task_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]

    attach_rel: List[str] = []
    for p in attachments:
        try:
            attach_rel.append(str(Path(p).resolve().relative_to(PROJECT_DIR.resolve())))
        except Exception:
            attach_rel.append(str(p))

    payload = {
        "task_id": task_id,
        "id": task_id,
        "mode": "prompt",
        "task_type": "prompt",
        "timestamp": _now_iso(),
        "session_id": session_id,
        "prompt": str(prompt_text or "").strip(),
        "attachments": attach_rel,
    }

    tmp_path = ACTIVE_DIR / f"{task_id}.tmp"
    final_path = ACTIVE_DIR / f"{task_id}.json"
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(final_path)
    except Exception:
        pass
    return task_id

def submit_aider_job(instructions: str, session_id: str, target_files: Optional[List[str]] = None, apply_on_pass: bool = False, timeout_s: Optional[int] = None) -> str:
    """
    Enqueue an Aider Bridge job (handled by aider_bridge.py) into aider_jobs/active.
    """
    ensure_layout()
    task_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    targets = target_files if isinstance(target_files, list) and target_files else ["worker.py"]

    payload: Dict[str, Any] = {
        "task_id": task_id,
        "id": task_id,
        "task_type": "aider_patch",
        "timestamp": _now_iso(),
        "session_id": session_id,
        "target_files": targets,
        "instructions": str(instructions or "").strip(),
        "apply_on_pass": bool(apply_on_pass),
    }
    if timeout_s is not None:
        payload["timeout_s"] = int(timeout_s)

    tmp_path = AIDER_ACTIVE_DIR / f"{task_id}.tmp"
    final_path = AIDER_ACTIVE_DIR / f"{task_id}.json"
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(final_path)
    except Exception:
        pass
    return task_id


def submit_self_upgrade_task(session_id: str) -> str:
    """
    Enqueue a worker self-upgrade pass.
    """
    ensure_layout()
    task_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]

    payload = {
        "task_id": task_id,
        "id": task_id,
        "mode": "self-upgrade",
        "task_type": "self-upgrade",
        "worker_mode": "self-upgrade",
        "timestamp": _now_iso(),
        "session_id": session_id,
        "prompt": "",
        "attachments": [],
    }

    tmp_path = ACTIVE_DIR / f"{task_id}.tmp"
    final_path = ACTIVE_DIR / f"{task_id}.json"
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(final_path)
    except Exception:
        pass
    return task_id



def _read_solution_text(task_id: str) -> str:
    sol_txt = SOLUTIONS_DIR / f"{task_id}.txt"
    if sol_txt.exists():
        return sol_txt.read_text(encoding="utf-8", errors="ignore").strip()
    return ""


def _resolve_done_payload(task_id: str) -> Tuple[str, str]:
    """
    Returns (chat_text, detail_text_for_right_panel).
    Chat text is sanitized and stripped of quality-report headers.
    """
    done_path = DONE_DIR / f"{task_id}.json"
    res = _safe_read_json(done_path, default={}) or {}
    response = str(res.get("response") or res.get("summary") or "").strip()
    detail = ""

    sol_ref = res.get("solution_file") or res.get("solution_path")
    if sol_ref:
        try:
            p = PROJECT_DIR / str(sol_ref)
            if p.exists():
                detail = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            detail = ""

    if not detail:
        detail = _read_solution_text(task_id)

    response_clean = _sanitize_luna_text(response)
    detail_clean = _sanitize_luna_text(detail)

    generic = {"task completed.", "task completed", "done", "success", "completed."}
    chat_text = response_clean
    if (not chat_text) or (chat_text.lower().strip() in generic):
        chat_text = detail_clean if detail_clean else (response_clean or "Task completed.")

    # Keep chat concise; right panel can show full detail
    chat_text = (chat_text or "").strip()
    if len(chat_text) > 8000:
        chat_text = chat_text[:8000].rstrip() + "\n…"

    return chat_text, (detail or "").strip()


def _run_git_diff(rel_path: str) -> str:
    try:
        if not (PROJECT_DIR / ".git").exists():
            return ""
        cmd = ["git", "-C", str(PROJECT_DIR), "diff", "--", rel_path]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=6, creationflags=CREATE_NO_WINDOW)
        return (p.stdout or "").strip()
    except Exception:
        return ""


def _latest_logic_updates_dir() -> Optional[Path]:
    for cand in (PROJECT_DIR / "logic_updates", PROJECT_DIR / "LOGIC_UPDATES", PROJECT_DIR / "logic-updates"):
        try:
            if cand.exists() and cand.is_dir():
                sub = [p for p in cand.iterdir() if p.is_dir()]
                if not sub:
                    return None
                sub.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                return sub[0]
        except Exception:
            continue
    return None


def _best_effort_staged_diff(target_path: Path) -> str:
    try:
        proposal = _latest_logic_updates_dir()
        if not proposal:
            return ""
        staged = proposal / target_path.name
        if not staged.exists():
            return ""
        a = staged.read_text(encoding="utf-8", errors="ignore").splitlines(keepends=True)
        b = target_path.read_text(encoding="utf-8", errors="ignore").splitlines(keepends=True)
        diff = difflib.unified_diff(a, b, fromfile=str(staged), tofile=str(target_path), lineterm="")
        return "".join(diff).strip()
    except Exception:
        return ""


def _project_files(filter_text: str) -> List[Path]:
    """
    Returns a list of files under project for Files tab.
    filter_text:
      - normal: substring match on relative path
      - starts with '?': content search (needle after '?') across small text files
    """
    q = (filter_text or "").strip()
    content_mode = q.startswith("?")
    needle = q[1:].strip().lower() if content_mode else q.lower()

    items: List[Path] = []
    try:
        for root, dirs, files in os.walk(str(PROJECT_DIR)):
            # prune noisy dirs
            dirs[:] = [d for d in dirs if d.lower() not in (
                ".git", "__pycache__", "venv", ".venv", "env", "node_modules",
                "memory", "logs", "tasks", "solutions", "uploads"
            )]
            for fn in files:
                ext = os.path.splitext(fn)[1].lower()
                if ext not in (".py", ".md", ".txt", ".bat", ".json", ".jsonl"):
                    continue
                p = Path(root) / fn
                rel = str(p.relative_to(PROJECT_DIR))
                # size guard
                try:
                    if p.stat().st_size > 2_000_000:
                        continue
                except Exception:
                    continue

                if content_mode:
                    if not needle:
                        continue
                    try:
                        txt = p.read_text(encoding="utf-8", errors="ignore")
                    except Exception:
                        continue
                    if needle not in txt.lower():
                        continue
                else:
                    if needle and needle not in rel.lower():
                        continue

                items.append(p)

        items.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0.0, reverse=True)
    except Exception:
        return []
    return items[:600]


# =============================================================================
# Threads
# =============================================================================
class WorkerWaitThread(QThread):
    task_finished = Signal(str, str, str, str)  # status, task_id, chat_text, detail_text
    task_error = Signal(str)

    def __init__(self, task_id: str, timeout_s: int = 600):
        super().__init__()
        self.task_id = task_id
        self.timeout_s = max(30, int(timeout_s))

    def run(self) -> None:
        done_path = DONE_DIR / f"{self.task_id}.json"
        failed_path = FAILED_DIR / f"{self.task_id}.json"
        t0 = time.time()
        while True:
            if time.time() - t0 > self.timeout_s:
                self.task_error.emit(f"Task {self.task_id} timed out after {self.timeout_s}s.")
                return

            if done_path.exists():
                try:
                    chat_text, detail = _resolve_done_payload(self.task_id)
                    self.task_finished.emit("done", self.task_id, chat_text, detail)
                    return
                except Exception as e:
                    self.task_error.emit(f"Error reading done task: {e}")
                    return

            if failed_path.exists():
                try:
                    res = _safe_read_json(failed_path, default={}) or {}
                    reason = str(res.get("error") or res.get("reason") or "Unknown failure").strip()
                    self.task_finished.emit("failed", self.task_id, reason, "")
                    return
                except Exception as e:
                    self.task_error.emit(f"Error reading failed task: {e}")
                    return

            time.sleep(0.35)


class ShellExecutionThread(QThread):
    shell_result = Signal(str, str, int)  # stdout, stderr, rc

    def __init__(self, cmd: str):
        super().__init__()
        self.cmd = cmd

    def run(self) -> None:
        try:
            result = subprocess.run(
                self.cmd,
                shell=True,
                capture_output=True,
                text=True,
                cwd=str(PROJECT_DIR),
                timeout=120,
                creationflags=CREATE_NO_WINDOW,
            )
            self.shell_result.emit(result.stdout or "", result.stderr or "", int(result.returncode))
        except Exception as e:
            self.shell_result.emit("", str(e), 1)


class RestartWorkerThread(QThread):
    """Kill the running worker, clear the lock, and start a fresh one."""
    restart_done = Signal(str)  # status message

    def run(self) -> None:
        try:
            lock_data = _safe_read_json(WORKER_LOCK_PATH, default={}) or {}
            pid = int(lock_data.get("pid") or 0)
            if pid and pid != os.getpid():
                try:
                    import signal as _signal
                    os.kill(pid, _signal.SIGTERM)
                except Exception:
                    try:
                        subprocess.run(
                            ["taskkill", "/F", "/PID", str(pid)],
                            creationflags=CREATE_NO_WINDOW,
                            capture_output=True,
                        )
                    except Exception:
                        pass
                time.sleep(1.5)

            try:
                WORKER_LOCK_PATH.unlink(missing_ok=True)
            except Exception:
                pass

            python_exe = Path(sys.executable)
            pythonw = python_exe.parent / "pythonw.exe"
            exe = str(pythonw) if pythonw.exists() else str(python_exe)
            subprocess.Popen(
                [exe, str(WORKER_PATH)],
                cwd=str(PROJECT_DIR),
                creationflags=CREATE_NO_WINDOW,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
            time.sleep(2)
            self.restart_done.emit("Worker restarted.")
        except Exception as exc:
            self.restart_done.emit(f"Restart error: {exc}")


class AiderLaunchThread(QThread):
    aider_done = Signal(int)  # exit code

    def run(self) -> None:
        bat = str(PROJECT_DIR / "run_aider_luna.bat")
        try:
            proc = subprocess.Popen(
                ["cmd.exe", "/c", bat],
                cwd=str(PROJECT_DIR),
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
            proc.wait()
            self.aider_done.emit(proc.returncode)
        except Exception:
            self.aider_done.emit(1)


class LiveFeedThread(QThread):
    feed_event = Signal(dict)

    def __init__(self):
        super().__init__()
        self._stop = False
        self._pos = 0

    def stop(self) -> None:
        self._stop = True

    def run(self) -> None:
        ensure_layout()
        try:
            if LIVE_FEED_PATH.exists():
                self._pos = LIVE_FEED_PATH.stat().st_size
        except Exception:
            self._pos = 0

        while not self._stop:
            try:
                time.sleep(0.35)
                if not LIVE_FEED_PATH.exists():
                    continue
                size = LIVE_FEED_PATH.stat().st_size
                if size < self._pos:
                    self._pos = 0
                if size <= self._pos:
                    continue
                with LIVE_FEED_PATH.open("r", encoding="utf-8", errors="replace") as f:
                    f.seek(self._pos)
                    data = f.read()
                    self._pos = f.tell()
                for line in data.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(row, dict):
                        self.feed_event.emit(row)
            except Exception:
                continue


# =============================================================================
# UI (Claude-like palette)
# =============================================================================
CLAUDE_DARK_QSS = """
QWidget { background: #0f0f10; color: #e9e9eb; font-family: "Segoe UI"; font-size: 12px; }
QFrame#Surface { background: #141416; border: 1px solid #242428; border-radius: 12px; }
QFrame#SurfaceRaised { background: #17171a; border: 1px solid #2a2a30; border-radius: 12px; }
QLabel#Muted { color: #a6a6ad; }
QLabel#Title { font-size: 13px; font-weight: 600; }
QPushButton { background: #1a1a1e; border: 1px solid #2a2a30; border-radius: 10px; padding: 8px 10px; }
QPushButton:hover { background: #202026; }
QPushButton:pressed { background: #26262e; }
QPushButton#Primary { background: #1c2b4a; border: 1px solid #2f4f89; }
QPushButton#Primary:hover { background: #243864; }
QPushButton#Ghost { background: transparent; border: 1px solid #2a2a30; }
QLineEdit { background: #141416; border: 1px solid #242428; border-radius: 10px; padding: 10px; color: #f2f2f4; selection-background-color: #2f4f89; }
QTextBrowser { background: transparent; border: none; color: #e9e9eb; selection-background-color: #2f4f89; }
QPlainTextEdit { background: #101012; border: 1px solid #242428; border-radius: 10px; padding: 10px; color: #e9e9eb; selection-background-color: #2f4f89; font-family: Consolas, "Courier New", monospace; font-size: 12px; }
QListWidget { background: transparent; border: none; outline: none; }
QListWidget::item { padding: 8px 10px; border-radius: 10px; color: #d8d8dc; }
QListWidget::item:selected { background: #1c2b4a; color: #ffffff; }
QTabWidget::pane { border: 1px solid #242428; border-radius: 12px; top: -1px; }
QTabBar::tab { background: #141416; border: 1px solid #242428; border-bottom: none;
               padding: 8px 10px; border-top-left-radius: 10px; border-top-right-radius: 10px; margin-right: 6px; color: #bdbdc4; }
QTabBar::tab:selected { background: #17171a; color: #ffffff; border-color: #2a2a30; }
QSplitter::handle { background: #1a1a1e; }
"""


def _icon_for_event(evt: str) -> str:
    evt = (evt or "").lower().strip()
    return {
        "scan": "🔎", "patch": "🧩", "patch_ok": "✅", "patch_fail": "❌",
        "tier_gen": "🧠", "tier_done": "✅", "agent": "👥", "debate": "💬",
        "evolution": "🧬", "error": "⚠️", "info": "ℹ️", "wire": "🔌",
        "convo": "💭", "repair": "🛠️",
    }.get(evt, "•")


def _color_for_event(evt: str) -> str:
    evt = (evt or "").lower().strip()
    if evt in ("error", "patch_fail"):
        return "#ff5f56"
    if evt in ("patch_ok", "tier_done"):
        return "#27c93f"
    if evt in ("patch", "debate", "repair"):
        return "#ffbd2e"
    if evt in ("wire",):
        return "#5b8cff"
    return "#a6a6ad"


def _esc(s: str) -> str:
    return html.escape(str(s or ""), quote=False)


def _fmt_hms() -> str:
    return datetime.now().strftime("%H:%M:%S")


# =============================================================================
# GUI
# =============================================================================
if PYSIDE_AVAILABLE:
    class RichText(QTextBrowser):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.setOpenExternalLinks(True)
            self.setReadOnly(True)

        def append_block(self, title: str, body: str, accent: str = "#5b8cff") -> None:
            title = _esc(title)
            body = _esc(body).replace("\n", "<br>")
            self.append(f"""
            <div style="margin: 10px 0; padding: 10px 12px; border-radius: 12px; background: rgba(255,255,255,0.03);
                        border: 1px solid rgba(255,255,255,0.06);">
              <div style="font-weight: 600; color: {accent}; margin-bottom: 6px;">{title}</div>
              <div style="color: #e9e9eb; line-height: 1.45;">{body}</div>
            </div>
            """)
            self.moveCursor(QTextCursor.End)

        def append_user(self, text: str) -> None:
            t = _esc(text)
            self.append(f"""
            <div style="margin: 10px 0; padding: 10px 12px; border-radius: 12px;
                        background: rgba(91,140,255,0.10); border: 1px solid rgba(91,140,255,0.25);">
              <div style="font-weight: 700; color: #5b8cff; margin-bottom: 6px;">You</div>
              <div style="color: #ffffff; line-height: 1.45;">{t}</div>
            </div>
            """)
            self.moveCursor(QTextCursor.End)

        def append_ai(self, text: str) -> None:
            t = _esc(text).replace("\n", "<br>")
            self.append(f"""
            <div style="margin: 10px 0; padding: 10px 12px; border-radius: 12px;
                        background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.06);">
              <div style="font-weight: 700; color: #e9e9eb; margin-bottom: 6px;">Luna</div>
              <div style="color: #e9e9eb; line-height: 1.45;">{t}</div>
            </div>
            """)
            self.moveCursor(QTextCursor.End)

        def append_error(self, text: str) -> None:
            self.append_block("Error", str(text or ""), accent="#ff5f56")

    class AttachLineEdit(QLineEdit):
        files_dropped = Signal(list)

        def __init__(self, parent=None):
            super().__init__(parent)
            self.setAcceptDrops(True)

        def dragEnterEvent(self, event: QDragEnterEvent) -> None:
            try:
                if event.mimeData().hasUrls():
                    event.acceptProposedAction()
                    return
            except Exception:
                pass
            super().dragEnterEvent(event)

        def dropEvent(self, event: QDropEvent) -> None:
            paths: List[str] = []
            try:
                for url in event.mimeData().urls():
                    p = url.toLocalFile()
                    if p:
                        paths.append(p)
            except Exception:
                paths = []
            if paths:
                self.files_dropped.emit(paths)
                event.acceptProposedAction()
                return
            super().dropEvent(event)

    class LunaClaudeStyleWindow(QWidget):
        def __init__(self):
            super().__init__()
            self.setWindowTitle("SurgeApp — Luna Command Center")
            self.resize(1520, 940)
            self.setMinimumSize(1120, 720)
            # Enable resize for frameless window (corner grip)
            self._size_grip = QSizeGrip(self)
            self._size_grip.setFixedSize(16, 16)
            self._size_grip.raise_()


            self.setWindowFlags(Qt.FramelessWindowHint | Qt.Window)
            self.setAttribute(Qt.WA_TranslucentBackground)

            self._dragging = False
            self._drag_pos = QPoint(0, 0)

            # Edge-resize state
            self._resizing = False
            self._resize_edges = (False, False, False, False)  # L, T, R, B
            self._resize_start_pos = QPoint(0, 0)
            self._resize_start_geom: Optional[QRect] = None
            self.setMouseTracking(True)

            self._paused = False
            self._pending_attachments: List[Path] = []

            # Sessions
            self._sessions: List[Dict[str, Any]] = []
            self._session: Dict[str, Any] = {}

            # File editor
            self._current_path: Optional[Path] = None
            self._editor_dirty = False

            # Threads
            self._live_feed_thread: Optional[LiveFeedThread] = None
            self._active_threads: List[QThread] = []
            self._restart_thread: Optional[RestartWorkerThread] = None
            self._aider_thread: Optional[AiderLaunchThread] = None

            # Splitter state (right panel collapsible)
            self._split: Optional[QSplitter] = None
            self._right_width = 520
            self._right_collapsed = False
            self._anim_timer: Optional[QTimer] = None
            self._anim_steps: List[List[int]] = []

            self._build_ui()
            self._start_timers()
            self._start_live_feed()

            # Sessions load
            self._sessions, self._session = _ensure_default_session(_load_sessions())
            self._render_sessions()
            self._select_session(self._session.get("id", ""))

            # Start on Terminal tab (live work)
            self.tabs.setCurrentIndex(2)

            # Auto-start worker.py in background if not already running
            self._ensure_worker_running()

        # ------------------------------
        # Worker auto-start
        # ------------------------------
        def _ensure_worker_running(self) -> None:
            """Start worker.py as a hidden background process if heartbeat is stale."""
            try:
                hb = _safe_read_json(WORKER_HEARTBEAT_PATH, default={}) or {}
                ts = hb.get("timestamp") or hb.get("ts") or ""
                if ts:
                    try:
                        age = (datetime.now() - datetime.fromisoformat(str(ts))).total_seconds()
                        if age < 60:
                            return  # worker is alive
                    except Exception:
                        pass

                # Prefer pythonw.exe (windowless) over python.exe
                python_exe = Path(sys.executable)
                pythonw = python_exe.parent / "pythonw.exe"
                exe = str(pythonw) if pythonw.exists() else str(python_exe)

                subprocess.Popen(
                    [exe, str(WORKER_PATH)],
                    cwd=str(PROJECT_DIR),
                    creationflags=CREATE_NO_WINDOW,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    close_fds=True,
                )
                self.plan.append_block("Worker", "Started worker.py in background.", accent="#27c93f")
            except Exception as exc:
                self.plan.append_block("Worker", f"Could not auto-start worker: {exc}", accent="#ffbd2e")

        # ------------------------------
        # Window visuals / drag
        # ------------------------------
        def paintEvent(self, event) -> None:
            p = QPainter(self)
            p.setRenderHint(QPainter.Antialiasing)
            full = self.rect()
            margin = 7   # glow thickness (halved from 14 — slimmer blue trim)

            # ── Navy blue glow — radiates outward from the content edge ──────
            # Draw rings from the inner content boundary into the transparent margin.
            content_rect = full.adjusted(margin, margin, -margin, -margin)
            p.setBrush(Qt.NoBrush)
            for i in range(1, margin):
                # Opacity curve: bright near edge, fades toward outer rim
                alpha = int(200 * ((margin - i) / margin) ** 1.6)
                if alpha <= 0:
                    continue
                glow_color = QColor(10, 60, 160, alpha)  # navy #0a3ca0
                pen = QPen(glow_color, 1.2)
                p.setPen(pen)
                r = content_rect.adjusted(-i, -i, i, i)
                radius = 14.0 + i * 0.4
                p.drawRoundedRect(r, radius, radius)

            # ── Solid dark fill for content area ─────────────────────────────
            path = QPainterPath()
            path.addRoundedRect(
                float(content_rect.x()), float(content_rect.y()),
                float(content_rect.width()), float(content_rect.height()),
                14, 14,
            )
            p.fillPath(path, QColor(10, 10, 12))

        _RESIZE_MARGIN = 8  # px from each edge that triggers resize

        def _resize_edges_at(self, pos: QPoint):
            """Return (left, top, right, bottom) bool tuple for resize hit-test."""
            m = self._RESIZE_MARGIN
            x, y, w, h = pos.x(), pos.y(), self.width(), self.height()
            return (x < m, y < m, x > w - m, y > h - m)

        def _cursor_for_edges(self, edges) -> Qt.CursorShape:
            left, top, right, bottom = edges
            if (left and top) or (right and bottom): return Qt.SizeFDiagCursor
            if (right and top) or (left and bottom): return Qt.SizeBDiagCursor
            if left or right:                         return Qt.SizeHorCursor
            if top or bottom:                         return Qt.SizeVerCursor
            return Qt.ArrowCursor

        def mousePressEvent(self, event) -> None:
            if event.button() == Qt.LeftButton:
                pos = event.position().toPoint()
                edges = self._resize_edges_at(pos)
                if any(edges):
                    # Start resize
                    self._resizing = True
                    self._resize_edges = edges
                    self._resize_start_pos = event.globalPosition().toPoint()
                    self._resize_start_geom = self.geometry()
                    self.setCursor(self._cursor_for_edges(edges))
                    event.accept()
                    return
                if pos.y() <= 62:
                    # Title-bar drag
                    self._dragging = True
                    self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
                    event.accept()

        def mouseMoveEvent(self, event) -> None:
            if self._resizing and self._resize_start_geom is not None:
                delta = event.globalPosition().toPoint() - self._resize_start_pos
                dx, dy = delta.x(), delta.y()
                g = self._resize_start_geom
                x, y, w, h = g.x(), g.y(), g.width(), g.height()
                left, top, right, bottom = self._resize_edges
                if left:
                    new_w = max(w - dx, self.minimumWidth())
                    x = g.right() - new_w + 1
                    w = new_w
                if right:
                    w = max(w + dx, self.minimumWidth())
                if top:
                    new_h = max(h - dy, self.minimumHeight())
                    y = g.bottom() - new_h + 1
                    h = new_h
                if bottom:
                    h = max(h + dy, self.minimumHeight())
                self.setGeometry(x, y, w, h)
                event.accept()
                return
            if self._dragging:
                self.move(event.globalPosition().toPoint() - self._drag_pos)
                event.accept()
                return
            # Hover: update cursor to hint at resize directions
            edges = self._resize_edges_at(event.position().toPoint())
            self.setCursor(self._cursor_for_edges(edges))

        def mouseReleaseEvent(self, event) -> None:
            self._dragging = False
            if self._resizing:
                self._resizing = False
                self._resize_start_geom = None
                self.setCursor(Qt.ArrowCursor)

        # ------------------------------
        # UI build
        # ------------------------------
        def resizeEvent(self, event) -> None:
            try:
                if hasattr(self, '_size_grip') and self._size_grip is not None:
                    self._size_grip.move(self.width() - self._size_grip.width() - 8, self.height() - self._size_grip.height() - 8)
            except Exception:
                pass
            return super().resizeEvent(event)

        def _build_ui(self) -> None:
            root = QVBoxLayout(self)
            root.setContentsMargins(14, 14, 14, 14)
            root.setSpacing(10)

            self.surface = QFrame()
            self.surface.setObjectName("Surface")
            root.addWidget(self.surface, 1)

            outer = QVBoxLayout(self.surface)
            outer.setContentsMargins(12, 12, 12, 12)
            outer.setSpacing(10)

            # Top bar
            top = QFrame()
            top.setObjectName("SurfaceRaised")
            top.setFixedHeight(52)
            outer.addWidget(top)

            top_l = QHBoxLayout(top)
            top_l.setContentsMargins(12, 10, 12, 10)
            top_l.setSpacing(10)

            title = QLabel("SurgeApp / Luna Command Center")
            title.setObjectName("Title")
            top_l.addWidget(title)

            top_l.addStretch(1)

            self.phase = QLabel("phase: unknown")
            self.phase.setObjectName("Muted")
            top_l.addWidget(self.phase)

            self.badge = QLabel("● offline")
            self.badge.setObjectName("Muted")
            top_l.addWidget(self.badge)

            # Right panel toggle
            self.btn_toggle_right = QPushButton("⟫")
            self.btn_toggle_right.setObjectName("Ghost")
            self.btn_toggle_right.setFixedSize(36, 30)
            self.btn_toggle_right.clicked.connect(self._toggle_right)
            top_l.addWidget(self.btn_toggle_right)

            self.btn_restart = QPushButton("↺ Restart")
            self.btn_restart.setObjectName("Ghost")
            self.btn_restart.setFixedHeight(30)
            self.btn_restart.setToolTip("Kill worker, clear lock, and start fresh (no window close needed)")
            self.btn_restart.clicked.connect(self._restart_worker)
            top_l.addWidget(self.btn_restart)

            self.btn_aider = QPushButton("⚡ Aider")
            self.btn_aider.setObjectName("Ghost")
            self.btn_aider.setFixedHeight(30)
            self.btn_aider.setToolTip("Launch Aider coding session in a new terminal")
            self.btn_aider.clicked.connect(self._launch_aider)
            top_l.addWidget(self.btn_aider)

            self.btn_pause = QPushButton("Pause")
            self.btn_pause.setObjectName("Primary")
            self.btn_pause.setFixedHeight(30)
            self.btn_pause.clicked.connect(self._toggle_pause)
            top_l.addWidget(self.btn_pause)

            # Window controls — Windows-style right edge
            _sep = QFrame()
            _sep.setFrameShape(QFrame.VLine)
            _sep.setFixedWidth(1)
            _sep.setStyleSheet("color: #2a2a30;")
            top_l.addWidget(_sep)

            self.btn_min = QPushButton("—")
            self.btn_min.setFixedSize(34, 30)
            self.btn_min.clicked.connect(self.showMinimized)
            top_l.addWidget(self.btn_min)

            self.btn_max = QPushButton("▢")
            self.btn_max.setFixedSize(34, 30)
            self.btn_max.clicked.connect(self._toggle_max_restore)
            top_l.addWidget(self.btn_max)

            self.btn_close = QPushButton("✕")
            self.btn_close.setFixedSize(34, 30)
            self.btn_close.setStyleSheet(
                "QPushButton { color: #e9e9eb; } QPushButton:hover { background: #c0392b; color: #ffffff; border-color: #c0392b; }"
            )
            self.btn_close.clicked.connect(self.close)
            top_l.addWidget(self.btn_close)

            # Splitter (left / center / right)
            self._split = QSplitter(Qt.Horizontal)
            outer.addWidget(self._split, 1)

            # Left: sessions
            self.sidebar = QFrame()
            self.sidebar.setObjectName("SurfaceRaised")
            self.sidebar.setMinimumWidth(240)
            self.sidebar.setMaximumWidth(380)
            self._split.addWidget(self.sidebar)

            sb = QVBoxLayout(self.sidebar)
            sb.setContentsMargins(12, 12, 12, 12)
            sb.setSpacing(10)

            sb_title = QLabel("Sessions")
            sb_title.setObjectName("Title")
            sb.addWidget(sb_title)

            btn_row = QHBoxLayout()
            self.btn_new = QPushButton("+ New")
            self.btn_new.clicked.connect(self._new_session)
            btn_row.addWidget(self.btn_new)

            self.btn_rename = QPushButton("✏")
            self.btn_rename.setObjectName("Ghost")
            self.btn_rename.setFixedWidth(36)
            self.btn_rename.setToolTip("Rename selected session (or double-click)")
            self.btn_rename.clicked.connect(self._rename_selected_session)
            btn_row.addWidget(self.btn_rename)

            self.btn_delete_session = QPushButton("✕")
            self.btn_delete_session.setObjectName("Ghost")
            self.btn_delete_session.setFixedWidth(36)
            self.btn_delete_session.setToolTip("Delete selected session")
            self.btn_delete_session.setStyleSheet(
                "QPushButton { color: #a6a6ad; } QPushButton:hover { background: #c0392b; color: #ffffff; border-color: #c0392b; }"
            )
            self.btn_delete_session.clicked.connect(self._delete_selected_session)
            btn_row.addWidget(self.btn_delete_session)

            sb.addLayout(btn_row)

            self.sessions_list = QListWidget()
            self.sessions_list.itemClicked.connect(self._on_session_clicked)
            self.sessions_list.itemChanged.connect(self._on_session_renamed)
            self.sessions_list.itemDoubleClicked.connect(self.sessions_list.editItem)
            self.sessions_list.setToolTip("Double-click to rename · select then ✕ to delete")
            sb.addWidget(self.sessions_list, 1)

            # Center: chat
            self.center = QFrame()
            self.center.setObjectName("SurfaceRaised")
            self._split.addWidget(self.center)

            cc = QVBoxLayout(self.center)
            cc.setContentsMargins(12, 12, 12, 12)
            cc.setSpacing(10)

            self.chat = RichText()
            cc.addWidget(self.chat, 1)

            # Input row with attachment button
            input_row = QHBoxLayout()
            self.input = AttachLineEdit()
            self.input.setPlaceholderText("Type /commands, plain text, or !shell")
            self.input.returnPressed.connect(self._on_send)
            self.input.files_dropped.connect(self._on_files_dropped)
            input_row.addWidget(self.input, 1)

            self.btn_attach = QPushButton("📎")
            self.btn_attach.setObjectName("Ghost")
            self.btn_attach.setFixedSize(40, 38)
            self.btn_attach.clicked.connect(self._pick_attachments)
            input_row.addWidget(self.btn_attach)

            self.btn_send = QPushButton("Send")
            self.btn_send.setObjectName("Primary")
            self.btn_send.clicked.connect(self._on_send)
            input_row.addWidget(self.btn_send)
            cc.addLayout(input_row)

            # Attachments hint line (hidden unless used)
            self.attach_hint = QLabel("")
            self.attach_hint.setObjectName("Muted")
            self.attach_hint.setWordWrap(True)
            self.attach_hint.setVisible(False)
            cc.addWidget(self.attach_hint)

            # Right: Inspector
            self.inspector = QFrame()
            self.inspector.setObjectName("SurfaceRaised")
            self.inspector.setMinimumWidth(340)
            self.inspector.setMaximumWidth(820)
            self._split.addWidget(self.inspector)

            ri = QVBoxLayout(self.inspector)
            ri.setContentsMargins(12, 12, 12, 12)
            ri.setSpacing(10)

            # Inspector header with Hide
            hdr = QHBoxLayout()
            lbl = QLabel("Inspector")
            lbl.setObjectName("Title")
            hdr.addWidget(lbl)
            hdr.addStretch(1)
            btn_hide = QPushButton("Hide")
            btn_hide.setObjectName("Ghost")
            btn_hide.clicked.connect(self._collapse_right)
            hdr.addWidget(btn_hide)
            ri.addLayout(hdr)

            self.tabs = QTabWidget()
            ri.addWidget(self.tabs, 1)

            self.preview  = RichText()
            self.diff = RichText()
            self.terminal = RichText()   # live work feed
            self.files = QWidget()       # editable files tab
            self.queue_tab = QWidget()   # queue hub
            self.tasks = QWidget()
            self.plan = RichText()       # system/control notes live here

            self.tabs.addTab(self.preview, "Preview")
            self.tabs.addTab(self.diff, "Diff")
            self.tabs.addTab(self.terminal, "Terminal")
            self.tabs.addTab(self.queue_tab, "⚡ Queue")
            self.tabs.addTab(self.files, "Files")
            self.tabs.addTab(self.tasks, "Tasks")
            self.tabs.addTab(self.plan, "Plan")

            self._build_files_tab()
            self._build_tasks_tab()
            self._build_queue_tab()

            # Initial sizes
            self._split.setSizes([300, 820, self._right_width])

            # Clean initial chat
            self.chat.append_ai("Online. Tell me what to do.")

            # Inspector help
            self.plan.append_block(
                "Controls",
                "Right toggle ⟫ hides/shows the Inspector.\n"
                "Files tab is editable: open a file, edit, then Save.\n"
                "Terminal tab is the live work feed (what Luna is doing).",
                accent="#a6a6ad",
            )

        # ------------------------------
        # Sessions UI
        # ------------------------------
        def _render_sessions(self) -> None:
            self.sessions_list.blockSignals(True)
            self.sessions_list.clear()
            for s in self._sessions:
                name = str(s.get("name") or s.get("id") or "Session")
                it = QListWidgetItem(name)
                it.setData(Qt.UserRole, str(s.get("id") or ""))
                it.setFlags(it.flags() | Qt.ItemIsEditable)
                self.sessions_list.addItem(it)
            self.sessions_list.blockSignals(False)

        def _on_session_clicked(self, item: QListWidgetItem) -> None:
            sid = str(item.data(Qt.UserRole) or "")
            self._select_session(sid)

        def _select_session(self, session_id: str) -> None:
            # Find session
            found = None
            for s in self._sessions:
                if str(s.get("id")) == str(session_id):
                    found = s
                    break
            if not found:
                return
            self._session = found
            found["last_used"] = _now_iso()
            _save_sessions(self._sessions)

            # Render chat history for session
            self.chat.clear()
            msgs = _session_messages(str(found.get("id")), limit=700)
            if not msgs:
                self.chat.append_ai("Ready. What's next?")
                return
            for role, text in msgs:
                if role == "user":
                    self.chat.append_user(text)
                else:
                    self.chat.append_ai(text)

        def _new_session(self) -> None:
            sid = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
            name = f"Session {len(self._sessions) + 1}"
            s = {"id": sid, "name": name, "created_at": _now_iso(), "last_used": _now_iso()}
            self._sessions.insert(0, s)
            _save_sessions(self._sessions)
            self._render_sessions()
            self._select_session(sid)
            # Put any notice on right panel only
            self.plan.append_block("Session", f"Opened: {name}", accent="#5b8cff")

        def _rename_selected_session(self) -> None:
            item = self.sessions_list.currentItem()
            if not item:
                return
            self.sessions_list.editItem(item)

        def _on_session_renamed(self, item: QListWidgetItem) -> None:
            sid = str(item.data(Qt.UserRole) or "")
            new_name = str(item.text() or "").strip()
            if not sid or not new_name:
                return
            for s in self._sessions:
                if str(s.get("id")) == sid:
                    s["name"] = new_name[:80]
                    s["last_used"] = _now_iso()
                    break
            _save_sessions(self._sessions)

        def _delete_selected_session(self) -> None:
            item = self.sessions_list.currentItem()
            if not item:
                return
            sid = str(item.data(Qt.UserRole) or "")
            if not sid:
                return
            name = str(item.text() or sid)
            # Keep at least one session
            if len(self._sessions) <= 1:
                self.plan.append_block("Sessions", "Cannot delete the last session.", accent="#ffbd2e")
                return
            self._sessions = [s for s in self._sessions if str(s.get("id")) != sid]
            _save_sessions(self._sessions)
            self._render_sessions()
            # Switch to first remaining session
            remaining = self._sessions[0] if self._sessions else {}
            new_sid = str(remaining.get("id") or "")
            if new_sid:
                self._select_session(new_sid)
            self.plan.append_block("Sessions", f"Deleted: {name}", accent="#ff5f56")

        # ------------------------------
        # Right panel collapse (smooth-ish)
        # ------------------------------
        def _toggle_right(self) -> None:
            if self._right_collapsed:
                self._expand_right()
            else:
                self._collapse_right()

        def _collapse_right(self) -> None:
            if not self._split:
                return
            sizes = self._split.sizes()
            self._right_width = max(340, sizes[2])
            self._right_collapsed = True
            self.btn_toggle_right.setText("⟪")
            self._animate_split([sizes[0], sizes[1] + sizes[2], 0])

        def _expand_right(self) -> None:
            if not self._split:
                return
            sizes = self._split.sizes()
            self._right_collapsed = False
            self.btn_toggle_right.setText("⟫")
            right = max(380, self._right_width)
            center = max(520, sizes[1] - right)
            self._animate_split([sizes[0], center, right])

        def _animate_split(self, target_sizes: List[int]) -> None:
            if not self._split:
                return
            if self._anim_timer and self._anim_timer.isActive():
                self._anim_timer.stop()

            current = self._split.sizes()
            steps = 10
            frames: List[List[int]] = []
            for i in range(1, steps + 1):
                frame = []
                for c, t in zip(current, target_sizes):
                    frame.append(int(c + (t - c) * (i / steps)))
                frames.append(frame)
            self._anim_steps = frames

            self._anim_timer = QTimer(self)
            self._anim_timer.timeout.connect(self._anim_tick)
            self._anim_timer.start(15)

        def _anim_tick(self) -> None:
            if not self._split or not self._anim_steps:
                if self._anim_timer:
                    self._anim_timer.stop()
                return
            frame = self._anim_steps.pop(0)
            self._split.setSizes(frame)
            if not self._anim_steps and self._anim_timer:
                self._anim_timer.stop()

        def _toggle_max_restore(self) -> None:
            if self.isMaximized():
                self.showNormal()
            else:
                self.showMaximized()

        # ------------------------------
        # Control plane
        # ------------------------------
        def _restart_worker(self) -> None:
            if self._restart_thread and self._restart_thread.isRunning():
                return
            self.btn_restart.setText("↺ …")
            self.btn_restart.setEnabled(False)
            self.badge.setText("● restarting")
            self.badge.setStyleSheet("color: #ffbd2e;")
            self.plan.append_block("Restart", "Stopping worker and clearing lock…", accent="#ffbd2e")
            t = RestartWorkerThread()
            t.restart_done.connect(self._on_restart_done)
            self._restart_thread = t
            t.start()

        @Slot(str)
        def _on_restart_done(self, msg: str) -> None:
            self.btn_restart.setText("↺ Restart")
            self.btn_restart.setEnabled(True)
            self.plan.append_block("Restart", msg, accent="#27c93f")

        def _launch_aider(self) -> None:
            if self._aider_thread and self._aider_thread.isRunning():
                return
            self.btn_aider.setText("⚡ …")
            self.btn_aider.setEnabled(False)
            t = AiderLaunchThread()
            t.aider_done.connect(self._on_aider_done)
            self._aider_thread = t
            t.start()

        @Slot(int)
        def _on_aider_done(self, rc: int) -> None:
            self.btn_aider.setText("⚡ Aider")
            self.btn_aider.setEnabled(True)

        def _toggle_pause(self) -> None:
            if self._paused:
                self._resume()
            else:
                self._pause()

        def _pause(self) -> None:
            self._paused = True
            try:
                KILL_SWITCH_PATH.touch()
            except Exception:
                pass
            self.btn_pause.setText("Resume")
            self.badge.setText("● paused")
            self.badge.setStyleSheet("color: #ffbd2e;")
            self.plan.append_block("Control", "Paused. Autonomy should stop at the next loop checkpoint.", accent="#ffbd2e")

        def _resume(self) -> None:
            self._paused = False
            try:
                if KILL_SWITCH_PATH.exists():
                    KILL_SWITCH_PATH.unlink()
            except Exception:
                pass
            self.btn_pause.setText("Pause")
            self.plan.append_block("Control", "Resumed.", accent="#27c93f")

        # ------------------------------
        # Attachments
        # ------------------------------
        def _pick_attachments(self) -> None:
            try:
                files, _ = QFileDialog.getOpenFileNames(self, "Attach files", str(PROJECT_DIR))
            except Exception:
                files = []
            if files:
                self._ingest_attachments(files)

        @Slot(list)
        def _on_files_dropped(self, paths: List[str]) -> None:
            self._ingest_attachments(paths)

        def _ingest_attachments(self, paths: List[str]) -> None:
            ensure_layout()
            copied: List[Path] = []
            for p in paths:
                src = Path(p)
                if not src.exists() or not src.is_file():
                    continue
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = src.name.replace(" ", "_")
                dest = UPLOADS_DIR / f"{stamp}_{uuid.uuid4().hex[:6]}_{safe_name}"
                try:
                    shutil.copy2(str(src), str(dest))
                    copied.append(dest)
                except Exception:
                    continue

            if not copied:
                return

            self._pending_attachments.extend(copied)

            rels = []
            for p in self._pending_attachments[-6:]:
                try:
                    rels.append(str(p.relative_to(PROJECT_DIR)))
                except Exception:
                    rels.append(str(p))
            suffix = " +more…" if len(self._pending_attachments) > 6 else ""
            self.attach_hint.setText("Attached: " + ", ".join(rels) + suffix)
            self.attach_hint.setVisible(True)

            self.plan.append_block("Attachments", "\n".join(f"- {r}" for r in rels) + (("\n…") if suffix else ""), accent="#5b8cff")

        # ------------------------------
        # Sending
        # ------------------------------
        @Slot()
        def _on_send(self) -> None:
            raw = (self.input.text() or "").strip()
            if not raw and not self._pending_attachments:
                return

            low = raw.lower().strip().rstrip("!.") if raw else ""

            if low in _STOP_WORDS:
                self.input.clear()
                self._pause()
                return
            if low in _RESUME_WORDS:
                self.input.clear()
                self._resume()
                return
            if low in ("exit", "quit"):
                self.close()
                return


            # --- Continues-Update commands -------------------------------------
            raw_lower = raw.lower().strip()

            # Stop continues update — all aliases + fuzzy ("stop" AND "update"/"continu")
            _is_stop_cu = raw_lower in (
                "stop continues update", "stop continuous update", "stop updates",
                "stop update", "stop all updates", "none stop update", "nonstop update",
                "/continuesupdate stop", "/cu stop", "/stop update", "/stopupdate",
            ) or (
                "stop" in raw_lower and (
                    "update" in raw_lower or "continu" in raw_lower
                )
            )
            if _is_stop_cu:
                self.input.clear()
                _cu_stop = MEMORY_DIR / "continues_update.stop"
                try:
                    _cu_stop.parent.mkdir(parents=True, exist_ok=True)
                    _cu_stop.write_text(_now_iso(), encoding="utf-8")
                    self._append_luna_msg(
                        "⏹ **Stop signal sent.**\n"
                        "Luna → Aider Bridge → Worker: stop flag written to `memory/continues_update.stop`.\n"
                        "The active cycle will finish its current step, then everything halts within 2–5s."
                    )
                except Exception as exc:
                    self._append_luna_msg(f"⚠ Could not write stop flag: {exc}")
                return

            # Start continues update — exact list + fuzzy ("continu" AND "update")
            _is_start_cu = raw_lower in (
                "continues update", "update continuous", "continuous update",
                "none stop update", "nonstop update", "non stop update",
                "start update", "start updates", "start continuous update",
                "/continuesupdate", "/cu start", "/cu",
            ) or (
                "continu" in raw_lower and "update" in raw_lower
                and "stop" not in raw_lower
            )
            if _is_start_cu:
                self.input.clear()
                # Remove stale stop flag first
                _cu_stop = MEMORY_DIR / "continues_update.stop"
                try:
                    _cu_stop.unlink(missing_ok=True)
                except Exception:
                    pass
                # Find pythonw
                def _ui_pythonw() -> str:
                    exe = str(sys.executable)
                    if exe.lower().endswith("python.exe"):
                        cand = exe[:-10] + "pythonw.exe"
                        if Path(cand).exists():
                            return cand
                    return exe
                # Guard: check if CU loop already running (avoid duplicate processes)
                _cu_already = False
                try:
                    _wmic = subprocess.run(
                        ["wmic", "process", "get", "CommandLine"],
                        capture_output=True, text=True, timeout=5,
                        creationflags=CREATE_NO_WINDOW,
                    )
                    _cu_already = "--continues-update-start" in (_wmic.stdout or "")
                except Exception:
                    pass
                if _cu_already:
                    self._append_luna_msg("▶ Continues-update loop is already running. Watch Inspector → Terminal for live progress.")
                else:
                    try:
                        subprocess.Popen(
                            [_ui_pythonw(), str(WORKER_PATH), "--continues-update-start"],
                            cwd=str(PROJECT_DIR),
                            creationflags=CREATE_NO_WINDOW,
                        )
                        self._append_luna_msg("▶ Continues-update loop started. Watch Inspector → Terminal for live progress.")
                    except Exception as exc:
                        self._append_luna_msg(f"⚠ Could not start continues-update: {exc}")
                return

            # Check for upgradeable packages
            if raw_lower in (
                "check for updates", "check updates", "/check updates", "/checkupdates",
                "what can we update", "what can we upgrade",
            ):
                self.input.clear()
                self._append_luna_msg("🔍 Checking for outdated packages…")
                def _do_check():
                    try:
                        r = subprocess.run(
                            [sys.executable, "-m", "pip", "list", "--outdated", "--format=columns"],
                            capture_output=True, text=True, timeout=30,
                            creationflags=CREATE_NO_WINDOW,
                        )
                        out = (r.stdout or "").strip()
                        if not out:
                            return "✅ All packages are up to date."
                        lines = out.splitlines()
                        # top 20 rows
                        shown = "\n".join(lines[:22])
                        extra = f"\n…and {len(lines)-22} more" if len(lines) > 22 else ""
                        return f"📦 **Outdated packages:**\n```\n{shown}{extra}\n```\nRun `/aider worker: upgrade <package>` to update one."
                    except Exception as exc:
                        return f"⚠ pip check failed: {exc}"
                import threading
                threading.Thread(target=lambda: self._append_luna_msg(_do_check()), daemon=True).start()
                return

            # Show nightly updates
            if raw_lower in ("what are your updates", "/updates", "/cu status"):
                self.input.clear()
                _nu = MEMORY_DIR / "nightly_updates.md"
                if _nu.exists():
                    try:
                        content = _nu.read_text(encoding="utf-8", errors="replace")
                        self._append_luna_msg(
                            "📋 **Nightly Updates**\n\n" + (content[-4000:] if len(content) > 4000 else content)
                        )
                    except Exception as exc:
                        self._append_luna_msg(f"⚠ Could not read nightly_updates.md: {exc}")
                else:
                    self._append_luna_msg("No nightly_updates.md yet. Start continues-update first.")
                return

            # --- Aider Bridge / Self-Upgrade commands --------------------------
            if raw.lower().startswith("/selfupgrade"):
                self.input.clear()
                tid = submit_self_upgrade_task(str(self._session.get("id") or ""))
                self.plan.append_block("Self-Upgrade", f"Queued worker self-upgrade: {tid}", accent="#5b8cff")
                self._refresh_tasks_list()
                return

            if raw.lower().startswith("/aider"):
                args = raw[len("/aider"):].strip()
                self.input.clear()

                apply_now = False
                if args.lower().startswith("apply "):
                    apply_now = True
                    args = args[6:].strip()

                targets = ["worker.py"]
                if args.lower().startswith("ui:"):
                    targets = ["SurgeApp_Claude_Terminal.py"]
                    args = args[3:].strip()
                elif args.lower().startswith("worker:"):
                    targets = ["worker.py"]
                    args = args[7:].strip()

                if not args:
                    self.plan.append_block("Aider", "Usage: /aider [apply] [worker:|ui:] <instructions>", accent="#ffbd2e")
                    return

                tid = submit_aider_job(args, str(self._session.get("id") or ""), target_files=targets, apply_on_pass=apply_now)
                self.plan.append_block("Aider Job", f"Queued Aider Bridge job: {tid}\nTargets: {', '.join(targets)}\nApply on pass: {apply_now}", accent="#5b8cff")
                self._refresh_tasks_list()
                return
            # -------------------------------------------------------------------

            # Shell passthrough
            if raw.startswith("!"):
                cmd = raw[1:].strip()
                self.input.clear()
                if cmd:
                    self.plan.append_block("Shell", f"$ {cmd}", accent="#a6a6ad")
                    t = ShellExecutionThread(cmd)
                    t.shell_result.connect(self._on_shell_result)
                    self._active_threads.append(t)
                    t.start()
                return

            if self._paused:
                self.plan.append_block("Paused", "Resume before sending tasks.", accent="#ffbd2e")
                return

            prompt = raw
            attach_for_task = list(self._pending_attachments)
            if attach_for_task:
                rels = []
                for p in attach_for_task:
                    try:
                        rels.append(str(p.relative_to(PROJECT_DIR)))
                    except Exception:
                        rels.append(str(p))
                prompt = (prompt + "\n\n[ATTACHMENTS]\n" + "\n".join(f"- {r}" for r in rels)).strip()

            # Clear input UI
            self.input.clear()
            self.attach_hint.setVisible(False)
            self.attach_hint.setText("")

            # Chat: only You/Luna
            if raw:
                self.chat.append_user(raw)
            write_history({"event": "user_input", "raw": raw, "session_id": self._session.get("id", "")})

            # Auto-name session if still default
            try:
                sname = str(self._session.get("name") or "")
                if sname.lower().startswith("session ") and raw:
                    new_name = raw.strip()
                    if len(new_name) > 32:
                        new_name = new_name[:32].rstrip() + "…"
                    self._session["name"] = new_name
                    for s in self._sessions:
                        if s.get("id") == self._session.get("id"):
                            s["name"] = new_name
                            break
                    _save_sessions(self._sessions)
                    self._render_sessions()
            except Exception:
                pass

            # Queue task
            task_id = submit_worker_task(prompt, str(self._session.get("id") or ""), attach_for_task)
            self.plan.append_block("Queued", f"task_id={task_id}", accent="#a6a6ad")
            self._refresh_tasks_list()

            # Clear attachments after queue
            self._pending_attachments = []

            t = WorkerWaitThread(task_id)
            t.task_finished.connect(self._on_task_done)
            t.task_error.connect(self._on_task_error)
            self._active_threads.append(t)
            t.start()

        @Slot(str, str, int)
        def _on_shell_result(self, stdout: str, stderr: str, rc: int) -> None:
            if stdout.strip():
                self.plan.append_block(f"stdout (rc={rc})", stdout.strip()[:12000], accent="#27c93f" if rc == 0 else "#ffbd2e")
            if stderr.strip():
                self.plan.append_block("stderr", stderr.strip()[:12000], accent="#ff5f56")
            self._cleanup_threads()

        @Slot(str, str, str, str)
        def _on_task_done(self, status: str, task_id: str, chat_text: str, detail: str) -> None:
            if status == "done":
                text = _sanitize_luna_text(chat_text)
                self.chat.append_ai(text if text else "Done.")
                write_history({"event": "luna_response", "task_id": task_id, "text": text[:8000], "session_id": self._session.get("id", "")})

                # Full detail stays on right panel
                if detail and detail.strip():
                    self.preview.clear()
                    self.preview.append_block("Details", detail[:200000], accent="#a6a6ad")
            else:
                self.chat.append_error(chat_text)

            self._refresh_tasks_list()
            self._update_plan_snapshot()
            self._cleanup_threads()

        @Slot(str)
        def _on_task_error(self, err: str) -> None:
            self.chat.append_error(err)
            self.plan.append_block("WorkerWaitThread", err, accent="#ff5f56")
            self._refresh_tasks_list()
            self._cleanup_threads()

        def _cleanup_threads(self) -> None:
            self._active_threads = [t for t in self._active_threads if t.isRunning()]

        # ------------------------------
        # Files tab (editable)
        # ------------------------------
        def _build_files_tab(self) -> None:
            lay = QVBoxLayout(self.files)
            lay.setContentsMargins(0, 0, 0, 0)
            lay.setSpacing(10)

            # Filter row
            row = QHBoxLayout()
            self.file_filter = QLineEdit()
            self.file_filter.setPlaceholderText("Filter files… (?text searches contents)")
            self.file_filter.textChanged.connect(self._refresh_files_list)
            row.addWidget(self.file_filter, 1)

            self.btn_files_refresh = QPushButton("Refresh")
            self.btn_files_refresh.setObjectName("Ghost")
            self.btn_files_refresh.clicked.connect(self._refresh_files_list)
            row.addWidget(self.btn_files_refresh)

            lay.addLayout(row)

            # Split list + editor
            inner = QSplitter(Qt.Vertical)
            lay.addWidget(inner, 1)

            self.file_list = QListWidget()
            self.file_list.itemClicked.connect(self._open_selected_file)
            inner.addWidget(self.file_list)

            editor_wrap = QFrame()
            editor_wrap.setObjectName("SurfaceRaised")
            inner.addWidget(editor_wrap)

            ew = QVBoxLayout(editor_wrap)
            ew.setContentsMargins(10, 10, 10, 10)
            ew.setSpacing(10)

            top = QHBoxLayout()
            self.file_path_lbl = QLabel("No file opened")
            self.file_path_lbl.setObjectName("Muted")
            top.addWidget(self.file_path_lbl, 1)

            self.btn_revert = QPushButton("Revert")
            self.btn_revert.setObjectName("Ghost")
            self.btn_revert.clicked.connect(self._revert_file)
            top.addWidget(self.btn_revert)

            self.btn_save = QPushButton("Save")
            self.btn_save.setObjectName("Primary")
            self.btn_save.clicked.connect(self._save_file)
            top.addWidget(self.btn_save)

            ew.addLayout(top)

            self.editor = QPlainTextEdit()
            self.editor.textChanged.connect(self._on_editor_changed)
            ew.addWidget(self.editor, 1)

            status_row = QHBoxLayout()
            self.editor_status = QLabel("")
            self.editor_status.setObjectName("Muted")
            status_row.addWidget(self.editor_status, 1)
            ew.addLayout(status_row)

            inner.setSizes([260, 520])

            self._refresh_files_list()

        def _refresh_files_list(self) -> None:
            items = _project_files(self.file_filter.text() if hasattr(self, "file_filter") else "")
            self.file_list.clear()
            for p in items:
                rel = str(p.relative_to(PROJECT_DIR))
                it = QListWidgetItem(rel)
                it.setData(Qt.UserRole, str(p))
                self.file_list.addItem(it)

        def _open_selected_file(self, item: QListWidgetItem) -> None:
            path = Path(str(item.data(Qt.UserRole) or ""))
            self._open_file(path)

        def _open_file(self, path: Path) -> None:
            if not path.exists() or not path.is_file():
                return
            self._current_path = path
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception as exc:
                self.plan.append_block("Files", f"Failed to read {path}: {exc}", accent="#ff5f56")
                return

            self.editor.blockSignals(True)
            self.editor.setPlainText(text)
            self.editor.blockSignals(False)

            self._editor_dirty = False
            self._update_editor_header()

            # Update other tabs
            self._update_preview_for_path(path, text)
            self._update_diff_for_path(path, disk_text=text)

        def _update_editor_header(self) -> None:
            if not self._current_path:
                self.file_path_lbl.setText("No file opened")
                self.editor_status.setText("")
                return
            rel = str(self._current_path.relative_to(PROJECT_DIR))
            dirty = " • modified" if self._editor_dirty else ""
            self.file_path_lbl.setText(rel + dirty)
            self.editor_status.setText(f"{len(self.editor.toPlainText())} chars")

        def _on_editor_changed(self) -> None:
            if self._current_path is None:
                return
            self._editor_dirty = True
            self._update_editor_header()

        def _save_file(self) -> None:
            if not self._current_path:
                return
            try:
                self._current_path.write_text(self.editor.toPlainText(), encoding="utf-8")
                self._editor_dirty = False
                self._update_editor_header()
                self.plan.append_block("Files", f"Saved: {self._current_path}", accent="#27c93f")
                # Refresh diff after save
                self._update_diff_for_path(self._current_path)
            except Exception as exc:
                self.plan.append_block("Files", f"Save failed: {exc}", accent="#ff5f56")

        def _revert_file(self) -> None:
            if not self._current_path:
                return
            self._open_file(self._current_path)
            self.plan.append_block("Files", f"Reverted: {self._current_path}", accent="#ffbd2e")

        # ------------------------------
        # Preview + Diff tabs
        # ------------------------------
        def _update_preview_for_path(self, path: Path, text: Optional[str] = None) -> None:
            try:
                if text is None:
                    text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                text = ""
            self.preview.clear()
            rel = str(path.relative_to(PROJECT_DIR))
            self.preview.append_block(rel, (text or "")[:200000], accent="#5b8cff")

        def _update_diff_for_path(self, path: Path, disk_text: Optional[str] = None) -> None:
            self.diff.clear()
            rel = str(path.relative_to(PROJECT_DIR))

            # If editor is open and dirty on this file, show unsaved diff vs disk
            if self._current_path and self._current_path == path and self._editor_dirty:
                try:
                    disk = disk_text if disk_text is not None else path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    disk = ""
                edited = self.editor.toPlainText()
                a = disk.splitlines(keepends=True)
                b = edited.splitlines(keepends=True)
                ud = difflib.unified_diff(a, b, fromfile=f"{rel} (disk)", tofile=f"{rel} (edited)", lineterm="")
                diff_text = "".join(ud).strip()
                self.diff.append_block(f"Unsaved diff: {rel}", diff_text[:200000] or "(no changes)", accent="#ffbd2e")
                return

            staged = _best_effort_staged_diff(path)
            if staged:
                self.diff.append_block(f"Staged diff: {rel}", staged[:200000], accent="#ffbd2e")
                return

            gd = _run_git_diff(rel)
            if gd:
                self.diff.append_block(f"git diff: {rel}", gd[:200000], accent="#ffbd2e")
                return

            self.diff.append_block("Diff", "No diff found.", accent="#a6a6ad")

        # ------------------------------
        # Tasks tab
        # ------------------------------
        def _build_tasks_tab(self) -> None:
            lay = QVBoxLayout(self.tasks)
            lay.setContentsMargins(0, 0, 0, 0)
            lay.setSpacing(10)

            hdr = QHBoxLayout()
            self.tasks_filter = QLineEdit()
            self.tasks_filter.setPlaceholderText("Filter tasks…")
            self.tasks_filter.textChanged.connect(self._refresh_tasks_list)
            hdr.addWidget(self.tasks_filter, 1)

            btn = QPushButton("Refresh")
            btn.setObjectName("Ghost")
            btn.clicked.connect(self._refresh_tasks_list)
            hdr.addWidget(btn)
            lay.addLayout(hdr)

            self.task_list = QListWidget()
            self.task_list.itemClicked.connect(self._select_task)
            lay.addWidget(self.task_list, 1)

            self._refresh_tasks_list()

        def _refresh_tasks_list(self) -> None:
            q = (self.tasks_filter.text() or "").strip().lower()
            rows: List[Tuple[str, str, Path]] = []

            def _collect(folder: Path, status: str) -> None:
                try:
                    for p in folder.glob("*.json"):
                        tid = p.stem
                        if q and q not in tid.lower():
                            continue
                        rows.append((f"{status:6}  {tid}", status, p))
                except Exception:
                    return

            _collect(ACTIVE_DIR, "active")
            _collect(AIDER_ACTIVE_DIR, "aider-active")
            _collect(DONE_DIR, "done")
            _collect(AIDER_DONE_DIR, "aider-done")
            _collect(FAILED_DIR, "failed")
            _collect(AIDER_FAILED_DIR, "aider-failed")

            try:
                rows.sort(key=lambda r: r[2].stat().st_mtime if r[2].exists() else 0.0, reverse=True)
            except Exception:
                pass

            self.task_list.clear()
            for label, status, p in rows[:500]:
                it = QListWidgetItem(label)
                it.setData(Qt.UserRole, str(p))
                self.task_list.addItem(it)

        def _select_task(self, item: QListWidgetItem) -> None:
            p = Path(str(item.data(Qt.UserRole) or ""))
            if not p.exists():
                return
            payload = _safe_read_json(p, default={}) or {}
            tid = str(payload.get("task_id") or payload.get("id") or p.stem)

            # Show task payload and best guess details in Preview
            self.preview.clear()
            self.preview.append_block("Task", json.dumps(payload, indent=2, ensure_ascii=False)[:200000], accent="#a6a6ad")

            if (DONE_DIR / f"{tid}.json").exists():
                chat_text, detail = _resolve_done_payload(tid)
                self.preview.append_block("Response", chat_text, accent="#27c93f")
                if detail:
                    self.preview.append_block("Detail", detail[:200000], accent="#a6a6ad")
            elif (FAILED_DIR / f"{tid}.json").exists():
                res = _safe_read_json(FAILED_DIR / f"{tid}.json", default={}) or {}
                self.preview.append_block("Failed", str(res.get("error") or res)[:200000], accent="#ff5f56")

        # ──────────────────────────────────────────────────────────────
        # Queue Hub tab
        # ──────────────────────────────────────────────────────────────
        def _build_queue_tab(self) -> None:
            lay = QVBoxLayout(self.queue_tab)
            lay.setContentsMargins(0, 6, 0, 0)
            lay.setSpacing(6)

            # ── Stats pills ───────────────────────────────────────────
            stats_row = QHBoxLayout()
            stats_row.setSpacing(6)

            _pill_css = (
                "font-weight:600; font-size:11px; padding:3px 10px;"
                "border-radius:10px; border:1px solid #2a2a30;"
            )
            self._q_lbl_queued  = QLabel("● 0 Queued")
            self._q_lbl_running = QLabel("⚡ 0 Running")
            self._q_lbl_failed  = QLabel("✕ 0 Failed")
            self._q_lbl_done    = QLabel("✓ 0 Done")
            for lbl, col in (
                (self._q_lbl_queued,  "#4a9eff"),
                (self._q_lbl_running, "#f5a623"),
                (self._q_lbl_failed,  "#ff5f56"),
                (self._q_lbl_done,    "#27c93f"),
            ):
                lbl.setStyleSheet(f"color:{col}; background:#1a1a1e; {_pill_css}")
                stats_row.addWidget(lbl)
            stats_row.addStretch(1)
            lay.addLayout(stats_row)

            # ── Controls ─────────────────────────────────────────────
            ctrl = QHBoxLayout()
            ctrl.setSpacing(6)

            self._q_filter = QLineEdit()
            self._q_filter.setPlaceholderText("Filter jobs…")
            self._q_filter.textChanged.connect(self._refresh_queue)
            ctrl.addWidget(self._q_filter, 1)

            self._q_flush_btn = QPushButton("🗑 Flush Active")
            self._q_flush_btn.setObjectName("Ghost")
            self._q_flush_btn.setToolTip(
                "Move all queued/active aider jobs to failed.\n"
                "Click once to arm, click again within 3s to confirm."
            )
            self._q_flush_btn.clicked.connect(self._queue_flush_click)
            self._q_flush_armed = False
            self._q_flush_timer = QTimer(self)
            self._q_flush_timer.setSingleShot(True)
            self._q_flush_timer.timeout.connect(self._queue_flush_disarm)
            ctrl.addWidget(self._q_flush_btn)

            btn_refresh = QPushButton("↺")
            btn_refresh.setObjectName("Ghost")
            btn_refresh.setFixedWidth(34)
            btn_refresh.setToolTip("Refresh queue")
            btn_refresh.clicked.connect(self._refresh_queue)
            ctrl.addWidget(btn_refresh)

            lay.addLayout(ctrl)

            # ── Job list ──────────────────────────────────────────────
            self._q_list = QListWidget()
            self._q_list.setStyleSheet(
                "QListWidget::item { padding: 5px 8px; border-bottom: 1px solid #1e1e22; }"
                "QListWidget::item:selected { background: #1c2b4a; }"
                "QListWidget::item:hover { background: #18181c; }"
            )
            self._q_list.setContextMenuPolicy(Qt.CustomContextMenu)
            self._q_list.customContextMenuRequested.connect(self._queue_context_menu)
            self._q_list.itemClicked.connect(self._queue_item_clicked)
            lay.addWidget(self._q_list, 1)

            # ── Inline edit panel (slide in from bottom) ──────────────
            self._q_edit_frame = QFrame()
            self._q_edit_frame.setObjectName("SurfaceRaised")
            self._q_edit_frame.setVisible(False)
            ef = QVBoxLayout(self._q_edit_frame)
            ef.setContentsMargins(8, 8, 8, 8)
            ef.setSpacing(5)

            edit_hdr = QHBoxLayout()
            self._q_edit_lbl = QLabel("Edit Instructions")
            self._q_edit_lbl.setObjectName("Muted")
            edit_hdr.addWidget(self._q_edit_lbl, 1)
            ef.addLayout(edit_hdr)

            self._q_edit_text = QPlainTextEdit()
            self._q_edit_text.setFixedHeight(88)
            self._q_edit_text.setPlaceholderText("Modify the instructions for this queued job…")
            ef.addWidget(self._q_edit_text)

            edit_btns = QHBoxLayout()
            edit_btns.addStretch(1)
            btn_cancel = QPushButton("Cancel")
            btn_cancel.setObjectName("Ghost")
            btn_cancel.clicked.connect(lambda: self._q_edit_frame.setVisible(False))
            btn_save = QPushButton("💾 Save")
            btn_save.clicked.connect(self._queue_save_edit)
            edit_btns.addWidget(btn_cancel)
            edit_btns.addWidget(btn_save)
            ef.addLayout(edit_btns)

            lay.addWidget(self._q_edit_frame)

            self._q_edit_path: Optional[Path] = None
            self._refresh_queue()

        # ── Queue helpers ──────────────────────────────────────────────
        def _q_collect_jobs(self) -> List[Dict]:
            """Scan all 6 queue dirs and return unified job dicts, newest first."""
            jobs: List[Dict] = []
            STATUS_MAP = {
                (AIDER_ACTIVE_DIR, "aider"): "active",
                (AIDER_DONE_DIR,   "aider"): "done",
                (AIDER_FAILED_DIR, "aider"): "failed",
                (ACTIVE_DIR,       "task"):  "active",
                (DONE_DIR,         "task"):  "done",
                (FAILED_DIR,       "task"):  "failed",
            }
            for (folder, group), base_status in STATUS_MAP.items():
                if not folder.exists():
                    continue
                # For done dirs only keep the 30 most-recent to avoid clutter
                files = sorted(folder.glob("*.json"),
                               key=lambda p: p.stat().st_mtime if p.exists() else 0,
                               reverse=True)
                if base_status == "done":
                    files = files[:30]
                for p in files:
                    try:
                        data = json.loads(p.read_text(encoding="utf-8", errors="replace"))
                        tfiles = data.get("target_files") or []
                        raw_target = (
                            str(tfiles[0]) if isinstance(tfiles, list) and tfiles
                            else str(tfiles) if tfiles
                            else str(data.get("target_file") or "")
                        )
                        target = Path(raw_target).name if raw_target else "—"
                        instr = str(
                            data.get("instructions") or data.get("prompt") or
                            data.get("task_type") or ""
                        )[:80]
                        # Refine active→running/queued
                        if base_status == "active":
                            file_status = str(data.get("status") or "").lower()
                            display = "running" if "run" in file_status else "queued"
                        else:
                            display = base_status
                        jobs.append({
                            "status":  display,
                            "group":   group,
                            "target":  target,
                            "instr":   instr,
                            "path":    p,
                            "mtime":   p.stat().st_mtime,
                            "data":    data,
                        })
                    except Exception:
                        pass
            jobs.sort(key=lambda j: j["mtime"], reverse=True)
            return jobs

        def _refresh_queue(self) -> None:
            try:
                q_text = (self._q_filter.text() or "").strip().lower()
                all_jobs = self._q_collect_jobs()

                # Apply filter
                if q_text:
                    all_jobs = [
                        j for j in all_jobs
                        if q_text in (j["target"] + j["instr"] + j["path"].stem).lower()
                    ]

                # Stats (from full unfiltered list for accuracy)
                full = self._q_collect_jobs()
                n_queued  = sum(1 for j in full if j["status"] == "queued")
                n_running = sum(1 for j in full if j["status"] == "running")
                n_failed  = sum(1 for j in full if j["status"] == "failed")
                n_done    = sum(1 for j in full if j["status"] == "done")

                self._q_lbl_queued.setText(f"● {n_queued} Queued")
                self._q_lbl_running.setText(f"⚡ {n_running} Running")
                self._q_lbl_failed.setText(f"✕ {n_failed} Failed")
                self._q_lbl_done.setText(f"✓ {n_done} Done")

                _STATUS_ICON  = {"queued": "🔵", "running": "🟡",
                                  "failed": "🔴", "done": "✅"}
                _STATUS_COLOR = {"queued": "#4a9eff", "running": "#f5a623",
                                  "failed": "#ff5f56", "done":   "#27c93f"}
                _STATUS_BG    = {"queued": "#0d1f38", "running": "#2a1e08",
                                  "failed": "#2a0a0a", "done":   "#0a1f10"}

                def _q_elapsed(data: dict) -> str:
                    from datetime import datetime as _dt
                    for key in ("finished_at", "updated_at", "timestamp"):
                        raw = str(data.get(key) or "")
                        if not raw:
                            continue
                        try:
                            dt = _dt.fromisoformat(raw.replace("Z", "+00:00").split("+")[0])
                            secs = int((_dt.now() - dt).total_seconds())
                            if secs < 60:   return f"{secs}s ago"
                            if secs < 3600: return f"{secs // 60}m ago"
                            return f"{secs // 3600}h {(secs % 3600) // 60}m ago"
                        except Exception:
                            pass
                    return ""

                def _q_ts_label(data: dict) -> str:
                    from datetime import datetime as _dt
                    for key in ("finished_at", "updated_at", "timestamp"):
                        raw = str(data.get(key) or "")
                        if not raw:
                            continue
                        try:
                            dt = _dt.fromisoformat(raw.replace("Z", "+00:00").split("+")[0])
                            today = _dt.now().date()
                            if dt.date() == today:
                                return dt.strftime("Today %H:%M:%S")
                            return dt.strftime("%b %d  %H:%M:%S")
                        except Exception:
                            pass
                    return ""

                def _q_diff_badge(data: dict) -> str:
                    diff = str(data.get("diff") or "")
                    if not diff.strip():
                        return "○ no diff"
                    lines = [l for l in diff.splitlines() if l.startswith(("+", "-")) and not l.startswith(("+++", "---"))]
                    return f"✎ {len(lines)} lines"

                self._q_list.clear()
                self._q_list.setSpacing(2)
                for job in all_jobs[:300]:
                    s    = job["status"]
                    data = job.get("data", {})
                    icon = _STATUS_ICON.get(s, "⬜")
                    grp  = "Aider" if job["group"] == "aider" else "Task"
                    tgt  = job["target"] or "—"
                    ins  = (job["instr"] or "—")
                    ts   = _q_ts_label(data)
                    ela  = _q_elapsed(data)
                    diff_badge = _q_diff_badge(data)
                    tid  = str(data.get("task_id") or data.get("id") or job["path"].stem)
                    short_tid = tid[-12:] if len(tid) > 12 else tid

                    # Build a rich item widget
                    frame = QFrame()
                    frame.setStyleSheet(
                        f"QFrame {{ background: {_STATUS_BG.get(s,'#111114')};"
                        f" border-radius: 8px; border: 1px solid #1e1e24; }}"
                    )
                    fl = QVBoxLayout(frame)
                    fl.setContentsMargins(10, 7, 10, 7)
                    fl.setSpacing(3)

                    # ── Top row: icon + status + group tag + target + elapsed ─
                    top_row = QHBoxLayout()
                    top_row.setSpacing(8)

                    status_lbl = QLabel(f"{icon} {s.upper()}")
                    status_lbl.setStyleSheet(
                        f"color:{_STATUS_COLOR.get(s,'#aaa')}; font-weight:700; font-size:11px; background:transparent; border:none;"
                    )
                    top_row.addWidget(status_lbl)

                    grp_lbl = QLabel(f"[{grp}]")
                    grp_lbl.setStyleSheet("color:#5a5a68; font-size:10px; background:transparent; border:none;")
                    top_row.addWidget(grp_lbl)

                    tgt_lbl = QLabel(tgt)
                    tgt_lbl.setStyleSheet("color:#e9e9eb; font-weight:600; font-size:12px; background:transparent; border:none;")
                    top_row.addWidget(tgt_lbl, 1)

                    if diff_badge and s == "done":
                        diff_lbl = QLabel(diff_badge)
                        diff_lbl.setStyleSheet(
                            "color:#3a9a55; font-size:10px; background:transparent; border:none;" if "✎" in diff_badge
                            else "color:#4a4a5a; font-size:10px; background:transparent; border:none;"
                        )
                        top_row.addWidget(diff_lbl)

                    if ela:
                        ela_lbl = QLabel(ela)
                        ela_lbl.setStyleSheet("color:#505060; font-size:10px; background:transparent; border:none;")
                        top_row.addWidget(ela_lbl)

                    fl.addLayout(top_row)

                    # ── Bottom row: timestamp · instruction snippet ────────
                    parts = []
                    if ts:
                        parts.append(ts)
                    if ins and ins != "—":
                        short_ins = ins[:90] + "…" if len(ins) > 90 else ins
                        parts.append(short_ins)
                    elif tid:
                        parts.append(f"id:{short_tid}")
                    bot_lbl = QLabel("  ·  ".join(parts) if parts else "")
                    bot_lbl.setStyleSheet("color:#6a6a7a; font-size:10px; background:transparent; border:none;")
                    bot_lbl.setWordWrap(False)
                    fl.addWidget(bot_lbl)

                    it = QListWidgetItem()
                    it.setData(Qt.UserRole,     str(job["path"]))
                    it.setData(Qt.UserRole + 1, s)
                    it.setSizeHint(frame.sizeHint())
                    self._q_list.addItem(it)
                    self._q_list.setItemWidget(it, frame)
            except Exception:
                pass

        def _queue_item_clicked(self, item: QListWidgetItem) -> None:
            """Single click → show full job JSON in Preview tab."""
            try:
                p = Path(str(item.data(Qt.UserRole) or ""))
                if not p.exists():
                    return
                data = json.loads(p.read_text(encoding="utf-8", errors="replace"))
                self.preview.clear()
                status = str(item.data(Qt.UserRole + 1) or "")
                accent = {"queued": "#4a9eff", "running": "#f5a623",
                           "failed": "#ff5f56", "done": "#27c93f"}.get(status, "#a6a6ad")
                self.preview.append_block(
                    f"Job — {status.upper()}  ·  {p.parent.name}/{p.name}",
                    json.dumps(data, indent=2, ensure_ascii=False)[:40000],
                    accent=accent,
                )
                self.tabs.setCurrentIndex(0)  # switch to Preview
            except Exception:
                pass

        def _queue_context_menu(self, pos) -> None:
            item = self._q_list.itemAt(pos)
            if not item:
                return
            path   = Path(str(item.data(Qt.UserRole) or ""))
            status = str(item.data(Qt.UserRole + 1) or "")

            menu = QMenu(self)
            act_view  = menu.addAction("👁  View Details")
            menu.addSeparator()
            act_edit  = menu.addAction("✏  Edit Prompt")
            act_del   = menu.addAction("🗑  Delete Job")
            menu.addSeparator()
            act_retry = menu.addAction("↺  Retry  (move → active)")

            act_edit.setEnabled(status in ("queued", "active"))
            act_retry.setEnabled(status == "failed")

            chosen = menu.exec(self._q_list.viewport().mapToGlobal(pos))
            if chosen == act_view:
                self._queue_item_clicked(item)
            elif chosen == act_edit:
                self._queue_open_edit(path)
            elif chosen == act_del:
                self._queue_delete(path)
            elif chosen == act_retry:
                self._queue_retry(path)

        def _queue_open_edit(self, path: Path) -> None:
            """Open the inline edit panel pre-filled with current instructions."""
            try:
                data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
                current = str(data.get("instructions") or data.get("prompt") or "")
                self._q_edit_path = path
                self._q_edit_lbl.setText(
                    f"Edit Instructions  ·  {path.parent.name}/{path.stem}"
                )
                self._q_edit_text.setPlainText(current)
                self._q_edit_frame.setVisible(True)
                self._q_edit_text.setFocus()
            except Exception:
                pass

        def _queue_save_edit(self) -> None:
            """Write edited instructions back to the job JSON."""
            try:
                if not self._q_edit_path or not self._q_edit_path.exists():
                    self._q_edit_frame.setVisible(False)
                    return
                new_text = self._q_edit_text.toPlainText().strip()
                data = json.loads(
                    self._q_edit_path.read_text(encoding="utf-8", errors="replace")
                )
                if "instructions" in data:
                    data["instructions"] = new_text
                else:
                    data["prompt"] = new_text
                tmp = self._q_edit_path.with_suffix(".edit.tmp")
                tmp.write_text(
                    json.dumps(data, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                tmp.replace(self._q_edit_path)
                self._q_edit_frame.setVisible(False)
                self._refresh_queue()
            except Exception:
                pass

        def _queue_delete(self, path: Path) -> None:
            """Delete a job file and refresh."""
            try:
                path.unlink(missing_ok=True)
                self._refresh_queue()
            except Exception:
                pass

        def _queue_retry(self, path: Path) -> None:
            """Move a failed job back to the aider active queue."""
            try:
                dest = AIDER_ACTIVE_DIR / path.name
                AIDER_ACTIVE_DIR.mkdir(parents=True, exist_ok=True)
                data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
                data["status"] = "queued"
                data.pop("state", None)
                data.pop("phase", None)
                data.pop("finished_at", None)
                data.pop("progress", None)
                dest.write_text(
                    json.dumps(data, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                path.unlink(missing_ok=True)
                self._refresh_queue()
            except Exception:
                pass

        def _queue_flush_click(self) -> None:
            """Two-click safety: first click arms, second executes within 3s."""
            if not self._q_flush_armed:
                # Arm phase
                self._q_flush_armed = True
                self._q_flush_btn.setText("⚠ Confirm Flush?")
                self._q_flush_btn.setStyleSheet("color:#ff5f56; border-color:#ff5f56;")
                self._q_flush_timer.start(3000)
            else:
                # Confirmed — execute
                self._q_flush_timer.stop()
                self._queue_flush_disarm()
                self._queue_do_flush()

        def _queue_flush_disarm(self) -> None:
            self._q_flush_armed = False
            self._q_flush_btn.setText("🗑 Flush Active")
            self._q_flush_btn.setStyleSheet("")

        def _queue_do_flush(self) -> None:
            """Move all active/queued aider jobs to failed, clear tasks active too."""
            moved = 0
            for src_dir, dst_dir in (
                (AIDER_ACTIVE_DIR, AIDER_FAILED_DIR),
                (ACTIVE_DIR,       FAILED_DIR),
            ):
                if not src_dir.exists():
                    continue
                for f in src_dir.glob("*.json"):
                    try:
                        dst_dir.mkdir(parents=True, exist_ok=True)
                        f.replace(dst_dir / f.name)
                        moved += 1
                    except Exception:
                        pass
            self._refresh_queue()
            self._append_luna_msg(
                f"🗑 **Queue flushed** — {moved} job(s) moved to failed.\n"
                "Aider bridge is now idle and waiting for fresh jobs."
            )

        # ------------------------------
        # Plan snapshot
        # ------------------------------
        def _update_plan_snapshot(self) -> None:
            hb = _safe_read_json(WORKER_HEARTBEAT_PATH, default={}) or {}
            lock = _safe_read_json(WORKER_LOCK_PATH, default={}) or {}
            state = str(hb.get("state") or "unknown")
            phase = str(hb.get("phase") or "unknown")
            ts = str(hb.get("timestamp") or hb.get("ts") or "")
            pid = str(lock.get("pid") or hb.get("pid") or "")

            try:
                active = len(list(ACTIVE_DIR.glob("*.json")))
                done = len(list(DONE_DIR.glob("*.json")))
                failed = len(list(FAILED_DIR.glob("*.json")))
            except Exception:
                active = done = failed = 0

            self.plan.append_block(
                "Worker Snapshot",
                f"state: {state}\nphase: {phase}\npid: {pid}\nheartbeat: {ts}\nqueue: active={active} done={done} failed={failed}\npaused={KILL_SWITCH_PATH.exists()}",
                accent="#a6a6ad",
            )

        # ------------------------------
        # Timers / Live feed
        # ------------------------------
        def _start_timers(self) -> None:
            self._hb_timer = QTimer(self)
            self._hb_timer.timeout.connect(self._tick_heartbeat)
            self._hb_timer.start(2000)

            self._tasks_timer = QTimer(self)
            self._tasks_timer.timeout.connect(self._tick_tasks)
            self._tasks_timer.start(2500)

        def _tick_tasks(self) -> None:
            try:
                idx = self.tabs.currentIndex()
                if idx == 5:   # Tasks (shifted by Queue tab insert)
                    self._refresh_tasks_list()
                elif idx == 3:  # Queue Hub — always refresh
                    self._refresh_queue()
            except Exception:
                pass

        def _tick_heartbeat(self) -> None:
            hb = _safe_read_json(WORKER_HEARTBEAT_PATH, default={}) or {}
            ts = hb.get("timestamp") or hb.get("ts") or ""
            phase = str(hb.get("phase") or "unknown")
            self.phase.setText(f"phase: {phase}")

            online = False
            age_s = None
            try:
                if ts:
                    dt = datetime.fromisoformat(str(ts))
                    age_s = (datetime.now() - dt).total_seconds()
                    online = age_s < 60
            except Exception:
                online = False

            if self._paused:
                self.badge.setText("● paused")
                self.badge.setStyleSheet("color: #ffbd2e;")
                return

            if online:
                self.badge.setText("● online")
                self.badge.setStyleSheet("color: #27c93f;")
            else:
                if age_s is None:
                    self.badge.setText("● offline")
                    self.badge.setStyleSheet("color: #ff5f56;")
                else:
                    self.badge.setText(f"● stale ({int(age_s)}s)")
                    self.badge.setStyleSheet("color: #ffbd2e;")

        def _start_live_feed(self) -> None:
            self._live_feed_thread = LiveFeedThread()
            self._live_feed_thread.feed_event.connect(self._on_feed_event)
            self._live_feed_thread.start()

        @Slot(dict)
        def _on_feed_event(self, row: dict) -> None:
            """Inspector terminal only — full live feed from worker/bridge/CU.
            Nothing is mirrored to main chat; Luna's own responses go there."""
            try:
                ts  = str(row.get("ts") or _fmt_hms())
                evt = str(row.get("event") or row.get("type") or "info")
                msg = str(row.get("msg") or row.get("message") or "").strip()
                detail = str(row.get("detail") or "").strip()
                icon  = _icon_for_event(evt)
                color = _color_for_event(evt)
                body  = msg if msg else "(no message)"
                if detail:
                    body += "\n" + detail
                self.terminal.append_block(f"[{ts}] {icon} {evt}", body[:9000], accent=color)
            except Exception:
                pass


def main() -> None:
    if not PYSIDE_AVAILABLE:
        print("PySide6 is required. Run: pip install PySide6")
        sys.exit(1)

    ensure_layout()

    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName("SurgeApp")
    app.setStyleSheet(CLAUDE_DARK_QSS)

    win = LunaClaudeStyleWindow()
    win.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()