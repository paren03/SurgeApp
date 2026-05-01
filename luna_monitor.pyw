"""luna_monitor.pyw — Luna Command Deck  (always-on-top dashboard)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from PyQt6.QtCore import QTimer, Qt
from PyQt6.QtGui import QColor, QFont, QFontDatabase, QPalette
from PyQt6.QtWidgets import (
    QApplication, QFrame, QHBoxLayout, QLabel,
    QProgressBar, QVBoxLayout, QWidget,
)

# ── paths ──────────────────────────────────────────────────────────────────────
ROOT              = Path(r"D:\SurgeApp")
STATE_FILE        = ROOT / "memory" / "continues_update_state.json"
NIGHTLY_JSONL     = ROOT / "memory" / "nightly_updates.jsonl"
LIVE_FEED_PATH    = ROOT / "logs"   / "luna_live_feed.jsonl"
HEARTBEAT_PATH    = ROOT / "logs"   / "luna_worker_heartbeat.json"
KILL_SWITCH_PATH  = ROOT / "LUNA_STOP_NOW.flag"
OLLAMA_BASE       = os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434")
NO_WIN            = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
AIDER_TIMEOUT     = 360  # seconds

# ── design tokens ──────────────────────────────────────────────────────────────
# Deep navy foundation
BG          = "#070d1a"
SURFACE     = "#0c1629"
CARD        = "#101e33"
CARD_BORDER = "#1a3254"
GLOW_EDGE   = "#1e4a7a"

# Typography
T_PRIMARY   = "#e8f4ff"
T_SECONDARY = "#7badd4"
T_DIM       = "#3d6088"

# Semantic status colors
S_LIVE      = "#00e5b0"   # teal-green  — working
S_WAIT      = "#f5a623"   # amber       — cooldown / deferred
S_IDLE      = "#4a9eff"   # sky blue    — ready / reviewed
S_DEAD      = "#ff4757"   # red         — stopped / paused / failed
S_OK        = "#2ed573"   # green       — success
S_INFO      = "#5dade2"   # soft blue   — informational

PHASE_COLOR = {
    "queueing":              S_LIVE,
    "starting":              S_LIVE,
    "queueing_plan":         S_LIVE,
    "reviewed":              S_OK,
    "ready":                 S_IDLE,
    "cooldown":              S_WAIT,
    "deferred_dirty_target": S_WAIT,
    "paused":                S_DEAD,
    "blocked_by_staged_edits": S_DEAD,
}

PHASE_TEXT = {
    "queueing":              "Luna is writing code right now",
    "starting":              "Starting a new improvement cycle",
    "queueing_plan":         "Building the next plan",
    "reviewed":              "Cycle complete — improvement applied",
    "ready":                 "Ready and waiting for next cycle",
    "cooldown":              "Short break between cycles",
    "deferred_dirty_target": "Waiting — file has uncommitted changes",
    "paused":                "Paused — needs attention",
    "blocked_by_staged_edits": "Blocked — staged edits in the way",
    "":                      "Idle",
}

STATUS_ICON = {
    "done":        ("✓", S_OK),
    "timeout":     ("⏱", S_WAIT),
    "failed":      ("✗", S_DEAD),
    "noop":        ("—", T_DIM),
    "deferred":    ("↷", S_WAIT),
    "blocked":     ("⛔", S_DEAD),
    "quarantined": ("⚠", S_DEAD),
}

EVENT_ICON = {
    "CU_IMPROVED":    ("↑", S_OK),
    "CU_QUEUED":      ("▶", S_LIVE),
    "CU_COOLDOWN":    ("⏳", S_WAIT),
    "CU_TIMEOUT":     ("⏱", S_WAIT),
    "CU_FAILURE":     ("✗", S_DEAD),
    "CU_STOP":        ("■", S_DEAD),
    "CU_START":       ("▶", S_LIVE),
    "CU_2X_REVIEW":   ("⟳", S_INFO),
    "CU_PLAN_COMPLETE": ("✓", S_OK),
    "GUARDIAN":       ("🛡", S_IDLE),
}


# ── data helpers ───────────────────────────────────────────────────────────────
def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace") or "{}")
    except Exception:
        return {}


def _tail_jsonl(path: Path, want: int = 20) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        if not path.exists():
            return out
        size = path.stat().st_size
        chunk = min(size, 32_768)
        with path.open("rb") as fh:
            fh.seek(max(0, size - chunk))
            if size > chunk:
                fh.readline()
            for line in fh.read().decode("utf-8", errors="replace").splitlines():
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        pass
    return out[-want:]


def _ollama_status() -> Tuple[bool, str]:
    try:
        with urllib.request.urlopen(OLLAMA_BASE + "/api/ps", timeout=2) as r:
            data = json.loads(r.read())
            models = data.get("models") or []
            if models:
                name = (models[0].get("name") or models[0].get("model") or "")
                return True, name.split(":")[0].split("/")[-1] or "running"
        return True, "idle"
    except Exception:
        pass
    try:
        urllib.request.urlopen(OLLAMA_BASE + "/api/tags", timeout=2)
        return True, "idle"
    except Exception:
        return False, "offline"


def _proc_rows() -> List[Dict[str, Any]]:
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process | "
             "Where-Object { $_.Name -match '^python' -and "
             "$_.CommandLine -match 'worker\\.py|aider_bridge|luna_guardian|luna_monitor' } | "
             "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"],
            capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=6, creationflags=NO_WIN,
        )
        raw = (r.stdout or "").strip()
        if not raw:
            return []
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return [parsed]
        return [x for x in parsed if isinstance(x, dict)]
    except Exception:
        return []


def _svc_alive(rows: List[Dict[str, Any]], marker: str) -> bool:
    m = marker.lower()
    return any(m in str(row.get("CommandLine") or "").lower() for row in rows)


def _fmt(secs: float) -> str:
    secs = max(0.0, secs)
    if secs < 60:
        return f"{int(secs)}s"
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m < 60 else f"{m//60}h {m%60:02d}m"


# ── widget helpers ─────────────────────────────────────────────────────────────
def _lbl(text: str = "", size: int = 10, bold: bool = False, color: str = T_PRIMARY) -> QLabel:
    w = QLabel(text)
    f = QFont("Segoe UI", size)
    f.setBold(bold)
    w.setFont(f)
    w.setStyleSheet(f"color:{color}; background:transparent;")
    return w


def _mono(text: str = "", size: int = 9, color: str = T_SECONDARY) -> QLabel:
    w = QLabel(text)
    f = QFont("Consolas", size)
    w.setFont(f)
    w.setStyleSheet(f"color:{color}; background:transparent;")
    return w


def _sep() -> QFrame:
    line = QFrame()
    line.setFrameShape(QFrame.Shape.HLine)
    line.setFixedHeight(1)
    line.setStyleSheet(f"background:{CARD_BORDER}; border:none;")
    return line


def _card() -> QFrame:
    f = QFrame()
    f.setStyleSheet(
        f"QFrame {{ background:{CARD}; border:1px solid {CARD_BORDER};"
        f" border-radius:12px; }}"
    )
    return f


# ── status chip ────────────────────────────────────────────────────────────────
class StatusChip(QLabel):
    """Pill-shaped coloured chip."""
    def set_state(self, text: str, color: str, dim: bool = False) -> None:
        alpha = "33" if dim else "22"
        edge  = "66" if dim else "88"
        self.setText(text)
        self.setStyleSheet(
            f"color:{color}; background:{color}{alpha};"
            f" border:1px solid {color}{edge};"
            f" border-radius:10px; padding:3px 10px;"
        )


# ── main window ────────────────────────────────────────────────────────────────
GLOBAL_SS = f"""
QWidget {{ background:{BG}; font-family:'Segoe UI'; }}
QProgressBar {{
    border:none; border-radius:6px; background:{SURFACE};
}}
QProgressBar::chunk {{
    background: qlineargradient(x1:0,y1:0,x2:1,y2:0,
        stop:0 {S_LIVE}, stop:0.6 {S_IDLE}, stop:1 {S_WAIT});
    border-radius:6px;
}}
"""


class LunaMonitor(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self._rows: List[Dict[str, Any]] = []
        self._ollama: Tuple[bool, str] = (False, "checking")
        self._tick = 0
        self.setWindowTitle("Luna Monitor")
        self.setWindowFlags(Qt.WindowType.WindowStaysOnTopHint | Qt.WindowType.Tool)
        self.setFixedWidth(460)
        self.setStyleSheet(GLOBAL_SS)
        self._build()
        self._refresh()
        t = QTimer(self)
        t.timeout.connect(self._refresh)
        t.start(1500)

    # ── layout ─────────────────────────────────────────────────────────────────
    def _build(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        # ── HEADER ─────────────────────────────────────────────────────────────
        hdr_card = _card()
        hdr_lay  = QVBoxLayout(hdr_card)
        hdr_lay.setContentsMargins(16, 14, 16, 14)
        hdr_lay.setSpacing(10)

        # top row: brand + clock
        top = QHBoxLayout()
        brand_col = QVBoxLayout()
        brand_col.setSpacing(2)
        brand_col.addWidget(_lbl("LUNA", 18, bold=True, color=T_PRIMARY))
        brand_col.addWidget(_lbl("autonomous self-improvement engine", 8, color=T_DIM))
        top.addLayout(brand_col)
        top.addStretch()
        right_col = QVBoxLayout()
        right_col.setAlignment(Qt.AlignmentFlag.AlignRight)
        self._clock = _mono("--:--:--", 11, T_SECONDARY)
        self._clock.setAlignment(Qt.AlignmentFlag.AlignRight)
        right_col.addWidget(self._clock)
        self._date  = _mono("", 8, T_DIM)
        self._date.setAlignment(Qt.AlignmentFlag.AlignRight)
        right_col.addWidget(self._date)
        top.addLayout(right_col)
        hdr_lay.addLayout(top)

        # status badge row
        badge_row = QHBoxLayout()
        badge_row.setSpacing(8)
        self._status_chip = StatusChip("IDLE")
        self._status_chip.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        self._status_chip.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._status_chip.setMinimumHeight(36)
        badge_row.addWidget(self._status_chip, 1)

        self._kill_badge = StatusChip("")
        self._kill_badge.setFont(QFont("Segoe UI", 8))
        self._kill_badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._kill_badge.hide()
        badge_row.addWidget(self._kill_badge)
        hdr_lay.addLayout(badge_row)

        # human-readable description
        self._phase_desc = _lbl("Waiting for state…", 9, color=T_SECONDARY)
        self._phase_desc.setWordWrap(True)
        hdr_lay.addWidget(self._phase_desc)

        root.addWidget(hdr_card)

        # ── TARGET & TIMER ─────────────────────────────────────────────────────
        target_card = _card()
        t_lay = QVBoxLayout(target_card)
        t_lay.setContentsMargins(16, 12, 16, 12)
        t_lay.setSpacing(8)

        tgt_row = QHBoxLayout()
        tgt_row.addWidget(_lbl("Current File", 8, color=T_DIM))
        tgt_row.addStretch()
        self._section_lbl = _mono("", 8, T_DIM)
        tgt_row.addWidget(self._section_lbl)
        t_lay.addLayout(tgt_row)

        self._target_lbl = _lbl("None", 12, bold=True, color=S_IDLE)
        t_lay.addWidget(self._target_lbl)

        t_lay.addWidget(_sep())

        # timer row
        timer_row = QHBoxLayout()
        self._timer_lbl = _lbl("Aider progress", 8, color=T_DIM)
        timer_row.addWidget(self._timer_lbl)
        timer_row.addStretch()
        self._remaining_lbl = _lbl("", 11, bold=True, color=S_WAIT)
        self._remaining_lbl.setAlignment(Qt.AlignmentFlag.AlignRight)
        timer_row.addWidget(self._remaining_lbl)
        t_lay.addLayout(timer_row)

        self._progress = QProgressBar()
        self._progress.setRange(0, AIDER_TIMEOUT)
        self._progress.setValue(0)
        self._progress.setTextVisible(False)
        self._progress.setFixedHeight(12)
        t_lay.addWidget(self._progress)

        root.addWidget(target_card)

        # ── STATS STRIP ────────────────────────────────────────────────────────
        stats_card = _card()
        s_lay = QHBoxLayout(stats_card)
        s_lay.setContentsMargins(16, 12, 16, 12)
        s_lay.setSpacing(0)

        def _stat_col(label: str) -> Tuple[QLabel, QLabel]:
            col = QVBoxLayout()
            col.setSpacing(3)
            val = _lbl("—", 14, bold=True, color=T_PRIMARY)
            val.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cap = _lbl(label, 7, color=T_DIM)
            cap.setAlignment(Qt.AlignmentFlag.AlignCenter)
            col.addWidget(val)
            col.addWidget(cap)
            s_lay.addLayout(col, 1)
            return val, cap

        self._stat_cycle,  _ = _stat_col("CYCLES RUN")
        self._stat_status, _ = _stat_col("LAST RESULT")
        self._stat_fails,  _ = _stat_col("FAILURES")
        self._stat_noop,   _ = _stat_col("NO-CHANGE")

        root.addWidget(stats_card)

        # ── SERVICES ───────────────────────────────────────────────────────────
        svc_card = _card()
        svc_lay  = QVBoxLayout(svc_card)
        svc_lay.setContentsMargins(16, 10, 16, 10)
        svc_lay.setSpacing(8)
        svc_lay.addWidget(_lbl("System Services", 8, bold=True, color=T_DIM))

        svc_row = QHBoxLayout()
        svc_row.setSpacing(6)
        self._svc = {}
        for key, label in [("worker", "Worker"), ("bridge", "Aider"), ("guardian", "Guardian"), ("ollama", "Ollama")]:
            chip = StatusChip(label)
            chip.setFont(QFont("Segoe UI", 8, QFont.Weight.Bold))
            chip.setAlignment(Qt.AlignmentFlag.AlignCenter)
            chip.setMinimumHeight(30)
            chip.set_state(label, T_DIM, dim=True)
            svc_row.addWidget(chip, 1)
            self._svc[key] = chip
        svc_lay.addLayout(svc_row)
        root.addWidget(svc_card)

        # ── LIVE FEED ──────────────────────────────────────────────────────────
        feed_card = _card()
        feed_lay  = QVBoxLayout(feed_card)
        feed_lay.setContentsMargins(16, 12, 16, 12)
        feed_lay.setSpacing(6)
        feed_lay.addWidget(_lbl("Live Activity", 8, bold=True, color=T_DIM))
        self._feed_rows: List[Tuple[QLabel, QLabel]] = []
        for _ in range(5):
            row = QHBoxLayout()
            row.setSpacing(8)
            icon = _mono("", 9)
            icon.setFixedWidth(14)
            icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
            msg  = _mono("", 8, T_DIM)
            msg.setWordWrap(False)
            row.addWidget(icon)
            row.addWidget(msg, 1)
            feed_lay.addLayout(row)
            self._feed_rows.append((icon, msg))
        root.addWidget(feed_card)

        # ── RECENT CYCLES ──────────────────────────────────────────────────────
        hist_card = _card()
        hist_lay  = QVBoxLayout(hist_card)
        hist_lay.setContentsMargins(16, 12, 16, 12)
        hist_lay.setSpacing(6)
        hist_lay.addWidget(_lbl("Recent Cycles", 8, bold=True, color=T_DIM))

        # column headers
        col_hdr = QHBoxLayout()
        for (txt, w) in [("Time", 48), ("File", 0), ("Result", 72)]:
            h = _mono(txt, 7, T_DIM)
            if w:
                h.setFixedWidth(w)
            col_hdr.addWidget(h, 0 if w else 1)
        hist_lay.addLayout(col_hdr)
        hist_lay.addWidget(_sep())

        self._hist_rows: List[Tuple[QLabel, QLabel, QLabel]] = []
        for _ in range(4):
            row = QHBoxLayout()
            row.setSpacing(8)
            ts  = _mono("", 8, T_DIM); ts.setFixedWidth(48)
            fn  = _mono("", 8, T_SECONDARY)
            st  = _mono("", 8, T_DIM); st.setFixedWidth(72); st.setAlignment(Qt.AlignmentFlag.AlignRight)
            row.addWidget(ts)
            row.addWidget(fn, 1)
            row.addWidget(st)
            hist_lay.addLayout(row)
            self._hist_rows.append((ts, fn, st))
        root.addWidget(hist_card)

    # ── refresh ────────────────────────────────────────────────────────────────
    def _refresh(self) -> None:
        now = datetime.now()
        self._clock.setText(now.strftime("%H:%M:%S"))
        self._date.setText(now.strftime("%a %d %b %Y"))
        self._tick += 1

        # Slow polls (every ~6s)
        if self._tick % 4 == 1:
            self._rows   = _proc_rows()
            self._ollama = _ollama_status()

        state        = _read_json(STATE_FILE)
        phase        = str(state.get("phase") or "")
        running      = bool(state.get("running"))
        targets      = state.get("active_target_files") or []
        sec_cursor   = state.get("file_section_cursor") or {}
        cycles       = int(state.get("cycles") or 0)
        last_status  = str(state.get("last_status") or "—")
        failures     = int(state.get("consecutive_failures") or 0)
        noop_count   = int(state.get("noop_count") or 0)
        cooldown_rem = float(state.get("cooldown_remaining_seconds") or 0)
        last_at      = str(state.get("last_cycle_at") or "")

        # ── status badge ──────────────────────────────────────────────────────
        color       = PHASE_COLOR.get(phase, S_IDLE) if running else S_DEAD
        label       = ("● " if running else "○ ") + (PHASE_TEXT.get(phase, phase or "Idle").split("—")[0].strip().upper())
        self._status_chip.set_state(label, color)
        self._phase_desc.setText(PHASE_TEXT.get(phase, "Waiting for activity…") if running else "Luna has stopped — check Recent Cycles for details")
        self._phase_desc.setStyleSheet(f"color:{color if running else S_DEAD}; background:transparent;")

        if KILL_SWITCH_PATH.exists():
            self._kill_badge.show()
            self._kill_badge.set_state("KILL SWITCH ACTIVE", S_DEAD)
        else:
            self._kill_badge.hide()

        # ── target & section ──────────────────────────────────────────────────
        if targets:
            tgt = str(targets[0])
            self._target_lbl.setText(Path(tgt).name)
            self._target_lbl.setStyleSheet(f"color:{color if running else T_DIM}; background:transparent;")
            sec = sec_cursor.get(tgt, "")
            self._section_lbl.setText(f"section: {sec}" if sec else "whole file")
        else:
            self._target_lbl.setText("No file selected")
            self._section_lbl.setText("")

        # ── aider progress bar ────────────────────────────────────────────────
        elapsed = 0.0
        if phase == "queueing" and last_at:
            try:
                elapsed = (now - datetime.fromisoformat(last_at)).total_seconds()
            except Exception:
                pass

        if phase == "queueing":
            self._progress.setValue(min(int(elapsed), AIDER_TIMEOUT))
            remaining = max(0.0, AIDER_TIMEOUT - elapsed)
            self._timer_lbl.setText(f"Aider working — {_fmt(elapsed)} elapsed")
            r_color = S_OK if remaining > 120 else (S_WAIT if remaining > 30 else S_DEAD)
            self._remaining_lbl.setText(f"{_fmt(remaining)} left")
            self._remaining_lbl.setStyleSheet(f"color:{r_color}; background:transparent;")
        elif phase == "cooldown":
            self._progress.setValue(0)
            self._timer_lbl.setText("Short break before next cycle")
            self._remaining_lbl.setText(f"{_fmt(cooldown_rem)}")
            self._remaining_lbl.setStyleSheet(f"color:{S_WAIT}; background:transparent;")
        else:
            self._progress.setValue(0)
            self._timer_lbl.setText("Aider idle")
            self._remaining_lbl.setText("")

        # ── stats strip ───────────────────────────────────────────────────────
        self._stat_cycle.setText(str(cycles))
        icon, col = STATUS_ICON.get(last_status, ("—", T_DIM))
        self._stat_status.setText(f"{icon} {last_status}")
        self._stat_status.setStyleSheet(f"color:{col}; background:transparent;")
        self._stat_fails.setText(str(failures))
        self._stat_fails.setStyleSheet(
            f"color:{'#ff4757' if failures > 0 else T_PRIMARY}; background:transparent;"
        )
        self._stat_noop.setText(str(noop_count))

        # ── services ──────────────────────────────────────────────────────────
        self._svc["worker"].set_state("Worker",   S_OK  if _svc_alive(self._rows, "worker.py") else S_DEAD)
        self._svc["bridge"].set_state("Aider",    S_OK  if _svc_alive(self._rows, "aider_bridge") else S_DEAD)
        self._svc["guardian"].set_state("Guard",  S_OK  if _svc_alive(self._rows, "luna_guardian") else S_DEAD)
        oll_ok, oll_name = self._ollama
        self._svc["ollama"].set_state(
            f"Ollama  {oll_name[:10]}" if (oll_ok and oll_name not in ("idle","")) else "Ollama",
            S_LIVE if (oll_ok and oll_name not in ("idle","offline")) else (S_OK if oll_ok else S_DEAD),
        )

        # ── live feed ─────────────────────────────────────────────────────────
        events = list(reversed(_tail_jsonl(LIVE_FEED_PATH, want=30)))
        shown = 0
        ei = 0
        for icon_lbl, msg_lbl in self._feed_rows:
            while ei < len(events):
                e = events[ei]; ei += 1
                if not isinstance(e, dict):
                    continue
                evt = str(e.get("event") or e.get("type") or "")
                txt = str(e.get("msg") or e.get("message") or "").strip()
                if not txt:
                    continue
                ts  = str(e.get("ts") or "")[-8:][:5]
                ev_icon, ev_color = EVENT_ICON.get(evt, ("·", T_DIM))
                icon_lbl.setText(ev_icon)
                icon_lbl.setStyleSheet(f"color:{ev_color}; background:transparent;")
                msg_lbl.setText(f"{ts}  {txt[:58]}")
                msg_lbl.setStyleSheet(f"color:{T_SECONDARY}; background:transparent;")
                shown += 1
                break
            else:
                icon_lbl.setText("")
                msg_lbl.setText("")
        for icon_lbl, msg_lbl in self._feed_rows[shown:]:
            icon_lbl.setText("")
            msg_lbl.setText("")

        # ── recent cycles ─────────────────────────────────────────────────────
        entries = list(reversed(_tail_jsonl(NIGHTLY_JSONL, want=20)))
        cycles_shown = 0
        ci = 0
        for ts_lbl, fn_lbl, st_lbl in self._hist_rows:
            while ci < len(entries):
                row = entries[ci]; ci += 1
                if not isinstance(row, dict) or row.get("task_id") is None:
                    continue
                st  = str(row.get("status") or "?")
                tgt = (row.get("target_files") or ["?"])[0]
                ts  = str(row.get("finished_at") or "")
                ts_short = ts[-8:][:5] if len(ts) >= 8 else ts[:5]
                icon, col = STATUS_ICON.get(st, ("?", T_DIM))
                ts_lbl.setText(ts_short)
                fn_lbl.setText(Path(str(tgt)).stem[:24])
                st_lbl.setText(f"{icon} {st}")
                st_lbl.setStyleSheet(f"color:{col}; background:transparent;")
                fn_lbl.setStyleSheet(f"color:{T_SECONDARY}; background:transparent;")
                cycles_shown += 1
                break
            else:
                for lbl in (ts_lbl, fn_lbl, st_lbl):
                    lbl.setText("")
        for ts_lbl, fn_lbl, st_lbl in self._hist_rows[cycles_shown:]:
            for lbl in (ts_lbl, fn_lbl, st_lbl):
                lbl.setText("")


# ── entry ──────────────────────────────────────────────────────────────────────
def main() -> None:
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    pal = QPalette()
    pal.setColor(QPalette.ColorRole.Window, QColor(BG))
    app.setPalette(pal)
    win = LunaMonitor()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
