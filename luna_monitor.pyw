"""luna_monitor.pyw - Floating Luna command deck."""
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
from PyQt6.QtGui import QColor, QFont, QPalette
from PyQt6.QtWidgets import (
    QApplication,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QVBoxLayout,
    QWidget,
)

ROOT = Path(r"D:\SurgeApp")
STATE_FILE = ROOT / "memory" / "continues_update_state.json"
NIGHTLY_JSONL = ROOT / "memory" / "nightly_updates.jsonl"
LIVE_FEED_PATH = ROOT / "logs" / "luna_live_feed.jsonl"
WORKER_HEARTBEAT_PATH = ROOT / "logs" / "luna_worker_heartbeat.json"
KILL_SWITCH_PATH = ROOT / "LUNA_STOP_NOW.flag"
OLLAMA_BASE = os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434")
NO_WIN = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
AIDER_TIMEOUT = 360

C_BG = "#08111f"
C_PANEL = "#0f1b2d"
C_CARD = "#12243b"
C_EDGE = "#214164"
C_TEXT = "#edf4ff"
C_MUTED = "#8fa9c6"
C_SKY = "#63c7ff"
C_AQUA = "#45f0df"
C_GOLD = "#f2c76f"
C_RED = "#ff6b6b"
C_GREEN = "#7ef0b8"

PHASE_COLORS = {
    "queueing": C_AQUA,
    "cooldown": C_GOLD,
    "ready": C_SKY,
    "reviewed": C_GREEN,
    "paused": C_RED,
    "blocked_by_staged_edits": C_RED,
    "deferred_dirty_target": C_GOLD,
    "starting": C_SKY,
    "queueing_plan": C_AQUA,
}

PHASE_LABELS = {
    "queueing": "LIVE PATCH",
    "cooldown": "COOLDOWN",
    "ready": "READY",
    "reviewed": "REVIEWED",
    "paused": "PAUSED",
    "blocked_by_staged_edits": "BLOCKED",
    "deferred_dirty_target": "DEFERRED",
    "starting": "STARTING",
    "queueing_plan": "PLANNING",
    "": "IDLE",
}


def _label(text: str, size: int = 10, bold: bool = False, color: str = C_TEXT) -> QLabel:
    lbl = QLabel(text)
    font = QFont("Consolas", size)
    font.setBold(bold)
    lbl.setFont(font)
    lbl.setStyleSheet(f"color: {color}; background: transparent;")
    return lbl


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace") or "{}")
    except Exception:
        return {}


def _tail_lines(path: Path, limit: int = 8, max_bytes: int = 262_144) -> List[str]:
    try:
        if not path.exists():
            return []
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > max_bytes:
                handle.seek(max(0, size - max_bytes))
                if size > max_bytes:
                    handle.readline()
            data = handle.read()
        return data.decode("utf-8", errors="replace").splitlines()[-limit:]
    except Exception:
        return []


def _recent_cycles(limit: int = 4) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for line in reversed(_tail_lines(NIGHTLY_JSONL, limit=40)):
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict) and row.get("task_id") is not None:
            entries.append(row)
        if len(entries) >= limit:
            break
    return entries


def _recent_events(limit: int = 5) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for line in reversed(_tail_lines(LIVE_FEED_PATH, limit=40)):
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            events.append(row)
        if len(events) >= limit:
            break
    return events


def _fmt_secs(value: float) -> str:
    value = max(0.0, value)
    if value < 60:
        return f"{int(value)}s"
    minutes, seconds = divmod(int(value), 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def _ollama_status() -> Tuple[bool, str]:
    try:
        with urllib.request.urlopen(OLLAMA_BASE + "/api/ps", timeout=2) as response:
            data = json.loads(response.read().decode())
            models = data.get("models") or []
            if models:
                name = models[0].get("name") or models[0].get("model") or "running"
                return True, name.split(":")[0].split("/")[-1]
        return True, "idle"
    except Exception:
        pass
    try:
        urllib.request.urlopen(OLLAMA_BASE + "/api/tags", timeout=2)
        return True, "idle"
    except Exception:
        return False, "offline"


def _process_rows() -> List[Dict[str, Any]]:
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    "Get-CimInstance Win32_Process | "
                    "Where-Object { $_.Name -match '^python' -and "
                    "$_.CommandLine -match 'SurgeApp|worker.py|aider_bridge.py|luna_guardian.py|luna_monitor.pyw' } | "
                    "Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Compress"
                ),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=6,
            creationflags=NO_WIN,
        )
        raw = (result.stdout or "").strip()
        if not raw:
            return []
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [row for row in parsed if isinstance(row, dict)]
        if isinstance(parsed, dict):
            return [parsed]
    except Exception:
        pass
    return []


def _service_alive(rows: List[Dict[str, Any]], marker: str) -> bool:
    marker = marker.lower()
    return any(marker in str(row.get("CommandLine") or "").lower() for row in rows)


def _system_stats() -> Tuple[str, str]:
    try:
        import psutil  # type: ignore

        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory().percent
        return f"{cpu:.0f}%", f"{ram:.0f}%"
    except Exception:
        return "?", "?"


class LunaMonitor(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self._rows: List[Dict[str, Any]] = []
        self._ollama = (False, "checking")
        self._sample_count = 0
        self.setWindowTitle("Luna Monitor")
        self.setWindowFlags(Qt.WindowType.WindowStaysOnTopHint | Qt.WindowType.Tool)
        self.setMinimumWidth(430)
        self.setMaximumWidth(430)
        self._build_ui()
        self._refresh()
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._refresh)
        self._timer.start(1500)

    def _build_ui(self) -> None:
        self.setStyleSheet(
            f"""
            QWidget {{
                background: {C_BG};
            }}
            QFrame#Card {{
                background: {C_CARD};
                border: 1px solid {C_EDGE};
                border-radius: 16px;
            }}
            QFrame#Hero {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #12243b, stop:0.6 #102033, stop:1 #0d1a2a);
                border: 1px solid #2a537d;
                border-radius: 18px;
            }}
            QProgressBar {{
                border: 1px solid {C_EDGE};
                border-radius: 7px;
                background: {C_PANEL};
            }}
            QProgressBar::chunk {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {C_AQUA}, stop:0.55 {C_SKY}, stop:1 {C_GOLD});
                border-radius: 6px;
            }}
            """
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        hero = QFrame()
        hero.setObjectName("Hero")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(14, 12, 14, 12)
        hero_layout.setSpacing(8)

        hero_top = QHBoxLayout()
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title_box.addWidget(_label("LUNA MONITOR", 12, bold=True, color=C_TEXT))
        title_box.addWidget(_label("autonomy pulse + service watch", 8, color=C_MUTED))
        hero_top.addLayout(title_box)
        hero_top.addStretch()
        self._clock = _label("--:--:--", 9, bold=True, color=C_SKY)
        hero_top.addWidget(self._clock)
        hero_layout.addLayout(hero_top)

        hero_mid = QHBoxLayout()
        self._phase = _label("IDLE", 10, bold=True, color=C_TEXT)
        self._phase.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._phase.setStyleSheet(
            f"color:{C_TEXT}; background:#18304e; border:1px solid #305c88; border-radius:11px; padding:6px 12px;"
        )
        hero_mid.addWidget(self._phase, 0)
        hero_mid.addStretch()
        self._next_action = _label("waiting for state…", 8, color=C_MUTED)
        hero_mid.addWidget(self._next_action, 0)
        hero_layout.addLayout(hero_mid)

        self._target = _label("No active target", 10, bold=True, color=C_SKY)
        self._section = _label("", 8, color=C_MUTED)
        hero_layout.addWidget(self._target)
        hero_layout.addWidget(self._section)
        root.addWidget(hero)

        stats = QFrame()
        stats.setObjectName("Card")
        stats_layout = QGridLayout(stats)
        stats_layout.setContentsMargins(12, 10, 12, 10)
        stats_layout.setHorizontalSpacing(12)
        stats_layout.setVerticalSpacing(8)

        self._cycle = _label("cycle --", 9, bold=True, color=C_TEXT)
        self._last = _label("last: --", 9, color=C_MUTED)
        self._cpu = _label("CPU --", 9, bold=True, color=C_TEXT)
        self._ram = _label("RAM --", 9, color=C_MUTED)
        self._queue = _label("queue --", 9, bold=True, color=C_TEXT)
        self._fails = _label("fails --", 9, color=C_MUTED)

        stats_layout.addWidget(self._cycle, 0, 0)
        stats_layout.addWidget(self._last, 0, 1)
        stats_layout.addWidget(self._cpu, 1, 0)
        stats_layout.addWidget(self._ram, 1, 1)
        stats_layout.addWidget(self._queue, 2, 0)
        stats_layout.addWidget(self._fails, 2, 1)
        root.addWidget(stats)

        services = QFrame()
        services.setObjectName("Card")
        services_layout = QVBoxLayout(services)
        services_layout.setContentsMargins(12, 10, 12, 10)
        services_layout.setSpacing(8)
        services_layout.addWidget(_label("Services", 8, color=C_MUTED))

        service_row = QHBoxLayout()
        service_row.setSpacing(8)
        self._svc_worker = _label("WORKER", 8, bold=True, color=C_MUTED)
        self._svc_bridge = _label("BRIDGE", 8, bold=True, color=C_MUTED)
        self._svc_guardian = _label("GUARDIAN", 8, bold=True, color=C_MUTED)
        self._svc_ollama = _label("OLLAMA", 8, bold=True, color=C_MUTED)
        for widget in (self._svc_worker, self._svc_bridge, self._svc_guardian, self._svc_ollama):
            widget.setAlignment(Qt.AlignmentFlag.AlignCenter)
            widget.setStyleSheet(
                f"color:{C_MUTED}; background:{C_PANEL}; border:1px solid {C_EDGE}; border-radius:10px; padding:6px 8px;"
            )
            service_row.addWidget(widget, 1)
        services_layout.addLayout(service_row)
        root.addWidget(services)

        progress_card = QFrame()
        progress_card.setObjectName("Card")
        progress_layout = QVBoxLayout(progress_card)
        progress_layout.setContentsMargins(12, 10, 12, 10)
        progress_layout.setSpacing(8)
        progress_layout.addWidget(_label("Aider Run", 8, color=C_MUTED))
        self._progress = QProgressBar()
        self._progress.setRange(0, AIDER_TIMEOUT)
        self._progress.setValue(0)
        self._progress.setTextVisible(False)
        self._progress.setFixedHeight(16)
        progress_layout.addWidget(self._progress)
        timer_row = QHBoxLayout()
        self._elapsed = _label("0s elapsed", 8, color=C_MUTED)
        self._remaining = _label("", 9, bold=True, color=C_GOLD)
        timer_row.addWidget(self._elapsed)
        timer_row.addStretch()
        timer_row.addWidget(self._remaining)
        progress_layout.addLayout(timer_row)
        root.addWidget(progress_card)

        events = QFrame()
        events.setObjectName("Card")
        events_layout = QVBoxLayout(events)
        events_layout.setContentsMargins(12, 10, 12, 10)
        events_layout.setSpacing(6)
        events_layout.addWidget(_label("Live Feed", 8, color=C_MUTED))
        self._event_labels: List[QLabel] = []
        for _ in range(5):
            lbl = _label("", 8, color=C_MUTED)
            events_layout.addWidget(lbl)
            self._event_labels.append(lbl)
        root.addWidget(events)

        cycles = QFrame()
        cycles.setObjectName("Card")
        cycles_layout = QVBoxLayout(cycles)
        cycles_layout.setContentsMargins(12, 10, 12, 10)
        cycles_layout.setSpacing(6)
        cycles_layout.addWidget(_label("Recent Cycles", 8, color=C_MUTED))
        self._cycle_labels: List[QLabel] = []
        for _ in range(4):
            lbl = _label("", 8, color=C_MUTED)
            cycles_layout.addWidget(lbl)
            self._cycle_labels.append(lbl)
        root.addWidget(cycles)

    def _set_service_chip(self, widget: QLabel, name: str, healthy: bool, detail: str = "") -> None:
        fg = C_GREEN if healthy else C_RED
        bg = "#133428" if healthy else "#361820"
        edge = "#2e7d60" if healthy else "#87414f"
        suffix = f" {detail}" if detail else ""
        widget.setText(f"{name}{suffix}")
        widget.setStyleSheet(
            f"color:{fg}; background:{bg}; border:1px solid {edge}; border-radius:10px; padding:6px 8px;"
        )

    def _refresh(self) -> None:
        self._clock.setText(datetime.now().strftime("%H:%M:%S"))
        state = _read_json(STATE_FILE)
        phase = str(state.get("phase") or "")
        running = bool(state.get("running"))
        targets = state.get("active_target_files") or []
        section_cursor = state.get("file_section_cursor") or {}
        cycle = int(state.get("cycles") or 0)
        last_status = str(state.get("last_status") or "—")
        failures = int(state.get("consecutive_failures") or 0)
        noop_count = int(state.get("noop_count") or 0)
        cooldown_rem = float(state.get("cooldown_remaining_seconds") or 0)
        heartbeat = _read_json(WORKER_HEARTBEAT_PATH)
        queue_depth = int(heartbeat.get("queue_depth") or 0)
        active_count = int(heartbeat.get("active_count") or 0)

        phase_name = PHASE_LABELS.get(phase, phase.upper() or "IDLE")
        phase_color = PHASE_COLORS.get(phase, C_MUTED) if running else C_RED
        self._phase.setText(phase_name)
        self._phase.setStyleSheet(
            f"color:{C_TEXT}; background:{phase_color}33; border:1px solid {phase_color}; border-radius:11px; padding:6px 12px;"
        )

        if running and phase == "cooldown":
            self._next_action.setText(f"next cycle in {_fmt_secs(cooldown_rem)}")
        elif running and phase == "queueing":
            self._next_action.setText("actively patching")
        elif KILL_SWITCH_PATH.exists():
            self._next_action.setText("kill switch active")
        else:
            self._next_action.setText("standing by")

        if targets:
            target = str(targets[0])
            self._target.setText(f"TARGET  {Path(target).name}")
            self._section.setText(f"section  {section_cursor.get(target, '—')}")
        else:
            self._target.setText("TARGET  none")
            self._section.setText("section  —")

        self._cycle.setText(f"cycle  {cycle}")
        self._last.setText(f"last  {last_status}")
        cpu, ram = _system_stats()
        self._cpu.setText(f"CPU  {cpu}")
        self._ram.setText(f"RAM  {ram}")
        self._queue.setText(f"queue  {queue_depth}  active {active_count}")
        self._fails.setText(f"fails  {failures}  noop {noop_count}")

        self._sample_count += 1
        if self._sample_count % 4 == 1:
            self._rows = _process_rows()
            self._ollama = _ollama_status()

        self._set_service_chip(self._svc_worker, "WORKER", _service_alive(self._rows, "worker.py"))
        self._set_service_chip(self._svc_bridge, "BRIDGE", _service_alive(self._rows, "aider_bridge.py"))
        self._set_service_chip(self._svc_guardian, "GUARD", _service_alive(self._rows, "luna_guardian.py"))
        ollama_ok, ollama_name = self._ollama
        self._set_service_chip(self._svc_ollama, "OLLAMA", ollama_ok, ollama_name[:8])

        elapsed = 0.0
        last_cycle_at = str(state.get("last_cycle_at") or "")
        if phase == "queueing" and last_cycle_at:
            try:
                elapsed = (datetime.now() - datetime.fromisoformat(last_cycle_at)).total_seconds()
            except Exception:
                elapsed = 0.0

        self._progress.setValue(min(int(elapsed), AIDER_TIMEOUT) if phase == "queueing" else 0)
        if phase == "queueing":
            remaining = max(0.0, AIDER_TIMEOUT - elapsed)
            self._elapsed.setText(f"{_fmt_secs(elapsed)} elapsed")
            self._remaining.setText(f"{_fmt_secs(remaining)} left")
            color = C_GREEN if remaining > 120 else (C_GOLD if remaining > 30 else C_RED)
            self._remaining.setStyleSheet(f"color:{color}; background: transparent;")
        elif phase == "cooldown":
            self._elapsed.setText("cooldown")
            self._remaining.setText(_fmt_secs(cooldown_rem))
            self._remaining.setStyleSheet(f"color:{C_GOLD}; background: transparent;")
        else:
            self._elapsed.setText(f"state  {phase or 'idle'}")
            self._remaining.setText("")

        events = _recent_events(5)
        for index, label in enumerate(self._event_labels):
            if index >= len(events):
                label.setText("")
                continue
            row = events[index]
            event = str(row.get("event") or row.get("type") or "?")
            message = str(row.get("msg") or row.get("message") or "").strip()[:52]
            ts = str(row.get("ts") or "")[-8:]
            color = C_GREEN if "DONE" in event else (C_RED if "FAIL" in event or "STOP" in event else C_SKY)
            label.setText(f"{ts}  {event:<18}  {message}")
            label.setStyleSheet(f"color:{color}; background: transparent;")

        cycles = _recent_cycles(4)
        for index, label in enumerate(self._cycle_labels):
            if index >= len(cycles):
                label.setText("")
                continue
            row = cycles[index]
            status = str(row.get("status") or "?")
            target_files = row.get("target_files") or ["?"]
            target = Path(str(target_files[0])).stem[:20]
            ts = str(row.get("finished_at") or "")[-8:]
            color = C_GREEN if status == "done" else (C_RED if status in {"failed", "timeout", "quarantined"} else C_GOLD)
            label.setText(f"{ts}  c{row.get('cycle', '?')}  {status:<10}  {target}")
            label.setStyleSheet(f"color:{color}; background: transparent;")


def main() -> None:
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window, QColor(C_BG))
    app.setPalette(palette)
    win = LunaMonitor()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
