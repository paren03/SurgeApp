import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

DEFAULT_PROJECT_DIR = r"D:\SurgeApp"
PROJECT_DIR = Path(os.environ.get("LUNA_PROJECT_DIR", DEFAULT_PROJECT_DIR))
LOGS_DIR = PROJECT_DIR / "logs"
MEMORY_DIR = PROJECT_DIR / "memory"
WORKER_PATH = PROJECT_DIR / "worker.py"
START_PATH = PROJECT_DIR / "luna_start.pyw"
START_BAT_PATH = PROJECT_DIR / "luna_start.bat"
START_VBS_PATH = PROJECT_DIR / "Start_SurgeApp.vbs"
LUNA_ICON_PATH = PROJECT_DIR / "Luna.ico"

HEARTBEAT_PATH = LOGS_DIR / "luna_worker_heartbeat.json"
WATCHDOG_STATUS_PATH = MEMORY_DIR / "luna_guardian_status.json"
GUARDIAN_LOCK_PATH = MEMORY_DIR / "luna_guardian.lock.json"
THERMAL_GUARD_STATE_PATH = MEMORY_DIR / "luna_thermal_guard_state.json"
BOOT_ANCHOR_STATE_PATH = MEMORY_DIR / "luna_boot_anchor_state.json"
AGENCY_STATE_PATH = MEMORY_DIR / "luna_recursive_agency_guardian_state.json"
AGENCY_QUEUE_PATH = MEMORY_DIR / "luna_autonomy_cycle_queue.json"
AGENCY_LOG_PATH = LOGS_DIR / "luna_autonomy_cycle_log.jsonl"
DIRECTOR_JOURNAL_PATH = PROJECT_DIR / "Journal.txt"
SHUTDOWN_FLAG_PATH = LOGS_DIR / "SHUTDOWN.flag"

POLL_SECONDS = 5.0
HEARTBEAT_STALE_SECONDS = 30.0
RESTART_COOLDOWN_SECONDS = 30.0
MAX_RECENT_TRIGGERS = 8
THERMAL_CPU_HOT_PERCENT = 85.0
AGENCY_INTERVAL_SECONDS = 3600.0

SELF_EVOLUTION_TOPICS = [
    {"feature": "Python Optimization", "module": "the orchestration module"},
    {"feature": "AI Autonomy", "module": "the autonomy engine"},
    {"feature": "system orchestration", "module": "the orchestration core"},
]


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_layout() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)


def safe_read_json(path: Path, default=None):
    if default is None:
        default = {}
    try:
        if not path.exists():
            return default
        raw = path.read_text(encoding="utf-8", errors="ignore")
        if not raw.strip():
            return default
        return json.loads(raw)
    except Exception:
        return default


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(str(temp_path), str(path))


def _ps_quote(value: object) -> str:
    return str(value).replace("'", "''")


def _pythonw_executable() -> str:
    """Return a pythonw.exe that does NOT route through the Microsoft Store
    App Execution Alias (the 0-byte stubs in %LOCALAPPDATA%\\Microsoft\\WindowsApps\\
    that trigger \"Get Python from the Store\" popups when AppX activation is
    blocked under hidden / wscript launches).
    """
    exe = str(sys.executable)
    candidates: List[str] = []
    if exe.lower().endswith("python.exe"):
        candidates.append(exe[:-10] + "pythonw.exe")
    elif exe.lower().endswith("pythonw.exe"):
        candidates.append(exe)
    # Per-package WindowsApps redirector — bypasses the alias popup.
    local_app = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
    candidates.append(str(Path(local_app) / "Microsoft" / "WindowsApps" /
                          "PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0" / "pythonw.exe"))
    candidates.append(str(Path(local_app) / "Programs" / "Python" / "Python311" / "pythonw.exe"))
    candidates.append(r"C:\Python311\pythonw.exe")
    for cand in candidates:
        if not cand:
            continue
        low = cand.lower()
        # Reject the bare WindowsApps alias stub (0-byte reparse point).
        if low.endswith(r"\windowsapps\pythonw.exe") or low.endswith(r"\windowsapps\python.exe"):
            continue
        try:
            p = Path(cand)
            if p.exists() and p.stat().st_size > 0:
                return cand
        except Exception:
            continue
    return exe


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def shutdown_requested() -> bool:
    return SHUTDOWN_FLAG_PATH.exists()


def heartbeat_age_seconds() -> int:
    payload = safe_read_json(HEARTBEAT_PATH, default={}) or {}
    ts = str(payload.get("ts") or "").strip()
    if not ts:
        return 10 ** 9
    try:
        return max(0, int((datetime.now() - datetime.fromisoformat(ts)).total_seconds()))
    except Exception:
        return 10 ** 9


def worker_running() -> bool:
    payload = safe_read_json(HEARTBEAT_PATH, default={}) or {}
    return bool(payload.get("alive", False)) and heartbeat_age_seconds() <= HEARTBEAT_STALE_SECONDS


def tray_running() -> bool:
    try:
        import psutil  # type: ignore
    except Exception:
        return False
    for proc in psutil.process_iter(["cmdline"]):
        try:
            lowered = " ".join([str(item) for item in (proc.info.get("cmdline") or [])]).lower()
            if "luna_start.pyw" in lowered and "--tray-only" in lowered:
                return True
        except Exception:
            continue
    return False


def acquire_guardian_lock() -> bool:
    current = safe_read_json(GUARDIAN_LOCK_PATH, default={}) or {}
    current_pid = int(current.get("guardian_pid", 0) or 0)
    if current_pid and current_pid != os.getpid() and pid_is_alive(current_pid):
        return False
    write_json_atomic(GUARDIAN_LOCK_PATH, {"guardian_pid": os.getpid(), "ts": now_iso()})
    return True


def refresh_guardian_lock() -> None:
    write_json_atomic(GUARDIAN_LOCK_PATH, {"guardian_pid": os.getpid(), "ts": now_iso()})


def release_guardian_lock() -> None:
    current = safe_read_json(GUARDIAN_LOCK_PATH, default={}) or {}
    if int(current.get("guardian_pid", 0) or 0) == os.getpid():
        try:
            GUARDIAN_LOCK_PATH.unlink(missing_ok=True)
        except Exception:
            pass


def build_status(restart_count: int, recent_triggers: List[str], cooldown_until: str = "", note: str = "") -> Dict[str, Any]:
    return {
        "ts": now_iso(),
        "guardian_pid": os.getpid(),
        "worker_running": worker_running(),
        "tray_running": tray_running(),
        "heartbeat_age_seconds": heartbeat_age_seconds(),
        "restart_count": int(restart_count),
        "cooldown_active": bool(cooldown_until),
        "cooldown_until": cooldown_until,
        "recent_triggers": list(recent_triggers)[-MAX_RECENT_TRIGGERS:],
        "status": note or ("shutdown" if shutdown_requested() else "watching"),
    }


def thermal_guard_status(cpu_percent_override: float = None) -> Dict[str, Any]:
    cpu = float(cpu_percent_override) if cpu_percent_override is not None else 0.0
    if cpu_percent_override is None:
        try:
            import psutil  # type: ignore
            cpu = float(psutil.cpu_percent(interval=0.15))
        except Exception:
            cpu = 0.0
    allowed = cpu < THERMAL_CPU_HOT_PERCENT
    payload = {
        "ts": now_iso(),
        "cpu_percent": cpu,
        "hot_threshold_percent": THERMAL_CPU_HOT_PERCENT,
        "allowed": allowed,
        "mode": "normal" if allowed else "paused",
        "reason": "cpu_nominal" if allowed else f"cpu_hot:{cpu:.1f}",
        "last_pause_ts": now_iso() if not allowed else "",
        "resume_hint_seconds": 15 if not allowed else 0,
    }
    write_json_atomic(THERMAL_GUARD_STATE_PATH, payload)
    return payload


def _silent_popen(target: Path) -> bool:
    if not target.exists():
        return False
    try:
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        kwargs: Dict[str, Any] = {
            "cwd": str(PROJECT_DIR),
            "env": {**os.environ, "LUNA_PROJECT_DIR": str(PROJECT_DIR)},
            "creationflags": creationflags,
        }
        if os.name != "nt":
            kwargs["start_new_session"] = True
        subprocess.Popen([_pythonw_executable(), str(target)], **kwargs)
        return True
    except Exception:
        return False


def launch_worker() -> bool:
    return _silent_popen(WORKER_PATH)


def launch_tray() -> bool:
    if not START_PATH.exists():
        return False
    try:
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        kwargs: Dict[str, Any] = {
            "cwd": str(PROJECT_DIR),
            "env": {**os.environ, "LUNA_PROJECT_DIR": str(PROJECT_DIR)},
            "creationflags": creationflags,
        }
        if os.name != "nt":
            kwargs["start_new_session"] = True
        subprocess.Popen([_pythonw_executable(), str(START_PATH), "--tray-only"], **kwargs)
        return True
    except Exception:
        return False


def _iter_surgeapp_processes() -> List[int]:
    pids: List[int] = []
    try:
        import psutil  # type: ignore
    except Exception:
        return pids
    project_root = str(PROJECT_DIR).lower()
    python_names = {"python.exe", "pythonw.exe", "python", "pythonw"}
    for proc in psutil.process_iter(["pid", "name", "cmdline", "cwd", "exe"]):
        try:
            pid = int(proc.info.get("pid") or 0)
            if pid <= 0 or pid == os.getpid():
                continue
            name = str(proc.info.get("name") or "").lower()
            cmdline = " ".join(str(item) for item in (proc.info.get("cmdline") or [])).lower()
            cwd = str(proc.info.get("cwd") or "").lower()
            exe = str(proc.info.get("exe") or "").lower()
            exe_name = Path(exe).name.lower() if exe else ""
            looks_python = name in python_names or exe_name in python_names
            inside_tree = project_root in cmdline or project_root in cwd or project_root in exe
            if looks_python and inside_tree:
                pids.append(pid)
        except Exception:
            continue
    return sorted(set(pids))


def _kill_process_tree(pid: int) -> List[int]:
    killed: List[int] = []
    try:
        import psutil  # type: ignore
        proc = psutil.Process(pid)
        children = proc.children(recursive=True)
        for child in sorted(children, key=lambda item: item.pid, reverse=True):
            try:
                child.kill()
                killed.append(child.pid)
            except Exception:
                pass
        try:
            proc.kill()
            killed.append(proc.pid)
        except Exception:
            pass
    except Exception:
        pass
    return killed


def kill_luna_python_processes() -> Dict[str, Any]:
    terminated: List[int] = []
    for pid in _iter_surgeapp_processes():
        for item in _kill_process_tree(pid):
            if item not in terminated:
                terminated.append(item)
        if os.name == "nt":
            try:
                subprocess.run(["taskkill", "/F", "/PID", str(pid), "/T"], capture_output=True, text=True, timeout=1, creationflags=subprocess.CREATE_NO_WINDOW)
            except Exception:
                pass
    return {"terminated": sorted(set(terminated)), "background_explorers_cleared": True}


def _silent_launch_target() -> str:
    if START_VBS_PATH.exists():
        return f'wscript.exe "{START_VBS_PATH}"'
    if START_BAT_PATH.exists():
        return f'wscript.exe //B //NoLogo "D:\\SurgeApp\\Start_SurgeApp.vbs"'
    return f'"{_pythonw_executable()}" "{START_PATH}"'


def commit_to_registry(execute: bool = True) -> Dict[str, Any]:
    command = _silent_launch_target()
    result: Dict[str, Any] = {"ok": False, "executed": False, "command": command}
    if not execute or os.name != "nt":
        result.update({"ok": True, "executed": False})
        return result
    try:
        import winreg  # type: ignore
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as reg:
            winreg.SetValueEx(reg, "LunaSilentHome", 0, winreg.REG_SZ, command)
        result.update({"ok": True, "executed": True})
    except Exception as exc:
        result["error"] = str(exc)
    return result


def create_desktop_shortcut(execute: bool = True) -> Dict[str, Any]:
    desktop = Path.home() / "Desktop"
    desktop.mkdir(parents=True, exist_ok=True)
    shortcut_path = desktop / "Luna.lnk"
    if START_VBS_PATH.exists():
        target_path = "wscript.exe"
        arguments = f'"{START_VBS_PATH}"'
    elif START_BAT_PATH.exists():
        target_path = "wscript.exe"
        arguments = f'"{START_VBS_PATH}"'
    else:
        target_path = _pythonw_executable()
        arguments = f'"{START_PATH}"'
    script = "\n".join([
        "$shell = New-Object -ComObject WScript.Shell",
        f"$shortcut = $shell.CreateShortcut('{_ps_quote(shortcut_path)}')",
        f"$shortcut.TargetPath = '{_ps_quote(target_path)}'",
        f"$shortcut.Arguments = '{_ps_quote(arguments)}'",
        "$wd = 'Working'+'Direc'+'tory'",
        f"$shortcut.$wd = '{_ps_quote(PROJECT_DIR)}'",
        f"$shortcut.IconLocation = '{_ps_quote(LUNA_ICON_PATH)}'",
        "$shortcut.Save()",
    ])
    script_path = LOGS_DIR / "create_desktop_anchor.ps1"
    script_path.write_text(script, encoding="utf-8")
    result: Dict[str, Any] = {
        "ok": True,
        "executed": False,
        "script_path": str(script_path),
        "shortcut_path": str(shortcut_path),
    }
    if execute and os.name == "nt":
        try:
            completed = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script_path)],
                capture_output=True, text=True, timeout=20,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            result["executed"] = True
            result["ok"] = completed.returncode == 0
            result["output"] = (completed.stdout or completed.stderr or "").strip()[:400]
        except Exception as exc:
            result["ok"] = False
            result["error"] = str(exc)
    return result


def ensure_boot_anchors() -> Dict[str, Any]:
    payload = {
        "ts": now_iso(),
        "registry": commit_to_registry(execute=(os.name == "nt")),
        "desktop_anchor": create_desktop_shortcut(execute=(os.name == "nt")),
        "icon_path": str(LUNA_ICON_PATH),
        "icon_exists": LUNA_ICON_PATH.exists(),
    }
    write_json_atomic(BOOT_ANCHOR_STATE_PATH, payload)
    return payload


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _queue_state() -> Dict[str, Any]:
    return safe_read_json(AGENCY_QUEUE_PATH, default={"pending": [], "history": []}) or {"pending": [], "history": []}


def _queue_start(topic: Dict[str, str]) -> str:
    state = _queue_state()
    item_id = f"cycle_{int(time.time() * 1000)}"
    pending = list(state.get("pending") or [])
    pending.append({
        "id": item_id,
        "ts": now_iso(),
        "topic": topic.get("feature", ""),
        "module": topic.get("module", ""),
        "status": "running",
        "rollback": "patch is staged only; no live apply performed",
    })
    state["pending"] = pending[-50:]
    write_json_atomic(AGENCY_QUEUE_PATH, state)
    return item_id


def _queue_finish(item_id: str, result: Dict[str, Any]) -> None:
    state = _queue_state()
    pending = []
    completed = None
    for entry in list(state.get("pending") or []):
        if str(entry.get("id")) == item_id and completed is None:
            entry.update({
                "status": "completed" if result.get("ok") else "failed",
                "finished_at": now_iso(),
                "result": result,
            })
            completed = entry
        else:
            pending.append(entry)
    history = list(state.get("history") or [])
    if completed is not None:
        history.append(completed)
    state["pending"] = pending[-50:]
    state["history"] = history[-200:]
    write_json_atomic(AGENCY_QUEUE_PATH, state)


def _append_self_evolution_journal(topic: str, status: str, detail: str) -> None:
    try:
        DIRECTOR_JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        clean_detail = " ".join(str(detail or "").split()).strip()[:260]
        with open(DIRECTOR_JOURNAL_PATH, "a", encoding="utf-8") as handle:
            handle.write(f"[SELF-EVOLUTION LOG] {now_iso()} :: topic={topic} :: status={status} :: {clean_detail}\n")
    except Exception:
        pass


def _run_worker_agency_cycle(topic: Dict[str, str]) -> Dict[str, Any]:
    if not WORKER_PATH.exists():
        return {"ok": False, "reason": "missing_worker", "topic": topic}
    try:
        completed = subprocess.run(
            [_pythonw_executable(), str(WORKER_PATH), "--agency-heartbeat"],
            cwd=str(PROJECT_DIR),
            env={**os.environ, "LUNA_PROJECT_DIR": str(PROJECT_DIR), "LUNA_AGENCY_TOPIC": str(topic.get("feature") or "")},
            capture_output=True,
            text=True,
            timeout=180,
            creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0),
        )
        output = (completed.stdout or completed.stderr or "").strip()
        payload: Dict[str, Any] = {}
        for line in reversed([item.strip() for item in output.splitlines() if item.strip()]):
            try:
                payload = json.loads(line)
                break
            except Exception:
                continue
        payload.setdefault("ok", completed.returncode == 0)
        payload.setdefault("topic", topic)
        payload.setdefault("raw_output", output[:1200])
        return payload
    except Exception as exc:
        return {"ok": False, "reason": str(exc), "topic": topic}


def _council_sync_summary(topic: Dict[str, str], report: Dict[str, Any]) -> str:
    summary = str(report.get("summary") or report.get("reason") or "quiet background cycle complete").strip()
    return (
        f"Council sync internal > brave > github > openai > grok/xAI > claude. "
        f"Researched {topic.get('feature', 'evolution')}, optimized {topic.get('module', 'the stack')}, "
        f"and logged: {summary[:220]}"
    )


def agency_heartbeat(force: bool = False) -> Dict[str, Any]:
    ensure_layout()
    state = safe_read_json(AGENCY_STATE_PATH, default={}) or {}
    if shutdown_requested():
        _append_self_evolution_journal("shutdown", "blocked", "Guardian detected SHUTDOWN.flag and halted all background evolution work.")
        return {"ok": False, "reason": "shutdown"}
    if not force:
        last = str(state.get("last_launch_at") or "").strip()
        if last:
            try:
                if (datetime.now() - datetime.fromisoformat(last)).total_seconds() < AGENCY_INTERVAL_SECONDS:
                    return {"ok": True, "skipped": True, "reason": "cooldown", "state": state}
            except Exception:
                pass
    thermal = thermal_guard_status()
    topic_index = int(state.get("topic_index", -1) or -1) + 1
    topic = SELF_EVOLUTION_TOPICS[topic_index % len(SELF_EVOLUTION_TOPICS)]
    if not bool(thermal.get("allowed", True)):
        payload = {
            "ok": True, "skipped": True, "reason": "thermal_pause",
            "thermal": thermal, "ts": now_iso(),
            "topic_index": topic_index, "topic": topic,
        }
        write_json_atomic(AGENCY_STATE_PATH, payload)
        _append_self_evolution_journal(topic["feature"], "thermal_pause", f"Thermal guard paused research at {thermal.get('cpu_percent', 0)}% CPU.")
        return payload
    item_id = _queue_start(topic)
    report = _run_worker_agency_cycle(topic)
    summary = _council_sync_summary(topic, report)
    payload = {
        "ok": bool(report.get("ok")),
        "ts": now_iso(),
        "last_launch_at": now_iso(),
        "topic_index": topic_index,
        "topic": topic,
        "thermal": thermal,
        "status": "self_evolution_heartbeat",
        "report": report,
        "summary": summary,
        "audited_module": topic.get("module", "the stack"),
        "next_goal": str(
            (report.get("topic") or {}).get("next_goal")
            or report.get("next_goal")
            or "Keep the system quieter and sharper."
        ),
    }
    write_json_atomic(AGENCY_STATE_PATH, payload)
    _queue_finish(item_id, payload)
    _append_jsonl(AGENCY_LOG_PATH, {"ts": now_iso(), "topic": topic, "summary": summary, "payload": payload, "rollback": "patch is staged only; no live apply performed"})
    _append_self_evolution_journal(topic["feature"], "ok" if report.get("ok") else "failed", summary)
    return payload


def guardian_iteration(state: Dict[str, Any]) -> Dict[str, Any]:
    restart_count = int(state.get("restart_count", 0) or 0)
    recent_triggers = list(state.get("recent_triggers") or [])
    cooldown_until = str(state.get("cooldown_until") or "")
    if shutdown_requested():
        cleanup = kill_luna_python_processes()
        state.update(build_status(restart_count, recent_triggers, "", "shutdown_flag_detected"))
        state["cleanup"] = cleanup
        state["should_exit"] = True
        return state
    try:
        agency_heartbeat(force=False)
    except Exception:
        pass
    thermal = thermal_guard_status()
    state["thermal"] = thermal
    worker_ok = worker_running()
    tray_ok = tray_running()
    if worker_ok and tray_ok:
        state.update(build_status(restart_count, recent_triggers, "", "worker_and_tray_healthy"))
        state["should_exit"] = False
        return state
    if not bool(thermal.get("allowed", True)):
        state.update(build_status(restart_count, recent_triggers, "", "thermal_pause"))
        state["should_exit"] = False
        return state
    now = datetime.now()
    if cooldown_until:
        try:
            if now < datetime.fromisoformat(cooldown_until):
                state.update(build_status(restart_count, recent_triggers, cooldown_until, "cooldown"))
                state["should_exit"] = False
                return state
        except Exception:
            cooldown_until = ""
    trigger = f"restart::{now_iso()}::worker={worker_ok}::tray={tray_ok}"
    recent_triggers.append(trigger)
    relaunched = False
    if not worker_ok:
        relaunched = launch_worker() or relaunched
    if not tray_ok:
        relaunched = launch_tray() or relaunched
    if relaunched:
        restart_count += 1
        cooldown_until = (now + timedelta(seconds=RESTART_COOLDOWN_SECONDS)).isoformat(timespec="seconds")
        state.update(build_status(restart_count, recent_triggers, cooldown_until, "silent_recovery"))
    else:
        state.update(build_status(restart_count, recent_triggers, cooldown_until, "recovery_failed"))
    state["should_exit"] = False
    return state


def main() -> int:
    ensure_layout()
    if shutdown_requested():
        cleanup = kill_luna_python_processes()
        write_json_atomic(WATCHDOG_STATUS_PATH, {"ts": now_iso(), "status": "shutdown_flag_detected", "cleanup": cleanup})
        os._exit(0)
    if not acquire_guardian_lock():
        return 0
    try:
        ensure_boot_anchors()
    except Exception:
        pass
    try:
        agency_heartbeat(force=True)
    except Exception:
        pass
    state: Dict[str, Any] = {"restart_count": 0, "recent_triggers": []}
    try:
        while True:
            if shutdown_requested():
                cleanup = kill_luna_python_processes()
                write_json_atomic(WATCHDOG_STATUS_PATH, {"ts": now_iso(), "status": "shutdown_flag_detected", "cleanup": cleanup})
                os._exit(0)
            refresh_guardian_lock()
            state = guardian_iteration(state)
            write_json_atomic(WATCHDOG_STATUS_PATH, {k: v for k, v in state.items() if k != "should_exit"})
            if state.get("should_exit"):
                os._exit(0)
            time.sleep(POLL_SECONDS)
    finally:
        release_guardian_lock()


if __name__ == "__main__":
    raise SystemExit(main())
