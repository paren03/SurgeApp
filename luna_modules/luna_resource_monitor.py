"""Phase 5J: Luna Resource Awareness + Hibernation foundation.

Stdlib only (psutil used opportunistically if already installed; no install).
Read-mostly. The module senses local machine pressure and recommends a mode
(normal / light / pause_high_intensity / hibernate / blocked). It does NOT:
  * actually pause, hibernate, kill, or throttle any process,
  * create or remove stop flags,
  * touch queues, logs, backups, uploads, memory content,
  * call cloud APIs (only localhost Ollama endpoints).

Tracked schema/policy:
  memory/luna_resource_monitor.schema.json
  memory/luna_hibernation.schema.json
  memory/luna_resource_policy.json

Generated runtime artifacts (gitignored):
  memory/luna_resource_status.json
  memory/luna_resource_status.md
  memory/luna_hibernation_plan.json
  memory/luna_hibernation_report.md
  memory/luna_hardware_profile.json
  memory/luna_resource_monitor_build_report.json

CLI:
  python -m luna_modules.luna_resource_monitor --self-test
  python -m luna_modules.luna_resource_monitor --snapshot
  python -m luna_modules.luna_resource_monitor --write
  python -m luna_modules.luna_resource_monitor --print-markdown
  python -m luna_modules.luna_resource_monitor --hibernation-plan
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import platform as _platform
import shutil
import socket
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

# Optional psutil — never installed by this module.
try:  # pragma: no cover
    import psutil as _psutil  # type: ignore
except Exception:  # pragma: no cover
    _psutil = None

VALID_MODES = ("normal", "light", "pause_high_intensity", "hibernate", "blocked")
VALID_STATUSES = ("excellent", "healthy", "watch", "degraded", "blocked", "unknown")

_DEFAULT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "disk_free_min_gb": 2.0,
    "disk_free_warn_gb": 5.0,
    "memory_free_min_percent": 10,
    "memory_free_warn_percent": 20,
    "gpu_free_min_percent": 10,
    "gpu_free_warn_percent": 20,
    "max_aider_active_jobs": 1,
    "max_aider_child_processes": 1,
    "max_total_log_mb_warn": 512,
    "max_largest_log_mb_warn": 100,
    "idle_hibernation_minutes": 30,
    "high_intensity_allowed_modes": ["normal"],
    "light_mode_allowed_tasks": [
        "read_only",
        "memory_index",
        "scorecard",
        "playbook_match",
        "file_map_refresh",
    ],
    "blocked_mode_forbidden_tasks": [
        "aider_patch",
        "self_upgrade",
        "package_install",
        "multi_file_refactor",
    ],
    "default_check_seconds": 60,
    "ollama_localhost_endpoints": [
        "http://127.0.0.1:11434/api/tags",
        "http://127.0.0.1:11434/api/ps",
    ],
}

DEFAULT_POLICY_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_resource_policy.json"


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def clamp_percent(value: Any) -> int:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0
    if v < 0:
        return 0
    if v > 100:
        return 100
    return int(round(v))


def bytes_to_gb(value: Any) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    return round(v / (1024 ** 3), 3)


def read_json(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return default
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError, UnicodeDecodeError):
        return default


def write_json_atomic(path: Path | str, data: Any) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=False), encoding="utf-8")
    os.replace(tmp, p)
    return p


def load_policy(policy_path: Path | None = None) -> dict[str, Any]:
    p = policy_path or DEFAULT_POLICY_PATH
    cfg = read_json(p, default=None)
    if not isinstance(cfg, dict):
        merged = dict(_DEFAULT_POLICY)
        merged["_source"] = "module_fallback"
        merged["_loaded_from_file"] = False
        return merged
    out = dict(_DEFAULT_POLICY)
    for k, v in cfg.items():
        out[k] = v
    out["_source"] = str(p)
    out["_loaded_from_file"] = True
    return out


def _status_for_band(value: float, *, excellent_min: float, healthy_min: float, watch_min: float, blocked_max: float) -> str:
    if value >= excellent_min:
        return "excellent"
    if value >= healthy_min:
        return "healthy"
    if value >= watch_min:
        return "watch"
    if value >= blocked_max:
        return "degraded"
    return "blocked"


# ---------- per-resource sensors ----------


def disk_usage_status(path: Path | str, policy: dict[str, Any] | None = None) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    p = Path(path)
    try:
        usage = shutil.disk_usage(str(p))
        total_gb = bytes_to_gb(usage.total)
        free_gb = bytes_to_gb(usage.free)
        free_pct = (usage.free / usage.total * 100.0) if usage.total else 0.0
        if free_gb < float(pol.get("disk_free_min_gb", 2.0)):
            status = "blocked"
        elif free_gb < float(pol.get("disk_free_warn_gb", 5.0)):
            status = "watch"
        elif free_pct >= 30:
            status = "healthy"
        else:
            status = "degraded"
        return {
            "project_drive_total_gb": total_gb,
            "project_drive_free_gb": free_gb,
            "project_drive_free_percent": clamp_percent(free_pct),
            "status": status,
        }
    except OSError as e:
        return {
            "project_drive_total_gb": 0.0,
            "project_drive_free_gb": 0.0,
            "project_drive_free_percent": 0,
            "status": "unknown",
            "error": f"{type(e).__name__}:{e}",
        }


def memory_status(policy: dict[str, Any] | None = None) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    if _psutil is not None:
        try:
            vm = _psutil.virtual_memory()
            total_gb = bytes_to_gb(vm.total)
            available_gb = bytes_to_gb(vm.available)
            available_pct = (vm.available / vm.total * 100.0) if vm.total else 0.0
            return _annotate_mem_status(total_gb, available_gb, available_pct, pol, "psutil")
        except Exception:
            pass
    if _platform.system().lower().startswith("win"):
        try:
            ps_cmd = (
                "Get-CimInstance Win32_OperatingSystem | "
                "Select-Object FreePhysicalMemory,TotalVisibleMemorySize | "
                "ConvertTo-Json -Compress"
            )
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_cmd],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                obj = json.loads(proc.stdout.strip())
                total_kb = safe_int(obj.get("TotalVisibleMemorySize"))
                free_kb = safe_int(obj.get("FreePhysicalMemory"))
                total_gb = round(total_kb / (1024 * 1024), 3)
                available_gb = round(free_kb / (1024 * 1024), 3)
                available_pct = (free_kb / total_kb * 100.0) if total_kb else 0.0
                return _annotate_mem_status(total_gb, available_gb, available_pct, pol, "win_cim")
        except (subprocess.TimeoutExpired, OSError, ValueError, FileNotFoundError):
            pass
    return {
        "total_gb": 0.0,
        "available_gb": 0.0,
        "available_percent": 0,
        "status": "unknown",
        "source": "fallback",
    }


def _annotate_mem_status(
    total_gb: float, available_gb: float, available_pct: float, policy: dict[str, Any], source: str
) -> dict[str, Any]:
    pct = clamp_percent(available_pct)
    if pct < int(policy.get("memory_free_min_percent", 10)):
        status = "blocked"
    elif pct < int(policy.get("memory_free_warn_percent", 20)):
        status = "watch"
    elif pct >= 50:
        status = "healthy"
    else:
        status = "degraded"
    return {
        "total_gb": total_gb,
        "available_gb": available_gb,
        "available_percent": pct,
        "status": status,
        "source": source,
    }


def cpu_status(policy: dict[str, Any] | None = None) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    if _psutil is not None:
        try:
            usage = float(_psutil.cpu_percent(interval=None))
            return _annotate_cpu(usage, "psutil")
        except Exception:
            pass
    try:
        load1, load5, load15 = os.getloadavg()  # type: ignore[attr-defined]
        cpu_count = os.cpu_count() or 1
        usage = (load1 / cpu_count) * 100.0
        return _annotate_cpu(usage, "loadavg")
    except (AttributeError, OSError):
        pass
    if _platform.system().lower().startswith("win"):
        try:
            ps_cmd = (
                "(Get-CimInstance Win32_Processor | "
                "Measure-Object -Property LoadPercentage -Average).Average"
            )
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_cmd],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                usage = float(proc.stdout.strip())
                return _annotate_cpu(usage, "win_cim")
        except (subprocess.TimeoutExpired, OSError, ValueError, FileNotFoundError):
            pass
    return {
        "usage_percent": 0,
        "load_status": "unknown",
        "source": "fallback",
    }


def _annotate_cpu(usage: float, source: str) -> dict[str, Any]:
    pct = clamp_percent(usage)
    if pct >= 95:
        load = "blocked"
    elif pct >= 85:
        load = "degraded"
    elif pct >= 70:
        load = "watch"
    elif pct >= 25:
        load = "healthy"
    else:
        load = "excellent"
    return {"usage_percent": pct, "load_status": load, "source": source}


def gpu_status(policy: dict[str, Any] | None = None) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    base: dict[str, Any] = {
        "detected": False,
        "name": "",
        "total_vram_gb": 0.0,
        "used_vram_gb": 0.0,
        "free_vram_gb": 0.0,
        "free_vram_percent": 0,
        "status": "unknown",
        "source": "none",
    }
    nvidia_smi = shutil.which("nvidia-smi")
    if nvidia_smi:
        try:
            proc = subprocess.run(
                [
                    nvidia_smi,
                    "--query-gpu=name,memory.total,memory.used,memory.free",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                first_line = proc.stdout.strip().splitlines()[0]
                parts = [p.strip() for p in first_line.split(",")]
                if len(parts) >= 4:
                    name = parts[0]
                    total_mb = float(parts[1] or 0)
                    used_mb = float(parts[2] or 0)
                    free_mb = float(parts[3] or 0)
                    total_gb = round(total_mb / 1024, 3)
                    used_gb = round(used_mb / 1024, 3)
                    free_gb = round(free_mb / 1024, 3)
                    free_pct = (free_mb / total_mb * 100.0) if total_mb else 0.0
                    return _annotate_gpu(name, total_gb, used_gb, free_gb, free_pct, pol, "nvidia_smi")
        except (subprocess.TimeoutExpired, OSError, ValueError, FileNotFoundError):
            pass
    if _platform.system().lower().startswith("win"):
        try:
            ps_cmd = (
                "Get-CimInstance Win32_VideoController | "
                "Select-Object Name,AdapterRAM | ConvertTo-Json -Compress"
            )
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_cmd],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                raw = proc.stdout.strip()
                obj = json.loads(raw)
                if isinstance(obj, list) and obj:
                    obj = obj[0]
                if isinstance(obj, dict):
                    name = str(obj.get("Name", "") or "")
                    adapter_ram = safe_int(obj.get("AdapterRAM"))
                    total_gb = bytes_to_gb(adapter_ram)
                    return _annotate_gpu(name, total_gb, 0.0, total_gb, 100.0, pol, "win_cim")
        except (subprocess.TimeoutExpired, OSError, ValueError, FileNotFoundError):
            pass
    return base


def _annotate_gpu(
    name: str,
    total_gb: float,
    used_gb: float,
    free_gb: float,
    free_pct: float,
    policy: dict[str, Any],
    source: str,
) -> dict[str, Any]:
    pct = clamp_percent(free_pct)
    if pct < int(policy.get("gpu_free_min_percent", 10)):
        status = "blocked"
    elif pct < int(policy.get("gpu_free_warn_percent", 20)):
        status = "watch"
    elif pct >= 50:
        status = "healthy"
    else:
        status = "degraded"
    return {
        "detected": True,
        "name": name,
        "total_vram_gb": total_gb,
        "used_vram_gb": used_gb,
        "free_vram_gb": free_gb,
        "free_vram_percent": pct,
        "status": status,
        "source": source,
    }


def _http_localhost_get(url: str, timeout: float = 3.0) -> dict[str, Any]:
    if not (url.startswith("http://127.0.0.1") or url.startswith("http://localhost")):
        return {"ok": False, "error": "non_localhost_blocked"}
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            data = resp.read(65536).decode("utf-8", errors="replace")
            try:
                return {"ok": True, "json": json.loads(data)}
            except ValueError:
                return {"ok": True, "text": data}
    except (urllib.error.URLError, socket.timeout, OSError, ValueError) as e:
        return {"ok": False, "error": f"{type(e).__name__}:{e}"}


def ollama_status(api_base: str = "http://127.0.0.1:11434", policy: dict[str, Any] | None = None) -> dict[str, Any]:
    if not api_base.startswith("http://127.0.0.1") and not api_base.startswith("http://localhost"):
        return {
            "api_reachable": False,
            "loaded_models": [],
            "status": "blocked",
            "source": "non_localhost_blocked",
        }
    tags = _http_localhost_get(api_base.rstrip("/") + "/api/tags", timeout=3.0)
    ps = _http_localhost_get(api_base.rstrip("/") + "/api/ps", timeout=3.0)
    reachable = tags.get("ok", False)
    loaded: list[str] = []
    if isinstance(ps.get("json"), dict):
        models = ps["json"].get("models") or []
        for m in models:
            name = m.get("name") if isinstance(m, dict) else None
            if isinstance(name, str):
                loaded.append(name)
    if reachable and loaded:
        status = "healthy"
    elif reachable:
        status = "watch"
    else:
        status = "degraded"
    return {
        "api_reachable": bool(reachable),
        "loaded_models": loaded,
        "status": status,
        "source": "localhost_http",
    }


# ---------- pressure helpers ----------


def _logical_pids_psutil(name_substring: str) -> int:
    if _psutil is None:
        return 0
    try:
        count = 0
        for proc in _psutil.process_iter(attrs=["name", "cmdline"]):
            try:
                cmd = " ".join(proc.info.get("cmdline") or [])
            except Exception:
                cmd = ""
            if name_substring.lower() in cmd.lower():
                count += 1
        return count
    except Exception:
        return 0


def process_pressure(project_dir: Path | str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "luna_process_count": 0,
        "worker_main_logical": 0,
        "worker_cu_logical": 0,
        "aider_bridge_logical": 0,
        "aider_child_count": 0,
        "status": "unknown",
        "source": "fallback",
    }
    if _psutil is not None:
        try:
            wm = _logical_pids_psutil("worker.py")
            ab = _logical_pids_psutil("aider_bridge.py")
            achild = _logical_pids_psutil("python.exe -m aider")
            out["worker_main_logical"] = wm
            out["aider_bridge_logical"] = ab
            out["aider_child_count"] = max(0, achild)
            out["luna_process_count"] = wm + ab + achild
            out["source"] = "psutil"
            out["status"] = "watch" if (achild > 1 or ab > 1) else "healthy"
            return out
        except Exception:
            pass
    if _platform.system().lower().startswith("win"):
        try:
            ps_cmd = (
                "Get-CimInstance Win32_Process | "
                "Where-Object { $_.CommandLine -match 'worker.py|aider_bridge.py|aider' } | "
                "Select-Object Name,CommandLine | ConvertTo-Json -Compress"
            )
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_cmd],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                raw = proc.stdout.strip()
                try:
                    items = json.loads(raw)
                except ValueError:
                    items = []
                if isinstance(items, dict):
                    items = [items]
                wm = ab = achild = 0
                for it in items or []:
                    cl = (it.get("CommandLine") or "").lower()
                    if "worker.py" in cl:
                        wm += 1
                    if "aider_bridge.py" in cl:
                        ab += 1
                    if "-m aider" in cl or cl.endswith(" aider"):
                        achild += 1
                out.update(
                    worker_main_logical=wm,
                    aider_bridge_logical=ab,
                    aider_child_count=achild,
                    luna_process_count=wm + ab + achild,
                    source="win_cim",
                    status="watch" if (achild > 1 or ab > 1) else "healthy",
                )
                return out
        except (subprocess.TimeoutExpired, OSError, ValueError, FileNotFoundError):
            pass
    return out


def _count_dir(p: Path) -> int:
    if not p.is_dir():
        return 0
    try:
        return sum(1 for _ in p.iterdir())
    except OSError:
        return -1


def queue_pressure(project_dir: Path | str, policy: dict[str, Any] | None = None) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    pdir = Path(project_dir)
    aider_jobs = pdir / "aider_jobs"
    tasks = pdir / "tasks"
    info = {
        "tasks_active": _count_dir(tasks / "active"),
        "tasks_done": _count_dir(tasks / "done"),
        "tasks_failed": _count_dir(tasks / "failed"),
        "aider_active": _count_dir(aider_jobs / "active"),
        "aider_done": _count_dir(aider_jobs / "completed"),
        "aider_failed": _count_dir(aider_jobs / "failed"),
        "aider_quarantine": _count_dir(aider_jobs / "quarantine"),
    }
    status = "healthy"
    if info["aider_active"] > int(pol.get("max_aider_active_jobs", 1)):
        status = "watch"
    if (info["aider_failed"] + info["aider_quarantine"]) >= 5:
        status = "degraded"
    info["status"] = status
    return info


def log_pressure(project_dir: Path | str, policy: dict[str, Any] | None = None) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    pdir = Path(project_dir)
    logs = pdir / "logs"
    largest: list[dict[str, Any]] = []
    total = 0
    if logs.is_dir():
        try:
            entries = []
            for p in logs.glob("**/*"):
                if p.is_file():
                    try:
                        sz = p.stat().st_size
                    except OSError:
                        continue
                    total += sz
                    entries.append((sz, p))
            entries.sort(reverse=True, key=lambda x: x[0])
            for sz, p in entries[:5]:
                largest.append(
                    {
                        "path": str(p.relative_to(pdir)).replace("\\", "/"),
                        "size_mb": round(sz / (1024 * 1024), 3),
                    }
                )
        except OSError:
            pass
    total_mb = round(total / (1024 * 1024), 3)
    status = "healthy"
    if total_mb >= float(pol.get("max_total_log_mb_warn", 512)):
        status = "watch"
    if largest and largest[0]["size_mb"] >= float(pol.get("max_largest_log_mb_warn", 100)):
        status = "watch" if status == "healthy" else "degraded"
    return {
        "largest_files": largest,
        "total_log_bytes": total,
        "total_log_mb": total_mb,
        "status": status,
    }


# ---------- snapshot + classify ----------


def build_hardware_profile(project_dir: Path | str) -> dict[str, Any]:
    pdir = Path(project_dir)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "project_dir": str(pdir).replace("\\", "/"),
        "host": _platform.node(),
        "platform": _platform.platform(),
        "machine": _platform.machine(),
        "processor": _platform.processor(),
        "python_version": sys.version.split()[0],
        "python_executable": sys.executable.replace("\\", "/"),
        "cpu_count_logical": os.cpu_count() or 0,
        "psutil_available": _psutil is not None,
    }


def build_resource_snapshot(
    project_dir: Path | str,
    policy: dict[str, Any] | None = None,
    *,
    fakes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the snapshot. `fakes` is for tests — pre-supplies any section."""
    pol = policy or load_policy()
    pdir = Path(project_dir)
    fakes = fakes or {}
    snap: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "project_dir": str(pdir).replace("\\", "/"),
        "host": _platform.node(),
        "platform": _platform.platform(),
        "python_executable": sys.executable.replace("\\", "/"),
        "disk": fakes.get("disk") or disk_usage_status(pdir, pol),
        "memory": fakes.get("memory") or memory_status(pol),
        "cpu": fakes.get("cpu") or cpu_status(pol),
        "gpu": fakes.get("gpu") or gpu_status(pol),
        "ollama": fakes.get("ollama") or ollama_status(policy=pol),
        "processes": fakes.get("processes") or process_pressure(pdir),
        "queues": fakes.get("queues") or queue_pressure(pdir, pol),
        "logs": fakes.get("logs") or log_pressure(pdir, pol),
        "blockers": [],
        "warnings": [],
        "recommended_mode": "normal",
    }
    decision = classify_resource_state(snap, pol)
    snap["recommended_mode"] = decision["mode"]
    snap["blockers"] = list(decision.get("blockers") or [])
    snap["warnings"] = list(decision.get("warnings") or [])
    return snap


def validate_resource_snapshot(snap: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(snap, dict):
        return False, ["snapshot not a dict"]
    for k in (
        "schema_version",
        "generated_at",
        "project_dir",
        "host",
        "platform",
        "python_executable",
        "disk",
        "memory",
        "cpu",
        "gpu",
        "ollama",
        "processes",
        "queues",
        "logs",
        "blockers",
        "warnings",
        "recommended_mode",
    ):
        if k not in snap:
            errors.append(f"snapshot.{k} missing")
    if snap.get("recommended_mode") not in VALID_MODES:
        errors.append(f"recommended_mode invalid: {snap.get('recommended_mode')!r}")
    return (not errors), errors


def classify_resource_state(
    snapshot: dict[str, Any],
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    blockers: list[str] = []
    warnings: list[str] = []
    reasons: list[str] = []
    disk = snapshot.get("disk") or {}
    mem = snapshot.get("memory") or {}
    cpu = snapshot.get("cpu") or {}
    gpu = snapshot.get("gpu") or {}
    ollama = snapshot.get("ollama") or {}
    processes = snapshot.get("processes") or {}
    queues = snapshot.get("queues") or {}
    logs = snapshot.get("logs") or {}

    disk_free_gb = float(disk.get("project_drive_free_gb", 0) or 0)
    if disk_free_gb < float(pol.get("disk_free_min_gb", 2.0)):
        blockers.append(f"disk free {disk_free_gb}GB below floor")
    elif disk_free_gb < float(pol.get("disk_free_warn_gb", 5.0)):
        warnings.append(f"disk free {disk_free_gb}GB below warn")

    mem_pct = int(mem.get("available_percent", 0) or 0)
    if mem.get("source") not in ("fallback",) and mem_pct < int(pol.get("memory_free_min_percent", 10)):
        blockers.append(f"memory free {mem_pct}% below floor")
    elif mem.get("source") not in ("fallback",) and mem_pct < int(pol.get("memory_free_warn_percent", 20)):
        warnings.append(f"memory free {mem_pct}% below warn")

    gpu_low = False
    if gpu.get("detected"):
        gpu_pct = int(gpu.get("free_vram_percent", 0) or 0)
        if gpu_pct < int(pol.get("gpu_free_min_percent", 10)):
            blockers.append(f"GPU free {gpu_pct}% below floor")
            gpu_low = True
        elif gpu_pct < int(pol.get("gpu_free_warn_percent", 20)):
            warnings.append(f"GPU free {gpu_pct}% below warn")
            gpu_low = True

    cpu_status_v = cpu.get("load_status")
    if cpu_status_v == "blocked":
        blockers.append("cpu usage at blocked threshold")
    elif cpu_status_v == "degraded":
        warnings.append("cpu usage degraded")

    if not ollama.get("api_reachable"):
        warnings.append("ollama localhost API unreachable")

    aider_active = int(queues.get("aider_active", 0) or 0)
    if aider_active > int(pol.get("max_aider_active_jobs", 1)):
        warnings.append(
            f"aider_active={aider_active} exceeds max {pol.get('max_aider_active_jobs', 1)}"
        )
    aider_child = int(processes.get("aider_child_count", 0) or 0)
    if aider_child > int(pol.get("max_aider_child_processes", 1)):
        warnings.append(f"aider child count {aider_child} above policy")

    log_status = logs.get("status")
    if log_status in ("watch", "degraded"):
        warnings.append(f"log volume status: {log_status}")

    if blockers:
        mode = "blocked"
        status = "blocked"
        reasons.append("hard blocker present")
    elif warnings and len(warnings) >= 2:
        mode = "pause_high_intensity"
        status = "degraded"
        reasons.append("multiple warnings — pause high-intensity work")
    elif gpu_low or not ollama.get("api_reachable"):
        mode = "light"
        status = "watch"
        reasons.append("GPU or model availability degraded — read-only/light tasks only")
    elif warnings:
        mode = "normal"
        status = "watch"
        reasons.append("single warning — keep normal but watch")
    else:
        mode = "normal"
        status = "healthy"
        reasons.append("all sensors within healthy range")

    forbidden = list(pol.get("blocked_mode_forbidden_tasks", []))
    if mode == "blocked":
        allowed = ["read_only"]
    elif mode == "hibernate":
        allowed = ["read_only"]
    elif mode == "pause_high_intensity":
        allowed = list(pol.get("light_mode_allowed_tasks", []))
    elif mode == "light":
        allowed = list(pol.get("light_mode_allowed_tasks", []))
    else:
        allowed = list(pol.get("light_mode_allowed_tasks", [])) + [
            "aider_patch",
            "self_upgrade",
            "multi_file_refactor",
        ]
    return {
        "mode": mode,
        "status": status,
        "allowed_task_classes": allowed,
        "forbidden_task_classes": forbidden if mode in ("blocked", "pause_high_intensity") else [],
        "reasons": reasons,
        "warnings": warnings,
        "blockers": blockers,
        "next_check_seconds": int(pol.get("default_check_seconds", 60)),
    }


def recommend_resource_action(snapshot: dict[str, Any], policy: dict[str, Any] | None = None) -> dict[str, Any]:
    decision = classify_resource_state(snapshot, policy or load_policy())
    advice: dict[str, Any] = {
        "mode": decision["mode"],
        "status": decision["status"],
        "operator_action": [],
        "luna_action": [],
    }
    if decision["mode"] == "blocked":
        advice["operator_action"].append("free disk/memory/GPU before resuming Luna")
        advice["luna_action"].append("refuse self-upgrade jobs; remain read-only")
    elif decision["mode"] == "pause_high_intensity":
        advice["luna_action"].append("pause aider/multi-file work; keep read-only loops")
    elif decision["mode"] == "light":
        advice["luna_action"].append("scorecard / playbook / index only")
    elif decision["mode"] == "hibernate":
        advice["luna_action"].append("propose hibernation plan; do not act")
    else:
        advice["luna_action"].append("proceed normally with safety gates")
    return advice


# ---------- hibernation plan ----------


def build_hibernation_plan(
    project_dir: Path | str,
    snapshot: dict[str, Any] | None = None,
    reason: str = "",
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or load_policy()
    pdir = Path(project_dir)
    snap = snapshot or build_resource_snapshot(pdir, pol)
    current_mode = snap.get("recommended_mode", "normal")
    actions = [
        {
            "action": "flush_status_files",
            "rationale": "Persist last-known CU + bridge + scorecard status before slowing down.",
            "owner": "operator",
            "side_effects": ["writes memory/luna_resource_status.* only"],
        },
        {
            "action": "write_daily_brief",
            "rationale": "Summarize the day's progress to memory/luna_daily_brief.md (separate phase).",
            "owner": "operator",
        },
        {
            "action": "compact_memory_index",
            "rationale": "Optional Phase 5D rebuild — never auto-run.",
            "owner": "operator",
        },
        {
            "action": "pause_high_intensity_loops",
            "rationale": "Stop aider/CU work; leave read-only telemetry running.",
            "owner": "luna_guardian",
        },
        {
            "action": "slow_heartbeat",
            "rationale": "Reduce worker heartbeat frequency while idle.",
            "owner": "luna_worker",
        },
        {
            "action": "keep_guardian_alive",
            "rationale": "Guardian must stay up to detect resume conditions.",
            "owner": "luna_guardian",
        },
        {
            "action": "keep_ui_available",
            "rationale": "Operator must be able to inspect status and exit hibernation.",
            "owner": "ui",
        },
        {
            "action": "refuse_aider_jobs_until_resume",
            "rationale": "No new aider work until snapshot conditions improve.",
            "owner": "aider_bridge",
        },
    ]
    files_to_write = [
        "memory/luna_resource_status.json",
        "memory/luna_resource_status.md",
        "memory/luna_hibernation_plan.json",
        "memory/luna_hibernation_report.md",
    ]
    files_not_to_touch = [
        "worker.py",
        "aider_bridge.py",
        "luna_guardian.py",
        "LaunchLuna.pyw",
        "SurgeApp_Claude_Terminal.py",
        "luna_start.pyw",
        "director_agent.py",
        "luna_modules/luna_hygiene.py",
        "luna_modules/luna_paths.py",
        "luna_modules/luna_routing.py",
        "luna_modules/luna_state.py",
        "LUNA_STOP_NOW.flag",
        "memory/continues_update.stop",
        "memory/luna_personality_state.json",
        "memory/luna_active_goal.json",
        "memory/luna_change_ledger.jsonl",
        "tasks/",
        "aider_jobs/",
        "logs/",
        "backups/",
        "uploads/",
    ]
    resume_conditions = [
        f"disk_free_gb >= {pol.get('disk_free_warn_gb', 5.0)}",
        f"memory_free_percent >= {pol.get('memory_free_warn_percent', 20)}",
        f"gpu_free_percent >= {pol.get('gpu_free_warn_percent', 20)} (if GPU detected)",
        "ollama localhost API reachable",
        "no aider quarantine surge in last hour",
    ]
    verification_commands = [
        "python -m py_compile worker.py",
        "python -c \"import sys; sys.path.insert(0, '.'); import worker; print('IMPORT_OK')\"",
        "powershell -ExecutionPolicy Bypass -File Luna_Post_Repair_Verify.ps1",
    ]
    safety_notes = [
        "Hibernation plan is proposal-only — Phase 5J does not execute any action.",
        "Plan must not delete, truncate, move, or overwrite memory/logs/queues/backups/uploads.",
        "Plan must not edit worker.py, aider_bridge.py, luna_guardian.py, LaunchLuna.pyw, SurgeApp_Claude_Terminal.py, luna_start.pyw, or director_agent.py.",
        "Plan must not create LUNA_STOP_NOW.flag or memory/continues_update.stop.",
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "plan_id": f"hib_{uuid.uuid4().hex[:10]}",
        "created_at": now_iso(),
        "reason": reason or f"resource_mode={current_mode}",
        "current_mode": current_mode,
        "proposed_actions": actions,
        "files_to_write": files_to_write,
        "files_not_to_touch": files_not_to_touch,
        "resume_conditions": resume_conditions,
        "verification_commands": verification_commands,
        "safety_notes": safety_notes,
    }


def validate_hibernation_plan(plan: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(plan, dict):
        return False, ["plan not a dict"]
    for k in (
        "schema_version",
        "plan_id",
        "created_at",
        "reason",
        "current_mode",
        "proposed_actions",
        "files_not_to_touch",
        "resume_conditions",
        "safety_notes",
    ):
        if k not in plan:
            errors.append(f"plan.{k} missing")
    if plan.get("current_mode") not in VALID_MODES:
        errors.append(f"current_mode invalid: {plan.get('current_mode')!r}")
    actions = plan.get("proposed_actions") or []
    if not isinstance(actions, list) or not actions:
        errors.append("proposed_actions must be non-empty list")
    return (not errors), errors


# ---------- rendering / writing ----------


def render_resource_status_markdown(
    snapshot: dict[str, Any], decision: dict[str, Any] | None = None
) -> str:
    lines: list[str] = []
    lines.append("# Luna Resource Status")
    lines.append("")
    lines.append(f"- **Generated**: {snapshot.get('generated_at', '?')}")
    lines.append(f"- **Host**: {snapshot.get('host', '?')}")
    lines.append(f"- **Platform**: {snapshot.get('platform', '?')}")
    lines.append(f"- **Recommended mode**: `{snapshot.get('recommended_mode', '?')}`")
    if decision:
        lines.append(f"- **Decision status**: `{decision.get('status', '?')}`")
        lines.append(f"- **Next check seconds**: {decision.get('next_check_seconds', '?')}")
    lines.append("")
    lines.append("## Sensors")
    for key in ("disk", "memory", "cpu", "gpu", "ollama", "processes", "queues", "logs"):
        rec = snapshot.get(key) or {}
        lines.append(f"### {key}")
        for k, v in rec.items():
            lines.append(f"- {k}: `{v}`")
        lines.append("")
    if snapshot.get("blockers"):
        lines.append("## Blockers")
        for b in snapshot["blockers"]:
            lines.append(f"- {b}")
        lines.append("")
    if snapshot.get("warnings"):
        lines.append("## Warnings")
        for w in snapshot["warnings"]:
            lines.append(f"- {w}")
        lines.append("")
    return "\n".join(lines) + "\n"


def render_hibernation_plan_markdown(plan: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Luna Hibernation Plan (proposal only)")
    lines.append("")
    lines.append(f"- **plan_id**: `{plan.get('plan_id', '?')}`")
    lines.append(f"- **created_at**: {plan.get('created_at', '?')}")
    lines.append(f"- **current_mode**: `{plan.get('current_mode', '?')}`")
    lines.append(f"- **reason**: {plan.get('reason', '')!r}")
    lines.append("")
    lines.append("## Proposed actions")
    for a in plan.get("proposed_actions") or []:
        lines.append(f"- **{a.get('action')}** — {a.get('rationale')} (owner: {a.get('owner', '?')})")
    lines.append("")
    lines.append("## Files plan WILL write")
    for f in plan.get("files_to_write") or []:
        lines.append(f"- `{f}`")
    lines.append("")
    lines.append("## Files plan WILL NOT touch")
    for f in plan.get("files_not_to_touch") or []:
        lines.append(f"- `{f}`")
    lines.append("")
    lines.append("## Resume conditions")
    for c in plan.get("resume_conditions") or []:
        lines.append(f"- {c}")
    lines.append("")
    lines.append("## Safety notes")
    for n in plan.get("safety_notes") or []:
        lines.append(f"- {n}")
    return "\n".join(lines) + "\n"


def write_resource_reports(
    project_dir: Path | str,
    snapshot: dict[str, Any],
    decision: dict[str, Any] | None = None,
    plan: dict[str, Any] | None = None,
    project_root: Path | str | None = None,
) -> dict[str, Any]:
    pdir = Path(project_dir).resolve()
    root = Path(project_root).resolve() if project_root else pdir
    mem_dir = pdir / "memory"
    mem_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, Any] = {}

    def _ensure_under(p: Path) -> Path:
        try:
            p.resolve().relative_to(root)
        except ValueError:
            raise ValueError(f"path escapes project root: {p}")
        return p

    snap_json = _ensure_under(mem_dir / "luna_resource_status.json")
    snap_md = _ensure_under(mem_dir / "luna_resource_status.md")
    write_json_atomic(snap_json, snapshot)
    snap_md.write_text(render_resource_status_markdown(snapshot, decision), encoding="utf-8")
    out["snapshot_json"] = str(snap_json)
    out["snapshot_md"] = str(snap_md)

    hw_json = _ensure_under(mem_dir / "luna_hardware_profile.json")
    write_json_atomic(hw_json, build_hardware_profile(pdir))
    out["hardware_profile_json"] = str(hw_json)

    if plan is not None:
        plan_json = _ensure_under(mem_dir / "luna_hibernation_plan.json")
        plan_md = _ensure_under(mem_dir / "luna_hibernation_report.md")
        write_json_atomic(plan_json, plan)
        plan_md.write_text(render_hibernation_plan_markdown(plan), encoding="utf-8")
        out["plan_json"] = str(plan_json)
        out["plan_md"] = str(plan_md)

    build_report = _ensure_under(mem_dir / "luna_resource_monitor_build_report.json")
    write_json_atomic(
        build_report,
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at": now_iso(),
            "wrote": out,
            "recommended_mode": snapshot.get("recommended_mode"),
            "decision_status": (decision or {}).get("status"),
        },
    )
    out["build_report_json"] = str(build_report)
    return out


# ---------- self-test ----------


def _self_test_inner(td: Path) -> dict[str, Any]:
    fakes = {
        "disk": {
            "project_drive_total_gb": 500.0,
            "project_drive_free_gb": 120.0,
            "project_drive_free_percent": 24,
            "status": "healthy",
        },
        "memory": {
            "total_gb": 32.0,
            "available_gb": 18.0,
            "available_percent": 56,
            "status": "healthy",
            "source": "fake",
        },
        "cpu": {"usage_percent": 22, "load_status": "excellent", "source": "fake"},
        "gpu": {
            "detected": True,
            "name": "fake_gpu",
            "total_vram_gb": 8.0,
            "used_vram_gb": 1.0,
            "free_vram_gb": 7.0,
            "free_vram_percent": 87,
            "status": "healthy",
            "source": "fake",
        },
        "ollama": {
            "api_reachable": True,
            "loaded_models": ["qwen2.5-coder:7b"],
            "status": "healthy",
            "source": "fake",
        },
        "processes": {
            "luna_process_count": 3,
            "worker_main_logical": 1,
            "worker_cu_logical": 1,
            "aider_bridge_logical": 1,
            "aider_child_count": 0,
            "status": "healthy",
            "source": "fake",
        },
        "queues": {
            "tasks_active": 0,
            "tasks_done": 0,
            "tasks_failed": 0,
            "aider_active": 0,
            "aider_done": 0,
            "aider_failed": 0,
            "aider_quarantine": 0,
            "status": "healthy",
        },
        "logs": {"largest_files": [], "total_log_bytes": 0, "total_log_mb": 0.0, "status": "healthy"},
    }
    snap = build_resource_snapshot(td, fakes=fakes)
    ok, errs = validate_resource_snapshot(snap)
    if not ok:
        return {"ok": False, "stage": "validate_snapshot", "errors": errs}
    decision = classify_resource_state(snap)
    plan = build_hibernation_plan(td, snap, reason="self-test")
    plan_ok, plan_errs = validate_hibernation_plan(plan)
    if not plan_ok:
        return {"ok": False, "stage": "validate_plan", "errors": plan_errs}
    write_resource_reports(td, snap, decision=decision, plan=plan, project_root=td)
    return {
        "ok": True,
        "recommended_mode": snap["recommended_mode"],
        "status": decision["status"],
        "allowed_task_classes": decision["allowed_task_classes"],
        "blockers": decision["blockers"],
        "warnings": decision["warnings"],
        "plan_actions": [a["action"] for a in plan["proposed_actions"]],
    }


def self_test() -> int:
    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)
        result = _self_test_inner(td)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Resource Awareness + Hibernation foundation (Phase 5J)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--snapshot", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--print-markdown", action="store_true")
    parser.add_argument("--hibernation-plan", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    pdir = Path(args.project_dir)
    pol = load_policy()
    snap = build_resource_snapshot(pdir, pol)
    decision = classify_resource_state(snap, pol)

    if args.hibernation_plan:
        plan = build_hibernation_plan(pdir, snap, reason="cli --hibernation-plan", policy=pol)
        if args.write:
            write_resource_reports(pdir, snap, decision=decision, plan=plan, project_root=pdir)
        out = {
            "plan_id": plan["plan_id"],
            "current_mode": plan["current_mode"],
            "actions": [a["action"] for a in plan["proposed_actions"]],
            "files_not_to_touch_count": len(plan["files_not_to_touch"]),
            "resume_conditions_count": len(plan["resume_conditions"]),
        }
        if args.print_markdown:
            sys.stdout.write(render_hibernation_plan_markdown(plan))
            return 0
        print(json.dumps(out, indent=2))
        return 0

    if args.write:
        written = write_resource_reports(pdir, snap, decision=decision, project_root=pdir)
        print(
            json.dumps(
                {
                    "recommended_mode": snap["recommended_mode"],
                    "status": decision["status"],
                    "wrote": written,
                },
                indent=2,
            )
        )
        return 0

    if args.print_markdown:
        sys.stdout.write(render_resource_status_markdown(snap, decision))
        return 0

    if args.snapshot or True:
        out = {
            "recommended_mode": snap["recommended_mode"],
            "status": decision["status"],
            "blockers": decision["blockers"],
            "warnings": decision["warnings"],
            "disk_free_gb": snap["disk"].get("project_drive_free_gb"),
            "memory_available_percent": snap["memory"].get("available_percent"),
            "cpu_usage_percent": snap["cpu"].get("usage_percent"),
            "gpu_detected": snap["gpu"].get("detected"),
            "ollama_reachable": snap["ollama"].get("api_reachable"),
        }
        print(json.dumps(out, indent=2))
        return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
