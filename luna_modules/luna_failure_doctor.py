"""
luna_failure_doctor.py — Failure Diagnostician

Diagnoses Luna's own failures before retrying. Checks Ollama, Aider,
Python runtime, no-diff floods, stale lockfiles, and kill switches.

Stdlib only, no print(), UTF-8 safe.
"""

import json
import os
import socket
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _live_feed(project_dir: Path, event: str, msg: str, details: Optional[dict] = None) -> None:
    log_path = project_dir / "logs" / "luna_live_feed.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": _now_iso(),
        "role": "operating_layer",
        "source": "luna_failure_doctor",
        "event": event,
        "message": msg,
        "details": details or {},
    }
    try:
        with open(log_path, "a", encoding="utf-8", errors="replace") as f:
            f.write(json.dumps(entry, ensure_ascii=True) + "\n")
    except Exception:
        pass


def _load_jsonl_tail(path: Path, n: int = 20) -> list:
    if not path.exists():
        return []
    lines = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception:
        return []
    tail = lines[-n:] if len(lines) > n else lines
    records = []
    for line in tail:
        try:
            records.append(json.loads(line.strip()))
        except Exception:
            pass
    return records


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8", errors="replace") as f:
        json.dump(data, f, indent=2, ensure_ascii=True)
    tmp.replace(path)


def _append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", errors="replace") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


# ---------------------------------------------------------------------------
# Sub-inspectors
# ---------------------------------------------------------------------------

def inspect_ollama(project_dir: Path) -> dict:
    """Check if Ollama is reachable and what models are loaded."""
    result: dict = {"reachable": False, "models": [], "issues": []}

    # Check env
    api_base = os.environ.get("OLLAMA_API_BASE", "")
    if api_base and "127.0.0.1" not in api_base and "localhost" not in api_base:
        result["issues"].append(f"OLLAMA_API_BASE may be misconfigured: {api_base}")

    url = "http://127.0.0.1:11434/api/tags"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "luna-failure-doctor/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            data = json.loads(body)
            models = [m.get("name", "") for m in data.get("models", [])]
            result["reachable"] = True
            result["models"] = models
            if not models:
                result["issues"].append("Ollama reachable but no models loaded")
    except urllib.error.URLError as exc:
        result["issues"].append(f"Ollama unreachable: {exc}")
    except Exception as exc:
        result["issues"].append(f"Ollama check error: {exc}")

    return result


def inspect_python_runtime(project_dir: Path) -> dict:
    """Check for WindowsApps stub Python usage and venv availability."""
    import sys
    result: dict = {"python_exe": sys.executable, "issues": []}

    exe = sys.executable.lower()
    if "windowsapps" in exe:
        result["issues"].append(
            "Using Windows App Store Python stub — this will silently fail. "
            "Use D:\\SurgeApp\\.aider_venv\\Scripts\\python.exe instead."
        )

    venv_python = project_dir / ".aider_venv" / "Scripts" / "python.exe"
    result["venv_python"] = str(venv_python)
    result["venv_exists"] = venv_python.exists()
    if not venv_python.exists():
        result["issues"].append(f"Project venv not found at {venv_python}")

    return result


def inspect_aider(project_dir: Path) -> dict:
    """Scan aider job logs for recent failures and noop patterns."""
    result: dict = {"issues": [], "recent_failures": 0, "recent_noops": 0, "checked_logs": []}

    log_sources = [
        project_dir / "logs" / "luna_live_feed.jsonl",
        project_dir / "memory" / "cycle_summaries.jsonl",
    ]

    failure_count = 0
    noop_count = 0
    for log_path in log_sources:
        records = _load_jsonl_tail(log_path, 50)
        result["checked_logs"].append(str(log_path))
        for rec in records:
            status = rec.get("status", rec.get("event", ""))
            if status in ("failed", "AIDER_FAILED", "error"):
                failure_count += 1
            if status in ("noop", "NOOP", "no_diff"):
                noop_count += 1

    result["recent_failures"] = failure_count
    result["recent_noops"] = noop_count

    if failure_count >= 5:
        result["issues"].append(f"High failure count in recent logs: {failure_count}")
    if noop_count >= 5:
        result["issues"].append(f"Noop/no-diff flood in recent logs: {noop_count}")

    # Check for stale lockfiles
    lock_candidates = [
        project_dir / "path_to_worker_lock",
        project_dir / "worker.lock",
        project_dir / "aider.lock",
    ]
    stale_locks = [str(p) for p in lock_candidates if p.exists()]
    if stale_locks:
        result["issues"].append(f"Possible stale lockfiles: {stale_locks}")
    result["stale_locks"] = stale_locks

    return result


def inspect_recent_jobs(project_dir: Path) -> dict:
    """Check aider_jobs / director_jobs for oversized targets and empty staged files."""
    result: dict = {"issues": [], "checked_dirs": [], "oversized_targets": [], "empty_staged": []}

    job_dirs = [
        project_dir / "aider_jobs",
        project_dir / "director_jobs",
    ]

    for jdir in job_dirs:
        if not jdir.exists():
            continue
        result["checked_dirs"].append(str(jdir))
        for jfile in sorted(jdir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)[:20]:
            if not jfile.suffix == ".json":
                continue
            try:
                with open(jfile, "r", encoding="utf-8", errors="replace") as f:
                    job = json.load(f)
                target = job.get("target_file", job.get("file", ""))
                if target:
                    tp = Path(target)
                    if tp.exists():
                        size_kb = tp.stat().st_size / 1024
                        if size_kb > 250:
                            result["oversized_targets"].append(
                                {"file": target, "size_kb": round(size_kb, 1), "job": jfile.name}
                            )
                staged = job.get("staged_file", job.get("excerpt_path", ""))
                if staged:
                    sp = Path(staged)
                    if sp.exists() and sp.stat().st_size == 0:
                        result["empty_staged"].append({"file": staged, "job": jfile.name})
            except Exception:
                pass

    if result["oversized_targets"]:
        result["issues"].append(
            f"{len(result['oversized_targets'])} oversized target files found (>250 KB). "
            "Luna should use function-scoped excerpts."
        )
    if result["empty_staged"]:
        result["issues"].append(
            f"{len(result['empty_staged'])} empty staged files found. "
            "Jobs with empty staged files should be quarantined."
        )

    return result


def inspect_context_overflow(project_dir: Path, window_hours: float = 4.0) -> dict:
    """
    Read memory/context_overflow_targets.jsonl and identify files that overflow
    the model context repeatedly.  Produces actionable recommendations so the CU
    can deprioritise those files or use smaller section windows.
    """
    import time as _t
    result: dict = {"issues": [], "overflow_files": {}, "total_overflows": 0, "recommendations": []}
    ov_path = project_dir / "memory" / "context_overflow_targets.jsonl"
    if not ov_path.exists():
        return result

    cutoff = _t.time() - window_hours * 3600.0
    try:
        records = []
        with open(ov_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    r = json.loads(line.strip())
                    ts_str = r.get("ts", "")
                    try:
                        from datetime import datetime as _dt
                        ts = _dt.fromisoformat(ts_str).timestamp()
                    except Exception:
                        ts = 0.0
                    if ts >= cutoff:
                        records.append(r)
                except Exception:
                    pass
    except Exception:
        return result

    for r in records:
        tgt = str(r.get("target") or "")
        if tgt:
            result["overflow_files"][tgt] = result["overflow_files"].get(tgt, 0) + 1
            result["total_overflows"] += 1

    repeat_offenders = {f: c for f, c in result["overflow_files"].items() if c >= 2}
    if result["total_overflows"] >= 3:
        result["issues"].append(
            f"{result['total_overflows']} context overflow(s) in the last {window_hours:.0f}h. "
            "Model (qwen2.5-coder:7b) context window is too small for these targets."
        )
        result["recommendations"].append(
            "Use smaller section windows (100 lines max) for large targets. "
            "Consider excluding files >500 lines from CU plan or switching to excerpt mode."
        )
    if repeat_offenders:
        for fname, count in repeat_offenders.items():
            result["recommendations"].append(
                f"File '{fname}' overflowed {count}x — deprioritise or use a dedicated excerpt job."
            )
    result["repeat_offenders"] = repeat_offenders
    return result


# ---------------------------------------------------------------------------
# Main public API
# ---------------------------------------------------------------------------

def diagnose_failures(project_dir: Path, limit: int = 20) -> dict:
    """
    Run all sub-inspectors and return a combined diagnosis.
    Returns: {status, issues, checks, kill_switch, ts}
    """
    diagnosis: dict = {
        "ts": _now_iso(),
        "status": "ok",
        "issues": [],
        "checks": {},
    }

    # Kill switch
    kill_flag = project_dir / "LUNA_STOP_NOW.flag"
    diagnosis["kill_switch"] = kill_flag.exists()
    if kill_flag.exists():
        diagnosis["issues"].append("Kill switch LUNA_STOP_NOW.flag is present — Luna should halt.")

    # Ollama
    ollama = inspect_ollama(project_dir)
    diagnosis["checks"]["ollama"] = ollama
    diagnosis["issues"].extend(ollama["issues"])

    # Python runtime
    runtime = inspect_python_runtime(project_dir)
    diagnosis["checks"]["python_runtime"] = runtime
    diagnosis["issues"].extend(runtime["issues"])

    # Aider log analysis
    aider = inspect_aider(project_dir)
    diagnosis["checks"]["aider"] = aider
    diagnosis["issues"].extend(aider["issues"])

    # Recent jobs
    jobs = inspect_recent_jobs(project_dir)
    diagnosis["checks"]["recent_jobs"] = jobs
    diagnosis["issues"].extend(jobs["issues"])

    # Context overflow pattern
    overflow = inspect_context_overflow(project_dir)
    diagnosis["checks"]["context_overflow"] = overflow
    diagnosis["issues"].extend(overflow["issues"])
    if overflow.get("recommendations"):
        diagnosis.setdefault("recommendations", []).extend(overflow["recommendations"])

    if diagnosis["issues"]:
        diagnosis["status"] = "degraded"

    write_failure_report(project_dir, diagnosis)
    _live_feed(project_dir, "FAILURE_DOCTOR_DONE",
               f"Diagnosis complete: {len(diagnosis['issues'])} issues",
               {"issue_count": len(diagnosis["issues"])})
    return diagnosis


def recommend_repair_mission(project_dir: Path, diagnosis: dict) -> dict:
    """Suggest a repair mission goal based on current diagnosis."""
    issues = diagnosis.get("issues", [])
    if not issues:
        return {"recommended": False, "reason": "No issues found"}

    # Priority: kill switch > Ollama > python > floods
    goals = []
    for issue in issues:
        il = issue.lower()
        if "kill switch" in il:
            goals.append("Remove kill switch and diagnose why it was set")
        elif "ollama unreachable" in il or "ollama" in il:
            goals.append("Restart Ollama service and verify model loading")
        elif "windowsapps" in il or "stub" in il:
            goals.append("Fix Python executable path to use project venv")
        elif "oversized" in il:
            goals.append("Enable function-scoped excerpts for large target files")
        elif "noop" in il or "no-diff" in il:
            goals.append("Diagnose and fix no-diff/noop flood pattern")
        elif "stale lock" in il:
            goals.append("Investigate and clear stale lockfiles")
        elif "empty staged" in il:
            goals.append("Quarantine jobs with empty staged files and investigate root cause")

    if not goals:
        goals = [f"Investigate: {issues[0][:80]}"]

    return {
        "recommended": True,
        "suggested_goal": goals[0],
        "all_suggestions": goals,
        "issue_count": len(issues),
    }


def write_failure_report(project_dir: Path, diagnosis: dict) -> dict:
    """Write JSON + Markdown failure reports."""
    json_path = project_dir / "memory" / "failure_diagnosis.json"
    md_path = project_dir / "memory" / "failure_diagnosis.md"
    mem_path = project_dir / "memory" / "failure_memory.jsonl"

    _write_json(json_path, diagnosis)
    _append_jsonl(mem_path, {"ts": diagnosis.get("ts", _now_iso()),
                              "issue_count": len(diagnosis.get("issues", [])),
                              "status": diagnosis.get("status", "")})

    lines = [
        "# Luna Failure Diagnosis Report",
        f"",
        f"**Generated:** {diagnosis.get('ts', '')}",
        f"**Status:** {diagnosis.get('status', 'unknown')}",
        f"**Kill Switch Active:** {diagnosis.get('kill_switch', False)}",
        "",
        "## Issues Found",
    ]
    issues = diagnosis.get("issues", [])
    if issues:
        for issue in issues:
            lines.append(f"- {issue}")
    else:
        lines.append("- No issues detected")

    lines += ["", "## Checks Run"]
    for check_name, check_data in diagnosis.get("checks", {}).items():
        lines.append(f"### {check_name}")
        sub_issues = check_data.get("issues", [])
        if sub_issues:
            for si in sub_issues:
                lines.append(f"  - {si}")
        else:
            lines.append("  - OK")

    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return {"json": str(json_path), "md": str(md_path), "memory": str(mem_path)}
