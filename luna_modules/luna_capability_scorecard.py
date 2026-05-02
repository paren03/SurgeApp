"""Phase 5G: Luna Capability Scorecard foundation.

Read-mostly. Stdlib only. Not wired into runtime services.

Rates Luna 0-100 across 15 operational dimensions using local evidence only.
Used by an operator to see, at a glance, what Luna can safely do right now,
what still needs approval, what is degraded, and what the smallest safe next
improvement is.

Configuration:
  memory/luna_capability_scorecard_config.json (tracked)
  memory/luna_capability_scorecard.schema.json (tracked)

Generated runtime artifacts (gitignored):
  memory/luna_capability_scorecard.json
  memory/luna_capability_scorecard.md
  memory/luna_capability_scorecard_build_report.json

CLI:
  python -m luna_modules.luna_capability_scorecard --self-test
  python -m luna_modules.luna_capability_scorecard --write
  python -m luna_modules.luna_capability_scorecard --print-markdown
"""
from __future__ import annotations

import argparse
import datetime as _dt
import glob
import json
import os
import py_compile
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

REQUIRED_DIMENSIONS: tuple[str, ...] = (
    "boot_health",
    "process_discipline",
    "memory_recall",
    "file_awareness",
    "change_tracking",
    "self_healing",
    "safe_self_upgrade",
    "ui_truthfulness",
    "aider_reliability",
    "test_coverage",
    "autonomy_safety",
    "resource_awareness",
    "rollback_readiness",
    "intent_alignment",
    "context_efficiency",
)

_VALID_STATUSES = (
    "excellent",
    "healthy",
    "watch",
    "degraded",
    "blocked",
    "unknown",
)

_VALID_READINESS = (
    "read_only",
    "safe_foundation",
    "controlled_autonomy_ready",
    "limited_self_upgrade_ready",
    "not_ready",
)

_DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "status_thresholds": {
        "excellent": 90,
        "healthy": 75,
        "watch": 55,
        "degraded": 30,
    },
    "dimension_weights": {
        "boot_health": 9,
        "process_discipline": 7,
        "memory_recall": 6,
        "file_awareness": 6,
        "change_tracking": 6,
        "self_healing": 6,
        "safe_self_upgrade": 8,
        "ui_truthfulness": 6,
        "aider_reliability": 7,
        "test_coverage": 6,
        "autonomy_safety": 9,
        "resource_awareness": 4,
        "rollback_readiness": 7,
        "intent_alignment": 5,
        "context_efficiency": 4,
    },
    "readiness_rules": {
        "not_ready_overall_below": 40,
        "safe_foundation_overall_below": 65,
        "controlled_autonomy_overall_below": 80,
        "limited_self_upgrade_overall_below": 90,
        "block_if_any_critical_blocker": True,
        "block_if_boot_health_below": 60,
        "block_if_autonomy_safety_below": 60,
    },
    "evidence_files": {
        "verifier_log_glob": "logs/luna_post_repair_verify_*.txt",
        "cu_status": "memory/continues_update_status.json",
        "bridge_status": "memory/aider_bridge_status.json",
        "self_map": "memory/luna_module_roles.json",
        "self_map_risk": "memory/luna_risk_zones.json",
        "change_ledger_schema": "memory/luna_change_ledger.schema.json",
        "memory_index_schema": "memory/luna_memory_index.schema.json",
        "memory_sources": "memory/luna_memory_sources.json",
        "playbook_schema": "memory/luna_self_healing_playbooks.schema.json",
        "playbook_seed": "memory/luna_self_healing_playbooks_seed.json",
        "upgrade_gate_schema": "memory/luna_upgrade_gate_policy.schema.json",
        "upgrade_gate_policy": "memory/luna_upgrade_gate_policy.json",
        "noop_budget": "logs/aider_bridge_noop_budget.json",
        "stop_flag": "LUNA_STOP_NOW.flag",
    },
    "core_compile_targets": [
        "worker.py",
        "aider_bridge.py",
        "luna_guardian.py",
        "director_agent.py",
        "SurgeApp_Claude_Terminal.py",
        "LaunchLuna.pyw",
        "luna_start.pyw",
    ],
}

DEFAULT_CONFIG_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_capability_scorecard_config.json"


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clamp_score(value: Any) -> int:
    try:
        v = int(round(float(value)))
    except (TypeError, ValueError):
        return 0
    if v < 0:
        return 0
    if v > 100:
        return 100
    return v


def weighted_average(items: Iterable[tuple[float, float]]) -> int:
    total_weight = 0.0
    total = 0.0
    for score, weight in items:
        try:
            s = float(score)
            w = float(weight)
        except (TypeError, ValueError):
            continue
        if w <= 0:
            continue
        total += s * w
        total_weight += w
    if total_weight <= 0:
        return 0
    return clamp_score(total / total_weight)


def load_json(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return default
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError, UnicodeDecodeError):
        return default


def file_exists(path: Path | str) -> bool:
    try:
        return Path(path).is_file()
    except OSError:
        return False


def read_text_tail(path: Path | str, max_bytes: int = 262144) -> str:
    p = Path(path)
    if not p.is_file():
        return ""
    try:
        size = p.stat().st_size
        with p.open("rb") as fh:
            if size > max_bytes:
                fh.seek(size - max_bytes)
            data = fh.read()
        return data.decode("utf-8", errors="replace")
    except OSError:
        return ""


def latest_matching_file(folder: Path | str, pattern: str) -> Path | None:
    base = Path(folder)
    if not base.is_dir():
        return None
    matches = sorted(base.glob(pattern), key=lambda p: p.stat().st_mtime if p.exists() else 0)
    return matches[-1] if matches else None


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    p = config_path or DEFAULT_CONFIG_PATH
    cfg = load_json(p, default=None)
    if not isinstance(cfg, dict):
        merged = dict(_DEFAULT_CONFIG)
        merged["_source"] = "module_fallback"
        merged["_loaded_from_file"] = False
        return merged
    out = dict(_DEFAULT_CONFIG)
    for k, v in cfg.items():
        out[k] = v
    out["_source"] = str(p)
    out["_loaded_from_file"] = True
    return out


# ---------- evidence parsers ----------


_VERIFIER_FAIL_RE = re.compile(r"\[FAIL\]|hard failures? found", re.IGNORECASE)
_VERIFIER_WARN_RE = re.compile(r"\[WARN\]|warnings? found", re.IGNORECASE)
_VERIFIER_PASS_NO_FAILS_RE = re.compile(r"No hard failures found", re.IGNORECASE)
_VERIFIER_PASS_NO_WARNS_RE = re.compile(r"No warnings found", re.IGNORECASE)


def parse_verifier_report(path_or_text: Path | str) -> dict[str, Any]:
    if isinstance(path_or_text, (str, Path)) and Path(path_or_text).is_file():
        text = read_text_tail(path_or_text, max_bytes=524288)
        source = str(Path(path_or_text))
    else:
        text = str(path_or_text or "")
        source = "<text>"
    if not text:
        return {
            "source": source,
            "found": False,
            "hard_failures": None,
            "warnings": None,
            "summary": "no_report",
        }
    no_fail = bool(_VERIFIER_PASS_NO_FAILS_RE.search(text))
    no_warn = bool(_VERIFIER_PASS_NO_WARNS_RE.search(text))
    fail_hits = len(re.findall(r"\[FAIL\]", text))
    warn_hits = len(re.findall(r"\[WARN\]", text))
    if no_fail and fail_hits <= 1:
        hard_failures = 0
    else:
        hard_failures = max(0, fail_hits - (1 if no_fail else 0))
    if no_warn and warn_hits <= 1:
        warnings = 0
    else:
        warnings = max(0, warn_hits - (1 if no_warn else 0))
    if hard_failures == 0 and warnings == 0:
        summary = "clean"
    elif hard_failures == 0:
        summary = "warnings_only"
    else:
        summary = "failures_present"
    return {
        "source": source,
        "found": True,
        "hard_failures": hard_failures,
        "warnings": warnings,
        "summary": summary,
    }


# ---------- check helpers ----------


def _project_path(project_dir: Path | str, *parts: str) -> Path:
    return Path(project_dir).joinpath(*parts)


def _status_for_score(score: int, thresholds: dict[str, int]) -> str:
    if score >= int(thresholds.get("excellent", 90)):
        return "excellent"
    if score >= int(thresholds.get("healthy", 75)):
        return "healthy"
    if score >= int(thresholds.get("watch", 55)):
        return "watch"
    if score >= int(thresholds.get("degraded", 30)):
        return "degraded"
    return "blocked"


def _dimension(
    name: str,
    score: int,
    weight: int,
    thresholds: dict[str, int],
    evidence: list[str],
    blockers: list[str],
    recommended_next_action: str,
    source_files: list[str],
    forced_status: str | None = None,
) -> dict[str, Any]:
    s = clamp_score(score)
    if forced_status and forced_status in _VALID_STATUSES:
        status = forced_status
    elif blockers:
        status = "blocked" if s < int(thresholds.get("watch", 55)) else "degraded"
    else:
        status = _status_for_score(s, thresholds)
    return {
        "name": name,
        "score": s,
        "weight": int(weight),
        "status": status,
        "evidence": list(evidence),
        "blockers": list(blockers),
        "recommended_next_action": recommended_next_action,
        "source_files": list(source_files),
    }


def check_core_compile_status(project_dir: Path | str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config or load_config()
    targets = cfg.get("core_compile_targets") or _DEFAULT_CONFIG["core_compile_targets"]
    results: list[dict[str, Any]] = []
    ok_count = 0
    for rel in targets:
        path = _project_path(project_dir, rel)
        if not path.exists():
            results.append({"target": rel, "exists": False, "ok": False, "error": "missing"})
            continue
        try:
            py_compile.compile(str(path), doraise=True)
            results.append({"target": rel, "exists": True, "ok": True, "error": ""})
            ok_count += 1
        except py_compile.PyCompileError as e:
            results.append({"target": rel, "exists": True, "ok": False, "error": str(e)[:200]})
        except OSError as e:
            results.append({"target": rel, "exists": True, "ok": False, "error": f"oserror:{e}"})
    return {
        "total": len(targets),
        "ok": ok_count,
        "all_ok": ok_count == len(targets),
        "results": results,
    }


def check_worker_import_status(project_dir: Path | str) -> dict[str, Any]:
    worker = _project_path(project_dir, "worker.py")
    if not worker.exists():
        return {"exists": False, "ok": False, "error": "missing"}
    py_exe_candidates = [
        Path(project_dir) / ".aider_venv" / "Scripts" / "python.exe",
        Path(sys.executable),
    ]
    py_exe = next((p for p in py_exe_candidates if p.exists()), Path(sys.executable))
    try:
        proc = subprocess.run(
            [
                str(py_exe),
                "-c",
                f"import sys; sys.path.insert(0, r'{project_dir}'); import worker; print('IMPORT_OK')",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        ok = proc.returncode == 0 and "IMPORT_OK" in (proc.stdout or "")
        return {
            "exists": True,
            "ok": ok,
            "rc": proc.returncode,
            "stdout_tail": (proc.stdout or "")[-256:],
            "stderr_tail": (proc.stderr or "")[-256:],
        }
    except (subprocess.TimeoutExpired, OSError) as e:
        return {"exists": True, "ok": False, "error": f"{type(e).__name__}:{e}"}


def check_verifier_status(project_dir: Path | str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config or load_config()
    pattern = cfg.get("evidence_files", {}).get(
        "verifier_log_glob", "logs/luna_post_repair_verify_*.txt"
    )
    folder = Path(project_dir) / Path(pattern).parent
    glob_pat = Path(pattern).name
    latest = latest_matching_file(folder, glob_pat)
    if latest is None:
        return {"found": False, "summary": "no_report", "log": None}
    parsed = parse_verifier_report(latest)
    parsed["log"] = str(latest)
    return parsed


def check_git_clean(project_dir: Path | str) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=30,
        )
        text = proc.stdout or ""
        tracked_dirty = [
            line
            for line in text.splitlines()
            if line and not line.startswith("??")
        ]
        return {
            "ok": proc.returncode == 0,
            "tracked_dirty_count": len(tracked_dirty),
            "tracked_dirty_clean": len(tracked_dirty) == 0,
            "tracked_dirty_sample": tracked_dirty[:8],
        }
    except (subprocess.TimeoutExpired, OSError, FileNotFoundError) as e:
        return {"ok": False, "error": f"{type(e).__name__}:{e}"}


def check_process_role_summary(project_dir: Path | str) -> dict[str, Any]:
    role_paths = [
        _project_path(project_dir, "memory", "luna_module_roles.json"),
        _project_path(project_dir, "memory", "process_role_assignments.json"),
    ]
    findings = []
    role_count = 0
    for p in role_paths:
        if p.is_file():
            data = load_json(p, default=None)
            if isinstance(data, dict):
                findings.append({"path": str(p), "keys": len(data)})
                role_count += len(data)
            elif isinstance(data, list):
                findings.append({"path": str(p), "items": len(data)})
                role_count += len(data)
    return {
        "files_found": findings,
        "role_count": role_count,
        "any_found": bool(findings),
    }


def check_aider_queue_status(project_dir: Path | str) -> dict[str, Any]:
    jobs_dir = _project_path(project_dir, "aider_jobs")
    active = jobs_dir / "active"
    completed = jobs_dir / "completed"
    failed = jobs_dir / "failed"
    quarantine = jobs_dir / "quarantine"
    info = {
        "active_count": 0,
        "completed_count": 0,
        "failed_count": 0,
        "quarantine_count": 0,
        "exists": jobs_dir.is_dir(),
    }
    for label, path in (
        ("active_count", active),
        ("completed_count", completed),
        ("failed_count", failed),
        ("quarantine_count", quarantine),
    ):
        if path.is_dir():
            try:
                info[label] = sum(1 for _ in path.iterdir())
            except OSError:
                info[label] = -1
    bridge_status = load_json(
        _project_path(project_dir, "memory", "aider_bridge_status.json"), default=None
    )
    if isinstance(bridge_status, dict):
        info["bridge_status"] = {
            "state": bridge_status.get("state"),
            "last_event": bridge_status.get("last_event"),
            "elapsed_seconds": bridge_status.get("elapsed_seconds"),
        }
    return info


def check_cu_status(project_dir: Path | str) -> dict[str, Any]:
    cu_status = load_json(
        _project_path(project_dir, "memory", "continues_update_status.json"), default=None
    )
    paused_user_stop = file_exists(_project_path(project_dir, "LUNA_STOP_NOW.flag"))
    resume_once = file_exists(
        _project_path(project_dir, "memory", "continues_update.resume_once")
    )
    info: dict[str, Any] = {
        "paused_user_stop": paused_user_stop,
        "resume_once_pending": resume_once,
    }
    if isinstance(cu_status, dict):
        info["ui_status"] = cu_status.get("ui_status")
        info["last_status"] = cu_status.get("last_status")
        info["pause_reason"] = cu_status.get("pause_reason")
        info["dirty_targets"] = cu_status.get("dirty_targets") or []
        info["last_event_at"] = cu_status.get("last_event_at")
        info["found"] = True
    else:
        info["found"] = False
    return info


def _phase_artifact_status(
    project_dir: Path | str,
    schema_rel: str,
    extra_rels: list[str],
    test_rel: str,
    module_rel: str,
) -> dict[str, Any]:
    schema_p = _project_path(project_dir, schema_rel)
    extras = {rel: file_exists(_project_path(project_dir, rel)) for rel in extra_rels}
    test_p = _project_path(project_dir, test_rel)
    module_p = _project_path(project_dir, module_rel)
    return {
        "schema_exists": schema_p.is_file(),
        "extras": extras,
        "test_exists": test_p.is_file(),
        "module_exists": module_p.is_file(),
    }


def check_self_map_status(project_dir: Path | str) -> dict[str, Any]:
    return _phase_artifact_status(
        project_dir,
        schema_rel="memory/luna_module_roles.json",
        extra_rels=["memory/luna_risk_zones.json"],
        test_rel="tests/test_luna_self_knowledge.py",
        module_rel="luna_modules/luna_self_knowledge.py",
    )


def check_change_ledger_status(project_dir: Path | str) -> dict[str, Any]:
    return _phase_artifact_status(
        project_dir,
        schema_rel="memory/luna_change_ledger.schema.json",
        extra_rels=[],
        test_rel="tests/test_luna_change_ledger.py",
        module_rel="luna_modules/luna_change_ledger.py",
    )


def check_memory_index_status(project_dir: Path | str) -> dict[str, Any]:
    return _phase_artifact_status(
        project_dir,
        schema_rel="memory/luna_memory_index.schema.json",
        extra_rels=["memory/luna_memory_sources.json"],
        test_rel="tests/test_luna_memory_index.py",
        module_rel="luna_modules/luna_memory_index.py",
    )


def check_playbook_status(project_dir: Path | str) -> dict[str, Any]:
    return _phase_artifact_status(
        project_dir,
        schema_rel="memory/luna_self_healing_playbooks.schema.json",
        extra_rels=["memory/luna_self_healing_playbooks_seed.json"],
        test_rel="tests/test_luna_playbook_engine.py",
        module_rel="luna_modules/luna_playbook_engine.py",
    )


def check_upgrade_gate_status(project_dir: Path | str) -> dict[str, Any]:
    return _phase_artifact_status(
        project_dir,
        schema_rel="memory/luna_upgrade_gate_policy.schema.json",
        extra_rels=["memory/luna_upgrade_gate_policy.json"],
        test_rel="tests/test_luna_upgrade_gate.py",
        module_rel="luna_modules/luna_upgrade_gate.py",
    )


def check_test_coverage(project_dir: Path | str) -> dict[str, Any]:
    tests_dir = _project_path(project_dir, "tests")
    if not tests_dir.is_dir():
        return {"exists": False, "test_files": 0, "phase_tests": []}
    test_files = [p.name for p in tests_dir.glob("test_*.py")]
    phase_tests = sorted(
        [
            n
            for n in test_files
            if n
            in {
                "test_luna_self_knowledge.py",
                "test_luna_change_ledger.py",
                "test_luna_memory_index.py",
                "test_luna_playbook_engine.py",
                "test_luna_upgrade_gate.py",
                "test_luna_capability_scorecard.py",
            }
        ]
    )
    return {
        "exists": True,
        "test_files": len(test_files),
        "phase_tests": phase_tests,
        "phase_tests_count": len(phase_tests),
    }


def check_rollback_readiness(project_dir: Path | str) -> dict[str, Any]:
    backups_dir = _project_path(project_dir, "backups")
    backup_count = 0
    latest = None
    if backups_dir.is_dir():
        try:
            entries = sorted(
                [p for p in backups_dir.iterdir() if p.is_dir()],
                key=lambda p: p.stat().st_mtime,
            )
            backup_count = len(entries)
            if entries:
                latest = entries[-1].name
        except OSError:
            backup_count = -1
    git_log = ""
    try:
        proc = subprocess.run(
            ["git", "log", "--oneline", "-1"],
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=15,
        )
        if proc.returncode == 0:
            git_log = (proc.stdout or "").strip()
    except (subprocess.TimeoutExpired, OSError, FileNotFoundError):
        git_log = ""
    return {
        "backup_count": backup_count,
        "latest_backup": latest,
        "git_head_oneline": git_log,
    }


def check_context_efficiency(project_dir: Path | str) -> dict[str, Any]:
    bridge_path = _project_path(project_dir, "aider_bridge.py")
    info: dict[str, Any] = {
        "bridge_exists": bridge_path.is_file(),
        "max_target_file_bytes": None,
        "ollama_num_ctx": None,
    }
    if bridge_path.is_file():
        try:
            text = bridge_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        m = re.search(r"MAX_TARGET_FILE_BYTES\s*=\s*(\d+)", text)
        if not m:
            m = re.search(
                r"MAX_TARGET_FILE_BYTES\s*=[^\n]*?[\"'](\d+)[\"']", text
            )
        if not m:
            m = re.search(r"MAX_TARGET_FILE_BYTES\s*=[^\n]*?(\d{3,})", text)
        if m:
            info["max_target_file_bytes"] = int(m.group(1))
        m2 = re.search(r"OLLAMA_NUM_CTX[^=\n]*=\s*[\"']?(\d+)", text)
        if not m2:
            m2 = re.search(
                r"OLLAMA_NUM_CTX[^\n]*?[\"'](\d+)[\"']", text
            )
        if not m2:
            m2 = re.search(r"num_ctx[\"']?\s*[:=]\s*(\d+)", text)
        if m2:
            info["ollama_num_ctx"] = int(m2.group(1))
    return info


def check_resource_awareness(project_dir: Path | str) -> dict[str, Any]:
    candidates = [
        "luna_modules/luna_resource_monitor.py",
        "luna_modules/luna_health.py",
        "memory/luna_resource_status.json",
    ]
    found = [rel for rel in candidates if file_exists(_project_path(project_dir, rel))]
    return {"candidates_found": found, "any_found": bool(found)}


def check_intent_alignment(project_dir: Path | str) -> dict[str, Any]:
    candidates = [
        "memory/luna_active_goal.json",
        "memory/luna_intent.json",
        "memory/user_goals.json",
        "luna_modules/luna_drift_detector.py",
    ]
    found = [rel for rel in candidates if file_exists(_project_path(project_dir, rel))]
    return {"candidates_found": found, "any_found": bool(found)}


# ---------- per-dimension scoring ----------


def _score_boot_health(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["boot_health"])
    thresholds = cfg["status_thresholds"]
    compile_info = check_core_compile_status(project_dir, cfg)
    import_info = check_worker_import_status(project_dir)
    verifier = check_verifier_status(project_dir, cfg)
    score = 0
    evidence: list[str] = []
    blockers: list[str] = []
    if compile_info["all_ok"]:
        score += 50
        evidence.append(f"py_compile ok: {compile_info['ok']}/{compile_info['total']} core files")
    else:
        score += int(50 * (compile_info["ok"] / max(1, compile_info["total"])))
        blockers.append(
            f"py_compile failed for {compile_info['total'] - compile_info['ok']} core files"
        )
    if import_info.get("ok"):
        score += 25
        evidence.append("import worker -> IMPORT_OK")
    else:
        blockers.append(f"import worker failed (rc={import_info.get('rc')})")
    if verifier.get("found"):
        if verifier.get("hard_failures") == 0 and verifier.get("warnings") == 0:
            score += 25
            evidence.append(f"verifier clean ({Path(verifier['log']).name})")
        elif verifier.get("hard_failures") == 0:
            score += 15
            evidence.append(
                f"verifier {verifier.get('warnings')} warnings, 0 hard failures"
            )
        else:
            blockers.append(
                f"verifier reports {verifier.get('hard_failures')} hard failure(s)"
            )
    else:
        evidence.append("no verifier report found")
    next_action = (
        "Re-run Luna_Post_Repair_Verify.ps1 if no current report"
        if not verifier.get("found")
        else "None — boot path healthy"
        if not blockers
        else "Resolve compile/import/verifier failures before any autonomy"
    )
    return _dimension(
        "boot_health",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        ["worker.py", "Luna_Post_Repair_Verify.ps1", "memory/luna_module_roles.json"],
    )


def _score_process_discipline(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["process_discipline"])
    thresholds = cfg["status_thresholds"]
    role_info = check_process_role_summary(project_dir)
    verifier = check_verifier_status(project_dir, cfg)
    score = 40
    evidence: list[str] = []
    blockers: list[str] = []
    if role_info.get("any_found"):
        score += 30
        evidence.append(f"process role artifacts: {len(role_info.get('files_found', []))} file(s)")
    else:
        evidence.append("no process role artifacts found")
    if verifier.get("found"):
        if verifier.get("hard_failures") == 0:
            score += 20
            evidence.append("verifier reports no role/process hard failures")
        else:
            blockers.append("verifier reports hard failures (may include role drift)")
    else:
        score += 5
    if not role_info.get("any_found"):
        next_action = "Generate memory/luna_module_roles.json via self-map refresh"
    elif blockers:
        next_action = "Inspect verifier log for role/process failures"
    else:
        next_action = "None — discipline acceptable for read-only autonomy"
    return _dimension(
        "process_discipline",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        ["memory/luna_module_roles.json", "Luna_Post_Repair_Verify.ps1"],
    )


def _score_memory_recall(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["memory_recall"])
    thresholds = cfg["status_thresholds"]
    info = check_memory_index_status(project_dir)
    score = 0
    evidence: list[str] = []
    blockers: list[str] = []
    if info["module_exists"]:
        score += 35
        evidence.append("luna_memory_index.py module present")
    else:
        blockers.append("luna_memory_index.py missing")
    if info["schema_exists"]:
        score += 20
        evidence.append("memory_index schema present")
    if info["extras"].get("memory/luna_memory_sources.json"):
        score += 20
        evidence.append("luna_memory_sources.json present")
    if info["test_exists"]:
        score += 25
        evidence.append("test_luna_memory_index.py present")
    next_action = (
        "Wire memory recall into UI as read-only query when ready"
        if not blockers
        else "Restore Phase 5D module before relying on recall"
    )
    return _dimension(
        "memory_recall",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [
            "luna_modules/luna_memory_index.py",
            "memory/luna_memory_index.schema.json",
            "memory/luna_memory_sources.json",
        ],
    )


def _score_file_awareness(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["file_awareness"])
    thresholds = cfg["status_thresholds"]
    info = check_self_map_status(project_dir)
    score = 0
    evidence: list[str] = []
    blockers: list[str] = []
    if info["module_exists"]:
        score += 30
        evidence.append("luna_self_knowledge.py present")
    else:
        blockers.append("luna_self_knowledge.py missing")
    if info["schema_exists"]:
        score += 30
        evidence.append("luna_module_roles.json present")
    if info["extras"].get("memory/luna_risk_zones.json"):
        score += 25
        evidence.append("luna_risk_zones.json present")
    if info["test_exists"]:
        score += 15
        evidence.append("test_luna_self_knowledge.py present")
    next_action = (
        "Refresh curated self-map periodically"
        if not blockers
        else "Restore Phase 5B self-map module"
    )
    return _dimension(
        "file_awareness",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [
            "luna_modules/luna_self_knowledge.py",
            "memory/luna_module_roles.json",
            "memory/luna_risk_zones.json",
        ],
    )


def _score_change_tracking(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["change_tracking"])
    thresholds = cfg["status_thresholds"]
    info = check_change_ledger_status(project_dir)
    score = 0
    evidence: list[str] = []
    blockers: list[str] = []
    if info["module_exists"]:
        score += 40
        evidence.append("luna_change_ledger.py present")
    else:
        blockers.append("luna_change_ledger.py missing")
    if info["schema_exists"]:
        score += 35
        evidence.append("change_ledger schema present")
    if info["test_exists"]:
        score += 25
        evidence.append("test_luna_change_ledger.py present")
    next_action = (
        "Begin appending real change records once gate is wired in"
        if not blockers
        else "Restore Phase 5C ledger module"
    )
    return _dimension(
        "change_tracking",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [
            "luna_modules/luna_change_ledger.py",
            "memory/luna_change_ledger.schema.json",
        ],
    )


def _score_self_healing(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["self_healing"])
    thresholds = cfg["status_thresholds"]
    info = check_playbook_status(project_dir)
    score = 0
    evidence: list[str] = []
    blockers: list[str] = []
    if info["module_exists"]:
        score += 35
        evidence.append("luna_playbook_engine.py present")
    else:
        blockers.append("luna_playbook_engine.py missing")
    if info["schema_exists"]:
        score += 20
        evidence.append("playbooks schema present")
    if info["extras"].get("memory/luna_self_healing_playbooks_seed.json"):
        score += 25
        evidence.append("playbook seed present")
    if info["test_exists"]:
        score += 20
        evidence.append("test_luna_playbook_engine.py present")
    next_action = (
        "Use playbook matches advisory-only until verified"
        if not blockers
        else "Restore Phase 5E playbook module"
    )
    return _dimension(
        "self_healing",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [
            "luna_modules/luna_playbook_engine.py",
            "memory/luna_self_healing_playbooks.schema.json",
            "memory/luna_self_healing_playbooks_seed.json",
        ],
    )


def _score_safe_self_upgrade(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["safe_self_upgrade"])
    thresholds = cfg["status_thresholds"]
    info = check_upgrade_gate_status(project_dir)
    score = 0
    evidence: list[str] = []
    blockers: list[str] = []
    if info["module_exists"]:
        score += 40
        evidence.append("luna_upgrade_gate.py present")
    else:
        blockers.append("luna_upgrade_gate.py missing")
    if info["schema_exists"]:
        score += 20
        evidence.append("upgrade_gate schema present")
    if info["extras"].get("memory/luna_upgrade_gate_policy.json"):
        score += 20
        evidence.append("upgrade_gate policy present (conservative defaults)")
    if info["test_exists"]:
        score += 20
        evidence.append("test_luna_upgrade_gate.py present")
    next_action = (
        "Gate is ready to evaluate proposals — not yet wired into runtime"
        if not blockers
        else "Restore Phase 5F gate module"
    )
    return _dimension(
        "safe_self_upgrade",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [
            "luna_modules/luna_upgrade_gate.py",
            "memory/luna_upgrade_gate_policy.schema.json",
            "memory/luna_upgrade_gate_policy.json",
        ],
    )


def _score_ui_truthfulness(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["ui_truthfulness"])
    thresholds = cfg["status_thresholds"]
    cu = check_cu_status(project_dir)
    bridge = load_json(
        _project_path(project_dir, "memory", "aider_bridge_status.json"), default=None
    )
    verifier = check_verifier_status(project_dir, cfg)
    score = 35
    evidence: list[str] = []
    blockers: list[str] = []
    if cu.get("found"):
        score += 25
        evidence.append(
            f"continues_update_status.json present (ui_status={cu.get('ui_status')!r})"
        )
    else:
        evidence.append("no continues_update_status.json — UI may show stale state")
    if isinstance(bridge, dict):
        score += 20
        evidence.append(f"aider_bridge_status.json present (state={bridge.get('state')!r})")
    if verifier.get("found") and verifier.get("hard_failures") == 0:
        score += 20
        evidence.append("verifier shows no UI/role drift hard failures")
    next_action = (
        "Periodically refresh CU + bridge status snapshots so UI stays truthful"
        if not blockers
        else "Investigate stale status files"
    )
    return _dimension(
        "ui_truthfulness",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [
            "memory/continues_update_status.json",
            "memory/aider_bridge_status.json",
        ],
    )


def _score_aider_reliability(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["aider_reliability"])
    thresholds = cfg["status_thresholds"]
    queue = check_aider_queue_status(project_dir)
    ctx = check_context_efficiency(project_dir)
    score = 30
    evidence: list[str] = []
    blockers: list[str] = []
    if queue.get("exists"):
        score += 15
        evidence.append(
            f"aider_jobs queues: active={queue['active_count']} "
            f"completed={queue['completed_count']} failed={queue['failed_count']} "
            f"quarantine={queue['quarantine_count']}"
        )
        if queue["active_count"] > 5:
            blockers.append(f"active queue depth high ({queue['active_count']})")
    else:
        evidence.append("aider_jobs/ folder not found")
    if ctx.get("max_target_file_bytes"):
        score += 25
        evidence.append(
            f"MAX_TARGET_FILE_BYTES guardrail = {ctx['max_target_file_bytes']}"
        )
    else:
        blockers.append("MAX_TARGET_FILE_BYTES guardrail not detected")
    if ctx.get("ollama_num_ctx"):
        score += 15
        evidence.append(f"OLLAMA_NUM_CTX = {ctx['ollama_num_ctx']}")
    noop_budget = file_exists(_project_path(project_dir, "logs", "aider_bridge_noop_budget.json"))
    if noop_budget:
        score += 15
        evidence.append("noop budget tracker present (24h cooldowns honored)")
    next_action = (
        "Monitor quarantine + noop budgets; do not bulk-flush"
        if not blockers
        else "Address active-queue depth or missing context guardrails"
    )
    return _dimension(
        "aider_reliability",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [
            "aider_bridge.py",
            "logs/aider_bridge_noop_budget.json",
            "memory/aider_bridge_status.json",
        ],
    )


def _score_test_coverage(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["test_coverage"])
    thresholds = cfg["status_thresholds"]
    info = check_test_coverage(project_dir)
    score = 0
    evidence: list[str] = []
    blockers: list[str] = []
    if not info.get("exists"):
        blockers.append("tests/ directory missing")
    else:
        n = info.get("test_files", 0)
        score += min(40, n * 4)
        evidence.append(f"tests/ contains {n} test file(s)")
        phase_n = info.get("phase_tests_count", 0)
        score += min(60, phase_n * 12)
        evidence.append(f"phase tests present: {phase_n}/6")
        if phase_n < 5:
            blockers.append(f"only {phase_n} of 6 phase tests present")
    next_action = (
        "Maintain phase tests; add new ones per phase"
        if not blockers
        else "Add missing phase tests"
    )
    return _dimension(
        "test_coverage",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        ["tests/"],
    )


def _score_autonomy_safety(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["autonomy_safety"])
    thresholds = cfg["status_thresholds"]
    cu = check_cu_status(project_dir)
    git = check_git_clean(project_dir)
    gate = check_upgrade_gate_status(project_dir)
    score = 25
    evidence: list[str] = []
    blockers: list[str] = []
    if cu.get("paused_user_stop"):
        score += 25
        evidence.append("LUNA_STOP_NOW.flag present (user-controlled stop)")
    else:
        evidence.append("no kill flag (Luna is not paused)")
    if gate["module_exists"] and gate["extras"].get("memory/luna_upgrade_gate_policy.json"):
        score += 25
        evidence.append("upgrade gate available with conservative policy")
    else:
        blockers.append("upgrade gate not fully available")
    if git.get("ok"):
        if git.get("tracked_dirty_clean"):
            score += 25
            evidence.append("git tracked tree clean")
        else:
            blockers.append(
                f"git tracked tree dirty ({git.get('tracked_dirty_count')} files)"
            )
    else:
        evidence.append(f"git status unavailable ({git.get('error', 'unknown')})")
    next_action = (
        "Stay at Tier 2 until full upgrade-gate dry-run review"
        if not blockers
        else "Resolve dirty git or restore upgrade gate before raising autonomy"
    )
    return _dimension(
        "autonomy_safety",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [
            "LUNA_STOP_NOW.flag",
            "memory/luna_upgrade_gate_policy.json",
        ],
    )


def _score_resource_awareness(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["resource_awareness"])
    thresholds = cfg["status_thresholds"]
    info = check_resource_awareness(project_dir)
    score = 25
    evidence: list[str] = []
    blockers: list[str] = []
    if info.get("any_found"):
        score += 35
        evidence.append(f"candidates found: {info['candidates_found']}")
    else:
        evidence.append("no resource monitor module/state file found")
    next_action = (
        "Add a small CPU/memory heartbeat module in a later phase"
        if not info.get("any_found")
        else "Wire resource heartbeat into UI as read-only telemetry"
    )
    return _dimension(
        "resource_awareness",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        [],
        forced_status="watch" if not info.get("any_found") else None,
    )


def _score_rollback_readiness(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["rollback_readiness"])
    thresholds = cfg["status_thresholds"]
    info = check_rollback_readiness(project_dir)
    ledger = check_change_ledger_status(project_dir)
    score = 0
    evidence: list[str] = []
    blockers: list[str] = []
    if info.get("backup_count", 0) > 0:
        score += 40
        evidence.append(
            f"backups/ has {info['backup_count']} folder(s); latest={info.get('latest_backup')}"
        )
    else:
        blockers.append("no backups/ folder content")
    if info.get("git_head_oneline"):
        score += 30
        evidence.append(f"git HEAD: {info['git_head_oneline']}")
    else:
        blockers.append("git HEAD unreadable")
    if ledger["module_exists"]:
        score += 30
        evidence.append("change ledger ready for rollback record-keeping")
    next_action = (
        "Maintain pre-phase backup folders + commit per phase"
        if not blockers
        else "Restore git availability and create a backup before further changes"
    )
    return _dimension(
        "rollback_readiness",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        ["backups/", "luna_modules/luna_change_ledger.py"],
    )


def _score_intent_alignment(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["intent_alignment"])
    thresholds = cfg["status_thresholds"]
    info = check_intent_alignment(project_dir)
    score = 30
    evidence: list[str] = []
    blockers: list[str] = []
    if info.get("any_found"):
        score += 30
        evidence.append(f"intent artifacts: {info['candidates_found']}")
    else:
        evidence.append("no formal active-goal/drift-detector artifacts yet")
    next_action = (
        "Add memory/luna_active_goal.json + drift detector in a later phase"
        if not info.get("any_found")
        else "Use active goal to gate non-trivial proposals"
    )
    return _dimension(
        "intent_alignment",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        ["memory/luna_active_goal.json"],
        forced_status="watch" if not info.get("any_found") else None,
    )


def _score_context_efficiency(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    weight = int(cfg["dimension_weights"]["context_efficiency"])
    thresholds = cfg["status_thresholds"]
    info = check_context_efficiency(project_dir)
    score = 30
    evidence: list[str] = []
    blockers: list[str] = []
    if info.get("max_target_file_bytes"):
        score += 35
        evidence.append(f"MAX_TARGET_FILE_BYTES = {info['max_target_file_bytes']}")
    else:
        blockers.append("MAX_TARGET_FILE_BYTES not discoverable in aider_bridge.py")
    if info.get("ollama_num_ctx"):
        score += 25
        evidence.append(f"num_ctx = {info['ollama_num_ctx']}")
    next_action = (
        "Keep MAX_TARGET_FILE_BYTES low; only raise after measured stability"
        if not blockers
        else "Restore size guardrails in aider_bridge.py"
    )
    return _dimension(
        "context_efficiency",
        score,
        weight,
        thresholds,
        evidence,
        blockers,
        next_action,
        ["aider_bridge.py"],
    )


_SCORERS = {
    "boot_health": _score_boot_health,
    "process_discipline": _score_process_discipline,
    "memory_recall": _score_memory_recall,
    "file_awareness": _score_file_awareness,
    "change_tracking": _score_change_tracking,
    "self_healing": _score_self_healing,
    "safe_self_upgrade": _score_safe_self_upgrade,
    "ui_truthfulness": _score_ui_truthfulness,
    "aider_reliability": _score_aider_reliability,
    "test_coverage": _score_test_coverage,
    "autonomy_safety": _score_autonomy_safety,
    "resource_awareness": _score_resource_awareness,
    "rollback_readiness": _score_rollback_readiness,
    "intent_alignment": _score_intent_alignment,
    "context_efficiency": _score_context_efficiency,
}


# ---------- top-level ----------


def _derive_overall(dimensions: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    overall = weighted_average(((d["score"], d["weight"]) for d in dimensions))
    rules = cfg.get("readiness_rules", {})
    boot = next((d for d in dimensions if d["name"] == "boot_health"), None)
    autonomy = next((d for d in dimensions if d["name"] == "autonomy_safety"), None)
    critical_blockers: list[str] = []
    for d in dimensions:
        for b in d.get("blockers", []):
            critical_blockers.append(f"{d['name']}: {b}")
    if boot and boot["score"] < int(rules.get("block_if_boot_health_below", 60)):
        readiness = "not_ready"
    elif autonomy and autonomy["score"] < int(rules.get("block_if_autonomy_safety_below", 60)):
        readiness = "not_ready"
    elif rules.get("block_if_any_critical_blocker", True) and any(
        d["status"] == "blocked" for d in dimensions
    ):
        readiness = "not_ready"
    elif overall < int(rules.get("not_ready_overall_below", 40)):
        readiness = "not_ready"
    elif overall < int(rules.get("safe_foundation_overall_below", 65)):
        readiness = "safe_foundation"
    elif overall < int(rules.get("controlled_autonomy_overall_below", 80)):
        readiness = "controlled_autonomy_ready"
    elif overall < int(rules.get("limited_self_upgrade_overall_below", 90)):
        readiness = "limited_self_upgrade_ready"
    else:
        readiness = "limited_self_upgrade_ready"
    overall_status = _status_for_score(overall, cfg["status_thresholds"])
    if any(d["status"] == "blocked" for d in dimensions) and overall_status not in ("blocked", "degraded"):
        overall_status = "degraded"
    return {
        "overall_score": overall,
        "overall_status": overall_status,
        "readiness_level": readiness,
        "critical_blockers": critical_blockers,
    }


def build_capability_scorecard(
    project_dir: Path | str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    cfg = config or load_config()
    dimensions: list[dict[str, Any]] = []
    for name in REQUIRED_DIMENSIONS:
        scorer = _SCORERS[name]
        try:
            dimensions.append(scorer(pdir, cfg))
        except Exception as e:  # noqa: BLE001
            dimensions.append(
                _dimension(
                    name,
                    0,
                    int(cfg["dimension_weights"].get(name, 1)),
                    cfg["status_thresholds"],
                    [f"scorer_error: {type(e).__name__}: {str(e)[:160]}"],
                    [f"{name} scorer raised exception"],
                    "Investigate scorer exception",
                    [],
                    forced_status="unknown",
                )
            )
    derived = _derive_overall(dimensions, cfg)
    safe_next_steps: list[str] = []
    approval_required_for: list[str] = []
    for d in dimensions:
        if d["status"] in ("watch", "degraded") and d["recommended_next_action"]:
            safe_next_steps.append(f"{d['name']}: {d['recommended_next_action']}")
        if d["status"] == "blocked":
            approval_required_for.append(
                f"{d['name']}: {d['recommended_next_action']}"
            )
    notes = [
        "Phase 5G scorecard. Read-mostly. Not wired into runtime services.",
        "Readiness intentionally conservative until Phase 5H planning approves elevation.",
    ]
    record = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "project_dir": str(pdir).replace("\\", "/"),
        **derived,
        "dimensions": dimensions,
        "safe_next_steps": safe_next_steps,
        "approval_required_for": approval_required_for,
        "notes": notes,
    }
    return record


def validate_scorecard(record: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(record, dict):
        return False, ["record is not a dict"]
    required = (
        "schema_version",
        "generated_at",
        "project_dir",
        "overall_score",
        "overall_status",
        "readiness_level",
        "dimensions",
    )
    for k in required:
        if k not in record:
            errors.append(f"missing top-level key: {k}")
    if record.get("overall_status") not in _VALID_STATUSES:
        errors.append(f"invalid overall_status: {record.get('overall_status')!r}")
    if record.get("readiness_level") not in _VALID_READINESS:
        errors.append(f"invalid readiness_level: {record.get('readiness_level')!r}")
    dims = record.get("dimensions")
    if not isinstance(dims, list) or not dims:
        errors.append("dimensions must be a non-empty list")
    else:
        names_seen = set()
        for i, d in enumerate(dims):
            if not isinstance(d, dict):
                errors.append(f"dimensions[{i}] is not a dict")
                continue
            for k in (
                "name",
                "score",
                "weight",
                "status",
                "evidence",
                "blockers",
                "recommended_next_action",
                "source_files",
            ):
                if k not in d:
                    errors.append(f"dimensions[{i}].{k} missing")
            if d.get("status") not in _VALID_STATUSES:
                errors.append(f"dimensions[{i}].status invalid: {d.get('status')!r}")
            try:
                s = int(d.get("score", -1))
                if s < 0 or s > 100:
                    errors.append(f"dimensions[{i}].score out of range: {s}")
            except (TypeError, ValueError):
                errors.append(f"dimensions[{i}].score not int")
            names_seen.add(d.get("name"))
        for required_name in REQUIRED_DIMENSIONS:
            if required_name not in names_seen:
                errors.append(f"required dimension missing: {required_name}")
    return (not errors), errors


# ---------- rendering / writing ----------


def render_scorecard_markdown(record: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Luna Capability Scorecard")
    lines.append("")
    lines.append(f"- **Generated**: {record.get('generated_at', '?')}")
    lines.append(f"- **Project**: `{record.get('project_dir', '?')}`")
    lines.append(f"- **Overall score**: **{record.get('overall_score', 0)}/100**")
    lines.append(f"- **Overall status**: `{record.get('overall_status', '?')}`")
    lines.append(f"- **Readiness level**: `{record.get('readiness_level', '?')}`")
    lines.append("")
    lines.append("## Dimensions")
    lines.append("")
    lines.append("| Dimension | Score | Weight | Status |")
    lines.append("|-----------|------:|------:|--------|")
    for d in record.get("dimensions", []):
        lines.append(
            f"| {d['name']} | {d['score']} | {d['weight']} | {d['status']} |"
        )
    lines.append("")
    crit = record.get("critical_blockers") or []
    if crit:
        lines.append("## Critical blockers")
        for b in crit:
            lines.append(f"- {b}")
        lines.append("")
    next_steps = record.get("safe_next_steps") or []
    if next_steps:
        lines.append("## Safe next steps")
        for s in next_steps:
            lines.append(f"- {s}")
        lines.append("")
    approvals = record.get("approval_required_for") or []
    if approvals:
        lines.append("## Approval required")
        for a in approvals:
            lines.append(f"- {a}")
        lines.append("")
    lines.append("## Per-dimension detail")
    for d in record.get("dimensions", []):
        lines.append("")
        lines.append(f"### {d['name']} — {d['score']}/100 ({d['status']})")
        lines.append(f"- **Weight**: {d['weight']}")
        if d.get("evidence"):
            lines.append("- **Evidence**:")
            for e in d["evidence"]:
                lines.append(f"  - {e}")
        if d.get("blockers"):
            lines.append("- **Blockers**:")
            for b in d["blockers"]:
                lines.append(f"  - {b}")
        if d.get("recommended_next_action"):
            lines.append(f"- **Next**: {d['recommended_next_action']}")
        if d.get("source_files"):
            lines.append(f"- **Source files**: {', '.join(d['source_files'])}")
    notes = record.get("notes") or []
    if notes:
        lines.append("")
        lines.append("## Notes")
        for n in notes:
            lines.append(f"- {n}")
    return "\n".join(lines) + "\n"


def _atomic_write(path: Path, data: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if isinstance(data, str):
        tmp.write_text(data, encoding="utf-8")
    else:
        tmp.write_bytes(data)
    os.replace(tmp, path)


def write_scorecard(
    record: dict[str, Any],
    json_path: Path | str,
    markdown_path: Path | str | None = None,
    build_report_path: Path | str | None = None,
    project_root: Path | str | None = None,
) -> dict[str, Any]:
    json_p = Path(json_path)
    root = Path(project_root).resolve() if project_root else _PROJECT_DIR_DEFAULT.resolve()
    try:
        json_p.resolve().relative_to(root)
    except ValueError:
        raise ValueError(f"json_path must be inside project root: {json_p}")
    _atomic_write(json_p, json.dumps(record, indent=2, sort_keys=False))
    written: dict[str, Any] = {"json": str(json_p)}
    if markdown_path:
        md_p = Path(markdown_path)
        try:
            md_p.resolve().relative_to(root)
        except ValueError:
            raise ValueError(f"markdown_path must be inside project root: {md_p}")
        _atomic_write(md_p, render_scorecard_markdown(record))
        written["markdown"] = str(md_p)
    if build_report_path:
        rp = Path(build_report_path)
        try:
            rp.resolve().relative_to(root)
        except ValueError:
            raise ValueError(f"build_report_path must be inside project root: {rp}")
        report = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": record.get("generated_at"),
            "overall_score": record.get("overall_score"),
            "overall_status": record.get("overall_status"),
            "readiness_level": record.get("readiness_level"),
            "dimension_count": len(record.get("dimensions", [])),
            "critical_blocker_count": len(record.get("critical_blockers") or []),
            "wrote": written,
        }
        _atomic_write(rp, json.dumps(report, indent=2))
        written["build_report"] = str(rp)
    return written


# ---------- self test ----------


def _build_temp_fixture(td: Path) -> dict[str, Any]:
    (td / "memory").mkdir(parents=True, exist_ok=True)
    (td / "luna_modules").mkdir(parents=True, exist_ok=True)
    (td / "tests").mkdir(parents=True, exist_ok=True)
    (td / "logs").mkdir(parents=True, exist_ok=True)
    (td / "backups").mkdir(parents=True, exist_ok=True)
    (td / "aider_jobs" / "active").mkdir(parents=True, exist_ok=True)
    (td / "aider_jobs" / "completed").mkdir(parents=True, exist_ok=True)
    (td / "aider_jobs" / "failed").mkdir(parents=True, exist_ok=True)
    (td / "aider_jobs" / "quarantine").mkdir(parents=True, exist_ok=True)
    (td / "worker.py").write_text("x = 1\n", encoding="utf-8")
    (td / "aider_bridge.py").write_text(
        "MAX_TARGET_FILE_BYTES = 50000\nOLLAMA_NUM_CTX = 16384\n",
        encoding="utf-8",
    )
    (td / "luna_guardian.py").write_text("x = 1\n", encoding="utf-8")
    (td / "director_agent.py").write_text("x = 1\n", encoding="utf-8")
    (td / "SurgeApp_Claude_Terminal.py").write_text("x = 1\n", encoding="utf-8")
    (td / "LaunchLuna.pyw").write_text("x = 1\n", encoding="utf-8")
    (td / "luna_start.pyw").write_text("x = 1\n", encoding="utf-8")
    (td / "luna_modules" / "luna_self_knowledge.py").write_text("x = 1\n", encoding="utf-8")
    (td / "luna_modules" / "luna_change_ledger.py").write_text("x = 1\n", encoding="utf-8")
    (td / "luna_modules" / "luna_memory_index.py").write_text("x = 1\n", encoding="utf-8")
    (td / "luna_modules" / "luna_playbook_engine.py").write_text("x = 1\n", encoding="utf-8")
    (td / "luna_modules" / "luna_upgrade_gate.py").write_text("x = 1\n", encoding="utf-8")
    for fname in (
        "test_luna_self_knowledge.py",
        "test_luna_change_ledger.py",
        "test_luna_memory_index.py",
        "test_luna_playbook_engine.py",
        "test_luna_upgrade_gate.py",
        "test_luna_capability_scorecard.py",
    ):
        (td / "tests" / fname).write_text("x = 1\n", encoding="utf-8")
    (td / "memory" / "luna_module_roles.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "luna_risk_zones.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "luna_change_ledger.schema.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "luna_memory_index.schema.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "luna_memory_sources.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "luna_self_healing_playbooks.schema.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "luna_self_healing_playbooks_seed.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "luna_upgrade_gate_policy.schema.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "luna_upgrade_gate_policy.json").write_text("{}", encoding="utf-8")
    (td / "memory" / "continues_update_status.json").write_text(
        json.dumps({"ui_status": "idle", "last_status": "ok"}), encoding="utf-8"
    )
    (td / "memory" / "aider_bridge_status.json").write_text(
        json.dumps({"state": "idle"}), encoding="utf-8"
    )
    (td / "logs" / "luna_post_repair_verify_20260101_000000.txt").write_text(
        "============================================================\n"
        "8. Summary\n"
        "============================================================\n"
        "[PASS] No hard failures found.\n"
        "[PASS] No warnings found.\n",
        encoding="utf-8",
    )
    (td / "logs" / "aider_bridge_noop_budget.json").write_text("{}", encoding="utf-8")
    (td / "backups" / "phase5g_temp").mkdir(parents=True, exist_ok=True)
    cfg = dict(_DEFAULT_CONFIG)
    return cfg


def self_test() -> int:
    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        cfg = _build_temp_fixture(td)
        record = build_capability_scorecard(td, cfg)
        ok, errs = validate_scorecard(record)
        if not ok:
            print(json.dumps({"ok": False, "errors": errs}, indent=2))
            return 1
        md = render_scorecard_markdown(record)
        if "Luna Capability Scorecard" not in md:
            print(json.dumps({"ok": False, "error": "markdown missing title"}))
            return 1
        for name in REQUIRED_DIMENSIONS:
            if name not in md:
                print(json.dumps({"ok": False, "error": f"markdown missing {name}"}))
                return 1
        json_p = td / "memory" / "luna_capability_scorecard.json"
        md_p = td / "memory" / "luna_capability_scorecard.md"
        rp = td / "memory" / "luna_capability_scorecard_build_report.json"
        write_scorecard(record, json_p, md_p, rp, project_root=td)
        out = {
            "ok": True,
            "overall_score": record["overall_score"],
            "overall_status": record["overall_status"],
            "readiness_level": record["readiness_level"],
            "dimension_count": len(record["dimensions"]),
        }
        print(json.dumps(out, indent=2))
        return 0


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Luna Capability Scorecard (Phase 5G)")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--print-markdown", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    parser.add_argument(
        "--out-json",
        default=str(_PROJECT_DIR_DEFAULT / "memory" / "luna_capability_scorecard.json"),
    )
    parser.add_argument(
        "--out-md",
        default=str(_PROJECT_DIR_DEFAULT / "memory" / "luna_capability_scorecard.md"),
    )
    parser.add_argument(
        "--out-report",
        default=str(
            _PROJECT_DIR_DEFAULT / "memory" / "luna_capability_scorecard_build_report.json"
        ),
    )
    args = parser.parse_args(argv)
    if args.self_test:
        return self_test()
    pdir = Path(args.project_dir)
    record = build_capability_scorecard(pdir)
    if args.write:
        write_scorecard(record, args.out_json, args.out_md, args.out_report)
        print(
            json.dumps(
                {
                    "wrote_json": args.out_json,
                    "wrote_md": args.out_md,
                    "wrote_report": args.out_report,
                    "overall_score": record["overall_score"],
                    "readiness_level": record["readiness_level"],
                },
                indent=2,
            )
        )
        return 0
    if args.print_markdown:
        sys.stdout.write(render_scorecard_markdown(record))
        return 0
    print(
        json.dumps(
            {
                "overall_score": record["overall_score"],
                "overall_status": record["overall_status"],
                "readiness_level": record["readiness_level"],
                "dimension_count": len(record["dimensions"]),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
