"""Phase 5K + 5K2: Luna Limited Routine Autonomy controller foundation.

Stdlib only. Read-mostly. Coordinates Phase 5B-5J foundation modules for safe
limited *routine* work — day or night, same safety rules. Refuses code edits,
Aider, installs, queue/log/memory deletion, worker/Guardian/bridge/launcher
edits, external network, git_reset/git_clean/git_push, process kill, and
continues_update_start.

Phase 5K2 renamed the operator-facing concept from "overnight" to "routine".
Backwards compatibility is preserved: every old field/CLI flag still works as
an alias of the new routine names.

Hard rules:
  * `safe_to_run_routine_code_edits` (alias: `safe_to_run_overnight_code_edits`)
    is ALWAYS False until Phase 5L (Delegated AI Approval Council) is built and
    an explicit operator config enables it.
  * Generated artifacts stay under `memory/luna_limited_autonomy*` and a few
    Phase-5 gitignored report files. Source files are never touched.
  * Optional Phase 5B-5J modules are imported with try/except and degrade
    gracefully if missing or broken.
  * Operator stop files (LUNA_STOP_NOW.flag, memory/limited_autonomy.stop,
    memory/routine_autonomy.stop, memory/continues_update.stop) always halt
    the cycle.

Tracked schema/policy:
  memory/luna_limited_autonomy.schema.json
  memory/luna_routine_policy.json   (canonical, Phase 5K2)
  memory/luna_overnight_policy.json (legacy alias, Phase 5K)
  memory/luna_autonomy_tiers.json

Generated runtime artifacts (gitignored):
  memory/luna_limited_autonomy_state.json
  memory/luna_limited_autonomy_cycle.jsonl
  memory/luna_limited_autonomy_report.json
  memory/luna_limited_autonomy_report.md
  memory/luna_overnight_brief.md
  memory/luna_recommended_next_actions.json
  memory/luna_autonomy_run_lock.json

CLI (Phase 5K2 prefers --routine-* names; --overnight-* still work):
  python -m luna_modules.luna_limited_autonomy --self-test
  python -m luna_modules.luna_limited_autonomy --plan "Improve Luna safely"
  python -m luna_modules.luna_limited_autonomy --run-once --dry-run
  python -m luna_modules.luna_limited_autonomy --routine-once --dry-run
  python -m luna_modules.luna_limited_autonomy --run-once --execute-generated-artifacts
  python -m luna_modules.luna_limited_autonomy --routine-dry-run --max-cycles 2
  python -m luna_modules.luna_limited_autonomy --routine-loop --max-cycles 2
  python -m luna_modules.luna_limited_autonomy --overnight-dry-run --max-cycles 2  # legacy alias
  python -m luna_modules.luna_limited_autonomy --print-report
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

# Optional Phase 5 foundation imports — every import wrapped to keep the
# controller working even if a module is missing or breaks.
try:  # pragma: no cover
    from luna_modules import luna_self_knowledge as _self_knowledge  # type: ignore
except Exception:  # pragma: no cover
    _self_knowledge = None
try:  # pragma: no cover
    from luna_modules import luna_change_ledger as _change_ledger  # type: ignore
except Exception:  # pragma: no cover
    _change_ledger = None
try:  # pragma: no cover
    from luna_modules import luna_memory_index as _memory_index  # type: ignore
except Exception:  # pragma: no cover
    _memory_index = None
try:  # pragma: no cover
    from luna_modules import luna_playbook_engine as _playbook_engine  # type: ignore
except Exception:  # pragma: no cover
    _playbook_engine = None
try:  # pragma: no cover
    from luna_modules import luna_upgrade_gate as _upgrade_gate  # type: ignore
except Exception:  # pragma: no cover
    _upgrade_gate = None
try:  # pragma: no cover
    from luna_modules import luna_capability_scorecard as _scorecard  # type: ignore
except Exception:  # pragma: no cover
    _scorecard = None
try:  # pragma: no cover
    from luna_modules import luna_task_graph as _task_graph  # type: ignore
except Exception:  # pragma: no cover
    _task_graph = None
try:  # pragma: no cover
    from luna_modules import luna_sandbox as _sandbox  # type: ignore
except Exception:  # pragma: no cover
    _sandbox = None
try:  # pragma: no cover
    from luna_modules import luna_resource_monitor as _resource_monitor  # type: ignore
except Exception:  # pragma: no cover
    _resource_monitor = None


ALLOWED_TASK_CLASSES: tuple[str, ...] = (
    "read_only_health_check",
    "file_map_refresh",
    "memory_index_refresh",
    "scorecard_refresh",
    "resource_snapshot",
    "playbook_match_recent_failures",
    "task_graph_plan_only",
    "sandbox_self_test",
    "upgrade_gate_evaluate_only",
    "daily_brief_report",
    "recommended_next_actions_report",
)

FORBIDDEN_TASK_CLASSES: tuple[str, ...] = (
    "code_edit",
    "aider_patch",
    "package_install",
    "multi_file_refactor",
    "worker_edit",
    "guardian_edit",
    "bridge_edit",
    "launcher_edit",
    "memory_delete",
    "log_delete",
    "queue_delete",
    "external_network",
    "git_reset",
    "git_clean",
    "git_push",
    "process_kill",
    "continues_update_start",
)

_DEFAULT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "default_mode": "read_only",
    "max_cycles": 12,
    "sleep_seconds": 300,
    "max_runtime_minutes": 360,
    "allow_code_edits": False,
    "allow_aider": False,
    "allow_installs": False,
    "allow_process_kill": False,
    "allow_external_network": False,
    "allowed_task_classes": list(ALLOWED_TASK_CLASSES),
    "forbidden_task_classes": list(FORBIDDEN_TASK_CLASSES),
    "stop_files": [
        "LUNA_STOP_NOW.flag",
        "memory/limited_autonomy.stop",
        "memory/routine_autonomy.stop",
        "memory/continues_update.stop",
    ],
    "requires_clean_verifier": True,
    "requires_git_clean_for_generated_only": True,
    "max_per_cycle_runtime_seconds": 600,
    "lock_stale_seconds": 3600,
}

_DEFAULT_TIERS: dict[str, Any] = {
    "schema_version": 1,
    "active_tier_max_for_phase_5k": 1,
    "tiers": [
        {"tier": 0, "name": "read_only", "allowed_task_classes": ["read_only_health_check", "resource_snapshot"]},
        {"tier": 1, "name": "reports_and_memory_refresh", "allowed_task_classes": list(ALLOWED_TASK_CLASSES)},
        {"tier": 2, "name": "proposal_only", "allowed_task_classes": ["task_graph_plan_only", "upgrade_gate_evaluate_only"], "phase_5k_status": "blocked"},
        {"tier": 3, "name": "sandbox_preview_with_approval", "allowed_task_classes": ["sandbox_self_test"], "phase_5k_status": "blocked"},
        {"tier": 4, "name": "limited_low_risk_auto_edit_future_disabled", "allowed_task_classes": [], "phase_5k_status": "disabled"},
        {"tier": 5, "name": "emergency_repair_future_disabled", "allowed_task_classes": [], "phase_5k_status": "disabled"},
    ],
    "non_delegable": [
        "personality_change", "identity_change", "goals_change",
        "memory_delete", "log_delete", "queue_delete",
        "kill_switch_remove", "package_install", "external_network",
        "git_reset", "git_push_force",
    ],
}

DEFAULT_ROUTINE_POLICY_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_routine_policy.json"
DEFAULT_OVERNIGHT_POLICY_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_overnight_policy.json"
# Legacy alias retained for any external caller.
DEFAULT_POLICY_PATH = DEFAULT_OVERNIGHT_POLICY_PATH
DEFAULT_TIERS_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_autonomy_tiers.json"
DEFAULT_LOCK_PATH = _PROJECT_DIR_DEFAULT / "memory" / "luna_autonomy_run_lock.json"


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_cycle_id(prefix: str = "auto") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def load_json(path: Path | str, default: Any = None) -> Any:
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


def append_jsonl(path: Path | str, row: dict[str, Any]) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=False) + "\n")
    return p


def _ensure_under(path: Path, project_root: Path) -> Path:
    p = Path(path).resolve()
    root = Path(project_root).resolve()
    try:
        p.relative_to(root)
    except ValueError:
        raise ValueError(f"path escapes project root: {p}")
    return p


def load_routine_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    """Phase 5K2: load the canonical limited-routine-autonomy policy.

    Tries memory/luna_routine_policy.json first (Phase 5K2 canonical), then
    falls back to memory/luna_overnight_policy.json (Phase 5K legacy alias),
    then to the in-module default. Hard rules (no code edits, no Aider, no
    installs, no process kill, no external network) are re-applied after load
    regardless of file contents.
    """
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    routine_p = pdir / "memory" / "luna_routine_policy.json"
    legacy_p = pdir / "memory" / "luna_overnight_policy.json"
    raw: Any = None
    source = "module_fallback"
    loaded_from_file = False
    if routine_p.is_file():
        raw = load_json(routine_p, default=None)
        if isinstance(raw, dict):
            source = str(routine_p)
            loaded_from_file = True
    if not isinstance(raw, dict) and legacy_p.is_file():
        raw = load_json(legacy_p, default=None)
        if isinstance(raw, dict):
            source = str(legacy_p)
            loaded_from_file = True
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_POLICY)
        out["_source"] = source
        out["_loaded_from_file"] = False
        return out
    out = dict(_DEFAULT_POLICY)
    for k, v in raw.items():
        out[k] = v
    out["_source"] = source
    out["_loaded_from_file"] = loaded_from_file
    # Hard rule: code edits forbidden regardless of file contents.
    out["allow_code_edits"] = False
    out["allow_aider"] = False
    out["allow_installs"] = False
    out["allow_process_kill"] = False
    out["allow_external_network"] = False
    return out


# Legacy alias retained — Phase 5K callers and tests still work.
def load_overnight_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    """Backwards-compatible alias of `load_routine_policy` (Phase 5K2)."""
    return load_routine_policy(project_dir)


def load_autonomy_tiers(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_autonomy_tiers.json"
    if not p.is_file():
        p = DEFAULT_TIERS_PATH
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_TIERS)
        out["_source"] = "module_fallback"
        out["_loaded_from_file"] = False
        return out
    out = dict(_DEFAULT_TIERS)
    for k, v in raw.items():
        out[k] = v
    out["_source"] = str(p)
    out["_loaded_from_file"] = True
    return out


# ---------- locks + stop checks ----------


def acquire_autonomy_lock(
    project_dir: Path | str,
    stale_seconds: int = 3600,
    lock_path: Path | str | None = None,
) -> dict[str, Any]:
    pdir = Path(project_dir)
    lock_p = Path(lock_path) if lock_path else pdir / "memory" / "luna_autonomy_run_lock.json"
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    now_ts = time.time()
    if lock_p.is_file():
        try:
            existing = load_json(lock_p, default={}) or {}
            created_at = float(existing.get("created_ts", 0))
            age = now_ts - created_at
            if age < stale_seconds:
                return {
                    "acquired": False,
                    "reason": "fresh_lock",
                    "lock_age_seconds": int(age),
                    "stale_seconds": stale_seconds,
                    "lock_path": str(lock_p),
                    "lock_id": existing.get("lock_id"),
                }
        except Exception:
            pass
    lock_id = make_cycle_id("lock")
    write_json_atomic(
        lock_p,
        {
            "lock_id": lock_id,
            "created_at": now_iso(),
            "created_ts": now_ts,
            "pid": os.getpid(),
            "host": os.environ.get("COMPUTERNAME", "") or os.environ.get("HOSTNAME", ""),
            "stale_seconds": stale_seconds,
        },
    )
    return {"acquired": True, "lock_id": lock_id, "lock_path": str(lock_p)}


def release_autonomy_lock(
    project_dir: Path | str,
    lock_id: str | None = None,
    lock_path: Path | str | None = None,
) -> dict[str, Any]:
    pdir = Path(project_dir)
    lock_p = Path(lock_path) if lock_path else pdir / "memory" / "luna_autonomy_run_lock.json"
    if not lock_p.is_file():
        return {"released": False, "reason": "no_lock_present"}
    if lock_id:
        existing = load_json(lock_p, default={}) or {}
        if existing.get("lock_id") and existing["lock_id"] != lock_id:
            return {"released": False, "reason": "lock_id_mismatch", "expected": lock_id, "actual": existing.get("lock_id")}
    try:
        lock_p.unlink()
    except OSError as e:
        return {"released": False, "reason": f"unlink_failed:{e}"}
    return {"released": True, "lock_path": str(lock_p)}


def check_operator_stop(
    project_dir: Path | str,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or load_overnight_policy(project_dir)
    pdir = Path(project_dir)
    found: list[str] = []
    for rel in pol.get("stop_files", []):
        if (pdir / rel).is_file():
            found.append(rel)
    return {"stopped": bool(found), "stop_files_present": found}


def check_git_clean(project_dir: Path | str) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=15,
        )
        text = proc.stdout or ""
        tracked_dirty = [
            line for line in text.splitlines()
            if line and not line.startswith("??")
        ]
        return {
            "ok": proc.returncode == 0,
            "tracked_dirty_count": len(tracked_dirty),
            "tracked_dirty_clean": len(tracked_dirty) == 0,
            "tracked_dirty_sample": tracked_dirty[:6],
        }
    except (subprocess.TimeoutExpired, OSError, FileNotFoundError) as e:
        return {"ok": False, "error": f"{type(e).__name__}:{e}", "tracked_dirty_clean": False}


def check_verifier_clean(project_dir: Path | str) -> dict[str, Any]:
    pdir = Path(project_dir)
    logs = pdir / "logs"
    if not logs.is_dir():
        return {"clean": False, "reason": "no_logs_dir"}
    matches = sorted(
        logs.glob("luna_post_repair_verify_*.txt"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
    )
    if not matches:
        return {"clean": False, "reason": "no_report"}
    latest = matches[-1]
    try:
        raw = latest.read_bytes()
    except OSError as e:
        return {"clean": False, "reason": f"read_failed:{e}"}
    if raw.startswith(b"\xff\xfe"):
        text = raw.decode("utf-16-le", errors="replace")
    elif raw.startswith(b"\xfe\xff"):
        text = raw.decode("utf-16-be", errors="replace")
    elif raw.startswith(b"\xef\xbb\xbf"):
        text = raw.decode("utf-8-sig", errors="replace")
    else:
        text = raw.decode("utf-8", errors="replace")
    has_no_fail = "No hard failures found" in text
    has_no_warn = "No warnings found" in text
    fails = text.count("[FAIL]")
    warns = text.count("[WARN]")
    return {
        "clean": bool(has_no_fail and has_no_warn),
        "no_fail": has_no_fail,
        "no_warn": has_no_warn,
        "fail_lines": fails,
        "warn_lines": warns,
        "log": str(latest),
    }


# ---------- runtime context ----------


def build_runtime_context(project_dir: Path | str) -> dict[str, Any]:
    pdir = Path(project_dir)
    ctx: dict[str, Any] = {
        "project_dir": str(pdir).replace("\\", "/"),
        "generated_at": now_iso(),
    }
    ctx["git"] = check_git_clean(pdir)
    ctx["operator_stop"] = check_operator_stop(pdir)
    ctx["verifier"] = check_verifier_clean(pdir)
    ctx["kill_switch_present"] = (pdir / "LUNA_STOP_NOW.flag").is_file()
    ctx["continues_update_stop_present"] = (pdir / "memory" / "continues_update.stop").is_file()
    ctx["limited_autonomy_stop_present"] = (pdir / "memory" / "limited_autonomy.stop").is_file()

    # Optional: Phase 5J resource snapshot
    if _resource_monitor is not None:
        try:
            snap = _resource_monitor.build_resource_snapshot(pdir)
            ctx["resource_mode"] = snap.get("recommended_mode")
            ctx["resource_blockers"] = list(snap.get("blockers") or [])
            ctx["resource_warnings"] = list(snap.get("warnings") or [])
        except Exception as e:
            ctx["resource_error"] = f"{type(e).__name__}:{str(e)[:160]}"
    # Optional: Phase 5G capability scorecard
    if _scorecard is not None:
        try:
            sc = _scorecard.build_capability_scorecard(pdir)
            ctx["scorecard_overall_score"] = sc.get("overall_score")
            ctx["scorecard_status"] = sc.get("overall_status")
            ctx["scorecard_readiness_level"] = sc.get("readiness_level")
        except Exception as e:
            ctx["scorecard_error"] = f"{type(e).__name__}:{str(e)[:160]}"

    # Queue counts (best-effort)
    try:
        aj = pdir / "aider_jobs"
        ctx["aider_active"] = sum(1 for _ in (aj / "active").iterdir()) if (aj / "active").is_dir() else 0
        ctx["aider_quarantine"] = sum(1 for _ in (aj / "quarantine").iterdir()) if (aj / "quarantine").is_dir() else 0
    except OSError:
        ctx["aider_active"] = -1
        ctx["aider_quarantine"] = -1

    # Aider bridge / worker status
    bs = load_json(pdir / "memory" / "aider_bridge_status.json", default=None)
    if isinstance(bs, dict):
        ctx["bridge_state"] = bs.get("state")
    cu = load_json(pdir / "memory" / "continues_update_status.json", default=None)
    if isinstance(cu, dict):
        ctx["cu_ui_status"] = cu.get("ui_status")
        ctx["cu_last_status"] = cu.get("last_status")

    # Recent failures review file
    rf = pdir / "logs" / "luna_recent_failures_review.txt"
    if rf.is_file():
        try:
            ctx["recent_failures_tail"] = rf.read_text(encoding="utf-8", errors="replace")[-2048:]
        except OSError:
            pass

    # Memory index build report
    mi = pdir / "memory" / "luna_memory_index_build_report.json"
    if mi.is_file():
        ctx["memory_index_build_report_present"] = True

    # Active autonomy lock
    lock = pdir / "memory" / "luna_autonomy_run_lock.json"
    if lock.is_file():
        ld = load_json(lock, default={})
        if isinstance(ld, dict):
            ctx["active_lock"] = {
                "lock_id": ld.get("lock_id"),
                "created_at": ld.get("created_at"),
                "pid": ld.get("pid"),
            }
    return ctx


def classify_allowed_task_classes(
    context: dict[str, Any],
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pol = policy or _DEFAULT_POLICY
    allowed = list(pol.get("allowed_task_classes") or [])
    skipped: list[dict[str, Any]] = []
    blockers: list[str] = []

    if context.get("operator_stop", {}).get("stopped"):
        blockers.append(
            f"operator_stop: {context['operator_stop'].get('stop_files_present')}"
        )
        return {"allowed": [], "skipped": allowed, "blockers": blockers}

    if pol.get("requires_clean_verifier", True):
        v = context.get("verifier") or {}
        if not v.get("clean"):
            blockers.append(
                f"verifier_not_clean: fails={v.get('fail_lines')} warns={v.get('warn_lines')} reason={v.get('reason', 'n/a')}"
            )

    if pol.get("requires_git_clean_for_generated_only", True):
        g = context.get("git") or {}
        if not g.get("tracked_dirty_clean"):
            blockers.append(
                f"git_dirty: count={g.get('tracked_dirty_count', '?')}"
            )

    res_mode = context.get("resource_mode")
    if res_mode in ("blocked",):
        blockers.append(f"resource_mode_blocked: {context.get('resource_blockers')}")
        return {"allowed": [], "skipped": allowed, "blockers": blockers}
    high_intensity_drop = res_mode in ("pause_high_intensity", "hibernate")
    final_allowed: list[str] = []
    for tc in allowed:
        if tc not in ALLOWED_TASK_CLASSES:
            skipped.append({"task_class": tc, "reason": "not_in_phase_5k_allowlist"})
            continue
        if high_intensity_drop and tc in {"file_map_refresh", "memory_index_refresh"}:
            skipped.append({"task_class": tc, "reason": f"resource_mode={res_mode}"})
            continue
        final_allowed.append(tc)
    return {"allowed": final_allowed, "skipped": skipped, "blockers": blockers}


# ---------- cycle plan ----------


def build_autonomy_cycle_plan(
    project_dir: Path | str,
    goal: str = "Improve Luna safely overnight",
    policy: dict[str, Any] | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    pol = policy or load_overnight_policy(project_dir)
    ctx = build_runtime_context(project_dir)
    cls = classify_allowed_task_classes(ctx, pol)
    selected: list[dict[str, Any]] = []
    for tc in cls["allowed"]:
        selected.append(
            {
                "task_class": tc,
                "dry_run": bool(dry_run),
                "max_runtime_seconds": int(pol.get("max_per_cycle_runtime_seconds", 600)),
                "approval_required": False,
                "rationale": f"Phase 5K allowlist; tier 1 reports/memory refresh class.",
            }
        )
    skipped_serial = [
        {"task_class": s["task_class"], "reason": s["reason"]}
        for s in cls["skipped"]
    ]
    plan = {
        "schema_version": SCHEMA_VERSION,
        "cycle_id": make_cycle_id(),
        "created_at": now_iso(),
        "goal": goal or "",
        "dry_run": bool(dry_run),
        "allowed_task_classes": cls["allowed"],
        "forbidden_task_classes": list(FORBIDDEN_TASK_CLASSES),
        "selected_tasks": selected,
        "skipped_tasks": skipped_serial,
        "approval_required": [],
        "blockers": list(cls["blockers"]),
        "expected_artifacts": [
            "memory/luna_limited_autonomy_report.json",
            "memory/luna_limited_autonomy_report.md",
            "memory/luna_overnight_brief.md",
            "memory/luna_recommended_next_actions.json",
        ],
        "safety_checks": [
            "operator_stop_files_absent",
            "verifier_clean",
            "tracked_git_clean",
            "resource_mode_not_blocked",
            "lock_acquired",
            "no_code_edits",
            "no_aider",
            "no_installs",
            "no_external_network",
            "no_process_kill",
        ],
        "exit_criteria": [
            "all selected tasks succeed or skip with reason",
            "no source files modified",
            "report written under memory/",
            "lock released",
        ],
        "max_runtime_seconds": int(pol.get("max_per_cycle_runtime_seconds", 600)),
        "one_task_at_a_time": True,
    }
    return plan


# ---------- gate evaluation (proposals only) ----------


def evaluate_task_with_gate(
    project_dir: Path | str,
    task_record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate a hypothetical proposal through the Phase 5F upgrade gate.

    Phase 5K never *applies* anything — it only asks the gate what would
    happen. If the gate module is missing, returns a synthetic record.
    """
    ctx = context or build_runtime_context(project_dir)
    if _upgrade_gate is None:
        return {
            "decision": "needs_approval",
            "reasons": ["upgrade_gate_module_unavailable"],
            "risk_level": str(task_record.get("risk_level", "medium")),
            "approval_tier_required": int(task_record.get("approval_tier_required", 3)),
            "evaluator": "fallback",
            "context_summary": {"verifier_clean": (ctx.get("verifier") or {}).get("clean", False)},
        }
    try:
        decision = _upgrade_gate.evaluate_upgrade_proposal(
            task_record,
            context={"git_status": "" if (ctx.get("git") or {}).get("tracked_dirty_clean") else " M dirty"},
        )
        return {
            "decision": decision.get("decision"),
            "reasons": list(decision.get("reasons") or []),
            "risk_level": decision.get("risk_level"),
            "approval_tier_required": decision.get("approval_tier_required"),
            "evaluator": "luna_upgrade_gate",
            "context_summary": {"verifier_clean": (ctx.get("verifier") or {}).get("clean", False)},
        }
    except Exception as e:
        return {
            "decision": "deny",
            "reasons": [f"upgrade_gate_error:{type(e).__name__}:{str(e)[:160]}"],
            "risk_level": str(task_record.get("risk_level", "high")),
            "approval_tier_required": int(task_record.get("approval_tier_required", 4)),
            "evaluator": "fallback_after_error",
        }


# ---------- foundation task runners (read-only / generated artifacts) ----------


def _safe_call(label: str, fn, *args, **kwargs) -> dict[str, Any]:
    try:
        result = fn(*args, **kwargs)
        return {"ok": True, "label": label, "result": result}
    except Exception as e:
        return {"ok": False, "label": label, "error": f"{type(e).__name__}:{str(e)[:200]}"}


def run_allowed_foundation_task(
    project_dir: Path | str,
    task_class: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    if task_class not in ALLOWED_TASK_CLASSES:
        return {"task_class": task_class, "status": "blocked", "reason": "not_in_phase_5k_allowlist"}
    pdir = Path(project_dir)
    started_at = now_iso()
    out: dict[str, Any] = {
        "task_class": task_class,
        "status": "ok",
        "dry_run": bool(dry_run),
        "started_at": started_at,
        "artifacts": [],
        "details": {},
    }

    if task_class == "read_only_health_check":
        ctx = build_runtime_context(pdir)
        out["details"] = {
            "verifier_clean": (ctx.get("verifier") or {}).get("clean"),
            "git_clean": (ctx.get("git") or {}).get("tracked_dirty_clean"),
            "kill_switch": ctx.get("kill_switch_present"),
            "resource_mode": ctx.get("resource_mode"),
            "scorecard_status": ctx.get("scorecard_status"),
        }

    elif task_class == "file_map_refresh":
        if dry_run or _self_knowledge is None:
            out["details"] = {"would_refresh": True, "module_available": _self_knowledge is not None}
        else:
            fn = getattr(_self_knowledge, "refresh_curated_self_map", None)
            if fn is None:
                out["status"] = "skipped"
                out["details"] = {"reason": "refresh_curated_self_map_not_available"}
            else:
                r = _safe_call("file_map_refresh", fn, str(pdir))
                out["details"] = r
                if r["ok"]:
                    for rel in ("memory/luna_file_map.json", "memory/luna_function_index.json"):
                        if (pdir / rel).is_file():
                            out["artifacts"].append(rel)
                else:
                    out["status"] = "failed"

    elif task_class == "memory_index_refresh":
        if dry_run or _memory_index is None:
            out["details"] = {"would_refresh": True, "module_available": _memory_index is not None}
        else:
            fn = getattr(_memory_index, "build_memory_index", None)
            if fn is None:
                out["status"] = "skipped"
                out["details"] = {"reason": "build_memory_index_not_available"}
            else:
                r = _safe_call("memory_index_refresh", fn, str(pdir))
                out["details"] = r
                if r["ok"]:
                    for rel in (
                        "memory/luna_memory_index.json",
                        "memory/luna_memory_summaries.jsonl",
                        "memory/luna_memory_index_build_report.json",
                    ):
                        if (pdir / rel).is_file():
                            out["artifacts"].append(rel)
                else:
                    out["status"] = "failed"

    elif task_class == "scorecard_refresh":
        if dry_run or _scorecard is None:
            out["details"] = {"would_refresh": True, "module_available": _scorecard is not None}
        else:
            build_fn = getattr(_scorecard, "build_capability_scorecard", None)
            write_fn = getattr(_scorecard, "write_scorecard", None)
            if build_fn is None or write_fn is None:
                out["status"] = "skipped"
                out["details"] = {"reason": "scorecard_helpers_missing"}
            else:
                try:
                    record = build_fn(pdir)
                    json_p = pdir / "memory" / "luna_capability_scorecard.json"
                    md_p = pdir / "memory" / "luna_capability_scorecard.md"
                    rp_p = pdir / "memory" / "luna_capability_scorecard_build_report.json"
                    write_fn(record, json_p, md_p, rp_p, project_root=pdir)
                    out["details"] = {"overall_score": record.get("overall_score"), "readiness_level": record.get("readiness_level")}
                    out["artifacts"] = [
                        "memory/luna_capability_scorecard.json",
                        "memory/luna_capability_scorecard.md",
                        "memory/luna_capability_scorecard_build_report.json",
                    ]
                except Exception as e:
                    out["status"] = "failed"
                    out["details"] = {"error": f"{type(e).__name__}:{str(e)[:200]}"}

    elif task_class == "resource_snapshot":
        if _resource_monitor is None:
            out["status"] = "skipped"
            out["details"] = {"reason": "luna_resource_monitor_unavailable"}
        else:
            snap_fn = getattr(_resource_monitor, "build_resource_snapshot", None)
            if snap_fn is None:
                out["status"] = "skipped"
                out["details"] = {"reason": "build_resource_snapshot_missing"}
            else:
                try:
                    snap = snap_fn(pdir)
                    out["details"] = {
                        "recommended_mode": snap.get("recommended_mode"),
                        "blockers": snap.get("blockers"),
                        "warnings": snap.get("warnings"),
                    }
                    if not dry_run:
                        write_fn = getattr(_resource_monitor, "write_resource_reports", None)
                        if write_fn is not None:
                            try:
                                from luna_modules.luna_resource_monitor import classify_resource_state  # type: ignore
                                decision = classify_resource_state(snap)
                            except Exception:
                                decision = None
                            try:
                                write_fn(pdir, snap, decision=decision, project_root=pdir)
                                for rel in (
                                    "memory/luna_resource_status.json",
                                    "memory/luna_resource_status.md",
                                    "memory/luna_hardware_profile.json",
                                    "memory/luna_resource_monitor_build_report.json",
                                ):
                                    if (pdir / rel).is_file():
                                        out["artifacts"].append(rel)
                                # cleanup: do NOT auto-create hibernation_plan files
                            except Exception as e:
                                out["status"] = "failed"
                                out["details"]["write_error"] = f"{type(e).__name__}:{str(e)[:200]}"
                except Exception as e:
                    out["status"] = "failed"
                    out["details"] = {"error": f"{type(e).__name__}:{str(e)[:200]}"}

    elif task_class == "playbook_match_recent_failures":
        if _playbook_engine is None:
            out["status"] = "skipped"
            out["details"] = {"reason": "luna_playbook_engine_unavailable"}
        else:
            match_fn = getattr(_playbook_engine, "match_playbooks", None)
            if match_fn is None:
                out["status"] = "skipped"
                out["details"] = {"reason": "match_playbooks_missing"}
            else:
                rf = pdir / "logs" / "luna_recent_failures_review.txt"
                signals: list[str] = []
                if rf.is_file():
                    try:
                        signals = [ln.strip() for ln in rf.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip()][:20]
                    except OSError:
                        signals = []
                try:
                    matches = match_fn({"signals": signals, "tags": [], "tokens": []}, project_dir=pdir, limit=5)
                    out["details"] = {"signal_count": len(signals), "match_count": len(matches), "top_matches": [m.get("playbook_id") for m in matches[:5] if isinstance(m, dict)]}
                except Exception as e:
                    out["status"] = "failed"
                    out["details"] = {"error": f"{type(e).__name__}:{str(e)[:200]}"}

    elif task_class == "task_graph_plan_only":
        if _task_graph is None:
            out["status"] = "skipped"
            out["details"] = {"reason": "luna_task_graph_unavailable"}
        else:
            try:
                graph = _task_graph.build_task_graph("Improve Luna safely overnight via reports and refreshed memory", project_dir=pdir)
                out["details"] = {
                    "graph_id": graph.get("graph_id"),
                    "task_count": len(graph.get("tasks") or []),
                    "overall_risk_level": graph.get("overall_risk_level"),
                    "overall_approval_tier_required": graph.get("overall_approval_tier_required"),
                    "intent_drift_status": (graph.get("intent_drift") or {}).get("status"),
                }
            except Exception as e:
                out["status"] = "failed"
                out["details"] = {"error": f"{type(e).__name__}:{str(e)[:200]}"}

    elif task_class == "sandbox_self_test":
        if _sandbox is None:
            out["status"] = "skipped"
            out["details"] = {"reason": "luna_sandbox_unavailable"}
        else:
            st_fn = getattr(_sandbox, "self_test", None)
            if st_fn is None:
                out["status"] = "skipped"
                out["details"] = {"reason": "self_test_missing"}
            else:
                try:
                    buf = io.StringIO()
                    with contextlib.redirect_stdout(buf):
                        rc = st_fn()
                    out["details"] = {
                        "sandbox_self_test_rc": int(rc),
                        "sandbox_stdout_tail": buf.getvalue()[-512:],
                    }
                    if rc != 0:
                        out["status"] = "failed"
                except Exception as e:
                    out["status"] = "failed"
                    out["details"] = {"error": f"{type(e).__name__}:{str(e)[:200]}"}

    elif task_class == "upgrade_gate_evaluate_only":
        low_proposal = {
            "plan_id": "phase5k_low_smoke",
            "title": "phase5k smoke — small comment edit",
            "actor": "luna_limited_autonomy",
            "target_files": ["luna_modules/luna_logging.py"],
            "line_ranges": {"luna_modules/luna_logging.py": [[60, 65]]},
            "action_type": "edit",
            "expected_diff_type": "small_edit",
            "risk_level": "low",
            "approval_tier": 2,
            "diff_stats": {"files_changed": 1, "insertions": 1, "deletions": 1},
            "verification_commands": ["python -m py_compile luna_modules/luna_logging.py"],
            "rollback_plan": "git checkout HEAD -- luna_modules/luna_logging.py",
            "install_commands": [],
            "external_network": False,
            "touches_personality_or_goals": False,
            "touches_memory_content": False,
            "touches_runtime_queue": False,
            "operator_approved": False,
        }
        high_proposal = dict(low_proposal)
        high_proposal["plan_id"] = "phase5k_high_smoke"
        high_proposal["title"] = "phase5k smoke — worker.py edit"
        high_proposal["target_files"] = ["worker.py"]
        high_proposal["line_ranges"] = {"worker.py": [[12200, 12210]]}
        high_proposal["risk_level"] = "high"
        high_proposal["approval_tier"] = 4
        low = evaluate_task_with_gate(pdir, low_proposal)
        high = evaluate_task_with_gate(pdir, high_proposal)
        out["details"] = {
            "low_decision": low.get("decision"),
            "low_tier": low.get("approval_tier_required"),
            "high_decision": high.get("decision"),
            "high_tier": high.get("approval_tier_required"),
            "evaluator": low.get("evaluator"),
        }

    elif task_class == "daily_brief_report":
        # Brief is built later from the cycle records; this task is a placeholder.
        out["details"] = {"would_render_brief": True}

    elif task_class == "recommended_next_actions_report":
        out["details"] = {"would_render_recommended_next_actions": True}

    out["finished_at"] = now_iso()
    return out


# ---------- cycle runner ----------


def _atomic_write(path: Path, data: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if isinstance(data, str):
        tmp.write_text(data, encoding="utf-8")
    else:
        tmp.write_bytes(data)
    os.replace(tmp, path)


def _hash_source_files(project_dir: Path, files: Iterable[str]) -> dict[str, str]:
    import hashlib
    out: dict[str, str] = {}
    for rel in files:
        p = (project_dir / rel)
        if not p.is_file():
            out[rel] = ""
            continue
        h = hashlib.sha256()
        try:
            with p.open("rb") as fh:
                for chunk in iter(lambda: fh.read(65536), b""):
                    h.update(chunk)
            out[rel] = h.hexdigest()
        except OSError:
            out[rel] = ""
    return out


_SOURCE_FILES_TO_PROTECT = (
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
)


def run_limited_autonomy_cycle(
    project_dir: Path | str,
    goal: str = "Improve Luna safely overnight",
    dry_run: bool = True,
    policy: dict[str, Any] | None = None,
    write_report: bool = True,
) -> dict[str, Any]:
    pdir = Path(project_dir)
    pol = policy or load_overnight_policy(pdir)

    started_at = now_iso()
    cycle_id = make_cycle_id()

    operator_stop = check_operator_stop(pdir, pol)
    if operator_stop["stopped"]:
        report = _build_report(
            cycle_id=cycle_id,
            started_at=started_at,
            finished_at=now_iso(),
            goal=goal,
            dry_run=dry_run,
            context={"operator_stop": operator_stop},
            attempted=[], succeeded=[], failed=[], skipped=[],
            approvals_required=[],
            artifacts=[],
            blockers=[f"operator_stop: {operator_stop['stop_files_present']}"],
            recommended_next_actions=[
                {"action": "remove_operator_stop_file", "rationale": "operator_stop file blocks the cycle"}
            ],
            safe_to_continue=False,
            safe_to_run_overnight_readonly=False,
            notes=["Cycle halted by operator stop file."],
        )
        if write_report:
            _persist_cycle_artifacts(pdir, report)
        return report

    lock_res = acquire_autonomy_lock(pdir, stale_seconds=int(pol.get("lock_stale_seconds", 3600)))
    if not lock_res.get("acquired"):
        report = _build_report(
            cycle_id=cycle_id,
            started_at=started_at,
            finished_at=now_iso(),
            goal=goal,
            dry_run=dry_run,
            context={"lock": lock_res},
            attempted=[], succeeded=[], failed=[], skipped=[],
            approvals_required=[],
            artifacts=[],
            blockers=[f"lock_held: {lock_res}"],
            recommended_next_actions=[],
            safe_to_continue=False,
            safe_to_run_overnight_readonly=False,
            notes=["Cycle halted because a fresh autonomy lock is held."],
        )
        if write_report:
            _persist_cycle_artifacts(pdir, report)
        return report

    try:
        plan = build_autonomy_cycle_plan(pdir, goal=goal, policy=pol, dry_run=dry_run)
        ctx = build_runtime_context(pdir)

        before_hashes = _hash_source_files(pdir, _SOURCE_FILES_TO_PROTECT)

        attempted: list[dict[str, Any]] = []
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = list(plan.get("skipped_tasks") or [])
        artifacts: list[str] = []

        per_task_timeout = int(pol.get("max_per_cycle_runtime_seconds", 600))
        cycle_deadline = time.time() + per_task_timeout

        for task in plan.get("selected_tasks") or []:
            if time.time() > cycle_deadline:
                skipped.append({"task_class": task["task_class"], "reason": "cycle_deadline_exceeded"})
                continue
            tc = task["task_class"]
            tr = run_allowed_foundation_task(pdir, tc, dry_run=dry_run)
            attempted.append(tr)
            if tr.get("status") == "ok":
                succeeded.append(tr)
                for art in tr.get("artifacts") or []:
                    if art not in artifacts:
                        artifacts.append(art)
            elif tr.get("status") == "skipped":
                skipped.append({"task_class": tc, "reason": tr.get("details", {}).get("reason", "skipped")})
            else:
                failed.append(tr)

        after_hashes = _hash_source_files(pdir, _SOURCE_FILES_TO_PROTECT)
        source_diff: list[str] = [
            rel for rel in _SOURCE_FILES_TO_PROTECT
            if before_hashes.get(rel, "") != after_hashes.get(rel, "")
        ]
        blockers = list(plan.get("blockers") or [])
        if source_diff:
            blockers.append(f"source_files_modified: {source_diff}")

        recommended = _recommend_next_actions(ctx, attempted, blockers)

        safe_overnight_readonly = (
            not blockers
            and not source_diff
            and not operator_stop["stopped"]
            and (ctx.get("verifier") or {}).get("clean", False)
        )

        report = _build_report(
            cycle_id=cycle_id,
            started_at=started_at,
            finished_at=now_iso(),
            goal=goal,
            dry_run=dry_run,
            context={
                "git_clean": (ctx.get("git") or {}).get("tracked_dirty_clean"),
                "verifier_clean": (ctx.get("verifier") or {}).get("clean"),
                "kill_switch_present": ctx.get("kill_switch_present"),
                "resource_mode": ctx.get("resource_mode"),
                "scorecard_status": ctx.get("scorecard_status"),
                "scorecard_readiness_level": ctx.get("scorecard_readiness_level"),
                "aider_active": ctx.get("aider_active"),
                "aider_quarantine": ctx.get("aider_quarantine"),
                "lock": lock_res,
                "plan_id": plan.get("cycle_id"),
                "selected_count": len(plan.get("selected_tasks") or []),
                "skipped_initial_count": len(plan.get("skipped_tasks") or []),
                "source_files_modified": source_diff,
            },
            attempted=attempted,
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            approvals_required=[],
            artifacts=artifacts,
            blockers=blockers,
            recommended_next_actions=recommended,
            safe_to_continue=(not blockers),
            safe_to_run_overnight_readonly=safe_overnight_readonly,
            notes=[
                "Phase 5K + 5K2 limited routine autonomy cycle (read-only/generated-artifacts).",
                "Same safety rules apply day or night — 'routine' supersedes 'overnight'.",
                "safe_to_run_routine_code_edits (alias safe_to_run_overnight_code_edits) is hard-coded false until Phase 5L (Delegated AI Approval Council) is built.",
            ],
        )

        if write_report:
            _persist_cycle_artifacts(pdir, report)
        return report
    finally:
        release_autonomy_lock(pdir, lock_id=lock_res.get("lock_id"))


def run_limited_autonomy_loop(
    project_dir: Path | str,
    goal: str = "Improve Luna safely overnight",
    max_cycles: int = 1,
    sleep_seconds: int = 60,
    dry_run: bool = True,
    policy: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    pdir = Path(project_dir)
    pol = policy or load_overnight_policy(pdir)
    reports: list[dict[str, Any]] = []
    max_c = max(1, min(int(max_cycles), int(pol.get("max_cycles", 12))))
    sleep_s = max(0, min(int(sleep_seconds), int(pol.get("sleep_seconds", 300))))
    for i in range(max_c):
        op = check_operator_stop(pdir, pol)
        if op["stopped"]:
            reports.append({"halted": True, "reason": "operator_stop", "details": op, "cycle_index": i})
            break
        rep = run_limited_autonomy_cycle(pdir, goal=goal, dry_run=dry_run, policy=pol)
        reports.append(rep)
        if i < max_c - 1:
            t_end = time.time() + sleep_s
            while time.time() < t_end:
                if check_operator_stop(pdir, pol)["stopped"]:
                    break
                time.sleep(min(1.0, max(0.0, t_end - time.time())))
    return reports


def _build_report(
    *,
    cycle_id: str,
    started_at: str,
    finished_at: str,
    goal: str,
    dry_run: bool,
    context: dict[str, Any],
    attempted: list[dict[str, Any]],
    succeeded: list[dict[str, Any]],
    failed: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    approvals_required: list[dict[str, Any]],
    artifacts: list[str],
    blockers: list[str],
    recommended_next_actions: list[dict[str, Any]],
    safe_to_continue: bool,
    safe_to_run_overnight_readonly: bool,
    notes: list[str],
) -> dict[str, Any]:
    safe_readonly = bool(safe_to_run_overnight_readonly)
    return {
        "schema_version": SCHEMA_VERSION,
        "cycle_id": cycle_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "goal": goal,
        "dry_run": bool(dry_run),
        "context_summary": context,
        "tasks_attempted": attempted,
        "tasks_succeeded": succeeded,
        "tasks_failed": failed,
        "tasks_skipped": skipped,
        "approvals_required": approvals_required,
        "artifacts_written": artifacts,
        "blockers": blockers,
        "recommended_next_actions": recommended_next_actions,
        "safe_to_continue": bool(safe_to_continue),
        # Phase 5K2 canonical names.
        "safe_to_run_routine_readonly": safe_readonly,
        "safe_to_run_routine_code_edits": False,
        # Phase 5K legacy aliases — kept identical to routine fields.
        "safe_to_run_overnight_readonly": safe_readonly,
        "safe_to_run_overnight_code_edits": False,
        "notes": list(notes),
    }


def _recommend_next_actions(
    ctx: dict[str, Any],
    attempted: list[dict[str, Any]],
    blockers: list[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if ctx.get("kill_switch_present"):
        out.append({"action": "remove_LUNA_STOP_NOW.flag_when_safe", "approval_required": True, "rationale": "Kill switch is currently active."})
    if not (ctx.get("verifier") or {}).get("clean", True):
        out.append({"action": "run_Luna_Post_Repair_Verify.ps1", "approval_required": False, "rationale": "Verifier report is missing or not clean."})
    if not (ctx.get("git") or {}).get("tracked_dirty_clean", True):
        out.append({"action": "stage_or_revert_dirty_tracked_files", "approval_required": False, "rationale": "Tracked tree is dirty; Phase 5K refuses code-edit work."})
    if (ctx.get("aider_quarantine") or 0) > 5:
        out.append({"action": "review_aider_quarantine_top_5", "approval_required": False, "rationale": "Quarantine has accumulated."})
    if not attempted:
        out.append({"action": "rerun_cycle_after_blocker_resolution", "approval_required": False, "rationale": "No tasks ran this cycle."})
    out.append({
        "action": "wait_for_phase_5L_council_before_any_code_edits",
        "approval_required": True,
        "rationale": "Phase 5K2 limited routine autonomy does not delegate edits; Phase 5L Delegated AI Approval Council is the next planned phase.",
    })
    return out[:8]


def _persist_cycle_artifacts(project_dir: Path, report: dict[str, Any]) -> None:
    mem = project_dir / "memory"
    mem.mkdir(parents=True, exist_ok=True)
    json_p = mem / "luna_limited_autonomy_report.json"
    md_p = mem / "luna_limited_autonomy_report.md"
    state_p = mem / "luna_limited_autonomy_state.json"
    cycle_p = mem / "luna_limited_autonomy_cycle.jsonl"
    brief_p = mem / "luna_overnight_brief.md"
    actions_p = mem / "luna_recommended_next_actions.json"

    project_root = project_dir.resolve()
    for p in (json_p, md_p, state_p, cycle_p, brief_p, actions_p):
        _ensure_under(p, project_root)

    write_json_atomic(json_p, report)
    _atomic_write(md_p, render_cycle_report_markdown(report))
    write_json_atomic(state_p, {
        "schema_version": SCHEMA_VERSION,
        "last_cycle_id": report["cycle_id"],
        "last_finished_at": report["finished_at"],
        "last_safe_to_continue": report["safe_to_continue"],
        # Phase 5K2 canonical
        "last_safe_to_run_routine_readonly": report.get("safe_to_run_routine_readonly", report["safe_to_run_overnight_readonly"]),
        "last_safe_to_run_routine_code_edits": False,
        # Phase 5K legacy aliases
        "last_safe_to_run_overnight_readonly": report["safe_to_run_overnight_readonly"],
        "last_safe_to_run_overnight_code_edits": False,
        "last_blockers": report["blockers"],
    })
    append_jsonl(cycle_p, {
        "ts": report["finished_at"],
        "cycle_id": report["cycle_id"],
        "goal": report["goal"],
        "dry_run": report["dry_run"],
        "succeeded": [t.get("task_class") for t in report["tasks_succeeded"]],
        "failed": [t.get("task_class") for t in report["tasks_failed"]],
        "skipped": [t.get("task_class") if isinstance(t, dict) else t for t in report["tasks_skipped"]],
        "blockers": report["blockers"],
    })
    _atomic_write(brief_p, build_overnight_brief(project_dir, [report]))
    write_json_atomic(actions_p, {
        "schema_version": SCHEMA_VERSION,
        "generated_at": report["finished_at"],
        "cycle_id": report["cycle_id"],
        "actions": report["recommended_next_actions"],
        # Phase 5K2 canonical
        "safe_to_run_routine_readonly": report.get("safe_to_run_routine_readonly", report["safe_to_run_overnight_readonly"]),
        "safe_to_run_routine_code_edits": False,
        # Phase 5K legacy aliases
        "safe_to_run_overnight_readonly": report["safe_to_run_overnight_readonly"],
        "safe_to_run_overnight_code_edits": False,
    })


# ---------- rendering ----------


def render_cycle_report_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Luna Limited Routine Autonomy — Cycle Report")
    lines.append("")
    lines.append(f"- **cycle_id**: `{report.get('cycle_id', '?')}`")
    lines.append(f"- **goal**: {report.get('goal', '')!r}")
    lines.append(f"- **started_at**: {report.get('started_at', '?')}")
    lines.append(f"- **finished_at**: {report.get('finished_at', '?')}")
    lines.append(f"- **dry_run**: `{report.get('dry_run')}`")
    lines.append(f"- **safe_to_continue**: `{report.get('safe_to_continue')}`")
    safe_ro = report.get("safe_to_run_routine_readonly", report.get("safe_to_run_overnight_readonly"))
    safe_ce = report.get("safe_to_run_routine_code_edits", report.get("safe_to_run_overnight_code_edits"))
    lines.append(f"- **safe_to_run_routine_readonly**: `{safe_ro}` (alias: `safe_to_run_overnight_readonly={report.get('safe_to_run_overnight_readonly')}`)")
    lines.append(f"- **safe_to_run_routine_code_edits**: `{safe_ce}` *(Phase 5K2 hard rule — always false; alias: `safe_to_run_overnight_code_edits={report.get('safe_to_run_overnight_code_edits')}`)*")
    lines.append("")
    lines.append("## Context")
    for k, v in (report.get("context_summary") or {}).items():
        lines.append(f"- {k}: `{v}`")
    lines.append("")
    lines.append("## Tasks")
    lines.append("| # | task_class | status | dry_run |")
    lines.append("|--:|------------|--------|---------|")
    for i, t in enumerate(report.get("tasks_attempted") or [], start=1):
        lines.append(f"| {i} | {t.get('task_class')} | {t.get('status')} | {t.get('dry_run')} |")
    skipped = report.get("tasks_skipped") or []
    if skipped:
        lines.append("")
        lines.append("## Skipped")
        for s in skipped:
            if isinstance(s, dict):
                lines.append(f"- {s.get('task_class')}: {s.get('reason')}")
            else:
                lines.append(f"- {s}")
    blockers = report.get("blockers") or []
    if blockers:
        lines.append("")
        lines.append("## Blockers")
        for b in blockers:
            lines.append(f"- {b}")
    rec = report.get("recommended_next_actions") or []
    if rec:
        lines.append("")
        lines.append("## Recommended next actions")
        for r in rec:
            req = "approval_required" if r.get("approval_required") else "no_approval_needed"
            lines.append(f"- **{r.get('action')}** ({req}) — {r.get('rationale')}")
    notes = report.get("notes") or []
    if notes:
        lines.append("")
        lines.append("## Notes")
        for n in notes:
            lines.append(f"- {n}")
    return "\n".join(lines) + "\n"


def build_routine_brief(project_dir: Path | str, cycle_records: list[dict[str, Any]]) -> str:
    """Phase 5K2: build the routine brief (replaces 'overnight brief' wording)."""
    pdir = Path(project_dir)
    lines: list[str] = []
    lines.append("# Luna Limited Routine Autonomy — Brief")
    lines.append("")
    lines.append(f"- **project_dir**: `{str(pdir).replace(chr(92), '/')}`")
    lines.append(f"- **generated_at**: {now_iso()}")
    lines.append(f"- **cycle_count**: {len(cycle_records)}")
    lines.append(f"- **safe_to_run_routine_code_edits**: `False` (Phase 5K2 hard rule)")
    lines.append(f"- **safe_to_run_overnight_code_edits**: `False` (legacy alias)")
    lines.append("")
    for i, rep in enumerate(cycle_records, start=1):
        if not isinstance(rep, dict):
            continue
        lines.append(f"## Cycle {i} — `{rep.get('cycle_id', '?')}`")
        lines.append(f"- finished_at: {rep.get('finished_at', '?')}")
        lines.append(f"- safe_to_continue: `{rep.get('safe_to_continue')}`")
        safe_ro = rep.get("safe_to_run_routine_readonly", rep.get("safe_to_run_overnight_readonly"))
        lines.append(f"- safe_to_run_routine_readonly: `{safe_ro}`")
        succ = [t.get("task_class") for t in rep.get("tasks_succeeded") or []]
        failed = [t.get("task_class") for t in rep.get("tasks_failed") or []]
        skipped = [s.get("task_class") if isinstance(s, dict) else s for s in rep.get("tasks_skipped") or []]
        lines.append(f"- succeeded: {succ}")
        lines.append(f"- failed: {failed}")
        lines.append(f"- skipped: {skipped}")
        if rep.get("blockers"):
            lines.append(f"- blockers: {rep['blockers']}")
        lines.append("")
    return "\n".join(lines) + "\n"


# Legacy alias retained — Phase 5K callers and tests still work.
def build_overnight_brief(project_dir: Path | str, cycle_records: list[dict[str, Any]]) -> str:
    """Backwards-compatible alias of `build_routine_brief` (Phase 5K2)."""
    return build_routine_brief(project_dir, cycle_records)


def write_cycle_report(project_dir: Path | str, report: dict[str, Any]) -> dict[str, Any]:
    pdir = Path(project_dir)
    _persist_cycle_artifacts(pdir, report)
    return {
        "json": str(pdir / "memory" / "luna_limited_autonomy_report.json"),
        "md": str(pdir / "memory" / "luna_limited_autonomy_report.md"),
        "state": str(pdir / "memory" / "luna_limited_autonomy_state.json"),
        "cycle_jsonl": str(pdir / "memory" / "luna_limited_autonomy_cycle.jsonl"),
        "brief": str(pdir / "memory" / "luna_overnight_brief.md"),
        "actions": str(pdir / "memory" / "luna_recommended_next_actions.json"),
    }


# ---------- self-test ----------


def self_test() -> int:
    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)
        (td / "logs").mkdir(parents=True, exist_ok=True)
        # Synthetic clean verifier log so the cycle is allowed to run.
        (td / "logs" / "luna_post_repair_verify_20260101_000000.txt").write_text(
            "[PASS] No hard failures found.\n[PASS] No warnings found.\n",
            encoding="utf-8",
        )
        # Build a cycle (dry_run) and persist.
        report = run_limited_autonomy_cycle(td, goal="self-test", dry_run=True, write_report=True)
        ok = (
            isinstance(report, dict)
            and report.get("safe_to_run_routine_code_edits") is False
            and report.get("safe_to_run_overnight_code_edits") is False
            and report.get("schema_version") == SCHEMA_VERSION
        )
        out = {
            "ok": bool(ok),
            "cycle_id": report.get("cycle_id"),
            "selected_count": len(report.get("tasks_attempted") or []),
            "succeeded_count": len(report.get("tasks_succeeded") or []),
            "blockers": report.get("blockers"),
            "safe_to_run_routine_readonly": report.get("safe_to_run_routine_readonly"),
            "safe_to_run_routine_code_edits": report.get("safe_to_run_routine_code_edits"),
            "safe_to_run_overnight_readonly": report.get("safe_to_run_overnight_readonly"),
            "safe_to_run_overnight_code_edits": report.get("safe_to_run_overnight_code_edits"),
        }
        print(json.dumps(out, indent=2))
        return 0 if ok else 1


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Limited Routine Autonomy controller (Phase 5K + 5K2)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--plan", default=None, help="Build a cycle plan for the given goal and print it.")
    parser.add_argument("--run-once", action="store_true", help="Phase 5K2 alias of --routine-once.")
    parser.add_argument("--routine-once", action="store_true", help="Run one limited routine autonomy cycle.")
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--execute-generated-artifacts", action="store_true", default=False, help="Allow non-dry-run regeneration of gitignored memory artifacts only. Source files remain untouched.")
    parser.add_argument("--routine-dry-run", action="store_true", help="Run a routine dry-run loop (Phase 5K2 canonical).")
    parser.add_argument("--routine-loop", action="store_true", help="Run a routine loop in dry-run mode by default.")
    parser.add_argument("--overnight-dry-run", action="store_true", help="Backwards-compatible alias of --routine-dry-run (Phase 5K).")
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=int, default=60)
    parser.add_argument("--print-report", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    parser.add_argument("--goal", default="Improve Luna safely (limited routine autonomy)")
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    pdir = Path(args.project_dir)
    pol = load_routine_policy(pdir)

    if args.plan is not None:
        plan = build_autonomy_cycle_plan(pdir, goal=args.plan or args.goal, policy=pol, dry_run=True)
        print(json.dumps(plan, indent=2))
        return 0

    if args.print_report:
        rep = load_json(pdir / "memory" / "luna_limited_autonomy_report.json", default=None)
        if not isinstance(rep, dict):
            print(json.dumps({"ok": False, "error": "no_report_present"}, indent=2))
            return 1
        sys.stdout.write(render_cycle_report_markdown(rep))
        return 0

    if args.run_once or args.routine_once:
        dry_run = bool(args.dry_run) or not bool(args.execute_generated_artifacts)
        report = run_limited_autonomy_cycle(pdir, goal=args.goal, dry_run=dry_run, policy=pol)
        out = {
            "cycle_id": report["cycle_id"],
            "dry_run": report["dry_run"],
            "safe_to_continue": report["safe_to_continue"],
            "safe_to_run_routine_readonly": report.get("safe_to_run_routine_readonly", report["safe_to_run_overnight_readonly"]),
            "safe_to_run_routine_code_edits": False,
            "safe_to_run_overnight_readonly": report["safe_to_run_overnight_readonly"],
            "safe_to_run_overnight_code_edits": report["safe_to_run_overnight_code_edits"],
            "succeeded": [t.get("task_class") for t in report.get("tasks_succeeded") or []],
            "failed": [t.get("task_class") for t in report.get("tasks_failed") or []],
            "blockers": report["blockers"],
            "artifacts_written": report["artifacts_written"],
        }
        print(json.dumps(out, indent=2))
        return 0

    if args.routine_dry_run or args.routine_loop or args.overnight_dry_run:
        loop_alias = (
            "routine_dry_run" if args.routine_dry_run else
            "routine_loop" if args.routine_loop else
            "overnight_dry_run"
        )
        reports = run_limited_autonomy_loop(
            pdir,
            goal=args.goal,
            max_cycles=args.max_cycles,
            sleep_seconds=args.sleep_seconds,
            dry_run=True,
            policy=pol,
        )
        out = {
            "cli_alias": loop_alias,
            "cycle_count": len(reports),
            "summaries": [
                {
                    "cycle_id": r.get("cycle_id"),
                    "safe_to_continue": r.get("safe_to_continue"),
                    "safe_to_run_routine_readonly": r.get("safe_to_run_routine_readonly", r.get("safe_to_run_overnight_readonly")),
                    "safe_to_run_routine_code_edits": False,
                    "safe_to_run_overnight_readonly": r.get("safe_to_run_overnight_readonly"),
                    "safe_to_run_overnight_code_edits": r.get("safe_to_run_overnight_code_edits", False),
                    "succeeded": [t.get("task_class") for t in (r.get("tasks_succeeded") or [])],
                    "blockers": r.get("blockers"),
                }
                for r in reports
                if isinstance(r, dict)
            ],
        }
        print(json.dumps(out, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
