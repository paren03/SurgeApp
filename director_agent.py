"""Staged Director Agent v1 for Luna Autonomy Control.

The Director converts CEO goals into small, reviewable missions. This staged
module is intentionally self-contained so it can be reviewed before any live
core file is changed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


DIRECTOR_STATES = ("active", "done", "failed", "quarantine")


@dataclass(frozen=True)
class DirectorPaths:
    project_dir: Path

    @property
    def jobs_dir(self) -> Path:
        """Return the path to the director jobs directory."""
        return self.project_dir / "director_jobs"

    @property
    def logs_dir(self) -> Path:
        return self.project_dir / "logs"

    @property
    def live_feed(self) -> Path:
        return self.logs_dir / "luna_live_feed.jsonl"

    @property
    def kill_switch(self) -> Path:
        return self.project_dir / "LUNA_STOP_NOW.flag"


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _slug(text: str) -> str:
    clean = "".join(char.lower() if char.isalnum() else "_" for char in text)
    parts = [part for part in clean.split("_") if part]
    return "_".join(parts[:8]) or "ceo_goal"


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def parse_ceo_command(text: str) -> Dict[str, Any]:
    """Accept `/ceo <goal>` commands and reject everything else."""
    raw = str(text or "").strip()
    if not raw.lower().startswith("/ceo "):
        return {"accepted": False, "reason": "not_ceo_command", "raw": raw}
    goal = raw[5:].strip()
    if not goal:
        return {"accepted": False, "reason": "empty_goal", "raw": raw}
    return {"accepted": True, "goal": goal, "raw": raw}


def ensure_director_folders(project_dir: str | Path) -> Dict[str, str]:
    """Create Director queue folders without deleting or overwriting queues."""
    paths = DirectorPaths(Path(project_dir))
    folders: Dict[str, str] = {}
    for state in DIRECTOR_STATES:
        folder = paths.jobs_dir / state
        folder.mkdir(parents=True, exist_ok=True)
        folders[state] = str(folder)
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    return folders


def build_director_missions(goal: str) -> List[Dict[str, Any]]:
    """Convert one CEO goal into small, bounded engineering missions."""
    goal_text = str(goal or "").strip() or "Unspecified CEO goal"
    autonomy_focus = "autonomy" in goal_text.lower() or "luna" in goal_text.lower()
    missions = [
        {
            "id": "aider_bridge_safety",
            "purpose": "Harden Aider Bridge so unsafe local-model jobs are refused before they create failure floods.",
            "target_files": ["aider_bridge.py", "luna_modules/luna_aider_result_policy.py"],
            "risk_level": "medium",
            "acceptance_test": "Aider Bridge checks Ollama, sets OLLAMA_API_BASE, writes per-job logs/diffs, and quarantines unsafe jobs.",
            "rollback_stage_plan": "Stage only; py_compile aider_bridge.py and run Aider safety tests before apply.",
            "expected_diff_type": "safety_guard",
            "max_lines_changed": 90,
            "function_scope_required": False,
        },
        {
            "id": "director_job_quality",
            "purpose": "Improve Director mission quality so CEO goals become bounded, high-impact tasks instead of random cleanup.",
            "target_files": ["director_agent.py"],
            "risk_level": "medium",
            "acceptance_test": "Each mission has required metadata, avoids low-value repeated work, and stays within max_lines_changed.",
            "rollback_stage_plan": "Stage only; py_compile director_agent.py and run Director tests before apply.",
            "expected_diff_type": "orchestration",
            "max_lines_changed": 90,
            "function_scope_required": False,
        },
        {
            "id": "guardian_duplicate_prevention",
            "purpose": "Strengthen Guardian process checks so worker, bridge, and guardian do not multiply in the background.",
            "target_files": ["luna_guardian.py"],
            "risk_level": "medium",
            "acceptance_test": "Duplicate start attempts write live-feed/log events and do not spawn extra service copies.",
            "rollback_stage_plan": "Stage only; py_compile luna_guardian.py and run guardian tests before apply.",
            "expected_diff_type": "process_guard",
            "max_lines_changed": 80,
            "function_scope_required": False,
        },
        {
            "id": "continues_update_cycle_telemetry",
            "purpose": "Expose continues_update cycle telemetry without letting local Aider edit the whole worker file blindly.",
            "target_files": ["worker.py"],
            "risk_level": "high",
            "acceptance_test": "Cycle state records jobs_created, failed_count, noop_count, done_count, and pause_reason.",
            "rollback_stage_plan": "Function-scoped stage only; py_compile worker.py and import worker before apply.",
            "expected_diff_type": "telemetry",
            "max_lines_changed": 80,
            "function_scope_required": True,
            "function_scope": "continues_update_loop",
        },
        {
            "id": "inspector_events",
            "purpose": "Make Inspector show structured plans, jobs, diffs, verification, failures, and summaries.",
            "target_files": ["SurgeApp_Claude_Terminal.py", "luna_modules/luna_inspector_autonomy_feed.py"],
            "risk_level": "medium",
            "acceptance_test": "Inspector reads live-feed autonomy events and shows current job/defer/quarantine status.",
            "rollback_stage_plan": "Stage only; py_compile UI modules before apply.",
            "expected_diff_type": "inspector_ui",
            "max_lines_changed": 120,
            "function_scope_required": False,
        },
        {
            "id": "nightly_summary_learning",
            "purpose": "Improve append-only nightly summaries so Luna learns from failures, no-diffs, and quarantines.",
            "target_files": ["memory/nightly_updates.md", "memory/nightly_updates.jsonl"],
            "risk_level": "low",
            "acceptance_test": "Summary includes attempted, changed, failed, no-diff, quarantined, learned, risky files, and next steps.",
            "rollback_stage_plan": "Append-only files; quarantine bad entries rather than deleting.",
            "expected_diff_type": "memory_summary",
            "max_lines_changed": 60,
            "function_scope_required": False,
        },
        {
            "id": "startup_environment",
            "purpose": "Keep one-click startup pinned to real local interpreters and UTF-8 safe background execution.",
            "target_files": ["LaunchLuna.pyw", "tools/ensure_luna_command_center_shortcut.ps1"],
            "risk_level": "medium",
            "acceptance_test": "Desktop launcher points to LaunchLuna.pyw, uses real Python, and records startup status.",
            "rollback_stage_plan": "Stage only; py_compile launcher and verify shortcut metadata before apply.",
            "expected_diff_type": "startup",
            "max_lines_changed": 90,
            "function_scope_required": False,
        },
        {
            "id": "self_test_harness",
            "purpose": "Add focused regression tests for Aider Bridge safety, Director quality, and Guardian duplicate prevention.",
            "target_files": ["tests/test_aider_bridge_safety.py", "tests/test_director_agent.py", "tests/test_luna_guardian.py"],
            "risk_level": "medium",
            "acceptance_test": "Tests fail before the guard and pass after the staged rescue patch.",
            "rollback_stage_plan": "Stage tests only; no runtime queue edits.",
            "expected_diff_type": "tests",
            "max_lines_changed": 100,
            "function_scope_required": False,
        },
    ]
    if not autonomy_focus:
        missions[0]["purpose"] = f"Create a safe Aider Bridge quality gate before starting CEO goal: {goal_text}"
    return missions


def emit_director_event(project_dir: str | Path, event_name: str, details: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Append a Director event for Inspector/live progress surfaces."""
    paths = DirectorPaths(Path(project_dir))
    event = {
        "ts": _now_iso(),
        "role": "director",
        "event": str(event_name),
        "details": details or {},
    }
    _append_jsonl(paths.live_feed, event)
    return event


def _enrich_missions_with_targets(project_dir: Path, goal: str, missions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Use find_targets to suggest relevant files for each mission."""
    try:
        from luna_modules.luna_self_knowledge import find_targets
        hits = find_targets(goal, project_dir, limit=6)
        suggested = [h.get("file", "") for h in hits if h.get("file")]
        if suggested:
            for mission in missions:
                existing = mission.get("target_files") or []
                merged = list(dict.fromkeys(existing + suggested[:3]))
                mission["suggested_targets_from_selfmap"] = suggested[:6]
                if not existing:
                    mission["target_files"] = merged[:3]
    except Exception:
        pass
    return missions


def write_director_job(project_dir: str | Path, command_text: str) -> Dict[str, Any]:
    """Write one `/ceo` job into director_jobs/active with bounded missions."""
    paths = DirectorPaths(Path(project_dir))
    folders = ensure_director_folders(paths.project_dir)
    parsed = parse_ceo_command(command_text)
    if not parsed.get("accepted"):
        payload = {
            "ts": _now_iso(),
            "role": "director",
            "state": "failed",
            "failure_reason": parsed["reason"],
            "raw": parsed.get("raw", ""),
        }
        failed_path = Path(folders["failed"]) / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_rejected.json"
        failed_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        emit_director_event(paths.project_dir, "DIRECTOR_JOB_REJECTED", payload)
        return {**payload, "path": str(failed_path)}

    goal = parsed["goal"]
    state = "active"
    failure_reason = ""
    if paths.kill_switch.exists():
        state = "failed"
        failure_reason = "kill_switch_present"
    payload = {
        "ts": _now_iso(),
        "role": "director",
        "state": state,
        "goal": goal,
        "source_command": parsed["raw"],
        "missions": _enrich_missions_with_targets(paths.project_dir, goal, build_director_missions(goal)),
        "failure_reason": failure_reason,
        "policy": {
            "stage_only": True,
            "delete": "never",
            "quarantine_bad_items": True,
        },
    }
    folder = Path(folders[state])
    job_path = folder / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_slug(goal)}.json"
    job_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    emit_director_event(paths.project_dir, "DIRECTOR_PLAN_CREATED", {"goal": goal, "state": state, "path": str(job_path)})
    return {**payload, "path": str(job_path)}


def write_director_refresh_job(project_dir: str | Path, quarantine_path: str | Path) -> Dict[str, Any]:
    """Promote a quarantined stale-plan record into an active refresh plan."""
    paths = DirectorPaths(Path(project_dir))
    folders = ensure_director_folders(paths.project_dir)
    source_path = Path(quarantine_path)
    try:
        source = json.loads(source_path.read_text(encoding="utf-8", errors="replace") or "{}")
    except Exception as exc:
        payload = {
            "ts": _now_iso(),
            "role": "director",
            "state": "failed",
            "failure_reason": f"refresh_source_unreadable:{exc}",
            "source_quarantine": str(source_path),
        }
        failed_path = Path(folders["failed"]) / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_refresh_failed.json"
        failed_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        emit_director_event(paths.project_dir, "DIRECTOR_REFRESH_FAILED", payload)
        return {**payload, "path": str(failed_path)}

    missions = list(source.get("missions") or [])
    refreshed: List[Dict[str, Any]] = []
    for index, mission in enumerate(missions[:12], start=1):
        item = dict(mission)
        item["id"] = f"director_refresh_{index:02d}_{item.get('id', 'mission')}"
        item["purpose"] = (
            f"{item.get('purpose', 'Refresh a stale mission')} "
            "Produce a concrete staged code/test diff or explicit compliance evidence; do not repeat the prior prompt family."
        )
        item["acceptance_test"] = item.get("acceptance_test") or "Fresh mission creates a real diff or clear already-compliant evidence."
        item["rollback_stage_plan"] = item.get("rollback_stage_plan") or "Stage only; rollback by not applying the staged patch."
        item["expected_diff_type"] = item.get("expected_diff_type") or "fresh bounded upgrade"
        item["max_lines_changed"] = min(int(item.get("max_lines_changed") or 180), 220)
        targets = [str(target) for target in item.get("target_files", [])]
        item["function_scope_required"] = bool(item.get("function_scope_required") or "worker.py" in targets)
        refreshed.append(item)

    payload = {
        "ts": _now_iso(),
        "role": "director",
        "state": "active",
        "goal": "Refresh stale continues_update plan with executable missions",
        "source_quarantine": str(source_path),
        "missions": refreshed,
        "failure_reason": "",
        "policy": {
            "stage_only": True,
            "delete": "never",
            "quarantine_bad_items": True,
            "avoid_prompt_families": [m.get("avoid_prompt_family", "") for m in refreshed if m.get("avoid_prompt_family")],
        },
    }
    active_path = Path(folders["active"]) / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_refresh_continues_update.json"
    active_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    emit_director_event(
        paths.project_dir,
        "DIRECTOR_REFRESH_PLAN_CREATED",
        {"source_quarantine": str(source_path), "path": str(active_path), "missions": len(refreshed)},
    )
    return {**payload, "path": str(active_path)}
