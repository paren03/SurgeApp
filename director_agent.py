"""Staged Director Agent v1 for Luna Autonomy Control.

The Director converts CEO goals into small, reviewable missions. This staged
module is intentionally self-contained so it can be reviewed before any live
core file is changed.

Phase 5P: every mission is enriched with conservative approval/council metadata.
executor_allowed is always False in this phase.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


DIRECTOR_STATES = ("active", "done", "failed", "quarantine")

# ---------------------------------------------------------------------------
# Approval metadata constants
# ---------------------------------------------------------------------------

# High-risk core files — any mission touching these gets tier 4
_HIGH_RISK_FILES: frozenset = frozenset({
    "worker.py", "aider_bridge.py", "luna_guardian.py", "launchluna.pyw",
    "surgeapp_claude_terminal.py", "luna_start.pyw", "director_agent.py",
    "luna_modules/luna_hygiene.py", "luna_modules/luna_paths.py",
    "luna_modules/luna_routing.py", "luna_modules/luna_state.py",
})

# Keyword patterns that make a mission non-delegable → needs human only
_NON_DELEGABLE_PATTERNS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("delete_memory",   ("delete memory", "wipe memory", "truncate memory", "clear memory")),
    ("delete_logs",     ("delete log", "wipe log", "truncate log", "clear log")),
    ("delete_queues",   ("delete queue", "wipe queue", "truncate queue")),
    ("delete_backups",  ("delete backup", "wipe backup")),
    ("delete_uploads",  ("delete upload", "wipe upload")),
    ("delete_tasks",    ("delete task", "wipe task")),
    ("delete_solutions", ("delete solution",)),
    ("change_identity", ("change identity", "change persona", "change name")),
    ("change_personality", ("change personality", "change character")),
    ("change_goals",    ("change goals", "change objectives", "change mission")),
    ("expose_secret",   ("expose secret", "expose api key", "expose token", "expose .env",
                         "expose vault", "leak key", "leak token")),
    ("package_install", ("pip install", "npm install", "install package", "install module")),
    ("external_network", ("external api", "call api", "http request", "fetch url")),
    ("git_push",        ("git push", "force push")),
    ("git_reset",       ("git reset", "git clean", "git checkout --")),
    ("architecture_replacement", ("replace architecture", "rewrite from scratch",
                                  "architectural replacement")),
    ("disable_verifier", ("disable verifier", "comment out fail", "comment out warn",
                          "weaken verifier", "bypass verifier", "skip verifier",
                          "disable check", "weaken check")),
    ("weaken_quorum",   ("weaken quorum", "reduce quorum", "approve itself")),
)

# Reviewer pools by tier
_REVIEWER_POOLS: Dict[int, List[str]] = {
    0: [],
    1: [],
    2: ["qa_review", "safety_review"],
    3: ["qa_review", "safety_review", "senior_review"],
    4: ["qa_review", "safety_review", "senior_review", "human_lead"],
    5: ["human_lead"],
}

# Approval metadata version marker
_APPROVAL_METADATA_VERSION = 1


# ---------------------------------------------------------------------------
# Director paths and utilities
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Approval metadata helpers
# ---------------------------------------------------------------------------


def _normalize_target_file(path: str) -> str:
    """Return a posix-normalized, lowercased relative file path."""
    return Path(str(path)).as_posix().lower()


def _risk_to_approval_tier(risk_level: str, target_files: List[str]) -> int:
    """Map risk_level + target_files to a conservative approval tier (0-5)."""
    norm = [_normalize_target_file(f) for f in (target_files or [])]
    # Any high-risk core file → tier 4 regardless of declared risk_level
    for f in norm:
        basename = Path(f).name
        if basename in _HIGH_RISK_FILES or f in _HIGH_RISK_FILES:
            return 4
    rl = str(risk_level or "").lower().strip()
    if rl == "high":
        return 4
    if rl == "medium":
        return 3
    # low risk: distinguish memory/docs (tier 1) from additive code (tier 2)
    if rl == "low":
        for f in norm:
            if f.startswith("memory/") or f.endswith(".md") or f.endswith(".jsonl"):
                return 1
        return 2
    return 3  # conservative default for unknown


def _mission_is_non_delegable(mission: dict) -> Tuple[bool, List[str]]:
    """Return (is_non_delegable, list_of_reasons) based on goal/purpose/targets."""
    text = " ".join([
        str(mission.get("purpose", "")),
        str(mission.get("id", "")),
        str(mission.get("goal", "")),
        str(mission.get("acceptance_test", "")),
    ]).lower()
    reasons = []
    for reason_key, keywords in _NON_DELEGABLE_PATTERNS:
        for kw in keywords:
            if kw in text:
                reasons.append(reason_key)
                break
    return (len(reasons) > 0), reasons


def _mission_requires_council(approval_tier_required: int, non_delegable: bool) -> bool:
    """Council required for tier >= 2 and NOT non-delegable (which needs human instead)."""
    if non_delegable:
        return False
    return approval_tier_required >= 2


def _quorum_for_tier(approval_tier_required: int, target_files: List[str]) -> dict:
    """Return quorum settings for a given approval tier."""
    if approval_tier_required <= 1:
        return {"mode": "not_required", "required_approvals": 0, "reviewer_count": 0, "unanimous": False}
    if approval_tier_required == 2:
        return {"mode": "local_simulated", "required_approvals": 1, "reviewer_count": 2, "unanimous": False}
    if approval_tier_required == 3:
        return {"mode": "local_simulated", "required_approvals": 2, "reviewer_count": 3, "unanimous": False}
    # Tier 4 and 5: unanimous
    return {"mode": "local_simulated", "required_approvals": 3, "reviewer_count": 3, "unanimous": True}


def _reviewer_pool_for_tier(approval_tier_required: int) -> List[str]:
    """Return reviewer pool list for a given tier."""
    return list(_REVIEWER_POOLS.get(min(approval_tier_required, 5), _REVIEWER_POOLS[4]))


def _build_planned_change_template(mission: dict) -> dict:
    """Build a conservative planned_change_template from mission fields."""
    targets = list(mission.get("target_files") or [])
    return {
        "target_files": targets,
        "expected_diff_type": str(mission.get("expected_diff_type") or "bounded_edit"),
        "max_lines_changed": int(mission.get("max_lines_changed") or 80),
        "function_scope_required": bool(mission.get("function_scope_required")),
        "function_scope": str(mission.get("function_scope") or ""),
        "verification_commands": _default_verification_commands(targets),
        "rollback_plan": str(mission.get("rollback_stage_plan") or "Stage only; do not apply without verification."),
        "exit_criteria": _default_exit_criteria(mission),
    }


def _default_verification_commands(target_files: List[str]) -> List[str]:
    cmds = []
    for f in target_files:
        nf = _normalize_target_file(f)
        if nf.endswith(".py"):
            cmds.append(f"py_compile {f}")
    return cmds


def _default_exit_criteria(mission: dict) -> List[str]:
    criteria = []
    at = str(mission.get("acceptance_test") or "")
    if at:
        criteria.append(at)
    criteria.append("py_compile passes for all target .py files")
    criteria.append("Luna_Post_Repair_Verify.ps1 shows no failures")
    return criteria


def _build_approval_packet_hint(mission: dict) -> dict:
    """Build a conservative approval_packet_hint from mission fields."""
    targets = list(mission.get("target_files") or [])
    tier = mission.get("approval_tier_required", 0)
    return {
        "goal": str(mission.get("purpose") or mission.get("goal") or ""),
        "task_id": str(mission.get("id") or ""),
        "risk_tier": int(tier),
        "target_files": targets,
        "action_type": _diff_type_to_action_type(str(mission.get("expected_diff_type") or "")),
        "planned_change_summary": str(mission.get("purpose") or ""),
        "verification_commands": _default_verification_commands(targets),
        "rollback_plan": str(mission.get("rollback_stage_plan") or "Stage only."),
        "question": "Approve this mission? yes/no with reason.",
    }


def _diff_type_to_action_type(diff_type: str) -> str:
    """Map expected_diff_type to a canonical action_type string."""
    mapping = {
        "memory_summary": "generated_artifact",
        "tests": "low_risk_additive",
        "safety_guard": "medium_code_edit",
        "orchestration": "medium_code_edit",
        "process_guard": "medium_code_edit",
        "telemetry": "medium_code_edit",
        "inspector_ui": "medium_code_edit",
        "startup": "medium_code_edit",
        "fresh bounded upgrade": "medium_code_edit",
    }
    return mapping.get(diff_type.lower().strip(), "medium_code_edit")


def enrich_mission_with_approval_metadata(mission: dict) -> dict:
    """Return a copy of mission enriched with conservative approval metadata.

    executor_allowed is always False in Phase 5P.
    """
    m = dict(mission)
    tier = _risk_to_approval_tier(
        m.get("risk_level", "medium"),
        list(m.get("target_files") or []),
    )
    non_delegable, nd_reasons = _mission_is_non_delegable(m)

    if non_delegable:
        tier = 5
        council_required = False
        receipt_required = False
        needs_human = True
        enforcement_mode = "human_only"
        quorum = {"mode": "not_required", "required_approvals": 0, "reviewer_count": 0, "unanimous": False}
        reviewer_pool: List[str] = ["human_lead"]
    else:
        council_required = _mission_requires_council(tier, non_delegable)
        receipt_required = tier >= 2
        needs_human = False
        enforcement_mode = "not_required" if tier <= 1 else "receipt_required"
        quorum = _quorum_for_tier(tier, list(m.get("target_files") or []))
        reviewer_pool = _reviewer_pool_for_tier(tier)

    m["approval_tier_required"] = tier
    m["council_required"] = council_required
    m["receipt_required"] = receipt_required
    m["needs_human"] = needs_human
    m["non_delegable"] = non_delegable
    m["non_delegable_reasons"] = nd_reasons
    m["reviewer_pool"] = reviewer_pool
    m["quorum_required"] = quorum
    m["approval_packet_hint"] = _build_approval_packet_hint({**m, "approval_tier_required": tier})
    m["planned_change_template"] = _build_planned_change_template(m)
    m["enforcement_mode"] = enforcement_mode
    m["executor_allowed"] = False
    return m


def enrich_missions_with_approval_metadata(missions: List[dict]) -> List[dict]:
    """Return a new list with each mission enriched with approval metadata."""
    return [enrich_mission_with_approval_metadata(m) for m in missions]


def validate_director_approval_metadata(mission: dict) -> Tuple[bool, List[str]]:
    """Validate that a mission has all required approval metadata fields."""
    required = [
        "approval_tier_required", "council_required", "receipt_required",
        "needs_human", "non_delegable", "non_delegable_reasons",
        "reviewer_pool", "quorum_required", "approval_packet_hint",
        "planned_change_template", "enforcement_mode", "executor_allowed",
    ]
    missing = [f for f in required if f not in mission]
    errors = list(missing)
    if "executor_allowed" in mission and mission["executor_allowed"] is not False:
        errors.append("executor_allowed must be False")
    return (len(errors) == 0), errors


# ---------------------------------------------------------------------------
# CEO command parsing
# ---------------------------------------------------------------------------


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
    """Convert one CEO goal into small, bounded engineering missions with approval metadata."""
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
    return enrich_missions_with_approval_metadata(missions)


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


def _approval_metadata_policy() -> Dict[str, Any]:
    """Return the standard approval metadata policy block for Director jobs."""
    return {
        "approval_metadata_version": _APPROVAL_METADATA_VERSION,
        "council_foundation_phase": "5L",
        "approval_router_phase": "5M",
        "council_enforcer_phase": "5O",
        "executor_allowed": False,
        "live_execution_enabled": False,
        "non_delegable_requires_serge": True,
        "receipt_required_for_tier": [2, 3, 4],
        "human_only_for_non_delegable": True,
    }


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
            **_approval_metadata_policy(),
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
        # Recompute approval metadata (conservative — never downgrade)
        refreshed.append(enrich_mission_with_approval_metadata(item))

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
            **_approval_metadata_policy(),
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
