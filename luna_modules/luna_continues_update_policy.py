"""Smart continues_update planning for Luna Autonomy Control v1."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Dict, List


HIGH_IMPACT_AREAS = (
    "director_agent",
    "cu_watchdog_cycle_telemetry",
    "inspector_live_progress",
    "function_scoped_editing",
    "self_test_harness",
    "startup_stability",
    "duplicate_process_prevention",
)

MICRO_JOB_COUNT = 8
MICRO_MAX_LINES = 90
MICRO_UI_MAX_LINES = 120

LOW_VALUE_MARKERS = (
    "one-line docstring",
    "one sentence docstring",
    "formatting-only",
    "format only",
    "repeated type hint",
    "no-op type hint",
    "micro change",
)

_TEMPLATES = [
    {
        "impact_area": "director_agent",
        "target_files": ["director_agent.py"],
        "prompt_family": "director_mission_planning",
        "prompt": "Implement or improve Director mission planning for `/ceo` goals with bounded missions, acceptance tests, and stage-only rollback notes.",
        "acceptance_test": "A `/ceo` goal produces active Director jobs with all required mission fields.",
        "verify": ["python -m py_compile director_agent.py"],
        "expected_diff_type": "orchestration",
        "max_lines_changed": MICRO_MAX_LINES,
    },
    {
        "impact_area": "cu_watchdog_cycle_telemetry",
        "target_files": ["worker.py"],
        "prompt_family": "cu_budget_telemetry",
        "prompt": "Add continues_update cycle telemetry that records budgets, failure counts, NOOP counts, and pause reasons without launching duplicate loops.",
        "acceptance_test": "A budget-exceeded cycle writes Inspector and nightly summary entries and stops new job creation.",
        "verify": ["python -m py_compile worker.py"],
        "expected_diff_type": "telemetry",
        "max_lines_changed": MICRO_MAX_LINES,
    },
    {
        "impact_area": "inspector_live_progress",
        "target_files": ["SurgeApp_Claude_Terminal.py"],
        "prompt_family": "inspector_autonomy_panel",
        "prompt": "Expose Autonomy Control plans, jobs, logs, diffs, verification, failures, and summaries in the Inspector without noisy main-chat output.",
        "acceptance_test": "Inspector can read live-feed autonomy events and show plan/job/failure summaries.",
        "verify": ["python -m py_compile SurgeApp_Claude_Terminal.py"],
        "expected_diff_type": "inspector_ui",
        "max_lines_changed": MICRO_UI_MAX_LINES,
    },
    {
        "impact_area": "function_scoped_editing",
        "target_files": ["aider_bridge.py"],
        "prompt_family": "function_scoped_editing",
        "prompt": "Constrain Aider Bridge jobs to explicit target functions or bounded regions and reject broad whole-file rewrite requests unless approved.",
        "acceptance_test": "Aider job metadata records target function or bounded region before execution.",
        "verify": ["python -m py_compile aider_bridge.py"],
        "expected_diff_type": "safety_guard",
        "max_lines_changed": MICRO_MAX_LINES,
    },
    {
        "impact_area": "self_test_harness",
        "target_files": ["tests/test_luna_autonomy_control.py"],
        "prompt_family": "self_test_harness",
        "prompt": "Expand the self-test harness for autonomy quality gates, Aider result policy, and continues_update cycle summaries.",
        "acceptance_test": "Unit tests cover DONE, NOOP, failure, budget pause, and nightly summary behavior.",
        "verify": ["python -m unittest discover -s tests -v"],
        "expected_diff_type": "tests",
        "max_lines_changed": MICRO_UI_MAX_LINES,
    },
    {
        "impact_area": "startup_stability",
        "target_files": ["LaunchLuna.pyw", "luna_start.pyw"],
        "prompt_family": "startup_stability",
        "prompt": "Improve startup stability so the desktop icon starts Luna services in the right order and records startup health without spawning duplicate loops.",
        "acceptance_test": "Launcher path, icon path, working directory, and service boot order are recorded and verified.",
        "verify": ["python -m py_compile LaunchLuna.pyw luna_start.pyw"],
        "expected_diff_type": "startup",
        "max_lines_changed": MICRO_MAX_LINES,
    },
    {
        "impact_area": "duplicate_process_prevention",
        "target_files": ["luna_guardian.py", "aider_bridge.py"],
        "prompt_family": "duplicate_process_prevention",
        "prompt": "Strengthen PID-lock and process-health checks so Guardian, worker, Aider Bridge, and continues_update do not launch duplicate instances.",
        "acceptance_test": "Duplicate start attempts exit cleanly and write a live-feed or log event explaining the existing PID.",
        "verify": ["python -m py_compile luna_guardian.py aider_bridge.py"],
        "expected_diff_type": "process_guard",
        "max_lines_changed": MICRO_MAX_LINES,
    },
    {
        "impact_area": "review_gate_stability",
        "target_files": ["luna_modules/luna_two_pass_review.py", "luna_modules/luna_verification.py"],
        "prompt_family": "review_gate_stability",
        "prompt": "Strengthen two-pass review and verification gates so staged upgrades are blocked when evidence is missing, stale, or ambiguous.",
        "acceptance_test": "Two-pass review reports a clear block reason when verification evidence is missing or stale.",
        "verify": ["python -m unittest tests.test_luna_two_pass_review -v"],
        "expected_diff_type": "review_gate",
        "max_lines_changed": MICRO_MAX_LINES,
    },
]


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def build_continues_update_plan(goal: str, max_jobs: int = 12) -> Dict[str, Any]:
    """Return a small, steady stage-only CU plan tuned for nonstop micro-upgrades."""

    limit = max(MICRO_JOB_COUNT, min(MICRO_JOB_COUNT, int(max_jobs or MICRO_JOB_COUNT)))
    jobs: List[Dict[str, Any]] = []
    for index, template in enumerate(_TEMPLATES[:limit], start=1):
        jobs.append({
            "id": f"cu_{index:02d}_{template['impact_area']}",
            "origin": "continues_update",
            "goal": str(goal or "continues update"),
            "task_type": "aider_patch",
            "queue_after_previous_result": True,
            "wait_for_result": True,
            "apply_on_pass": False,
            "stage_only": True,
            **template,
        })
    return {
        "ts": _now_iso(),
        "goal": str(goal or "continues update"),
        "queue_mode": "one_by_one",
        "verify_each_job": True,
        "stage_only": True,
        "write_morning_summary": True,
        "micro_upgrade_mode": True,
        "jobs": jobs,
    }


def validate_continues_update_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Validate the smart plan against anti-loop budgets."""
    jobs = list(plan.get("jobs") or [])
    violations: List[Dict[str, Any]] = []
    if not 8 <= len(jobs) <= 12:
        violations.append({"budget": "job_count", "observed": len(jobs), "limit": "8..12"})
    file_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    for job in jobs:
        for target in job.get("target_files") or []:
            file_counts[str(target).lower()] += 1
        family_counts[str(job.get("prompt_family") or "").lower()] += 1
        prompt = str(job.get("prompt") or "").lower()
        for marker in LOW_VALUE_MARKERS:
            if marker in prompt:
                violations.append({"budget": "low_value_prompt", "marker": marker, "job": job.get("id")})
    for target, count in file_counts.items():
        if count > 3:
            violations.append({"budget": "max_same_file_jobs_per_cycle", "target_file": target, "observed": count, "limit": 3})
    for family, count in family_counts.items():
        if count > 2:
            violations.append({"budget": "max_same_prompt_family_per_cycle", "prompt_family": family, "observed": count, "limit": 2})
    return {"ok": not violations, "violations": violations}


def build_morning_summary(cycle: Dict[str, Any]) -> str:
    """Build the required learning summary for nightly/morning reports."""
    sections = [
        ("what was attempted", cycle.get("attempted") or []),
        ("what changed", cycle.get("changed") or []),
        ("what failed", cycle.get("failed") or []),
        ("what produced no diff", cycle.get("noop") or cycle.get("no_diff") or []),
        ("what Luna learned", cycle.get("learned") or []),
        ("what should be tried next", cycle.get("next") or []),
        ("which files are risky", cycle.get("risky_files") or []),
        ("which prompts worked", cycle.get("prompts_worked") or []),
        ("which prompts failed", cycle.get("prompts_failed") or []),
    ]
    lines = [f"## Continues Update Morning Summary - {_now_iso()}"]
    for title, values in sections:
        lines.append(f"### {title}")
        items = values if isinstance(values, list) else [values]
        if not items:
            lines.append("- none recorded")
        else:
            lines.extend(f"- {item}" for item in items)
    return "\n".join(lines) + "\n"
