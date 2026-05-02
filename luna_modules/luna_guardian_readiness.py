"""Phase 5RS: Luna Guardian Enforcement Readiness — advisory soak module.

Stdlib only. Advisory-only: never edits luna_guardian.py, never starts/stops
services, never enables live enforcement. Reads enforcer/executor/resource/
scorecard reports and produces an advisory status showing whether Guardian
would be ready to enforce approval receipts in a future phase.

Hard rules in Phase 5RS:
  * advisory_only must be True in every status.
  * guardian_enforcing_live must be False always.
  * ready_for_live_guardian_enforcement is False (or "not_yet") in Phase 5RS.
  * safe_to_execute_now must be False for every action.
  * No process management, no service edits, no live wiring.

Tracked schema/policy:
  memory/luna_guardian_readiness.schema.json
  memory/luna_guardian_readiness_policy.json

Generated runtime artifacts (gitignored):
  memory/luna_guardian_readiness_report.json
  memory/luna_guardian_readiness_report.md
  memory/luna_guardian_enforcement_readiness.json
  memory/luna_guardian_enforcement_soak.jsonl

CLI:
  python -m luna_modules.luna_guardian_readiness --self-test
  python -m luna_modules.luna_guardian_readiness --status
  python -m luna_modules.luna_guardian_readiness --write-status
  python -m luna_modules.luna_guardian_readiness --soak --cycles 3 --sleep-seconds 1
  python -m luna_modules.luna_guardian_readiness --print-markdown
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

# Optional module imports — degrade gracefully.
try:  # pragma: no cover
    from luna_modules import luna_council_enforcer as _enforcer  # type: ignore
except Exception:  # pragma: no cover
    _enforcer = None
try:  # pragma: no cover
    from luna_modules import luna_deterministic_executor as _executor  # type: ignore
except Exception:  # pragma: no cover
    _executor = None
try:  # pragma: no cover
    from luna_modules import luna_resource_monitor as _resource_monitor  # type: ignore
except Exception:  # pragma: no cover
    _resource_monitor = None
try:  # pragma: no cover
    from luna_modules import luna_capability_scorecard as _scorecard  # type: ignore
except Exception:  # pragma: no cover
    _scorecard = None

# Action types and tier constants.
_TIER_LABELS = {
    0: "read_only",
    1: "generated_artifact",
    2: "low_risk_additive",
    3: "medium_code_edit",
    4: "high_risk_core_edit",
    5: "non_delegable",
}

_NON_DELEGABLE_ACTION_TYPES = frozenset({
    "package_install", "external_network", "memory_delete", "log_delete",
    "queue_delete", "git_reset", "git_clean", "git_push", "process_kill",
})

_DEFAULT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "advisory_only": True,
    "guardian_enforcing_live": False,
    "ready_for_live_guardian_enforcement_always_false_in_phase5rs": True,
    "soak_cycles_default": 3,
    "soak_sleep_seconds_default": 1,
    "required_before_live_enforcement": [
        "24_hour_advisory_soak",
        "manual_review_of_guardian_status_schema",
        "no_verifier_failures",
        "no_resource_blockers",
        "no_receipt_mismatch_cases",
        "explicit_serge_approval",
    ],
    "generated_report_outputs": [
        "memory/luna_guardian_readiness_report.json",
        "memory/luna_guardian_readiness_report.md",
        "memory/luna_guardian_enforcement_readiness.json",
        "memory/luna_guardian_enforcement_soak.jsonl",
    ],
}


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_readiness_id(prefix: str = "grd") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def load_json(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.is_file():
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


# ---------- policy ----------


def load_readiness_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_guardian_readiness_policy.json"
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_POLICY)
        out["_source"] = "module_fallback"
        return out
    out = dict(_DEFAULT_POLICY)
    for k, v in raw.items():
        out[k] = v
    # Hard rules always enforced.
    out["advisory_only"] = True
    out["guardian_enforcing_live"] = False
    out["ready_for_live_guardian_enforcement_always_false_in_phase5rs"] = True
    out["_source"] = str(p)
    return out


# ---------- status file readers ----------


def latest_file(path: Path | str) -> dict[str, Any] | None:
    """Read a JSON file, returning None if missing or malformed."""
    return load_json(path, default=None)


def read_guardian_status(project_dir: Path | str) -> dict[str, Any]:
    """Read luna_guardian state — advisory read-only, never modifies."""
    pdir = Path(project_dir)
    candidates = [
        pdir / "memory" / "luna_council_enforcer_state.json",
        pdir / "memory" / "luna_guardian_approval_status.json",
        pdir / "memory" / "luna_delegated_approval_state.json",
    ]
    for c in candidates:
        data = load_json(c, default=None)
        if isinstance(data, dict):
            return {"source": str(c.name), "data": data, "found": True}
    return {"source": "none", "data": {}, "found": False}


def read_enforcer_status(project_dir: Path | str) -> dict[str, Any]:
    """Read council enforcer advisory status."""
    pdir = Path(project_dir)
    candidates = [
        pdir / "memory" / "luna_council_enforcer_report.json",
        pdir / "memory" / "luna_guardian_approval_status.json",
    ]
    for c in candidates:
        data = load_json(c, default=None)
        if isinstance(data, dict):
            return {"source": str(c.name), "data": data, "found": True}
    return {"source": "none", "data": {}, "found": False}


def read_executor_report(project_dir: Path | str) -> dict[str, Any]:
    """Read latest deterministic executor report."""
    pdir = Path(project_dir)
    p = pdir / "memory" / "luna_deterministic_executor_report.json"
    data = load_json(p, default=None)
    if isinstance(data, dict):
        return {"source": str(p.name), "data": data, "found": True}
    return {"source": "none", "data": {}, "found": False}


def read_resource_status(project_dir: Path | str) -> dict[str, Any]:
    """Read resource monitor status."""
    pdir = Path(project_dir)
    candidates = [
        pdir / "memory" / "luna_resource_status.json",
        pdir / "memory" / "luna_hardware_profile.json",
    ]
    for c in candidates:
        data = load_json(c, default=None)
        if isinstance(data, dict):
            return {"source": str(c.name), "data": data, "found": True}
    return {"source": "none", "data": {}, "found": False}


def read_scorecard(project_dir: Path | str) -> dict[str, Any]:
    """Read capability scorecard."""
    pdir = Path(project_dir)
    p = pdir / "memory" / "luna_capability_scorecard.json"
    data = load_json(p, default=None)
    if isinstance(data, dict):
        return {"source": str(p.name), "data": data, "found": True}
    return {"source": "none", "data": {}, "found": False}


# ---------- synthetic pending actions ----------


def build_synthetic_pending_actions() -> list[dict[str, Any]]:
    """Build a representative set of synthetic pending actions for soak testing.

    Returns one action per risk tier (0-5) plus one non-delegable, covering
    all evaluation paths without touching any real approvals.
    """
    return [
        {
            "action_id": make_readiness_id("syn"),
            "action_type": "read_only_health_check",
            "risk_tier": 0,
            "receipt_id": "",
            "target_files": ["memory/luna_resource_status.json"],
            "description": "Synthetic: tier-0 read-only health check",
        },
        {
            "action_id": make_readiness_id("syn"),
            "action_type": "low_risk_additive",
            "risk_tier": 2,
            "receipt_id": "",
            "target_files": ["memory/luna_capability_scorecard.json"],
            "description": "Synthetic: tier-2 low-risk additive, no receipt",
        },
        {
            "action_id": make_readiness_id("syn"),
            "action_type": "medium_code_edit",
            "risk_tier": 3,
            "receipt_id": "rcpt_synthetic_003",
            "target_files": ["luna_modules/luna_deterministic_executor.py"],
            "description": "Synthetic: tier-3 medium code edit with fake receipt",
        },
        {
            "action_id": make_readiness_id("syn"),
            "action_type": "high_risk_core_edit",
            "risk_tier": 4,
            "receipt_id": "",
            "target_files": ["worker.py"],
            "description": "Synthetic: tier-4 high-risk core edit, no receipt",
        },
        {
            "action_id": make_readiness_id("syn"),
            "action_type": "process_kill",
            "risk_tier": 5,
            "receipt_id": "",
            "target_files": [],
            "description": "Synthetic: non-delegable process_kill",
        },
    ]


# ---------- action evaluation ----------


def evaluate_readiness_action(
    project_dir: Path | str,
    action: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate a single pending action in advisory mode.

    Returns a result dict with would_allow/would_block/needs_human/non_delegable.
    safe_to_execute_now is ALWAYS False.
    """
    pdir = Path(project_dir)
    action_type = str(action.get("action_type", ""))
    risk_tier = int(action.get("risk_tier", 2))
    receipt_id = str(action.get("receipt_id", "") or "")

    # Non-delegable actions always need human.
    if action_type in _NON_DELEGABLE_ACTION_TYPES or risk_tier >= 5:
        return {
            "action_id": action.get("action_id", make_readiness_id("eval")),
            "action_type": action_type,
            "risk_tier": risk_tier,
            "target_files": list(action.get("target_files") or []),
            "safe_to_execute_now": False,
            "would_allow": False,
            "would_block": False,
            "needs_human": True,
            "non_delegable": True,
            "reason": f"non_delegable_action_type:{action_type}",
            "notes": ["Non-delegable actions always require human approval"],
        }

    # Tier 0-1: generally allowed without receipt.
    if risk_tier <= 1:
        return {
            "action_id": action.get("action_id", make_readiness_id("eval")),
            "action_type": action_type,
            "risk_tier": risk_tier,
            "target_files": list(action.get("target_files") or []),
            "safe_to_execute_now": False,
            "would_allow": True,
            "would_block": False,
            "needs_human": False,
            "non_delegable": False,
            "reason": "tier_0_1_no_receipt_required",
            "notes": ["Advisory only — safe_to_execute_now is False"],
        }

    # Tier 2+: receipt required.
    if not receipt_id:
        return {
            "action_id": action.get("action_id", make_readiness_id("eval")),
            "action_type": action_type,
            "risk_tier": risk_tier,
            "target_files": list(action.get("target_files") or []),
            "safe_to_execute_now": False,
            "would_allow": False,
            "would_block": True,
            "needs_human": False,
            "non_delegable": False,
            "reason": f"missing_receipt_for_tier_{risk_tier}",
            "notes": [f"Receipt required for tier {risk_tier}; advisory block"],
        }

    # Try enforcer advisory check if available.
    if _enforcer is not None:
        try:
            action_record = {
                "action_type": action_type,
                "target_files": list(action.get("target_files") or []),
                "receipt_id": receipt_id,
                "approval_tier_required": risk_tier,
                "created_at": now_iso(),
            }
            status = _enforcer.evaluate_action_enforcement(pdir, action_record)
            decision = status.get("decision", "unknown")
            would_allow = decision in ("would_allow", "not_required")
            would_block = decision in ("would_block", "blocked")
            needs_human = decision in ("needs_human", "escalate")
            return {
                "action_id": action.get("action_id", make_readiness_id("eval")),
                "action_type": action_type,
                "risk_tier": risk_tier,
                "target_files": list(action.get("target_files") or []),
                "safe_to_execute_now": False,
                "would_allow": would_allow,
                "would_block": would_block,
                "needs_human": needs_human,
                "non_delegable": False,
                "reason": f"enforcer_decision:{decision}",
                "notes": ["Advisory only — safe_to_execute_now is False"],
            }
        except Exception:
            pass

    # Fallback: receipt present but enforcer unavailable — advisory allow for tier 2-3.
    would_allow = risk_tier <= 3
    return {
        "action_id": action.get("action_id", make_readiness_id("eval")),
        "action_type": action_type,
        "risk_tier": risk_tier,
        "target_files": list(action.get("target_files") or []),
        "safe_to_execute_now": False,
        "would_allow": would_allow,
        "would_block": not would_allow,
        "needs_human": risk_tier >= 4,
        "non_delegable": False,
        "reason": "enforcer_unavailable_receipt_present",
        "notes": ["Enforcer unavailable — degraded advisory evaluation", "safe_to_execute_now is False"],
    }


# ---------- readiness status builder ----------


def build_guardian_readiness_status(
    project_dir: Path | str,
    pending_actions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the guardian readiness advisory status.

    advisory_only, guardian_enforcing_live, and safe_to_execute_now are
    all hard-coded to their safe values regardless of policy.
    """
    pdir = Path(project_dir)
    policy = load_readiness_policy(pdir)

    # Read all status inputs.
    guardian_st = read_guardian_status(pdir)
    enforcer_st = read_enforcer_status(pdir)
    executor_rep = read_executor_report(pdir)
    resource_st = read_resource_status(pdir)
    scorecard_st = read_scorecard(pdir)

    # Derive summary labels.
    enforcer_label = "unknown"
    if enforcer_st["found"]:
        d = enforcer_st["data"]
        enforcer_label = str(d.get("overall_status") or d.get("status") or "present")

    executor_label = "unknown"
    if executor_rep["found"]:
        d = executor_rep["data"]
        executor_label = "ok" if d.get("success") else "last_run_failed"
        if d.get("safe_to_apply_real_project") is True:
            executor_label = "UNSAFE_policy_violation"

    resource_label = "unknown"
    if resource_st["found"]:
        d = resource_st["data"]
        resource_label = str(d.get("mode") or d.get("resource_mode") or "present")

    scorecard_label = "unknown"
    if scorecard_st["found"]:
        d = scorecard_st["data"]
        scorecard_label = str(d.get("overall_health") or d.get("status") or "present")

    # Evaluate pending actions.
    actions_to_eval = pending_actions if pending_actions is not None else []
    evaluated: list[dict[str, Any]] = []
    for action in actions_to_eval:
        result = evaluate_readiness_action(pdir, action)
        evaluated.append(result)

    would_allow_count = sum(1 for r in evaluated if r.get("would_allow"))
    would_block_count = sum(1 for r in evaluated if r.get("would_block"))
    needs_human_count = sum(1 for r in evaluated if r.get("needs_human"))
    non_delegable_count = sum(1 for r in evaluated if r.get("non_delegable"))

    # Determine overall status.
    blockers: list[str] = []
    notes: list[str] = []

    if executor_label == "UNSAFE_policy_violation":
        blockers.append("executor_safe_to_apply_real_project_was_true")
    if would_block_count > 0:
        notes.append(f"{would_block_count} action(s) would be blocked by enforcer")
    if needs_human_count > 0:
        notes.append(f"{needs_human_count} action(s) need human review")

    if blockers:
        overall_status = "blocked"
    elif would_block_count > 0 or needs_human_count > 0:
        overall_status = "watch"
    elif enforcer_st["found"] and executor_rep["found"]:
        overall_status = "healthy"
    else:
        overall_status = "watch"

    notes.append("advisory_only: This report does not enable live Guardian enforcement")

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "advisory_only": True,
        "guardian_enforcing_live": False,
        "ready_for_live_guardian_enforcement": False,
        "overall_status": overall_status,
        "receipt_enforcer_status": enforcer_label,
        "executor_status": executor_label,
        "resource_mode": resource_label,
        "scorecard_readiness": scorecard_label,
        "pending_action_count": len(evaluated),
        "would_allow_count": would_allow_count,
        "would_block_count": would_block_count,
        "needs_human_count": needs_human_count,
        "non_delegable_count": non_delegable_count,
        "actions": evaluated,
        "required_before_live_enforcement": list(
            policy.get("required_before_live_enforcement", [])
        ),
        "recommended_next_action": (
            "resolve_blockers" if blockers
            else "continue_advisory_soak"
        ),
        "notes": notes,
        "blockers": blockers,
    }


# ---------- rendering ----------


def render_guardian_readiness_markdown(status: dict[str, Any]) -> str:
    lines = [
        "# Luna Guardian Readiness Status",
        "",
        f"**generated_at**: {status.get('generated_at', '')}",
        f"**advisory_only**: {status.get('advisory_only', True)}",
        f"**guardian_enforcing_live**: {status.get('guardian_enforcing_live', False)}",
        f"**ready_for_live_guardian_enforcement**: {status.get('ready_for_live_guardian_enforcement', False)}",
        f"**overall_status**: {status.get('overall_status', 'unknown')}",
        "",
        "## Component Status",
        f"- receipt_enforcer_status: {status.get('receipt_enforcer_status', 'unknown')}",
        f"- executor_status: {status.get('executor_status', 'unknown')}",
        f"- resource_mode: {status.get('resource_mode', 'unknown')}",
        f"- scorecard_readiness: {status.get('scorecard_readiness', 'unknown')}",
        "",
        "## Action Summary",
        f"- pending_action_count: {status.get('pending_action_count', 0)}",
        f"- would_allow_count: {status.get('would_allow_count', 0)}",
        f"- would_block_count: {status.get('would_block_count', 0)}",
        f"- needs_human_count: {status.get('needs_human_count', 0)}",
        f"- non_delegable_count: {status.get('non_delegable_count', 0)}",
    ]

    actions = status.get("actions") or []
    if actions:
        lines += ["", "## Action Evaluations"]
        for a in actions:
            outcome = "ALLOW" if a.get("would_allow") else ("HUMAN" if a.get("needs_human") else "BLOCK")
            nd = " [NON-DELEGABLE]" if a.get("non_delegable") else ""
            lines.append(
                f"- [{outcome}]{nd} `{a.get('action_type')}` tier={a.get('risk_tier')} "
                f"safe_to_execute_now={a.get('safe_to_execute_now', False)}"
            )

    req = status.get("required_before_live_enforcement") or []
    if req:
        lines += ["", "## Required Before Live Enforcement"]
        for r in req:
            lines.append(f"- {r}")

    blockers = status.get("blockers") or []
    if blockers:
        lines += ["", "## Blockers"]
        for b in blockers:
            lines.append(f"- {b}")

    notes = status.get("notes") or []
    if notes:
        lines += ["", "## Notes"]
        for n in notes:
            lines.append(f"- {n}")

    lines += [
        "",
        f"**recommended_next_action**: {status.get('recommended_next_action', '')}",
        "",
    ]
    return "\n".join(lines)


# ---------- report writing ----------


def write_guardian_readiness_report(
    project_dir: Path | str,
    status: dict[str, Any],
) -> dict[str, str]:
    """Write guardian readiness artifacts under memory/. Returns paths dict."""
    pdir = Path(project_dir).resolve()
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)

    json_p = mem / "luna_guardian_readiness_report.json"
    md_p = mem / "luna_guardian_readiness_report.md"
    readiness_p = mem / "luna_guardian_enforcement_readiness.json"
    soak_p = mem / "luna_guardian_enforcement_soak.jsonl"

    write_json_atomic(json_p, status)

    tmp = md_p.with_suffix(md_p.suffix + ".tmp")
    tmp.write_text(render_guardian_readiness_markdown(status), encoding="utf-8")
    os.replace(tmp, md_p)

    readiness_summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": status.get("generated_at", now_iso()),
        "advisory_only": True,
        "guardian_enforcing_live": False,
        "ready_for_live_guardian_enforcement": False,
        "overall_status": status.get("overall_status", "unknown"),
        "recommended_next_action": status.get("recommended_next_action", ""),
    }
    write_json_atomic(readiness_p, readiness_summary)

    append_jsonl(soak_p, {
        "ts": now_iso(),
        "readiness_id": status.get("generated_at", now_iso()),
        "overall_status": status.get("overall_status", "unknown"),
        "advisory_only": True,
        "guardian_enforcing_live": False,
        "ready_for_live_guardian_enforcement": False,
        "would_allow_count": status.get("would_allow_count", 0),
        "would_block_count": status.get("would_block_count", 0),
        "needs_human_count": status.get("needs_human_count", 0),
    })

    return {
        "json": str(json_p),
        "md": str(md_p),
        "readiness": str(readiness_p),
        "soak": str(soak_p),
    }


# ---------- soak ----------


def run_readiness_soak(
    project_dir: Path | str,
    cycles: int = 3,
    sleep_seconds: float = 1.0,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Run bounded advisory soak cycles. Returns soak summary.

    dry_run has no effect on safety — all cycles are advisory-only regardless.
    Cycle count is clamped to 1-20. Sleep is clamped to 0-60s.
    """
    pdir = Path(project_dir)
    cycles = max(1, min(20, int(cycles)))
    sleep_seconds = max(0.0, min(60.0, float(sleep_seconds)))

    results: list[dict[str, Any]] = []
    for i in range(cycles):
        pending = build_synthetic_pending_actions()
        status = build_guardian_readiness_status(pdir, pending_actions=pending)
        written = write_guardian_readiness_report(pdir, status)
        results.append({
            "cycle": i + 1,
            "overall_status": status.get("overall_status"),
            "advisory_only": True,
            "guardian_enforcing_live": False,
            "ready_for_live_guardian_enforcement": False,
            "would_allow_count": status.get("would_allow_count", 0),
            "would_block_count": status.get("would_block_count", 0),
            "needs_human_count": status.get("needs_human_count", 0),
        })
        if i < cycles - 1 and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    statuses = [r["overall_status"] for r in results]
    return {
        "cycles_run": cycles,
        "advisory_only": True,
        "guardian_enforcing_live": False,
        "ready_for_live_guardian_enforcement": False,
        "cycle_results": results,
        "final_status": statuses[-1] if statuses else "unknown",
        "all_healthy": all(s == "healthy" for s in statuses),
        "any_blocked": any(s == "blocked" for s in statuses),
    }


# ---------- self-test ----------


def self_test() -> int:
    """Run a complete self-test using a temporary project directory. Returns 0 on success."""
    import tempfile

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)
        (td / "luna_modules").mkdir(parents=True, exist_ok=True)

        # Build status without any pending actions.
        status = build_guardian_readiness_status(td)
        assert status["advisory_only"] is True, "advisory_only must be True"
        assert status["guardian_enforcing_live"] is False, "guardian_enforcing_live must be False"
        assert status["ready_for_live_guardian_enforcement"] is False, \
            "ready_for_live_guardian_enforcement must be False"
        assert "schema_version" in status
        assert "generated_at" in status

        # Build status with synthetic pending actions.
        pending = build_synthetic_pending_actions()
        status2 = build_guardian_readiness_status(td, pending_actions=pending)
        for action in status2["actions"]:
            assert action.get("safe_to_execute_now") is False, \
                f"safe_to_execute_now must be False for action {action.get('action_id')}"

        # Write report and verify paths stay under temp dir.
        written = write_guardian_readiness_report(td, status2)
        for key, path in written.items():
            p = Path(path)
            assert str(p).startswith(str(td)), f"Report path {p} escapes temp dir"
            assert "memory" in str(p), f"Report path {p} must be under memory/"

        # Render markdown.
        md = render_guardian_readiness_markdown(status2)
        assert "advisory_only" in md
        assert "False" in md  # ready_for_live_guardian_enforcement: False

        # Run a mini soak.
        soak_result = run_readiness_soak(td, cycles=2, sleep_seconds=0)
        assert soak_result["advisory_only"] is True
        assert soak_result["guardian_enforcing_live"] is False
        assert soak_result["cycles_run"] == 2

        print(json.dumps({
            "self_test": "PASS",
            "advisory_only": True,
            "guardian_enforcing_live": False,
            "ready_for_live_guardian_enforcement": False,
            "cycles_run": soak_result["cycles_run"],
        }, indent=2))
    return 0


# ---------- CLI ----------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Guardian Readiness Advisory Module (Phase 5RS)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--write-status", action="store_true")
    parser.add_argument("--soak", action="store_true")
    parser.add_argument("--print-markdown", action="store_true")
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    pdir = Path(args.project_dir)

    if args.self_test:
        return self_test()

    if args.status:
        pending = build_synthetic_pending_actions()
        status = build_guardian_readiness_status(pdir, pending_actions=pending)
        summary = {
            "advisory_only": status["advisory_only"],
            "guardian_enforcing_live": status["guardian_enforcing_live"],
            "ready_for_live_guardian_enforcement": status["ready_for_live_guardian_enforcement"],
            "overall_status": status["overall_status"],
            "pending_action_count": status["pending_action_count"],
            "would_allow_count": status["would_allow_count"],
            "would_block_count": status["would_block_count"],
            "needs_human_count": status["needs_human_count"],
            "non_delegable_count": status["non_delegable_count"],
            "recommended_next_action": status["recommended_next_action"],
        }
        print(json.dumps(summary, indent=2))
        return 0

    if args.write_status:
        pending = build_synthetic_pending_actions()
        status = build_guardian_readiness_status(pdir, pending_actions=pending)
        written = write_guardian_readiness_report(pdir, status)
        print(json.dumps({
            "advisory_only": True,
            "guardian_enforcing_live": False,
            "ready_for_live_guardian_enforcement": False,
            "overall_status": status["overall_status"],
            "written": written,
        }, indent=2))
        return 0

    if args.soak:
        result = run_readiness_soak(
            pdir,
            cycles=args.cycles,
            sleep_seconds=args.sleep_seconds,
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.print_markdown:
        p = pdir / "memory" / "luna_guardian_readiness_report.json"
        status = load_json(p, default=None)
        if not isinstance(status, dict):
            # Generate fresh.
            pending = build_synthetic_pending_actions()
            status = build_guardian_readiness_status(pdir, pending_actions=pending)
        import sys as _sys
        _sys.stdout.write(render_guardian_readiness_markdown(status))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
