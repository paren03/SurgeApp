"""Luna Council Enforcer — Phase 5O read-only advisory enforcement foundation.

Advisory only. safe_to_execute_now is always False.
Does not modify receipts, queues, or runtime services.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Optional module imports — graceful degradation
# ---------------------------------------------------------------------------
_COUNCIL = None
_ROUTER = None
_UPGRADE_GATE = None

try:
    from luna_modules import luna_ai_council as _COUNCIL  # type: ignore
except Exception:
    pass

try:
    from luna_modules import luna_approval_router as _ROUTER  # type: ignore
except Exception:
    pass

try:
    from luna_modules import luna_upgrade_gate as _UPGRADE_GATE  # type: ignore
except Exception:
    pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCHEMA_VERSION = 1
PROJECT_DIR_DEFAULT = str(pathlib.Path(__file__).parent.parent)

ACTION_TYPES: dict[str, int] = {
    "read_only": 0,
    "generated_artifact": 1,
    "low_risk_additive": 2,
    "medium_code_edit": 3,
    "high_risk_core_edit": 4,
    "emergency_repair": 4,
    "non_delegable": 5,
}

DEFAULT_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "advisory_only": True,
    "safe_to_execute_now_always_false_in_phase5o": True,
    "receipt_max_age_minutes": 120,
    "require_receipt_for_tiers": [2, 3, 4, 5],
    "receipt_optional_for_tiers": [0, 1],
    "require_diff_hash_for_tiers": [3, 4],
    "require_command_binding_for_tiers": [3, 4],
    "high_risk_paths": [
        "worker.py", "aider_bridge.py", "luna_guardian.py",
        "LaunchLuna.pyw", "SurgeApp_Claude_Terminal.py",
        "luna_start.pyw", "director_agent.py",
    ],
    "non_delegable_actions": [
        "delete_memory", "delete_logs", "delete_queues", "delete_backups",
        "delete_uploads", "change_identity", "change_personality", "change_goals",
        "expose_secret", "package_install", "external_network", "git_push",
        "git_reset", "git_clean", "architecture_replacement",
        "disable_verifier", "weaken_quorum_policy",
    ],
    "status_output": "memory/luna_guardian_approval_status.json",
}

_RECEIPT_REQUIRED_FIELDS = frozenset({
    "schema_version", "receipt_id", "approval_id", "created_at",
    "decision", "task_id", "action_type", "target_files",
})

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_check_id(prefix: str = "enf") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


def sha256_json(data: Any) -> str:
    serialized = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


def load_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default if default is not None else {}


def write_json_atomic(path: str, data: Any) -> None:
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(p) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def append_jsonl(path: str, row: Any) -> None:
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_enforcer_policy(project_dir: str) -> dict:
    policy_path = os.path.join(project_dir, "memory", "luna_council_enforcer_policy.json")
    raw = load_json(policy_path, {})
    if not isinstance(raw, dict):
        return dict(DEFAULT_POLICY)
    merged = dict(DEFAULT_POLICY)
    merged.update({k: v for k, v in raw.items() if k in DEFAULT_POLICY})
    return merged


def normalize_target_files(target_files: Any) -> list:
    if not target_files:
        return []
    if isinstance(target_files, str):
        target_files = [target_files]
    seen: set = set()
    result = []
    for f in target_files:
        posix = pathlib.Path(str(f)).as_posix()
        if posix not in seen:
            seen.add(posix)
            result.append(posix)
    return sorted(result)


def target_files_hash(target_files: Any) -> str:
    return sha256_json(normalize_target_files(target_files))


def read_council_receipts(project_dir: str, limit: int = 200) -> list:
    path = os.path.join(project_dir, "memory", "luna_ai_council_approvals.jsonl")
    receipts: list = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    receipts.append(json.loads(line))
                except Exception:
                    pass  # skip corrupt rows
    except FileNotFoundError:
        pass
    return receipts[-limit:] if len(receipts) > limit else receipts


def latest_receipts_by_task(
    project_dir: str,
    task_id: str = "",
    target_files: Any = None,
    action_type: str = "",
) -> list:
    receipts = read_council_receipts(project_dir)
    norm_targets = normalize_target_files(target_files) if target_files else None
    matches = []
    for r in reversed(receipts):
        if task_id and r.get("task_id", "") != task_id:
            continue
        if action_type and r.get("action_type", "") != action_type:
            continue
        if norm_targets is not None:
            if normalize_target_files(r.get("target_files", [])) != norm_targets:
                continue
        matches.append(r)
    return matches


def classify_action_type(
    action_type: str,
    target_files: Any = None,
    goal: str = "",
) -> tuple:
    """Return (canonical_action_type, risk_tier)."""
    at = str(action_type).lower().strip() if action_type else ""
    tier = ACTION_TYPES.get(at)
    if tier is not None:
        return at, tier
    if target_files:
        high_names = {
            "worker.py", "aider_bridge.py", "luna_guardian.py",
            "launchluna.pyw", "surgeapp_claude_terminal.py",
            "luna_start.pyw", "director_agent.py",
        }
        for f in normalize_target_files(target_files):
            if pathlib.Path(f).name.lower() in high_names:
                return "high_risk_core_edit", 4
    return "unknown", -1


def classify_non_delegable(
    action_type: str,
    target_files: Any = None,
    goal: str = "",
    policy: dict = None,
) -> list:
    if policy is None:
        policy = DEFAULT_POLICY
    nd_actions = policy.get("non_delegable_actions", DEFAULT_POLICY["non_delegable_actions"])
    high_risk = policy.get("high_risk_paths", DEFAULT_POLICY["high_risk_paths"])
    flags = []
    at = str(action_type).lower().strip() if action_type else ""
    if at in nd_actions:
        flags.append(f"non_delegable_action:{at}")
    if target_files:
        high_names = {pathlib.Path(p).name.lower() for p in high_risk}
        for f in normalize_target_files(target_files):
            if pathlib.Path(f).name.lower() in high_names:
                flag = f"high_risk_target:{f}"
                if flag not in flags:
                    flags.append(flag)
    goal_lower = (goal or "").lower()
    for nd in nd_actions:
        keyword = nd.replace("_", " ")
        if keyword in goal_lower:
            tag = f"non_delegable_goal_keyword:{nd}"
            if tag not in flags:
                flags.append(tag)
    return flags


def verify_receipt_fields(receipt: dict) -> tuple:
    """Returns (is_valid, missing_fields)."""
    missing = [f for f in _RECEIPT_REQUIRED_FIELDS if f not in receipt]
    return (len(missing) == 0), missing


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def verify_receipt_for_action(
    project_dir: str,
    receipt: dict,
    action_record: dict,
    policy: dict = None,
) -> tuple:
    """Returns (is_valid, reason, errors)."""
    if policy is None:
        policy = load_enforcer_policy(project_dir)

    ok, missing = verify_receipt_fields(receipt)
    if not ok:
        return False, f"missing_fields:{','.join(missing)}", list(missing)

    max_age = policy.get("receipt_max_age_minutes", 120)
    created = _parse_iso(receipt.get("created_at", ""))
    if created is None:
        return False, "unparseable_created_at", ["unparseable_created_at"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    age_min = (datetime.now(timezone.utc) - created).total_seconds() / 60
    if age_min > max_age:
        return False, "expired", [f"age_minutes={age_min:.1f} > max={max_age}"]

    if receipt.get("decision", "") not in ("approve", "approved"):
        return False, f"decision_not_approve:{receipt.get('decision', '')}", []

    receipt_targets = normalize_target_files(receipt.get("target_files", []))
    action_targets = normalize_target_files(action_record.get("target_files", []))
    if action_targets and receipt_targets and receipt_targets != action_targets:
        return False, "target_files_mismatch", [f"receipt={receipt_targets} action={action_targets}"]

    r_type = receipt.get("action_type", "")
    a_type = action_record.get("action_type", "")
    if r_type and a_type and r_type != a_type:
        return False, f"action_type_mismatch:{r_type}!={a_type}", []

    a_phash = action_record.get("packet_hash", "")
    r_phash = receipt.get("packet_hash", "")
    if a_phash and r_phash and a_phash != r_phash:
        return False, "packet_hash_mismatch", []

    a_nonce = action_record.get("nonce", "")
    r_nonce = receipt.get("nonce", "")
    if a_nonce and r_nonce and a_nonce != r_nonce:
        return False, "nonce_mismatch", []

    return True, "valid", []


def evaluate_action_enforcement(
    project_dir: str,
    action_record: dict,
    policy: dict = None,
) -> dict:
    if policy is None:
        policy = load_enforcer_policy(project_dir)

    action_id = action_record.get("action_id", "")
    task_id = action_record.get("task_id", "")
    action_type_raw = action_record.get("action_type", "unknown")
    target_files = normalize_target_files(action_record.get("target_files", []))
    goal = action_record.get("goal", "")

    at, risk_tier = classify_action_type(action_type_raw, target_files, goal)
    if at == "unknown":
        at = action_type_raw
    if action_record.get("risk_tier") is not None:
        risk_tier = int(action_record["risk_tier"])

    result: dict = {
        "schema_version": SCHEMA_VERSION,
        "check_id": make_check_id("enf"),
        "checked_at": now_iso(),
        "action_id": action_id,
        "task_id": task_id,
        "action_type": at,
        "risk_tier": risk_tier,
        "target_files": target_files,
        "decision": "unknown",
        "reason": "",
        "receipt_required": False,
        "receipt_found": False,
        "receipt_valid": False,
        "receipt_id": "",
        "approval_id": "",
        "errors": [],
        "warnings": [],
        "non_delegable_flags": [],
        "safe_to_execute_now": False,
        "advisory_only": True,
    }

    nd_flags = classify_non_delegable(at, target_files, goal, policy)
    result["non_delegable_flags"] = nd_flags

    if nd_flags:
        result["decision"] = "needs_human"
        result["reason"] = f"non_delegable:{nd_flags[0]}"
        result["receipt_required"] = True
        attach_decision_card_to_enforcement_result(project_dir, result)
        return result

    optional_tiers = policy.get("receipt_optional_for_tiers", [0, 1])
    if risk_tier in optional_tiers:
        result["decision"] = "not_required"
        result["reason"] = "tier_read_only_or_generated_artifact"
        attach_decision_card_to_enforcement_result(project_dir, result)
        return result

    result["receipt_required"] = True

    receipts = latest_receipts_by_task(
        project_dir, task_id=task_id,
        target_files=target_files if target_files else None,
        action_type=at,
    )
    if not receipts and task_id:
        receipts = latest_receipts_by_task(
            project_dir,
            target_files=target_files if target_files else None,
            action_type=at,
        )

    if not receipts:
        result["decision"] = "would_block"
        result["reason"] = "missing_receipt"
        attach_decision_card_to_enforcement_result(project_dir, result)
        return result

    result["receipt_found"] = True
    receipt = receipts[0]
    result["receipt_id"] = receipt.get("receipt_id", "")
    result["approval_id"] = receipt.get("approval_id", "")

    valid, reason, errors = verify_receipt_for_action(project_dir, receipt, action_record, policy)
    result["errors"] = errors

    if not valid:
        result["decision"] = "stale" if reason == "expired" else "invalid"
        result["reason"] = reason
        attach_decision_card_to_enforcement_result(project_dir, result)
        return result

    req_diff_tiers = policy.get("require_diff_hash_for_tiers", [3, 4])
    if risk_tier in req_diff_tiers:
        a_diff = action_record.get("diff_hash", "")
        r_diff = receipt.get("diff_hash", "")
        if not a_diff:
            result["decision"] = "would_block"
            result["reason"] = "diff_hash_missing_in_action"
            attach_decision_card_to_enforcement_result(project_dir, result)
            return result
        if r_diff and a_diff != r_diff:
            result["decision"] = "invalid"
            result["reason"] = "diff_hash_mismatch"
            attach_decision_card_to_enforcement_result(project_dir, result)
            return result

    result["receipt_valid"] = True
    result["decision"] = "would_allow"
    result["reason"] = "valid_receipt_found"
    attach_decision_card_to_enforcement_result(project_dir, result)
    return result


# ---------- Phase 5U: decision-card integration (advisory only) ----------


def _serge_policy_module():
    """Defensively import luna_serge_policy. Returns module or None."""
    try:
        from luna_modules import luna_serge_policy as _sp  # type: ignore
        return _sp
    except Exception:
        return None


def build_decision_context_from_enforcement_result(result: dict) -> dict:
    """Build a decision_context dict (Phase 5T schema) from an enforcement result."""
    if not isinstance(result, dict):
        return {}
    decision = str(result.get("decision") or "unknown")
    receipt_valid = bool(result.get("receipt_valid"))
    nd_flags = list(result.get("non_delegable_flags") or [])
    return {
        "schema_version": 1,
        "goal": str(result.get("goal", "") or result.get("action_type", "")),
        "action_type": str(result.get("action_type") or ""),
        "risk_tier": int(result.get("risk_tier") or 0),
        "target_files": list(result.get("target_files") or []),
        "router_decision": "unknown",
        "council_decision": "unknown",
        "enforcer_decision": (
            decision if decision in (
                "not_required", "would_allow", "would_block", "needs_human",
                "invalid", "stale", "blocked"
            ) else "unknown"
        ),
        "sandbox_result": "unknown",
        "verifier_result": "unknown",
        "rollback_exists": bool(result.get("rollback_exists", False)),
        "secrets_scan": "unknown",
        "resource_status": "unknown",
        "non_delegable_flags": nd_flags,
        "reviewer_votes": [],
        "summary": str(result.get("reason") or ""),
        "evidence": [
            f"receipt_id={result.get('receipt_id', '')!r}",
            f"receipt_found={result.get('receipt_found', False)}",
            f"receipt_valid={receipt_valid}",
            f"enforcer_decision={decision}",
        ],
    }


def attach_decision_card_to_enforcement_result(
    project_dir: str,
    result: dict,
) -> dict:
    """Attach a Serge-readable decision card to an enforcement result.

    safe_to_execute_now stays False. advisory_only stays True. Returns the
    (mutated) result. Degrades gracefully if luna_serge_policy is unavailable.
    """
    if not isinstance(result, dict):
        return result
    sp = _serge_policy_module()
    if sp is None:
        result["decision_card"] = {
            "decision_card_status": "unavailable",
            "reason": "luna_serge_policy module not importable",
        }
        result["decision_card_recommendation"] = "UNAVAILABLE"
        result["plain_english_decision"] = (
            "Decision card module unavailable. Defer to existing enforcer result."
        )
        warns = list(result.get("warnings") or [])
        warns.append("decision_card_module_unavailable")
        result["warnings"] = warns
        return result

    try:
        ctx = build_decision_context_from_enforcement_result(result)
        ns = sp.load_north_star_policy(project_dir)
        pol = sp.load_standing_approval_policy(project_dir)
        card = sp.build_decision_card(ctx, policy=pol, north_star=ns)
    except Exception as e:
        result["decision_card"] = {
            "decision_card_status": "error",
            "error": f"{type(e).__name__}:{str(e)[:200]}",
        }
        result["decision_card_recommendation"] = "UNAVAILABLE"
        result["plain_english_decision"] = (
            "Decision card generation failed; defer to existing enforcer result."
        )
        warns = list(result.get("warnings") or [])
        warns.append(f"decision_card_error:{type(e).__name__}")
        result["warnings"] = warns
        return result

    card["safe_to_execute_now"] = False
    result["decision_card"] = card
    result["decision_card_recommendation"] = card.get("recommendation", "")
    result["plain_english_decision"] = card.get(
        "plain_english_final_recommendation", ""
    )
    result["safe_to_execute_now"] = False
    result["advisory_only"] = True
    return result


def build_guardian_approval_status(
    project_dir: str,
    pending_actions: list = None,
    policy: dict = None,
) -> dict:
    if policy is None:
        policy = load_enforcer_policy(project_dir)

    if pending_actions is None:
        preview_path = os.path.join(project_dir, "memory", "luna_approval_enforcement_preview.json")
        data = load_json(preview_path, {})
        pending_actions = data.get("pending_actions", []) if isinstance(data, dict) else []
    if not isinstance(pending_actions, list):
        pending_actions = []

    receipts = read_council_receipts(project_dir)
    max_age = policy.get("receipt_max_age_minutes", 120)
    now = datetime.now(timezone.utc)
    valid_count = 0
    expired_count = 0
    for r in receipts:
        created = _parse_iso(r.get("created_at", ""))
        if created is None:
            continue
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        age_min = (now - created).total_seconds() / 60
        if age_min > max_age:
            expired_count += 1
        elif r.get("decision", "") in ("approve", "approved"):
            valid_count += 1

    files_checked = [
        os.path.join(project_dir, "memory", "luna_ai_council_approvals.jsonl"),
        os.path.join(project_dir, "memory", "luna_approval_receipt_checks.jsonl"),
        os.path.join(project_dir, "memory", "luna_approval_enforcement_preview.json"),
    ]

    action_results = []
    missing_count = 0
    non_delegable_count = 0
    for action in pending_actions:
        res = evaluate_action_enforcement(project_dir, action, policy)
        action_results.append(res)
        if res["decision"] == "would_block" and "missing_receipt" in res.get("reason", ""):
            missing_count += 1
        if res.get("non_delegable_flags"):
            non_delegable_count += 1

    decisions = [r["decision"] for r in action_results]
    if any(d in ("would_block", "invalid", "needs_human") for d in decisions):
        overall = "blocked"
    elif "stale" in decisions:
        overall = "watch"
    elif action_results:
        overall = "healthy"
    else:
        overall = "unknown"

    if overall == "blocked":
        guardian_msg = "One or more pending actions are blocked or require human approval."
    elif overall == "watch":
        guardian_msg = "Some receipts are stale. Review before proceeding."
    elif overall == "healthy":
        guardian_msg = "All pending actions have valid advisory receipts. (advisory only — no execution)"
    else:
        guardian_msg = "No pending actions to evaluate. Advisory status available."

    recommended = (
        "Review blocked actions and obtain fresh council receipts."
        if overall == "blocked"
        else "No immediate action required."
    )

    # Phase 5U: aggregate decision-card recommendations across actions.
    card_summary = {
        "approve_recommended": 0,
        "wait_for_more_evidence": 0,
        "do_not_approve": 0,
        "serge_only": 0,
        "unavailable": 0,
    }
    for r in action_results:
        rec = str(r.get("decision_card_recommendation") or "").upper()
        if rec == "APPROVE_RECOMMENDED":
            card_summary["approve_recommended"] += 1
        elif rec == "WAIT_FOR_MORE_EVIDENCE":
            card_summary["wait_for_more_evidence"] += 1
        elif rec == "DO_NOT_APPROVE":
            card_summary["do_not_approve"] += 1
        elif rec == "SERGE_ONLY":
            card_summary["serge_only"] += 1
        else:
            card_summary["unavailable"] += 1

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "project_dir": project_dir,
        "advisory_only": True,
        "overall_status": overall,
        "pending_action_count": len(pending_actions),
        "receipt_count": len(receipts),
        "valid_receipt_count": valid_count,
        "expired_receipt_count": expired_count,
        "missing_receipt_count": missing_count,
        "non_delegable_count": non_delegable_count,
        "actions": action_results,
        "decision_card_summary": card_summary,
        "guardian_message": guardian_msg,
        "recommended_next_action": recommended,
        "files_checked": files_checked,
        "notes": [
            "Phase 5O: advisory only. safe_to_execute_now is always false.",
            "Phase 5U: actions include Serge-readable decision cards.",
        ],
    }


def render_enforcer_report_markdown(report: dict) -> str:
    lines = [
        "# Luna Council Enforcer Report",
        "",
        f"**Generated:** {report.get('generated_at', 'N/A')}",
        f"**Advisory only:** {report.get('advisory_only', True)}",
        f"**Overall status:** {report.get('overall_status', 'unknown')}",
        "",
        f"**Pending actions:** {report.get('pending_action_count', 0)}",
        f"**Receipt count:** {report.get('receipt_count', 0)}",
        f"**Valid receipts:** {report.get('valid_receipt_count', 0)}",
        f"**Expired receipts:** {report.get('expired_receipt_count', 0)}",
        f"**Missing receipts:** {report.get('missing_receipt_count', 0)}",
        f"**Non-delegable:** {report.get('non_delegable_count', 0)}",
        "",
        f"**Guardian message:** {report.get('guardian_message', '')}",
        f"**Recommended next action:** {report.get('recommended_next_action', '')}",
        "",
        "## Action Results",
        "",
    ]
    for action in report.get("actions", []):
        lines.append(f"### {action.get('action_id') or action.get('check_id', 'N/A')}")
        lines.append(f"- **Decision:** {action.get('decision', 'unknown')}")
        lines.append(f"- **Reason:** {action.get('reason', '')}")
        lines.append(f"- **Risk tier:** {action.get('risk_tier', '?')}")
        lines.append(f"- **Action type:** {action.get('action_type', '')}")
        lines.append(f"- **safe_to_execute_now:** {action.get('safe_to_execute_now', False)}")
        lines.append(f"- **advisory_only:** {action.get('advisory_only', True)}")
        if action.get("non_delegable_flags"):
            lines.append(f"- **Non-delegable flags:** {action['non_delegable_flags']}")
        lines.append("")
    lines += [
        "---",
        "*Phase 5O: advisory only. No actions are executed by this report.*",
    ]
    return "\n".join(lines)


def write_enforcer_report(project_dir: str, report: dict) -> None:
    json_path = os.path.join(project_dir, "memory", "luna_council_enforcer_report.json")
    md_path = os.path.join(project_dir, "memory", "luna_council_enforcer_report.md")
    audit_path = os.path.join(project_dir, "memory", "luna_council_enforcer_audit.jsonl")
    write_json_atomic(json_path, report)
    pathlib.Path(md_path).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(md_path).write_text(render_enforcer_report_markdown(report), encoding="utf-8")
    append_jsonl(audit_path, {
        "ts": report.get("generated_at", now_iso()),
        "event": "ENFORCER_REPORT_WRITTEN",
        "overall_status": report.get("overall_status"),
        "pending_count": report.get("pending_action_count", 0),
    })


def write_guardian_approval_status(project_dir: str, status: dict) -> None:
    out = status.get("status_output") or os.path.join(
        project_dir, "memory", "luna_guardian_approval_status.json"
    )
    if not os.path.isabs(str(out)):
        out = os.path.join(project_dir, str(out))
    write_json_atomic(str(out), status)


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------


def self_test() -> int:
    """Run internal self-test using a TemporaryDirectory. Returns 0 on pass."""
    with tempfile.TemporaryDirectory() as tmpdir:
        mem = os.path.join(tmpdir, "memory")
        os.makedirs(mem, exist_ok=True)
        write_json_atomic(os.path.join(mem, "luna_council_enforcer_policy.json"), DEFAULT_POLICY)

        policy = load_enforcer_policy(tmpdir)
        assert policy["advisory_only"] is True
        assert policy["safe_to_execute_now_always_false_in_phase5o"] is True

        cid = make_check_id("enf")
        assert cid.startswith("enf-"), f"bad check_id: {cid}"
        assert len(cid) > 10

        h1 = sha256_json({"b": 2, "a": 1})
        h2 = sha256_json({"a": 1, "b": 2})
        assert h1 == h2, "sha256_json key-order mismatch"

        nf = normalize_target_files(["b.py", "a.py", "b.py"])
        assert len(nf) == 2
        assert nf == sorted(nf)

        assert target_files_hash(["b.py", "a.py"]) == target_files_hash(["a.py", "b.py"])

        def _act(at, tier, targets=None, **kw):
            return {
                "schema_version": 1, "action_id": make_check_id("act"),
                "created_at": now_iso(), "source": "test", "goal": "test",
                "task_id": kw.get("task_id", ""), "action_type": at,
                "risk_tier": tier, "target_files": targets or [],
                "diff_hash": kw.get("diff_hash", ""), "planned_commands": [],
                "receipt_id": "", "approval_id": "",
                "packet_hash": kw.get("packet_hash", ""), "nonce": kw.get("nonce", ""),
                "metadata": {},
            }

        r = evaluate_action_enforcement(tmpdir, _act("read_only", 0), policy)
        assert r["decision"] == "not_required", r["decision"]
        assert r["safe_to_execute_now"] is False

        r = evaluate_action_enforcement(tmpdir, _act("generated_artifact", 1), policy)
        assert r["decision"] == "not_required"

        r = evaluate_action_enforcement(tmpdir, _act("low_risk_additive", 2), policy)
        assert r["decision"] == "would_block"
        assert r["safe_to_execute_now"] is False

        rcpt_path = os.path.join(mem, "luna_ai_council_approvals.jsonl")
        rcpt = {
            "schema_version": 1, "receipt_id": "rcpt-selftest",
            "approval_id": "appr-selftest", "created_at": now_iso(),
            "decision": "approve", "task_id": "task-st",
            "action_type": "low_risk_additive", "target_files": ["luna_modules/x.py"],
        }
        append_jsonl(rcpt_path, rcpt)

        r = evaluate_action_enforcement(
            tmpdir, _act("low_risk_additive", 2, ["luna_modules/x.py"], task_id="task-st"), policy
        )
        assert r["decision"] == "would_allow", r["decision"]
        assert r["safe_to_execute_now"] is False

        r = evaluate_action_enforcement(tmpdir, _act("medium_code_edit", 3), policy)
        assert r["decision"] == "would_block"

        r = evaluate_action_enforcement(tmpdir, _act("delete_memory", 5), policy)
        assert r["decision"] == "needs_human"
        assert r["safe_to_execute_now"] is False

        status = build_guardian_approval_status(tmpdir, pending_actions=[], policy=policy)
        assert status["advisory_only"] is True
        assert status["schema_version"] == SCHEMA_VERSION

        print("[LUNA SELF-TEST] All checks passed.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _synthetic_action(tier: int, action: str, target: str) -> dict:
    return {
        "schema_version": 1,
        "action_id": make_check_id("sim"),
        "created_at": now_iso(),
        "source": "test",
        "goal": f"simulate tier={tier} action={action}",
        "task_id": make_check_id("task"),
        "action_type": action,
        "risk_tier": tier,
        "target_files": [target] if target else [],
        "diff_hash": "",
        "planned_commands": [],
        "receipt_id": "",
        "approval_id": "",
        "packet_hash": "",
        "nonce": "",
        "metadata": {},
    }


def _non_delegable_action() -> dict:
    return {
        "schema_version": 1,
        "action_id": make_check_id("nd"),
        "created_at": now_iso(),
        "source": "test",
        "goal": "delete memory",
        "task_id": make_check_id("task"),
        "action_type": "delete_memory",
        "risk_tier": 5,
        "target_files": [],
        "diff_hash": "",
        "planned_commands": [],
        "receipt_id": "",
        "approval_id": "",
        "packet_hash": "",
        "nonce": "",
        "metadata": {},
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Luna Council Enforcer — Phase 5O advisory only"
    )
    parser.add_argument("--self-test", action="store_true", dest="self_test")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--write-status", action="store_true", dest="write_status")
    parser.add_argument("--simulate", action="store_true")
    parser.add_argument("--tier", type=int, default=1)
    parser.add_argument("--action", type=str, default="generated_artifact")
    parser.add_argument("--target", type=str, default="")
    parser.add_argument("--non-delegable", action="store_true", dest="non_delegable")
    parser.add_argument("--print-markdown", action="store_true", dest="print_markdown")
    parser.add_argument("--project-dir", type=str, default=PROJECT_DIR_DEFAULT, dest="project_dir")
    args = parser.parse_args()

    project_dir = args.project_dir
    policy = load_enforcer_policy(project_dir)

    if args.self_test:
        return self_test()

    if args.simulate:
        action = _non_delegable_action() if args.non_delegable else _synthetic_action(
            args.tier, args.action, args.target
        )
        result = evaluate_action_enforcement(project_dir, action, policy)
        print(json.dumps(result, indent=2))
        return 0

    if args.status or args.write_status or args.print_markdown:
        status = build_guardian_approval_status(project_dir, policy=policy)
        write_enforcer_report(project_dir, status)
        if args.write_status:
            write_guardian_approval_status(project_dir, status)
            print("[ENFORCER] Status written -> memory/luna_guardian_approval_status.json")
        if args.print_markdown:
            print(render_enforcer_report_markdown(status))
        else:
            print(json.dumps(status, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
