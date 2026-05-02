"""Phase 5T: Luna Serge Standing Approval Policy + Decision Cards.

Stdlib only. Policy/advisory/roadmap layer that encodes Serge's north star
("Make Luna into a Super AI as fast as safely possible") and his standing
approval intent (YES when green, NO/WAIT when red or unclear). Produces
plain-English decision cards so Serge approves direction, not source code.

Hard rules in Phase 5T:
  * safe_to_execute_now is ALWAYS False on every card.
  * serge_should_need_to_review_code is ALWAYS False.
  * Wipe-computer / delete-memory / non-delegable actions are NEVER
    APPROVE_RECOMMENDED — they are SERGE_ONLY or DO_NOT_APPROVE.
  * No external API calls. No package installs. No Aider invocations.
  * No execution; this module only produces advisory recommendations.

Tracked schema/policy/roadmap files:
  memory/luna_super_ai_north_star.json
  memory/luna_serge_standing_approval_policy.json
  memory/luna_decision_card.schema.json
  memory/luna_aider_tutor_mode_roadmap.json

Generated runtime artifacts (gitignored):
  memory/luna_decision_card_preview.json
  memory/luna_decision_card_preview.md
  memory/luna_serge_policy_report.json
  memory/luna_serge_policy_report.md

CLI:
  python -m luna_modules.luna_serge_policy --self-test
  python -m luna_modules.luna_serge_policy --sample-green-card
  python -m luna_modules.luna_serge_policy --sample-yellow-card
  python -m luna_modules.luna_serge_policy --sample-red-card
  python -m luna_modules.luna_serge_policy --sample-wipe-computer-card
  python -m luna_modules.luna_serge_policy --print-policy
  python -m luna_modules.luna_serge_policy --print-aider-tutor-roadmap
  python -m luna_modules.luna_serge_policy --write-sample-card
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

_THIS_FILE = Path(__file__).resolve()
_PROJECT_DIR_DEFAULT = _THIS_FILE.parent.parent

# Recommendation constants.
APPROVE_RECOMMENDED = "APPROVE_RECOMMENDED"
WAIT_FOR_MORE_EVIDENCE = "WAIT_FOR_MORE_EVIDENCE"
DO_NOT_APPROVE = "DO_NOT_APPROVE"
SERGE_ONLY = "SERGE_ONLY"

# Destructive intent keywords — used to detect dangerous goal/summary text.
_DESTRUCTIVE_KEYWORDS = (
    "wipe computer", "wipe drive", "format drive", "format c:",
    "rm -rf", "rmdir /s",
    "delete memory", "delete all memory", "wipe memory",
    "delete logs", "wipe logs", "delete backup", "wipe backup",
    "delete queue", "wipe queue", "delete task", "delete solution",
    "delete upload", "wipe upload",
    "git reset --hard", "git clean -fd", "git push --force", "git push -f",
    "expose secret", "leak api key", "post token",
    "disable verifier", "weaken verifier", "comment out fail",
    "replace architecture", "rewrite from scratch",
    "change identity", "change personality", "change goals",
    "edit policy to approve itself",
)

_SUPER_AI_KEYWORDS = (
    "luna", "super ai", "super-ai", "self-heal", "self heal",
    "memory recall", "sandbox", "verifier", "rollback",
    "approval", "council", "guardian", "scorecard",
    "playbook", "change ledger", "file map", "task graph",
    "snapshot", "decision card",
)

_DEFAULT_NORTH_STAR: dict[str, Any] = {
    "schema_version": 1,
    "north_star": "Make Luna into a Super AI as fast as safely possible.",
    "definition": {
        "super_ai_means": [],
        "super_ai_does_not_mean": [
            "destructive actions",
            "wiping Serge's computer",
            "leaking secrets",
        ],
    },
    "standing_goal_alignment": {
        "primary_goal": "Make Luna stronger safely every day.",
        "default_bias": "progress_when_safe",
    },
}

_DEFAULT_STANDING_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "serge_is_not_expected_to_review_code_line_by_line": True,
    "standing_yes_when_green": True,
    "standing_no_when_red_or_unclear": True,
    "eligible_delegated_tiers_future": [0, 1, 2, 3],
    "high_risk_core_requires_strict_review": True,
    "non_delegable_always_waits_for_serge": True,
    "plain_english_decision_cards_required": True,
    "default_approve_requires": [],
    "default_wait_or_deny_if": [],
    "non_delegable_actions": [
        "delete_memory", "delete_logs", "delete_queues", "delete_backups",
        "delete_uploads", "delete_tasks", "delete_solutions",
        "wipe_computer", "format_drive", "change_identity",
        "change_personality", "change_goals", "expose_secret",
        "package_install_without_policy", "external_network_without_policy",
        "git_push_force", "git_reset_hard", "git_clean_fd",
        "disable_verifier", "weaken_quorum_policy",
        "replace_architecture", "edit_policy_to_approve_itself",
    ],
}

_DEFAULT_AIDER_TUTOR_ROADMAP: dict[str, Any] = {
    "schema_version": 1,
    "status": "planned_after_safety_chain",
    "title": "Aider Tutor Mode / Luna Coding School",
    "core_rule": "Aider teaches. Luna learns. Luna does not let Aider blindly edit her.",
    "purpose": "Use Aider as a coding teacher, reviewer, and example generator after safety gates are complete.",
    "allowed_future_behavior": [],
    "forbidden_behavior": [],
    "future_outputs": [],
}


# ---------- pure helpers ----------


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_card_id(prefix: str = "card") -> str:
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


# ---------- policy loaders ----------


def load_north_star_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_super_ai_north_star.json"
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_NORTH_STAR)
        out["_source"] = "module_fallback"
        return out
    out = dict(raw)
    out["_source"] = str(p)
    return out


def load_standing_approval_policy(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_serge_standing_approval_policy.json"
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_STANDING_POLICY)
        out["_source"] = "module_fallback"
        return out
    out = dict(raw)
    # Hard rules always enforced.
    out["serge_is_not_expected_to_review_code_line_by_line"] = True
    out["non_delegable_always_waits_for_serge"] = True
    out["_source"] = str(p)
    return out


def load_aider_tutor_roadmap(project_dir: Path | str | None = None) -> dict[str, Any]:
    pdir = Path(project_dir) if project_dir else _PROJECT_DIR_DEFAULT
    p = pdir / "memory" / "luna_aider_tutor_mode_roadmap.json"
    raw = load_json(p, default=None)
    if not isinstance(raw, dict):
        out = dict(_DEFAULT_AIDER_TUTOR_ROADMAP)
        out["_source"] = "module_fallback"
        return out
    out = dict(raw)
    # Hard rule: Aider remains a tutor, never an executor.
    out["status"] = raw.get("status", "planned_after_safety_chain")
    if "core_rule" not in out:
        out["core_rule"] = "Aider teaches. Luna learns. Luna does not let Aider blindly edit her."
    out["_source"] = str(p)
    return out


# ---------- text helpers ----------


def normalize_action_text(text: Any) -> str:
    if text is None:
        return ""
    return " ".join(str(text).lower().split())


# ---------- intent / alignment detection ----------


def detect_destructive_intent(
    goal: str = "",
    action_type: str = "",
    target_files: list[str] | None = None,
    summary: str = "",
) -> dict[str, Any]:
    """Detect destructive intent in a proposed action. Returns flags and reasons."""
    blob = " ".join([
        normalize_action_text(goal),
        normalize_action_text(action_type),
        normalize_action_text(summary),
        " ".join(normalize_action_text(t) for t in (target_files or [])),
    ])

    flags: list[str] = []
    reasons: list[str] = []

    for kw in _DESTRUCTIVE_KEYWORDS:
        if kw in blob:
            flags.append(kw.replace(" ", "_"))
            reasons.append(f"destructive_keyword_detected: {kw!r}")

    # Action-type level detection.
    action_lower = normalize_action_text(action_type)
    destructive_action_types = (
        "delete_memory", "delete_logs", "delete_queues", "delete_backups",
        "delete_uploads", "delete_tasks", "delete_solutions",
        "wipe_computer", "format_drive", "expose_secret",
        "git_reset_hard", "git_clean_fd", "git_push_force",
        "disable_verifier", "weaken_quorum_policy", "replace_architecture",
        "change_identity", "change_personality", "change_goals",
        "edit_policy_to_approve_itself",
    )
    for da in destructive_action_types:
        if da == action_lower or da.replace("_", " ") in blob:
            flags.append(da)
            reasons.append(f"destructive_action_type: {da!r}")

    return {
        "destructive": len(flags) > 0,
        "flags": sorted(set(flags)),
        "reasons": reasons,
    }


def detect_goal_alignment(
    goal: str = "",
    summary: str = "",
    north_star: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Detect whether a goal is aligned with the Super-AI north star.

    Returns: {"alignment": "aligned|watch|misaligned|unknown", "reasons": [...]}
    """
    blob = " ".join([
        normalize_action_text(goal),
        normalize_action_text(summary),
    ])

    if not blob.strip():
        return {"alignment": "unknown", "reasons": ["empty_goal_and_summary"]}

    # Destructive override — destructive intent is misaligned regardless.
    destruct = detect_destructive_intent(goal=goal, summary=summary)
    if destruct["destructive"]:
        return {
            "alignment": "misaligned",
            "reasons": ["destructive_intent_detected"] + destruct["reasons"][:3],
        }

    # Check super-AI keyword presence.
    matched = [kw for kw in _SUPER_AI_KEYWORDS if kw in blob]
    if matched:
        return {
            "alignment": "aligned",
            "reasons": [f"super_ai_keyword_match: {matched[:3]!r}"],
        }

    # Check north_star "does not mean" list — if any are present, watch/misaligned.
    if isinstance(north_star, dict):
        bad_list = (
            north_star.get("definition", {}) or {}
        ).get("super_ai_does_not_mean", []) or []
        for bad in bad_list:
            if bad and normalize_action_text(bad) in blob:
                return {
                    "alignment": "misaligned",
                    "reasons": [f"matches_super_ai_does_not_mean:{bad!r}"],
                }

    return {
        "alignment": "watch",
        "reasons": ["no_super_ai_keyword_match_but_not_destructive"],
    }


# ---------- standing intent classifier ----------


def _is_non_delegable_action(action_type: str, policy: dict[str, Any]) -> bool:
    nd_list = policy.get("non_delegable_actions", []) or []
    a = normalize_action_text(action_type).replace(" ", "_")
    return a in nd_list


def classify_serge_standing_intent(
    decision_context: dict[str, Any],
    policy: dict[str, Any] | None = None,
    north_star: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify Serge's standing intent for a proposed action.

    Returns dict with recommendation, risk_level, green/yellow/red checks,
    non_delegable_flags, goal_alignment, plain_english_summary.
    """
    pol = policy or _DEFAULT_STANDING_POLICY
    ns = north_star or _DEFAULT_NORTH_STAR

    goal = str(decision_context.get("goal", "") or "")
    action_type = str(decision_context.get("action_type", "") or "")
    risk_tier = int(decision_context.get("risk_tier", 0) or 0)
    target_files = list(decision_context.get("target_files") or [])
    summary = str(decision_context.get("summary", "") or "")
    router = str(decision_context.get("router_decision", "unknown") or "unknown")
    council = str(decision_context.get("council_decision", "unknown") or "unknown")
    enforcer = str(decision_context.get("enforcer_decision", "unknown") or "unknown")
    sandbox = str(decision_context.get("sandbox_result", "unknown") or "unknown")
    verifier = str(decision_context.get("verifier_result", "unknown") or "unknown")
    rollback_exists = bool(decision_context.get("rollback_exists", False))
    secrets_scan = str(decision_context.get("secrets_scan", "unknown") or "unknown")
    resource_status = str(decision_context.get("resource_status", "unknown") or "unknown")
    nd_flags_input = list(decision_context.get("non_delegable_flags") or [])
    reviewer_votes = list(decision_context.get("reviewer_votes") or [])

    green: list[str] = []
    yellow: list[str] = []
    red: list[str] = []
    non_delegable_flags: list[str] = list(nd_flags_input)

    # Destructive intent + non-delegable detection.
    destruct = detect_destructive_intent(
        goal=goal, action_type=action_type,
        target_files=target_files, summary=summary,
    )
    if destruct["destructive"]:
        non_delegable_flags.extend(destruct["flags"])
        red.append(f"destructive_intent: {destruct['flags'][:3]!r}")

    if _is_non_delegable_action(action_type, pol):
        non_delegable_flags.append(action_type)
        red.append(f"non_delegable_action_type: {action_type!r}")

    # Goal alignment.
    align = detect_goal_alignment(goal=goal, summary=summary, north_star=ns)
    goal_alignment = align["alignment"]
    if goal_alignment == "aligned":
        green.append("goal_aligned_with_super_ai_north_star")
    elif goal_alignment == "watch":
        yellow.append("goal_alignment_watch")
    elif goal_alignment == "misaligned":
        red.append("goal_misaligned_with_north_star")
    else:
        yellow.append("goal_alignment_unknown")

    # Verifier.
    if verifier == "pass":
        green.append("verifier_passed")
    elif verifier == "fail":
        red.append("verifier_failed")
    else:
        yellow.append("verifier_unknown")

    # Sandbox.
    if sandbox == "pass":
        green.append("sandbox_passed")
    elif sandbox == "fail":
        red.append("sandbox_failed")
    elif sandbox == "not_required":
        green.append("sandbox_not_required")
    else:
        yellow.append("sandbox_unknown")

    # Secrets scan.
    if secrets_scan == "pass":
        green.append("no_secret_exposure")
    elif secrets_scan == "fail":
        red.append("secret_detected")
    else:
        yellow.append("secrets_scan_unknown")

    # Resource status.
    if resource_status == "normal":
        green.append("resource_status_normal")
    elif resource_status in ("light", "pause_high_intensity"):
        yellow.append(f"resource_status_{resource_status}")
    elif resource_status in ("hibernate", "blocked"):
        red.append(f"resource_status_{resource_status}")
    else:
        yellow.append("resource_status_unknown")

    # Rollback.
    if rollback_exists:
        green.append("rollback_exists")
    else:
        # Rollback is required for tier 2+; for lower tiers it's a yellow.
        if risk_tier >= 2:
            red.append("rollback_missing")
        else:
            yellow.append("rollback_missing_low_risk_tier")

    # Router/council/enforcer decisions.
    if router in ("approved", "not_required"):
        green.append(f"router_{router}")
    elif router in ("denied", "blocked"):
        red.append(f"router_{router}")
    elif router in ("needs_human",):
        red.append("router_needs_human")
    elif router in ("stale",):
        red.append("router_stale_receipt")
    else:
        yellow.append(f"router_{router}")

    if council in ("approve",):
        green.append("council_approve")
    elif council in ("deny",):
        red.append("council_deny")
    elif council in ("needs_human",):
        yellow.append("council_needs_human")
    else:
        yellow.append(f"council_{council}")

    if enforcer in ("would_allow", "not_required"):
        green.append(f"enforcer_{enforcer}")
    elif enforcer in ("would_block", "invalid"):
        red.append(f"enforcer_{enforcer}")
    elif enforcer in ("needs_human",):
        red.append("enforcer_needs_human")
    elif enforcer in ("stale",):
        red.append("enforcer_stale")
    else:
        yellow.append(f"enforcer_{enforcer}")

    # Reviewer votes.
    deny_votes = [v for v in reviewer_votes if str(v).lower() in ("deny", "no", "block")]
    if deny_votes:
        red.append(f"reviewer_denial:{len(deny_votes)}")

    # ----- Determine recommendation -----
    risk_level: str
    if non_delegable_flags or red:
        # Non-delegable and destructive go to SERGE_ONLY; other reds are DO_NOT_APPROVE.
        if non_delegable_flags:
            recommendation = SERGE_ONLY
            risk_level = "critical"
            reason = f"non_delegable_or_destructive: {non_delegable_flags[:3]!r}"
        else:
            recommendation = DO_NOT_APPROVE
            risk_level = "high"
            reason = f"red_checks: {red[:3]!r}"
    elif yellow:
        recommendation = WAIT_FOR_MORE_EVIDENCE
        risk_level = "medium"
        reason = f"yellow_checks: {yellow[:3]!r}"
    elif green:
        recommendation = APPROVE_RECOMMENDED
        risk_level = "low" if risk_tier <= 2 else "medium"
        reason = "all_green_checks_passed"
    else:
        recommendation = WAIT_FOR_MORE_EVIDENCE
        risk_level = "unknown"
        reason = "no_evidence_provided"

    plain_english = _plain_english_summary(
        recommendation, goal_alignment, non_delegable_flags, red, yellow, green, risk_tier
    )

    return {
        "recommendation": recommendation,
        "risk_level": risk_level,
        "standing_policy_applies": True,
        "reason": reason,
        "green_checks": green,
        "yellow_checks": yellow,
        "red_checks": red,
        "non_delegable_flags": sorted(set(non_delegable_flags)),
        "goal_alignment": goal_alignment,
        "serge_should_need_to_review_code": False,
        "plain_english_summary": plain_english,
    }


def _plain_english_summary(
    recommendation: str,
    goal_alignment: str,
    non_delegable_flags: list[str],
    red: list[str],
    yellow: list[str],
    green: list[str],
    risk_tier: int,
) -> str:
    if recommendation == SERGE_ONLY:
        if non_delegable_flags:
            return (
                "Serge must decide this one personally. The action is "
                "non-delegable or destructive (for example: deleting memory, "
                "wiping the computer, exposing secrets, or weakening safety "
                "gates). No automated approval applies."
            )
        return (
            "Serge must decide this one personally. The signals are too risky "
            "for delegated approval."
        )
    if recommendation == DO_NOT_APPROVE:
        return (
            "Do not approve right now. Something failed: "
            f"{(red[:3] or ['unknown failure'])!r}. Fix the failure and re-run "
            "the safety checks before asking again."
        )
    if recommendation == WAIT_FOR_MORE_EVIDENCE:
        return (
            "Wait — not enough evidence yet. Missing or unknown signals: "
            f"{(yellow[:3] or ['evidence missing'])!r}. Gather sandbox + "
            "verifier + receipt evidence and re-evaluate."
        )
    if recommendation == APPROVE_RECOMMENDED:
        align_label = goal_alignment if goal_alignment != "aligned" else "north-star aligned"
        return (
            f"Safe to approve direction. Goal is {align_label}, all required "
            f"safety checks passed ({len(green)} green checks, risk tier {risk_tier}). "
            "Serge can approve direction without reading source code. "
            "Note: safe_to_execute_now is still False until live wiring is enabled."
        )
    return "Recommendation unclear — defer to Serge."


# ---------- decision card ----------


def build_decision_card(
    decision_context: dict[str, Any],
    policy: dict[str, Any] | None = None,
    north_star: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a plain-English decision card for Serge.

    safe_to_execute_now is ALWAYS False in Phase 5T.
    """
    pol = policy or _DEFAULT_STANDING_POLICY
    ns = north_star or _DEFAULT_NORTH_STAR

    classification = classify_serge_standing_intent(decision_context, pol, ns)

    goal = str(decision_context.get("goal", "") or "")
    summary = str(decision_context.get("summary", "") or "")
    action_type = str(decision_context.get("action_type", "") or "")
    target_files = list(decision_context.get("target_files") or [])
    risk_tier = int(decision_context.get("risk_tier", 0) or 0)
    sandbox_result = str(decision_context.get("sandbox_result", "unknown") or "unknown")
    verifier_result = str(decision_context.get("verifier_result", "unknown") or "unknown")
    rollback_exists = bool(decision_context.get("rollback_exists", False))
    reviewer_votes = list(decision_context.get("reviewer_votes") or [])

    rec = classification["recommendation"]

    # Build "what Luna wants to do" text.
    what = goal or summary or f"Action of type {action_type!r}"
    if target_files:
        what += f" (targets: {', '.join(target_files[:3])}"
        if len(target_files) > 3:
            what += f" +{len(target_files) - 3} more"
        what += ")"

    # Build "why this supports Super-AI goal" text.
    if classification["goal_alignment"] == "aligned":
        why_super = (
            "This action moves Luna toward the north star: "
            "self-awareness, safe self-upgrade, and reversible progress."
        )
    elif classification["goal_alignment"] == "watch":
        why_super = (
            "This action does not obviously align with the Super-AI north star. "
            "Confirm goal before approving."
        )
    elif classification["goal_alignment"] == "misaligned":
        why_super = (
            "This action does NOT align with the Super-AI north star. It looks "
            "destructive or off-mission and must not be approved."
        )
    else:
        why_super = "Goal alignment unknown — needs Serge clarification."

    # Build "why safe / not safe" text.
    if rec == APPROVE_RECOMMENDED:
        why_safe = (
            f"All required safety checks passed: {len(classification['green_checks'])} green, "
            f"{len(classification['yellow_checks'])} yellow, "
            f"{len(classification['red_checks'])} red. "
            "Sandbox/verifier/rollback signals are clean."
        )
    elif rec == WAIT_FOR_MORE_EVIDENCE:
        why_safe = (
            "Some signals are unknown or missing. The hierarchy needs more "
            "evidence (verifier results, sandbox results, receipts) before "
            "Serge can give a standing yes."
        )
    elif rec == DO_NOT_APPROVE:
        why_safe = (
            "One or more safety signals failed. Do not approve until the failures "
            "are fixed and re-verified."
        )
    else:  # SERGE_ONLY
        why_safe = (
            "This action is non-delegable or destructive. Even with all green "
            "signals, only Serge can approve it. The hierarchy must wait."
        )

    # Could go wrong list.
    could_go_wrong: list[str] = []
    if classification["non_delegable_flags"]:
        could_go_wrong.append(
            f"Non-delegable risk: {classification['non_delegable_flags'][:3]!r}"
        )
    if classification["red_checks"]:
        could_go_wrong.append(f"Red signals: {classification['red_checks'][:3]!r}")
    if classification["yellow_checks"]:
        could_go_wrong.append(f"Unknown signals: {classification['yellow_checks'][:3]!r}")
    if not could_go_wrong:
        could_go_wrong.append("No specific risks detected at this evidence level.")

    # Undo plan.
    if rollback_exists:
        undo_plan = "Rollback available — restore from snapshot/backup if needed."
    else:
        undo_plan = (
            "No rollback registered. The action will not be approved without a "
            "rollback path."
        )

    # Reviewer summary.
    reviewer_summary: list[str] = []
    if reviewer_votes:
        for v in reviewer_votes[:5]:
            reviewer_summary.append(f"reviewer: {v}")
    else:
        reviewer_summary.append("no reviewers recorded")

    # Serge action needed.
    if rec == APPROVE_RECOMMENDED:
        serge_action = "approve_direction"
    elif rec == WAIT_FOR_MORE_EVIDENCE:
        serge_action = "review_morning_report"
    elif rec == SERGE_ONLY:
        serge_action = "explicit_manual_approval_required"
    else:  # DO_NOT_APPROVE
        serge_action = "review_morning_report"

    notes: list[str] = [
        "advisory_only: Phase 5T policy layer; no execution enabled.",
        "safe_to_execute_now is False; future live wiring requires explicit phase.",
    ]

    # Non-delegable check label.
    if classification["non_delegable_flags"]:
        nd_check = (
            f"NON-DELEGABLE — flags: {classification['non_delegable_flags'][:3]!r}"
        )
    else:
        nd_check = "delegable_within_policy"

    return {
        "schema_version": SCHEMA_VERSION,
        "card_id": make_card_id(),
        "created_at": now_iso(),
        "goal": goal,
        "recommendation": rec,
        "risk_level": classification["risk_level"],
        "goal_alignment": classification["goal_alignment"],
        "what_luna_wants_to_do": what,
        "why_this_supports_super_ai_goal": why_super,
        "why_it_is_or_is_not_safe": why_safe,
        "reviewer_summary": reviewer_summary,
        "sandbox_result": sandbox_result,
        "verifier_result": verifier_result,
        "rollback_path_or_status": "available" if rollback_exists else "missing",
        "non_delegable_check": nd_check,
        "could_go_wrong": could_go_wrong,
        "undo_plan": undo_plan,
        "plain_english_final_recommendation": classification["plain_english_summary"],
        "serge_action_needed": serge_action,
        "safe_to_execute_now": False,
        "serge_should_need_to_review_code": False,
        "green_checks": classification["green_checks"],
        "yellow_checks": classification["yellow_checks"],
        "red_checks": classification["red_checks"],
        "notes": notes,
    }


def validate_decision_card(card: Any) -> tuple[bool, list[str]]:
    """Validate a decision card. Returns (ok, errors)."""
    errs: list[str] = []
    if not isinstance(card, dict):
        return False, ["card is not a dict"]

    required = (
        "schema_version", "card_id", "created_at", "goal",
        "recommendation", "risk_level", "goal_alignment",
        "what_luna_wants_to_do", "plain_english_final_recommendation",
        "serge_action_needed", "safe_to_execute_now",
    )
    for f in required:
        if f not in card:
            errs.append(f"missing_required_field: {f!r}")

    if card.get("schema_version") != 1:
        errs.append(f"invalid_schema_version: {card.get('schema_version')!r}")

    if card.get("safe_to_execute_now") is not False:
        errs.append("safe_to_execute_now must be False in Phase 5T")

    valid_recs = (APPROVE_RECOMMENDED, WAIT_FOR_MORE_EVIDENCE, DO_NOT_APPROVE, SERGE_ONLY)
    if card.get("recommendation") not in valid_recs:
        errs.append(f"invalid_recommendation: {card.get('recommendation')!r}")

    valid_risks = ("low", "medium", "high", "critical", "unknown")
    if card.get("risk_level") not in valid_risks:
        errs.append(f"invalid_risk_level: {card.get('risk_level')!r}")

    return len(errs) == 0, errs


# ---------- markdown rendering ----------


def render_decision_card_markdown(card: dict[str, Any]) -> str:
    lines = [
        "# Luna Decision Card",
        "",
        f"**card_id**: {card.get('card_id', '')}",
        f"**created_at**: {card.get('created_at', '')}",
        f"**goal**: {card.get('goal', '')}",
        "",
        f"## Recommendation: {card.get('recommendation', '')}",
        "",
        f"- **risk_level**: {card.get('risk_level', '')}",
        f"- **goal_alignment**: {card.get('goal_alignment', '')}",
        f"- **safe_to_execute_now**: {card.get('safe_to_execute_now', False)}",
        f"- **serge_action_needed**: {card.get('serge_action_needed', '')}",
        f"- **non_delegable_check**: {card.get('non_delegable_check', '')}",
        "",
        "## What Luna Wants To Do",
        card.get("what_luna_wants_to_do", ""),
        "",
        "## Why This Supports the Super-AI Goal",
        card.get("why_this_supports_super_ai_goal", ""),
        "",
        "## Why It Is (Or Is Not) Safe",
        card.get("why_it_is_or_is_not_safe", ""),
        "",
        "## Safety Signals",
        f"- sandbox_result: {card.get('sandbox_result', '')}",
        f"- verifier_result: {card.get('verifier_result', '')}",
        f"- rollback: {card.get('rollback_path_or_status', '')}",
    ]

    cgw = card.get("could_go_wrong") or []
    if cgw:
        lines += ["", "## What Could Go Wrong"]
        for x in cgw:
            lines.append(f"- {x}")

    lines += [
        "",
        "## Undo Plan",
        card.get("undo_plan", ""),
        "",
        "## Plain-English Final Recommendation",
        card.get("plain_english_final_recommendation", ""),
        "",
    ]

    notes = card.get("notes") or []
    if notes:
        lines.append("## Notes")
        for n in notes:
            lines.append(f"- {n}")
        lines.append("")

    return "\n".join(lines)


def write_decision_card(project_dir: Path | str, card: dict[str, Any]) -> dict[str, str]:
    """Write decision card preview artifacts under memory/. Returns paths dict."""
    pdir = Path(project_dir).resolve()
    mem = pdir / "memory"
    mem.mkdir(parents=True, exist_ok=True)

    json_p = mem / "luna_decision_card_preview.json"
    md_p = mem / "luna_decision_card_preview.md"

    write_json_atomic(json_p, card)
    tmp = md_p.with_suffix(md_p.suffix + ".tmp")
    tmp.write_text(render_decision_card_markdown(card), encoding="utf-8")
    os.replace(tmp, md_p)

    return {"json": str(json_p), "md": str(md_p)}


# ---------- sample contexts ----------


def _sample_green_context() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "goal": "Add a new sandbox-only test for Luna decision card rendering.",
        "action_type": "low_risk_additive",
        "risk_tier": 1,
        "target_files": ["tests/test_luna_serge_policy.py"],
        "router_decision": "approved",
        "council_decision": "approve",
        "enforcer_decision": "would_allow",
        "sandbox_result": "pass",
        "verifier_result": "pass",
        "rollback_exists": True,
        "secrets_scan": "pass",
        "resource_status": "normal",
        "non_delegable_flags": [],
        "reviewer_votes": ["approve", "approve"],
        "summary": "Add a unit test that helps Luna self-verify her decision-card layer.",
        "evidence": ["sandbox_report_id=exec_abc", "verifier=passed"],
    }


def _sample_yellow_context() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "goal": "Improve Luna's playbook matcher.",
        "action_type": "medium_code_edit",
        "risk_tier": 3,
        "target_files": ["luna_modules/luna_self_healing_playbooks.py"],
        "router_decision": "unknown",
        "council_decision": "unknown",
        "enforcer_decision": "unknown",
        "sandbox_result": "unknown",
        "verifier_result": "unknown",
        "rollback_exists": True,
        "secrets_scan": "pass",
        "resource_status": "light",
        "non_delegable_flags": [],
        "reviewer_votes": [],
        "summary": "Make Luna better at recognizing repeated failure patterns.",
        "evidence": [],
    }


def _sample_red_context() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "goal": "Patch worker.py to enable a new feature.",
        "action_type": "high_risk_core_edit",
        "risk_tier": 4,
        "target_files": ["worker.py"],
        "router_decision": "denied",
        "council_decision": "deny",
        "enforcer_decision": "would_block",
        "sandbox_result": "fail",
        "verifier_result": "fail",
        "rollback_exists": False,
        "secrets_scan": "pass",
        "resource_status": "blocked",
        "non_delegable_flags": [],
        "reviewer_votes": ["deny", "deny"],
        "summary": "Patch core worker logic — sandbox simulation failed.",
        "evidence": [],
    }


def _sample_wipe_computer_context() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "goal": "Wipe computer to free disk space.",
        "action_type": "wipe_computer",
        "risk_tier": 5,
        "target_files": [],
        "router_decision": "blocked",
        "council_decision": "deny",
        "enforcer_decision": "would_block",
        "sandbox_result": "not_required",
        "verifier_result": "unknown",
        "rollback_exists": False,
        "secrets_scan": "unknown",
        "resource_status": "normal",
        "non_delegable_flags": ["wipe_computer"],
        "reviewer_votes": ["deny", "deny", "deny"],
        "summary": "Format C: drive and delete all memory and logs.",
        "evidence": [],
    }


# ---------- self-test ----------


def self_test() -> int:
    """Run self-test using a temporary project directory. Returns 0 on success."""
    import tempfile

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        (td / "memory").mkdir(parents=True, exist_ok=True)

        # Test green sample.
        green_card = build_decision_card(_sample_green_context())
        ok, errs = validate_decision_card(green_card)
        assert ok, f"green card invalid: {errs}"
        assert green_card["recommendation"] == APPROVE_RECOMMENDED, \
            f"green card should APPROVE_RECOMMENDED, got {green_card['recommendation']}"
        assert green_card["safe_to_execute_now"] is False
        assert green_card["serge_should_need_to_review_code"] is False

        # Test wipe-computer sample.
        wipe_card = build_decision_card(_sample_wipe_computer_context())
        assert wipe_card["recommendation"] in (SERGE_ONLY, DO_NOT_APPROVE), \
            f"wipe card must NOT be APPROVE; got {wipe_card['recommendation']}"
        assert wipe_card["safe_to_execute_now"] is False

        # Test red sample.
        red_card = build_decision_card(_sample_red_context())
        assert red_card["recommendation"] in (DO_NOT_APPROVE, SERGE_ONLY)
        assert red_card["safe_to_execute_now"] is False

        # Test yellow sample.
        yellow_card = build_decision_card(_sample_yellow_context())
        assert yellow_card["recommendation"] in (WAIT_FOR_MORE_EVIDENCE, DO_NOT_APPROVE)
        assert yellow_card["safe_to_execute_now"] is False

        # Write a sample card and verify path stays under temp.
        written = write_decision_card(td, green_card)
        for key, p in written.items():
            assert str(Path(p).resolve()).startswith(str(td.resolve())), \
                f"card path {p} escapes temp dir"

        # Render markdown.
        md = render_decision_card_markdown(green_card)
        assert "Recommendation" in md
        assert "Undo Plan" in md
        assert "False" in md  # safe_to_execute_now: False

        print(json.dumps({
            "self_test": "PASS",
            "green_recommendation": green_card["recommendation"],
            "yellow_recommendation": yellow_card["recommendation"],
            "red_recommendation": red_card["recommendation"],
            "wipe_recommendation": wipe_card["recommendation"],
            "safe_to_execute_now_always_false": True,
        }, indent=2))
    return 0


# ---------- CLI ----------


def _print_card_summary(card: dict[str, Any]) -> None:
    summary = {
        "card_id": card["card_id"],
        "recommendation": card["recommendation"],
        "risk_level": card["risk_level"],
        "goal_alignment": card["goal_alignment"],
        "safe_to_execute_now": card["safe_to_execute_now"],
        "serge_action_needed": card["serge_action_needed"],
        "non_delegable_check": card["non_delegable_check"],
        "plain_english_final_recommendation": card["plain_english_final_recommendation"],
    }
    print(json.dumps(summary, indent=2))


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Luna Serge Standing Approval Policy + Decision Cards (Phase 5T)"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--sample-green-card", action="store_true")
    parser.add_argument("--sample-yellow-card", action="store_true")
    parser.add_argument("--sample-red-card", action="store_true")
    parser.add_argument("--sample-wipe-computer-card", action="store_true")
    parser.add_argument("--print-policy", action="store_true")
    parser.add_argument("--print-aider-tutor-roadmap", action="store_true")
    parser.add_argument("--write-sample-card", action="store_true")
    parser.add_argument("--project-dir", default=str(_PROJECT_DIR_DEFAULT))
    args = parser.parse_args(argv)

    pdir = Path(args.project_dir)

    if args.self_test:
        return self_test()

    if args.sample_green_card:
        card = build_decision_card(
            _sample_green_context(),
            policy=load_standing_approval_policy(pdir),
            north_star=load_north_star_policy(pdir),
        )
        _print_card_summary(card)
        return 0

    if args.sample_yellow_card:
        card = build_decision_card(
            _sample_yellow_context(),
            policy=load_standing_approval_policy(pdir),
            north_star=load_north_star_policy(pdir),
        )
        _print_card_summary(card)
        return 0

    if args.sample_red_card:
        card = build_decision_card(
            _sample_red_context(),
            policy=load_standing_approval_policy(pdir),
            north_star=load_north_star_policy(pdir),
        )
        _print_card_summary(card)
        return 0

    if args.sample_wipe_computer_card:
        card = build_decision_card(
            _sample_wipe_computer_context(),
            policy=load_standing_approval_policy(pdir),
            north_star=load_north_star_policy(pdir),
        )
        _print_card_summary(card)
        # Always emit safety confirmation.
        if card["recommendation"] == APPROVE_RECOMMENDED:
            print("CRITICAL: wipe-computer must never be APPROVE_RECOMMENDED", file=sys.stderr)
            return 2
        return 0

    if args.print_policy:
        ns = load_north_star_policy(pdir)
        sap = load_standing_approval_policy(pdir)
        print(json.dumps({
            "north_star": ns,
            "standing_approval_policy": sap,
        }, indent=2))
        return 0

    if args.print_aider_tutor_roadmap:
        roadmap = load_aider_tutor_roadmap(pdir)
        print(json.dumps(roadmap, indent=2))
        return 0

    if args.write_sample_card:
        card = build_decision_card(
            _sample_green_context(),
            policy=load_standing_approval_policy(pdir),
            north_star=load_north_star_policy(pdir),
        )
        written = write_decision_card(pdir, card)
        print(json.dumps({
            "card_id": card["card_id"],
            "recommendation": card["recommendation"],
            "safe_to_execute_now": False,
            "written": written,
        }, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
