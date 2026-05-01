"""Two-pass review gate for continues_update jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _is_status_done(status: str) -> bool:
    return status == "done"

def _has_valid_diff_path(diff_path: str) -> bool:
    return diff_path.strip() != ""

def _verify_passed(verify_passed: Any, verification_passed: Any) -> bool:
    return bool(verify_passed or verification_passed)

def _reviewer_decision(name: str, report: Dict[str, Any]) -> Dict[str, Any]:
    status = str(report.get("status") or "").lower()
    diff_path = str(report.get("diff_path") or "").strip()
    verify_passed = _verify_passed(report.get("verify_passed"), report.get("verification_passed"))
    failure_reason = str(report.get("failure_reason") or report.get("summary") or "").strip()

    if _is_status_done(status) and _has_valid_diff_path(diff_path) and verify_passed:
        return {
            "reviewer": name,
            "satisfied": True,
            "reason": "real_diff_verified",
        }
    if status in {"analysis_only", "already_compliant"}:
        evidence = str(report.get("evidence") or "").strip()
        return {
            "reviewer": name,
            "satisfied": bool(evidence),
            "reason": f"{'analysis_only_accepted' if status == 'analysis_only' else 'already_compliant_with_evidence'}",
        }
    if status in {"noop", "timeout"}:
        return {
            "reviewer": name,
            "satisfied": False,
            "reason": f"{'noop_is_not_upgrade' if status == 'noop' else 'timeout_needs_repair'}",
        }
    if status in {"failed", "stopped", "quarantined"}:
        return {
            "reviewer": name,
            "satisfied": False,
            "reason": failure_reason or status or "failed",
        }
    return {
        "reviewer": name,
        "satisfied": False,
        "reason": "unverified_or_missing_diff",
    }


def build_two_pass_review(report: Dict[str, Any]) -> Dict[str, Any]:
    """Require two independent policy checks before CU advances."""
    reviews: List[Dict[str, Any]] = [
        _reviewer_decision("qa_review", report),
        _reviewer_decision("safety_review", report),
    ]
    satisfied = all(row.get("satisfied") for row in reviews)
    return {
        "ts": _now_iso(),
        "required_reviews": 2,
        "satisfied": satisfied,
        "action": "continue_next_task" if satisfied else "pause_for_inspection",
        "reviews": reviews,
    }
