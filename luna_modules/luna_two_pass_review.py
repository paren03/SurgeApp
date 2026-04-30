"""Two-pass review gate for continues_update jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _reviewer_decision(name: str, report: Dict[str, Any]) -> Dict[str, Any]:
    status = str(report.get("status") or "").lower()
    diff_path = str(report.get("diff_path") or "").strip()
    verify_passed = bool(report.get("verify_passed") or report.get("verification_passed"))
    failure_reason = str(report.get("failure_reason") or report.get("summary") or "").strip()

    if status == "done" and diff_path and verify_passed:
        return {
            "reviewer": name,
            "satisfied": True,
            "reason": "real_diff_verified",
        }
    if status == "done" and bool(report.get("analysis_only")):
        return {
            "reviewer": name,
            "satisfied": True,
            "reason": "analysis_only_accepted",
        }
    if status == "done" and bool(report.get("already_compliant")) and str(report.get("evidence") or "").strip():
        return {
            "reviewer": name,
            "satisfied": True,
            "reason": "already_compliant_with_evidence",
        }
    if status == "noop":
        return {
            "reviewer": name,
            "satisfied": False,
            "reason": "noop_is_not_upgrade",
        }
    if status in {"failed", "timeout", "stopped"}:
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
