"""Phase 30 - Emergency Kill Switch.

If enabled, blocks ALL adapter calls — regardless of consent, approval,
or descriptor validity. Default is disabled but always evaluated.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase30.kill_switch.v1"


def get_kill_switch_state(default_enabled: bool = False) -> dict[str, Any]:
    return {
        "enabled": bool(default_enabled),
        "reason": "",
        "phase": _PHASE,
        "notes": ("phase30 kill switch state; default disabled; "
                  "when enabled all calls refused"),
    }


def create_kill_switch_policy(
    enabled: bool = False,
    reason: str = "",
) -> dict[str, Any]:
    return {
        "enabled": bool(enabled),
        "reason": str(reason or ""),
        "created_at": time.time(),
        "phase": _PHASE,
    }


def validate_kill_switch_policy(policy: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(policy, dict):
        return {"ok": False, "reasons": ["policy_not_dict"],
                "fail_closed": True}
    for f in ("enabled", "reason", "phase"):
        if f not in policy:
            reasons.append(f"missing_field:{f}")
    if not isinstance(policy.get("enabled"), bool):
        reasons.append("enabled_not_bool")
    return {
        "ok": not reasons,
        "reasons": reasons,
        "fail_closed": True,
        "phase": _PHASE,
    }


def enforce_kill_switch(
    policy: Any,
    request: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Returns {allow, reason, phase}. allow=False means BLOCK.
    Fails closed if policy is malformed."""
    pv = validate_kill_switch_policy(policy)
    if not pv["ok"]:
        return {
            "allow": False,
            "reason": "kill_switch_policy_invalid:" +
                      ",".join(pv["reasons"]),
            "phase": _PHASE,
            "fail_closed": True,
        }
    if policy.get("enabled") is True:
        return {
            "allow": False,
            "reason": "kill_switch_enabled:" +
                      str(policy.get("reason") or "no_reason_given"),
            "phase": _PHASE,
            "fail_closed": False,
        }
    return {
        "allow": True,
        "reason": "",
        "phase": _PHASE,
        "fail_closed": False,
    }


def explain_kill_switch_decision(decision: Any) -> dict[str, Any]:
    if not isinstance(decision, dict):
        return {"ok": False, "summary": "no_decision"}
    return {
        "ok": True,
        "summary": (
            f"phase30 kill switch: allow={bool(decision.get('allow'))} "
            f"reason={decision.get('reason') or 'none'}"),
        "allow": bool(decision.get("allow")),
        "reason": decision.get("reason") or "",
        "phase": _PHASE,
        "advice": (
            "Kill switch overrides consent, approval, and adapter "
            "compatibility. When enabled, every callable adapter "
            "request is refused without invocation."),
    }


def write_kill_switch_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_kill_switch_state",
    "create_kill_switch_policy",
    "validate_kill_switch_policy",
    "enforce_kill_switch",
    "explain_kill_switch_decision",
    "write_kill_switch_report",
]
