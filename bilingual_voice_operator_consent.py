"""Phase 28 - Operator Consent Contract.

Defines explicit operator consent requirements for any future voice
adapter execution. Phase 28 only permits requested_action="dry_run_prepare".
Any execute_audio / run_tts / run_subprocess / write_audio / speak_now
request is rejected. approve=True still cannot cross the execution
boundary in Phase 28.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase28.consent.v1"

SUPPORTED_ACTIONS = ("dry_run_prepare",)
REJECTED_ACTIONS = (
    "execute_audio", "run_tts", "run_subprocess",
    "write_audio", "speak_now",
)


def get_operator_consent_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "supported_actions": list(SUPPORTED_ACTIONS),
        "rejected_actions": list(REJECTED_ACTIONS),
        "required_request_fields": [
            "consent_request_id", "created_at", "render_job_id",
            "adapter_name", "adapter_type", "requested_action",
            "phase",
        ],
        "required_decision_fields": [
            "consent_decision_id", "created_at", "consent_request_id",
            "approved", "operator_id", "reason", "phase", "dry_run_only",
        ],
        "dry_run_only_in_phase28": True,
        "notes": [
            "Phase 28 only permits dry_run_prepare.",
            "approve=True does NOT enable audio/TTS/subprocess.",
            "Missing operator_id rejects any non-dry-run request.",
        ],
    }


def _new_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def create_consent_request(
    render_job: dict[str, Any],
    adapter_descriptor: dict[str, Any],
    requested_action: str = "dry_run_prepare",
) -> dict[str, Any]:
    rj = render_job if isinstance(render_job, dict) else {}
    desc = adapter_descriptor if isinstance(adapter_descriptor, dict) else {}
    return {
        "consent_request_id": _new_id("creq"),
        "created_at": time.time(),
        "render_job_id": rj.get("job_id") or "",
        "adapter_name": desc.get("adapter_name") or "",
        "adapter_type": desc.get("adapter_type") or "",
        "requested_action": str(requested_action or "dry_run_prepare"),
        "phase": _PHASE,
        "notes": "phase28 consent request",
    }


def validate_consent_request(consent_request: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(consent_request, dict):
        return {"ok": False, "reasons": ["consent_request_not_dict"]}
    for f in ("consent_request_id", "created_at", "render_job_id",
              "adapter_name", "adapter_type", "requested_action", "phase"):
        if f not in consent_request:
            reasons.append(f"missing_field:{f}")
    action = str(consent_request.get("requested_action") or "").lower()
    if action in REJECTED_ACTIONS:
        reasons.append(f"rejected_action:{action}")
    if action not in SUPPORTED_ACTIONS and action not in REJECTED_ACTIONS:
        reasons.append(f"unsupported_action:{action}")
    return {"ok": not reasons, "reasons": reasons}


def create_consent_decision(
    consent_request: dict[str, Any],
    approved: bool = False,
    operator_id: str = "",
    reason: str = "",
) -> dict[str, Any]:
    cr = consent_request if isinstance(consent_request, dict) else {}
    return {
        "consent_decision_id": _new_id("cdec"),
        "created_at": time.time(),
        "consent_request_id": cr.get("consent_request_id") or "",
        "approved": bool(approved),
        "operator_id": str(operator_id or ""),
        "reason": str(reason or ""),
        "phase": _PHASE,
        "dry_run_only": True,
        "requested_action": str(cr.get("requested_action") or ""),
        "notes": "phase28 consent decision; dry_run_only=True",
    }


def validate_consent_decision(consent_decision: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(consent_decision, dict):
        return {"ok": False, "reasons": ["consent_decision_not_dict"]}
    for f in ("consent_decision_id", "created_at", "consent_request_id",
              "approved", "operator_id", "reason", "phase",
              "dry_run_only"):
        if f not in consent_decision:
            reasons.append(f"missing_field:{f}")
    if consent_decision.get("dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    action = str(consent_decision.get("requested_action") or "").lower()
    if action and action in REJECTED_ACTIONS:
        reasons.append(f"rejected_action:{action}")
    if consent_decision.get("approved") and not \
            consent_decision.get("operator_id"):
        reasons.append("operator_id_required_when_approved")
    return {"ok": not reasons, "reasons": reasons}


def require_phase28_dry_run_only(consent_decision: Any) -> dict[str, Any]:
    """Phase 28 always returns dry_run_only=True regardless of input."""
    reasons: list[str] = []
    if not isinstance(consent_decision, dict):
        return {"ok": False, "dry_run_only": True,
                "reasons": ["consent_decision_not_dict"]}
    action = str(consent_decision.get("requested_action") or "").lower()
    if action in REJECTED_ACTIONS:
        reasons.append(f"rejected_action:{action}")
    if action and action not in SUPPORTED_ACTIONS:
        reasons.append(f"unsupported_action:{action}")
    return {
        "ok": not reasons,
        "dry_run_only": True,
        "phase": _PHASE,
        "reasons": reasons,
    }


def explain_consent_boundary(consent_decision: Any) -> dict[str, Any]:
    if not isinstance(consent_decision, dict):
        return {"ok": False, "summary": "no_consent_decision"}
    approved = bool(consent_decision.get("approved"))
    action = str(consent_decision.get("requested_action") or "")
    return {
        "ok": True,
        "summary": (
            f"phase28 boundary: approved={approved} "
            f"requested_action={action or 'dry_run_prepare'} "
            f"dry_run_only=True"
        ),
        "approved": approved,
        "operator_id": consent_decision.get("operator_id", ""),
        "requested_action": action,
        "execution_blocked": True,
        "phase": _PHASE,
        "advice": (
            "Phase 28 refuses audio/TTS/subprocess/PowerShell/SAPI/Piper "
            "regardless of approval. A future phase may add a separately "
            "consented runtime path with per-invocation audit logging."),
    }


def write_operator_consent_report(
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
    "SUPPORTED_ACTIONS",
    "REJECTED_ACTIONS",
    "get_operator_consent_schema",
    "create_consent_request",
    "validate_consent_request",
    "create_consent_decision",
    "validate_consent_decision",
    "require_phase28_dry_run_only",
    "explain_consent_boundary",
    "write_operator_consent_report",
]
