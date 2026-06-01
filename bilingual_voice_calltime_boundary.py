"""Phase 29 - Call-Time Boundary Validator.

Re-runs the execution-boundary check immediately before producing a
would-call dry-run packet. Uses Phase 28 boundary guard.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_execution_boundary as veb
import bilingual_voice_call_envelope as vce
import bilingual_voice_invocation_consent as ic


_PHASE = "phase29.calltime_boundary.v1"


def recheck_no_audio_fields(envelope: Any) -> dict[str, Any]:
    res = veb.reject_if_audio_or_subprocess_requested(envelope)
    return {
        "ok": not res.get("audio_hits"),
        "audio_hits": res.get("audio_hits", []),
        "phase": _PHASE,
    }


def recheck_no_execution_fields(envelope: Any) -> dict[str, Any]:
    res = veb.reject_if_audio_or_subprocess_requested(envelope)
    enforce = veb.enforce_phase28_no_execution(envelope)
    exec_hits = list(set(res.get("subprocess_hits", []) +
                         enforce.get("hits", [])))
    # Filter out audio-only hits
    audio_only = set(res.get("audio_hits", []))
    exec_hits = [h for h in exec_hits if h not in audio_only]
    return {
        "ok": not exec_hits,
        "execution_hits": exec_hits,
        "phase": _PHASE,
    }


def recheck_dry_run_only(
    envelope: Any,
    invocation_token: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(envelope, dict):
        return {"ok": False, "reasons": ["envelope_not_dict"]}
    if envelope.get("dry_run") is not True:
        reasons.append("envelope_dry_run_not_true")
    if envelope.get("execution_blocked") is not True:
        reasons.append("envelope_execution_blocked_not_true")
    if invocation_token is not None:
        if not isinstance(invocation_token, dict):
            reasons.append("token_not_dict")
        elif invocation_token.get("dry_run_only") is not True:
            reasons.append("token_not_dry_run_only")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def recheck_safety_summary(envelope: Any) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        return {"ok": False, "reasons": ["envelope_not_dict"]}
    safety = (envelope.get("render_job") or {}).get(
        "safety_summary") or envelope.get("safety_summary") or {}
    if safety.get("blocked") or safety.get("unsafe"):
        return {"ok": False, "reasons": ["unsafe_payload"],
                "safety_summary": safety, "phase": _PHASE}
    return {"ok": True, "reasons": [], "safety_summary": safety,
            "phase": _PHASE}


def validate_calltime_boundary(
    envelope: Any,
    invocation_token: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return build_calltime_boundary_result(envelope, invocation_token)


def build_calltime_boundary_result(
    envelope: Any,
    invocation_token: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    # Envelope structural check
    env_val = vce.validate_call_envelope(envelope)
    if not env_val["ok"]:
        reasons.append("envelope_invalid:" + ",".join(env_val["reasons"]))
    # Invocation token check
    token_check = {"ok": True, "reasons": []}
    if invocation_token is not None:
        token_check = ic.require_valid_invocation_consent(
            invocation_token, envelope)
        if not token_check["ok"]:
            reasons.append("token_invalid:" +
                           ",".join(token_check.get("reasons", [])))
    else:
        reasons.append("token_missing")
    # Audio / execution rechecks
    audio = recheck_no_audio_fields(envelope)
    if not audio["ok"]:
        reasons.append("audio_field_present:" +
                       ",".join(audio["audio_hits"]))
    execu = recheck_no_execution_fields(envelope)
    if not execu["ok"]:
        reasons.append("execution_field_present:" +
                       ",".join(execu["execution_hits"]))
    # Dry-run recheck
    dry = recheck_dry_run_only(envelope, invocation_token)
    if not dry["ok"]:
        reasons.append("dry_run_recheck_failed:" +
                       ",".join(dry["reasons"]))
    # Safety recheck
    safety = recheck_safety_summary(envelope)
    if not safety["ok"]:
        reasons.append("safety_recheck_failed:" +
                       ",".join(safety["reasons"]))
    return {
        "ok": not reasons,
        "reasons": reasons,
        "execution_blocked": True,
        "envelope_validation": env_val,
        "token_check": token_check,
        "audio_check": audio,
        "execution_check": execu,
        "dry_run_check": dry,
        "safety_check": safety,
        "phase": _PHASE,
    }


def write_calltime_boundary_report(
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
    "validate_calltime_boundary",
    "recheck_no_audio_fields",
    "recheck_no_execution_fields",
    "recheck_dry_run_only",
    "recheck_safety_summary",
    "build_calltime_boundary_result",
    "write_calltime_boundary_report",
]
