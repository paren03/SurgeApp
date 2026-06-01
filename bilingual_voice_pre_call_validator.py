"""Phase 30 - Pre-Call Validator.

Re-runs every safety / consent / boundary / kill-switch check
immediately before the dummy adapter is invoked. Fails closed on any
issue.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_callable_adapter_interface as cai
import bilingual_voice_invocation_consent as ic
import bilingual_voice_execution_boundary as veb
import bilingual_voice_emergency_kill_switch as eks


_PHASE = "phase30.pre_call.v1"


def verify_phase29_packet(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {"ok": False, "reasons": ["request_not_dict"]}
    pkt29_id = request.get("phase29_packet_id")
    if not pkt29_id:
        return {"ok": False, "reasons": ["phase29_packet_id_missing"]}
    return {"ok": True, "reasons": [],
            "phase29_packet_id": pkt29_id}


def verify_invocation_consent(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {"ok": False, "reasons": ["request_not_dict"]}
    token = request.get("invocation_token") or {}
    if isinstance(token, dict) and token:
        v = ic.validate_invocation_consent_token(token)
        if not v["ok"]:
            return {"ok": False, "reasons": ["token_invalid:" +
                    ",".join(v["reasons"])]}
        # operator_id + approve required
        if not token.get("operator_id"):
            return {"ok": False, "reasons": ["operator_id_missing"]}
        if not token.get("approved"):
            return {"ok": False, "reasons": ["approve_required"]}
        return {"ok": True, "reasons": []}
    # Token may also be referenced by id only — fall back to request
    # fields.
    if not request.get("invocation_token_id"):
        return {"ok": False, "reasons": ["invocation_token_missing"]}
    if not request.get("operator_id_hash"):
        return {"ok": False, "reasons": ["operator_id_hash_missing"]}
    if not request.get("approved"):
        return {"ok": False, "reasons": ["approve_required"]}
    return {"ok": True, "reasons": []}


def verify_calltime_boundary(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {"ok": False, "reasons": ["request_not_dict"]}
    if request.get("dry_run") is not True:
        return {"ok": False, "reasons": ["dry_run_must_be_true"]}
    if request.get("test_only") is not True:
        return {"ok": False, "reasons": ["test_only_must_be_true"]}
    safety = request.get("safety_summary") or {}
    if safety.get("unsafe") or safety.get("blocked"):
        return {"ok": False, "reasons": ["unsafe_payload"]}
    return {"ok": True, "reasons": []}


def verify_operator_review_packet(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {"ok": False, "reasons": ["request_not_dict"]}
    pid = request.get("phase29_packet_id")
    if not pid:
        return {"ok": False, "reasons": ["phase29_packet_id_missing"]}
    return {"ok": True, "reasons": []}


def verify_adapter_descriptor(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {"ok": False, "reasons": ["request_not_dict"]}
    desc = request.get("adapter_descriptor") or {}
    v = cai.validate_callable_adapter_descriptor(desc)
    return v


_FORBIDDEN_REQUEST_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command",
)


def verify_no_audio_or_command_fields(request: Any) -> dict[str, Any]:
    """Direct field-name scan for forbidden audio / command fields at
    the top level of the request and in nested dicts. Independent of
    the execution-intent scanner."""
    if not isinstance(request, dict):
        return {"ok": False, "reasons": ["request_not_dict"]}
    hits: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                if ks in _FORBIDDEN_REQUEST_FIELDS and ks not in hits:
                    hits.append(ks)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)

    _walk(request)
    return {"ok": not hits,
            "reasons": (["forbidden_field:" + ",".join(hits)]
                         if hits else [])}


def verify_no_execution_fields_pre_call(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {"ok": False, "reasons": ["request_not_dict"]}
    res = veb.build_boundary_result(request)
    if not res["ok"]:
        return {"ok": False, "reasons": ["execution_intent:" +
                ",".join(res.get("hits", []))]}
    # Combine with audio/command-field scan
    af = verify_no_audio_or_command_fields(request)
    if not af["ok"]:
        return af
    return {"ok": True, "reasons": []}


def validate_pre_call_requirements(
    request: Any,
    kill_switch_policy: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    # Kill switch FIRST — it overrides everything
    ks_policy = kill_switch_policy or eks.create_kill_switch_policy(
        enabled=False)
    ks_decision = eks.enforce_kill_switch(ks_policy, request)
    if not ks_decision["allow"]:
        reasons.append("kill_switch:" + ks_decision["reason"])
    # Request structural validation
    req_val = cai.validate_callable_adapter_request(request)
    if not req_val["ok"]:
        reasons.append("request_invalid:" +
                       ",".join(req_val["reasons"]))
    # Sub-checks
    sub_results = {
        "phase29_packet": verify_phase29_packet(request),
        "invocation_consent": verify_invocation_consent(request),
        "calltime_boundary": verify_calltime_boundary(request),
        "operator_review_packet":
            verify_operator_review_packet(request),
        "adapter_descriptor": verify_adapter_descriptor(request),
        "no_execution_fields":
            verify_no_execution_fields_pre_call(request),
    }
    for name, r in sub_results.items():
        if not r["ok"]:
            reasons.append(f"{name}_failed:" +
                           ",".join(r.get("reasons", [])))
    return {
        "ok": not reasons,
        "reasons": reasons,
        "execution_blocked": True,
        "kill_switch_decision": ks_decision,
        "request_validation": req_val,
        "sub_results": sub_results,
        "phase": _PHASE,
    }


def write_pre_call_validation_report(
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
    "validate_pre_call_requirements",
    "verify_phase29_packet",
    "verify_invocation_consent",
    "verify_calltime_boundary",
    "verify_operator_review_packet",
    "verify_adapter_descriptor",
    "verify_no_execution_fields_pre_call",
    "write_pre_call_validation_report",
]
