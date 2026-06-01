"""Phase 28 - Operator-Gated Voice Adapter Runtime (single entrypoint).

Turns user text or Phase 27 render jobs into audited dry-run call
envelopes. approve=True still cannot cross the execution boundary in
Phase 28.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_dry_run_pipeline as drp
import bilingual_voice_adapter_registry as vreg
import bilingual_voice_operator_consent as voc
import bilingual_voice_adapter_audit_log as val
import bilingual_voice_execution_boundary as veb
import bilingual_voice_capability_negotiator as vcn
import bilingual_voice_call_envelope as vce
import bilingual_voice_adapter_errors as vae


_PHASE = "phase28.runtime.v1"


def _new_id() -> str:
    return f"p28_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _audit(events, et, status, msg="", md=None):
    return val.append_audit_event(
        events, val.create_audit_event(et, status, msg, md or {}))


def prepare_operator_gated_voice_call(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    adapter_name: Optional[str] = None,
    operator_id: str = "",
    approve: bool = False,
    limit: int = 25,
) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    events = _audit(events, "preflight", "ok",
                    "phase28 prepare_operator_gated_voice_call begin",
                    {"approve": bool(approve), "phase": _PHASE})

    pipeline = drp.run_dry_run_pipeline(
        user_text=user_text,
        draft_response_text=draft_response_text,
        conversation_state=conversation_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
        adapter_name=adapter_name,
        limit=limit,
    )
    events = _audit(events, "payload_validation",
                    "ok" if pipeline.get("dry_run_status") ==
                    "planned_dry_run" else "warn",
                    f"phase27 pipeline status={pipeline.get('dry_run_status')}")

    # Unsafe / invalid pipeline outcomes short-circuit
    if pipeline.get("dry_run_status") in ("refused_unsafe_payload",
                                          "rejected_invalid_input"):
        errors.append(vae.create_adapter_error(
            "UNSAFE_PAYLOAD" if pipeline.get("dry_run_status") ==
            "refused_unsafe_payload" else "PAYLOAD_INVALID",
            f"pipeline rejected: {pipeline.get('dry_run_status')}",
            severity="blocking"))
        events = _audit(events, "refusal", "refused",
                        pipeline.get("dry_run_status") or "")
        envelope = vce.create_call_envelope({}, {}, {})
        envelope = vce.mark_envelope_refused(
            envelope, pipeline.get("dry_run_status") or "")
        return _result(pipeline, None, None, {}, {}, envelope,
                       events, errors,
                       "refused", forbidden=True, gap_notes=[
                           pipeline.get("dry_run_status") or ""])

    render_job = pipeline.get("render_job") or {}
    adapter_choice = pipeline.get("adapter_choice") or {}
    descriptor = adapter_choice.get("chosen") or {}

    # Unknown adapter — fail-soft with explicit error
    if adapter_name and not vreg.find_adapter_by_name(adapter_name):
        errors.append(vae.create_adapter_error(
            "UNKNOWN_ADAPTER",
            f"adapter_name={adapter_name} not found",
            severity="error"))
        events = _audit(events, "adapter_selection", "warn",
                        f"unknown adapter:{adapter_name}")

    # Consent request
    consent_req = voc.create_consent_request(
        render_job, descriptor, requested_action="dry_run_prepare")
    consent_req_val = voc.validate_consent_request(consent_req)
    events = _audit(events, "consent_request",
                    "ok" if consent_req_val["ok"] else "error",
                    json.dumps(consent_req_val["reasons"]),
                    {"consent_request_id":
                        consent_req.get("consent_request_id")})

    # Consent decision (still capped to dry_run in Phase 28 regardless of approve)
    consent_dec = voc.create_consent_decision(
        consent_req, approved=bool(approve),
        operator_id=operator_id,
        reason="phase28 operator-gated dry-run only")
    consent_dec_val = voc.validate_consent_decision(consent_dec)
    dry_only = voc.require_phase28_dry_run_only(consent_dec)
    events = _audit(events, "consent_decision",
                    "ok" if consent_dec_val["ok"] else "warn",
                    json.dumps(consent_dec_val["reasons"]),
                    {"approved": bool(approve),
                     "operator_id": operator_id,
                     "dry_run_only": True})
    if not consent_dec_val["ok"]:
        errors.append(vae.create_adapter_error(
            "CONSENT_INVALID",
            "consent decision missing required fields",
            severity="blocking",
            metadata={"reasons": consent_dec_val["reasons"]}))
    if not dry_only["ok"]:
        errors.append(vae.create_adapter_error(
            "ADAPTER_DRY_RUN_REQUIRED",
            "phase28 enforces dry-run only",
            severity="blocking",
            metadata={"reasons": dry_only["reasons"]}))

    # Execution-boundary check (scan the inputs that arrived in the
    # render job + adapter choice for execution intent)
    boundary = veb.build_boundary_result({
        "render_job": render_job,
        "adapter_choice": adapter_choice,
        "consent_decision": consent_dec,
        "approve": bool(approve),
        "operator_id": operator_id,
    })
    events = _audit(events, "boundary_guard",
                    "ok" if boundary["ok"] else "blocked",
                    json.dumps(boundary.get("reasons", [])))
    if not boundary["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "execution intent detected",
            severity="blocking",
            metadata={"hits": boundary.get("hits", [])}))

    # Capability negotiation
    negotiation = vcn.negotiate_capabilities(
        pipeline.get("spoken_payload") or {}, descriptor)
    events = _audit(events, "compatibility_check",
                    "ok" if negotiation["ok"] else "warn",
                    json.dumps(negotiation.get("unsupported_features", [])))
    if negotiation.get("rejected"):
        errors.append(vae.create_adapter_error(
            "CAPABILITY_MISMATCH",
            negotiation.get("reason", "capability_mismatch"),
            severity="blocking"))

    # Build envelope
    envelope = vce.create_call_envelope(
        render_job=render_job,
        consent_decision=consent_dec,
        adapter_choice=adapter_choice,
        audit_events=events,
    )
    envelope["boundary_checks"] = {
        "execution_boundary": boundary,
        "capability_negotiation": negotiation,
        "consent_dry_run_only": dry_only,
    }
    # Refuse envelope if any blocking error
    if any(vae.is_blocking_error(e) for e in errors):
        envelope = vce.mark_envelope_refused(
            envelope, "phase28_blocking_errors_present")
        events = _audit(events, "render_envelope_created", "refused",
                        "envelope refused due to blocking errors")
    else:
        envelope = vce.mark_envelope_dry_run_ready(envelope)
        events = _audit(events, "render_envelope_created", "ok",
                        "envelope marked dry_run_ready")

    # Final dry-run-complete audit
    events = _audit(events, "dry_run_complete",
                    "ok" if envelope["status"] == "dry_run_ready"
                    else "refused",
                    f"phase28 envelope status={envelope['status']}")

    return _result(pipeline, consent_req, consent_dec, boundary,
                   negotiation, envelope, events, errors,
                   envelope["status"],
                   forbidden=False,
                   gap_notes=[])


def prepare_from_existing_render_job(
    render_job: dict[str, Any],
    adapter_name: Optional[str] = None,
    operator_id: str = "",
    approve: bool = False,
) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    events = _audit(events, "preflight", "ok",
                    "prepare_from_existing_render_job begin")
    descriptor = (render_job or {}).get("adapter_descriptor") or {}
    if adapter_name:
        found = vreg.find_adapter_by_name(adapter_name)
        if found:
            descriptor = found
        else:
            errors.append(vae.create_adapter_error(
                "UNKNOWN_ADAPTER",
                f"adapter_name={adapter_name} not in registry"))
    adapter_choice = {"chosen": descriptor, "score": 0.0,
                      "compatibility_reasons": [],
                      "safety_reasons": [],
                      "policy_version": "phase27.policy.v1"}
    consent_req = voc.create_consent_request(
        render_job, descriptor, requested_action="dry_run_prepare")
    consent_dec = voc.create_consent_decision(
        consent_req, approved=bool(approve),
        operator_id=operator_id,
        reason="phase28 prepare_from_existing_render_job")
    boundary = veb.build_boundary_result({
        "render_job": render_job, "consent_decision": consent_dec,
        "approve": bool(approve)})
    if not boundary["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "execution intent detected", severity="blocking"))
    negotiation = vcn.negotiate_capabilities(
        (render_job or {}).get("spoken_render_payload") or {},
        descriptor)
    envelope = vce.create_call_envelope(
        render_job=render_job or {},
        consent_decision=consent_dec,
        adapter_choice=adapter_choice,
        audit_events=events)
    envelope["boundary_checks"] = {
        "execution_boundary": boundary,
        "capability_negotiation": negotiation,
    }
    if any(vae.is_blocking_error(e) for e in errors) or \
            negotiation.get("rejected"):
        envelope = vce.mark_envelope_refused(envelope, "blocking_errors")
    else:
        envelope = vce.mark_envelope_dry_run_ready(envelope)
    return {
        "phase28_id": _new_id(),
        "dry_run_pipeline_result": {},
        "consent_request": consent_req,
        "consent_decision": consent_dec,
        "boundary_result": boundary,
        "capability_negotiation": negotiation,
        "call_envelope": envelope,
        "audit_events": events,
        "errors": errors,
        "status": envelope["status"],
        "next_allowed_actions": ["validate", "plan", "simulate_acceptance",
                                 "write_report"],
        "forbidden_actions": list(
            vce._FORBIDDEN_ACTIONS_DEFAULT),  # noqa: SLF001
        "gap_notes": [],
        "phase": _PHASE,
    }


def validate_phase28_voice_call_envelope(envelope: Any) -> dict[str, Any]:
    return vce.validate_call_envelope(envelope)


def _result(pipeline, consent_req, consent_dec, boundary, negotiation,
            envelope, events, errors, status, forbidden, gap_notes):
    return {
        "phase28_id": _new_id(),
        "dry_run_pipeline_result": pipeline or {},
        "consent_request": consent_req or {},
        "consent_decision": consent_dec or {},
        "boundary_result": boundary or {},
        "capability_negotiation": negotiation or {},
        "call_envelope": envelope or {},
        "audit_events": events or [],
        "errors": errors or [],
        "status": status,
        "next_allowed_actions": ["validate", "plan", "simulate_acceptance",
                                 "write_report"],
        "forbidden_actions": list(
            vce._FORBIDDEN_ACTIONS_DEFAULT),  # noqa: SLF001
        "gap_notes": list(gap_notes or []),
        "phase": _PHASE,
    }


def demo_phase28_operator_gated_calls(limit: int = 12) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 12))
    scenarios = [
        ("Hello Luna, how are you?", "I'm fine.", "conversation", None,
         "", False),
        ("Привет Луна!", "Привет!", "conversation", "russian", "", False),
        ("Mix russian and english", "Sure, давай попробуем.",
         "conversation", None, "operator_local", False),
        ("Teach me a Russian word", "", "teacher", "russian",
         "operator_local", True),
        ("Practice english with me", "", "teacher", "english",
         "operator_local", True),
        ("Talk slower please", "", "conversation", None, "", False),
        ("Speak english only please", "", "conversation", "english",
         "", False),
        ("Use professional tone", "", "professional", None,
         "operator_local", True),
        ("Stop mixing languages", "", "conversation", None, "", False),
        ("Что нового?", "", "conversation", None, "", False),
        ("Use bilingual mode", "", "conversation", None,
         "operator_local", True),
        ("Slower russian", "", "conversation", "russian", "", False),
    ][:cap]
    out: list[dict[str, Any]] = []
    for ut, dt, mode, pref, op, ap in scenarios:
        r = prepare_operator_gated_voice_call(
            user_text=ut, draft_response_text=dt,
            conversation_mode=mode, user_preference=pref,
            operator_id=op, approve=ap)
        env = r.get("call_envelope") or {}
        out.append({
            "user_text": ut,
            "status": r.get("status"),
            "envelope_status": env.get("status"),
            "adapter": (env.get("adapter_choice") or {}).get(
                "chosen", {}).get("adapter_name"),
            "execution_blocked": env.get("execution_blocked"),
            "approve_requested": ap,
            "blocking_errors":
                sum(1 for e in (r.get("errors") or [])
                    if vae.is_blocking_error(e)),
        })
    return {"demo": out, "count": len(out), "phase": _PHASE}


def write_phase28_runtime_report(
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
    "prepare_operator_gated_voice_call",
    "prepare_from_existing_render_job",
    "validate_phase28_voice_call_envelope",
    "demo_phase28_operator_gated_calls",
    "write_phase28_runtime_report",
]
