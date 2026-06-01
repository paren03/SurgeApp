"""Phase 29 - Operator-Gated Runtime Adapter Phase B (standalone).

Composes Phase 28 envelope + per-invocation consent token +
tamper-evident audit chain + call-time boundary + operator review
packet + dry-run queue + refusal analytics into a single dry-run-only
flow. approve=True still cannot execute in Phase 29.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_adapter_phase28_runtime as p28
import bilingual_voice_call_envelope as vce
import bilingual_voice_invocation_consent as ic
import bilingual_voice_audit_chain as vac
import bilingual_voice_calltime_boundary as ctb
import bilingual_voice_operator_review_packet as vrp
import bilingual_voice_dry_run_queue as vdq
import bilingual_voice_refusal_analytics as vra
import bilingual_voice_adapter_errors as vae


_PHASE = "phase29.runtime.v1"


def _new_id() -> str:
    return f"p29_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _chain_event(chain, et, status, msg="", md=None):
    prev = chain[-1].get("event_hash") if chain else ""
    ev = vac.create_audit_chain_event(et, status, msg, md or {},
                                       previous_hash=prev)
    return vac.append_chain_event(chain, ev)


def prepare_phase29_invocation(
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
    chain: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    chain = _chain_event(chain, "preflight", "ok",
                         "phase29 prepare_phase29_invocation begin",
                         {"approve": bool(approve), "phase": _PHASE})

    p28_result = p28.prepare_operator_gated_voice_call(
        user_text=user_text,
        draft_response_text=draft_response_text,
        conversation_state=conversation_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
        adapter_name=adapter_name,
        operator_id=operator_id,
        approve=bool(approve),
        limit=limit,
    )
    envelope = (p28_result or {}).get("call_envelope") or {}
    p28_status = (p28_result or {}).get("status") or ""
    chain = _chain_event(chain, "consent_request", "ok",
                         "phase28 envelope received",
                         {"p28_status": p28_status,
                          "envelope_id": envelope.get("envelope_id")})

    if p28_status == "refused" or envelope.get("status") == "refused":
        errors.append(vae.create_adapter_error(
            "UNSAFE_PAYLOAD",
            "phase28 refused upstream",
            severity="blocking"))
        chain = _chain_event(chain, "refusal", "refused",
                             "phase28 refused upstream")
        return _result(p28_result, None, None, None, None,
                       None, chain, errors,
                       "refused")

    # Per-invocation consent token
    token = ic.create_invocation_consent_token(
        envelope, operator_id=operator_id,
        approved=bool(approve),
        scope="dry_run_prepare",
        expires_in_seconds=300)
    chain = _chain_event(chain, "invocation_token_created", "ok",
                         "phase29 invocation token created",
                         {"token_id": token.get("token_id"),
                          "approved": bool(approve)})
    token_val = ic.validate_invocation_consent_token(token)
    if not token_val["ok"]:
        errors.append(vae.create_adapter_error(
            "CONSENT_INVALID", "invocation token invalid",
            severity="blocking",
            metadata={"reasons": token_val["reasons"]}))
    require = ic.require_valid_invocation_consent(token, envelope)
    chain = _chain_event(chain, "invocation_token_validated",
                         "ok" if require["ok"] else "blocked",
                         json.dumps(require.get("reasons", [])))

    # Call-time boundary recheck
    boundary = ctb.build_calltime_boundary_result(envelope, token)
    chain = _chain_event(chain, "calltime_boundary",
                         "ok" if boundary["ok"] else "blocked",
                         json.dumps(boundary.get("reasons", [])))
    if not boundary["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "phase29 call-time boundary failed",
            severity="blocking",
            metadata={"reasons": boundary["reasons"]}))

    # Operator review packet
    packet = vrp.create_operator_review_packet(
        envelope, invocation_token=token,
        boundary_result=boundary, audit_chain=chain)
    packet_val = vrp.validate_operator_review_packet(packet)
    chain = _chain_event(chain, "review_packet_created",
                         "ok" if packet_val["ok"] else "warn",
                         json.dumps(packet_val.get("reasons", [])),
                         {"packet_id": packet.get("packet_id")})

    # Dry-run queue (one-shot, no worker)
    queue = vdq.create_dry_run_queue()
    queue = vdq.enqueue_dry_run_packet(queue, packet)
    queue_summary = vdq.summarize_dry_run_queue(queue)
    chain = _chain_event(chain, "queue_enqueued", "ok",
                         "phase29 dry-run packet enqueued",
                         {"queue_id": queue.get("queue_id"),
                          "length": queue_summary.get("length")})

    # Refusal analytics over (Phase 28 errors + Phase 29 errors)
    refusals_input: list[Any] = list(
        (p28_result or {}).get("errors") or [])
    refusals_input.extend(errors)
    refusals_input.append(boundary)
    analytics = vra.summarize_refusal_patterns(refusals_input)
    recos = vra.recommend_safe_next_steps(refusals_input)

    # Refuse packet if blocking
    if any(vae.is_blocking_error(e) for e in errors):
        packet["execution_blocked"] = True
        packet["dry_run"] = True
        chain = _chain_event(chain, "refusal", "refused",
                             "phase29 blocking errors present")
        status = "refused"
    else:
        chain = _chain_event(chain, "dry_run_complete", "ok",
                             "phase29 dry-run packet ready")
        status = "dry_run_ready"

    return _result(p28_result, token, boundary, packet,
                   queue, {"analytics": analytics,
                           "recommendations": recos},
                   chain, errors, status)


def prepare_phase29_from_phase28_envelope(
    envelope: dict[str, Any],
    operator_id: str = "",
    approve: bool = False,
) -> dict[str, Any]:
    chain: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    chain = _chain_event(chain, "preflight", "ok",
                         "phase29 prepare_from_envelope begin",
                         {"approve": bool(approve)})
    token = ic.create_invocation_consent_token(
        envelope, operator_id=operator_id,
        approved=bool(approve),
        scope="dry_run_prepare",
        expires_in_seconds=300)
    boundary = ctb.build_calltime_boundary_result(envelope, token)
    if not boundary["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "phase29 boundary failed", severity="blocking"))
    packet = vrp.create_operator_review_packet(
        envelope, invocation_token=token,
        boundary_result=boundary, audit_chain=chain)
    queue = vdq.create_dry_run_queue()
    queue = vdq.enqueue_dry_run_packet(queue, packet)
    refusals_input = list(errors) + [boundary]
    analytics = vra.summarize_refusal_patterns(refusals_input)
    recos = vra.recommend_safe_next_steps(refusals_input)
    status = ("refused" if any(vae.is_blocking_error(e)
                                for e in errors)
              else "dry_run_ready")
    chain = _chain_event(chain, "dry_run_complete",
                         "ok" if status == "dry_run_ready" else "refused",
                         status)
    return _result({}, token, boundary, packet, queue,
                   {"analytics": analytics,
                    "recommendations": recos},
                   chain, errors, status)


def create_phase29_review_packet(
    envelope: dict[str, Any],
    operator_id: str = "",
    approve: bool = False,
) -> dict[str, Any]:
    token = ic.create_invocation_consent_token(
        envelope, operator_id=operator_id,
        approved=bool(approve))
    boundary = ctb.build_calltime_boundary_result(envelope, token)
    return vrp.create_operator_review_packet(
        envelope, invocation_token=token,
        boundary_result=boundary)


def queue_phase29_dry_run_packet(
    packet: dict[str, Any],
    queue: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if queue is None:
        queue = vdq.create_dry_run_queue()
    return vdq.enqueue_dry_run_packet(queue, packet)


def validate_phase29_packet(packet: Any) -> dict[str, Any]:
    return vrp.validate_operator_review_packet(packet)


def _result(p28_result, token, boundary, packet, queue,
            refusal, chain, errors, status):
    return {
        "phase29_id": _new_id(),
        "phase28_result": p28_result or {},
        "invocation_consent_token": token or {},
        "calltime_boundary_result": boundary or {},
        "review_packet": packet or {},
        "audit_chain": chain or [],
        "queue_status": (vdq.summarize_dry_run_queue(queue)
                          if queue else {}),
        "refusal_analytics": refusal or {},
        "status": status,
        "next_allowed_actions": ["review", "approve_dry_run_only",
                                 "refuse"],
        "forbidden_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "errors": errors or [],
        "gap_notes": [],
        "phase": _PHASE,
    }


def demo_phase29_invocations(limit: int = 12) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 12))
    scenarios = [
        ("Hello Luna", "Hi.", "conversation", None, "", False),
        ("Привет Луна", "Привет!", "conversation", "russian",
         "", False),
        ("Mix russian and english", "ok, давай.",
         "conversation", None, "operator_local", False),
        ("Teach me a Russian word", "", "teacher", "russian",
         "operator_local", True),
        ("Practice english", "", "teacher", "english",
         "operator_local", True),
        ("Talk slower please", "", "conversation", None, "", False),
        ("Speak english only", "", "conversation", "english",
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
        r = prepare_phase29_invocation(
            user_text=ut, draft_response_text=dt,
            conversation_mode=mode, user_preference=pref,
            operator_id=op, approve=ap)
        env = r.get("review_packet") or {}
        out.append({
            "user_text": ut,
            "status": r.get("status"),
            "packet_id": env.get("packet_id"),
            "adapter": env.get("adapter_name"),
            "execution_blocked": env.get("execution_blocked"),
            "approve_requested": ap,
            "chain_length": len(r.get("audit_chain") or []),
        })
    return {"demo": out, "count": len(out), "phase": _PHASE}


def write_phase29_runtime_report(
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
    "prepare_phase29_invocation",
    "prepare_phase29_from_phase28_envelope",
    "create_phase29_review_packet",
    "queue_phase29_dry_run_packet",
    "validate_phase29_packet",
    "demo_phase29_invocations",
    "write_phase29_runtime_report",
]
