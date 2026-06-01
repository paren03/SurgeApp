"""Phase 30 - Operator-Gated Callable Runtime (standalone).

Composes Phase 29 packet → callable adapter request → kill switch →
pre-call validation → dummy adapter call → post-call validation →
invocation receipt. Only dummy_metadata_adapter is permitted.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_adapter_phase29_runtime as p29
import bilingual_voice_invocation_consent as ic
import bilingual_voice_audit_chain as vac
import bilingual_voice_callable_adapter_interface as cai
import bilingual_voice_dummy_metadata_adapter as dma
import bilingual_voice_emergency_kill_switch as eks
import bilingual_voice_pre_call_validator as pre
import bilingual_voice_post_call_validator as post
import bilingual_voice_invocation_receipt as recv
import bilingual_voice_adapter_errors as vae


_PHASE = "phase30.runtime.v1"


def _new_id() -> str:
    return f"p30_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _chain(chain, et, status, msg="", md=None):
    prev = chain[-1].get("event_hash") if chain else ""
    ev = vac.create_audit_chain_event(et, status, msg, md or {},
                                       previous_hash=prev)
    return vac.append_chain_event(chain, ev)


def prepare_phase30_callable_invocation(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    adapter_name: str = "dummy_metadata_adapter",
    operator_id: str = "",
    approve: bool = False,
    kill_switch_enabled: bool = False,
    limit: int = 25,
) -> dict[str, Any]:
    chain: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    chain = _chain(chain, "preflight", "ok",
                    "phase30 prepare_phase30_callable_invocation begin",
                    {"approve": bool(approve),
                     "kill_switch_enabled": bool(kill_switch_enabled)})

    # Phase 29 packet (dry-run-ready)
    p29_result = p29.prepare_phase29_invocation(
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
    packet = p29_result.get("review_packet") or {}
    token = p29_result.get("invocation_consent_token") or {}
    if not packet or p29_result.get("status") == "refused":
        errors.append(vae.create_adapter_error(
            "UNSAFE_PAYLOAD", "phase29 refused upstream",
            severity="blocking"))
        chain = _chain(chain, "refusal", "refused",
                        "phase29 refused upstream")
        return _result(p29_result, None, None, None, None, None,
                        None, chain, errors, "refused")

    # Only dummy_metadata_adapter is allowed at this layer
    desc = cai.create_callable_adapter_descriptor(
        adapter_name=adapter_name or "dummy_metadata_adapter",
        adapter_type=("dummy_metadata_adapter"
                       if adapter_name in (None, "",
                                           "dummy_metadata_adapter")
                       else adapter_name),
        test_only=True)
    desc_val = cai.validate_callable_adapter_descriptor(desc)
    if not desc_val["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "non-dummy adapter requested",
            severity="blocking",
            metadata={"reasons": desc_val["reasons"]}))
        chain = _chain(chain, "refusal", "refused",
                        "non-dummy adapter rejected")
        return _result(p29_result, None, None, None, None, None,
                        None, chain, errors, "refused")

    request = cai.create_callable_adapter_request(packet, desc, token)
    # Pull token operator_id forward for pre-call consent verification
    request["invocation_token"] = token
    request["safety_summary"] = packet.get("safety_summary") or {}
    chain = _chain(chain, "consent_request", "ok",
                    "phase30 callable request built",
                    {"request_id": request.get("request_id")})

    # Kill switch
    ks_policy = eks.create_kill_switch_policy(
        enabled=bool(kill_switch_enabled),
        reason=("operator_kill_switch_enabled"
                if kill_switch_enabled else ""))
    ks_decision = eks.enforce_kill_switch(ks_policy, request)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if ks_decision["allow"] else "blocked",
                    ks_decision.get("reason", ""))
    if not ks_decision["allow"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            ks_decision.get("reason", "kill_switch"),
            severity="blocking"))
        return _result(p29_result, request, ks_decision, None, None,
                        None, None, chain, errors, "kill_switch_blocked")

    # Pre-call validation
    pre_res = pre.validate_pre_call_requirements(request, ks_policy)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if pre_res["ok"] else "blocked",
                    json.dumps(pre_res.get("reasons", [])))
    if not pre_res["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "pre-call validation failed",
            severity="blocking",
            metadata={"reasons": pre_res["reasons"]}))
        chain = _chain(chain, "refusal", "refused",
                        "pre-call validation failed")
        return _result(p29_result, request, ks_decision, pre_res,
                        None, None, None, chain, errors,
                        "pre_call_refused")

    # Dummy adapter call (metadata only — no audio, no engine, no IO)
    result = dma.call_dummy_metadata_adapter(request)
    result_val = dma.validate_dummy_metadata_result(result)
    chain = _chain(chain, "review_packet_created",
                    "ok" if result_val["ok"] else "warn",
                    json.dumps(result_val.get("reasons", [])),
                    {"result_id": result.get("result_id")})

    # Post-call validation
    post_res = post.validate_post_call_result(result, request)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if post_res["ok"] else "blocked",
                    json.dumps(post_res.get("reasons", [])))
    if not post_res["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "post-call validation failed",
            severity="blocking",
            metadata={"reasons": post_res["reasons"]}))

    # Receipt
    receipt = recv.create_invocation_receipt(
        request, result, pre_res, post_res, audit_chain=chain)
    receipt_val = recv.validate_invocation_receipt(receipt)
    chain = _chain(chain, "queue_enqueued",
                    "ok" if receipt_val["ok"] else "warn",
                    "phase30 receipt created",
                    {"receipt_id": receipt.get("receipt_id")})

    status = ("ok" if (pre_res["ok"] and post_res["ok"] and
                       result_val["ok"] and receipt_val["ok"])
              else "refused")
    chain = _chain(chain, "dry_run_complete",
                    "ok" if status == "ok" else "refused",
                    f"phase30 complete status={status}")
    return _result(p29_result, request, ks_decision, pre_res, result,
                    post_res, receipt, chain, errors, status)


def invoke_phase30_dummy_adapter(
    phase29_packet: dict[str, Any],
    operator_id: str = "",
    approve: bool = False,
    kill_switch_enabled: bool = False,
) -> dict[str, Any]:
    chain: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    desc = cai.create_callable_adapter_descriptor(
        "dummy_metadata_adapter",
        adapter_type="dummy_metadata_adapter",
        test_only=True)
    # Create an invocation token directly off the packet envelope shape
    env_like = {
        "envelope_id": (phase29_packet or {}).get("envelope_id") or "",
        "render_job": {"job_id":
                       (phase29_packet or {}).get("job_id") or ""},
    }
    token = ic.create_invocation_consent_token(
        env_like, operator_id=operator_id,
        approved=bool(approve),
        scope="dry_run_prepare", expires_in_seconds=300)
    request = cai.create_callable_adapter_request(
        phase29_packet or {}, desc, token)
    request["invocation_token"] = token
    request["safety_summary"] = (phase29_packet or {}).get(
        "safety_summary") or {}
    ks_policy = eks.create_kill_switch_policy(
        enabled=bool(kill_switch_enabled))
    ks_decision = eks.enforce_kill_switch(ks_policy, request)
    if not ks_decision["allow"]:
        return {
            "ok": False, "reason": "kill_switch_blocked",
            "kill_switch_decision": ks_decision, "phase": _PHASE,
        }
    pre_res = pre.validate_pre_call_requirements(request, ks_policy)
    if not pre_res["ok"]:
        return {
            "ok": False, "reason": "pre_call_failed",
            "pre_call_validation": pre_res, "phase": _PHASE,
        }
    result = dma.call_dummy_metadata_adapter(request)
    post_res = post.validate_post_call_result(result, request)
    receipt = recv.create_invocation_receipt(
        request, result, pre_res, post_res, audit_chain=chain)
    return {
        "ok": post_res["ok"] and pre_res["ok"],
        "adapter_request": request,
        "adapter_result": result,
        "pre_call_validation": pre_res,
        "post_call_validation": post_res,
        "invocation_receipt": receipt,
        "kill_switch_decision": ks_decision,
        "phase": _PHASE,
        "errors": errors,
    }


def validate_phase30_invocation_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    receipt = result.get("invocation_receipt") or {}
    return recv.validate_invocation_receipt(receipt)


def _result(p29_result, request, ks_decision, pre_res, adapter_result,
            post_res, receipt, chain, errors, status):
    return {
        "phase30_id": _new_id(),
        "phase29_packet": p29_result or {},
        "callable_adapter_request": request or {},
        "kill_switch_decision": ks_decision or {},
        "pre_call_validation": pre_res or {},
        "adapter_result": adapter_result or {},
        "post_call_validation": post_res or {},
        "invocation_receipt": receipt or {},
        "audit_chain": chain or [],
        "status": status,
        "next_allowed_actions": ["review", "refuse",
                                  "queue_for_phase31_audit"],
        "forbidden_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "errors": errors or [],
        "gap_notes": [],
        "phase": _PHASE,
    }


def demo_phase30_callable_invocations(limit: int = 12) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 12))
    scenarios = [
        ("Hello Luna", "Hi.", "conversation", None,
         "operator_local", True, False),
        ("Привет Луна", "Привет!", "conversation", "russian",
         "operator_local", True, False),
        ("Mix russian and english", "ok, давай.",
         "conversation", None, "operator_local", True, False),
        ("Teach me a Russian word", "", "teacher", "russian",
         "operator_local", True, False),
        ("approve=False refusal test", "", "conversation", None,
         "", False, False),
        ("kill switch test", "", "conversation", None,
         "operator_local", True, True),
        ("Speak english only", "", "conversation", "english",
         "operator_local", True, False),
        ("Use professional tone", "", "professional", None,
         "operator_local", True, False),
        ("Stop mixing languages", "", "conversation", None,
         "operator_local", True, False),
        ("Что нового?", "", "conversation", None,
         "operator_local", True, False),
        ("Use bilingual mode", "", "conversation", None,
         "operator_local", True, False),
        ("Slower russian", "", "conversation", "russian",
         "operator_local", True, False),
    ][:cap]
    out: list[dict[str, Any]] = []
    for ut, dt, mode, pref, op, ap, ks in scenarios:
        r = prepare_phase30_callable_invocation(
            user_text=ut, draft_response_text=dt,
            conversation_mode=mode, user_preference=pref,
            operator_id=op, approve=ap,
            kill_switch_enabled=ks)
        rc = r.get("invocation_receipt") or {}
        out.append({
            "user_text": ut,
            "status": r.get("status"),
            "execution_boundary_preserved":
                rc.get("execution_boundary_preserved"),
            "audio_generated": rc.get("audio_generated"),
            "tts_invoked": rc.get("tts_invoked"),
            "subprocess_used": rc.get("subprocess_used"),
            "kill_switch_blocked":
                not r.get("kill_switch_decision", {}).get(
                    "allow", True),
            "approve_requested": ap,
        })
    return {"demo": out, "count": len(out), "phase": _PHASE}


def write_phase30_runtime_report(
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
    "prepare_phase30_callable_invocation",
    "invoke_phase30_dummy_adapter",
    "validate_phase30_invocation_result",
    "demo_phase30_callable_invocations",
    "write_phase30_runtime_report",
]
