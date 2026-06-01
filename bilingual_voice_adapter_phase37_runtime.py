"""Phase 37 - Four-Adapter Runtime (standalone).

Composes Phase 29 packet → Phase 37 adapter request → 4-way
selection → kill switch → pre-call → adapter dispatch → post-call →
receipts → signed witness pipeline (signed evidence + witness export
+ exchange + optional handoff) → governance recheck → result
verification.
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
import bilingual_voice_phase37_adapter_interface as p37i
import bilingual_voice_phase37_selection_policy as p37s
import bilingual_voice_phase37_signed_witness_pipeline as p37p
import bilingual_voice_phase37_governance_recheck as p37gr
import bilingual_voice_phase37_result_verifier as p37rv
import bilingual_voice_dummy_metadata_adapter as dma
import bilingual_segment_metadata_adapter as bsma
import bilingual_prosody_density_metadata_adapter as pdma
import bilingual_safety_redaction_trace_adapter as srta
import bilingual_voice_emergency_kill_switch as eks
import bilingual_voice_pre_call_validator as pre
import bilingual_voice_post_call_validator as post
import bilingual_voice_invocation_receipt as recv
import bilingual_voice_phase31_selection_receipt as p31r
import bilingual_voice_adapter_errors as vae


_PHASE = "phase37.runtime.v1"


def _new_id() -> str:
    return f"p37_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _chain(chain, et, status, msg="", md=None):
    prev = chain[-1].get("event_hash") if chain else ""
    ev = vac.create_audit_chain_event(et, status, msg, md or {},
                                       previous_hash=prev)
    return vac.append_chain_event(chain, ev)


def _dispatch_adapter(descriptor: dict[str, Any],
                      request: dict[str, Any]) -> dict[str, Any]:
    at = str(descriptor.get("adapter_type") or "")
    if at == "dummy_metadata_adapter":
        return dma.call_dummy_metadata_adapter(request)
    if at == "bilingual_segment_metadata_adapter":
        return bsma.call_bilingual_segment_metadata_adapter(request)
    if at == "prosody_density_metadata_adapter":
        return pdma.call_prosody_density_metadata_adapter(request)
    if at == "safety_redaction_trace_metadata_adapter":
        return srta.call_safety_redaction_trace_adapter(request)
    return {
        "result_id": "", "adapter_name": at,
        "status": "refused_disallowed_adapter",
        "produced_audio": False, "invoked_tts": False,
        "used_subprocess": False, "used_network": False,
        "wrote_files": False, "phase": _PHASE,
    }


def _phase37_pre_call(
    request: dict[str, Any],
    ks_policy: dict[str, Any],
) -> dict[str, Any]:
    sub_consent = pre.verify_invocation_consent(request)
    sub_calltime = pre.verify_calltime_boundary(request)
    sub_packet = pre.verify_phase29_packet(request)
    sub_review = pre.verify_operator_review_packet(request)
    sub_desc = p37i.validate_phase37_adapter_descriptor(
        request.get("adapter_descriptor") or {})
    sub_exec = pre.verify_no_execution_fields_pre_call(request)
    reasons: list[str] = []
    if not eks.enforce_kill_switch(ks_policy, request)["allow"]:
        reasons.append("kill_switch")
    for label, sr in (("invocation_consent", sub_consent),
                       ("calltime_boundary", sub_calltime),
                       ("phase29_packet", sub_packet),
                       ("operator_review_packet", sub_review),
                       ("phase37_adapter_descriptor", sub_desc),
                       ("no_execution_fields", sub_exec)):
        if not sr["ok"]:
            reasons.append(f"{label}_failed:" +
                           ",".join(sr.get("reasons", [])))
    return {
        "ok": not reasons, "reasons": reasons,
        "execution_blocked": True,
        "sub_results": {
            "invocation_consent": sub_consent,
            "calltime_boundary": sub_calltime,
            "phase29_packet": sub_packet,
            "operator_review_packet": sub_review,
            "phase37_adapter_descriptor": sub_desc,
            "no_execution_fields": sub_exec,
        },
        "phase": "phase37.pre_call.v1",
    }


def prepare_phase37_four_adapter_invocation(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    preferred_adapter: Optional[str] = None,
    operator_id: str = "",
    approve: bool = False,
    kill_switch_enabled: bool = False,
    sign_evidence: bool = True,
    include_witness_export: bool = True,
    include_exchange: bool = True,
    include_handoff: bool = False,
    consent_marker: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    chain: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    chain = _chain(chain, "preflight", "ok",
                    "phase37 prepare_phase37_four_adapter_invocation",
                    {"approve": bool(approve),
                     "kill_switch_enabled": bool(kill_switch_enabled),
                     "preferred_adapter": preferred_adapter,
                     "sign_evidence": bool(sign_evidence),
                     "include_witness_export":
                         bool(include_witness_export),
                     "include_exchange": bool(include_exchange),
                     "include_handoff": bool(include_handoff)})

    p29_result = p29.prepare_phase29_invocation(
        user_text=user_text,
        draft_response_text=draft_response_text,
        conversation_state=conversation_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
        operator_id=operator_id,
        approve=bool(approve), limit=limit,
    )
    packet = p29_result.get("review_packet") or {}
    token = p29_result.get("invocation_consent_token") or {}
    if p29_result.get("status") == "refused" or not packet:
        errors.append(vae.create_adapter_error(
            "UNSAFE_PAYLOAD", "phase29 refused upstream",
            severity="blocking"))
        chain = _chain(chain, "refusal", "refused",
                        "phase29 refused upstream")
        return _result(p29_result, None, None, None, None, None,
                        None, None, None, chain, errors, "refused")

    spoken = ((p29_result.get("phase28_result") or {})
               .get("dry_run_pipeline_result", {})
               .get("spoken_payload") or {})
    packet_aug = dict(packet)
    packet_aug["spoken_render_payload"] = spoken

    stub_desc = p37i.create_phase37_adapter_descriptor(
        "dummy_metadata_adapter",
        "dummy_metadata_adapter", test_only=True)
    stub_request = p37i.create_phase37_adapter_request(
        packet_aug, stub_desc, token)
    stub_request["invocation_token"] = token
    stub_request["safety_summary"] = (packet.get("safety_summary")
                                         or {})

    selection = p37s.choose_phase37_adapter(
        stub_request, preferred_adapter=preferred_adapter)
    chain = _chain(chain, "adapter_selection",
                    "ok" if selection.get("ok") else "blocked",
                    json.dumps([selection.get("reason", "")]))
    if not selection.get("ok") or not selection.get("chosen"):
        errors.append(vae.create_adapter_error(
            "UNKNOWN_ADAPTER",
            "no allowed adapter selected", severity="blocking"))
        return _result(p29_result, stub_request, selection, None,
                        None, None, None, None, None, chain,
                        errors, "selection_failed")

    chosen_desc = selection["chosen"]
    request = p37i.create_phase37_adapter_request(
        packet_aug, chosen_desc, token)
    request["invocation_token"] = token
    request["safety_summary"] = stub_request["safety_summary"]
    request["spoken_render_payload"] = spoken
    sp_segs = (spoken or {}).get("segments") or []
    if isinstance(sp_segs, list):
        request["segment_count"] = len(sp_segs)

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
        return _result(p29_result, request, selection, None, None,
                        None, None, None, None, chain, errors,
                        "kill_switch_blocked")

    pre_res = _phase37_pre_call(request, ks_policy)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if pre_res["ok"] else "blocked",
                    json.dumps(pre_res.get("reasons", [])))
    if not pre_res["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "phase37 pre-call failed", severity="blocking",
            metadata={"reasons": pre_res["reasons"]}))
        return _result(p29_result, request, selection, None, None,
                        None, None, None, None, chain, errors,
                        "pre_call_refused")

    result = _dispatch_adapter(chosen_desc, request)
    chain = _chain(chain, "review_packet_created",
                    "ok" if result.get("status") ==
                    "metadata_only_ok" else "warn",
                    "phase37 dispatch complete",
                    {"result_id": result.get("result_id"),
                     "adapter_type":
                         chosen_desc.get("adapter_type")})

    post_res = post.validate_post_call_result(result, request)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if post_res["ok"] else "blocked",
                    json.dumps(post_res.get("reasons", [])))
    if not post_res["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "phase37 post-call failed", severity="blocking",
            metadata={"reasons": post_res.get("reasons")}))

    invocation_receipt = recv.create_invocation_receipt(
        request, result, pre_res, post_res, audit_chain=chain)
    selection_receipt = p31r.create_selection_receipt(
        request, selection, adapter_result=result,
        comparison=None, audit_chain=chain)

    pre_pipeline_status = "ok" if (pre_res["ok"]
                                     and post_res["ok"]) else "refused"
    pipeline_input = {
        "audit_chain": chain,
        "invocation_receipt": invocation_receipt,
        "selection_receipt": selection_receipt,
        "selected_adapter_result": result,
        "status": pre_pipeline_status,
    }
    signed_pipeline: dict[str, Any] = {}
    if pre_pipeline_status == "ok" and sign_evidence and \
            include_witness_export and include_exchange:
        signed_pipeline = p37p.create_phase37_signed_witness_pipeline(
            pipeline_input,
            operator_id=operator_id,
            consent_marker=consent_marker,
            include_handoff=bool(include_handoff))
        chain = _chain(chain, "dry_run_complete",
                        "ok" if signed_pipeline.get(
                            "status") == "ok" else "refused",
                        "phase37 signed witness pipeline built")
    else:
        chain = _chain(chain, "dry_run_complete",
                        "refused",
                        "phase37 signed witness pipeline skipped"
                        " or upstream not ok")

    governance = {
        "phase30_strict":
            p37gr.verify_phase37_phase30_strictness(),
        "phase31_boundary":
            p37gr.verify_phase37_phase31_boundary(),
        "phase33_boundary":
            p37gr.verify_phase37_phase33_boundary(),
        "phase37_boundary":
            p37gr.verify_phase37_four_adapter_boundary(),
        "phase36_secret_boundary":
            p37gr.verify_phase37_phase36_secret_boundary(),
        "allowed_adapters_only":
            p37gr.verify_phase37_allowed_adapters_only(
                [invocation_receipt, selection_receipt, result]),
        "metadata_only":
            p37gr.verify_phase37_metadata_only_results([result]),
        "signed_evidence_required": {"ok": True}
            if pre_pipeline_status != "ok" else
            p37gr.verify_phase37_signed_evidence_required(
                {"status": "ok",
                 "signed_witness_pipeline": signed_pipeline}),
        "witness_export_required": {"ok": True}
            if pre_pipeline_status != "ok" else
            p37gr.verify_phase37_witness_export_required(
                signed_pipeline),
        "exchange_required": {"ok": True}
            if pre_pipeline_status != "ok" else
            p37gr.verify_phase37_exchange_required(
                signed_pipeline),
        "no_secret_leakage":
            p37gr.verify_phase37_no_secret_leakage(
                [invocation_receipt, selection_receipt, result,
                 signed_pipeline]),
    }

    status = ("ok" if (pre_res["ok"] and post_res["ok"] and
                        bool(signed_pipeline) and
                        signed_pipeline.get("status") == "ok")
              else "refused")

    final = _result(p29_result, request, selection, result,
                     selection_receipt, invocation_receipt,
                     signed_pipeline, governance, None,
                     chain, errors, status)
    final["result_verification"] = \
        p37rv.verify_phase37_complete_output(final)
    if not final["result_verification"]["ok"] and \
            final["status"] == "ok":
        final["status"] = "refused"
    return final


def invoke_phase37_selected_adapter(
    phase29_packet: dict[str, Any],
    preferred_adapter: Optional[str] = None,
    operator_id: str = "",
    approve: bool = False,
    kill_switch_enabled: bool = False,
    sign_evidence: bool = True,
    include_witness_export: bool = True,
    include_exchange: bool = True,
    include_handoff: bool = False,
    consent_marker: str = "",
) -> dict[str, Any]:
    env_like = {
        "envelope_id": (phase29_packet or {}).get("envelope_id")
            or "",
        "render_job": {"job_id":
                       (phase29_packet or {}).get("job_id") or ""},
    }
    token = ic.create_invocation_consent_token(
        env_like, operator_id=operator_id,
        approved=bool(approve),
        scope="dry_run_prepare", expires_in_seconds=300)
    stub_desc = p37i.create_phase37_adapter_descriptor(
        "dummy_metadata_adapter",
        "dummy_metadata_adapter", test_only=True)
    stub_req = p37i.create_phase37_adapter_request(
        phase29_packet or {}, stub_desc, token)
    stub_req["invocation_token"] = token
    stub_req["safety_summary"] = (phase29_packet or {}).get(
        "safety_summary") or {}
    selection = p37s.choose_phase37_adapter(
        stub_req, preferred_adapter=preferred_adapter)
    if not selection.get("ok") or not selection.get("chosen"):
        return {"ok": False, "reason": "no_adapter_selected",
                "selection": selection, "phase": _PHASE}
    chosen_desc = selection["chosen"]
    request = p37i.create_phase37_adapter_request(
        phase29_packet or {}, chosen_desc, token)
    request["invocation_token"] = token
    request["safety_summary"] = stub_req["safety_summary"]
    ks_policy = eks.create_kill_switch_policy(
        enabled=bool(kill_switch_enabled))
    ks_decision = eks.enforce_kill_switch(ks_policy, request)
    if not ks_decision["allow"]:
        return {"ok": False, "reason": "kill_switch_blocked",
                "kill_switch_decision": ks_decision,
                "phase": _PHASE}
    pre_res = _phase37_pre_call(request, ks_policy)
    if not pre_res["ok"]:
        return {"ok": False, "reason": "pre_call_failed",
                "pre_call_validation": pre_res, "phase": _PHASE}
    result = _dispatch_adapter(chosen_desc, request)
    post_res = post.validate_post_call_result(result, request)
    pipeline: dict[str, Any] = {}
    if sign_evidence and include_witness_export and \
            include_exchange and post_res["ok"]:
        pipeline = p37p.create_phase37_signed_witness_pipeline({
            "audit_chain": [], "invocation_receipt": {},
            "selection_receipt": {},
            "selected_adapter_result": result, "status": "ok"},
            operator_id=operator_id,
            consent_marker=consent_marker,
            include_handoff=bool(include_handoff))
    return {
        "ok": post_res["ok"] and pre_res["ok"] and
              bool(pipeline) and pipeline.get("status") == "ok",
        "selection": selection,
        "adapter_request": request,
        "adapter_result": result,
        "pre_call_validation": pre_res,
        "post_call_validation": post_res,
        "signed_witness_pipeline": pipeline,
        "kill_switch_decision": ks_decision,
        "phase": _PHASE,
    }


def validate_phase37_invocation_output(output: Any) -> dict[str, Any]:
    return p37rv.verify_phase37_complete_output(output)


def _result(p29_result, request, selection, adapter_result,
            selection_receipt, invocation_receipt, signed_pipeline,
            governance, result_verification, chain, errors, status):
    return {
        "phase37_id": _new_id(),
        "phase29_packet": p29_result or {},
        "phase37_adapter_request": request or {},
        "selection_choice": selection or {},
        "selected_adapter_result": adapter_result or {},
        "selection_receipt": selection_receipt or {},
        "invocation_receipt": invocation_receipt or {},
        "signed_witness_pipeline": signed_pipeline or {},
        "governance_recheck": governance or {},
        "result_verification": result_verification or {},
        "audit_chain": chain or [],
        "status": status,
        "next_allowed_actions": ["review", "refuse",
                                  "compare_adapters",
                                  "verify_signed_evidence",
                                  "export_witness",
                                  "verify_exchange"],
        "forbidden_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "errors": errors or [],
        "gap_notes": [],
        "phase": _PHASE,
    }


def demo_phase37_four_adapter_invocations(
    limit: int = 12,
) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 12))
    scenarios = [
        ("Hello Luna", "Hi.", "conversation", None, None,
         "operator_local", True, False, True),
        ("Привет Луна", "Привет!", "conversation", "russian",
         None, "operator_local", True, False, True),
        ("Mix russian and english", "ok, давай.",
         "conversation", None, None,
         "operator_local", True, False, True),
        ("Teach me a Russian word", "", "teacher", "russian",
         "bilingual_segment_metadata_adapter",
         "operator_local", True, False, True),
        ("approve=False refusal test", "", "conversation",
         None, None, "", False, False, True),
        ("kill switch test", "", "conversation", None, None,
         "operator_local", True, True, True),
        ("Slow with pauses and emphasis", "",
         "conversation", None,
         "prosody_density_metadata_adapter",
         "operator_local", True, False, True),
        ("Safety redaction check", "", "conversation", None,
         "safety_redaction_trace_metadata_adapter",
         "operator_local", True, False, True),
        ("Use bilingual mode", "", "conversation", None,
         "bilingual_segment_metadata_adapter",
         "operator_local", True, False, True),
        ("Что нового?", "", "conversation", None, None,
         "operator_local", True, False, True),
        ("Slower russian", "", "conversation", "russian", None,
         "operator_local", True, False, True),
        ("Simple English", "", "conversation", "english",
         "dummy_metadata_adapter",
         "operator_local", True, False, True),
    ][:cap]
    out: list[dict[str, Any]] = []
    for ut, dt, mode, pref, pref_adapter, op, ap, ks, se in scenarios:
        r = prepare_phase37_four_adapter_invocation(
            user_text=ut, draft_response_text=dt,
            conversation_mode=mode, user_preference=pref,
            preferred_adapter=pref_adapter,
            operator_id=op, approve=ap,
            kill_switch_enabled=ks, sign_evidence=se,
            include_witness_export=True,
            include_exchange=True,
            include_handoff=False)
        sel = r.get("selection_choice") or {}
        chosen = ((sel.get("chosen") or {}).get("adapter_name")
                   if isinstance(sel.get("chosen"), dict) else None)
        pipe = r.get("signed_witness_pipeline") or {}
        out.append({
            "user_text": ut,
            "status": r.get("status"),
            "selected_adapter": chosen,
            "signed_pipeline_status": pipe.get("status"),
            "approve_requested": ap,
            "kill_switch_blocked":
                r.get("status") == "kill_switch_blocked",
        })
    return {"demo": out, "count": len(out), "phase": _PHASE}


def write_phase37_runtime_report(
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
    "prepare_phase37_four_adapter_invocation",
    "invoke_phase37_selected_adapter",
    "validate_phase37_invocation_output",
    "demo_phase37_four_adapter_invocations",
    "write_phase37_runtime_report",
]
