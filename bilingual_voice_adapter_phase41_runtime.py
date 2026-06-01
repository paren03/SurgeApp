"""Phase 41 - Five-Adapter Runtime (standalone).

Composes Phase 29 packet -> Phase 41 adapter request (with
voice_memory_state) -> 5-way selection -> kill switch -> pre-call
-> adapter dispatch (memory adapter routes through the new
Phase 41 callable; the other four route through their existing
in-process modules) -> post-call -> receipts -> signed witness
pipeline (Phase 32 signing + Phase 34 witness + Phase 35
exchange + optional Phase 36 handoff) -> governance recheck ->
result verification -> Phase 40-style replay projection.
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
import bilingual_voice_phase41_adapter_interface as p41i
import bilingual_voice_phase41_selection_policy as p41s
import bilingual_voice_phase37_signed_witness_pipeline as p37p
import bilingual_voice_phase41_governance_recheck as p41gr
import bilingual_voice_phase41_result_verifier as p41rv
import bilingual_voice_phase41_replay_bridge as p41rb
import bilingual_voice_dummy_metadata_adapter as dma
import bilingual_segment_metadata_adapter as bsma
import bilingual_prosody_density_metadata_adapter as pdma
import bilingual_safety_redaction_trace_adapter as srta
import bilingual_memory_continuity_audit_adapter as mcaa
import bilingual_voice_emergency_kill_switch as eks
import bilingual_voice_pre_call_validator as pre
import bilingual_voice_post_call_validator as post
import bilingual_voice_invocation_receipt as recv
import bilingual_voice_phase31_selection_receipt as p31r
import bilingual_voice_adapter_errors as vae


_PHASE = "phase41.runtime.v1"


def _new_id() -> str:
    return f"p41_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _chain(chain, et, status, msg="", md=None):
    prev = chain[-1].get("event_hash") if chain else ""
    ev = vac.create_audit_chain_event(
        et, status, msg, md or {}, previous_hash=prev)
    return vac.append_chain_event(chain, ev)


def _dispatch_adapter(descriptor: dict[str, Any],
                      request: dict[str, Any]) -> dict[str, Any]:
    at = str(descriptor.get("adapter_type") or "")
    if at == "dummy_metadata_adapter":
        return dma.call_dummy_metadata_adapter(request)
    if at == "bilingual_segment_metadata_adapter":
        return bsma.call_bilingual_segment_metadata_adapter(
            request)
    if at == "prosody_density_metadata_adapter":
        return pdma.call_prosody_density_metadata_adapter(
            request)
    if at == "safety_redaction_trace_metadata_adapter":
        return srta.call_safety_redaction_trace_adapter(
            request)
    if at == "memory_continuity_audit_metadata_adapter":
        return mcaa.call_memory_continuity_audit_adapter(
            request)
    return {
        "result_id": "", "adapter_name": at,
        "status": "refused_disallowed_adapter",
        "produced_audio": False, "invoked_tts": False,
        "used_subprocess": False, "used_network": False,
        "wrote_files": False, "phase": _PHASE,
    }


def _phase41_pre_call(
    request: dict[str, Any],
    ks_policy: dict[str, Any],
) -> dict[str, Any]:
    sub_consent = pre.verify_invocation_consent(request)
    sub_calltime = pre.verify_calltime_boundary(request)
    sub_packet = pre.verify_phase29_packet(request)
    sub_review = pre.verify_operator_review_packet(request)
    sub_desc = p41i.validate_phase41_adapter_descriptor(
        request.get("adapter_descriptor") or {})
    sub_exec = pre.verify_no_execution_fields_pre_call(request)
    reasons: list[str] = []
    if not eks.enforce_kill_switch(ks_policy,
                                    request)["allow"]:
        reasons.append("kill_switch")
    for label, sr in (("invocation_consent", sub_consent),
                       ("calltime_boundary", sub_calltime),
                       ("phase29_packet", sub_packet),
                       ("operator_review_packet", sub_review),
                       ("phase41_adapter_descriptor",
                        sub_desc),
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
            "phase41_adapter_descriptor": sub_desc,
            "no_execution_fields": sub_exec,
        },
        "phase": "phase41.pre_call.v1",
    }


def _result(p29_result, request, selection, adapter_result,
            selection_receipt, invocation_receipt,
            signed_pipeline, governance, result_verification,
            replay_projection, chain, errors, status):
    return {
        "phase41_id": _new_id(),
        "phase29_packet": p29_result or {},
        "phase41_adapter_request": request or {},
        "selection_choice": selection or {},
        "selected_adapter_result": adapter_result or {},
        "selection_receipt": selection_receipt or {},
        "invocation_receipt": invocation_receipt or {},
        "signed_witness_pipeline": signed_pipeline or {},
        "governance_recheck": governance or {},
        "result_verification": result_verification or {},
        "replay_projection": replay_projection or {},
        "audit_chain": chain or [],
        "status": status,
        "next_allowed_actions": [
            "review", "refuse", "compare_adapters",
            "verify_signed_evidence", "export_witness",
            "verify_exchange", "audit_replay",
            "memory_continuity_summary"],
        "forbidden_actions": [
            "generate_audio", "invoke_tts",
            "run_subprocess", "call_powershell",
            "call_sapi", "call_piper",
            "write_audio_file", "clone_voice",
            "network_call",
        ],
        "errors": errors or [],
        "gap_notes": [],
        "phase": _PHASE,
    }


def prepare_phase41_five_adapter_invocation(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    preferred_adapter: Optional[str] = None,
    voice_memory_state: Optional[dict[str, Any]] = None,
    operator_id: str = "",
    approve: bool = False,
    kill_switch_enabled: bool = False,
    sign_evidence: bool = True,
    include_witness_export: bool = True,
    include_exchange: bool = True,
    include_replay_projection: bool = True,
    consent_marker: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    chain: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    chain = _chain(chain, "preflight", "ok",
                    "phase41 prepare_phase41_five_adapter_invocation",
                    {"approve": bool(approve),
                     "kill_switch_enabled":
                         bool(kill_switch_enabled),
                     "preferred_adapter": preferred_adapter,
                     "sign_evidence": bool(sign_evidence),
                     "include_witness_export":
                         bool(include_witness_export),
                     "include_exchange":
                         bool(include_exchange),
                     "include_replay_projection":
                         bool(include_replay_projection)})

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
            "UNSAFE_PAYLOAD",
            "phase29 refused upstream", severity="blocking"))
        chain = _chain(chain, "refusal", "refused",
                        "phase29 refused upstream")
        return _result(p29_result, None, None, None, None,
                        None, None, None, None, None,
                        chain, errors, "refused")

    spoken = ((p29_result.get("phase28_result") or {})
               .get("dry_run_pipeline_result", {})
               .get("spoken_payload") or {})
    packet_aug = dict(packet)
    packet_aug["spoken_render_payload"] = spoken

    stub_desc = p41i.create_phase41_adapter_descriptor(
        "dummy_metadata_adapter",
        "dummy_metadata_adapter", test_only=True)
    stub_request = p41i.create_phase41_adapter_request(
        packet_aug, stub_desc, token,
        voice_memory_state=voice_memory_state)
    stub_request["invocation_token"] = token
    stub_request["safety_summary"] = (
        packet.get("safety_summary") or {})

    selection = p41s.choose_phase41_adapter(
        stub_request, preferred_adapter=preferred_adapter)
    chain = _chain(chain, "adapter_selection",
                    "ok" if selection.get("ok") else "blocked",
                    json.dumps([selection.get("reason", "")]))
    if not selection.get("ok") or not selection.get("chosen"):
        errors.append(vae.create_adapter_error(
            "UNKNOWN_ADAPTER",
            "no allowed adapter selected",
            severity="blocking"))
        return _result(p29_result, stub_request, selection,
                        None, None, None, None, None, None,
                        None, chain, errors,
                        "selection_failed")

    chosen_desc = selection["chosen"]
    request = p41i.create_phase41_adapter_request(
        packet_aug, chosen_desc, token,
        voice_memory_state=voice_memory_state)
    request["invocation_token"] = token
    request["safety_summary"] = stub_request[
        "safety_summary"]
    request["spoken_render_payload"] = spoken
    sp_segs = (spoken or {}).get("segments") or []
    if isinstance(sp_segs, list):
        request["segment_count"] = len(sp_segs)

    ks_policy = eks.create_kill_switch_policy(
        enabled=bool(kill_switch_enabled),
        reason=("operator_kill_switch_enabled"
                if kill_switch_enabled else ""))
    ks_decision = eks.enforce_kill_switch(ks_policy,
                                            request)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if ks_decision["allow"] else "blocked",
                    ks_decision.get("reason", ""))
    if not ks_decision["allow"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            ks_decision.get("reason", "kill_switch"),
            severity="blocking"))
        return _result(p29_result, request, selection, None,
                        None, None, None, None, None, None,
                        chain, errors, "kill_switch_blocked")

    pre_res = _phase41_pre_call(request, ks_policy)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if pre_res["ok"] else "blocked",
                    json.dumps(pre_res.get("reasons", [])))
    if not pre_res["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "phase41 pre-call failed",
            severity="blocking",
            metadata={"reasons": pre_res["reasons"]}))
        return _result(p29_result, request, selection, None,
                        None, None, None, None, None, None,
                        chain, errors, "pre_call_refused")

    result = _dispatch_adapter(chosen_desc, request)
    chain = _chain(chain, "review_packet_created",
                    "ok" if result.get("status") in
                    ("metadata_only_ok", "ok") else "warn",
                    "phase41 dispatch complete",
                    {"result_id": result.get("result_id"),
                     "adapter_type":
                         chosen_desc.get("adapter_type")})

    post_res = post.validate_post_call_result(result,
                                                request)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if post_res["ok"] else "blocked",
                    json.dumps(post_res.get("reasons", [])))
    if not post_res["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "phase41 post-call failed",
            severity="blocking",
            metadata={"reasons": post_res.get("reasons")}))

    invocation_receipt = recv.create_invocation_receipt(
        request, result, pre_res, post_res,
        audit_chain=chain)
    selection_receipt = p31r.create_selection_receipt(
        request, selection, adapter_result=result,
        comparison=None, audit_chain=chain)

    pre_pipeline_status = ("ok" if (pre_res["ok"]
                                      and post_res["ok"])
                            else "refused")
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
        signed_pipeline = \
            p37p.create_phase37_signed_witness_pipeline(
                pipeline_input,
                operator_id=operator_id,
                consent_marker=consent_marker,
                include_handoff=False)
        chain = _chain(chain, "dry_run_complete",
                        "ok" if signed_pipeline.get(
                            "status") == "ok" else "refused",
                        "phase41 signed witness pipeline "
                        "built")
    else:
        chain = _chain(chain, "dry_run_complete",
                        "refused",
                        "phase41 signed witness pipeline "
                        "skipped or upstream not ok")

    governance = {
        "phase30_strict":
            p41gr.verify_phase41_phase30_strictness(),
        "phase31_boundary":
            p41gr.verify_phase41_phase31_boundary(),
        "phase33_boundary":
            p41gr.verify_phase41_phase33_boundary(),
        "phase37_boundary":
            p41gr.verify_phase41_phase37_boundary(),
        "phase41_boundary":
            p41gr.verify_phase41_five_adapter_boundary(),
        "allowed_adapters_only":
            p41gr.verify_phase41_allowed_adapters_only(
                [invocation_receipt, selection_receipt,
                 result]),
        "metadata_only":
            p41gr.verify_phase41_metadata_only_results(
                [result]),
        "memory_privacy_boundary":
            p41gr.verify_phase41_memory_privacy_boundary(
                [result]),
        "signed_evidence_required": {"ok": True}
            if pre_pipeline_status != "ok" else
            p41gr.verify_phase41_signed_evidence_required(
                {"status": "ok",
                 "signed_witness_pipeline":
                     signed_pipeline}),
        "witness_export_required": {"ok": True}
            if pre_pipeline_status != "ok" else
            p41gr.verify_phase41_witness_export_required(
                signed_pipeline),
        "exchange_required": {"ok": True}
            if pre_pipeline_status != "ok" else
            p41gr.verify_phase41_exchange_required(
                signed_pipeline),
        "no_secret_leakage":
            p41gr.verify_phase41_no_secret_leakage(
                [invocation_receipt, selection_receipt,
                 result, signed_pipeline]),
    }
    governance["ok"] = all(
        v.get("ok") is True for v in governance.values()
        if isinstance(v, dict))

    # Compute replay projection BEFORE finalising status
    # (so its absence can cause status=refused)
    pre_proj_status = ("ok"
                       if (pre_res["ok"] and post_res["ok"]
                           and bool(signed_pipeline)
                           and signed_pipeline.get(
                               "status") == "ok")
                       else "refused")
    interim = _result(p29_result, request, selection, result,
                       selection_receipt, invocation_receipt,
                       signed_pipeline, governance, None,
                       None, chain, errors, pre_proj_status)
    replay_projection: dict[str, Any] = {}
    if pre_proj_status == "ok" and include_replay_projection:
        replay_projection = \
            p41rb.create_phase41_replay_projection(interim)
        chain = _chain(chain, "review_packet_created",
                        "ok",
                        "phase41 replay projection built")

    status = (
        "ok" if (pre_res["ok"] and post_res["ok"]
                  and bool(signed_pipeline)
                  and signed_pipeline.get("status") == "ok"
                  and (not include_replay_projection
                       or bool(replay_projection)))
        else "refused")

    final = _result(p29_result, request, selection, result,
                     selection_receipt, invocation_receipt,
                     signed_pipeline, governance, None,
                     replay_projection, chain, errors,
                     status)
    final["result_verification"] = \
        p41rv.verify_phase41_complete_output(final)
    if not final["result_verification"]["ok"] and \
            final["status"] == "ok":
        final["status"] = "refused"
    return final


def invoke_phase41_selected_adapter(
    phase29_packet: dict[str, Any],
    preferred_adapter: Optional[str] = None,
    voice_memory_state: Optional[dict[str, Any]] = None,
    operator_id: str = "",
    approve: bool = False,
    kill_switch_enabled: bool = False,
    sign_evidence: bool = True,
    include_witness_export: bool = True,
    include_exchange: bool = True,
    include_replay_projection: bool = True,
    consent_marker: str = "",
) -> dict[str, Any]:
    env_like = {
        "envelope_id": (phase29_packet or {}).get(
            "envelope_id") or "",
        "render_job": {
            "job_id": (phase29_packet or {}).get(
                "job_id") or ""},
    }
    token = ic.create_invocation_consent_token(
        env_like, operator_id=operator_id,
        approved=bool(approve),
        scope="dry_run_prepare", expires_in_seconds=300)
    stub_desc = p41i.create_phase41_adapter_descriptor(
        "dummy_metadata_adapter",
        "dummy_metadata_adapter", test_only=True)
    stub_req = p41i.create_phase41_adapter_request(
        phase29_packet or {}, stub_desc, token,
        voice_memory_state=voice_memory_state)
    stub_req["invocation_token"] = token
    stub_req["safety_summary"] = (
        (phase29_packet or {}).get("safety_summary") or {})
    selection = p41s.choose_phase41_adapter(
        stub_req, preferred_adapter=preferred_adapter)
    if not selection.get("ok") or not selection.get("chosen"):
        return {"ok": False, "reason": "no_adapter_selected",
                "selection": selection, "phase": _PHASE}
    chosen_desc = selection["chosen"]
    request = p41i.create_phase41_adapter_request(
        phase29_packet or {}, chosen_desc, token,
        voice_memory_state=voice_memory_state)
    request["invocation_token"] = token
    request["safety_summary"] = stub_req["safety_summary"]
    ks_policy = eks.create_kill_switch_policy(
        enabled=bool(kill_switch_enabled))
    ks_decision = eks.enforce_kill_switch(ks_policy,
                                            request)
    if not ks_decision["allow"]:
        return {"ok": False,
                "reason": "kill_switch_blocked",
                "kill_switch_decision": ks_decision,
                "phase": _PHASE}
    pre_res = _phase41_pre_call(request, ks_policy)
    if not pre_res["ok"]:
        return {"ok": False, "reason": "pre_call_failed",
                "pre_call_validation": pre_res,
                "phase": _PHASE}
    result = _dispatch_adapter(chosen_desc, request)
    post_res = post.validate_post_call_result(result,
                                                request)
    pipeline: dict[str, Any] = {}
    if (sign_evidence and include_witness_export
            and include_exchange and post_res["ok"]):
        pipeline = p37p.create_phase37_signed_witness_pipeline(
            {"audit_chain": [], "invocation_receipt": {},
             "selection_receipt": {},
             "selected_adapter_result": result,
             "status": "ok"},
            operator_id=operator_id,
            consent_marker=consent_marker,
            include_handoff=False)
    proj: dict[str, Any] = {}
    if include_replay_projection and pipeline.get(
            "status") == "ok":
        proj = p41rb.create_phase41_replay_projection({
            "phase41_id": _new_id(),
            "selection_choice": selection,
            "signed_witness_pipeline": pipeline,
            "invocation_receipt": {},
            "result_verification": {"ok": True},
            "governance_recheck": {"ok": True},
        })
    return {
        "ok": (post_res["ok"] and pre_res["ok"]
                and bool(pipeline)
                and pipeline.get("status") == "ok"
                and (not include_replay_projection
                     or bool(proj))),
        "selection": selection,
        "adapter_request": request,
        "selected_adapter_result": result,
        "signed_witness_pipeline": pipeline,
        "replay_projection": proj,
        "phase": _PHASE,
    }


def validate_phase41_invocation_output(
    output: Any,
) -> dict[str, Any]:
    return p41rv.verify_phase41_complete_output(output)


def demo_phase41_five_adapter_invocations(
    limit: int = 12,
) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 12))
    memory_state = {
        "preferred_language_mode": "english",
        "preferred_spoken_mode": "neutral",
        "code_switch_density": 0.05,
        "correction_pattern_count": 2,
        "recent_language_modes": ["english", "russian"],
        "recent_correction_kinds": ["pronoun"],
        "continuity_confidence_score": 0.82,
        "memory_scope": "session",
        "persistence_status": "ephemeral",
        "recent_drift_signal": False,
        "voice_style_continuity": "stable",
        "user_preference_drift": False,
        "session_memory_bounded": True,
        "recent_turn_count": 4,
    }
    scenarios = [
        ("Hello Luna", "Hi.", "conversation", None, None,
         None),
        ("Привет Луна", "Привет!", "conversation",
         "russian", None, None),
        ("Mix russian and english", "ok, давай.",
         "conversation", None,
         "bilingual_segment_metadata_adapter", None),
        ("Memory continuity audit drill", "",
         "conversation", None,
         "memory_continuity_audit_metadata_adapter",
         memory_state),
        ("approve=False refusal test", "", "conversation",
         None, None, None),
        ("kill switch test", "", "conversation", None,
         None, None),
        ("Slow with pauses and emphasis", "",
         "conversation", None,
         "prosody_density_metadata_adapter", None),
        ("Safety redaction check", "", "conversation",
         None,
         "safety_redaction_trace_metadata_adapter", None),
        ("Continuity drift signal", "", "conversation",
         None, None,
         {**memory_state, "recent_drift_signal": True,
          "user_preference_drift": True}),
        ("Simple English", "", "conversation",
         "english", "dummy_metadata_adapter", None),
        ("Что нового?", "", "conversation", None, None,
         None),
        ("Operator hand-off rehearsal", "",
         "conversation", None, None, None),
    ][:cap]
    out: list[dict[str, Any]] = []
    for ut, dt, mode, pref, pref_adapter, vms in scenarios:
        approve = not ("approve=False" in ut)
        ks = ("kill switch" in ut)
        r = prepare_phase41_five_adapter_invocation(
            user_text=ut, draft_response_text=dt,
            conversation_mode=mode, user_preference=pref,
            preferred_adapter=pref_adapter,
            voice_memory_state=vms,
            operator_id="operator_local",
            approve=approve, kill_switch_enabled=ks,
            sign_evidence=True,
            include_witness_export=True,
            include_exchange=True,
            include_replay_projection=True)
        sel = r.get("selection_choice") or {}
        chosen = ((sel.get("chosen") or {}).get(
            "adapter_name")
                   if isinstance(sel.get("chosen"), dict)
                   else None)
        pipe = r.get("signed_witness_pipeline") or {}
        out.append({
            "user_text": ut,
            "status": r.get("status"),
            "selected_adapter": chosen,
            "signed_pipeline_status": pipe.get("status"),
            "approve_requested": approve,
            "kill_switch_blocked":
                r.get("status") == "kill_switch_blocked",
            "replay_projection_present":
                bool(r.get("replay_projection")),
        })
    return {"demo": out, "count": len(out),
            "phase": _PHASE}


def write_phase41_runtime_report(
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
    "prepare_phase41_five_adapter_invocation",
    "invoke_phase41_selected_adapter",
    "validate_phase41_invocation_output",
    "demo_phase41_five_adapter_invocations",
    "write_phase41_runtime_report",
]
