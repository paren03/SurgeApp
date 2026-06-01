"""Phase 31 - Multi-Adapter Runtime (standalone).

Composes Phase 30 callable invocation flow + Phase 31 selection
between the two permitted metadata-only adapters. approve=True still
cannot enable audio/TTS/subprocess; only dummy_metadata_adapter or
bilingual_segment_metadata_adapter can be invoked.
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
import bilingual_voice_phase31_adapter_interface as p31i
import bilingual_voice_phase31_selection_policy as p31s
import bilingual_voice_phase31_adapter_comparison as p31c
import bilingual_voice_phase31_selection_receipt as p31r
import bilingual_voice_phase31_post_call_equivalence as p31eq
import bilingual_voice_dummy_metadata_adapter as dma
import bilingual_segment_metadata_adapter as bsma
import bilingual_voice_emergency_kill_switch as eks
import bilingual_voice_pre_call_validator as pre
import bilingual_voice_post_call_validator as post
import bilingual_voice_invocation_receipt as recv
import bilingual_voice_adapter_errors as vae


_PHASE = "phase31.runtime.v1"


def _new_id() -> str:
    return f"p31_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _chain(chain, et, status, msg="", md=None):
    prev = chain[-1].get("event_hash") if chain else ""
    ev = vac.create_audit_chain_event(et, status, msg, md or {},
                                       previous_hash=prev)
    return vac.append_chain_event(chain, ev)


def _phase31_pre_call(
    request: dict[str, Any],
    ks_policy: dict[str, Any],
) -> dict[str, Any]:
    """Phase 31 pre-call: uses the Phase 30 sub-checks but substitutes
    the Phase 31 descriptor validator so both metadata adapters are
    accepted. Phase 30 module is not modified."""
    sub_consent = pre.verify_invocation_consent(request)
    sub_calltime = pre.verify_calltime_boundary(request)
    sub_packet = pre.verify_phase29_packet(request)
    sub_review = pre.verify_operator_review_packet(request)
    sub_desc = p31i.validate_phase31_adapter_descriptor(
        request.get("adapter_descriptor") or {})
    sub_exec = pre.verify_no_execution_fields_pre_call(request)
    reasons: list[str] = []
    if not eks.enforce_kill_switch(ks_policy, request)["allow"]:
        reasons.append("kill_switch")
    for label, sr in (("invocation_consent", sub_consent),
                       ("calltime_boundary", sub_calltime),
                       ("phase29_packet", sub_packet),
                       ("operator_review_packet", sub_review),
                       ("phase31_adapter_descriptor", sub_desc),
                       ("no_execution_fields", sub_exec)):
        if not sr["ok"]:
            reasons.append(f"{label}_failed:" +
                           ",".join(sr.get("reasons", [])))
    return {
        "ok": not reasons,
        "reasons": reasons,
        "execution_blocked": True,
        "sub_results": {
            "invocation_consent": sub_consent,
            "calltime_boundary": sub_calltime,
            "phase29_packet": sub_packet,
            "operator_review_packet": sub_review,
            "phase31_adapter_descriptor": sub_desc,
            "no_execution_fields": sub_exec,
        },
        "phase": "phase31.pre_call.v1",
    }


def _dispatch_adapter(descriptor: dict[str, Any],
                      request: dict[str, Any]) -> dict[str, Any]:
    """In-process dispatch to one of the two permitted callables."""
    at = str(descriptor.get("adapter_type") or "")
    if at == "dummy_metadata_adapter":
        return dma.call_dummy_metadata_adapter(request)
    if at == "bilingual_segment_metadata_adapter":
        return bsma.call_bilingual_segment_metadata_adapter(request)
    return {
        "result_id": "",
        "adapter_name": at,
        "status": "refused_disallowed_adapter",
        "produced_audio": False,
        "invoked_tts": False,
        "used_subprocess": False,
        "used_network": False,
        "wrote_files": False,
        "phase": _PHASE,
    }


def prepare_phase31_multi_adapter_invocation(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    preferred_adapter: Optional[str] = None,
    operator_id: str = "",
    approve: bool = False,
    kill_switch_enabled: bool = False,
    limit: int = 25,
) -> dict[str, Any]:
    chain: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    chain = _chain(chain, "preflight", "ok",
                    "phase31 prepare_phase31_multi_adapter_invocation",
                    {"approve": bool(approve),
                     "kill_switch_enabled": bool(kill_switch_enabled),
                     "preferred_adapter": preferred_adapter})

    p29_result = p29.prepare_phase29_invocation(
        user_text=user_text,
        draft_response_text=draft_response_text,
        conversation_state=conversation_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
        operator_id=operator_id,
        approve=bool(approve),
        limit=limit,
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
                        None, chain, errors, "refused")

    # Build a stub request to score / select against. The descriptor
    # for the stub-only path is set to the dummy adapter; we replace
    # it after selection.
    stub_desc = p31i.create_phase31_adapter_descriptor(
        "dummy_metadata_adapter",
        adapter_type="dummy_metadata_adapter", test_only=True)
    stub_request = p31i.create_phase31_adapter_request(
        packet, stub_desc, token)
    stub_request["invocation_token"] = token
    stub_request["safety_summary"] = (packet.get("safety_summary")
                                         or {})
    stub_request["spoken_render_payload"] = (
        (p29_result.get("phase28_result") or {}).get(
            "dry_run_pipeline_result", {}).get("spoken_payload") or {})

    # Selection
    selection = p31s.choose_phase31_adapter(
        stub_request, preferred_adapter=preferred_adapter)
    chain = _chain(chain, "adapter_selection",
                    "ok" if selection.get("ok") else "blocked",
                    json.dumps([selection.get("reason", "")]))
    if not selection.get("ok") or not selection.get("chosen"):
        errors.append(vae.create_adapter_error(
            "UNKNOWN_ADAPTER",
            "no allowed adapter selected", severity="blocking"))
        return _result(p29_result, stub_request, selection, None,
                        None, None, None, chain, errors,
                        "selection_failed")

    chosen_desc = selection["chosen"]
    # Rebuild request with the actually-chosen descriptor
    request = p31i.create_phase31_adapter_request(
        packet, chosen_desc, token)
    request["invocation_token"] = token
    request["safety_summary"] = stub_request["safety_summary"]
    request["spoken_render_payload"] = stub_request[
        "spoken_render_payload"]
    # Sync segment_count from the actual spoken_render_payload now
    # that it's been attached (Phase 29 packet does not carry it).
    sp_segs = (request["spoken_render_payload"] or {}).get(
        "segments") or []
    if isinstance(sp_segs, list):
        request["segment_count"] = len(sp_segs)
    sp_lang = (request["spoken_render_payload"] or {}).get(
        "language_mode")
    if sp_lang and not request.get("language_mode"):
        request["language_mode"] = sp_lang

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
        return _result(p29_result, request, selection, None, None,
                        None, None, chain, errors,
                        "kill_switch_blocked")

    # Phase 31 pre-call validation (substitutes Phase 31 descriptor
    # validator so both permitted metadata adapters pass).
    pre_res = _phase31_pre_call(request, ks_policy)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if pre_res["ok"] else "blocked",
                    json.dumps(pre_res.get("reasons", [])))
    if not pre_res["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "phase31 pre-call validation failed",
            severity="blocking",
            metadata={"reasons": pre_res["reasons"]}))
        return _result(p29_result, request, selection, None, None,
                        None, None, chain, errors,
                        "pre_call_refused")

    # Dispatch
    result = _dispatch_adapter(chosen_desc, request)
    chain = _chain(chain, "review_packet_created",
                    "ok" if result.get("status") ==
                    "metadata_only_ok" else "warn",
                    "phase31 dispatch complete",
                    {"result_id": result.get("result_id"),
                     "adapter_type":
                         chosen_desc.get("adapter_type")})

    # Post-call boundary (Phase 30 post-call) + Phase 31 equivalence
    post_res = post.validate_post_call_result(result, request)
    eq_res = p31eq.validate_phase31_result_boundary(result, request)
    chain = _chain(chain, "calltime_boundary",
                    "ok" if (post_res["ok"] and eq_res["ok"])
                    else "blocked",
                    json.dumps((post_res.get("reasons", []) +
                                eq_res.get("reasons", []))))
    if not post_res["ok"] or not eq_res["ok"]:
        errors.append(vae.create_adapter_error(
            "PHASE28_EXECUTION_BLOCKED",
            "phase31 post-call validation failed",
            severity="blocking",
            metadata={"post_reasons": post_res.get("reasons"),
                       "eq_reasons": eq_res.get("reasons")}))

    # Receipts
    invocation_receipt = recv.create_invocation_receipt(
        request, result, pre_res, post_res, audit_chain=chain)
    selection_receipt = p31r.create_selection_receipt(
        request, selection, adapter_result=result,
        comparison=None, audit_chain=chain)
    inv_rec_val = recv.validate_invocation_receipt(invocation_receipt)
    sel_rec_val = p31r.validate_selection_receipt(selection_receipt)
    chain = _chain(chain, "queue_enqueued",
                    "ok" if (inv_rec_val["ok"] and
                              sel_rec_val["ok"]) else "warn",
                    "phase31 receipts created",
                    {"invocation_receipt_id":
                         invocation_receipt.get("receipt_id"),
                     "selection_receipt_id":
                         selection_receipt.get("receipt_id")})

    status = ("ok" if (pre_res["ok"] and post_res["ok"] and
                       eq_res["ok"] and inv_rec_val["ok"] and
                       sel_rec_val["ok"]) else "refused")
    chain = _chain(chain, "dry_run_complete",
                    "ok" if status == "ok" else "refused",
                    f"phase31 complete status={status}")
    return _result(p29_result, request, selection, result, eq_res,
                    invocation_receipt, selection_receipt, chain,
                    errors, status)


def invoke_phase31_selected_adapter(
    phase29_packet: dict[str, Any],
    preferred_adapter: Optional[str] = None,
    operator_id: str = "",
    approve: bool = False,
    kill_switch_enabled: bool = False,
) -> dict[str, Any]:
    chain: list[dict[str, Any]] = []
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
    stub_desc = p31i.create_phase31_adapter_descriptor(
        "dummy_metadata_adapter",
        adapter_type="dummy_metadata_adapter", test_only=True)
    stub_req = p31i.create_phase31_adapter_request(
        phase29_packet or {}, stub_desc, token)
    stub_req["invocation_token"] = token
    stub_req["safety_summary"] = (phase29_packet or {}).get(
        "safety_summary") or {}
    selection = p31s.choose_phase31_adapter(
        stub_req, preferred_adapter=preferred_adapter)
    if not selection.get("ok") or not selection.get("chosen"):
        return {"ok": False, "reason": "no_adapter_selected",
                "selection": selection, "phase": _PHASE}
    chosen_desc = selection["chosen"]
    request = p31i.create_phase31_adapter_request(
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
    pre_res = _phase31_pre_call(request, ks_policy)
    if not pre_res["ok"]:
        return {"ok": False, "reason": "pre_call_failed",
                "pre_call_validation": pre_res, "phase": _PHASE}
    result = _dispatch_adapter(chosen_desc, request)
    post_res = post.validate_post_call_result(result, request)
    eq_res = p31eq.validate_phase31_result_boundary(result, request)
    return {
        "ok": post_res["ok"] and pre_res["ok"] and eq_res["ok"],
        "selection": selection,
        "adapter_request": request,
        "adapter_result": result,
        "pre_call_validation": pre_res,
        "post_call_validation": post_res,
        "post_call_equivalence": eq_res,
        "kill_switch_decision": ks_decision,
        "phase": _PHASE,
    }


def validate_phase31_invocation_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    receipt = result.get("invocation_receipt") or {}
    return recv.validate_invocation_receipt(receipt)


def _result(p29_result, request, selection, adapter_result,
            eq_res, invocation_receipt, selection_receipt,
            chain, errors, status):
    return {
        "phase31_id": _new_id(),
        "phase29_packet": p29_result or {},
        "phase31_adapter_request": request or {},
        "selection_choice": selection or {},
        "selected_adapter_result": adapter_result or {},
        "post_call_equivalence": eq_res or {},
        "invocation_receipt": invocation_receipt or {},
        "selection_receipt": selection_receipt or {},
        "audit_chain": chain or [],
        "status": status,
        "next_allowed_actions": ["review", "refuse",
                                  "compare_adapters"],
        "forbidden_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "errors": errors or [],
        "gap_notes": [],
        "phase": _PHASE,
    }


def demo_phase31_multi_adapter_invocations(
    limit: int = 12,
) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 12))
    scenarios = [
        ("Hello Luna", "Hi.", "conversation", None, None,
         "operator_local", True, False),
        ("Привет Луна", "Привет!", "conversation", "russian", None,
         "operator_local", True, False),
        ("Mix russian and english", "ok, давай.",
         "conversation", None, None, "operator_local", True, False),
        ("Teach me a Russian word", "", "teacher", "russian",
         "bilingual_segment_metadata_adapter",
         "operator_local", True, False),
        ("approve=False refusal test", "", "conversation", None,
         None, "", False, False),
        ("kill switch test", "", "conversation", None, None,
         "operator_local", True, True),
        ("Speak english only", "", "conversation", "english",
         "dummy_metadata_adapter", "operator_local", True, False),
        ("Use professional tone", "", "professional", None, None,
         "operator_local", True, False),
        ("Stop mixing languages", "", "conversation", None, None,
         "operator_local", True, False),
        ("Что нового?", "", "conversation", None, None,
         "operator_local", True, False),
        ("Use bilingual mode", "", "conversation", None,
         "bilingual_segment_metadata_adapter",
         "operator_local", True, False),
        ("Slower russian", "", "conversation", "russian", None,
         "operator_local", True, False),
    ][:cap]
    out: list[dict[str, Any]] = []
    for ut, dt, mode, pref, pref_adapter, op, ap, ks in scenarios:
        r = prepare_phase31_multi_adapter_invocation(
            user_text=ut, draft_response_text=dt,
            conversation_mode=mode, user_preference=pref,
            preferred_adapter=pref_adapter,
            operator_id=op, approve=ap,
            kill_switch_enabled=ks)
        rc = r.get("invocation_receipt") or {}
        sel = r.get("selection_choice") or {}
        chosen = (sel.get("chosen") or {}).get("adapter_name") \
            if isinstance(sel.get("chosen"), dict) else None
        out.append({
            "user_text": ut,
            "status": r.get("status"),
            "selected_adapter": chosen,
            "execution_boundary_preserved":
                rc.get("execution_boundary_preserved"),
            "audio_generated": rc.get("audio_generated"),
            "tts_invoked": rc.get("tts_invoked"),
            "kill_switch_blocked":
                not (r.get("phase31_id") and
                     r.get("status") != "kill_switch_blocked"),
            "approve_requested": ap,
        })
    return {"demo": out, "count": len(out), "phase": _PHASE}


def write_phase31_runtime_report(
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
    "prepare_phase31_multi_adapter_invocation",
    "invoke_phase31_selected_adapter",
    "validate_phase31_invocation_result",
    "demo_phase31_multi_adapter_invocations",
    "write_phase31_runtime_report",
]
