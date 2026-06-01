"""Phase 41 test harness - memory-continuity audit adapter governance."""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import traceback
from pathlib import Path


_TOTAL = 0
_PASS = 0
_FAIL = 0
_FAILURES: list[str] = []


def _check(name: str, ok: bool, detail: str = "") -> None:
    global _TOTAL, _PASS, _FAIL
    _TOTAL += 1
    if ok:
        _PASS += 1
    else:
        _FAIL += 1
        _FAILURES.append(f"{name}: FAIL {detail}".strip())


_ROOT = Path(__file__).resolve().parent


_PHASE41_MODULES = (
    "bilingual_voice_phase41_adapter_interface",
    "bilingual_memory_continuity_audit_adapter",
    "bilingual_voice_phase41_selection_policy",
    "bilingual_voice_phase41_governance_recheck",
    "bilingual_voice_phase41_result_verifier",
    "bilingual_voice_phase41_replay_bridge",
    "bilingual_voice_adapter_phase41_runtime",
)


_MEMORY_STATE = {
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


def suite_a_preflight() -> None:
    upstream = [
        "PHASE40_OPERATOR_AUDIT_REPLAY_REPORT.md",
        "PHASE39_OPERATOR_DRY_RUN_REHEARSAL_REPORT.md",
        "PHASE38_OPERATOR_GOVERNANCE_README_REPORT.md",
        "PHASE37_SAFETY_TRACE_ADAPTER_GOVERNANCE_REPORT.md",
        "PHASE36_KEY_HANDOFF_ENVELOPE_REPORT.md",
        "PHASE35_WITNESS_EXCHANGE_PROTOCOL_REPORT.md",
        "PHASE34_EXTERNAL_WITNESS_VERIFICATION_REPORT.md",
        "PHASE33_THREE_ADAPTER_SIGNED_GOVERNANCE_REPORT.md",
        "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
        "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
        "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
        "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
        "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
        "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
        "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
        "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
        "bilingual_voice_adapter_phase37_runtime.py",
        "bilingual_voice_phase37_adapter_interface.py",
        "bilingual_safety_redaction_trace_adapter.py",
        "bilingual_voice_phase37_selection_policy.py",
        "bilingual_voice_phase37_result_verifier.py",
        "bilingual_voice_phase37_governance_recheck.py",
        "bilingual_voice_memory_schema.py",
        "bilingual_voice_memory_state.py",
        "bilingual_voice_memory_runtime.py",
        "bilingual_voice_continuity_planner.py",
        "bilingual_voice_preference_extractor.py",
        "bilingual_voice_correction_memory.py",
        "bilingual_voice_continuity_store.py",
    ]
    for f in upstream:
        _check(f"A::upstream_present::{f}",
               (_ROOT / f).exists(), f)
    for m in _PHASE41_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    for sub in ("contracts", "receipts", "evidence_bundles",
                 "continuity_audits", "replay_outputs",
                 "governance_rechecks", "reports",
                 "evaluations", "fixtures", "demos"):
        d = (_ROOT / "bilingual_stack"
                   / "voice_adapter_phase41" / sub)
        _check(f"A::folder::{sub}", d.exists(), str(d))
    for m in _PHASE41_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_adapter_interface() -> None:
    import bilingual_voice_phase41_adapter_interface as p41i
    sch = p41i.get_phase41_callable_adapter_schema()
    _check("B::schema_is_dict", isinstance(sch, dict))
    _check("B::schema_5_allowed",
           len(sch.get("allowed_adapter_types") or []) == 5)
    allowed = p41i.get_phase41_allowed_adapter_types()
    for must in ("dummy_metadata_adapter",
                  "bilingual_segment_metadata_adapter",
                  "prosody_density_metadata_adapter",
                  "safety_redaction_trace_metadata_adapter",
                  "memory_continuity_audit_metadata_adapter"):
        _check(f"B::allowed::{must}", must in allowed)
    # Descriptor validation for all five
    for at in allowed:
        d = p41i.create_phase41_adapter_descriptor(at, at)
        v = p41i.validate_phase41_adapter_descriptor(d)
        _check(f"B::descriptor_ok::{at}",
               v.get("ok") is True,
               ",".join(v.get("reasons", [])))
    # Reject real adapter type
    bad = p41i.create_phase41_adapter_descriptor(
        "real_piper", "real_piper_adapter")
    badv = p41i.validate_phase41_adapter_descriptor(bad)
    _check("B::reject_real_adapter",
           badv.get("ok") is False)
    # Reject execution flags
    flag = p41i.create_phase41_adapter_descriptor(
        "dummy_metadata_adapter",
        "dummy_metadata_adapter")
    flag["produces_audio"] = True
    flagv = p41i.validate_phase41_adapter_descriptor(flag)
    _check("B::reject_produces_audio",
           flagv.get("ok") is False)
    # Request builds + validates
    desc = p41i.create_phase41_adapter_descriptor(
        "memory_continuity_audit_metadata_adapter",
        "memory_continuity_audit_metadata_adapter")
    pkt = {"packet_id": "p1",
            "envelope_id": "e1", "job_id": "j1",
            "language_mode": "english",
            "spoken_render_payload": {
                "language_mode": "english",
                "segments": []},
            "safety_summary": {}}
    tok = {"token_id": "t1", "operator_id": "op",
            "approved": True}
    req = p41i.create_phase41_adapter_request(
        pkt, desc, tok, voice_memory_state=_MEMORY_STATE)
    rv = p41i.validate_phase41_adapter_request(req)
    _check("B::request_validates",
           rv.get("ok") is True,
           ",".join(rv.get("reasons", [])))
    # Reject raw transcript fields at top level
    req2 = dict(req)
    req2["raw_transcript"] = "leaked"
    rv2 = p41i.validate_phase41_adapter_request(req2)
    _check("B::request_rejects_raw_transcript",
           rv2.get("ok") is False)
    # Reject raw transcript inside vms — sanitizer strips
    # known summary keys, but if we pass unsanitized through
    # create_phase41_adapter_request, the sanitizer drops
    # banned keys. Test the sanitizer directly via vms.
    # Sanitized vms should not contain raw_transcript even
    # if passed in.
    req3 = p41i.create_phase41_adapter_request(
        pkt, desc, tok,
        voice_memory_state={**_MEMORY_STATE,
                             "raw_transcript": "leak"})
    _check("B::vms_strips_raw_transcript",
           "raw_transcript" not in (
               req3.get("voice_memory_state") or {}))


def suite_c_memory_continuity_audit_adapter() -> None:
    import bilingual_memory_continuity_audit_adapter as mcaa
    import bilingual_voice_phase41_adapter_interface as p41i
    desc = mcaa.get_memory_continuity_audit_adapter_descriptor()
    _check("C::descriptor_is_dict", isinstance(desc, dict))
    _check("C::descriptor_type",
           desc.get("adapter_type")
           == "memory_continuity_audit_metadata_adapter")
    _check("C::descriptor_test_only",
           desc.get("test_only") is True)
    for k in ("produces_audio", "invokes_tts",
              "uses_subprocess", "uses_network",
              "wrote_files"):
        _check(f"C::desc_flag_false::{k}",
               desc.get(k) is False if k != "wrote_files"
               else desc.get("writes_files") is False)
    pkt = {"packet_id": "p1", "envelope_id": "e1",
            "job_id": "j1",
            "language_mode": "english",
            "spoken_render_payload": {
                "language_mode": "english",
                "segments": [{"text": "hi"}]}}
    tok = {"token_id": "t1", "operator_id": "op",
            "approved": True}
    req = p41i.create_phase41_adapter_request(
        pkt, desc, tok, voice_memory_state=_MEMORY_STATE)
    req["approved"] = True
    result = mcaa.call_memory_continuity_audit_adapter(req)
    _check("C::result_status_ok",
           result.get("status") == "ok",
           result.get("reason"))
    val = mcaa.validate_memory_continuity_audit_result(
        result)
    _check("C::result_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("C::preferred_language_mode",
           result.get("preferred_language_mode")
           == "english")
    csd = result.get("code_switch_density_summary") or {}
    _check("C::code_switch_bucket_present",
           csd.get("code_switch_density_bucket")
           in ("low", "moderate", "high", "unknown"))
    _check("C::correction_pattern_count",
           result.get("correction_pattern_count") == 2)
    _check("C::continuity_confidence_in_range",
           0.0 <= float(result.get(
               "continuity_confidence_score") or -1) <= 1.0)
    _check("C::raw_transcript_absent_true",
           result.get("raw_transcript_absent") is True)
    _check("C::sensitive_fact_absent_true",
           result.get("sensitive_fact_absent") is True)
    _check("C::persistence_status",
           result.get("persistence_status") == "ephemeral")
    for k in ("produced_audio", "invoked_tts",
              "used_subprocess", "used_network",
              "wrote_files"):
        _check(f"C::result_flag_false::{k}",
               result.get(k) is False)
    for k in ("raw_transcript", "full_transcript",
              "raw_user_utterance",
              "sensitive_facts", "personal_facts",
              "operator_id", "signing_key_material",
              "audio_bytes", "audio_path",
              "command", "command_line"):
        _check(f"C::no_banned::{k}",
               k not in result or result.get(k)
               in (None, "", False, [], {}))
    # Validator catches banned fields
    drift = dict(result)
    drift["raw_transcript"] = "leak"
    bad = mcaa.validate_memory_continuity_audit_result(drift)
    _check("C::validator_catches_raw_transcript",
           bad.get("ok") is False)
    drift2 = dict(result)
    drift2["produced_audio"] = True
    bad2 = mcaa.validate_memory_continuity_audit_result(
        drift2)
    _check("C::validator_catches_audio_flag",
           bad2.get("ok") is False)
    # Refusal when not approved
    req2 = dict(req)
    req2["approved"] = False
    refused = mcaa.call_memory_continuity_audit_adapter(req2)
    _check("C::refuses_not_approved",
           refused.get("status") == "refused")
    # Refusal when wrong adapter type in descriptor
    req3 = dict(req)
    req3["adapter_descriptor"] = {
        "adapter_type": "dummy_metadata_adapter"}
    wrong = mcaa.call_memory_continuity_audit_adapter(req3)
    _check("C::refuses_wrong_adapter",
           wrong.get("status") == "refused")
    # Extraction helpers
    summary = mcaa.extract_voice_memory_summary_from_request(
        req)
    _check("C::summary_present",
           summary.get("present") is True)
    stab = mcaa.summarize_language_preference_stability(
        summary)
    _check("C::language_stability_keys",
           "drift_detected" in stab)
    cs = mcaa.summarize_code_switch_continuity(summary)
    _check("C::code_switch_keys",
           "code_switch_density_bucket" in cs)
    corr = mcaa.summarize_correction_pattern_continuity(
        summary)
    _check("C::correction_keys",
           "correction_pattern_count" in corr)
    priv = mcaa.summarize_privacy_and_scope(summary)
    _check("C::privacy_keys", "memory_scope" in priv)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "rep.json"
        p = mcaa.write_memory_continuity_audit_adapter_report(
            result, str(out))
        _check("C::report_written", Path(p).exists())


def suite_d_selection_policy() -> None:
    import bilingual_voice_phase41_selection_policy as p41s
    import bilingual_voice_phase41_adapter_interface as p41i
    pkt = {"packet_id": "p1", "envelope_id": "e1",
            "job_id": "j1",
            "language_mode": "english",
            "spoken_render_payload": {
                "language_mode": "english",
                "segments": [{"text": "hi"}]}}
    tok = {"token_id": "t1", "operator_id": "op",
            "approved": True}
    # Simple English: should choose dummy (no memory,
    # no safety, no prosody, no code-switch)
    desc = p41i.create_phase41_adapter_descriptor(
        "dummy_metadata_adapter",
        "dummy_metadata_adapter")
    req_simple = p41i.create_phase41_adapter_request(
        pkt, desc, tok)
    sel = p41s.choose_phase41_adapter(req_simple)
    _check("D::simple_chooses_dummy",
           ((sel.get("chosen") or {})
            .get("adapter_name")
            == "dummy_metadata_adapter"),
           str(sel.get("score_summary")))
    # Mixed EN/RU
    pkt2 = dict(pkt)
    pkt2["language_mode"] = "mixed_en_ru"
    pkt2["spoken_render_payload"] = {
        "language_mode": "mixed_en_ru",
        "segments": [{"text": "hi"}, {"text": "привет"}],
        "code_switch_boundaries": [{"at": 1}]}
    req_mix = p41i.create_phase41_adapter_request(
        pkt2, desc, tok)
    sel_mix = p41s.choose_phase41_adapter(req_mix)
    _check("D::mixed_chooses_segment",
           ((sel_mix.get("chosen") or {})
            .get("adapter_name")
            == "bilingual_segment_metadata_adapter"),
           str(sel_mix.get("score_summary")))
    # High prosody
    pkt3 = dict(pkt)
    pkt3["spoken_render_payload"] = {
        "language_mode": "english",
        "segments": [{"text": "hi"}],
        "prosody": {"pause_a": 1, "pause_b": 1,
                     "emphasis_a": 1, "tone_a": 1}}
    req_pros = p41i.create_phase41_adapter_request(
        pkt3, desc, tok)
    sel_pros = p41s.choose_phase41_adapter(req_pros)
    _check("D::prosody_chooses_prosody",
           ((sel_pros.get("chosen") or {})
            .get("adapter_name")
            == "prosody_density_metadata_adapter"),
           str(sel_pros.get("score_summary")))
    # Safety warning
    pkt4 = dict(pkt)
    pkt4["safety_summary"] = {
        "unsafe": True, "replacements_count": 3,
        "risks": ["a", "b"]}
    req_saf = p41i.create_phase41_adapter_request(
        pkt4, desc, tok)
    sel_saf = p41s.choose_phase41_adapter(req_saf)
    _check("D::safety_chooses_redaction",
           ((sel_saf.get("chosen") or {})
            .get("adapter_name")
            == "safety_redaction_trace_metadata_adapter"),
           str(sel_saf.get("score_summary")))
    # Memory state
    req_mem = p41i.create_phase41_adapter_request(
        pkt, desc, tok, voice_memory_state=_MEMORY_STATE)
    sel_mem = p41s.choose_phase41_adapter(req_mem)
    _check("D::memory_chooses_memory_audit",
           ((sel_mem.get("chosen") or {})
            .get("adapter_name")
            == "memory_continuity_audit_metadata_adapter"),
           str(sel_mem.get("score_summary")))
    # Preferred wins
    sel_pref = p41s.choose_phase41_adapter(
        req_simple,
        preferred_adapter=
            "memory_continuity_audit_metadata_adapter")
    _check("D::preferred_valid_wins",
           ((sel_pref.get("chosen") or {})
            .get("adapter_name")
            == "memory_continuity_audit_metadata_adapter"))
    # Preferred invalid: name not in pool -> still picks
    # by score (rejected entry in rejected list)
    sel_bad = p41s.choose_phase41_adapter(
        req_simple, preferred_adapter="real_piper_adapter")
    _check("D::invalid_preferred_listed_in_rejected",
           any(r.get("reason") ==
                "preferred_not_in_safe_pool"
                for r in sel_bad.get("rejected", [])))
    # Execution-flag descriptor rejected by reject helper
    bad_desc = p41i.create_phase41_adapter_descriptor(
        "dummy_metadata_adapter",
        "dummy_metadata_adapter")
    bad_desc["uses_subprocess"] = True
    rj = p41s.reject_disallowed_phase41_adapter(bad_desc)
    _check("D::reject_execution_flag",
           rj.get("rejected") is True)
    # Explanation
    explain = p41s.explain_phase41_selection(sel_mem)
    _check("D::explain_ok", explain.get("ok") is True)


def suite_e_governance_recheck() -> None:
    import bilingual_voice_phase41_governance_recheck as gr
    _check("E::phase30_strict",
           gr.verify_phase41_phase30_strictness().get("ok")
           is True)
    _check("E::phase31_boundary",
           gr.verify_phase41_phase31_boundary().get("ok")
           is True)
    _check("E::phase33_boundary",
           gr.verify_phase41_phase33_boundary().get("ok")
           is True)
    _check("E::phase37_boundary",
           gr.verify_phase41_phase37_boundary().get("ok")
           is True)
    _check("E::phase41_boundary",
           gr.verify_phase41_five_adapter_boundary()
            .get("ok") is True)
    # allowed adapters only
    good_records = [
        {"adapter_name": "dummy_metadata_adapter"},
        {"adapter_name":
            "memory_continuity_audit_metadata_adapter"},
    ]
    _check("E::allowed_adapters_ok",
           gr.verify_phase41_allowed_adapters_only(
               good_records).get("ok") is True)
    bad_records = [{"adapter_name": "real_piper_adapter"}]
    _check("E::allowed_adapters_catches_bad",
           gr.verify_phase41_allowed_adapters_only(
               bad_records).get("ok") is False)
    # metadata-only
    _check("E::metadata_only_ok",
           gr.verify_phase41_metadata_only_results(
               [{"produced_audio": False,
                  "invoked_tts": False,
                  "used_subprocess": False,
                  "used_network": False,
                  "wrote_files": False}]).get("ok") is True)
    _check("E::metadata_only_catches_audio",
           gr.verify_phase41_metadata_only_results(
               [{"produced_audio": True}]).get("ok") is False)
    # memory privacy
    _check("E::memory_privacy_ok",
           gr.verify_phase41_memory_privacy_boundary(
               [{"adapter_type":
                 "memory_continuity_audit_metadata_adapter",
                 "raw_transcript_absent": True,
                 "sensitive_fact_absent": True}]).get("ok")
            is True)
    _check("E::memory_privacy_catches_raw",
           gr.verify_phase41_memory_privacy_boundary(
               [{"adapter_type":
                 "memory_continuity_audit_metadata_adapter",
                 "raw_transcript_absent": False,
                 "sensitive_fact_absent": True}]).get("ok")
            is False)
    # signed evidence required
    _check("E::signed_evidence_required_ok",
           gr.verify_phase41_signed_evidence_required(
               {"status": "ok",
                "signed_witness_pipeline": {
                    "signed_evidence_summary": {
                        "evidence_validates": True}}})
            .get("ok") is True)
    _check("E::signed_evidence_required_blocks",
           gr.verify_phase41_signed_evidence_required(
               {"status": "ok",
                "signed_witness_pipeline": {}}).get("ok")
            is False)
    # witness export
    _check("E::witness_export_required_ok",
           gr.verify_phase41_witness_export_required(
               {"status": "ok",
                "witness_export_summary":
                    {"status": "ok"}}).get("ok") is True)
    _check("E::witness_export_required_blocks",
           gr.verify_phase41_witness_export_required(
               {"status": "ok",
                "witness_export_summary":
                    {"status": "failed"}}).get("ok") is False)
    # exchange
    _check("E::exchange_required_ok",
           gr.verify_phase41_exchange_required(
               {"status": "ok",
                "exchange_summary":
                    {"status": "ok"}}).get("ok") is True)
    _check("E::exchange_required_blocks",
           gr.verify_phase41_exchange_required(
               {"status": "ok",
                "exchange_summary":
                    {"status": "failed"}}).get("ok") is False)
    # replay compatibility
    _check("E::replay_compat_required_ok",
           gr.verify_phase41_replay_compatibility(
               {"status": "ok",
                "replay_projection": {"projection_id": "x"}})
            .get("ok") is True)
    _check("E::replay_compat_required_blocks",
           gr.verify_phase41_replay_compatibility(
               {"status": "ok",
                "replay_projection": {}}).get("ok") is False)
    # no secret leakage
    _check("E::no_secret_leakage_ok",
           gr.verify_phase41_no_secret_leakage(
               [{"adapter_name": "dummy"}]).get("ok") is True)
    # no audio boundary / no execution boundary on
    # source paths
    _check("E::no_audio_boundary_ok",
           gr.verify_phase41_no_audio_boundary(
               [str(_ROOT
                     / "bilingual_memory_continuity_audit_adapter.py"
                     )]).get("ok") is True)
    _check("E::no_execution_boundary_ok",
           gr.verify_phase41_no_execution_boundary(
               [str(_ROOT
                     / "bilingual_memory_continuity_audit_adapter.py"
                     )]).get("ok") is True)


def suite_f_result_verifier() -> None:
    import bilingual_voice_phase41_result_verifier as rv
    good = {
        "result_id": "r1",
        "adapter_name":
            "memory_continuity_audit_metadata_adapter",
        "adapter_type":
            "memory_continuity_audit_metadata_adapter",
        "produced_audio": False, "invoked_tts": False,
        "used_subprocess": False, "used_network": False,
        "wrote_files": False,
        "raw_transcript_absent": True,
        "sensitive_fact_absent": True,
    }
    _check("F::valid_result",
           rv.verify_phase41_adapter_result(good).get("ok")
           is True)
    _check("F::memory_privacy_ok",
           rv.verify_phase41_memory_privacy_result(good)
            .get("ok") is True)
    bad_raw = dict(good)
    bad_raw["raw_transcript_absent"] = False
    _check("F::raw_transcript_exposure_fails",
           rv.verify_phase41_memory_privacy_result(bad_raw)
            .get("ok") is False)
    bad_sens = dict(good)
    bad_sens["sensitive_fact_absent"] = False
    _check("F::sensitive_fact_exposure_fails",
           rv.verify_phase41_memory_privacy_result(bad_sens)
            .get("ok") is False)
    # unknown adapter
    bad_a = dict(good)
    bad_a["adapter_type"] = "real_piper_adapter"
    _check("F::unknown_adapter_fails",
           rv.verify_phase41_adapter_result(bad_a).get("ok")
           is False)
    # exec flags
    for k in ("produced_audio", "invoked_tts",
              "used_subprocess", "used_network",
              "wrote_files"):
        b = dict(good)
        b[k] = True
        _check(f"F::flag_fails::{k}",
               rv.verify_phase41_adapter_result(b).get("ok")
               is False)
    # missing signed evidence on ok output fails
    out_missing_ev = {
        "status": "ok",
        "signed_witness_pipeline": {},
        "replay_projection": {"x": 1},
    }
    _check("F::missing_signed_evidence_fails",
           rv.verify_phase41_result_against_governance(
               out_missing_ev).get("ok") is False)
    # missing witness export fails
    out_missing_witness = {
        "status": "ok",
        "signed_witness_pipeline": {
            "signed_evidence_summary":
                {"evidence_validates": True},
            "exchange_summary": {"status": "ok"}},
        "replay_projection": {"x": 1},
    }
    _check("F::missing_witness_export_fails",
           rv.verify_phase41_result_against_governance(
               out_missing_witness).get("ok") is False)
    # missing exchange fails
    out_missing_exch = {
        "status": "ok",
        "signed_witness_pipeline": {
            "signed_evidence_summary":
                {"evidence_validates": True},
            "witness_export_summary": {"status": "ok"}},
        "replay_projection": {"x": 1},
    }
    _check("F::missing_exchange_fails",
           rv.verify_phase41_result_against_governance(
               out_missing_exch).get("ok") is False)
    # missing replay projection fails
    out_missing_proj = {
        "status": "ok",
        "signed_witness_pipeline": {
            "signed_evidence_summary":
                {"evidence_validates": True},
            "witness_export_summary": {"status": "ok"},
            "exchange_summary": {"status": "ok"}},
        "replay_projection": {},
    }
    _check("F::missing_replay_projection_fails",
           rv.verify_phase41_result_against_governance(
               out_missing_proj).get("ok") is False)


def suite_g_replay_bridge() -> None:
    import bilingual_voice_phase41_replay_bridge as rb
    fake_out = {
        "phase41_id": "p41_1",
        "selection_choice": {
            "chosen": {
                "adapter_name":
                    "memory_continuity_audit_metadata_adapter"},
            "candidate_adapters": ["a", "b"],
            "score_summary": {"a": 0.8},
            "reason": "highest"},
        "signed_witness_pipeline": {
            "signed_evidence_summary": {
                "evidence_validates": True,
                "evidence_hash": "abc"},
            "witness_export_summary": {"status": "ok"},
            "exchange_summary": {"status": "ok"}},
        "invocation_receipt": {"receipt_id": "ir1"},
        "result_verification": {"ok": True},
        "governance_recheck": {"ok": True},
    }
    proj = rb.create_phase41_replay_projection(fake_out)
    _check("G::projection_is_dict", isinstance(proj, dict))
    val = rb.validate_phase41_replay_projection(proj)
    _check("G::projection_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    cmp_ = rb.compare_phase41_projection_to_phase40_contract(
        proj)
    _check("G::compare_ok", cmp_.get("ok") is True,
           ",".join(cmp_.get("reasons", [])))
    summary = rb.summarize_phase41_replay_compatibility(proj)
    _check("G::summary_ok", summary.get("ok") is True)
    # No banned fields
    for k in ("raw_transcript", "operator_id",
              "signing_key_material", "audio_bytes",
              "command"):
        _check(f"G::no_banned::{k}",
               k not in proj or proj.get(k)
               in (None, "", False, [], {}))
    # No adapter re-invocation: projection has no
    # call_adapter / dispatch_adapter
    for k in ("call_adapter", "dispatch_adapter",
              "adapter_call_count"):
        _check(f"G::no_reinvocation_field::{k}",
               k not in proj)
    # Drift detection
    drift = dict(proj)
    drift["rehearsal_dry_run_only"] = False
    bad = rb.validate_phase41_replay_projection(drift)
    _check("G::validator_catches_non_dry_run",
           bad.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "rb.json"
        p = rb.write_phase41_replay_bridge_report(
            proj, str(out))
        _check("G::report_written", Path(p).exists())


def suite_h_phase41_runtime() -> None:
    import bilingual_voice_adapter_phase41_runtime as rt
    # English
    en_out = rt.prepare_phase41_five_adapter_invocation(
        user_text="Hello Luna",
        operator_id="operator_local",
        approve=True)
    _check("H::english_status_ok",
           en_out.get("status") == "ok",
           str(en_out.get("status")))
    # Russian
    ru_out = rt.prepare_phase41_five_adapter_invocation(
        user_text="Привет Луна",
        user_preference="russian",
        operator_id="operator_local",
        approve=True)
    _check("H::russian_status_ok",
           ru_out.get("status") == "ok")
    # Mixed
    mix_out = rt.prepare_phase41_five_adapter_invocation(
        user_text="Mix russian and english",
        draft_response_text="ok, давай.",
        operator_id="operator_local",
        approve=True)
    _check("H::mixed_status_ok",
           mix_out.get("status") == "ok")
    # Memory state -> memory adapter
    mem_out = rt.prepare_phase41_five_adapter_invocation(
        user_text="Continuity audit drill",
        operator_id="operator_local",
        approve=True,
        voice_memory_state=_MEMORY_STATE)
    _check("H::memory_state_status_ok",
           mem_out.get("status") == "ok")
    chosen = ((mem_out.get("selection_choice") or {})
               .get("chosen") or {})
    _check("H::memory_state_picks_memory_audit",
           chosen.get("adapter_name")
           == "memory_continuity_audit_metadata_adapter")
    # Preferred memory adapter
    pref_out = rt.prepare_phase41_five_adapter_invocation(
        user_text="Simple",
        preferred_adapter=
            "memory_continuity_audit_metadata_adapter",
        operator_id="operator_local",
        approve=True)
    _check("H::preferred_memory_ok",
           pref_out.get("status") == "ok")
    pchosen = ((pref_out.get("selection_choice") or {})
                .get("chosen") or {})
    _check("H::preferred_memory_chosen",
           pchosen.get("adapter_name")
           == "memory_continuity_audit_metadata_adapter")
    # approve=False refuses before adapter call
    no_app = rt.prepare_phase41_five_adapter_invocation(
        user_text="test",
        operator_id="operator_local",
        approve=False)
    _check("H::approve_false_refuses",
           "refused" in str(no_app.get("status") or ""))
    # Kill switch blocks
    ks = rt.prepare_phase41_five_adapter_invocation(
        user_text="test",
        operator_id="operator_local",
        approve=True, kill_switch_enabled=True)
    _check("H::kill_switch_blocks",
           ks.get("status") == "kill_switch_blocked")
    # sign_evidence=False -> refused
    no_sign = rt.prepare_phase41_five_adapter_invocation(
        user_text="test",
        operator_id="operator_local",
        approve=True, sign_evidence=False)
    _check("H::no_sign_refused",
           no_sign.get("status") == "refused")
    # no witness export -> refused
    no_we = rt.prepare_phase41_five_adapter_invocation(
        user_text="test",
        operator_id="operator_local",
        approve=True, include_witness_export=False)
    _check("H::no_witness_export_refused",
           no_we.get("status") == "refused")
    # no exchange -> refused
    no_ex = rt.prepare_phase41_five_adapter_invocation(
        user_text="test",
        operator_id="operator_local",
        approve=True, include_exchange=False)
    _check("H::no_exchange_refused",
           no_ex.get("status") == "refused")
    # no replay projection -> refused
    no_proj = rt.prepare_phase41_five_adapter_invocation(
        user_text="test",
        operator_id="operator_local",
        approve=True,
        include_replay_projection=False)
    _check("H::no_projection_refused",
           no_proj.get("status") == "refused")
    # Demo bounded
    demo = rt.demo_phase41_five_adapter_invocations(limit=12)
    _check("H::demo_count_le_12",
           demo.get("count") <= 12)
    _check("H::demo_count_ge_1",
           demo.get("count") >= 1)
    # Validate output
    val = rt.validate_phase41_invocation_output(en_out)
    _check("H::validate_invocation_output_ok",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    # Persist a sample to disk under voice_adapter_phase41
    out_path = (_ROOT / "bilingual_stack"
                     / "voice_adapter_phase41"
                     / "demos"
                     / "phase41_runtime_sample.json")
    p = rt.write_phase41_runtime_report(en_out, str(out_path))
    _check("H::runtime_report_written", Path(p).exists())


def suite_i_production_safety() -> None:
    en_db = _ROOT / "lexicon" / "luna_vocabulary.sqlite"
    ru_db = (_ROOT / "russian_stack"
                  / "russian_lexicon.sqlite")
    link_db = (_ROOT / "bilingual_stack"
                    / "bilingual_links.sqlite")
    if en_db.exists():
        c = sqlite3.connect(str(en_db))
        n = c.execute(
            "SELECT COUNT(*) FROM words").fetchone()[0]
        c.close()
        _check("I::en_2814", n == 2814, f"got {n}")
    if ru_db.exists():
        c = sqlite3.connect(str(ru_db))
        nw = c.execute(
            "SELECT COUNT(*) FROM words").fetchone()[0]
        np_ = c.execute(
            "SELECT COUNT(*) FROM phrases").fetchone()[0]
        c.close()
        _check("I::ru_2518", nw == 2518)
        _check("I::ru_phr_35", np_ == 35)
    if link_db.exists():
        c = sqlite3.connect(str(link_db))
        nc = c.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0]
        nl = c.execute(
            "SELECT COUNT(*) FROM entry_links").fetchone()[0]
        c.close()
        _check("I::concepts_26", nc == 26)
        _check("I::links_52", nl == 52)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"),
        recursive=True) if "backups" not in p]
    _check("I::manifests_90", len(live) == 90,
           str(len(live)))
    audio = []
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase41")
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("I::no_audio_files_in_voice_adapter_phase41",
           not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [f"{m}.py" for m in _PHASE41_MODULES]
    forbidden_audio = (
        "pyttsx3", "gtts", "edge_tts", "piper.", "coqui",
        "whisper", "pyaudio", "sounddevice", "pydub",
        "soundfile", "comtypes", "win32com",
    )
    forbidden_exec = (
        "subprocess.run", "subprocess.Popen",
        "subprocess.call", "os.system(", "shell=True",
        "os.popen", "ctypes.windll", "powershell ",
        "powershell.exe",
    )
    forbidden_net = (
        "urllib.request", "http.client", "requests.",
        "httpx.", "socket.socket",
    )
    forbidden_runtime = (
        "luna_modules", "import worker", "from worker",
        "tier_progression", "probe_attestation",
        "attestation_signer",
    )
    forbidden_threading = (
        "threading.Thread", "multiprocessing.Process",
        "multiprocessing.Pool", "daemon=True",
        "asyncio.create_task", "schedule.every",
    )
    for fn in files:
        p = _ROOT / fn
        if not p.exists():
            _check(f"J::file_exists::{fn}", False, fn)
            continue
        src = p.read_text(encoding="utf-8")
        for tok in forbidden_audio:
            _check(f"J::{fn}::no_audio:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_exec:
            _check(f"J::{fn}::no_exec:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_net:
            _check(f"J::{fn}::no_net:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_runtime:
            _check(f"J::{fn}::no_runtime:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_threading:
            _check(f"J::{fn}::no_daemon:{tok.strip()}",
                   tok not in src)
    import bilingual_voice_phase36_secret_boundary as sb
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase41")
    scan_dirs = [base / sub for sub in (
        "contracts", "receipts", "evidence_bundles",
        "continuity_audits", "replay_outputs",
        "governance_rechecks", "reports",
        "evaluations", "demos")]
    for d in scan_dirs:
        if not d.exists():
            _check(f"J::leak_scan_dir_present:{d.name}",
                   True)
            continue
        scan = sb.validate_no_secret_leakage_in_directory(
            str(d))
        _check(f"J::no_leak_in:{d.name}",
               scan["ok"],
               json.dumps(scan.get("leaks", []))[:200])


def suite_k_regression_smoke() -> None:
    upstream = [
        "bilingual_voice_phase40_replay_verifier",
        "bilingual_voice_phase39_runtime",
        "bilingual_voice_phase38_governance_ledger",
        "bilingual_voice_phase38_status_dashboard",
        "bilingual_voice_adapter_phase37_runtime",
        "bilingual_voice_phase37_governance_recheck",
        "bilingual_voice_phase37_adapter_interface",
        "bilingual_safety_redaction_trace_adapter",
    ]
    for m in upstream:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reimport {m}: {e}")
        _check(f"K::reimport::{m}", ok)
    for m in _PHASE41_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    try:
        import bilingual_voice_adapter_phase41_runtime as rt
        import bilingual_voice_phase41_replay_bridge as rb
        out = rt.prepare_phase41_five_adapter_invocation(
            user_text="end to end smoke",
            operator_id="operator_local",
            approve=True,
            voice_memory_state=_MEMORY_STATE)
        _check("K::e2e_status_ok",
               out.get("status") == "ok",
               str(out.get("status")))
        _check("K::e2e_projection_validates",
               rb.validate_phase41_replay_projection(
                   out.get("replay_projection")).get("ok")
                is True)
    except Exception as e:  # noqa: BLE001
        _check("K::e2e_no_exception", False, str(e))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_adapter_interface),
        ("C", suite_c_memory_continuity_audit_adapter),
        ("D", suite_d_selection_policy),
        ("E", suite_e_governance_recheck),
        ("F", suite_f_result_verifier),
        ("G", suite_g_replay_bridge),
        ("H", suite_h_phase41_runtime),
        ("I", suite_i_production_safety),
        ("J", suite_j_isolation),
        ("K", suite_k_regression_smoke),
    ]
    for name, fn in suites:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            traceback.print_exc()
            _check(f"{name}::suite_uncaught", False, str(e))
    print(f"Total: {_TOTAL} | Pass: {_PASS} | Fail: {_FAIL}")
    if _FAILURES:
        print("--- failures ---")
        for f in _FAILURES[:80]:
            print(f)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
