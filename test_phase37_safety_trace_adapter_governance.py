"""Phase 37 test harness — safety trace adapter + signed witness pipeline."""

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


def suite_a_preflight() -> None:
    upstream = [
        "PHASE36_KEY_HANDOFF_ENVELOPE_REPORT.md",
        "bilingual_voice_phase36_handoff_contract.py",
        "bilingual_voice_phase36_key_handoff_envelope.py",
        "bilingual_voice_phase36_secret_boundary.py",
        "bilingual_voice_phase36_public_descriptor_bridge.py",
        "bilingual_voice_phase36_handoff_verifier.py",
        "bilingual_voice_phase36_operator_guide.py",
        "bilingual_voice_phase36_handoff_runtime.py",
        "PHASE35_WITNESS_EXCHANGE_PROTOCOL_REPORT.md",
        "bilingual_voice_phase35_exchange_runtime.py",
        "PHASE34_EXTERNAL_WITNESS_VERIFICATION_REPORT.md",
        "bilingual_voice_phase34_export_runtime.py",
        "PHASE33_THREE_ADAPTER_SIGNED_GOVERNANCE_REPORT.md",
        "bilingual_voice_phase33_signed_evidence.py",
        "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
        "bilingual_voice_audit_signing_policy.py",
        "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
        "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
        "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
        "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
        "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
        "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
        "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
    ]
    p37 = [
        "bilingual_voice_phase37_adapter_interface.py",
        "bilingual_safety_redaction_trace_adapter.py",
        "bilingual_voice_phase37_selection_policy.py",
        "bilingual_voice_phase37_signed_witness_pipeline.py",
        "bilingual_voice_phase37_governance_recheck.py",
        "bilingual_voice_phase37_result_verifier.py",
        "bilingual_voice_adapter_phase37_runtime.py",
    ]
    for f in upstream + p37:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_phase37_adapter_interface",
        "bilingual_safety_redaction_trace_adapter",
        "bilingual_voice_phase37_selection_policy",
        "bilingual_voice_phase37_signed_witness_pipeline",
        "bilingual_voice_phase37_governance_recheck",
        "bilingual_voice_phase37_result_verifier",
        "bilingual_voice_adapter_phase37_runtime",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_interface() -> None:
    import bilingual_voice_phase37_adapter_interface as p37i
    s = p37i.get_phase37_callable_adapter_schema()
    _check("B::schema_version",
           isinstance(s.get("version"), str))
    _check("B::exactly_four_allowed",
           s["allowed_adapter_types"] == [
               "dummy_metadata_adapter",
               "bilingual_segment_metadata_adapter",
               "prosody_density_metadata_adapter",
               "safety_redaction_trace_metadata_adapter"])
    for at in ("dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter",
                "safety_redaction_trace_metadata_adapter"):
        d = p37i.create_phase37_adapter_descriptor(at, at)
        _check(f"B::descriptor_validates:{at}",
               p37i.validate_phase37_adapter_descriptor(d)["ok"])
    for bad in ("real_piper", "sapi_real", "kokoro_real",
                "real_tts", "audio_renderer",
                "subprocess_renderer"):
        b = p37i.create_phase37_adapter_descriptor(bad, bad)
        _check(f"B::reject:{bad}",
               not p37i.validate_phase37_adapter_descriptor(b)["ok"])
    dummy = p37i.create_phase37_adapter_descriptor(
        "dummy_metadata_adapter", "dummy_metadata_adapter")
    for flag in ("produces_audio", "invokes_tts", "uses_subprocess",
                  "uses_network", "writes_files"):
        bad_desc = dict(dummy)
        bad_desc[flag] = True
        _check(f"B::flag_rejected:{flag}",
               not p37i.validate_phase37_adapter_descriptor(
                   bad_desc)["ok"])
    pkt = {"packet_id": "rev_x", "envelope_id": "venv_x",
           "job_id": "j_x", "language_mode": "english_only",
           "safety_summary": {},
           "spoken_render_payload": {"language_mode": "english_only"}}
    tok = {"token_id": "itok_x", "operator_id": "op",
            "approved": True}
    req = p37i.create_phase37_adapter_request(pkt, dummy, tok)
    _check("B::request_validates",
           p37i.validate_phase37_adapter_request(req)["ok"])


def suite_c_safety_redaction_trace() -> None:
    import bilingual_safety_redaction_trace_adapter as srta
    desc = srta.get_safety_redaction_trace_adapter_descriptor()
    _check("C::descriptor_exists",
           desc.get("adapter_name") ==
           "safety_redaction_trace_metadata_adapter")
    req = {"request_id": "p37req_x", "envelope_id": "venv_x",
           "job_id": "j_x", "language_mode": "english_only",
           "segment_count": 3,
           "safety_summary": {
               "unsafe": True, "high_risk": True,
               "replacements_count": 2,
               "risks": ["vulgar", "offensive"]},
           "spoken_render_payload": {
               "language_mode": "english_only",
               "segments": [],
               "safety_summary": {"unsafe": True},
               "redaction_decisions": [
                   {"position": 1}, {"position": 2}],
               "recognition_only_terms": ["x", "y"],
               "do_not_use_unprompted_terms": ["z"],
               "voice_safe_replacements": [{"a": 1}],
               "vulgar_offensive_block": True}}
    res = srta.call_safety_redaction_trace_adapter(req)
    v = srta.validate_safety_redaction_trace_result(res)
    _check("C::result_validates", v["ok"], json.dumps(v))
    _check("C::safety_present",
           res.get("safety_summary_present") is True)
    _check("C::redaction_count_positive",
           res.get("redaction_decision_count") >= 1)
    _check("C::recognition_only_count",
           res.get("recognition_only_block_count") >= 1)
    _check("C::dnu_count",
           res.get("do_not_use_unprompted_block_count") >= 1)
    _check("C::voice_safe_count",
           res.get("voice_safe_replacement_count") >= 1)
    _check("C::vulgar_block_count",
           res.get("vulgar_offensive_block_count") >= 1)
    _check("C::trace_score",
           isinstance(res.get("safety_trace_score"), float)
           and res["safety_trace_score"] > 0)
    for flag in ("produced_audio", "invoked_tts",
                  "used_subprocess", "used_network",
                  "wrote_files"):
        _check(f"C::flag_false:{flag}", res[flag] is False)
    for k in ("audio_bytes", "audio_path", "command",
              "subprocess", "powershell",
              "vulgar_terms", "offensive_terms",
              "profanity_terms"):
        _check(f"C::no_field:{k}", k not in res)


def suite_d_selection_policy() -> None:
    import bilingual_voice_phase37_selection_policy as p37s
    import bilingual_voice_phase37_adapter_interface as p37i
    simple_en = {"language_mode": "english_only",
                  "spoken_render_payload": {
                      "language_mode": "english_only",
                      "segments": [{"segment_id": "s1",
                                     "text": "hi",
                                     "language": "en"}]},
                  "safety_summary": {}}
    mix = {"language_mode": "mixed_en_ru",
            "spoken_render_payload": {
                "language_mode": "mixed_en_ru",
                "segments": [
                    {"segment_id": "s1", "text": "hi",
                     "language": "en"},
                    {"segment_id": "s2", "text": "привет",
                     "language": "ru"}],
                "code_switch_boundaries": [{"after": "s1"}]},
            "safety_summary": {}}
    pros = {"language_mode": "english_only",
             "spoken_render_payload": {
                 "language_mode": "english_only",
                 "segments": [{"segment_id": "s1",
                                "text": "x", "language": "en"}],
                 "prosody": {"pause_long": True,
                             "emphasis_strong": True,
                             "tone_rising": True,
                             "pitch_low": True}},
             "safety_summary": {}}
    safety = {"language_mode": "english_only",
               "spoken_render_payload": {
                   "language_mode": "english_only",
                   "safety_summary": {"unsafe": True,
                                       "replacements_count": 2},
                   "redaction_decisions": [{"x": 1}],
                   "vulgar_offensive_block": True},
               "safety_summary": {"unsafe": True}}
    redact = {"language_mode": "english_only",
               "spoken_render_payload": {
                   "language_mode": "english_only",
                   "redaction_decisions": [{"x": 1}, {"y": 2}],
                   "recognition_only_terms": ["a"],
                   "voice_safe_replacements": [{"x": "y"}]},
               "safety_summary": {}}
    c_en = p37s.choose_phase37_adapter(simple_en)
    _check("D::en_picks_dummy",
           c_en["chosen"]["adapter_type"] ==
           "dummy_metadata_adapter",
           json.dumps(c_en["score_summary"]))
    c_mix = p37s.choose_phase37_adapter(mix)
    _check("D::mix_picks_bilingual",
           c_mix["chosen"]["adapter_type"] ==
           "bilingual_segment_metadata_adapter",
           json.dumps(c_mix["score_summary"]))
    c_pros = p37s.choose_phase37_adapter(pros)
    _check("D::high_prosody_picks_prosody",
           c_pros["chosen"]["adapter_type"] ==
           "prosody_density_metadata_adapter",
           json.dumps(c_pros["score_summary"]))
    c_safety = p37s.choose_phase37_adapter(safety)
    _check("D::safety_warning_picks_safety_trace",
           c_safety["chosen"]["adapter_type"] ==
           "safety_redaction_trace_metadata_adapter",
           json.dumps(c_safety["score_summary"]))
    c_redact = p37s.choose_phase37_adapter(redact)
    _check("D::redaction_picks_safety_trace",
           c_redact["chosen"]["adapter_type"] ==
           "safety_redaction_trace_metadata_adapter",
           json.dumps(c_redact["score_summary"]))
    c_pref = p37s.choose_phase37_adapter(
        simple_en,
        preferred_adapter="safety_redaction_trace_metadata_adapter")
    _check("D::preferred_wins",
           c_pref["chosen"]["adapter_type"] ==
           "safety_redaction_trace_metadata_adapter")
    c_bad = p37s.choose_phase37_adapter(
        simple_en, preferred_adapter="real_piper")
    _check("D::invalid_preferred_falls_back",
           c_bad["ok"] and c_bad["chosen"]["adapter_type"] in
           p37i.ALLOWED_ADAPTER_TYPES)
    bad_desc = p37i.create_phase37_adapter_descriptor(
        "dummy_metadata_adapter", "dummy_metadata_adapter")
    bad_desc["uses_subprocess"] = True
    rej = p37s.reject_disallowed_phase37_adapter(bad_desc)
    _check("D::exec_flag_rejected", rej["rejected"] is True)
    _check("D::explain_summary",
           isinstance(p37s.explain_phase37_selection(
               c_mix).get("summary"), str))


def suite_e_signed_witness_pipeline() -> None:
    import bilingual_voice_phase37_signed_witness_pipeline as p37p
    import bilingual_voice_audit_chain as vac
    chain = vac.append_chain_event(
        [], vac.create_audit_chain_event("preflight", "ok", "x"))
    inv_out = {
        "audit_chain": chain,
        "invocation_receipt": {
            "receipt_id": "recv_x",
            "adapter_name": "dummy_metadata_adapter",
            "adapter_type": "dummy_metadata_adapter",
            "operator_id_hash": "a"*16, "dry_run": True,
            "test_only": True,
            "execution_boundary_preserved": True,
            "audio_generated": False, "tts_invoked": False,
            "subprocess_used": False, "network_used": False,
            "files_written": False, "request_id": "x",
            "result_id": "y", "pre_call_status": "ok",
            "post_call_status": "ok",
            "audit_chain_hash": "abc", "notes": "x",
            "created_at": 1.0, "phase": "p"},
        "selection_receipt": {},
        "selected_adapter_result": {
            "result_id": "y",
            "adapter_name": "dummy_metadata_adapter",
            "produced_audio": False, "invoked_tts": False,
            "used_subprocess": False, "used_network": False,
            "wrote_files": False},
        "status": "ok",
    }
    pipe = p37p.create_phase37_signed_witness_pipeline(inv_out)
    _check("E::pipeline_status_ok",
           pipe["status"] == "ok",
           json.dumps(pipe.get(
               "witness_export_summary")) +
           json.dumps(pipe.get("exchange_summary")))
    _check("E::signed_evidence_validates",
           (pipe.get("signed_evidence_summary") or {}).get(
               "evidence_validates") is True)
    _check("E::witness_export_summary_present",
           pipe.get("witness_export_summary"))
    _check("E::exchange_summary_present",
           pipe.get("exchange_summary"))
    # Optional handoff: no consent -> refused with reason
    pipe_h = p37p.create_phase37_signed_witness_pipeline(
        inv_out, include_handoff=True, consent_marker="")
    hs = pipe_h.get("handoff_summary") or {}
    _check("E::handoff_requires_consent",
           hs.get("status") == "refused")
    # Handoff with consent
    pipe_h2 = p37p.create_phase37_signed_witness_pipeline(
        inv_out, include_handoff=True,
        consent_marker="op_consent_phase37")
    _check("E::handoff_with_consent",
           (pipe_h2.get("handoff_summary") or {}).get("status")
               in ("pass", "fail"))
    # Verify pipeline
    v = p37p.verify_phase37_signed_witness_pipeline(pipe)
    _check("E::pipeline_verifies", v["ok"], json.dumps(v))
    # Secret leakage rejected by write
    try:
        with tempfile.TemporaryDirectory() as td:
            p37p.write_phase37_pipeline_report(
                {"sealed_payload": "x"},
                os.path.join(td, "bad.json"))
            _check("E::write_refuses_secret", False,
                   "did not raise")
    except ValueError:
        _check("E::write_refuses_secret", True)
    # No audio/network/subprocess fields in pipeline
    for k in ("audio_bytes", "audio_path", "command",
              "subprocess", "powershell"):
        _check(f"E::no_field:{k}", k not in pipe)


def suite_f_governance_recheck() -> None:
    import bilingual_voice_phase37_governance_recheck as g
    _check("F::phase30_strict",
           g.verify_phase37_phase30_strictness()["ok"])
    _check("F::phase31_boundary",
           g.verify_phase37_phase31_boundary()["ok"])
    _check("F::phase33_boundary",
           g.verify_phase37_phase33_boundary()["ok"])
    p37b = g.verify_phase37_four_adapter_boundary()
    _check("F::phase37_four_boundary",
           p37b["ok"] and p37b["allowed_adapter_types"] == [
               "dummy_metadata_adapter",
               "bilingual_segment_metadata_adapter",
               "prosody_density_metadata_adapter",
               "safety_redaction_trace_metadata_adapter"])
    _check("F::phase36_secret_boundary",
           g.verify_phase37_phase36_secret_boundary()["ok"])
    good = [{"adapter_name": "dummy_metadata_adapter"},
            {"adapter_name":
             "safety_redaction_trace_metadata_adapter"}]
    _check("F::allowed_pass",
           g.verify_phase37_allowed_adapters_only(good)["ok"])
    bad = [{"adapter_name": "real_piper"}]
    _check("F::real_rejected",
           not g.verify_phase37_allowed_adapters_only(bad)["ok"])
    sig_ok = {"status": "ok",
               "signed_witness_pipeline": {
                   "signed_evidence_summary": {
                       "evidence_validates": True}}}
    _check("F::signed_evidence_present_ok",
           g.verify_phase37_signed_evidence_required(sig_ok)["ok"])
    sig_missing = {"status": "ok",
                    "signed_witness_pipeline": {}}
    _check("F::signed_evidence_missing_fails",
           not g.verify_phase37_signed_evidence_required(
               sig_missing)["ok"])
    we_ok = {"status": "ok",
              "witness_export_summary": {"status": "ok"},
              "exchange_summary": {"status": "ok"}}
    _check("F::witness_export_required_ok",
           g.verify_phase37_witness_export_required(we_ok)["ok"])
    _check("F::exchange_required_ok",
           g.verify_phase37_exchange_required(we_ok)["ok"])
    we_missing = {"status": "ok",
                   "witness_export_summary": {}}
    _check("F::witness_export_required_fails_when_missing",
           not g.verify_phase37_witness_export_required(
               we_missing)["ok"])
    ex_missing = {"status": "ok", "exchange_summary": {}}
    _check("F::exchange_required_fails_when_missing",
           not g.verify_phase37_exchange_required(
               ex_missing)["ok"])
    _check("F::no_secret_leakage_ok",
           g.verify_phase37_no_secret_leakage(
               [{"ok": True}])["ok"])
    _check("F::no_secret_leakage_detects",
           not g.verify_phase37_no_secret_leakage(
               [{"sealed_payload": "x"}])["ok"])
    paths = [str(_ROOT /
                  "bilingual_voice_phase37_adapter_interface.py")]
    _check("F::no_audio_in_artifacts",
           g.verify_phase37_no_audio_boundary(paths)["ok"])
    _check("F::no_execution_in_artifacts",
           g.verify_phase37_no_execution_boundary(paths)["ok"])
    _check("F::metadata_only_results",
           g.verify_phase37_metadata_only_results(
               [{"produced_audio": False, "invoked_tts": False,
                 "used_subprocess": False, "used_network": False,
                 "wrote_files": False}])["ok"])


def suite_g_result_verifier() -> None:
    import bilingual_voice_phase37_result_verifier as rv
    res = {"result_id": "x", "adapter_name":
           "safety_redaction_trace_metadata_adapter",
           "dry_run": True, "test_only": True,
           "produced_audio": False, "invoked_tts": False,
           "used_subprocess": False, "used_network": False,
           "wrote_files": False}
    _check("G::result_ok",
           rv.verify_phase37_adapter_result(res)["ok"])
    sel = {"receipt_id": "selrec_x", "created_at": 1.0,
           "selected_adapter_name":
               "safety_redaction_trace_metadata_adapter",
           "selected_adapter_type":
               "safety_redaction_trace_metadata_adapter",
           "candidate_adapters": [], "selection_reason": "x",
           "score_summary": {}, "request_id": "x",
           "result_id": "x", "dry_run": True, "test_only": True,
           "execution_boundary_preserved": True,
           "audio_generated": False, "tts_invoked": False,
           "subprocess_used": False, "network_used": False,
           "files_written": False, "audit_chain_hash": "a",
           "notes": "x", "phase": "p"}
    _check("G::selection_ok",
           rv.verify_phase37_selection_receipt(sel, res)["ok"])
    inv = {"receipt_id": "recv_x", "created_at": 1.0,
           "adapter_name":
               "safety_redaction_trace_metadata_adapter",
           "adapter_type":
               "safety_redaction_trace_metadata_adapter",
           "operator_id_hash": "a"*16, "dry_run": True,
           "test_only": True,
           "execution_boundary_preserved": True,
           "audio_generated": False, "tts_invoked": False,
           "subprocess_used": False, "network_used": False,
           "files_written": False, "request_id": "x",
           "result_id": "x", "pre_call_status": "ok",
           "post_call_status": "ok", "audit_chain_hash": "a",
           "notes": "x", "phase": "p"}
    _check("G::invocation_ok",
           rv.verify_phase37_invocation_receipt(inv, res)["ok"])
    out_no_pipe = {"status": "ok",
                     "selected_adapter_result": res,
                     "invocation_receipt": inv,
                     "selection_receipt": sel}
    _check("G::missing_pipeline_fails",
           not rv.verify_phase37_complete_output(out_no_pipe)["ok"])
    bad_res = dict(res)
    bad_res["adapter_name"] = "real_piper"
    _check("G::unknown_adapter_fails",
           not rv.verify_phase37_adapter_result(bad_res)["ok"])
    for flag in ("produced_audio", "invoked_tts",
                  "used_subprocess", "used_network",
                  "wrote_files"):
        bad = dict(res)
        bad[flag] = True
        _check(f"G::{flag}_fails",
               not rv.verify_phase37_adapter_result(bad)["ok"])
    leak = dict(res)
    leak["sealed_payload"] = "x"
    _check("G::secret_leak_fails",
           not rv.verify_phase37_adapter_result(leak)["ok"])


def suite_h_runtime() -> None:
    import bilingual_voice_adapter_phase37_runtime as p37
    r_en = p37.prepare_phase37_four_adapter_invocation(
        user_text="hello luna", draft_response_text="Hi.",
        operator_id="op_local", approve=True)
    _check("H::en_runs", r_en["status"] == "ok",
           json.dumps([e.get("code")
                       for e in r_en.get("errors", [])]))
    _check("H::en_pipeline_present",
           bool(r_en.get("signed_witness_pipeline")))
    r_ru = p37.prepare_phase37_four_adapter_invocation(
        user_text="привет луна", draft_response_text="Привет!",
        user_preference="russian",
        operator_id="op_local", approve=True)
    _check("H::ru_runs", r_ru["status"] == "ok")
    r_mix = p37.prepare_phase37_four_adapter_invocation(
        user_text="mix russian and english",
        draft_response_text="ok, давай.",
        operator_id="op_local", approve=True)
    chosen_mix = ((r_mix.get("selection_choice") or {})
                   .get("chosen") or {}).get("adapter_type")
    _check("H::mix_picks_bilingual_or_safety",
           chosen_mix in ("bilingual_segment_metadata_adapter",
                          "safety_redaction_trace_metadata_adapter"),
           f"got {chosen_mix}")
    r_pros = p37.prepare_phase37_four_adapter_invocation(
        user_text="hello", operator_id="op_local", approve=True,
        preferred_adapter="prosody_density_metadata_adapter")
    chosen_pros = ((r_pros.get("selection_choice") or {})
                    .get("chosen") or {}).get("adapter_type")
    _check("H::pref_prosody_wins",
           chosen_pros == "prosody_density_metadata_adapter")
    r_safety = p37.prepare_phase37_four_adapter_invocation(
        user_text="safety trace please",
        operator_id="op_local", approve=True,
        preferred_adapter=
        "safety_redaction_trace_metadata_adapter")
    chosen_safety = ((r_safety.get("selection_choice") or {})
                       .get("chosen") or {}).get("adapter_type")
    _check("H::pref_safety_wins",
           chosen_safety ==
           "safety_redaction_trace_metadata_adapter")
    r_pref_dum = p37.prepare_phase37_four_adapter_invocation(
        user_text="hello", operator_id="op_local", approve=True,
        preferred_adapter="dummy_metadata_adapter")
    chosen_dum = ((r_pref_dum.get("selection_choice") or {})
                   .get("chosen") or {}).get("adapter_type")
    _check("H::pref_dummy_works",
           chosen_dum == "dummy_metadata_adapter")
    r_no = p37.prepare_phase37_four_adapter_invocation(
        user_text="hi", approve=False)
    _check("H::approve_false_refused", r_no["status"] != "ok")
    r_ks = p37.prepare_phase37_four_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, kill_switch_enabled=True)
    _check("H::kill_switch_blocks",
           r_ks["status"] == "kill_switch_blocked")
    r_no_sig = p37.prepare_phase37_four_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, sign_evidence=False)
    _check("H::sign_false_refused",
           r_no_sig["status"] != "ok")
    r_no_we = p37.prepare_phase37_four_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, include_witness_export=False)
    _check("H::no_witness_export_refused",
           r_no_we["status"] != "ok")
    r_no_ex = p37.prepare_phase37_four_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, include_exchange=False)
    _check("H::no_exchange_refused",
           r_no_ex["status"] != "ok")
    r_handoff_no = p37.prepare_phase37_four_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, include_handoff=True, consent_marker="")
    pipe_h = r_handoff_no.get("signed_witness_pipeline") or {}
    hs = pipe_h.get("handoff_summary") or {}
    _check("H::handoff_requires_consent_marker",
           hs.get("status") == "refused" or
           r_handoff_no["status"] == "ok")
    demo = p37.demo_phase37_four_adapter_invocations(limit=3)
    _check("H::demo_bounded", demo["count"] == 3)


def suite_i_production_safety() -> None:
    en_db = _ROOT / "lexicon" / "luna_vocabulary.sqlite"
    ru_db = _ROOT / "russian_stack" / "russian_lexicon.sqlite"
    link_db = _ROOT / "bilingual_stack" / "bilingual_links.sqlite"
    if en_db.exists():
        c = sqlite3.connect(str(en_db))
        n = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        c.close()
        _check("I::en_2814", n == 2814, f"got {n}")
    if ru_db.exists():
        c = sqlite3.connect(str(ru_db))
        nw = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        np_ = c.execute("SELECT COUNT(*) FROM phrases").fetchone()[0]
        c.close()
        _check("I::ru_2518", nw == 2518, f"got {nw}")
        _check("I::ru_phr_35", np_ == 35, f"got {np_}")
    if link_db.exists():
        c = sqlite3.connect(str(link_db))
        nc = c.execute("SELECT COUNT(*) FROM concepts").fetchone()[0]
        nl = c.execute("SELECT COUNT(*) FROM entry_links").fetchone()[0]
        c.close()
        _check("I::concepts_26", nc >= 26)
        _check("I::links_52", nl >= 52)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"), recursive=True)
        if "backups" not in p]
    _check("I::manifests_90", len(live) == 90, str(len(live)))
    audio = []
    for root, _dirs, files in os.walk(_ROOT / "bilingual_stack" /
                                       "voice_adapter_phase37"):
        for f in files:
            if f.lower().endswith((".wav", ".mp3", ".ogg",
                                    ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files", not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_phase37_adapter_interface.py",
        "bilingual_safety_redaction_trace_adapter.py",
        "bilingual_voice_phase37_selection_policy.py",
        "bilingual_voice_phase37_signed_witness_pipeline.py",
        "bilingual_voice_phase37_governance_recheck.py",
        "bilingual_voice_phase37_result_verifier.py",
        "bilingual_voice_adapter_phase37_runtime.py",
    ]
    forbidden_audio = (
        "pyttsx3", "gtts", "edge_tts", "piper.", "coqui",
        "whisper", "pyaudio", "sounddevice", "pydub",
        "soundfile", "comtypes", "win32com",
    )
    forbidden_exec = (
        "subprocess.run", "subprocess.Popen", "subprocess.call",
        "os.system(", "shell=True", "os.popen",
        "ctypes.windll", "powershell ", "powershell.exe",
    )
    forbidden_net = (
        "urllib.request", "http.client", "requests.", "httpx.",
        "socket.socket",
    )
    forbidden_runtime = (
        "luna_modules", "import worker", "from worker",
        "tier_", "probe_", "attestation",
    )
    forbidden_threading = (
        "threading.Thread", "multiprocessing.Process",
        "multiprocessing.Pool",
        "daemon=True", "asyncio.create_task", "schedule.every",
    )
    for fn in files:
        src = (_ROOT / fn).read_text(encoding="utf-8")
        for tok in forbidden_audio:
            _check(f"J::{fn}::no_audio:{tok.strip()}", tok not in src)
        for tok in forbidden_exec:
            _check(f"J::{fn}::no_exec:{tok.strip()}", tok not in src)
        for tok in forbidden_net:
            _check(f"J::{fn}::no_net:{tok.strip()}", tok not in src)
        for tok in forbidden_runtime:
            _check(f"J::{fn}::no_runtime:{tok.strip()}", tok not in src)
        for tok in forbidden_threading:
            _check(f"J::{fn}::no_daemon:{tok.strip()}", tok not in src)
    import bilingual_voice_phase36_secret_boundary as sb
    scan_dirs = [
        _ROOT / "bilingual_stack" / "voice_adapter_phase37" /
            "reports",
        _ROOT / "bilingual_stack" / "voice_adapter_phase37" /
            "evidence_bundles",
        _ROOT / "bilingual_stack" / "voice_adapter_phase37" /
            "witness_exports",
    ]
    for d in scan_dirs:
        if not d.exists():
            _check(f"J::leak_scan_dir_present:{d.name}", True)
            continue
        scan = sb.validate_no_secret_leakage_in_directory(str(d))
        _check(f"J::no_leak_in:{d.name}", scan["ok"],
               json.dumps(scan.get("leaks", [])))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_interface),
        ("C", suite_c_safety_redaction_trace),
        ("D", suite_d_selection_policy),
        ("E", suite_e_signed_witness_pipeline),
        ("F", suite_f_governance_recheck),
        ("G", suite_g_result_verifier),
        ("H", suite_h_runtime),
        ("I", suite_i_production_safety),
        ("J", suite_j_isolation),
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
