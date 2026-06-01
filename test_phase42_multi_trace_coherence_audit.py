"""Phase 42 test harness - multi-trace coherence audit."""

from __future__ import annotations

import copy
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


_PHASE42_MODULES = (
    "bilingual_voice_phase42_audit_contract",
    "bilingual_voice_phase42_scenario_builder",
    "bilingual_voice_phase42_trace_runner",
    "bilingual_voice_phase42_coherence_auditor",
    "bilingual_voice_phase42_replay_matrix",
    "bilingual_voice_phase42_drift_stability_matrix",
    "bilingual_voice_phase42_operator_packet",
    "bilingual_voice_phase42_runtime",
)


def suite_a_preflight() -> None:
    upstream = [
        "PHASE41_MEMORY_CONTINUITY_ADAPTER_GOVERNANCE_REPORT.md",
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
        "bilingual_voice_adapter_phase41_runtime.py",
        "bilingual_voice_phase41_adapter_interface.py",
        "bilingual_memory_continuity_audit_adapter.py",
        "bilingual_voice_phase41_replay_bridge.py",
        "bilingual_voice_phase40_replay_verifier.py",
        "bilingual_voice_phase39_runtime.py",
        "bilingual_voice_phase38_status_dashboard.py",
        "bilingual_voice_memory_runtime.py",
        "bilingual_voice_memory_state.py",
    ]
    for f in upstream:
        _check(f"A::upstream_present::{f}",
               (_ROOT / f).exists(), f)
    for m in _PHASE42_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    for sub in ("contracts", "trace_runs",
                 "coherence_audits", "replay_projections",
                 "drift_matrices", "governance_rechecks",
                 "operator_packets", "dashboards",
                 "reports", "fixtures", "demos"):
        d = (_ROOT / "bilingual_stack"
                   / "voice_adapter_phase42" / sub)
        _check(f"A::folder::{sub}", d.exists(), str(d))
    for m in _PHASE42_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_audit_contract() -> None:
    import bilingual_voice_phase42_audit_contract as ac
    sch = ac.get_phase42_audit_contract_schema()
    _check("B::schema_is_dict", isinstance(sch, dict))
    _check("B::schema_dry_run",
           sch.get("rehearsal_dry_run_only") is True)
    _check("B::schema_no_new_adapter",
           sch.get("new_adapter_invocation_forbidden")
           is True)
    scen = ac.get_phase42_required_scenarios()
    for must in ("simple_english", "russian_first",
                  "mixed_code_switch", "high_prosody",
                  "safety_redaction", "memory_continuity",
                  "approve_false_refusal",
                  "kill_switch_refusal"):
        _check(f"B::scenario::{must}", must in scen)
    cov = ac.get_phase42_required_adapter_coverage()
    for must in ("dummy_metadata_adapter",
                  "bilingual_segment_metadata_adapter",
                  "prosody_density_metadata_adapter",
                  "safety_redaction_trace_metadata_adapter",
                  "memory_continuity_audit_metadata_adapter"):
        _check(f"B::coverage::{must}", must in cov)
    _check("B::coverage_count_5", len(cov) == 5)
    forb = ac.get_phase42_forbidden_actions()
    for must in ("new_adapter_invocation",
                  "generate_audio", "invoke_tts",
                  "run_subprocess", "network_call",
                  "multiprocessing", "corpus_import"):
        _check(f"B::forb::{must}", must in forb)
    c = ac.create_phase42_audit_contract(
        audit_id="aid_test", scenario_count=8)
    val = ac.validate_phase42_audit_contract(c)
    _check("B::contract_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    # Reject non-dict
    bad = ac.validate_phase42_audit_contract("notdict")
    _check("B::validator_rejects_non_dict",
           bad.get("ok") is False)
    # Reject out-of-range scenario_count
    c_out = ac.create_phase42_audit_contract(
        "aid", scenario_count=99)
    _check("B::scenario_count_clamped",
           c_out.get("scenario_count") == 12)
    # Drift catches: flip dry_run
    drift = dict(c)
    drift["rehearsal_dry_run_only"] = False
    bad2 = ac.validate_phase42_audit_contract(drift)
    _check("B::validator_catches_non_dry_run",
           bad2.get("ok") is False)
    # Write
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = ac.write_phase42_audit_contract_report(
            c, str(out))
        _check("B::contract_written", Path(p).exists())


def suite_c_scenario_builder() -> None:
    import bilingual_voice_phase42_scenario_builder as sb
    scen = sb.create_phase42_scenarios()
    _check("C::scenario_count_8", len(scen) == 8)
    expected = ("simple_english", "russian_first",
                 "mixed_code_switch", "high_prosody",
                 "safety_redaction", "memory_continuity",
                 "approve_false_refusal",
                 "kill_switch_refusal")
    types = [s.get("scenario_type") for s in scen]
    for must in expected:
        _check(f"C::scenario_type::{must}",
               must in types)
    # Per-scenario validation
    for s in scen:
        v = sb.validate_phase42_scenario(s)
        _check(f"C::scenario_validates::"
               f"{s.get('scenario_id')}",
               v.get("ok") is True,
               ",".join(v.get("reasons", [])))
    # Memory scenario has voice_memory_state
    mem = next(s for s in scen
                if s.get("scenario_type")
                == "memory_continuity")
    _check("C::memory_has_vms",
           isinstance(mem.get("voice_memory_state"), dict)
           and bool(mem.get("voice_memory_state")))
    # Memory vms is sanitized (no raw_transcript etc.)
    for k in ("raw_transcript", "full_transcript",
              "sensitive_facts", "personal_facts"):
        _check(f"C::memory_vms_no_banned::{k}",
               k not in (mem.get("voice_memory_state")
                         or {}))
    # Safety scenario does not echo unsafe terms in full --
    # the user_text is just "Safety redaction check"
    saf = next(s for s in scen
                if s.get("scenario_type")
                == "safety_redaction")
    _check("C::safety_text_safe",
           "Safety redaction check" in str(
               saf.get("user_text") or ""))
    # Refusal scenario approve=False
    ref = next(s for s in scen
                if s.get("scenario_type")
                == "approve_false_refusal")
    _check("C::refusal_approve_false",
           ref.get("approve") is False)
    # Kill switch scenario kill_switch_enabled=True
    ks = next(s for s in scen
               if s.get("scenario_type")
               == "kill_switch_refusal")
    _check("C::ks_enabled",
           ks.get("kill_switch_enabled") is True)
    # Per-builder smoke
    for fn in ("create_simple_english_scenario",
                "create_russian_first_scenario",
                "create_mixed_code_switch_scenario",
                "create_high_prosody_scenario",
                "create_safety_redaction_scenario",
                "create_memory_continuity_scenario",
                "create_refusal_scenario",
                "create_kill_switch_scenario"):
        s = getattr(sb, fn)()
        v = sb.validate_phase42_scenario(s)
        _check(f"C::builder_validates::{fn}",
               v.get("ok") is True)
    # Reject non-dict
    bad = sb.validate_phase42_scenario("notdict")
    _check("C::validator_rejects_non_dict",
           bad.get("ok") is False)
    # Reject banned field at top level
    s_bad = dict(scen[0])
    s_bad["raw_transcript"] = "leak"
    bad2 = sb.validate_phase42_scenario(s_bad)
    _check("C::validator_catches_raw_transcript",
           bad2.get("ok") is False)
    # Write
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "sc.json"
        p = sb.write_phase42_scenario_report(
            {"scenarios": scen}, str(out))
        _check("C::scenarios_written",
               Path(p).exists())


def suite_d_trace_runner() -> None:
    import bilingual_voice_phase42_trace_runner as tr
    import bilingual_voice_phase42_scenario_builder as sb
    scen = sb.create_phase42_scenarios()
    results = tr.run_phase42_trace_batch(scenarios=scen)
    _check("D::trace_count_8", len(results) == 8)
    # Per-result validate
    for r in results:
        v = tr.validate_phase42_trace_result(r)
        _check(f"D::result_validates::"
               f"{r.get('scenario_id')}",
               v.get("ok") is True,
               ",".join(v.get("reasons", [])))
    # Status by scenario type
    by_type = {r.get("scenario_type"): r for r in results}
    for t in ("simple_english", "russian_first",
               "mixed_code_switch", "high_prosody",
               "safety_redaction", "memory_continuity"):
        r = by_type.get(t)
        _check(f"D::ok_status::{t}",
               r is not None and r.get("status") == "ok",
               str(r and r.get("status")))
    ref = by_type.get("approve_false_refusal")
    _check("D::refusal_status",
           ref is not None
           and "refused" in str(ref.get("status") or ""))
    ks = by_type.get("kill_switch_refusal")
    _check("D::ks_blocked",
           ks is not None
           and ks.get("status") == "kill_switch_blocked")
    # Raw operator_id absent in every result
    for r in results:
        _check(f"D::no_raw_operator_id::"
               f"{r.get('scenario_id')}",
               "operator_id" not in r
               or r.get("operator_id") in (None, ""))
    # Refusal/kill-switch must NOT have signed pipeline ok
    _check("D::refusal_no_signed_pipeline",
           ref is None
           or ref.get("signed_pipeline_status") != "ok")
    _check("D::ks_no_signed_pipeline",
           ks is None
           or ks.get("signed_pipeline_status") != "ok")
    summary = tr.summarize_phase42_trace_batch(results)
    _check("D::summary_ok", summary.get("ok") is True)
    _check("D::summary_ok_count",
           summary.get("ok_count") == 6,
           str(summary.get("ok_count")))
    # Refuse non-dict scenario
    bad = tr.run_phase42_trace_scenario("notdict")
    _check("D::refuses_non_dict",
           bad.get("status") == "refused")
    # Write batch
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "tb.json"
        p = tr.write_phase42_trace_batch(
            results, str(out))
        _check("D::batch_written", Path(p).exists())


def suite_e_coherence_auditor() -> None:
    import bilingual_voice_phase42_coherence_auditor as ca
    import bilingual_voice_phase42_trace_runner as tr
    results = tr.run_phase42_trace_batch()
    audit = ca.create_phase42_coherence_audit(results)
    val = ca.validate_phase42_coherence_audit(audit)
    _check("E::audit_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("E::audit_ok",
           audit.get("ok") is True,
           audit.get("summary"))
    _check("E::adapter_coverage_ok",
           audit["adapter_coverage"]["ok"] is True)
    _check("E::status_coherence_ok",
           audit["status_coherence"]["ok"] is True)
    _check("E::evidence_coherence_ok",
           audit["evidence_coherence"]["ok"] is True)
    _check("E::projection_coherence_ok",
           audit["replay_projection_coherence"]["ok"]
           is True)
    _check("E::memory_privacy_ok",
           audit["memory_privacy"]["ok"] is True)
    _check("E::boundary_preservation_ok",
           audit["boundary_preservation"]["ok"] is True)
    # Missing adapter: drop the memory scenario, audit must
    # catch missing coverage
    drop = [r for r in results
             if r.get("scenario_type")
             != "memory_continuity"]
    drop_audit = ca.create_phase42_coherence_audit(drop)
    _check("E::missing_adapter_catches_coverage",
           drop_audit["adapter_coverage"]["ok"] is False)
    # Inject a refusal that claims signed pipeline ok
    mutated = copy.deepcopy(results)
    for r in mutated:
        if r.get("scenario_type") \
                == "approve_false_refusal":
            r["signed_pipeline_status"] = "ok"
    mutated_audit = ca.create_phase42_coherence_audit(
        mutated)
    _check("E::status_catches_refusal_with_pipeline",
           mutated_audit["status_coherence"]["ok"]
           is False)
    # Inject runtime flag: boundary catches
    mutated2 = copy.deepcopy(results)
    mutated2[0]["selected_result_metadata"] = dict(
        mutated2[0].get("selected_result_metadata") or {})
    mutated2[0]["selected_result_metadata"][
        "produced_audio"] = True
    mutated2_audit = ca.create_phase42_coherence_audit(
        mutated2)
    _check("E::boundary_catches_audio",
           mutated2_audit["boundary_preservation"]["ok"]
           is False)
    # Inject memory privacy fail
    mutated3 = copy.deepcopy(results)
    for r in mutated3:
        if (r.get("selected_result_metadata") or {}).get(
                "adapter_type") == \
                "memory_continuity_audit_metadata_adapter":
            r["selected_result_metadata"] = dict(
                r["selected_result_metadata"])
            r["selected_result_metadata"][
                "raw_transcript_absent"] = False
    mutated3_audit = ca.create_phase42_coherence_audit(
        mutated3)
    _check("E::memory_privacy_catches_raw",
           mutated3_audit["memory_privacy"]["ok"]
           is False)
    # Inject evidence drop
    mutated4 = copy.deepcopy(results)
    for r in mutated4:
        if r.get("status") == "ok":
            r["witness_export_status"] = "failed"
            break
    mutated4_audit = ca.create_phase42_coherence_audit(
        mutated4)
    _check("E::evidence_catches_missing_witness",
           mutated4_audit["evidence_coherence"]["ok"]
           is False)
    # Inject projection drop
    mutated5 = copy.deepcopy(results)
    for r in mutated5:
        if r.get("status") == "ok":
            r["replay_projection_present"] = False
            break
    mutated5_audit = ca.create_phase42_coherence_audit(
        mutated5)
    _check("E::projection_catches_missing",
           mutated5_audit["replay_projection_coherence"]
                          ["ok"] is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "ca.json"
        p = ca.write_phase42_coherence_audit_report(
            audit, str(out))
        _check("E::audit_written", Path(p).exists())


def suite_f_replay_matrix() -> None:
    import bilingual_voice_phase42_replay_matrix as rm
    import bilingual_voice_phase42_trace_runner as tr
    results = tr.run_phase42_trace_batch()
    matrix = rm.create_phase42_replay_matrix(results)
    val = rm.validate_phase42_replay_matrix(matrix)
    _check("F::matrix_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("F::projection_count_6",
           matrix.get("projection_count") == 6,
           str(matrix.get("projection_count")))
    _check("F::compat_ok",
           matrix.get("compatibility_status") == "ok")
    verify = rm.verify_phase42_replay_projections(matrix)
    _check("F::projections_verify",
           verify.get("ok") is True,
           ",".join(verify.get("reasons", []))[:300])
    # No banned fields
    for k in ("raw_transcript", "operator_id",
              "signing_key_material", "audio_bytes",
              "command"):
        _check(f"F::no_banned::{k}",
               k not in matrix
               or matrix.get(k) in
               (None, "", False, [], {}))
    # No adapter re-invocation: matrix has no
    # call_adapter / dispatch fields
    for k in ("call_adapter", "dispatch_adapter",
              "adapter_call_count"):
        _check(f"F::no_reinvocation_field::{k}",
               k not in matrix)
    # Reject non-dict
    bad = rm.validate_phase42_replay_matrix("notdict")
    _check("F::validator_rejects_non_dict",
           bad.get("ok") is False)
    summary = rm.summarize_phase42_replay_matrix(matrix)
    _check("F::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "rm.json"
        p = rm.write_phase42_replay_matrix(
            matrix, str(out))
        _check("F::matrix_written", Path(p).exists())


def suite_g_drift_stability_matrix() -> None:
    import bilingual_voice_phase42_drift_stability_matrix \
        as dsm
    import bilingual_voice_phase42_trace_runner as tr
    import bilingual_voice_phase42_coherence_auditor as ca
    import bilingual_voice_phase42_replay_matrix as rm
    results = tr.run_phase42_trace_batch()
    ca_audit = ca.create_phase42_coherence_audit(results)
    rmat = rm.create_phase42_replay_matrix(results)
    matrix = dsm.create_phase42_drift_stability_matrix(
        results, coherence_audit=ca_audit,
        replay_matrix=rmat)
    val = dsm.validate_phase42_drift_stability_matrix(
        matrix)
    _check("G::matrix_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("G::matrix_ok",
           matrix.get("ok") is True,
           matrix.get("summary"))
    _check("G::no_fail_clean",
           matrix.get("fail_count") == 0)
    # Adapter selection drift: mutate adapter to a wrong
    # value but keep expected_adapter_family
    mutated = copy.deepcopy(results)
    for r in mutated:
        if r.get("scenario_type") == "memory_continuity":
            r["adapter_matches_expected"] = False
            r["selected_adapter_name"] = \
                "dummy_metadata_adapter"
    asd = dsm.detect_phase42_adapter_selection_drift(
        {"trace_results": mutated})
    _check("G::adapter_drift_detected",
           asd.get("drifted") is True)
    # Boundary drift detection
    mutated2 = copy.deepcopy(results)
    mutated2[0]["selected_result_metadata"] = dict(
        mutated2[0].get("selected_result_metadata") or {})
    mutated2[0]["selected_result_metadata"][
        "used_subprocess"] = True
    bdd = dsm.detect_phase42_boundary_drift(
        {"trace_results": mutated2})
    _check("G::boundary_drift_detected",
           bdd.get("drifted") is True)
    # Baseline drift uses live DB; this should pass (no
    # mutation possible without writing DB)
    bld = dsm.detect_phase42_baseline_drift(
        {"trace_results": results})
    _check("G::baseline_no_drift",
           bld.get("drifted") is False,
           ",".join(bld.get("drifts") or []))
    # Phase 21 status — clean tree should be BLOCKED
    p21 = dsm.detect_phase42_phase21_status_drift(
        {"trace_results": results})
    _check("G::phase21_blocked",
           p21.get("phase21_status_text") == "BLOCKED")
    summary = dsm.summarize_phase42_drift_stability(
        matrix)
    _check("G::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "dsm.json"
        p = dsm.write_phase42_drift_stability_matrix(
            matrix, str(out))
        _check("G::matrix_written", Path(p).exists())


def suite_h_operator_packet() -> None:
    import bilingual_voice_phase42_operator_packet as op
    import bilingual_voice_phase42_audit_contract as ac
    import bilingual_voice_phase42_scenario_builder as sb
    import bilingual_voice_phase42_trace_runner as tr
    import bilingual_voice_phase42_coherence_auditor as caa
    import bilingual_voice_phase42_replay_matrix as rm
    import bilingual_voice_phase42_drift_stability_matrix \
        as dsm
    contract = ac.create_phase42_audit_contract(
        "aid_h", 8)
    scenarios = sb.create_phase42_scenarios()
    results = tr.run_phase42_trace_batch(scenarios)
    ca_audit = caa.create_phase42_coherence_audit(results)
    rmat = rm.create_phase42_replay_matrix(results)
    dmat = dsm.create_phase42_drift_stability_matrix(
        results, ca_audit, rmat)
    pkt = op.create_phase42_operator_packet(
        contract, scenarios, results, ca_audit, rmat,
        dmat)
    val = op.validate_phase42_operator_packet(pkt)
    _check("H::packet_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("H::audit_status_ok",
           pkt.get("audit_status") in
           ("ok", "ok_with_warnings"),
           str(pkt.get("audit_status")))
    # No banned fields
    for k in ("operator_id", "signing_key_material",
              "raw_transcript", "audio_bytes", "command"):
        _check(f"H::no_banned::{k}",
               k not in pkt or pkt.get(k) in
               (None, "", False, [], {}))
    # Phase 21 included
    p21 = pkt.get("phase21_import_status") or {}
    _check("H::phase21_present",
           "status_text" in p21)
    _check("H::phase21_blocked",
           p21.get("status_text") == "BLOCKED"
           or "STAGED" in str(p21.get("status_text", "")))
    _check("H::next_phase_present",
           bool(pkt.get("next_recommended_phase")))
    # Markdown
    md = op.create_phase42_operator_packet_markdown(pkt)
    _check("H::md_nonempty",
           isinstance(md, str) and len(md) > 400)
    for needle in ("Phase 42", "Trace count",
                    "Phase 21", "Next recommended"):
        _check(f"H::md_contains::{needle}",
               needle in md, needle)
    # Reject non-dict
    bad = op.validate_phase42_operator_packet("notdict")
    _check("H::validator_rejects_non_dict",
           bad.get("ok") is False)
    # Inject banned field
    drift = dict(pkt)
    drift["operator_id"] = "raw"
    drift_val = op.validate_phase42_operator_packet(drift)
    _check("H::validator_catches_operator_id",
           drift_val.get("ok") is False)
    summary = op.summarize_phase42_operator_packet(pkt)
    _check("H::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "pkt.json"
        out_md = Path(td) / "pkt.md"
        p1 = op.write_phase42_operator_packet(
            pkt, str(out))
        p2 = op.write_phase42_operator_packet_markdown(
            md, str(out_md))
        _check("H::packet_written", Path(p1).exists())
        _check("H::md_written", Path(p2).exists())


def suite_i_phase42_runtime() -> None:
    import bilingual_voice_phase42_runtime as rt
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase42")
    out = rt.run_phase42_multi_trace_audit(
        operator_id="phase42_operator",
        output_dir=str(base))
    _check("I::status_ok",
           out.get("status") in
           ("ok", "ok_with_warnings"),
           str(out.get("status")))
    val = rt.validate_phase42_multi_trace_output(out)
    _check("I::output_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("I::trace_count_8",
           len(out.get("trace_results") or []) == 8)
    _check("I::phase21_blocked",
           out.get("phase21_status") == "BLOCKED")
    # Artifacts written
    for sub, fname in (
        ("contracts", "audit_contract.json"),
        ("trace_runs", "trace_batch.json"),
        ("coherence_audits", "coherence_audit.json"),
        ("replay_projections", "replay_matrix.json"),
        ("drift_matrices",
         "drift_stability_matrix.json"),
        ("operator_packets", "operator_packet.json"),
        ("dashboards", "OPERATOR_PACKET.md"),
    ):
        p = base / sub / fname
        _check(f"I::written::{sub}/{fname}",
               p.exists(), str(p))
    summary = rt.summarize_phase42_multi_trace_output(out)
    _check("I::summary_ok", summary.get("ok") is True)
    # Demo bounded
    out2 = rt.run_phase42_multi_trace_audit(
        operator_id="phase42_operator",
        output_dir=None, limit=4)
    _check("I::demo_bounded_le_4",
           len(out2.get("trace_results") or []) <= 4)
    # No production DB writes (counts unchanged, checked
    # in suite J via DB queries)


def suite_j_production_isolation() -> None:
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
        _check("J::en_2814", n == 2814, f"got {n}")
    if ru_db.exists():
        c = sqlite3.connect(str(ru_db))
        nw = c.execute(
            "SELECT COUNT(*) FROM words").fetchone()[0]
        np_ = c.execute(
            "SELECT COUNT(*) FROM phrases").fetchone()[0]
        c.close()
        _check("J::ru_2518", nw == 2518)
        _check("J::ru_phr_35", np_ == 35)
    if link_db.exists():
        c = sqlite3.connect(str(link_db))
        nc = c.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0]
        nl = c.execute(
            "SELECT COUNT(*) FROM entry_links").fetchone()[0]
        c.close()
        _check("J::concepts_26", nc == 26)
        _check("J::links_52", nl == 52)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"),
        recursive=True) if "backups" not in p]
    _check("J::manifests_90", len(live) == 90,
           str(len(live)))
    audio = []
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase42")
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("J::no_audio_in_voice_adapter_phase42",
           not audio, ",".join(audio))
    # Isolation: forbidden-token scan over Phase 42
    # modules
    files = [f"{m}.py" for m in _PHASE42_MODULES]
    files.append("test_phase42_multi_trace_coherence_audit.py")
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
    # Harness file is the scanner — skip itself
    scan_files = [f for f in files
                   if f != "test_phase42_multi_trace_coherence_audit.py"]
    for fn in scan_files:
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
    # Phase 36 secret-boundary scan over all Phase 42
    # outputs
    import bilingual_voice_phase36_secret_boundary as sb
    scan_dirs = [base / sub for sub in (
        "contracts", "trace_runs", "coherence_audits",
        "replay_projections", "drift_matrices",
        "governance_rechecks", "operator_packets",
        "dashboards", "reports", "demos")]
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
        "bilingual_voice_adapter_phase41_runtime",
        "bilingual_voice_phase41_adapter_interface",
        "bilingual_memory_continuity_audit_adapter",
        "bilingual_voice_phase41_replay_bridge",
        "bilingual_voice_phase40_replay_verifier",
        "bilingual_voice_phase39_runtime",
        "bilingual_voice_phase38_status_dashboard",
        "bilingual_voice_adapter_phase37_runtime",
    ]
    for m in upstream:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reimport {m}: {e}")
        _check(f"K::reimport::{m}", ok)
    for m in _PHASE42_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    try:
        import bilingual_voice_phase42_runtime as rt
        out = rt.run_phase42_multi_trace_audit(
            operator_id="phase42_operator",
            output_dir=None)
        _check("K::e2e_status_ok",
               out.get("status") in
               ("ok", "ok_with_warnings"),
               str(out.get("status")))
        _check("K::e2e_phase21_blocked",
               out.get("phase21_status") == "BLOCKED")
    except Exception as e:  # noqa: BLE001
        _check("K::e2e_no_exception", False, str(e))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_audit_contract),
        ("C", suite_c_scenario_builder),
        ("D", suite_d_trace_runner),
        ("E", suite_e_coherence_auditor),
        ("F", suite_f_replay_matrix),
        ("G", suite_g_drift_stability_matrix),
        ("H", suite_h_operator_packet),
        ("I", suite_i_phase42_runtime),
        ("J", suite_j_production_isolation),
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
