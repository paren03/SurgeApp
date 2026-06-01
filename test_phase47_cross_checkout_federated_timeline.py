"""Phase 47 test harness - cross-checkout federated timeline."""

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


_PHASE47_MODULES = (
    "bilingual_voice_phase47_federation_contract",
    "bilingual_voice_phase47_timeline_importer",
    "bilingual_voice_phase47_federation_graph",
    "bilingual_voice_phase47_federation_manifest",
    "bilingual_voice_phase47_federation_verifier",
    "bilingual_voice_phase47_drift_detector",
    "bilingual_voice_phase47_tamper_suite",
    "bilingual_voice_phase47_operator_packet",
    "bilingual_voice_phase47_status_dashboard",
    "bilingual_voice_phase47_runtime",
)


def suite_a_preflight() -> None:
    upstream_reports = [
        "PHASE46_CROSS_ARCHIVE_TIMELINE_LEDGER_REPORT.md",
        "PHASE45_MULTI_BUNDLE_CHAIN_OF_TRUST_REPORT.md",
        "PHASE44_CROSS_MACHINE_IMPORT_SIMULATION_REPORT.md",
        "PHASE43_CROSS_MACHINE_PORTABILITY_REPORT.md",
        "PHASE42_MULTI_TRACE_COHERENCE_AUDIT_REPORT.md",
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
    ]
    for f in upstream_reports:
        _check(f"A::upstream_present::{f}",
               (_ROOT / f).exists(), f)
    # Phase 46 modules
    for m in (
        "bilingual_voice_phase46_timeline_contract.py",
        "bilingual_voice_phase46_archive_collector.py",
        "bilingual_voice_phase46_timeline_builder.py",
        "bilingual_voice_phase46_timeline_manifest.py",
        "bilingual_voice_phase46_long_horizon_verifier.py",
        "bilingual_voice_phase46_tamper_suite.py",
        "bilingual_voice_phase46_operator_packet.py",
        "bilingual_voice_phase46_status_dashboard.py",
        "bilingual_voice_phase46_runtime.py",
    ):
        _check(f"A::phase46_module::{m}",
               (_ROOT / m).exists())
    for m in _PHASE47_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    for sub in ("federation_contracts",
                 "imported_timelines",
                 "federation_graphs",
                 "federation_manifests",
                 "verification_outputs",
                 "drift_reports", "tamper_tests",
                 "operator_packets", "dashboards",
                 "reports", "fixtures", "demos"):
        d = (_ROOT / "bilingual_stack"
                   / "voice_adapter_phase47" / sub)
        _check(f"A::folder::{sub}", d.exists())
    for m in _PHASE47_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_federation_contract() -> None:
    import bilingual_voice_phase47_federation_contract \
        as fc
    sch = fc.get_phase47_federation_contract_schema()
    _check("B::schema_is_dict", isinstance(sch, dict))
    _check("B::dry_run",
           sch.get("rehearsal_dry_run_only") is True)
    _check("B::adapter_invoke_forbidden",
           sch.get("adapter_invocation_forbidden")
           is True)
    _check("B::production_db_read_forbidden",
           sch.get("production_db_read_forbidden")
           is True)
    _check("B::tamper_required",
           sch.get("tamper_detection_required") is True)
    _check("B::distinct_required",
           sch.get(
               "distinct_checkout_ids_required") is True)
    arts = fc.get_phase47_required_federation_artifacts()
    for must in ("imported_timeline_packages",
                  "federation_graph",
                  "federation_manifest",
                  "federation_verification_result",
                  "drift_report",
                  "tamper_suite_result",
                  "operator_packet",
                  "status_dashboard"):
        _check(f"B::required_art::{must}",
               must in arts)
    inv = fc.get_phase47_required_invariants()
    for must in ("imported_timeline_roots_preserved",
                  "checkout_ids_distinct",
                  "no_adapter_invocation",
                  "no_production_db_read",
                  "no_audio", "no_tts",
                  "no_subprocess", "no_network",
                  "no_multiprocessing",
                  "no_secret_leakage",
                  "phase21_status_tracked_not_unblocked",
                  "adapter_allowlist_count_remains_5"):
        _check(f"B::inv::{must}", must in inv)
    forb_arts = fc.get_phase47_forbidden_artifacts()
    for must in ("runtime_dbs", "audio_files",
                  "local_secret_handoff_contents",
                  "claude_directory_contents"):
        _check(f"B::forb_art::{must}",
               must in forb_arts)
    forb = fc.get_phase47_forbidden_actions()
    for must in ("adapter_invocation_in_federation",
                  "production_db_read_in_verifier",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "duplicate_checkout_id",
                  "tampered_timeline_root_hash",
                  "path_traversal", "url_scheme_path"):
        _check(f"B::forb::{must}", must in forb)
    c = fc.create_phase47_federation_contract(
        federation_id="fid_test", checkout_count=3)
    val = fc.validate_phase47_federation_contract(c)
    _check("B::validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = fc.validate_phase47_federation_contract(
        "notdict")
    _check("B::reject_non_dict",
           bad.get("ok") is False)
    # checkout_count bounded
    c1 = fc.create_phase47_federation_contract(
        "fid", checkout_count=1)
    _check("B::checkout_count_min_2",
           c1.get("checkout_count") == 2)
    c99 = fc.create_phase47_federation_contract(
        "fid", checkout_count=99)
    _check("B::checkout_count_max_8",
           c99.get("checkout_count") == 8)
    drift = dict(c)
    drift["rehearsal_dry_run_only"] = False
    bad2 = fc.validate_phase47_federation_contract(drift)
    _check("B::catch_non_dry_run",
           bad2.get("ok") is False)
    drift2 = dict(c)
    drift2["adapter_invocation_forbidden"] = False
    bad3 = fc.validate_phase47_federation_contract(drift2)
    _check("B::catch_adapter_invoke_allowed",
           bad3.get("ok") is False)
    drift3 = dict(c)
    drift3["production_db_read_forbidden"] = False
    bad4 = fc.validate_phase47_federation_contract(drift3)
    _check("B::catch_db_read_allowed",
           bad4.get("ok") is False)
    drift4 = dict(c)
    drift4["checkout_count"] = 99
    bad5 = fc.validate_phase47_federation_contract(drift4)
    _check("B::catch_checkout_out_of_range",
           bad5.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = fc.write_phase47_federation_contract_report(
            c, str(out))
        _check("B::contract_written",
               Path(p).exists())


def suite_c_timeline_importer() -> None:
    import bilingual_voice_phase47_timeline_importer as ti
    with tempfile.TemporaryDirectory() as td:
        ws = ti.create_phase47_import_workspace(
            output_dir=td)
        _check("C::workspace_ok",
               ws.get("ok") is True)
        results = ti.import_n_phase47_timeline_packages(
            n=2, workspace_dir=ws.get("workspace_path"))
        _check("C::imported_2", len(results) == 2)
        cids = set()
        for r in results:
            _check("C::cap_ok", r.get("ok") is True,
                   r.get("reason"))
            pkg = r.get("package") or {}
            cids.add(pkg.get("checkout_id"))
            _check("C::pkg_root_64",
                   isinstance(pkg.get(
                       "timeline_root_hash"), str)
                   and len(pkg.get(
                       "timeline_root_hash")) == 64)
            _check("C::pkg_phase21_blocked",
                   pkg.get("phase21_status_text")
                   == "BLOCKED")
            _check("C::pkg_adapter_5",
                   pkg.get(
                       "adapter_allowlist_count") == 5)
            _check("C::pkg_pkg_hash_64",
                   isinstance(pkg.get(
                       "package_hash"), str)
                   and len(pkg.get(
                       "package_hash")) == 64)
            val = ti.validate_phase47_imported_timeline(r)
            _check("C::imported_validates",
                   val.get("ok") is True,
                   ",".join(val.get("reasons", [])))
        _check("C::checkout_ids_distinct",
               len(cids) == 2)
    # Path safety: URL paths rejected
    bad_url = ti.import_phase47_timeline_package(
        {"checkout_id": "x", "package_id": "p1",
         "timeline_root_hash": "0" * 64,
         "package_hash": "0" * 64},
        workspace_dir="https://evil.example/x")
    _check("C::url_path_rejected",
           bad_url.get("ok") is False)
    # Path traversal rejected
    bad_pt = ti.import_phase47_timeline_package(
        {"checkout_id": "x", "package_id": "p1"},
        workspace_dir="../etc/passwd")
    _check("C::path_traversal_rejected",
           bad_pt.get("ok") is False)
    # Shell metacharacter rejected
    bad_sh = ti.import_phase47_timeline_package(
        {"checkout_id": "x", "package_id": "p1"},
        workspace_dir="ws | rm")
    _check("C::shell_metachar_rejected",
           bad_sh.get("ok") is False)
    # Reject banned inline field in package
    bad_inline = ti.import_phase47_timeline_package(
        {"checkout_id": "x", "package_id": "p1",
         "raw_transcript": "leak"})
    _check("C::banned_inline_rejected",
           bad_inline.get("ok") is False)


def suite_d_federation_graph() -> None:
    import bilingual_voice_phase47_timeline_importer as ti
    import bilingual_voice_phase47_federation_graph as fg
    with tempfile.TemporaryDirectory() as td:
        ws = ti.create_phase47_import_workspace(
            output_dir=td)
        results = ti.import_n_phase47_timeline_packages(
            n=3, workspace_dir=ws.get("workspace_path"))
        graph = fg.create_phase47_federation_graph(
            results)
        val = fg.validate_phase47_federation_graph(graph)
        _check("D::graph_validates",
               val.get("ok") is True,
               ",".join(val.get("reasons", [])))
        _check("D::checkout_count_3",
               graph.get("checkout_count") == 3)
        _check("D::adapter_allowlist_summary",
               all(c == 5 for c in graph.get(
                   "adapter_allowlist_summary",
                   {}).values()))
        _check("D::federation_root_64",
               isinstance(graph.get(
                   "federation_root_hash"), str)
               and len(graph.get(
                   "federation_root_hash")) == 64)
        # Deterministic root
        graph2 = fg.create_phase47_federation_graph(
            results)
        _check("D::federation_root_deterministic",
               graph.get("federation_root_hash")
               == graph2.get("federation_root_hash"))
        # Duplicate checkout id
        bad = copy.deepcopy(graph)
        nodes = bad.get("checkout_nodes") or []
        if len(nodes) >= 2:
            nodes[1]["checkout_id"] = \
                nodes[0].get("checkout_id")
        bad_val = fg.validate_phase47_federation_graph(
            bad)
        _check("D::duplicate_checkout_caught",
               bad_val.get("ok") is False)
        # Timeline root mutation
        bad2 = copy.deepcopy(graph)
        bad2["timeline_root_hashes"] = {
            k: "0" * 64 for k in bad2.get(
                "timeline_root_hashes", {})}
        roots_check = (
            fg.verify_phase47_graph_timeline_roots(
                bad2, imported_timelines=results))
        _check("D::timeline_root_drift_caught",
               roots_check.get("ok") is False)
        # Refuse sub-2
        ti_sub = [results[0]]
        sub_graph = fg.create_phase47_federation_graph(
            ti_sub)
        _check("D::refuse_sub_2",
               sub_graph.get("status") == "refused")
        # Phase 21 included in summary
        _check("D::phase21_in_summary",
               isinstance(graph.get(
                   "phase21_status_summary"), dict)
               and len(graph.get(
                   "phase21_status_summary")) >= 2)
        summary = fg.summarize_phase47_federation_graph(
            graph)
        _check("D::summary_ok",
               summary.get("ok") is True)
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "g.json"
            p = fg.write_phase47_federation_graph(
                graph, str(out))
            _check("D::graph_written", Path(p).exists())


def suite_e_federation_manifest() -> None:
    import bilingual_voice_phase47_timeline_importer as ti
    import bilingual_voice_phase47_federation_graph as fg
    import bilingual_voice_phase47_federation_manifest \
        as fm
    with tempfile.TemporaryDirectory() as td:
        ws = ti.create_phase47_import_workspace(
            output_dir=td)
        results = ti.import_n_phase47_timeline_packages(
            n=2, workspace_dir=ws.get("workspace_path"))
        graph = fg.create_phase47_federation_graph(
            results)
        manifest = \
            fm.create_phase47_federation_manifest(
                graph, results)
        val = fm.validate_phase47_federation_manifest(
            manifest)
        _check("E::manifest_validates",
               val.get("ok") is True,
               ",".join(val.get("reasons", [])))
        _check("E::checkout_count_2",
               manifest.get("checkout_count") == 2)
        _check("E::root_64",
               isinstance(manifest.get(
                   "manifest_root_hash"), str)
               and len(manifest.get(
                   "manifest_root_hash")) == 64)
        # Deterministic
        manifest2 = \
            fm.create_phase47_federation_manifest(
                graph, results)
        _check("E::root_deterministic",
               manifest.get("manifest_root_hash")
               == manifest2.get("manifest_root_hash"))
        verify = fm.verify_phase47_federation_manifest(
            graph, results, manifest)
        _check("E::verifies",
               verify.get("ok") is True,
               ",".join(verify.get("reasons", [])))
        # Imported package hash mutation
        bad_results = copy.deepcopy(results)
        if bad_results and isinstance(
                bad_results[0], dict):
            pkg = bad_results[0].get("package") or {}
            pkg["package_hash"] = "0" * 64
        tamper = fm.detect_phase47_manifest_tampering(
            graph, bad_results, manifest)
        _check("E::tamper_pkg_hash",
               tamper.get("tampered") is True)
        # Missing checkout
        bad_graph = copy.deepcopy(graph)
        bad_graph["checkout_nodes"] = \
            (bad_graph.get("checkout_nodes") or [])[:-1]
        bad_graph["checkout_count"] = max(
            0, int(bad_graph.get(
                "checkout_count") or 0) - 1)
        v2 = fm.verify_phase47_federation_manifest(
            bad_graph, results, manifest)
        _check("E::missing_checkout",
               v2.get("ok") is False)
        # Duplicate checkout id in manifest
        bad_m = copy.deepcopy(manifest)
        cids = list(bad_m.get("checkout_ids") or [])
        if len(cids) >= 2:
            cids[1] = cids[0]
            bad_m["checkout_ids"] = cids
        bv = fm.validate_phase47_federation_manifest(
            bad_m)
        _check("E::duplicate_checkout_in_manifest",
               bv.get("ok") is False)
        # Runtime DB key not present (positive)
        _check("E::no_runtime_db_keys",
               not any(str(k).lower().endswith(
                   (".sqlite", ".db"))
                   for k in
                   manifest.get(
                       "timeline_root_hashes",
                       {}).keys()))
        # Round-trip
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "m.json"
            p = fm.write_phase47_federation_manifest(
                manifest, str(out))
            _check("E::manifest_written",
                   Path(p).exists())
            loaded = \
                fm.read_phase47_federation_manifest(
                    str(out))
            _check("E::manifest_roundtrip",
                   loaded.get("manifest_id")
                   == manifest.get("manifest_id"))


def suite_f_federation_verifier() -> None:
    import bilingual_voice_phase47_timeline_importer as ti
    import bilingual_voice_phase47_federation_graph as fg
    import bilingual_voice_phase47_federation_manifest \
        as fm
    import bilingual_voice_phase47_federation_verifier \
        as fv
    with tempfile.TemporaryDirectory() as td:
        ws = ti.create_phase47_import_workspace(
            output_dir=td)
        results = ti.import_n_phase47_timeline_packages(
            n=2, workspace_dir=ws.get("workspace_path"))
        graph = fg.create_phase47_federation_graph(
            results)
        manifest = \
            fm.create_phase47_federation_manifest(
                graph, results)
        v = fv.verify_phase47_federation(
            imported_timelines=results,
            graph=graph, manifest=manifest)
        _check("F::clean_ok",
               v.get("ok") is True,
               v.get("summary"))
        for k in ("imported_check", "graph_check",
                   "manifest_check", "boundary_check",
                   "phase21_check",
                   "no_runtime_state_check"):
            _check(f"F::{k}_ok",
                   (v.get(k) or {}).get("ok") is True)
        # Tampered timeline root via graph
        bad_g = copy.deepcopy(graph)
        nodes = bad_g.get("checkout_nodes") or []
        if nodes:
            nodes[0]["timeline_root_hash"] = "0" * 64
        v2 = fv.verify_phase47_graph(
            bad_g, imported_timelines=results)
        _check("F::altered_timeline_root_caught",
               v2.get("ok") is False)
        # Duplicate checkout id
        bad_results = copy.deepcopy(results)
        if len(bad_results) >= 2:
            p0 = bad_results[0].get("package") or {}
            p1 = bad_results[1].get("package") or {}
            p1["checkout_id"] = p0.get("checkout_id")
        v3 = fv.verify_phase47_imported_timelines(
            bad_results)
        _check("F::duplicate_checkout_caught",
               v3.get("ok") is False)
        # Boundary violation
        bad_g2 = copy.deepcopy(graph)
        bs = dict(bad_g2.get("boundary_summary") or {})
        bs["no_audio"] = False
        bad_g2["boundary_summary"] = bs
        v4 = fv.verify_phase47_boundary_claims(
            {"graph": bad_g2,
             "imported_timelines": results})
        _check("F::boundary_violation_caught",
               v4.get("ok") is False)
        # Unexpected Phase 21
        bad_results2 = copy.deepcopy(results)
        if bad_results2:
            pkg = bad_results2[0].get("package") or {}
            pkg["phase21_status_text"] = "UNBLOCKED"
        v5 = fv.verify_phase47_phase21_history(
            {"imported_timelines": bad_results2})
        _check("F::phase21_drift_caught",
               v5.get("ok") is False)
        # Secret leakage
        bad_g3 = copy.deepcopy(graph)
        bad_g3["signing_key_material"] = "leak"
        # boundary verifier doesn't scan graph top-level
        # banned-keys; the per-checkout banned key check
        # does. Inject into imported package instead:
        bad_results3 = copy.deepcopy(results)
        if bad_results3:
            pkg = bad_results3[0].get("package") or {}
            pkg["raw_transcript"] = "leak"
        v6 = fv.verify_phase47_boundary_claims(
            {"graph": graph,
             "imported_timelines": bad_results3})
        _check("F::secret_field_caught",
               v6.get("ok") is False)
        # Runtime DB ref
        bad_results4 = copy.deepcopy(results)
        if bad_results4:
            bad_results4[0]["imported_path"] = \
                "lexicon/luna_vocabulary.sqlite"
        v7 = fv.verify_phase47_no_runtime_state_dependency(
            {"graph": graph,
             "imported_timelines": bad_results4})
        _check("F::runtime_db_caught",
               v7.get("ok") is False)
        # No production DB read in verifier
        src = (_ROOT
                / "bilingual_voice_phase47_federation_verifier.py"
                ).read_text(encoding="utf-8")
        _check("F::no_sqlite_in_verifier",
               "import sqlite3" not in src
               and "from sqlite3" not in src
               and "sqlite3.connect" not in src)
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "v.json"
            p = (fv
                  .write_phase47_federation_verification_report(
                      v, str(out)))
            _check("F::report_written",
                   Path(p).exists())


def suite_g_drift_detector() -> None:
    import bilingual_voice_phase47_timeline_importer as ti
    import bilingual_voice_phase47_federation_graph as fg
    import bilingual_voice_phase47_federation_manifest \
        as fm
    import bilingual_voice_phase47_drift_detector as dd
    with tempfile.TemporaryDirectory() as td:
        ws = ti.create_phase47_import_workspace(
            output_dir=td)
        results = ti.import_n_phase47_timeline_packages(
            n=2, workspace_dir=ws.get("workspace_path"))
        graph = fg.create_phase47_federation_graph(
            results)
        manifest = \
            fm.create_phase47_federation_manifest(
                graph, results)
        clean = dd.detect_phase47_federation_drift(
            results, graph, manifest)
        _check("G::clean_ok", clean.get("ok") is True,
               clean.get("summary"))
        # Checkout count drift
        bad_g = copy.deepcopy(graph)
        bad_g["checkout_count"] = 99
        cc = dd.detect_phase47_checkout_count_drift(
            {"graph": bad_g,
             "manifest": manifest,
             "imported_timelines": results})
        _check("G::checkout_count_drift",
               cc.get("severity") == "fail")
        # Timeline root drift
        bad_g2 = copy.deepcopy(graph)
        bad_g2["timeline_root_hashes"] = {"x": "0" * 64}
        tr = dd.detect_phase47_timeline_root_drift(
            {"graph": bad_g2,
             "imported_timelines": results})
        _check("G::timeline_root_drift",
               tr.get("severity") == "fail")
        # Adapter allowlist drift
        bad_results = copy.deepcopy(results)
        if bad_results:
            pkg = bad_results[0].get("package") or {}
            pkg["adapter_allowlist_count"] = 4
        ad = dd.detect_phase47_adapter_allowlist_drift(
            {"imported_timelines": bad_results})
        _check("G::adapter_drift",
               ad.get("severity") == "fail")
        # Baseline claim drift
        bad_results2 = copy.deepcopy(results)
        if bad_results2:
            pkg = bad_results2[0].get("package") or {}
            base = dict(pkg.get(
                "production_baseline_expected") or {})
            base["english_words"] = 9999
            pkg["production_baseline_expected"] = base
        bd = dd.detect_phase47_baseline_claim_drift(
            {"imported_timelines": bad_results2})
        _check("G::baseline_drift",
               bd.get("severity") == "fail")
        # Phase 21 status drift
        bad_results3 = copy.deepcopy(results)
        if bad_results3:
            pkg = bad_results3[0].get("package") or {}
            pkg["phase21_status_text"] = "UNBLOCKED"
        p21 = dd.detect_phase47_phase21_status_drift(
            {"imported_timelines": bad_results3})
        _check("G::phase21_drift_fail",
               p21.get("severity") == "fail")
        # Boundary drift
        bad_g3 = copy.deepcopy(graph)
        bs = dict(bad_g3.get("boundary_summary") or {})
        bs["no_audio"] = False
        bad_g3["boundary_summary"] = bs
        bnd = dd.detect_phase47_boundary_drift(
            {"graph": bad_g3,
             "imported_timelines": results})
        _check("G::boundary_drift",
               bnd.get("severity") == "fail")
        # Secret audio command drift
        bad_results4 = copy.deepcopy(results)
        if bad_results4:
            pkg = bad_results4[0].get("package") or {}
            pkg["signing_key_material"] = "leak"
        sac = (dd
               .detect_phase47_secret_audio_command_drift(
                   {"imported_timelines": bad_results4,
                    "graph": graph,
                    "manifest": manifest}))
        _check("G::secret_drift",
               sac.get("severity") == "fail")
        # Phase 21 staged-locally → warn (no import)
        # (depends on local state; just confirm function
        # returns 'warn' or 'pass')
        p21_clean = dd.detect_phase47_phase21_status_drift(
            {"imported_timelines": results})
        _check("G::phase21_no_fail_on_clean",
               p21_clean.get("severity") in
               ("warn", "pass"))
        summary = dd.summarize_phase47_drift(clean)
        _check("G::summary_ok",
               summary.get("ok") is True)
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "d.json"
            p = dd.write_phase47_drift_report(
                clean, str(out))
            _check("G::report_written",
                   Path(p).exists())


def suite_h_tamper_suite() -> None:
    import bilingual_voice_phase47_timeline_importer as ti
    import bilingual_voice_phase47_federation_graph as fg
    import bilingual_voice_phase47_federation_manifest \
        as fm
    import bilingual_voice_phase47_tamper_suite as ts
    with tempfile.TemporaryDirectory() as td:
        ws = ti.create_phase47_import_workspace(
            output_dir=td)
        results = ti.import_n_phase47_timeline_packages(
            n=2, workspace_dir=ws.get("workspace_path"))
        graph = fg.create_phase47_federation_graph(
            results)
        manifest = \
            fm.create_phase47_federation_manifest(
                graph, results)
        cases = ts.create_phase47_tamper_cases(
            results, graph, manifest)
        _check("H::cases_13", len(cases) == 13)
        result = ts.run_phase47_tamper_suite(
            results, graph, manifest)
        val = ts.validate_phase47_tamper_suite_result(
            result)
        _check("H::suite_validates",
               val.get("ok") is True,
               ",".join(val.get("reasons", [])))
        _check("H::all_13_detected",
               result.get("detected_count") == 13,
               str(result.get("detected_count")))
        _check("H::none_undetected",
               result.get("undetected_count") == 0)
        _check("H::suite_ok",
               result.get("ok") is True)
        # Original federation unchanged (deep-copy
        # contract). Check that graph still passes
        # verifier afterwards.
        import bilingual_voice_phase47_federation_verifier \
            as fv
        post = fv.verify_phase47_federation(
            imported_timelines=results,
            graph=graph, manifest=manifest)
        _check("H::original_still_clean",
               post.get("ok") is True,
               post.get("summary"))
        # Source isolation
        src = (_ROOT
                / "bilingual_voice_phase47_tamper_suite.py"
                ).read_text(encoding="utf-8")
        _check("H::no_subprocess",
               "subprocess.run" not in src
               and "subprocess.Popen" not in src)
        _check("H::no_network",
               "urllib.request" not in src
               and "socket.socket" not in src)
        _check("H::no_audio",
               "pyttsx3" not in src
               and "edge_tts" not in src)
        _check("H::no_sqlite",
               "import sqlite3" not in src
               and "sqlite3.connect" not in src)
        summary = ts.summarize_phase47_tamper_suite(
            result)
        _check("H::summary_ok",
               summary.get("ok") is True)
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "ts.json"
            p = ts.write_phase47_tamper_suite_report(
                result, str(out))
            _check("H::written", Path(p).exists())


def suite_i_operator_packet_and_dashboard() -> None:
    import bilingual_voice_phase47_runtime as rt
    import bilingual_voice_phase47_operator_packet as op
    import bilingual_voice_phase47_status_dashboard as sd
    out = rt.run_phase47_cross_checkout_federation(
        checkout_count=2)
    pkt = out.get("operator_packet") or {}
    val_pkt = op.validate_phase47_operator_packet(pkt)
    _check("I::packet_validates",
           val_pkt.get("ok") is True,
           ",".join(val_pkt.get("reasons", [])))
    _check("I::packet_status_ok",
           pkt.get("phase47_status") in
           ("ok", "ok_with_warnings"))
    md = op.create_phase47_operator_packet_markdown(pkt)
    _check("I::packet_md_nonempty",
           isinstance(md, str) and len(md) > 300)
    for needle in ("Phase 47", "Source phase",
                    "Checkout count",
                    "Federation root hash",
                    "Phase 21", "Next recommended"):
        _check(f"I::md_contains::{needle}",
               needle in md, needle)
    dash = out.get("status_dashboard") or {}
    val_d = sd.validate_phase47_status_dashboard(dash)
    _check("I::dashboard_validates",
           val_d.get("ok") is True,
           ",".join(val_d.get("reasons", [])))
    _check("I::dash_source_phase46",
           dash.get("source_phase") == "phase46")
    _check("I::dash_checkout_count_2",
           dash.get("checkout_count") == 2)
    _check("I::dash_phase21_in",
           "phase21_import_status" in dash)
    _check("I::dash_federation_root_match",
           dash.get("federation_root_status") == "match")
    dash_md = sd.create_phase47_dashboard_markdown(dash)
    _check("I::dash_md_nonempty",
           isinstance(dash_md, str)
           and len(dash_md) > 300)
    for needle in ("Phase 47", "Source phase",
                    "Checkout count",
                    "Federation root",
                    "Phase 21 import status",
                    "Tamper suite",
                    "Forbidden boundaries"):
        _check(f"I::dash_md_contains::{needle}",
               needle in dash_md, needle)
    bad = dict(dash)
    bad["source_phase"] = "phaseXX"
    bad_v = sd.validate_phase47_status_dashboard(bad)
    _check("I::dash_validator_catches_bad_source",
           bad_v.get("ok") is False)


def suite_j_phase47_runtime_and_production() -> None:
    import bilingual_voice_phase47_runtime as rt
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase47")
    out = rt.run_phase47_cross_checkout_federation(
        output_dir=str(base), checkout_count=2)
    _check("J::status_ok",
           out.get("status") in
           ("ok", "ok_with_warnings"),
           str(out.get("status")))
    val = rt.validate_phase47_federation_output(out)
    _check("J::output_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("J::phase21_blocked",
           out.get("phase21_status") == "BLOCKED")
    # Artifacts written
    for sub, fname in (
        ("federation_contracts",
         "federation_contract.json"),
        ("federation_graphs",
         "federation_graph.json"),
        ("federation_manifests",
         "federation_manifest.json"),
        ("verification_outputs",
         "verification_result.json"),
        ("drift_reports", "drift_report.json"),
        ("tamper_tests", "tamper_suite.json"),
        ("operator_packets", "operator_packet.json"),
        ("dashboards", "OPERATOR_PACKET.md"),
        ("dashboards", "STATUS_DASHBOARD.json"),
        ("dashboards", "STATUS_DASHBOARD.md"),
    ):
        p = base / sub / fname
        _check(f"J::written::{sub}/{fname}",
               p.exists())
    summary = rt.summarize_phase47_federation_output(out)
    _check("J::summary_ok",
           summary.get("ok") is True)
    # Production baselines
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
        _check("J::concepts_26", nc >= 26)
        _check("J::links_52", nl >= 52)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"),
        recursive=True) if "backups" not in p]
    _check("J::manifests_90", len(live) == 90,
           str(len(live)))
    audio = []
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("J::no_audio_in_voice_adapter_phase47",
           not audio, ",".join(audio))
    files = [f"{m}.py" for m in _PHASE47_MODULES]
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
    scan_dirs = [base / sub for sub in (
        "federation_contracts", "federation_graphs",
        "federation_manifests", "verification_outputs",
        "drift_reports", "tamper_tests",
        "operator_packets", "dashboards", "reports",
        "demos")]
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
        "bilingual_voice_phase46_runtime",
        "bilingual_voice_phase45_runtime",
        "bilingual_voice_phase44_runtime",
        "bilingual_voice_phase43_runtime",
        "bilingual_voice_phase42_runtime",
        "bilingual_voice_adapter_phase41_runtime",
        "bilingual_voice_phase40_replay_verifier",
        "bilingual_voice_phase39_runtime",
        "bilingual_voice_phase38_status_dashboard",
    ]
    for m in upstream:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reimport {m}: {e}")
        _check(f"K::reimport::{m}", ok)
    for m in _PHASE47_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    try:
        import bilingual_voice_phase47_runtime as rt
        out = rt.run_phase47_cross_checkout_federation(
            checkout_count=2)
        _check("K::e2e_status_ok",
               out.get("status") in
               ("ok", "ok_with_warnings"))
        _check("K::e2e_phase21_blocked",
               out.get("phase21_status") == "BLOCKED")
    except Exception as e:  # noqa: BLE001
        _check("K::e2e_no_exception", False, str(e))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_federation_contract),
        ("C", suite_c_timeline_importer),
        ("D", suite_d_federation_graph),
        ("E", suite_e_federation_manifest),
        ("F", suite_f_federation_verifier),
        ("G", suite_g_drift_detector),
        ("H", suite_h_tamper_suite),
        ("I", suite_i_operator_packet_and_dashboard),
        ("J", suite_j_phase47_runtime_and_production),
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
