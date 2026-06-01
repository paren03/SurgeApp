"""Phase 46 test harness - cross-archive long-horizon timeline."""

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


_PHASE46_MODULES = (
    "bilingual_voice_phase46_timeline_contract",
    "bilingual_voice_phase46_archive_collector",
    "bilingual_voice_phase46_timeline_builder",
    "bilingual_voice_phase46_timeline_manifest",
    "bilingual_voice_phase46_long_horizon_verifier",
    "bilingual_voice_phase46_tamper_suite",
    "bilingual_voice_phase46_operator_packet",
    "bilingual_voice_phase46_status_dashboard",
    "bilingual_voice_phase46_runtime",
)


def suite_a_preflight() -> None:
    upstream_reports = [
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
    # Phase 45 modules + tests required
    p45_modules = [
        "bilingual_voice_phase45_archive_contract.py",
        "bilingual_voice_phase45_archive_builder.py",
        "bilingual_voice_phase45_archive_manifest.py",
        "bilingual_voice_phase45_chain_ledger.py",
        "bilingual_voice_phase45_archive_verifier.py",
        "bilingual_voice_phase45_tamper_suite.py",
        "bilingual_voice_phase45_operator_packet.py",
        "bilingual_voice_phase45_status_dashboard.py",
        "bilingual_voice_phase45_runtime.py",
    ]
    for f in p45_modules:
        _check(f"A::phase45_module_present::{f}",
               (_ROOT / f).exists(), f)
    for m in _PHASE46_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    for sub in ("timeline_contracts", "captured_archives",
                 "timelines", "timeline_manifests",
                 "verification_outputs", "tamper_tests",
                 "operator_packets", "dashboards",
                 "reports", "fixtures", "demos"):
        d = (_ROOT / "bilingual_stack"
                   / "voice_adapter_phase46" / sub)
        _check(f"A::folder::{sub}", d.exists())
    for m in _PHASE46_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_timeline_contract() -> None:
    import bilingual_voice_phase46_timeline_contract as tc
    sch = tc.get_phase46_timeline_contract_schema()
    _check("B::schema_is_dict", isinstance(sch, dict))
    _check("B::dry_run",
           sch.get("rehearsal_dry_run_only") is True)
    _check("B::adapter_invoke_forbidden",
           sch.get("adapter_invocation_forbidden")
           is True)
    _check("B::production_db_read_forbidden",
           sch.get("production_db_read_forbidden")
           is True)
    _check("B::monotonic_required",
           sch.get("monotonic_ordering_required")
           is True)
    _check("B::tamper_required",
           sch.get("tamper_detection_required") is True)
    fields = tc.get_phase46_required_per_archive_fields()
    for must in ("archive_id", "created_at",
                  "source_phases", "phase_counts",
                  "artifact_count", "artifact_hashes",
                  "phase21_status_text",
                  "boundary_summary"):
        _check(f"B::per_archive::{must}",
               must in fields)
    inv = tc.get_phase46_required_chain_invariants()
    for must in ("monotonic_created_at",
                  "deterministic_root_hash",
                  "all_archives_phase21_blocked_or_staged",
                  "all_archives_boundary_intact",
                  "no_runtime_db_reference_in_timeline",
                  "no_adapter_invocation_in_timeline"):
        _check(f"B::invariant::{must}", must in inv)
    forb = tc.get_phase46_forbidden_actions()
    for must in ("adapter_invocation_in_timeline",
                  "production_db_read_in_verifier",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "broken_monotonic_order",
                  "tampered_archive_hash",
                  "duplicate_archive_id"):
        _check(f"B::forb::{must}", must in forb)
    c = tc.create_phase46_timeline_contract(
        timeline_id="tid_test")
    val = tc.validate_phase46_timeline_contract(c)
    _check("B::validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = tc.validate_phase46_timeline_contract(
        "notdict")
    _check("B::reject_non_dict",
           bad.get("ok") is False)
    drift = dict(c)
    drift["rehearsal_dry_run_only"] = False
    bad2 = tc.validate_phase46_timeline_contract(drift)
    _check("B::catch_non_dry_run",
           bad2.get("ok") is False)
    drift2 = dict(c)
    drift2["monotonic_ordering_required"] = False
    bad3 = tc.validate_phase46_timeline_contract(drift2)
    _check("B::catch_non_monotonic_required",
           bad3.get("ok") is False)
    drift3 = dict(c)
    drift3["production_db_read_forbidden"] = False
    bad4 = tc.validate_phase46_timeline_contract(drift3)
    _check("B::catch_db_read_allowed",
           bad4.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = tc.write_phase46_timeline_contract_report(
            c, str(out))
        _check("B::contract_written", Path(p).exists())


def suite_c_archive_collector() -> None:
    import bilingual_voice_phase46_archive_collector as ac
    with tempfile.TemporaryDirectory() as td:
        # Capture 2 archives with timestamp spacing
        results = ac.capture_n_phase45_archives(
            2, output_dir=td, spacing_seconds=1.05)
        _check("C::captured_2", len(results) == 2)
        for r in results:
            _check("C::cap_ok", r.get("ok") is True)
            _check("C::cap_sha_64",
                   isinstance(r.get(
                       "captured_sha256"), str)
                   and len(r.get(
                       "captured_sha256")) == 64)
            _check("C::artifact_count_25",
                   r.get("artifact_count") == 25)
            _check("C::phase21_blocked",
                   r.get("phase21_status_text")
                   == "BLOCKED")
        # Distinct archive_ids
        aids = {r.get("archive_id") for r in results}
        _check("C::archive_ids_distinct",
               len(aids) == 2)
        # Monotonic timestamps
        ts0 = results[0].get("archive_created_at") or 0
        ts1 = results[1].get("archive_created_at") or 0
        _check("C::monotonic_timestamps", ts1 >= ts0)
        # Load
        coll = ac.load_captured_archives(base_dir=td)
        _check("C::collection_ok",
               coll.get("ok") is True)
        _check("C::collection_count_2",
               coll.get("count") == 2)
        summary = ac.summarize_collected_archives(coll)
        _check("C::collector_summary_ok",
               summary.get("ok") is True)


def suite_d_timeline_builder() -> None:
    import bilingual_voice_phase46_archive_collector as ac
    import bilingual_voice_phase46_timeline_builder as tb
    with tempfile.TemporaryDirectory() as td:
        ac.capture_n_phase45_archives(
            3, output_dir=td, spacing_seconds=1.05)
        coll = ac.load_captured_archives(base_dir=td)
        timeline = tb.build_phase46_timeline(coll)
        val = tb.validate_phase46_timeline(timeline)
        _check("D::timeline_validates",
               val.get("ok") is True,
               ",".join(val.get("reasons", [])))
        _check("D::archive_count_3",
               timeline.get("archive_count") == 3)
        _check("D::chain_link_count_2",
               len(timeline.get("chain_links") or [])
               == 2)
        # Strict monotonic order
        prev = -1.0
        for s in timeline.get("ordered_archives") or []:
            ts = float(s.get(
                "archive_created_at") or 0)
            _check("D::monotonic", ts >= prev,
                   f"{ts}<{prev}")
            prev = ts
        # All chain links ok
        for c in timeline.get("chain_links") or []:
            _check(f"D::link_ok::{c.get('index')}",
                   c.get("ok") is True)
        # Reject sub-2 input
        bad_coll = {"entries":
                     coll.get("entries", [])[:1],
                     "count": 1, "ok": True}
        bad_timeline = tb.build_phase46_timeline(
            bad_coll)
        _check("D::reject_sub_2",
               bad_timeline.get("status") == "refused")
        # Reject non-dict
        bad_t = tb.validate_phase46_timeline("notdict")
        _check("D::validator_rejects_non_dict",
               bad_t.get("ok") is False)
        # Drift
        drift = copy.deepcopy(timeline)
        drift["rehearsal_dry_run_only"] = False
        bad_t2 = tb.validate_phase46_timeline(drift)
        _check("D::catch_non_dry_run",
               bad_t2.get("ok") is False)
        # Banned field
        drift2 = copy.deepcopy(timeline)
        drift2["raw_transcript"] = "leak"
        bad_t3 = tb.validate_phase46_timeline(drift2)
        _check("D::catch_raw_transcript",
               bad_t3.get("ok") is False)
        summary = tb.summarize_phase46_timeline(timeline)
        _check("D::summary_ok",
               summary.get("ok") is True)
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "t.json"
            p = tb.write_phase46_timeline(
                timeline, str(out))
            _check("D::timeline_written",
                   Path(p).exists())


def suite_e_timeline_manifest() -> None:
    import bilingual_voice_phase46_archive_collector as ac
    import bilingual_voice_phase46_timeline_builder as tb
    import bilingual_voice_phase46_timeline_manifest as tm
    with tempfile.TemporaryDirectory() as td:
        ac.capture_n_phase45_archives(
            3, output_dir=td, spacing_seconds=1.05)
        coll = ac.load_captured_archives(base_dir=td)
        timeline = tb.build_phase46_timeline(coll)
        manifest = tm.create_phase46_timeline_manifest(
            timeline)
        val = tm.validate_phase46_timeline_manifest(
            manifest)
        _check("E::manifest_validates",
               val.get("ok") is True,
               ",".join(val.get("reasons", [])))
        _check("E::archive_count_3",
               manifest.get("archive_count") == 3)
        _check("E::root_64",
               isinstance(manifest.get(
                   "manifest_root_hash"), str)
               and len(manifest.get(
                   "manifest_root_hash")) == 64)
        # Deterministic
        manifest2 = tm.create_phase46_timeline_manifest(
            timeline)
        _check("E::root_deterministic",
               manifest.get("manifest_root_hash")
               == manifest2.get("manifest_root_hash"))
        verify = tm.verify_phase46_timeline_manifest(
            timeline, manifest)
        _check("E::verifies",
               verify.get("ok") is True,
               ",".join(verify.get("reasons", [])))
        # Tampered timeline
        bad = copy.deepcopy(timeline)
        if bad.get("ordered_archives"):
            bad["ordered_archives"][0][
                "captured_sha256"] = "0" * 64
        tamper = tm.detect_phase46_manifest_tampering(
            bad, manifest)
        _check("E::tamper_detects_sha",
               tamper.get("tampered") is True)
        # Round-trip
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "m.json"
            p = tm.write_phase46_timeline_manifest(
                manifest, str(out))
            _check("E::manifest_written",
                   Path(p).exists())
            loaded = tm.read_phase46_timeline_manifest(
                str(out))
            _check("E::manifest_roundtrip",
                   loaded.get("manifest_id")
                   == manifest.get("manifest_id"))


def suite_f_long_horizon_verifier() -> None:
    import bilingual_voice_phase46_archive_collector as ac
    import bilingual_voice_phase46_timeline_builder as tb
    import bilingual_voice_phase46_timeline_manifest as tm
    import bilingual_voice_phase46_long_horizon_verifier \
        as lhv
    with tempfile.TemporaryDirectory() as td:
        ac.capture_n_phase45_archives(
            3, output_dir=td, spacing_seconds=1.05)
        coll = ac.load_captured_archives(base_dir=td)
        timeline = tb.build_phase46_timeline(coll)
        manifest = tm.create_phase46_timeline_manifest(
            timeline)
        result = lhv.verify_phase46_long_horizon_timeline(
            timeline, manifest=manifest)
        _check("F::clean_ok",
               result.get("ok") is True,
               result.get("summary"))
        for k in ("monotonic_check", "unique_ids_check",
                   "chain_check", "boundary_check",
                   "phase21_check",
                   "no_runtime_state_check",
                   "root_hash_check",
                   "manifest_check"):
            _check(f"F::{k}_ok",
                   (result.get(k) or {}).get("ok")
                   is True)
        # Break monotonic
        bad = copy.deepcopy(timeline)
        if len(bad.get("ordered_archives") or []) >= 2:
            bad["ordered_archives"][1][
                "archive_created_at"] = \
                float(bad["ordered_archives"][0].get(
                    "archive_created_at") or 0) - 5
        r2 = lhv.verify_phase46_monotonic_ordering(bad)
        _check("F::monotonic_break_caught",
               r2.get("ok") is False)
        # Duplicate archive id
        bad2 = copy.deepcopy(timeline)
        if len(bad2.get("ordered_archives") or []) >= 2:
            bad2["ordered_archives"][1][
                "archive_id"] = \
                bad2["ordered_archives"][0].get(
                    "archive_id")
        r3 = lhv.verify_phase46_unique_archive_ids(bad2)
        _check("F::duplicate_id_caught",
               r3.get("ok") is False)
        # Boundary violation per archive
        bad3 = copy.deepcopy(timeline)
        if bad3.get("ordered_archives"):
            bs = dict(bad3["ordered_archives"][0].get(
                "boundary_summary") or {})
            bs[
                "no_adapter_invocation_in_archive"] = \
                False
            bad3["ordered_archives"][0][
                "boundary_summary"] = bs
        r4 = lhv.verify_phase46_boundary_claims(bad3)
        _check("F::archive_boundary_violation_caught",
               r4.get("ok") is False)
        # Boundary violation timeline-level
        bad4 = copy.deepcopy(timeline)
        bs = dict(bad4.get("boundary_summary") or {})
        bs["no_adapter_invocation_in_timeline"] = False
        bad4["boundary_summary"] = bs
        r5 = lhv.verify_phase46_boundary_claims(bad4)
        _check("F::timeline_boundary_violation_caught",
               r5.get("ok") is False)
        # Phase 21 drift
        bad5 = copy.deepcopy(timeline)
        bad5["phase21_status_text"] = "UNBLOCKED"
        r6 = lhv.verify_phase46_phase21_claim(bad5)
        _check("F::phase21_drift_caught",
               r6.get("ok") is False)
        # Root hash drift
        bad6 = copy.deepcopy(timeline)
        bad6["timeline_root_hash"] = "0" * 64
        r7 = lhv.verify_phase46_timeline_root_hash(bad6)
        _check("F::root_hash_drift_caught",
               r7.get("ok") is False)
        # No production DB read source check
        src = (_ROOT
                / "bilingual_voice_phase46_long_horizon_verifier.py"
                ).read_text(encoding="utf-8")
        _check("F::no_sqlite_in_verifier",
               "import sqlite3" not in src
               and "from sqlite3" not in src
               and "sqlite3.connect" not in src)
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "v.json"
            p = lhv.write_phase46_long_horizon_verification_report(
                result, str(out))
            _check("F::report_written",
                   Path(p).exists())


def suite_g_tamper_suite() -> None:
    import bilingual_voice_phase46_archive_collector as ac
    import bilingual_voice_phase46_timeline_builder as tb
    import bilingual_voice_phase46_timeline_manifest as tm
    import bilingual_voice_phase46_tamper_suite as ts
    with tempfile.TemporaryDirectory() as td:
        ac.capture_n_phase45_archives(
            3, output_dir=td, spacing_seconds=1.05)
        coll = ac.load_captured_archives(base_dir=td)
        timeline = tb.build_phase46_timeline(coll)
        manifest = tm.create_phase46_timeline_manifest(
            timeline)
        cases = ts.create_phase46_tamper_cases(timeline)
        _check("G::cases_12", len(cases) == 12)
        for c in cases:
            _check(f"G::case_expected::{c.get('case')}",
                   c.get("expected_detection") is True)
        result = ts.run_phase46_tamper_suite(
            timeline, manifest=manifest)
        val = ts.validate_phase46_tamper_suite_result(
            result)
        _check("G::suite_validates",
               val.get("ok") is True,
               ",".join(val.get("reasons", [])))
        _check("G::all_12_detected",
               result.get("detected_count") == 12,
               str(result.get("detected_count")))
        _check("G::none_undetected",
               result.get("undetected_count") == 0)
        _check("G::suite_ok",
               result.get("ok") is True)
        # Original timeline unchanged after suite
        _check("G::original_count_unchanged",
               timeline.get("archive_count") == 3)
        # Source isolation
        src = (_ROOT
                / "bilingual_voice_phase46_tamper_suite.py"
                ).read_text(encoding="utf-8")
        _check("G::no_subprocess",
               "subprocess.run" not in src
               and "subprocess.Popen" not in src)
        _check("G::no_network",
               "urllib.request" not in src
               and "socket.socket" not in src)
        _check("G::no_audio",
               "pyttsx3" not in src
               and "edge_tts" not in src)
        _check("G::no_sqlite",
               "import sqlite3" not in src
               and "sqlite3.connect" not in src)
        summary = ts.summarize_phase46_tamper_suite(
            result)
        _check("G::summary_ok",
               summary.get("ok") is True)
        with tempfile.TemporaryDirectory() as td2:
            out = Path(td2) / "ts.json"
            p = ts.write_phase46_tamper_suite_report(
                result, str(out))
            _check("G::written", Path(p).exists())


def suite_h_operator_packet_and_dashboard() -> None:
    import bilingual_voice_phase46_runtime as rt
    import bilingual_voice_phase46_operator_packet as op
    import bilingual_voice_phase46_status_dashboard as sd
    out = rt.run_phase46_long_horizon_timeline(
        archive_count=3)
    pkt = out.get("operator_packet") or {}
    val_pkt = op.validate_phase46_operator_packet(pkt)
    _check("H::packet_validates",
           val_pkt.get("ok") is True,
           ",".join(val_pkt.get("reasons", [])))
    _check("H::packet_status_ok",
           pkt.get("phase46_status") in
           ("ok", "ok_with_warnings"))
    md = op.create_phase46_operator_packet_markdown(pkt)
    _check("H::packet_md_nonempty",
           isinstance(md, str) and len(md) > 300)
    for needle in ("Phase 46", "Source phase",
                    "Monotonic ordering",
                    "Chain integrity",
                    "Long-horizon verification",
                    "Phase 21", "Next recommended"):
        _check(f"H::md_contains::{needle}",
               needle in md, needle)
    dash = out.get("status_dashboard") or {}
    val_d = sd.validate_phase46_status_dashboard(dash)
    _check("H::dashboard_validates",
           val_d.get("ok") is True,
           ",".join(val_d.get("reasons", [])))
    _check("H::dash_source_phase45",
           dash.get("source_phase") == "phase45")
    _check("H::dash_archive_count_3",
           dash.get("archive_count") == 3)
    ts_status = dash.get("tamper_suite_status") or {}
    _check("H::dash_tamper_ok",
           ts_status.get("ok") is True)
    _check("H::dash_tamper_12",
           ts_status.get("case_count") == 12)
    _check("H::dash_phase21_in",
           "phase21_import_status" in dash)
    dash_md = sd.create_phase46_dashboard_markdown(dash)
    _check("H::dash_md_nonempty",
           isinstance(dash_md, str)
           and len(dash_md) > 300)
    for needle in ("Phase 46", "Source phase",
                    "Monotonic ordering",
                    "Phase 21 import status",
                    "Tamper suite",
                    "Forbidden boundaries"):
        _check(f"H::dash_md_contains::{needle}",
               needle in dash_md, needle)
    bad = dict(dash)
    bad["source_phase"] = "phaseXX"
    bad_v = sd.validate_phase46_status_dashboard(bad)
    _check("H::dash_validator_catches_bad_source",
           bad_v.get("ok") is False)


def suite_i_phase46_runtime() -> None:
    import bilingual_voice_phase46_runtime as rt
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase46")
    out = rt.run_phase46_long_horizon_timeline(
        output_dir=str(base), archive_count=3)
    _check("I::status_ok",
           out.get("status") in
           ("ok", "ok_with_warnings"),
           str(out.get("status")))
    val = rt.validate_phase46_timeline_output(out)
    _check("I::output_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("I::phase21_blocked",
           out.get("phase21_status") == "BLOCKED")
    t = out.get("timeline") or {}
    _check("I::archive_count_3",
           t.get("archive_count") == 3)
    ts_res = out.get("tamper_suite_result") or {}
    _check("I::all_12_detected",
           ts_res.get("detected_count") == 12)
    # Artifacts written
    for sub, fname in (
        ("timeline_contracts",
         "timeline_contract.json"),
        ("timelines", "timeline.json"),
        ("timeline_manifests",
         "timeline_manifest.json"),
        ("verification_outputs",
         "verification_result.json"),
        ("tamper_tests", "tamper_suite.json"),
        ("operator_packets", "operator_packet.json"),
        ("dashboards", "OPERATOR_PACKET.md"),
        ("dashboards", "STATUS_DASHBOARD.json"),
        ("dashboards", "STATUS_DASHBOARD.md"),
    ):
        p = base / sub / fname
        _check(f"I::written::{sub}/{fname}",
               p.exists())
    summary = rt.summarize_phase46_timeline_output(out)
    _check("I::summary_ok",
           summary.get("ok") is True)


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
                  / "voice_adapter_phase46")
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("J::no_audio_in_voice_adapter_phase46",
           not audio, ",".join(audio))
    files = [f"{m}.py" for m in _PHASE46_MODULES]
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
        "timeline_contracts", "captured_archives",
        "timelines", "timeline_manifests",
        "verification_outputs", "tamper_tests",
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
        "bilingual_voice_phase45_runtime",
        "bilingual_voice_phase45_archive_builder",
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
    for m in _PHASE46_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    try:
        import bilingual_voice_phase46_runtime as rt
        out = rt.run_phase46_long_horizon_timeline(
            archive_count=3)
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
        ("B", suite_b_timeline_contract),
        ("C", suite_c_archive_collector),
        ("D", suite_d_timeline_builder),
        ("E", suite_e_timeline_manifest),
        ("F", suite_f_long_horizon_verifier),
        ("G", suite_g_tamper_suite),
        ("H", suite_h_operator_packet_and_dashboard),
        ("I", suite_i_phase46_runtime),
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
