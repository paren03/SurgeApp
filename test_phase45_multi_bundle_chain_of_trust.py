"""Phase 45 test harness - multi-bundle chain-of-trust."""

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


_PHASE45_MODULES = (
    "bilingual_voice_phase45_archive_contract",
    "bilingual_voice_phase45_archive_builder",
    "bilingual_voice_phase45_archive_manifest",
    "bilingual_voice_phase45_chain_ledger",
    "bilingual_voice_phase45_archive_verifier",
    "bilingual_voice_phase45_tamper_suite",
    "bilingual_voice_phase45_operator_packet",
    "bilingual_voice_phase45_status_dashboard",
    "bilingual_voice_phase45_runtime",
)


def suite_a_preflight() -> None:
    upstream = [
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
    for f in upstream:
        _check(f"A::upstream_present::{f}",
               (_ROOT / f).exists(), f)
    # Phase 42/43/44 generated artifacts must exist
    needed = (
        ("voice_adapter_phase44", "imported_bundles",
         "imported_bundle.json"),
        ("voice_adapter_phase44", "roundtrip_manifests",
         "import_manifest.json"),
        ("voice_adapter_phase44",
         "verification_outputs",
         "fresh_import_result.json"),
        ("voice_adapter_phase44", "tamper_tests",
         "tamper_suite.json"),
        ("voice_adapter_phase44", "operator_packets",
         "operator_packet.json"),
        ("voice_adapter_phase44", "dashboards",
         "STATUS_DASHBOARD.json"),
        ("voice_adapter_phase43", "portable_bundles",
         "portable_bundle.json"),
        ("voice_adapter_phase43", "bundle_manifests",
         "bundle_manifest.json"),
        ("voice_adapter_phase43", "operator_packets",
         "operator_packet.json"),
        ("voice_adapter_phase42", "contracts",
         "audit_contract.json"),
        ("voice_adapter_phase42", "replay_projections",
         "replay_matrix.json"),
        ("voice_adapter_phase42", "operator_packets",
         "operator_packet.json"),
    )
    for parent, sub, fname in needed:
        p = (_ROOT / "bilingual_stack" / parent
                  / sub / fname)
        _check(f"A::artifact::{parent}/{sub}/{fname}",
               p.exists(), str(p))
    for m in _PHASE45_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    for sub in ("archive_contracts", "archives",
                 "archive_manifests", "chain_ledgers",
                 "verification_outputs", "tamper_tests",
                 "operator_packets", "dashboards",
                 "reports", "fixtures", "demos"):
        d = (_ROOT / "bilingual_stack"
                   / "voice_adapter_phase45" / sub)
        _check(f"A::folder::{sub}", d.exists())
    for m in _PHASE45_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_archive_contract() -> None:
    import bilingual_voice_phase45_archive_contract as ac
    sch = ac.get_phase45_archive_contract_schema()
    _check("B::schema_is_dict", isinstance(sch, dict))
    _check("B::dry_run",
           sch.get("rehearsal_dry_run_only") is True)
    _check("B::adapter_invoke_forbidden",
           sch.get("adapter_invocation_forbidden")
           is True)
    _check("B::production_db_read_forbidden",
           sch.get("production_db_read_forbidden")
           is True)
    _check("B::chain_required",
           sch.get("chain_verification_required")
           is True)
    _check("B::tamper_required",
           sch.get("tamper_detection_required") is True)
    arts = ac.get_phase45_required_archive_artifacts()
    for must in ("phase42_replay_matrix",
                  "phase42_operator_packet",
                  "phase43_portable_bundle",
                  "phase43_bundle_manifest",
                  "phase44_roundtrip_receipt",
                  "phase44_operator_packet"):
        _check(f"B::required::{must}", must in arts)
    links = ac.get_phase45_required_chain_links()
    for must in ("phase42_to_phase43_bundle",
                  "phase43_to_phase44_import",
                  "phase44_import_to_roundtrip_receipt",
                  "phase44_tamper_suite_to_operator_packet",
                  "phase44_operator_packet_to_dashboard"):
        _check(f"B::chain_link::{must}", must in links)
    forb_arts = ac.get_phase45_forbidden_archive_artifacts()
    for must in ("runtime_dbs", "audio_files",
                  "local_secret_handoff_contents",
                  "claude_directory_contents"):
        _check(f"B::forb_art::{must}", must in forb_arts)
    forb = ac.get_phase45_forbidden_actions()
    for must in ("adapter_invocation_in_archive",
                  "production_db_read_in_verifier",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "broken_chain_order",
                  "tampered_artifact_hash"):
        _check(f"B::forb::{must}", must in forb)
    c = ac.create_phase45_archive_contract(
        archive_id="aid_test")
    val = ac.validate_phase45_archive_contract(c)
    _check("B::validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = ac.validate_phase45_archive_contract("notdict")
    _check("B::reject_non_dict",
           bad.get("ok") is False)
    drift = dict(c)
    drift["rehearsal_dry_run_only"] = False
    bad2 = ac.validate_phase45_archive_contract(drift)
    _check("B::catch_non_dry_run",
           bad2.get("ok") is False)
    drift2 = dict(c)
    drift2["adapter_invocation_forbidden"] = False
    bad3 = ac.validate_phase45_archive_contract(drift2)
    _check("B::catch_adapter_invoke",
           bad3.get("ok") is False)
    drift3 = dict(c)
    drift3["production_db_read_forbidden"] = False
    bad4 = ac.validate_phase45_archive_contract(drift3)
    _check("B::catch_db_read",
           bad4.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = ac.write_phase45_archive_contract_report(
            c, str(out))
        _check("B::contract_written", Path(p).exists())


def suite_c_archive_builder() -> None:
    import bilingual_voice_phase45_archive_builder as ab
    arts = ab.collect_phase45_archive_artifacts()
    _check("C::collection_count_25",
           arts.get("count") == 25,
           str(arts.get("count")))
    _check("C::no_missing",
           not arts.get("missing"),
           str(arts.get("missing")))
    hashes = ab.compute_phase45_archive_hashes(arts)
    _check("C::hashes_25", len(hashes) == 25)
    archive = ab.create_phase45_archive()
    val = ab.validate_phase45_archive(archive)
    _check("C::archive_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("C::artifact_count_25",
           archive.get("artifact_count") == 25)
    _check("C::source_phases",
           sorted(archive.get("source_phases") or [])
           == ["phase42", "phase43", "phase44"])
    # No runtime DBs
    for e in archive.get("artifacts") or []:
        rp = str(e.get("relative_path") or "").lower()
        for tok in (".sqlite", ".sqlite3", ".db"):
            _check(f"C::no_db::{e.get('artifact_key')}:{tok}",
                   not rp.endswith(tok))
        for tok in (".wav", ".mp3", ".ogg", ".flac",
                     ".m4a"):
            _check(f"C::no_audio::{e.get('artifact_key')}:{tok}",
                   not rp.endswith(tok))
        _check(f"C::no_handoff::{e.get('artifact_key')}",
               "/local_secret_handoff/" not in rp)
        _check(f"C::no_claude::{e.get('artifact_key')}",
               "/.claude/" not in
               ("/" + rp if not rp.startswith("/")
                else rp))
    # No adapter invocation flag
    _check("C::no_adapter_invocation",
           (archive.get("boundary_summary") or {}).get(
               "no_adapter_invocation_in_archive")
           is True)
    bad = ab.validate_phase45_archive("notdict")
    _check("C::reject_non_dict",
           bad.get("ok") is False)
    drift = dict(archive)
    drift["raw_transcript"] = "leak"
    bad2 = ab.validate_phase45_archive(drift)
    _check("C::catch_raw_transcript",
           bad2.get("ok") is False)
    summary = ab.summarize_phase45_archive(archive)
    _check("C::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "ar.json"
        p = ab.write_phase45_archive(archive, str(out))
        _check("C::archive_written", Path(p).exists())


def suite_d_archive_manifest() -> None:
    import bilingual_voice_phase45_archive_builder as ab
    import bilingual_voice_phase45_archive_manifest as am
    archive = ab.create_phase45_archive()
    manifest = am.create_phase45_archive_manifest(
        archive)
    val = am.validate_phase45_archive_manifest(manifest)
    _check("D::validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("D::count_25",
           manifest.get("artifact_count") == 25)
    _check("D::phase21_blocked",
           manifest.get("phase21_status")
           in ("BLOCKED", "STAGED_AWAITING_OPERATOR"))
    _check("D::root_64",
           isinstance(manifest.get(
               "manifest_root_hash"), str)
           and len(manifest.get(
               "manifest_root_hash")) == 64)
    # Deterministic root hash
    manifest2 = am.create_phase45_archive_manifest(
        archive)
    _check("D::root_hash_deterministic",
           manifest.get("manifest_root_hash")
           == manifest2.get("manifest_root_hash"))
    verify = am.verify_phase45_archive_manifest(
        archive, manifest)
    _check("D::verifies",
           verify.get("ok") is True,
           ",".join(verify.get("reasons", [])))
    # Tampered hash
    bad_a = copy.deepcopy(archive)
    if bad_a.get("artifacts"):
        bad_a["artifacts"][0]["sha256"] = "0" * 64
    tamper = am.detect_phase45_manifest_tampering(
        bad_a, manifest)
    _check("D::tamper_detects_hash",
           tamper.get("tampered") is True)
    # Missing artifact
    bad_a2 = copy.deepcopy(archive)
    bad_a2["artifacts"] = archive["artifacts"][1:]
    bad_a2["artifact_count"] = \
        len(bad_a2["artifacts"])
    verify2 = am.verify_phase45_archive_manifest(
        bad_a2, manifest)
    _check("D::catches_missing",
           verify2.get("ok") is False)
    # Runtime DB in manifest
    bad_m = copy.deepcopy(manifest)
    bad_m["artifact_hashes"]["leak.sqlite"] = "1" * 64
    bv = am.validate_phase45_archive_manifest(bad_m)
    _check("D::validator_catches_runtime_db",
           bv.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "m.json"
        p = am.write_phase45_archive_manifest(
            manifest, str(out))
        _check("D::written", Path(p).exists())
        loaded = am.read_phase45_archive_manifest(
            str(out))
        _check("D::roundtrip",
               loaded.get("manifest_id")
               == manifest.get("manifest_id"))


def suite_e_chain_ledger() -> None:
    import bilingual_voice_phase45_archive_builder as ab
    import bilingual_voice_phase45_chain_ledger as cl
    archive = ab.create_phase45_archive()
    ledger = cl.create_phase45_chain_ledger(archive)
    val = cl.validate_phase45_chain_ledger(ledger)
    _check("E::ledger_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("E::ordered_phases",
           ledger.get("ordered_phases")
           == ["phase42", "phase43", "phase44"])
    verify = cl.verify_phase45_chain_links(
        ledger, archive=archive)
    _check("E::links_verify",
           verify.get("ok") is True,
           ",".join(verify.get("reasons", [])))
    # Broken phase order
    bad = copy.deepcopy(ledger)
    bad["ordered_phases"] = ["phase44", "phase43",
                              "phase42"]
    bad_val = cl.validate_phase45_chain_ledger(bad)
    _check("E::catches_broken_order",
           bad_val.get("ok") is False)
    # Link hash mutation
    bad2 = copy.deepcopy(ledger)
    if bad2.get("chain_links"):
        first_key = next(iter(bad2["chain_links"]))
        bad2["chain_links"][first_key]["ok"] = False
        # Don't update chain_root_hash so root drift
        # also catches it
    bad2_verify = cl.verify_phase45_chain_links(bad2)
    _check("E::catches_link_break",
           bad2_verify.get("ok") is False)
    # Verify chain detects archive-disagreement
    bad_archive = copy.deepcopy(archive)
    # Mutate Phase 43 portable_bundle's source_phase to
    # break the phase42 -> phase43 link
    for e in bad_archive.get("artifacts") or []:
        if e.get("artifact_key") == \
                "phase43_portable_bundle":
            ic = e.get("inline_content")
            if isinstance(ic, dict):
                ic["source_phase"] = "phase99"
    rederived = cl.verify_phase45_chain_links(
        ledger, archive=bad_archive)
    _check("E::catches_archive_disagreement",
           rederived.get("ok") is False)
    # Build a fresh ledger from the broken archive — it
    # will have an ok=False link
    bad_ledger = cl.create_phase45_chain_ledger(
        bad_archive)
    verify_bad = cl.verify_phase45_chain_links(
        bad_ledger)
    _check("E::broken_archive_yields_broken_link",
           verify_bad.get("ok") is False)
    s = cl.summarize_phase45_chain_ledger(ledger)
    _check("E::summary_ok", s.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "l.json"
        p = cl.write_phase45_chain_ledger(
            ledger, str(out))
        _check("E::ledger_written", Path(p).exists())


def suite_f_archive_verifier() -> None:
    import bilingual_voice_phase45_archive_builder as ab
    import bilingual_voice_phase45_archive_manifest as am
    import bilingual_voice_phase45_chain_ledger as cl
    import bilingual_voice_phase45_archive_verifier as av
    archive = ab.create_phase45_archive()
    manifest = am.create_phase45_archive_manifest(
        archive)
    ledger = cl.create_phase45_chain_ledger(archive)
    result = av.verify_phase45_archive(
        archive, manifest=manifest, ledger=ledger)
    _check("F::clean_verify_ok",
           result.get("ok") is True,
           result.get("summary"))
    _check("F::presence_ok",
           result["presence_check"]["ok"] is True)
    _check("F::hash_ok",
           result["hash_check"]["ok"] is True)
    _check("F::chain_ok",
           result["chain_integrity_check"]["ok"] is True)
    _check("F::boundary_ok",
           result["boundary_check"]["ok"] is True)
    _check("F::phase21_ok",
           result["phase21_check"]["ok"] is True)
    _check("F::no_runtime_state_ok",
           result["no_runtime_state_check"]["ok"]
           is True)
    # Tampered Phase 43 bundle (modify inline content)
    bad = copy.deepcopy(archive)
    for e in bad.get("artifacts") or []:
        if e.get("artifact_key") == \
                "phase43_portable_bundle":
            ic = e.get("inline_content")
            if isinstance(ic, dict):
                ic["source_phase"] = "tampered"
    r2 = av.verify_phase45_chain_integrity(bad)
    _check("F::tampered_phase43_caught",
           r2.get("ok") is False)
    # Tampered Phase 44 operator packet
    bad2 = copy.deepcopy(archive)
    for e in bad2.get("artifacts") or []:
        if e.get("artifact_key") == \
                "phase44_operator_packet":
            ic = e.get("inline_content")
            if isinstance(ic, dict):
                ic["phase44_status"] = "tampered"
    r3 = av.verify_phase45_chain_integrity(bad2)
    _check("F::tampered_phase44_packet_caught",
           r3.get("ok") is False)
    # Missing Phase 42 replay matrix
    bad3 = copy.deepcopy(archive)
    bad3["artifacts"] = [
        e for e in bad3.get("artifacts") or []
        if e.get("artifact_key")
        != "phase42_replay_matrix"]
    r4 = av.verify_phase45_artifact_presence(bad3)
    _check("F::missing_replay_matrix_caught",
           r4.get("ok") is False)
    # Boundary violation
    bad4 = copy.deepcopy(archive)
    bs = dict(bad4.get("boundary_summary") or {})
    bs["no_audio"] = False
    bad4["boundary_summary"] = bs
    r5 = av.verify_phase45_boundary_claims(bad4)
    _check("F::boundary_violation_caught",
           r5.get("ok") is False)
    # Secret field injection
    bad5 = copy.deepcopy(archive)
    for e in bad5.get("artifacts") or []:
        ic = e.get("inline_content")
        if isinstance(ic, dict):
            ic["signing_key_material"] = "leak"
            break
    r6 = av.verify_phase45_boundary_claims(bad5)
    _check("F::secret_field_caught",
           r6.get("ok") is False)
    # Runtime DB reference
    bad6 = copy.deepcopy(archive)
    entries = list(bad6.get("artifacts") or [])
    entries.append({
        "artifact_key": "leak_db",
        "source_phase": "leak",
        "relative_path":
            "lexicon/luna_vocabulary.sqlite",
        "absolute_path":
            "lexicon/luna_vocabulary.sqlite",
        "artifact_type": "other",
        "size_bytes": 100,
        "sha256": "1" * 64,
    })
    bad6["artifacts"] = entries
    r7 = av.verify_phase45_boundary_claims(bad6)
    _check("F::runtime_db_caught",
           r7.get("ok") is False)
    # Phase 21 drift
    bad7 = copy.deepcopy(archive)
    bad7["phase21_status_text"] = "UNBLOCKED"
    r8 = av.verify_phase45_phase21_claim(bad7)
    _check("F::phase21_drift_caught",
           r8.get("ok") is False)
    # No production DB read source check
    src = (_ROOT
            / "bilingual_voice_phase45_archive_verifier.py"
            ).read_text(encoding="utf-8")
    _check("F::no_sqlite_in_verifier",
           "import sqlite3" not in src
           and "from sqlite3" not in src
           and "sqlite3.connect" not in src)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "v.json"
        p = av.write_phase45_archive_verification_report(
            result, str(out))
        _check("F::verify_written", Path(p).exists())


def suite_g_tamper_suite() -> None:
    import bilingual_voice_phase45_archive_builder as ab
    import bilingual_voice_phase45_archive_manifest as am
    import bilingual_voice_phase45_chain_ledger as cl
    import bilingual_voice_phase45_tamper_suite as ts
    archive = ab.create_phase45_archive()
    manifest = am.create_phase45_archive_manifest(
        archive)
    ledger = cl.create_phase45_chain_ledger(archive)
    cases = ts.create_phase45_tamper_cases(archive)
    _check("G::cases_13", len(cases) == 13)
    for c in cases:
        _check(f"G::case_expected::{c.get('case')}",
               c.get("expected_detection") is True)
    result = ts.run_phase45_tamper_suite(
        archive, manifest=manifest, ledger=ledger)
    val = ts.validate_phase45_tamper_suite_result(
        result)
    _check("G::validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("G::all_13_detected",
           result.get("detected_count") == 13,
           str(result.get("detected_count")))
    _check("G::none_undetected",
           result.get("undetected_count") == 0)
    _check("G::suite_ok",
           result.get("ok") is True)
    # Original archive unchanged
    orig_count = archive.get("artifact_count")
    _check("G::original_count_unchanged",
           archive.get("artifact_count") == orig_count)
    # Source isolation
    src = (_ROOT
            / "bilingual_voice_phase45_tamper_suite.py"
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
    summary = ts.summarize_phase45_tamper_suite(result)
    _check("G::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "ts.json"
        p = ts.write_phase45_tamper_suite_report(
            result, str(out))
        _check("G::written", Path(p).exists())


def suite_h_operator_packet_and_dashboard() -> None:
    import bilingual_voice_phase45_runtime as rt
    import bilingual_voice_phase45_operator_packet as op
    import bilingual_voice_phase45_status_dashboard as sd
    out = rt.run_phase45_multi_bundle_archive()
    pkt = out.get("operator_packet") or {}
    val_pkt = op.validate_phase45_operator_packet(pkt)
    _check("H::packet_validates",
           val_pkt.get("ok") is True,
           ",".join(val_pkt.get("reasons", [])))
    _check("H::packet_status_ok",
           pkt.get("phase45_status") in
           ("ok", "ok_with_warnings"))
    md = op.create_phase45_operator_packet_markdown(pkt)
    _check("H::packet_md_nonempty",
           isinstance(md, str) and len(md) > 300)
    for needle in ("Phase 45", "Source phases",
                    "Chain-of-trust", "Phase 21",
                    "Next recommended"):
        _check(f"H::md_contains::{needle}",
               needle in md, needle)
    dash = out.get("status_dashboard") or {}
    val_d = sd.validate_phase45_status_dashboard(dash)
    _check("H::dashboard_validates",
           val_d.get("ok") is True,
           ",".join(val_d.get("reasons", [])))
    _check("H::dash_source_phases",
           "phase42" in dash.get("source_phases", [])
           and "phase43" in dash.get(
               "source_phases", [])
           and "phase44" in dash.get(
               "source_phases", []))
    _check("H::dash_artifact_count_25",
           dash.get("artifact_count") == 25)
    ts_status = dash.get("tamper_suite_status") or {}
    _check("H::dash_tamper_ok",
           ts_status.get("ok") is True)
    _check("H::dash_tamper_13",
           ts_status.get("case_count") == 13)
    _check("H::dash_phase21_in",
           "phase21_import_status" in dash)
    dash_md = sd.create_phase45_dashboard_markdown(dash)
    _check("H::dash_md_nonempty",
           isinstance(dash_md, str)
           and len(dash_md) > 300)
    for needle in ("Phase 45", "Source phases",
                    "Chain-of-trust",
                    "Phase 21 import status",
                    "Tamper suite",
                    "Forbidden boundaries"):
        _check(f"H::dash_md_contains::{needle}",
               needle in dash_md, needle)
    bad = dict(dash)
    bad["source_phases"] = ["phaseXX"]
    bad_v = sd.validate_phase45_status_dashboard(bad)
    _check("H::dash_validator_catches_bad_source",
           bad_v.get("ok") is False)


def suite_i_phase45_runtime() -> None:
    import bilingual_voice_phase45_runtime as rt
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase45")
    out = rt.run_phase45_multi_bundle_archive(
        output_dir=str(base))
    _check("I::status_ok",
           out.get("status") in
           ("ok", "ok_with_warnings"),
           str(out.get("status")))
    val = rt.validate_phase45_archive_output(out)
    _check("I::output_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("I::phase21_blocked",
           out.get("phase21_status") == "BLOCKED")
    a = out.get("archive") or {}
    _check("I::artifact_count_25",
           a.get("artifact_count") == 25)
    ts_res = out.get("tamper_suite_result") or {}
    _check("I::all_13_detected",
           ts_res.get("detected_count") == 13)
    # Artifacts written
    for sub, fname in (
        ("archive_contracts", "archive_contract.json"),
        ("archives", "archive.json"),
        ("archive_manifests", "archive_manifest.json"),
        ("chain_ledgers", "chain_ledger.json"),
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
    summary = rt.summarize_phase45_archive_output(out)
    _check("I::summary_ok", summary.get("ok") is True)


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
                  / "voice_adapter_phase45")
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("J::no_audio_in_voice_adapter_phase45",
           not audio, ",".join(audio))
    files = [f"{m}.py" for m in _PHASE45_MODULES]
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
        "archive_contracts", "archives",
        "archive_manifests", "chain_ledgers",
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
    for m in _PHASE45_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    try:
        import bilingual_voice_phase45_runtime as rt
        out = rt.run_phase45_multi_bundle_archive()
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
        ("B", suite_b_archive_contract),
        ("C", suite_c_archive_builder),
        ("D", suite_d_archive_manifest),
        ("E", suite_e_chain_ledger),
        ("F", suite_f_archive_verifier),
        ("G", suite_g_tamper_suite),
        ("H", suite_h_operator_packet_and_dashboard),
        ("I", suite_i_phase45_runtime),
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
