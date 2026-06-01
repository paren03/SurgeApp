"""Phase 44 test harness - cross-machine import simulation."""

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


_PHASE44_MODULES = (
    "bilingual_voice_phase44_import_contract",
    "bilingual_voice_phase44_bundle_importer",
    "bilingual_voice_phase44_import_manifest",
    "bilingual_voice_phase44_fresh_import_verifier",
    "bilingual_voice_phase44_tamper_suite",
    "bilingual_voice_phase44_roundtrip_receipt",
    "bilingual_voice_phase44_operator_packet",
    "bilingual_voice_phase44_status_dashboard",
    "bilingual_voice_phase44_runtime",
)


def suite_a_preflight() -> None:
    upstream = [
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
    # Phase 43 portable bundle artifacts must exist
    p43_base = (_ROOT / "bilingual_stack"
                      / "voice_adapter_phase43")
    for sub, fname in (
        ("portable_bundles", "portable_bundle.json"),
        ("portable_bundles", "portability_contract.json"),
        ("bundle_manifests", "bundle_manifest.json"),
        ("fresh_checkout_outputs",
         "fresh_checkout_result.json"),
        ("portability_audits", "portability_audit.json"),
        ("operator_packets", "operator_packet.json"),
        ("dashboards", "STATUS_DASHBOARD.json"),
        ("dashboards", "STATUS_DASHBOARD.md"),
    ):
        _check(f"A::phase43_artifact::{sub}/{fname}",
               (p43_base / sub / fname).exists())
    for m in _PHASE44_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    for sub in ("import_contracts",
                 "fresh_checkout_simulation",
                 "imported_bundles",
                 "roundtrip_manifests",
                 "verification_outputs",
                 "tamper_tests", "operator_packets",
                 "dashboards", "reports", "fixtures",
                 "demos"):
        d = (_ROOT / "bilingual_stack"
                   / "voice_adapter_phase44" / sub)
        _check(f"A::folder::{sub}", d.exists())
    for m in _PHASE44_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_import_contract() -> None:
    import bilingual_voice_phase44_import_contract as ic
    sch = ic.get_phase44_import_contract_schema()
    _check("B::schema_is_dict", isinstance(sch, dict))
    _check("B::dry_run",
           sch.get("rehearsal_dry_run_only") is True)
    _check("B::adapter_invoke_forbidden",
           sch.get("adapter_invocation_forbidden")
           is True)
    _check("B::production_db_read_forbidden",
           sch.get("production_db_read_forbidden")
           is True)
    arts = ic.get_phase44_required_import_artifacts()
    for must in ("portable_bundle", "bundle_manifest",
                  "source_operator_packet",
                  "source_status_dashboard",
                  "source_phase43_report",
                  "import_manifest",
                  "fresh_checkout_verification_result",
                  "roundtrip_receipt",
                  "operator_packet"):
        _check(f"B::required_artifact::{must}",
               must in arts)
    forb_arts = ic.get_phase44_forbidden_import_artifacts()
    for must in ("runtime_dbs", "audio_files",
                  "local_secret_handoff_contents",
                  "corpus_incoming_files",
                  "claude_directory_contents"):
        _check(f"B::forbidden_artifact::{must}",
               must in forb_arts)
    forb = ic.get_phase44_forbidden_actions()
    for must in ("adapter_invocation_on_import",
                  "production_db_read_in_fresh_checkout",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "path_traversal", "url_scheme_path",
                  "shell_metacharacter_path"):
        _check(f"B::forb::{must}", must in forb)
    c = ic.create_phase44_import_contract(
        import_id="iid_test")
    val = ic.validate_phase44_import_contract(c)
    _check("B::contract_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = ic.validate_phase44_import_contract("notdict")
    _check("B::validator_rejects_non_dict",
           bad.get("ok") is False)
    drift = dict(c)
    drift["rehearsal_dry_run_only"] = False
    bad2 = ic.validate_phase44_import_contract(drift)
    _check("B::validator_catches_non_dry_run",
           bad2.get("ok") is False)
    drift2 = dict(c)
    drift2["adapter_invocation_forbidden"] = False
    bad3 = ic.validate_phase44_import_contract(drift2)
    _check("B::validator_catches_adapter_invoke",
           bad3.get("ok") is False)
    drift3 = dict(c)
    drift3["production_db_read_forbidden"] = False
    bad4 = ic.validate_phase44_import_contract(drift3)
    _check("B::validator_catches_db_read",
           bad4.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = ic.write_phase44_import_contract_report(
            c, str(out))
        _check("B::contract_written", Path(p).exists())


def suite_c_bundle_importer() -> None:
    import bilingual_voice_phase44_bundle_importer as bi
    ws = bi.create_phase44_import_workspace()
    _check("C::workspace_ok", ws.get("ok") is True)
    _check("C::workspace_artifacts_dir_exists",
           Path(ws.get("artifacts_dir") or "").exists())
    imported = bi.import_phase43_bundle_to_workspace(
        workspace_dir=ws.get("workspace_path"))
    _check("C::import_ok", imported.get("ok") is True,
           str(imported.get("rejected")))
    val = bi.validate_phase44_imported_bundle(imported)
    _check("C::import_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("C::imported_count_5",
           imported.get("imported_count") == 5,
           str(imported.get("imported_count")))
    # Path traversal rejected
    bad = bi.import_phase43_bundle_to_workspace(
        source_bundle_path="../etc/passwd",
        workspace_dir=ws.get("workspace_path"))
    rej_keys = {r.get("key") for r in
                 bad.get("rejected") or []}
    _check("C::path_traversal_rejected",
           "portable_bundle" in rej_keys)
    # URL path rejected
    bad2 = bi.import_phase43_bundle_to_workspace(
        source_bundle_path="https://evil.example/x.json",
        workspace_dir=ws.get("workspace_path"))
    rej_keys2 = {r.get("key") for r in
                  bad2.get("rejected") or []}
    _check("C::url_path_rejected",
           "portable_bundle" in rej_keys2)
    # Shell metacharacter rejected
    bad3 = bi.import_phase43_bundle_to_workspace(
        source_bundle_path="x | rm -rf /",
        workspace_dir=ws.get("workspace_path"))
    rej_keys3 = {r.get("key") for r in
                  bad3.get("rejected") or []}
    _check("C::shell_metachar_rejected",
           "portable_bundle" in rej_keys3)
    # Runtime DB extension rejected
    bad4 = bi.import_phase43_bundle_to_workspace(
        source_bundle_path="leak.sqlite",
        workspace_dir=ws.get("workspace_path"))
    rej_keys4 = {r.get("key") for r in
                  bad4.get("rejected") or []}
    _check("C::runtime_db_path_rejected",
           "portable_bundle" in rej_keys4)
    # Audio extension rejected
    bad5 = bi.import_phase43_bundle_to_workspace(
        source_bundle_path="leak.wav",
        workspace_dir=ws.get("workspace_path"))
    rej_keys5 = {r.get("key") for r in
                  bad5.get("rejected") or []}
    _check("C::audio_path_rejected",
           "portable_bundle" in rej_keys5)
    # Hashes preserved on legitimate import
    for e in imported.get("entries") or []:
        _check(f"C::sha_match::{e.get('artifact_key')}",
               e.get("sha_matches") is True)
        _check(f"C::sha_64::{e.get('artifact_key')}",
               isinstance(e.get("imported_sha256"), str)
               and len(e.get("imported_sha256")) == 64)
    # No banned fields
    bad_b = dict(imported)
    bad_b["raw_transcript"] = "leak"
    v_bad = bi.validate_phase44_imported_bundle(bad_b)
    _check("C::validator_catches_raw_transcript",
           v_bad.get("ok") is False)
    summary = bi.summarize_phase44_imported_bundle(
        imported)
    _check("C::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "ib.json"
        p = bi.write_phase44_imported_bundle(
            imported, str(out))
        _check("C::imported_written", Path(p).exists())


def suite_d_import_manifest() -> None:
    import bilingual_voice_phase44_bundle_importer as bi
    import bilingual_voice_phase44_import_manifest as im
    ws = bi.create_phase44_import_workspace()
    imported = bi.import_phase43_bundle_to_workspace(
        workspace_dir=ws.get("workspace_path"))
    manifest = im.create_phase44_import_manifest(imported)
    val = im.validate_phase44_import_manifest(manifest)
    _check("D::manifest_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("D::manifest_count_5",
           manifest.get("imported_artifact_count") == 5)
    _check("D::manifest_phase21_blocked",
           manifest.get("phase21_status")
           in ("BLOCKED", "STAGED_AWAITING_OPERATOR"))
    _check("D::manifest_source_root_64",
           isinstance(manifest.get(
               "source_manifest_root_hash"), str)
           and len(manifest.get(
               "source_manifest_root_hash")) == 64)
    verify = im.verify_phase44_import_manifest(
        imported, manifest)
    _check("D::manifest_verifies",
           verify.get("ok") is True,
           ",".join(verify.get("reasons", [])))
    # Tampered hash
    bad_imp = copy.deepcopy(imported)
    if bad_imp.get("entries"):
        bad_imp["entries"][0]["imported_sha256"] = \
            "0" * 64
    tamper = im.detect_phase44_import_manifest_tampering(
        bad_imp, manifest)
    _check("D::tamper_detects_hash",
           tamper.get("tampered") is True)
    # Missing artifact
    bad_imp2 = copy.deepcopy(imported)
    bad_imp2["entries"] = imported["entries"][1:]
    bad_imp2["imported_count"] = \
        len(bad_imp2["entries"])
    verify2 = im.verify_phase44_import_manifest(
        bad_imp2, manifest)
    _check("D::manifest_catches_missing",
           verify2.get("ok") is False)
    # Runtime DB reference in manifest
    bad_man = copy.deepcopy(manifest)
    bad_man["imported_artifact_hashes"]["leak.sqlite"] = \
        "1" * 64
    bv = im.validate_phase44_import_manifest(bad_man)
    _check("D::manifest_validator_catches_runtime_db",
           bv.get("ok") is False)
    # Round-trip
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "im.json"
        p = im.write_phase44_import_manifest(
            manifest, str(out))
        _check("D::manifest_written", Path(p).exists())
        loaded = im.read_phase44_import_manifest(str(out))
        _check("D::manifest_roundtrip",
               loaded.get("import_manifest_id")
               == manifest.get("import_manifest_id"))


def suite_e_fresh_import_verifier() -> None:
    import bilingual_voice_phase44_bundle_importer as bi
    import bilingual_voice_phase44_import_manifest as im
    import bilingual_voice_phase44_fresh_import_verifier \
        as fiv
    ws = bi.create_phase44_import_workspace()
    imported = bi.import_phase43_bundle_to_workspace(
        workspace_dir=ws.get("workspace_path"))
    manifest = im.create_phase44_import_manifest(imported)
    result = fiv.verify_phase44_imported_bundle_fresh(
        imported, import_manifest=manifest)
    _check("E::result_ok",
           result.get("ok") is True,
           result.get("summary"))
    _check("E::presence_ok",
           result["presence_check"]["ok"] is True)
    _check("E::hash_ok",
           result["hash_check"]["ok"] is True)
    _check("E::boundary_ok",
           result["boundary_check"]["ok"] is True)
    _check("E::phase21_ok",
           result["phase21_check"]["ok"] is True)
    _check("E::no_runtime_state_ok",
           result["no_runtime_state_check"]["ok"]
           is True)
    # Boundary violation
    bad = copy.deepcopy(imported)
    bs = dict(bad.get("boundary_summary") or {})
    bs["no_audio"] = False
    bad["boundary_summary"] = bs
    r2 = fiv.verify_phase44_import_boundary_claims(bad)
    _check("E::boundary_violation_fails",
           r2.get("ok") is False)
    # Phase 21 drift
    bad2 = copy.deepcopy(imported)
    bad2["phase21_status_text"] = "UNBLOCKED"
    r3 = fiv.verify_phase44_import_phase21_status(bad2)
    _check("E::phase21_drift_fails",
           r3.get("ok") is False)
    # No runtime state: violate by setting flag false
    bad3 = copy.deepcopy(imported)
    bs2 = dict(bad3.get("boundary_summary") or {})
    bs2["no_production_db_read_on_import"] = False
    bad3["boundary_summary"] = bs2
    r4 = fiv.verify_phase44_no_runtime_state_dependency(
        bad3)
    _check("E::no_runtime_state_violation_fails",
           r4.get("ok") is False)
    # Hash drift
    bad4 = copy.deepcopy(imported)
    if bad4.get("entries"):
        bad4["entries"][0]["imported_sha256"] = "0" * 64
    r5 = fiv.verify_phase44_import_hashes(bad4)
    _check("E::hash_drift_fails",
           r5.get("ok") is False)
    # Missing artifact
    bad5 = copy.deepcopy(imported)
    bad5["entries"] = imported["entries"][1:]
    r6 = fiv.verify_phase44_import_artifact_presence(
        bad5)
    _check("E::missing_artifact_fails",
           r6.get("ok") is False)
    # No production DB read: confirm verifier source has
    # no sqlite3 import / connect
    src = (_ROOT
            / "bilingual_voice_phase44_fresh_import_verifier.py"
            ).read_text(encoding="utf-8")
    _check("E::no_sqlite3_import",
           "import sqlite3" not in src
           and "from sqlite3" not in src
           and "sqlite3.connect" not in src)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "fr.json"
        p = fiv.write_phase44_fresh_import_report(
            result, str(out))
        _check("E::fresh_written", Path(p).exists())


def suite_f_tamper_suite() -> None:
    import bilingual_voice_phase44_bundle_importer as bi
    import bilingual_voice_phase44_import_manifest as im
    import bilingual_voice_phase44_tamper_suite as ts
    ws = bi.create_phase44_import_workspace()
    imported = bi.import_phase43_bundle_to_workspace(
        workspace_dir=ws.get("workspace_path"))
    manifest = im.create_phase44_import_manifest(imported)
    cases = ts.create_phase44_tamper_cases(imported)
    _check("F::8_cases", len(cases) == 8)
    for c in cases:
        _check(f"F::case_expected::{c.get('case')}",
               c.get("expected_detection") is True)
    # Run full suite
    result = ts.run_phase44_tamper_suite(
        imported, import_manifest=manifest)
    val = ts.validate_phase44_tamper_suite_result(result)
    _check("F::suite_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("F::all_detected",
           result.get("detected_count") == 8,
           str(result.get("detected_count")))
    _check("F::none_undetected",
           result.get("undetected_count") == 0)
    _check("F::suite_ok",
           result.get("ok") is True)
    # Original imported bundle unchanged
    orig_count = imported.get("imported_count")
    orig_entries_keys = sorted(
        [e.get("artifact_key") for e in
         imported.get("entries") or []])
    # Re-derive verifier presence — confirm original is
    # still clean
    import bilingual_voice_phase44_fresh_import_verifier \
        as fiv
    fr = fiv.verify_phase44_imported_bundle_fresh(
        imported, import_manifest=manifest)
    _check("F::original_still_clean",
           fr.get("ok") is True,
           fr.get("summary"))
    _check("F::original_count_unchanged",
           imported.get("imported_count") == orig_count)
    _check("F::original_keys_unchanged",
           sorted([e.get("artifact_key") for e in
                    imported.get("entries") or []])
           == orig_entries_keys)
    # No subprocess/network/audio in tamper suite source
    src = (_ROOT
            / "bilingual_voice_phase44_tamper_suite.py"
            ).read_text(encoding="utf-8")
    _check("F::no_subprocess_in_tamper_source",
           "subprocess.run" not in src
           and "subprocess.Popen" not in src)
    _check("F::no_network_in_tamper_source",
           "urllib.request" not in src
           and "socket.socket" not in src)
    _check("F::no_audio_in_tamper_source",
           "pyttsx3" not in src and "edge_tts" not in src)
    # No sqlite3 import
    _check("F::no_sqlite3_in_tamper_source",
           "import sqlite3" not in src
           and "sqlite3.connect" not in src)
    summary = ts.summarize_phase44_tamper_suite(result)
    _check("F::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "ts.json"
        p = ts.write_phase44_tamper_suite_report(
            result, str(out))
        _check("F::tamper_written", Path(p).exists())


def suite_g_roundtrip_receipt() -> None:
    import bilingual_voice_phase44_bundle_importer as bi
    import bilingual_voice_phase44_import_manifest as im
    import bilingual_voice_phase44_fresh_import_verifier \
        as fiv
    import bilingual_voice_phase44_tamper_suite as ts
    import bilingual_voice_phase44_roundtrip_receipt as rr
    import bilingual_voice_phase44_import_contract as ic
    ws = bi.create_phase44_import_workspace()
    imported = bi.import_phase43_bundle_to_workspace(
        workspace_dir=ws.get("workspace_path"))
    manifest = im.create_phase44_import_manifest(imported)
    fresh = fiv.verify_phase44_imported_bundle_fresh(
        imported, import_manifest=manifest)
    tamper = ts.run_phase44_tamper_suite(
        imported, import_manifest=manifest)
    contract = ic.create_phase44_import_contract("iid_g")
    receipt = rr.create_phase44_roundtrip_receipt(
        contract, imported, manifest, fresh, tamper)
    val = rr.validate_phase44_roundtrip_receipt(receipt)
    _check("G::receipt_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("G::receipt_status_ok",
           receipt.get("import_status") in
           ("ok", "ok_with_warnings"),
           str(receipt.get("import_status")))
    for k in ("operator_id", "signing_key_material",
              "raw_transcript", "audio_bytes",
              "command"):
        _check(f"G::no_banned::{k}",
               k not in receipt
               or receipt.get(k) in
               (None, "", False, [], {}))
    _check("G::phase21_present",
           receipt.get("phase21_status") in
           ("BLOCKED", "STAGED_AWAITING_OPERATOR"))
    _check("G::next_phase_present",
           bool(receipt.get("next_recommended_phase")))
    _check("G::tamper_status_ok",
           (receipt.get("tamper_suite_status")
            or {}).get("ok") is True)
    drift = dict(receipt)
    drift["operator_id"] = "raw"
    bad = rr.validate_phase44_roundtrip_receipt(drift)
    _check("G::validator_catches_operator_id",
           bad.get("ok") is False)
    summary = rr.summarize_phase44_roundtrip_receipt(
        receipt)
    _check("G::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "r.json"
        p = rr.write_phase44_roundtrip_receipt(
            receipt, str(out))
        _check("G::receipt_written", Path(p).exists())


def suite_h_operator_packet_and_dashboard() -> None:
    import bilingual_voice_phase44_runtime as rt
    import bilingual_voice_phase44_operator_packet as op
    import bilingual_voice_phase44_status_dashboard as sd
    out = rt.run_phase44_cross_machine_import_simulation()
    pkt = out.get("operator_packet") or {}
    val_pkt = op.validate_phase44_operator_packet(pkt)
    _check("H::packet_validates",
           val_pkt.get("ok") is True,
           ",".join(val_pkt.get("reasons", [])))
    _check("H::packet_status_ok",
           pkt.get("phase44_status") in
           ("ok", "ok_with_warnings"),
           str(pkt.get("phase44_status")))
    md = op.create_phase44_operator_packet_markdown(pkt)
    _check("H::packet_md_nonempty",
           isinstance(md, str) and len(md) > 300)
    for needle in ("Phase 44", "Source phase",
                    "Phase 21", "Next recommended"):
        _check(f"H::md_contains::{needle}",
               needle in md, needle)
    dash = out.get("status_dashboard") or {}
    val_d = sd.validate_phase44_status_dashboard(dash)
    _check("H::dashboard_validates",
           val_d.get("ok") is True,
           ",".join(val_d.get("reasons", [])))
    _check("H::dash_source_phase43",
           dash.get("source_phase") == "phase43")
    _check("H::dash_imported_count_5",
           dash.get("imported_artifact_count") == 5)
    ts_status = dash.get("tamper_suite_status") or {}
    _check("H::dash_tamper_ok",
           ts_status.get("ok") is True)
    _check("H::dash_tamper_8",
           ts_status.get("case_count") == 8)
    _check("H::dash_phase21_in",
           "phase21_import_status" in dash)
    dash_md = sd.create_phase44_dashboard_markdown(dash)
    _check("H::dash_md_nonempty",
           isinstance(dash_md, str)
           and len(dash_md) > 300)
    for needle in ("Phase 44", "Source phase",
                    "Phase 21 import status",
                    "Tamper suite",
                    "Forbidden boundaries"):
        _check(f"H::dash_md_contains::{needle}",
               needle in dash_md, needle)
    # Validator rejects wrong source phase
    bad = dict(dash)
    bad["source_phase"] = "phaseXX"
    bad_v = sd.validate_phase44_status_dashboard(bad)
    _check("H::dash_validator_catches_wrong_source",
           bad_v.get("ok") is False)


def suite_i_phase44_runtime() -> None:
    import bilingual_voice_phase44_runtime as rt
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase44")
    out = rt.run_phase44_cross_machine_import_simulation(
        output_dir=str(base))
    _check("I::status_ok",
           out.get("status") in
           ("ok", "ok_with_warnings"),
           str(out.get("status")))
    val = rt.validate_phase44_import_simulation_output(
        out)
    _check("I::output_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("I::phase21_blocked",
           out.get("phase21_status") == "BLOCKED")
    ib = out.get("imported_bundle") or {}
    _check("I::imported_5", ib.get("imported_count") == 5)
    ts_res = out.get("tamper_suite_result") or {}
    _check("I::tamper_all_detected",
           ts_res.get("detected_count") == 8)
    # Artifacts written
    for sub, fname in (
        ("import_contracts", "import_contract.json"),
        ("imported_bundles", "imported_bundle.json"),
        ("roundtrip_manifests", "import_manifest.json"),
        ("verification_outputs",
         "fresh_import_result.json"),
        ("tamper_tests", "tamper_suite.json"),
        ("reports", "roundtrip_receipt.json"),
        ("operator_packets", "operator_packet.json"),
        ("dashboards", "OPERATOR_PACKET.md"),
        ("dashboards", "STATUS_DASHBOARD.json"),
        ("dashboards", "STATUS_DASHBOARD.md"),
    ):
        p = base / sub / fname
        _check(f"I::written::{sub}/{fname}",
               p.exists())
    summary = rt.summarize_phase44_import_simulation_output(
        out)
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
        _check("J::concepts_26", nc >= 26)
        _check("J::links_52", nl >= 52)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"),
        recursive=True) if "backups" not in p]
    _check("J::manifests_90", len(live) == 90,
           str(len(live)))
    audio = []
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase44")
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("J::no_audio_in_voice_adapter_phase44",
           not audio, ",".join(audio))
    files = [f"{m}.py" for m in _PHASE44_MODULES]
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
        "import_contracts", "imported_bundles",
        "roundtrip_manifests", "verification_outputs",
        "tamper_tests", "operator_packets",
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
    for m in _PHASE44_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    try:
        import bilingual_voice_phase44_runtime as rt
        out = rt.run_phase44_cross_machine_import_simulation()
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
        ("B", suite_b_import_contract),
        ("C", suite_c_bundle_importer),
        ("D", suite_d_import_manifest),
        ("E", suite_e_fresh_import_verifier),
        ("F", suite_f_tamper_suite),
        ("G", suite_g_roundtrip_receipt),
        ("H", suite_h_operator_packet_and_dashboard),
        ("I", suite_i_phase44_runtime),
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
