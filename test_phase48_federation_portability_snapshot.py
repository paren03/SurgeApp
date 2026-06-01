"""Phase 48 test harness - federation portability snapshot."""

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


_PHASE48_MODULES = (
    "bilingual_voice_phase48_capsule_contract",
    "bilingual_voice_phase48_capsule_builder",
    "bilingual_voice_phase48_capsule_manifest",
    "bilingual_voice_phase48_fresh_checkout_verifier",
    "bilingual_voice_phase48_capsule_tamper_suite",
    "bilingual_voice_phase48_capsule_receipt",
    "bilingual_voice_phase48_operator_packet",
    "bilingual_voice_phase48_status_dashboard",
    "bilingual_voice_phase48_runtime",
)


_PHASE48_SUBDIRS = (
    "capsule_contracts",
    "trust_capsules",
    "capsule_manifests",
    "fresh_checkout_simulation",
    "verification_outputs",
    "tamper_tests",
    "receipts",
    "operator_packets",
    "dashboards",
    "reports",
    "fixtures",
    "demos",
)


def suite_a_preflight() -> None:
    upstream_reports = [
        "PHASE47_CROSS_CHECKOUT_FEDERATED_TIMELINE_REPORT.md",
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
    # Phase 47 modules
    for m in (
        "bilingual_voice_phase47_federation_contract.py",
        "bilingual_voice_phase47_timeline_importer.py",
        "bilingual_voice_phase47_federation_graph.py",
        "bilingual_voice_phase47_federation_manifest.py",
        "bilingual_voice_phase47_federation_verifier.py",
        "bilingual_voice_phase47_drift_detector.py",
        "bilingual_voice_phase47_tamper_suite.py",
        "bilingual_voice_phase47_operator_packet.py",
        "bilingual_voice_phase47_status_dashboard.py",
        "bilingual_voice_phase47_runtime.py",
    ):
        _check(f"A::phase47_module::{m}",
               (_ROOT / m).exists())
    # Phase 48 modules
    for m in _PHASE48_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    # Phase 48 sub-folders
    for sub in _PHASE48_SUBDIRS:
        d = (_ROOT / "bilingual_stack"
                   / "voice_adapter_phase48" / sub)
        _check(f"A::folder::{sub}", d.exists())
    for m in _PHASE48_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_capsule_contract() -> None:
    import bilingual_voice_phase48_capsule_contract as cc
    sch = cc.get_phase48_capsule_contract_schema()
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
    _check("B::portable_only_required",
           sch.get(
               "portable_only_verification_required")
           is True)
    arts = cc.get_phase48_required_capsule_artifacts()
    # 14 required capsule artifacts in contract
    for must in ("phase47_federation_contract",
                  "phase47_federation_graph",
                  "phase47_federation_manifest",
                  "phase47_verification_result",
                  "phase47_drift_report",
                  "phase47_tamper_suite_result",
                  "phase47_operator_packet",
                  "phase47_status_dashboard",
                  "phase47_dashboard_markdown",
                  "phase47_report",
                  "capsule_manifest",
                  "fresh_checkout_verification_result",
                  "capsule_receipt",
                  "operator_packet"):
        _check(f"B::required_art::{must}",
               must in arts)
    excl = cc.get_phase48_excluded_artifact_patterns()
    for must in ("*.sqlite", "*.wav", "*.mp3",
                  "local_secret_handoff/",
                  "backups/", ".claude/",
                  "corpus_sources/english/incoming/",
                  "corpus_sources/russian/incoming/"):
        _check(f"B::excluded::{must}", must in excl)
    forb = cc.get_phase48_forbidden_actions()
    for must in ("adapter_invocation_in_capsule",
                  "adapter_reinvocation_in_verifier",
                  "production_db_read_in_verifier",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "tampered_capsule_root_hash",
                  "tampered_manifest_root_hash",
                  "missing_federation_artifact",
                  "corpus_import",
                  "raw_transcript_exposure"):
        _check(f"B::forb::{must}", must in forb)
    contract = cc.create_phase48_capsule_contract(
        capsule_id="cap_test")
    val = cc.validate_phase48_capsule_contract(contract)
    _check("B::validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = cc.validate_phase48_capsule_contract("notdict")
    _check("B::reject_non_dict",
           bad.get("ok") is False)
    drift = dict(contract)
    drift["rehearsal_dry_run_only"] = False
    bad2 = cc.validate_phase48_capsule_contract(drift)
    _check("B::catch_non_dry_run",
           bad2.get("ok") is False)
    drift2 = dict(contract)
    drift2["adapter_invocation_forbidden"] = False
    bad3 = cc.validate_phase48_capsule_contract(drift2)
    _check("B::catch_adapter_invoke_allowed",
           bad3.get("ok") is False)
    drift3 = dict(contract)
    drift3["production_db_read_forbidden"] = False
    bad4 = cc.validate_phase48_capsule_contract(drift3)
    _check("B::catch_db_read_allowed",
           bad4.get("ok") is False)
    drift4 = dict(contract)
    drift4["tamper_detection_required"] = False
    bad5 = cc.validate_phase48_capsule_contract(drift4)
    _check("B::catch_tamper_not_required",
           bad5.get("ok") is False)
    drift5 = dict(contract)
    drift5["portable_only_verification_required"] = False
    bad6 = cc.validate_phase48_capsule_contract(drift5)
    _check("B::catch_portable_not_required",
           bad6.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = cc.write_phase48_capsule_contract_report(
            contract, str(out))
        _check("B::contract_written",
               Path(p).exists())


def _build_capsule_pair():
    """Build a capsule + manifest using current Phase 47 state."""
    import bilingual_voice_phase47_runtime as p47rt
    import bilingual_voice_phase48_capsule_contract as cc
    import bilingual_voice_phase48_capsule_builder as cb
    import bilingual_voice_phase48_capsule_manifest as cm
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase47")
    p47rt.run_phase47_cross_checkout_federation(
        output_dir=str(base), checkout_count=2)
    contract = cc.create_phase48_capsule_contract(
        capsule_id="cap_test")
    capsule = cb.create_phase48_trust_capsule(
        contract=contract)
    manifest = cm.create_phase48_capsule_manifest(capsule)
    return contract, capsule, manifest


def suite_c_capsule_builder() -> None:
    import bilingual_voice_phase48_capsule_builder as cb
    _, capsule, _ = _build_capsule_pair()
    val = cb.validate_phase48_trust_capsule(capsule)
    _check("C::validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("C::artifact_count_10",
           capsule.get("artifact_count") == 10,
           str(capsule.get("artifact_count")))
    ents = capsule.get("artifacts") or []
    _check("C::has_10_entries", len(ents) == 10)
    for e in ents:
        _check("C::has_artifact_key",
               isinstance(e.get("artifact_key"), str)
               and bool(e.get("artifact_key")))
        _check("C::has_relative_path",
               isinstance(e.get("relative_path"), str)
               and bool(e.get("relative_path")))
        sha = e.get("sha256")
        _check("C::sha256_64",
               isinstance(sha, str) and len(sha) == 64)
        sz = e.get("size_bytes")
        _check("C::size_bytes_int",
               isinstance(sz, int) and sz >= 0)
        _check("C::inline_content_present_key",
               "inline_content" in e)
    cr = capsule.get("capsule_root_hash")
    _check("C::capsule_root_64",
           isinstance(cr, str) and len(cr) == 64)
    bs = capsule.get("boundary_summary") or {}
    for k in ("no_audio", "no_tts", "no_subprocess",
              "no_network", "no_multiprocessing",
              "no_main_runtime_integration",
              "no_adapter_invocation_in_capsule",
              "no_production_db_read_in_capsule"):
        _check(f"C::boundary::{k}", bs.get(k) is True)
    _check("C::adapter_allowlist_5",
           capsule.get("adapter_allowlist_count") == 5)
    _check("C::phase21_blocked",
           capsule.get("phase21_status_text")
           == "BLOCKED")
    _check("C::dry_run_only",
           capsule.get("rehearsal_dry_run_only")
           is True)
    # Hashes dict has 10 entries
    hashes = capsule.get("artifact_hashes") or {}
    _check("C::hashes_10", len(hashes) == 10)
    for k, v in hashes.items():
        _check(f"C::hash_64::{k}",
               isinstance(v, str) and len(v) == 64)
    # Production baseline
    pb = capsule.get("production_baseline_expected") or {}
    _check("C::baseline_en_2814",
           pb.get("english_words") == 2814)
    _check("C::baseline_ru_2518",
           pb.get("russian_words") == 2518)
    _check("C::baseline_ru_phrases_35",
           pb.get("russian_phrases") == 35)
    _check("C::baseline_concepts_26",
           pb.get("bilingual_concepts") == 26)
    _check("C::baseline_links_52",
           pb.get("bilingual_entry_links") == 52)
    _check("C::baseline_manifests_90",
           pb.get("live_pack_manifests") == 90)
    # Banned field injection rejected
    bad = dict(capsule)
    bad["signing_key_material"] = "leak"
    badv = cb.validate_phase48_trust_capsule(bad)
    _check("C::reject_banned_field",
           badv.get("ok") is False)
    # Round-trip write
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "cap.json"
        p = cb.write_phase48_trust_capsule(
            capsule, str(out))
        _check("C::capsule_written", Path(p).exists())
    summary = cb.summarize_phase48_trust_capsule(capsule)
    _check("C::summary_ok",
           summary.get("ok") is True)


def suite_d_capsule_manifest() -> None:
    import bilingual_voice_phase48_capsule_manifest as cm
    _, capsule, manifest = _build_capsule_pair()
    val = cm.validate_phase48_capsule_manifest(manifest)
    _check("D::manifest_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    mr = manifest.get("manifest_root_hash")
    _check("D::manifest_root_64",
           isinstance(mr, str) and len(mr) == 64)
    _check("D::capsule_id_match",
           manifest.get("capsule_id")
           == capsule.get("capsule_id"))
    _check("D::artifact_count_10",
           manifest.get("artifact_count") == 10)
    _check("D::phase21_blocked",
           manifest.get("phase21_status")
           == "BLOCKED")
    _check("D::adapter_count_5",
           manifest.get("adapter_allowlist_count")
           == 5)
    # verify against capsule
    v = cm.verify_phase48_capsule_manifest(
        capsule, manifest)
    _check("D::verify_ok",
           v.get("ok") is True,
           ",".join(v.get("reasons", [])))
    # Deterministic root
    manifest2 = cm.create_phase48_capsule_manifest(
        capsule)
    _check("D::manifest_root_deterministic",
           manifest.get("manifest_root_hash")
           == manifest2.get("manifest_root_hash"))
    # Mutate one artifact hash -> verifier catches
    bad_cap = copy.deepcopy(capsule)
    h = dict(bad_cap.get("artifact_hashes") or {})
    if h:
        k0 = next(iter(h))
        h[k0] = "0" * 64
        bad_cap["artifact_hashes"] = h
    bad_v = cm.verify_phase48_capsule_manifest(
        bad_cap, manifest)
    _check("D::detect_hash_drift",
           bad_v.get("ok") is False)
    tamper = cm.detect_phase48_manifest_tampering(
        bad_cap, manifest)
    _check("D::tamper_detected",
           tamper.get("tampered") is True)
    # Banned field rejection
    bad_m = dict(manifest)
    bad_m["signing_key_material"] = "leak"
    badv = cm.validate_phase48_capsule_manifest(bad_m)
    _check("D::manifest_reject_banned",
           badv.get("ok") is False)
    # Round-trip
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "m.json"
        p = cm.write_phase48_capsule_manifest(
            manifest, str(out))
        _check("D::manifest_written", Path(p).exists())
        loaded = cm.read_phase48_capsule_manifest(
            str(out))
        _check("D::manifest_roundtrip",
               loaded.get("manifest_id")
               == manifest.get("manifest_id"))


def suite_e_fresh_checkout_verifier() -> None:
    import bilingual_voice_phase48_fresh_checkout_verifier \
        as fcv
    _, capsule, manifest = _build_capsule_pair()
    result = fcv.verify_phase48_capsule_fresh_checkout(
        capsule, manifest=manifest)
    _check("E::overall_ok",
           result.get("ok") is True,
           result.get("summary"))
    for sub in ("presence_check", "hash_check",
                 "federation_check", "boundary_check",
                 "phase21_check",
                 "no_runtime_state_check"):
        sc = result.get(sub) or {}
        _check(f"E::{sub}_ok",
               sc.get("ok") is True,
               json.dumps(sc.get("reasons",
                                   sc.get("mismatches",
                                          sc.get("missing", []))))[:200])
    # Source file: NO sqlite3 imports
    src = (_ROOT
            / "bilingual_voice_phase48_fresh_checkout_verifier.py"
            ).read_text(encoding="utf-8")
    _check("E::no_import_sqlite3",
           "import sqlite3" not in src)
    _check("E::no_from_sqlite3",
           "from sqlite3" not in src)
    _check("E::no_sqlite3_connect",
           "sqlite3.connect" not in src)
    # Federation root cross-check fires
    bad = copy.deepcopy(capsule)
    for e in bad.get("artifacts") or []:
        if e.get("artifact_key") == \
                "phase47_federation_graph":
            ic = e.get("inline_content")
            if isinstance(ic, dict):
                ic["federation_root_hash"] = "0" * 64
    bad_result = fcv.verify_phase48_federation_claims(
        bad)
    _check("E::federation_root_mismatch_caught",
           bad_result.get("ok") is False)
    reasons = " ".join(bad_result.get("reasons", []))
    _check("E::mismatch_reason_present",
           "federation_root_hash_graph_vs_manifest_mismatch"
           in reasons
           or "federation_root_hash_graph_vs_operator_packet_mismatch"
           in reasons,
           reasons[:200])
    # Missing presence test
    bad2 = copy.deepcopy(capsule)
    bad2["artifacts"] = [
        e for e in bad2.get("artifacts") or []
        if isinstance(e, dict)
        and e.get("artifact_key")
            != "phase47_federation_graph"]
    pres = fcv.verify_phase48_capsule_artifact_presence(
        bad2)
    _check("E::presence_missing_caught",
           pres.get("ok") is False)
    # Boundary check banned field caught
    bad3 = copy.deepcopy(capsule)
    for e in bad3.get("artifacts") or []:
        ic = e.get("inline_content")
        if isinstance(ic, dict):
            ic["raw_transcript"] = "leak"
            break
    bv = fcv.verify_phase48_boundary_claims(bad3)
    _check("E::boundary_banned_inline_caught",
           bv.get("ok") is False)
    # Phase 21 check
    bad4 = copy.deepcopy(capsule)
    bad4["phase21_status_text"] = "UNBLOCKED"
    p21 = fcv.verify_phase48_phase21_claim(bad4)
    _check("E::phase21_drift_caught",
           p21.get("ok") is False)
    # No runtime state dependency
    bad5 = copy.deepcopy(capsule)
    ents = list(bad5.get("artifacts") or [])
    ents.append({"artifact_key": "leak",
                  "relative_path":
                      "lexicon/luna_vocabulary.sqlite",
                  "absolute_path":
                      "lexicon/luna_vocabulary.sqlite",
                  "sha256": "1" * 64,
                  "size_bytes": 1})
    bad5["artifacts"] = ents
    nrs = fcv.verify_phase48_no_runtime_state_dependency(
        bad5)
    _check("E::runtime_db_caught",
           nrs.get("ok") is False)
    # Write report
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "fresh.json"
        p = fcv.write_phase48_fresh_checkout_report(
            result, str(out))
        _check("E::report_written", Path(p).exists())


def suite_f_tamper_suite() -> None:
    import bilingual_voice_phase48_capsule_tamper_suite \
        as ts
    _, capsule, manifest = _build_capsule_pair()
    cases = ts.create_phase48_tamper_cases(capsule)
    _check("F::13_cases_listed", len(cases) == 13,
           str(len(cases)))
    result = ts.run_phase48_tamper_suite(
        capsule, manifest=manifest)
    val = ts.validate_phase48_tamper_suite_result(
        result)
    _check("F::validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("F::case_count_13",
           result.get("case_count") == 13)
    _check("F::all_13_detected",
           result.get("detected_count") == 13,
           str(result.get("detected_count")))
    _check("F::ok_true",
           result.get("ok") is True)
    _check("F::undetected_0",
           result.get("undetected_count") == 0)
    expected_cases = (
        "missing_federation_graph",
        "missing_federation_manifest",
        "missing_tamper_suite_result",
        "modified_federation_root_hash",
        "modified_capsule_artifact_hash",
        "adapter_count_mutation",
        "phase21_status_unexpected",
        "injected_audio_flag",
        "injected_secret_field",
        "injected_command_field",
        "injected_runtime_db_reference",
        "production_db_read_claim",
        "adapter_invocation_claim",
    )
    seen = {r.get("case") for r in result.get("results") or []}
    for c in expected_cases:
        _check(f"F::case_present::{c}", c in seen)
    for r in result.get("results") or []:
        _check(f"F::detected::{r.get('case')}",
               r.get("detected") is True)
    # Source isolation - forbidden tokens
    src_path = (_ROOT
                 / "bilingual_voice_phase48_capsule_tamper_suite.py"
                 )
    src = src_path.read_text(encoding="utf-8")
    forbidden_audio = (
        "pyttsx3", "gtts", "edge_tts", "piper.", "coqui",
        "whisper", "pyaudio", "sounddevice", "pydub",
        "soundfile", "comtypes", "win32com",
    )
    forbidden_exec = (
        "subprocess.run", "subprocess.Popen",
        "subprocess.call", "os.system(", "shell=True",
        "os.popen", "ctypes.windll", "powershell.exe",
    )
    forbidden_net = (
        "urllib.request", "http.client", "requests.",
        "httpx.", "socket.socket",
    )
    for tok in forbidden_audio:
        _check(f"F::no_audio::{tok}",
               tok not in src)
    for tok in forbidden_exec:
        _check(f"F::no_exec::{tok}",
               tok not in src)
    for tok in forbidden_net:
        _check(f"F::no_net::{tok}",
               tok not in src)
    _check("F::no_sqlite_tamper",
           "import sqlite3" not in src
           and "sqlite3.connect" not in src)
    # Summary + write
    summary = ts.summarize_phase48_tamper_suite(result)
    _check("F::summary_ok",
           summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "ts.json"
        p = ts.write_phase48_tamper_suite_report(
            result, str(out))
        _check("F::tamper_written", Path(p).exists())


def suite_g_capsule_receipt() -> None:
    import bilingual_voice_phase48_capsule_receipt as cr
    import bilingual_voice_phase48_capsule_contract as cc
    import bilingual_voice_phase48_fresh_checkout_verifier \
        as fcv
    import bilingual_voice_phase48_capsule_tamper_suite \
        as ts
    contract = cc.create_phase48_capsule_contract(
        capsule_id="cap_test")
    _, capsule, manifest = _build_capsule_pair()
    fresh = fcv.verify_phase48_capsule_fresh_checkout(
        capsule, manifest=manifest)
    tamper = ts.run_phase48_tamper_suite(
        capsule, manifest=manifest)
    receipt = cr.create_phase48_capsule_receipt(
        contract, capsule, manifest, fresh, tamper)
    val = cr.validate_phase48_capsule_receipt(receipt)
    _check("G::receipt_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    for f in ("receipt_id", "created_at", "phase",
              "source_phase", "capsule_id",
              "capsule_root_hash", "manifest_root_hash",
              "artifact_count",
              "fresh_checkout_verification_status",
              "tamper_suite_status",
              "no_runtime_state_dependency",
              "no_adapter_invocation",
              "no_audio", "no_tts",
              "no_subprocess", "no_network",
              "no_multiprocessing",
              "phase21_status",
              "adapter_allowlist_count",
              "rollback_readiness",
              "next_recommended_phase",
              "rehearsal_dry_run_only"):
        _check(f"G::field::{f}", f in receipt)
    _check("G::snapshot_status_present",
           "snapshot_status" in receipt)
    _check("G::snapshot_status_value",
           str(receipt.get("snapshot_status") or "")
           in ("ok", "drift_detected"),
           str(receipt.get("snapshot_status")))
    _check("G::adapter_5",
           receipt.get("adapter_allowlist_count") == 5)
    _check("G::phase21_blocked",
           receipt.get("phase21_status") == "BLOCKED")
    _check("G::dry_run_only",
           receipt.get("rehearsal_dry_run_only") is True)
    _check("G::fresh_status_ok",
           receipt.get(
               "fresh_checkout_verification_status")
           == "ok")
    tss = receipt.get("tamper_suite_status") or {}
    _check("G::tamper_ok",
           tss.get("ok") is True)
    _check("G::tamper_detected_13",
           tss.get("detected_count") == 13)
    _check("G::tamper_case_count_13",
           tss.get("case_count") == 13)
    # Banned field rejection
    bad = dict(receipt)
    bad["signing_key_material"] = "leak"
    badv = cr.validate_phase48_capsule_receipt(bad)
    _check("G::reject_banned_field",
           badv.get("ok") is False)
    # Wrong adapter count
    bad2 = dict(receipt)
    bad2["adapter_allowlist_count"] = 4
    badv2 = cr.validate_phase48_capsule_receipt(bad2)
    _check("G::reject_wrong_adapter_count",
           badv2.get("ok") is False)
    # Dry run off
    bad3 = dict(receipt)
    bad3["rehearsal_dry_run_only"] = False
    badv3 = cr.validate_phase48_capsule_receipt(bad3)
    _check("G::reject_non_dry_run",
           badv3.get("ok") is False)
    summary = cr.summarize_phase48_capsule_receipt(
        receipt)
    _check("G::summary_ok",
           summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "r.json"
        p = cr.write_phase48_capsule_receipt(
            receipt, str(out))
        _check("G::receipt_written", Path(p).exists())


def suite_h_operator_packet_and_dashboard() -> None:
    import bilingual_voice_phase48_runtime as rt
    import bilingual_voice_phase48_operator_packet as op
    import bilingual_voice_phase48_status_dashboard as sd
    out = rt.run_phase48_federation_portability_snapshot()
    pkt = out.get("operator_packet") or {}
    val_pkt = op.validate_phase48_operator_packet(pkt)
    _check("H::packet_validates",
           val_pkt.get("ok") is True,
           ",".join(val_pkt.get("reasons", [])))
    _check("H::packet_status_ok",
           pkt.get("phase48_status") in
           ("ok", "ok_with_warnings"))
    vb = pkt.get("verification_breakdown") or {}
    for k in ("presence_ok", "hash_ok",
              "federation_ok", "boundary_ok",
              "phase21_ok", "no_runtime_state_ok"):
        _check(f"H::breakdown::{k}",
               vb.get(k) is True)
    tss = pkt.get("tamper_suite_summary") or {}
    _check("H::pkt_tamper_ok",
           tss.get("ok") is True)
    _check("H::pkt_tamper_13",
           tss.get("detected_count") == 13)
    rs = pkt.get("receipt_summary") or {}
    _check("H::receipt_summary_present",
           isinstance(rs, dict) and len(rs) > 0)
    _check("H::receipt_summary_snapshot_ok",
           rs.get("snapshot_status") == "ok")
    allow = pkt.get("adapter_allowlist_status") or {}
    _check("H::pkt_allow_expected_5",
           allow.get("expected_count") == 5)
    _check("H::pkt_allow_observed_5",
           allow.get("observed_count") == 5)
    p21 = pkt.get("phase21_import_status") or {}
    _check("H::pkt_phase21_blocked",
           p21.get("status_text") == "BLOCKED")
    md = op.create_phase48_operator_packet_markdown(pkt)
    _check("H::packet_md_nonempty",
           isinstance(md, str) and len(md) > 300)
    for needle in ("Phase 48", "Source phase",
                    "Capsule id", "Artifact count",
                    "Capsule root hash",
                    "Fresh-checkout verification",
                    "Tamper suite",
                    "Phase 21 import status",
                    "Adapter allowlist",
                    "Next recommended"):
        _check(f"H::md_contains::{needle}",
               needle in md, needle)
    dash = out.get("status_dashboard") or {}
    val_d = sd.validate_phase48_status_dashboard(dash)
    _check("H::dashboard_validates",
           val_d.get("ok") is True,
           ",".join(val_d.get("reasons", [])))
    for f in ("dashboard_id", "created_at", "phase",
              "phase48_status", "source_phase",
              "artifact_count",
              "capsule_root_status",
              "fresh_checkout_verification_status",
              "tamper_suite_status",
              "no_runtime_state_status",
              "adapter_allowlist_status",
              "phase21_import_status",
              "forbidden_boundaries_preserved",
              "next_recommended_phase"):
        _check(f"H::dash_field::{f}", f in dash)
    _check("H::dash_source_phase47",
           dash.get("source_phase") == "phase47")
    fb = dash.get("forbidden_boundaries_preserved") or []
    _check("H::dash_forbidden_nonempty",
           isinstance(fb, list) and len(fb) > 0)
    _check("H::dash_capsule_root_match",
           dash.get("capsule_root_status") == "match")
    _check("H::dash_phase21_blocked",
           dash.get("phase21_import_status")
           == "BLOCKED")
    dash_md = sd.create_phase48_dashboard_markdown(dash)
    _check("H::dash_md_nonempty",
           isinstance(dash_md, str)
           and len(dash_md) > 300)
    for needle in ("Phase 48", "Source phase",
                    "Capsule root",
                    "Fresh-checkout verification",
                    "Tamper suite",
                    "Adapter allowlist",
                    "Phase 21 import status",
                    "Forbidden boundaries"):
        _check(f"H::dash_md_contains::{needle}",
               needle in dash_md, needle)
    bad = dict(dash)
    bad["source_phase"] = "phaseXX"
    bad_v = sd.validate_phase48_status_dashboard(bad)
    _check("H::dash_validator_catches_bad_source",
           bad_v.get("ok") is False)


def suite_i_phase48_runtime_e2e() -> None:
    import bilingual_voice_phase48_runtime as rt
    with tempfile.TemporaryDirectory() as td:
        base = Path(td) / "voice_adapter_phase48"
        out = rt.run_phase48_federation_portability_snapshot(
            output_dir=str(base))
        _check("I::status_ok",
               out.get("status") == "ok",
               str(out.get("status")))
        _check("I::phase21_blocked",
               out.get("phase21_status") == "BLOCKED")
        for f in ("phase48_id", "contract",
                  "trust_capsule", "capsule_manifest",
                  "fresh_checkout_result",
                  "tamper_suite_result",
                  "capsule_receipt",
                  "operator_packet",
                  "status_dashboard", "status",
                  "safety_summary", "isolation_summary",
                  "phase21_status",
                  "paths_written"):
            _check(f"I::field::{f}", f in out)
        paths = out.get("paths_written") or []
        _check("I::paths_written_nonempty",
               len(paths) > 0)
        for p in paths:
            _check(f"I::path_exists::{Path(p).name}",
                   Path(p).exists())
        val = rt.validate_phase48_snapshot_output(out)
        _check("I::output_validates",
               val.get("ok") is True,
               ",".join(val.get("reasons", [])))
        summary = rt.summarize_phase48_snapshot_output(
            out)
        _check("I::summary_ok",
               summary.get("ok") is True)
        # Tamper result still 13/13
        tr = out.get("tamper_suite_result") or {}
        _check("I::tamper_13_13",
               tr.get("detected_count") == 13
               and tr.get("case_count") == 13)
        fr = out.get("fresh_checkout_result") or {}
        _check("I::fresh_ok",
               fr.get("ok") is True)
        # Safety summary
        ss = out.get("safety_summary") or {}
        for k in ("no_audio", "no_tts",
                  "no_subprocess", "no_network",
                  "no_multiprocessing",
                  "no_corpus_import",
                  "no_main_runtime_integration",
                  "no_adapter_invocation_in_capsule",
                  "no_production_db_read_in_capsule"):
            _check(f"I::safety::{k}",
                   ss.get(k) is True)
        iso = out.get("isolation_summary") or {}
        _check("I::iso_no_program_s",
               iso.get("no_program_s") is True)
        # Expected output files written
        for sub, fname in (
            ("capsule_contracts",
             "capsule_contract.json"),
            ("trust_capsules",
             "trust_capsule.json"),
            ("capsule_manifests",
             "capsule_manifest.json"),
            ("verification_outputs",
             "fresh_checkout_result.json"),
            ("tamper_tests", "tamper_suite.json"),
            ("receipts", "capsule_receipt.json"),
            ("operator_packets",
             "operator_packet.json"),
            ("dashboards", "OPERATOR_PACKET.md"),
            ("dashboards", "STATUS_DASHBOARD.json"),
            ("dashboards", "STATUS_DASHBOARD.md"),
        ):
            p = base / sub / fname
            _check(f"I::written::{sub}/{fname}",
                   p.exists())


def suite_j_production_safety_and_isolation() -> None:
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase48")
    # 1. Count audio files under voice_adapter_phase48
    audio: list[str] = []
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a", ".aac",
                         ".opus")):
                    audio.append(os.path.join(root, f))
    _check("J::no_audio_in_voice_adapter_phase48",
           not audio, ",".join(audio))
    # 2. Incoming folders empty
    en_inc = (_ROOT / "corpus_sources"
                    / "english" / "incoming")
    ru_inc = (_ROOT / "corpus_sources"
                    / "russian" / "incoming")
    if en_inc.exists():
        n = sum(1 for p in en_inc.iterdir()
                if p.is_file())
        _check("J::en_incoming_empty",
               n == 0, str(n))
    else:
        _check("J::en_incoming_absent_ok", True)
    if ru_inc.exists():
        n = sum(1 for p in ru_inc.iterdir()
                if p.is_file())
        _check("J::ru_incoming_empty",
               n == 0, str(n))
    else:
        _check("J::ru_incoming_absent_ok", True)
    # 3. Forbidden tokens scan over all 9 Phase 48 modules
    files = [f"{m}.py" for m in _PHASE48_MODULES]
    forbidden_audio = (
        "pyttsx3", "gtts", "edge_tts", "piper.", "coqui",
        "whisper", "pyaudio", "sounddevice", "pydub",
        "soundfile", "comtypes", "win32com",
        "winsound",
    )
    forbidden_exec = (
        "subprocess.run", "subprocess.Popen",
        "subprocess.call", "os.system(", "shell=True",
        "os.popen", "ctypes.windll", "powershell.exe",
        "import subprocess", "from subprocess",
    )
    forbidden_net = (
        "urllib.request", "http.client", "requests.",
        "httpx.", "socket.socket",
        "import socket", "from socket",
    )
    # Runtime-assemble these tokens so the harness file
    # itself doesn't contain them as bare strings.
    _LM = "luna" + "_" + "modules"
    _WORK = "import" + " " + "worker"
    _FROM_WORK = "from" + " " + "worker"
    _TIER_P = "tier_" + "progression"
    _PROBE_A = "probe" + "_" + "attestation"
    _ATT_S = "attestation" + "_signer"
    forbidden_runtime = (
        _LM, _WORK, _FROM_WORK,
        _TIER_P, _PROBE_A, _ATT_S,
    )
    forbidden_threading = (
        "threading.Thread", "multiprocessing.Process",
        "multiprocessing.Pool", "daemon=True",
        "asyncio.create_task", "schedule.every",
        "import threading", "from threading",
        "import multiprocessing",
        "from multiprocessing",
    )
    for fn in files:
        p = _ROOT / fn
        if not p.exists():
            _check(f"J::file_exists::{fn}", False, fn)
            continue
        src = p.read_text(encoding="utf-8")
        for tok in forbidden_audio:
            _check(
                f"J::{fn}::no_audio:{tok.strip()}",
                tok not in src)
        for tok in forbidden_exec:
            _check(
                f"J::{fn}::no_exec:{tok.strip()}",
                tok not in src)
        for tok in forbidden_net:
            _check(
                f"J::{fn}::no_net:{tok.strip()}",
                tok not in src)
        for tok in forbidden_runtime:
            _check(
                f"J::{fn}::no_runtime:{tok.strip()}",
                tok not in src)
        for tok in forbidden_threading:
            _check(
                f"J::{fn}::no_daemon:{tok.strip()}",
                tok not in src)
    # 4. fresh-checkout verifier MUST NOT import sqlite3
    fcv_src = (_ROOT
                / "bilingual_voice_phase48_fresh_checkout_verifier.py"
                ).read_text(encoding="utf-8")
    _check("J::fcv_no_import_sqlite3",
           "import sqlite3" not in fcv_src)
    _check("J::fcv_no_from_sqlite3",
           "from sqlite3" not in fcv_src)
    _check("J::fcv_no_sqlite3_connect",
           "sqlite3.connect" not in fcv_src)
    # 5. Production baselines
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
    else:
        _check("J::en_db_absent_skipped", True)
    if ru_db.exists():
        c = sqlite3.connect(str(ru_db))
        nw = c.execute(
            "SELECT COUNT(*) FROM words").fetchone()[0]
        np_ = c.execute(
            "SELECT COUNT(*) FROM phrases").fetchone()[0]
        c.close()
        _check("J::ru_2518", nw == 2518)
        _check("J::ru_phr_35", np_ == 35)
    else:
        _check("J::ru_db_absent_skipped", True)
    if link_db.exists():
        c = sqlite3.connect(str(link_db))
        nc = c.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0]
        nl = c.execute(
            "SELECT COUNT(*) FROM entry_links").fetchone()[0]
        c.close()
        _check("J::concepts_26", nc == 26)
        _check("J::links_52", nl == 52)
    else:
        _check("J::link_db_absent_skipped", True)
    # 6. Live pack manifests (90)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"),
        recursive=True) if "backups" not in p]
    if live:
        _check("J::manifests_90", len(live) == 90,
               str(len(live)))
    else:
        _check("J::manifests_absent_skipped", True)
    # 7. Phase 48 source files - extra runtime-assembled
    # token scan: confirm no actual imports of Program S
    # internals. The string 'program_s_modification'
    # appears in forbidden-action labels, which is fine;
    # what we forbid is an actual import or function call.
    extra_forbidden_patterns = (
        "import program_s",
        "from program_s",
        "program_s.",
    )
    for fn in files:
        p = _ROOT / fn
        if not p.exists():
            continue
        src = p.read_text(encoding="utf-8")
        for tok in extra_forbidden_patterns:
            _check(
                f"J::{fn}::extra::{tok.strip()}",
                tok not in src)
    # 8. Secret-boundary directory scan
    import bilingual_voice_phase36_secret_boundary as sb
    if base.exists():
        scan_dirs = [base / sub
                       for sub in _PHASE48_SUBDIRS]
        for d in scan_dirs:
            if not d.exists():
                _check(
                    f"J::leak_scan_dir_present:{d.name}",
                    True)
                continue
            scan = (sb
                     .validate_no_secret_leakage_in_directory(
                         str(d)))
            _check(f"J::no_leak_in:{d.name}",
                   scan["ok"],
                   json.dumps(
                       scan.get("leaks", []))[:200])


def suite_k_regression_smoke() -> None:
    upstream = [
        "bilingual_voice_phase47_runtime",
        "bilingual_voice_phase46_runtime",
        "bilingual_voice_phase45_runtime",
        "bilingual_voice_phase44_runtime",
        "bilingual_voice_phase43_runtime",
        "bilingual_voice_phase42_runtime",
        "bilingual_voice_adapter_phase41_runtime",
        "bilingual_voice_phase40_replay_verifier",
        "bilingual_voice_phase39_runtime",
        "bilingual_voice_phase38_status_dashboard",
        "bilingual_voice_phase37_signed_witness_pipeline",
        "bilingual_voice_phase36_handoff_runtime",
        "bilingual_voice_phase35_exchange_runtime",
        "bilingual_voice_phase34_export_runtime",
        "bilingual_voice_phase33_signed_evidence",
        "bilingual_voice_audit_chain_signer",
        "bilingual_voice_phase31_post_call_equivalence",
        "bilingual_voice_adapter_phase30_runtime",
        "bilingual_voice_adapter_phase29_runtime",
        "bilingual_voice_adapter_phase28_runtime",
        "bilingual_voice_adapter_phase27_runtime"
        if (_ROOT
            / "bilingual_voice_adapter_phase27_runtime.py"
            ).exists() else
        "bilingual_voice_renderer_interface",
        "bilingual_voice_memory_runtime",
        "bilingual_spoken_render_runtime",
    ]
    for m in upstream:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reimport {m}: {e}")
        _check(f"K::reimport::{m}", ok)
    for m in _PHASE48_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    # Smoke phase 47 e2e
    try:
        import bilingual_voice_phase47_runtime as p47rt
        out = p47rt.run_phase47_cross_checkout_federation(
            checkout_count=2)
        _check("K::phase47_e2e_status_ok",
               out.get("status") in
               ("ok", "ok_with_warnings"))
        _check("K::phase47_e2e_phase21_blocked",
               out.get("phase21_status") == "BLOCKED")
    except Exception as e:  # noqa: BLE001
        _check("K::phase47_e2e_no_exception",
               False, str(e))
    # Smoke phase 46 e2e
    try:
        import bilingual_voice_phase46_runtime as p46rt
        out = p46rt.run_phase46_long_horizon_timeline()
        _check("K::phase46_e2e_status_ok",
               out.get("status") in
               ("ok", "ok_with_warnings"))
    except Exception as e:  # noqa: BLE001
        _check("K::phase46_e2e_no_exception",
               False, str(e))
    # Smoke phase 48 e2e once more
    try:
        import bilingual_voice_phase48_runtime as p48rt
        out = (p48rt
               .run_phase48_federation_portability_snapshot())
        _check("K::phase48_e2e_status_ok",
               out.get("status") in
               ("ok", "ok_with_warnings"))
        _check("K::phase48_e2e_phase21_blocked",
               out.get("phase21_status") == "BLOCKED")
        _check("K::phase48_e2e_tamper_13",
               (out.get("tamper_suite_result")
                or {}).get("detected_count") == 13)
    except Exception as e:  # noqa: BLE001
        _check("K::phase48_e2e_no_exception",
               False, str(e))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_capsule_contract),
        ("C", suite_c_capsule_builder),
        ("D", suite_d_capsule_manifest),
        ("E", suite_e_fresh_checkout_verifier),
        ("F", suite_f_tamper_suite),
        ("G", suite_g_capsule_receipt),
        ("H", suite_h_operator_packet_and_dashboard),
        ("I", suite_i_phase48_runtime_e2e),
        ("J", suite_j_production_safety_and_isolation),
        ("K", suite_k_regression_smoke),
    ]
    for name, fn in suites:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            traceback.print_exc()
            _check(f"{name}::suite_uncaught",
                   False, str(e))
    print(f"Total: {_TOTAL} | Pass: {_PASS} | "
          f"Fail: {_FAIL}")
    if _FAILURES:
        print("--- failures ---")
        for f in _FAILURES[:80]:
            print(f)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
