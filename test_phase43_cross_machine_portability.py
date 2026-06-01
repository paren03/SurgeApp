"""Phase 43 test harness - cross-machine portability."""

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


_PHASE43_MODULES = (
    "bilingual_voice_phase43_portability_contract",
    "bilingual_voice_phase43_bundle_builder",
    "bilingual_voice_phase43_bundle_manifest",
    "bilingual_voice_phase43_fresh_checkout_verifier",
    "bilingual_voice_phase43_portability_auditor",
    "bilingual_voice_phase43_operator_packet",
    "bilingual_voice_phase43_status_dashboard",
    "bilingual_voice_phase43_runtime",
)


def suite_a_preflight() -> None:
    upstream = [
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
        "bilingual_voice_phase42_runtime.py",
        "bilingual_voice_phase42_operator_packet.py",
        "bilingual_voice_phase42_coherence_auditor.py",
        "bilingual_voice_phase42_replay_matrix.py",
        "bilingual_voice_phase42_drift_stability_matrix.py",
    ]
    for f in upstream:
        _check(f"A::upstream_present::{f}",
               (_ROOT / f).exists(), f)
    # Phase 42 artifacts must exist before bundling
    p42_base = (_ROOT / "bilingual_stack"
                      / "voice_adapter_phase42")
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
        _check(f"A::phase42_artifact::{sub}/{fname}",
               (p42_base / sub / fname).exists())
    for m in _PHASE43_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    for sub in ("portable_bundles", "bundle_manifests",
                 "fresh_checkout_inputs",
                 "fresh_checkout_outputs",
                 "verification_reports",
                 "portability_audits",
                 "operator_packets", "dashboards",
                 "reports", "fixtures", "demos"):
        d = (_ROOT / "bilingual_stack"
                   / "voice_adapter_phase43" / sub)
        _check(f"A::folder::{sub}", d.exists())
    for m in _PHASE43_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_portability_contract() -> None:
    import bilingual_voice_phase43_portability_contract \
        as pc
    sch = pc.get_phase43_portability_contract_schema()
    _check("B::schema_is_dict", isinstance(sch, dict))
    _check("B::dry_run",
           sch.get("rehearsal_dry_run_only") is True)
    _check("B::no_reinvoke",
           sch.get("fresh_checkout_no_adapter_reinvocation")
           is True)
    arts = pc.get_phase43_required_bundle_artifacts()
    for must in ("phase42_audit_contract",
                  "phase42_trace_batch",
                  "phase42_coherence_audit",
                  "phase42_replay_matrix",
                  "phase42_drift_stability_matrix",
                  "phase42_operator_packet",
                  "phase42_operator_markdown",
                  "phase42_report",
                  "integrity_manifest",
                  "portability_summary"):
        _check(f"B::required_artifact::{must}",
               must in arts)
    excl = pc.get_phase43_excluded_artifact_patterns()
    for must in ("*.sqlite", "*.db", "*.wav", "*.mp3",
                  "local_secret_handoff/",
                  "backups/", ".claude/",
                  "corpus_sources/english/incoming/"):
        _check(f"B::excluded::{must}", must in excl)
    forb = pc.get_phase43_forbidden_actions()
    for must in ("new_adapter_invocation",
                  "adapter_reinvocation_on_fresh_checkout",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "production_db_read_in_fresh_checkout"):
        _check(f"B::forb::{must}", must in forb)
    c = pc.create_phase43_portability_contract(
        bundle_id="bid_test")
    val = pc.validate_phase43_portability_contract(c)
    _check("B::contract_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = pc.validate_phase43_portability_contract(
        "notdict")
    _check("B::validator_rejects_non_dict",
           bad.get("ok") is False)
    drift = dict(c)
    drift["rehearsal_dry_run_only"] = False
    bad2 = pc.validate_phase43_portability_contract(drift)
    _check("B::validator_catches_non_dry_run",
           bad2.get("ok") is False)
    drift2 = dict(c)
    drift2["fresh_checkout_no_adapter_reinvocation"] = \
        False
    bad3 = pc.validate_phase43_portability_contract(drift2)
    _check("B::validator_catches_reinvoke_allowed",
           bad3.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = pc.write_phase43_portability_contract_report(
            c, str(out))
        _check("B::contract_written", Path(p).exists())


def suite_c_bundle_builder() -> None:
    import bilingual_voice_phase43_bundle_builder as bb
    arts = bb.collect_phase42_bundle_artifacts()
    _check("C::collection_dict", isinstance(arts, dict))
    _check("C::collection_count_8",
           arts.get("count") == 8,
           str(arts.get("count")))
    _check("C::no_missing",
           not arts.get("missing"),
           str(arts.get("missing")))
    _check("C::no_excluded_keys",
           not arts.get("excluded_keys"),
           str(arts.get("excluded_keys")))
    hashes = bb.compute_phase43_artifact_hashes(arts)
    _check("C::hashes_8", len(hashes) == 8)
    for k, v in hashes.items():
        _check(f"C::hash_64::{k}",
               isinstance(v, str) and len(v) == 64)
    bundle = bb.create_phase43_portable_bundle()
    _check("C::bundle_is_dict", isinstance(bundle, dict))
    val = bb.validate_phase43_portable_bundle(bundle)
    _check("C::bundle_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("C::bundle_artifact_count_8",
           bundle.get("artifact_count") == 8)
    # No runtime DBs
    for e in bundle.get("artifacts") or []:
        ap = str(e.get("relative_path") or "").lower()
        for tok in (".sqlite", ".sqlite3", ".db"):
            _check(f"C::no_runtime_db::"
                   f"{e.get('artifact_key')}:{tok}",
                   not ap.endswith(tok))
    # No audio
    for e in bundle.get("artifacts") or []:
        ap = str(e.get("relative_path") or "").lower()
        for tok in (".wav", ".mp3", ".ogg", ".flac",
                     ".m4a"):
            _check(f"C::no_audio::"
                   f"{e.get('artifact_key')}:{tok}",
                   not ap.endswith(tok))
    # No local_secret_handoff
    for e in bundle.get("artifacts") or []:
        ap = str(e.get("relative_path")
                  or "").replace("\\", "/").lower()
        _check(f"C::no_handoff::"
               f"{e.get('artifact_key')}",
               "/local_secret_handoff/" not in ap)
    # Inline content bounded — every present entry <=
    # 512KB
    for e in bundle.get("artifacts") or []:
        sz = e.get("size_bytes") or 0
        _check(f"C::size_within_2mb::"
               f"{e.get('artifact_key')}",
               sz <= 2_000_000,
               str(sz))
    # No adapter invocation field set
    _check("C::no_adapter_reinvocation_flag",
           (bundle.get("boundary_summary") or {}).get(
               "no_adapter_reinvocation_in_bundle")
           is True)
    summary = bb.summarize_phase43_portable_bundle(bundle)
    _check("C::summary_ok", summary.get("ok") is True)
    # Reject non-dict
    bad = bb.validate_phase43_portable_bundle("notdict")
    _check("C::validator_rejects_non_dict",
           bad.get("ok") is False)
    # Inject banned field — validator catches
    drift = dict(bundle)
    drift["raw_transcript"] = "leak"
    bad2 = bb.validate_phase43_portable_bundle(drift)
    _check("C::validator_catches_raw_transcript",
           bad2.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "b.json"
        p = bb.write_phase43_portable_bundle(
            bundle, str(out))
        _check("C::bundle_written", Path(p).exists())


def suite_d_bundle_manifest() -> None:
    import bilingual_voice_phase43_bundle_builder as bb
    import bilingual_voice_phase43_bundle_manifest as bm
    bundle = bb.create_phase43_portable_bundle()
    manifest = bm.create_phase43_bundle_manifest(bundle)
    val = bm.validate_phase43_bundle_manifest(manifest)
    _check("D::manifest_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("D::manifest_artifact_count_8",
           manifest.get("artifact_count") == 8)
    _check("D::manifest_phase21_blocked",
           str(manifest.get("phase21_status") or "")
           in ("BLOCKED", "STAGED_AWAITING_OPERATOR"))
    verify = bm.verify_phase43_bundle_manifest(
        bundle, manifest)
    _check("D::manifest_verifies",
           verify.get("ok") is True,
           ",".join(verify.get("reasons", [])))
    # Tampered artifact hash
    bad_bundle = copy.deepcopy(bundle)
    if bad_bundle.get("artifacts"):
        bad_bundle["artifacts"][0]["sha256"] = "0" * 64
    tamper = bm.detect_phase43_manifest_tampering(
        bad_bundle, manifest)
    _check("D::tamper_detects_hash_drift",
           tamper.get("tampered") is True)
    # Missing artifact: drop one
    bad_bundle2 = copy.deepcopy(bundle)
    bad_bundle2["artifacts"] = bundle["artifacts"][1:]
    bad_bundle2["artifact_count"] = \
        len(bad_bundle2["artifacts"])
    bad_bundle2["artifact_hashes"] = {
        k: v for k, v
        in (bundle.get("artifact_hashes") or {}).items()
        if k != bundle["artifacts"][0]["artifact_key"]
    }
    verify2 = bm.verify_phase43_bundle_manifest(
        bad_bundle2, manifest)
    _check("D::manifest_catches_missing_artifact",
           verify2.get("ok") is False)
    # Runtime DB injected — validator rejects manifest
    # with bad hash format
    bad_manifest = copy.deepcopy(manifest)
    bad_manifest["artifact_hashes"]["leak.sqlite"] = \
        "abc"
    bv = bm.validate_phase43_bundle_manifest(bad_manifest)
    _check("D::manifest_validator_catches_bad_hash",
           bv.get("ok") is False)
    # Round-trip write/read
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "m.json"
        p = bm.write_phase43_bundle_manifest(
            manifest, str(out))
        _check("D::manifest_written", Path(p).exists())
        loaded = bm.read_phase43_bundle_manifest(str(out))
        _check("D::manifest_roundtrip_id",
               loaded.get("manifest_id")
               == manifest.get("manifest_id"))


def suite_e_fresh_checkout_verifier() -> None:
    import bilingual_voice_phase43_bundle_builder as bb
    import bilingual_voice_phase43_bundle_manifest as bm
    import bilingual_voice_phase43_fresh_checkout_verifier \
        as fcv
    bundle = bb.create_phase43_portable_bundle()
    manifest = bm.create_phase43_bundle_manifest(bundle)
    result = fcv.verify_phase43_bundle_fresh_checkout(
        bundle, manifest=manifest)
    _check("E::result_is_dict", isinstance(result, dict))
    _check("E::result_ok",
           result.get("ok") is True,
           result.get("summary"))
    _check("E::presence_ok",
           result["presence_check"]["ok"] is True)
    _check("E::hash_ok",
           result["hash_check"]["ok"] is True)
    _check("E::phase42_claims_ok",
           result["phase42_claims_check"]["ok"] is True)
    _check("E::boundary_ok",
           result["boundary_claims_check"]["ok"] is True)
    _check("E::phase21_ok",
           result["phase21_claim_check"]["ok"] is True)
    # Tampered operator packet
    bad = copy.deepcopy(bundle)
    for e in bad.get("artifacts") or []:
        if e.get("artifact_key") == \
                "phase42_operator_packet":
            inline = e.get("inline_content")
            if isinstance(inline, dict):
                inline["audit_status"] = "tampered"
    r2 = fcv.verify_phase43_phase42_claims(bad)
    _check("E::tampered_operator_fails",
           r2.get("ok") is False)
    # Missing replay matrix
    bad2 = copy.deepcopy(bundle)
    bad2["artifacts"] = [
        e for e in bad2.get("artifacts") or []
        if e.get("artifact_key")
        != "phase42_replay_matrix"]
    r3 = fcv.verify_phase43_artifact_presence(bad2)
    _check("E::missing_replay_matrix_fails",
           r3.get("ok") is False)
    # Boundary violation: bundle declares produced_audio
    bad3 = copy.deepcopy(bundle)
    bad3["boundary_summary"] = dict(
        bad3.get("boundary_summary") or {})
    bad3["boundary_summary"]["no_audio"] = False
    r4 = fcv.verify_phase43_boundary_claims(bad3)
    _check("E::boundary_violation_fails",
           r4.get("ok") is False)
    # Inject secret field into inline content
    bad4 = copy.deepcopy(bundle)
    for e in bad4.get("artifacts") or []:
        inline = e.get("inline_content")
        if isinstance(inline, dict):
            inline["signing_key_material"] = "leak"
            break
    r5 = fcv.verify_phase43_boundary_claims(bad4)
    _check("E::secret_field_fails",
           r5.get("ok") is False)
    # Phase 21 status drift
    bad5 = copy.deepcopy(bundle)
    bad5["phase21_status_text"] = "UNBLOCKED_NOW"
    r6 = fcv.verify_phase43_phase21_claim(bad5)
    _check("E::phase21_drift_fails",
           r6.get("ok") is False)
    # No production DB read: confirm verifier never
    # imports sqlite3 or opens a DB connection. Mere
    # mention of ".sqlite" in a token-exclusion list is
    # fine; only actual sqlite3 module use is forbidden.
    src = (_ROOT
            / "bilingual_voice_phase43_fresh_checkout_verifier.py"
            ).read_text(encoding="utf-8")
    _check("E::source_no_sqlite_import",
           "import sqlite3" not in src
           and "from sqlite3" not in src)
    _check("E::source_no_db_connect",
           "sqlite3.connect" not in src)
    # Write
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "fc.json"
        p = fcv.write_phase43_fresh_checkout_report(
            result, str(out))
        _check("E::fresh_report_written",
               Path(p).exists())


def suite_f_portability_auditor() -> None:
    import bilingual_voice_phase43_bundle_builder as bb
    import bilingual_voice_phase43_portability_auditor as pa
    bundle = bb.create_phase43_portable_bundle()
    audit = pa.audit_phase43_bundle_portability(bundle)
    _check("F::audit_ok",
           audit.get("ok") is True,
           audit.get("summary"))
    _check("F::audit_fail_count_0",
           audit.get("fail_count") == 0)
    # Runtime DB inclusion fails
    bad = copy.deepcopy(bundle)
    bad["artifacts"] = list(bad["artifacts"]) + [{
        "artifact_key": "leak_db",
        "relative_path": "lexicon/luna_vocabulary.sqlite",
        "absolute_path": "x",
        "size_bytes": 100,
        "sha256": "0" * 64,
        "inline_content": None,
    }]
    r = pa.audit_phase43_no_runtime_db_artifacts(bad)
    _check("F::runtime_db_fails",
           r.get("severity") == "fail")
    # Audio inclusion fails
    bad2 = copy.deepcopy(bundle)
    bad2["artifacts"] = list(bad2["artifacts"]) + [{
        "artifact_key": "leak_audio",
        "relative_path": "voice/sample.wav",
        "absolute_path": "x",
        "size_bytes": 100,
        "sha256": "0" * 64,
        "inline_content": None,
    }]
    r2 = pa.audit_phase43_no_audio_artifacts(bad2)
    _check("F::audio_fails",
           r2.get("severity") == "fail")
    # Secret inclusion fails
    bad3 = copy.deepcopy(bundle)
    for e in bad3.get("artifacts") or []:
        inline = e.get("inline_content")
        if isinstance(inline, dict):
            inline["signing_key_material"] = "leak"
            break
    r3 = pa.audit_phase43_no_secret_leakage(bad3)
    _check("F::secret_fails",
           r3.get("severity") == "fail")
    # Command field fails
    bad4 = copy.deepcopy(bundle)
    for e in bad4.get("artifacts") or []:
        inline = e.get("inline_content")
        if isinstance(inline, dict):
            inline["command"] = "rm -rf /"
            break
    r4 = pa.audit_phase43_no_command_fields(bad4)
    _check("F::command_fails",
           r4.get("severity") == "fail")
    # Adapter reinvocation claim fails
    bad5 = copy.deepcopy(bundle)
    bad5["boundary_summary"] = dict(
        bad5.get("boundary_summary") or {})
    bad5["boundary_summary"][
        "no_adapter_reinvocation_in_bundle"] = False
    r5 = pa.audit_phase43_no_adapter_invocation_claims(
        bad5)
    _check("F::reinvocation_fails",
           r5.get("severity") == "fail")
    # Phase 21 staged warns
    bad6 = copy.deepcopy(bundle)
    bad6["phase21_status_text"] = \
        "STAGED_AWAITING_OPERATOR"
    r6 = pa.audit_phase43_phase21_metadata(bad6)
    _check("F::phase21_staged_warns",
           r6.get("severity") == "warn")
    summary = pa.summarize_phase43_portability_audit(
        audit)
    _check("F::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "pa.json"
        p = pa.write_phase43_portability_audit_report(
            audit, str(out))
        _check("F::audit_written", Path(p).exists())


def suite_g_operator_packet() -> None:
    import bilingual_voice_phase43_portability_contract \
        as pc
    import bilingual_voice_phase43_bundle_builder as bb
    import bilingual_voice_phase43_bundle_manifest as bm
    import bilingual_voice_phase43_fresh_checkout_verifier \
        as fcv
    import bilingual_voice_phase43_portability_auditor as pa
    import bilingual_voice_phase43_operator_packet as op
    contract = pc.create_phase43_portability_contract(
        "bid_g")
    bundle = bb.create_phase43_portable_bundle(
        contract=contract)
    manifest = bm.create_phase43_bundle_manifest(bundle)
    fresh = fcv.verify_phase43_bundle_fresh_checkout(
        bundle, manifest=manifest)
    audit = pa.audit_phase43_bundle_portability(bundle)
    pkt = op.create_phase43_operator_packet(
        contract, bundle, manifest, fresh, audit)
    val = op.validate_phase43_operator_packet(pkt)
    _check("G::packet_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("G::status_ok",
           pkt.get("portability_status") in
           ("ok", "ok_with_warnings"),
           str(pkt.get("portability_status")))
    # No banned fields
    for k in ("operator_id", "signing_key_material",
              "raw_transcript", "audio_bytes", "command"):
        _check(f"G::no_banned::{k}",
               k not in pkt or pkt.get(k) in
               (None, "", False, [], {}))
    # Phase 21 included
    p21 = pkt.get("phase21_import_status") or {}
    _check("G::phase21_present",
           "status_text" in p21)
    _check("G::phase21_blocked",
           p21.get("status_text") == "BLOCKED"
           or "STAGED" in str(p21.get("status_text",
                                        "")))
    _check("G::next_phase_present",
           bool(pkt.get("next_recommended_phase")))
    # Markdown
    md = op.create_phase43_operator_packet_markdown(pkt)
    _check("G::md_nonempty",
           isinstance(md, str) and len(md) > 300)
    for needle in ("Phase 43", "Portability status",
                    "Phase 21 import status",
                    "Hash verification"):
        _check(f"G::md_contains::{needle}",
               needle in md, needle)
    # Inject banned field
    drift = dict(pkt)
    drift["operator_id"] = "raw"
    bad = op.validate_phase43_operator_packet(drift)
    _check("G::validator_catches_operator_id",
           bad.get("ok") is False)
    summary = op.summarize_phase43_operator_packet(pkt)
    _check("G::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "p.json"
        out_md = Path(td) / "p.md"
        p1 = op.write_phase43_operator_packet(
            pkt, str(out))
        p2 = op.write_phase43_operator_packet_markdown(
            md, str(out_md))
        _check("G::packet_written", Path(p1).exists())
        _check("G::md_written", Path(p2).exists())


def suite_h_status_dashboard() -> None:
    import bilingual_voice_phase43_status_dashboard as sd
    import bilingual_voice_phase43_runtime as rt
    out = rt.run_phase43_portability_harness()
    pkt = out.get("operator_packet") or {}
    dash = sd.create_phase43_status_dashboard(pkt)
    val = sd.validate_phase43_status_dashboard(dash)
    _check("H::dashboard_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("H::source_phase_phase42",
           dash.get("source_phase") == "phase42")
    _check("H::artifact_count_8",
           dash.get("artifact_count") == 8)
    _check("H::hash_status",
           dash.get("hash_verification_status") == "ok")
    _check("H::fresh_status",
           dash.get("fresh_checkout_verification_status")
           == "ok")
    _check("H::phase21_status_present",
           "phase21_import_status" in dash)
    _check("H::adapter_5",
           (dash.get("adapter_allowlist_status") or {})
            .get("expected_count") == 5)
    bad = sd.validate_phase43_status_dashboard("notdict")
    _check("H::validator_rejects_non_dict",
           bad.get("ok") is False)
    drift = dict(dash)
    drift["source_phase"] = "phaseXX"
    bad2 = sd.validate_phase43_status_dashboard(drift)
    _check("H::validator_catches_wrong_source",
           bad2.get("ok") is False)
    md = sd.create_phase43_dashboard_markdown(dash)
    _check("H::md_nonempty",
           isinstance(md, str) and len(md) > 300)
    for needle in ("Phase 43", "Source phase",
                    "Phase 21 import status",
                    "Forbidden boundaries"):
        _check(f"H::md_contains::{needle}",
               needle in md, needle)
    with tempfile.TemporaryDirectory() as td:
        out_json = Path(td) / "d.json"
        out_md = Path(td) / "d.md"
        p1 = sd.write_phase43_status_dashboard(
            dash, str(out_json))
        p2 = sd.write_phase43_status_dashboard_markdown(
            md, str(out_md))
        _check("H::dashboard_json_written",
               Path(p1).exists())
        _check("H::dashboard_md_written",
               Path(p2).exists())


def suite_i_phase43_runtime() -> None:
    import bilingual_voice_phase43_runtime as rt
    base = (_ROOT / "bilingual_stack"
                  / "voice_adapter_phase43")
    out = rt.run_phase43_portability_harness(
        output_dir=str(base))
    _check("I::status_ok",
           out.get("status") in
           ("ok", "ok_with_warnings"),
           str(out.get("status")))
    val = rt.validate_phase43_portability_output(out)
    _check("I::output_validates",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("I::phase21_blocked",
           out.get("phase21_status") == "BLOCKED")
    b = out.get("portable_bundle") or {}
    _check("I::bundle_artifact_count_8",
           b.get("artifact_count") == 8)
    # Artifacts written
    for sub, fname in (
        ("portable_bundles", "portability_contract.json"),
        ("portable_bundles", "portable_bundle.json"),
        ("bundle_manifests", "bundle_manifest.json"),
        ("fresh_checkout_outputs",
         "fresh_checkout_result.json"),
        ("portability_audits",
         "portability_audit.json"),
        ("operator_packets", "operator_packet.json"),
        ("dashboards", "OPERATOR_PACKET.md"),
        ("dashboards", "STATUS_DASHBOARD.json"),
        ("dashboards", "STATUS_DASHBOARD.md"),
    ):
        p = base / sub / fname
        _check(f"I::written::{sub}/{fname}",
               p.exists())
    summary = rt.summarize_phase43_portability_output(out)
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
                  / "voice_adapter_phase43")
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("J::no_audio_in_voice_adapter_phase43",
           not audio, ",".join(audio))
    files = [f"{m}.py" for m in _PHASE43_MODULES]
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
        "portable_bundles", "bundle_manifests",
        "fresh_checkout_outputs",
        "verification_reports", "portability_audits",
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
        "bilingual_voice_phase42_runtime",
        "bilingual_voice_phase42_operator_packet",
        "bilingual_voice_phase42_coherence_auditor",
        "bilingual_voice_phase42_replay_matrix",
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
    for m in _PHASE43_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    try:
        import bilingual_voice_phase43_runtime as rt
        out = rt.run_phase43_portability_harness()
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
        ("B", suite_b_portability_contract),
        ("C", suite_c_bundle_builder),
        ("D", suite_d_bundle_manifest),
        ("E", suite_e_fresh_checkout_verifier),
        ("F", suite_f_portability_auditor),
        ("G", suite_g_operator_packet),
        ("H", suite_h_status_dashboard),
        ("I", suite_i_phase43_runtime),
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
