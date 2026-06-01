"""Phase 40 test harness - operator audit-replay."""

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


_PHASE40_MODULES = (
    "bilingual_voice_phase40_replay_contract",
    "bilingual_voice_phase40_replay_loader",
    "bilingual_voice_phase40_trace_replayer",
    "bilingual_voice_phase40_drift_detector",
    "bilingual_voice_phase40_replay_verifier",
    "bilingual_voice_phase40_operator_packet",
    "bilingual_voice_phase40_status_dashboard",
)


def suite_a_preflight() -> None:
    upstream = [
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
        "bilingual_voice_phase39_rehearsal_contract.py",
        "bilingual_voice_phase39_consent_orchestrator.py",
        "bilingual_voice_phase39_stage_executor.py",
        "bilingual_voice_phase39_trace_assembler.py",
        "bilingual_voice_phase39_governance_recheck.py",
        "bilingual_voice_phase39_rehearsal_report.py",
        "bilingual_voice_phase39_runtime.py",
        "bilingual_voice_phase38_governance_ledger.py",
        "bilingual_voice_phase38_operator_readme.py",
        "bilingual_voice_phase38_verification_checklist.py",
        "bilingual_voice_phase38_rollback_matrix.py",
        "bilingual_voice_phase38_commit_safety_audit.py",
        "bilingual_voice_phase38_status_dashboard.py",
        "bilingual_voice_phase38_integrity_sweep.py",
    ]
    for f in upstream:
        _check(f"A::upstream_present::{f}",
               (_ROOT / f).exists(), f)
    rehearsal = (_ROOT / "bilingual_stack"
                       / "rehearsal_phase39")
    for sub, fname in (
        ("contracts", "rehearsal_contract.json"),
        ("consents", "umbrella_consent.json"),
        ("traces", "rehearsal_trace.json"),
        ("recheck", "rehearsal_recheck.json"),
        ("reports", "rehearsal_report.json"),
        ("reports", "rehearsal_report.md"),
    ):
        _check(f"A::phase39_artifact::{sub}/{fname}",
               (rehearsal / sub / fname).exists(),
               f"{sub}/{fname}")
    for m in _PHASE40_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists())
    for sub in ("replay_inputs", "replay_outputs",
                 "drift_reports", "verification_reports",
                 "operator_packets", "reports",
                 "dashboards", "fixtures"):
        d = (_ROOT / "bilingual_stack"
                   / "governance_phase40" / sub)
        _check(f"A::folder::{sub}", d.exists(), str(d))
    for m in _PHASE40_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_replay_contract() -> None:
    import bilingual_voice_phase40_replay_contract as rc
    sch = rc.get_phase40_replay_contract_schema()
    _check("B::schema_is_dict", isinstance(sch, dict))
    _check("B::schema_dry_run_only",
           sch.get("rehearsal_dry_run_only") is True)
    _check("B::schema_no_new_adapter",
           sch.get("new_adapter_invocation_forbidden")
           is True)
    inputs = rc.get_phase40_required_replay_inputs()
    for must in ("rehearsal_contract", "umbrella_consent",
                  "stage_receipts", "rehearsal_trace",
                  "rehearsal_recheck", "rehearsal_report",
                  "phase38_governance_artifacts"):
        _check(f"B::required_input::{must}",
               must in inputs)
    cats = rc.get_phase40_drift_categories()
    for must in ("missing_artifact", "hash_chain_drift",
                  "adapter_allowlist_drift",
                  "baseline_drift",
                  "phase21_status_drift",
                  "secret_leakage"):
        _check(f"B::drift_cat::{must}",
               must in cats)
    forb = rc.get_phase40_forbidden_actions()
    for must in ("new_adapter_invocation",
                  "generate_audio", "invoke_tts",
                  "run_subprocess", "network_call",
                  "multiprocessing",
                  "production_db_modification"):
        _check(f"B::forb::{must}", must in forb)
    c = rc.create_phase40_replay_contract(
        replay_id="rid_test_1")
    _check("B::contract_is_dict", isinstance(c, dict))
    val = rc.validate_phase40_replay_contract(c)
    _check("B::contract_validates", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = rc.validate_phase40_replay_contract("notdict")
    _check("B::validator_rejects_non_dict",
           bad.get("ok") is False)
    drift = dict(c)
    drift["rehearsal_dry_run_only"] = False
    bad2 = rc.validate_phase40_replay_contract(drift)
    _check("B::validator_catches_non_dry_run",
           bad2.get("ok") is False)
    drift2 = dict(c)
    drift2["new_adapter_invocation_forbidden"] = False
    bad3 = rc.validate_phase40_replay_contract(drift2)
    _check("B::validator_catches_new_adapter_allowed",
           bad3.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = rc.write_phase40_replay_contract_report(
            c, str(out))
        _check("B::contract_written", Path(p).exists())


def suite_c_replay_loader() -> None:
    import bilingual_voice_phase40_replay_loader as rl
    arts = rl.load_phase39_replay_artifacts()
    _check("C::artifacts_dict", isinstance(arts, dict))
    _check("C::artifacts_ok",
           arts.get("ok") is True,
           "missing="
           f"{arts.get('missing')} rejected="
           f"{arts.get('rejected')}")
    loaded = arts.get("loaded") or {}
    for k in ("rehearsal_contract", "umbrella_consent",
              "rehearsal_trace", "rehearsal_recheck",
              "rehearsal_report"):
        _check(f"C::loaded::{k}", k in loaded)
    _check("C::stage_receipts_10",
           arts.get("stage_receipt_count") == 10,
           str(arts.get("stage_receipt_count")))
    val = rl.validate_loaded_replay_artifacts(arts)
    _check("C::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    summary = rl.summarize_loaded_replay_artifacts(arts)
    _check("C::summary_ok", summary.get("ok") is True)
    # Path safety
    safe_paths = ["bilingual_stack/rehearsal_phase39/"
                  "contracts/rehearsal_contract.json"]
    sres = rl.reject_unsafe_replay_paths(safe_paths)
    _check("C::safe_paths_accepted",
           sres.get("ok") is True)
    unsafe = [
        "https://attacker.example/data.json",
        "file:///etc/passwd",
        "../etc/passwd; rm -rf /",
        "x | nc evil",
        "lexicon/luna_vocabulary.sqlite",
        "bilingual_stack/voice_adapter_phase36/"
            "local_secret_handoff/seal.json",
        "corpus_sources/backups/snap.tar.gz",
    ]
    ures = rl.reject_unsafe_replay_paths(unsafe)
    _check("C::unsafe_paths_all_rejected",
           ures.get("ok") is False)
    _check("C::unsafe_rejected_all_count",
           ures.get("rejected_count") == len(unsafe),
           str(ures.get("rejected_count")))
    # Bounded read enforcement: tiny limit
    tiny = rl.load_json_artifact(
        str(_ROOT / "bilingual_stack" / "rehearsal_phase39"
                  / "reports" / "rehearsal_report.json"),
        max_bytes=10)
    _check("C::bounded_read_enforced",
           tiny.get("ok") is False
           and "too_large" in str(tiny.get("reason", "")))
    # URL path through load_json_artifact rejected
    url_load = rl.load_json_artifact(
        "https://example.com/data.json")
    _check("C::load_rejects_url",
           url_load.get("ok") is False)
    # Runtime DB extension rejected
    db_load = rl.load_json_artifact(
        "lexicon/luna_vocabulary.sqlite")
    _check("C::load_rejects_runtime_db",
           db_load.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "ld.json"
        p = rl.write_phase40_replay_loader_report(
            arts, str(out))
        _check("C::loader_report_written",
               Path(p).exists())


def suite_d_trace_replayer() -> None:
    import bilingual_voice_phase40_trace_replayer as tr
    import bilingual_voice_phase40_replay_loader as rl
    arts = rl.load_phase39_replay_artifacts()
    chain = tr.rederive_receipt_hash_chain(
        arts.get("stage_receipts") or [])
    _check("D::chain_len_10",
           len(chain) == 10, str(len(chain)))
    root = tr.rederive_trace_root_hash(chain)
    _check("D::root_hash_64",
           isinstance(root, str) and len(root) == 64)
    rep = tr.replay_phase39_trace(arts)
    _check("D::replay_ok",
           rep.get("ok") is True,
           json.dumps(rep.get("tampering_check") or {})[
               :300])
    tamper = tr.detect_trace_tampering(arts)
    _check("D::tamper_clean_ok",
           tamper.get("ok") is True)
    _check("D::tamper_chain_matches_loaded",
           tamper.get("chain_matches_loaded_receipts")
           is True)
    # Mutate one receipt: hash chain should break
    mutated = copy.deepcopy(arts)
    receipts = mutated.get("stage_receipts") or []
    if receipts:
        obj = receipts[0].get("object")
        if isinstance(obj, dict):
            obj["status"] = "mutated"
    tamper2 = tr.detect_trace_tampering(mutated)
    _check("D::tamper_detects_receipt_mutation",
           tamper2.get("ok") is False)
    # Tamper the stored trace hash chain itself
    mutated2 = copy.deepcopy(arts)
    trace_obj = ((mutated2.get("loaded") or {})
                  .get("rehearsal_trace") or {}).get(
                      "object")
    if isinstance(trace_obj, dict):
        trace_obj["receipt_hash_chain"] = ["0" * 64]
    tamper3 = tr.detect_trace_tampering(mutated2)
    _check("D::tamper_detects_chain_swap",
           tamper3.get("ok") is False)
    # Inject disallowed adapter into a receipt
    mutated3 = copy.deepcopy(arts)
    receipts3 = mutated3.get("stage_receipts") or []
    if receipts3:
        obj = receipts3[0].get("object")
        if isinstance(obj, dict):
            obj["selected_adapter_name"] = \
                "real_piper_adapter"
    tamper4 = tr.detect_trace_tampering(mutated3)
    _check("D::tamper_detects_bad_adapter",
           tamper4.get("ok") is False)
    # Inject audio/secret/command fields
    mutated4 = copy.deepcopy(arts)
    receipts4 = mutated4.get("stage_receipts") or []
    if receipts4:
        obj = receipts4[0].get("object")
        if isinstance(obj, dict):
            obj["produced_audio"] = True
    tamper5 = tr.detect_trace_tampering(mutated4)
    _check("D::tamper_detects_audio_flag",
           tamper5.get("ok") is False)
    mutated5 = copy.deepcopy(arts)
    receipts5 = mutated5.get("stage_receipts") or []
    if receipts5:
        obj = receipts5[0].get("object")
        if isinstance(obj, dict):
            obj["signing_key_material"] = "leak"
    tamper6 = tr.detect_trace_tampering(mutated5)
    _check("D::tamper_detects_secret_field",
           tamper6.get("ok") is False)
    # Non-dict
    nd = tr.detect_trace_tampering("notdict")
    _check("D::tamper_rejects_non_dict",
           nd.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "trep.json"
        p = tr.write_phase40_trace_replay_report(
            rep, str(out))
        _check("D::replay_report_written",
               Path(p).exists())


def suite_e_drift_detector() -> None:
    import bilingual_voice_phase40_drift_detector as dd
    import bilingual_voice_phase40_replay_loader as rl
    arts = rl.load_phase39_replay_artifacts()
    drift = dd.detect_phase40_drift(arts)
    _check("E::drift_is_dict", isinstance(drift, dict))
    _check("E::drift_ok",
           drift.get("ok") is True,
           f"fail={drift.get('fail_count')} "
           f"warn={drift.get('warn_count')}")
    _check("E::drift_no_fail",
           drift.get("fail_count") == 0)
    cats = {r.get("category")
             for r in drift.get("results") or []}
    for must in ("missing_artifact",
                  "consent_binding_drift",
                  "adapter_allowlist_drift",
                  "governance_doc_drift",
                  "baseline_drift",
                  "phase21_status_drift",
                  "forbidden_boundary_drift",
                  "secret_audio_command_drift"):
        _check(f"E::category_present::{must}",
               must in cats)
    # Adapter allowlist baseline matches
    al = dd.check_adapter_allowlist_drift(arts)
    _check("E::allowlist_no_drift",
           al.get("drifted") is False)
    # Baseline drift catch: mutate observed by reading
    # production DBs and asserting drift==False (already
    # covered)
    bl = dd.check_baseline_drift(arts)
    _check("E::baseline_no_drift",
           bl.get("drifted") is False,
           ",".join(bl.get("drifts") or []))
    # Phase 21 status
    p21 = dd.check_phase21_status_drift(arts)
    _check("E::phase21_blocked",
           p21.get("phase21_status_text") == "BLOCKED")
    _check("E::phase21_drift_warn_or_pass",
           p21.get("severity") in ("warn", "pass"))
    # Forbidden boundary
    bd = dd.check_boundary_drift(arts)
    _check("E::boundary_no_drift",
           bd.get("drifted") is False)
    # Secret/audio/command scan
    sc = dd.check_secret_audio_command_drift(arts)
    _check("E::secret_audio_command_no_drift",
           sc.get("drifted") is False)
    # Inject runtime flag and re-detect
    mutated = copy.deepcopy(arts)
    receipts = mutated.get("stage_receipts") or []
    if receipts:
        obj = receipts[0].get("object")
        if isinstance(obj, dict):
            obj["used_subprocess"] = True
    sc2 = dd.check_secret_audio_command_drift(mutated)
    _check("E::secret_audio_command_catches_runtime",
           sc2.get("drifted") is True)
    # Inject baseline drift by swapping allowlist
    bad_arts = copy.deepcopy(arts)
    rc_obj = ((bad_arts.get("loaded") or {})
               .get("rehearsal_recheck") or {}).get(
                   "object")
    if isinstance(rc_obj, dict):
        rc_obj["adapter_allowlist"] = [
            "rogue_only_adapter"]
    al2 = dd.check_adapter_allowlist_drift(bad_arts)
    _check("E::allowlist_drift_detected",
           al2.get("drifted") is True)
    s = dd.summarize_phase40_drift(drift)
    _check("E::drift_summary_ok",
           s.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "drift.json"
        p = dd.write_phase40_drift_report(drift, str(out))
        _check("E::drift_report_written",
               Path(p).exists())


def suite_f_replay_verifier() -> None:
    import bilingual_voice_phase40_replay_verifier as rv
    import bilingual_voice_phase40_replay_loader as rl
    r = rv.verify_phase40_replay()
    _check("F::verify_is_dict", isinstance(r, dict))
    _check("F::status_ok_or_warnings",
           r.get("status") in ("ok", "ok_with_warnings"),
           str(r.get("status")))
    val = rv.validate_phase40_verification_result(r)
    _check("F::validate_ok",
           val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    s = rv.summarize_phase40_verification_result(r)
    _check("F::summary_ok", s.get("ok") is True)
    # Missing artifact path: point at empty temp dir
    with tempfile.TemporaryDirectory() as td:
        empty_arts = rl.load_phase39_replay_artifacts(
            base_dir=td)
        r2 = rv.verify_phase40_replay_from_artifacts(
            empty_arts)
        _check("F::missing_artifact_blocks",
               r2.get("status") == "drift_detected")
    # Tampered receipts -> trace replay fails
    arts = rl.load_phase39_replay_artifacts()
    mutated = copy.deepcopy(arts)
    receipts = mutated.get("stage_receipts") or []
    if receipts:
        obj = receipts[0].get("object")
        if isinstance(obj, dict):
            obj["status"] = "mutated"
    r3 = rv.verify_phase40_replay_from_artifacts(mutated)
    _check("F::tamper_blocks_ok",
           r3.get("status") == "drift_detected")
    # Non-dict input
    bad = rv.validate_phase40_verification_result(
        "notdict")
    _check("F::validator_rejects_non_dict",
           bad.get("ok") is False)
    # Write verification result to disk
    out = (_ROOT / "bilingual_stack" / "governance_phase40"
                 / "verification_reports"
                 / "verification_result.json")
    p = rv.write_phase40_verification_result(r, str(out))
    _check("F::verification_written",
           Path(p).exists())


def suite_g_operator_packet() -> None:
    import bilingual_voice_phase40_operator_packet as op
    import bilingual_voice_phase40_replay_verifier as rv
    r = rv.verify_phase40_replay()
    pkt = op.create_phase40_operator_packet(r)
    _check("G::packet_is_dict",
           isinstance(pkt, dict))
    val = op.validate_phase40_operator_packet(pkt)
    _check("G::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    # No raw operator_id / no signing material
    for k in ("operator_id", "signing_key_material",
              "private_key", "material_hex",
              "sealed_payload"):
        _check(f"G::no_banned::{k}", k not in pkt
               or pkt.get(k) in (None, "", False))
    # No audio / command-line fields
    for k in ("produced_audio", "invoked_tts",
              "used_subprocess", "command",
              "command_line", "spoken_render_payload"):
        _check(f"G::no_runtime::{k}", k not in pkt)
    # Phase 21 status included
    p21 = pkt.get("phase21_status") or {}
    _check("G::phase21_status_included",
           "status_text" in p21)
    _check("G::phase21_blocked",
           p21.get("status_text") == "BLOCKED"
           or "STAGED" in str(p21.get("status_text", "")))
    _check("G::next_phase_present",
           bool(pkt.get("next_recommended_phase")))
    # Inject banned field via patching: validator catches
    bad = dict(pkt)
    bad["operator_id"] = "raw_operator"
    bad_val = op.validate_phase40_operator_packet(bad)
    _check("G::validator_catches_operator_id",
           bad_val.get("ok") is False)
    bad2 = dict(pkt)
    bad2["signing_key_material"] = "leak"
    bad2_val = op.validate_phase40_operator_packet(bad2)
    _check("G::validator_catches_signing_key",
           bad2_val.get("ok") is False)
    s = op.summarize_phase40_operator_packet(pkt)
    _check("G::summary_ok", s.get("ok") is True)
    out = (_ROOT / "bilingual_stack" / "governance_phase40"
                 / "operator_packets"
                 / "operator_packet.json")
    p = op.write_phase40_operator_packet(pkt, str(out))
    _check("G::packet_written", Path(p).exists())


def suite_h_status_dashboard() -> None:
    import bilingual_voice_phase40_status_dashboard as sd
    import bilingual_voice_phase40_replay_verifier as rv
    r = rv.verify_phase40_replay()
    d = sd.create_phase40_status_dashboard(r)
    _check("H::dashboard_is_dict", isinstance(d, dict))
    val = sd.validate_phase40_status_dashboard(d)
    _check("H::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = sd.validate_phase40_status_dashboard("notdict")
    _check("H::validator_rejects_non_dict",
           bad.get("ok") is False)
    drift = dict(d)
    drift["source_phase"] = "phaseXX"
    bad2 = sd.validate_phase40_status_dashboard(drift)
    _check("H::validator_catches_wrong_source",
           bad2.get("ok") is False)
    _check("H::trace_hash_match",
           d.get("trace_hash_status") == "match")
    _check("H::source_phase_is_phase39",
           d.get("source_phase") == "phase39")
    _check("H::phase21_status_in_dashboard",
           "phase21_import_status" in d)
    _check("H::adapter_4",
           (d.get("adapter_allowlist_status") or {})
            .get("expected_count") == 4)
    md = sd.create_phase40_dashboard_markdown(d)
    _check("H::md_nonempty", isinstance(md, str)
           and len(md) > 300)
    for needle in ("Phase 40", "Trace hash",
                    "Phase 21 import status",
                    "Forbidden boundaries"):
        _check(f"H::md_contains::{needle}",
               needle in md, needle)
    base = (_ROOT / "bilingual_stack"
                  / "governance_phase40" / "dashboards")
    out_json = base / "AUDIT_REPLAY_DASHBOARD.json"
    out_md = base / "AUDIT_REPLAY_DASHBOARD.md"
    p1 = sd.write_phase40_status_dashboard(d, str(out_json))
    p2 = sd.write_phase40_status_dashboard_markdown(
        md, str(out_md))
    _check("H::dashboard_json_written",
           Path(p1).exists())
    _check("H::dashboard_md_written",
           Path(p2).exists())


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
                  / "governance_phase40")
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("I::no_audio_files_in_governance_phase40",
           not audio, ",".join(audio))
    # No corpus import: incoming folders stay empty
    en_inc = (_ROOT / "corpus_sources" / "english"
                    / "incoming")
    ru_inc = (_ROOT / "corpus_sources" / "russian"
                    / "incoming")
    if en_inc.exists():
        files = list(en_inc.iterdir())
        _check("I::en_incoming_empty",
               not [f for f in files if f.is_file()],
               str(len(files)))
    if ru_inc.exists():
        files = list(ru_inc.iterdir())
        _check("I::ru_incoming_empty",
               not [f for f in files if f.is_file()],
               str(len(files)))


def suite_j_isolation() -> None:
    files = [f"{m}.py" for m in _PHASE40_MODULES]
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
                  / "governance_phase40")
    scan_dirs = [
        base / sub for sub in (
            "replay_inputs", "replay_outputs",
            "drift_reports", "verification_reports",
            "operator_packets", "reports",
            "dashboards")
    ]
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
        "bilingual_voice_phase39_runtime",
        "bilingual_voice_phase39_rehearsal_contract",
        "bilingual_voice_phase39_consent_orchestrator",
        "bilingual_voice_phase39_stage_executor",
        "bilingual_voice_phase39_trace_assembler",
        "bilingual_voice_phase39_governance_recheck",
        "bilingual_voice_phase39_rehearsal_report",
        "bilingual_voice_phase38_governance_ledger",
        "bilingual_voice_phase38_status_dashboard",
        "bilingual_voice_phase38_integrity_sweep",
        "bilingual_voice_adapter_phase37_runtime",
        "bilingual_voice_phase37_governance_recheck",
        "bilingual_voice_phase37_adapter_interface",
    ]
    for m in upstream:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reimport {m}: {e}")
        _check(f"K::reimport::{m}", ok)
    for m in _PHASE40_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    try:
        import bilingual_voice_phase40_replay_verifier as rv
        import bilingual_voice_phase40_operator_packet as op
        import bilingual_voice_phase40_status_dashboard \
            as sd
        r = rv.verify_phase40_replay()
        pkt = op.create_phase40_operator_packet(r)
        dash = sd.create_phase40_status_dashboard(r)
        _check("K::e2e_verify_ok",
               r.get("status") in
               ("ok", "ok_with_warnings"))
        _check("K::e2e_packet_validates",
               op.validate_phase40_operator_packet(pkt)
                .get("ok") is True)
        _check("K::e2e_dashboard_validates",
               sd.validate_phase40_status_dashboard(dash)
                .get("ok") is True)
    except Exception as e:  # noqa: BLE001
        _check("K::e2e_no_exception", False, str(e))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_replay_contract),
        ("C", suite_c_replay_loader),
        ("D", suite_d_trace_replayer),
        ("E", suite_e_drift_detector),
        ("F", suite_f_replay_verifier),
        ("G", suite_g_operator_packet),
        ("H", suite_h_status_dashboard),
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
