"""Phase 39 test harness - operator dry-run rehearsal."""

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


_PHASE39_MODULES = (
    "bilingual_voice_phase39_rehearsal_contract",
    "bilingual_voice_phase39_consent_orchestrator",
    "bilingual_voice_phase39_stage_executor",
    "bilingual_voice_phase39_trace_assembler",
    "bilingual_voice_phase39_governance_recheck",
    "bilingual_voice_phase39_rehearsal_report",
    "bilingual_voice_phase39_runtime",
)


def suite_a_preflight() -> None:
    upstream = [
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
        "bilingual_voice_adapter_phase37_runtime.py",
        "bilingual_voice_phase37_governance_recheck.py",
        "bilingual_voice_phase37_adapter_interface.py",
    ]
    for f in upstream:
        _check(f"A::upstream_present::{f}",
               (_ROOT / f).exists(), f)
    for m in _PHASE39_MODULES:
        _check(f"A::file_exists::{m}",
               (_ROOT / f"{m}.py").exists(), m)
    for sub in ("contracts", "consents", "stages", "traces",
                 "recheck", "reports", "fixtures"):
        d = _ROOT / "bilingual_stack" / "rehearsal_phase39" / sub
        _check(f"A::folder::{sub}", d.exists(), str(d))
    for m in _PHASE39_MODULES:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_rehearsal_contract() -> None:
    import bilingual_voice_phase39_rehearsal_contract as rc
    scen = rc.get_canonical_scenarios()
    _check("B::canonical_scenarios_10",
           isinstance(scen, list) and len(scen) >= 10,
           str(len(scen)))
    ids = {s.get("scenario_id") for s in scen}
    _check("B::unique_scenario_ids", len(ids) == len(scen))
    contract = rc.create_rehearsal_contract()
    _check("B::contract_is_dict",
           isinstance(contract, dict))
    val = rc.validate_rehearsal_contract(contract)
    _check("B::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    bad = rc.validate_rehearsal_contract("notdict")
    _check("B::validate_rejects_non_dict",
           bad.get("ok") is False)
    # Drop a required stage
    drift = dict(contract)
    drift["expected_phase_stages"] = ["phase37_only"]
    bad2 = rc.validate_rehearsal_contract(drift)
    _check("B::validate_catches_missing_stage",
           bad2.get("ok") is False)
    # Flip dry_run_only
    drift2 = dict(contract)
    drift2["rehearsal_dry_run_only"] = False
    bad3 = rc.validate_rehearsal_contract(drift2)
    _check("B::validate_catches_non_dry_run",
           bad3.get("ok") is False)
    stages = rc.get_expected_phase_stages()
    for must in ("phase29_per_invocation_consent",
                  "phase32_audit_chain_signing",
                  "phase34_witness_export",
                  "phase35_local_exchange",
                  "phase37_signed_witness_pipeline"):
        _check(f"B::stage::{must}", must in stages)
    forb = rc.get_forbidden_runtime_actions()
    for must in ("generate_audio", "invoke_tts",
                  "run_subprocess", "network_call",
                  "multiprocessing"):
        _check(f"B::forb::{must}", must in forb)
    s = rc.summarize_rehearsal_contract(contract)
    _check("B::summary_ok", s.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "c.json"
        p = rc.write_rehearsal_contract(contract, str(out))
        _check("B::write_path", Path(p).exists())
        loaded = json.loads(out.read_text(encoding="utf-8"))
        _check("B::roundtrip_scenarios",
               loaded.get("scenario_count")
               == contract.get("scenario_count"))


def suite_c_consent_orchestrator() -> None:
    import bilingual_voice_phase39_consent_orchestrator as co
    import bilingual_voice_phase39_rehearsal_contract as rc
    contract = rc.create_rehearsal_contract()
    sc = contract.get("scenario_count") or 0
    consent = co.create_umbrella_consent(
        operator_id="operator_local", scenario_count=sc)
    _check("C::consent_ok",
           consent.get("status") == "ok")
    val = co.validate_umbrella_consent(consent)
    _check("C::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    # Reject missing operator_id
    bad = co.create_umbrella_consent(
        operator_id="", scenario_count=sc)
    _check("C::reject_missing_operator",
           bad.get("status") == "refused")
    # Reject out-of-range scenario count
    bad2 = co.create_umbrella_consent(
        operator_id="op", scenario_count=0)
    _check("C::reject_zero_scenarios",
           bad2.get("status") == "refused")
    bad3 = co.create_umbrella_consent(
        operator_id="op", scenario_count=99)
    _check("C::reject_too_many_scenarios",
           bad3.get("status") == "refused")
    # No raw operator_id in consent surface
    _check("C::no_raw_operator_id",
           "operator_id" not in consent
           or not consent.get("operator_id"))
    # Binding hash is reproducible
    op_hash = consent.get("operator_id_hash")
    _check("C::operator_id_hash_present",
           isinstance(op_hash, str) and len(op_hash) == 64)
    # Binding hash mismatch is caught
    drift = dict(consent)
    drift["binding_hash"] = "0" * 64
    bad4 = co.validate_umbrella_consent(drift)
    _check("C::validate_catches_bad_binding",
           bad4.get("ok") is False)
    # Expiry catch
    expired = dict(consent)
    expired["expiry_at"] = 1
    bad5 = co.validate_umbrella_consent(expired)
    _check("C::validate_catches_expiry",
           bad5.get("ok") is False)
    # Bind to contract
    binding = co.bind_consent_to_contract(consent, contract)
    _check("C::bind_ok", binding.get("ok") is True,
           ",".join(binding.get("reasons", [])))
    # Mismatch scenario count is caught
    drift_contract = dict(contract)
    drift_contract["scenario_count"] = 999
    bad6 = co.bind_consent_to_contract(
        consent, drift_contract)
    _check("C::bind_catches_count_mismatch",
           bad6.get("ok") is False)
    summary = co.summarize_umbrella_consent(consent)
    _check("C::summary_ok", summary.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "consent.json"
        p = co.write_umbrella_consent(consent, str(out))
        _check("C::write_path", Path(p).exists())


def suite_d_stage_executor() -> None:
    import bilingual_voice_phase39_stage_executor as se
    import bilingual_voice_phase39_rehearsal_contract as rc
    scen = rc.get_canonical_scenarios()
    # Execute first scenario (english_simple_dummy)
    receipt = se.execute_scenario(
        scen[0], operator_id="operator_local")
    _check("D::receipt_is_dict",
           isinstance(receipt, dict))
    val = se.validate_scenario_receipt(receipt)
    _check("D::receipt_validates", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("D::status_ok",
           receipt.get("status") == "ok")
    _check("D::adapter_dummy",
           receipt.get("selected_adapter_name")
           == "dummy_metadata_adapter")
    _check("D::pipe_status_ok",
           str(receipt.get("signed_pipeline_status")
                or "") in ("ok", "ok_with_warnings"))
    stages = receipt.get("stages_present") or {}
    for k in ("phase29_per_invocation_consent",
              "phase30_callable_boundary",
              "phase31_two_adapter_selection",
              "phase32_audit_chain_signing",
              "phase34_witness_export",
              "phase35_local_exchange",
              "phase37_signed_witness_pipeline"):
        _check(f"D::stage_present::{k}",
               stages.get(k) is True, k)
    _check("D::no_runtime_leak",
           receipt.get("no_runtime_leak") is True)
    _check("D::audit_chain_nonempty",
           (receipt.get("audit_chain_length") or 0) >= 1)
    # Refuse non-dict scenario
    bad = se.execute_scenario("notdict")
    _check("D::refuses_non_dict",
           bad.get("status") == "refused")
    # Kill-switch scenario yields kill_switch_blocked
    ks_scen = next(s for s in scen
                    if s.get("scenario_id") == "S06")
    ks_receipt = se.execute_scenario(ks_scen)
    _check("D::ks_blocked",
           ks_receipt.get("kill_switch_blocked") is True)
    # approve=False scenario yields a refused-shaped status
    no_app = next(s for s in scen
                   if s.get("scenario_id") == "S05")
    no_receipt = se.execute_scenario(no_app)
    _check("D::approve_false_refused",
           "refused" in str(no_receipt.get("status") or ""))
    # Validator catches banned fields
    drift = dict(receipt)
    drift["produced_audio"] = True
    bad2 = se.validate_scenario_receipt(drift)
    _check("D::validator_catches_audio",
           bad2.get("ok") is False)
    drift2 = dict(receipt)
    drift2["signing_key_material"] = "secret"
    bad3 = se.validate_scenario_receipt(drift2)
    _check("D::validator_catches_secret",
           bad3.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "r.json"
        p = se.write_scenario_receipt(receipt, str(out))
        _check("D::write_path", Path(p).exists())


def suite_e_trace_assembler() -> None:
    import bilingual_voice_phase39_trace_assembler as ta
    import bilingual_voice_phase39_rehearsal_contract as rc
    import bilingual_voice_phase39_consent_orchestrator as co
    import bilingual_voice_phase39_stage_executor as se
    contract = rc.create_rehearsal_contract()
    consent = co.create_umbrella_consent(
        "operator_local",
        contract.get("scenario_count") or 0)
    receipts = [se.execute_scenario(s)
                 for s in contract.get("scenarios") or []]
    trace = ta.assemble_rehearsal_trace(
        contract, consent, receipts)
    _check("E::trace_is_dict", isinstance(trace, dict))
    val = ta.validate_rehearsal_trace(trace)
    _check("E::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("E::receipt_count",
           trace.get("receipt_count") == len(receipts))
    _check("E::dry_run_only",
           trace.get("rehearsal_dry_run_only") is True)
    cov = trace.get("per_stage_coverage") or {}
    for k in ("phase29_per_invocation_consent",
              "phase32_audit_chain_signing",
              "phase34_witness_export",
              "phase35_local_exchange",
              "phase37_signed_witness_pipeline"):
        _check(f"E::coverage_key_present::{k}", k in cov)
    # Adapter distribution covers all four
    dist = trace.get("adapter_distribution") or {}
    for a in ("dummy_metadata_adapter",
              "bilingual_segment_metadata_adapter",
              "prosody_density_metadata_adapter",
              "safety_redaction_trace_metadata_adapter"):
        _check(f"E::adapter_in_dist::{a}", a in dist, a)
    # Hash chain integrity
    _check("E::hash_chain_len",
           len(trace.get("receipt_hash_chain") or [])
           == len(receipts))
    _check("E::trace_root_hash_present",
           isinstance(trace.get("trace_root_hash"), str)
           and len(trace.get("trace_root_hash")) == 64)
    # Validator catches tampered chain
    drift = dict(trace)
    drift["receipt_hash_chain"] = ["0" * 64]
    bad = ta.validate_rehearsal_trace(drift)
    _check("E::validator_catches_chain_drift",
           bad.get("ok") is False)
    # Validator catches non-dry-run flip
    drift2 = dict(trace)
    drift2["rehearsal_dry_run_only"] = False
    bad2 = ta.validate_rehearsal_trace(drift2)
    _check("E::validator_catches_non_dry_run",
           bad2.get("ok") is False)
    s = ta.summarize_rehearsal_trace(trace)
    _check("E::summary_ok", s.get("ok") is True)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "t.json"
        p = ta.write_rehearsal_trace(trace, str(out))
        _check("E::write_path", Path(p).exists())


def suite_f_governance_recheck() -> None:
    import bilingual_voice_phase39_governance_recheck as gr
    import bilingual_voice_phase39_trace_assembler as ta
    import bilingual_voice_phase39_rehearsal_contract as rc
    import bilingual_voice_phase39_consent_orchestrator as co
    import bilingual_voice_phase39_stage_executor as se
    contract = rc.create_rehearsal_contract()
    consent = co.create_umbrella_consent(
        "operator_local",
        contract.get("scenario_count") or 0)
    receipts = [se.execute_scenario(s)
                 for s in contract.get("scenarios") or []]
    trace = ta.assemble_rehearsal_trace(
        contract, consent, receipts)
    recheck = gr.recheck_rehearsal_trace(trace)
    _check("F::recheck_is_dict",
           isinstance(recheck, dict))
    val = gr.validate_rehearsal_recheck(recheck)
    _check("F::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("F::ok", recheck.get("ok") is True,
           recheck.get("summary"))
    _check("F::allowlist_4",
           len(recheck.get("adapter_allowlist") or []) == 4)
    _check("F::all_within_allowlist",
           recheck.get("all_receipts_within_allowlist")
           is True)
    _check("F::all_metadata_only",
           recheck.get("all_receipts_metadata_only")
           is True)
    _check("F::secret_leakage_ok",
           recheck.get("secret_leakage_ok") is True)
    # Inject a bad adapter — recheck must catch
    bad_receipts = [dict(r) for r in receipts]
    bad_receipts[0]["selected_adapter_name"] = \
        "real_piper_adapter"
    bad_trace = ta.assemble_rehearsal_trace(
        contract, consent, bad_receipts)
    bad_recheck = gr.recheck_rehearsal_trace(bad_trace)
    _check("F::recheck_catches_bad_adapter",
           bad_recheck.get("ok") is False)
    # Inject a runtime leak — recheck must catch
    leak_receipts = [dict(r) for r in receipts]
    leak_receipts[0]["produced_audio"] = True
    leak_receipts[0]["no_runtime_leak"] = False
    leak_receipts[0]["runtime_leak_details"] = [
        "adapter:produced_audio"]
    leak_trace = ta.assemble_rehearsal_trace(
        contract, consent, leak_receipts)
    leak_recheck = gr.recheck_rehearsal_trace(leak_trace)
    _check("F::recheck_catches_leak",
           leak_recheck.get("ok") is False)
    # Non-dict trace
    nd = gr.recheck_rehearsal_trace("notdict")
    _check("F::recheck_rejects_non_dict",
           nd.get("ok") is False)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "rc.json"
        p = gr.write_rehearsal_recheck(recheck, str(out))
        _check("F::write_path", Path(p).exists())


def suite_g_rehearsal_report() -> None:
    import bilingual_voice_phase39_rehearsal_report as rr
    import bilingual_voice_phase39_governance_recheck as gr
    import bilingual_voice_phase39_trace_assembler as ta
    import bilingual_voice_phase39_rehearsal_contract as rc
    import bilingual_voice_phase39_consent_orchestrator as co
    import bilingual_voice_phase39_stage_executor as se
    contract = rc.create_rehearsal_contract()
    consent = co.create_umbrella_consent(
        "operator_local",
        contract.get("scenario_count") or 0)
    receipts = [se.execute_scenario(s)
                 for s in contract.get("scenarios") or []]
    trace = ta.assemble_rehearsal_trace(
        contract, consent, receipts)
    recheck = gr.recheck_rehearsal_trace(trace)
    report = rr.bundle_rehearsal_report(
        contract, consent, trace, recheck)
    _check("G::report_is_dict", isinstance(report, dict))
    val = rr.validate_rehearsal_report(report)
    _check("G::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    _check("G::report_ok", report.get("ok") is True)
    pi = report.get("production_invariants_expected") or {}
    for k, v in (("english_words", 2814),
                  ("russian_words", 2518),
                  ("russian_phrases", 35),
                  ("bilingual_concepts", 26),
                  ("bilingual_entry_links", 52),
                  ("live_pack_manifests", 90)):
        _check(f"G::pi::{k}", pi.get(k) == v, str(pi.get(k)))
    forb = report.get("forbidden_runtime_actions") or []
    for k in ("generate_audio", "invoke_tts",
              "run_subprocess", "network_call"):
        _check(f"G::forbidden::{k}", k in forb)
    # Drift catch
    drift = dict(report)
    drift["production_invariants_expected"] = {
        "english_words": 9999}
    bad = rr.validate_rehearsal_report(drift)
    _check("G::validator_catches_drift",
           bad.get("ok") is False)
    md = rr.create_rehearsal_markdown(report)
    _check("G::md_nonempty", isinstance(md, str)
           and len(md) > 200)
    for needle in ("Receipts", "kill-switch", "recheck",
                    "metadata-only"):
        _check(f"G::md_contains::{needle}",
               needle in md, needle)
    with tempfile.TemporaryDirectory() as td:
        out_json = Path(td) / "rep.json"
        out_md = Path(td) / "rep.md"
        p1 = rr.write_rehearsal_report(
            report, str(out_json))
        p2 = rr.write_rehearsal_markdown(md, str(out_md))
        _check("G::json_written", Path(p1).exists())
        _check("G::md_written", Path(p2).exists())


def suite_h_runtime_orchestrator() -> None:
    import bilingual_voice_phase39_runtime as rt
    base = (_ROOT / "bilingual_stack" / "rehearsal_phase39")
    out = rt.run_phase39_rehearsal(
        operator_id="operator_local",
        output_dir=str(base),
        write_per_scenario_receipts=True)
    _check("H::status_ok", out.get("status") == "ok",
           str(out.get("reason")))
    val = rt.validate_phase39_runtime_output(out)
    _check("H::validate_ok", val.get("ok") is True,
           ",".join(val.get("reasons", [])))
    trace = out.get("trace") or {}
    _check("H::receipts_10",
           trace.get("receipt_count") == 10,
           str(trace.get("receipt_count")))
    _check("H::ok_count_at_least_8",
           (trace.get("ok_receipt_count") or 0) >= 8)
    _check("H::refused_at_least_1",
           (trace.get("refused_receipt_count") or 0) >= 1)
    _check("H::ks_blocked_1",
           (trace.get("kill_switch_blocked_count") or 0)
           == 1)
    dist = trace.get("adapter_distribution") or {}
    for a in ("dummy_metadata_adapter",
              "bilingual_segment_metadata_adapter",
              "prosody_density_metadata_adapter",
              "safety_redaction_trace_metadata_adapter"):
        _check(f"H::adapter_present_in_dist::{a}",
               dist.get(a, 0) >= 1, a)
    recheck = out.get("recheck") or {}
    _check("H::recheck_ok",
           recheck.get("ok") is True)
    paths = out.get("paths_written") or []
    _check("H::paths_written_nonempty",
           len(paths) >= 6, str(len(paths)))
    # Confirm written files exist
    for sub, fname in (
        ("contracts", "rehearsal_contract.json"),
        ("consents", "umbrella_consent.json"),
        ("traces", "rehearsal_trace.json"),
        ("recheck", "rehearsal_recheck.json"),
        ("reports", "rehearsal_report.json"),
        ("reports", "rehearsal_report.md"),
    ):
        p = base / sub / fname
        _check(f"H::written::{sub}/{fname}", p.exists())


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
                  / "rehearsal_phase39")
    if base.exists():
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.lower().endswith(
                        (".wav", ".mp3", ".ogg",
                         ".flac", ".m4a")):
                    audio.append(os.path.join(root, f))
    _check("I::no_audio_files_in_rehearsal_phase39",
           not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [f"{m}.py" for m in _PHASE39_MODULES]
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
    # Secret-leakage scan over rehearsal_phase39/ output dirs
    import bilingual_voice_phase36_secret_boundary as sb
    base = (_ROOT / "bilingual_stack"
                  / "rehearsal_phase39")
    scan_dirs = [
        base / "contracts", base / "consents",
        base / "stages", base / "traces",
        base / "recheck", base / "reports",
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
    upstream_runtimes = [
        "bilingual_voice_adapter_phase28_runtime",
        "bilingual_voice_adapter_phase29_runtime",
        "bilingual_voice_adapter_phase30_runtime",
        "bilingual_voice_adapter_phase31_runtime",
        "bilingual_voice_adapter_phase33_runtime",
        "bilingual_voice_phase34_export_runtime",
        "bilingual_voice_phase35_exchange_runtime",
        "bilingual_voice_phase36_handoff_runtime",
        "bilingual_voice_adapter_phase37_runtime",
    ]
    for m in upstream_runtimes:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reimport {m}: {e}")
        _check(f"K::reimport::{m}", ok)
    for m in _PHASE39_MODULES:
        try:
            mod = importlib.import_module(m)
            importlib.reload(mod)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"K::reload {m}: {e}")
        _check(f"K::reload::{m}", ok)
    # End-to-end: one more rehearsal run from scratch
    try:
        import bilingual_voice_phase39_runtime as rt
        out = rt.run_phase39_rehearsal(
            operator_id="operator_local",
            output_dir=None,
            write_per_scenario_receipts=False)
        _check("K::e2e_status_ok",
               out.get("status") == "ok")
        _check("K::e2e_recheck_ok",
               (out.get("recheck") or {}).get("ok") is True)
        _check("K::e2e_receipts_10",
               (out.get("trace") or {}).get("receipt_count")
               == 10)
    except Exception as e:  # noqa: BLE001
        _check("K::e2e_no_exception", False, str(e))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_rehearsal_contract),
        ("C", suite_c_consent_orchestrator),
        ("D", suite_d_stage_executor),
        ("E", suite_e_trace_assembler),
        ("F", suite_f_governance_recheck),
        ("G", suite_g_rehearsal_report),
        ("H", suite_h_runtime_orchestrator),
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
