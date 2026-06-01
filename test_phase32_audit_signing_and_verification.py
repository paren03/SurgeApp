"""Phase 32 test harness — audit chain signing + verification."""

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
        "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
        "test_phase31_multi_adapter_boundary.py",
        "bilingual_voice_phase31_adapter_interface.py",
        "bilingual_segment_metadata_adapter.py",
        "bilingual_voice_phase31_selection_policy.py",
        "bilingual_voice_phase31_adapter_comparison.py",
        "bilingual_voice_phase31_selection_receipt.py",
        "bilingual_voice_phase31_post_call_equivalence.py",
        "bilingual_voice_adapter_phase31_runtime.py",
        "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
        "bilingual_voice_callable_adapter_interface.py",
        "bilingual_voice_dummy_metadata_adapter.py",
        "bilingual_voice_invocation_receipt.py",
        "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
        "bilingual_voice_audit_chain.py",
        "bilingual_voice_invocation_consent.py",
        "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
        "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
        "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
        "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
    ]
    p32 = [
        "bilingual_voice_audit_signing_policy.py",
        "bilingual_voice_audit_chain_signer.py",
        "bilingual_voice_receipt_verifier.py",
        "bilingual_voice_evidence_bundle.py",
        "bilingual_voice_report_integrity_manifest.py",
        "bilingual_voice_governance_verifier.py",
        "bilingual_voice_verification_cli.py",
    ]
    for f in upstream + p32:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_audit_signing_policy",
        "bilingual_voice_audit_chain_signer",
        "bilingual_voice_receipt_verifier",
        "bilingual_voice_evidence_bundle",
        "bilingual_voice_report_integrity_manifest",
        "bilingual_voice_governance_verifier",
        "bilingual_voice_verification_cli",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_signing_policy() -> None:
    import bilingual_voice_audit_signing_policy as asp
    pol = asp.get_audit_signing_policy()
    _check("B::policy_version", isinstance(pol.get("version"), str))
    _check("B::hmac_sha256_supported",
           "HMAC-SHA256" in asp.get_supported_signature_algorithms())
    # Canonicalization deterministic
    a = asp.canonicalize_for_signing({"b": 1, "a": 2})
    b = asp.canonicalize_for_signing({"a": 2, "b": 1})
    _check("B::canon_deterministic", a == b)
    # Test key descriptor
    key = asp.create_test_signing_key("phase32_test_key_x")
    _check("B::test_key_valid",
           asp.validate_signing_key_descriptor(key)["ok"])
    # Non-test forbidden
    try:
        asp.create_test_signing_key("prod_key")
        _check("B::prod_key_rejected", False, "did not raise")
    except ValueError:
        _check("B::prod_key_rejected", True)
    # Forced non-test descriptor rejected
    bad = dict(key)
    bad["test_only"] = False
    _check("B::non_test_rejected",
           not asp.validate_signing_key_descriptor(bad)["ok"])
    # Unsupported algorithm rejected
    md = asp.create_signature_metadata(algorithm="RSA-PSS-X")
    md["algorithm"] = "RSA-PSS-X"
    _check("B::unsupported_alg_rejected",
           not asp.validate_signature_metadata(md)["ok"])


def suite_c_audit_chain_signer() -> None:
    import bilingual_voice_audit_signing_policy as asp
    import bilingual_voice_audit_chain as vac
    import bilingual_voice_audit_chain_signer as acs
    key = asp.create_test_signing_key("phase32_test_key_c")
    ev = vac.create_audit_chain_event("preflight", "ok", "hello",
                                       {"k": "v"})
    r = acs.sign_audit_chain_event(ev, key)
    _check("C::event_signs", r["ok"])
    signed = r["signed_event"]
    v = acs.verify_signed_audit_chain_event(signed, key)
    _check("C::signed_event_verifies", v["ok"], json.dumps(v))
    # Build a chain
    chain: list = []
    chain = vac.append_chain_event(chain, ev)
    for i in range(5):
        e = vac.create_audit_chain_event(
            "calltime_boundary", "ok", f"step{i}",
            previous_hash=chain[-1].get("event_hash") or "")
        chain = vac.append_chain_event(chain, e)
    rc = acs.sign_audit_chain(chain, key)
    _check("C::chain_signs", rc["ok"])
    sc = rc["signed_chain"]
    vc = acs.verify_signed_audit_chain(sc, key)
    _check("C::signed_chain_verifies", vc["ok"],
           json.dumps(vc.get("reasons")))
    # Tamper event metadata
    sc2 = [dict(x) for x in sc]
    sc2[2]["message"] = "tampered"
    _check("C::tamper_metadata_fails",
           not acs.verify_signed_audit_chain(sc2, key)["ok"])
    # Tamper previous_hash
    sc3 = [dict(x) for x in sc]
    sc3[3]["previous_hash"] = "deadbeef"
    _check("C::tamper_prev_hash_fails",
           not acs.verify_signed_audit_chain(sc3, key)["ok"])
    # Tamper event_hash
    sc4 = [dict(x) for x in sc]
    sc4[1]["event_hash"] = "f" * 64
    _check("C::tamper_event_hash_fails",
           not acs.verify_signed_audit_chain(sc4, key)["ok"])
    # Tamper signature
    sc5 = [dict(x) for x in sc]
    sc5[4]["signature"] = "a" * 64
    _check("C::tamper_signature_fails",
           not acs.verify_signed_audit_chain(sc5, key)["ok"])
    # detect_signed_chain_tampering
    t = acs.detect_signed_chain_tampering(sc5, key)
    _check("C::detect_returns_tampered", t["tampered"] is True)
    # Read bounded
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "c.json")
        out = acs.write_signed_audit_chain(sc, p)
        _check("C::wrote_chain", os.path.exists(out))
        re_chain = acs.read_signed_audit_chain(p, limit=3)
        _check("C::read_bounded", len(re_chain) <= 3)


def suite_d_receipt_verifier() -> None:
    import bilingual_voice_receipt_verifier as rv
    # Phase 30 invocation receipt
    inv = {
        "receipt_id": "recv_x",
        "adapter_name": "dummy_metadata_adapter",
        "adapter_type": "dummy_metadata_adapter",
        "request_id": "creq_x",
        "result_id": "dres_x",
        "operator_id_hash": "deadbeef" * 8,
        "dry_run": True, "test_only": True,
        "execution_boundary_preserved": True,
        "audio_generated": False, "tts_invoked": False,
        "subprocess_used": False, "network_used": False,
        "files_written": False,
        "pre_call_status": "ok", "post_call_status": "ok",
        "audit_chain_hash": "abc", "notes": "x",
        "created_at": 1.0, "phase": "phase30.receipt.v1",
    }
    _check("D::invocation_ok", rv.verify_invocation_receipt(inv)["ok"])
    # Phase 31 selection receipt
    sel = {
        "receipt_id": "selrec_x", "created_at": 1.0,
        "selected_adapter_name": "bilingual_segment_metadata_adapter",
        "selected_adapter_type": "bilingual_segment_metadata_adapter",
        "candidate_adapters": [
            "dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter"],
        "selection_reason": "x", "score_summary": {},
        "request_id": "p31req_x", "result_id": "bsres_x",
        "dry_run": True, "test_only": True,
        "execution_boundary_preserved": True,
        "audio_generated": False, "tts_invoked": False,
        "subprocess_used": False, "network_used": False,
        "files_written": False, "audit_chain_hash": "abc",
        "notes": "x", "phase": "phase31.selection_receipt.v1",
    }
    _check("D::selection_ok",
           rv.verify_selection_receipt(sel)["ok"])
    # Adapter result matches receipt
    res = {"result_id": "dres_x", "adapter_name": "dummy_metadata_adapter",
           "produced_audio": False, "invoked_tts": False,
           "used_subprocess": False, "used_network": False,
           "wrote_files": False}
    _check("D::result_matches",
           rv.verify_adapter_result_against_receipt(res, inv)["ok"])
    # Raw operator_id rejected
    bad = dict(inv)
    bad["operator_id"] = "raw"
    _check("D::raw_operator_rejected",
           not rv.verify_invocation_receipt(bad)["ok"])
    # audio_generated=True rejected
    bad2 = dict(inv)
    bad2["audio_generated"] = True
    _check("D::audio_rejected",
           not rv.verify_invocation_receipt(bad2)["ok"])
    # tts_invoked=True rejected
    bad3 = dict(inv)
    bad3["tts_invoked"] = True
    _check("D::tts_rejected",
           not rv.verify_invocation_receipt(bad3)["ok"])
    # subprocess_used=True rejected
    bad4 = dict(inv)
    bad4["subprocess_used"] = True
    _check("D::sub_rejected",
           not rv.verify_invocation_receipt(bad4)["ok"])
    # Unknown adapter rejected
    bad5 = dict(inv)
    bad5["adapter_name"] = "real_piper"
    _check("D::unknown_adapter_rejected",
           not rv.verify_invocation_receipt(bad5)["ok"])
    summary = rv.summarize_receipt_verification(
        [{"ok": True, "reasons": []},
         {"ok": False, "reasons": ["x"]}])
    _check("D::summary_total",
           summary["total"] == 2 and summary["failed"] == 1)


def suite_e_evidence_bundle() -> None:
    import bilingual_voice_evidence_bundle as veb
    import bilingual_voice_audit_signing_policy as asp
    import bilingual_voice_audit_chain as vac
    import bilingual_voice_audit_chain_signer as acs
    chain = vac.append_chain_event(
        [], vac.create_audit_chain_event("preflight", "ok", "x"))
    inv = {"receipt_id": "recv_x",
           "adapter_name": "dummy_metadata_adapter",
           "adapter_type": "dummy_metadata_adapter",
           "operator_id_hash": "a" * 16,
           "dry_run": True, "test_only": True,
           "execution_boundary_preserved": True,
           "audio_generated": False, "tts_invoked": False,
           "subprocess_used": False, "network_used": False,
           "files_written": False,
           "request_id": "x", "result_id": "y",
           "pre_call_status": "ok", "post_call_status": "ok",
           "audit_chain_hash": "abc", "notes": "x",
           "created_at": 1.0, "phase": "p"}
    bundle = veb.create_evidence_bundle(
        "bundle_x", audit_chain=chain, invocation_receipt=inv,
        reports=[{"path": "/x", "sha256": "0" * 64}])
    v = veb.validate_evidence_bundle(bundle)
    _check("E::bundle_validates", v["ok"], json.dumps(v))
    summary = veb.summarize_evidence_bundle(bundle)
    _check("E::summary_string",
           isinstance(summary.get("summary"), str))
    # Write/read
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "b.json")
        out = veb.write_evidence_bundle(bundle, p)
        _check("E::writes", os.path.exists(out))
        re = veb.read_evidence_bundle(p)
        _check("E::reads_matches",
               re.get("bundle_id") == bundle["bundle_id"])
    # Verification path
    key = asp.create_test_signing_key("phase32_test_key_e")
    vres = veb.verify_evidence_bundle(bundle, key)
    _check("E::bundle_verifies", vres["ok"])
    # Audio field rejected
    bad = dict(bundle)
    bad["audio_bytes"] = "fake"
    _check("E::audio_field_rejected",
           not veb.validate_evidence_bundle(bad)["ok"])
    # Command field rejected
    bad2 = dict(bundle)
    bad2["command"] = "tts -q"
    _check("E::command_field_rejected",
           not veb.validate_evidence_bundle(bad2)["ok"])
    # Execution boundary violation rejected
    bad3 = dict(bundle)
    bad3["boundary_summary"] = dict(bundle["boundary_summary"])
    bad3["boundary_summary"]["execution_blocked"] = False
    _check("E::execution_violation_rejected",
           not veb.validate_evidence_bundle(bad3)["ok"])
    # Report hashes included
    _check("E::report_hashes_included",
           isinstance(bundle["report_hashes"], list)
           and len(bundle["report_hashes"]) == 1)


def suite_f_integrity_manifest() -> None:
    import bilingual_voice_report_integrity_manifest as rim
    # Streaming hash
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tf:
        tf.write(b"hello world")
        tmp_path = tf.name
    try:
        r = rim.compute_file_sha256(tmp_path)
        _check("F::sha256_computed", r["ok"]
               and len(r["sha256"]) == 64)
    finally:
        os.unlink(tmp_path)
    # Manifest over Phase 27-31 reports
    candidates = [
        str(_ROOT / "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md"),
        str(_ROOT / "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md"),
        str(_ROOT / "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md"),
        str(_ROOT / "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md"),
        str(_ROOT / "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md"),
    ]
    manifest = rim.create_report_integrity_manifest(
        candidates, manifest_id="test")
    _check("F::manifest_validates",
           rim.validate_report_integrity_manifest(manifest)["ok"])
    vres = rim.verify_report_integrity_manifest(manifest)
    _check("F::manifest_verifies", vres["ok"], json.dumps(vres))
    # Missing file reported
    m2 = rim.create_report_integrity_manifest(
        ["/nonexistent/path.md"])
    _check("F::missing_reported",
           len(m2["skipped"]) >= 1
           and m2["skipped"][0]["reason"] in
           ("file_not_found", "excluded_pattern"))
    # Runtime DB excluded
    m3 = rim.create_report_integrity_manifest(
        ["lexicon/luna_vocabulary.sqlite",
         "ruvector.db", "corpus_sources/backups/x.sqlite"])
    _check("F::runtime_db_excluded",
           all(s["reason"] == "excluded_pattern"
               for s in m3["skipped"]))


def suite_g_governance() -> None:
    import bilingual_voice_governance_verifier as gv
    p30s = gv.verify_phase30_strictness()
    _check("G::phase30_strict", p30s["ok"]
           and p30s["allowed_adapter_types"] ==
           ["dummy_metadata_adapter"])
    p31t = gv.verify_phase31_two_adapter_boundary()
    _check("G::phase31_two", p31t["ok"]
           and p31t["allowed_adapter_types"] ==
           ["dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter"])
    # Allowed adapters only
    good = [{"adapter_name": "dummy_metadata_adapter"},
            {"selected_adapter_name":
             "bilingual_segment_metadata_adapter"}]
    _check("G::allowed_pass",
           gv.verify_allowed_adapters_only(good)["ok"])
    bad = [{"adapter_name": "real_piper"}]
    _check("G::real_rejected",
           not gv.verify_allowed_adapters_only(bad)["ok"])
    # Audio + execution boundary in this very test file should be ok
    # (we use safe synonyms / quoted forbidden tokens)
    test_paths = [str(_ROOT / "bilingual_voice_audit_signing_policy.py"),
                  str(_ROOT / "bilingual_voice_audit_chain_signer.py")]
    _check("G::no_audio_in_modules",
           gv.verify_no_audio_boundary_in_artifacts(test_paths)["ok"])
    _check("G::no_execution_in_modules",
           gv.verify_no_execution_boundary_in_artifacts(test_paths)["ok"])
    with tempfile.TemporaryDirectory() as td:
        out = gv.write_governance_verification_report(
            {"p30": p30s, "p31": p31t}, os.path.join(td, "g.json"))
        _check("G::report_writes", os.path.exists(out))


def suite_h_cli() -> None:
    import bilingual_voice_verification_cli as cli
    import bilingual_voice_audit_chain as vac
    import bilingual_voice_audit_chain_signer as acs
    import bilingual_voice_audit_signing_policy as asp
    import bilingual_voice_evidence_bundle as veb
    import bilingual_voice_report_integrity_manifest as rim
    # Pre-register the CLI default key so verify-chain / verify-bundle
    # use the same material as we use to sign.
    key_label = "phase32_test_key"
    key = asp.create_test_signing_key(key_label)
    cli._KEY_REGISTRY[key_label] = key  # type: ignore  # noqa: SLF001
    # Build signed chain + bundle + manifest in temp
    with tempfile.TemporaryDirectory() as td:
        chain = vac.append_chain_event(
            [], vac.create_audit_chain_event("preflight", "ok"))
        sc = acs.sign_audit_chain(chain, key)["signed_chain"]
        chain_path = os.path.join(td, "chain.json")
        acs.write_signed_audit_chain(sc, chain_path)
        # verify-chain
        rc = cli.main(["verify-chain", chain_path])
        _check("H::verify_chain", rc.get("ok") and
               rc.get("verification", {}).get("ok"))
        # verify-bundle
        bundle = veb.create_evidence_bundle(
            "b1", audit_chain=sc,
            invocation_receipt={
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
                "created_at": 1.0, "phase": "p"})
        bp = os.path.join(td, "b.json")
        veb.write_evidence_bundle(bundle, bp)
        rb = cli.main(["verify-bundle", bp])
        _check("H::verify_bundle", rb.get("ok") and
               rb.get("verification", {}).get("ok"))
        # verify-manifest
        m = rim.create_report_integrity_manifest(
            [str(_ROOT / "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md")])
        mp = os.path.join(td, "m.json")
        rim.write_report_integrity_manifest(m, mp)
        rm = cli.main(["verify-manifest", mp])
        _check("H::verify_manifest", rm.get("ok") and
               rm.get("verification", {}).get("ok"))
        # verify-governance
        rg = cli.main(["verify-governance"])
        _check("H::verify_governance",
               rg.get("phase30_strict", {}).get("ok") is True
               and rg.get("phase31_two_adapter", {}).get(
                   "ok") is True)
        # run-suite
        rs = cli.main(["run-suite", td])
        _check("H::run_suite",
               isinstance(rs.get("governance"), dict))
        # No-arg returns supported list
        nores = cli.main([])
        _check("H::no_command_returns_supported",
               nores.get("ok") is False
               and "verify-bundle" in nores.get("supported", []))
        # Unknown command
        unk = cli.main(["banana"])
        _check("H::unknown_command",
               unk.get("ok") is False
               and "unknown_command" in str(unk.get("reason")))


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
                                       "voice_adapter_phase32"):
        for f in files:
            if f.lower().endswith((".wav", ".mp3", ".ogg",
                                    ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files", not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_audit_signing_policy.py",
        "bilingual_voice_audit_chain_signer.py",
        "bilingual_voice_receipt_verifier.py",
        "bilingual_voice_evidence_bundle.py",
        "bilingual_voice_report_integrity_manifest.py",
        "bilingual_voice_governance_verifier.py",
        "bilingual_voice_verification_cli.py",
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
        _check(f"J::{fn}::no_sys_exit",
               "sys.exit(" not in src)


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_signing_policy),
        ("C", suite_c_audit_chain_signer),
        ("D", suite_d_receipt_verifier),
        ("E", suite_e_evidence_bundle),
        ("F", suite_f_integrity_manifest),
        ("G", suite_g_governance),
        ("H", suite_h_cli),
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
