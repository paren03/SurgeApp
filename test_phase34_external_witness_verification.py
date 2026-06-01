"""Phase 34 test harness — external witness verification."""

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
        "PHASE33_THREE_ADAPTER_SIGNED_GOVERNANCE_REPORT.md",
        "test_phase33_three_adapter_signed_governance.py",
        "bilingual_voice_phase33_adapter_interface.py",
        "bilingual_prosody_density_metadata_adapter.py",
        "bilingual_voice_phase33_selection_policy.py",
        "bilingual_voice_phase33_signed_evidence.py",
        "bilingual_voice_phase33_governance_recheck.py",
        "bilingual_voice_phase33_result_verifier.py",
        "bilingual_voice_adapter_phase33_runtime.py",
        "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
        "bilingual_voice_audit_signing_policy.py",
        "bilingual_voice_audit_chain_signer.py",
        "bilingual_voice_receipt_verifier.py",
        "bilingual_voice_report_integrity_manifest.py",
        "bilingual_voice_evidence_bundle.py",
        "bilingual_voice_governance_verifier.py",
        "bilingual_voice_verification_cli.py",
        "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
        "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
        "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
        "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
        "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
        "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
        "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
    ]
    p34 = [
        "bilingual_voice_phase34_witness_package.py",
        "bilingual_voice_phase34_offline_verifier.py",
        "bilingual_voice_phase34_key_descriptor_export.py",
        "bilingual_voice_phase34_operator_guide.py",
        "bilingual_voice_phase34_witness_receipt.py",
        "bilingual_voice_phase34_export_runtime.py",
        "bilingual_voice_phase34_witness_governance.py",
    ]
    for f in upstream + p34:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_phase34_witness_package",
        "bilingual_voice_phase34_offline_verifier",
        "bilingual_voice_phase34_key_descriptor_export",
        "bilingual_voice_phase34_operator_guide",
        "bilingual_voice_phase34_witness_receipt",
        "bilingual_voice_phase34_export_runtime",
        "bilingual_voice_phase34_witness_governance",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def _build_pkg():
    import bilingual_voice_phase34_witness_package as wp
    import bilingual_voice_phase34_key_descriptor_export as kde
    import bilingual_voice_audit_signing_policy as asp
    import bilingual_voice_audit_chain as vac
    import bilingual_voice_audit_chain_signer as acs
    import bilingual_voice_phase33_signed_evidence as p33sev
    key = asp.create_test_signing_key("phase34_test_key")
    chain = vac.append_chain_event(
        [], vac.create_audit_chain_event("preflight", "ok", "x"))
    inv = {"receipt_id": "recv_x",
           "adapter_name": "dummy_metadata_adapter",
           "adapter_type": "dummy_metadata_adapter",
           "operator_id_hash": "a"*16, "dry_run": True,
           "test_only": True,
           "execution_boundary_preserved": True,
           "audio_generated": False, "tts_invoked": False,
           "subprocess_used": False, "network_used": False,
           "files_written": False, "request_id": "x",
           "result_id": "dres_x", "pre_call_status": "ok",
           "post_call_status": "ok",
           "audit_chain_hash": "abc", "notes": "x",
           "created_at": 1.0, "phase": "p"}
    res = {"result_id": "dres_x",
           "adapter_name": "dummy_metadata_adapter",
           "produced_audio": False, "invoked_tts": False,
           "used_subprocess": False, "used_network": False,
           "wrote_files": False}
    sev = p33sev.create_phase33_signed_evidence({
        "audit_chain": chain,
        "invocation_receipt": inv,
        "selection_receipt": {},
        "selected_adapter_result": res,
        "status": "ok"}, key)
    pkg = wp.create_witness_package(
        "pkg_test", signed_evidence=sev,
        integrity_manifest={
            "manifest_id": "x", "created_at": 1.0,
            "phase": "phase32.integrity_manifest.v1",
            "entries": [], "skipped": [],
            "entry_count": 0, "skipped_count": 0},
        governance_report={})
    pub = kde.create_public_test_key_descriptor(key)
    pkg["key_descriptor_public"] = pub
    return key, pkg, pub


def suite_b_witness_package() -> None:
    import bilingual_voice_phase34_witness_package as wp
    key, pkg, pub = _build_pkg()
    _check("B::package_validates",
           wp.validate_witness_package(pkg)["ok"])
    with tempfile.TemporaryDirectory() as td:
        out = wp.write_witness_package(
            pkg, os.path.join(td, "p.json"))
        _check("B::writes", os.path.exists(out))
        re = wp.read_witness_package(out)
        _check("B::reads_matches",
               re.get("package_id") == pkg["package_id"])
    # Strip secrets
    polluted = dict(pkg)
    polluted["secret"] = "x"
    polluted["private_key"] = "y"
    stripped = wp.strip_witness_package_secrets(polluted)
    _check("B::strip_removes_secrets",
           "secret" not in stripped and
           "private_key" not in stripped)
    # Secret field at top level rejected
    bad = dict(pkg)
    bad["material_hex"] = "aa"
    _check("B::secret_rejected",
           not wp.validate_witness_package(bad)["ok"])
    # Audio field rejected
    bad2 = dict(pkg)
    bad2["audio_bytes"] = "fake"
    _check("B::audio_rejected",
           not wp.validate_witness_package(bad2)["ok"])
    # Command field rejected
    bad3 = dict(pkg)
    bad3["command"] = "tts speak"
    _check("B::command_rejected",
           not wp.validate_witness_package(bad3)["ok"])
    # Boundary violation rejected
    bad4 = dict(pkg)
    bad4["boundary_summary"] = dict(pkg["boundary_summary"])
    bad4["boundary_summary"]["execution_blocked"] = False
    _check("B::boundary_violation_rejected",
           not wp.validate_witness_package(bad4)["ok"])


def suite_c_offline_verifier() -> None:
    import bilingual_voice_phase34_offline_verifier as ov
    key, pkg, pub = _build_pkg()
    res = ov.verify_witness_package(pkg, pub)
    # Note: verify uses PUBLIC descriptor which has no material;
    # signed evidence verification expects PRIVATE key. So we expect
    # the chain step to FAIL when given public key only. The
    # composed result is therefore "fail" — that's a real boundary.
    # For HMAC the verifier needs the actual material. So use the
    # private key for chain verification.
    res2 = ov.verify_witness_package(pkg, key)
    _check("C::valid_package_verifies",
           res2["status"] == "pass",
           json.dumps(res2.get("checks_failed")))
    # Tampered signed evidence
    bad_pkg = json.loads(json.dumps(pkg))
    bad_pkg["signed_evidence_payload"]["signed_audit_chain"][0][
        "message"] = "tampered"
    res_t = ov.verify_witness_package(bad_pkg, key)
    _check("C::tampered_fails", res_t["status"] == "fail")
    # Hash mismatch in integrity manifest -- forge entry
    bad_pkg2 = json.loads(json.dumps(pkg))
    bad_pkg2["report_integrity_manifest"]["entries"] = [{
        "path": "/nonexistent_file.md", "sha256": "0" * 64,
        "size_bytes": 1}]
    res_m = ov.verify_witness_package(bad_pkg2, key)
    _check("C::manifest_mismatch_fails",
           res_m["status"] == "fail")
    # Boundary violation
    bad_pkg3 = json.loads(json.dumps(pkg))
    bad_pkg3["boundary_summary"]["execution_blocked"] = False
    res_b = ov.verify_witness_package(bad_pkg3, key)
    _check("C::boundary_violation_fails",
           res_b["status"] == "fail")
    # Secret leakage
    bad_pkg4 = json.loads(json.dumps(pkg))
    bad_pkg4["secret"] = "x"
    res_s = ov.verify_witness_package(bad_pkg4, key)
    _check("C::secret_leak_fails", res_s["status"] == "fail")
    # Structured pass/fail/warn
    _check("C::structured",
           isinstance(res2.get("checks_passed"), list)
           and isinstance(res2.get("checks_failed"), list)
           and isinstance(res2.get("checks_warned"), list))


def suite_d_key_descriptor_export() -> None:
    import bilingual_voice_phase34_key_descriptor_export as kde
    import bilingual_voice_audit_signing_policy as asp
    key = asp.create_test_signing_key("phase34_test_key_d")
    pub = kde.create_public_test_key_descriptor(key)
    _check("D::public_validates",
           kde.validate_public_key_descriptor(pub)["ok"])
    # Secret material absent
    for k in ("private_key", "secret", "material_hex",
              "signing_key_material"):
        _check(f"D::no_secret:{k}", k not in pub)
    # Strip from polluted descriptor
    polluted = dict(key)
    polluted["secret"] = "x"
    stripped = kde.strip_key_secret_material(polluted)
    _check("D::strip_removes",
           "secret" not in stripped and
           "material_hex" not in stripped)
    # Refuse to write secret material
    with tempfile.TemporaryDirectory() as td:
        try:
            kde.write_public_key_descriptor(
                key, os.path.join(td, "p.json"))
            _check("D::write_refused_secret", False,
                   "should have refused")
        except ValueError:
            _check("D::write_refused_secret", True)
        out = kde.write_public_key_descriptor(
            pub, os.path.join(td, "pub.json"))
        _check("D::write_public_ok", os.path.exists(out))
        re = kde.read_public_key_descriptor(out)
        _check("D::read_matches", re.get("key_id") == pub["key_id"])
    # Identity comparison
    cmp_ok = kde.compare_key_descriptor_identity(key, pub)
    _check("D::identity_match", cmp_ok["ok"])
    # Production / live / etc rejected
    for bad in ("prod_key", "production_main", "live_key",
                "real_key", "kms_key", "cloud_key", "external_key"):
        try:
            asp.create_test_signing_key(bad)
            _check(f"D::reject_label:{bad}", False)
        except ValueError:
            _check(f"D::reject_label:{bad}", True)


def suite_e_operator_guide() -> None:
    import bilingual_voice_phase34_operator_guide as og
    guide = og.create_operator_verification_guide()
    _check("E::guide_id", isinstance(guide.get("guide_id"), str))
    _check("E::steps_present",
           isinstance(guide.get("step_by_step", {}).get(
               "steps"), list)
           and len(guide["step_by_step"]["steps"]) >= 5)
    _check("E::boundary_explanation",
           isinstance(guide.get("boundary_explanation"), dict))
    _check("E::failure_interpretation",
           isinstance(guide.get("failure_interpretation"), dict))
    text = json.dumps(guide).lower()
    _check("E::states_not_real_voice",
           "not real voice execution" in text)
    _check("E::states_phase21_separate",
           "phase 21" in text and "separate" in text)
    with tempfile.TemporaryDirectory() as td:
        out = og.write_operator_verification_guide(
            guide, os.path.join(td, "g.json"))
        _check("E::writes", os.path.exists(out))


def suite_f_witness_receipt() -> None:
    import bilingual_voice_phase34_witness_receipt as wr
    import bilingual_voice_phase34_offline_verifier as ov
    key, pkg, pub = _build_pkg()
    result = ov.verify_witness_package(pkg, key)
    rec = wr.create_witness_verification_receipt(
        result, pkg["package_id"])
    _check("F::receipt_validates",
           wr.validate_witness_verification_receipt(rec)["ok"])
    _check("F::evidence_hash",
           isinstance(rec.get("evidence_hash"), str)
           and len(rec["evidence_hash"]) == 64)
    _check("F::boundary_preserved",
           rec.get("boundary_preserved") is True)
    _check("F::secrets_absent",
           rec.get("secrets_absent") is True)
    _check("F::audio_absent",
           rec.get("audio_absent") is True)
    _check("F::execution_absent",
           rec.get("execution_absent") is True)
    for k in ("audio_bytes", "command", "subprocess",
              "transcript", "secret", "private_key"):
        _check(f"F::no_field:{k}", k not in rec)


def suite_g_export_runtime() -> None:
    import bilingual_voice_phase34_export_runtime as er
    r_en = er.create_phase34_witness_export(
        user_text="hello luna",
        operator_id="operator_local", approve=True)
    _check("G::en_status_ok", r_en["status"] == "ok",
           json.dumps(r_en.get(
               "offline_verification_result", {}).get(
                   "checks_failed")))
    pub = r_en.get("public_key_descriptor") or {}
    for k in ("private_key", "secret", "material_hex",
              "signing_key_material"):
        _check(f"G::no_secret_pub:{k}", k not in pub)
    _check("G::ofv_pass",
           (r_en.get("offline_verification_result") or {}).get(
               "status") == "pass")
    rec = r_en.get("witness_receipt") or {}
    import bilingual_voice_phase34_witness_receipt as wr
    _check("G::receipt_validates",
           wr.validate_witness_verification_receipt(rec)["ok"])
    # Russian / mixed
    r_ru = er.create_phase34_witness_export(
        user_text="привет луна",
        operator_id="operator_local", approve=True)
    _check("G::ru_status_ok", r_ru["status"] == "ok")
    r_mix = er.create_phase34_witness_export(
        user_text="mix russian and english",
        operator_id="operator_local", approve=True)
    _check("G::mix_status_ok", r_mix["status"] == "ok")
    # Operator bundle
    with tempfile.TemporaryDirectory() as td:
        bundle = er.create_phase34_operator_bundle(
            output_dir=td, include_demo=True)
        _check("G::bundle_guide",
               "guide_path" in bundle and
               os.path.exists(bundle["guide_path"]))
        _check("G::bundle_demo",
               bundle.get("demo_status") == "ok"
               and os.path.exists(bundle.get(
                   "witness_package_path", "")))
    # Demo bounded
    demo = er.demo_phase34_witness_exports(limit=3)
    _check("G::demo_bounded", demo["count"] == 3)
    # sign_evidence=False on a successful Phase 33 invocation
    # produces no signed_evidence, which makes our witness package
    # signed_evidence_payload empty and verification falls to fail.
    r_no_sig = er.create_phase34_witness_export(
        user_text="hello", operator_id="operator_local",
        approve=True, sign_evidence=False)
    _check("G::sign_false_fails",
           r_no_sig["status"] != "ok")


def suite_h_witness_governance() -> None:
    import bilingual_voice_phase34_witness_governance as wg
    _check("H::phase30_strict",
           wg.verify_phase34_phase30_strictness()["ok"])
    _check("H::phase31_boundary",
           wg.verify_phase34_phase31_boundary()["ok"])
    _check("H::phase33_boundary",
           wg.verify_phase34_phase33_boundary()["ok"])
    key, pkg, pub = _build_pkg()
    _check("H::adapter_legality_ok",
           wg.verify_phase34_package_adapter_legality(pkg)["ok"])
    _check("H::no_runtime_exec_ok",
           wg.verify_phase34_package_no_runtime_execution(pkg)["ok"])
    _check("H::no_secret_material_ok",
           wg.verify_phase34_package_no_secret_material(pkg)["ok"])
    _check("H::no_audio_material_ok",
           wg.verify_phase34_package_no_audio_material(pkg)["ok"])
    # Inject illegal adapter token in a value
    bad = json.loads(json.dumps(pkg))
    bad["governance_summary"] = {"misc": "real_piper"}
    _check("H::illegal_adapter_value_detected",
           not wg.verify_phase34_package_adapter_legality(bad)["ok"])
    # Inject secret field
    bad2 = json.loads(json.dumps(pkg))
    bad2["material_hex"] = "aa"
    _check("H::secret_present_detected",
           not wg.verify_phase34_package_no_secret_material(
               bad2)["ok"])
    # Inject audio key
    bad3 = json.loads(json.dumps(pkg))
    bad3["audio_bytes"] = "x"
    _check("H::audio_present_detected",
           not wg.verify_phase34_package_no_audio_material(
               bad3)["ok"])
    # Inject exec key
    bad4 = json.loads(json.dumps(pkg))
    bad4["command"] = "x"
    _check("H::exec_present_detected",
           not wg.verify_phase34_package_no_runtime_execution(
               bad4)["ok"])


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
        _check("I::concepts_26", nc == 26)
        _check("I::links_52", nl == 52)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"), recursive=True)
        if "backups" not in p]
    _check("I::manifests_90", len(live) == 90, str(len(live)))
    audio = []
    for root, _dirs, files in os.walk(_ROOT / "bilingual_stack" /
                                       "voice_adapter_phase34"):
        for f in files:
            if f.lower().endswith((".wav", ".mp3", ".ogg",
                                    ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files", not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_phase34_witness_package.py",
        "bilingual_voice_phase34_offline_verifier.py",
        "bilingual_voice_phase34_key_descriptor_export.py",
        "bilingual_voice_phase34_operator_guide.py",
        "bilingual_voice_phase34_witness_receipt.py",
        "bilingual_voice_phase34_export_runtime.py",
        "bilingual_voice_phase34_witness_governance.py",
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


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_witness_package),
        ("C", suite_c_offline_verifier),
        ("D", suite_d_key_descriptor_export),
        ("E", suite_e_operator_guide),
        ("F", suite_f_witness_receipt),
        ("G", suite_g_export_runtime),
        ("H", suite_h_witness_governance),
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
