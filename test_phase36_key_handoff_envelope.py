"""Phase 36 test harness — out-of-band key handoff envelope."""

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
        "PHASE35_WITNESS_EXCHANGE_PROTOCOL_REPORT.md",
        "test_phase35_witness_exchange_protocol.py",
        "bilingual_voice_phase35_exchange_contract.py",
        "bilingual_voice_phase35_exporter_packet.py",
        "bilingual_voice_phase35_witness_input.py",
        "bilingual_voice_phase35_witness_verifier.py",
        "bilingual_voice_phase35_handshake_record.py",
        "bilingual_voice_phase35_operator_exchange_guide.py",
        "bilingual_voice_phase35_exchange_runtime.py",
        "PHASE34_EXTERNAL_WITNESS_VERIFICATION_REPORT.md",
        "bilingual_voice_phase34_key_descriptor_export.py",
        "bilingual_voice_phase34_offline_verifier.py",
        "bilingual_voice_phase34_witness_package.py",
        "bilingual_voice_phase34_export_runtime.py",
        "PHASE33_THREE_ADAPTER_SIGNED_GOVERNANCE_REPORT.md",
        "bilingual_voice_phase33_signed_evidence.py",
        "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
        "bilingual_voice_audit_signing_policy.py",
        "bilingual_voice_audit_chain_signer.py",
        "bilingual_voice_evidence_bundle.py",
        "bilingual_voice_verification_cli.py",
        "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
        "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
        "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
        "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
        "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
        "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
        "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
    ]
    p36 = [
        "bilingual_voice_phase36_handoff_contract.py",
        "bilingual_voice_phase36_key_handoff_envelope.py",
        "bilingual_voice_phase36_secret_boundary.py",
        "bilingual_voice_phase36_public_descriptor_bridge.py",
        "bilingual_voice_phase36_handoff_verifier.py",
        "bilingual_voice_phase36_operator_guide.py",
        "bilingual_voice_phase36_handoff_runtime.py",
    ]
    for f in upstream + p36:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_phase36_handoff_contract",
        "bilingual_voice_phase36_key_handoff_envelope",
        "bilingual_voice_phase36_secret_boundary",
        "bilingual_voice_phase36_public_descriptor_bridge",
        "bilingual_voice_phase36_handoff_verifier",
        "bilingual_voice_phase36_operator_guide",
        "bilingual_voice_phase36_handoff_runtime",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_contract() -> None:
    import bilingual_voice_phase36_handoff_contract as hc
    s = hc.get_phase36_handoff_contract_schema()
    _check("B::schema_version",
           isinstance(s.get("version"), str))
    c = hc.create_handoff_contract()
    _check("B::contract_validates",
           hc.validate_handoff_contract(c)["ok"],
           json.dumps(hc.validate_handoff_contract(c)))
    _check("B::consent_required",
           c.get("consent_required") is True)
    _check("B::consent_marker_required",
           c.get("consent_marker_required") is True)
    sbar = c.get("secret_bearing_artifact_policy") or {}
    _check("B::secret_bearing_policy",
           sbar.get("must_be_gitignored") is True
           and sbar.get("must_be_test_only") is True)
    pap = c.get("public_artifact_policy") or {}
    _check("B::public_artifact_policy",
           pap.get("secret_leakage_check_required") is True)
    gip = c.get("gitignore_policy") or {}
    _check("B::gitignore_policy",
           gip.get("must_be_gitignored") is True)
    _check("B::no_network",
           c["no_network_policy"]["internet_disabled"] is True)
    _check("B::no_subprocess",
           c["no_subprocess_policy"]["subprocess_disabled"] is True)
    _check("B::no_audio",
           c["no_audio_policy"]["audio_fields_rejected"] is True)
    _check("B::no_production_key",
           c["no_production_key_policy"]["test_only_required"]
               is True)


def suite_c_envelope() -> None:
    import bilingual_voice_phase36_key_handoff_envelope as henv
    import bilingual_voice_audit_signing_policy as asp
    # Missing consent
    r1 = henv.create_key_handoff_envelope(
        asp.create_test_signing_key("phase36_c1"),
        consent_marker="")
    _check("C::missing_consent_rejected",
           "ok" in r1 and r1.get("ok") is False)
    # Valid
    key = asp.create_test_signing_key("phase36_c2")
    env = henv.create_key_handoff_envelope(
        key, consent_marker="op_consent_alpha")
    _check("C::envelope_validates",
           henv.validate_key_handoff_envelope(env)["ok"])
    # Non-test key rejected
    bad_key = dict(key)
    bad_key["test_only"] = False
    r2 = henv.create_key_handoff_envelope(
        bad_key, consent_marker="op_consent_beta")
    _check("C::non_test_rejected",
           r2.get("ok") is False)
    # Production / live / etc rejected by Phase 32 label rule
    for bad_label in ("prod_main", "production_main", "live_main",
                       "real_main", "kms_main", "cloud_main",
                       "external_main"):
        try:
            asp.create_test_signing_key(bad_label)
            _check(f"C::label_rejected:{bad_label}", False,
                   "did not raise")
        except ValueError:
            _check(f"C::label_rejected:{bad_label}", True)
    # Seal/unseal correct
    sealed = henv.seal_key_handoff_envelope(env)
    un = henv.unseal_key_handoff_envelope(
        sealed, consent_marker="op_consent_alpha")
    _check("C::unseal_correct", un["ok"]
           and un["key_descriptor"]["material_hex"]
               == key["material_hex"])
    # Wrong consent
    un_bad = henv.unseal_key_handoff_envelope(
        sealed, consent_marker="WRONG")
    _check("C::wrong_consent_fails", un_bad["ok"] is False)
    # Consent stored only as hash
    _check("C::consent_hash_stored",
           isinstance(env.get("consent_marker_hash"), str)
           and len(env["consent_marker_hash"]) == 64
           and "op_consent_alpha" not in
               env["consent_marker_hash"])
    # Write without allow_secret_write refused
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "env.json")
        try:
            henv.write_key_handoff_envelope(env, path,
                                             allow_secret_write=False)
            _check("C::write_without_flag_refused", False,
                   "did not raise")
        except ValueError:
            _check("C::write_without_flag_refused", True)
        # Write outside local_secret_handoff refused
        try:
            henv.write_key_handoff_envelope(env, path,
                                             allow_secret_write=True)
            _check("C::write_outside_safe_refused", False,
                   "did not raise")
        except ValueError:
            _check("C::write_outside_safe_refused", True)
    # Write inside local_secret_handoff allowed when flag set
    safe_dir = (_ROOT / "bilingual_stack" /
                "voice_adapter_phase36" / "local_secret_handoff")
    safe_dir.mkdir(parents=True, exist_ok=True)
    safe_path = (safe_dir /
                  f"_test_envelope_{int(__import__('time').time())}.json")
    try:
        out = henv.write_key_handoff_envelope(
            env, str(safe_path), allow_secret_write=True)
        _check("C::write_inside_safe_with_flag", os.path.exists(out))
    finally:
        try:
            os.unlink(str(safe_path))
        except OSError:
            pass
    # write_key_handoff_envelope_report refuses secret fields
    try:
        henv.write_key_handoff_envelope_report(
            {"sealed_payload": "x"}, str(safe_dir / "_bad.json"))
        _check("C::report_refuses_secret", False,
               "did not raise")
    except ValueError:
        _check("C::report_refuses_secret", True)


def suite_d_secret_boundary() -> None:
    import bilingual_voice_phase36_secret_boundary as sb
    pol = sb.get_phase36_secret_boundary_policy()
    _check("D::policy_version",
           isinstance(pol.get("version"), str))
    # Path checks
    _check("D::safe_path_accepted",
           sb.is_secret_safe_path(
               "bilingual_stack/voice_adapter_phase36/"
               "local_secret_handoff/env.json"))
    _check("D::reports_path_rejected",
           sb.validate_secret_artifact_location(
               "bilingual_stack/voice_adapter_phase36/reports/"
               "env.json")["ok"] is False)
    _check("D::public_descriptors_path_rejected",
           sb.validate_secret_artifact_location(
               "bilingual_stack/voice_adapter_phase36/"
               "public_descriptors/env.json")["ok"] is False)
    # Object scan
    hits = sb.scan_object_for_secret_fields({
        "private_key": "x", "ok": True,
        "nested": {"signing_key_material": "y"}})
    _check("D::object_scan_detects",
           "private_key" in hits and
           "signing_key_material" in hits)
    # Public artifact leakage rejected
    pa = sb.validate_no_secret_leakage_in_public_artifact(
        {"material_hex": "deadbeef"})
    _check("D::public_artifact_leakage_rejected",
           pa["ok"] is False)
    # Directory scan bounded
    with tempfile.TemporaryDirectory() as td:
        for i in range(5):
            (Path(td) / f"clean_{i}.json").write_text(
                json.dumps({"ok": True}), encoding="utf-8")
        (Path(td) / "leak.json").write_text(
            json.dumps({"private_key": "should_not_be_here",
                         "material_hex": "f"*64}),
            encoding="utf-8")
        scan = sb.validate_no_secret_leakage_in_directory(td)
        _check("D::dir_scan_detects_leak",
               not scan["ok"]
               and any("leak.json" in (l["path"] or "")
                       for l in scan.get("leaks", [])))
    # Large file scan bounded
    with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt") as tf:
        tf.write("x" * (2_000_000))  # 2 MB
        tmp = tf.name
    try:
        res = sb.scan_file_for_secret_indicators(
            tmp, max_bytes=1_000_000)
        _check("D::large_file_bounded",
               res.get("reason") == "file_too_large")
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def suite_e_public_descriptor() -> None:
    import bilingual_voice_phase36_public_descriptor_bridge as pdb
    import bilingual_voice_phase36_key_handoff_envelope as henv
    import bilingual_voice_audit_signing_policy as asp
    env = henv.create_key_handoff_envelope(
        asp.create_test_signing_key("phase36_e1"),
        consent_marker="op_consent")
    desc = pdb.create_public_descriptor_from_handoff(env)
    _check("E::descriptor_validates",
           pdb.validate_public_descriptor_from_handoff(desc)["ok"])
    _check("E::fingerprint_present",
           isinstance(desc.get("public_fingerprint"), str)
           and len(desc["public_fingerprint"]) == 64)
    for k in ("sealed_payload", "private_key", "secret",
              "material_hex", "signing_key_material",
              "raw_key", "hmac_key"):
        _check(f"E::no_secret:{k}", k not in desc)
    cmp_ok = pdb.compare_handoff_to_public_descriptor(env, desc)
    _check("E::identity_match", cmp_ok["ok"])
    # Production label rejected
    bad_env = dict(env)
    bad_env["key_label"] = "production_main"
    bad_desc = pdb.create_public_descriptor_from_handoff(bad_env)
    _check("E::production_label_rejected",
           bad_desc.get("ok") is False)


def suite_f_handoff_verifier() -> None:
    import bilingual_voice_phase36_handoff_verifier as hv
    import bilingual_voice_phase36_key_handoff_envelope as henv
    import bilingual_voice_audit_signing_policy as asp
    import bilingual_voice_audit_chain as vac
    import bilingual_voice_phase33_signed_evidence as p33sev
    key = asp.create_test_signing_key("phase36_f1")
    env = henv.create_key_handoff_envelope(
        key, consent_marker="op_consent_f")
    chain = vac.append_chain_event(
        [], vac.create_audit_chain_event("preflight", "ok", "x"))
    inv = {"receipt_id": "recv_x",
            "adapter_name": "dummy_metadata_adapter",
            "adapter_type": "dummy_metadata_adapter",
            "operator_id_hash": "a"*16,
            "dry_run": True, "test_only": True,
            "execution_boundary_preserved": True,
            "audio_generated": False, "tts_invoked": False,
            "subprocess_used": False, "network_used": False,
            "files_written": False, "request_id": "x",
            "result_id": "y", "pre_call_status": "ok",
            "post_call_status": "ok",
            "audit_chain_hash": "abc", "notes": "x",
            "created_at": 1.0, "phase": "p"}
    sev = p33sev.create_phase33_signed_evidence({
        "audit_chain": chain,
        "invocation_receipt": inv,
        "selection_receipt": {},
        "selected_adapter_result": {
            "result_id": "y",
            "adapter_name": "dummy_metadata_adapter",
            "produced_audio": False, "invoked_tts": False,
            "used_subprocess": False, "used_network": False,
            "wrote_files": False},
        "status": "ok"}, key)
    res = hv.verify_with_handoff_envelope(
        sev, env, consent_marker="op_consent_f")
    _check("F::signed_evidence_passes", res["status"] == "pass",
           json.dumps(res.get("checks_failed")))
    # Wrong consent
    res2 = hv.verify_with_handoff_envelope(
        sev, env, consent_marker="WRONG")
    _check("F::wrong_consent_fails", res2["status"] == "fail")
    # Tampered evidence
    bad = json.loads(json.dumps(sev))
    bad["signed_audit_chain"][0]["message"] = "tampered"
    res3 = hv.verify_with_handoff_envelope(
        bad, env, consent_marker="op_consent_f")
    _check("F::tampered_fails", res3["status"] == "fail")
    # Witness package path — use Phase 34 export rebuilt with key
    import bilingual_voice_phase34_export_runtime as p34
    export = p34.create_phase34_witness_export(
        "hello", operator_id="operator_local", approve=True,
        sign_evidence=True)
    pkg = dict(export["witness_package"])
    # Replace signed_evidence_payload with one signed by OUR key
    pkg["signed_evidence_payload"] = p33sev.create_phase33_signed_evidence({
        "audit_chain":
            (export.get("phase33_output") or {}).get(
                "audit_chain") or [],
        "invocation_receipt":
            (export.get("phase33_output") or {}).get(
                "invocation_receipt") or {},
        "selection_receipt":
            (export.get("phase33_output") or {}).get(
                "selection_receipt") or {},
        "selected_adapter_result":
            (export.get("phase33_output") or {}).get(
                "selected_adapter_result") or {},
        "status": "ok"}, key)
    res4 = hv.verify_witness_package_with_handoff(
        pkg, env, consent_marker="op_consent_f")
    _check("F::witness_package_path",
           res4["status"] == "pass",
           json.dumps(res4.get("checks_failed")))
    # Verification result has no secret material
    for k in ("private_key", "secret", "material_hex",
              "signing_key_material", "sealed_payload",
              "raw_key", "hmac_key"):
        _check(f"F::no_secret_in_result:{k}", k not in res
               and k not in res4)


def suite_g_operator_guide() -> None:
    import bilingual_voice_phase36_operator_guide as oag
    guide = oag.create_phase36_operator_handoff_guide()
    _check("G::guide_id", isinstance(guide.get("guide_id"), str))
    _check("G::creator_steps",
           isinstance(guide.get("creator_role", {}).get(
               "steps"), list)
           and len(guide["creator_role"]["steps"]) >= 5)
    _check("G::verifier_steps",
           isinstance(guide.get("verifier_role", {}).get(
               "steps"), list)
           and len(guide["verifier_role"]["steps"]) >= 4)
    _check("G::cleanup_steps",
           isinstance(guide.get("cleanup_rotation"), dict))
    _check("G::gitignore_safety",
           isinstance(guide.get("gitignore_safety"), dict))
    _check("G::failure_interpretation",
           isinstance(guide.get("failure_interpretation"), dict))
    text = json.dumps(guide).lower()
    _check("G::says_test_only",
           "test-only" in text or "test only" in text)
    _check("G::no_network",
           "no network" in text)
    _check("G::no_subprocess",
           "no subprocess" in text)
    _check("G::no_audio",
           "no audio" in text)
    _check("G::phase21_separate",
           "phase 21" in text and "separate" in text)


def suite_h_runtime() -> None:
    import bilingual_voice_phase36_handoff_runtime as rt
    # No consent → refused
    r0 = rt.create_phase36_key_handoff(consent_marker="")
    _check("H::refuses_without_consent",
           r0.get("status") == "refused")
    # No allow_secret_write → summary only, no envelope written
    with tempfile.TemporaryDirectory() as td:
        r1 = rt.create_phase36_key_handoff(
            consent_marker="op_consent_h1",
            output_dir=td, allow_secret_write=False)
        _check("H::status_ok",
               r1.get("status") == "ok",
               json.dumps(r1.get(
                   "handoff_verification_result", {}).get(
                       "checks_failed")))
        written = r1.get("written_paths") or {}
        _check("H::no_envelope_when_flag_off",
               "sealed_envelope" not in written)
        _check("H::summary_written",
               "envelope_summary" in written
               and os.path.exists(written["envelope_summary"]))
        # Confirm summary has no sealed_payload
        s = json.loads(
            Path(written["envelope_summary"]).read_text(
                encoding="utf-8"))
        _check("H::summary_has_no_sealed_payload",
               "sealed_payload" not in s)
    # allow_secret_write=True → envelope written only inside
    # local_secret_handoff
    safe_dir = (_ROOT / "bilingual_stack" /
                "voice_adapter_phase36" / "local_secret_handoff")
    pre_existing = set(safe_dir.glob("handoff_envelope_*.json"))
    with tempfile.TemporaryDirectory() as td2:
        r2 = rt.create_phase36_key_handoff(
            consent_marker="op_consent_h2",
            output_dir=td2, allow_secret_write=True)
        _check("H::secret_write_ok",
               r2.get("status") == "ok")
        written2 = r2.get("written_paths") or {}
        env_path = written2.get("sealed_envelope")
        _check("H::sealed_env_inside_safe",
               env_path and "local_secret_handoff" in
               env_path.replace("\\", "/"))
    # Clean up envelopes we created
    for p in safe_dir.glob("handoff_envelope_*.json"):
        if p not in pre_existing:
            try:
                p.unlink()
            except OSError:
                pass
    # verify_phase36_with_handoff
    r3 = rt.verify_phase36_with_handoff(
        consent_marker="op_consent_h3",
        include_demo=False)
    _check("H::verify_status_ok", r3.get("status") == "ok",
           json.dumps(r3.get("witness_package_check", {}).get(
               "checks_failed")))
    # Demo bounded
    demo = rt.create_phase36_handoff_demo(limit=3)
    _check("H::demo_bounded", demo["count"] == 3)
    # Output validates
    out_val = rt.validate_phase36_handoff_output(r1)
    _check("H::output_validates", out_val["ok"],
           json.dumps(out_val))
    # write_phase36_handoff_runtime_report refuses sealed_payload
    try:
        with tempfile.TemporaryDirectory() as td3:
            rt.write_phase36_handoff_runtime_report(
                {"sealed_payload": "x"},
                os.path.join(td3, "bad.json"))
            _check("H::report_refuses_sealed", False,
                   "did not raise")
    except ValueError:
        _check("H::report_refuses_sealed", True)


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
                                       "voice_adapter_phase36"):
        for f in files:
            if f.lower().endswith((".wav", ".mp3", ".ogg",
                                    ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files", not audio, ",".join(audio))


def suite_j_isolation_and_leakage() -> None:
    files = [
        "bilingual_voice_phase36_handoff_contract.py",
        "bilingual_voice_phase36_key_handoff_envelope.py",
        "bilingual_voice_phase36_secret_boundary.py",
        "bilingual_voice_phase36_public_descriptor_bridge.py",
        "bilingual_voice_phase36_handoff_verifier.py",
        "bilingual_voice_phase36_operator_guide.py",
        "bilingual_voice_phase36_handoff_runtime.py",
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
        "multiprocessing.Pool",
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
    # No secret material in reports/witness packages/public descriptors
    import bilingual_voice_phase36_secret_boundary as sb
    scan_dirs = [
        _ROOT / "bilingual_stack" / "voice_adapter_phase36" /
            "reports",
        _ROOT / "bilingual_stack" / "voice_adapter_phase36" /
            "public_descriptors",
        _ROOT / "bilingual_stack" / "voice_adapter_phase34" /
            "operator_guides",
    ]
    for d in scan_dirs:
        if not d.exists():
            _check(f"J::leak_scan_dir_present:{d.name}", True)
            continue
        scan = sb.validate_no_secret_leakage_in_directory(str(d))
        _check(f"J::no_leak_in:{d.name}", scan["ok"],
               json.dumps(scan.get("leaks", [])))


def main() -> int:
    suites = [
        ("A", suite_a_preflight),
        ("B", suite_b_contract),
        ("C", suite_c_envelope),
        ("D", suite_d_secret_boundary),
        ("E", suite_e_public_descriptor),
        ("F", suite_f_handoff_verifier),
        ("G", suite_g_operator_guide),
        ("H", suite_h_runtime),
        ("I", suite_i_production_safety),
        ("J", suite_j_isolation_and_leakage),
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
