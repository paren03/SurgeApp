"""Phase 30 test harness — callable adapter boundary."""

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
    p29 = [
        "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
        "test_phase29_operator_gated_runtime_adapter_b.py",
        "bilingual_voice_invocation_consent.py",
        "bilingual_voice_audit_chain.py",
        "bilingual_voice_calltime_boundary.py",
        "bilingual_voice_operator_review_packet.py",
        "bilingual_voice_dry_run_queue.py",
        "bilingual_voice_refusal_analytics.py",
        "bilingual_voice_adapter_phase29_runtime.py",
    ]
    p28 = ["PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
           "bilingual_voice_adapter_phase28_runtime.py"]
    p27 = ["PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
           "bilingual_voice_dry_run_pipeline.py"]
    p26 = ["PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md"]
    p25 = ["PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
           "bilingual_spoken_render_runtime.py",
           "bilingual_spoken_render_contract.py"]
    p30 = [
        "bilingual_voice_callable_adapter_interface.py",
        "bilingual_voice_dummy_metadata_adapter.py",
        "bilingual_voice_emergency_kill_switch.py",
        "bilingual_voice_pre_call_validator.py",
        "bilingual_voice_post_call_validator.py",
        "bilingual_voice_invocation_receipt.py",
        "bilingual_voice_adapter_phase30_runtime.py",
    ]
    for f in p29 + p28 + p27 + p26 + p25 + p30:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_callable_adapter_interface",
        "bilingual_voice_dummy_metadata_adapter",
        "bilingual_voice_emergency_kill_switch",
        "bilingual_voice_pre_call_validator",
        "bilingual_voice_post_call_validator",
        "bilingual_voice_invocation_receipt",
        "bilingual_voice_adapter_phase30_runtime",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_interface() -> None:
    import bilingual_voice_callable_adapter_interface as cai
    s = cai.get_callable_adapter_interface_schema()
    _check("B::schema_version", isinstance(s.get("version"), str))
    _check("B::only_dummy_allowed",
           s["allowed_adapter_types"] == ["dummy_metadata_adapter"])
    desc = cai.create_callable_adapter_descriptor(
        "dummy_metadata_adapter")
    v = cai.validate_callable_adapter_descriptor(desc)
    _check("B::valid_descriptor", v["ok"], json.dumps(v))
    # Real adapter rejected
    bad = cai.create_callable_adapter_descriptor(
        "real_piper", adapter_type="piper_real")
    bv = cai.validate_callable_adapter_descriptor(bad)
    _check("B::real_adapter_rejected", not bv["ok"])
    # All flags must be False
    bad_flags = dict(desc)
    bad_flags["produces_audio"] = True
    _check("B::produces_audio_rejected",
           not cai.validate_callable_adapter_descriptor(bad_flags)["ok"])
    bad_flags2 = dict(desc)
    bad_flags2["invokes_tts"] = True
    _check("B::invokes_tts_rejected",
           not cai.validate_callable_adapter_descriptor(bad_flags2)["ok"])
    bad_flags3 = dict(desc)
    bad_flags3["uses_subprocess"] = True
    _check("B::uses_subprocess_rejected",
           not cai.validate_callable_adapter_descriptor(bad_flags3)["ok"])
    bad_flags4 = dict(desc)
    bad_flags4["uses_network"] = True
    _check("B::uses_network_rejected",
           not cai.validate_callable_adapter_descriptor(bad_flags4)["ok"])
    bad_flags5 = dict(desc)
    bad_flags5["writes_files"] = True
    _check("B::writes_files_rejected",
           not cai.validate_callable_adapter_descriptor(bad_flags5)["ok"])
    # Request validates
    pkt = {"packet_id": "rev_x", "envelope_id": "venv_x", "job_id": "j_x",
           "language_mode": "english_only", "safety_summary": {}}
    tok = {"token_id": "itok_x", "operator_id": "op",
            "approved": True}
    req = cai.create_callable_adapter_request(pkt, desc, tok)
    rv = cai.validate_callable_adapter_request(req)
    _check("B::request_validates", rv["ok"], json.dumps(rv))


def suite_c_dummy_adapter() -> None:
    import bilingual_voice_dummy_metadata_adapter as dma
    desc = dma.get_dummy_metadata_adapter_descriptor()
    _check("C::descriptor_exists",
           desc.get("adapter_name") == "dummy_metadata_adapter")
    req = {"request_id": "creq_x", "envelope_id": "venv_x",
           "job_id": "j_x", "language_mode": "english_only",
           "segment_count": 2, "prosody_count": 0,
           "safety_summary": {"unsafe": False, "blocked": False}}
    res = dma.call_dummy_metadata_adapter(req)
    v = dma.validate_dummy_metadata_result(res)
    _check("C::result_validates", v["ok"], json.dumps(v))
    _check("C::produced_audio_false",
           res["produced_audio"] is False)
    _check("C::invoked_tts_false", res["invoked_tts"] is False)
    _check("C::used_subprocess_false",
           res["used_subprocess"] is False)
    _check("C::used_network_false", res["used_network"] is False)
    _check("C::wrote_files_false", res["wrote_files"] is False)
    # No audio / command fields
    for k in ("audio_bytes", "audio_path", "wav_path", "voice_clone_ref",
              "command", "subprocess", "powershell"):
        _check(f"C::no_field:{k}", k not in res)
    lat = dma.simulate_adapter_latency_metadata(req)
    _check("C::latency_meta_synthetic", lat["synthetic"] is True)


def suite_d_kill_switch() -> None:
    import bilingual_voice_emergency_kill_switch as eks
    state = eks.get_kill_switch_state(default_enabled=False)
    _check("D::default_state_disabled", state["enabled"] is False)
    p_disabled = eks.create_kill_switch_policy(enabled=False)
    d_disabled = eks.enforce_kill_switch(p_disabled)
    _check("D::disabled_allows", d_disabled["allow"] is True)
    p_enabled = eks.create_kill_switch_policy(
        enabled=True, reason="manual")
    d_enabled = eks.enforce_kill_switch(p_enabled)
    _check("D::enabled_blocks", d_enabled["allow"] is False)
    # Malformed policy fails closed
    d_bad = eks.enforce_kill_switch({"bogus": True})
    _check("D::malformed_fails_closed", d_bad["allow"] is False)
    # Override approve=True
    d_override = eks.enforce_kill_switch(
        p_enabled, request={"approved": True})
    _check("D::overrides_approve", d_override["allow"] is False)
    explain = eks.explain_kill_switch_decision(d_enabled)
    _check("D::explain_summary",
           isinstance(explain.get("summary"), str))


def suite_e_pre_call() -> None:
    import bilingual_voice_pre_call_validator as pre
    import bilingual_voice_callable_adapter_interface as cai
    import bilingual_voice_invocation_consent as ic
    import bilingual_voice_emergency_kill_switch as eks
    env_like = {"envelope_id": "venv_x",
                "render_job": {"job_id": "j_x"}}
    tok = ic.create_invocation_consent_token(
        env_like, operator_id="op", approved=True)
    desc = cai.create_callable_adapter_descriptor(
        "dummy_metadata_adapter")
    pkt = {"packet_id": "rev_x", "envelope_id": "venv_x",
           "job_id": "j_x", "language_mode": "en",
           "safety_summary": {}}
    req = cai.create_callable_adapter_request(pkt, desc, tok)
    req["invocation_token"] = tok
    req["safety_summary"] = {"unsafe": False, "blocked": False}
    res = pre.validate_pre_call_requirements(req)
    _check("E::valid_request_passes", res["ok"], json.dumps(res))
    # Missing token
    req_no = dict(req)
    req_no.pop("invocation_token", None)
    req_no["invocation_token_id"] = ""
    res_no = pre.validate_pre_call_requirements(req_no)
    _check("E::missing_token_rejected", not res_no["ok"])
    # Expired token
    t_exp = dict(tok)
    t_exp["expires_at"] = 0.0
    t_exp_req = dict(req)
    t_exp_req["invocation_token"] = t_exp
    res_exp = pre.validate_pre_call_requirements(t_exp_req)
    _check("E::expired_rejected", not res_exp["ok"])
    # approve=False
    t_noap = ic.create_invocation_consent_token(
        env_like, operator_id="op", approved=False)
    req_noap = dict(req)
    req_noap["invocation_token"] = t_noap
    req_noap["approved"] = False
    res_noap = pre.validate_pre_call_requirements(req_noap)
    _check("E::approve_false_rejected", not res_noap["ok"])
    # Missing operator_id
    t_noop = ic.create_invocation_consent_token(
        env_like, operator_id="", approved=True)
    req_noop = dict(req)
    req_noop["invocation_token"] = t_noop
    res_noop = pre.validate_pre_call_requirements(req_noop)
    _check("E::missing_operator_id_rejected", not res_noop["ok"])
    # dry_run=False
    req_dr = dict(req)
    req_dr["dry_run"] = False
    res_dr = pre.validate_pre_call_requirements(req_dr)
    _check("E::dry_run_false_rejected", not res_dr["ok"])
    # Non-dummy adapter
    bad_desc = cai.create_callable_adapter_descriptor(
        "real", adapter_type="piper_real")
    req_bad = cai.create_callable_adapter_request(pkt, bad_desc, tok)
    req_bad["invocation_token"] = tok
    res_bad = pre.validate_pre_call_requirements(req_bad)
    _check("E::non_dummy_rejected", not res_bad["ok"])
    # Audio field
    req_aud = dict(req)
    req_aud["audio_bytes"] = "fake"
    res_aud = pre.validate_pre_call_requirements(req_aud)
    _check("E::audio_field_rejected", not res_aud["ok"])
    # Unsafe payload
    req_uns = dict(req)
    req_uns["safety_summary"] = {"unsafe": True, "blocked": True}
    res_uns = pre.validate_pre_call_requirements(req_uns)
    _check("E::unsafe_payload_rejected", not res_uns["ok"])
    # Kill switch blocks even valid request
    ks_on = eks.create_kill_switch_policy(enabled=True, reason="test")
    res_ks = pre.validate_pre_call_requirements(req, ks_on)
    _check("E::kill_switch_blocks", not res_ks["ok"])


def suite_f_post_call() -> None:
    import bilingual_voice_post_call_validator as post
    import bilingual_voice_dummy_metadata_adapter as dma
    req = {"language_mode": "english_only", "segment_count": 0}
    safe_result = dma.call_dummy_metadata_adapter(req)
    res = post.validate_post_call_result(safe_result, req)
    _check("F::safe_passes", res["ok"], json.dumps(res))
    # produced_audio=True
    bad = dict(safe_result)
    bad["produced_audio"] = True
    _check("F::audio_rejected",
           not post.validate_post_call_result(bad)["ok"])
    # invoked_tts=True
    bad2 = dict(safe_result)
    bad2["invoked_tts"] = True
    _check("F::tts_rejected",
           not post.validate_post_call_result(bad2)["ok"])
    # used_subprocess=True
    bad3 = dict(safe_result)
    bad3["used_subprocess"] = True
    _check("F::sub_rejected",
           not post.validate_post_call_result(bad3)["ok"])
    # used_network=True
    bad4 = dict(safe_result)
    bad4["used_network"] = True
    _check("F::net_rejected",
           not post.validate_post_call_result(bad4)["ok"])
    # wrote_files=True
    bad5 = dict(safe_result)
    bad5["wrote_files"] = True
    _check("F::files_rejected",
           not post.validate_post_call_result(bad5)["ok"])
    # Audio field
    bad6 = dict(safe_result)
    bad6["audio_bytes"] = "x"
    _check("F::audio_field_rejected",
           not post.validate_post_call_result(bad6)["ok"])
    # Command field
    bad7 = dict(safe_result)
    bad7["command"] = "x"
    _check("F::command_field_rejected",
           not post.validate_post_call_result(bad7)["ok"])
    # Request mismatch
    bad8 = dict(safe_result)
    bad8["received_language_mode"] = "russian_only"
    _check("F::mismatch_rejected",
           not post.validate_post_call_result(bad8, req)["ok"])


def suite_g_receipt() -> None:
    import bilingual_voice_invocation_receipt as recv
    import bilingual_voice_dummy_metadata_adapter as dma
    req = {"request_id": "creq_x", "language_mode": "en",
           "segment_count": 0, "operator_id_hash": "deadbeef",
           "adapter_descriptor": {
               "adapter_name": "dummy_metadata_adapter",
               "adapter_type": "dummy_metadata_adapter"}}
    result = dma.call_dummy_metadata_adapter(req)
    pre_ok = {"ok": True, "reasons": []}
    post_ok = {"ok": True, "reasons": []}
    receipt = recv.create_invocation_receipt(req, result, pre_ok,
                                              post_ok, audit_chain=[])
    v = recv.validate_invocation_receipt(receipt)
    _check("G::receipt_validates", v["ok"], json.dumps(v))
    _check("G::operator_id_hash_present",
           bool(receipt.get("operator_id_hash")))
    _check("G::raw_operator_id_absent",
           "operator_id" not in receipt)
    _check("G::execution_boundary_preserved",
           receipt["execution_boundary_preserved"] is True)
    # JSON-serializable
    try:
        json.dumps(receipt, default=str)
        ok = True
    except Exception:  # noqa: BLE001
        ok = False
    _check("G::json_serializable", ok)
    with tempfile.TemporaryDirectory() as td:
        out = recv.write_invocation_receipt(
            receipt, os.path.join(td, "r.json"))
        _check("G::writes", os.path.exists(out))
    # No forbidden fields
    for k in ("audio_bytes", "audio_path", "command", "subprocess",
              "transcript", "operator_id"):
        _check(f"G::no_field:{k}", k not in receipt)


def suite_h_runtime() -> None:
    import bilingual_voice_adapter_phase30_runtime as p30
    # English
    r_en = p30.prepare_phase30_callable_invocation(
        user_text="hello luna", draft_response_text="Hi.",
        operator_id="op_local", approve=True)
    _check("H::en_runs", r_en["status"] == "ok",
           json.dumps([e.get("code") for e in r_en.get("errors", [])]))
    rc = r_en.get("invocation_receipt") or {}
    _check("H::en_boundary_preserved",
           rc.get("execution_boundary_preserved") is True)
    _check("H::en_no_audio",
           rc.get("audio_generated") is False)
    _check("H::en_no_tts", rc.get("tts_invoked") is False)
    _check("H::en_no_sub", rc.get("subprocess_used") is False)
    # Russian
    r_ru = p30.prepare_phase30_callable_invocation(
        user_text="привет луна", draft_response_text="Привет!",
        user_preference="russian",
        operator_id="op_local", approve=True)
    _check("H::ru_runs", r_ru["status"] == "ok")
    # Mixed
    r_mix = p30.prepare_phase30_callable_invocation(
        user_text="mix russian and english",
        draft_response_text="sure, давай.",
        operator_id="op_local", approve=True)
    _check("H::mix_runs", r_mix["status"] == "ok")
    # approve=False → refused
    r_no = p30.prepare_phase30_callable_invocation(
        user_text="hi", approve=False)
    _check("H::approve_false_refused",
           r_no["status"] != "ok")
    # kill switch blocks even valid
    r_ks = p30.prepare_phase30_callable_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, kill_switch_enabled=True)
    _check("H::kill_switch_blocks",
           r_ks["status"] == "kill_switch_blocked")
    # Non-dummy adapter rejected
    r_real = p30.prepare_phase30_callable_invocation(
        user_text="hello", adapter_name="real_piper",
        operator_id="op_local", approve=True)
    _check("H::non_dummy_rejected", r_real["status"] != "ok")
    # Demo bounded
    demo = p30.demo_phase30_callable_invocations(limit=4)
    _check("H::demo_bounded", demo["count"] == 4)
    # All demos confirm no audio
    for d in demo["demo"]:
        _check(f"H::demo_no_audio:{d['user_text'][:20]}",
               d.get("audio_generated") in (False, None))


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
                                       "voice_adapter_phase30"):
        for f in files:
            if f.lower().endswith((".wav", ".mp3", ".ogg",
                                    ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files", not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_callable_adapter_interface.py",
        "bilingual_voice_dummy_metadata_adapter.py",
        "bilingual_voice_emergency_kill_switch.py",
        "bilingual_voice_pre_call_validator.py",
        "bilingual_voice_post_call_validator.py",
        "bilingual_voice_invocation_receipt.py",
        "bilingual_voice_adapter_phase30_runtime.py",
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
        ("B", suite_b_interface),
        ("C", suite_c_dummy_adapter),
        ("D", suite_d_kill_switch),
        ("E", suite_e_pre_call),
        ("F", suite_f_post_call),
        ("G", suite_g_receipt),
        ("H", suite_h_runtime),
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
