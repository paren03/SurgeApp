"""Phase 33 test harness — three-adapter + signed evidence."""

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
        "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
        "test_phase32_audit_signing_and_verification.py",
        "bilingual_voice_audit_signing_policy.py",
        "bilingual_voice_audit_chain_signer.py",
        "bilingual_voice_evidence_bundle.py",
        "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
        "bilingual_voice_phase31_adapter_interface.py",
        "bilingual_segment_metadata_adapter.py",
        "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
        "bilingual_voice_callable_adapter_interface.py",
        "bilingual_voice_dummy_metadata_adapter.py",
        "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
        "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
        "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
        "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
        "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
    ]
    p33 = [
        "bilingual_voice_phase33_adapter_interface.py",
        "bilingual_prosody_density_metadata_adapter.py",
        "bilingual_voice_phase33_selection_policy.py",
        "bilingual_voice_phase33_signed_evidence.py",
        "bilingual_voice_phase33_governance_recheck.py",
        "bilingual_voice_phase33_result_verifier.py",
        "bilingual_voice_adapter_phase33_runtime.py",
    ]
    for f in upstream + p33:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_phase33_adapter_interface",
        "bilingual_prosody_density_metadata_adapter",
        "bilingual_voice_phase33_selection_policy",
        "bilingual_voice_phase33_signed_evidence",
        "bilingual_voice_phase33_governance_recheck",
        "bilingual_voice_phase33_result_verifier",
        "bilingual_voice_adapter_phase33_runtime",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_interface() -> None:
    import bilingual_voice_phase33_adapter_interface as p33i
    s = p33i.get_phase33_callable_adapter_schema()
    _check("B::schema_version", isinstance(s.get("version"), str))
    _check("B::exactly_three_allowed",
           s["allowed_adapter_types"] == [
               "dummy_metadata_adapter",
               "bilingual_segment_metadata_adapter",
               "prosody_density_metadata_adapter"])
    for at in ("dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter"):
        d = p33i.create_phase33_adapter_descriptor(at, at)
        _check(f"B::descriptor_validates:{at}",
               p33i.validate_phase33_adapter_descriptor(d)["ok"])
    for bad in ("real_piper", "sapi_real", "kokoro_real",
                "real_tts", "audio_renderer",
                "subprocess_renderer", "powershell_renderer",
                "network_renderer"):
        b = p33i.create_phase33_adapter_descriptor(bad, bad)
        _check(f"B::reject:{bad}",
               not p33i.validate_phase33_adapter_descriptor(b)["ok"])
    dummy = p33i.create_phase33_adapter_descriptor(
        "dummy_metadata_adapter", "dummy_metadata_adapter")
    for flag in ("produces_audio", "invokes_tts", "uses_subprocess",
                  "uses_network", "writes_files"):
        bad_desc = dict(dummy)
        bad_desc[flag] = True
        _check(f"B::flag_rejected:{flag}",
               not p33i.validate_phase33_adapter_descriptor(
                   bad_desc)["ok"])
    pkt = {"packet_id": "rev_x", "envelope_id": "venv_x",
           "job_id": "j_x", "language_mode": "english_only",
           "safety_summary": {},
           "spoken_render_payload": {"language_mode": "english_only"}}
    tok = {"token_id": "itok_x", "operator_id": "op",
            "approved": True}
    req = p33i.create_phase33_adapter_request(pkt, dummy, tok)
    _check("B::request_validates",
           p33i.validate_phase33_adapter_request(req)["ok"])


def suite_c_prosody_density() -> None:
    import bilingual_prosody_density_metadata_adapter as pdma
    desc = pdma.get_prosody_density_metadata_adapter_descriptor()
    _check("C::descriptor_exists",
           desc.get("adapter_name") ==
           "prosody_density_metadata_adapter")
    req = {"request_id": "p33req_x", "envelope_id": "venv_x",
           "job_id": "j_x", "language_mode": "english_only",
           "segment_count": 3,
           "spoken_render_payload": {
               "language_mode": "english_only",
               "segments": [
                   {"segment_id": "s1", "text": "hello",
                    "language": "en"},
                   {"segment_id": "s2", "text": "world",
                    "language": "en"},
                   {"segment_id": "s3", "text": "ok",
                    "language": "en"}],
               "code_switch_boundaries": [{"after": "s1"}],
               "prosody": {"pause_long": True, "emphasis_strong": True,
                           "tone_rising": True}}}
    res = pdma.call_prosody_density_metadata_adapter(req)
    v = pdma.validate_prosody_density_metadata_result(res)
    _check("C::result_validates", v["ok"], json.dumps(v))
    _check("C::pause_count",
           res.get("pause_marker_count") >= 1)
    _check("C::emp_count",
           res.get("emphasis_marker_count") >= 1)
    _check("C::tone_count",
           res.get("tone_marker_count") >= 1)
    _check("C::cs_count",
           res.get("code_switch_boundary_count") >= 1)
    _check("C::density_score",
           isinstance(res.get("prosody_density_score"), float)
           and res["prosody_density_score"] > 0)
    _check("C::complexity_score",
           isinstance(res.get("spoken_complexity_score"), float)
           and res["spoken_complexity_score"] > 0)
    for flag in ("produced_audio", "invoked_tts",
                  "used_subprocess", "used_network",
                  "wrote_files"):
        _check(f"C::flag_false:{flag}", res[flag] is False)
    for k in ("audio_bytes", "audio_path", "command",
              "subprocess", "powershell"):
        _check(f"C::no_field:{k}", k not in res)


def suite_d_selection_policy() -> None:
    import bilingual_voice_phase33_selection_policy as p33s
    import bilingual_voice_phase33_adapter_interface as p33i
    simple_en = {"language_mode": "english_only",
                  "spoken_render_payload": {
                      "language_mode": "english_only",
                      "segments": [{"segment_id": "s1",
                                     "text": "hi",
                                     "language": "en"}]},
                  "safety_summary": {}}
    mix = {"language_mode": "mixed_en_ru",
            "spoken_render_payload": {
                "language_mode": "mixed_en_ru",
                "segments": [
                    {"segment_id": "s1", "text": "hi", "language": "en"},
                    {"segment_id": "s2", "text": "привет",
                     "language": "ru"}],
                "code_switch_boundaries": [{"after": "s1"}]},
            "safety_summary": {}}
    pros = {"language_mode": "english_only",
             "spoken_render_payload": {
                 "language_mode": "english_only",
                 "segments": [{"segment_id": "s1",
                                "text": "x", "language": "en"}],
                 "prosody": {"pause_long": True,
                             "emphasis_strong": True,
                             "tone_rising": True,
                             "pitch_low": True}},
             "safety_summary": {}}
    c_en = p33s.choose_phase33_adapter(simple_en)
    _check("D::en_choice_ok", c_en["ok"])
    # Simple EN should give dummy the highest score
    _check("D::en_picks_dummy",
           c_en["chosen"]["adapter_type"] ==
           "dummy_metadata_adapter",
           json.dumps(c_en["score_summary"]))
    c_mix = p33s.choose_phase33_adapter(mix)
    _check("D::mix_picks_bilingual",
           c_mix["chosen"]["adapter_type"] ==
           "bilingual_segment_metadata_adapter",
           json.dumps(c_mix["score_summary"]))
    c_pros = p33s.choose_phase33_adapter(pros)
    _check("D::high_prosody_picks_prosody",
           c_pros["chosen"]["adapter_type"] ==
           "prosody_density_metadata_adapter",
           json.dumps(c_pros["score_summary"]))
    c_pref = p33s.choose_phase33_adapter(
        simple_en,
        preferred_adapter="prosody_density_metadata_adapter")
    _check("D::preferred_wins",
           c_pref["chosen"]["adapter_type"] ==
           "prosody_density_metadata_adapter")
    c_bad = p33s.choose_phase33_adapter(
        simple_en, preferred_adapter="real_piper")
    _check("D::invalid_preferred_falls_back",
           c_bad["ok"] and c_bad["chosen"]["adapter_type"] in
           p33i.ALLOWED_ADAPTER_TYPES)
    safety = {"language_mode": "english_only",
               "spoken_render_payload": {
                   "language_mode": "english_only",
                   "segments": [{"segment_id": "s1",
                                  "text": "x", "language": "en"}]},
               "safety_summary": {"high_risk": True}}
    c_safe = p33s.choose_phase33_adapter(safety)
    _check("D::safety_warn_not_dummy",
           c_safe["chosen"]["adapter_type"] !=
           "dummy_metadata_adapter")
    bad_desc = p33i.create_phase33_adapter_descriptor(
        "dummy_metadata_adapter", "dummy_metadata_adapter")
    bad_desc["uses_subprocess"] = True
    rej = p33s.reject_disallowed_phase33_adapter(bad_desc)
    _check("D::exec_flag_rejected", rej["rejected"] is True)
    _check("D::explain_summary",
           isinstance(p33s.explain_phase33_selection(
               c_mix).get("summary"), str))


def suite_e_signed_evidence() -> None:
    import bilingual_voice_phase33_signed_evidence as p33sev
    import bilingual_voice_audit_signing_policy as asp
    import bilingual_voice_audit_chain as vac
    # Provisional invocation output
    chain = vac.append_chain_event(
        [], vac.create_audit_chain_event("preflight", "ok", "x"))
    out = {
        "audit_chain": chain,
        "invocation_receipt": {
            "receipt_id": "recv_x",
            "adapter_name": "dummy_metadata_adapter",
            "adapter_type": "dummy_metadata_adapter",
            "operator_id_hash": "a"*16,
            "dry_run": True, "test_only": True,
            "execution_boundary_preserved": True,
            "audio_generated": False, "tts_invoked": False,
            "subprocess_used": False, "network_used": False,
            "files_written": False, "request_id": "x",
            "result_id": "dres_x", "pre_call_status": "ok",
            "post_call_status": "ok",
            "audit_chain_hash": "abc", "notes": "x",
            "created_at": 1.0, "phase": "p"},
        "selection_receipt": {},
        "selected_adapter_result": {
            "result_id": "dres_x",
            "adapter_name": "dummy_metadata_adapter",
            "produced_audio": False, "invoked_tts": False,
            "used_subprocess": False, "used_network": False,
            "wrote_files": False},
        "status": "ok",
    }
    ev = p33sev.create_phase33_signed_evidence(out)
    v = p33sev.validate_phase33_signed_evidence(ev)
    _check("E::evidence_validates", v["ok"], json.dumps(v))
    key = asp.create_test_signing_key("phase33_test_key")
    # Verify with explicit key — the create function used a fresh
    # default test key, so we need to capture and reuse it via the
    # signing_metadata. For coverage, re-sign with our key:
    ev2 = p33sev.create_phase33_signed_evidence(out, key)
    ver = p33sev.verify_phase33_signed_evidence(ev2, key)
    _check("E::evidence_verifies", ver["ok"], json.dumps(ver))
    # Tampered evidence
    bad = json.loads(json.dumps(ev2))
    bad["signed_audit_chain"][0]["message"] = "tampered"
    vbad = p33sev.verify_phase33_signed_evidence(bad, key)
    _check("E::tampered_fails", not vbad["ok"])
    # Audio field rejected
    bad2 = dict(ev2)
    bad2["audio_bytes"] = "fake"
    _check("E::audio_field_rejected",
           not p33sev.validate_phase33_signed_evidence(bad2)["ok"])
    # Command field rejected
    bad3 = dict(ev2)
    bad3["command"] = "tts --speak"
    _check("E::command_field_rejected",
           not p33sev.validate_phase33_signed_evidence(bad3)["ok"])
    # Non-test key rejected at create time
    try:
        asp.create_test_signing_key("prod_main")
        _check("E::prod_key_rejected", False, "did not raise")
    except ValueError:
        _check("E::prod_key_rejected", True)
    # Forced non-test key fails creation
    bad_key = asp.create_test_signing_key("phase33_x")
    bad_key["test_only"] = False
    ev_badkey = p33sev.create_phase33_signed_evidence(out, bad_key)
    _check("E::nontest_key_rejected",
           "ok" in ev_badkey and ev_badkey.get("ok") is False)
    summary = p33sev.summarize_phase33_signed_evidence(ev2)
    _check("E::summary_string",
           isinstance(summary.get("summary"), str))


def suite_f_governance_recheck() -> None:
    import bilingual_voice_phase33_governance_recheck as g
    p30 = g.verify_phase33_phase30_strictness()
    _check("F::phase30_strict", p30["ok"]
           and p30["allowed_adapter_types"] ==
           ["dummy_metadata_adapter"])
    p31 = g.verify_phase33_phase31_boundary()
    _check("F::phase31_two", p31["ok"])
    p33 = g.verify_phase33_three_adapter_boundary()
    _check("F::phase33_three", p33["ok"]
           and p33["allowed_adapter_types"] ==
           ["dummy_metadata_adapter",
            "bilingual_segment_metadata_adapter",
            "prosody_density_metadata_adapter"])
    good = [{"adapter_name": "dummy_metadata_adapter"},
            {"adapter_name": "bilingual_segment_metadata_adapter"},
            {"adapter_name": "prosody_density_metadata_adapter"}]
    _check("F::allowed_pass",
           g.verify_phase33_allowed_adapters_only(good)["ok"])
    bad = [{"adapter_name": "real_piper"}]
    _check("F::real_rejected",
           not g.verify_phase33_allowed_adapters_only(bad)["ok"])
    out_ok = {"status": "ok", "signed_evidence": {"a": 1}}
    _check("F::signed_evidence_present_ok",
           g.verify_phase33_signed_evidence_required(out_ok)["ok"])
    out_missing = {"status": "ok"}
    _check("F::signed_evidence_missing_fails",
           not g.verify_phase33_signed_evidence_required(
               out_missing)["ok"])
    paths = [str(_ROOT / "bilingual_voice_phase33_adapter_interface.py")]
    _check("F::no_audio_in_artifacts",
           g.verify_phase33_no_audio_boundary(paths)["ok"])
    _check("F::no_execution_in_artifacts",
           g.verify_phase33_no_execution_boundary(paths)["ok"])
    _check("F::metadata_only_results",
           g.verify_phase33_metadata_only_results(
               [{"produced_audio": False, "invoked_tts": False,
                 "used_subprocess": False, "used_network": False,
                 "wrote_files": False}])["ok"])


def suite_g_result_verifier() -> None:
    import bilingual_voice_phase33_result_verifier as rv
    res = {"result_id": "x", "adapter_name": "dummy_metadata_adapter",
           "dry_run": True, "test_only": True,
           "produced_audio": False, "invoked_tts": False,
           "used_subprocess": False, "used_network": False,
           "wrote_files": False}
    _check("G::result_ok",
           rv.verify_phase33_adapter_result(res)["ok"])
    sel = {"receipt_id": "selrec_x", "created_at": 1.0,
           "selected_adapter_name": "dummy_metadata_adapter",
           "selected_adapter_type": "dummy_metadata_adapter",
           "candidate_adapters": [], "selection_reason": "x",
           "score_summary": {}, "request_id": "x",
           "result_id": "x", "dry_run": True, "test_only": True,
           "execution_boundary_preserved": True,
           "audio_generated": False, "tts_invoked": False,
           "subprocess_used": False, "network_used": False,
           "files_written": False, "audit_chain_hash": "a",
           "notes": "x", "phase": "p"}
    _check("G::selection_ok",
           rv.verify_phase33_selection_receipt(sel, res)["ok"])
    inv = {"receipt_id": "recv_x", "created_at": 1.0,
           "adapter_name": "dummy_metadata_adapter",
           "adapter_type": "dummy_metadata_adapter",
           "operator_id_hash": "a"*16, "dry_run": True,
           "test_only": True,
           "execution_boundary_preserved": True,
           "audio_generated": False, "tts_invoked": False,
           "subprocess_used": False, "network_used": False,
           "files_written": False, "request_id": "x",
           "result_id": "x", "pre_call_status": "ok",
           "post_call_status": "ok", "audit_chain_hash": "a",
           "notes": "x", "phase": "p"}
    _check("G::invocation_ok",
           rv.verify_phase33_invocation_receipt(inv, res)["ok"])
    # Missing evidence on ok status fails complete
    out_no_ev = {"status": "ok",
                  "selected_adapter_result": res,
                  "invocation_receipt": inv,
                  "selection_receipt": sel}
    _check("G::missing_evidence_fails",
           not rv.verify_phase33_complete_output(out_no_ev)["ok"])
    # Unknown adapter fails
    bad_res = dict(res)
    bad_res["adapter_name"] = "real_piper"
    _check("G::unknown_adapter_fails",
           not rv.verify_phase33_adapter_result(bad_res)["ok"])
    for flag in ("produced_audio", "invoked_tts", "used_subprocess",
                  "used_network", "wrote_files"):
        bad = dict(res)
        bad[flag] = True
        _check(f"G::{flag}_fails",
               not rv.verify_phase33_adapter_result(bad)["ok"])


def suite_h_runtime() -> None:
    import bilingual_voice_adapter_phase33_runtime as p33
    r_en = p33.prepare_phase33_three_adapter_invocation(
        user_text="hello luna", draft_response_text="Hi.",
        operator_id="op_local", approve=True)
    _check("H::en_runs", r_en["status"] == "ok",
           json.dumps([e.get("code") for e in r_en.get("errors", [])]))
    _check("H::en_signed",
           bool(r_en.get("signed_evidence")))
    r_ru = p33.prepare_phase33_three_adapter_invocation(
        user_text="привет луна", draft_response_text="Привет!",
        user_preference="russian",
        operator_id="op_local", approve=True)
    _check("H::ru_runs", r_ru["status"] == "ok")
    r_mix = p33.prepare_phase33_three_adapter_invocation(
        user_text="mix russian and english",
        draft_response_text="ok, давай.",
        operator_id="op_local", approve=True)
    chosen_mix = ((r_mix.get("selection_choice") or {})
                   .get("chosen") or {}).get("adapter_type")
    _check("H::mix_picks_bilingual",
           chosen_mix == "bilingual_segment_metadata_adapter",
           f"got {chosen_mix}")
    r_pros = p33.prepare_phase33_three_adapter_invocation(
        user_text="hello", operator_id="op_local", approve=True,
        preferred_adapter="prosody_density_metadata_adapter")
    chosen_pros = ((r_pros.get("selection_choice") or {})
                    .get("chosen") or {}).get("adapter_type")
    _check("H::pref_prosody_wins",
           chosen_pros == "prosody_density_metadata_adapter")
    r_pref_dum = p33.prepare_phase33_three_adapter_invocation(
        user_text="hello", operator_id="op_local", approve=True,
        preferred_adapter="dummy_metadata_adapter")
    chosen_dum = ((r_pref_dum.get("selection_choice") or {})
                   .get("chosen") or {}).get("adapter_type")
    _check("H::pref_dummy_works",
           chosen_dum == "dummy_metadata_adapter")
    r_no = p33.prepare_phase33_three_adapter_invocation(
        user_text="hi", approve=False)
    _check("H::approve_false_refused", r_no["status"] != "ok")
    r_ks = p33.prepare_phase33_three_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, kill_switch_enabled=True)
    _check("H::kill_switch_blocks",
           r_ks["status"] == "kill_switch_blocked")
    # sign_evidence=False makes successful output INVALID per result_verifier
    r_no_sig = p33.prepare_phase33_three_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, sign_evidence=False)
    # status starts ok but result_verifier flips to refused
    _check("H::sign_false_makes_output_refused",
           r_no_sig["status"] != "ok"
           or r_no_sig.get("result_verification", {}).get("ok")
               is False)
    demo = p33.demo_phase33_three_adapter_invocations(limit=4)
    _check("H::demo_bounded", demo["count"] == 4)
    for d in demo["demo"]:
        _check(f"H::demo_no_audio:{d['user_text'][:20]}",
               (d.get("status") in ("ok", "refused",
                                     "kill_switch_blocked")))


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
                                       "voice_adapter_phase33"):
        for f in files:
            if f.lower().endswith((".wav", ".mp3", ".ogg",
                                    ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files", not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_phase33_adapter_interface.py",
        "bilingual_prosody_density_metadata_adapter.py",
        "bilingual_voice_phase33_selection_policy.py",
        "bilingual_voice_phase33_signed_evidence.py",
        "bilingual_voice_phase33_governance_recheck.py",
        "bilingual_voice_phase33_result_verifier.py",
        "bilingual_voice_adapter_phase33_runtime.py",
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
        ("C", suite_c_prosody_density),
        ("D", suite_d_selection_policy),
        ("E", suite_e_signed_evidence),
        ("F", suite_f_governance_recheck),
        ("G", suite_g_result_verifier),
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
