"""Phase 31 test harness — multi-adapter callable boundary."""

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
    p30 = [
        "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
        "test_phase30_callable_adapter_boundary.py",
        "bilingual_voice_callable_adapter_interface.py",
        "bilingual_voice_dummy_metadata_adapter.py",
        "bilingual_voice_emergency_kill_switch.py",
        "bilingual_voice_pre_call_validator.py",
        "bilingual_voice_post_call_validator.py",
        "bilingual_voice_invocation_receipt.py",
        "bilingual_voice_adapter_phase30_runtime.py",
    ]
    p29 = ["PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
           "bilingual_voice_adapter_phase29_runtime.py"]
    p28 = ["PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md"]
    p27 = ["PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md"]
    p26 = ["PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md"]
    p25 = ["PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md"]
    p31 = [
        "bilingual_voice_phase31_adapter_interface.py",
        "bilingual_segment_metadata_adapter.py",
        "bilingual_voice_phase31_selection_policy.py",
        "bilingual_voice_phase31_adapter_comparison.py",
        "bilingual_voice_phase31_selection_receipt.py",
        "bilingual_voice_phase31_post_call_equivalence.py",
        "bilingual_voice_adapter_phase31_runtime.py",
    ]
    for f in p30 + p29 + p28 + p27 + p26 + p25 + p31:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_phase31_adapter_interface",
        "bilingual_segment_metadata_adapter",
        "bilingual_voice_phase31_selection_policy",
        "bilingual_voice_phase31_adapter_comparison",
        "bilingual_voice_phase31_selection_receipt",
        "bilingual_voice_phase31_post_call_equivalence",
        "bilingual_voice_adapter_phase31_runtime",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_interface() -> None:
    import bilingual_voice_phase31_adapter_interface as p31i
    s = p31i.get_phase31_callable_adapter_schema()
    _check("B::schema_version", isinstance(s.get("version"), str))
    _check("B::exactly_two_allowed",
           s["allowed_adapter_types"] == [
               "dummy_metadata_adapter",
               "bilingual_segment_metadata_adapter"])
    dummy = p31i.create_phase31_adapter_descriptor(
        "dummy_metadata_adapter", "dummy_metadata_adapter")
    _check("B::dummy_descriptor_validates",
           p31i.validate_phase31_adapter_descriptor(dummy)["ok"])
    bs = p31i.create_phase31_adapter_descriptor(
        "bilingual_segment_metadata_adapter",
        "bilingual_segment_metadata_adapter")
    _check("B::bs_descriptor_validates",
           p31i.validate_phase31_adapter_descriptor(bs)["ok"])
    # Real adapter rejected
    for bad in ("real_piper", "sapi_real", "kokoro_real",
                "real_tts", "audio_renderer", "subprocess_renderer",
                "powershell_renderer", "network_renderer"):
        b = p31i.create_phase31_adapter_descriptor(bad, bad)
        _check(f"B::reject:{bad}",
               not p31i.validate_phase31_adapter_descriptor(b)["ok"])
    # Flag must-be-false
    for flag in ("produces_audio", "invokes_tts", "uses_subprocess",
                  "uses_network", "writes_files"):
        bad_desc = dict(dummy)
        bad_desc[flag] = True
        _check(f"B::flag_rejected:{flag}",
               not p31i.validate_phase31_adapter_descriptor(
                   bad_desc)["ok"])
    # Request validates
    pkt = {"packet_id": "rev_x", "envelope_id": "venv_x",
           "job_id": "j_x", "language_mode": "english_only",
           "safety_summary": {}}
    tok = {"token_id": "itok_x", "operator_id": "op",
            "approved": True}
    req = p31i.create_phase31_adapter_request(pkt, dummy, tok)
    rv = p31i.validate_phase31_adapter_request(req)
    _check("B::request_validates", rv["ok"], json.dumps(rv))


def suite_c_bilingual_segment() -> None:
    import bilingual_segment_metadata_adapter as bsma
    desc = bsma.get_bilingual_segment_metadata_adapter_descriptor()
    _check("C::descriptor_exists",
           desc.get("adapter_name") ==
           "bilingual_segment_metadata_adapter")
    req = {"request_id": "p31req_x", "envelope_id": "venv_x",
           "job_id": "j_x", "language_mode": "mixed_en_ru",
           "segment_count": 2,
           "spoken_render_payload": {
               "language_mode": "mixed_en_ru",
               "segments": [
                   {"segment_id": "s1", "text": "hello",
                    "language": "en"},
                   {"segment_id": "s2", "text": "привет",
                    "language": "ru"}],
               "code_switch_boundaries": [{"after": "s1"}],
               "prosody": {"pace": "normal"},
               "pronunciation_notes": [{"token": "hello"}],
           },
           "safety_summary": {"unsafe": False, "blocked": False}}
    res = bsma.call_bilingual_segment_metadata_adapter(req)
    v = bsma.validate_bilingual_segment_metadata_result(res)
    _check("C::result_validates", v["ok"], json.dumps(v))
    _check("C::lang_counts",
           isinstance(res.get("language_segment_counts"), dict)
           and res["language_segment_counts"].get("en", 0) >= 1
           and res["language_segment_counts"].get("ru", 0) >= 1)
    _check("C::cs_boundary_count",
           res.get("code_switch_boundary_count") >= 1)
    _check("C::prosody_count",
           res.get("prosody_marker_count") >= 1)
    _check("C::pron_count",
           res.get("pronunciation_hint_count") >= 1)
    _check("C::safety_count",
           res.get("safety_flag_count") == 0)
    for flag in ("produced_audio", "invoked_tts",
                  "used_subprocess", "used_network",
                  "wrote_files"):
        _check(f"C::flag_false:{flag}", res[flag] is False)
    for k in ("audio_bytes", "audio_path", "command", "subprocess",
              "powershell"):
        _check(f"C::no_field:{k}", k not in res)


def suite_d_selection_policy() -> None:
    import bilingual_voice_phase31_selection_policy as p31s
    import bilingual_voice_phase31_adapter_interface as p31i
    en_req = {"language_mode": "english_only",
              "spoken_render_payload": {
                  "language_mode": "english_only",
                  "segments": [{"segment_id": "s1", "text": "hello",
                                "language": "en"}]},
              "safety_summary": {}}
    mix_req = {"language_mode": "mixed_en_ru",
               "spoken_render_payload": {
                   "language_mode": "mixed_en_ru",
                   "segments": [
                       {"segment_id": "s1", "text": "hello",
                        "language": "en"},
                       {"segment_id": "s2", "text": "привет",
                        "language": "ru"}],
                   "code_switch_boundaries": [{"after": "s1"}]},
               "safety_summary": {}}
    safe_warn_req = {"language_mode": "english_only",
                      "spoken_render_payload": {
                          "language_mode": "english_only"},
                      "safety_summary": {"high_risk": True}}
    c_en = p31s.choose_phase31_adapter(en_req)
    _check("D::en_choice_ok", c_en["ok"])
    _check("D::en_dummy_allowed",
           c_en["chosen"]["adapter_type"] in (
               "dummy_metadata_adapter",
               "bilingual_segment_metadata_adapter"))
    c_mix = p31s.choose_phase31_adapter(mix_req)
    _check("D::mix_chooses_bilingual_segment",
           c_mix["chosen"]["adapter_type"] ==
           "bilingual_segment_metadata_adapter")
    # Preferred adapter wins
    c_pref = p31s.choose_phase31_adapter(
        en_req, preferred_adapter="bilingual_segment_metadata_adapter")
    _check("D::preferred_wins",
           c_pref["chosen"]["adapter_type"] ==
           "bilingual_segment_metadata_adapter"
           and c_pref["reason"] == "preferred_adapter_valid")
    # Invalid preferred adapter falls back to scoring
    c_bad_pref = p31s.choose_phase31_adapter(
        en_req, preferred_adapter="piper_real")
    _check("D::invalid_preferred_falls_back",
           c_bad_pref["ok"]
           and c_bad_pref["chosen"]["adapter_type"] in (
               "dummy_metadata_adapter",
               "bilingual_segment_metadata_adapter"))
    # Safety warning prefers bilingual_segment
    c_safe = p31s.choose_phase31_adapter(safe_warn_req)
    _check("D::safety_warn_bs_score",
           c_safe["score_summary"].get(
               "bilingual_segment_metadata_adapter", 0) >=
           c_safe["score_summary"].get(
               "dummy_metadata_adapter", 0))
    # Execution-flag adapter rejected
    bad_desc = p31i.create_phase31_adapter_descriptor(
        "dummy_metadata_adapter", "dummy_metadata_adapter")
    bad_desc["uses_subprocess"] = True
    rej = p31s.reject_disallowed_phase31_adapter(bad_desc)
    _check("D::exec_flag_rejected", rej["rejected"] is True)
    explain = p31s.explain_phase31_selection(c_mix)
    _check("D::explain_summary",
           isinstance(explain.get("summary"), str))


def suite_e_comparison() -> None:
    import bilingual_voice_phase31_adapter_comparison as p31c
    import bilingual_voice_dummy_metadata_adapter as dma
    import bilingual_segment_metadata_adapter as bsma
    descs = [dma.get_dummy_metadata_adapter_descriptor(),
             bsma.get_bilingual_segment_metadata_adapter_descriptor()]
    cmp_d = p31c.compare_adapter_descriptors(descs)
    _check("E::descriptors_compare", cmp_d["ok"]
           and cmp_d["count"] == 2)
    req = {"request_id": "x", "language_mode": "english_only",
           "segment_count": 0,
           "spoken_render_payload": {"language_mode": "english_only",
                                       "segments": []}}
    rd = dma.call_dummy_metadata_adapter(req)
    rb = bsma.call_bilingual_segment_metadata_adapter(req)
    cmp_r = p31c.compare_metadata_results(rd, rb)
    _check("E::results_compare", cmp_r["ok"]
           and cmp_r["boundary_equal"] is True)
    score = p31c.score_result_usefulness(rb)
    _check("E::score_present", isinstance(score.get("score"), float))
    gaps = p31c.identify_adapter_result_gaps(rd)
    _check("E::gaps_returned", isinstance(gaps.get("gaps"), list))
    text = (json.dumps(p31c.identify_adapter_result_gaps(rb))
            + json.dumps(p31c.score_result_usefulness(rb))).lower()
    for bypass in ("bypass", "skip safety", "disable boundary",
                   "ignore safety"):
        _check(f"E::no_bypass:{bypass}", bypass not in text)
    with tempfile.TemporaryDirectory() as td:
        out = p31c.write_adapter_comparison_report(
            {"cmp": cmp_r}, os.path.join(td, "c.json"))
        _check("E::report_writes", os.path.exists(out))


def suite_f_selection_receipt() -> None:
    import bilingual_voice_phase31_selection_receipt as p31r
    sel = {"chosen": {"adapter_name": "dummy_metadata_adapter",
                       "adapter_type": "dummy_metadata_adapter"},
           "reason": "highest_score:single_language_payload",
           "candidate_adapters": [
               "dummy_metadata_adapter",
               "bilingual_segment_metadata_adapter"],
           "score_summary": {"dummy_metadata_adapter": 0.75,
                              "bilingual_segment_metadata_adapter":
                                  0.55}}
    req = {"request_id": "p31req_x"}
    rs = {"result_id": "dres_x", "produced_audio": False,
          "invoked_tts": False, "used_subprocess": False,
          "used_network": False, "wrote_files": False}
    rec = p31r.create_selection_receipt(req, sel, adapter_result=rs)
    v = p31r.validate_selection_receipt(rec)
    _check("F::receipt_validates", v["ok"], json.dumps(v))
    _check("F::selected_adapter_recorded",
           rec["selected_adapter_name"] ==
           "dummy_metadata_adapter")
    _check("F::candidate_list",
           isinstance(rec["candidate_adapters"], list)
           and len(rec["candidate_adapters"]) == 2)
    _check("F::execution_boundary_preserved",
           rec["execution_boundary_preserved"] is True)
    for k in ("audio_bytes", "command", "subprocess",
              "transcript", "operator_id"):
        _check(f"F::no_field:{k}", k not in rec)
    try:
        json.dumps(rec, default=str)
        ok = True
    except Exception:  # noqa: BLE001
        ok = False
    _check("F::json_serializable", ok)


def suite_g_post_call_equivalence() -> None:
    import bilingual_voice_phase31_post_call_equivalence as eq
    import bilingual_voice_dummy_metadata_adapter as dma
    import bilingual_segment_metadata_adapter as bsma
    req = {"language_mode": "english_only", "segment_count": 0,
           "spoken_render_payload": {"language_mode": "english_only",
                                       "segments": []}}
    rd = dma.call_dummy_metadata_adapter(req)
    rb = bsma.call_bilingual_segment_metadata_adapter(req)
    _check("G::dummy_boundary_ok",
           eq.validate_phase31_result_boundary(rd, req)["ok"])
    _check("G::bs_boundary_ok",
           eq.validate_phase31_result_boundary(rb, req)["ok"])
    bad = dict(rd)
    bad["produced_audio"] = True
    _check("G::audio_rejected",
           not eq.validate_phase31_result_boundary(bad)["ok"])
    bad2 = dict(rd)
    bad2["invoked_tts"] = True
    _check("G::tts_rejected",
           not eq.validate_phase31_result_boundary(bad2)["ok"])
    for flag in ("used_subprocess", "used_network", "wrote_files"):
        bad_x = dict(rd)
        bad_x[flag] = True
        _check(f"G::{flag}_rejected",
               not eq.validate_phase31_result_boundary(bad_x)["ok"])
    bad3 = dict(rd)
    bad3["received_language_mode"] = "russian_only"
    _check("G::mismatch_rejected",
           not eq.validate_phase31_result_boundary(bad3, req)["ok"])
    cmp_b = eq.compare_phase30_phase31_boundaries(rd, rb)
    _check("G::cross_boundary_equal", cmp_b["ok"])


def suite_h_runtime() -> None:
    import bilingual_voice_adapter_phase31_runtime as p31
    r_en = p31.prepare_phase31_multi_adapter_invocation(
        user_text="hello luna", draft_response_text="Hi.",
        operator_id="op_local", approve=True)
    _check("H::en_runs", r_en["status"] == "ok",
           json.dumps([e.get("code") for e in r_en.get("errors", [])]))
    rc = r_en.get("invocation_receipt") or {}
    _check("H::en_boundary_preserved",
           rc.get("execution_boundary_preserved") is True)
    r_ru = p31.prepare_phase31_multi_adapter_invocation(
        user_text="привет луна", draft_response_text="Привет!",
        user_preference="russian",
        operator_id="op_local", approve=True)
    _check("H::ru_runs", r_ru["status"] == "ok")
    r_mix = p31.prepare_phase31_multi_adapter_invocation(
        user_text="mix russian and english",
        draft_response_text="ok, давай.",
        operator_id="op_local", approve=True)
    _check("H::mix_runs", r_mix["status"] == "ok")
    # Preferred dummy when safe
    r_pref = p31.prepare_phase31_multi_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True,
        preferred_adapter="dummy_metadata_adapter")
    chosen = (r_pref.get("selection_choice") or {}).get(
        "chosen", {}).get("adapter_type")
    _check("H::preferred_dummy", chosen == "dummy_metadata_adapter")
    # approve=False → refused
    r_no = p31.prepare_phase31_multi_adapter_invocation(
        user_text="hi", approve=False)
    _check("H::approve_false_refused", r_no["status"] != "ok")
    # Kill switch
    r_ks = p31.prepare_phase31_multi_adapter_invocation(
        user_text="hello", operator_id="op_local",
        approve=True, kill_switch_enabled=True)
    _check("H::kill_switch_blocks",
           r_ks["status"] == "kill_switch_blocked")
    # Non-allowed preferred adapter
    r_real = p31.prepare_phase31_multi_adapter_invocation(
        user_text="hello", operator_id="op_local", approve=True,
        preferred_adapter="real_piper")
    # Falls back to scoring an allowed adapter
    chosen2 = (r_real.get("selection_choice") or {}).get(
        "chosen", {}).get("adapter_type")
    _check("H::non_allowed_falls_back_to_allowed",
           chosen2 in ("dummy_metadata_adapter",
                       "bilingual_segment_metadata_adapter"))
    # Demo bounded
    demo = p31.demo_phase31_multi_adapter_invocations(limit=4)
    _check("H::demo_bounded", demo["count"] == 4)
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
        _check("I::concepts_26", nc == 26)
        _check("I::links_52", nl == 52)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"), recursive=True)
        if "backups" not in p]
    _check("I::manifests_90", len(live) == 90, str(len(live)))
    audio = []
    for root, _dirs, files in os.walk(_ROOT / "bilingual_stack" /
                                       "voice_adapter_phase31"):
        for f in files:
            if f.lower().endswith((".wav", ".mp3", ".ogg",
                                    ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files", not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_phase31_adapter_interface.py",
        "bilingual_segment_metadata_adapter.py",
        "bilingual_voice_phase31_selection_policy.py",
        "bilingual_voice_phase31_adapter_comparison.py",
        "bilingual_voice_phase31_selection_receipt.py",
        "bilingual_voice_phase31_post_call_equivalence.py",
        "bilingual_voice_adapter_phase31_runtime.py",
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
        ("C", suite_c_bilingual_segment),
        ("D", suite_d_selection_policy),
        ("E", suite_e_comparison),
        ("F", suite_f_selection_receipt),
        ("G", suite_g_post_call_equivalence),
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
