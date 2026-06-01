"""Phase 28 test harness — operator-gated voice adapter."""

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
    p27 = [
        "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
        "test_phase27_voice_render_adapter_skeleton.py",
        "bilingual_voice_adapter_contract.py",
        "bilingual_voice_adapter_policy.py",
        "bilingual_voice_adapter_registry.py",
        "bilingual_piper_adapter_contract.py",
        "bilingual_sapi_adapter_contract.py",
        "bilingual_voice_dry_run_pipeline.py",
        "bilingual_voice_adapter_validation.py",
    ]
    p26 = [
        "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
        "bilingual_voice_memory_runtime.py",
        "bilingual_voice_memory_state.py",
        "bilingual_voice_memory_schema.py",
    ]
    p25 = [
        "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
        "bilingual_spoken_render_runtime.py",
        "bilingual_voice_renderer_interface.py",
        "bilingual_spoken_render_contract.py",
    ]
    p28 = [
        "bilingual_voice_operator_consent.py",
        "bilingual_voice_adapter_audit_log.py",
        "bilingual_voice_call_envelope.py",
        "bilingual_voice_execution_boundary.py",
        "bilingual_voice_capability_negotiator.py",
        "bilingual_voice_adapter_errors.py",
        "bilingual_voice_adapter_phase28_runtime.py",
    ]
    for f in p27 + p26 + p25 + p28:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_operator_consent",
        "bilingual_voice_adapter_audit_log",
        "bilingual_voice_call_envelope",
        "bilingual_voice_execution_boundary",
        "bilingual_voice_capability_negotiator",
        "bilingual_voice_adapter_errors",
        "bilingual_voice_adapter_phase28_runtime",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_consent() -> None:
    import bilingual_voice_operator_consent as voc
    schema = voc.get_operator_consent_schema()
    _check("B::schema_version", isinstance(schema.get("version"), str))
    _check("B::dry_run_only_flag",
           schema.get("dry_run_only_in_phase28") is True)
    rj = {"job_id": "vrjob_x"}
    desc = {"adapter_name": "x_adapter", "adapter_type": "dry_run_renderer"}
    req = voc.create_consent_request(rj, desc,
                                     requested_action="dry_run_prepare")
    val = voc.validate_consent_request(req)
    _check("B::dry_run_prepare_validates", val["ok"], json.dumps(val))
    for bad in ("execute_audio", "run_tts", "run_subprocess",
                "write_audio", "speak_now"):
        b = voc.create_consent_request(rj, desc, requested_action=bad)
        bv = voc.validate_consent_request(b)
        _check(f"B::reject_{bad}", not bv["ok"])
    dec_no_op = voc.create_consent_decision(req, approved=True,
                                            operator_id="")
    dv = voc.validate_consent_decision(dec_no_op)
    _check("B::approve_without_operator_id_rejected", not dv["ok"])
    dec = voc.create_consent_decision(req, approved=True,
                                      operator_id="op1")
    dv2 = voc.validate_consent_decision(dec)
    _check("B::valid_decision_ok", dv2["ok"], json.dumps(dv2))
    _check("B::dry_run_only_enforced",
           dec.get("dry_run_only") is True)
    dro = voc.require_phase28_dry_run_only(dec)
    _check("B::require_phase28_dry_run_only",
           dro["dry_run_only"] is True)
    explain = voc.explain_consent_boundary(dec)
    _check("B::explain_returns_summary",
           isinstance(explain.get("summary"), str)
           and explain.get("execution_blocked") is True)


def suite_c_audit_log() -> None:
    import bilingual_voice_adapter_audit_log as val
    e = val.create_audit_event("preflight", "ok", "hello",
                               {"k": "v"})
    v = val.validate_audit_event(e)
    _check("C::event_validates", v["ok"], json.dumps(v))
    events: list[dict] = []
    for _ in range(10):
        events = val.append_audit_event(events, val.create_audit_event(
            "consent_request", "ok", "x"))
    _check("C::append_bounded", len(events) == 10)
    # bounded under cap
    for _ in range(2000):
        events = val.append_audit_event(events, val.create_audit_event(
            "boundary_guard", "ok"), limit=500)
    _check("C::cap_enforced", len(events) <= 500)
    summary = val.summarize_audit_events(events)
    _check("C::summary_count_matches", summary.get("count") == len(events))
    _check("C::summary_has_by_type",
           isinstance(summary.get("by_event_type"), dict))
    # transcript not persisted by default
    e_bad = val.create_audit_event("preflight", "ok", "x",
                                   {"transcript": "secret"})
    _check("C::transcript_stripped",
           "transcript" not in e_bad.get("metadata", {}))
    # write + read
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "log.json")
        out = val.write_audit_log(events, p)
        _check("C::wrote_log", os.path.exists(out))
        reread = val.read_audit_log(p, limit=100)
        _check("C::read_bounded", len(reread) <= 100)


def suite_d_call_envelope() -> None:
    import bilingual_voice_call_envelope as vce
    import bilingual_voice_operator_consent as voc
    schema = vce.get_call_envelope_schema()
    _check("D::schema_version",
           isinstance(schema.get("version"), str))
    rj = {"job_id": "vrjob_x", "dry_run": True}
    desc = {"adapter_name": "x", "adapter_type": "dry_run_renderer"}
    creq = voc.create_consent_request(rj, desc)
    cdec = voc.create_consent_decision(creq, approved=False,
                                       operator_id="")
    env = vce.create_call_envelope(rj, cdec, {"chosen": desc})
    v = vce.validate_call_envelope(env)
    _check("D::envelope_validates", v["ok"], json.dumps(v))
    _check("D::dry_run_true", env["dry_run"] is True)
    _check("D::execution_blocked_true",
           env["execution_blocked"] is True)
    for bad in ("generate_audio", "invoke_tts", "run_subprocess",
                "call_powershell", "call_sapi", "call_piper",
                "write_audio_file", "clone_voice", "network_call"):
        _check(f"D::forbidden_action_present:{bad}",
               bad in env["forbidden_actions"])
    # audio field rejected
    bad_env = dict(env)
    bad_env["audio_bytes"] = "fake"
    bv = vce.validate_call_envelope(bad_env)
    _check("D::audio_field_rejected", not bv["ok"])
    # JSON serializable
    try:
        json.dumps(env, default=str)
        ok_json = True
    except Exception:  # noqa: BLE001
        ok_json = False
    _check("D::json_serializable", ok_json)
    norm = vce.normalize_call_envelope({})
    _check("D::normalize_forces_dry_run", norm["dry_run"] is True
           and norm["execution_blocked"] is True)
    refused = vce.mark_envelope_refused(env, "test_reason")
    _check("D::mark_refused", refused["status"] == "refused")
    ready = vce.mark_envelope_dry_run_ready(env)
    _check("D::mark_dry_run_ready",
           ready["status"] == "dry_run_ready")


def suite_e_boundary() -> None:
    import bilingual_voice_execution_boundary as veb
    forbidden = veb.get_forbidden_execution_actions()
    _check("E::forbidden_list_nonempty", isinstance(forbidden, list)
           and len(forbidden) > 5)
    # Audio intent detected
    hits = veb.scan_for_execution_intent({"generate_audio": True})
    _check("E::generate_audio_detected", "generate_audio" in hits)
    # Subprocess key detected
    hits2 = veb.scan_for_execution_intent({"subprocess": "foo"})
    _check("E::subprocess_detected", "subprocess" in hits2)
    # Negation key NOT detected (false positive avoidance)
    hits3 = veb.scan_for_execution_intent({"no_subprocess": True,
                                           "supports_speak": False})
    _check("E::no_subprocess_false_positive_avoided",
           "subprocess" not in hits3)
    _check("E::supports_speak_false_positive_avoided",
           "speak" not in hits3)
    # Ambiguous execution intent in a VALUE (e.g. action="run_subprocess")
    val = veb.validate_no_execution_request({"action": "run_subprocess"})
    _check("E::value_run_subprocess_detected", not val["ok"])
    # Audio path key
    aud = veb.reject_if_audio_or_subprocess_requested(
        {"audio_path": "/tmp/x.wav"})
    _check("E::audio_path_rejected", aud["rejected"] is True)
    # Voice clone
    vc = veb.reject_if_audio_or_subprocess_requested(
        {"voice_clone_ref": "luna_voice"})
    _check("E::voice_clone_rejected", vc["rejected"] is True)
    # Boundary result structured
    r = veb.build_boundary_result({"powershell": "Speak('hi')"})
    _check("E::boundary_result_structured",
           isinstance(r, dict) and "execution_blocked" in r
           and "hits" in r and r["execution_blocked"] is True)
    # Clean payload passes
    safe = veb.build_boundary_result(
        {"output_policy": {"no_audio": True, "no_subprocess": True},
         "forbidden_runtime_actions": ["audio_generation"]})
    _check("E::clean_payload_passes", safe["ok"] is True)


def suite_f_capability() -> None:
    import bilingual_voice_capability_negotiator as vcn
    import bilingual_voice_adapter_contract as vac
    payload_mix = {
        "language_mode": "mixed_en_ru",
        "voice_safe_text": "hello привет",
        "segments": [
            {"segment_id": "s1", "text": "hello", "language": "en"},
            {"segment_id": "s2", "text": "привет", "language": "ru"},
        ],
        "code_switch_boundaries": [{"after": "s1"}],
        "safety_summary": {"unsafe": False, "blocked": False},
        "prosody": {"pace": "normal"},
    }
    req = vcn.extract_payload_requirements(payload_mix)
    _check("F::payload_requires_code_switching",
           req["requires_code_switching"] is True)
    cs_adapter = vac.create_voice_adapter_descriptor(
        "cs_adapter", "dry_run_renderer", capabilities={
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": True,
            "supports_segments": True,
            "supports_prosody": True,
            "supports_pronunciation_hints": True})
    caps = vcn.extract_adapter_capabilities(cs_adapter)
    _check("F::caps_dry_run", caps["dry_run"] is True)
    neg = vcn.negotiate_capabilities(payload_mix, cs_adapter)
    _check("F::mix_passes_with_cs_adapter", neg["ok"] is True,
           json.dumps(neg.get("unsupported_features")))
    nocs = vac.create_voice_adapter_descriptor(
        "nocs_adapter", "dry_run_renderer", capabilities={
            "supports_languages": ["en", "ru"],
            "supports_code_switching": False,
            "supports_segments": True})
    neg_nocs = vcn.negotiate_capabilities(payload_mix, nocs)
    _check("F::mix_shows_unsupported_with_nocs",
           "feature:code_switching" in neg_nocs["unsupported_features"])
    # No safety support → reject
    nosafe = vac.create_voice_adapter_descriptor(
        "nosafe", "dry_run_renderer", capabilities={
            "supports_languages": ["en"]})
    nosafe["forbidden_runtime_actions"] = []
    neg_nosafe = vcn.negotiate_capabilities(payload_mix, nosafe)
    _check("F::missing_safety_rejected",
           neg_nosafe["rejected"] is True
           and "missing_safety" in neg_nosafe["reason"])
    # Downgrade plan never strips safety/lang
    dg = vcn.propose_safe_downgrade_plan(payload_mix, nocs)
    _check("F::downgrade_keeps_safety",
           dg["strip_safety_metadata"] is False)
    _check("F::downgrade_keeps_lang",
           dg["strip_language_labels"] is False)
    _check("F::downgrade_metadata_notes",
           isinstance(dg.get("annotate_unsupported_features"), list))
    # Score
    s = vcn.score_negotiation_result(neg)
    _check("F::score_dict",
           isinstance(s.get("score"), float))


def suite_g_errors() -> None:
    import bilingual_voice_adapter_errors as vae
    codes = vae.get_voice_adapter_error_codes()
    expected_subset = ("PHASE28_EXECUTION_BLOCKED", "CONSENT_MISSING",
                       "CONSENT_INVALID", "UNSAFE_PAYLOAD",
                       "UNSUPPORTED_LANGUAGE_MODE",
                       "ADAPTER_DRY_RUN_REQUIRED",
                       "AUDIO_FIELD_FORBIDDEN",
                       "SUBPROCESS_FIELD_FORBIDDEN",
                       "NETWORK_FIELD_FORBIDDEN",
                       "VOICE_CLONE_FIELD_FORBIDDEN",
                       "CAPABILITY_MISMATCH",
                       "UNKNOWN_ADAPTER")
    for c in expected_subset:
        _check(f"G::code_present:{c}", c in codes)
    e = vae.create_adapter_error("PHASE28_EXECUTION_BLOCKED",
                                 "test", severity="blocking")
    _check("G::error_validates", vae.validate_adapter_error(e)["ok"])
    _check("G::blocking_detected", vae.is_blocking_error(e) is True)
    e2 = vae.create_adapter_error("CAPABILITY_MISMATCH",
                                  "test2", severity="warn")
    s = vae.summarize_adapter_errors([e, e2])
    _check("G::summary_count", s["count"] == 2)
    _check("G::summary_blocking_count", s["blocking_count"] == 1)
    # Invalid code falls back
    e3 = vae.create_adapter_error("NOT_A_CODE", "x")
    _check("G::unknown_code_falls_back",
           e3["code"] == "PAYLOAD_INVALID")


def suite_h_runtime() -> None:
    import bilingual_voice_adapter_phase28_runtime as p28
    r_en = p28.prepare_operator_gated_voice_call(
        user_text="hello luna",
        draft_response_text="I'm well.",
        conversation_mode="conversation")
    _check("H::en_runs", r_en.get("status") == "dry_run_ready",
           json.dumps([e.get("code") for e in r_en.get("errors", [])]))
    env_en = r_en.get("call_envelope") or {}
    _check("H::en_envelope_execution_blocked",
           env_en.get("execution_blocked") is True)
    _check("H::en_envelope_dry_run",
           env_en.get("dry_run") is True)
    r_ru = p28.prepare_operator_gated_voice_call(
        user_text="привет луна",
        draft_response_text="Привет!",
        conversation_mode="conversation",
        user_preference="russian")
    _check("H::ru_runs",
           r_ru.get("status") == "dry_run_ready",
           json.dumps([e.get("code") for e in r_ru.get("errors", [])]))
    r_mix = p28.prepare_operator_gated_voice_call(
        user_text="mix english and russian",
        draft_response_text="sure, давай.",
        conversation_mode="conversation")
    _check("H::mix_runs",
           r_mix.get("status") == "dry_run_ready",
           json.dumps([e.get("code") for e in r_mix.get("errors", [])]))
    # approve=False produces dry-run only envelope
    r_noapprove = p28.prepare_operator_gated_voice_call(
        user_text="hi", draft_response_text="hello",
        approve=False)
    env_no = r_noapprove.get("call_envelope") or {}
    _check("H::approve_false_dry_run",
           env_no.get("execution_blocked") is True
           and env_no.get("dry_run") is True)
    # approve=True still cannot execute
    r_approve = p28.prepare_operator_gated_voice_call(
        user_text="hi", draft_response_text="hello",
        operator_id="op_local", approve=True)
    env_ap = r_approve.get("call_envelope") or {}
    _check("H::approve_true_still_no_exec",
           env_ap.get("execution_blocked") is True
           and env_ap.get("dry_run") is True)
    # Unsafe input → refused
    r_empty = p28.prepare_operator_gated_voice_call(
        user_text="", draft_response_text="", conversation_mode="")
    _check("H::invalid_input_refused",
           r_empty.get("status") == "refused"
           or r_empty.get("call_envelope", {}).get(
               "status") == "refused")
    # Adapter name valid (kokoro_shaped_dry_run is built-in)
    r_kk = p28.prepare_operator_gated_voice_call(
        user_text="hello", adapter_name="kokoro_shaped_dry_run")
    env_kk = r_kk.get("call_envelope") or {}
    _check("H::kokoro_selected",
           (env_kk.get("adapter_choice") or {}).get(
               "chosen", {}).get("adapter_name") ==
           "kokoro_shaped_dry_run")
    # Unknown adapter name produces UNKNOWN_ADAPTER OR safe fallback
    r_unknown = p28.prepare_operator_gated_voice_call(
        user_text="hello", adapter_name="not_a_real_adapter")
    codes = [e.get("code") for e in r_unknown.get("errors", [])]
    env_un = r_unknown.get("call_envelope") or {}
    has_fallback = bool((env_un.get("adapter_choice") or {}).get(
        "chosen"))
    _check("H::unknown_adapter_handled",
           "UNKNOWN_ADAPTER" in codes or has_fallback)
    # Demo bounded
    demo = p28.demo_phase28_operator_gated_calls(limit=4)
    _check("H::demo_bounded", demo["count"] == 4)


def suite_i_production_safety() -> None:
    en_db = _ROOT / "lexicon" / "luna_vocabulary.sqlite"
    ru_db = _ROOT / "russian_stack" / "russian_lexicon.sqlite"
    link_db = _ROOT / "bilingual_stack" / "bilingual_links.sqlite"
    if en_db.exists():
        c = sqlite3.connect(str(en_db))
        n = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        c.close()
        _check("I::en_words_unchanged_2814", n == 2814, f"got {n}")
    else:
        _check("I::en_db_present", False, "missing")
    if ru_db.exists():
        c = sqlite3.connect(str(ru_db))
        nw = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        np_ = c.execute("SELECT COUNT(*) FROM phrases").fetchone()[0]
        c.close()
        _check("I::ru_words_unchanged_2518", nw == 2518, f"got {nw}")
        _check("I::ru_phrases_unchanged_35", np_ == 35, f"got {np_}")
    else:
        _check("I::ru_db_present", False, "missing")
    if link_db.exists():
        c = sqlite3.connect(str(link_db))
        nc = c.execute("SELECT COUNT(*) FROM concepts").fetchone()[0]
        nl = c.execute("SELECT COUNT(*) FROM entry_links").fetchone()[0]
        c.close()
        _check("I::link_concepts_unchanged_26", nc == 26, f"got {nc}")
        _check("I::link_entry_links_unchanged_52", nl == 52,
               f"got {nl}")
    else:
        _check("I::link_db_present", False, "missing")
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"), recursive=True)
        if "backups" not in p]
    _check("I::live_manifests_eq_90", len(live) == 90, str(len(live)))
    audio = []
    for root, _dirs, files in os.walk(_ROOT / "bilingual_stack"
                                      / "voice_adapter_phase28"):
        for f in files:
            if f.lower().endswith(
                    (".wav", ".mp3", ".ogg", ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files_in_phase28", not audio,
           ",".join(audio))


def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_operator_consent.py",
        "bilingual_voice_adapter_audit_log.py",
        "bilingual_voice_call_envelope.py",
        "bilingual_voice_execution_boundary.py",
        "bilingual_voice_capability_negotiator.py",
        "bilingual_voice_adapter_errors.py",
        "bilingual_voice_adapter_phase28_runtime.py",
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
        "luna_modules", "import worker", "from worker", "tier_",
        "probe_", "attestation",
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
        ("B", suite_b_consent),
        ("C", suite_c_audit_log),
        ("D", suite_d_call_envelope),
        ("E", suite_e_boundary),
        ("F", suite_f_capability),
        ("G", suite_g_errors),
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
        for f in _FAILURES[:60]:
            print(f)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
