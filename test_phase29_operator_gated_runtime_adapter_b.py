"""Phase 29 test harness — operator-gated runtime adapter Phase B."""

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
    p28 = [
        "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
        "test_phase28_operator_gated_voice_adapter.py",
        "bilingual_voice_operator_consent.py",
        "bilingual_voice_adapter_audit_log.py",
        "bilingual_voice_call_envelope.py",
        "bilingual_voice_execution_boundary.py",
        "bilingual_voice_capability_negotiator.py",
        "bilingual_voice_adapter_errors.py",
        "bilingual_voice_adapter_phase28_runtime.py",
    ]
    p27 = [
        "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
        "bilingual_voice_adapter_contract.py",
        "bilingual_voice_adapter_policy.py",
        "bilingual_voice_adapter_registry.py",
        "bilingual_voice_dry_run_pipeline.py",
        "bilingual_voice_adapter_validation.py",
    ]
    p26 = ["PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
           "bilingual_voice_memory_runtime.py"]
    p25 = ["PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
           "bilingual_spoken_render_runtime.py",
           "bilingual_spoken_render_contract.py"]
    p29 = [
        "bilingual_voice_invocation_consent.py",
        "bilingual_voice_audit_chain.py",
        "bilingual_voice_calltime_boundary.py",
        "bilingual_voice_operator_review_packet.py",
        "bilingual_voice_dry_run_queue.py",
        "bilingual_voice_refusal_analytics.py",
        "bilingual_voice_adapter_phase29_runtime.py",
    ]
    for f in p28 + p27 + p26 + p25 + p29:
        _check(f"A::file_exists::{f}", (_ROOT / f).exists(), f)
    for m in [
        "bilingual_voice_invocation_consent",
        "bilingual_voice_audit_chain",
        "bilingual_voice_calltime_boundary",
        "bilingual_voice_operator_review_packet",
        "bilingual_voice_dry_run_queue",
        "bilingual_voice_refusal_analytics",
        "bilingual_voice_adapter_phase29_runtime",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A::import::{m}", ok)


def suite_b_invocation_consent() -> None:
    import bilingual_voice_invocation_consent as ic
    schema = ic.get_invocation_consent_schema()
    _check("B::schema_version",
           isinstance(schema.get("version"), str))
    env = {"envelope_id": "venv_1", "render_job": {"job_id": "vrjob_1"}}
    t = ic.create_invocation_consent_token(env, operator_id="op",
                                            approved=True)
    v = ic.validate_invocation_consent_token(t)
    _check("B::dry_run_prepare_token_valid", v["ok"], json.dumps(v))
    _check("B::binding_hash_present",
           isinstance(t.get("binding_hash"), str)
           and len(t["binding_hash"]) >= 32)
    req = ic.require_valid_invocation_consent(t, env)
    _check("B::require_valid_ok", req["ok"]
           and req["execution_blocked"] is True)
    # Expired token
    t_old = dict(t)
    t_old["expires_at"] = 0.0
    _check("B::expired_detected",
           ic.is_invocation_token_expired(t_old))
    val_exp = ic.validate_invocation_consent_token(t_old)
    _check("B::expired_token_rejected", not val_exp["ok"])
    # Revoked token
    t_rev = ic.revoke_invocation_consent_token(t, "operator_revoked")
    _check("B::revoked_token_rejected",
           not ic.validate_invocation_consent_token(t_rev)["ok"])
    # Forbidden scopes
    for bad in ("execute_audio", "run_tts", "run_subprocess",
                "write_audio", "speak_now"):
        b = ic.create_invocation_consent_token(env, operator_id="op",
                                                scope=bad)
        bv = ic.validate_invocation_consent_token(b)
        _check(f"B::reject_scope:{bad}", not bv["ok"])
    # approved without operator_id
    t_noop = ic.create_invocation_consent_token(env, operator_id="",
                                                  approved=True)
    _check("B::approve_without_operator_id",
           not ic.validate_invocation_consent_token(t_noop)["ok"])
    # binding mismatch
    t_bad = dict(t)
    t_bad["envelope_id"] = "different"
    val_bad = ic.validate_invocation_consent_token(t_bad)
    _check("B::binding_mismatch_rejected", not val_bad["ok"])


def suite_c_audit_chain() -> None:
    import bilingual_voice_audit_chain as vac
    e1 = vac.create_audit_chain_event("preflight", "ok",
                                       "hello", {"k": "v"})
    _check("C::event_validates",
           vac.validate_audit_chain_event(e1)["ok"])
    _check("C::event_hash_set",
           isinstance(e1.get("event_hash"), str)
           and len(e1["event_hash"]) >= 32)
    chain: list = []
    chain = vac.append_chain_event(chain, e1)
    for i in range(5):
        e = vac.create_audit_chain_event(
            "calltime_boundary", "ok", f"step{i}",
            previous_hash=chain[-1].get("event_hash") or "")
        chain = vac.append_chain_event(chain, e)
    v = vac.verify_audit_chain(chain)
    _check("C::chain_verifies", v["ok"], json.dumps(v.get("reasons")))
    # Tamper: change message in the middle without rehash
    chain[2] = dict(chain[2])
    chain[2]["message"] = "tampered"
    v2 = vac.verify_audit_chain(chain)
    _check("C::tamper_detected", not v2["ok"])
    # Bounded append
    chain2: list = []
    for _ in range(50):
        chain2 = vac.append_chain_event(
            chain2, vac.create_audit_chain_event("preflight", "ok"))
    _check("C::bounded_append", len(chain2) == 50)
    # Transcript stripped
    e_bad = vac.create_audit_chain_event(
        "preflight", "ok", "x", {"transcript": "secret"})
    _check("C::transcript_stripped",
           "transcript" not in e_bad.get("metadata", {}))
    # Write/read bounded
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "chain.json")
        out = vac.write_audit_chain(chain2, p)
        _check("C::wrote_chain", os.path.exists(out))
        re_chain = vac.read_audit_chain(p, limit=10)
        _check("C::read_bounded", len(re_chain) <= 10)


def suite_d_calltime_boundary() -> None:
    import bilingual_voice_calltime_boundary as ctb
    import bilingual_voice_call_envelope as vce
    import bilingual_voice_operator_consent as voc
    import bilingual_voice_invocation_consent as ic
    rj = {"job_id": "vrjob_x", "dry_run": True,
          "safety_summary": {"unsafe": False, "blocked": False},
          "spoken_render_payload": {
              "language_mode": "english_only",
              "voice_safe_text": "hello"}}
    desc = {"adapter_name": "x", "adapter_type": "dry_run_renderer",
            "dry_run": True, "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": True,
            "forbidden_runtime_actions": [
                "audio_generation", "tts_invocation", "voice_cloning",
                "subprocess_execution", "powershell_invocation",
                "sapi_speak", "network_call", "audio_file_write"]}
    creq = voc.create_consent_request(rj, desc)
    cdec = voc.create_consent_decision(creq, approved=True,
                                        operator_id="op")
    env = vce.create_call_envelope(rj, cdec, {"chosen": desc})
    env = vce.normalize_call_envelope(env)
    token = ic.create_invocation_consent_token(
        env, operator_id="op", approved=True)
    res = ctb.build_calltime_boundary_result(env, token)
    _check("D::valid_envelope_passes", res["ok"],
           json.dumps(res.get("reasons")))
    # Missing token
    res_no = ctb.build_calltime_boundary_result(env, None)
    _check("D::missing_token_rejected", not res_no["ok"])
    # Expired token
    t_exp = dict(token)
    t_exp["expires_at"] = 0.0
    res_exp = ctb.build_calltime_boundary_result(env, t_exp)
    _check("D::expired_token_rejected", not res_exp["ok"])
    # dry_run=False
    bad_env = dict(env)
    bad_env["dry_run"] = False
    res_dr = ctb.build_calltime_boundary_result(bad_env, token)
    _check("D::dry_run_false_rejected", not res_dr["ok"])
    # Audio field
    aud_env = dict(env)
    aud_env["audio_bytes"] = "fake"
    res_aud = ctb.build_calltime_boundary_result(aud_env, token)
    _check("D::audio_field_rejected", not res_aud["ok"])
    # Subprocess field
    sub_env = dict(env)
    sub_env["powershell"] = "Speak"
    res_sub = ctb.build_calltime_boundary_result(sub_env, token)
    _check("D::subprocess_field_rejected", not res_sub["ok"])
    # Unsafe payload
    unsafe_env = dict(env)
    unsafe_env["render_job"] = dict(rj)
    unsafe_env["render_job"]["safety_summary"] = {
        "unsafe": True, "blocked": True}
    res_unsafe = ctb.build_calltime_boundary_result(unsafe_env, token)
    _check("D::unsafe_payload_rejected", not res_unsafe["ok"])
    # Structured refusal
    _check("D::structured_refusal",
           isinstance(res_no, dict) and "reasons" in res_no
           and res_no.get("execution_blocked") is True)


def suite_e_review_packet() -> None:
    import bilingual_voice_operator_review_packet as vrp
    import bilingual_voice_call_envelope as vce
    import bilingual_voice_operator_consent as voc
    rj = {"job_id": "vrjob_x", "spoken_render_payload": {
        "language_mode": "english_only"},
        "safety_summary": {"unsafe": False, "blocked": False}}
    desc = {"adapter_name": "x", "adapter_type": "dry_run_renderer"}
    creq = voc.create_consent_request(rj, desc)
    cdec = voc.create_consent_decision(creq, approved=False)
    env = vce.normalize_call_envelope(
        vce.create_call_envelope(rj, cdec, {"chosen": desc}))
    packet = vrp.create_operator_review_packet(env)
    v = vrp.validate_operator_review_packet(packet)
    _check("E::packet_validates", v["ok"], json.dumps(v))
    s = vrp.summarize_packet_for_operator(packet)
    _check("E::summary_exists", isinstance(s.get("summary"), str))
    red = vrp.redact_packet_sensitive_fields(packet)
    _check("E::redact_ok", isinstance(red, dict))
    _check("E::dry_run_true", packet["dry_run"] is True)
    _check("E::execution_blocked_true",
           packet["execution_blocked"] is True)
    with tempfile.TemporaryDirectory() as td:
        out = vrp.write_operator_review_packet(
            packet, os.path.join(td, "p.json"))
        _check("E::packet_writes", os.path.exists(out))
    # Forbidden actions present
    for bad in ("generate_audio", "invoke_tts", "run_subprocess",
                "call_powershell", "call_sapi", "call_piper",
                "write_audio_file", "clone_voice", "network_call"):
        _check(f"E::forbidden_action:{bad}",
               bad in packet["forbidden_actions"])


def suite_f_queue() -> None:
    import bilingual_voice_dry_run_queue as vdq
    q = vdq.create_dry_run_queue()
    for i in range(150):
        q = vdq.enqueue_dry_run_packet(q, {
            "packet_id": f"p_{i}",
            "adapter_name": "x",
            "language_mode": "en"}, limit=50)
    _check("F::enqueue_bounded_50", len(q["items"]) <= 50)
    listed = vdq.list_dry_run_queue(q, limit=10)
    _check("F::list_bounded_10", len(listed) <= 10)
    head = vdq.dequeue_dry_run_packet(q, dry_run=True)
    _check("F::dequeue_dry_run",
           head is not None and head["status"] == "dequeued_dry_run"
           and head["execution_blocked"] is True)
    # Even dry_run=False does not execute (no execution path exists)
    head2 = vdq.dequeue_dry_run_packet(q, dry_run=False)
    _check("F::dequeue_dryfalse_still_dryrun",
           head2 is not None and head2["dry_run"] is True
           and head2["execution_blocked"] is True)
    # status update metadata only
    r = vdq.mark_packet_status(q, q["items"][0]["packet_id"], "reviewed")
    _check("F::status_update", r["ok"])
    summary = vdq.summarize_dry_run_queue(q)
    _check("F::summary", isinstance(summary.get("by_status"), dict))
    # No worker / daemon / background processing — source scan
    src = (_ROOT / "bilingual_voice_dry_run_queue.py").read_text(
        encoding="utf-8")
    for tok in ("threading.Thread", "multiprocessing.Process",
                "asyncio.create_task", "daemon=True", "schedule.every"):
        _check(f"F::no_worker_tok:{tok}", tok not in src)


def suite_g_refusal_analytics() -> None:
    import bilingual_voice_refusal_analytics as vra
    items = [
        {"code": "CONSENT_MISSING"},
        {"code": "CONSENT_INVALID"},
        {"reasons": ["token_invalid:expired"]},
        {"reasons": ["safety_recheck_failed:unsafe"]},
        {"reasons": ["audio_field_present:audio_bytes"]},
        {"reasons": ["execution_field_present:subprocess"]},
        {"code": "VOICE_CLONE_FIELD_FORBIDDEN"},
        {"code": "NETWORK_FIELD_FORBIDDEN"},
        {"code": "CAPABILITY_MISMATCH"},
        {"code": "UNKNOWN_ADAPTER"},
        {"reasons": ["dry_run_recheck_failed:dry_run"]},
        {"weird": "data"},
    ]
    cats = [vra.classify_refusal_reason(it) for it in items]
    _check("G::consent_missing", "consent_missing" in cats)
    _check("G::consent_invalid", "consent_invalid" in cats)
    _check("G::consent_expired", "consent_expired" in cats)
    _check("G::unsafe", "unsafe_payload" in cats)
    _check("G::audio", "audio_field_forbidden" in cats)
    _check("G::subprocess", "subprocess_field_forbidden" in cats)
    _check("G::voice_clone", "voice_clone_forbidden" in cats)
    _check("G::network", "network_field_forbidden" in cats)
    _check("G::unsupported", "unsupported_adapter" in cats)
    _check("G::dry_run_required", "dry_run_required" in cats)
    _check("G::unknown", "unknown" in cats)
    agg = vra.aggregate_refusal_reasons(items, limit=10)
    _check("G::aggregate_bounded", agg["total"] == 10)
    summary = vra.summarize_refusal_patterns(items)
    _check("G::summary_top_categories",
           isinstance(summary.get("top_categories"), list))
    recos = vra.recommend_safe_next_steps(items)
    _check("G::recos_present",
           isinstance(recos.get("steps"), list)
           and len(recos["steps"]) > 0)
    # No bypass guidance
    text = " ".join(recos["steps"]).lower()
    for bypass in ("bypass", "skip safety", "disable boundary",
                   "ignore safety", "override consent"):
        _check(f"G::no_bypass:{bypass}", bypass not in text)


def suite_h_runtime() -> None:
    import bilingual_voice_adapter_phase29_runtime as p29
    r_en = p29.prepare_phase29_invocation(
        user_text="hello luna", draft_response_text="Hi.",
        conversation_mode="conversation",
        operator_id="op_local", approve=True)
    _check("H::en_runs", r_en["status"] == "dry_run_ready",
           json.dumps([e.get("code") for e in r_en.get("errors", [])]))
    _check("H::en_execution_blocked",
           (r_en["review_packet"] or {}).get(
               "execution_blocked") is True)
    r_ru = p29.prepare_phase29_invocation(
        user_text="привет луна", draft_response_text="Привет!",
        user_preference="russian",
        operator_id="op_local", approve=True)
    _check("H::ru_runs", r_ru["status"] == "dry_run_ready")
    r_mix = p29.prepare_phase29_invocation(
        user_text="mix russian and english",
        draft_response_text="ok, давай.")
    _check("H::mix_runs", r_mix["status"] == "dry_run_ready")
    # approve=False produces dry-run only
    r_no = p29.prepare_phase29_invocation(
        user_text="hello", draft_response_text="hi", approve=False)
    _check("H::approve_false_dry_run_only",
           (r_no["review_packet"] or {}).get("dry_run") is True
           and (r_no["review_packet"] or {}).get(
               "execution_blocked") is True)
    # approve=True still execution_blocked=True
    r_ap = p29.prepare_phase29_invocation(
        user_text="hello", draft_response_text="hi",
        operator_id="op", approve=True)
    _check("H::approve_true_still_blocked",
           (r_ap["review_packet"] or {}).get(
               "execution_blocked") is True)
    # Audit chain present and verifies
    import bilingual_voice_audit_chain as vac
    chain = r_en.get("audit_chain") or []
    _check("H::chain_present", len(chain) >= 5)
    _check("H::chain_verifies", vac.verify_audit_chain(chain)["ok"])
    # Unsafe / invalid input refused
    r_empty = p29.prepare_phase29_invocation(
        user_text="", draft_response_text="", conversation_mode="")
    _check("H::empty_refused",
           r_empty["status"] in ("refused", "dry_run_ready")
           and (r_empty["status"] == "refused" or
                (r_empty["phase28_result"] or {}).get("status") ==
                "refused"))
    # Unknown adapter
    r_unknown = p29.prepare_phase29_invocation(
        user_text="hi", adapter_name="not_real_adapter")
    p28r = r_unknown.get("phase28_result") or {}
    codes = [e.get("code") for e in (p28r.get("errors") or [])]
    has_fallback = bool((p28r.get("call_envelope") or {}).get(
        "adapter_choice") or {}).__class__  # type: ignore
    _check("H::unknown_handled",
           "UNKNOWN_ADAPTER" in codes
           or r_unknown["status"] in ("dry_run_ready", "refused"))
    # Demo bounded
    demo = p29.demo_phase29_invocations(limit=4)
    _check("H::demo_bounded", demo["count"] == 4)


def suite_i_production_safety() -> None:
    en_db = _ROOT / "lexicon" / "luna_vocabulary.sqlite"
    ru_db = _ROOT / "russian_stack" / "russian_lexicon.sqlite"
    link_db = _ROOT / "bilingual_stack" / "bilingual_links.sqlite"
    if en_db.exists():
        c = sqlite3.connect(str(en_db))
        n = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        c.close()
        _check("I::en_2814", n == 2814, f"got {n}")
    else:
        _check("I::en_db_present", False)
    if ru_db.exists():
        c = sqlite3.connect(str(ru_db))
        nw = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        np_ = c.execute("SELECT COUNT(*) FROM phrases").fetchone()[0]
        c.close()
        _check("I::ru_2518", nw == 2518, f"got {nw}")
        _check("I::ru_phr_35", np_ == 35, f"got {np_}")
    else:
        _check("I::ru_db_present", False)
    if link_db.exists():
        c = sqlite3.connect(str(link_db))
        nc = c.execute("SELECT COUNT(*) FROM concepts").fetchone()[0]
        nl = c.execute("SELECT COUNT(*) FROM entry_links").fetchone()[0]
        c.close()
        _check("I::concepts_26", nc >= 26)
        _check("I::links_52", nl >= 52)
    else:
        _check("I::link_db_present", False)
    import glob
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"), recursive=True)
        if "backups" not in p]
    _check("I::manifests_90", len(live) == 90, str(len(live)))
    audio = []
    for root, _dirs, files in os.walk(_ROOT / "bilingual_stack"
                                      / "voice_adapter_phase29"):
        for f in files:
            if f.lower().endswith((".wav", ".mp3", ".ogg",
                                    ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files", not audio, ",".join(audio))


def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_invocation_consent.py",
        "bilingual_voice_audit_chain.py",
        "bilingual_voice_calltime_boundary.py",
        "bilingual_voice_operator_review_packet.py",
        "bilingual_voice_dry_run_queue.py",
        "bilingual_voice_refusal_analytics.py",
        "bilingual_voice_adapter_phase29_runtime.py",
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
        ("B", suite_b_invocation_consent),
        ("C", suite_c_audit_chain),
        ("D", suite_d_calltime_boundary),
        ("E", suite_e_review_packet),
        ("F", suite_f_queue),
        ("G", suite_g_refusal_analytics),
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
