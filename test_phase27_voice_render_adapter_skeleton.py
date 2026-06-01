"""Phase 27 test harness — voice-render adapter skeleton.

10 suites + regression list. No audio, no subprocess, no TTS imports.
"""

from __future__ import annotations

import importlib
import json
import os
import re
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
        msg = f"{name}: FAIL {detail}".strip()
        _FAILURES.append(msg)


_ROOT = Path(__file__).resolve().parent


# ---------- A_PREFLIGHT ----------
def suite_a_preflight() -> None:
    p25 = [
        "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
        "test_phase25_spoken_render_contract.py",
        "bilingual_spoken_render_contract.py",
        "bilingual_voice_text_normalizer.py",
        "bilingual_prosody_markup.py",
        "bilingual_pronunciation_hinting.py",
        "bilingual_spoken_safety_redactor.py",
        "bilingual_voice_renderer_interface.py",
        "bilingual_spoken_render_runtime.py",
    ]
    p26 = [
        "PHASE26_VOICE_MEMORY_CONTINUITY_REPORT.md",
        "test_phase26_voice_memory_continuity.py",
        "bilingual_voice_memory_schema.py",
        "bilingual_voice_memory_state.py",
        "bilingual_voice_preference_extractor.py",
        "bilingual_voice_continuity_planner.py",
        "bilingual_voice_correction_memory.py",
        "bilingual_voice_continuity_store.py",
        "bilingual_voice_memory_runtime.py",
    ]
    p24 = [
        "PHASE24_BILINGUAL_VOICE_PERSONALITY_REPORT.md",
        "bilingual_voice_style_runtime.py",
        "bilingual_voice_safety_filter.py",
    ]
    p23 = [
        "PHASE23_HUMAN_CODE_SWITCHING_REPORT.md",
        "bilingual_language_mode_detector.py",
        "bilingual_code_switch_policy.py",
    ]
    p22 = [
        "PHASE22_BILINGUAL_LINKER_AND_RETRIEVAL_BRIDGE_REPORT.md",
        "bilingual_retrieval_bridge.py",
    ]
    for f in p25 + p26 + p24 + p23 + p22:
        _check(f"A_PREFLIGHT::{f}_exists",
               (_ROOT / f).exists(), f)
    # link DB optional, check via stack path
    _check("A_PREFLIGHT::bilingual_links_sqlite_in_voice_adapter_optional",
           True, "intentionally optional in worktree")
    # Phase 27 created files exist
    for f in [
        "bilingual_voice_adapter_contract.py",
        "bilingual_voice_adapter_policy.py",
        "bilingual_voice_adapter_registry.py",
        "bilingual_piper_adapter_contract.py",
        "bilingual_sapi_adapter_contract.py",
        "bilingual_voice_dry_run_pipeline.py",
        "bilingual_voice_adapter_validation.py",
    ]:
        _check(f"A_PREFLIGHT::phase27_{f}_exists",
               (_ROOT / f).exists(), f)
    # Idempotent re-run: importable
    for m in [
        "bilingual_voice_adapter_contract",
        "bilingual_voice_adapter_policy",
        "bilingual_voice_adapter_registry",
        "bilingual_piper_adapter_contract",
        "bilingual_sapi_adapter_contract",
        "bilingual_voice_dry_run_pipeline",
        "bilingual_voice_adapter_validation",
    ]:
        try:
            importlib.import_module(m)
            ok = True
        except Exception as e:  # noqa: BLE001
            ok = False
            _FAILURES.append(f"import {m}: {e}")
        _check(f"A_PREFLIGHT::import_{m}", ok)


# ---------- B_ADAPTER_CONTRACT ----------
def suite_b_contract() -> None:
    import bilingual_voice_adapter_contract as vac
    schema = vac.get_voice_adapter_schema()
    _check("B::schema_version_present",
           isinstance(schema.get("version"), str))
    _check("B::supported_types_present",
           isinstance(vac.get_supported_adapter_types(), list)
           and len(vac.get_supported_adapter_types()) >= 5)
    _check("B::required_fields_present",
           isinstance(vac.get_required_adapter_fields(), list))
    d = vac.create_voice_adapter_descriptor(
        "test_dry_run", "dry_run_renderer",
        capabilities={
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": True,
            "supports_prosody": True,
            "supports_pronunciation_hints": True,
        })
    val = vac.validate_voice_adapter_descriptor(d)
    _check("B::valid_descriptor_validates",
           val["ok"], json.dumps(val))
    bad = vac.create_voice_adapter_descriptor(
        "non_dry", "dry_run_renderer", dry_run=True)
    bad["dry_run"] = False
    bad_val = vac.validate_voice_adapter_descriptor(bad)
    _check("B::non_dry_run_descriptor_rejected", not bad_val["ok"])
    payload = {
        "language_mode": "english_only",
        "voice_safe_text": "hello",
        "segments": [{"segment_id": "seg_0001_aa", "text": "hello",
                      "language": "en", "segment_type": "phrase"}],
        "safety_summary": {"blocked": False, "unsafe": False},
    }
    job = vac.create_render_job(payload, d)
    jv = vac.validate_render_job(job)
    _check("B::valid_render_job_validates",
           jv["ok"], json.dumps(jv))
    try:
        json.dumps(job, default=str)
        ok_json = True
    except Exception:  # noqa: BLE001
        ok_json = False
    _check("B::render_job_json_serializable", ok_json)
    # Audio field injected → rejected
    bad_job = dict(job)
    bad_job["audio_bytes"] = "FAKE"
    bj = vac.validate_render_job(bad_job)
    _check("B::audio_field_rejected", not bj["ok"])
    # External command field → rejected
    bad_job2 = dict(job)
    bad_job2["powershell"] = "Speak('hi')"
    bj2 = vac.validate_render_job(bad_job2)
    _check("B::external_command_field_rejected", not bj2["ok"])
    # Normalize forces dry_run flags
    deranged = dict(job)
    deranged["dry_run"] = False
    deranged["output_policy"] = {}
    norm = vac.normalize_render_job(deranged)
    _check("B::normalize_forces_dry_run", norm["dry_run"] is True)
    _check("B::normalize_forces_output_policy",
           all(norm["output_policy"].get(k) for k in
               ["no_audio", "no_subprocess", "no_network",
                "no_voice_clone", "no_audio_file_write", "plan_only"]))
    # report writes
    with tempfile.TemporaryDirectory() as td:
        out = vac.write_voice_adapter_contract_report(
            {"name": "smoke"}, os.path.join(td, "r.json"))
        _check("B::report_written", os.path.exists(out))


# ---------- C_ADAPTER_POLICY ----------
def suite_c_policy() -> None:
    import bilingual_voice_adapter_contract as vac
    import bilingual_voice_adapter_policy as vap
    pol = vap.get_adapter_selection_policy()
    _check("C::policy_has_version",
           isinstance(pol.get("version"), str))
    # Mixed payload prefers code-switch-capable
    payload_mixed = {
        "language_mode": "mixed_en_ru",
        "voice_safe_text": "hello привет",
        "segments": [
            {"segment_id": "s1", "text": "hello", "language": "en",
             "segment_type": "phrase"},
            {"segment_id": "s2", "text": "привет", "language": "ru",
             "segment_type": "phrase"},
        ],
        "code_switch_boundaries": [{"after": "s1"}],
        "safety_summary": {"blocked": False, "unsafe": False},
    }
    no_cs = vac.create_voice_adapter_descriptor(
        "no_cs", "dry_run_renderer", capabilities={
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": False})
    yes_cs = vac.create_voice_adapter_descriptor(
        "yes_cs", "dry_run_renderer", capabilities={
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": True,
            "supports_pronunciation_hints": True,
            "supports_prosody": True})
    choice = vap.choose_adapter_for_payload(
        payload_mixed, available_adapters=[no_cs, yes_cs])
    _check("C::mixed_prefers_code_switch",
           (choice.get("chosen") or {}).get("adapter_name") == "yes_cs",
           json.dumps(choice))
    # Russian payload prefers pronunciation-hint-capable
    payload_ru = {
        "language_mode": "russian_only",
        "voice_safe_text": "привет",
        "segments": [{"segment_id": "s1", "text": "привет", "language": "ru",
                      "segment_type": "phrase"}],
        "safety_summary": {"blocked": False, "unsafe": False},
    }
    no_ph = vac.create_voice_adapter_descriptor(
        "no_ph", "dry_run_renderer", capabilities={
            "supports_languages": ["en", "ru", "mixed"],
            "supports_pronunciation_hints": False})
    yes_ph = vac.create_voice_adapter_descriptor(
        "yes_ph", "dry_run_renderer", capabilities={
            "supports_languages": ["en", "ru", "mixed"],
            "supports_pronunciation_hints": True})
    choice_ru = vap.choose_adapter_for_payload(
        payload_ru, available_adapters=[no_ph, yes_ph])
    _check("C::ru_prefers_pronunciation_hints",
           (choice_ru.get("chosen") or {}).get("adapter_name") == "yes_ph")
    # Unsupported language → rejected
    payload_fr = {
        "language_mode": "french",
        "voice_safe_text": "bonjour",
        "segments": [{"segment_id": "s1", "text": "bonjour",
                      "language": "fr", "segment_type": "phrase"}],
        "safety_summary": {"blocked": False, "unsafe": False},
    }
    only_en_ru = vac.create_voice_adapter_descriptor(
        "only_en_ru", "dry_run_renderer", capabilities={
            "supports_languages": ["en", "ru"]})
    cscore = vap.score_adapter_compatibility(payload_fr, only_en_ru)
    _check("C::unsupported_language_rejected", not cscore["compatible"])
    # Non-dry-run rejected by safety policy
    nondry = vac.create_voice_adapter_descriptor(
        "nondry", "dry_run_renderer")
    nondry["dry_run"] = False
    safe = vap.enforce_adapter_safety_policy(nondry, payload_mixed)
    _check("C::non_dry_run_rejected_by_safety_policy", not safe["allowed"])
    # Forbidden actions missing → rejected
    missing_blocks = vac.create_voice_adapter_descriptor(
        "missing_blocks", "dry_run_renderer")
    missing_blocks["forbidden_runtime_actions"] = ["audio_generation"]
    safe2 = vap.enforce_adapter_safety_policy(missing_blocks, payload_mixed)
    _check("C::missing_blocks_rejected", not safe2["allowed"])
    # Runtime exec attempt always rejected
    rej = vap.reject_runtime_execution_attempt(yes_cs)
    _check("C::runtime_exec_always_rejected", rej["rejected"] is True)
    # Explanation produced
    explain = vap.explain_adapter_choice(choice)
    _check("C::adapter_explanation_produced",
           explain.get("ok") is True and "summary" in explain)


# ---------- D_REGISTRY ----------
def suite_d_registry() -> None:
    import bilingual_voice_adapter_registry as vreg
    builtins = vreg.get_builtin_dry_run_adapters()
    names = [d["adapter_name"] for d in builtins]
    expected = [
        "dry_run_basic", "dry_run_code_switch",
        "piper_shaped_dry_run", "sapi_shaped_dry_run",
        "kokoro_shaped_dry_run", "local_renderer_shaped_dry_run",
    ]
    for e in expected:
        _check(f"D::builtin_{e}_exists", e in names)
    _check("D::all_builtins_dry_run",
           all(d["dry_run"] is True for d in builtins))
    listed = vreg.list_registered_adapters(limit=3)
    _check("D::listing_bounded", len(listed) == 3)
    f = vreg.find_adapter_by_name("dry_run_basic")
    _check("D::find_by_name_works", f is not None and
           f.get("adapter_name") == "dry_run_basic")
    f_miss = vreg.find_adapter_by_name("does_not_exist")
    _check("D::find_by_name_miss_returns_none", f_miss is None)
    v = vreg.validate_registry()
    _check("D::default_registry_valid", v["ok"], json.dumps(v))
    # No engine imports — scan source for forbidden imports
    src = (_ROOT / "bilingual_voice_adapter_registry.py").read_text(
        encoding="utf-8")
    for tok in ["import piper", "import pyttsx3", "import comtypes",
                "edge_tts", "from piper", "subprocess.run", "os.system"]:
        _check(f"D::no_engine_import:{tok}", tok not in src)


# ---------- E_PIPER ----------
def suite_e_piper() -> None:
    import bilingual_piper_adapter_contract as pip_
    caps = pip_.get_piper_shaped_capabilities()
    _check("E::piper_caps_exist", isinstance(caps, dict) and
           caps.get("supports_languages"))
    desc = pip_.create_piper_shaped_descriptor()
    _check("E::piper_desc_dry_run", desc.get("dry_run") is True)
    payload_en = {
        "language_mode": "english_only",
        "voice_safe_text": "hello world",
        "segments": [{"segment_id": "s1", "text": "hello",
                      "language": "en", "segment_type": "phrase",
                      "emphasis": "strong", "pause_after_ms": 100,
                      "pronunciation_hint": "ˈhɛloʊ"}],
        "safety_summary": {"blocked": False, "unsafe": False},
    }
    comp = pip_.validate_piper_payload_compatibility(payload_en)
    _check("E::piper_en_compat", comp["ok"])
    plan = pip_.map_payload_to_piper_plan(payload_en)
    _check("E::piper_plan_ok", plan["ok"])
    _check("E::piper_plan_has_language_segments",
           plan["plan"].get("language_segments"))
    _check("E::piper_plan_has_prosody",
           plan["plan"].get("prosody_notes"))
    _check("E::piper_plan_has_pause",
           plan["plan"].get("pause_plan"))
    _check("E::piper_plan_has_pronunciation",
           plan["plan"].get("pronunciation_notes"))
    sim = pip_.simulate_piper_acceptance(payload_en)
    _check("E::piper_sim_dry_run", sim["dry_run"] is True
           and sim["no_audio_generated"] is True
           and sim["no_process_spawn"] is True
           and sim["no_audio_file_written"] is True)
    src = (_ROOT / "bilingual_piper_adapter_contract.py").read_text(
        encoding="utf-8")
    for tok in ["import piper", "from piper", "subprocess",
                "os.system", "shell=True"]:
        _check(f"E::no_piper_import:{tok}", tok not in src)


# ---------- F_SAPI ----------
def suite_f_sapi() -> None:
    import bilingual_sapi_adapter_contract as sap
    caps = sap.get_sapi_shaped_capabilities()
    _check("F::sapi_caps_exist", isinstance(caps, dict))
    desc = sap.create_sapi_shaped_descriptor()
    _check("F::sapi_desc_dry_run", desc.get("dry_run") is True)
    payload_en = {
        "language_mode": "english_only",
        "voice_safe_text": "hello",
        "segments": [{"segment_id": "s1", "text": "hello",
                      "language": "en", "segment_type": "phrase",
                      "pause_after_ms": 50}],
        "safety_summary": {"blocked": False, "unsafe": False},
    }
    comp = sap.validate_sapi_payload_compatibility(payload_en)
    _check("F::sapi_en_compat", comp["ok"])
    plan = sap.map_payload_to_sapi_plan(payload_en)
    _check("F::sapi_plan_ok", plan["ok"])
    _check("F::sapi_plan_has_language_segments",
           plan["plan"].get("language_segments"))
    sim = sap.simulate_sapi_acceptance(payload_en)
    _check("F::sapi_no_shell", sim["no_shell_invocation"] is True)
    _check("F::sapi_no_process_spawn", sim["no_process_spawn"] is True)
    _check("F::sapi_no_engine_called", sim["no_engine_called"] is True)
    _check("F::sapi_no_audio_file", sim["no_audio_file_written"] is True)
    src = (_ROOT / "bilingual_sapi_adapter_contract.py").read_text(
        encoding="utf-8")
    for tok in ["import pyttsx3", "import comtypes", "PowerShell",
                "subprocess", ".Speak(", "win32com", "os.system"]:
        _check(f"F::no_sapi_import:{tok}", tok not in src)


# ---------- G_PIPELINE ----------
def suite_g_pipeline() -> None:
    import bilingual_voice_dry_run_pipeline as pipe
    r_en = pipe.run_dry_run_pipeline(
        user_text="hello luna, how are you",
        draft_response_text="I'm well, thanks.",
        conversation_mode="conversation")
    _check("G::en_pipeline_planned",
           r_en["dry_run_status"] == "planned_dry_run",
           json.dumps(r_en.get("gap_notes", [])))
    _check("G::en_pipeline_has_render_job",
           bool(r_en.get("render_job")))
    r_ru = pipe.run_dry_run_pipeline(
        user_text="привет луна, расскажи мне новости",
        draft_response_text="Конечно, вот новости.",
        conversation_mode="conversation", user_preference="russian")
    _check("G::ru_pipeline_planned",
           r_ru["dry_run_status"] == "planned_dry_run")
    r_mix = pipe.run_dry_run_pipeline(
        user_text="mix english and russian please",
        draft_response_text="ok мы попробуем mixed mode.",
        conversation_mode="conversation")
    _check("G::mixed_pipeline_planned",
           r_mix["dry_run_status"] == "planned_dry_run")
    # voice-memory influence
    state = {
        "preferred_language_mode": "russian_only",
        "preferred_spoken_mode": "russian_only",
    }
    r_with_vm = pipe.run_dry_run_pipeline(
        user_text="speak russian please",
        draft_response_text="Хорошо.",
        conversation_state=state,
        conversation_mode="conversation")
    _check("G::vm_state_used",
           r_with_vm.get("voice_memory_summary", {}).get(
               "preferred_language_mode") == "russian_only")
    _check("G::adapter_chosen",
           r_with_vm.get("adapter_choice", {}).get("chosen") is not None)
    # Unsafe payload rejected via input check
    r_blank = pipe.run_dry_run_pipeline(
        user_text="", draft_response_text="",
        conversation_mode="conversation")
    _check("G::empty_input_rejected",
           r_blank["dry_run_status"] == "rejected_invalid_input")
    # Demo bounded
    demo = pipe.demo_dry_run_voice_pipeline(limit=4)
    _check("G::demo_bounded", demo["count"] == 4)
    # No persistent memory writes — pipeline does not call store APIs
    src = (_ROOT / "bilingual_voice_dry_run_pipeline.py").read_text(
        encoding="utf-8")
    _check("G::no_continuity_store_writes",
           "save_voice_session_state" not in src
           and "write_voice_continuity_event" not in src)
    _check("G::no_main_runtime_integration",
           "luna_modules" not in src and "worker.py" not in src)


# ---------- H_VALIDATION ----------
def suite_h_validation() -> None:
    import bilingual_voice_adapter_contract as vac
    import bilingual_voice_adapter_validation as vav
    desc = vac.create_voice_adapter_descriptor(
        "v_dry", "dry_run_renderer", capabilities={
            "supports_languages": ["en", "ru", "mixed"],
            "supports_code_switching": True,
            "supports_prosody": True})
    payload = {
        "language_mode": "english_only",
        "voice_safe_text": "hello",
        "segments": [{"segment_id": "s1", "text": "hello",
                      "language": "en", "segment_type": "phrase"}],
        "safety_summary": {"blocked": False, "unsafe": False},
    }
    job = vac.create_render_job(payload, desc)
    job = vac.normalize_render_job(job)
    b = vav.validate_adapter_boundary(job)
    _check("H::valid_job_passes_boundary", b["ok"], json.dumps(b))
    # audio field
    bad = dict(job)
    bad["audio_bytes"] = "data"
    a = vav.validate_no_audio_payload(bad)
    _check("H::audio_field_detected", not a["ok"])
    # exec field
    bad2 = dict(job)
    bad2["run_command"] = "speak.exe"
    e = vav.validate_no_runtime_execution_fields(bad2)
    _check("H::exec_field_detected", not e["ok"])
    # non-dry-run
    bad3 = dict(job)
    bad3["dry_run"] = False
    d = vav.validate_dry_run_only(bad3)
    _check("H::non_dry_run_detected", not d["ok"])
    with tempfile.TemporaryDirectory() as td:
        out = vav.write_adapter_validation_report(
            {"name": "vsmoke"}, os.path.join(td, "v.json"))
        _check("H::report_written", os.path.exists(out))


# ---------- I_PRODUCTION_SAFETY ----------
def suite_i_production_safety() -> None:
    en_db = _ROOT / "lexicon" / "luna_vocabulary.sqlite"
    ru_db = _ROOT / "russian_stack" / "russian_lexicon.sqlite"
    link_db = _ROOT / "bilingual_stack" / "bilingual_links.sqlite"
    if en_db.exists():
        c = sqlite3.connect(str(en_db))
        n = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        c.close()
        _check("I::en_words_unchanged_2814", n == 2814,
               f"got {n}")
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
        _check("I::link_entry_links_unchanged_52", nl == 52, f"got {nl}")
    else:
        _check("I::link_db_present", False, "missing")
    # No manifests added/removed by Phase 27
    import glob
    n_man = len(glob.glob(str(_ROOT / "**" / "*pack_manifest*.json"),
                          recursive=True))
    # 90 live + 90 backed-up backups → use the not-in-backups slice
    live = [p for p in glob.glob(
        str(_ROOT / "**" / "*pack_manifest*.json"), recursive=True)
        if "/backups/" not in p.replace("\\", "/")]
    _check("I::live_manifests_eq_90", len(live) == 90,
           f"got {len(live)} of {n_man}")
    # incoming dirs still empty
    en_in = _ROOT / "corpus_sources" / "english" / "incoming"
    ru_in = _ROOT / "corpus_sources" / "russian" / "incoming"
    _check("I::phase21_en_incoming_still_empty",
           (not en_in.exists()) or len(list(en_in.iterdir())) == 0)
    _check("I::phase21_ru_incoming_still_empty",
           (not ru_in.exists()) or len(list(ru_in.iterdir())) == 0)
    # No audio files created
    audio = []
    for root, _dirs, files in os.walk(_ROOT / "bilingual_stack"
                                      / "voice_adapter"):
        for f in files:
            low = f.lower()
            if low.endswith((".wav", ".mp3", ".ogg", ".flac", ".m4a")):
                audio.append(os.path.join(root, f))
    _check("I::no_audio_files_in_voice_adapter",
           not audio, ",".join(audio))


# ---------- J_ISOLATION ----------
def suite_j_isolation() -> None:
    files = [
        "bilingual_voice_adapter_contract.py",
        "bilingual_voice_adapter_policy.py",
        "bilingual_voice_adapter_registry.py",
        "bilingual_piper_adapter_contract.py",
        "bilingual_sapi_adapter_contract.py",
        "bilingual_voice_dry_run_pipeline.py",
        "bilingual_voice_adapter_validation.py",
    ]
    forbidden_audio = (
        "pyttsx3", "gtts", "edge_tts", "piper.", "coqui",
        "whisper", "pyaudio", "sounddevice", "pydub",
        "soundfile", " wave ", "comtypes", "win32com",
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
            _check(f"J::{fn}::no_audio_token:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_exec:
            _check(f"J::{fn}::no_exec_token:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_net:
            _check(f"J::{fn}::no_net_token:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_runtime:
            _check(f"J::{fn}::no_runtime_token:{tok.strip()}",
                   tok not in src)
        for tok in forbidden_threading:
            _check(f"J::{fn}::no_daemon_token:{tok.strip()}",
                   tok not in src)


def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_a_preflight),
        ("B_CONTRACT", suite_b_contract),
        ("C_POLICY", suite_c_policy),
        ("D_REGISTRY", suite_d_registry),
        ("E_PIPER", suite_e_piper),
        ("F_SAPI", suite_f_sapi),
        ("G_PIPELINE", suite_g_pipeline),
        ("H_VALIDATION", suite_h_validation),
        ("I_PRODUCTION_SAFETY", suite_i_production_safety),
        ("J_ISOLATION", suite_j_isolation),
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
        for f in _FAILURES[:50]:
            print(f)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
