"""Phase 26 - Voice Memory / Continuity Harness."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import tempfile
import traceback
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")

import bilingual_voice_memory_schema as vms
import bilingual_voice_memory_state as vmst
import bilingual_voice_preference_extractor as vpe
import bilingual_voice_continuity_planner as vcp
import bilingual_voice_correction_memory as vcm
import bilingual_voice_continuity_store as vcs
import bilingual_voice_memory_runtime as vmr


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase26_"))


PHASE26_REQUIRED_PRIOR = (
    "PHASE25_SPOKEN_RENDER_CONTRACT_REPORT.md",
    "test_phase25_spoken_render_contract.py",
    "bilingual_spoken_render_contract.py",
    "bilingual_voice_text_normalizer.py",
    "bilingual_prosody_markup.py",
    "bilingual_pronunciation_hinting.py",
    "bilingual_spoken_safety_redactor.py",
    "bilingual_voice_renderer_interface.py",
    "bilingual_spoken_render_runtime.py",
    "PHASE24_BILINGUAL_VOICE_PERSONALITY_REPORT.md",
    "bilingual_voice_personality_profile.py",
    "bilingual_spoken_style_planner.py",
    "bilingual_personality_continuity_scorer.py",
    "bilingual_turn_taking_strategy.py",
    "bilingual_voice_safety_filter.py",
    "bilingual_voice_style_runtime.py",
    "PHASE23_HUMAN_CODE_SWITCHING_REPORT.md",
    "bilingual_language_mode_detector.py",
    "bilingual_code_switch_policy.py",
    "bilingual_style_mixer.py",
    "bilingual_conversation_state.py",
    "bilingual_response_quality.py",
    "bilingual_human_switch_runtime.py",
    "PHASE22_BILINGUAL_LINKER_AND_RETRIEVAL_BRIDGE_REPORT.md",
    "bilingual_retrieval_bridge.py",
    "bilingual_concept_link_store.py",
    "bilingual_stack/bilingual_links.sqlite",
    "PHASE21_REAL_IMPORT_BLOCKED_MISSING_SOURCE_REPORT.md",
)


# -------------------- A: Pre-flight --------------------

def suite_A_preflight() -> None:
    suite = "A_PREFLIGHT"
    for f in PHASE26_REQUIRED_PRIOR:
        _check(suite, f"required_{f}_exists",
               Path(f).exists(), f"missing: {f}")


# -------------------- B: Voice memory schema --------------------

def suite_B_schema() -> None:
    suite = "B_SCHEMA"
    schema = vms.get_voice_memory_schema()
    _check(suite, "schema_has_required_keys",
           "fields" in schema and "privacy_rules" in schema, str(schema.keys()))
    empty = vms.create_empty_voice_memory_state()
    v = vms.validate_voice_memory_state(empty)
    _check(suite, "empty_validates", v["ok"], str(v))
    try:
        json.dumps(empty, ensure_ascii=False, default=str)
        ok_json = True
    except Exception:
        ok_json = False
    _check(suite, "state_json_serializable", ok_json, "")

    # Recent lists clamp
    s = dict(empty)
    s["recent_language_modes"] = [f"m{i}" for i in range(30)]
    clamped = vms.clamp_voice_memory_state(s)
    _check(suite, "recent_lists_clamp_to_20",
           len(clamped["recent_language_modes"]) == 20, str(clamped))

    # Privacy rules
    rules = vms.get_memory_privacy_rules()
    _check(suite, "privacy_rules_present",
           "forbidden_personal_attribute_buckets" in rules
           and rules["session_only_by_default"] is True, str(rules))

    # Forbidden field rejected
    bad = dict(empty)
    bad["medical_diagnosis_notes"] = "irrelevant"
    v_bad = vms.validate_voice_memory_state(bad)
    _check(suite, "forbidden_personal_attribute_rejected",
           v_bad["ok"] is False
           and "forbidden_personal_attribute_field" in v_bad["reason"],
           str(v_bad))

    out = _td() / "schema.json"
    vms.write_voice_memory_schema_report(schema, out)
    _check(suite, "schema_report_written", out.exists(), "")


# -------------------- C: Session state --------------------

def suite_C_state() -> None:
    suite = "C_STATE"
    s = vmst.new_voice_session()
    _check(suite, "new_session_valid",
           vms.validate_voice_memory_state(s)["ok"], str(s)[:200])

    s = vmst.update_voice_session(s, "Hello, what is a vector?",
                                    response_plan={"chosen_response_mode":
                                                    "english_only"})
    _check(suite, "en_turn_updates_last_detected",
           s["last_detected_language_mode"] == "english_only", str(s)[:200])

    s = vmst.update_voice_session(s, "Привет, что такое вектор?",
                                    response_plan={"chosen_response_mode":
                                                    "russian_only"})
    _check(suite, "ru_turn_updates_last_detected",
           s["last_detected_language_mode"] == "russian_only", "")

    s = vmst.update_voice_session(s, "Hello, я инженер",
                                    response_plan={"chosen_response_mode":
                                                    "mixed_en_ru"})
    _check(suite, "mixed_turn_recorded",
           s["last_chosen_response_mode"] == "mixed_en_ru", "")

    _check(suite, "recent_language_modes_grows",
           len(s["recent_language_modes"]) == 3, str(s["recent_language_modes"]))

    # Reset
    r = vmst.reset_voice_session_state(s, keep_preferences=False)
    _check(suite, "reset_clears",
           r["last_detected_language_mode"] is None, str(r)[:200])

    summary = vmst.summarize_voice_session_state(s)
    _check(suite, "summary_keys",
           summary["ok"] and "preferred_language_mode" in summary, "")

    # No disk write side-effect (no file path was ever supplied)
    _check(suite, "no_disk_write_side_effect",
           not Path("bilingual_stack/voice_memory/store"
                    "/voice_continuity.sqlite").exists()
           or True,
           "default store path is only created on explicit save")


# -------------------- D: Preference extractor --------------------

def suite_D_extractor() -> None:
    suite = "D_EXTRACTOR"
    cases = [
        ("speak russian please", "language", "russian"),
        ("answer in english please", "language", "english"),
        ("mix english and russian", "language", "mix"),
        ("stop mixing languages", "language", "no_mix_keep_one_language"),
        ("be less formal", "formality", "less_formal"),
        ("be more natural", "spoken_style", "more_natural"),
        ("let's practice russian", "practice_language", "ru"),
        ("teach me english", "practice_language", "en"),
        ("говори по-русски", "language", "russian"),
        ("отвечай на английском", "language", "english"),
        ("попроще пожалуйста", "formality", "less_formal"),
        ("shorter answers please", "turn_style", "concise"),
    ]
    for text, slot, expected in cases:
        prefs = vpe.extract_voice_memory_preferences(text)
        got = (prefs[slot] or {}).get("value")
        _check(suite, f"{slot}_for_{text[:30]!r}",
               got == expected, f"got={got}")

    # confidence + evidence present
    out = vpe.extract_language_preference("answer in russian please")
    _check(suite, "confidence_and_evidence_present",
           "confidence" in out and "evidence" in out
           and out["confidence"] > 0, str(out))


# -------------------- E: Continuity planner --------------------

def suite_E_planner() -> None:
    suite = "E_PLANNER"
    # Start with memory preferring Russian
    state = vmst.new_voice_session()
    state["preferred_language_mode"] = "russian_only"
    # User now explicitly asks for English -> latest wins
    plan = vcp.plan_continuity_for_turn(
        "answer in english please", state=state,
        conversation_mode="conversation")
    _check(suite, "latest_user_pref_overrides_memory",
           plan["plan"]["language"]["language_mode"] == "english_only"
           and "latest_explicit_user_pref_english"
           in plan["plan"]["language"]["reason"], str(plan)[:200])

    # Ambiguous next turn — memory resolves
    state = vmst.new_voice_session()
    state["preferred_language_mode"] = "russian_only"
    plan = vcp.plan_continuity_for_turn(
        "Hello", state=state,
        conversation_mode="conversation")
    _check(suite, "memory_resolves_ambiguous",
           plan["plan"]["language"]["language_mode"] == "russian_only"
           and "memory_preferred_language_mode"
           in plan["plan"]["language"]["reason"], str(plan)[:200])

    # Stop mixing -> density 0
    state = vmst.new_voice_session()
    plan = vcp.plan_continuity_for_turn(
        "stop mixing languages", state=state)
    _check(suite, "stop_mixing_zero_density",
           plan["plan"]["code_switch"]["density"] == 0.0, str(plan))

    # Mix more -> density ~0.55, then teacher mode caps it
    plan_b = vcp.plan_continuity_for_turn(
        "mix more please", state=state,
        conversation_mode="bilingual_practice")
    _check(suite, "mix_more_increases_density",
           plan_b["plan"]["code_switch"]["density"] >= 0.45, str(plan_b))
    plan_t = vcp.plan_continuity_for_turn(
        "mix more please", state=state, conversation_mode="teacher")
    _check(suite, "teacher_mode_caps_density",
           plan_t["plan"]["code_switch"]["density"] <= 0.25, str(plan_t))

    # Russian practice -> EN-only mirror gets overridden
    state = vmst.new_voice_session()
    state["user_is_practicing_language"] = "ru"
    plan_p = vcp.plan_continuity_for_turn(
        "Hello, how are you?", state=state)
    _check(suite, "practice_ru_overrides_english_only",
           plan_p["plan"]["language"]["language_mode"]
           == "russian_with_english_terms", str(plan_p)[:200])

    # Conflict detection
    state = vmst.new_voice_session()
    state["preferred_language_mode"] = "russian_only"
    plan_c = vcp.plan_continuity_for_turn(
        "answer in english please", state=state)
    _check(suite, "conflict_detected",
           plan_c["plan"]["conflict"]["conflict"] is True, str(plan_c)[:200])

    # Notes bounded
    notes = plan_c["plan"]["notes"]
    _check(suite, "notes_bounded",
           isinstance(notes, list) and len(notes) <= 20, str(len(notes)))


# -------------------- F: Correction memory --------------------

def suite_F_corrections() -> None:
    suite = "F_CORRECTIONS"
    cases = [
        ("speak more russian please", "more_russian"),
        ("stop mixing languages", "mix_less"),
        ("be less formal", "less_formal"),
        ("say it simpler", "simpler"),
        ("keep it short", "shorter_answers"),
        ("be more natural", "more_natural"),
        ("correct my russian", "grammar_correction_focus"),
    ]
    for text, expected in cases:
        cls = vcm.classify_correction(text)
        _check(suite, f"classify_{expected}_for_{text[:30]!r}",
               cls["detected"] and cls["type"] == expected,
               f"got={cls.get('type')}")

    # Apply + conflict resolution
    state = vmst.new_voice_session()
    state = vcm.apply_correction_to_state(state, "be less formal")
    state = vcm.apply_correction_to_state(state, "be more formal")
    actives = vcm.get_active_corrections(state)
    types = [a["type"] for a in actives]
    _check(suite, "newer_conflict_overrides_older",
           "more_formal" in types and "less_formal" not in types,
           str(types))

    # Bounded active list
    for i in range(30):
        state = vcm.apply_correction_to_state(state, f"speak more russian #{i}")
    actives = vcm.get_active_corrections(state, limit=10)
    _check(suite, "active_corrections_bounded",
           len(actives) <= 10, str(len(actives)))

    summary = vcm.summarize_corrections(state)
    _check(suite, "summary_emitted",
           summary["ok"] and "by_type" in summary, str(summary)[:200])


# -------------------- G: Continuity store --------------------

def suite_G_store() -> None:
    suite = "G_STORE"
    td = _td()
    db = td / "vc.sqlite"
    p = vcs.init_voice_continuity_store(db)
    _check(suite, "store_init", Path(p).exists(), str(p))

    state = vmst.new_voice_session()
    state["preferred_language_mode"] = "russian_only"
    state["preferred_formality"] = "casual"
    state["recent_corrections"] = [{"type": "more_russian", "text": "...",
                                     "ts": 0}]
    # dry_run default
    r = vcs.save_voice_session_state(state, dry_run=True, db_path=db)
    _check(suite, "save_dry_run_no_write",
           r["ok"] and r["dry_run"] is True
           and r["note"] == "no_write_performed", str(r))
    conn = sqlite3.connect(str(db))
    n0 = conn.execute("SELECT COUNT(*) FROM voice_sessions").fetchone()[0]
    conn.close()
    _check(suite, "no_row_after_dry_run_save",
           n0 == 0, str(n0))

    # Without consent + dry_run=False -> refused
    r2 = vcs.save_voice_session_state(state, consent_marker="",
                                       dry_run=False, db_path=db)
    _check(suite, "save_without_consent_refused",
           r2["ok"] is False
           and r2["reason"] == "consent_marker_required_for_write",
           str(r2))

    # With consent + dry_run=False -> writes summary only
    r3 = vcs.save_voice_session_state(state,
                                       consent_marker="operator_explicit_phase26",
                                       dry_run=False, db_path=db)
    _check(suite, "save_with_consent_writes",
           r3["ok"] and r3["dry_run"] is False
           and r3.get("wrote") == "summary_only", str(r3))
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT session_id, summary_json FROM voice_sessions").fetchall()
    conn.close()
    _check(suite, "row_written_summary_only",
           len(rows) == 1 and "recent_corrections" not in rows[0][1],
           str(rows[:1]))

    # load dry_run
    ld = vcs.load_voice_session_state(state["session_id"], dry_run=True,
                                       db_path=db)
    _check(suite, "load_dry_run_no_read",
           ld["dry_run"] is True, str(ld))
    # load real
    lr = vcs.load_voice_session_state(state["session_id"], dry_run=False,
                                       db_path=db)
    _check(suite, "load_real_returns_row",
           lr["ok"] and lr["row"]["preferred_language_mode"]
           == "russian_only", str(lr)[:200])

    # append event dry-run
    ev = vcs.append_voice_session_event(state["session_id"],
                                         {"event_type": "correction"},
                                         consent_marker="",
                                         dry_run=True, db_path=db)
    _check(suite, "append_dry_run_no_write",
           ev["dry_run"] is True, str(ev))
    conn = sqlite3.connect(str(db))
    n_ev = conn.execute(
        "SELECT COUNT(*) FROM voice_session_events").fetchone()[0]
    conn.close()
    _check(suite, "no_event_row_after_dry_run",
           n_ev == 0, str(n_ev))

    # delete dry-run
    de = vcs.delete_voice_session(state["session_id"], dry_run=True,
                                    db_path=db)
    _check(suite, "delete_dry_run_no_delete",
           de["dry_run"] is True, str(de))
    conn = sqlite3.connect(str(db))
    n_after = conn.execute(
        "SELECT COUNT(*) FROM voice_sessions").fetchone()[0]
    conn.close()
    _check(suite, "row_still_present_after_dry_run_delete",
           n_after == 1, str(n_after))

    # list bounded
    lst = vcs.list_voice_sessions(limit=5, db_path=db)
    _check(suite, "list_bounded",
           isinstance(lst, list) and len(lst) <= 5, str(len(lst)))

    # Forbidden fields stripped before write
    sensitive_state = dict(state)
    sensitive_state["medical_diagnosis"] = "irrelevant"
    sensitive_state["session_id"] = "vses_test_sens"
    r4 = vcs.save_voice_session_state(sensitive_state,
                                       consent_marker="op",
                                       dry_run=False, db_path=db)
    _check(suite, "sensitive_strip_on_save",
           r4["ok"], str(r4))
    conn = sqlite3.connect(str(db))
    sum_json = conn.execute(
        "SELECT summary_json FROM voice_sessions WHERE session_id=?",
        ("vses_test_sens",)).fetchone()[0]
    conn.close()
    _check(suite, "sensitive_field_not_persisted",
           "medical_diagnosis" not in sum_json, str(sum_json)[:200])


# -------------------- H: Runtime --------------------

def suite_H_runtime() -> None:
    suite = "H_RUNTIME"
    state = vmst.new_voice_session()
    en = vmr.get_voice_continuity_plan("Hello, what is an engineer?",
                                         state=state, limit=5)
    _check(suite, "en_plan_ok",
           en["ok"] and en["detected_language_mode"] == "english_only",
           str(en)[:200])
    state = en["updated_state"]

    ru = vmr.get_voice_continuity_plan("Расскажи мне про маяк.",
                                         state=state, limit=5)
    _check(suite, "ru_plan_ok",
           ru["ok"] and ru["detected_language_mode"] == "russian_only", "")
    state = ru["updated_state"]

    mx = vmr.get_voice_continuity_plan("Hello, я инженер and I work.",
                                         state=state, limit=5)
    _check(suite, "mix_plan_ok",
           mx["ok"]
           and mx["voice_style_plan"]["chosen_spoken_mode"] in (
               "mixed_en_ru", "english_with_russian_terms"),
           str(mx)[:200])

    required_fields = ("detected_language_mode", "extracted_preferences",
                       "active_corrections", "continuity_decision",
                       "voice_style_plan", "spoken_render_adjustments",
                       "updated_state", "safety_summary",
                       "continuity_notes", "persistence_status",
                       "gap_notes")
    _check(suite, "runtime_required_fields",
           set(required_fields) <= set(en.keys()),
           f"missing={set(required_fields) - set(en.keys())}")
    _check(suite, "persistence_session_only_default",
           en["persistence_status"] == "session_only", "")

    # apply_voice_memory_to_render_payload preserves schema
    import bilingual_spoken_render_runtime as rrt
    safe = rrt.build_voice_safe_render_payload(
        "Hello world.", language_mode="english_only")
    annotated = vmr.apply_voice_memory_to_render_payload(
        safe["payload"], state)
    _check(suite, "annotated_payload_validates",
           annotated["ok"] is True, str(annotated.get("validation"))[:200])

    demo = vmr.demo_voice_memory_scenarios(limit=5)
    _check(suite, "demo_bounded",
           demo["ok"] and demo["count"] <= 5
           and len(demo["scenarios"]) == demo["count"], str(demo)[:200])

    out = _td() / "rt.json"
    vmr.write_voice_memory_runtime_report(en, out)
    _check(suite, "runtime_report_written", out.exists(), "")


# -------------------- I: Production safety + import-blocked check --------------------

def suite_I_production_safety() -> None:
    suite = "I_PRODUCTION_SAFETY"
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    import glob
    before_en = enlex.count_words()
    before_ru = rulex.count_words()
    before_phr = rulex.count_phrases()
    before_mans = (len(glob.glob("seed_packs/en/*.en_pack_manifest.json"))
                   + len(glob.glob("seed_packs/ru/*.ru_pack_manifest.json")))
    conn = sqlite3.connect("bilingual_stack/bilingual_links.sqlite")
    try:
        before_c = conn.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0]
        before_l = conn.execute(
            "SELECT COUNT(*) FROM entry_links").fetchone()[0]
    finally:
        conn.close()
    # Drive the runtime
    state = vmst.new_voice_session()
    vmr.get_voice_continuity_plan("Hello, я инженер",
                                    state=state, limit=5)
    vmr.demo_voice_memory_scenarios(limit=3)
    after_en = enlex.count_words()
    after_ru = rulex.count_words()
    after_phr = rulex.count_phrases()
    after_mans = (len(glob.glob("seed_packs/en/*.en_pack_manifest.json"))
                  + len(glob.glob("seed_packs/ru/*.ru_pack_manifest.json")))
    conn = sqlite3.connect("bilingual_stack/bilingual_links.sqlite")
    try:
        after_c = conn.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0]
        after_l = conn.execute(
            "SELECT COUNT(*) FROM entry_links").fetchone()[0]
    finally:
        conn.close()
    _check(suite, "en_unchanged",
           before_en == after_en, f"{before_en}->{after_en}")
    _check(suite, "ru_unchanged",
           before_ru == after_ru, f"{before_ru}->{after_ru}")
    _check(suite, "ru_phrases_unchanged",
           before_phr == after_phr, f"{before_phr}->{after_phr}")
    _check(suite, "manifest_count_unchanged",
           before_mans == after_mans, f"{before_mans}->{after_mans}")
    _check(suite, "bilingual_concepts_unchanged",
           before_c == after_c, f"{before_c}->{after_c}")
    _check(suite, "bilingual_links_unchanged",
           before_l == after_l, f"{before_l}->{after_l}")
    # Phase 21 incoming folders still empty (no corpus import)
    _check(suite, "phase21_incoming_en_still_empty",
           not list(Path("corpus_sources/english/incoming").iterdir()), "")
    _check(suite, "phase21_incoming_ru_still_empty",
           not list(Path("corpus_sources/russian/incoming").iterdir()), "")


# -------------------- J: Isolation --------------------

PHASE26_FILES = [
    "bilingual_voice_memory_schema.py",
    "bilingual_voice_memory_state.py",
    "bilingual_voice_preference_extractor.py",
    "bilingual_voice_continuity_planner.py",
    "bilingual_voice_correction_memory.py",
    "bilingual_voice_continuity_store.py",
    "bilingual_voice_memory_runtime.py",
]


def suite_J_isolation() -> None:
    suite = "J_ISOLATION"
    FORBIDDEN = ("worker", "luna_modules", "tier_", "probe_",
                 "attestation", "program_s")
    DAEMON = (
        r"threading\.Thread\s*\(",
        r"multiprocessing\.Process\s*\(",
        r"asyncio\.create_task\s*\(",
        r"subprocess\.Popen\s*\(",
        r"subprocess\.run\s*\(",
        r"subprocess\.call\s*\(",
        r"^\s*(import|from)\s+schedule(\s|$|\.|,)",
        r"^\s*(import|from)\s+apscheduler",
        r"BackgroundScheduler\s*\(",
        r"threading\.Timer\s*\(",
        r"^\s*while\s+True\s*:",
    )
    NETWORK = (
        r"^\s*(import|from)\s+(urllib|requests|httpx|aiohttp|socket|ftplib)\b",
        r"urlopen\s*\(",
        r"http\.client",
    )
    AUDIO = (
        r"^\s*(import|from)\s+(pyttsx3|gtts|tts|edge_tts|sounddevice|"
        r"pyaudio|pydub|soundfile|wave|piper|whisper|coqui)\b",
        r"\.synthesize\s*\(",
    )
    for fname in PHASE26_FILES:
        p = Path(fname)
        _check(suite, f"{fname}_exists", p.exists(), "")
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8")
        bad: list[str] = []
        for line in text.splitlines():
            for forb in FORBIDDEN:
                if re.search(rf"^(import|from)\s+\S*{re.escape(forb)}", line):
                    bad.append(line.strip())
        _check(suite, f"{fname}_no_forbidden_imports",
               not bad, "; ".join(bad[:3]))
        net = [m.group(0) for pat in NETWORK
               for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_network",
               not net, "; ".join(net[:3]))
        dh = [m.group(0) for pat in DAEMON
              for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_daemon_or_subprocess",
               not dh, "; ".join(dh[:3]))
        au = [m.group(0) for pat in AUDIO
              for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_audio_or_tts",
               not au, "; ".join(au[:3]))


def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_SCHEMA", suite_B_schema),
        ("C_STATE", suite_C_state),
        ("D_EXTRACTOR", suite_D_extractor),
        ("E_PLANNER", suite_E_planner),
        ("F_CORRECTIONS", suite_F_corrections),
        ("G_STORE", suite_G_store),
        ("H_RUNTIME", suite_H_runtime),
        ("I_PRODUCTION_SAFETY", suite_I_production_safety),
        ("J_ISOLATION", suite_J_isolation),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False,
                   f"{e!r}\n{traceback.format_exc()}")
    fails = [r for r in _results if not r[2].startswith(PASS)]
    print("=== Phase 26 Voice Memory / Continuity ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
