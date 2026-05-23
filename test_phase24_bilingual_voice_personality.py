"""Phase 24 - Bilingual Voice / Personality Harness."""

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

import bilingual_voice_personality_profile as vpp
import bilingual_spoken_style_planner as ssp
import bilingual_personality_continuity_scorer as pcs
import bilingual_turn_taking_strategy as tts
import bilingual_voice_safety_filter as vsf
import bilingual_voice_style_runtime as vsr


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase24_"))


PHASE24_REQUIRED_PRIOR = (
    "PHASE23_HUMAN_CODE_SWITCHING_REPORT.md",
    "test_phase23_human_code_switching.py",
    "bilingual_language_mode_detector.py",
    "bilingual_code_switch_policy.py",
    "bilingual_style_mixer.py",
    "bilingual_conversation_state.py",
    "bilingual_response_quality.py",
    "bilingual_human_switch_runtime.py",
    "PHASE22_BILINGUAL_LINKER_AND_RETRIEVAL_BRIDGE_REPORT.md",
    "bilingual_concept_link_store.py",
    "bilingual_link_builder.py",
    "bilingual_retrieval_bridge.py",
    "russian_morphology_upgrade_path.py",
    "bilingual_coverage_gap_reporter.py",
    "bilingual_stack/bilingual_links.sqlite",
    "cognitive_lexicon_store.py",
    "cognitive_word_policy.py",
    "cognitive_vocabulary_runtime.py",
    "russian_lexicon_store.py",
    "russian_language_router.py",
    "russian_morphology_layer.py",
    "russian_personality_layer.py",
    "russian_response_quality.py",
    "coverage_taxonomy.py",
    "pack_manifest.py",
)


# -------------------- A: Pre-flight --------------------

def suite_A_preflight() -> None:
    suite = "A_PREFLIGHT"
    for f in PHASE24_REQUIRED_PRIOR:
        _check(suite, f"required_{f}_exists",
               Path(f).exists(), f"missing: {f}")


# -------------------- B: Personality profile --------------------

def suite_B_profile() -> None:
    suite = "B_PROFILE"
    bp = vpp.get_luna_bilingual_personality_profile()
    _check(suite, "bilingual_profile_exists",
           "core_identity" in bp and "warmth_level" in bp, str(bp.keys()))
    en = vpp.get_language_specific_personality("en")
    _check(suite, "english_profile_exists",
           en["language"] == "en" and "tone_rules" in en, str(en))
    ru = vpp.get_language_specific_personality("ru")
    _check(suite, "russian_profile_exists",
           ru["language"] == "ru" and "tone_rules" in ru, str(ru))
    mx = vpp.get_mixed_language_personality_profile()
    _check(suite, "mixed_profile_exists",
           mx["language"] == "mixed_en_ru", str(mx))
    forb = vpp.get_forbidden_voice_style_traits()
    _check(suite, "forbidden_traits_present",
           isinstance(forb, list) and "robotic" in forb, str(forb))
    allowed = vpp.get_allowed_voice_style_traits()
    _check(suite, "allowed_traits_present",
           isinstance(allowed, list) and "warm" in allowed, str(allowed))
    v = vpp.validate_personality_profile(bp)
    _check(suite, "profile_validates", v["ok"], str(v))
    bad = vpp.validate_personality_profile({"core_identity": "x"})
    _check(suite, "missing_required_rejected",
           bad.get("ok") is False, str(bad))
    out_p = _td() / "profile.json"
    vpp.write_personality_profile_report(bp, out_p)
    _check(suite, "report_written", out_p.exists(), "")

    # Spoken style profile per language_mode + conversation_mode
    sp = vpp.get_spoken_style_profile("mixed_en_ru", "conversation")
    _check(suite, "spoken_style_has_sentence_length",
           "sentence_length_chars" in sp
           and "min" in sp["sentence_length_chars"], str(sp))
    sp_t = vpp.get_spoken_style_profile("mixed_en_ru", "teacher")
    _check(suite, "teacher_reduces_density",
           sp_t["code_switch_density"] <= 0.25, str(sp_t))
    sp_b = vpp.get_spoken_style_profile("code_switch_sentence_level",
                                          "bilingual_practice")
    _check(suite, "bilingual_practice_raises_density",
           sp_b["code_switch_density"] >= 0.4, str(sp_b))


# -------------------- C: Spoken style planner --------------------

def suite_C_planner() -> None:
    suite = "C_PLANNER"
    en_plan = ssp.plan_spoken_response_style("Hello, what is a vector?",
                                               conversation_mode="conversation",
                                               limit=5)
    p_en = en_plan["plan"]
    _check(suite, "english_chooses_english_spoken",
           p_en["spoken_mode"] == "english_only", str(p_en["spoken_mode"]))
    ru_plan = ssp.plan_spoken_response_style("Привет, что такое вектор?",
                                               conversation_mode="conversation",
                                               limit=5)
    p_ru = ru_plan["plan"]
    _check(suite, "russian_chooses_russian_spoken",
           p_ru["spoken_mode"] == "russian_only", str(p_ru["spoken_mode"]))
    mix_plan = ssp.plan_spoken_response_style("Hello, я инженер and I work",
                                                conversation_mode="conversation",
                                                limit=5)
    p_mix = mix_plan["plan"]
    _check(suite, "mixed_chooses_mixed_or_terms",
           p_mix["spoken_mode"] in (
               "mixed_en_ru", "english_with_russian_terms"),
           str(p_mix["spoken_mode"]))

    # Teacher mode reduces switching
    t_plan = ssp.plan_spoken_response_style("Hello, я инженер",
                                              conversation_mode="teacher",
                                              limit=5)
    _check(suite, "teacher_reduces_switching",
           t_plan["plan"]["code_switch_density"]["code_switch_density"]
           <= 0.25,
           str(t_plan["plan"]["code_switch_density"]))

    # Bilingual practice raises switching
    bp = ssp.plan_spoken_response_style(
        "Hello world. Привет мир.",
        conversation_mode="bilingual_practice", limit=5)
    _check(suite, "bilingual_practice_raises_density_plan",
           bp["plan"]["code_switch_density"]["code_switch_density"] >= 0.40,
           str(bp["plan"]["code_switch_density"]))

    # Sentence length present
    sl = ssp.choose_spoken_sentence_length("mixed_en_ru", "conversation")
    _check(suite, "sentence_length_present",
           sl["min_chars"] > 0 and sl["max_chars"] > sl["min_chars"],
           str(sl))

    # Voice-ready skeleton bounded
    sk = ssp.produce_voice_ready_response_skeleton(
        "Hello world", t_plan["plan"], limit=3)
    _check(suite, "skeleton_bounded",
           sk["ok"] and len(sk["skeleton_steps"]) <= 3, str(sk))

    # Spoken style instructions emitted
    ins = ssp.generate_spoken_style_instructions(p_mix)
    _check(suite, "instructions_emitted",
           isinstance(ins, list) and len(ins) >= 4, str(ins)[:200])

    # Register filter strips slang in teacher mode
    reg = ssp.choose_spoken_register("mixed_en_ru", "teacher")
    _check(suite, "teacher_strips_slang",
           "slang" not in reg["allowed_registers"]
           and "vulgar" not in reg["allowed_registers"], str(reg))

    # User preference can force english-only / russian-only density to 0
    csd = ssp.choose_code_switch_density(
        "mixed_en_ru", "conversation", user_preference="english")
    _check(suite, "user_pref_english_density_zero",
           csd["code_switch_density"] == 0.0, str(csd))


# -------------------- D: Personality continuity scorer --------------------

def suite_D_continuity() -> None:
    suite = "D_CONTINUITY"
    warm_text = ("Of course! Let me explain that simply. "
                 "Luna is happy to help. По-простому: всё ясно.")
    robotic_text = ("As an AI, I cannot have feelings. "
                     "My programming dictates that I respond. Beep boop.")
    w_score = pcs.score_personality_continuity(warm_text,
                                                  language_mode="mixed_en_ru")
    r_score = pcs.score_personality_continuity(robotic_text,
                                                  language_mode="english_only")
    _check(suite, "warm_scores_higher_than_robotic",
           w_score["overall_score"] > r_score["overall_score"],
           f"warm={w_score['overall_score']} robotic={r_score['overall_score']}")
    _check(suite, "robotic_verdict_drift",
           r_score["verdict"] in ("drift", "passable"), str(r_score))

    # Overly formal Russian
    of_ru = pcs.detect_personality_drift(
        "Позвольте мне выразить мою благодарность. "
        "Вышеизложенное полностью отражает семантическую интенцию.",
        language_mode="russian_only")
    _check(suite, "overly_formal_ru_detected",
           "overly_formal_russian" in of_ru["drift_kinds"], str(of_ru))

    # Excessive slang
    slang = pcs.detect_personality_drift(
        "yo bruh лол кек this is great лол кек",
        language_mode="mixed_en_ru")
    _check(suite, "excessive_slang_detected",
           "excessive_slang" in slang["drift_kinds"], str(slang))

    # Translation artifacts
    trans = pcs.detect_personality_drift(
        "это очень important для нас",
        language_mode="mixed_en_ru")
    _check(suite, "translation_artifact_detected",
           "word_for_word_translation" in trans["drift_kinds"], str(trans))

    sug = pcs.suggest_personality_corrections(robotic_text,
                                                language_mode="english_only",
                                                limit=5)
    _check(suite, "suggestions_bounded",
           isinstance(sug["suggestions"], list)
           and len(sug["suggestions"]) <= 5, str(sug))

    out = _td() / "cont.json"
    pcs.write_personality_continuity_report(w_score, out)
    _check(suite, "continuity_report_written", out.exists(), "")


# -------------------- E: Turn-taking strategy --------------------

def suite_E_turn() -> None:
    suite = "E_TURN"
    cases = [
        ("What is a vector?", "question"),
        ("Build me a function please", "command"),
        ("Actually, that's wrong.", "correction"),
        ("I'm feeling tired today.", "emotional_share"),
        ("Translate verse into Russian please.", "translation_request"),
        ("Let's practice Russian.", "bilingual_practice"),
        ("hi", "casual_chat"),
        ("Fix this code function compile error", "technical_task"),
        ("Wait, hold on.", "interruption"),
        ("", "ambiguous"),
    ]
    for text, expected in cases:
        out = tts.classify_turn_type(text)
        _check(suite, f"classify_{expected}_for_{text[:20]!r}",
               out["turn_type"] == expected, f"got={out['turn_type']}")

    # Clarification
    cl = tts.detect_clarification_needed("?")
    _check(suite, "short_input_needs_clarification",
           cl["needed"] is True, str(cl))
    cl2 = tts.detect_clarification_needed(
        "Could you explain vectors with examples?")
    _check(suite, "long_input_does_not_need_clarification",
           cl2["needed"] is False, str(cl2))

    opts = tts.generate_clarification_options("?", language_mode="english_only",
                                                limit=3)
    _check(suite, "clarification_options_bounded",
           len(opts["options"]) <= 3 and len(opts["options"]) >= 1,
           str(opts))

    rp_en = tts.generate_repair_phrase("english_only", "misunderstanding")
    _check(suite, "repair_phrase_english",
           rp_en["phrase"] == "Let me clarify.", str(rp_en))
    rp_ru = tts.generate_repair_phrase("russian_only", "misunderstanding")
    _check(suite, "repair_phrase_russian",
           rp_ru["phrase"] == "Поясню.", str(rp_ru))
    rp_mix = tts.generate_repair_phrase("mixed_en_ru", "misunderstanding")
    _check(suite, "repair_phrase_mixed",
           "/" in rp_mix["phrase"], str(rp_mix))

    plan = tts.plan_bilingual_turn("What is a vector?",
                                     conversation_mode="conversation")
    _check(suite, "turn_plan_produced",
           plan["ok"] and "turn_strategy" in plan, str(plan)[:200])


# -------------------- F: Voice safety filter --------------------

def suite_F_safety() -> None:
    suite = "F_SAFETY"
    entries = [
        {"target_word": "ok1", "safety_tags": []},
        {"target_word": "ok2", "safety_tags": ["recognition_only"]},
        {"target_word": "ok3", "safety_tags": ["do_not_use_unprompted"]},
        {"target_word": "ok4", "register_tags": ["vulgar"],
         "safety_tags": ["vulgar"]},
    ]
    f = vsf.filter_voice_style_terms(entries, mode="conversation",
                                       is_user_prompted=False)
    _check(suite, "do_not_use_unprompted_blocked",
           f["blocked_count"] >= 1, str(f))
    _check(suite, "recognition_only_not_in_spoken_safe",
           all("recognition_only"
               not in (e.get("safety_tags") or [])
               for e in f["spoken_safe"]), str(f))
    _check(suite, "vulgar_blocked_in_non_slang",
           any(b.get("reason") in (
               "vulgar_or_offensive_in_non_slang_mode",
               "vulgar_or_offensive_unprompted") for b in f["blocked"]),
           str(f))

    # Teacher mode: slang in text flagged
    bad = vsf.check_voice_safe_register(" yo bruh ",
                                          language_mode="english_only",
                                          conversation_mode="teacher")
    _check(suite, "teacher_slang_flagged",
           bad["ok"] is False and len(bad["flags"]) >= 1, str(bad))
    good = vsf.check_voice_safe_register("Hello, how are you today?",
                                            language_mode="english_only",
                                            conversation_mode="teacher")
    _check(suite, "clean_text_passes",
           good["ok"] is True, str(good))

    leak = vsf.detect_spoken_unsafe_leakage(
        "step by step instructions to bypass auth")
    _check(suite, "spoken_unsafe_leakage_detected",
           leak["unsafe_leakage_detected"] is True, str(leak))
    leak_ok = vsf.detect_spoken_unsafe_leakage("Hello world")
    _check(suite, "clean_no_leakage",
           leak_ok["unsafe_leakage_detected"] is False, str(leak_ok))

    decision = vsf.explain_voice_safety_decision(f)
    _check(suite, "decision_explained",
           "Allowed for spoken output" in decision["explanation"],
           str(decision))

    out = _td() / "vs.json"
    vsf.write_voice_safety_report(f, out)
    _check(suite, "voice_safety_report_written", out.exists(), "")


# -------------------- G: Voice style runtime --------------------

def suite_G_runtime() -> None:
    suite = "G_RUNTIME"
    en = vsr.get_bilingual_voice_style_plan("Hello, what is an engineer?",
                                              limit=5)
    _check(suite, "en_plan_ok",
           en["ok"] and en["chosen_spoken_mode"] == "english_only",
           str(en)[:200])
    required_fields = ("detected_language_mode", "chosen_spoken_mode",
                       "code_switch_density", "spoken_register",
                       "sentence_length_guidance",
                       "personality_profile",
                       "spoken_style_instructions",
                       "turn_strategy", "voice_safety_summary",
                       "continuity_score", "quality_notes",
                       "demo_response_skeleton",
                       "updated_conversation_state", "gap_notes")
    _check(suite, "en_plan_required_fields",
           set(required_fields) <= set(en.keys()),
           f"missing={set(required_fields) - set(en.keys())}")

    ru = vsr.get_bilingual_voice_style_plan("Привет, что такое инженер?",
                                              limit=5)
    _check(suite, "ru_plan_ok",
           ru["ok"] and ru["chosen_spoken_mode"] == "russian_only", "")

    mx = vsr.get_bilingual_voice_style_plan("Hello, я инженер and I work.",
                                              limit=5)
    _check(suite, "mix_plan_ok",
           mx["ok"] and mx["chosen_spoken_mode"]
           in ("mixed_en_ru", "english_with_russian_terms"), "")

    guidance = vsr.get_voice_ready_guidance("Translate verse to Russian.",
                                              conversation_mode="translation_help",
                                              limit=5)
    _check(suite, "voice_guidance_runs",
           guidance["ok"] and "spoken_style_instructions" in guidance,
           str(guidance)[:200])

    ev = vsr.evaluate_voice_style_output("Hello, how are you today?",
                                            language_mode="english_only")
    _check(suite, "evaluation_emits_verdict",
           ev["verdict"] in ("pass", "warn", "fail"), str(ev))

    demo = vsr.demo_bilingual_voice_style_scenarios(limit=5)
    _check(suite, "demo_bounded",
           demo["ok"] and demo["count"] <= 5
           and len(demo["scenarios"]) == demo["count"], str(demo)[:200])

    out = _td() / "rt.json"
    vsr.write_voice_style_runtime_report(en, out)
    _check(suite, "runtime_report_written", out.exists(), "")


# -------------------- H: Production safety --------------------

def suite_H_production_safety() -> None:
    suite = "H_PRODUCTION_SAFETY"
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    import glob
    before_en = enlex.count_words()
    before_ru = rulex.count_words()
    before_phr = rulex.count_phrases()
    before_mans = (len(glob.glob("seed_packs/en/*.en_pack_manifest.json"))
                   + len(glob.glob("seed_packs/ru/*.ru_pack_manifest.json")))
    # bilingual link DB snapshot
    conn = sqlite3.connect("bilingual_stack/bilingual_links.sqlite")
    try:
        before_concepts = conn.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0]
        before_links = conn.execute(
            "SELECT COUNT(*) FROM entry_links").fetchone()[0]
    finally:
        conn.close()
    # Run a few voice-style operations
    vsr.get_bilingual_voice_style_plan("Hello, как дела?", limit=5)
    vsr.demo_bilingual_voice_style_scenarios(limit=3)
    after_en = enlex.count_words()
    after_ru = rulex.count_words()
    after_phr = rulex.count_phrases()
    after_mans = (len(glob.glob("seed_packs/en/*.en_pack_manifest.json"))
                  + len(glob.glob("seed_packs/ru/*.ru_pack_manifest.json")))
    conn = sqlite3.connect("bilingual_stack/bilingual_links.sqlite")
    try:
        after_concepts = conn.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0]
        after_links = conn.execute(
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
           before_concepts == after_concepts,
           f"{before_concepts}->{after_concepts}")
    _check(suite, "bilingual_entry_links_unchanged",
           before_links == after_links,
           f"{before_links}->{after_links}")


# -------------------- I: Isolation --------------------

PHASE24_FILES = [
    "bilingual_voice_personality_profile.py",
    "bilingual_spoken_style_planner.py",
    "bilingual_personality_continuity_scorer.py",
    "bilingual_turn_taking_strategy.py",
    "bilingual_voice_safety_filter.py",
    "bilingual_voice_style_runtime.py",
]


def suite_I_isolation() -> None:
    suite = "I_ISOLATION"
    FORBIDDEN = ("worker", "luna_modules", "tier_", "probe_",
                 "attestation", "program_s")
    DAEMON = (
        r"threading\.Thread\s*\(",
        r"multiprocessing\.Process\s*\(",
        r"asyncio\.create_task\s*\(",
        r"subprocess\.Popen\s*\(",
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
        r"speak\s*\(",
        r"\.synthesize\s*\(",
    )
    for fname in PHASE24_FILES:
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
        _check(suite, f"{fname}_no_daemon",
               not dh, "; ".join(dh[:3]))
        au = [m.group(0) for pat in AUDIO
              for m in re.finditer(pat, text, flags=re.MULTILINE)]
        _check(suite, f"{fname}_no_audio_or_tts",
               not au, "; ".join(au[:3]))


def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_PROFILE", suite_B_profile),
        ("C_PLANNER", suite_C_planner),
        ("D_CONTINUITY", suite_D_continuity),
        ("E_TURN", suite_E_turn),
        ("F_SAFETY", suite_F_safety),
        ("G_RUNTIME", suite_G_runtime),
        ("H_PRODUCTION_SAFETY", suite_H_production_safety),
        ("I_ISOLATION", suite_I_isolation),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False,
                   f"{e!r}\n{traceback.format_exc()}")
    fails = [r for r in _results if not r[2].startswith(PASS)]
    print("=== Phase 24 Bilingual Voice / Personality ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
