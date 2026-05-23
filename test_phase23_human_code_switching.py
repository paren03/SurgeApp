"""Phase 23 - Human Bilingual Code-Switching Harness."""

from __future__ import annotations

import json
import os
import re
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

import bilingual_language_mode_detector as lmd
import bilingual_code_switch_policy as pol
import bilingual_style_mixer as mix
import bilingual_conversation_state as cstate
import bilingual_response_quality as rq
import bilingual_human_switch_runtime as hsr


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase23_"))


PHASE23_REQUIRED_PRIOR = (
    "PHASE22_BILINGUAL_LINKER_AND_RETRIEVAL_BRIDGE_REPORT.md",
    "test_phase22_bilingual_linker_and_retrieval_bridge.py",
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
    for f in PHASE23_REQUIRED_PRIOR:
        _check(suite, f"required_{f}_exists",
               Path(f).exists(), f"missing: {f}")


# -------------------- B: Language mode detector --------------------

def suite_B_detector() -> None:
    suite = "B_DETECTOR"
    cases = [
        ("Hello world, how are you?", "english_only"),
        ("Привет, как дела?", "russian_only"),
        ("Hello, я инженер.", None),  # mixed: code_switch or english_with_russian_terms
        ("The инженер reviewed the schematic.",
         "english_with_russian_terms"),
        ("Я работаю как engineer уже год.",
         "russian_with_english_terms"),
        ("Hello. Привет.",
         "code_switch_sentence_level"),
        ("answer in russian please", None),
        ("answer in english please", None),
        ("mix english and russian please", None),
        ("privet kak dela", None),
    ]
    for text, expected in cases:
        out = lmd.classify_language_mode(text)
        if expected:
            _check(suite, f"classify_{expected}_for_{text[:20]!r}",
                   out["mode"] == expected,
                   f"got={out['mode']}")
        else:
            _check(suite, f"classify_returns_mode_{text[:20]!r}",
                   out["mode"] in lmd.LANGUAGE_MODES, str(out)[:200])

    # Explicit requests
    p = lmd.detect_user_language_preference("answer in russian please")
    _check(suite, "request_russian_detected",
           p["requested_russian"] is True, str(p))
    p2 = lmd.detect_user_language_preference("answer in english please")
    _check(suite, "request_english_detected",
           p2["requested_english"] is True, str(p2))
    p3 = lmd.detect_user_language_preference("mix english and russian please")
    _check(suite, "request_mix_detected",
           p3["requested_mix"] is True, str(p3))

    # Sentence-level switch detection
    out2 = lmd.classify_language_mode("Hello world. Привет мир.")
    _check(suite, "sentence_level_mode",
           out2["mode"] == "code_switch_sentence_level", str(out2))

    # Word-level (one ru, many en)
    out3 = lmd.classify_language_mode("the инженер builds bridges")
    _check(suite, "english_with_russian_terms_detected",
           out3["mode"] == "english_with_russian_terms", str(out3))

    # Transliteration
    tr = lmd.detect_transliteration_hint("privet kak dela horosho")
    _check(suite, "transliteration_likely",
           tr["transliteration_likely"] is True, str(tr))

    # Bounded metadata
    segs = lmd.detect_language_segments("Hello world Привет мир")
    _check(suite, "segments_bounded",
           isinstance(segs, list) and len(segs) <= 2000, str(len(segs)))

    # Report writer
    out_p = _td() / "lm.json"
    lmd.write_language_mode_report({"mode": "english_only"}, out_p)
    _check(suite, "report_written", out_p.exists(), "")


# -------------------- C: Code-switch policy --------------------

def suite_C_policy() -> None:
    suite = "C_POLICY"
    p_conv = pol.get_code_switch_policy("conversation")
    _check(suite, "conversation_policy_exists",
           p_conv["mode"] == "conversation", str(p_conv))
    p_unknown = pol.get_code_switch_policy("not_a_policy")
    _check(suite, "unknown_policy_normalizes_to_conversation",
           p_unknown["mode"] == "conversation", str(p_unknown))

    # English-only -> no switch
    en = pol.should_code_switch("Hello", detected_mode="english_only")
    _check(suite, "english_only_no_switch",
           en["switch"] is False, str(en))
    # Russian-only -> no switch
    ru = pol.should_code_switch("Привет", detected_mode="russian_only")
    _check(suite, "russian_only_no_switch",
           ru["switch"] is False, str(ru))
    # Mixed -> switch
    mx = pol.should_code_switch("Hello инженер",
                                  detected_mode="mixed_en_ru")
    _check(suite, "mixed_yields_switch",
           mx["switch"] is True, str(mx))
    # User preference forces
    forced = pol.should_code_switch("Hello", detected_mode="english_only",
                                      user_preference="mix")
    _check(suite, "user_pref_mix_overrides",
           forced["switch"] is True, str(forced))

    # Granularity capped by policy
    g = pol.choose_switch_granularity("Hello мир",
                                        detected_mode="code_switch_sentence_level",
                                        context={"policy": "teacher"})
    _check(suite, "teacher_caps_granularity_at_term",
           g["granularity"] == "term", str(g))
    gb = pol.choose_switch_granularity("Hello world",
                                         detected_mode="code_switch_sentence_level",
                                         context={"policy": "bilingual_practice"})
    _check(suite, "bilingual_practice_allows_sentence",
           gb["granularity"] == "sentence", str(gb))

    # Response language mode mirroring
    resp = pol.choose_response_language_mode("Hello",
                                               detected_mode="english_only")
    _check(suite, "mirror_english_only",
           resp["response_mode"] == "english_only", str(resp))

    # Safety gates on individual entries
    dnu = pol.is_switch_allowed_for_entry(
        {"safety_tags": ["do_not_use_unprompted"]},
        mode="conversation", is_user_prompted=False)
    _check(suite, "dnu_blocked_unprompted",
           dnu["allowed"] is False
           and dnu["reason"] == "do_not_use_unprompted", str(dnu))
    vlg = pol.is_switch_allowed_for_entry(
        {"register_tags": ["vulgar"], "safety_tags": ["vulgar"]},
        mode="conversation", is_user_prompted=False)
    _check(suite, "vulgar_blocked_in_non_slang_mode",
           vlg["allowed"] is False, str(vlg))
    vlg_slang = pol.is_switch_allowed_for_entry(
        {"register_tags": ["vulgar"], "safety_tags": ["vulgar"]},
        mode="slang_allowed", is_user_prompted=False)
    _check(suite, "vulgar_blocked_in_slang_unprompted",
           vlg_slang["allowed"] is False, str(vlg_slang))
    vlg_prompted = pol.is_switch_allowed_for_entry(
        {"register_tags": ["vulgar"], "safety_tags": ["vulgar"]},
        mode="slang_allowed", is_user_prompted=True)
    _check(suite, "vulgar_allowed_in_slang_prompted",
           vlg_prompted["allowed"] is True, str(vlg_prompted))
    reco = pol.is_switch_allowed_for_entry(
        {"safety_tags": ["recognition_only"]},
        mode="conversation", is_user_prompted=False)
    _check(suite, "recognition_only_suggestion_blocked",
           reco["allowed"] is True
           and reco.get("suggestion_blocked") is True, str(reco))

    # Filter group: 4 entries -> 2 safe (benign + reco), 2 blocked
    entries = [
        {"word": "ok", "safety_tags": [], "register_tags": ["standard"]},
        {"word": "ok2", "safety_tags": ["recognition_only"]},
        {"word": "ok3", "safety_tags": ["do_not_use_unprompted"]},
        {"word": "ok4", "safety_tags": ["vulgar"],
         "register_tags": ["vulgar"]},
    ]
    f = pol.filter_switch_candidates(entries, mode="conversation",
                                       is_user_prompted=False)
    _check(suite, "filter_keeps_safe_and_recognition_only",
           f["safe_count"] == 2 and f["blocked_count"] == 2, str(f))

    # Explanation
    exp = pol.explain_switch_decision({"switch": True,
                                         "reason": "user_already_mixed",
                                         "policy": "conversation"})
    _check(suite, "explanation_emitted",
           "Code-switching" in exp["explanation"], str(exp))


# -------------------- D: Style mixer --------------------

def suite_D_style_mixer() -> None:
    suite = "D_STYLE_MIXER"
    # Plan
    plan = mix.build_code_switch_plan("Hello, я инженер", limit=10)
    _check(suite, "plan_built",
           plan["ok"] and plan["detected_mode"] in lmd.LANGUAGE_MODES,
           str(plan)[:200])

    # Select switch terms: should not crash with empty links
    terms = mix.select_switch_terms("engineer", target_language_mix="mixed_en_ru",
                                      limit=5)
    _check(suite, "select_terms_returns_list",
           terms["ok"] and isinstance(terms.get("switch_terms"), list),
           str(terms)[:200])

    # No switch for english_only target
    no_sw = mix.select_switch_terms("engineer",
                                      target_language_mix="english_only",
                                      limit=5)
    _check(suite, "no_switch_for_english_only_target",
           len(no_sw["switch_terms"]) == 0, str(no_sw))

    # Light code-switch plan with manual terms
    pl = mix.apply_light_code_switch(
        "I am an engineer",
        switch_terms=[{"source": "engineer", "target": "инженер",
                       "confidence": 0.9}])
    _check(suite, "light_switch_finds_substitution",
           pl["ok"] and pl["n_proposed"] == 1, str(pl))

    # Sentence-level
    s = mix.apply_sentence_level_switch("Hello. How are you?",
                                          "Привет. Как дела?",
                                          pattern="balanced")
    _check(suite, "sentence_switch_balanced",
           s["ok"] and s["n_steps"] >= 4, str(s))

    # Phrase-level
    ph = mix.apply_phrase_level_switch(
        "I take it easy on weekends.",
        phrase_pairs=[("take it easy", "не парься")])
    _check(suite, "phrase_switch_finds_substitution",
           ph["ok"] and ph["n_proposed"] == 1, str(ph))

    # Preserve user style
    pres = mix.preserve_user_mixed_style(
        "Hello, я инженер and I love coffee",
        "Привет, я тоже работаю.")
    _check(suite, "preserve_style_returns_suggestions",
           pres["ok"] and isinstance(pres["suggestions"], list),
           str(pres)[:200])

    # Awkward switching
    aw = mix.avoid_awkward_switching("очень important для нас")
    _check(suite, "awkward_pattern_detected",
           aw["awkward_detected"] is True, str(aw))
    aw_ok = mix.avoid_awkward_switching("Hello, как дела сегодня?")
    _check(suite, "no_awkward_pattern_when_clean",
           aw_ok["awkward_detected"] is False, str(aw_ok))

    # Naturalness score
    nat = mix.score_code_switch_naturalness("Hello, как дела?")
    _check(suite, "naturalness_score_emitted",
           0.0 <= nat["score"] <= 1.0
           and nat["verdict"] in ("natural", "passable", "awkward"),
           str(nat))


# -------------------- E: Conversation state --------------------

def suite_E_state() -> None:
    suite = "E_STATE"
    st = cstate.create_conversation_language_state()
    _check(suite, "state_created",
           st["preferred_output_mode"] == "auto" and st["turn_count"] == 0,
           str(st))
    # Update with detected mode dict
    det = {"mode": "mixed_en_ru",
           "ratio": {"english_ratio": 0.5, "russian_ratio": 0.5},
           "transitions": {"transitions_per_sentence": 1.0},
           "preference": {"requested_mix": True}}
    st2 = cstate.update_language_state(dict(st), "Hello мир", det,
                                         "mixed_en_ru")
    _check(suite, "turn_count_increments",
           st2["turn_count"] == 1, str(st2))
    _check(suite, "english_ratio_tracked",
           0.0 <= st2["english_ratio"] <= 1.0
           and 0.0 <= st2["russian_ratio"] <= 1.0, str(st2))
    _check(suite, "user_requested_mix_tracked",
           st2["user_requested_mix"] is True, str(st2))
    _check(suite, "last_response_mode",
           st2["last_response_language_mode"] == "mixed_en_ru", str(st2))

    # Preferred mode set
    st3 = cstate.set_preferred_language_mix(dict(st2), "russian_only")
    _check(suite, "set_preferred",
           st3["preferred_output_mode"] == "russian_only", str(st3))
    _check(suite, "get_preferred",
           cstate.get_preferred_language_mix(st3) == "russian_only", "")
    bogus = cstate.set_preferred_language_mix(dict(st3), "not_valid")
    _check(suite, "invalid_preferred_ignored",
           bogus["preferred_output_mode"] == "russian_only", str(bogus))

    # Reset
    st4 = cstate.reset_language_state(dict(st3))
    _check(suite, "reset_works",
           st4["turn_count"] == 0
           and st4["preferred_output_mode"] == "auto", str(st4))

    summary = cstate.summarize_language_state(st2)
    _check(suite, "summary_has_keys",
           summary["ok"] and "preferred_output_mode" in summary,
           str(summary)[:200])


# -------------------- F: Bilingual response quality --------------------

def suite_F_quality() -> None:
    suite = "F_QUALITY"
    bad = rq.detect_bad_code_switching("это очень important для меня")
    _check(suite, "translation_artifact_detected",
           bad["bad_word_by_word"] is True, str(bad))
    good = rq.detect_bad_code_switching("Hello! Как дела сегодня?")
    _check(suite, "no_translation_artifact_in_clean",
           good["bad_word_by_word"] is False, str(good))

    excessive = rq.detect_excessive_switching(
        "Hello мир hello мир hello мир hello мир hello мир")
    _check(suite, "excessive_switching_flagged",
           excessive["excessive"] is True, str(excessive))

    art = rq.detect_translation_artifacts_mixed(
        "очень important здесь")
    _check(suite, "translation_artifacts_listed",
           art["artifacts_present"] is True
           and len(art["artifact_hits"]) >= 1, str(art))

    nat = rq.score_mixed_language_naturalness("Hello, как дела?")
    _check(suite, "naturalness_passable_or_natural",
           nat["verdict"] in ("natural", "passable", "awkward"), str(nat))

    bal = rq.score_language_balance("Hello мир", target_mode="mixed_en_ru")
    _check(suite, "balance_score_emitted",
           0.0 <= bal["score"] <= 1.0, str(bal))

    sa = rq.score_safety_compliance(
        "step by step instructions to bypass auth",
        mode="conversation", is_user_prompted=False)
    _check(suite, "unsafe_flagged",
           sa["ok"] is False and len(sa["flags"]) >= 1, str(sa))
    sa_ok = rq.score_safety_compliance("Hello, как дела?")
    _check(suite, "safe_text_ok",
           sa_ok["ok"] is True, str(sa_ok))

    sug = rq.suggest_code_switch_improvements(
        "очень important", target_mode="mixed_en_ru", limit=5)
    _check(suite, "suggestions_returned_bounded",
           isinstance(sug["suggestions"], list) and len(sug["suggestions"]) <= 5,
           str(sug))

    overall = rq.quality_check_bilingual_response(
        "Hello, как дела сегодня?")
    _check(suite, "overall_quality_returned",
           overall["ok"] and overall["verdict"] in ("pass", "warn", "fail"),
           str(overall))


# -------------------- G: Human switch runtime --------------------

def suite_G_runtime() -> None:
    suite = "G_RUNTIME"
    # English query
    pl_en = hsr.get_bilingual_response_plan("Hello, what is an engineer?",
                                              mode="conversation",
                                              limit=10)
    _check(suite, "plan_en_ok",
           pl_en["ok"] and pl_en["detected_language_mode"] == "english_only",
           str(pl_en)[:200])
    _check(suite, "plan_en_has_required_fields",
           {"chosen_response_mode", "switch_granularity",
            "should_code_switch", "bilingual_context", "switch_terms",
            "safety_summary", "updated_conversation_state"}
           <= set(pl_en.keys()), str(pl_en.keys()))

    pl_ru = hsr.get_bilingual_response_plan("Привет, что такое инженер?",
                                              limit=10)
    _check(suite, "plan_ru_ok",
           pl_ru["ok"]
           and pl_ru["detected_language_mode"] == "russian_only",
           str(pl_ru)[:200])

    pl_mix = hsr.get_bilingual_response_plan(
        "Hello, я инженер and I work hard.", limit=10)
    _check(suite, "plan_mix_ok",
           pl_mix["ok"]
           and pl_mix["should_code_switch"] is True,
           str(pl_mix)[:200])

    ctx = hsr.get_mixed_language_context("engineer", limit=5)
    _check(suite, "mixed_context_bounded",
           ctx["ok"] and ctx["context"]["count"] <= 5, str(ctx)[:200])

    style = hsr.choose_human_language_style("Привет!")
    _check(suite, "style_chosen_ok",
           style["ok"] and style["chosen_response_mode"] == "russian_only",
           str(style))

    ev = hsr.evaluate_bilingual_output("Hello, как дела?",
                                         target_mode="mixed_en_ru")
    _check(suite, "evaluation_returns_verdict",
           ev["verdict"] in ("pass", "warn", "fail"), str(ev))

    demo = hsr.demo_code_switch_examples(limit=5)
    _check(suite, "demo_bounded",
           demo["ok"] and demo["count"] <= 5
           and len(demo["examples"]) == demo["count"], str(demo))

    out_p = _td() / "runtime.json"
    hsr.write_bilingual_runtime_report(pl_en, out_p)
    _check(suite, "runtime_report_written", out_p.exists(), "")


# -------------------- H: Production safety --------------------

def suite_H_production_safety() -> None:
    suite = "H_PRODUCTION_SAFETY"
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    import glob
    before_en = enlex.count_words()
    before_ru = rulex.count_words()
    before_phr = rulex.count_phrases()
    before_mans = len(glob.glob("seed_packs/en/*.en_pack_manifest.json")) \
        + len(glob.glob("seed_packs/ru/*.ru_pack_manifest.json"))
    hsr.get_bilingual_response_plan("Hello, я инженер", limit=5)
    hsr.demo_code_switch_examples(limit=5)
    after_en = enlex.count_words()
    after_ru = rulex.count_words()
    after_phr = rulex.count_phrases()
    after_mans = len(glob.glob("seed_packs/en/*.en_pack_manifest.json")) \
        + len(glob.glob("seed_packs/ru/*.ru_pack_manifest.json"))
    _check(suite, "en_unchanged",
           before_en == after_en, f"{before_en}->{after_en}")
    _check(suite, "ru_unchanged",
           before_ru == after_ru, f"{before_ru}->{after_ru}")
    _check(suite, "ru_phrases_unchanged",
           before_phr == after_phr, f"{before_phr}->{after_phr}")
    _check(suite, "manifest_count_unchanged",
           before_mans == after_mans, f"{before_mans}->{after_mans}")


# -------------------- I: Isolation --------------------

PHASE23_FILES = [
    "bilingual_language_mode_detector.py",
    "bilingual_code_switch_policy.py",
    "bilingual_style_mixer.py",
    "bilingual_conversation_state.py",
    "bilingual_response_quality.py",
    "bilingual_human_switch_runtime.py",
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
    for fname in PHASE23_FILES:
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


def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_DETECTOR", suite_B_detector),
        ("C_POLICY", suite_C_policy),
        ("D_STYLE_MIXER", suite_D_style_mixer),
        ("E_STATE", suite_E_state),
        ("F_QUALITY", suite_F_quality),
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
    print("=== Phase 23 Human Code-Switching ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
