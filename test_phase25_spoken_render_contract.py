"""Phase 25 - Bilingual Spoken Render Contract Harness."""

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

import bilingual_spoken_render_contract as src
import bilingual_voice_text_normalizer as vtn
import bilingual_prosody_markup as pmk
import bilingual_pronunciation_hinting as pph
import bilingual_spoken_safety_redactor as ssr
import bilingual_voice_renderer_interface as vri
import bilingual_spoken_render_runtime as rrt


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase25_"))


PHASE25_REQUIRED_PRIOR = (
    "PHASE24_BILINGUAL_VOICE_PERSONALITY_REPORT.md",
    "test_phase24_bilingual_voice_personality.py",
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
    for f in PHASE25_REQUIRED_PRIOR:
        _check(suite, f"required_{f}_exists",
               Path(f).exists(), f"missing: {f}")


# -------------------- B: Render contract --------------------

def suite_B_contract() -> None:
    suite = "B_CONTRACT"
    schema = src.get_spoken_render_schema()
    _check(suite, "schema_has_required_fields",
           "required_payload_fields" in schema
           and "supported_language_modes" in schema, str(schema)[:120])

    payload = src.create_spoken_render_payload(
        "Hello world.", language_mode="english_only",
        segments=[{"text": "Hello world.", "language": "en",
                    "segment_type": "sentence",
                    "start_index": 0, "end_index": 12}],
        safety_summary={"unsafe_leakage_detected": False})
    _check(suite, "payload_created",
           "render_id" in payload and "raw_text" in payload, str(payload)[:200])

    v = src.validate_spoken_render_payload(payload)
    _check(suite, "payload_validates",
           v["ok"] is True, str(v))

    # Missing required field
    bad = dict(payload)
    del bad["raw_text"]
    v_bad = src.validate_spoken_render_payload(bad)
    _check(suite, "missing_required_rejected",
           v_bad["ok"] is False and v_bad["reason"] == "missing_required",
           str(v_bad))

    # Unsupported language mode
    bad_lm = dict(payload)
    bad_lm["language_mode"] = "klingon"
    v_lm = src.validate_spoken_render_payload(bad_lm)
    _check(suite, "unsupported_language_mode_rejected",
           v_lm["ok"] is False
           and "unsupported_language_mode" in v_lm["reason"], str(v_lm))

    # Segment cap enforced via creation
    too_many = [{"text": "x", "language": "en", "segment_type": "word",
                 "start_index": 0, "end_index": 1} for _ in range(300)]
    cap_payload = src.create_spoken_render_payload(
        "x", language_mode="english_only", segments=too_many)
    _check(suite, "segment_cap_enforced_on_create",
           len(cap_payload["segments"]) == src.HARD_SEGMENT_CAP,
           str(len(cap_payload["segments"])))

    # JSON serializable
    try:
        json.dumps(payload, ensure_ascii=False, default=str)
        ok_json = True
    except Exception:
        ok_json = False
    _check(suite, "payload_json_serializable", ok_json, "")

    # No audio data allowed
    bad_audio = dict(payload)
    bad_audio["audio_bytes"] = b"\x00\x01"
    v_audio = src.validate_spoken_render_payload(bad_audio)
    _check(suite, "audio_bytes_rejected",
           v_audio["ok"] is False
           and "disallowed_field" in v_audio["reason"], str(v_audio))

    # Unsafe leakage in safety_summary fails closed
    bad_unsafe = dict(payload)
    bad_unsafe["safety_summary"] = {"unsafe_leakage_detected": True}
    v_unsafe = src.validate_spoken_render_payload(bad_unsafe)
    _check(suite, "unsafe_payload_rejected",
           v_unsafe["ok"] is False
           and "unsafe_text_present_in_payload" in v_unsafe["reason"],
           str(v_unsafe))

    out = _td() / "contract.json"
    src.write_spoken_render_contract_report(schema, out)
    _check(suite, "contract_report_written", out.exists(), "")


# -------------------- C: Text normalizer --------------------

def suite_C_normalizer() -> None:
    suite = "C_NORMALIZER"
    en = vtn.normalize_english_spoken_text("Mr. Smith, i.e. the engineer, said etc.")
    _check(suite, "english_abbrev_expanded",
           "mister" in en["text"].lower() and "et cetera" in en["text"].lower(),
           str(en))
    ru = vtn.normalize_russian_spoken_text("Инженер и т.д. сказал.")
    _check(suite, "russian_abbrev_expanded",
           "и так далее" in ru["text"], str(ru))

    mx = vtn.normalize_mixed_spoken_text("Hello, я инженер. Etc.")
    _check(suite, "mixed_preserves_code_switch",
           any(t.get("kind") == "ru_term_in_en"
               or t.get("kind") == "mixed_token"
               for t in mx["preserved_tokens"])
           or any(t.get("kind") == "en_term_in_ru"
                   for t in mx["preserved_tokens"])
           or any(t.get("token") == "инженер"
                   for t in mx["preserved_tokens"]),
           str(mx["preserved_tokens"])[:200])

    sym = vtn.remove_or_convert_unspoken_symbols("Hello * world # test",
                                                  "english_only")
    _check(suite, "symbols_removed",
           "*" not in sym["text"] and "#" not in sym["text"], str(sym))

    sp = vtn.normalize_spacing_and_punctuation("hello   world!!!",
                                                 "english_only")
    _check(suite, "spacing_collapsed",
           "  " not in sp["text"] and "!!!" not in sp["text"], str(sp))

    # No destructive Russian transliteration
    ru_safe = vtn.normalize_russian_spoken_text("Привет, как дела?")
    _check(suite, "russian_no_destructive_translit",
           "Привет" in ru_safe["text"] and "Privet" not in ru_safe["text"],
           str(ru_safe))

    rep = _td() / "norm.json"
    vtn.write_normalization_report(en, rep)
    _check(suite, "normalization_report_written", rep.exists(), "")


# -------------------- D: Prosody --------------------

def suite_D_prosody() -> None:
    suite = "D_PROSODY"
    segs = pmk.segment_text_for_prosody("Hello. Привет мир.", "mixed_en_ru")
    _check(suite, "segments_produced",
           len(segs) >= 2, str(len(segs)))

    p = pmk.assign_pause_hints(segs, "conversation")
    _check(suite, "pauses_assigned",
           all("pause_after_ms" in s and s["pause_after_ms"] >= 0
               for s in p), str(p)[:200])

    em = pmk.assign_emphasis_hints(segs, "conversation")
    _check(suite, "emphasis_assigned",
           all(s.get("emphasis") in ("normal", "moderate", "strong")
               for s in em), str(em)[:200])

    pace = pmk.assign_pace_hints(segs, "teacher")
    _check(suite, "pace_slow_in_teacher",
           all(s.get("pace") == "slow" for s in pace), str(pace)[:200])

    tone = pmk.assign_tone_hints(segs, "warm")
    _check(suite, "tone_warm",
           all(s.get("tone") == "warm" for s in tone), str(tone)[:200])

    marked, boundaries = pmk.mark_code_switch_boundaries(segs)
    _check(suite, "code_switch_boundaries_marked",
           any(b for b in boundaries), str(boundaries))

    plan = pmk.create_prosody_plan("Hello мир.",
                                     language_mode="mixed_en_ru",
                                     conversation_mode="conversation",
                                     emotional_tone="warm")
    _check(suite, "plan_validates",
           plan["validation"]["ok"] is True, str(plan["validation"]))

    too_long = "Hi. " * 300
    big_plan = pmk.create_prosody_plan(too_long, "english_only")
    _check(suite, "segment_cap_enforced_in_prosody",
           len(big_plan["segments"]) <= 200,
           str(len(big_plan["segments"])))

    rep = _td() / "prosody.json"
    pmk.write_prosody_report(plan, rep)
    _check(suite, "prosody_report_written", rep.exists(), "")


# -------------------- E: Pronunciation hinting --------------------

def suite_E_pronunciation() -> None:
    suite = "E_PRONUNCIATION"
    hints = pph.detect_pronunciation_sensitive_terms(
        "The инженер reviewed the schematic, NASA, FAA, USSR.",
        "english_with_russian_terms")
    _check(suite, "sensitive_terms_detected",
           any(h["kind"] == "ru_term_in_en_context" for h in hints)
           and any(h["kind"] == "acronym" for h in hints), str(hints)[:300])

    segs = [{"segment_id": "s1", "text": "NASA FAA", "language": "en"},
            {"segment_id": "s2", "text": "Привет", "language": "ru"}]
    en_h = pph.create_english_pronunciation_hints(segs)
    _check(suite, "english_hints_created",
           any(h["kind"] == "acronym" for h in en_h), str(en_h))
    ru_h = pph.create_russian_pronunciation_hints(segs)
    _check(suite, "russian_hints_created",
           any(h["kind"] == "ru_stress_uncertainty" for h in ru_h),
           str(ru_h))
    cs_h = pph.create_code_switch_pronunciation_hints(segs)
    _check(suite, "code_switch_hints_created",
           any(h["kind"] == "code_switch_boundary" for h in cs_h),
           str(cs_h))

    tr = pph.flag_transliteration_risk("privet kak dela horosho")
    _check(suite, "transliteration_risk_flagged",
           tr["transliteration_risk"] is True, str(tr))
    tr_safe = pph.flag_transliteration_risk("Hello world")
    _check(suite, "no_translit_risk_clean",
           tr_safe["transliteration_risk"] is False, str(tr_safe))

    stress = pph.flag_russian_stress_uncertainty("Привет, как дела?")
    _check(suite, "stress_uncertainty_flagged",
           stress["russian_token_count"] >= 2
           and stress["stress_uncertain_for_all_tokens"] is True,
           str(stress))

    acro = pph.flag_acronym_pronunciation("NASA, FAA, CNN")
    _check(suite, "acronyms_detected",
           acro["n_acronyms"] >= 3, str(acro))

    rep = _td() / "pron.json"
    pph.write_pronunciation_hint_report({"hints": hints}, rep)
    _check(suite, "pronunciation_report_written", rep.exists(), "")


# -------------------- F: Safety redactor --------------------

def suite_F_safety_redact() -> None:
    suite = "F_SAFETY_REDACT"
    red = ssr.redact_for_spoken_voice(
        "Step by step instructions to bypass auth.",
        language_mode="english_only", conversation_mode="conversation")
    _check(suite, "unsafe_redacted",
           red["safety_summary"]["unsafe_leakage_detected"] is True
           and red["safety_summary"]["replacements_count"] >= 1, str(red))

    seg = [{"segment_id": "s1", "text": "x",
             "safety_tags": ["recognition_only"]},
            {"segment_id": "s2", "text": "y",
             "safety_tags": ["do_not_use_unprompted"]},
            {"segment_id": "s3", "text": "z",
             "register_tags": ["vulgar"], "safety_tags": ["vulgar"]},
            {"segment_id": "s4", "text": "ok",
             "register_tags": ["standard"]}]
    out = ssr.redact_segments_for_voice(seg, conversation_mode="conversation",
                                          is_user_prompted=False)
    _check(suite, "recognition_only_flagged_not_blocked",
           any(s["segment_id"] == "s1"
               and s.get("_suggestion_blocked") is True
               for s in out["safe"]),
           str(out))
    _check(suite, "do_not_use_unprompted_blocked",
           any(b["segment_id"] == "s2" for b in out["blocked"]), str(out))
    _check(suite, "vulgar_blocked",
           any(b["segment_id"] == "s3" for b in out["blocked"]), str(out))
    _check(suite, "benign_kept",
           any(s["segment_id"] == "s4" for s in out["safe"]), str(out))

    risk = ssr.classify_spoken_safety_risk(
        "yo bruh", language_mode="english_only", conversation_mode="teacher")
    _check(suite, "slang_in_teacher_flagged",
           risk["ok"] is False and len(risk["risks"]) >= 1, str(risk))

    val = ssr.validate_voice_safe_text("Hello, how are you today?",
                                        language_mode="english_only")
    _check(suite, "clean_text_validates",
           val["ok"] is True, str(val))

    exp = ssr.explain_spoken_redaction(red["decision"])
    _check(suite, "redaction_explanation",
           "Replacements" in exp["explanation"], str(exp))

    rep = _td() / "safety.json"
    ssr.write_spoken_safety_report(red, rep)
    _check(suite, "safety_report_written", rep.exists(), "")


# -------------------- G: Renderer interface --------------------

def suite_G_renderer() -> None:
    suite = "G_RENDERER"
    contract = vri.get_voice_renderer_contract()
    _check(suite, "contract_unbound",
           contract["binding"] == "UNBOUND_FUTURE_RENDERER"
           and contract["audio_synthesis_in_this_phase"] is False,
           str(contract)[:120])

    payload = src.create_spoken_render_payload(
        "Hello.", language_mode="english_only",
        segments=[{"text": "Hello.", "language": "en",
                    "segment_type": "sentence",
                    "start_index": 0, "end_index": 6}])
    req = vri.create_renderer_request_from_payload(payload)
    _check(suite, "renderer_request_dry_run",
           req["dry_run"] is True, str(req)[:200])
    v = vri.validate_renderer_request(req)
    _check(suite, "renderer_request_validates",
           v["ok"] is True, str(v))

    # Non-dry-run rejected
    nondry = dict(req)
    nondry["dry_run"] = False
    v_nd = vri.validate_renderer_request(nondry)
    _check(suite, "non_dry_run_rejected",
           v_nd["ok"] is False
           and "dry_run_must_be_true_in_phase25" in v_nd["reason"],
           str(v_nd))

    # Capabilities validator
    caps = {"supports_code_switching": True,
            "supports_prosody": True,
            "supports_pronunciation_hints": True,
            "supports_emotional_tone": True,
            "accepted_languages": ["en", "ru"],
            "max_text_chars": 5000, "max_segments": 100}
    v_caps = vri.validate_renderer_capabilities(caps)
    _check(suite, "capabilities_validate",
           v_caps["ok"] is True, str(v_caps))

    sim = vri.simulate_renderer_acceptance(payload)
    _check(suite, "simulated_acceptance_ok",
           sim["accepted"] is True
           and "no_renderer_invoked" in sim["note"], str(sim))

    rep = _td() / "ri.json"
    vri.write_renderer_interface_report(contract, rep)
    _check(suite, "renderer_report_written", rep.exists(), "")


# -------------------- H: Spoken render runtime --------------------

def suite_H_runtime() -> None:
    suite = "H_RUNTIME"
    en = rrt.build_spoken_render_payload(
        "Hello, what is an engineer?", limit=5)
    _check(suite, "en_runtime_ok",
           en["ok"] is True
           and en["language_detection"]["chosen_spoken_mode"]
           == "english_only", str(en)[:200])

    ru = rrt.build_spoken_render_payload(
        "Привет, что такое инженер?", limit=5)
    _check(suite, "ru_runtime_ok",
           ru["ok"] is True
           and ru["language_detection"]["chosen_spoken_mode"]
           == "russian_only", str(ru)[:200])

    mx = rrt.build_spoken_render_payload(
        "Hello, я инженер and I work.", limit=5)
    _check(suite, "mix_runtime_ok",
           mx["ok"] is True
           and mx["language_detection"]["chosen_spoken_mode"]
           in ("mixed_en_ru", "english_with_russian_terms"),
           str(mx)[:200])

    required_fields = ("language_detection", "voice_style_plan",
                       "normalized_text", "voice_safe_text",
                       "segments", "prosody_plan",
                       "pronunciation_hints",
                       "code_switch_boundaries",
                       "safety_summary",
                       "renderer_request_dry_run",
                       "validation", "gap_notes")
    _check(suite, "en_runtime_required_fields",
           set(required_fields) <= set(en.keys()),
           f"missing={set(required_fields) - set(en.keys())}")
    _check(suite, "renderer_dry_run_true",
           en["renderer_request_dry_run"]["dry_run"] is True, "")

    safe_only = rrt.build_voice_safe_render_payload(
        "Hello world.", language_mode="english_only",
        conversation_mode="conversation")
    _check(suite, "safe_only_helper",
           safe_only["ok"] is True
           and safe_only["validation"]["ok"] is True, str(safe_only)[:200])

    val_pack = rrt.validate_and_prepare_renderer_request(safe_only["payload"])
    _check(suite, "renderer_request_prepared_ok",
           val_pack["ok"] is True
           and val_pack["request"]["dry_run"] is True, str(val_pack)[:200])

    demo = rrt.demo_spoken_render_payloads(limit=5)
    _check(suite, "demo_bounded",
           demo["ok"] is True and demo["count"] <= 5
           and len(demo["scenarios"]) == demo["count"], str(demo)[:200])

    out = _td() / "rt.json"
    rrt.write_spoken_render_runtime_report(en, out)
    _check(suite, "runtime_report_written", out.exists(), "")


# -------------------- I: Production safety --------------------

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
        before_c = conn.execute("SELECT COUNT(*) FROM concepts").fetchone()[0]
        before_l = conn.execute(
            "SELECT COUNT(*) FROM entry_links").fetchone()[0]
    finally:
        conn.close()
    rrt.build_spoken_render_payload("Hello, я инженер", limit=5)
    rrt.demo_spoken_render_payloads(limit=3)
    after_en = enlex.count_words()
    after_ru = rulex.count_words()
    after_phr = rulex.count_phrases()
    after_mans = (len(glob.glob("seed_packs/en/*.en_pack_manifest.json"))
                  + len(glob.glob("seed_packs/ru/*.ru_pack_manifest.json")))
    conn = sqlite3.connect("bilingual_stack/bilingual_links.sqlite")
    try:
        after_c = conn.execute("SELECT COUNT(*) FROM concepts").fetchone()[0]
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


# -------------------- J: Isolation --------------------

PHASE25_FILES = [
    "bilingual_spoken_render_contract.py",
    "bilingual_voice_text_normalizer.py",
    "bilingual_prosody_markup.py",
    "bilingual_pronunciation_hinting.py",
    "bilingual_spoken_safety_redactor.py",
    "bilingual_voice_renderer_interface.py",
    "bilingual_spoken_render_runtime.py",
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
    for fname in PHASE25_FILES:
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
        ("B_CONTRACT", suite_B_contract),
        ("C_NORMALIZER", suite_C_normalizer),
        ("D_PROSODY", suite_D_prosody),
        ("E_PRONUNCIATION", suite_E_pronunciation),
        ("F_SAFETY_REDACT", suite_F_safety_redact),
        ("G_RENDERER", suite_G_renderer),
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
    print("=== Phase 25 Spoken Render Contract ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
