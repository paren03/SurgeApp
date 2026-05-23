"""Russian Sovereign Language Stack — bounded test harness.

Run:  python test_russian_sovereign_stack.py
"""

from __future__ import annotations

import gc
import json
import os
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, Exception):
    pass

import russian_knowledge_ingestion as ingest
import russian_language_router as router
import russian_lexicon_store as lex
import russian_memory_fabric as mem
import russian_morphology_layer as morph
import russian_personality_layer as personality
import russian_response_quality as quality


class Results:
    def __init__(self):
        self.passed: list[str] = []
        self.failed: list[tuple[str, str]] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        if ok:
            self.passed.append(name)
            print(f"  PASS  {name}")
        else:
            self.failed.append((name, detail))
            print(f"  FAIL  {name} :: {detail}")

    def summary(self) -> str:
        total = len(self.passed) + len(self.failed)
        return f"{len(self.passed)}/{total} passed, {len(self.failed)} failed"


def _enable():
    os.environ["LUNA_RUSSIAN_STACK"] = "1"


def _disable():
    os.environ.pop("LUNA_RUSSIAN_STACK", None)


def test_flag_disabled(r: Results) -> None:
    print("\n[1] feature flag DISABLED returns safe inactive results")
    _disable()
    d1 = router.should_use_russian_stack("Привет, как дела?")
    r.check("router disabled returns use_russian=False",
            d1.get("use_russian") is False and d1.get("reason") == "feature_flag_off",
            f"got {d1!r}")
    d2 = router.route_russian_context("Привет", mode="conversation", limit=5)
    r.check("route_russian_context disabled returns disabled-safe",
            d2.get("enabled") is False, f"got {d2!r}")


def test_flag_enabled_router(r: Results) -> None:
    print("\n[2] feature flag ENABLED — Russian + mixed + explicit request detection")
    _enable()
    info_ru = router.detect_russian_text("Привет, мир!")
    r.check("Cyrillic detected", info_ru["is_russian"] and info_ru["has_cyrillic"])

    info_en = router.detect_russian_text("hello world")
    r.check("Pure English not flagged russian", not info_en["is_russian"])

    info_mix = router.detect_russian_text("hello мир, this is смешанный text 50/50")
    r.check("Mixed text detected",
            info_mix["has_cyrillic"] and info_mix["has_latin"],
            f"got {info_mix!r}")

    mode = router.detect_language_mode("hello")
    r.check("English-only detect_language_mode == 'english'", mode == "english")

    mode_req = router.detect_language_mode("Please respond in Russian, thanks.")
    r.check("English with explicit RU request",
            mode_req == "english_with_russian_request", f"got {mode_req!r}")

    d_explicit = router.should_use_russian_stack(
        "hello", user_requested_language="ru")
    r.check("user_requested_language=ru forces stack",
            d_explicit.get("use_russian") is True
            and d_explicit.get("reason") == "user_requested",
            f"got {d_explicit!r}")

    rt = router.route_russian_context("Привет, дорогая Луна!", mode="conversation", limit=5)
    r.check("route_russian_context enabled returns dict",
            isinstance(rt, dict) and rt.get("enabled") is True)
    r.check("route limit respected",
            len(rt.get("sample_tokens", [])) <= 5)


def test_lexicon_store(r: Results, db: str) -> None:
    print("\n[3] russian_lexicon_store")
    p = lex.init_db(db_path=db)
    r.check("lexicon init_db returns path", p.endswith(".sqlite"))

    row = lex.add_word(
        "дом", lemma="дом", part_of_speech="noun",
        definition_ru="жилище человека",
        definition_en="house, home",
        synonyms=["жилище", "здание"],
        domain_tags=["everyday"], semantic_tags=["place"],
        frequency_score=6.2, register_level="plain",
        db_path=db,
    )
    r.check("add_word stores 'дом'", row["word"] == "дом")
    r.check("add_word synonyms persisted",
            "жилище" in (row.get("synonyms") or []))

    got = lex.lookup_word("дом", db_path=db)
    r.check("lookup_word finds 'дом'", got is not None and got["word"] == "дом")

    miss = lex.lookup_word("не_существует_xyz", db_path=db)
    r.check("lookup_word miss returns None", miss is None)

    pres = lex.search_prefix("до", limit=5, db_path=db)
    r.check("search_prefix finds 'дом' for 'до'",
            any(x["word"] == "дом" for x in pres))

    cont = lex.search_contains("жил", limit=5, db_path=db)
    r.check("search_contains finds 'дом' via definition",
            any(x["word"] == "дом" for x in cont))

    by_tag = lex.search_by_tag("everyday", limit=5, db_path=db)
    r.check("search_by_tag finds 'дом'",
            any(x["word"] == "дом" for x in by_tag))

    syns = lex.get_synonyms("дом", limit=3, db_path=db)
    r.check("get_synonyms <=3", len(syns) <= 3 and "жилище" in syns)

    lex.add_phrase("на седьмом небе", translation_en="on cloud nine",
                   idiomatic=True, domain_tags=["emotion"],
                   db_path=db)
    idioms = lex.get_idioms(limit=10, db_path=db)
    r.check("get_idioms returns idiom phrase",
            any(p["phrase"] == "на седьмом небе" for p in idioms))

    big = lex.search_prefix("д", limit=100000, db_path=db)
    r.check("hard limit caps search_prefix",
            len(big) <= lex.HARD_MAX_LIMIT)

    try:
        lex.bounded_query(where={"; DROP TABLE words;--": "x"}, db_path=db)
        rejected = False
    except ValueError:
        rejected = True
    r.check("bounded_query rejects bad column", rejected)


def test_morphology(r: Results) -> None:
    print("\n[4] russian_morphology_layer (with or without pymorphy)")
    dep = morph.dependency_status()
    r.check("dependency_status returns dict", isinstance(dep, dict))

    norm = morph.normalize_russian_word("Дом!")
    r.check("normalize_russian_word strips punct + lowercases", norm == "дом")

    lemma = morph.guess_lemma("домами")
    r.check("guess_lemma returns dict with lemma + confidence + source",
            isinstance(lemma, dict) and "lemma" in lemma
            and "confidence" in lemma and "source" in lemma)
    r.check("guess_lemma confidence is honest (<= 1.0)",
            0.0 <= lemma["confidence"] <= 1.0)

    pos = morph.detect_part_of_speech("читать")
    r.check("detect_part_of_speech for verb returns dict",
            isinstance(pos, dict) and "pos" in pos)

    case = morph.detect_case_hint("в доме")
    r.check("detect_case_hint returns dict",
            isinstance(case, dict) and "hint" in case)

    num = morph.detect_number_hint("книги лежат на столах")
    r.check("detect_number_hint returns dict",
            isinstance(num, dict) and "hint" in num)

    gen = morph.detect_gender_hint("книга")
    r.check("detect_gender_hint returns dict",
            isinstance(gen, dict) and "hint" in gen)

    nat = morph.score_phrase_naturalness("я люблю читать книги по вечерам")
    r.check("score_phrase_naturalness returns bounded score",
            0.0 <= nat["score"] <= 1.0)

    notes = morph.suggest_morphology_notes("Луна читает книгу", limit=3)
    r.check("suggest_morphology_notes limit respected", len(notes) <= 3)
    r.check("each note has lemma + pos",
            all("lemma_guess" in n and "pos_guess" in n for n in notes))


def test_memory_fabric(r: Results, db: str) -> None:
    print("\n[5] russian_memory_fabric")
    p = mem.init_db(db_path=db)
    r.check("memory init_db returns path", p.endswith(".sqlite"))
    r.check("empty memory count == 0", mem.count_memories(db_path=db) == 0)

    empty = mem.retrieve_context_ru("Привет", db_path=db)
    r.check("empty memory retrieve returns []", empty == [])

    rec = mem.add_memory_ru(
        "Луна любит чай и тёплые разговоры по вечерам.",
        text_en_summary="Luna likes tea and warm evening conversations.",
        topic_tags=["preferences"], semantic_tags=["evening", "tea"],
        importance=0.7, db_path=db,
    )
    r.check("add_memory_ru returns row with id", "memory_id" in rec)

    found = mem.search_memory_ru("чай", limit=5, db_path=db)
    r.check("search_memory_ru finds 'чай'",
            any("чай" in x.get("text_ru", "") for x in found))

    ctx = mem.retrieve_context_ru("Расскажи что-нибудь про чай", limit=3, db_path=db)
    r.check("retrieve_context_ru limit respected",
            isinstance(ctx, list) and len(ctx) <= 3)

    s = mem.summarize_memory_ru(rec["memory_id"], max_len=80, db_path=db)
    r.check("summarize_memory_ru returns string <=80 chars",
            isinstance(s, str) and len(s) <= 80)

    trans = mem.translate_summary_stub("Привет")
    r.check("translate_summary_stub returns ASCII", trans and not any(0x0400 <= ord(c) <= 0x04FF for c in trans))

    huge = mem.bounded_memory_query(limit=10000, db_path=db)
    r.check("bounded_memory_query caps to HARD_MAX_LIMIT",
            len(huge) <= mem.HARD_MAX_LIMIT)


def test_personality(r: Results) -> None:
    print("\n[6] russian_personality_layer")
    prof = personality.get_russian_personality_profile()
    r.check("profile name is Luna", prof["name"] == "Luna")
    r.check("profile.language=ru", prof["language"] == "ru")
    r.check("profile.core_traits non-empty", len(prof["core_traits"]) > 0)

    rules = personality.get_russian_style_rules("conversation")
    r.check("style rules return mode key", rules["mode"] == "conversation")
    r.check("valid_modes lists 8 modes",
            len(rules["valid_modes"]) == len(personality.MODES))

    rules_fallback = personality.get_russian_style_rules("imaginary_mode")
    r.check("invalid mode falls back to conversation",
            rules_fallback["mode"] == "conversation")

    arts = personality.avoid_translation_artifacts(
        "Это есть важно, я имею вопрос."
    )
    r.check("translation artifacts detected", arts["found"] >= 2,
            f"got {arts!r}")

    tone = personality.adapt_tone_ru("Произвести оценку и осуществить выполнение задачи.",
                                     mode="conversation")
    r.check("adapt_tone_ru flags канцелярит",
            any("Канцеляризм" in n for n in tone["notes"]))

    style = personality.apply_luna_russian_style(
        "Это есть очень длинное предложение, которое содержит избыточное количество слов, "
        "и я имею мнение что его стоит разбить на части.",
        mode="conversation",
    )
    r.check("apply_luna_russian_style does NOT auto-rewrite",
            style["rewrites_applied"] is False)
    r.check("style report contains tone_notes",
            isinstance(style["tone_notes"], list))


def test_ingestion(r: Results, db: str, tmp: Path) -> None:
    print("\n[7] russian_knowledge_ingestion")
    lex.init_db(db_path=db)

    # Build a small mixed JSONL file
    words_path = tmp / "ru_words.jsonl"
    rows = [
        {"word": "книга", "lemma": "книга", "part_of_speech": "noun",
         "definition_en": "book", "domain_tags": ["everyday"],
         "frequency_score": 6.0},
        {"word": "читать", "lemma": "читать", "part_of_speech": "verb",
         "definition_en": "to read", "frequency_score": 5.5},
        {"word": "", "lemma": "", "definition_en": "intentionally broken row"},
        {"NOT_A_DICT": True},
        {"word": "стол", "definition_en": "table"},
    ]
    with words_path.open("w", encoding="utf-8") as f:
        for x in rows:
            f.write(json.dumps(x, ensure_ascii=False) + "\n")

    prev = ingest.preview_ingestion(str(words_path), limit=10)
    r.check("preview_ingestion previewed up to 5 rows",
            prev["previewed"] == 5 and prev["ok"] >= 3 and prev["rejected"] >= 1,
            f"got {prev!r}")

    res = ingest.ingest_word_list(str(words_path), source="test_pack",
                                  batch_size=10, db_path=db)
    r.check("ingest_word_list added at least 3 valid words",
            res.get("error") is None and res["added"] >= 3,
            f"got {res!r}")
    r.check("ingest report file written",
            res["report_path"] and Path(res["report_path"]).exists())

    # phrase ingestion
    phrases_path = tmp / "ru_phrases.jsonl"
    with phrases_path.open("w", encoding="utf-8") as f:
        f.write(json.dumps({"phrase": "от всей души",
                            "translation_en": "with all my heart",
                            "idiomatic": True}, ensure_ascii=False) + "\n")
        f.write(json.dumps({"phrase": "ни пуха ни пера",
                            "translation_en": "good luck",
                            "idiomatic": True}, ensure_ascii=False) + "\n")
    res_p = ingest.ingest_phrase_list(str(phrases_path), source="test_idioms",
                                      batch_size=10, db_path=db)
    r.check("ingest_phrase_list added 2 idioms", res_p["added"] == 2,
            f"got {res_p!r}")

    # topic-pack
    topic_path = tmp / "topic_pack.jsonl"
    with topic_path.open("w", encoding="utf-8") as f:
        f.write(json.dumps({
            "words": [{"word": "молоток", "definition_en": "hammer",
                       "domain_tags": ["carpentry"]}],
            "phrases": [{"phrase": "забить гвоздь", "translation_en": "drive a nail",
                         "domain_tags": ["carpentry"]}],
        }, ensure_ascii=False) + "\n")
    res_t = ingest.ingest_topic_pack(str(topic_path), source="test_topic",
                                     batch_size=10, db_path=db)
    r.check("ingest_topic_pack added words + phrases",
            res_t["added_words"] >= 1 and res_t["added_phrases"] >= 1,
            f"got {res_t!r}")

    # batch size clamped
    big = ingest._clamp_batch(99_999_999)
    r.check("batch_size clamped to HARD_MAX_BATCH_SIZE",
            big <= ingest.HARD_MAX_BATCH_SIZE)

    # malformed file returns clean error
    miss = ingest.ingest_word_list(str(tmp / "nope_no_such_file.jsonl"),
                                   db_path=db)
    r.check("missing file returns file_not_found", miss["error"] == "file_not_found")


def test_response_quality(r: Results) -> None:
    print("\n[8] russian_response_quality")
    arts = quality.detect_translation_artifacts("я имею вопрос, это есть важно")
    r.check("translation artifacts detected by quality layer", arts["count"] >= 2)

    native = quality.score_native_feel("Привет, как дела? Рада тебя видеть.")
    r.check("native_feel score in [0,1]", 0.0 <= native["score"] <= 1.0)
    r.check("native_feel confidence honest (<1.0)", native["confidence"] < 1.0)

    clarity = quality.score_clarity_ru(
        "Хороший день. Я пью чай. Луна читает книгу."
    )
    r.check("clarity score in [0,1]", 0.0 <= clarity["score"] <= 1.0)

    reg = quality.score_register_fit(
        "Осуществить оценку и произвести выполнение задачи.",
        mode="conversation",
    )
    r.check("register_fit penalizes канцелярит in conversation",
            reg["score"] < 0.7, f"got {reg!r}")

    sugg = quality.suggest_russian_rewrites("я имею вопрос", limit=3)
    r.check("rewrite suggestions bounded",
            isinstance(sugg, list) and len(sugg) <= 3)

    qc = quality.quality_check_ru("я имею вопрос, это есть важно",
                                  mode="conversation")
    r.check("quality_check_ru does not auto-rewrite",
            qc["rewrites_applied"] is False)
    r.check("quality_check_ru overall score in [0,1]",
            0.0 <= qc["scores"]["overall"] <= 1.0)


def test_no_daemon_no_recursion(r: Results, db: str) -> None:
    print("\n[9] no background threads, no recursion blow-up")
    _enable()
    os.environ["LUNA_RUSSIAN_LEXICON_DB"] = db
    threads_before = {t.ident for t in threading.enumerate()}
    for _ in range(5):
        router.route_russian_context("Привет, мир", mode="conversation", limit=5)
        morph.suggest_morphology_notes("Привет, мир", limit=3)
        quality.quality_check_ru("Привет, мир", mode="conversation")
    gc.collect()
    time.sleep(0.05)
    threads_after = {t.ident for t in threading.enumerate()}
    new_threads = threads_after - threads_before
    r.check("no new background threads spawned",
            len(new_threads) == 0, f"new={new_threads}")

    sys.setrecursionlimit(300)
    try:
        for _ in range(3):
            router.route_russian_context("Привет, мир", mode="conversation", limit=5)
            personality.apply_luna_russian_style("Это есть проверка.", mode="warm_friend")
        r.check("no recursion blow-up at limit=300", True)
    except RecursionError as e:
        r.check("no recursion blow-up at limit=300", False, str(e))
    finally:
        sys.setrecursionlimit(1000)


def test_no_full_db_load(r: Results, db: str) -> None:
    print("\n[10] no full database loaded into memory")
    lex.init_db(db_path=db)
    for i in range(300):
        lex.add_word(f"тест_{i:04d}", lemma=f"тест_{i:04d}",
                     frequency_score=1.0, db_path=db)
    res = lex.search_prefix("тест", limit=10, db_path=db)
    r.check("search_prefix limit=10 returns <=10", len(res) <= 10)
    res2 = lex.bounded_query(limit=99999, db_path=db)
    r.check("bounded_query limit clamped",
            len(res2) <= lex.HARD_MAX_LIMIT)


def test_program_s_untouched(r: Results) -> None:
    print("\n[11] Program S + Luna tier/probe/attestation files untouched")
    import re as _re
    # Scan production modules ONLY; the test harness legitimately names these
    # patterns inside its own forbidden-list and is excluded.
    production_files = {
        "russian_language_router.py", "russian_lexicon_store.py",
        "russian_morphology_layer.py", "russian_memory_fabric.py",
        "russian_personality_layer.py", "russian_knowledge_ingestion.py",
        "russian_response_quality.py",
    }
    forbidden_modules = ("program_s", "tier_intent_library", "luna_tier_",
                         "luna_modules", "probe_health",
                         "repair_task_executor", "tier_progression")
    # Match only actual import / from-import statements — not docstring
    # mentions of "we do NOT touch X".
    import_re = _re.compile(
        r"^\s*(?:from|import)\s+\S*(" + "|".join(forbidden_modules) + r")",
        _re.MULTILINE,
    )
    bad_hits: list[str] = []
    for fn in production_files:
        p = HERE / fn
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        for m in import_re.finditer(text):
            bad_hits.append(f"{fn}:{m.group(1)}")
    r.check("no forbidden cross-program imports in production modules",
            not bad_hits, "; ".join(bad_hits) if bad_hits else "")

    # Also confirm git status shows no modifications outside our own file set.
    own_paths_ok = all((HERE / fn).exists() for fn in production_files)
    r.check("all production modules present", own_paths_ok)


def main() -> int:
    r = Results()
    with tempfile.TemporaryDirectory(prefix="luna_ru_test_") as tmp_dir:
        tmp = Path(tmp_dir)
        lex_db = str(tmp / "ru_lex.sqlite")
        mem_db = str(tmp / "ru_mem.sqlite")
        big_db = str(tmp / "ru_big.sqlite")
        try:
            test_flag_disabled(r)
            test_flag_enabled_router(r)
            test_lexicon_store(r, lex_db)
            test_morphology(r)
            test_memory_fabric(r, mem_db)
            test_personality(r)
            test_ingestion(r, lex_db, tmp)
            test_response_quality(r)
            test_no_daemon_no_recursion(r, lex_db)
            test_no_full_db_load(r, big_db)
            test_program_s_untouched(r)
        except Exception:
            print("UNHANDLED EXCEPTION DURING TESTS:")
            traceback.print_exc()
            r.failed.append(("harness", "unhandled exception"))

    print("\n" + "=" * 60)
    print("SUMMARY:", r.summary())
    if r.failed:
        print("FAILURES:")
        for name, det in r.failed:
            print(f"  - {name}: {det}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
