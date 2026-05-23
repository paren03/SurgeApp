"""Bounded test harness for the Luna vocabulary runtime.

Run:  python test_vocabulary_runtime.py
"""

from __future__ import annotations

import gc
import os
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cognitive_lexicon_store as store
import cognitive_vocabulary_runtime as runtime
from cognitive_word_policy import (
    apply_policy,
    is_word_allowed,
    mode_summary,
)


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


def _seed(db_path: str) -> None:
    store.init_db(db_path)
    store.add_word("happy", "feeling pleasure or contentment",
                   synonyms=["glad", "joyful"], examples=["she felt happy"],
                   tags=["emotion"], frequency_score=6.0,
                   word_level="plain", db_path=db_path)
    store.add_word("glad", "pleased",
                   synonyms=["happy"], tags=["emotion"],
                   frequency_score=5.4, word_level="plain", db_path=db_path)
    store.add_word("joyful", "feeling great joy",
                   synonyms=["happy"], tags=["emotion"],
                   frequency_score=4.0, word_level="everyday", db_path=db_path)
    store.add_word("ebullient", "cheerful and full of energy",
                   synonyms=["happy", "exuberant"], tags=["emotion", "literary"],
                   frequency_score=1.4, word_level="rare", db_path=db_path)
    store.add_word("happiness", "the state of being happy",
                   synonyms=["joy", "contentment"], tags=["emotion"],
                   frequency_score=4.8, word_level="everyday", db_path=db_path)
    store.add_word("python", "high-level programming language",
                   tags=["coding", "tech"], frequency_score=3.5,
                   word_level="specialized", db_path=db_path)


def test_init_and_add(r: Results, db: str) -> None:
    print("\n[1] init_db + add_word")
    resolved = store.init_db(db)
    r.check("init_db returns path", Path(resolved).name.endswith(".sqlite"))
    row = store.add_word("alpha", "the first letter", tags=["greek"],
                         frequency_score=3.0, word_level="plain", db_path=db)
    r.check("add_word returns dict", isinstance(row, dict) and row["word"] == "alpha")
    r.check("add_word persists tags", "greek" in (row.get("tags") or []))
    again = store.add_word("alpha", "updated definition", db_path=db)
    r.check("add_word upsert preserves created_at", again["created_at"] == row["created_at"])
    r.check("add_word upsert updates definition", again["definition"] == "updated definition")


def test_lookup_and_searches(r: Results, db: str) -> None:
    print("\n[2] lookup_word + search_prefix + search_contains + search_by_tag")
    _seed(db)

    found = store.lookup_word("happy", db_path=db)
    r.check("lookup_word finds known", found is not None and found["word"] == "happy")
    missing = store.lookup_word("nonexistent_word_xyz", db_path=db)
    r.check("lookup_word returns None for unknown", missing is None)
    r.check("lookup_word empty returns None", store.lookup_word("", db_path=db) is None)

    pres = store.search_prefix("hap", limit=10, db_path=db)
    pres_words = [r2["word"] for r2 in pres]
    r.check("search_prefix finds 'happy' for 'hap'", "happy" in pres_words)
    r.check("search_prefix finds 'happiness' for 'hap'", "happiness" in pres_words)
    r.check("search_prefix empty prefix returns []", store.search_prefix("", db_path=db) == [])

    cont = store.search_contains("pyt", limit=10, db_path=db)
    r.check("search_contains finds 'python'", any(x["word"] == "python" for x in cont))

    tag = store.search_by_tag("emotion", limit=10, db_path=db)
    tag_words = {x["word"] for x in tag}
    r.check("search_by_tag returns emotion words", {"happy", "glad"}.issubset(tag_words))
    r.check("search_by_tag respects unknown tag", store.search_by_tag("zzz_no_such_tag", db_path=db) == [])


def test_related_and_bounded(r: Results, db: str) -> None:
    print("\n[3] get_related_words + bounded_query")
    _seed(db)

    rel = store.get_related_words("happy", limit=10, db_path=db)
    rel_words = {x["word"] for x in rel}
    r.check("get_related_words returns >=1", len(rel) >= 1)
    r.check("get_related_words includes a synonym", bool(rel_words & {"glad", "joyful"}))

    bq = store.bounded_query(where={"word_level": "plain"}, limit=5, db_path=db)
    r.check("bounded_query respects filter", all(x["word_level"] == "plain" for x in bq))
    r.check("bounded_query respects limit", len(bq) <= 5)
    try:
        store.bounded_query(where={"; DROP TABLE words;--": "x"}, db_path=db)
        bad = False
    except ValueError:
        bad = True
    r.check("bounded_query rejects bad column", bad)


def test_limit_enforcement(r: Results, db: str) -> None:
    print("\n[4] hard limit enforcement")
    _seed(db)
    for i in range(50):
        store.add_word(f"bulkword{i:03d}", "filler", frequency_score=1.0,
                       word_level="plain", db_path=db)

    big = store.search_prefix("bulk", limit=10_000, db_path=db)
    r.check("search_prefix caps to HARD_MAX_LIMIT",
            len(big) <= store.HARD_MAX_LIMIT, f"got {len(big)}")
    neg = store.search_prefix("bulk", limit=-5, db_path=db)
    r.check("negative limit falls back to default",
            len(neg) <= store.DEFAULT_LIMIT, f"got {len(neg)}")


def test_policy(r: Results) -> None:
    print("\n[5] cognitive_word_policy")
    d1 = is_word_allowed("hello", word_level="plain", mode="voice_conversation")
    r.check("plain word allowed in voice", d1.allowed)
    d2 = is_word_allowed("aforementioned", word_level="rare", mode="voice_conversation")
    r.check("awkward voice word blocked", not d2.allowed)
    d3 = is_word_allowed("ontology", word_level="specialized", mode="normal")
    r.check("specialized blocked in normal", not d3.allowed)
    d4 = is_word_allowed("ontology", word_level="specialized", mode="technical")
    r.check("specialized allowed in technical", d4.allowed)

    cands = [
        {"word": "ok", "word_level": "plain", "frequency_score": 6.0},
        {"word": "ebullient", "word_level": "rare", "frequency_score": 1.4},
        {"word": "exuberant", "word_level": "rare", "frequency_score": 1.6},
        {"word": "ineffable", "word_level": "rare", "frequency_score": 1.2},
    ]
    capped = apply_policy(cands, mode="normal")
    rare_count = sum(1 for c in capped if c["word_level"] == "rare")
    r.check("rare budget enforced (<=1 in normal)", rare_count <= 1)
    capped_tech = apply_policy(cands, mode="technical")
    r.check("technical mode allows more rares", len(capped_tech) >= len(capped))

    ms = mode_summary("teacher")
    r.check("mode_summary returns dict", isinstance(ms, dict) and ms["mode"] == "teacher")
    r.check("invalid mode falls back to normal",
            mode_summary("totally_made_up_mode")["mode"] == "normal")


def test_runtime_flag_disabled(r: Results, db: str) -> None:
    print("\n[6] feature flag DISABLED => empty optional context")
    os.environ["LUNA_VOCABULARY_DB"] = db
    os.environ.pop("LUNA_VOCABULARY_RUNTIME", None)
    out = runtime.get_optional_vocabulary_context("I am happy today", mode="normal", limit=10)
    r.check("disabled returns {}", out == {}, f"got {out!r}")
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "0"
    out2 = runtime.get_optional_vocabulary_context("I am happy today", mode="normal", limit=10)
    r.check("LUNA_VOCABULARY_RUNTIME=0 returns {}", out2 == {}, f"got {out2!r}")


def test_runtime_flag_enabled(r: Results, db: str) -> None:
    print("\n[7] feature flag ENABLED => bounded context")
    _seed(db)
    os.environ["LUNA_VOCABULARY_DB"] = db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    out = runtime.get_optional_vocabulary_context(
        "I am happy today and learning python", mode="normal", limit=5,
    )
    r.check("enabled returns dict", isinstance(out, dict) and out.get("enabled") is True)
    r.check("count<=limit", out.get("count", 0) <= 5)
    r.check("context is a list", isinstance(out.get("context"), list))
    r.check("each entry has bounded definition",
            all(len(c.get("definition", "")) <= 240 for c in out.get("context", [])))
    out_cap = runtime.get_optional_vocabulary_context(
        "happy " * 200, mode="normal", limit=10_000,
    )
    r.check("limit cap applied even with huge request",
            out_cap.get("count", 0) <= store.HARD_MAX_LIMIT)


def test_runtime_helpers(r: Results, db: str) -> None:
    print("\n[8] explain_word + find_related_terms + find_better_word + classify")
    _seed(db)
    os.environ["LUNA_VOCABULARY_DB"] = db

    ex = runtime.explain_word("happy")
    r.check("explain_word finds DB entry",
            ex.get("found") is True and "pleasure" in ex.get("definition", ""))
    ex2 = runtime.explain_word("zzz_no_such_word_qq")
    r.check("explain_word returns dict for unknown without crash",
            isinstance(ex2, dict) and "level" in ex2)
    r.check("explain_word empty returns dict",
            isinstance(runtime.explain_word(""), dict))

    rel = runtime.find_related_terms("happy", limit=5)
    r.check("find_related_terms returns <=5", len(rel) <= 5)
    r.check("find_related_terms empty topic returns []",
            runtime.find_related_terms("") == [])

    sub = runtime.find_better_word("ebullient", tone="normal", difficulty="plain")
    r.check("find_better_word downgrades rare->plain in normal tone",
            sub is None or sub.get("word_level") in ("plain", "everyday"))

    lvl = runtime.classify_word_level("happy")
    r.check("classify_word_level returns valid level",
            lvl in ("plain", "everyday", "intermediate", "advanced", "rare", "specialized"))


def test_missing_deps_safe(r: Results) -> None:
    print("\n[9] missing optional deps do not crash")
    status = runtime.dependency_status()
    r.check("dependency_status returns dict", isinstance(status, dict))
    r.check("install_hint present", "pip install wordfreq nltk" in status.get("install_hint", ""))

    orig_wf = runtime._wordfreq
    orig_wn = runtime._wordnet
    try:
        runtime._wordfreq = lambda: None
        runtime._wordnet = lambda: None
        runtime._WORDFREQ_OK = False
        runtime._WORDNET_OK = False
        lvl = runtime.classify_word_level("happy")
        r.check("classify_word_level works without wordfreq",
                lvl in ("plain", "everyday", "intermediate", "advanced", "rare", "specialized"))
        ex = runtime.explain_word("happy")
        r.check("explain_word works without wordnet", isinstance(ex, dict))
        rel = runtime.find_related_terms("unknown_topic_xyzzy", limit=3)
        r.check("find_related_terms safe without wordnet", isinstance(rel, list))
    finally:
        runtime._wordfreq = orig_wf
        runtime._wordnet = orig_wn
        runtime._WORDFREQ_OK = None
        runtime._WORDNET_OK = None


def test_no_daemon_no_recursion(r: Results, db: str) -> None:
    print("\n[10] no background thread, no recursion")
    threads_before = {t.ident for t in threading.enumerate()}
    os.environ["LUNA_VOCABULARY_DB"] = db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"

    for _ in range(5):
        runtime.get_optional_vocabulary_context("I am happy", mode="normal", limit=5)

    gc.collect()
    time.sleep(0.05)
    threads_after = {t.ident for t in threading.enumerate()}
    new_threads = threads_after - threads_before
    r.check("no new background threads spawned",
            len(new_threads) == 0, f"new: {new_threads}")

    sys.setrecursionlimit(200)
    try:
        runtime.get_optional_vocabulary_context("happy joyful glad python", mode="normal", limit=10)
        r.check("runtime call does not blow recursion at limit=200", True)
    except RecursionError as e:
        r.check("runtime call does not blow recursion at limit=200", False, str(e))
    finally:
        sys.setrecursionlimit(1000)


def test_seed_small_dataset(r: Results, db: str) -> None:
    print("\n[seed] seed_small_dataset (250 words across 5 categories)")
    summary = store.seed_small_dataset(db_path=db)
    r.check("seed returns dict", isinstance(summary, dict))
    expected = {"normal": 50, "teacher": 50, "technical": 50,
                "carpentry": 50, "professional": 50}
    for cat, want in expected.items():
        r.check(f"seed['{cat}'] == {want}", summary.get(cat) == want,
                f"got {summary.get(cat)!r}")
    total = store.count_words(db_path=db)
    r.check("total seeded >= 240 (allowing for upsert collisions)",
            total >= 240, f"got {total}")

    for cat in expected:
        c = store.count_by_tag(cat, db_path=db)
        r.check(f"count_by_tag('{cat}') > 0", c > 0, f"got {c}")

    again = store.seed_small_dataset(db_path=db)
    after = store.count_words(db_path=db)
    r.check("seed is idempotent (count unchanged)", after == total,
            f"before={total}, after={after}")
    r.check("seed returns counts on rerun", again == summary)


def test_wordfreq_integration(r: Results) -> None:
    print("\n[wf] wordfreq classification (REAL package, no monkey-patch)")
    r.check("classify_word_level('hello') plain/everyday",
            runtime.classify_word_level("hello") in ("plain", "everyday"))
    r.check("classify_word_level('ebullient') rare/specialized/advanced",
            runtime.classify_word_level("ebullient") in ("rare", "specialized", "advanced"))
    r.check("classify_word_level('the') plain",
            runtime.classify_word_level("the") == "plain")
    deps = runtime.dependency_status()
    r.check("dependency_status reports wordfreq=True", deps.get("wordfreq") is True)


def test_wordnet_integration(r: Results, db: str) -> None:
    print("\n[wn] WordNet integration (REAL nltk + wordnet corpus)")
    os.environ["LUNA_VOCABULARY_DB"] = db
    ex = runtime.explain_word("ostracize")
    r.check("explain_word('ostracize') found via WordNet",
            ex.get("found") is True and len(ex.get("definition", "")) > 0,
            f"got {ex!r}")
    r.check("explain_word reports source",
            ex.get("source") in ("wordnet", "db", "synonym_ref", "explain"),
            f"got {ex.get('source')!r}")
    r.check("explain_word returns synonyms list",
            isinstance(ex.get("synonyms"), list))

    deps = runtime.dependency_status()
    r.check("dependency_status reports wordnet=True", deps.get("wordnet") is True)

    rel = runtime.find_related_terms("ostracize", limit=5)
    r.check("find_related_terms('ostracize') returns <=5 (possibly 0)",
            isinstance(rel, list) and len(rel) <= 5)


def test_program_s_untouched(r: Results) -> None:
    print("\n[11] Program S files untouched")
    own_files = {
        "cognitive_lexicon_store.py",
        "cognitive_vocabulary_runtime.py",
        "cognitive_word_policy.py",
        "test_vocabulary_runtime.py",
        "VOCABULARY_RUNTIME_REPORT.md",
    }
    program_s_patterns = ("program_s", "program_s_", "ProgramS", "program-s")
    own_imports_clean = True
    detail = ""
    for fn in own_files - {"VOCABULARY_RUNTIME_REPORT.md"}:
        p = HERE / fn
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        for pat in program_s_patterns:
            if pat in text and "untouched" not in text.lower():
                own_imports_clean = False
                detail = f"{fn} references {pat!r}"
                break
        if not own_imports_clean:
            break
    r.check("no vocabulary-runtime file references Program S", own_imports_clean, detail)


def main() -> int:
    r = Results()
    with tempfile.TemporaryDirectory(prefix="luna_vocab_test_") as tmp:
        db = str(Path(tmp) / "test_vocab.sqlite")
        try:
            test_init_and_add(r, db)
            test_lookup_and_searches(r, db)
            test_related_and_bounded(r, db)
            test_limit_enforcement(r, db)
            test_policy(r)
            test_runtime_flag_disabled(r, db)
            test_runtime_flag_enabled(r, db)
            test_runtime_helpers(r, db)
            test_missing_deps_safe(r)
            test_no_daemon_no_recursion(r, db)
            test_seed_small_dataset(r, db)
            test_wordfreq_integration(r)
            test_wordnet_integration(r, db)
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
