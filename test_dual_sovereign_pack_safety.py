"""Dual Sovereign — Phase 12 safety / taxonomy / pack-manifest tests.

Run:  python test_dual_sovereign_pack_safety.py
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

import cognitive_lexicon_store as en_store
import cognitive_vocabulary_runtime as en_runtime
import cognitive_word_policy as en_policy
import coverage_taxonomy as tax
import english_knowledge_ingestion as en_ingest
import pack_manifest as pm
import russian_lexicon_store as ru_store
import russian_personality_layer as ru_personality


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


def test_taxonomy(r: Results) -> None:
    print("\n[1] coverage_taxonomy")
    cov = tax.validate_coverage_categories(
        ["core_vocabulary", "slang_street_talk", "totally_made_up", "coding"])
    r.check("known cov categories accepted",
            "core_vocabulary" in cov["accepted"]
            and "slang_street_talk" in cov["accepted"])
    r.check("alias 'coding' → 'coding_technology'",
            "coding_technology" in cov["accepted"])
    r.check("unknown category rejected",
            "totally_made_up" in cov["rejected"])

    reg = tax.validate_register_tags(
        ["standard", "vulgar", "definitely_invalid", "noprompt"])
    r.check("known register tags accepted",
            "standard" in reg["accepted"] and "vulgar" in reg["accepted"])
    r.check("alias 'noprompt' → 'do_not_use_unprompted'",
            "do_not_use_unprompted" in reg["accepted"])
    r.check("bad register tag rejected", "definitely_invalid" in reg["rejected"])

    saf = tax.validate_safety_tags(
        ["vulgar", "recognition_only", "do_not_use_unprompted", "harmless"])
    r.check("safety tags accepted", len(saf["accepted"]) == 3)
    r.check("safety: harmless rejected (not a safety tag)",
            "harmless" in saf["rejected"])


def test_english_migration(r: Results, db: str) -> None:
    print("\n[2] English store additive migration")
    p = en_store.init_db(db)
    r.check("english init_db returns path", p.endswith(".sqlite"))
    import sqlite3
    cols = {row[1] for row in
            sqlite3.connect(db).execute("PRAGMA table_info(words)")}
    for col in ("register_tags_json", "safety_tags_json",
                "coverage_categories_json", "pack_source", "pack_id"):
        r.check(f"english.words has '{col}'", col in cols)

    # existing add_word still works
    row = en_store.add_word("freedom", "the state of being free",
                            tags=["concept"], frequency_score=5.0,
                            word_level="everyday", db_path=db)
    r.check("english add_word still works",
            row["word"] == "freedom" and "concept" in row["tags"])

    # new-style add_word with safety + register + coverage
    row2 = en_store.add_word(
        "fuck", "vulgar exclamation",
        word_level="rare", frequency_score=4.5,
        register_tags=["vulgar", "slang"], safety_tags=["vulgar"],
        coverage_categories=["slang_street_talk"],
        pack_source="test_safety_pack", pack_id="pack_001",
        db_path=db,
    )
    r.check("english add_word persists safety_tags",
            "vulgar" in (row2.get("safety_tags") or []))
    r.check("english add_word persists register_tags",
            "vulgar" in (row2.get("register_tags") or []))
    r.check("english add_word persists coverage_categories",
            "slang_street_talk" in (row2.get("coverage_categories") or []))
    r.check("english add_word persists pack_source/pack_id",
            row2.get("pack_source") == "test_safety_pack"
            and row2.get("pack_id") == "pack_001")

    # idempotent re-migration
    en_store.init_db(db)
    cols2 = {row[1] for row in
             sqlite3.connect(db).execute("PRAGMA table_info(words)")}
    r.check("re-running init_db is idempotent", cols == cols2)


def test_russian_migration(r: Results, db: str) -> None:
    print("\n[3] Russian store additive migration")
    p = ru_store.init_db(db)
    r.check("russian init_db returns path", p.endswith(".sqlite"))
    import sqlite3
    cw = {row[1] for row in
          sqlite3.connect(db).execute("PRAGMA table_info(words)")}
    cp = {row[1] for row in
          sqlite3.connect(db).execute("PRAGMA table_info(phrases)")}
    for col in ("register_tags_json", "safety_tags_json",
                "coverage_categories_json", "pack_source", "pack_id"):
        r.check(f"russian.words has '{col}'", col in cw)
        r.check(f"russian.phrases has '{col}'", col in cp)

    # add_word with new fields persists
    row = ru_store.add_word(
        "хуй", lemma="хуй", part_of_speech="noun",
        definition_ru="вульгарное обозначение",
        register_tags=["vulgar"], safety_tags=["vulgar"],
        coverage_categories=["slang_street_talk"],
        pack_source="test_ru_safety", pack_id="ru_pack_001",
        db_path=db,
    )
    r.check("russian add_word persists safety_tags",
            "vulgar" in (row.get("safety_tags") or []))
    r.check("russian add_word persists coverage_categories",
            "slang_street_talk" in (row.get("coverage_categories") or []))
    r.check("russian add_word persists pack_id",
            row.get("pack_id") == "ru_pack_001")

    # add_phrase preserves too
    ph = ru_store.add_phrase(
        "пошёл ты", translation_en="get lost (rude)",
        register_tags=["offensive"], safety_tags=["offensive"],
        coverage_categories=["slang_street_talk"],
        pack_source="test_ru_safety", pack_id="ru_pack_001",
        db_path=db,
    )
    r.check("russian add_phrase persists safety_tags",
            "offensive" in (ph.get("safety_tags") or []))


def test_english_policy(r: Results) -> None:
    print("\n[4] English policy enforcement (safety + is_user_prompted + contexts)")
    rec_only = en_policy.is_word_allowed(
        "secret_term", word_level="plain", mode="normal",
        safety_tags=["recognition_only"],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("recognition_only blocks suggestion", not rec_only.allowed)

    rec_explain = en_policy.is_word_allowed(
        "secret_term", word_level="plain", mode="normal",
        safety_tags=["recognition_only"],
        is_user_prompted=False, decision_context="explanation",
    )
    r.check("recognition_only allows explanation", rec_explain.allowed)

    rec_recog = en_policy.is_word_allowed(
        "secret_term", word_level="plain", mode="normal",
        safety_tags=["recognition_only"],
        is_user_prompted=True, decision_context="recognition",
    )
    r.check("recognition context always allowed", rec_recog.allowed)

    dnu_unprompted = en_policy.is_word_allowed(
        "edgy_term", word_level="plain", mode="normal",
        safety_tags=["do_not_use_unprompted"],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("do_not_use_unprompted blocks when not prompted", not dnu_unprompted.allowed)

    dnu_prompted = en_policy.is_word_allowed(
        "edgy_term", word_level="plain", mode="normal",
        safety_tags=["do_not_use_unprompted"],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("do_not_use_unprompted allows when user-prompted", dnu_prompted.allowed)

    vulgar_normal = en_policy.is_word_allowed(
        "fuck", word_level="rare", mode="normal",
        safety_tags=["vulgar"],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("vulgar blocked in normal mode even when prompted",
            not vulgar_normal.allowed)

    vulgar_voice = en_policy.is_word_allowed(
        "fuck", word_level="rare", mode="voice_conversation",
        safety_tags=["vulgar"],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("vulgar blocked in voice mode", not vulgar_voice.allowed)

    vulgar_teacher = en_policy.is_word_allowed(
        "fuck", word_level="rare", mode="teacher",
        safety_tags=["vulgar"],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("vulgar blocked in teacher mode", not vulgar_teacher.allowed)

    slang_normal = en_policy.is_word_allowed(
        "lit", word_level="plain", mode="normal",
        register_tags=["slang"],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("slang blocked in normal mode w/o prompt", not slang_normal.allowed)

    slang_prompted = en_policy.is_word_allowed(
        "lit", word_level="plain", mode="normal",
        register_tags=["slang"],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("slang allowed when user-prompted in normal mode",
            slang_prompted.allowed)

    slang_informal = en_policy.is_word_allowed(
        "lit", word_level="plain", mode="voice_conversation",
        register_tags=["slang"],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("slang allowed in voice_conversation (informal-class) mode",
            slang_informal.allowed,
            f"reason={slang_informal.reason}")


def test_russian_policy(r: Results) -> None:
    print("\n[5] Russian policy enforcement (safety + is_user_prompted)")
    rec_only = ru_personality.is_entry_allowed_ru(
        "тайное_слово", mode="conversation",
        safety_tags=["recognition_only"],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("RU recognition_only blocks suggestion", not rec_only["allowed"])

    rec_explain = ru_personality.is_entry_allowed_ru(
        "тайное_слово", mode="teacher",
        safety_tags=["recognition_only"],
        is_user_prompted=False, decision_context="explanation",
    )
    r.check("RU recognition_only allows explanation", rec_explain["allowed"])

    dnu = ru_personality.is_entry_allowed_ru(
        "острое_слово", mode="conversation",
        safety_tags=["do_not_use_unprompted"],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("RU do_not_use_unprompted blocked when not prompted",
            not dnu["allowed"])

    dnu_prompted = ru_personality.is_entry_allowed_ru(
        "острое_слово", mode="warm_friend",
        safety_tags=["do_not_use_unprompted"],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("RU do_not_use_unprompted allowed when prompted",
            dnu_prompted["allowed"])

    vulgar_teacher = ru_personality.is_entry_allowed_ru(
        "вульгарное", mode="teacher",
        safety_tags=["vulgar"],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("RU vulgar blocked in teacher mode even when prompted",
            not vulgar_teacher["allowed"])

    slang_conv = ru_personality.is_entry_allowed_ru(
        "сленг_слово", mode="conversation",
        register_tags=["slang"],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("RU slang allowed in conversation mode (informal)",
            slang_conv["allowed"])

    slang_prof = ru_personality.is_entry_allowed_ru(
        "сленг_слово", mode="professional",
        register_tags=["slang"],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("RU slang blocked in professional mode w/o prompt",
            not slang_prof["allowed"])

    filt = ru_personality.filter_russian_entries(
        [{"word": "норм", "safety_tags": [], "register_tags": []},
         {"word": "тайное", "safety_tags": ["recognition_only"], "register_tags": []}],
        mode="conversation", is_user_prompted=False, decision_context="suggestion",
    )
    r.check("RU filter drops recognition_only",
            len(filt) == 1 and filt[0]["word"] == "норм")


def test_pack_manifest(r: Results, tmp: Path) -> None:
    print("\n[6] pack_manifest")
    src = tmp / "fake_pack.jsonl"
    src.write_bytes(b'{"word": "alpha"}\n{"word": "beta"}\n' * 1000)

    sha = pm.compute_sha256(src)
    r.check("sha256 length 64", len(sha) == 64)

    missing = pm.compute_sha256(tmp / "no_such.jsonl")
    r.check("sha256 of missing file is ''", missing == "")

    m = pm.create_pack_manifest(
        source_name="fake_pack",
        language="en",
        coverage_categories=["slang_street_talk", "totally_bogus"],
        register_tags=["slang"],
        safety_tags=["vulgar", "harmless"],
        domain_tags=["test", "test", "demo"],
        row_count=10, accepted_count=8, rejected_count=2, duplicate_count=0,
        source_path=str(src), notes="demo",
    )
    r.check("manifest coverage filtered to known",
            "slang_street_talk" in m["coverage_categories"]
            and "totally_bogus" not in m["coverage_categories"])
    r.check("manifest safety filtered to canonical",
            "vulgar" in m["safety_tags"] and "harmless" not in m["safety_tags"])
    r.check("manifest sha256 populated", len(m["sha256"]) == 64)
    r.check("manifest pack_id auto-generated", len(m["pack_id"]) > 0)

    v = pm.validate_pack_manifest(m)
    r.check("manifest validates", v["ok"], f"missing={v['missing']} invalid={v['invalid']}")

    bad = pm.validate_pack_manifest({"foo": "bar"})
    r.check("invalid manifest reports missing fields",
            not bad["ok"] and len(bad["missing"]) > 0)

    out = pm.write_pack_manifest(m, tmp / "m.json")
    r.check("write_pack_manifest creates file", Path(out).exists())
    rt = pm.read_pack_manifest(out)
    r.check("read_pack_manifest roundtrip",
            rt.get("pack_id") == m["pack_id"])

    try:
        pm.create_pack_manifest("x", language="zz",
                                coverage_categories=[], register_tags=[],
                                safety_tags=[], domain_tags=[])
        bad_lang = False
    except ValueError:
        bad_lang = True
    r.check("manifest rejects unknown language", bad_lang)


def test_english_ingestion(r: Results, db: str, tmp: Path) -> None:
    print("\n[7] english_knowledge_ingestion")
    path = tmp / "en_words.jsonl"
    rows = [
        {"word": "trowel", "definition_en": "small tool for spreading mortar",
         "register_tags": ["construction", "technical"],
         "coverage_categories": ["trades_construction"],
         "safety_tags": [], "tags": ["mason"]},
        {"word": "OSHA", "definition_en": "safety regulator (gov agency)",
         "register_tags": ["legal", "professional"],
         "coverage_categories": ["law_government", "trades_construction"]},
        {"word": "lit", "definition_en": "informal for 'cool' or 'exciting'",
         "register_tags": ["slang"],
         "coverage_categories": ["slang_street_talk"]},
        {"word": "fuck", "definition_en": "vulgar exclamation",
         "register_tags": ["vulgar"],
         "safety_tags": ["vulgar"],
         "coverage_categories": ["slang_street_talk"]},
        {"word": "", "definition_en": "broken row"},
        {"NOT_A_DICT": True},
    ]
    with path.open("w", encoding="utf-8") as f:
        for x in rows:
            f.write(json.dumps(x) + "\n")

    prev = en_ingest.preview_ingestion(str(path), limit=10)
    r.check("preview returns counts",
            prev["previewed"] == 6 and prev["ok"] >= 4 and prev["rejected"] >= 1,
            f"got {prev!r}")

    res = en_ingest.ingest_word_list(str(path), source="test_en_pack",
                                     batch_size=10, db_path=db)
    r.check("ingest added at least 4 valid rows",
            res.get("error") is None and res["added"] >= 4,
            f"got {res!r}")
    r.check("ingest report written",
            res["report_path"] and Path(res["report_path"]).exists())
    r.check("ingest manifest written",
            res["manifest_path"] and Path(res["manifest_path"]).exists())

    # Confirm safety/register persisted into DB
    fuck_row = en_store.lookup_word("fuck", db_path=db)
    r.check("vulgar row persisted with safety_tags",
            fuck_row and "vulgar" in (fuck_row.get("safety_tags") or []))
    r.check("vulgar row tagged with coverage_categories",
            fuck_row and "slang_street_talk" in (fuck_row.get("coverage_categories") or []))

    # Manifest validates
    m = pm.read_pack_manifest(res["manifest_path"])
    v = pm.validate_pack_manifest(m)
    r.check("emitted manifest validates", v["ok"],
            f"missing={v.get('missing')} invalid={v.get('invalid')}")

    # Missing file safety
    miss = en_ingest.ingest_word_list(str(tmp / "nope.jsonl"), db_path=db)
    r.check("missing file returns file_not_found",
            miss["error"] == "file_not_found")

    # Batch clamp
    r.check("batch_size clamp respected",
            en_ingest._clamp_batch(99_999_999) <= en_ingest.HARD_MAX_BATCH_SIZE)


def test_runtime_integration(r: Results, db: str) -> None:
    print("\n[8] runtime integration: vulgar is NOT auto-suggested")
    os.environ["LUNA_VOCABULARY_DB"] = db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    # Prompt mentions a known vulgar token; suggestion must NOT include it.
    out = en_runtime.get_optional_vocabulary_context(
        "talk about freedom and trowel and fuck off",
        mode="normal", limit=20, is_user_prompted=False,
    )
    suggested = {c["word"] for c in out.get("context", [])}
    r.check("vulgar NOT in normal-mode suggestions",
            "fuck" not in suggested, f"suggested={suggested!r}")
    r.check("freedom IS in suggestions",
            "freedom" in suggested)


def test_no_daemon_no_recursion(r: Results, db: str) -> None:
    print("\n[9] no background thread, no recursion blow-up")
    os.environ["LUNA_VOCABULARY_DB"] = db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    threads_before = {t.ident for t in threading.enumerate()}
    for _ in range(5):
        en_runtime.get_optional_vocabulary_context("freedom and trowel",
                                                   mode="normal", limit=5)
    gc.collect()
    time.sleep(0.05)
    new_threads = ({t.ident for t in threading.enumerate()} - threads_before)
    r.check("no new background threads spawned", len(new_threads) == 0,
            f"new={new_threads}")
    sys.setrecursionlimit(300)
    try:
        en_runtime.get_optional_vocabulary_context("freedom", mode="normal", limit=5)
        r.check("no recursion blow-up at limit=300", True)
    except RecursionError as e:
        r.check("no recursion blow-up at limit=300", False, str(e))
    finally:
        sys.setrecursionlimit(1000)


def test_no_program_s(r: Results) -> None:
    print("\n[10] No Program S / tier / probe / attestation imports")
    import re as _re
    production = {
        "coverage_taxonomy.py", "pack_manifest.py",
        "english_knowledge_ingestion.py",
        "cognitive_lexicon_store.py", "cognitive_word_policy.py",
        "cognitive_vocabulary_runtime.py",
        "russian_lexicon_store.py", "russian_personality_layer.py",
    }
    forbidden = ("program_s", "tier_intent_library", "luna_tier_",
                 "luna_modules", "probe_health", "repair_task_executor",
                 "tier_progression")
    import_re = _re.compile(
        r"^\s*(?:from|import)\s+\S*(" + "|".join(forbidden) + r")",
        _re.MULTILINE,
    )
    hits: list[str] = []
    for fn in production:
        p = HERE / fn
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        for m in import_re.finditer(text):
            hits.append(f"{fn}:{m.group(1)}")
    r.check("no forbidden imports in production modules",
            not hits, "; ".join(hits))


def main() -> int:
    r = Results()
    with tempfile.TemporaryDirectory(prefix="dual_safety_test_") as tmp_dir:
        tmp = Path(tmp_dir)
        en_db = str(tmp / "en.sqlite")
        ru_db = str(tmp / "ru.sqlite")
        try:
            test_taxonomy(r)
            test_english_migration(r, en_db)
            test_russian_migration(r, ru_db)
            test_english_policy(r)
            test_russian_policy(r)
            test_pack_manifest(r, tmp)
            test_english_ingestion(r, en_db, tmp)
            test_runtime_integration(r, en_db)
            test_no_daemon_no_recursion(r, en_db)
            test_no_program_s(r)
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
