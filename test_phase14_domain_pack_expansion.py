"""Phase 14 — Domain Pack Expansion test harness.

Run:  python test_phase14_domain_pack_expansion.py
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
import coverage_taxonomy as tax
import dual_pack_importer as importer
import pack_manifest as pm
import russian_lexicon_store as ru_store
import russian_personality_layer as ru_personality

SEED_DIR = HERE / "seed_packs"

# All 14 Phase-14 categories (Phase 13 packs intentionally excluded here).
PHASE14_FILES = (
    "professions_jobs.jsonl",
    "business_finance.jsonl",
    "law_government.jsonl",
    "science_math.jsonl",
    "poetry_literary.jsonl",
    "philosophy_abstract.jsonl",
    "art_music_culture.jsonl",
    "history_geography.jsonl",
    "psychology_education.jsonl",
    "mechanics_transportation.jsonl",
    "food_home_daily_life.jsonl",
    "regional_dialect.jsonl",
    "formal_informal_speech.jsonl",
    "voice_personality.jsonl",
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


def test_files_exist_and_parse(r: Results) -> None:
    print("\n[1] all 14 Phase-14 packs exist for both languages and parse as JSONL")
    for lang in ("en", "ru"):
        for fn in PHASE14_FILES:
            p = SEED_DIR / lang / fn
            r.check(f"{lang}/{fn} exists", p.exists())
            if p.exists():
                lines = [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
                ok = 0
                for ln in lines:
                    try:
                        obj = json.loads(ln)
                        if isinstance(obj, dict):
                            ok += 1
                    except json.JSONDecodeError:
                        pass
                r.check(f"{lang}/{fn} all lines parse as JSON dicts",
                        ok == len(lines) and len(lines) > 0,
                        f"ok={ok} total={len(lines)}")


def test_taxonomy_validation(r: Results) -> None:
    print("\n[2] each pack's declared categories validate against coverage_taxonomy")
    bad: list[str] = []
    for lang in ("en", "ru"):
        for fn in PHASE14_FILES:
            p = SEED_DIR / lang / fn
            if not p.exists():
                continue
            with p.open("r", encoding="utf-8") as f:
                for i, ln in enumerate(f):
                    ln = ln.strip()
                    if not ln:
                        continue
                    try:
                        obj = json.loads(ln)
                    except json.JSONDecodeError:
                        continue
                    cov = tax.validate_coverage_categories(obj.get("coverage_categories", []))
                    if cov["rejected"]:
                        bad.append(f"{lang}/{fn}#{i}: rejected_cov={cov['rejected']}")
                    reg = tax.validate_register_tags(obj.get("register_tags", []))
                    if reg["rejected"]:
                        bad.append(f"{lang}/{fn}#{i}: rejected_reg={reg['rejected']}")
                    saf = tax.validate_safety_tags(obj.get("safety_tags", []))
                    if saf["rejected"]:
                        bad.append(f"{lang}/{fn}#{i}: rejected_saf={saf['rejected']}")
    r.check("zero taxonomy violations across all Phase-14 entries",
            not bad, "; ".join(bad[:5]))


def test_import_phase14_only(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[3] import full seed_packs/ end-to-end (Phase 13 + 14)")
    en_store.init_db(en_db)
    ru_store.init_db(ru_db)

    out = importer.import_seed_directory(SEED_DIR, en_db=en_db, ru_db=ru_db)
    all_packs = out["packs"]
    r.check("imported >= 28 packs total", len(all_packs) >= 28,
            f"got {len(all_packs)}")

    phase14 = [x for x in all_packs if Path(x["path"]).name in PHASE14_FILES]
    r.check("Phase-14 packs present in import (14 × 2 langs)",
            len(phase14) == 28, f"got {len(phase14)}")

    errs = [x for x in phase14 if x.get("error")]
    r.check("no per-pack errors in Phase-14", not errs,
            f"errs={[e['path']+'::'+str(e['error']) for e in errs[:3]]}")
    no_manifest = [x for x in phase14 if not x.get("manifest_path")]
    r.check("every Phase-14 pack emitted a manifest", not no_manifest,
            f"missing={[Path(x['path']).name for x in no_manifest[:3]]}")

    total_added = sum(int(x.get("added", 0)) for x in phase14)
    r.check("Phase-14 total added > 600 entries", total_added > 600,
            f"got {total_added}")

    bad_manifests: list[str] = []
    for x in phase14:
        mpath = x.get("manifest_path")
        if not mpath or not Path(mpath).exists():
            bad_manifests.append(f"{Path(x['path']).name}: no_file")
            continue
        m = pm.read_pack_manifest(mpath)
        v = pm.validate_pack_manifest(m)
        if not v["ok"]:
            bad_manifests.append(f"{Path(x['path']).name}: {v['missing']} {v['invalid']}")
        if len(m.get("sha256", "")) != 64:
            bad_manifests.append(f"{Path(x['path']).name}: bad_sha")
    r.check("every Phase-14 manifest validates and has sha256",
            not bad_manifests, "; ".join(bad_manifests[:3]))


def test_safety_tags_preserved(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[4] safety/register/coverage tags persisted into DBs")
    # English: a row from law_government with 'legal'
    statute = en_store.lookup_word("statute", db_path=en_db)
    r.check("en 'statute' present", statute is not None)
    if statute:
        r.check("en 'statute' has register=legal",
                "legal" in (statute.get("register_tags") or []),
                f"got {statute.get('register_tags')}")
        r.check("en 'statute' has coverage=law_government",
                "law_government" in (statute.get("coverage_categories") or []))
        r.check("en 'statute' pack_source set",
                statute.get("pack_source") == "seed_en_law_government")

    # English slang/regional row
    yall = en_store.lookup_word("y_all", db_path=en_db)
    r.check("en 'y_all' present", yall is not None)
    if yall:
        r.check("en 'y_all' has register=regional+informal",
                {"regional", "informal"}.issubset(set(yall.get("register_tags") or [])))

    # Russian: business term
    aktiv = ru_store.lookup_word("актив", db_path=ru_db)
    r.check("ru 'актив' present", aktiv is not None)
    if aktiv:
        r.check("ru 'актив' has register=business",
                "business" in (aktiv.get("register_tags") or []))
        r.check("ru 'актив' has coverage=business_finance",
                "business_finance" in (aktiv.get("coverage_categories") or []))
        r.check("ru 'актив' pack_source set",
                aktiv.get("pack_source") == "seed_ru_business_finance")


def test_recognition_only_blocked(r: Results, en_db: str) -> None:
    print("\n[5] recognition_only entries from earlier packs still blocked")
    os.environ["LUNA_VOCABULARY_DB"] = en_db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    # We won't add a Phase-14 recognition_only entry — but the pipeline still
    # honors them. Seed one synthetic recognition_only row and verify.
    en_store.add_word(
        "phase14_secret_demo", "synthetic recognition_only test",
        word_level="plain",
        safety_tags=["recognition_only"],
        register_tags=["recognition_only"],
        coverage_categories=["recognition_only_sensitive"],
        pack_source="phase14_test", pack_id="phase14_test_pid",
        db_path=en_db,
    )
    out = en_runtime.get_optional_vocabulary_context(
        "discuss phase14_secret_demo and statute and freedom",
        mode="teacher", limit=20, is_user_prompted=False,
    )
    suggested = {c["word"] for c in out.get("context", [])}
    r.check("recognition_only NOT suggested",
            "phase14_secret_demo" not in suggested,
            f"suggested={suggested}")


def test_do_not_use_unprompted(r: Results, en_db: str) -> None:
    print("\n[6] do_not_use_unprompted enforcement")
    en_store.add_word(
        "phase14_edgy_demo", "synthetic do_not_use_unprompted demo",
        word_level="plain",
        safety_tags=["do_not_use_unprompted"],
        coverage_categories=["core_vocabulary"],
        pack_source="phase14_test", pack_id="phase14_test_pid",
        db_path=en_db,
    )
    os.environ["LUNA_VOCABULARY_DB"] = en_db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    unprompted = en_runtime.get_optional_vocabulary_context(
        "say something about phase14_edgy_demo",
        mode="normal", limit=10, is_user_prompted=False,
    )
    prompted = en_runtime.get_optional_vocabulary_context(
        "say something about phase14_edgy_demo",
        mode="normal", limit=10, is_user_prompted=True,
    )
    r.check("do_not_use_unprompted NOT surfaced when unprompted",
            "phase14_edgy_demo" not in {c["word"] for c in unprompted.get("context", [])})
    r.check("do_not_use_unprompted IS surfaced when prompted",
            "phase14_edgy_demo" in {c["word"] for c in prompted.get("context", [])})


def test_slang_regional_gating(r: Results, en_db: str) -> None:
    print("\n[7] slang/regional gating for Phase-14 regional pack")
    os.environ["LUNA_VOCABULARY_DB"] = en_db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    # 'mate' is tagged regional+informal (British/Australian English)
    un = en_runtime.get_optional_vocabulary_context(
        "hello mate, how are you today",
        mode="teacher", limit=10, is_user_prompted=False,
    )
    pr = en_runtime.get_optional_vocabulary_context(
        "hello mate, how are you today",
        mode="teacher", limit=10, is_user_prompted=True,
    )
    r.check("regional/informal blocked from teacher mode w/o prompt",
            "mate" not in {c["word"] for c in un.get("context", [])})
    r.check("regional/informal surfaced when user-prompted",
            "mate" in {c["word"] for c in pr.get("context", [])},
            f"prompted={[c['word'] for c in pr.get('context', [])]}")


def test_russian_policy_persisted(r: Results, ru_db: str) -> None:
    print("\n[8] Russian policy still gates Phase-14 entries")
    # Synthetic recognition_only Russian row
    ru_store.add_word(
        "тестовое_секретное", lemma="тестовое_секретное",
        part_of_speech="noun",
        definition_ru="синтетический тест recognition_only",
        safety_tags=["recognition_only"],
        coverage_categories=["recognition_only_sensitive"],
        pack_source="phase14_test", pack_id="phase14_test_pid",
        db_path=ru_db,
    )
    row = ru_store.lookup_word("тестовое_секретное", db_path=ru_db)
    r.check("ru recognition_only row stored",
            row and "recognition_only" in (row.get("safety_tags") or []))
    d = ru_personality.is_entry_allowed_ru(
        "тестовое_секретное", mode="conversation",
        safety_tags=row.get("safety_tags") if row else [],
        register_tags=row.get("register_tags") if row else [],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("ru policy blocks recognition_only suggestion",
            not d["allowed"], f"reason={d.get('reason')}")


def test_idempotent_reimport(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[9] re-running orchestrator is idempotent (no row growth)")
    en_before = en_store.count_words(db_path=en_db)
    ru_before_w = ru_store.count_words(db_path=ru_db)
    ru_before_p = ru_store.count_phrases(db_path=ru_db)
    importer.import_seed_directory(SEED_DIR, en_db=en_db, ru_db=ru_db)
    en_after = en_store.count_words(db_path=en_db)
    ru_after_w = ru_store.count_words(db_path=ru_db)
    ru_after_p = ru_store.count_phrases(db_path=ru_db)
    r.check("en row count unchanged after re-import",
            en_after == en_before, f"{en_before} -> {en_after}")
    r.check("ru words unchanged after re-import",
            ru_after_w == ru_before_w, f"{ru_before_w} -> {ru_after_w}")
    r.check("ru phrases unchanged after re-import",
            ru_after_p == ru_before_p, f"{ru_before_p} -> {ru_after_p}")


def test_no_daemon_no_recursion(r: Results) -> None:
    print("\n[10] no background threads spawned; no recursion blow-up")
    threads_before = {t.ident for t in threading.enumerate()}
    importer.preview_seed_directory(SEED_DIR, per_pack_limit=3)
    gc.collect()
    time.sleep(0.05)
    new_threads = {t.ident for t in threading.enumerate()} - threads_before
    r.check("no new background threads", len(new_threads) == 0,
            f"new={new_threads}")
    sys.setrecursionlimit(400)
    try:
        importer.preview_seed_directory(SEED_DIR, per_pack_limit=2)
        r.check("no recursion blow-up at limit=400", True)
    except RecursionError as e:
        r.check("no recursion blow-up at limit=400", False, str(e))
    finally:
        sys.setrecursionlimit(1000)


def test_no_full_corpus_load(r: Results, tmp: Path) -> None:
    print("\n[11] preview hard-caps even on very large input")
    big = tmp / "ten_thousand.jsonl"
    with big.open("w", encoding="utf-8") as f:
        for i in range(10_000):
            f.write(json.dumps({"word": f"foo_{i:05d}",
                                "register_tags": ["standard"],
                                "coverage_categories": ["core_vocabulary"]}) + "\n")
    import english_knowledge_ingestion as en_ingest
    out = en_ingest.preview_ingestion(str(big), limit=99_999)
    r.check("preview capped to PREVIEW_HARD_MAX on a 10K-row file",
            out["previewed"] <= en_ingest.PREVIEW_HARD_MAX,
            f"got {out['previewed']}")


def test_no_program_s(r: Results) -> None:
    print("\n[12] no forbidden imports in production / orchestrator")
    import re as _re
    production = {
        "coverage_taxonomy.py", "pack_manifest.py",
        "english_knowledge_ingestion.py", "russian_knowledge_ingestion.py",
        "dual_pack_importer.py",
        "cognitive_lexicon_store.py", "cognitive_word_policy.py",
        "cognitive_vocabulary_runtime.py",
        "russian_lexicon_store.py", "russian_personality_layer.py",
        "russian_language_router.py", "russian_morphology_layer.py",
        "russian_memory_fabric.py", "russian_response_quality.py",
    }
    forbidden = ("program_s", "tier_intent_library", "luna_tier_",
                 "luna_modules", "probe_health",
                 "repair_task_executor", "tier_progression",
                 "worker\\.py", "attestation_")
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
    r.check("no forbidden cross-program imports", not hits,
            "; ".join(hits))


def main() -> int:
    r = Results()
    with tempfile.TemporaryDirectory(prefix="phase14_test_") as tmp_dir:
        tmp = Path(tmp_dir)
        en_db = str(tmp / "en.sqlite")
        ru_db = str(tmp / "ru.sqlite")
        try:
            test_files_exist_and_parse(r)
            test_taxonomy_validation(r)
            test_import_phase14_only(r, en_db, ru_db)
            test_safety_tags_preserved(r, en_db, ru_db)
            test_recognition_only_blocked(r, en_db)
            test_do_not_use_unprompted(r, en_db)
            test_slang_regional_gating(r, en_db)
            test_russian_policy_persisted(r, ru_db)
            test_idempotent_reimport(r, en_db, ru_db)
            test_no_daemon_no_recursion(r)
            test_no_full_corpus_load(r, tmp)
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
