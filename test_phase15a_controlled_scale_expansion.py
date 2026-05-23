"""Phase 15A — Controlled Scale Expansion test harness.

Run:  python test_phase15a_controlled_scale_expansion.py
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
import english_knowledge_ingestion as en_ingest
import pack_manifest as pm
import russian_lexicon_store as ru_store
import russian_personality_layer as ru_personality

SEED_DIR = HERE / "seed_packs"

PHASE15A_CATS = (
    "professions_jobs",
    "trades_construction",
    "poetry_literary",
    "philosophy_abstract",
    "slang_street_talk",
)
CANONICAL_PREFIX = "phase15a_"
LEGACY_SUFFIX = "_expansion.jsonl"
MIN_ENTRIES_PER_PACK = 100  # Phase 15A spec minimum

# Canonical Phase 15A naming (this scale pass).
PHASE15A_FILES_EN = tuple(f"{CANONICAL_PREFIX}{c}.jsonl" for c in PHASE15A_CATS)
PHASE15A_FILES_RU = tuple(f"{CANONICAL_PREFIX}{c}.jsonl" for c in PHASE15A_CATS)

# Legacy expansion packs from the earlier pass remain in place (additive).
LEGACY_FILES_EN = tuple(f"{c}{LEGACY_SUFFIX}" for c in PHASE15A_CATS)
LEGACY_FILES_RU = tuple(f"{c}{LEGACY_SUFFIX}" for c in PHASE15A_CATS)


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


def test_files_exist(r: Results) -> None:
    print("\n[1] All 5 categories present in both languages with >= 100 entries")
    for lang, files in (("en", PHASE15A_FILES_EN), ("ru", PHASE15A_FILES_RU)):
        for fn in files:
            p = SEED_DIR / lang / fn
            r.check(f"{lang}/{fn} exists", p.exists())
            if not p.exists():
                continue
            lines = [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
            ok_parse = 0
            for ln in lines:
                try:
                    obj = json.loads(ln)
                    if isinstance(obj, dict):
                        ok_parse += 1
                except json.JSONDecodeError:
                    pass
            r.check(f"{lang}/{fn} all lines parse",
                    ok_parse == len(lines), f"ok={ok_parse} total={len(lines)}")
            r.check(f"{lang}/{fn} has at least {MIN_ENTRIES_PER_PACK} entries",
                    len(lines) >= MIN_ENTRIES_PER_PACK, f"got {len(lines)}")


def test_taxonomy_validation(r: Results) -> None:
    print("\n[2] All Phase-15A entries validate against coverage_taxonomy")
    bad: list[str] = []
    for lang, files in (("en", PHASE15A_FILES_EN), ("ru", PHASE15A_FILES_RU)):
        for fn in files:
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
                        bad.append(f"{lang}/{fn}#{i}: bad_cov={cov['rejected']}")
                    reg = tax.validate_register_tags(obj.get("register_tags", []))
                    if reg["rejected"]:
                        bad.append(f"{lang}/{fn}#{i}: bad_reg={reg['rejected']}")
                    saf = tax.validate_safety_tags(obj.get("safety_tags", []))
                    if saf["rejected"]:
                        bad.append(f"{lang}/{fn}#{i}: bad_saf={saf['rejected']}")
    r.check("zero taxonomy rejections across Phase-15A",
            not bad, "; ".join(bad[:5]))


def test_full_import(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[3] full seed_packs/ import + Phase-15A manifests")
    en_store.init_db(en_db)
    ru_store.init_db(ru_db)
    out = importer.import_seed_directory(SEED_DIR, en_db=en_db, ru_db=ru_db)
    all_packs = out["packs"]
    phase15a = [p for p in all_packs
                if Path(p["path"]).name in (set(PHASE15A_FILES_EN) | set(PHASE15A_FILES_RU))]
    r.check("10 Phase-15A packs imported",
            len(phase15a) == 10, f"got {len(phase15a)}")
    errs = [p for p in phase15a if p.get("error")]
    r.check("no per-pack errors", not errs)
    missing_m = [p for p in phase15a if not p.get("manifest_path")]
    r.check("every Phase-15A pack has a manifest path",
            not missing_m, f"missing={[Path(p['path']).name for p in missing_m]}")

    bad_m: list[str] = []
    for p in phase15a:
        mpath = p.get("manifest_path")
        if not mpath or not Path(mpath).exists():
            bad_m.append(f"{Path(p['path']).name}: no_file")
            continue
        m = pm.read_pack_manifest(mpath)
        v = pm.validate_pack_manifest(m)
        if not v["ok"]:
            bad_m.append(f"{Path(p['path']).name}: {v['missing']}|{v['invalid']}")
        if len(m.get("sha256", "")) != 64:
            bad_m.append(f"{Path(p['path']).name}: bad_sha")
    r.check("every Phase-15A manifest validates with sha256",
            not bad_m, "; ".join(bad_m[:3]))

    total_added = sum(int(p.get("added", 0)) for p in phase15a)
    r.check("Phase-15A added >= 1000 entries total",
            total_added >= 1000, f"got {total_added}")


def test_safety_persistence(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[4] safety/register/coverage tags persisted into DBs")
    # English phase15a slang has synthetic recognition_only rows.
    rec = en_store.lookup_word("based_recognition_only_term", db_path=en_db)
    r.check("en phase15a recognition_only sentinel present", rec is not None)
    if rec:
        r.check("en recognition_only safety_tag stored",
                "recognition_only" in (rec.get("safety_tags") or []))
        r.check("en recognition_only coverage stored",
                "recognition_only_sensitive" in (rec.get("coverage_categories") or []))
        r.check("en phase15a recognition_only pack_source canonical",
                rec.get("pack_source") == "seed_en_phase15a_slang_street_talk",
                f"got {rec.get('pack_source')!r}")
    # English do_not_use_unprompted (new phase15a entry)
    thirst = en_store.lookup_word("thirst_trap", db_path=en_db)
    r.check("en 'thirst_trap' has do_not_use_unprompted safety_tag",
            thirst and "do_not_use_unprompted" in (thirst.get("safety_tags") or []))
    # English vulgar (new phase15a entry)
    shitpost = en_store.lookup_word("shitpost", db_path=en_db)
    r.check("en 'shitpost' has vulgar safety_tag",
            shitpost and "vulgar" in (shitpost.get("safety_tags") or []))
    # English phase15a pack_source set for a normal slang entry
    bestie = en_store.lookup_word("bestie", db_path=en_db)
    r.check("en 'bestie' pack_source canonical",
            bestie and bestie.get("pack_source") == "seed_en_phase15a_slang_street_talk",
            f"got {bestie.get('pack_source') if bestie else None!r}")

    # Russian recognition_only categorical placeholder (new phase15a entry)
    mat_cat = ru_store.lookup_word("оскорбления_категория_рус", db_path=ru_db)
    r.check("ru phase15a recognition_only sentinel present",
            mat_cat is not None)
    if mat_cat:
        r.check("ru recognition_only safety_tag stored",
                "recognition_only" in (mat_cat.get("safety_tags") or []))
    # Russian do_not_use_unprompted (new phase15a entry)
    hate = ru_store.lookup_word("хейтить", db_path=ru_db)
    r.check("ru 'хейтить' has do_not_use_unprompted safety_tag",
            hate and "do_not_use_unprompted" in (hate.get("safety_tags") or []))
    # Russian pack_source set for a phase15a entry
    bratan = ru_store.lookup_word("братан", db_path=ru_db)
    r.check("ru 'братан' pack_source canonical",
            bratan and bratan.get("pack_source") == "seed_ru_phase15a_slang_street_talk",
            f"got {bratan.get('pack_source') if bratan else None!r}")


def test_runtime_policy_en(r: Results, en_db: str) -> None:
    print("\n[5] English runtime policy enforcement")
    os.environ["LUNA_VOCABULARY_DB"] = en_db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    # recognition_only not surfaced
    out = en_runtime.get_optional_vocabulary_context(
        "talk about edge_term_demo and freedom and friend",
        mode="teacher", limit=20, is_user_prompted=False,
    )
    s = {c["word"] for c in out.get("context", [])}
    r.check("edge_term_demo NOT suggested in teacher mode",
            "edge_term_demo" not in s, f"suggested={s}")

    # do_not_use_unprompted gated by prompt
    un = en_runtime.get_optional_vocabulary_context(
        "discuss wasted and stoked",
        mode="normal", limit=10, is_user_prompted=False,
    )
    pr = en_runtime.get_optional_vocabulary_context(
        "discuss wasted and stoked",
        mode="normal", limit=10, is_user_prompted=True,
    )
    r.check("en do_not_use_unprompted 'wasted' blocked w/o prompt",
            "wasted" not in {c["word"] for c in un.get("context", [])})

    # vulgar blocked in teacher even when prompted
    teacher_prompted = en_runtime.get_optional_vocabulary_context(
        "talk about damn and crap",
        mode="teacher", limit=10, is_user_prompted=True,
    )
    sset = {c["word"] for c in teacher_prompted.get("context", [])}
    r.check("en vulgar 'damn' blocked in teacher mode even when prompted",
            "damn" not in sset, f"got {sset}")

    # slang allowed in voice_conversation (informal-class) when prompted
    informal_prompted = en_runtime.get_optional_vocabulary_context(
        "chat about lit and vibe and dope",
        mode="voice_conversation", limit=20, is_user_prompted=True,
    )
    informal_set = {c["word"] for c in informal_prompted.get("context", [])}
    r.check("en slang surfaces in voice_conversation when prompted",
            bool(informal_set & {"lit", "vibe", "dope"}),
            f"got {informal_set}")


def test_runtime_policy_ru(r: Results, ru_db: str) -> None:
    print("\n[6] Russian runtime policy enforcement")
    # recognition_only
    mat = ru_store.lookup_word("мат_общий", db_path=ru_db)
    d_rec = ru_personality.is_entry_allowed_ru(
        "мат_общий", mode="conversation",
        safety_tags=mat.get("safety_tags") if mat else [],
        register_tags=mat.get("register_tags") if mat else [],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("ru recognition_only 'мат_общий' blocked from suggestion",
            not d_rec["allowed"], f"reason={d_rec.get('reason')}")

    # do_not_use_unprompted
    bukhoi = ru_store.lookup_word("бухой", db_path=ru_db)
    d_dnu = ru_personality.is_entry_allowed_ru(
        "бухой", mode="conversation",
        safety_tags=bukhoi.get("safety_tags") if bukhoi else [],
        register_tags=bukhoi.get("register_tags") if bukhoi else [],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("ru 'бухой' blocked when not prompted",
            not d_dnu["allowed"])
    d_dnu_p = ru_personality.is_entry_allowed_ru(
        "бухой", mode="warm_friend",
        safety_tags=bukhoi.get("safety_tags") if bukhoi else [],
        register_tags=bukhoi.get("register_tags") if bukhoi else [],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("ru 'бухой' allowed when user-prompted in warm_friend",
            d_dnu_p["allowed"])

    # vulgar blocked in teacher even prompted
    padla = ru_store.lookup_word("падла", db_path=ru_db)
    d_vulg = ru_personality.is_entry_allowed_ru(
        "падла", mode="teacher",
        safety_tags=padla.get("safety_tags") if padla else [],
        register_tags=padla.get("register_tags") if padla else [],
        is_user_prompted=True, decision_context="suggestion",
    )
    r.check("ru vulgar 'падла' blocked in teacher mode even prompted",
            not d_vulg["allowed"])


def test_russian_phrases_route(r: Results, ru_db: str) -> None:
    print("\n[7] Russian phrase entries route correctly (idioms_expansion sanity)")
    # Phase-15A doesn't have explicit phrase files. But our slang pack has
    # multi-word entries stored as words; check at least one of them landed.
    nepar = ru_store.lookup_word("не_парься", db_path=ru_db)
    r.check("ru multi-word slang 'не_парься' stored in words table",
            nepar is not None)


def test_idempotent(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[8] re-import is idempotent")
    en_before = en_store.count_words(db_path=en_db)
    ru_before_w = ru_store.count_words(db_path=ru_db)
    ru_before_p = ru_store.count_phrases(db_path=ru_db)
    importer.import_seed_directory(SEED_DIR, en_db=en_db, ru_db=ru_db)
    r.check("en unchanged after re-import",
            en_store.count_words(db_path=en_db) == en_before)
    r.check("ru words unchanged after re-import",
            ru_store.count_words(db_path=ru_db) == ru_before_w)
    r.check("ru phrases unchanged after re-import",
            ru_store.count_phrases(db_path=ru_db) == ru_before_p)


def test_no_daemon_no_recursion(r: Results) -> None:
    print("\n[9] no background threads; no recursion blow-up")
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


def test_no_full_corpus(r: Results, tmp: Path) -> None:
    print("\n[10] preview hard-caps even on synthetic large file")
    big = tmp / "very_big_en_pack.jsonl"
    with big.open("w", encoding="utf-8") as f:
        for i in range(20_000):
            f.write(json.dumps({
                "word": f"sx_{i:05d}",
                "register_tags": ["standard"],
                "coverage_categories": ["core_vocabulary"],
            }) + "\n")
    out = en_ingest.preview_ingestion(str(big), limit=99_999)
    r.check("preview capped to PREVIEW_HARD_MAX even on 20K input",
            out["previewed"] <= en_ingest.PREVIEW_HARD_MAX,
            f"got {out['previewed']}")


def test_no_program_s(r: Results) -> None:
    print("\n[11] no forbidden imports in production modules")
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
    r.check("no forbidden cross-program imports", not hits, "; ".join(hits))


def main() -> int:
    r = Results()
    with tempfile.TemporaryDirectory(prefix="phase15a_test_") as tmp_dir:
        tmp = Path(tmp_dir)
        en_db = str(tmp / "en.sqlite")
        ru_db = str(tmp / "ru.sqlite")
        try:
            test_files_exist(r)
            test_taxonomy_validation(r)
            test_full_import(r, en_db, ru_db)
            test_safety_persistence(r, en_db, ru_db)
            test_runtime_policy_en(r, en_db)
            test_runtime_policy_ru(r, ru_db)
            test_russian_phrases_route(r, ru_db)
            test_idempotent(r, en_db, ru_db)
            test_no_daemon_no_recursion(r)
            test_no_full_corpus(r, tmp)
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
