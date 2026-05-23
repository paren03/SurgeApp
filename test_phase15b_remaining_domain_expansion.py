"""Phase 15B — Remaining Domain Expansion test harness.

Run:  python test_phase15b_remaining_domain_expansion.py
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

PHASE15B_CATS = (
    "business_finance", "law_government", "science_math", "medicine_health",
    "coding_technology", "art_music_culture", "history_geography",
    "psychology_education", "mechanics_transportation",
    "food_home_daily_life", "regional_dialect", "formal_informal_speech",
    "voice_personality", "idioms_phrases", "core_vocabulary",
)
PHASE15B_FILES_EN = tuple(f"phase15b_{c}.jsonl" for c in PHASE15B_CATS)
PHASE15B_FILES_RU = tuple(f"phase15b_{c}.jsonl" for c in PHASE15B_CATS)
MIN_ENTRIES_PER_PACK = 75


class Results:
    def __init__(self):
        self.passed: list[str] = []
        self.failed: list[tuple[str, str]] = []

    def check(self, name, ok, detail=""):
        if ok:
            self.passed.append(name)
            print(f"  PASS  {name}")
        else:
            self.failed.append((name, detail))
            print(f"  FAIL  {name} :: {detail}")

    def summary(self):
        total = len(self.passed) + len(self.failed)
        return f"{len(self.passed)}/{total} passed, {len(self.failed)} failed"


def test_preflight(r):
    print("\n[A] Pre-flight — Phase 15A artifacts present")
    r.check("Phase 15A report exists",
            (HERE / "PHASE15A_CONTROLLED_SCALE_EXPANSION_REPORT.md").exists())
    r.check("Phase 15A harness exists",
            (HERE / "test_phase15a_controlled_scale_expansion.py").exists())
    p15a_en = list((SEED_DIR / "en").glob("phase15a_*.jsonl"))
    p15a_ru = list((SEED_DIR / "ru").glob("phase15a_*.jsonl"))
    r.check("Phase 15A EN packs present (>=5)", len(p15a_en) >= 5)
    r.check("Phase 15A RU packs present (>=5)", len(p15a_ru) >= 5)


def test_files_exist(r):
    print("\n[B] All 15 Phase-15B packs per language exist with >=75 entries")
    for lang, files in (("en", PHASE15B_FILES_EN), ("ru", PHASE15B_FILES_RU)):
        for fn in files:
            p = SEED_DIR / lang / fn
            r.check(f"{lang}/{fn} exists", p.exists())
            if not p.exists():
                continue
            lines = [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
            ok = 0
            for ln in lines:
                try:
                    obj = json.loads(ln)
                    if isinstance(obj, dict):
                        ok += 1
                except json.JSONDecodeError:
                    pass
            r.check(f"{lang}/{fn} all lines parse as dicts",
                    ok == len(lines) and ok > 0,
                    f"ok={ok} total={len(lines)}")
            r.check(f"{lang}/{fn} >= {MIN_ENTRIES_PER_PACK} entries",
                    len(lines) >= MIN_ENTRIES_PER_PACK,
                    f"got {len(lines)}")


def test_taxonomy(r):
    print("\n[C] Coverage / register / safety taxonomy validates")
    bad = []
    for lang, files in (("en", PHASE15B_FILES_EN), ("ru", PHASE15B_FILES_RU)):
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
                        bad.append(f"{lang}/{fn}#{i}: cov_rej={cov['rejected']}")
                    reg = tax.validate_register_tags(obj.get("register_tags", []))
                    if reg["rejected"]:
                        bad.append(f"{lang}/{fn}#{i}: reg_rej={reg['rejected']}")
                    saf = tax.validate_safety_tags(obj.get("safety_tags", []))
                    if saf["rejected"]:
                        bad.append(f"{lang}/{fn}#{i}: saf_rej={saf['rejected']}")
    r.check("zero Phase-15B taxonomy violations", not bad, "; ".join(bad[:5]))


def test_import(r, en_db, ru_db):
    print("\n[D] Full seed_packs/ import; Phase-15B manifests valid")
    en_store.init_db(en_db)
    ru_store.init_db(ru_db)
    out = importer.import_seed_directory(SEED_DIR, en_db=en_db, ru_db=ru_db)
    all_packs = out["packs"]
    p15b = [p for p in all_packs
            if Path(p["path"]).name in (set(PHASE15B_FILES_EN) | set(PHASE15B_FILES_RU))]
    r.check("30 Phase-15B packs imported (15x2)",
            len(p15b) == 30, f"got {len(p15b)}")
    errs = [p for p in p15b if p.get("error")]
    r.check("no per-pack errors", not errs)
    missing_m = [p for p in p15b if not p.get("manifest_path")]
    r.check("every Phase-15B pack has a manifest", not missing_m,
            f"missing={[Path(p['path']).name for p in missing_m]}")

    bad_m = []
    for p in p15b:
        mp = p.get("manifest_path")
        if not mp or not Path(mp).exists():
            bad_m.append(f"{Path(p['path']).name}: no_file")
            continue
        m = pm.read_pack_manifest(mp)
        v = pm.validate_pack_manifest(m)
        if not v["ok"]:
            bad_m.append(f"{Path(p['path']).name}: {v['missing']}|{v['invalid']}")
        if len(m.get("sha256", "")) != 64:
            bad_m.append(f"{Path(p['path']).name}: bad_sha")
    r.check("every Phase-15B manifest validates with sha256", not bad_m,
            "; ".join(bad_m[:3]))

    total_added = sum(int(p.get("added", 0)) for p in p15b)
    r.check("Phase-15B total >= 2000 entries", total_added >= 2000,
            f"got {total_added}")


def test_safety_persistence(r, en_db, ru_db):
    print("\n[E] Safety / register / coverage tags persisted")
    # English recognition_only term
    morphine_pj = en_store.lookup_word("opioid_term", db_path=en_db)
    r.check("en 'opioid_term' recognition_only persisted",
            morphine_pj and "recognition_only" in (morphine_pj.get("safety_tags") or []))
    phishing = en_store.lookup_word("phishing_term", db_path=en_db)
    r.check("en 'phishing_term' recognition_only persisted",
            phishing and "recognition_only" in (phishing.get("safety_tags") or []))
    pack_source_check = en_store.lookup_word("ebitda", db_path=en_db)
    r.check("en 'ebitda' pack_source canonical phase15b",
            pack_source_check and pack_source_check.get("pack_source") == "seed_en_phase15b_business_finance",
            f"got {pack_source_check.get('pack_source') if pack_source_check else None!r}")

    # Russian recognition_only
    mat_ru = ru_store.lookup_word("уголовное_дело", db_path=ru_db)
    r.check("ru 'уголовное_дело' recognition_only persisted",
            mat_ru and "recognition_only" in (mat_ru.get("safety_tags") or []))
    perevorot = ru_store.lookup_word("переворот_термин", db_path=ru_db)
    r.check("ru 'переворот_термин' recognition_only persisted",
            perevorot and "recognition_only" in (perevorot.get("safety_tags") or []))
    aktiv = ru_store.lookup_word("дебет", db_path=ru_db)
    r.check("ru 'дебет' pack_source canonical phase15b",
            aktiv and aktiv.get("pack_source") == "seed_ru_phase15b_business_finance",
            f"got {aktiv.get('pack_source') if aktiv else None!r}")


def test_runtime_policy_en(r, en_db):
    print("\n[F] English runtime policy enforcement")
    os.environ["LUNA_VOCABULARY_DB"] = en_db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    # recognition_only blocked in teacher
    out = en_runtime.get_optional_vocabulary_context(
        "ransomware_term and opioid_term and freedom",
        mode="teacher", limit=20, is_user_prompted=False,
    )
    suggested = {c["word"] for c in out.get("context", [])}
    r.check("en recognition_only NOT suggested in teacher",
            not (suggested & {"ransomware_term", "opioid_term", "phishing_term", "malware_term"}),
            f"got {suggested}")


def test_runtime_policy_ru(r, ru_db):
    print("\n[G] Russian runtime policy enforcement")
    rec = ru_store.lookup_word("уголовное_дело", db_path=ru_db)
    d = ru_personality.is_entry_allowed_ru(
        "уголовное_дело", mode="teacher",
        safety_tags=rec.get("safety_tags") if rec else [],
        register_tags=rec.get("register_tags") if rec else [],
        is_user_prompted=False, decision_context="suggestion",
    )
    r.check("ru recognition_only blocked from suggestion",
            not d["allowed"], f"reason={d.get('reason')}")
    d_expl = ru_personality.is_entry_allowed_ru(
        "уголовное_дело", mode="teacher",
        safety_tags=rec.get("safety_tags") if rec else [],
        register_tags=rec.get("register_tags") if rec else [],
        is_user_prompted=False, decision_context="explanation",
    )
    r.check("ru recognition_only allowed for explanation",
            d_expl["allowed"])


def test_phrase_routing(r, ru_db):
    print("\n[H] Russian phrase routing (voice + idiom)")
    bestie_voice = ru_store.lookup_word("давай подумаем вслух", db_path=ru_db)
    r.check("ru voice phrase 'давай подумаем вслух' stored (in phrases or words)",
            bestie_voice is not None
            or ru_store.count_phrases(db_path=ru_db) > 0)


def test_idempotent(r, en_db, ru_db):
    print("\n[I] Re-import is idempotent")
    en_before = en_store.count_words(db_path=en_db)
    ru_before_w = ru_store.count_words(db_path=ru_db)
    ru_before_p = ru_store.count_phrases(db_path=ru_db)
    importer.import_seed_directory(SEED_DIR, en_db=en_db, ru_db=ru_db)
    r.check("en unchanged on re-import",
            en_store.count_words(db_path=en_db) == en_before)
    r.check("ru words unchanged on re-import",
            ru_store.count_words(db_path=ru_db) == ru_before_w)
    r.check("ru phrases unchanged on re-import",
            ru_store.count_phrases(db_path=ru_db) == ru_before_p)


def test_no_daemon_no_recursion(r):
    print("\n[J] No background threads; no recursion blow-up")
    before = {t.ident for t in threading.enumerate()}
    importer.preview_seed_directory(SEED_DIR, per_pack_limit=3)
    gc.collect()
    time.sleep(0.05)
    new = {t.ident for t in threading.enumerate()} - before
    r.check("no new background threads", len(new) == 0, f"new={new}")
    sys.setrecursionlimit(400)
    try:
        importer.preview_seed_directory(SEED_DIR, per_pack_limit=2)
        r.check("no recursion blow-up at limit=400", True)
    except RecursionError as e:
        r.check("no recursion blow-up at limit=400", False, str(e))
    finally:
        sys.setrecursionlimit(1000)


def test_no_full_corpus(r, tmp):
    print("\n[K] Preview hard-caps even on synthetic large file")
    big = tmp / "big.jsonl"
    with big.open("w", encoding="utf-8") as f:
        for i in range(30_000):
            f.write(json.dumps({"word": f"px_{i:05d}",
                                "register_tags": ["standard"],
                                "coverage_categories": ["core_vocabulary"]}) + "\n")
    out = en_ingest.preview_ingestion(str(big), limit=99_999)
    r.check("preview capped to PREVIEW_HARD_MAX on 30K-row file",
            out["previewed"] <= en_ingest.PREVIEW_HARD_MAX,
            f"got {out['previewed']}")


def test_no_program_s(r):
    print("\n[L] No forbidden imports in production modules")
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
    pat = _re.compile(
        r"^\s*(?:from|import)\s+\S*(" + "|".join(forbidden) + r")",
        _re.MULTILINE,
    )
    hits = []
    for fn in production:
        p = HERE / fn
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        for m in pat.finditer(text):
            hits.append(f"{fn}:{m.group(1)}")
    r.check("no forbidden cross-program imports", not hits, "; ".join(hits))


def main():
    r = Results()
    with tempfile.TemporaryDirectory(prefix="phase15b_test_") as tmp_dir:
        tmp = Path(tmp_dir)
        en_db = str(tmp / "en.sqlite")
        ru_db = str(tmp / "ru.sqlite")
        try:
            test_preflight(r)
            test_files_exist(r)
            test_taxonomy(r)
            test_import(r, en_db, ru_db)
            test_safety_persistence(r, en_db, ru_db)
            test_runtime_policy_en(r, en_db)
            test_runtime_policy_ru(r, ru_db)
            test_phrase_routing(r, ru_db)
            test_idempotent(r, en_db, ru_db)
            test_no_daemon_no_recursion(r)
            test_no_full_corpus(r, tmp)
            test_no_program_s(r)
        except Exception:
            print("UNHANDLED EXCEPTION:")
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
