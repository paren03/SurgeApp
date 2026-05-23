"""Phase 13 — Dual Sovereign Pack Importer + seed-pack test harness.

Run:  python test_dual_pack_importer.py
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
import dual_pack_importer as importer
import pack_manifest as pm
import russian_lexicon_store as ru_store

SEED_DIR = HERE / "seed_packs"

EN_EXPECTED = {
    "core.jsonl":     15,
    "idioms.jsonl":   10,
    "trades.jsonl":   12,
    "medical.jsonl":  10,
    "coding.jsonl":   12,
    "slang.jsonl":    10,
}
RU_EXPECTED = {
    "core.jsonl":     15,
    "idioms.jsonl":   10,
    "trades.jsonl":   10,
    "medical.jsonl":  10,
    "coding.jsonl":   10,
    "slang.jsonl":    10,
}


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


def test_seed_files_present(r: Results) -> None:
    print("\n[1] seed pack files present and well-formed")
    for sub, expected in (("en", EN_EXPECTED), ("ru", RU_EXPECTED)):
        for fn, want in expected.items():
            p = SEED_DIR / sub / fn
            r.check(f"{sub}/{fn} exists", p.exists())
            if p.exists():
                lines = [ln for ln in p.read_text(encoding="utf-8").splitlines()
                         if ln.strip()]
                r.check(f"{sub}/{fn} has {want} json lines",
                        len(lines) == want, f"got {len(lines)}")
                ok_parse = 0
                for ln in lines:
                    try:
                        json.loads(ln)
                        ok_parse += 1
                    except json.JSONDecodeError:
                        pass
                r.check(f"{sub}/{fn} all lines parse as JSON",
                        ok_parse == len(lines))


def test_preview(r: Results) -> None:
    print("\n[2] preview_seed_directory (no DB writes)")
    out = importer.preview_seed_directory(SEED_DIR, per_pack_limit=5)
    r.check("preview returns previews list",
            isinstance(out.get("previews"), list))
    r.check("preview covers at least 12 packs",
            len(out["previews"]) >= 12, f"got {len(out['previews'])}")
    # Only audit the Phase-13 packs we own
    p13_files = EN_EXPECTED.keys() | RU_EXPECTED.keys()
    p13_previews = [p for p in out["previews"]
                    if Path(p["path"]).name in p13_files]
    r.check("12 Phase-13 previews present", len(p13_previews) == 12)
    for prev in p13_previews:
        r.check(f"preview {Path(prev['path']).name} previewed > 0",
                prev.get("previewed", 0) > 0,
                f"got {prev.get('previewed')}")


def test_import_full(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[3] import_seed_directory end-to-end (Phase-13 packs only filtered)")
    out = importer.import_seed_directory(SEED_DIR, en_db=en_db, ru_db=ru_db)
    totals = out["totals"]

    # Filter to just the Phase-13 packs this harness owns. Later phases may
    # add more packs to the same seed_packs/ tree; that's fine.
    p13_packs = [p for p in out["packs"]
                 if Path(p["path"]).name in (EN_EXPECTED.keys() | RU_EXPECTED.keys())]
    p13_en = [p for p in p13_packs if p["lang"] == "en"]
    p13_ru = [p for p in p13_packs if p["lang"] == "ru"]

    r.check("12 Phase-13 packs present in import", len(p13_packs) == 12,
            f"got {len(p13_packs)}")
    r.check("6 Phase-13 English packs", len(p13_en) == 6)
    r.check("6 Phase-13 Russian packs", len(p13_ru) == 6)
    r.check("no per-pack errors (whole import)", not totals["errors"],
            f"errors={totals['errors']}")
    p13_manifests = sum(1 for p in p13_packs if p.get("manifest_path"))
    r.check("manifests written for every Phase-13 pack",
            p13_manifests == 12, f"got {p13_manifests}")

    # Per-pack added counts match expected (Phase-13 only)
    for pack in p13_packs:
        name = Path(pack["path"]).name
        if pack["lang"] == "en":
            want = EN_EXPECTED[name]
        else:
            want = RU_EXPECTED[name]
        r.check(f"{pack['lang']}/{name} added=={want}",
                pack["added"] == want,
                f"got {pack['added']} (rejected={pack['rejected']})")

    # Manifest files exist + validate (Phase-13 only)
    for pack in p13_packs:
        mpath = pack.get("manifest_path")
        r.check(f"{pack['lang']}/{Path(pack['path']).name} manifest file exists",
                mpath and Path(mpath).exists())
        if mpath and Path(mpath).exists():
            m = pm.read_pack_manifest(mpath)
            v = pm.validate_pack_manifest(m)
            r.check(f"{pack['lang']}/{Path(pack['path']).name} manifest validates",
                    v["ok"], f"missing={v['missing']} invalid={v['invalid']}")


def test_safety_tags_persisted(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[4] safety tags persisted into both DBs")
    en_morph = en_store.lookup_word("morphine", db_path=en_db)
    r.check("en morphine present after import", en_morph is not None)
    if en_morph:
        r.check("en morphine has safety_tags=recognition_only",
                "recognition_only" in (en_morph.get("safety_tags") or []),
                f"got safety_tags={en_morph.get('safety_tags')!r}")
        r.check("en morphine has coverage_categories",
                "medicine_health" in (en_morph.get("coverage_categories") or []))
        r.check("en morphine pack_source set",
                en_morph.get("pack_source") == "seed_en_medical")

    ru_morph = ru_store.lookup_word("морфин", db_path=ru_db)
    r.check("ru морфин present after import", ru_morph is not None)
    if ru_morph:
        r.check("ru морфин has safety_tags=recognition_only",
                "recognition_only" in (ru_morph.get("safety_tags") or []),
                f"got safety_tags={ru_morph.get('safety_tags')!r}")


def test_policy_blocks_recognition_only(r: Results, en_db: str) -> None:
    print("\n[5] runtime policy blocks recognition_only in suggestions")
    os.environ["LUNA_VOCABULARY_DB"] = en_db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    out = en_runtime.get_optional_vocabulary_context(
        "tell me about morphine and anesthesia and triage",
        mode="teacher", limit=20, is_user_prompted=False,
    )
    suggested = {c["word"] for c in out.get("context", [])}
    r.check("morphine NOT suggested in teacher mode (recognition_only)",
            "morphine" not in suggested,
            f"suggested={suggested!r}")
    r.check("anesthesia IS suggested (safe medical term)",
            "anesthesia" in suggested or "triage" in suggested,
            f"suggested={suggested!r}")


def test_slang_gating(r: Results, en_db: str) -> None:
    print("\n[6] slang gating in normal vs prompted contexts")
    os.environ["LUNA_VOCABULARY_DB"] = en_db
    os.environ["LUNA_VOCABULARY_RUNTIME"] = "1"
    unprompted = en_runtime.get_optional_vocabulary_context(
        "let's chat about vibe and lit and ghost",
        mode="normal", limit=20, is_user_prompted=False,
    )
    suggested_un = {c["word"] for c in unprompted.get("context", [])}
    r.check("slang NOT suggested when not user-prompted in normal mode",
            not (suggested_un & {"vibe", "lit", "ghost"}),
            f"suggested={suggested_un!r}")

    prompted = en_runtime.get_optional_vocabulary_context(
        "let's chat about vibe and lit and ghost",
        mode="normal", limit=20, is_user_prompted=True,
    )
    suggested_pr = {c["word"] for c in prompted.get("context", [])}
    r.check("slang ALLOWED when user-prompted in normal mode",
            bool(suggested_pr & {"vibe", "lit", "ghost"}),
            f"suggested={suggested_pr!r}")


def test_idempotent_reimport(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[7] re-running importer is idempotent (no duplicate rows)")
    en_before = en_store.count_words(db_path=en_db)
    ru_before_w = ru_store.count_words(db_path=ru_db)
    ru_before_p = ru_store.count_phrases(db_path=ru_db)
    importer.import_seed_directory(SEED_DIR, en_db=en_db, ru_db=ru_db)
    en_after = en_store.count_words(db_path=en_db)
    ru_after_w = ru_store.count_words(db_path=ru_db)
    ru_after_p = ru_store.count_phrases(db_path=ru_db)
    r.check("english word count unchanged after re-import",
            en_after == en_before, f"{en_before} -> {en_after}")
    r.check("russian word count unchanged after re-import",
            ru_after_w == ru_before_w, f"{ru_before_w} -> {ru_after_w}")
    r.check("russian phrase count unchanged after re-import",
            ru_after_p == ru_before_p, f"{ru_before_p} -> {ru_after_p}")


def test_per_pack_taxonomy_in_manifest(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[8] emitted manifests carry the pack's taxonomy")
    en_medical_m_path = SEED_DIR / "en" / "medical.jsonl.en_pack_manifest.json"
    ru_idioms_m_path = SEED_DIR / "ru" / "idioms.jsonl.ru_pack_manifest.json"
    r.check("en medical manifest exists", en_medical_m_path.exists())
    r.check("ru idioms manifest exists", ru_idioms_m_path.exists())
    if en_medical_m_path.exists():
        m = pm.read_pack_manifest(en_medical_m_path)
        r.check("en medical manifest lists medicine_health",
                "medicine_health" in (m.get("coverage_categories") or []))
        r.check("en medical manifest lists recognition_only safety tag",
                "recognition_only" in (m.get("safety_tags") or []))
        r.check("en medical manifest language=en", m.get("language") == "en")
    if ru_idioms_m_path.exists():
        m = pm.read_pack_manifest(ru_idioms_m_path)
        r.check("ru idioms manifest lists idioms_phrases",
                "idioms_phrases" in (m.get("coverage_categories") or []))
        r.check("ru idioms manifest language=ru", m.get("language") == "ru")


def test_no_daemon_no_recursion(r: Results, en_db: str, ru_db: str) -> None:
    print("\n[9] no background threads, no recursion blow-up")
    threads_before = {t.ident for t in threading.enumerate()}
    importer.preview_seed_directory(SEED_DIR, per_pack_limit=3)
    gc.collect()
    time.sleep(0.05)
    new_threads = {t.ident for t in threading.enumerate()} - threads_before
    r.check("no new background threads",
            len(new_threads) == 0, f"new={new_threads}")

    sys.setrecursionlimit(400)
    try:
        importer.preview_seed_directory(SEED_DIR, per_pack_limit=2)
        r.check("no recursion blow-up at limit=400", True)
    except RecursionError as e:
        r.check("no recursion blow-up at limit=400", False, str(e))
    finally:
        sys.setrecursionlimit(1000)


def test_no_full_corpus_in_memory(r: Results, tmp: Path) -> None:
    print("\n[10] importer streams files (no full-file load)")
    big = tmp / "big_en_pack.jsonl"
    with big.open("w", encoding="utf-8") as f:
        for i in range(2000):
            f.write(json.dumps({
                "word": f"streamtest_{i:04d}",
                "definition": "synthetic",
                "register_tags": ["standard"],
                "coverage_categories": ["core_vocabulary"],
            }) + "\n")
    # preview should never load whole file (capped to PREVIEW_HARD_MAX=100)
    import english_knowledge_ingestion as en_ingest
    prev = en_ingest.preview_ingestion(str(big), limit=10000)
    r.check("preview capped to PREVIEW_HARD_MAX",
            prev["previewed"] <= en_ingest.PREVIEW_HARD_MAX,
            f"got {prev['previewed']}")


def test_no_program_s(r: Results) -> None:
    print("\n[11] no forbidden imports in production modules")
    import re as _re
    production = {
        "dual_pack_importer.py",
        "english_knowledge_ingestion.py",
        "russian_knowledge_ingestion.py",
        "pack_manifest.py",
        "coverage_taxonomy.py",
    }
    forbidden = ("program_s", "tier_intent_library", "luna_tier_",
                 "luna_modules", "probe_health",
                 "repair_task_executor", "tier_progression")
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
    r.check("no forbidden cross-program imports",
            not hits, "; ".join(hits))


def main() -> int:
    r = Results()
    with tempfile.TemporaryDirectory(prefix="dual_pack_test_") as tmp_dir:
        tmp = Path(tmp_dir)
        en_db = str(tmp / "en.sqlite")
        ru_db = str(tmp / "ru.sqlite")
        try:
            test_seed_files_present(r)
            test_preview(r)
            test_import_full(r, en_db, ru_db)
            test_safety_tags_persisted(r, en_db, ru_db)
            test_policy_blocks_recognition_only(r, en_db)
            test_slang_gating(r, en_db)
            test_idempotent_reimport(r, en_db, ru_db)
            test_per_pack_taxonomy_in_manifest(r, en_db, ru_db)
            test_no_daemon_no_recursion(r, en_db, ru_db)
            test_no_full_corpus_in_memory(r, tmp)
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
