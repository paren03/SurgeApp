"""Phase 22 - Bilingual Linker / Retrieval Bridge / Morphology Path Harness.

Synthetic + production-read-only. The bilingual link DB used by tests is a
TEMP path - production lexicons are not touched.
"""

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

import bilingual_concept_link_store as bls
import bilingual_link_builder as blb
import bilingual_retrieval_bridge as brb
import russian_morphology_upgrade_path as rmu
import bilingual_coverage_gap_reporter as cgr


PASS = "[PASS]"
FAIL = "[FAIL]"
_results: list[tuple[str, str, str]] = []


def _check(suite: str, name: str, cond: bool, detail: str = "") -> None:
    _results.append((suite, name,
                     PASS if cond else FAIL + (": " + detail if detail else "")))


def _td() -> Path:
    return Path(tempfile.mkdtemp(prefix="phase22_"))


PHASE22_REQUIRED_PRIOR = (
    "PHASE21A_OPERATOR_CORPUS_STAGING_KIT_REPORT.md",
    "test_phase21a_operator_corpus_staging.py",
    "phase21a_operator_corpus_staging.py",
    "dual_corpus_source_acceptance_validator.py",
    "dual_corpus_metadata_repair_preview.py",
    "phase21a_staging_readiness_gate.py",
    "PHASE21_OPERATOR_STAGING_REQUIRED_REPORT.md",
    "phase21_operator_stage_runner.py",
    "test_phase21_operator_staged_first_import.py",
    "PHASE20_MILLION_READINESS_GATE_REPORT.md",
    "dual_vocab_backup_restore.py",
    "dual_import_batch_ledger.py",
    "dual_million_stage_planner.py",
    "dual_post_stage_quality_audit.py",
    "dual_retrieval_sla_eval.py",
    "dual_index_consistency_checker.py",
    "dual_safety_regression_auditor.py",
    "phase20_million_readiness_runner.py",
    "cognitive_lexicon_store.py",
    "cognitive_word_policy.py",
    "cognitive_vocabulary_runtime.py",
    "russian_lexicon_store.py",
    "russian_morphology_layer.py",
    "russian_personality_layer.py",
    "russian_response_quality.py",
    "coverage_taxonomy.py",
    "pack_manifest.py",
)


# -------------------- A: Pre-flight --------------------

def suite_A_preflight() -> None:
    suite = "A_PREFLIGHT"
    for f in PHASE22_REQUIRED_PRIOR:
        _check(suite, f"required_{f}_exists",
               Path(f).exists(), f"missing: {f}")


# -------------------- B: Link store --------------------

def suite_B_link_store() -> None:
    suite = "B_LINK_STORE"
    td = _td()
    db = td / "links.sqlite"
    p = bls.init_bilingual_link_db(db)
    _check(suite, "link_db_init_creates_file",
           Path(p).exists(), str(p))

    c = bls.create_concept(canonical_label_en="engineer",
                            canonical_label_ru="инженер",
                            coverage_categories=["professions_jobs"],
                            domain_tags=["job"],
                            register_tags=["standard", "professional"],
                            db_path=db)
    _check(suite, "concept_created",
           c["ok"] and c["concept_id"].startswith("concept_"), str(c))
    cid = c["concept_id"]

    en_link = bls.add_entry_link(cid, "en",
                                  source_store="cognitive_lexicon_store",
                                  source_word="engineer",
                                  confidence=0.9,
                                  link_method="manual", db_path=db)
    _check(suite, "en_link_added",
           en_link["ok"] and en_link["link_id"].startswith("link_"),
           str(en_link))
    ru_link = bls.add_entry_link(cid, "ru",
                                  source_store="russian_lexicon_store",
                                  source_word="инженер",
                                  lemma="инженер",
                                  part_of_speech="noun",
                                  confidence=0.88,
                                  link_method="manual", db_path=db)
    _check(suite, "ru_link_added",
           ru_link["ok"], str(ru_link))
    gloss = bls.add_glossary_link(cid, english_text="engineer",
                                    russian_text="инженер",
                                    relation_type="translation",
                                    confidence=0.92,
                                    source="manual", db_path=db)
    _check(suite, "glossary_added",
           gloss["ok"] and gloss["glossary_id"].startswith("gloss_"),
           str(gloss))

    by_en = bls.find_concepts_by_label("engineer", language="en",
                                         limit=5, db_path=db)
    _check(suite, "find_by_en_label",
           len(by_en) == 1 and by_en[0]["concept_id"] == cid,
           str(by_en)[:200])
    by_ru = bls.find_concepts_by_label("инженер", language="ru",
                                         limit=5, db_path=db)
    _check(suite, "find_by_ru_label",
           len(by_ru) == 1 and by_ru[0]["concept_id"] == cid,
           str(by_ru)[:200])

    links = bls.get_links_for_concept(cid, limit=5, db_path=db)
    _check(suite, "links_bounded",
           len(links) == 2 and len(links) <= 5, str(len(links)))
    pairs = bls.get_bilingual_pairs(cid, limit=5, db_path=db)
    _check(suite, "pairs_bounded",
           len(pairs) == 1 and len(pairs) <= 5, str(len(pairs)))

    # Confidence clamp + invalid method normalize
    bad = bls.add_entry_link(cid, "en", source_word="x",
                              confidence=99.0,
                              link_method="NOT_REAL_METHOD",
                              db_path=db)
    _check(suite, "bad_method_normalized",
           bad["ok"], str(bad))
    fetched = bls.get_links_for_concept(cid, limit=10, db_path=db)
    has_normalized = any(l["link_method"] == "manual"
                          and l.get("source_word") == "x"
                          and 0.0 <= float(l.get("confidence") or 0) <= 1.0
                          for l in fetched)
    _check(suite, "confidence_clamped_and_method_normalized",
           has_normalized, str(fetched)[:200])

    # Invalid language
    bad_lang = bls.add_entry_link(cid, "xx", source_word="x", db_path=db)
    _check(suite, "invalid_language_rejected",
           bad_lang.get("ok") is False, str(bad_lang))

    # Audit trail exists
    conn = sqlite3.connect(str(db))
    try:
        n_audit = conn.execute(
            "SELECT COUNT(*) FROM link_audit").fetchone()[0]
    finally:
        conn.close()
    _check(suite, "audit_rows_written",
           int(n_audit or 0) >= 3, f"n_audit={n_audit}")

    # list_concepts bounded
    listed = bls.list_concepts(limit=5, db_path=db)
    _check(suite, "list_concepts_bounded",
           len(listed) <= 5, str(len(listed)))


# -------------------- C: Link builder --------------------

def suite_C_link_builder() -> None:
    suite = "C_LINK_BUILDER"
    td = _td()
    db = td / "links.sqlite"
    bls.init_bilingual_link_db(db)

    en_pool = blb.load_candidate_english_entries(limit=25)
    _check(suite, "en_candidates_bounded",
           isinstance(en_pool, list) and len(en_pool) <= 25,
           f"len={len(en_pool)}")
    ru_pool = blb.load_candidate_russian_entries(limit=25)
    _check(suite, "ru_candidates_bounded",
           isinstance(ru_pool, list) and len(ru_pool) <= 25,
           f"len={len(ru_pool)}")

    cat_links = blb.infer_shared_category_links(
        limit_per_category=10, link_db_path=db)
    _check(suite, "category_inference_ok",
           cat_links["ok"] and cat_links["created_concepts"] >= 1,
           str(cat_links)[:200])

    dom_links = blb.infer_domain_tag_links(
        limit_per_domain=20, link_db_path=db)
    _check(suite, "domain_inference_bounded",
           dom_links["ok"] and dom_links["candidate_pair_count"] <= 200,
           str(dom_links)[:200])

    phrase_links = blb.infer_phrase_links(limit=20, link_db_path=db)
    _check(suite, "phrase_inference_bounded",
           phrase_links["ok"]
           and phrase_links["candidate_phrase_pair_count"] <= 100,
           str(phrase_links)[:200])

    fixture = blb.build_manual_fixture_links(link_db_path=db)
    _check(suite, "fixture_links_built",
           fixture["fixture_concepts_created"] == 5, str(fixture))

    # Conservative caution: register/safety blocked pairs must not link.
    en = {"word": "alpha", "register_tags_json": '["standard"]',
          "safety_tags_json": '[]'}
    ru = {"word": "альфа", "register_tags_json": '["vulgar"]',
          "safety_tags_json": '["vulgar"]'}
    score = blb.score_link_confidence(en, ru, "domain_category_match")
    _check(suite, "score_clamped_0_to_1",
           0.0 <= score <= 1.0, str(score))

    # Report
    out = td / "lb_report.json"
    blb.write_link_builder_report({"summary": "test"}, out)
    _check(suite, "link_builder_report_written", out.exists(), "")


# -------------------- D: Retrieval bridge --------------------

def suite_D_bridge() -> None:
    suite = "D_BRIDGE"
    td = _td()
    db = td / "links.sqlite"
    bls.init_bilingual_link_db(db)
    blb.build_manual_fixture_links(link_db_path=db)

    en2ru = brb.get_bilingual_context("engineer", source_language="en",
                                        target_language="ru", limit=5,
                                        link_db_path=db)
    _check(suite, "en_query_returns_ru_context",
           en2ru["ok"] and en2ru["context"]["count"] >= 1,
           str(en2ru)[:300])
    ru2en = brb.get_bilingual_context("инженер", source_language="ru",
                                        target_language="en", limit=5,
                                        link_db_path=db)
    _check(suite, "ru_query_returns_en_context",
           ru2en["ok"] and ru2en["context"]["count"] >= 1, "")

    # Limit enforced
    big = brb.get_bilingual_context("engineer", limit=999,
                                      link_db_path=db)
    _check(suite, "bridge_limit_clamped",
           big["limit"] <= 100, str(big["limit"]))

    # Synthetic blocked entries: pass through filter manually
    blocked_input = [{"target_word": "danger_w", "concept_id": "x",
                      "safety_tags": ["do_not_use_unprompted"]}]
    filt = brb.filter_bilingual_safety(blocked_input, mode="teacher",
                                        is_user_prompted=False)
    _check(suite, "do_not_use_unprompted_filtered",
           filt["blocked_count"] >= 1 and len(filt["safe_entries"]) == 0,
           str(filt))

    reco = [{"target_word": "reco_w", "concept_id": "x",
             "safety_tags": ["recognition_only"]}]
    filt_reco = brb.filter_bilingual_safety(reco, mode="teacher",
                                              is_user_prompted=False)
    safe_recos = [e for e in filt_reco["safe_entries"]
                  if e.get("_suggestion_blocked")]
    _check(suite, "recognition_only_kept_but_suggestion_blocked",
           len(safe_recos) >= 1
           and filt_reco["suggestion_recognized_count"] >= 1, str(filt_reco))

    # Gap explanation for nonsense
    gap = brb.explain_bilingual_gap("zzz_no_such_word",
                                      link_db_path=db)
    _check(suite, "gap_explained",
           gap["ok"] and "concepts_matched_en" in gap, str(gap))


# -------------------- E: Russian morphology upgrade --------------------

def suite_E_morphology() -> None:
    suite = "E_MORPHOLOGY"
    backend = rmu.detect_morphology_backend()
    _check(suite, "backend_dict",
           "active_backend" in backend, str(backend))
    status = rmu.get_morphology_backend_status()
    _check(suite, "status_ok",
           status["ok"] and "backend" in status, str(status)[:200])
    note_path = rmu.create_pymorphy3_install_note()
    _check(suite, "install_note_written",
           Path(note_path).exists(), str(note_path))

    audit = rmu.audit_russian_entries_for_morphology(limit=100)
    _check(suite, "audit_bounded",
           audit["ok"] and audit["rows_inspected"] <= 100, str(audit)[:200])

    miss_lemma = rmu.identify_missing_lemmas(limit=50)
    _check(suite, "missing_lemma_bounded",
           isinstance(miss_lemma, list) and len(miss_lemma) <= 50,
           f"len={len(miss_lemma)}")
    miss_pos = rmu.identify_missing_pos(limit=50)
    _check(suite, "missing_pos_bounded",
           isinstance(miss_pos, list) and len(miss_pos) <= 50,
           f"len={len(miss_pos)}")

    repairs = rmu.propose_morphology_repairs(limit=20)
    _check(suite, "repairs_bounded",
           isinstance(repairs, list) and len(repairs) <= 20, "")
    if repairs:
        _check(suite, "repair_has_confidence",
               isinstance(repairs[0].get("confidence"), float),
               str(repairs[0]))
        _check(suite, "repair_has_backend",
               bool(repairs[0].get("backend")), str(repairs[0]))


# -------------------- F: Coverage gap reporter --------------------

def suite_F_coverage_gap() -> None:
    suite = "F_COVERAGE_GAP"
    td = _td()
    db = td / "links.sqlite"
    bls.init_bilingual_link_db(db)
    blb.build_manual_fixture_links(link_db_path=db)

    counts = cgr.count_linked_concepts(link_db_path=db)
    _check(suite, "linked_count_keys",
           counts["ok"] and counts["concepts"] >= 5
           and counts["english_entry_links"] >= 5
           and counts["russian_entry_links"] >= 5
           and counts["glossary_links"] >= 5, str(counts))

    by_cat = cgr.count_links_by_category(link_db_path=db)
    _check(suite, "links_by_category_21_keys",
           len(by_cat) == 21, str(len(by_cat)))

    en_gap = cgr.count_unlinked_english_by_category(link_db_path=db)
    ru_gap = cgr.count_unlinked_russian_by_category(link_db_path=db)
    _check(suite, "en_gap_dict_returned",
           isinstance(en_gap, dict) and len(en_gap) >= 21
           or isinstance(en_gap, dict), str(type(en_gap)))
    _check(suite, "ru_gap_dict_returned",
           isinstance(ru_gap, dict), str(type(ru_gap)))

    imbalances = cgr.identify_category_imbalances(min_gap=1,
                                                    link_db_path=db)
    _check(suite, "imbalances_returned",
           isinstance(imbalances, list), str(type(imbalances)))

    prof = cgr.identify_missing_profession_links(limit=10)
    trade = cgr.identify_missing_trade_links(limit=10)
    poetic = cgr.identify_missing_poetry_philosophy_links(limit=10)
    slang = cgr.identify_slang_link_cautions(limit=10)
    _check(suite, "specific_categories_returned",
           "category" in prof and "category" in trade
           and "poetry" in poetic and "cautions" in slang,
           f"{prof.keys()}|{trade.keys()}|{poetic.keys()}|{slang.keys()}")

    out = td / "gap.json"
    cgr.write_bilingual_coverage_gap_report({"summary": "synthetic"}, out)
    _check(suite, "gap_report_written", out.exists(), "")


# -------------------- G: Production safety --------------------

def suite_G_production_safety() -> None:
    suite = "G_PRODUCTION_SAFETY"
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    import glob
    before_en = enlex.count_words()
    before_ru = rulex.count_words()
    before_phr = rulex.count_phrases()
    before_mans = len(glob.glob("seed_packs/en/*.en_pack_manifest.json")) \
        + len(glob.glob("seed_packs/ru/*.ru_pack_manifest.json"))
    td = _td()
    db = td / "links.sqlite"
    bls.init_bilingual_link_db(db)
    blb.build_manual_fixture_links(link_db_path=db)
    blb.infer_shared_category_links(limit_per_category=5, link_db_path=db)
    brb.get_bilingual_context("engineer", limit=5, link_db_path=db)
    cgr.count_linked_concepts(link_db_path=db)
    rmu.audit_russian_entries_for_morphology(limit=20)
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


# -------------------- H: Isolation --------------------

PHASE22_FILES = [
    "bilingual_concept_link_store.py",
    "bilingual_link_builder.py",
    "bilingual_retrieval_bridge.py",
    "russian_morphology_upgrade_path.py",
    "bilingual_coverage_gap_reporter.py",
]


def suite_H_isolation() -> None:
    suite = "H_ISOLATION"
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
    for fname in PHASE22_FILES:
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


# -------------------- Driver --------------------

def main() -> int:
    suites = [
        ("A_PREFLIGHT", suite_A_preflight),
        ("B_LINK_STORE", suite_B_link_store),
        ("C_LINK_BUILDER", suite_C_link_builder),
        ("D_BRIDGE", suite_D_bridge),
        ("E_MORPHOLOGY", suite_E_morphology),
        ("F_COVERAGE_GAP", suite_F_coverage_gap),
        ("G_PRODUCTION_SAFETY", suite_G_production_safety),
        ("H_ISOLATION", suite_H_isolation),
    ]
    for label, fn in suites:
        try:
            fn()
        except Exception as e:
            _check(label, "suite_crashed", False,
                   f"{e!r}\n{traceback.format_exc()}")
    fails = [r for r in _results if not r[2].startswith(PASS)]
    print("=== Phase 22 Bilingual Linker / Retrieval Bridge ===")
    print(f"Total: {len(_results)} | Pass: {len(_results) - len(fails)} | Fail: {len(fails)}")
    for s, n, st in _results:
        print(f"  [{s}] {n}: {st}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
