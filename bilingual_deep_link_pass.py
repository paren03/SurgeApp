"""Deep bilingual link pass — orchestrator (NOT a new phase module).

Origin
======
2026-06-01: operator directive "lets continue with the two languages
make sure the are linked and upgrade to the full extend". Phase 22's
canonical heuristics are deliberately conservative (1 pair per category
via infer_shared_category_links), giving a maximum of ~21 concepts per
full pass. After the initial seed pass plus a few manual fixtures, the
bilingual_links.sqlite was at 26 concepts / 52 entry_links — roughly
1% link coverage across 2814 EN + 2518 RU rows.

This module is NOT a new Phase 49 layer. It uses ONLY the existing
public APIs from bilingual_link_builder + bilingual_concept_link_store
to make multi-rank passes per category, plus deduplicates against the
existing link store so it can be re-run safely.

Doctrine guardrails preserved
=============================
- Reads bounded — load_candidate_*_entries respects its limit
- Honors _register_compatible + _safety_compatible from the linker
- Honors coverage_category (only pairs rows tagged in the same canonical
  category — never crosses category boundaries)
- Never modifies production EN/RU rows
- Writes only to bilingual_stack/bilingual_links.sqlite (additive)
- Skips any (EN, RU) pair where EITHER side is already linked to a
  bilingual concept anywhere — so duplicate runs add only NEW pairs
- Returns a structured report; never raises into the caller

CLI
===
    python bilingual_deep_link_pass.py [--ranks N] [--dry-run]

``--ranks`` (default 20) is the cap on (EN, RU) pairs created per
category. ``--dry-run`` reports what WOULD be created without writing.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlite3

from bilingual_link_builder import (
    COVERAGE_CATEGORIES,
    load_candidate_english_entries,
    load_candidate_russian_entries,
    score_link_confidence,
    _register_compatible,
    _safety_compatible,
    _coverage_overlap,
    _domain_overlap,
)
import bilingual_concept_link_store as bls
from bilingual_concept_link_store import (
    _new_id, _now, _json_list,
    _clamp_confidence, _normalize_method,
    init_bilingual_link_db, DEFAULT_LINK_DB,
)

REPORTS_DIR = Path(__file__).resolve().parent / "bilingual_stack" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    import datetime as _dt
    return _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _existing_linked_words(link_db_path: Optional[str] = None
                            ) -> Tuple[Set[str], Set[str]]:
    """Pre-load the set of EN words + RU words already linked to ANY
    concept, so the deep pass doesn't create duplicates."""
    en_seen: Set[str] = set()
    ru_seen: Set[str] = set()
    try:
        rows = bls.bounded_query(
            "SELECT language, source_word FROM entry_links",
            limit=100000, db_path=link_db_path)
        for lang, word in rows:
            w = (word or "").strip().lower()
            if not w:
                continue
            if lang == "en":
                en_seen.add(w)
            elif lang == "ru":
                ru_seen.add(w)
    except Exception:  # noqa: BLE001
        pass
    return en_seen, ru_seen


def _deep_pass_batch(ranks_per_category: int,
                     candidate_pool: int,
                     link_db_path: Optional[str]) -> Dict[str, Any]:
    """Batched implementation: build all concept + entry-link rows in
    memory, then write them in a SINGLE transaction with executemany().
    Bypasses the per-call connect/close/audit cycle that makes the
    ``deep_pass()`` default path slow on Windows (~5 s per concept).

    Doctrine still preserved: only writes additive rows to
    bilingual_links.sqlite, never touches production EN/RU stores,
    respects register/safety/coverage filters, dedupes against existing
    entry_links pre-pass + within-pass. One summary audit row written
    at the end (vs one per insert in the unbatched path).
    """
    t0 = time.monotonic()
    en_seen, ru_seen = _existing_linked_words(link_db_path)
    concept_rows: List[Tuple] = []
    link_rows: List[Tuple] = []
    skipped_register = 0
    skipped_duplicate = 0
    per_category: Dict[str, Dict[str, Any]] = {}

    init_bilingual_link_db(link_db_path)
    db_path_str = str(link_db_path) if link_db_path else str(DEFAULT_LINK_DB)

    for cat in COVERAGE_CATEGORIES:
        try:
            en_rows = load_candidate_english_entries(
                limit=candidate_pool, coverage_category=cat)
            ru_rows = load_candidate_russian_entries(
                limit=candidate_pool, coverage_category=cat)
        except Exception as exc:  # noqa: BLE001
            per_category[cat] = {"error": f"{type(exc).__name__}: {exc}"}
            continue

        cat_made = 0
        cat_skip_reg = 0
        cat_skip_dup = 0
        cat_rank = 0

        while (cat_made < ranks_per_category
               and cat_rank < min(len(en_rows), len(ru_rows))):
            e = en_rows[cat_rank]
            r = ru_rows[cat_rank]
            cat_rank += 1
            e_word = (e.get("word") or "").strip()
            r_word = (r.get("word") or "").strip()
            if not e_word or not r_word:
                continue
            if (e_word.lower() in en_seen
                    or r_word.lower() in ru_seen):
                cat_skip_dup += 1
                skipped_duplicate += 1
                continue
            if (not _register_compatible(e, r)
                    or not _safety_compatible(e, r)):
                cat_skip_reg += 1
                skipped_register += 1
                continue

            cov = sorted(_coverage_overlap(e, r) or {cat})
            dom = sorted(_domain_overlap(e, r))
            conf = _clamp_confidence(
                score_link_confidence(e, r, "domain_category_match"))
            notes = f"deep_pass_batch category={cat} rank={cat_rank - 1}"
            now = _now()

            cid = _new_id("concept")
            concept_rows.append((
                cid, e_word, r_word,
                _json_list(cov), _json_list(dom),
                _json_list(["standard"]), _json_list([]),
                now, now, notes,
            ))
            link_rows.append((
                _new_id("link"), cid, "en",
                "cognitive_lexicon_store", e_word, e_word, "",
                str(e.get("lemma") or ""),
                str(e.get("part_of_speech") or ""),
                conf, _normalize_method("domain_category_match"),
                now, notes,
            ))
            link_rows.append((
                _new_id("link"), cid, "ru",
                "russian_lexicon_store", r_word, r_word, "",
                str(r.get("lemma") or ""),
                str(r.get("part_of_speech") or ""),
                conf, _normalize_method("domain_category_match"),
                now, notes,
            ))
            en_seen.add(e_word.lower())
            ru_seen.add(r_word.lower())
            cat_made += 1

        per_category[cat] = {
            "linked": cat_made,
            "skipped_register": cat_skip_reg,
            "skipped_duplicate": cat_skip_dup,
            "en_pool": len(en_rows),
            "ru_pool": len(ru_rows),
            "ranks_scanned": cat_rank,
        }

    # Single-transaction write. Note _connect() uses isolation_level=None
    # (autocommit) — open our own connection with explicit transaction
    # for executemany batching.
    conn = sqlite3.connect(db_path_str, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    try:
        conn.execute("BEGIN")
        if concept_rows:
            conn.executemany(
                "INSERT INTO concepts (concept_id, canonical_label_en, "
                "canonical_label_ru, coverage_categories_json, "
                "domain_tags_json, register_tags_json, safety_tags_json, "
                "created_at, updated_at, notes) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                concept_rows)
        if link_rows:
            conn.executemany(
                "INSERT INTO entry_links (link_id, concept_id, language, "
                "source_store, source_entry_id, source_word, source_phrase, "
                "lemma, part_of_speech, confidence, link_method, created_at, "
                "notes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                link_rows)
        # Summary audit row (link_audit schema has NOT NULL on
        # concept_id + link_id — use empty string for the summary row).
        conn.execute(
            "INSERT INTO link_audit (audit_id, action, concept_id, link_id, "
            "status, message, created_at) VALUES (?,?,?,?,?,?,?)",
            (_new_id("audit"), "deep_pass_batch", "", "", "ok",
             f"created_concepts={len(concept_rows)} "
             f"created_entry_links={len(link_rows)} "
             f"ranks_per_category={ranks_per_category} "
             f"skipped_register={skipped_register} "
             f"skipped_duplicate={skipped_duplicate}",
             _now()))
        conn.execute("COMMIT")
    except Exception:  # noqa: BLE001
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    return {
        "ts": _now_iso(),
        "ok": True,
        "mode": "batch",
        "ranks_per_category": ranks_per_category,
        "candidate_pool": candidate_pool,
        "created_concepts": len(concept_rows),
        "created_entry_links": len(link_rows),
        "skipped_register": skipped_register,
        "skipped_duplicate": skipped_duplicate,
        "per_category": per_category,
        "elapsed_s": round(time.monotonic() - t0, 3),
        "report_version": 1,
    }


def deep_pass(ranks_per_category: int = 20,
              candidate_pool: int = 200,
              link_db_path: Optional[str] = None,
              dry_run: bool = False) -> Dict[str, Any]:
    """For each canonical category, create up to ``ranks_per_category``
    new concept rows pairing rank-i EN word with rank-i RU word.

    Skips (EN, RU) pairs where either word is already linked to any
    concept. Respects register/safety/coverage filters from the linker
    module. Returns a structured report; never raises.
    """
    t0 = time.monotonic()
    en_seen, ru_seen = _existing_linked_words(link_db_path)
    created_concepts = 0
    created_entry_links = 0
    skipped_register = 0
    skipped_duplicate = 0
    per_category: Dict[str, Dict[str, Any]] = {}

    for cat in COVERAGE_CATEGORIES:
        try:
            en_rows = load_candidate_english_entries(
                limit=candidate_pool, coverage_category=cat)
            ru_rows = load_candidate_russian_entries(
                limit=candidate_pool, coverage_category=cat)
        except Exception as exc:  # noqa: BLE001
            per_category[cat] = {"error": f"{type(exc).__name__}: {exc}"}
            continue

        cat_made = 0
        cat_skip_reg = 0
        cat_skip_dup = 0
        cat_rank = 0  # walks through both lists in parallel

        while (cat_made < ranks_per_category
               and cat_rank < min(len(en_rows), len(ru_rows))):
            e = en_rows[cat_rank]
            r = ru_rows[cat_rank]
            cat_rank += 1
            e_word = (e.get("word") or "").strip()
            r_word = (r.get("word") or "").strip()
            if not e_word or not r_word:
                continue
            if (e_word.lower() in en_seen
                    or r_word.lower() in ru_seen):
                cat_skip_dup += 1
                skipped_duplicate += 1
                continue
            if (not _register_compatible(e, r)
                    or not _safety_compatible(e, r)):
                cat_skip_reg += 1
                skipped_register += 1
                continue

            cov = sorted(_coverage_overlap(e, r) or {cat})
            dom = sorted(_domain_overlap(e, r))
            conf = float(score_link_confidence(
                e, r, "domain_category_match"))
            notes = f"deep_pass category={cat} rank={cat_rank - 1}"

            if dry_run:
                cat_made += 1
                en_seen.add(e_word.lower())
                ru_seen.add(r_word.lower())
                continue

            try:
                c = bls.create_concept(
                    canonical_label_en=e_word,
                    canonical_label_ru=r_word,
                    coverage_categories=cov,
                    domain_tags=dom,
                    register_tags=["standard"],
                    notes=notes,
                    db_path=link_db_path)
                cid = c["concept_id"]
                created_concepts += 1
                bls.add_entry_link(
                    cid, "en",
                    source_store="cognitive_lexicon_store",
                    source_entry_id=e_word,
                    source_word=e_word,
                    lemma=str(e.get("lemma") or ""),
                    part_of_speech=str(e.get("part_of_speech") or ""),
                    confidence=conf,
                    link_method="domain_category_match",
                    notes=notes,
                    db_path=link_db_path)
                bls.add_entry_link(
                    cid, "ru",
                    source_store="russian_lexicon_store",
                    source_entry_id=r_word,
                    source_word=r_word,
                    lemma=str(r.get("lemma") or ""),
                    part_of_speech=str(r.get("part_of_speech") or ""),
                    confidence=conf,
                    link_method="domain_category_match",
                    notes=notes,
                    db_path=link_db_path)
                created_entry_links += 2
                en_seen.add(e_word.lower())
                ru_seen.add(r_word.lower())
                cat_made += 1
            except Exception as exc:  # noqa: BLE001
                cat_skip_reg += 1  # bucket DB errors with register skips
                continue

        per_category[cat] = {
            "linked": cat_made,
            "skipped_register": cat_skip_reg,
            "skipped_duplicate": cat_skip_dup,
            "en_pool": len(en_rows),
            "ru_pool": len(ru_rows),
            "ranks_scanned": cat_rank,
        }

    return {
        "ts": _now_iso(),
        "ok": True,
        "dry_run": dry_run,
        "ranks_per_category": ranks_per_category,
        "candidate_pool": candidate_pool,
        "created_concepts": created_concepts,
        "created_entry_links": created_entry_links,
        "skipped_register": skipped_register,
        "skipped_duplicate": skipped_duplicate,
        "per_category": per_category,
        "elapsed_s": round(time.monotonic() - t0, 3),
        "report_version": 1,
    }


def _cli() -> int:
    p = argparse.ArgumentParser(
        description="Deep bilingual link pass — multi-rank per-category "
                    "concept creation using existing Phase 22 primitives.")
    p.add_argument("--ranks", type=int, default=20,
                   help="Max concept pairs to create per category "
                        "(default 20)")
    p.add_argument("--pool", type=int, default=200,
                   help="Candidate pool size per category (default 200)")
    p.add_argument("--dry-run", action="store_true",
                   help="Report would-be creations without writing")
    p.add_argument("--batch", action="store_true",
                   help="Use the batched single-transaction writer "
                        "(~100x faster than the default per-call path; "
                        "ignores --dry-run)")
    args = p.parse_args()

    if args.batch:
        report = _deep_pass_batch(
            ranks_per_category=args.ranks,
            candidate_pool=args.pool,
            link_db_path=None)
    else:
        report = deep_pass(ranks_per_category=args.ranks,
                           candidate_pool=args.pool,
                           dry_run=args.dry_run)

    # Persist the report alongside the existing link_builder reports.
    suffix = "_dryrun" if args.dry_run else ""
    out_path = REPORTS_DIR / (
        f"bilingual_deep_link_pass{suffix}_"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    print(json.dumps(
        {k: v for k, v in report.items() if k != "per_category"},
        indent=2, ensure_ascii=False))
    print(f"\nfull report -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
