"""Exact-word match orchestrator for main's 1M+1M corpus.

First-pass linker at production scale: finds EN/RU pairs whose `word`
column is identical (case-insensitive). Catches:
  - Latinized loanwords in RU (Wikidata lexemes that kept Latin form)
  - Named entities from DBpedia RU corpus that share English form
  - Code/tech terms ("Python", "WiFi", "JSON")
  - Identical proper nouns

Confidence is HIGH for this match type — these aren't category-rank
guesses, they're spelling-identical pairs. Confidence = 0.9 with
link_method = "exact_match" (pre-blessed in the Phase 22 valid set).

Algorithm
=========
1. Load all lowercased EN words into a dict mapping lc(word) → first id
   (~30 MB RAM for 1M words).
2. Stream the RU table once; for each row, check whether its lc(word)
   is in the EN map. If yes + neither side is already linked, queue
   a concept + 2 entry_links insert.
3. Batched single-transaction write at end.

Doctrine guardrails
===================
- Main's bilingual_links.sqlite opened READ-ONLY via URI mode.
- Production EN/RU rows NOT mutated.
- New rows only in `D:/SurgeApp/bilingual_concept_links.sqlite`
  (separate file, no schema mutation of production DB).
- Dedupes against existing entry_links in the concept DB on insert,
  so re-runs add only NEW pairs (idempotent).
- NEVER raises into the caller.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import bilingual_main_adapter as _mainadapt
import bilingual_concept_links_db as _cdb

MAIN_DB = _mainadapt.MAIN_DB_PATH
CONCEPT_DB = _cdb.CONCEPT_DB_PATH
REPORTS_DIR = Path(__file__).resolve().parent / "memory" / "bilingual_main_reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

EXACT_CONFIDENCE = 0.9


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_en_word_index() -> Dict[str, Dict[str, Any]]:
    """lc(word) -> {id, word, register, coverage_categories, pos,
                    source_pack}. Picks the FIRST id per word."""
    conn = sqlite3.connect(
        f"file:{MAIN_DB.as_posix()}?mode=ro", uri=True, timeout=15.0)
    conn.row_factory = sqlite3.Row
    out: Dict[str, Dict[str, Any]] = {}
    try:
        for r in conn.execute(
                "SELECT id, word, register, coverage_categories, "
                "safety_tags, pos, source_pack FROM english_words"):
            w = (r["word"] or "").strip()
            if not w:
                continue
            lc = w.lower()
            if lc in out:
                continue
            out[lc] = {
                "id": r["id"], "word": w, "register": r["register"] or "",
                "coverage_categories": r["coverage_categories"] or "[]",
                "safety_tags": r["safety_tags"] or "[]",
                "pos": r["pos"] or "",
                "source_pack": r["source_pack"] or "",
            }
    finally:
        conn.close()
    return out


def exact_match_pass(limit: Optional[int] = None,
                      progress_every: int = 100_000
                      ) -> Dict[str, Any]:
    """Run the pass. Returns a report dict."""
    t0 = time.monotonic()
    report: Dict[str, Any] = {
        "ts": _now_iso(),
        "pass": "exact_match",
        "main_db": str(MAIN_DB),
        "concept_db": str(CONCEPT_DB),
        "limit": limit,
        "report_version": 1,
    }
    print("loading EN word index...", flush=True)
    t_idx = time.monotonic()
    en_index = _load_en_word_index()
    report["en_index_size"] = len(en_index)
    report["en_index_load_s"] = round(time.monotonic() - t_idx, 2)
    print(f"  loaded {len(en_index):,} EN words in {report['en_index_load_s']:.1f}s",
          flush=True)

    # Pre-load already-linked words for dedup.
    print("loading existing linked-words set...", flush=True)
    en_seen, ru_seen = _cdb.existing_linked_words()
    report["pre_en_seen"] = len(en_seen)
    report["pre_ru_seen"] = len(ru_seen)
    print(f"  pre-linked: EN={len(en_seen):,} RU={len(ru_seen):,}", flush=True)

    # Stream RU and match.
    print("streaming RU words...", flush=True)
    conn = sqlite3.connect(
        f"file:{MAIN_DB.as_posix()}?mode=ro", uri=True, timeout=15.0)
    conn.row_factory = sqlite3.Row
    concept_rows: List[Tuple] = []
    link_rows: List[Tuple] = []
    matched = 0
    skipped_duplicate = 0
    scanned = 0
    try:
        q = ("SELECT id, word, register, coverage_categories, "
             "safety_tags, pos, source_pack FROM russian_words")
        if limit:
            q += f" LIMIT {int(limit)}"
        for r in conn.execute(q):
            scanned += 1
            if scanned % progress_every == 0:
                elapsed = time.monotonic() - t_idx
                rate = scanned / max(elapsed, 0.001)
                print(f"  {scanned:,} scanned, {matched:,} matched, "
                      f"{skipped_duplicate:,} dup-skipped, "
                      f"{rate:,.0f} rows/s", flush=True)
            ru_word = (r["word"] or "").strip()
            if not ru_word:
                continue
            lc = ru_word.lower()
            en = en_index.get(lc)
            if not en:
                continue
            # Dedup: skip if EITHER side already linked.
            en_word_lc = en["word"].lower()
            if en_word_lc in en_seen or lc in ru_seen:
                skipped_duplicate += 1
                continue
            # Compose concept + 2 entry_links rows.
            cid = _cdb._new_id("concept")
            now = _now_iso()
            # Coverage: union of EN + RU coverage_categories.
            try:
                en_cov = set(json.loads(en["coverage_categories"] or "[]"))
            except Exception:  # noqa: BLE001
                en_cov = set()
            try:
                ru_cov = set(json.loads(r["coverage_categories"] or "[]"))
            except Exception:  # noqa: BLE001
                ru_cov = set()
            coverage = sorted(en_cov | ru_cov)
            register_union = sorted({en["register"], r["register"] or ""} - {""})
            notes = f"main_exact_match en_id={en['id']} ru_id={r['id']}"
            concept_rows.append((
                cid, en["word"], ru_word,
                _cdb.json_list(coverage),
                _cdb.json_list(coverage),  # domain_tags == coverage at 1M scale
                _cdb.json_list(register_union or ["standard"]),
                "[]",
                now, now, notes,
            ))
            link_rows.append((
                _cdb._new_id("link"), cid, "en",
                "main_english_words", str(en["id"]), en["word"], "",
                "", en["pos"],
                EXACT_CONFIDENCE, "exact_match", now, notes,
            ))
            link_rows.append((
                _cdb._new_id("link"), cid, "ru",
                "main_russian_words", str(r["id"]), ru_word, "",
                "", r["pos"] or "",
                EXACT_CONFIDENCE, "exact_match", now, notes,
            ))
            en_seen.add(en_word_lc)
            ru_seen.add(lc)
            matched += 1
    finally:
        conn.close()

    report["scanned_ru"] = scanned
    report["matched_pairs"] = matched
    report["skipped_duplicate"] = skipped_duplicate
    report["queued_concepts"] = len(concept_rows)
    report["queued_entry_links"] = len(link_rows)

    if not concept_rows:
        report["insert_skipped"] = "no_new_pairs"
        report["elapsed_s"] = round(time.monotonic() - t0, 2)
        return report

    print(f"inserting {len(concept_rows):,} concepts + "
          f"{len(link_rows):,} entry_links in single transaction...",
          flush=True)
    t_ins = time.monotonic()
    ins = _cdb.insert_concept_batch(
        concept_rows, link_rows,
        audit_message=f"exact_match scanned={scanned} matched={matched} "
                      f"skipped_dup={skipped_duplicate}")
    report["insert"] = ins
    report["insert_s"] = round(time.monotonic() - t_ins, 2)
    report["elapsed_s"] = round(time.monotonic() - t0, 2)
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Exact-word match orchestrator at 1M scale.")
    p.add_argument("--limit", type=int, default=None,
                   help="Cap RU rows scanned (for sampling).")
    args = p.parse_args()
    report = exact_match_pass(limit=args.limit)
    out_path = REPORTS_DIR / (
        f"bilingual_main_exact_match_"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nreport -> {out_path}")
