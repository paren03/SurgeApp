"""Latin-substring extract pass for main's 1M+1M corpus.

Targeted orchestrator: among RU rows whose `word` column contains at
least one Latin character (~20,022 rows on main as of 2026-06-01),
extract the longest Latin run and see if it matches any EN word.

Why this matters
================
At 1M+1M scale, EN (gcide dictionary) and RU (Russian Wiktionary +
DBpedia) corpora share no Cyrillic↔Latin string identity. The simple
exact-word match yields ~0. But: a small subset of RU rows preserves
the original Latin form (named entities like "iPhone", "Apple Inc.",
brand names, tech identifiers). Those are real translation pairs
operators care about for retrieval.

Yield estimate: 20k candidates × ~50% with Latin run that matches an
EN word ≈ ~5-10k high-confidence pairs.

Honest note
===========
This is the best exact-character match available on this corpus.
Going beyond requires semantic embeddings (multilingual sentence-
transformer), which is a multi-hour compute at 1M×1M and a separate
session. The foundation built by this orchestrator family
(bilingual_main_adapter + bilingual_concept_links_db) is the
reusable platform for that.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import bilingual_main_adapter as _mainadapt
import bilingual_concept_links_db as _cdb

MAIN_DB = _mainadapt.MAIN_DB_PATH
CONCEPT_DB = _cdb.CONCEPT_DB_PATH
REPORTS_DIR = Path(__file__).resolve().parent / "memory" / "bilingual_main_reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

LATIN_RUN_RE = re.compile(r"[A-Za-z][A-Za-z0-9 .'_-]*[A-Za-z0-9]|[A-Za-z]")
LATIN_CONFIDENCE = 0.8


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_en_word_index() -> Dict[str, Dict[str, Any]]:
    """lc(word) -> EN row dict (first id wins on duplicate words)."""
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
            out[lc] = dict(r)
    finally:
        conn.close()
    return out


def _extract_latin_runs(s: str) -> List[str]:
    """Return non-trivial Latin substrings (length >= 2)."""
    if not s:
        return []
    out = []
    for m in LATIN_RUN_RE.finditer(s):
        run = m.group(0).strip()
        if len(run) >= 2 and any(c.isalpha() for c in run):
            out.append(run)
    return out


def latin_extract_pass() -> Dict[str, Any]:
    t0 = time.monotonic()
    report: Dict[str, Any] = {
        "ts": _now_iso(),
        "pass": "main_latin_extract",
        "main_db": str(MAIN_DB),
        "concept_db": str(CONCEPT_DB),
        "report_version": 1,
    }
    print("loading EN word index...", flush=True)
    t_idx = time.monotonic()
    en_index = _load_en_word_index()
    report["en_index_size"] = len(en_index)
    report["en_index_load_s"] = round(time.monotonic() - t_idx, 2)
    print(f"  loaded {len(en_index):,} EN words in {report['en_index_load_s']:.1f}s",
          flush=True)

    en_seen, ru_seen = _cdb.existing_linked_words()
    report["pre_en_seen"] = len(en_seen)
    report["pre_ru_seen"] = len(ru_seen)

    print("streaming RU rows with Latin chars...", flush=True)
    conn = sqlite3.connect(
        f"file:{MAIN_DB.as_posix()}?mode=ro", uri=True, timeout=15.0)
    conn.row_factory = sqlite3.Row
    concept_rows: List[Tuple] = []
    link_rows: List[Tuple] = []
    matched = 0
    skipped_dup = 0
    scanned = 0
    no_latin_match = 0
    try:
        # Only scan rows whose word contains at least one Latin char.
        q = ("SELECT id, word, register, coverage_categories, "
             "safety_tags, pos, source_pack, definition "
             "FROM russian_words "
             "WHERE word GLOB '*[A-Za-z]*'")
        for r in conn.execute(q):
            scanned += 1
            if scanned % 5000 == 0:
                print(f"  {scanned:,} scanned, {matched:,} matched", flush=True)
            ru_word = (r["word"] or "").strip()
            if not ru_word:
                continue
            runs = _extract_latin_runs(ru_word)
            # Match the longest Latin run first (head noun heuristic).
            runs.sort(key=lambda s: -len(s))
            matched_en: Optional[Dict[str, Any]] = None
            matched_run: Optional[str] = None
            for run in runs:
                lc = run.lower()
                en = en_index.get(lc)
                if en:
                    matched_en = en
                    matched_run = run
                    break
            if not matched_en:
                no_latin_match += 1
                continue
            en_word_lc = (matched_en["word"] or "").lower()
            ru_word_lc = ru_word.lower()
            if en_word_lc in en_seen or ru_word_lc in ru_seen:
                skipped_dup += 1
                continue
            cid = _cdb._new_id("concept")
            now = _now_iso()
            try:
                en_cov = set(json.loads(matched_en["coverage_categories"] or "[]"))
            except Exception:  # noqa: BLE001
                en_cov = set()
            try:
                ru_cov = set(json.loads(r["coverage_categories"] or "[]"))
            except Exception:  # noqa: BLE001
                ru_cov = set()
            coverage = sorted(en_cov | ru_cov)
            registers = sorted(
                {matched_en["register"] or "", r["register"] or ""} - {""})
            notes = (f"main_latin_extract en_id={matched_en['id']} "
                     f"ru_id={r['id']} matched_run={matched_run!r}")
            concept_rows.append((
                cid, matched_en["word"], ru_word,
                _cdb.json_list(coverage),
                _cdb.json_list(coverage),
                _cdb.json_list(registers or ["standard"]),
                "[]",
                now, now, notes,
            ))
            link_rows.append((
                _cdb._new_id("link"), cid, "en",
                "main_english_words", str(matched_en["id"]),
                matched_en["word"], "",
                "", matched_en["pos"] or "",
                LATIN_CONFIDENCE, "exact_match", now, notes,
            ))
            link_rows.append((
                _cdb._new_id("link"), cid, "ru",
                "main_russian_words", str(r["id"]),
                ru_word, "",
                "", r["pos"] or "",
                LATIN_CONFIDENCE, "exact_match", now, notes,
            ))
            en_seen.add(en_word_lc)
            ru_seen.add(ru_word_lc)
            matched += 1
    finally:
        conn.close()

    report["scanned_ru_with_latin"] = scanned
    report["matched_pairs"] = matched
    report["skipped_duplicate"] = skipped_dup
    report["no_latin_match"] = no_latin_match
    if concept_rows:
        t_ins = time.monotonic()
        ins = _cdb.insert_concept_batch(
            concept_rows, link_rows,
            audit_message=f"latin_extract scanned={scanned} matched={matched}")
        report["insert"] = ins
        report["insert_s"] = round(time.monotonic() - t_ins, 2)
    else:
        report["insert_skipped"] = "no_new_pairs"
    report["elapsed_s"] = round(time.monotonic() - t0, 2)
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    args = p.parse_args()
    report = latin_extract_pass()
    out_path = REPORTS_DIR / (
        f"bilingual_main_latin_extract_"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nreport -> {out_path}")
