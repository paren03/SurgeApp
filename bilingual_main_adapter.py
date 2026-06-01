"""Schema adapter — read main's 1M+1M flat bilingual_links.sqlite as
orchestrator-shape rows.

Background
==========
Main's `D:/SurgeApp/bilingual_links.sqlite` (1.05 GB) was built by the
Phase 21 production import path with a FLAT schema:
    english_words(id, word, definition, language, register,
                  coverage_categories, safety_tags, pos,
                  examples, source_pack, metadata, imported_at)
    russian_words(id, word, definition, language, cyrillic,
                  transliteration, register, coverage_categories,
                  safety_tags, pos, examples, source_pack,
                  metadata, imported_at)
    pack_manifests, import_log, sqlite_sequence

The worktree's orchestrators (`bilingual_deep_link_pass.py` etc.) expect
row dicts shaped like the seed corpus produced by `cognitive_lexicon_store`
and `russian_lexicon_store`:
    word, definition, lemma, part_of_speech, register_tags_json,
    safety_tags_json, coverage_categories_json, pack_source, ...

This module bridges the gap. It is READ-ONLY (uses sqlite URI ?mode=ro)
so it cannot mutate the 1.05 GB production database. All loaders return
plain dicts in the orchestrator-expected shape.

The adapter is the cheapest part of the 1M-scale integration: no copying,
no writes to production, just a schema translation layer.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

MAIN_DB_PATH = Path(__file__).resolve().parent / "bilingual_links.sqlite"


def _connect_readonly(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Open main's bilingual_links.sqlite in READ-ONLY mode.

    URI form prevents any accidental write. timeout=10s in case the file
    is being read by another reader (WAL allows concurrent readers).
    """
    p = Path(db_path) if db_path else MAIN_DB_PATH
    uri = f"file:{p.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _adapt_en_row(r: sqlite3.Row) -> Dict[str, Any]:
    """Map an english_words row to the orchestrator's expected dict shape."""
    d = dict(r)
    register = d.get("register") or ""
    register_tags = [register] if register else []
    return {
        "word": d.get("word") or "",
        "definition": d.get("definition") or "",
        "lemma": "",  # main schema has no lemma; orchestrators tolerate ""
        "part_of_speech": d.get("pos") or "",
        "register_tags_json": json.dumps(register_tags, ensure_ascii=False),
        "safety_tags_json": d.get("safety_tags") or "[]",
        "coverage_categories_json": d.get("coverage_categories") or "[]",
        "tags_json": d.get("coverage_categories") or "[]",  # alias for linker
        "domain_tags_json": d.get("coverage_categories") or "[]",
        "pack_source": d.get("source_pack") or "",
        "pack_id": "",
        "source": d.get("source_pack") or "",
        "language": "en",
        "frequency_score": 0.0,  # main doesn't store; orchestrators sort by it but tolerate 0
        "word_level": "",
        "_main_id": d.get("id"),  # preserve for traceability
    }


def _adapt_ru_row(r: sqlite3.Row) -> Dict[str, Any]:
    """Map a russian_words row to the orchestrator's expected dict shape.

    Note: main's russian_words.definition is typically Russian (or
    Russian Wikipedia lead for DBpedia rows). The orchestrators' definition
    matching path assumes RU.definition_en holds an English string — that
    column does NOT exist on main. Callers that want definition-match
    must use a different probe (e.g., RU.word in English/Latin form, or
    parse metadata for explicit English glosses).
    """
    d = dict(r)
    register = d.get("register") or ""
    register_tags = [register] if register else []
    return {
        "word": d.get("word") or "",
        "definition": d.get("definition") or "",
        "definition_ru": d.get("definition") or "",
        # No native definition_en column on main — leave empty so callers
        # that probe definition_en skip cleanly. They can use metadata
        # parsing or word-form heuristics instead.
        "definition_en": "",
        "lemma": "",
        "part_of_speech": d.get("pos") or "",
        "register_tags_json": json.dumps(register_tags, ensure_ascii=False),
        "safety_tags_json": d.get("safety_tags") or "[]",
        "coverage_categories_json": d.get("coverage_categories") or "[]",
        "tags_json": d.get("coverage_categories") or "[]",
        "domain_tags_json": d.get("coverage_categories") or "[]",
        "semantic_tags_json": d.get("coverage_categories") or "[]",
        "pack_source": d.get("source_pack") or "",
        "pack_id": "",
        "source": d.get("source_pack") or "",
        "register_level": register,
        "language": "ru",
        "transliteration": d.get("transliteration") or "",
        "cyrillic": int(d.get("cyrillic") or 1),
        "frequency_score": 0.0,
        "_main_id": d.get("id"),
    }


# ---- public loader API (matches worktree orchestrator expectations) ----


def load_candidate_english_entries(
    limit: int = 1000,
    coverage_category: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """Stream up to ``limit`` english_words rows in adapter shape.

    If ``coverage_category`` is provided, only rows whose
    coverage_categories JSON column contains that category are returned.
    Order is by id (insertion order) — main has no frequency_score column.
    """
    conn = _connect_readonly(db_path)
    try:
        if coverage_category:
            cur = conn.execute(
                "SELECT * FROM english_words "
                "WHERE coverage_categories LIKE ? "
                "ORDER BY id LIMIT ?",
                (f'%"{coverage_category}"%', int(limit)),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM english_words ORDER BY id LIMIT ?",
                (int(limit),),
            )
        return [_adapt_en_row(r) for r in cur]
    finally:
        conn.close()


def load_candidate_russian_entries(
    limit: int = 1000,
    coverage_category: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    conn = _connect_readonly(db_path)
    try:
        if coverage_category:
            cur = conn.execute(
                "SELECT * FROM russian_words "
                "WHERE coverage_categories LIKE ? "
                "ORDER BY id LIMIT ?",
                (f'%"{coverage_category}"%', int(limit)),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM russian_words ORDER BY id LIMIT ?",
                (int(limit),),
            )
        return [_adapt_ru_row(r) for r in cur]
    finally:
        conn.close()


def list_coverage_categories(language: str = "en",
                              db_path: Optional[Path] = None,
                              limit: int = 100) -> Dict[str, int]:
    """Survey: what coverage_categories actually exist + how many rows per.

    Useful before running orchestrators to know which categories are
    populated on each side. Slow on first call (full scan), but
    sqlite3 caches if you run it twice in the same process.
    """
    import collections
    table = "english_words" if language == "en" else "russian_words"
    conn = _connect_readonly(db_path)
    counter: collections.Counter = collections.Counter()
    try:
        for (cats_json,) in conn.execute(
                f"SELECT coverage_categories FROM {table}"):
            try:
                cats = json.loads(cats_json or "[]")
            except Exception:  # noqa: BLE001
                continue
            for c in cats:
                counter[c] += 1
    finally:
        conn.close()
    return dict(counter.most_common(limit))


def main_db_stats(db_path: Optional[Path] = None) -> Dict[str, Any]:
    """Cheap summary of main's bilingual_links.sqlite content."""
    conn = _connect_readonly(db_path)
    try:
        return {
            "english_words": conn.execute(
                "SELECT COUNT(*) FROM english_words").fetchone()[0],
            "russian_words": conn.execute(
                "SELECT COUNT(*) FROM russian_words").fetchone()[0],
            "pack_manifests": conn.execute(
                "SELECT COUNT(*) FROM pack_manifests").fetchone()[0],
            "db_path": str(db_path or MAIN_DB_PATH),
        }
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    print(json.dumps(main_db_stats(), indent=2))
    print()
    print("=== Top 20 EN coverage_categories ===")
    print(json.dumps(list_coverage_categories("en", limit=20),
                     indent=2, ensure_ascii=False))
    print()
    print("=== Top 20 RU coverage_categories ===")
    print(json.dumps(list_coverage_categories("ru", limit=20),
                     indent=2, ensure_ascii=False))
