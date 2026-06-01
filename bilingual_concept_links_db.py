"""Concept-link DB for main's 1M+1M corpus.

A SEPARATE sqlite file (`D:/SurgeApp/bilingual_concept_links.sqlite`) that
holds the concept-linking layer for production EN/RU vocab. Schema mirrors
the worktree's bilingual_stack/bilingual_links.sqlite so the orchestrators
can write rows in the same shape.

Why separate?
=============
Main's `D:/SurgeApp/bilingual_links.sqlite` (1.05 GB) holds the flat
production vocab (1M EN + 1M RU words). Adding 4 new tables + millions of
concept rows to a 1.05 GB file is doable but RISKY: every write incurs WAL
+ checkpoint cost on the whole file, and an accidental schema corruption
could destroy the 2M production rows. Putting concept-links in a separate
file is safer (production DB stays read-only-from-orchestrators) AND
faster (concept DB stays small, WAL is cheap, can be backed up
independently).

Schema (matches worktree's bilingual_stack/bilingual_links.sqlite v1)
=====================================================================
- concepts        : (concept_id, canonical_label_en, canonical_label_ru,
                    coverage_categories_json, domain_tags_json,
                    register_tags_json, safety_tags_json,
                    created_at, updated_at, notes)
- entry_links     : (link_id, concept_id, language, source_store,
                    source_entry_id, source_word, source_phrase,
                    lemma, part_of_speech, confidence, link_method,
                    created_at, notes)
- bilingual_glossary_links : (glossary_id, concept_id, english_text,
                              russian_text, english_entry_id,
                              russian_entry_id, relation_type,
                              confidence, source, created_at, notes)
- link_audit      : (audit_id, action, concept_id, link_id, status,
                    message, created_at)

Plus indexes for 1M-scale lookups (the worktree DB doesn't index because
3942 concepts fit fine without it; at 1M+ we need real indexes).
"""
from __future__ import annotations

import json
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

CONCEPT_DB_PATH = Path(__file__).resolve().parent / "bilingual_concept_links.sqlite"

# Valid link_method values per Phase 22 spec (preserved here so callers
# can validate locally without a worktree import).
VALID_LINK_METHODS = frozenset({
    "manual", "exact_match", "lemma_match", "domain_category_match",
    "glossary_import", "heuristic", "evaluation_fixture",
})


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _new_id(prefix: str) -> str:
    """Compact unique id — matches the worktree's _new_id format."""
    return f"{prefix}_{secrets.token_hex(8)}"


def init_concept_db(db_path: Optional[Path] = None) -> str:
    """Create the concept-link DB + indexes if missing. Idempotent."""
    p = Path(db_path) if db_path else CONCEPT_DB_PATH
    conn = sqlite3.connect(str(p), timeout=15.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS concepts (
            concept_id              TEXT PRIMARY KEY,
            canonical_label_en      TEXT NOT NULL,
            canonical_label_ru      TEXT NOT NULL DEFAULT '',
            coverage_categories_json TEXT NOT NULL DEFAULT '[]',
            domain_tags_json        TEXT NOT NULL DEFAULT '[]',
            register_tags_json      TEXT NOT NULL DEFAULT '[]',
            safety_tags_json        TEXT NOT NULL DEFAULT '[]',
            created_at              TEXT NOT NULL,
            updated_at              TEXT NOT NULL,
            notes                   TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS ix_concepts_label_en
            ON concepts(canonical_label_en);
        CREATE INDEX IF NOT EXISTS ix_concepts_label_ru
            ON concepts(canonical_label_ru);

        CREATE TABLE IF NOT EXISTS entry_links (
            link_id           TEXT PRIMARY KEY,
            concept_id        TEXT NOT NULL,
            language          TEXT NOT NULL,
            source_store      TEXT NOT NULL DEFAULT '',
            source_entry_id   TEXT NOT NULL DEFAULT '',
            source_word       TEXT NOT NULL DEFAULT '',
            source_phrase     TEXT NOT NULL DEFAULT '',
            lemma             TEXT NOT NULL DEFAULT '',
            part_of_speech    TEXT NOT NULL DEFAULT '',
            confidence        REAL NOT NULL DEFAULT 0.5,
            link_method       TEXT NOT NULL DEFAULT 'manual',
            created_at        TEXT NOT NULL,
            notes             TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS ix_entry_links_concept
            ON entry_links(concept_id);
        CREATE INDEX IF NOT EXISTS ix_entry_links_lang_word
            ON entry_links(language, source_word);

        CREATE TABLE IF NOT EXISTS bilingual_glossary_links (
            glossary_id        TEXT PRIMARY KEY,
            concept_id         TEXT NOT NULL DEFAULT '',
            english_text       TEXT NOT NULL DEFAULT '',
            russian_text       TEXT NOT NULL DEFAULT '',
            english_entry_id   TEXT NOT NULL DEFAULT '',
            russian_entry_id   TEXT NOT NULL DEFAULT '',
            relation_type      TEXT NOT NULL DEFAULT 'translation',
            confidence         REAL NOT NULL DEFAULT 0.5,
            source             TEXT NOT NULL DEFAULT 'manual',
            created_at         TEXT NOT NULL,
            notes              TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS ix_glossary_concept
            ON bilingual_glossary_links(concept_id);

        CREATE TABLE IF NOT EXISTS link_audit (
            audit_id    TEXT PRIMARY KEY,
            action      TEXT NOT NULL,
            concept_id  TEXT NOT NULL DEFAULT '',
            link_id     TEXT NOT NULL DEFAULT '',
            status      TEXT NOT NULL DEFAULT 'ok',
            message     TEXT NOT NULL DEFAULT '',
            created_at  TEXT NOT NULL
        );
        """)
        conn.commit()
    finally:
        conn.close()
    return str(p)


def stats(db_path: Optional[Path] = None) -> Dict[str, Any]:
    """Read-only counts of rows in each table."""
    p = Path(db_path) if db_path else CONCEPT_DB_PATH
    if not p.exists():
        return {"ok": False, "reason": "db_missing", "path": str(p)}
    conn = sqlite3.connect(str(p), timeout=10.0)
    try:
        return {
            "ok": True,
            "path": str(p),
            "size_mb": round(p.stat().st_size / (1024 * 1024), 2),
            "concepts": conn.execute("SELECT COUNT(*) FROM concepts").fetchone()[0],
            "entry_links": conn.execute("SELECT COUNT(*) FROM entry_links").fetchone()[0],
            "bilingual_glossary_links": conn.execute(
                "SELECT COUNT(*) FROM bilingual_glossary_links").fetchone()[0],
            "link_audit": conn.execute("SELECT COUNT(*) FROM link_audit").fetchone()[0],
        }
    finally:
        conn.close()


def existing_linked_words(db_path: Optional[Path] = None
                           ) -> "tuple[set[str], set[str]]":
    """Return (en_seen_lowercased, ru_seen_lowercased) for dedup-on-insert."""
    p = Path(db_path) if db_path else CONCEPT_DB_PATH
    if not p.exists():
        return (set(), set())
    en_seen: set[str] = set()
    ru_seen: set[str] = set()
    conn = sqlite3.connect(str(p), timeout=10.0)
    try:
        for lang, word in conn.execute(
                "SELECT language, source_word FROM entry_links"):
            w = (word or "").strip().lower()
            if not w:
                continue
            if lang == "en":
                en_seen.add(w)
            elif lang == "ru":
                ru_seen.add(w)
    finally:
        conn.close()
    return (en_seen, ru_seen)


def insert_concept_batch(
    concept_rows: Iterable[tuple],
    link_rows: Iterable[tuple],
    audit_message: str = "",
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Single-transaction batch insert. Returns inserted counts.

    Each ``concept_rows`` tuple is (concept_id, en_label, ru_label,
    coverage_json, domain_json, register_json, safety_json, created_at,
    updated_at, notes). Each ``link_rows`` tuple is (link_id, concept_id,
    language, source_store, source_entry_id, source_word, source_phrase,
    lemma, part_of_speech, confidence, link_method, created_at, notes).
    """
    p = Path(db_path) if db_path else CONCEPT_DB_PATH
    init_concept_db(p)
    concept_rows = list(concept_rows)
    link_rows = list(link_rows)
    conn = sqlite3.connect(str(p), timeout=30.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
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
        conn.execute(
            "INSERT INTO link_audit (audit_id, action, concept_id, link_id, "
            "status, message, created_at) VALUES (?,?,?,?,?,?,?)",
            (_new_id("audit"), "batch_insert", "", "", "ok",
             f"concepts={len(concept_rows)} links={len(link_rows)}"
             + (f" {audit_message}" if audit_message else ""),
             _now()))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    return {
        "ok": True,
        "inserted_concepts": len(concept_rows),
        "inserted_entry_links": len(link_rows),
        "audit_message": audit_message,
    }


def normalize_register(register: Any) -> str:
    """Clamp register to the canonical Phase 22 string set, return ""."""
    if isinstance(register, list) and register:
        return str(register[0])
    if isinstance(register, str):
        return register
    return ""


def clamp_confidence(c: Any) -> float:
    try:
        v = float(c)
    except Exception:  # noqa: BLE001
        return 0.5
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def normalize_method(m: Any) -> str:
    m_str = str(m or "manual")
    if m_str in VALID_LINK_METHODS:
        return m_str
    return "heuristic"


def json_list(items: Any) -> str:
    """Best-effort: render to a JSON list string."""
    try:
        if items is None:
            return "[]"
        if isinstance(items, str):
            # if already JSON, pass through; else wrap
            try:
                parsed = json.loads(items)
                if isinstance(parsed, list):
                    return items
            except Exception:  # noqa: BLE001
                pass
            return json.dumps([items], ensure_ascii=False)
        return json.dumps(list(items), ensure_ascii=False)
    except Exception:  # noqa: BLE001
        return "[]"


if __name__ == "__main__":
    path = init_concept_db()
    print(f"initialized concept DB at: {path}")
    print(json.dumps(stats(), indent=2))
