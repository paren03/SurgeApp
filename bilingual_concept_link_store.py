"""Phase 22 - Bilingual Concept Link Store.

A SEPARATE SQLite database that maps English entries to Russian entries
via "concepts". Never merges the EN/RU lexicons. All queries bounded.
No daemon, no scheduler, no internet.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Optional


DEFAULT_LINK_DB = Path("bilingual_stack/bilingual_links.sqlite")


VALID_LINK_METHODS = (
    "manual",
    "exact_match",
    "lemma_match",
    "domain_category_match",
    "glossary_import",
    "heuristic",
    "evaluation_fixture",
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS concepts (
    concept_id              TEXT PRIMARY KEY,
    canonical_label_en      TEXT NOT NULL DEFAULT '',
    canonical_label_ru      TEXT NOT NULL DEFAULT '',
    coverage_categories_json TEXT NOT NULL DEFAULT '[]',
    domain_tags_json        TEXT NOT NULL DEFAULT '[]',
    register_tags_json      TEXT NOT NULL DEFAULT '[]',
    safety_tags_json        TEXT NOT NULL DEFAULT '[]',
    created_at              REAL NOT NULL,
    updated_at              REAL NOT NULL,
    notes                   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_concepts_label_en ON concepts(LOWER(canonical_label_en));
CREATE INDEX IF NOT EXISTS ix_concepts_label_ru ON concepts(canonical_label_ru);

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
    created_at        REAL NOT NULL,
    notes             TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_entry_links_concept ON entry_links(concept_id);
CREATE INDEX IF NOT EXISTS ix_entry_links_lang    ON entry_links(language);
CREATE INDEX IF NOT EXISTS ix_entry_links_word_lc ON entry_links(LOWER(source_word));

CREATE TABLE IF NOT EXISTS bilingual_glossary_links (
    glossary_id        TEXT PRIMARY KEY,
    concept_id         TEXT NOT NULL,
    english_text       TEXT NOT NULL DEFAULT '',
    russian_text       TEXT NOT NULL DEFAULT '',
    english_entry_id   TEXT NOT NULL DEFAULT '',
    russian_entry_id   TEXT NOT NULL DEFAULT '',
    relation_type      TEXT NOT NULL DEFAULT 'translation',
    confidence         REAL NOT NULL DEFAULT 0.5,
    source             TEXT NOT NULL DEFAULT 'manual',
    created_at         REAL NOT NULL,
    notes              TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_gloss_concept ON bilingual_glossary_links(concept_id);
CREATE INDEX IF NOT EXISTS ix_gloss_en      ON bilingual_glossary_links(LOWER(english_text));
CREATE INDEX IF NOT EXISTS ix_gloss_ru      ON bilingual_glossary_links(russian_text);

CREATE TABLE IF NOT EXISTS link_audit (
    audit_id    TEXT PRIMARY KEY,
    action      TEXT NOT NULL,
    concept_id  TEXT NOT NULL DEFAULT '',
    link_id     TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'ok',
    message     TEXT NOT NULL DEFAULT '',
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_audit_action ON link_audit(action);
CREATE INDEX IF NOT EXISTS ix_audit_concept ON link_audit(concept_id);
"""


def _connect(db_path: Optional[str | Path]) -> sqlite3.Connection:
    p = Path(db_path) if db_path is not None else DEFAULT_LINK_DB
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_bilingual_link_db(db_path: Optional[str | Path] = None) -> str:
    p = Path(db_path) if db_path is not None else DEFAULT_LINK_DB
    conn = _connect(p)
    try:
        for stmt in [s.strip() for s in SCHEMA.split(";") if s.strip()]:
            conn.execute(stmt)
    finally:
        conn.close()
    return str(p)


def _clamp_confidence(c: Any) -> float:
    try:
        v = float(c)
    except Exception:
        return 0.5
    return max(0.0, min(1.0, v))


def _normalize_method(method: str) -> str:
    m = (method or "manual").strip().lower()
    return m if m in VALID_LINK_METHODS else "manual"


def _json_list(it: Optional[Iterable[str]]) -> str:
    return json.dumps(sorted({str(x) for x in (it or []) if x}),
                       ensure_ascii=False)


def _now() -> float:
    return time.time()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def write_link_audit(action: str, concept_id: Optional[str] = None,
                     link_id: Optional[str] = None, status: str = "ok",
                     message: str = "",
                     db_path: Optional[str | Path] = None
                     ) -> dict[str, Any]:
    init_bilingual_link_db(db_path)
    aid = _new_id("audit")
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO link_audit "
            "(audit_id, action, concept_id, link_id, status, message, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (aid, str(action), str(concept_id or ""), str(link_id or ""),
             str(status or "ok"), str(message or ""), _now()))
    finally:
        conn.close()
    return {"ok": True, "audit_id": aid}


def create_concept(canonical_label_en: str,
                   canonical_label_ru: str = "",
                   coverage_categories: Optional[Iterable[str]] = None,
                   domain_tags: Optional[Iterable[str]] = None,
                   register_tags: Optional[Iterable[str]] = None,
                   safety_tags: Optional[Iterable[str]] = None,
                   notes: str = "",
                   db_path: Optional[str | Path] = None
                   ) -> dict[str, Any]:
    init_bilingual_link_db(db_path)
    cid = _new_id("concept")
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO concepts (concept_id, canonical_label_en, canonical_label_ru, "
            " coverage_categories_json, domain_tags_json, register_tags_json, "
            " safety_tags_json, created_at, updated_at, notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (cid, str(canonical_label_en or ""),
             str(canonical_label_ru or ""),
             _json_list(coverage_categories),
             _json_list(domain_tags),
             _json_list(register_tags),
             _json_list(safety_tags),
             now, now, str(notes or "")))
    finally:
        conn.close()
    write_link_audit("create_concept", concept_id=cid, db_path=db_path)
    return {"ok": True, "concept_id": cid}


def add_entry_link(concept_id: str, language: str,
                   source_store: str = "",
                   source_entry_id: Optional[str] = None,
                   source_word: str = "",
                   source_phrase: str = "",
                   lemma: str = "",
                   part_of_speech: str = "",
                   confidence: float = 0.5,
                   link_method: str = "manual",
                   notes: str = "",
                   db_path: Optional[str | Path] = None
                   ) -> dict[str, Any]:
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    init_bilingual_link_db(db_path)
    lid = _new_id("link")
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO entry_links (link_id, concept_id, language, "
            " source_store, source_entry_id, source_word, source_phrase, "
            " lemma, part_of_speech, confidence, link_method, created_at, "
            " notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (lid, str(concept_id), language, str(source_store or ""),
             str(source_entry_id or ""), str(source_word or ""),
             str(source_phrase or ""), str(lemma or ""),
             str(part_of_speech or ""),
             _clamp_confidence(confidence),
             _normalize_method(link_method), _now(),
             str(notes or "")))
    finally:
        conn.close()
    write_link_audit("add_entry_link", concept_id=concept_id,
                     link_id=lid, db_path=db_path)
    return {"ok": True, "link_id": lid}


def add_glossary_link(concept_id: str, english_text: str = "",
                      russian_text: str = "",
                      english_entry_id: Optional[str] = None,
                      russian_entry_id: Optional[str] = None,
                      relation_type: str = "translation",
                      confidence: float = 0.5,
                      source: str = "manual",
                      notes: str = "",
                      db_path: Optional[str | Path] = None
                      ) -> dict[str, Any]:
    init_bilingual_link_db(db_path)
    gid = _new_id("gloss")
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO bilingual_glossary_links "
            "(glossary_id, concept_id, english_text, russian_text, "
            " english_entry_id, russian_entry_id, relation_type, "
            " confidence, source, created_at, notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (gid, str(concept_id), str(english_text or ""),
             str(russian_text or ""), str(english_entry_id or ""),
             str(russian_entry_id or ""), str(relation_type or "translation"),
             _clamp_confidence(confidence), str(source or "manual"),
             _now(), str(notes or "")))
    finally:
        conn.close()
    write_link_audit("add_glossary_link", concept_id=concept_id,
                     link_id=gid, db_path=db_path)
    return {"ok": True, "glossary_id": gid}


_CONCEPT_FIELDS = ("concept_id", "canonical_label_en", "canonical_label_ru",
                   "coverage_categories_json", "domain_tags_json",
                   "register_tags_json", "safety_tags_json",
                   "created_at", "updated_at", "notes")


_ENTRY_LINK_FIELDS = ("link_id", "concept_id", "language", "source_store",
                      "source_entry_id", "source_word", "source_phrase",
                      "lemma", "part_of_speech", "confidence",
                      "link_method", "created_at", "notes")


_GLOSS_FIELDS = ("glossary_id", "concept_id", "english_text", "russian_text",
                 "english_entry_id", "russian_entry_id", "relation_type",
                 "confidence", "source", "created_at", "notes")


def _row_to_concept(row: tuple) -> dict[str, Any]:
    d = dict(zip(_CONCEPT_FIELDS, row))
    for f in ("coverage_categories_json", "domain_tags_json",
              "register_tags_json", "safety_tags_json"):
        try:
            d[f.replace("_json", "")] = json.loads(d[f]) if d.get(f) else []
        except Exception:
            d[f.replace("_json", "")] = []
    return d


def _row_to_entry_link(row: tuple) -> dict[str, Any]:
    return dict(zip(_ENTRY_LINK_FIELDS, row))


def _row_to_gloss(row: tuple) -> dict[str, Any]:
    return dict(zip(_GLOSS_FIELDS, row))


def get_concept(concept_id: str,
                db_path: Optional[str | Path] = None
                ) -> Optional[dict[str, Any]]:
    init_bilingual_link_db(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT " + ",".join(_CONCEPT_FIELDS)
            + " FROM concepts WHERE concept_id=?", (concept_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    return _row_to_concept(row) if row else None


def _clamp_limit(n: Optional[int], default: int = 25,
                 hard: int = 500) -> int:
    if n is None:
        return default
    try:
        v = int(n)
    except Exception:
        return default
    return max(1, min(v, hard))


def find_concepts_by_label(text: str, language: Optional[str] = None,
                           limit: int = 25,
                           db_path: Optional[str | Path] = None
                           ) -> list[dict[str, Any]]:
    init_bilingual_link_db(db_path)
    cap = _clamp_limit(limit)
    needle = (text or "").strip()
    if not needle:
        return []
    pat = "%" + needle.lower().replace("%", "").replace("_", "") + "%"
    conn = _connect(db_path)
    try:
        if language == "en":
            cur = conn.execute(
                "SELECT " + ",".join(_CONCEPT_FIELDS)
                + " FROM concepts WHERE LOWER(canonical_label_en) LIKE ? "
                "ORDER BY canonical_label_en ASC LIMIT ?", (pat, cap))
        elif language == "ru":
            cur = conn.execute(
                "SELECT " + ",".join(_CONCEPT_FIELDS)
                + " FROM concepts WHERE canonical_label_ru LIKE ? "
                "ORDER BY canonical_label_ru ASC LIMIT ?", (pat, cap))
        else:
            cur = conn.execute(
                "SELECT " + ",".join(_CONCEPT_FIELDS)
                + " FROM concepts "
                "WHERE LOWER(canonical_label_en) LIKE ? "
                "   OR canonical_label_ru LIKE ? "
                "ORDER BY canonical_label_en ASC LIMIT ?",
                (pat, pat, cap))
        rows = cur.fetchall()
    finally:
        conn.close()
    return [_row_to_concept(r) for r in rows]


def get_links_for_concept(concept_id: str, limit: int = 50,
                          db_path: Optional[str | Path] = None
                          ) -> list[dict[str, Any]]:
    init_bilingual_link_db(db_path)
    cap = _clamp_limit(limit, default=50)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT " + ",".join(_ENTRY_LINK_FIELDS)
            + " FROM entry_links WHERE concept_id=? "
            "ORDER BY created_at ASC LIMIT ?", (concept_id, cap))
        rows = cur.fetchall()
    finally:
        conn.close()
    return [_row_to_entry_link(r) for r in rows]


def get_bilingual_pairs(concept_id: str, limit: int = 50,
                        db_path: Optional[str | Path] = None
                        ) -> list[dict[str, Any]]:
    init_bilingual_link_db(db_path)
    cap = _clamp_limit(limit, default=50)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT " + ",".join(_GLOSS_FIELDS)
            + " FROM bilingual_glossary_links WHERE concept_id=? "
            "ORDER BY created_at ASC LIMIT ?", (concept_id, cap))
        rows = cur.fetchall()
    finally:
        conn.close()
    return [_row_to_gloss(r) for r in rows]


def find_concepts_by_entry_word(word: str,
                                 language: Optional[str] = None,
                                 limit: int = 25,
                                 db_path: Optional[str | Path] = None
                                 ) -> list[dict[str, Any]]:
    """Look up concepts via the entry_links table (matches the row's
    `source_word`). Bridges the gap between canonical-label lookup and
    actual source-word values."""
    init_bilingual_link_db(db_path)
    cap = _clamp_limit(limit)
    needle = (word or "").strip().lower()
    if not needle:
        return []
    clean = needle.replace("%", "").replace("_", " ")
    pat = "%" + clean + "%"
    conn = _connect(db_path)
    try:
        # Bidirectional containment: either the candidate word contains the
        # stored source_word (e.g. "civil_engineer" contains "engineer") or
        # the stored source_word contains the candidate word.
        if language in ("en", "ru"):
            cur = conn.execute(
                "SELECT DISTINCT concept_id FROM entry_links "
                "WHERE language=? AND ("
                "  LOWER(source_word)=? "
                "  OR LOWER(source_word) LIKE ? "
                "  OR INSTR(?, LOWER(source_word)) > 0"
                ") LIMIT ?",
                (language, needle, pat, clean, cap))
        else:
            cur = conn.execute(
                "SELECT DISTINCT concept_id FROM entry_links "
                "WHERE LOWER(source_word)=? "
                "   OR LOWER(source_word) LIKE ? "
                "   OR INSTR(?, LOWER(source_word)) > 0 "
                "LIMIT ?", (needle, pat, clean, cap))
        ids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for cid in ids[:cap]:
        c = get_concept(cid, db_path=db_path)
        if c:
            out.append(c)
    return out


def list_concepts(limit: int = 100,
                  db_path: Optional[str | Path] = None
                  ) -> list[dict[str, Any]]:
    init_bilingual_link_db(db_path)
    cap = _clamp_limit(limit, default=100)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT " + ",".join(_CONCEPT_FIELDS)
            + " FROM concepts ORDER BY created_at DESC LIMIT ?", (cap,))
        rows = cur.fetchall()
    finally:
        conn.close()
    return [_row_to_concept(r) for r in rows]


def bounded_query(sql: str, params: Optional[Iterable[Any]] = None,
                  limit: int = 100,
                  db_path: Optional[str | Path] = None
                  ) -> list[tuple]:
    """Read-only query that enforces a LIMIT clause if missing."""
    init_bilingual_link_db(db_path)
    s = (sql or "").strip()
    if not s.lower().startswith("select"):
        return []
    if " limit " not in s.lower():
        s = s + f" LIMIT {_clamp_limit(limit, default=100)}"
    conn = _connect(db_path)
    try:
        cur = conn.execute(s, tuple(params or ()))
        return cur.fetchall()
    finally:
        conn.close()


__all__ = [
    "DEFAULT_LINK_DB",
    "VALID_LINK_METHODS",
    "init_bilingual_link_db",
    "create_concept",
    "add_entry_link",
    "add_glossary_link",
    "get_concept",
    "find_concepts_by_label",
    "get_links_for_concept",
    "get_bilingual_pairs",
    "list_concepts",
    "write_link_audit",
    "bounded_query",
]
