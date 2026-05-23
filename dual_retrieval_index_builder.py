"""Phase 19 - Dual Retrieval Index Builder.

Builds SQLite indexes (and optionally FTS5 virtual tables) for the English
and Russian sovereign lexicon stores. All builds and all queries are
bounded. No daemon, no scheduler, no auto-runner, no main-runtime hook.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional


_DEFAULT_LIMIT = 25
_HARD_LIMIT = 200


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def _en_connect(db_path: Optional[str | Path] = None):
    import cognitive_lexicon_store as enlex
    enlex.init_db(db_path)
    return enlex._connect(db_path)


def _ru_connect(db_path: Optional[str | Path] = None):
    import russian_lexicon_store as rulex
    rulex.init_db(db_path)
    return rulex._connect(db_path)


# -------------------- FTS5 detection --------------------

def detect_sqlite_fts5_support() -> dict[str, Any]:
    """Probe whether the runtime SQLite has FTS5 compiled in."""
    try:
        conn = sqlite3.connect(":memory:")
    except Exception as e:
        return {"ok": False, "fts5_available": False,
                "reason": f"sqlite_connect_failed: {e}"}
    try:
        try:
            conn.execute("CREATE VIRTUAL TABLE _t USING fts5(x)")
            conn.execute("DROP TABLE _t")
            return {"ok": True, "fts5_available": True}
        except Exception as e:
            return {"ok": True, "fts5_available": False, "reason": str(e)}
    finally:
        conn.close()


# -------------------- Normal index ensure --------------------

_EN_INDEXES = (
    ("ix_words_pack_id", "words", "pack_id"),
    ("ix_words_pack_source", "words", "pack_source"),
    ("ix_words_word_lc", "words", "LOWER(word)"),
    ("ix_words_language", "words", "language"),
)

_RU_INDEXES = (
    ("ix_words_pack_id", "words", "pack_id"),
    ("ix_words_pack_source", "words", "pack_source"),
    ("ix_words_word_lc", "words", "LOWER(word)"),
    ("ix_words_lemma", "words", "lemma"),
    ("ix_phrases_pack_id", "phrases", "pack_id"),
    ("ix_phrases_phrase_lc", "phrases", "LOWER(phrase)"),
)


def _create_indexes(conn: sqlite3.Connection,
                    specs) -> list[str]:
    created: list[str] = []
    for name, table, col in specs:
        try:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {name} ON {table}({col})")
            created.append(name)
        except Exception:
            pass
    return created


def ensure_english_indexes(db_path: Optional[str | Path] = None
                           ) -> dict[str, Any]:
    _ensure_flags()
    with _en_connect(db_path) as conn:
        created = _create_indexes(conn, _EN_INDEXES)
        existing = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
    return {"ok": True, "created_or_existing": created,
            "all_indexes": existing}


def ensure_russian_indexes(db_path: Optional[str | Path] = None
                           ) -> dict[str, Any]:
    _ensure_flags()
    with _ru_connect(db_path) as conn:
        created = _create_indexes(conn, _RU_INDEXES)
        existing = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
    return {"ok": True, "created_or_existing": created,
            "all_indexes": existing}


def ensure_manifest_indexes(db_path: Optional[str | Path] = None,
                            lang: str = "en") -> dict[str, Any]:
    """Ensure the pack/manifest related indexes exist (delegates to lang)."""
    return (ensure_english_indexes(db_path) if lang == "en"
            else ensure_russian_indexes(db_path))


# -------------------- FTS5 build / fallback --------------------

def _build_fts(lang: str, conn: sqlite3.Connection,
               limit: Optional[int], rebuild: bool) -> dict[str, Any]:
    """Try to build an FTS5 virtual table that mirrors the words table.

    If FTS5 is unavailable, create a plain fallback search table and
    populate it (bounded by ``limit``).
    """
    fts5 = detect_sqlite_fts5_support()
    fts_name = f"words_fts_{lang}"
    fb_name = f"words_search_fallback_{lang}"
    if fts5.get("fts5_available"):
        if rebuild:
            conn.execute(f"DROP TABLE IF EXISTS {fts_name}")
        try:
            conn.execute(
                f"CREATE VIRTUAL TABLE IF NOT EXISTS {fts_name} "
                f"USING fts5(word, definition, tokenize='unicode61')")
        except Exception as e:
            return {"ok": False, "error": f"fts5_create_failed: {e}"}
        defn_col = "definition" if lang == "en" else "definition_ru"
        try:
            conn.execute(f"DELETE FROM {fts_name}")
        except Exception:
            pass
        sql = (f"INSERT INTO {fts_name}(word, definition) "
               f"SELECT word, COALESCE({defn_col}, '') FROM words")
        if limit is not None:
            sql += f" LIMIT {int(max(1, limit))}"
        conn.execute(sql)
        n = conn.execute(f"SELECT COUNT(*) FROM {fts_name}").fetchone()[0]
        return {"ok": True, "fts5_used": True, "table": fts_name,
                "indexed_rows": int(n or 0)}
    # Fallback: plain table + LOWER index
    if rebuild:
        conn.execute(f"DROP TABLE IF EXISTS {fb_name}")
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {fb_name} ("
        "rowid INTEGER PRIMARY KEY, word TEXT, definition TEXT)")
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS ix_{fb_name}_word "
        f"ON {fb_name}(LOWER(word))")
    defn_col = "definition" if lang == "en" else "definition_ru"
    sql = (f"INSERT INTO {fb_name}(rowid, word, definition) "
           f"SELECT rowid, word, COALESCE({defn_col}, '') FROM words")
    if limit is not None:
        sql += f" LIMIT {int(max(1, limit))}"
    conn.execute(f"DELETE FROM {fb_name}")
    conn.execute(sql)
    n = conn.execute(f"SELECT COUNT(*) FROM {fb_name}").fetchone()[0]
    return {"ok": True, "fts5_used": False, "table": fb_name,
            "indexed_rows": int(n or 0)}


def build_english_fts_index(rebuild: bool = False,
                            limit: Optional[int] = None,
                            db_path: Optional[str | Path] = None
                            ) -> dict[str, Any]:
    _ensure_flags()
    with _en_connect(db_path) as conn:
        return _build_fts("en", conn, limit, rebuild)


def build_russian_fts_index(rebuild: bool = False,
                            limit: Optional[int] = None,
                            db_path: Optional[str | Path] = None
                            ) -> dict[str, Any]:
    _ensure_flags()
    with _ru_connect(db_path) as conn:
        return _build_fts("ru", conn, limit, rebuild)


def rebuild_all_indexes(limit: Optional[int] = None,
                        en_db_path: Optional[str | Path] = None,
                        ru_db_path: Optional[str | Path] = None
                        ) -> dict[str, Any]:
    return {
        "en_normal": ensure_english_indexes(en_db_path),
        "ru_normal": ensure_russian_indexes(ru_db_path),
        "en_fts": build_english_fts_index(rebuild=True, limit=limit,
                                          db_path=en_db_path),
        "ru_fts": build_russian_fts_index(rebuild=True, limit=limit,
                                          db_path=ru_db_path),
    }


# -------------------- Bounded queries --------------------

def _clamp_limit(n: Optional[int]) -> int:
    if n is None:
        return _DEFAULT_LIMIT
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_LIMIT
    return max(1, min(v, _HARD_LIMIT))


def _query_via_fts_or_fallback(conn: sqlite3.Connection, lang: str,
                               needle: str, limit: int) -> list[dict[str, Any]]:
    fts5 = detect_sqlite_fts5_support()
    n = _clamp_limit(limit)
    pat = "%" + (needle or "").strip().lower().replace("%", "").replace("_", "") + "%"
    if fts5.get("fts5_available"):
        try:
            fts_name = f"words_fts_{lang}"
            rows = conn.execute(
                f"SELECT w.* FROM {fts_name} f JOIN words w ON w.word=f.word "
                f"WHERE {fts_name} MATCH ? LIMIT ?",
                ((needle or "").strip(), n)).fetchall()
            if rows:
                return [dict(r) for r in rows]
        except Exception:
            pass
    rows = conn.execute(
        "SELECT * FROM words WHERE LOWER(word) LIKE ? "
        "ORDER BY frequency_score DESC, word ASC LIMIT ?",
        (pat, n)).fetchall()
    return [dict(r) for r in rows]


def _row_factory_dict(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row


def query_english_index(query: str, limit: int = _DEFAULT_LIMIT,
                        filters: Optional[dict[str, Any]] = None,
                        db_path: Optional[str | Path] = None
                        ) -> list[dict[str, Any]]:
    _ensure_flags()
    n = _clamp_limit(limit)
    with _en_connect(db_path) as conn:
        _row_factory_dict(conn)
        results = _query_via_fts_or_fallback(conn, "en", query, n)
    return results[:n]


def query_russian_index(query: str, limit: int = _DEFAULT_LIMIT,
                        filters: Optional[dict[str, Any]] = None,
                        db_path: Optional[str | Path] = None
                        ) -> list[dict[str, Any]]:
    _ensure_flags()
    n = _clamp_limit(limit)
    with _ru_connect(db_path) as conn:
        _row_factory_dict(conn)
        results = _query_via_fts_or_fallback(conn, "ru", query, n)
    return results[:n]


def _query_by_json_tag(conn: sqlite3.Connection, table: str,
                       column: str, tag: str,
                       limit: int) -> list[dict[str, Any]]:
    pat = f'%"{tag}"%'
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE {column} LIKE ? LIMIT ?",
        (pat, limit)).fetchall()
    return [dict(r) for r in rows]


def query_by_category(language: str, coverage_category: str,
                      limit: int = _DEFAULT_LIMIT,
                      db_path: Optional[str | Path] = None
                      ) -> list[dict[str, Any]]:
    _ensure_flags()
    n = _clamp_limit(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        _row_factory_dict(conn)
        return _query_by_json_tag(conn, "words",
                                  "coverage_categories_json",
                                  coverage_category, n)


def query_by_register(language: str, register_tag: str,
                      limit: int = _DEFAULT_LIMIT,
                      db_path: Optional[str | Path] = None
                      ) -> list[dict[str, Any]]:
    _ensure_flags()
    n = _clamp_limit(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        _row_factory_dict(conn)
        return _query_by_json_tag(conn, "words",
                                  "register_tags_json", register_tag, n)


def query_by_safety(language: str, safety_tag: str,
                    limit: int = _DEFAULT_LIMIT,
                    db_path: Optional[str | Path] = None
                    ) -> list[dict[str, Any]]:
    _ensure_flags()
    n = _clamp_limit(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        _row_factory_dict(conn)
        return _query_by_json_tag(conn, "words",
                                  "safety_tags_json", safety_tag, n)


def index_health_report(en_db_path: Optional[str | Path] = None,
                        ru_db_path: Optional[str | Path] = None
                        ) -> dict[str, Any]:
    _ensure_flags()
    fts = detect_sqlite_fts5_support()
    with _en_connect(en_db_path) as enconn:
        en_idx = [r[0] for r in enconn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
        en_words = enconn.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        en_fts_count = 0
        try:
            en_fts_count = enconn.execute(
                "SELECT COUNT(*) FROM words_fts_en").fetchone()[0]
        except Exception:
            pass
    with _ru_connect(ru_db_path) as ruconn:
        ru_idx = [r[0] for r in ruconn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
        ru_words = ruconn.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        try:
            ru_phrases = ruconn.execute(
                "SELECT COUNT(*) FROM phrases").fetchone()[0]
        except Exception:
            ru_phrases = 0
        ru_fts_count = 0
        try:
            ru_fts_count = ruconn.execute(
                "SELECT COUNT(*) FROM words_fts_ru").fetchone()[0]
        except Exception:
            pass
    return {
        "ok": True,
        "fts5_available": fts.get("fts5_available", False),
        "generated_at": time.time(),
        "english": {"indexes": en_idx, "word_count": int(en_words or 0),
                    "fts_indexed_rows": int(en_fts_count or 0)},
        "russian": {"indexes": ru_idx, "word_count": int(ru_words or 0),
                    "phrase_count": int(ru_phrases or 0),
                    "fts_indexed_rows": int(ru_fts_count or 0)},
    }


def write_index_report(report: dict[str, Any],
                       output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "detect_sqlite_fts5_support",
    "ensure_english_indexes",
    "ensure_russian_indexes",
    "ensure_manifest_indexes",
    "build_english_fts_index",
    "build_russian_fts_index",
    "rebuild_all_indexes",
    "query_english_index",
    "query_russian_index",
    "query_by_category",
    "query_by_register",
    "query_by_safety",
    "index_health_report",
    "write_index_report",
]
