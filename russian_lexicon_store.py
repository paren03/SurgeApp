"""Russian Sovereign Language Stack — SQLite lexicon store.

Bounded, local, inspectable. Ready for future near-million-word import via
chunked ingestion. Never loads the full DB into memory.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional

FEATURE_FLAG = "LUNA_RUSSIAN_STACK"

DEFAULT_LIMIT = 25
HARD_MAX_LIMIT = 500

_DEFAULT_DB = Path(__file__).resolve().parent / "russian_stack" / "russian_lexicon.sqlite"


def _flag_enabled() -> bool:
    return os.environ.get(FEATURE_FLAG, "").strip() in ("1", "true", "yes", "on")


def _resolve(db_path: Optional[str | Path] = None) -> Path:
    if db_path is not None:
        return Path(db_path)
    env = os.environ.get("LUNA_RUSSIAN_LEXICON_DB")
    return Path(env) if env else _DEFAULT_DB


def _clamp(limit: Optional[int]) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    try:
        n = int(limit)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if n <= 0:
        return DEFAULT_LIMIT
    return min(n, HARD_MAX_LIMIT)


@contextmanager
def _connect(db_path: Optional[str | Path] = None):
    p = _resolve(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        yield conn
    finally:
        conn.close()


_EXTRA_WORDS_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("register_tags_json",       "TEXT", "'[]'"),
    ("safety_tags_json",         "TEXT", "'[]'"),
    ("coverage_categories_json", "TEXT", "'[]'"),
    ("pack_source",              "TEXT", "''"),
    ("pack_id",                  "TEXT", "''"),
)

_EXTRA_PHRASES_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("register_tags_json",       "TEXT", "'[]'"),
    ("safety_tags_json",         "TEXT", "'[]'"),
    ("coverage_categories_json", "TEXT", "'[]'"),
    ("pack_source",              "TEXT", "''"),
    ("pack_id",                  "TEXT", "''"),
)


def _apply_migrations(conn: sqlite3.Connection) -> dict[str, list[str]]:
    added: dict[str, list[str]] = {"words": [], "phrases": []}
    existing_w = {r[1] for r in conn.execute("PRAGMA table_info(words)")}
    for col, ctype, default in _EXTRA_WORDS_COLUMNS:
        if col not in existing_w:
            conn.execute(
                f"ALTER TABLE words ADD COLUMN {col} {ctype} NOT NULL DEFAULT {default}"
            )
            added["words"].append(col)
    existing_p = {r[1] for r in conn.execute("PRAGMA table_info(phrases)")}
    for col, ctype, default in _EXTRA_PHRASES_COLUMNS:
        if col not in existing_p:
            conn.execute(
                f"ALTER TABLE phrases ADD COLUMN {col} {ctype} NOT NULL DEFAULT {default}"
            )
            added["phrases"].append(col)
    return added


def init_db(db_path: Optional[str | Path] = None) -> str:
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS words (
                word              TEXT PRIMARY KEY,
                lemma             TEXT NOT NULL DEFAULT '',
                part_of_speech    TEXT NOT NULL DEFAULT '',
                definition_ru     TEXT NOT NULL DEFAULT '',
                definition_en     TEXT NOT NULL DEFAULT '',
                synonyms_json     TEXT NOT NULL DEFAULT '[]',
                antonyms_json     TEXT NOT NULL DEFAULT '[]',
                examples_json     TEXT NOT NULL DEFAULT '[]',
                phrase_examples_json TEXT NOT NULL DEFAULT '[]',
                idioms_json       TEXT NOT NULL DEFAULT '[]',
                domain_tags_json  TEXT NOT NULL DEFAULT '[]',
                semantic_tags_json TEXT NOT NULL DEFAULT '[]',
                frequency_score   REAL NOT NULL DEFAULT 0.0,
                register_level    TEXT NOT NULL DEFAULT 'plain',
                source            TEXT NOT NULL DEFAULT 'manual',
                created_at        REAL NOT NULL,
                updated_at        REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ru_lemma ON words(lemma);
            CREATE INDEX IF NOT EXISTS idx_ru_pos ON words(part_of_speech);
            CREATE INDEX IF NOT EXISTS idx_ru_freq ON words(frequency_score);

            CREATE TABLE IF NOT EXISTS phrases (
                phrase            TEXT PRIMARY KEY,
                translation_en    TEXT NOT NULL DEFAULT '',
                gloss_ru          TEXT NOT NULL DEFAULT '',
                idiomatic         INTEGER NOT NULL DEFAULT 0,
                domain_tags_json  TEXT NOT NULL DEFAULT '[]',
                semantic_tags_json TEXT NOT NULL DEFAULT '[]',
                frequency_score   REAL NOT NULL DEFAULT 0.0,
                register_level    TEXT NOT NULL DEFAULT 'plain',
                source            TEXT NOT NULL DEFAULT 'manual',
                created_at        REAL NOT NULL,
                updated_at        REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_phrase_idiomatic ON phrases(idiomatic);
            """
        )
        _apply_migrations(conn)
    return str(_resolve(db_path))


def _row_to_word(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    for j, plain in (
        ("synonyms_json", "synonyms"),
        ("antonyms_json", "antonyms"),
        ("examples_json", "examples"),
        ("phrase_examples_json", "phrase_examples"),
        ("idioms_json", "idioms"),
        ("domain_tags_json", "domain_tags"),
        ("semantic_tags_json", "semantic_tags"),
        ("register_tags_json", "register_tags"),
        ("safety_tags_json", "safety_tags"),
        ("coverage_categories_json", "coverage_categories"),
    ):
        raw = d.pop(j, "[]")
        try:
            d[plain] = json.loads(raw) if raw else []
        except json.JSONDecodeError:
            d[plain] = []
    d.setdefault("pack_source", "")
    d.setdefault("pack_id", "")
    return d


def _row_to_phrase(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    for j, plain in (
        ("domain_tags_json", "domain_tags"),
        ("semantic_tags_json", "semantic_tags"),
        ("register_tags_json", "register_tags"),
        ("safety_tags_json", "safety_tags"),
        ("coverage_categories_json", "coverage_categories"),
    ):
        raw = d.pop(j, "[]")
        try:
            d[plain] = json.loads(raw) if raw else []
        except json.JSONDecodeError:
            d[plain] = []
    d.setdefault("pack_source", "")
    d.setdefault("pack_id", "")
    d["idiomatic"] = bool(d.get("idiomatic", 0))
    return d


def _jsonset(items: Optional[Iterable[str]]) -> str:
    return json.dumps(sorted({s.strip() for s in (items or []) if s and s.strip()}))


def _jsonlist(items: Optional[Iterable[str]]) -> str:
    return json.dumps([s for s in (items or []) if isinstance(s, str) and s.strip()])


def add_word(
    word: str,
    lemma: str = "",
    part_of_speech: str = "",
    definition_ru: str = "",
    definition_en: str = "",
    synonyms: Optional[Iterable[str]] = None,
    antonyms: Optional[Iterable[str]] = None,
    examples: Optional[Iterable[str]] = None,
    phrase_examples: Optional[Iterable[str]] = None,
    idioms: Optional[Iterable[str]] = None,
    domain_tags: Optional[Iterable[str]] = None,
    semantic_tags: Optional[Iterable[str]] = None,
    frequency_score: float = 0.0,
    register_level: str = "plain",
    source: str = "manual",
    register_tags: Optional[Iterable[str]] = None,
    safety_tags: Optional[Iterable[str]] = None,
    coverage_categories: Optional[Iterable[str]] = None,
    pack_source: str = "",
    pack_id: str = "",
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if not isinstance(word, str) or not word.strip():
        raise ValueError("word must be a non-empty string")
    w = word.strip()
    now = time.time()
    with _connect(db_path) as conn:
        existing = conn.execute("SELECT created_at FROM words WHERE word=?", (w,)).fetchone()
        created = existing["created_at"] if existing else now
        conn.execute(
            """INSERT INTO words(word, lemma, part_of_speech, definition_ru, definition_en,
                synonyms_json, antonyms_json, examples_json, phrase_examples_json,
                idioms_json, domain_tags_json, semantic_tags_json, frequency_score,
                register_level, source, created_at, updated_at,
                register_tags_json, safety_tags_json, coverage_categories_json,
                pack_source, pack_id)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(word) DO UPDATE SET
                 lemma=excluded.lemma,
                 part_of_speech=excluded.part_of_speech,
                 definition_ru=excluded.definition_ru,
                 definition_en=excluded.definition_en,
                 synonyms_json=excluded.synonyms_json,
                 antonyms_json=excluded.antonyms_json,
                 examples_json=excluded.examples_json,
                 phrase_examples_json=excluded.phrase_examples_json,
                 idioms_json=excluded.idioms_json,
                 domain_tags_json=excluded.domain_tags_json,
                 semantic_tags_json=excluded.semantic_tags_json,
                 frequency_score=excluded.frequency_score,
                 register_level=excluded.register_level,
                 source=excluded.source,
                 updated_at=excluded.updated_at,
                 register_tags_json=excluded.register_tags_json,
                 safety_tags_json=excluded.safety_tags_json,
                 coverage_categories_json=excluded.coverage_categories_json,
                 pack_source=excluded.pack_source,
                 pack_id=excluded.pack_id
            """,
            (w, (lemma or "").strip(), (part_of_speech or "").strip(),
             definition_ru or "", definition_en or "",
             _jsonset(synonyms), _jsonset(antonyms),
             _jsonlist(examples), _jsonlist(phrase_examples), _jsonlist(idioms),
             _jsonset(domain_tags), _jsonset(semantic_tags),
             float(frequency_score), register_level or "plain", source or "manual",
             created, now,
             _jsonset(register_tags), _jsonset(safety_tags),
             _jsonset(coverage_categories),
             pack_source or "", pack_id or ""),
        )
        row = conn.execute("SELECT * FROM words WHERE word=?", (w,)).fetchone()
    return _row_to_word(row)


def add_phrase(
    phrase: str,
    translation_en: str = "",
    gloss_ru: str = "",
    idiomatic: bool = False,
    domain_tags: Optional[Iterable[str]] = None,
    semantic_tags: Optional[Iterable[str]] = None,
    frequency_score: float = 0.0,
    register_level: str = "plain",
    source: str = "manual",
    register_tags: Optional[Iterable[str]] = None,
    safety_tags: Optional[Iterable[str]] = None,
    coverage_categories: Optional[Iterable[str]] = None,
    pack_source: str = "",
    pack_id: str = "",
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if not isinstance(phrase, str) or not phrase.strip():
        raise ValueError("phrase must be a non-empty string")
    p = phrase.strip()
    now = time.time()
    with _connect(db_path) as conn:
        existing = conn.execute("SELECT created_at FROM phrases WHERE phrase=?", (p,)).fetchone()
        created = existing["created_at"] if existing else now
        conn.execute(
            """INSERT INTO phrases(phrase, translation_en, gloss_ru, idiomatic,
                 domain_tags_json, semantic_tags_json, frequency_score,
                 register_level, source, created_at, updated_at,
                 register_tags_json, safety_tags_json, coverage_categories_json,
                 pack_source, pack_id)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(phrase) DO UPDATE SET
                 translation_en=excluded.translation_en,
                 gloss_ru=excluded.gloss_ru,
                 idiomatic=excluded.idiomatic,
                 domain_tags_json=excluded.domain_tags_json,
                 semantic_tags_json=excluded.semantic_tags_json,
                 frequency_score=excluded.frequency_score,
                 register_level=excluded.register_level,
                 source=excluded.source,
                 updated_at=excluded.updated_at,
                 register_tags_json=excluded.register_tags_json,
                 safety_tags_json=excluded.safety_tags_json,
                 coverage_categories_json=excluded.coverage_categories_json,
                 pack_source=excluded.pack_source,
                 pack_id=excluded.pack_id
            """,
            (p, translation_en or "", gloss_ru or "", 1 if idiomatic else 0,
             _jsonset(domain_tags), _jsonset(semantic_tags),
             float(frequency_score), register_level or "plain", source or "manual",
             created, now,
             _jsonset(register_tags), _jsonset(safety_tags),
             _jsonset(coverage_categories),
             pack_source or "", pack_id or ""),
        )
        row = conn.execute("SELECT * FROM phrases WHERE phrase=?", (p,)).fetchone()
    return _row_to_phrase(row)


def lookup_word(word: str, db_path: Optional[str | Path] = None) -> Optional[dict[str, Any]]:
    if not isinstance(word, str) or not word.strip():
        return None
    with _connect(db_path) as conn:
        row = conn.execute("SELECT * FROM words WHERE word=? LIMIT 1",
                           (word.strip(),)).fetchone()
    return _row_to_word(row) if row else None


def lookup_lemma(
    lemma: str,
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    if not isinstance(lemma, str) or not lemma.strip():
        return []
    n = _clamp(limit)
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM words WHERE lemma=? ORDER BY frequency_score DESC LIMIT ?",
            (lemma.strip(), n),
        ).fetchall()
    return [_row_to_word(r) for r in rows]


def search_prefix(prefix: str, limit: int = DEFAULT_LIMIT,
                  db_path: Optional[str | Path] = None) -> list[dict[str, Any]]:
    if not isinstance(prefix, str) or not prefix.strip():
        return []
    n = _clamp(limit)
    pat = prefix.strip().replace("%", "").replace("_", "") + "%"
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM words WHERE word LIKE ? ORDER BY frequency_score DESC, word ASC LIMIT ?",
            (pat, n),
        ).fetchall()
    return [_row_to_word(r) for r in rows]


def search_contains(needle: str, limit: int = DEFAULT_LIMIT,
                    db_path: Optional[str | Path] = None) -> list[dict[str, Any]]:
    if not isinstance(needle, str) or not needle.strip():
        return []
    n = _clamp(limit)
    pat = "%" + needle.strip().replace("%", "").replace("_", "") + "%"
    with _connect(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM words
               WHERE word LIKE ? OR lemma LIKE ? OR definition_ru LIKE ? OR definition_en LIKE ?
               ORDER BY frequency_score DESC, word ASC LIMIT ?""",
            (pat, pat, pat, pat, n),
        ).fetchall()
    return [_row_to_word(r) for r in rows]


def search_by_tag(tag: str, limit: int = DEFAULT_LIMIT,
                  scope: str = "any",
                  db_path: Optional[str | Path] = None) -> list[dict[str, Any]]:
    """scope ∈ {'any','domain','semantic'}"""
    if not isinstance(tag, str) or not tag.strip():
        return []
    n = _clamp(limit)
    pat = f'%"{tag.strip()}"%'
    sql_clauses: list[str] = []
    if scope in ("any", "domain"):
        sql_clauses.append("domain_tags_json LIKE ?")
    if scope in ("any", "semantic"):
        sql_clauses.append("semantic_tags_json LIKE ?")
    if not sql_clauses:
        return []
    where = " OR ".join(sql_clauses)
    params = [pat] * len(sql_clauses) + [n]
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM words WHERE {where} ORDER BY frequency_score DESC LIMIT ?",
            tuple(params),
        ).fetchall()
    return [_row_to_word(r) for r in rows]


def get_synonyms(word: str, limit: int = DEFAULT_LIMIT,
                 db_path: Optional[str | Path] = None) -> list[str]:
    row = lookup_word(word, db_path=db_path)
    if not row:
        return []
    return (row.get("synonyms") or [])[:_clamp(limit)]


def get_examples(word: str, limit: int = 10,
                 db_path: Optional[str | Path] = None) -> list[str]:
    row = lookup_word(word, db_path=db_path)
    if not row:
        return []
    return (row.get("examples") or [])[:_clamp(limit)]


def get_idioms(tag: Optional[str] = None, limit: int = DEFAULT_LIMIT,
               db_path: Optional[str | Path] = None) -> list[dict[str, Any]]:
    n = _clamp(limit)
    with _connect(db_path) as conn:
        if tag and isinstance(tag, str) and tag.strip():
            pat = f'%"{tag.strip()}"%'
            rows = conn.execute(
                """SELECT * FROM phrases
                   WHERE idiomatic=1 AND (domain_tags_json LIKE ? OR semantic_tags_json LIKE ?)
                   ORDER BY frequency_score DESC LIMIT ?""",
                (pat, pat, n),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM phrases WHERE idiomatic=1 ORDER BY frequency_score DESC LIMIT ?",
                (n,),
            ).fetchall()
    return [_row_to_phrase(r) for r in rows]


_ALLOWED_COLS = {
    "word", "lemma", "part_of_speech", "register_level", "source",
}


def bounded_query(
    where: Optional[dict[str, Any]] = None,
    order_by: str = "frequency_score DESC, word ASC",
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    n = _clamp(limit)
    clauses: list[str] = []
    params: list[Any] = []
    for col, val in (where or {}).items():
        if col not in _ALLOWED_COLS:
            raise ValueError(f"filter column not allowed: {col!r}")
        clauses.append(f"{col} = ?")
        params.append(val)
    safe_order_cols = {
        "word", "lemma", "part_of_speech", "register_level",
        "source", "frequency_score", "created_at", "updated_at",
    }
    safe_order = order_by if all(
        tok.strip().split()[0] in safe_order_cols
        for tok in order_by.split(",")
    ) else "frequency_score DESC, word ASC"
    sql = "SELECT * FROM words"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += f" ORDER BY {safe_order} LIMIT ?"
    params.append(n)
    with _connect(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [_row_to_word(r) for r in rows]


def count_words(db_path: Optional[str | Path] = None) -> int:
    with _connect(db_path) as conn:
        r = conn.execute("SELECT COUNT(*) AS n FROM words").fetchone()
    return int(r["n"]) if r else 0


def count_phrases(db_path: Optional[str | Path] = None) -> int:
    with _connect(db_path) as conn:
        r = conn.execute("SELECT COUNT(*) AS n FROM phrases").fetchone()
    return int(r["n"]) if r else 0
