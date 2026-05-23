"""Russian Sovereign Language Stack — durable Russian memory fabric.

Isolated SQLite store for Russian-language memories. Does NOT integrate with
Luna's main memory yet — that's an explicit future step. Bounded retrieval,
no background summarizer, no daemon, no external API calls.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional

FEATURE_FLAG = "LUNA_RUSSIAN_STACK"

DEFAULT_LIMIT = 10
HARD_MAX_LIMIT = 200

_DEFAULT_DB = Path(__file__).resolve().parent / "russian_stack" / "russian_memory.sqlite"

_CYR_WORD = re.compile(r"[Ѐ-ӿԀ-ԯ][Ѐ-ӿԀ-ԯ\-]*")


def _flag_enabled() -> bool:
    return os.environ.get(FEATURE_FLAG, "").strip() in ("1", "true", "yes", "on")


def _resolve(db_path: Optional[str | Path] = None) -> Path:
    if db_path is not None:
        return Path(db_path)
    env = os.environ.get("LUNA_RUSSIAN_MEMORY_DB")
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


def init_db(db_path: Optional[str | Path] = None) -> str:
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS memories (
                memory_id        TEXT PRIMARY KEY,
                text_ru          TEXT NOT NULL,
                text_en_summary  TEXT NOT NULL DEFAULT '',
                topic_tags_json  TEXT NOT NULL DEFAULT '[]',
                semantic_tags_json TEXT NOT NULL DEFAULT '[]',
                source           TEXT NOT NULL DEFAULT 'manual',
                importance       REAL NOT NULL DEFAULT 0.5,
                created_at       REAL NOT NULL,
                updated_at       REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_mem_importance ON memories(importance);
            CREATE INDEX IF NOT EXISTS idx_mem_created ON memories(created_at);
            """
        )
    return str(_resolve(db_path))


def _row(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    for j, plain in (
        ("topic_tags_json", "topic_tags"),
        ("semantic_tags_json", "semantic_tags"),
    ):
        raw = d.pop(j, "[]")
        try:
            d[plain] = json.loads(raw) if raw else []
        except json.JSONDecodeError:
            d[plain] = []
    return d


def _jsonset(items: Optional[Iterable[str]]) -> str:
    return json.dumps(sorted({s.strip() for s in (items or []) if s and s.strip()}))


def add_memory_ru(
    text_ru: str,
    text_en_summary: str = "",
    topic_tags: Optional[Iterable[str]] = None,
    semantic_tags: Optional[Iterable[str]] = None,
    source: str = "manual",
    importance: float = 0.5,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if not isinstance(text_ru, str) or not text_ru.strip():
        raise ValueError("text_ru must be a non-empty string")
    mid = uuid.uuid4().hex
    now = time.time()
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO memories(memory_id, text_ru, text_en_summary,
                 topic_tags_json, semantic_tags_json, source, importance,
                 created_at, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (mid, text_ru.strip(), text_en_summary or "",
             _jsonset(topic_tags), _jsonset(semantic_tags),
             source or "manual",
             max(0.0, min(1.0, float(importance))),
             now, now),
        )
        row = conn.execute("SELECT * FROM memories WHERE memory_id=?", (mid,)).fetchone()
    return _row(row)


def search_memory_ru(
    needle: str,
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    if not isinstance(needle, str) or not needle.strip():
        return []
    n = _clamp(limit)
    pat = "%" + needle.strip().replace("%", "").replace("_", "") + "%"
    with _connect(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM memories
               WHERE text_ru LIKE ? OR text_en_summary LIKE ?
                  OR topic_tags_json LIKE ? OR semantic_tags_json LIKE ?
               ORDER BY importance DESC, created_at DESC LIMIT ?""",
            (pat, pat, pat, pat, n),
        ).fetchall()
    return [_row(r) for r in rows]


def summarize_memory_ru(
    memory_id: str,
    max_len: int = 200,
    db_path: Optional[str | Path] = None,
) -> str:
    """Truncate-and-collapse summary. No external API. Returns '' if not found."""
    if not isinstance(memory_id, str) or not memory_id.strip():
        return ""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT text_ru, text_en_summary FROM memories WHERE memory_id=? LIMIT 1",
            (memory_id.strip(),),
        ).fetchone()
    if not row:
        return ""
    text = (row["text_en_summary"] or row["text_ru"] or "").strip()
    text = re.sub(r"\s+", " ", text)
    cap = max(20, min(int(max_len), 1000))
    return text[:cap]


def retrieve_context_ru(
    prompt: str,
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    """Best-effort context retrieval. Uses Cyrillic tokens from `prompt` as needles."""
    if not isinstance(prompt, str) or not prompt.strip():
        return []
    n = _clamp(limit)
    tokens = [t.lower() for t in _CYR_WORD.findall(prompt)][:8]
    if not tokens:
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for tok in tokens:
        hits = search_memory_ru(tok, limit=n, db_path=db_path)
        for h in hits:
            if h["memory_id"] not in seen:
                seen.add(h["memory_id"])
                out.append(h)
                if len(out) >= n:
                    return out[:n]
    return out[:n]


def translate_summary_stub(text: str) -> str:
    """Deterministic ASCII transliteration stub. No network calls."""
    if not isinstance(text, str) or not text:
        return ""
    table = str.maketrans({
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo",
        "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
        "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
        "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
        "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
        "А": "A", "Б": "B", "В": "V", "Г": "G", "Д": "D", "Е": "E", "Ё": "Yo",
        "Ж": "Zh", "З": "Z", "И": "I", "Й": "Y", "К": "K", "Л": "L", "М": "M",
        "Н": "N", "О": "O", "П": "P", "Р": "R", "С": "S", "Т": "T", "У": "U",
        "Ф": "F", "Х": "Kh", "Ц": "Ts", "Ч": "Ch", "Ш": "Sh", "Щ": "Shch",
        "Ъ": "", "Ы": "Y", "Ь": "", "Э": "E", "Ю": "Yu", "Я": "Ya",
    })
    return text.translate(table)[:400]


def bounded_memory_query(
    where: Optional[dict[str, Any]] = None,
    limit: int = DEFAULT_LIMIT,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    n = _clamp(limit)
    allowed = {"memory_id", "source"}
    clauses: list[str] = []
    params: list[Any] = []
    for col, val in (where or {}).items():
        if col not in allowed:
            raise ValueError(f"filter column not allowed: {col!r}")
        clauses.append(f"{col} = ?")
        params.append(val)
    sql = "SELECT * FROM memories"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY importance DESC, created_at DESC LIMIT ?"
    params.append(n)
    with _connect(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [_row(r) for r in rows]


def count_memories(db_path: Optional[str | Path] = None) -> int:
    with _connect(db_path) as conn:
        r = conn.execute("SELECT COUNT(*) AS n FROM memories").fetchone()
    return int(r["n"]) if r else 0


__all__ = [
    "FEATURE_FLAG",
    "init_db",
    "add_memory_ru",
    "search_memory_ru",
    "summarize_memory_ru",
    "retrieve_context_ru",
    "translate_summary_stub",
    "bounded_memory_query",
    "count_memories",
]
