"""Phase 16 - Dual Corpus Checkpoint Store.

SQLite-backed progress records for chunked corpus imports. Allows a stopped or
crashed import to be resumed from the last byte offset / line number.

No daemon, no auto-resume. Resume is operator-initiated by passing
``resume_checkpoint_id`` into the chunked importer.

Fields:
    checkpoint_id    text   stable id
    corpus_id        text   FK to corpus_sources.corpus_id (logical, not enforced)
    source_path      text   resolved at checkpoint creation
    language         text   'en' | 'ru'
    last_byte_offset int    bytes consumed from source
    last_line_number int    1-based line of last attempted row
    accepted_count   int    rows added to lexicon store
    rejected_count   int    rows rejected by validator/policy
    duplicate_count  int    rows skipped because of in-batch or DB dedup
    batch_count      int    batches completed
    status           text   'running' | 'paused' | 'completed' | 'failed'
    notes            text
    created_at       real
    updated_at       real
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional


DEFAULT_CHECKPOINT_DB = Path("corpus_sources") / "checkpoints" / "checkpoints.sqlite3"


VALID_STATUSES: tuple[str, ...] = ("running", "paused", "completed", "failed")


SCHEMA = """
CREATE TABLE IF NOT EXISTS corpus_checkpoints (
    checkpoint_id    TEXT PRIMARY KEY,
    corpus_id        TEXT NOT NULL,
    source_path      TEXT NOT NULL,
    language         TEXT NOT NULL,
    last_byte_offset INTEGER NOT NULL DEFAULT 0,
    last_line_number INTEGER NOT NULL DEFAULT 0,
    accepted_count   INTEGER NOT NULL DEFAULT 0,
    rejected_count   INTEGER NOT NULL DEFAULT 0,
    duplicate_count  INTEGER NOT NULL DEFAULT 0,
    batch_count      INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'running',
    notes            TEXT NOT NULL DEFAULT '',
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ckpt_corpus ON corpus_checkpoints(corpus_id);
CREATE INDEX IF NOT EXISTS idx_ckpt_status ON corpus_checkpoints(status);
"""


def _connect(db_path: Optional[str | Path]) -> sqlite3.Connection:
    p = Path(db_path) if db_path is not None else DEFAULT_CHECKPOINT_DB
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_checkpoint_store(db_path: Optional[str | Path] = None) -> str:
    p = Path(db_path) if db_path is not None else DEFAULT_CHECKPOINT_DB
    conn = _connect(p)
    try:
        for stmt in [s.strip() for s in SCHEMA.split(";") if s.strip()]:
            conn.execute(stmt)
    finally:
        conn.close()
    return str(p)


def _now() -> float:
    return time.time()


def _new_id() -> str:
    return f"ckpt_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def create_checkpoint(
    corpus_id: str,
    source_path: str | Path,
    language: str,
    notes: str = "",
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    init_checkpoint_store(db_path)
    cid = _new_id()
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO corpus_checkpoints "
            "(checkpoint_id, corpus_id, source_path, language, last_byte_offset, "
            " last_line_number, accepted_count, rejected_count, duplicate_count, "
            " batch_count, status, notes, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (cid, str(corpus_id), str(source_path), language, 0, 0, 0, 0, 0, 0,
             "running", str(notes), now, now),
        )
    finally:
        conn.close()
    return {"ok": True, "checkpoint_id": cid, "corpus_id": corpus_id,
            "source_path": str(source_path), "language": language}


_FIELDS = ("checkpoint_id", "corpus_id", "source_path", "language",
           "last_byte_offset", "last_line_number", "accepted_count",
           "rejected_count", "duplicate_count", "batch_count", "status",
           "notes", "created_at", "updated_at")


def _row_to_dict(row: tuple) -> dict[str, Any]:
    return dict(zip(_FIELDS, row))


def load_checkpoint(checkpoint_id: str,
                    db_path: Optional[str | Path] = None) -> Optional[dict[str, Any]]:
    init_checkpoint_store(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT " + ", ".join(_FIELDS) +
            " FROM corpus_checkpoints WHERE checkpoint_id=?", (checkpoint_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    return _row_to_dict(row) if row else None


def update_checkpoint(
    checkpoint_id: str,
    *,
    last_byte_offset: Optional[int] = None,
    last_line_number: Optional[int] = None,
    accepted_count: Optional[int] = None,
    rejected_count: Optional[int] = None,
    duplicate_count: Optional[int] = None,
    batch_count: Optional[int] = None,
    notes: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    fields: list[str] = []
    args: list[Any] = []
    for name, val in (
        ("last_byte_offset", last_byte_offset),
        ("last_line_number", last_line_number),
        ("accepted_count", accepted_count),
        ("rejected_count", rejected_count),
        ("duplicate_count", duplicate_count),
        ("batch_count", batch_count),
    ):
        if val is not None:
            fields.append(f"{name}=?")
            args.append(int(val))
    if notes is not None:
        fields.append("notes=?")
        args.append(str(notes))
    if not fields:
        return {"ok": False, "error": "nothing_to_update"}
    fields.append("updated_at=?")
    args.append(_now())
    args.append(checkpoint_id)

    init_checkpoint_store(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "UPDATE corpus_checkpoints SET " + ", ".join(fields) +
            " WHERE checkpoint_id=?", tuple(args))
        n = cur.rowcount
    finally:
        conn.close()
    return {"ok": bool(n), "checkpoint_id": checkpoint_id, "rows_updated": int(n)}


def mark_checkpoint_complete(
    checkpoint_id: str,
    notes: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    return _mark_status(checkpoint_id, "completed", notes, db_path)


def mark_checkpoint_failed(
    checkpoint_id: str,
    notes: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    return _mark_status(checkpoint_id, "failed", notes, db_path)


def _mark_status(checkpoint_id: str, status: str,
                 notes: Optional[str],
                 db_path: Optional[str | Path]) -> dict[str, Any]:
    if status not in VALID_STATUSES:
        return {"ok": False, "error": f"invalid_status: {status!r}"}
    init_checkpoint_store(db_path)
    now = _now()
    conn = _connect(db_path)
    try:
        if notes is None:
            cur = conn.execute(
                "UPDATE corpus_checkpoints SET status=?, updated_at=? "
                "WHERE checkpoint_id=?", (status, now, checkpoint_id))
        else:
            cur = conn.execute(
                "UPDATE corpus_checkpoints SET status=?, notes=?, updated_at=? "
                "WHERE checkpoint_id=?", (status, str(notes), now, checkpoint_id))
        n = cur.rowcount
    finally:
        conn.close()
    return {"ok": bool(n), "checkpoint_id": checkpoint_id, "status": status,
            "rows_updated": int(n)}


def list_checkpoints(
    corpus_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    init_checkpoint_store(db_path)
    cap = max(1, min(int(limit), 2000))
    q = "SELECT " + ", ".join(_FIELDS) + " FROM corpus_checkpoints"
    where: list[str] = []
    args: list[Any] = []
    if corpus_id:
        where.append("corpus_id=?")
        args.append(corpus_id)
    if status:
        where.append("status=?")
        args.append(status)
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY created_at DESC LIMIT ?"
    args.append(cap)
    conn = _connect(db_path)
    try:
        cur = conn.execute(q, tuple(args))
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


__all__ = [
    "DEFAULT_CHECKPOINT_DB",
    "VALID_STATUSES",
    "init_checkpoint_store",
    "create_checkpoint",
    "load_checkpoint",
    "update_checkpoint",
    "mark_checkpoint_complete",
    "mark_checkpoint_failed",
    "list_checkpoints",
]
