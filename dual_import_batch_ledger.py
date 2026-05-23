"""Phase 20 - Dual Import Batch Ledger.

SQLite-backed audit trail for every staged import batch. Pure recordkeeping -
this module never rolls back, never mutates lexicon rows. Rollback is the
operator's responsibility (using the rollback_key column stored here).
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional


DEFAULT_LEDGER_PATH = Path("corpus_sources/phase20/ledger.sqlite3")


SCHEMA = """
CREATE TABLE IF NOT EXISTS import_batches (
    batch_id            TEXT PRIMARY KEY,
    stage_id            TEXT NOT NULL DEFAULT '',
    corpus_id           TEXT NOT NULL DEFAULT '',
    pack_id             TEXT NOT NULL DEFAULT '',
    language            TEXT NOT NULL,
    source_path         TEXT NOT NULL DEFAULT '',
    source_sha256       TEXT NOT NULL DEFAULT '',
    started_at          REAL NOT NULL,
    completed_at        REAL NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'running',
    dry_run             INTEGER NOT NULL DEFAULT 1,
    accepted_count      INTEGER NOT NULL DEFAULT 0,
    rejected_count      INTEGER NOT NULL DEFAULT 0,
    duplicate_count     INTEGER NOT NULL DEFAULT 0,
    before_word_count   INTEGER NOT NULL DEFAULT 0,
    after_word_count    INTEGER NOT NULL DEFAULT 0,
    before_phrase_count INTEGER NOT NULL DEFAULT 0,
    after_phrase_count  INTEGER NOT NULL DEFAULT 0,
    manifest_path       TEXT NOT NULL DEFAULT '',
    checkpoint_id       TEXT NOT NULL DEFAULT '',
    rollback_key        TEXT NOT NULL DEFAULT '',
    backup_snapshot_id  TEXT NOT NULL DEFAULT '',
    quality_report_path TEXT NOT NULL DEFAULT '',
    safety_audit_path   TEXT NOT NULL DEFAULT '',
    notes               TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_ib_stage ON import_batches(stage_id);
CREATE INDEX IF NOT EXISTS ix_ib_lang ON import_batches(language);
CREATE INDEX IF NOT EXISTS ix_ib_status ON import_batches(status);
CREATE INDEX IF NOT EXISTS ix_ib_corpus ON import_batches(corpus_id);
CREATE INDEX IF NOT EXISTS ix_ib_pack ON import_batches(pack_id);
CREATE INDEX IF NOT EXISTS ix_ib_rollback ON import_batches(rollback_key);
"""


_FIELDS = (
    "batch_id", "stage_id", "corpus_id", "pack_id", "language",
    "source_path", "source_sha256", "started_at", "completed_at",
    "status", "dry_run", "accepted_count", "rejected_count",
    "duplicate_count", "before_word_count", "after_word_count",
    "before_phrase_count", "after_phrase_count", "manifest_path",
    "checkpoint_id", "rollback_key", "backup_snapshot_id",
    "quality_report_path", "safety_audit_path", "notes",
)


VALID_STATUSES = ("running", "completed", "failed", "rolled_back")


def _connect(db_path: Optional[str | Path]) -> sqlite3.Connection:
    p = Path(db_path) if db_path is not None else DEFAULT_LEDGER_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_ledger(db_path: Optional[str | Path] = None) -> str:
    p = Path(db_path) if db_path is not None else DEFAULT_LEDGER_PATH
    conn = _connect(p)
    try:
        for stmt in [s.strip() for s in SCHEMA.split(";") if s.strip()]:
            conn.execute(stmt)
    finally:
        conn.close()
    return str(p)


def _new_batch_id() -> str:
    return f"batch_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def create_batch_record(
    *,
    language: str,
    stage_id: str = "",
    corpus_id: str = "",
    pack_id: str = "",
    source_path: str = "",
    source_sha256: str = "",
    dry_run: bool = True,
    manifest_path: str = "",
    checkpoint_id: str = "",
    rollback_key: str = "",
    backup_snapshot_id: str = "",
    before_word_count: int = 0,
    before_phrase_count: int = 0,
    notes: str = "",
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    init_ledger(db_path)
    bid = _new_batch_id()
    now = time.time()
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO import_batches "
            "(batch_id, stage_id, corpus_id, pack_id, language, source_path, "
            " source_sha256, started_at, completed_at, status, dry_run, "
            " accepted_count, rejected_count, duplicate_count, "
            " before_word_count, after_word_count, before_phrase_count, "
            " after_phrase_count, manifest_path, checkpoint_id, rollback_key, "
            " backup_snapshot_id, quality_report_path, safety_audit_path, notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (bid, str(stage_id), str(corpus_id), str(pack_id), language,
             str(source_path), str(source_sha256), now, 0.0, "running",
             1 if dry_run else 0, 0, 0, 0,
             int(before_word_count), 0, int(before_phrase_count), 0,
             str(manifest_path), str(checkpoint_id), str(rollback_key),
             str(backup_snapshot_id), "", "", str(notes)),
        )
    finally:
        conn.close()
    return {"ok": True, "batch_id": bid}


def update_batch_status(
    batch_id: str,
    status: str,
    *,
    accepted_count: Optional[int] = None,
    rejected_count: Optional[int] = None,
    duplicate_count: Optional[int] = None,
    after_word_count: Optional[int] = None,
    after_phrase_count: Optional[int] = None,
    quality_report_path: Optional[str] = None,
    safety_audit_path: Optional[str] = None,
    completed: bool = False,
    notes: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if status not in VALID_STATUSES:
        return {"ok": False, "error": f"invalid_status: {status!r}"}
    init_ledger(db_path)
    sets: list[str] = ["status=?"]
    args: list[Any] = [status]
    for name, val in (
        ("accepted_count", accepted_count),
        ("rejected_count", rejected_count),
        ("duplicate_count", duplicate_count),
        ("after_word_count", after_word_count),
        ("after_phrase_count", after_phrase_count),
    ):
        if val is not None:
            sets.append(f"{name}=?")
            args.append(int(val))
    for name, val in (("quality_report_path", quality_report_path),
                      ("safety_audit_path", safety_audit_path),
                      ("notes", notes)):
        if val is not None:
            sets.append(f"{name}=?")
            args.append(str(val))
    if completed:
        sets.append("completed_at=?")
        args.append(time.time())
    args.append(batch_id)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "UPDATE import_batches SET " + ", ".join(sets)
            + " WHERE batch_id=?", tuple(args))
        n = cur.rowcount
    finally:
        conn.close()
    return {"ok": bool(n), "batch_id": batch_id, "rows_updated": int(n)}


def _row_to_dict(row: tuple) -> dict[str, Any]:
    d = dict(zip(_FIELDS, row))
    d["dry_run"] = bool(d.get("dry_run"))
    return d


def get_batch(batch_id: str,
              db_path: Optional[str | Path] = None
              ) -> Optional[dict[str, Any]]:
    init_ledger(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT " + ",".join(_FIELDS) + " FROM import_batches WHERE batch_id=?",
            (batch_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    return _row_to_dict(row) if row else None


def list_batches(language: Optional[str] = None,
                 status: Optional[str] = None,
                 limit: int = 100,
                 db_path: Optional[str | Path] = None
                 ) -> list[dict[str, Any]]:
    init_ledger(db_path)
    cap = max(1, min(int(limit), 5000))
    where: list[str] = []
    args: list[Any] = []
    if language:
        where.append("language=?")
        args.append(language)
    if status:
        where.append("status=?")
        args.append(status)
    q = "SELECT " + ",".join(_FIELDS) + " FROM import_batches"
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY started_at DESC LIMIT ?"
    args.append(cap)
    conn = _connect(db_path)
    try:
        cur = conn.execute(q, tuple(args))
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_batches_by_stage(stage_id: str, limit: int = 1000,
                         db_path: Optional[str | Path] = None
                         ) -> list[dict[str, Any]]:
    init_ledger(db_path)
    cap = max(1, min(int(limit), 5000))
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT " + ",".join(_FIELDS)
            + " FROM import_batches WHERE stage_id=? "
            "ORDER BY started_at ASC LIMIT ?",
            (stage_id, cap))
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_batches_by_rollback_key(rollback_key: str, limit: int = 1000,
                                db_path: Optional[str | Path] = None
                                ) -> list[dict[str, Any]]:
    init_ledger(db_path)
    cap = max(1, min(int(limit), 5000))
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT " + ",".join(_FIELDS)
            + " FROM import_batches WHERE rollback_key=? "
            "ORDER BY started_at ASC LIMIT ?",
            (rollback_key, cap))
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def write_ledger_report(output_path: str | Path,
                        limit: int = 500,
                        db_path: Optional[str | Path] = None
                        ) -> str:
    items = list_batches(limit=limit, db_path=db_path)
    payload = {
        "generated_at": time.time(),
        "batch_count": len(items),
        "batches": items,
    }
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "DEFAULT_LEDGER_PATH",
    "VALID_STATUSES",
    "init_ledger",
    "create_batch_record",
    "update_batch_status",
    "get_batch",
    "list_batches",
    "get_batches_by_stage",
    "get_batches_by_rollback_key",
    "write_ledger_report",
]
