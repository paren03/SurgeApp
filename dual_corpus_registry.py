"""Phase 16 - Dual Corpus Registry.

SQLite-backed registry of LOCAL corpus source files staged for chunked import.
Does NOT touch the lexicon stores. Does NOT download anything. Does NOT run
daemons or schedulers. Pure introspection + record-keeping.

Source files live under ``corpus_sources/<lang>/incoming/``.

The registry stores:
  - corpus_id (UUID-ish stable id)
  - language ('en' | 'ru')
  - source_type (word_list/phrase_list/idiom_list/slang_list/street_talk_list/
                 profession_job_list/domain_terms/semantic_clusters/topic_pack/
                 mixed_jsonl/csv/txt)
  - expected_format ('jsonl' | 'txt' | 'csv')
  - source_path (absolute or repo-relative)
  - source_sha256 (streamed)
  - declared_row_count_estimate (optional, from bounded scan)
  - declared_categories / declared_registers / declared_safety (operator hints)
  - status ('registered'|'queued'|'in_progress'|'completed'|'failed'|'rejected')
  - notes
  - created_at / updated_at

No daemon, no auto-runner, no watchdog.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Optional

from pack_manifest import compute_sha256


DEFAULT_REGISTRY_PATH = Path("corpus_sources") / "corpus_registry.sqlite3"


LANGUAGES: tuple[str, ...] = ("en", "ru")

SOURCE_TYPES: tuple[str, ...] = (
    "word_list",
    "phrase_list",
    "idiom_list",
    "slang_list",
    "street_talk_list",
    "profession_job_list",
    "domain_terms",
    "semantic_clusters",
    "topic_pack",
    "mixed_jsonl",
    "csv",
    "txt",
)

EXPECTED_FORMATS: tuple[str, ...] = ("jsonl", "txt", "csv")

VALID_STATUSES: tuple[str, ...] = (
    "registered",
    "queued",
    "in_progress",
    "completed",
    "failed",
    "rejected",
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS corpus_sources (
    corpus_id TEXT PRIMARY KEY,
    language TEXT NOT NULL,
    source_type TEXT NOT NULL,
    expected_format TEXT NOT NULL,
    source_path TEXT NOT NULL,
    source_sha256 TEXT NOT NULL DEFAULT '',
    declared_row_count_estimate INTEGER NOT NULL DEFAULT 0,
    declared_categories TEXT NOT NULL DEFAULT '[]',
    declared_registers TEXT NOT NULL DEFAULT '[]',
    declared_safety TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'registered',
    notes TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_corpus_sources_lang   ON corpus_sources(language);
CREATE INDEX IF NOT EXISTS idx_corpus_sources_status ON corpus_sources(status);
CREATE INDEX IF NOT EXISTS idx_corpus_sources_path   ON corpus_sources(source_path);
"""


def _connect(db_path: Optional[str | Path]) -> sqlite3.Connection:
    p = Path(db_path) if db_path is not None else DEFAULT_REGISTRY_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_registry(db_path: Optional[str | Path] = None) -> str:
    p = Path(db_path) if db_path is not None else DEFAULT_REGISTRY_PATH
    conn = _connect(p)
    try:
        for stmt in [s.strip() for s in SCHEMA.split(";") if s.strip()]:
            conn.execute(stmt)
    finally:
        conn.close()
    return str(p)


def _now() -> float:
    return time.time()


def _validate_lang(lang: str) -> bool:
    return lang in LANGUAGES


def _validate_source_type(st: str) -> bool:
    return st in SOURCE_TYPES


def _validate_format(fmt: str) -> bool:
    return fmt in EXPECTED_FORMATS


def _validate_status(status: str) -> bool:
    return status in VALID_STATUSES


def validate_corpus_source_record(record: dict[str, Any]) -> dict[str, Any]:
    """Validate the shape of a registry record without writing it."""
    missing: list[str] = []
    for f in ("language", "source_type", "expected_format", "source_path"):
        if not record.get(f):
            missing.append(f)
    if missing:
        return {"ok": False, "reason": "missing_fields", "missing": missing}
    if not _validate_lang(record["language"]):
        return {"ok": False, "reason": f"invalid_language: {record['language']!r}"}
    if not _validate_source_type(record["source_type"]):
        return {"ok": False, "reason": f"invalid_source_type: {record['source_type']!r}"}
    if not _validate_format(record["expected_format"]):
        return {"ok": False, "reason": f"invalid_expected_format: {record['expected_format']!r}"}
    return {"ok": True}


def compute_source_sha256(path: str | Path) -> str:
    """Streaming SHA256 of a local file. Returns '' if path does not exist."""
    p = Path(path)
    if not p.exists() or not p.is_file():
        return ""
    return compute_sha256(p)


def estimate_rows_streaming(path: str | Path, max_scan_rows: int = 10000) -> int:
    """Stream up to ``max_scan_rows`` rows and report count.

    NEVER loads the whole file. Caller can read this as a lower-bound estimate
    capped at ``max_scan_rows``.
    """
    p = Path(path)
    if not p.exists() or not p.is_file():
        return 0
    cap = max(1, int(max_scan_rows))
    n = 0
    try:
        with p.open("r", encoding="utf-8", errors="replace") as fh:
            for _line in fh:
                n += 1
                if n >= cap:
                    break
    except Exception:
        return n
    return n


def _stable_corpus_id(lang: str, source_path: str) -> str:
    base = f"{lang}::{Path(source_path).name}::{uuid.uuid4().hex[:12]}"
    return base


def register_corpus_source(
    language: str,
    source_type: str,
    expected_format: str,
    source_path: str | Path,
    declared_categories: Optional[Iterable[str]] = None,
    declared_registers: Optional[Iterable[str]] = None,
    declared_safety: Optional[Iterable[str]] = None,
    notes: str = "",
    db_path: Optional[str | Path] = None,
    compute_hash: bool = True,
    scan_rows: bool = True,
    max_scan_rows: int = 10000,
) -> dict[str, Any]:
    """Insert a corpus source row.

    Does NOT touch the file beyond optional SHA256 + bounded row scan.
    Returns the persisted record + ok flag.
    """
    record = {
        "language": language,
        "source_type": source_type,
        "expected_format": expected_format,
        "source_path": str(source_path),
    }
    v = validate_corpus_source_record(record)
    if not v["ok"]:
        return {"ok": False, "error": v["reason"], "details": v}

    p = Path(source_path)
    if not p.exists():
        return {"ok": False, "error": "file_not_found", "source_path": str(p)}

    init_registry(db_path)
    cid = _stable_corpus_id(language, str(p))
    now = _now()
    sha = compute_source_sha256(p) if compute_hash else ""
    estimate = estimate_rows_streaming(p, max_scan_rows) if scan_rows else 0

    def _json_list(it: Optional[Iterable[str]]) -> str:
        return json.dumps(sorted({str(x) for x in (it or []) if x}),
                          ensure_ascii=False)

    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO corpus_sources "
            "(corpus_id, language, source_type, expected_format, source_path, "
            " source_sha256, declared_row_count_estimate, declared_categories, "
            " declared_registers, declared_safety, status, notes, "
            " created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (cid, language, source_type, expected_format, str(p),
             sha, int(estimate), _json_list(declared_categories),
             _json_list(declared_registers), _json_list(declared_safety),
             "registered", str(notes), now, now),
        )
    finally:
        conn.close()
    return {"ok": True, "corpus_id": cid,
            "source_sha256": sha, "row_estimate": estimate}


def _row_to_dict(row: sqlite3.Row | tuple) -> dict[str, Any]:
    keys = ("corpus_id", "language", "source_type", "expected_format",
            "source_path", "source_sha256", "declared_row_count_estimate",
            "declared_categories", "declared_registers", "declared_safety",
            "status", "notes", "created_at", "updated_at")
    d = dict(zip(keys, row))
    for f in ("declared_categories", "declared_registers", "declared_safety"):
        try:
            d[f] = json.loads(d[f]) if d.get(f) else []
        except Exception:
            d[f] = []
    return d


def get_corpus_source(corpus_id: str,
                      db_path: Optional[str | Path] = None) -> Optional[dict[str, Any]]:
    init_registry(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT corpus_id, language, source_type, expected_format, source_path, "
            "source_sha256, declared_row_count_estimate, declared_categories, "
            "declared_registers, declared_safety, status, notes, created_at, updated_at "
            "FROM corpus_sources WHERE corpus_id=?", (corpus_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    return _row_to_dict(row) if row else None


def list_corpus_sources(
    language: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    init_registry(db_path)
    cap = max(1, min(int(limit), 2000))
    q = ("SELECT corpus_id, language, source_type, expected_format, source_path, "
         "source_sha256, declared_row_count_estimate, declared_categories, "
         "declared_registers, declared_safety, status, notes, created_at, updated_at "
         "FROM corpus_sources")
    where: list[str] = []
    args: list[Any] = []
    if language:
        where.append("language=?")
        args.append(language)
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


def update_corpus_status(
    corpus_id: str,
    status: str,
    notes: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if not _validate_status(status):
        return {"ok": False, "error": f"invalid_status: {status!r}"}
    init_registry(db_path)
    now = _now()
    conn = _connect(db_path)
    try:
        if notes is None:
            cur = conn.execute(
                "UPDATE corpus_sources SET status=?, updated_at=? WHERE corpus_id=?",
                (status, now, corpus_id))
        else:
            cur = conn.execute(
                "UPDATE corpus_sources SET status=?, notes=?, updated_at=? WHERE corpus_id=?",
                (status, str(notes), now, corpus_id))
        changed = cur.rowcount
    finally:
        conn.close()
    return {"ok": bool(changed), "corpus_id": corpus_id,
            "status": status, "rows_updated": int(changed)}


def preview_corpus_source(
    path: str | Path,
    expected_format: str,
    limit: int = 25,
) -> dict[str, Any]:
    """Read up to ``limit`` rows from the source and return raw samples.

    Bounded: stops at min(limit, 100). Never loads full file.
    """
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "path": str(p), "samples": []}
    if not _validate_format(expected_format):
        return {"ok": False, "error": f"invalid_expected_format: {expected_format!r}",
                "samples": []}
    cap = max(1, min(int(limit), 100))
    samples: list[Any] = []
    try:
        with p.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if len(samples) >= cap:
                    break
                s = line.strip()
                if not s:
                    continue
                if expected_format == "jsonl":
                    try:
                        samples.append(json.loads(s))
                    except Exception:
                        samples.append({"_unparsed": s[:200]})
                elif expected_format == "csv":
                    samples.append({"_csv_row": s[:1000]})
                else:
                    samples.append({"_txt_row": s[:1000]})
    except Exception as e:
        return {"ok": False, "error": f"read_failed: {e}", "path": str(p),
                "samples": samples}
    return {"ok": True, "path": str(p), "expected_format": expected_format,
            "previewed": len(samples), "samples": samples}


__all__ = [
    "DEFAULT_REGISTRY_PATH",
    "LANGUAGES",
    "SOURCE_TYPES",
    "EXPECTED_FORMATS",
    "VALID_STATUSES",
    "init_registry",
    "register_corpus_source",
    "list_corpus_sources",
    "get_corpus_source",
    "update_corpus_status",
    "estimate_rows_streaming",
    "compute_source_sha256",
    "validate_corpus_source_record",
    "preview_corpus_source",
]
