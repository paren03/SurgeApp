"""Luna Memory Index — local memory compression + fast recall.

Phase 5D foundation. Pure Python stdlib. Reads bounded chunks of source
log/memory files, produces compact summary records (one paragraph + tags
+ source pointer + counts + hashes), and offers two search backends:

  1. SQLite FTS5 if the local Python is built with FTS5 support;
  2. Otherwise an in-memory keyword/inverted index (always works).

This module is **additive foundation only**. It is not wired into any
runtime service. Calling its functions cannot affect worker, bridge,
guardian, terminal, or launcher. Source files are read but never
mutated.

CLI:
    python -m luna_modules.luna_memory_index --self-test
    python -m luna_modules.luna_memory_index --build [--query "phase 3" --limit 5]
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1

# Resolve project root from this module's location: luna_modules/<this>.py
_THIS_FILE = Path(__file__).resolve()
PROJECT_DIR = _THIS_FILE.parent.parent

# Default I/O paths under <project>/memory/
_OUT_INDEX_PATH        = PROJECT_DIR / "memory" / "luna_memory_index.json"
_OUT_SUMMARIES_PATH    = PROJECT_DIR / "memory" / "luna_memory_summaries.jsonl"
_OUT_SQLITE_PATH       = PROJECT_DIR / "memory" / "luna_fast_recall.sqlite"
_OUT_BUILD_REPORT_PATH = PROJECT_DIR / "memory" / "luna_memory_index_build_report.json"

# Default per-source read limits — bounded so giant log files don't blow memory.
DEFAULT_TAIL_BYTES = 1_500_000     # last ~1.5 MB of any text/log file
DEFAULT_JSONL_LINES = 4_000        # last 4k JSONL records per source
DEFAULT_JSONL_BYTES = 6_000_000    # plus a hard 6 MB byte cap per JSONL
DEFAULT_SUMMARY_MAX_CHARS = 1200   # paragraph cap per summary record

# Curated source list. Files that don't exist are silently skipped.
_DEFAULT_SOURCES: Tuple[str, ...] = (
    # Memory side
    "memory/nightly_updates.jsonl",
    "memory/nightly_updates.md",
    "memory/luna_change_ledger.jsonl",
    "memory/luna_lessons_learned.jsonl",
    "memory/luna_autonomy_journal.jsonl",
    "memory/cu_self_repair_history.jsonl",
    "memory/luna_task_memory.json",
    "memory/luna_session_memory.json",
    # Logs side
    "logs/luna_live_feed.jsonl",
    "logs/luna_audit_trail.jsonl",
    "logs/luna_worker.log",
    "logs/aider_bridge.log",
    "logs/luna_guardian.log",
    "logs/luna_recent_failures_review.txt",
)

# Tag dictionary — coarse labels we extract from text content. Adding more
# is a low-risk additive change.
_TAG_PATTERNS: Tuple[Tuple[str, str], ...] = (
    ("worker",            r"\bworker(?:\.py|_main|_cu)?\b"),
    ("aider",             r"\baider(?:_bridge)?\b|--continues-update-start|aider-chat"),
    ("guardian",          r"\bluna_guardian\b|\bguardian\b"),
    ("bridge",            r"\baider_bridge\b|aider bridge"),
    ("continues_update",  r"\bcontinues[_\- ]update\b|\bCU\b|\bcu_loop\b"),
    ("phase3",            r"\bphase\s*3\b"),
    ("phase4",            r"\bphase\s*4[a-d]?\b"),
    ("phase5",            r"\bphase\s*5[a-z]?\b"),
    ("timeout",           r"\btimeout\b|aider_timeout"),
    ("context_overflow",  r"\bcontext[_\- ]overflow\b|token limit|exceeds the"),
    ("noop",              r"\bnoop\b|no diff|no_diff|no changes"),
    ("ollama",            r"\bollama\b|qwen2\.5-coder|num_ctx"),
    ("memory",            r"\bmemory\b|nightly_updates"),
    ("verifier",          r"Luna_Post_Repair_Verify|verifier"),
    ("rollback",          r"\brollback\b|reverted|git checkout"),
    ("self_repair",       r"self[-_ ]repair|self_repair_engine"),
    ("self_teacher",      r"self[-_ ]teacher|LESSONS"),
    ("autonomy",          r"\bautonomy\b|autonomous"),
    ("startup",           r"\bLaunchLuna\b|startup gate|one_click"),
    ("queue",             r"queue_governor|aider_jobs|active queue"),
    ("dirty_core",        r"dirty[_\- ]core|paused_dirty_core"),
    ("approval",          r"\bapprov(?:e|ed|al)\b|approval_queue"),
)

# Cheap word splitter for keyword index
_TOKEN_RE = re.compile(r"[a-zA-Z0-9_]{2,}")


# ---------------------------------------------------------------------------
# Small primitives
# ---------------------------------------------------------------------------

def now_iso() -> str:
    """UTC ISO-8601 with timezone offset."""
    return datetime.now(timezone.utc).isoformat()


def sha256_text(text: str) -> str:
    h = hashlib.sha256()
    h.update((text or "").encode("utf-8"))
    return h.hexdigest()


def sha256_file(path: Path) -> str:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1_048_576), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return ""


def normalize_source_path(path: str, project_dir: Optional[Path] = None) -> str:
    """Return project-relative POSIX path. Empty input returns ''."""
    raw = "" if path is None else str(path).strip()
    if not raw:
        return ""
    cleaned = raw.replace("\\", "/").rstrip("/")
    p = Path(cleaned)
    if p.is_absolute() and project_dir is not None:
        try:
            return str(p.resolve().relative_to(Path(project_dir).resolve())).replace("\\", "/")
        except Exception:
            return str(p).replace("\\", "/")
    return str(p).replace("\\", "/")


def default_memory_sources(project_dir: Optional[Path] = None) -> List[str]:
    """Curated default source list. Order is preserved; relative paths only."""
    return list(_DEFAULT_SOURCES)


# ---------------------------------------------------------------------------
# Bounded readers
# ---------------------------------------------------------------------------

def read_text_tail(path: Path, max_bytes: int = DEFAULT_TAIL_BYTES) -> str:
    """Read at most `max_bytes` from the END of the file. Empty on errors."""
    p = Path(path)
    try:
        size = p.stat().st_size
        if size == 0:
            return ""
        with open(p, "rb") as f:
            if size > max_bytes:
                f.seek(size - max_bytes)
                # Skip a possibly-partial first line
                f.readline()
            data = f.read()
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def iter_jsonl_records(path: Path,
                       max_lines: int = DEFAULT_JSONL_LINES,
                       max_bytes: int = DEFAULT_JSONL_BYTES) -> Iterator[Tuple[Optional[Dict[str, Any]], int, int]]:
    """Yield (record_or_None, line_no, byte_pos) tuples for the last ~max_lines.

    `record_or_None` is None when the JSON line is corrupt — caller can count
    those. `byte_pos` is the byte offset where the line started (approximate).
    Reads from the END of the file forward by up to `max_bytes`.
    """
    p = Path(path)
    try:
        size = p.stat().st_size
        if size == 0:
            return
        with open(p, "rb") as f:
            if size > max_bytes:
                f.seek(size - max_bytes)
                f.readline()  # skip partial line
            blob = f.read()
        text = blob.decode("utf-8", errors="replace")
    except Exception:
        return
    # Last `max_lines` of text
    lines = text.splitlines()
    if max_lines and len(lines) > max_lines:
        lines = lines[-max_lines:]
    line_no = 0
    byte_pos = 0
    for raw in lines:
        line_no += 1
        s = raw.strip()
        if not s:
            byte_pos += len(raw) + 1
            continue
        try:
            yield json.loads(s), line_no, byte_pos
        except Exception:
            yield None, line_no, byte_pos
        byte_pos += len(raw) + 1


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------

def extract_tags(text: str) -> List[str]:
    """Return sorted unique coarse tags found in `text`."""
    if not text:
        return []
    found = set()
    for tag, pat in _TAG_PATTERNS:
        try:
            if re.search(pat, text, re.IGNORECASE):
                found.add(tag)
        except re.error:
            continue
    return sorted(found)


# ---------------------------------------------------------------------------
# Summary records
# ---------------------------------------------------------------------------

def _make_summary_id(source_rel: str) -> str:
    """Stable per-source id incorporating source-rel and a short random suffix."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    suffix = sha256_text(f"{source_rel}|{ts}|{uuid.uuid4().hex}")[:8]
    return f"mem_{ts}_{suffix}"


def _condense_paragraph(text: str, max_chars: int = DEFAULT_SUMMARY_MAX_CHARS) -> str:
    """Collapse whitespace and clip to max_chars. No giant raw log paste."""
    if not text:
        return ""
    flat = re.sub(r"\s+", " ", text).strip()
    if len(flat) <= max_chars:
        return flat
    head = flat[: max_chars - 80].rstrip()
    return head + " …(truncated)"


def _extract_date_range(records_or_text: Any) -> Dict[str, str]:
    """Find earliest/latest ISO-8601 timestamps in records or free text.

    Looks for keys ts/timestamp/ts_iso/finished_at on dict records, OR a
    YYYY-MM-DD(THH:MM:SS) substring scan on free text. Returns {"start", "end"}
    with empty strings if none found.
    """
    iso_re = re.compile(r"\b(\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?)\b")
    earliest = None
    latest = None

    def _consider(s: str) -> None:
        nonlocal earliest, latest
        s = s.strip()
        if not s:
            return
        if earliest is None or s < earliest:
            earliest = s
        if latest is None or s > latest:
            latest = s

    if isinstance(records_or_text, list):
        for r in records_or_text:
            if not isinstance(r, dict):
                continue
            for k in ("ts", "timestamp", "ts_iso", "finished_at",
                      "created_at", "ts_utc"):
                v = r.get(k)
                if isinstance(v, str):
                    m = iso_re.search(v)
                    if m:
                        _consider(m.group(1))
    elif isinstance(records_or_text, str):
        for m in iso_re.finditer(records_or_text):
            _consider(m.group(1))
    return {"start": earliest or "", "end": latest or ""}


def summarize_text_block(source_path: str,
                         text: str,
                         project_dir: Optional[Path] = None,
                         max_chars: int = DEFAULT_SUMMARY_MAX_CHARS) -> Dict[str, Any]:
    """Wrap a raw text block in a single summary record.

    Note this always returns a record (even for short text); callers decide
    whether to actually persist it.
    """
    rel = normalize_source_path(source_path, project_dir=project_dir)
    para = _condense_paragraph(text, max_chars=max_chars)
    tags = extract_tags(text)
    drange = _extract_date_range(text)
    return build_summary_record(
        source_path=rel,
        source_sha256="",
        source_size_bytes=len(text.encode("utf-8")),
        source_modified_at="",
        date_range=drange,
        tags=tags,
        summary=para,
        record_count=1,
        corrupt_count=0,
        line_refs=[],
    )


def build_summary_record(*,
                         source_path: str,
                         source_sha256: str,
                         source_size_bytes: int,
                         source_modified_at: str,
                         date_range: Dict[str, str],
                         tags: List[str],
                         summary: str,
                         record_count: int,
                         corrupt_count: int,
                         line_refs: List[Dict[str, int]]) -> Dict[str, Any]:
    """Build a fully populated summary record dict."""
    return {
        "schema_version": SCHEMA_VERSION,
        "summary_id": _make_summary_id(source_path or "unknown"),
        "source_path": source_path or "",
        "source_sha256": source_sha256 or "",
        "source_size_bytes": int(source_size_bytes or 0),
        "source_modified_at": source_modified_at or "",
        "date_range": dict(date_range or {"start": "", "end": ""}),
        "tags": list(tags or []),
        "summary": str(summary or ""),
        "record_count": int(record_count or 0),
        "corrupt_count": int(corrupt_count or 0),
        "line_refs": list(line_refs or []),
        "created_at": now_iso(),
    }


def validate_summary_record(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Return (ok, errors) for a record dict."""
    errors: List[str] = []
    if not isinstance(record, dict):
        return False, ["record is not a dict"]
    required = ("schema_version", "summary_id", "source_path",
                "summary", "tags", "date_range", "created_at")
    for k in required:
        if k not in record:
            errors.append(f"missing required field: {k}")
    if "schema_version" in record:
        if not isinstance(record["schema_version"], int) or record["schema_version"] < 1:
            errors.append("schema_version must be int >= 1")
    if "tags" in record and not isinstance(record["tags"], list):
        errors.append("tags must be a list")
    if "date_range" in record:
        dr = record["date_range"]
        if not isinstance(dr, dict) or "start" not in dr or "end" not in dr:
            errors.append("date_range must be {start, end}")
    if "record_count" in record and not isinstance(record["record_count"], int):
        errors.append("record_count must be int")
    if "corrupt_count" in record and not isinstance(record["corrupt_count"], int):
        errors.append("corrupt_count must be int")
    if "line_refs" in record and not isinstance(record["line_refs"], list):
        errors.append("line_refs must be a list")
    return (len(errors) == 0), errors


# ---------------------------------------------------------------------------
# Per-source summarizer
# ---------------------------------------------------------------------------

def _summarize_one_source(rel: str,
                          project_dir: Path,
                          limits: Optional[Dict[str, int]] = None) -> Optional[Dict[str, Any]]:
    """Read one source file in bounded chunks and produce one summary record."""
    p = (project_dir / rel)
    if not p.exists() or not p.is_file():
        return None
    limits = limits or {}
    max_lines = int(limits.get("max_jsonl_lines", DEFAULT_JSONL_LINES))
    max_bytes = int(limits.get("max_jsonl_bytes", DEFAULT_JSONL_BYTES))
    tail_bytes = int(limits.get("max_tail_bytes", DEFAULT_TAIL_BYTES))
    max_chars = int(limits.get("summary_max_chars", DEFAULT_SUMMARY_MAX_CHARS))

    try:
        st = p.stat()
        modified_iso = datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat()
    except Exception:
        st = None
        modified_iso = ""

    record_count = 0
    corrupt_count = 0
    snippets: List[str] = []
    drange = {"start": "", "end": ""}

    suffix = p.suffix.lower()
    if suffix == ".jsonl":
        records: List[Dict[str, Any]] = []
        for rec, _line_no, _byte_pos in iter_jsonl_records(p, max_lines=max_lines, max_bytes=max_bytes):
            if rec is None:
                corrupt_count += 1
                continue
            record_count += 1
            if isinstance(rec, dict):
                records.append(rec)
                # cheap snippet from common message-ish keys
                for k in ("msg", "message", "summary", "reason",
                          "event", "action", "ui_status", "phase"):
                    v = rec.get(k)
                    if isinstance(v, str) and v.strip():
                        snippets.append(v.strip())
                        break
        drange = _extract_date_range(records)
    else:
        # .md / .log / .json / .txt — read tail
        text = read_text_tail(p, max_bytes=tail_bytes)
        if not text:
            return None
        record_count = text.count("\n") + (1 if text and not text.endswith("\n") else 0)
        snippets.append(text)
        drange = _extract_date_range(text)

    summary_text = _condense_paragraph(" • ".join(snippets[:80]), max_chars=max_chars)
    tags = extract_tags(" ".join(snippets[:200])) or extract_tags(rel)
    line_refs: List[Dict[str, int]] = []
    if record_count > 0:
        line_refs.append({"start_line": 1, "end_line": int(record_count)})

    return build_summary_record(
        source_path=rel,
        source_sha256=sha256_file(p),
        source_size_bytes=int(st.st_size) if st else 0,
        source_modified_at=modified_iso,
        date_range=drange,
        tags=tags,
        summary=summary_text,
        record_count=record_count,
        corrupt_count=corrupt_count,
        line_refs=line_refs,
    )


def build_memory_summaries(project_dir: Optional[Path] = None,
                           source_paths: Optional[List[str]] = None,
                           limits: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """Produce summary records for each present source file.

    Returns {records, missing_sources, build_errors}. Never crashes on a
    single bad source.
    """
    project_dir = Path(project_dir).resolve() if project_dir else PROJECT_DIR
    sources = list(source_paths or default_memory_sources(project_dir))
    records: List[Dict[str, Any]] = []
    missing: List[str] = []
    errors: List[Dict[str, str]] = []
    for rel in sources:
        try:
            full = project_dir / rel
            if not full.exists() or not full.is_file():
                missing.append(rel)
                continue
            rec = _summarize_one_source(rel, project_dir, limits=limits)
            if rec is None:
                missing.append(rel)
                continue
            ok, errs = validate_summary_record(rec)
            if not ok:
                errors.append({"path": rel, "error": f"validation: {errs}"})
                continue
            records.append(rec)
        except Exception as exc:
            errors.append({"path": rel, "error": str(exc)[:300]})
    return {
        "records": records,
        "missing_sources": missing,
        "build_errors": errors,
    }


def write_summaries_jsonl(records: List[Dict[str, Any]],
                          path: Path) -> int:
    """Write summary records to JSONL. Returns count written."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(target, "w", encoding="utf-8") as f:
        for rec in records or []:
            ok, _ = validate_summary_record(rec)
            if not ok:
                continue
            f.write(json.dumps(rec, ensure_ascii=True, sort_keys=True) + "\n")
            n += 1
    return n


# ---------------------------------------------------------------------------
# Keyword index (always-available fallback)
# ---------------------------------------------------------------------------

def _tokenize_query(query: str) -> List[str]:
    """Tokenize a search query.

    Standard tokens are >=2 chars. We additionally fuse adjacent letter/digit
    pairs (e.g. "phase 3" -> "phase3") because the indexer stores compact
    forms like "phase3" without an internal space.
    """
    if not query:
        return []
    parts = [t for t in re.split(r"[^a-zA-Z0-9_]+", str(query).lower()) if t]
    tokens: List[str] = [t for t in parts if len(t) >= 2]
    for i in range(len(parts) - 1):
        a, b = parts[i], parts[i + 1]
        if (a.isalpha() and b.isdigit()) or (a.isdigit() and b.isalpha()):
            fused = a + b
            if fused and fused not in tokens:
                tokens.append(fused)
    return tokens


def build_keyword_index(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build a small inverted-index dict over summary records."""
    index: Dict[str, List[int]] = {}
    for i, rec in enumerate(records or []):
        if not isinstance(rec, dict):
            continue
        haystack = " ".join([
            str(rec.get("summary") or ""),
            str(rec.get("source_path") or ""),
            " ".join(rec.get("tags") or []),
        ]).lower()
        seen = set()
        for tok in _TOKEN_RE.findall(haystack):
            t = tok.lower()
            if t in seen:
                continue
            seen.add(t)
            index.setdefault(t, []).append(i)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "record_count": len(records or []),
        "token_count": len(index),
        "tokens": dict(sorted(index.items())),
    }


def search_keyword_index(records_or_index: Any,
                         query: str,
                         limit: int = 10) -> List[Dict[str, Any]]:
    """Score and return top-K records. Accepts either a records list or a
    pre-built index dict; if records_or_index is a list, an index is built
    on the fly.
    """
    tokens = _tokenize_query(query)
    if not tokens:
        return []

    if isinstance(records_or_index, list):
        records = records_or_index
        index = build_keyword_index(records)
    elif isinstance(records_or_index, dict) and "tokens" in records_or_index:
        index = records_or_index
        # We need the records list too — caller probably passed an index built
        # alongside an in-memory `records` list; if not present, accept that
        # we can only score by token frequency, not content.
        records = records_or_index.get("_records") or []
    else:
        return []

    tokens_index = index.get("tokens", {})
    score: Dict[int, int] = {}
    for tok in tokens:
        if tok in tokens_index:
            for idx in tokens_index[tok]:
                score[idx] = score.get(idx, 0) + 1
    if not score:
        return []
    # If we don't have records (pure index), surface the top indices only.
    ranked = sorted(score.items(), key=lambda kv: (-kv[1], kv[0]))[: max(0, int(limit))]
    out: List[Dict[str, Any]] = []
    for idx, sc in ranked:
        if records and 0 <= idx < len(records):
            rec = dict(records[idx])
            rec["_score"] = int(sc)
            out.append(rec)
        else:
            out.append({"_index": idx, "_score": int(sc)})
    return out


# ---------------------------------------------------------------------------
# SQLite FTS5 backend (graceful fallback if FTS5 unavailable)
# ---------------------------------------------------------------------------

def sqlite_fts5_available(db_path: Optional[Path] = None) -> bool:
    """Return True iff this Python's sqlite3 supports FTS5 virtual tables."""
    try:
        target = Path(db_path) if db_path is not None else Path(":memory:")
        if str(target) != ":memory:":
            target.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(target))
        try:
            conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS _fts5_probe USING fts5(c)")
            conn.execute("DROP TABLE IF EXISTS _fts5_probe")
            return True
        finally:
            conn.close()
    except Exception:
        return False


def build_sqlite_fts_index(records: List[Dict[str, Any]],
                           db_path: Path) -> Dict[str, Any]:
    """Create/refresh a `mem_fts` FTS5 table at db_path. Returns a small
    report dict {ok, fts5_available, rows_inserted, db_path}.
    """
    target = Path(db_path)
    if not sqlite_fts5_available(target):
        return {
            "ok": False,
            "fts5_available": False,
            "rows_inserted": 0,
            "db_path": str(target),
            "reason": "sqlite3 FTS5 not available in this Python; using keyword fallback",
        }
    target.parent.mkdir(parents=True, exist_ok=True)
    # Wipe and rebuild — this is an idempotent, regenerable artifact.
    conn = sqlite3.connect(str(target))
    try:
        conn.executescript("""
            DROP TABLE IF EXISTS mem_fts;
            CREATE VIRTUAL TABLE mem_fts USING fts5(
                summary_id UNINDEXED,
                source_path,
                tags,
                summary,
                date_start UNINDEXED,
                date_end UNINDEXED
            );
        """)
        rows = []
        for rec in records or []:
            if not isinstance(rec, dict):
                continue
            rows.append((
                str(rec.get("summary_id") or ""),
                str(rec.get("source_path") or ""),
                " ".join(rec.get("tags") or []),
                str(rec.get("summary") or ""),
                str((rec.get("date_range") or {}).get("start") or ""),
                str((rec.get("date_range") or {}).get("end") or ""),
            ))
        conn.executemany(
            "INSERT INTO mem_fts (summary_id, source_path, tags, summary, "
            "date_start, date_end) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
        return {
            "ok": True,
            "fts5_available": True,
            "rows_inserted": len(rows),
            "db_path": str(target),
        }
    finally:
        conn.close()


def search_sqlite_fts(db_path: Path,
                      query: str,
                      limit: int = 10) -> List[Dict[str, Any]]:
    """Search the `mem_fts` table at db_path. Returns rows with bm25 rank."""
    target = Path(db_path)
    if not target.exists():
        return []
    if not query:
        return []
    # FTS5 query — defensive: replace ' with space and strip control chars
    safe_q = re.sub(r"[\x00-\x1f]+", " ", str(query)).replace("'", " ").strip()
    if not safe_q:
        return []
    try:
        conn = sqlite3.connect(str(target))
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT summary_id, source_path, tags, summary, date_start, date_end, "
                "bm25(mem_fts) AS rank FROM mem_fts "
                "WHERE mem_fts MATCH ? ORDER BY rank LIMIT ?",
                (safe_q, max(0, int(limit))),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception:
        return []


# ---------------------------------------------------------------------------
# High-level build + search
# ---------------------------------------------------------------------------

def build_memory_index(project_dir: Optional[Path] = None,
                       write: bool = True,
                       source_paths: Optional[List[str]] = None,
                       limits: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """Build summaries + keyword index + (optional) sqlite FTS5 index.

    When write=True, writes:
      memory/luna_memory_index.json                — pure-keyword index
      memory/luna_memory_summaries.jsonl           — summary records (one per source)
      memory/luna_fast_recall.sqlite               — FTS5 index when available
      memory/luna_memory_index_build_report.json   — build report
    """
    project_dir = Path(project_dir).resolve() if project_dir else PROJECT_DIR

    summaries_path     = project_dir / "memory" / "luna_memory_summaries.jsonl"
    keyword_index_path = project_dir / "memory" / "luna_memory_index.json"
    fts_db_path        = project_dir / "memory" / "luna_fast_recall.sqlite"
    report_path        = project_dir / "memory" / "luna_memory_index_build_report.json"

    sums = build_memory_summaries(project_dir, source_paths=source_paths, limits=limits)
    records = sums["records"]
    missing = sums["missing_sources"]
    build_errors = sums["build_errors"]

    keyword_index = build_keyword_index(records)
    fts_report: Dict[str, Any] = {"ok": False, "fts5_available": False, "rows_inserted": 0}
    summaries_written = 0
    if write:
        summaries_written = write_summaries_jsonl(records, summaries_path)
        # Keyword index: include records inline so search works without the JSONL
        keyword_index_for_disk = dict(keyword_index)
        keyword_index_for_disk["_records_summary_pointer"] = str(summaries_path).replace("\\", "/")
        keyword_index_path.parent.mkdir(parents=True, exist_ok=True)
        keyword_index_path.write_text(
            json.dumps(keyword_index_for_disk, ensure_ascii=True, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        # SQLite FTS5 — best-effort
        fts_report = build_sqlite_fts_index(records, fts_db_path)

    report = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "project_dir": str(project_dir),
        "source_count_attempted": len(source_paths or default_memory_sources(project_dir)),
        "summary_count": len(records),
        "missing_sources": missing,
        "build_errors": build_errors,
        "summaries_written": summaries_written,
        "fts5_report": fts_report,
        "keyword_token_count": int(keyword_index.get("token_count", 0)),
        "outputs": {
            "summaries_jsonl": str(summaries_path).replace("\\", "/"),
            "keyword_index_json": str(keyword_index_path).replace("\\", "/"),
            "sqlite_fts": str(fts_db_path).replace("\\", "/"),
            "build_report_json": str(report_path).replace("\\", "/"),
        },
    }
    if write:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    # Stash records for in-memory callers
    report["_records"] = records
    return report


def search_memory(project_dir: Optional[Path] = None,
                  query: str = "",
                  limit: int = 10,
                  prefer: str = "auto") -> List[Dict[str, Any]]:
    """Search the local memory index. Tries FTS5 first when available; falls
    back to the keyword index. `prefer` can be 'auto', 'fts', or 'keyword'.
    """
    project_dir = Path(project_dir).resolve() if project_dir else PROJECT_DIR
    fts_db_path = project_dir / "memory" / "luna_fast_recall.sqlite"

    use_fts = (prefer in ("auto", "fts")) and fts_db_path.exists() and sqlite_fts5_available(fts_db_path)
    if use_fts:
        rows = search_sqlite_fts(fts_db_path, query, limit=limit)
        if rows:
            return rows
    if prefer == "fts":
        return []  # caller asked for FTS only

    # Keyword fallback — load summaries from JSONL on disk
    summaries_path = project_dir / "memory" / "luna_memory_summaries.jsonl"
    records: List[Dict[str, Any]] = []
    if summaries_path.exists():
        try:
            for raw in summaries_path.read_text(encoding="utf-8", errors="replace").splitlines():
                s = raw.strip()
                if not s:
                    continue
                try:
                    records.append(json.loads(s))
                except Exception:
                    continue
        except Exception:
            records = []
    if not records:
        # Build in-memory if no on-disk summaries
        sums = build_memory_summaries(project_dir)
        records = sums["records"]
    return search_keyword_index(records, query, limit=limit)


# ---------------------------------------------------------------------------
# Self-test CLI
# ---------------------------------------------------------------------------

def self_test() -> int:
    """Self-test. Builds a tiny in-memory index from synthetic sources in a
    temp project dir, validates record shape, and runs both backends.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        proj = Path(td)
        (proj / "memory").mkdir(parents=True, exist_ok=True)
        (proj / "logs").mkdir(parents=True, exist_ok=True)
        # Seed two synthetic sources
        (proj / "memory" / "nightly_updates.jsonl").write_text(
            "\n".join([
                '{"ts":"2026-05-01T20:00:00","msg":"phase 3 stabilization","tag":"phase3"}',
                '{"ts":"2026-05-01T20:30:00","event":"CU_PAUSED_DIRTY_CORE","msg":"continues_update paused dirty core"}',
                "BAD JSON LINE",
                '{"ts":"2026-05-01T21:00:00","msg":"aider timeout on worker.py"}',
            ]),
            encoding="utf-8",
        )
        (proj / "logs" / "luna_worker.log").write_text(
            "[2026-05-01 20:31:00] worker import: IMPORT_OK\n"
            "[2026-05-01 20:32:00] continues_update paused; reason=paused_dirty_core\n"
            "[2026-05-01 20:33:00] guardian restart budget OK\n",
            encoding="utf-8",
        )
        report = build_memory_index(proj, write=True)
        if not report.get("ok") or report.get("summary_count", 0) < 2:
            print(f"[FAIL] build_memory_index report: {report}")
            return 1
        # Validate every record
        for rec in report.get("_records", []):
            ok, errs = validate_summary_record(rec)
            if not ok:
                print(f"[FAIL] invalid record: {errs}")
                return 2

        # Search via the high-level entrypoint
        hits = search_memory(proj, "phase 3", limit=5)
        if not hits:
            print("[FAIL] expected at least one hit for 'phase 3'")
            return 3

        print(json.dumps({
            "ok": True,
            "summary_count": report["summary_count"],
            "fts5_available": bool(report.get("fts5_report", {}).get("fts5_available")),
            "first_hit_summary_id": (hits[0].get("summary_id")
                                     or hits[0].get("_index", "?")),
            "missing_sources": report.get("missing_sources", []),
        }, indent=2))
    return 0


def _cli(argv: List[str]) -> int:
    args = list(argv or [])
    if "--self-test" in args:
        return self_test()
    if "--build" in args:
        report = build_memory_index(write=True)
        print(json.dumps({k: v for k, v in report.items() if k != "_records"},
                         indent=2, sort_keys=True))
        if "--query" in args:
            try:
                qi = args.index("--query")
                query = args[qi + 1] if qi + 1 < len(args) else ""
            except Exception:
                query = ""
            limit = 10
            if "--limit" in args:
                try:
                    li = args.index("--limit")
                    limit = int(args[li + 1])
                except Exception:
                    limit = 10
            print("---- search results ----")
            print(json.dumps(search_memory(query=query, limit=limit), indent=2, sort_keys=True))
        return 0
    print("luna_memory_index — pass --self-test or --build [--query \"text\" --limit N]")
    return 0


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
