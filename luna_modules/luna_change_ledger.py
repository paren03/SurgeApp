"""Luna Change Ledger — append-only record of every Luna-driven change.

Phase 5C foundation: this module is NOT wired into runtime services. It
provides a stable, validated, JSONL-backed ledger that later phases (5F
upgrade gate, 5K limited autonomous improvement) will call BEFORE every
self-edit. The ledger is the source of truth for "what changed, when,
why, by whom, with what verification, and how to roll it back."

Design:
- Pure stdlib only (json, hashlib, uuid, datetime, pathlib, typing,
  dataclasses, os, re).
- Append-only: append_change_record() never rewrites. One JSON object
  per line. ensure_ascii=True for robust Windows behavior.
- Schema-validated: validate_change_record() returns (ok, errors).
- Path-safe: normalize_target_file() refuses traversal outside
  project_dir when project_dir is supplied.
- Function-aware: infer_affected_functions() reads the optional Phase
  5B index (memory/luna_function_index.json) and finds overlapping
  symbols by line range.
- Read-resilient: read_change_records() / find_change_records() /
  summarize_change_records() skip corrupt rows and surface a count.

CLI for manual smoke testing (does not run automatically):
    python -m luna_modules.luna_change_ledger --self-test
"""
from __future__ import annotations

import json
import hashlib
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1

REQUIRED_FIELDS: Tuple[str, ...] = (
    "schema_version",
    "ledger_id",
    "ts",
    "actor",
    "action_type",
    "target_files",
    "reason",
    "status",
)

OPTIONAL_FIELDS: Tuple[str, ...] = (
    "affected_functions",
    "line_ranges",
    "plan_id",
    "diff_hash",
    "diff_path",
    "verification",
    "rollback_path",
    "commit_hash",
    "risk_score",
    "notes",
)

# Allowed action_type values. Adding new types here is a low-risk additive
# change; removing or renaming would break readers, so don't.
ALLOWED_ACTION_TYPES: Tuple[str, ...] = (
    "edit",
    "additive",
    "refactor",
    "rollback",
    "delete",
    "rename",
    "move",
    "create",
    "config",
    "telemetry",
    "verify_only",
)

# Allowed status values for a record at write time.
ALLOWED_STATUSES: Tuple[str, ...] = (
    "proposed",
    "approved",
    "applied",
    "verified",
    "failed",
    "rolled_back",
    "rejected",
    "noop",
)

# Resolve project root from this module's location: luna_modules/<this>.py
_THIS_FILE = Path(__file__).resolve()
PROJECT_DIR = _THIS_FILE.parent.parent

DEFAULT_LEDGER_PATH = PROJECT_DIR / "memory" / "luna_change_ledger.jsonl"
DEFAULT_FUNCTION_INDEX_PATH = PROJECT_DIR / "memory" / "luna_function_index.json"


# ---------------------------------------------------------------------------
# Small primitives
# ---------------------------------------------------------------------------

def now_iso() -> str:
    """UTC ISO-8601 with timezone — comparable + sortable."""
    return datetime.now(timezone.utc).isoformat()


def make_ledger_id(prefix: str = "chg") -> str:
    """Time-prefixed ULID-ish id. ts_micro + 8 random hex chars."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    rand = uuid.uuid4().hex[:8]
    return f"{prefix}_{ts}_{rand}"


def sha256_text(text: str) -> str:
    """Stable SHA-256 of `text` (UTF-8 encoded)."""
    h = hashlib.sha256()
    h.update((text or "").encode("utf-8"))
    return h.hexdigest()


def sha256_file(path: Path) -> str:
    """SHA-256 of file contents. Returns '' on read error."""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1_048_576), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------

def normalize_target_file(path_or_text: str,
                          project_dir: Optional[Path] = None) -> str:
    """Return a project-relative POSIX-style path string.

    If `project_dir` is provided, refuses path traversal outside that
    project (raises ValueError). When the input is already relative, it is
    returned unchanged with backslashes normalized to forward slashes.

    The empty string is returned unchanged so the function is safe to call
    on optional fields.
    """
    raw = "" if path_or_text is None else str(path_or_text).strip()
    if not raw:
        return ""

    # Normalize separators first
    cleaned = raw.replace("\\", "/").rstrip("/")

    # Reject obvious traversal markers when inside a project
    if project_dir is not None and (".." in Path(cleaned).parts):
        raise ValueError(f"path traversal not allowed: {raw!r}")

    # If absolute, try to relativize against project_dir
    p = Path(cleaned)
    if p.is_absolute():
        if project_dir is None:
            return str(p).replace("\\", "/")
        try:
            rel = p.resolve().relative_to(Path(project_dir).resolve())
        except ValueError:
            raise ValueError(
                f"target file is outside project_dir: "
                f"{cleaned!r} not under {project_dir!s}"
            )
        return str(rel).replace("\\", "/")

    return str(p).replace("\\", "/")


# ---------------------------------------------------------------------------
# Build / validate
# ---------------------------------------------------------------------------

def build_change_record(
    *,
    actor: str,
    action_type: str,
    target_files: Iterable[str],
    reason: str,
    status: str = "proposed",
    affected_functions: Optional[List[Dict[str, Any]]] = None,
    line_ranges: Optional[List[List[int]]] = None,
    plan_id: str = "",
    diff_hash: str = "",
    diff_path: str = "",
    verification: Optional[Dict[str, Any]] = None,
    rollback_path: str = "",
    commit_hash: str = "",
    risk_score: Optional[int] = None,
    notes: str = "",
    project_dir: Optional[Path] = None,
    ledger_id: Optional[str] = None,
    ts: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a fully populated record dict ready to validate + append."""
    norm_targets = [
        normalize_target_file(t, project_dir=project_dir) for t in (target_files or [])
        if str(t or "").strip()
    ]
    rec: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "ledger_id": ledger_id or make_ledger_id(),
        "ts": ts or now_iso(),
        "actor": str(actor or "").strip(),
        "action_type": str(action_type or "").strip(),
        "target_files": norm_targets,
        "affected_functions": list(affected_functions or []),
        "line_ranges": [list(r) for r in (line_ranges or [])],
        "reason": str(reason or "").strip(),
        "plan_id": str(plan_id or ""),
        "diff_hash": str(diff_hash or ""),
        "diff_path": str(diff_path or ""),
        # Verification placeholder includes secret_scan="not_run" per spec
        "verification": dict(verification) if verification else {"secret_scan": "not_run"},
        "rollback_path": str(rollback_path or ""),
        "commit_hash": str(commit_hash or ""),
        "risk_score": (None if risk_score is None else int(risk_score)),
        "status": str(status or "").strip(),
        "notes": str(notes or ""),
    }
    # Make sure verification.secret_scan is present even when caller passed
    # a verification dict that omitted it.
    if isinstance(rec["verification"], dict) and "secret_scan" not in rec["verification"]:
        rec["verification"]["secret_scan"] = "not_run"
    return rec


def validate_change_record(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Return (ok, errors) for a record dict. Cheap; no file I/O."""
    errors: List[str] = []
    if not isinstance(record, dict):
        return False, ["record is not a dict"]

    # Required fields presence
    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"missing required field: {field}")
            continue
        val = record[field]
        if field == "schema_version":
            if not isinstance(val, int) or val < 1:
                errors.append(f"schema_version must be int >= 1, got {val!r}")
        elif field == "target_files":
            if not isinstance(val, list) or not val:
                errors.append("target_files must be a non-empty list")
            elif not all(isinstance(x, str) and x for x in val):
                errors.append("target_files must contain non-empty strings")
        elif field in ("ledger_id", "ts", "actor", "action_type", "reason", "status"):
            if not isinstance(val, str) or not val.strip():
                errors.append(f"{field} must be a non-empty string")

    # action_type allowed
    if isinstance(record.get("action_type"), str) and record["action_type"]:
        if record["action_type"] not in ALLOWED_ACTION_TYPES:
            errors.append(
                f"action_type {record['action_type']!r} not in "
                f"{ALLOWED_ACTION_TYPES}"
            )

    # status allowed
    if isinstance(record.get("status"), str) and record["status"]:
        if record["status"] not in ALLOWED_STATUSES:
            errors.append(
                f"status {record['status']!r} not in {ALLOWED_STATUSES}"
            )

    # Optional field types (only if present)
    if "line_ranges" in record:
        lr = record["line_ranges"]
        if not isinstance(lr, list):
            errors.append("line_ranges must be a list")
        else:
            for i, span in enumerate(lr):
                if not (isinstance(span, list) and len(span) == 2
                        and all(isinstance(x, int) for x in span)):
                    errors.append(f"line_ranges[{i}] must be [int, int]")
    if "affected_functions" in record and not isinstance(
            record["affected_functions"], list):
        errors.append("affected_functions must be a list")
    if "verification" in record and not isinstance(record["verification"], dict):
        errors.append("verification must be a dict")
    if "risk_score" in record:
        rs = record["risk_score"]
        if rs is not None and not isinstance(rs, int):
            errors.append("risk_score must be int or None")
        elif isinstance(rs, int) and not (0 <= rs <= 10):
            errors.append("risk_score must be 0..10")

    return (len(errors) == 0), errors


# ---------------------------------------------------------------------------
# Append / read / find / summarize
# ---------------------------------------------------------------------------

def append_change_record(record: Dict[str, Any],
                         ledger_path: Optional[Path] = None) -> Dict[str, Any]:
    """Validate then append a single record as one JSONL line.

    - Creates parent folders if needed.
    - Never rewrites prior rows.
    - Returns the same record on success.
    - Raises ValueError on validation failure (caller decides how to log).
    """
    target = Path(ledger_path) if ledger_path is not None else DEFAULT_LEDGER_PATH
    ok, errors = validate_change_record(record)
    if not ok:
        raise ValueError(f"invalid change record: {errors}")
    target.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=True, sort_keys=True)
    # Open in append-binary so we don't accidentally rewrite or truncate.
    with open(target, "ab") as f:
        f.write(line.encode("utf-8") + b"\n")
    return record


def read_change_records(ledger_path: Optional[Path] = None,
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Read up to `limit` valid records from the ledger.

    Corrupt JSONL rows are skipped silently (count exposed via
    summarize_change_records). Missing ledger file returns [].
    """
    target = Path(ledger_path) if ledger_path is not None else DEFAULT_LEDGER_PATH
    if not target.exists():
        return []
    out: List[Dict[str, Any]] = []
    cap = None if limit is None else max(0, int(limit))
    if cap == 0:
        return out
    try:
        with open(target, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if isinstance(rec, dict):
                    out.append(rec)
                    if cap is not None and len(out) >= cap:
                        break
    except Exception:
        return out
    return out


def find_change_records(
    ledger_path: Optional[Path] = None,
    target: Optional[str] = None,
    actor: Optional[str] = None,
    plan_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Filter records by target_file (substring match), actor, and/or plan_id.

    Always returns the most-recent-first slice (records are appended in time
    order, so we read all and reverse before applying limit).
    """
    all_records = read_change_records(ledger_path=ledger_path, limit=None)
    # Most recent first
    all_records.reverse()
    out: List[Dict[str, Any]] = []
    target_norm = (target or "").replace("\\", "/").strip().lower()
    for rec in all_records:
        if actor is not None and str(rec.get("actor") or "") != str(actor):
            continue
        if plan_id is not None and str(rec.get("plan_id") or "") != str(plan_id):
            continue
        if target_norm:
            tf = [str(t or "").replace("\\", "/").lower()
                  for t in (rec.get("target_files") or [])]
            if not any(target_norm in t for t in tf):
                continue
        out.append(rec)
        if len(out) >= max(0, int(limit)):
            break
    return out


def summarize_change_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Produce a small summary: counts by actor, status, and target_file.

    Treats `records` as the already-loaded subset (the caller chooses scope).
    Counts only entries that pass minimal shape checks; the rest go into
    `corrupt_count`.
    """
    by_actor: Dict[str, int] = {}
    by_status: Dict[str, int] = {}
    by_target: Dict[str, int] = {}
    by_action_type: Dict[str, int] = {}
    corrupt = 0
    earliest = None
    latest = None
    for rec in records or []:
        if not isinstance(rec, dict):
            corrupt += 1
            continue
        actor = str(rec.get("actor") or "")
        status = str(rec.get("status") or "")
        action = str(rec.get("action_type") or "")
        ts = str(rec.get("ts") or "")
        if actor:
            by_actor[actor] = by_actor.get(actor, 0) + 1
        if status:
            by_status[status] = by_status.get(status, 0) + 1
        if action:
            by_action_type[action] = by_action_type.get(action, 0) + 1
        for t in rec.get("target_files") or []:
            tt = str(t or "").replace("\\", "/")
            if tt:
                by_target[tt] = by_target.get(tt, 0) + 1
        if ts:
            if earliest is None or ts < earliest:
                earliest = ts
            if latest is None or ts > latest:
                latest = ts
    return {
        "total": len(records or []),
        "corrupt_count": corrupt,
        "by_actor": dict(sorted(by_actor.items())),
        "by_status": dict(sorted(by_status.items())),
        "by_action_type": dict(sorted(by_action_type.items())),
        "by_target": dict(sorted(by_target.items())),
        "earliest_ts": earliest or "",
        "latest_ts": latest or "",
    }


# ---------------------------------------------------------------------------
# Function-index integration
# ---------------------------------------------------------------------------

def _ranges_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    """Half-open-friendly overlap. -1 in a span means 'to end of file'."""
    if a_end == -1:
        a_end = 10 ** 9
    if b_end == -1:
        b_end = 10 ** 9
    return a_start <= b_end and b_start <= a_end


def infer_affected_functions(
    target_file: str,
    line_ranges: List[List[int]],
    function_index_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """Return symbol entries from luna_function_index.json that overlap.

    - If the index file is missing or unreadable, returns [].
    - Each returned entry includes name, kind, parent, file, start_line,
      end_line, risk_level (when available in the index).
    - line_ranges of [] returns []. Caller can choose to treat that as
      "whole-file edit" elsewhere.
    """
    if not target_file or not line_ranges:
        return []
    path = Path(function_index_path) if function_index_path is not None \
        else DEFAULT_FUNCTION_INDEX_PATH
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, dict):
        return []
    symbols = data.get("symbols") or []
    if not isinstance(symbols, list):
        return []
    target_norm = str(target_file).replace("\\", "/")
    out: List[Dict[str, Any]] = []
    for sym in symbols:
        if not isinstance(sym, dict):
            continue
        if str(sym.get("file") or "").replace("\\", "/") != target_norm:
            continue
        try:
            s_start = int(sym.get("start_line") or 0)
            s_end = int(sym.get("end_line") or 0)
        except Exception:
            continue
        for span in line_ranges:
            if not (isinstance(span, list) and len(span) == 2):
                continue
            try:
                a, b = int(span[0]), int(span[1])
            except Exception:
                continue
            if _ranges_overlap(a, b, s_start, s_end):
                out.append({
                    "name": sym.get("name", ""),
                    "kind": sym.get("kind", ""),
                    "parent": sym.get("parent", ""),
                    "file": sym.get("file", ""),
                    "start_line": s_start,
                    "end_line": s_end,
                    "risk_level": sym.get("risk_level", ""),
                })
                break
    # Deterministic order: by start_line, name
    out.sort(key=lambda r: (int(r.get("start_line") or 0), r.get("name", "")))
    return out


# ---------------------------------------------------------------------------
# Optional CLI
# ---------------------------------------------------------------------------

def _self_test() -> int:
    """Build → validate → append → read → summarize a throwaway record.

    Writes to a temp file, never to the real ledger. Returns 0 on success.
    """
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        ledger = Path(td) / "ledger.jsonl"
        rec = build_change_record(
            actor="self_test",
            action_type="verify_only",
            target_files=["luna_modules/luna_change_ledger.py"],
            reason="phase 5C self-test",
            status="verified",
            line_ranges=[[1, 50]],
            risk_score=1,
        )
        ok, errs = validate_change_record(rec)
        if not ok:
            print(f"[FAIL] validation errors: {errs}")
            return 1
        append_change_record(rec, ledger_path=ledger)
        records = read_change_records(ledger_path=ledger)
        if not records or records[0]["ledger_id"] != rec["ledger_id"]:
            print("[FAIL] read mismatch")
            return 2
        summary = summarize_change_records(records)
        print(json.dumps({
            "ok": True,
            "record_count": len(records),
            "summary": summary,
        }, indent=2))
    return 0


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        sys.exit(_self_test())
    print("luna_change_ledger module — pass --self-test to run a smoke test")
    sys.exit(0)
