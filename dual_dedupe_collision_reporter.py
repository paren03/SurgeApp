"""Phase 19 - Dual Deduplication / Collision Reporter.

Non-destructive: reports duplicates, pack collisions, cross-category reuse,
and metadata gaps. ``mark_duplicate_candidates`` is dry_run=True by default
and does NOT delete or merge anything.

All listings bounded. No daemon. No network.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Optional


_DEFAULT_LIMIT = 1000
_HARD_LIMIT = 5000


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


def _clamp(n: Optional[int]) -> int:
    if n is None:
        return _DEFAULT_LIMIT
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_LIMIT
    return max(1, min(v, _HARD_LIMIT))


def normalize_key(value: str) -> str:
    return (value or "").strip().lower()


def find_exact_duplicates(language: str, limit: int = _DEFAULT_LIMIT,
                          db_path: Optional[str | Path] = None
                          ) -> list[dict[str, Any]]:
    """Group rows by LOWER(word). Single-row groups are filtered out."""
    _ensure_flags()
    cap = _clamp(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        rows = conn.execute(
            "SELECT LOWER(word) AS key, COUNT(*) AS n "
            "FROM words GROUP BY LOWER(word) HAVING COUNT(*) > 1 "
            "ORDER BY n DESC, key ASC LIMIT ?", (cap,)).fetchall()
    return [{"key": r[0] or "", "count": int(r[1] or 0)} for r in rows]


def find_pack_collisions(language: str, limit: int = _DEFAULT_LIMIT,
                         db_path: Optional[str | Path] = None
                         ) -> list[dict[str, Any]]:
    """Same LOWER(word) appearing in >1 distinct pack_id."""
    _ensure_flags()
    cap = _clamp(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        rows = conn.execute(
            "SELECT LOWER(word) AS key, COUNT(DISTINCT pack_id) AS n_packs "
            "FROM words GROUP BY LOWER(word) "
            "HAVING COUNT(DISTINCT pack_id) > 1 "
            "ORDER BY n_packs DESC, key ASC LIMIT ?", (cap,)).fetchall()
    return [{"key": r[0] or "", "pack_count": int(r[1] or 0)} for r in rows]


def find_cross_category_reuse(language: str, limit: int = _DEFAULT_LIMIT,
                              db_path: Optional[str | Path] = None
                              ) -> list[dict[str, Any]]:
    """Words whose coverage_categories_json string differs between rows."""
    _ensure_flags()
    cap = _clamp(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        rows = conn.execute(
            "SELECT LOWER(word) AS key, "
            "COUNT(DISTINCT coverage_categories_json) AS variant_count "
            "FROM words GROUP BY LOWER(word) "
            "HAVING COUNT(DISTINCT coverage_categories_json) > 1 "
            "ORDER BY variant_count DESC, key ASC LIMIT ?",
            (cap,)).fetchall()
    return [{"key": r[0] or "", "category_variants": int(r[1] or 0)}
            for r in rows]


def find_missing_pack_ids(language: str, limit: int = _DEFAULT_LIMIT,
                          db_path: Optional[str | Path] = None
                          ) -> list[dict[str, Any]]:
    _ensure_flags()
    cap = _clamp(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        rows = conn.execute(
            "SELECT word FROM words WHERE pack_id='' OR pack_id IS NULL LIMIT ?",
            (cap,)).fetchall()
    return [{"word": r[0]} for r in rows]


def find_missing_safety_tags(language: str, limit: int = _DEFAULT_LIMIT,
                             db_path: Optional[str | Path] = None
                             ) -> list[dict[str, Any]]:
    _ensure_flags()
    cap = _clamp(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        rows = conn.execute(
            "SELECT word FROM words WHERE safety_tags_json IN ('[]','') LIMIT ?",
            (cap,)).fetchall()
    return [{"word": r[0]} for r in rows]


def find_missing_register_tags(language: str, limit: int = _DEFAULT_LIMIT,
                               db_path: Optional[str | Path] = None
                               ) -> list[dict[str, Any]]:
    _ensure_flags()
    cap = _clamp(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        rows = conn.execute(
            "SELECT word FROM words WHERE register_tags_json IN ('[]','') LIMIT ?",
            (cap,)).fetchall()
    return [{"word": r[0]} for r in rows]


def score_duplicate_severity(duplicate_group: dict[str, Any]) -> dict[str, Any]:
    """Heuristic severity score on a single duplicate group descriptor."""
    n = int(duplicate_group.get("count") or duplicate_group.get("pack_count")
            or duplicate_group.get("category_variants") or 0)
    if n <= 1:
        sev, label = 0.0, "none"
    elif n == 2:
        sev, label = 0.3, "low"
    elif n <= 4:
        sev, label = 0.6, "moderate"
    elif n <= 8:
        sev, label = 0.8, "high"
    else:
        sev, label = 1.0, "critical"
    return {"severity_score": round(sev, 3),
            "severity_label": label,
            "input": duplicate_group}


def mark_duplicate_candidates(language: str,
                              duplicate_groups: Iterable[dict[str, Any]],
                              dry_run: bool = True,
                              db_path: Optional[str | Path] = None,
                              ) -> dict[str, Any]:
    """Non-destructive marker. Dry_run=True by default and the function will
    NOT mutate any row. If ``dry_run=False`` is explicitly passed, the
    function still refuses to delete - it only sets ``pack_source`` field
    with the suffix '|dup_candidate', and only when explicitly requested.

    For Phase 19 the spec forbids destructive cleanup; we therefore default
    to dry_run=True and treat ``dry_run=False`` as a no-op writeback that
    annotates one column without dropping rows. The caller MUST audit before
    flipping this flag.
    """
    _ensure_flags()
    groups = list(duplicate_groups or [])
    if dry_run:
        return {"ok": True, "dry_run": True,
                "groups_seen": len(groups),
                "would_annotate": [g.get("key") for g in groups
                                   if isinstance(g, dict)][:200],
                "note": "no_mutation_performed"}
    # Even with dry_run=False, we ONLY annotate, never delete or merge.
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    annotated = 0
    with cm as conn:
        for g in groups:
            if not isinstance(g, dict):
                continue
            key = g.get("key")
            if not key:
                continue
            try:
                cur = conn.execute(
                    "UPDATE words "
                    "SET pack_source = pack_source || '|dup_candidate' "
                    "WHERE LOWER(word)=? "
                    "AND INSTR(pack_source, 'dup_candidate')=0",
                    (str(key).lower(),))
                annotated += cur.rowcount
            except Exception:
                continue
    return {"ok": True, "dry_run": False,
            "rows_annotated": int(annotated),
            "groups_seen": len(groups),
            "note": "non_destructive_annotation_only"}


def write_dedupe_report(report: dict[str, Any],
                        output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "normalize_key",
    "find_exact_duplicates",
    "find_pack_collisions",
    "find_cross_category_reuse",
    "find_missing_pack_ids",
    "find_missing_safety_tags",
    "find_missing_register_tags",
    "score_duplicate_severity",
    "mark_duplicate_candidates",
    "write_dedupe_report",
]
