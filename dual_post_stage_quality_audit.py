"""Phase 20 - Post-Stage Quality Audit.

After a dry-run or real stage, sample recent rows and audit:
    * metadata completeness
    * language consistency
    * safety/register/coverage tag consistency
    * duplicate rate (bounded)
    * rejected-row summary (bounded)
Returns a per-stage quality_score and writes a JSON report.

Read-only on production DBs. No daemon, no scheduler.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from coverage_taxonomy import (
    COVERAGE_CATEGORIES, REGISTER_TAGS, SAFETY_TAGS,
)


_DEFAULT_SAMPLE = 500
_HARD_SAMPLE = 5000


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
        return _DEFAULT_SAMPLE
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_SAMPLE
    return max(1, min(v, _HARD_SAMPLE))


def sample_recent_import_rows(language: str,
                              batch_id: Optional[str] = None,
                              pack_id: Optional[str] = None,
                              limit: int = _DEFAULT_SAMPLE,
                              db_path: Optional[str | Path] = None
                              ) -> list[dict[str, Any]]:
    """Pull a bounded sample of the most recently inserted rows.

    Lexicon stores carry a ``created_at`` column; we order by it DESC.
    """
    _ensure_flags()
    cap = _clamp(limit)
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    where: list[str] = []
    args: list[Any] = []
    if pack_id:
        where.append("pack_id=?")
        args.append(str(pack_id))
    sql = ("SELECT * FROM words"
           + (" WHERE " + " AND ".join(where) if where else "")
           + " ORDER BY created_at DESC LIMIT ?")
    args.append(cap)
    with cm as conn:
        try:
            rows = conn.execute(sql, tuple(args)).fetchall()
        except Exception:
            return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(dict(r))
    return out


def _parse_json_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            d = json.loads(v)
            if isinstance(d, list):
                return [str(x) for x in d]
        except Exception:
            return []
    return []


def audit_metadata_completeness(rows: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    if n == 0:
        return {"ok": True, "n": 0, "completeness_ratio": 0.0,
                "missing_word": 0, "missing_definition": 0,
                "missing_coverage": 0, "missing_register": 0}
    mw = sum(1 for r in rows if not (r.get("word") or "").strip())
    md = sum(1 for r in rows if not (r.get("definition") or "").strip())
    mc = sum(1 for r in rows
             if not _parse_json_list(r.get("coverage_categories_json")
                                     or r.get("coverage_categories")))
    mr = sum(1 for r in rows
             if not _parse_json_list(r.get("register_tags_json")
                                     or r.get("register_tags")))
    completeness = round(1.0 - (mw + md + mc + mr) / (4 * n), 3)
    return {"ok": True, "n": n,
            "completeness_ratio": max(0.0, completeness),
            "missing_word": mw, "missing_definition": md,
            "missing_coverage": mc, "missing_register": mr}


def audit_language_consistency(rows: list[dict[str, Any]],
                               expected_language: str) -> dict[str, Any]:
    n = len(rows)
    if n == 0:
        return {"ok": True, "n": 0, "mismatch_count": 0,
                "mismatch_ratio": 0.0}
    mismatch = 0
    for r in rows:
        w = (r.get("word") or "").strip()
        has_cyr = any("Ѐ" <= c <= "ӿ" for c in w)
        if expected_language == "ru" and not has_cyr:
            mismatch += 1
        elif expected_language == "en" and has_cyr:
            mismatch += 1
    return {"ok": True, "n": n, "expected": expected_language,
            "mismatch_count": mismatch,
            "mismatch_ratio": round(mismatch / n, 3)}


def _ratio_invalid_tags(rows: list[dict[str, Any]], col: str,
                       valid: set[str]) -> dict[str, Any]:
    n = len(rows)
    if n == 0:
        return {"ok": True, "n": 0, "invalid_rows": 0, "invalid_ratio": 0.0}
    bad = 0
    for r in rows:
        tags = set(_parse_json_list(r.get(col)))
        if tags and not tags.issubset(valid):
            bad += 1
    return {"ok": True, "n": n, "invalid_rows": bad,
            "invalid_ratio": round(bad / n, 3)}


def audit_safety_tag_consistency(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _ratio_invalid_tags(rows, "safety_tags_json", set(SAFETY_TAGS))


def audit_register_tag_consistency(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _ratio_invalid_tags(rows, "register_tags_json", set(REGISTER_TAGS))


def audit_coverage_category_consistency(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _ratio_invalid_tags(rows, "coverage_categories_json",
                               set(COVERAGE_CATEGORIES))


def audit_duplicate_rate(language: str,
                         batch_id: Optional[str] = None,
                         db_path: Optional[str | Path] = None
                         ) -> dict[str, Any]:
    _ensure_flags()
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        try:
            n_words = int(conn.execute(
                "SELECT COUNT(*) FROM words").fetchone()[0])
            n_keys = int(conn.execute(
                "SELECT COUNT(DISTINCT LOWER(word)) FROM words").fetchone()[0])
        except Exception:
            return {"ok": False, "error": "query_failed"}
    duplicates = n_words - n_keys
    ratio = round(duplicates / max(1, n_words), 4)
    return {"ok": True, "n_words": n_words, "distinct_words": n_keys,
            "duplicates_estimated": int(duplicates),
            "duplicate_ratio": ratio}


def audit_rejected_rows(language: str,
                        batch_id: Optional[str] = None,
                        limit: int = _DEFAULT_SAMPLE) -> dict[str, Any]:
    """Phase 16 importer writes rejections to a per-source jsonl. We sample
    the most recent rejection log under corpus_sources/<lang>/rejected/."""
    cap = _clamp(limit)
    sub = "english" if language == "en" else "russian"
    base = Path("corpus_sources") / sub / "rejected"
    if not base.exists():
        return {"ok": True, "n": 0, "samples": [],
                "note": "no_rejections_dir"}
    files = sorted([p for p in base.glob("*.jsonl")],
                   key=lambda x: x.stat().st_mtime, reverse=True)
    samples: list[dict[str, Any]] = []
    for f in files[:5]:
        try:
            with f.open("r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if len(samples) >= cap:
                        break
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        samples.append(json.loads(s))
                    except Exception:
                        continue
        except Exception:
            continue
        if len(samples) >= cap:
            break
    return {"ok": True, "n": len(samples), "samples": samples}


def compute_stage_quality_score(audit_results: dict[str, Any]) -> dict[str, Any]:
    comp = float((audit_results.get("metadata_completeness") or {}).get("completeness_ratio", 0.0))
    lang_mm = float((audit_results.get("language_consistency") or {}).get("mismatch_ratio", 0.0))
    saf = float((audit_results.get("safety_consistency") or {}).get("invalid_ratio", 0.0))
    reg = float((audit_results.get("register_consistency") or {}).get("invalid_ratio", 0.0))
    cov = float((audit_results.get("coverage_consistency") or {}).get("invalid_ratio", 0.0))
    dup = float((audit_results.get("duplicate_rate") or {}).get("duplicate_ratio", 0.0))
    score = (
        comp * 0.55
        + (1.0 - min(1.0, lang_mm * 2.0)) * 0.10
        + (1.0 - min(1.0, saf * 5.0)) * 0.10
        + (1.0 - min(1.0, reg * 5.0)) * 0.10
        + (1.0 - min(1.0, cov * 5.0)) * 0.10
        + (1.0 - min(1.0, dup * 10.0)) * 0.05
    )
    return {"ok": True, "quality_score": round(max(0.0, min(1.0, score)), 3),
            "verdict": ("pass" if score >= 0.75
                        else "warn" if score >= 0.55 else "fail")}


def write_post_stage_quality_audit(audit: dict[str, Any],
                                   output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(audit)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "sample_recent_import_rows",
    "audit_metadata_completeness",
    "audit_language_consistency",
    "audit_safety_tag_consistency",
    "audit_register_tag_consistency",
    "audit_coverage_category_consistency",
    "audit_duplicate_rate",
    "audit_rejected_rows",
    "compute_stage_quality_score",
    "write_post_stage_quality_audit",
]
