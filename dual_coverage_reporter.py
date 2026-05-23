"""Phase 17 - Dual Coverage Reporter.

Read-only aggregate reporting against the English and Russian sovereign
lexicon stores. SQL aggregates only - no full-row loads. Categories are
extracted by ``LIKE '%"<tag>"%'`` against the per-row JSON columns; that
matches the existing on-disk convention used everywhere else in the stack.
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


_DEFAULT_LISTING_LIMIT = 100
_HARD_LISTING_MAX = 1000


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


def _count_with_pat(conn: sqlite3.Connection, table: str,
                    column: str, pat: str) -> int:
    cur = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} LIKE ?", (pat,))
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def count_entries_by_language(en_db_path: Optional[str | Path] = None,
                              ru_db_path: Optional[str | Path] = None
                              ) -> dict[str, int]:
    _ensure_flags()
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    return {
        "en_words": int(enlex.count_words(en_db_path)),
        "ru_words": int(rulex.count_words(ru_db_path)),
        "ru_phrases": int(rulex.count_phrases(ru_db_path)),
    }


def count_entries_by_coverage_category(language: str,
                                       db_path: Optional[str | Path] = None
                                       ) -> dict[str, int]:
    _ensure_flags()
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    out: dict[str, int] = {}
    with cm as conn:
        for cat in COVERAGE_CATEGORIES:
            pat = f'%"{cat}"%'
            out[cat] = _count_with_pat(conn, "words",
                                       "coverage_categories_json", pat)
    return out


def count_entries_by_register_tag(language: str,
                                  db_path: Optional[str | Path] = None
                                  ) -> dict[str, int]:
    _ensure_flags()
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    out: dict[str, int] = {}
    with cm as conn:
        for tag in REGISTER_TAGS:
            pat = f'%"{tag}"%'
            out[tag] = _count_with_pat(conn, "words",
                                       "register_tags_json", pat)
    return out


def count_entries_by_safety_tag(language: str,
                                db_path: Optional[str | Path] = None
                                ) -> dict[str, int]:
    _ensure_flags()
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    out: dict[str, int] = {}
    with cm as conn:
        for tag in SAFETY_TAGS:
            pat = f'%"{tag}"%'
            out[tag] = _count_with_pat(conn, "words",
                                       "safety_tags_json", pat)
    return out


def count_entries_by_pack_id(language: str, limit: int = _DEFAULT_LISTING_LIMIT,
                             db_path: Optional[str | Path] = None
                             ) -> list[dict[str, Any]]:
    _ensure_flags()
    cap = max(1, min(int(limit), _HARD_LISTING_MAX))
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        cur = conn.execute(
            "SELECT pack_id, COUNT(*) AS n FROM words "
            "GROUP BY pack_id ORDER BY n DESC LIMIT ?", (cap,))
        rows = cur.fetchall()
    return [{"pack_id": r[0] or "", "count": int(r[1] or 0)} for r in rows]


def identify_low_coverage_categories(language: str,
                                     min_entries: int = 100,
                                     db_path: Optional[str | Path] = None
                                     ) -> list[dict[str, Any]]:
    counts = count_entries_by_coverage_category(language, db_path=db_path)
    return [{"category": c, "count": n, "min_entries": int(min_entries)}
            for c, n in sorted(counts.items()) if n < int(min_entries)]


def identify_missing_metadata(language: str, limit: int = _DEFAULT_LISTING_LIMIT,
                              db_path: Optional[str | Path] = None
                              ) -> dict[str, Any]:
    _ensure_flags()
    cap = max(1, min(int(limit), _HARD_LISTING_MAX))
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        no_cov = conn.execute(
            "SELECT COUNT(*) FROM words WHERE coverage_categories_json IN ('[]','')").fetchone()[0]
        no_reg = conn.execute(
            "SELECT COUNT(*) FROM words WHERE register_tags_json IN ('[]','')").fetchone()[0]
        sample = conn.execute(
            "SELECT word FROM words "
            "WHERE coverage_categories_json IN ('[]','') "
            "OR register_tags_json IN ('[]','') "
            "LIMIT ?", (cap,)).fetchall()
    return {"language": language,
            "rows_with_no_coverage": int(no_cov or 0),
            "rows_with_no_register": int(no_reg or 0),
            "samples": [s[0] for s in sample]}


def compare_english_russian_category_balance(
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    en_counts = count_entries_by_coverage_category("en", db_path=en_db_path)
    ru_counts = count_entries_by_coverage_category("ru", db_path=ru_db_path)
    rows: list[dict[str, Any]] = []
    for cat in COVERAGE_CATEGORIES:
        e = int(en_counts.get(cat, 0))
        r = int(ru_counts.get(cat, 0))
        diff = e - r
        rows.append({"category": cat, "en": e, "ru": r,
                     "diff_en_minus_ru": diff})
    return {"per_category": rows,
            "en_total": sum(int(v) for v in en_counts.values()),
            "ru_total": sum(int(v) for v in ru_counts.values())}


def _count_manifests() -> int:
    base = Path("seed_packs")
    if not base.exists():
        return 0
    en = sum(1 for _ in base.glob("en/*.en_pack_manifest.json"))
    ru = sum(1 for _ in base.glob("ru/*.ru_pack_manifest.json"))
    return en + ru


def write_coverage_report(output_path: str | Path,
                          en_db_path: Optional[str | Path] = None,
                          ru_db_path: Optional[str | Path] = None,
                          ) -> dict[str, Any]:
    _ensure_flags()
    totals = count_entries_by_language(en_db_path=en_db_path,
                                       ru_db_path=ru_db_path)
    en_cov = count_entries_by_coverage_category("en", db_path=en_db_path)
    ru_cov = count_entries_by_coverage_category("ru", db_path=ru_db_path)
    en_reg = count_entries_by_register_tag("en", db_path=en_db_path)
    ru_reg = count_entries_by_register_tag("ru", db_path=ru_db_path)
    en_safe = count_entries_by_safety_tag("en", db_path=en_db_path)
    ru_safe = count_entries_by_safety_tag("ru", db_path=ru_db_path)
    en_low = identify_low_coverage_categories("en", db_path=en_db_path)
    ru_low = identify_low_coverage_categories("ru", db_path=ru_db_path)
    en_gaps = identify_missing_metadata("en", db_path=en_db_path)
    ru_gaps = identify_missing_metadata("ru", db_path=ru_db_path)
    balance = compare_english_russian_category_balance(en_db_path=en_db_path,
                                                       ru_db_path=ru_db_path)
    en_packs = count_entries_by_pack_id("en", limit=200, db_path=en_db_path)
    ru_packs = count_entries_by_pack_id("ru", limit=200, db_path=ru_db_path)
    manifests = _count_manifests()

    recommended_next = []
    for row in balance["per_category"]:
        if row["en"] < 50:
            recommended_next.append({"language": "en", "category": row["category"],
                                     "current": row["en"], "reason": "below_50"})
        if row["ru"] < 50:
            recommended_next.append({"language": "ru", "category": row["category"],
                                     "current": row["ru"], "reason": "below_50"})

    report = {
        "generated_at": time.time(),
        "totals": totals,
        "manifest_count": manifests,
        "coverage": {"en": en_cov, "ru": ru_cov},
        "register": {"en": en_reg, "ru": ru_reg},
        "safety": {"en": en_safe, "ru": ru_safe},
        "low_coverage_categories": {"en": en_low, "ru": ru_low},
        "metadata_gaps": {"en": en_gaps, "ru": ru_gaps},
        "english_russian_balance": balance,
        "pack_id_top": {"en": en_packs, "ru": ru_packs},
        "recommended_next_imports": recommended_next,
    }
    outp = Path(output_path)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(report, ensure_ascii=False, indent=2,
                               default=str), encoding="utf-8")
    return {"ok": True, "report_path": str(outp), "report": report}


__all__ = [
    "count_entries_by_language",
    "count_entries_by_coverage_category",
    "count_entries_by_register_tag",
    "count_entries_by_safety_tag",
    "count_entries_by_pack_id",
    "identify_low_coverage_categories",
    "identify_missing_metadata",
    "compare_english_russian_category_balance",
    "write_coverage_report",
]
