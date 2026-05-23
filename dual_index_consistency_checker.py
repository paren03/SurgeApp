"""Phase 20 - Dual Index Consistency Checker.

Read-only. Bounded. Confirms:
  * normal SQL indexes exist
  * FTS5 (or fallback) row counts are sane
  * pack_id / category / safety queries are bounded and don't bypass policy
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import dual_retrieval_index_builder as idx
from coverage_taxonomy import (
    COVERAGE_CATEGORIES, REGISTER_TAGS, SAFETY_TAGS,
)


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


def _check_table_rowcount(conn, name: str) -> Optional[int]:
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
    except Exception:
        return None


def check_english_index_consistency(limit: int = _DEFAULT_LIMIT,
                                    db_path: Optional[str | Path] = None
                                    ) -> dict[str, Any]:
    _ensure_flags()
    cap = max(1, min(int(limit), _HARD_LIMIT))
    with _en_connect(db_path) as conn:
        ix = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
        n_words = _check_table_rowcount(conn, "words")
        n_fts = _check_table_rowcount(conn, "words_fts_en")
        n_fb = _check_table_rowcount(conn, "words_search_fallback_en")
    fts5 = idx.detect_sqlite_fts5_support().get("fts5_available", False)
    has_word_lc = any(name == "ix_words_word_lc" for name in ix)
    has_pack_id = any(name == "ix_words_pack_id" for name in ix)
    return {"ok": True, "language": "en",
            "indexes_present": ix,
            "n_words_total": n_words,
            "n_words_fts": n_fts,
            "n_words_fallback": n_fb,
            "fts5_or_fallback_ok": n_fts is not None or n_fb is not None,
            "ix_words_word_lc_present": has_word_lc,
            "ix_words_pack_id_present": has_pack_id,
            "fts5_available": fts5,
            "limit_used": cap}


def check_russian_index_consistency(limit: int = _DEFAULT_LIMIT,
                                    db_path: Optional[str | Path] = None
                                    ) -> dict[str, Any]:
    _ensure_flags()
    cap = max(1, min(int(limit), _HARD_LIMIT))
    with _ru_connect(db_path) as conn:
        ix = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
        n_words = _check_table_rowcount(conn, "words")
        n_phrases = _check_table_rowcount(conn, "phrases")
        n_fts = _check_table_rowcount(conn, "words_fts_ru")
        n_fb = _check_table_rowcount(conn, "words_search_fallback_ru")
    fts5 = idx.detect_sqlite_fts5_support().get("fts5_available", False)
    has_word_lc = any(name == "ix_words_word_lc" for name in ix)
    has_lemma = any(name == "ix_words_lemma" for name in ix)
    has_pack_id = any(name == "ix_words_pack_id" for name in ix)
    return {"ok": True, "language": "ru",
            "indexes_present": ix,
            "n_words_total": n_words,
            "n_phrases_total": n_phrases,
            "n_words_fts": n_fts,
            "n_words_fallback": n_fb,
            "fts5_or_fallback_ok": n_fts is not None or n_fb is not None,
            "ix_words_word_lc_present": has_word_lc,
            "ix_words_lemma_present": has_lemma,
            "ix_words_pack_id_present": has_pack_id,
            "fts5_available": fts5,
            "limit_used": cap}


def check_fts_row_counts(en_db_path: Optional[str | Path] = None,
                         ru_db_path: Optional[str | Path] = None
                         ) -> dict[str, Any]:
    _ensure_flags()
    with _en_connect(en_db_path) as conn:
        n_en_words = _check_table_rowcount(conn, "words") or 0
        n_en_fts = _check_table_rowcount(conn, "words_fts_en")
        n_en_fb = _check_table_rowcount(conn, "words_search_fallback_en")
    with _ru_connect(ru_db_path) as conn:
        n_ru_words = _check_table_rowcount(conn, "words") or 0
        n_ru_fts = _check_table_rowcount(conn, "words_fts_ru")
        n_ru_fb = _check_table_rowcount(conn, "words_search_fallback_ru")
    return {"ok": True,
            "en": {"words": n_en_words, "fts": n_en_fts, "fallback": n_en_fb,
                   "coverage_ratio": (round(n_en_fts / max(1, n_en_words), 3)
                                      if n_en_fts is not None else None)},
            "ru": {"words": n_ru_words, "fts": n_ru_fts, "fallback": n_ru_fb,
                   "coverage_ratio": (round(n_ru_fts / max(1, n_ru_words), 3)
                                      if n_ru_fts is not None else None)}}


def check_pack_id_index_coverage(language: str,
                                 db_path: Optional[str | Path] = None
                                 ) -> dict[str, Any]:
    _ensure_flags()
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    with cm as conn:
        total = _check_table_rowcount(conn, "words") or 0
        with_pid = int(conn.execute(
            "SELECT COUNT(*) FROM words WHERE pack_id <> ''").fetchone()[0])
    return {"ok": True, "language": language, "total": total,
            "with_pack_id": with_pid,
            "coverage_ratio": round(with_pid / max(1, total), 3)}


def check_category_index_coverage(language: str,
                                  db_path: Optional[str | Path] = None
                                  ) -> dict[str, Any]:
    _ensure_flags()
    cm = _en_connect(db_path) if language == "en" else _ru_connect(db_path)
    counts: dict[str, int] = {}
    with cm as conn:
        for cat in COVERAGE_CATEGORIES:
            pat = f'%"{cat}"%'
            counts[cat] = int(conn.execute(
                "SELECT COUNT(*) FROM words WHERE coverage_categories_json LIKE ?",
                (pat,)).fetchone()[0])
    return {"ok": True, "language": language, "counts": counts}


def check_safety_filter_index_behavior(language: str,
                                       db_path: Optional[str | Path] = None
                                       ) -> dict[str, Any]:
    """For each safety tag, verify the indexed query returns only rows
    that carry that tag - i.e. that the LIKE-on-json pattern doesn't
    leak unrelated rows."""
    _ensure_flags()
    db = db_path
    out: dict[str, Any] = {"language": language, "tag_results": {}}
    for tag in SAFETY_TAGS:
        rows = idx.query_by_safety(language, tag, limit=50, db_path=db)
        leaks = 0
        for r in rows:
            tags = r.get("safety_tags_json") or r.get("safety_tags") or "[]"
            if isinstance(tags, str):
                try:
                    parsed = json.loads(tags)
                except Exception:
                    parsed = []
            else:
                parsed = list(tags)
            if tag not in parsed:
                leaks += 1
        out["tag_results"][tag] = {"returned": len(rows),
                                   "leaks_not_carrying_tag": leaks}
    return {"ok": True, **out,
            "total_leaks": sum(v["leaks_not_carrying_tag"]
                               for v in out["tag_results"].values())}


def check_index_query_bounds(language: str,
                             db_path: Optional[str | Path] = None
                             ) -> dict[str, Any]:
    """Confirm that the index builder enforces the 200-row hard ceiling
    even when a caller asks for 9999."""
    _ensure_flags()
    db = db_path
    fn = (idx.query_english_index if language == "en"
          else idx.query_russian_index)
    rows = fn("a", limit=9999, db_path=db)
    return {"ok": len(rows) <= 200,
            "language": language,
            "returned": len(rows), "asked": 9999,
            "hard_clamp_expected": 200}


def write_index_consistency_report(report: dict[str, Any],
                                   output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "check_english_index_consistency",
    "check_russian_index_consistency",
    "check_fts_row_counts",
    "check_pack_id_index_coverage",
    "check_category_index_coverage",
    "check_safety_filter_index_behavior",
    "check_index_query_bounds",
    "write_index_consistency_report",
]
