"""Phase 22 - Bilingual Coverage Gap Reporter.

Read-only aggregate reports on cross-language coverage imbalances and
missing bilingual links. Never creates links or modifies source DBs.
Bounded queries only.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_concept_link_store as bls
from coverage_taxonomy import COVERAGE_CATEGORIES


_DEFAULT_LISTING = 100
_HARD_LISTING = 1000


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def _link_db_connect(db_path: Optional[str | Path] = None
                     ) -> sqlite3.Connection:
    bls.init_bilingual_link_db(db_path)
    p = Path(db_path) if db_path is not None else bls.DEFAULT_LINK_DB
    return sqlite3.connect(str(p), timeout=5.0)


def _en_connect(db_path: Optional[str | Path] = None):
    import cognitive_lexicon_store as enlex
    enlex.init_db(db_path)
    return enlex._connect(db_path)


def _ru_connect(db_path: Optional[str | Path] = None):
    import russian_lexicon_store as rulex
    rulex.init_db(db_path)
    return rulex._connect(db_path)


def _clamp(n: Optional[int], default: int = _DEFAULT_LISTING,
           hard: int = _HARD_LISTING) -> int:
    if n is None:
        return default
    try:
        v = int(n)
    except Exception:
        return default
    return max(1, min(v, hard))


def count_linked_concepts(link_db_path: Optional[str | Path] = None
                          ) -> dict[str, Any]:
    conn = _link_db_connect(link_db_path)
    try:
        n_concepts = int(conn.execute(
            "SELECT COUNT(*) FROM concepts").fetchone()[0])
        n_en_links = int(conn.execute(
            "SELECT COUNT(*) FROM entry_links WHERE language='en'"
        ).fetchone()[0])
        n_ru_links = int(conn.execute(
            "SELECT COUNT(*) FROM entry_links WHERE language='ru'"
        ).fetchone()[0])
        n_gloss = int(conn.execute(
            "SELECT COUNT(*) FROM bilingual_glossary_links").fetchone()[0])
    finally:
        conn.close()
    return {"ok": True, "concepts": n_concepts,
            "english_entry_links": n_en_links,
            "russian_entry_links": n_ru_links,
            "glossary_links": n_gloss}


def count_links_by_category(link_db_path: Optional[str | Path] = None
                            ) -> dict[str, int]:
    """For each canonical category, count concepts whose
    coverage_categories_json mentions it."""
    conn = _link_db_connect(link_db_path)
    out: dict[str, int] = {}
    try:
        for cat in COVERAGE_CATEGORIES:
            pat = f'%"{cat}"%'
            n = int(conn.execute(
                "SELECT COUNT(*) FROM concepts "
                "WHERE coverage_categories_json LIKE ?", (pat,)
            ).fetchone()[0])
            out[cat] = n
    finally:
        conn.close()
    return out


def _category_count(conn: sqlite3.Connection, category: str) -> int:
    pat = f'%"{category}"%'
    return int(conn.execute(
        "SELECT COUNT(*) FROM words WHERE coverage_categories_json LIKE ?",
        (pat,)
    ).fetchone()[0])


def count_unlinked_english_by_category(limit: int = 100,
                                       en_db_path: Optional[str | Path] = None,
                                       link_db_path: Optional[str | Path] = None
                                       ) -> dict[str, dict[str, int]]:
    _ensure_flags()
    cap = _clamp(limit)
    out: dict[str, dict[str, int]] = {}
    with _en_connect(en_db_path) as enconn:
        with _link_db_connect(link_db_path) as lconn:
            for cat in COVERAGE_CATEGORIES:
                total = _category_count(enconn, cat)
                pat = f'%"{cat}"%'
                linked = int(lconn.execute(
                    "SELECT COUNT(DISTINCT c.concept_id) "
                    "FROM concepts c JOIN entry_links l "
                    "ON c.concept_id=l.concept_id "
                    "WHERE c.coverage_categories_json LIKE ? "
                    "AND l.language='en'", (pat,)
                ).fetchone()[0])
                gap = max(0, total - linked)
                out[cat] = {"total": int(total), "linked": int(linked),
                            "gap": int(gap)}
                if len(out) >= 21:  # all categories
                    break
    return {k: v for k, v in list(out.items())[:cap]} if cap < len(out) else out


def count_unlinked_russian_by_category(limit: int = 100,
                                       ru_db_path: Optional[str | Path] = None,
                                       link_db_path: Optional[str | Path] = None
                                       ) -> dict[str, dict[str, int]]:
    _ensure_flags()
    cap = _clamp(limit)
    out: dict[str, dict[str, int]] = {}
    with _ru_connect(ru_db_path) as ruconn:
        with _link_db_connect(link_db_path) as lconn:
            for cat in COVERAGE_CATEGORIES:
                total = _category_count(ruconn, cat)
                pat = f'%"{cat}"%'
                linked = int(lconn.execute(
                    "SELECT COUNT(DISTINCT c.concept_id) "
                    "FROM concepts c JOIN entry_links l "
                    "ON c.concept_id=l.concept_id "
                    "WHERE c.coverage_categories_json LIKE ? "
                    "AND l.language='ru'", (pat,)
                ).fetchone()[0])
                gap = max(0, total - linked)
                out[cat] = {"total": int(total), "linked": int(linked),
                            "gap": int(gap)}
                if len(out) >= 21:
                    break
    return {k: v for k, v in list(out.items())[:cap]} if cap < len(out) else out


def identify_category_imbalances(min_gap: int = 25,
                                 en_db_path: Optional[str | Path] = None,
                                 ru_db_path: Optional[str | Path] = None,
                                 link_db_path: Optional[str | Path] = None
                                 ) -> list[dict[str, Any]]:
    _ensure_flags()
    en_gaps = count_unlinked_english_by_category(en_db_path=en_db_path,
                                                  link_db_path=link_db_path)
    ru_gaps = count_unlinked_russian_by_category(ru_db_path=ru_db_path,
                                                  link_db_path=link_db_path)
    out: list[dict[str, Any]] = []
    for cat in COVERAGE_CATEGORIES:
        en = en_gaps.get(cat, {"total": 0, "linked": 0, "gap": 0})
        ru = ru_gaps.get(cat, {"total": 0, "linked": 0, "gap": 0})
        diff = abs(en["gap"] - ru["gap"])
        if diff >= int(min_gap):
            out.append({"category": cat,
                        "en_total": en["total"], "en_linked": en["linked"],
                        "en_gap": en["gap"],
                        "ru_total": ru["total"], "ru_linked": ru["linked"],
                        "ru_gap": ru["gap"],
                        "imbalance": diff})
    out.sort(key=lambda x: x["imbalance"], reverse=True)
    return out


def _missing_by_category(category: str, limit: int,
                         en_db_path, ru_db_path) -> dict[str, Any]:
    _ensure_flags()
    cap = _clamp(limit)
    with _en_connect(en_db_path) as enconn:
        en_count = _category_count(enconn, category)
    with _ru_connect(ru_db_path) as ruconn:
        ru_count = _category_count(ruconn, category)
    return {"category": category, "en_total": en_count,
            "ru_total": ru_count, "limit_used": cap}


def identify_missing_profession_links(limit: int = 100,
                                       en_db_path=None, ru_db_path=None,
                                       link_db_path=None) -> dict[str, Any]:
    return _missing_by_category("professions_jobs", limit,
                                 en_db_path, ru_db_path)


def identify_missing_trade_links(limit: int = 100,
                                  en_db_path=None, ru_db_path=None,
                                  link_db_path=None) -> dict[str, Any]:
    return _missing_by_category("trades_construction", limit,
                                 en_db_path, ru_db_path)


def identify_missing_poetry_philosophy_links(limit: int = 100,
                                              en_db_path=None,
                                              ru_db_path=None,
                                              link_db_path=None
                                              ) -> dict[str, Any]:
    poetry = _missing_by_category("poetry_literary", limit,
                                   en_db_path, ru_db_path)
    philo = _missing_by_category("philosophy_abstract", limit,
                                  en_db_path, ru_db_path)
    return {"poetry": poetry, "philosophy": philo}


def identify_slang_link_cautions(limit: int = 100,
                                  en_db_path=None, ru_db_path=None,
                                  link_db_path=None) -> dict[str, Any]:
    """Slang/street categories warrant cautious linking. Report counts +
    explicit caution notes."""
    s = _missing_by_category("slang_street_talk", limit,
                              en_db_path, ru_db_path)
    s["cautions"] = [
        "Slang pairings often have no clean translation; mark "
        "register_tags=['slang']/['street'] before pairing.",
        "Never auto-elevate vulgar/offensive slang to general suggestion.",
        "Apply recognition_only safety tag for sensitive slang.",
    ]
    return s


def write_bilingual_coverage_gap_report(report: dict[str, Any],
                                        output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "count_linked_concepts",
    "count_links_by_category",
    "count_unlinked_english_by_category",
    "count_unlinked_russian_by_category",
    "identify_category_imbalances",
    "identify_missing_profession_links",
    "identify_missing_trade_links",
    "identify_missing_poetry_philosophy_links",
    "identify_slang_link_cautions",
    "write_bilingual_coverage_gap_report",
]
