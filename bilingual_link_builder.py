"""Phase 22 - Bilingual Link Builder.

Builds initial bilingual concept links from existing local EN/RU rows using
conservative heuristics. Writes ONLY to the separate bilingual link DB.
Never modifies the English or Russian lexicons.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_concept_link_store as bls
from coverage_taxonomy import COVERAGE_CATEGORIES


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


def _clamp(n: Optional[int], default: int = 1000, hard: int = 5000) -> int:
    if n is None:
        return default
    try:
        v = int(n)
    except Exception:
        return default
    return max(1, min(v, hard))


def normalize_concept_key(text: str) -> str:
    return (text or "").strip().lower()


def load_candidate_english_entries(limit: int = 1000,
                                   coverage_category: Optional[str] = None,
                                   db_path: Optional[str | Path] = None
                                   ) -> list[dict[str, Any]]:
    _ensure_flags()
    cap = _clamp(limit)
    with _en_connect(db_path) as conn:
        if coverage_category:
            pat = f'%"{coverage_category}"%'
            cur = conn.execute(
                "SELECT * FROM words WHERE coverage_categories_json LIKE ? "
                "ORDER BY frequency_score DESC, word ASC LIMIT ?",
                (pat, cap))
        else:
            cur = conn.execute(
                "SELECT * FROM words ORDER BY frequency_score DESC, word "
                "ASC LIMIT ?", (cap,))
        return [dict(r) for r in cur.fetchall()]


def load_candidate_russian_entries(limit: int = 1000,
                                   coverage_category: Optional[str] = None,
                                   db_path: Optional[str | Path] = None
                                   ) -> list[dict[str, Any]]:
    _ensure_flags()
    cap = _clamp(limit)
    with _ru_connect(db_path) as conn:
        if coverage_category:
            pat = f'%"{coverage_category}"%'
            cur = conn.execute(
                "SELECT * FROM words WHERE coverage_categories_json LIKE ? "
                "ORDER BY frequency_score DESC, word ASC LIMIT ?",
                (pat, cap))
        else:
            cur = conn.execute(
                "SELECT * FROM words ORDER BY frequency_score DESC, word "
                "ASC LIMIT ?", (cap,))
        return [dict(r) for r in cur.fetchall()]


def _parse_json_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            d = json.loads(v)
            return [str(x) for x in d] if isinstance(d, list) else []
        except Exception:
            return []
    return []


def _domain_overlap(en_entry: dict[str, Any],
                    ru_entry: dict[str, Any]) -> set[str]:
    en_doms = set(_parse_json_list(en_entry.get("tags_json")
                                    or en_entry.get("domain_tags_json")
                                    or en_entry.get("tags")))
    ru_doms = set(_parse_json_list(ru_entry.get("domain_tags_json")
                                    or ru_entry.get("tags_json")
                                    or ru_entry.get("domain_tags")))
    return en_doms & ru_doms


def _pack_match(en_entry: dict[str, Any],
                ru_entry: dict[str, Any]) -> bool:
    en_pack = (en_entry.get("pack_source") or "").lower()
    ru_pack = (ru_entry.get("pack_source") or "").lower()
    if not en_pack or not ru_pack:
        return False
    # Detect mirrored seed packs whose pack_source strings overlap (e.g.
    # "core_vocabulary" appears in both en and ru variants).
    en_tokens = set(en_pack.replace("_", " ").split())
    ru_tokens = set(ru_pack.replace("_", " ").split())
    return bool(en_tokens & ru_tokens)


def score_link_confidence(en_entry: dict[str, Any],
                          ru_entry: dict[str, Any],
                          method: str) -> float:
    base = {
        "manual": 0.95,
        "exact_match": 0.85,
        "lemma_match": 0.78,
        "domain_category_match": 0.55,
        "glossary_import": 0.90,
        "heuristic": 0.45,
        "evaluation_fixture": 0.70,
    }.get(method, 0.40)
    bonus = 0.0
    overlap = _domain_overlap(en_entry, ru_entry)
    if overlap:
        bonus += min(0.10, 0.05 * len(overlap))
    if _pack_match(en_entry, ru_entry):
        bonus += 0.05
    return round(max(0.0, min(1.0, base + bonus)), 3)


def _coverage_overlap(en_entry: dict[str, Any],
                      ru_entry: dict[str, Any]) -> set[str]:
    en_cov = set(_parse_json_list(en_entry.get("coverage_categories_json")
                                    or en_entry.get("coverage_categories")))
    ru_cov = set(_parse_json_list(ru_entry.get("coverage_categories_json")
                                    or ru_entry.get("coverage_categories")))
    return en_cov & ru_cov


def _register_compatible(en_entry: dict[str, Any],
                         ru_entry: dict[str, Any]) -> bool:
    """Don't pair a 'standard' English row with a 'vulgar' Russian row."""
    en_reg = set(_parse_json_list(en_entry.get("register_tags_json")
                                   or en_entry.get("register_tags")))
    ru_reg = set(_parse_json_list(ru_entry.get("register_tags_json")
                                   or ru_entry.get("register_tags")))
    blocking = {"vulgar", "offensive"}
    return not (bool(en_reg & blocking) ^ bool(ru_reg & blocking))


def _safety_compatible(en_entry: dict[str, Any],
                       ru_entry: dict[str, Any]) -> bool:
    en_safe = set(_parse_json_list(en_entry.get("safety_tags_json")
                                    or en_entry.get("safety_tags")))
    ru_safe = set(_parse_json_list(ru_entry.get("safety_tags_json")
                                    or ru_entry.get("safety_tags")))
    blocking = {"vulgar", "offensive"}
    return not (bool(en_safe & blocking) ^ bool(ru_safe & blocking))


def infer_shared_category_links(limit_per_category: int = 100,
                                en_db_path: Optional[str | Path] = None,
                                ru_db_path: Optional[str | Path] = None,
                                link_db_path: Optional[str | Path] = None
                                ) -> dict[str, Any]:
    """For each canonical category, pair the highest-frequency EN word with
    the highest-frequency RU word in the same category. Conservative: 1
    pair per category, all marked link_method=domain_category_match."""
    _ensure_flags()
    cap = _clamp(limit_per_category, default=100, hard=500)
    per_cat: dict[str, dict[str, Any]] = {}
    created_concepts = 0
    created_entry_links = 0
    skipped = 0
    for cat in COVERAGE_CATEGORIES:
        en_rows = load_candidate_english_entries(limit=cap,
                                                  coverage_category=cat,
                                                  db_path=en_db_path)
        ru_rows = load_candidate_russian_entries(limit=cap,
                                                  coverage_category=cat,
                                                  db_path=ru_db_path)
        if not en_rows or not ru_rows:
            per_cat[cat] = {"linked": 0, "en_pool": len(en_rows),
                            "ru_pool": len(ru_rows),
                            "reason": "missing_pool"}
            continue
        en = en_rows[0]
        ru = ru_rows[0]
        if not _register_compatible(en, ru) or not _safety_compatible(en, ru):
            skipped += 1
            per_cat[cat] = {"linked": 0, "en_pool": len(en_rows),
                            "ru_pool": len(ru_rows),
                            "reason": "register_or_safety_blocked"}
            continue
        conf = score_link_confidence(en, ru, "domain_category_match")
        cov = sorted(_coverage_overlap(en, ru) or {cat})
        dom = sorted(_domain_overlap(en, ru))
        c = bls.create_concept(
            canonical_label_en=en.get("word", ""),
            canonical_label_ru=ru.get("word", ""),
            coverage_categories=cov,
            domain_tags=dom,
            register_tags=["standard"],
            notes=f"category={cat}",
            db_path=link_db_path)
        cid = c["concept_id"]
        created_concepts += 1
        bls.add_entry_link(cid, "en",
                           source_store="cognitive_lexicon_store",
                           source_word=en.get("word", ""),
                           confidence=conf,
                           link_method="domain_category_match",
                           db_path=link_db_path)
        bls.add_entry_link(cid, "ru",
                           source_store="russian_lexicon_store",
                           source_word=ru.get("word", ""),
                           lemma=ru.get("lemma", ""),
                           part_of_speech=ru.get("part_of_speech", ""),
                           confidence=conf,
                           link_method="domain_category_match",
                           db_path=link_db_path)
        created_entry_links += 2
        per_cat[cat] = {"linked": 1, "en_pool": len(en_rows),
                        "ru_pool": len(ru_rows),
                        "confidence": conf,
                        "concept_id": cid,
                        "en_word": en.get("word"),
                        "ru_word": ru.get("word")}
    return {"ok": True, "created_concepts": created_concepts,
            "created_entry_links": created_entry_links,
            "skipped_register_or_safety": skipped,
            "per_category": per_cat}


def infer_domain_tag_links(limit_per_domain: int = 100,
                           en_db_path: Optional[str | Path] = None,
                           ru_db_path: Optional[str | Path] = None,
                           link_db_path: Optional[str | Path] = None
                           ) -> dict[str, Any]:
    """Cross-category domain-tag pairing: find rows that share any domain
    tag and aren't already directly linked. Bounded by sampling."""
    _ensure_flags()
    cap = _clamp(limit_per_domain, default=100, hard=300)
    en = load_candidate_english_entries(limit=cap, db_path=en_db_path)
    ru = load_candidate_russian_entries(limit=cap, db_path=ru_db_path)
    pairs: list[dict[str, Any]] = []
    seen_concept_keys: set[str] = set()
    for e in en:
        e_doms = set(_parse_json_list(e.get("tags_json")
                                       or e.get("domain_tags")))
        if not e_doms:
            continue
        for r in ru:
            r_doms = set(_parse_json_list(r.get("domain_tags_json")
                                           or r.get("domain_tags")))
            shared = e_doms & r_doms
            if not shared:
                continue
            if not _register_compatible(e, r) or not _safety_compatible(e, r):
                continue
            key = f"{(e.get('word') or '').lower()}::{(r.get('word') or '').lower()}"
            if key in seen_concept_keys:
                continue
            seen_concept_keys.add(key)
            conf = score_link_confidence(e, r, "domain_category_match")
            pairs.append({"en": e.get("word"), "ru": r.get("word"),
                          "shared_domains": sorted(shared),
                          "confidence": conf})
            if len(pairs) >= cap:
                break
        if len(pairs) >= cap:
            break
    return {"ok": True, "candidate_pair_count": len(pairs),
            "candidates": pairs[:25]}


def infer_phrase_links(limit: int = 100,
                       en_db_path: Optional[str | Path] = None,
                       ru_db_path: Optional[str | Path] = None,
                       link_db_path: Optional[str | Path] = None
                       ) -> dict[str, Any]:
    """Pair EN idiom rows with RU phrase rows sharing the idioms_phrases
    coverage."""
    _ensure_flags()
    cap = _clamp(limit, default=100, hard=200)
    en = load_candidate_english_entries(
        limit=cap, coverage_category="idioms_phrases",
        db_path=en_db_path)
    try:
        with _ru_connect(ru_db_path) as conn:
            pat = '%"idioms_phrases"%'
            cur = conn.execute(
                "SELECT phrase, language, register_tags_json, "
                "safety_tags_json, coverage_categories_json, frequency_score "
                "FROM phrases WHERE coverage_categories_json LIKE ? "
                "ORDER BY frequency_score DESC LIMIT ?", (pat, cap))
            ru_phr = [dict(r) for r in cur.fetchall()]
    except Exception:
        ru_phr = []
    pairs: list[dict[str, Any]] = []
    for e in en[: min(cap, 25)]:
        for r in ru_phr[: min(cap, 25)]:
            if not _safety_compatible(e, r) or not _register_compatible(e, r):
                continue
            pairs.append({"en": e.get("word"), "ru_phrase": r.get("phrase"),
                          "confidence": 0.40})
            if len(pairs) >= cap:
                break
        if len(pairs) >= cap:
            break
    return {"ok": True, "candidate_phrase_pair_count": len(pairs),
            "candidates": pairs[:25]}


def build_seed_bilingual_links(limit_per_category: int = 50,
                               en_db_path: Optional[str | Path] = None,
                               ru_db_path: Optional[str | Path] = None,
                               link_db_path: Optional[str | Path] = None
                               ) -> dict[str, Any]:
    return infer_shared_category_links(
        limit_per_category=limit_per_category,
        en_db_path=en_db_path, ru_db_path=ru_db_path,
        link_db_path=link_db_path)


def build_manual_fixture_links(link_db_path: Optional[str | Path] = None
                               ) -> dict[str, Any]:
    """Insert a tiny known-good bilingual fixture for harness tests.

    These are evaluation_fixture links (low-cost, separate from runtime),
    not production seed packs.
    """
    pairs = [
        ("engineer", "инженер", "professions_jobs"),
        ("ledger", "бюджет", "business_finance"),
        ("verse", "стих", "poetry_literary"),
        ("vector", "число", "science_math"),
        ("essence", "сущность", "philosophy_abstract"),
    ]
    created = 0
    for en_label, ru_label, cat in pairs:
        c = bls.create_concept(
            canonical_label_en=en_label, canonical_label_ru=ru_label,
            coverage_categories=[cat],
            register_tags=["standard"],
            notes=f"evaluation_fixture:{cat}",
            db_path=link_db_path)
        cid = c["concept_id"]
        bls.add_entry_link(cid, "en", source_word=en_label,
                           confidence=0.80,
                           link_method="evaluation_fixture",
                           db_path=link_db_path)
        bls.add_entry_link(cid, "ru", source_word=ru_label,
                           confidence=0.80,
                           link_method="evaluation_fixture",
                           db_path=link_db_path)
        bls.add_glossary_link(cid, english_text=en_label,
                              russian_text=ru_label,
                              relation_type="translation",
                              confidence=0.80,
                              source="evaluation_fixture",
                              db_path=link_db_path)
        created += 1
    return {"ok": True, "fixture_concepts_created": created}


def write_link_builder_report(report: dict[str, Any],
                              output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "load_candidate_english_entries",
    "load_candidate_russian_entries",
    "normalize_concept_key",
    "infer_shared_category_links",
    "infer_domain_tag_links",
    "infer_phrase_links",
    "build_seed_bilingual_links",
    "build_manual_fixture_links",
    "score_link_confidence",
    "write_link_builder_report",
]
