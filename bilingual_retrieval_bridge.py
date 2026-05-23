"""Phase 22 - Bilingual Retrieval Bridge.

Bounded read-only retrieval that joins EN and RU stores via the bilingual
link DB. Always applies safety policy from `dual_retrieval_quality_eval`
before returning. Not wired into Luna main runtime.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_concept_link_store as bls
import dual_retrieval_quality_eval as rqe


_DEFAULT_LIMIT = 25
_HARD_LIMIT = 100


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def _clamp(n: Optional[int]) -> int:
    if n is None:
        return _DEFAULT_LIMIT
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_LIMIT
    return max(1, min(v, _HARD_LIMIT))


def retrieve_english_candidates(query: str, limit: int = _DEFAULT_LIMIT,
                                db_path: Optional[str | Path] = None
                                ) -> list[dict[str, Any]]:
    _ensure_flags()
    import cognitive_lexicon_store as enlex
    return enlex.search_contains(query, limit=_clamp(limit),
                                  db_path=db_path)


def retrieve_russian_candidates(query: str, limit: int = _DEFAULT_LIMIT,
                                db_path: Optional[str | Path] = None
                                ) -> list[dict[str, Any]]:
    _ensure_flags()
    import russian_lexicon_store as rulex
    return rulex.search_contains(query, limit=_clamp(limit),
                                  db_path=db_path)


def retrieve_linked_counterparts(entries: list[dict[str, Any]],
                                 target_language: str,
                                 limit: int = _DEFAULT_LIMIT,
                                 link_db_path: Optional[str | Path] = None
                                 ) -> list[dict[str, Any]]:
    """For each entry, look up the bilingual link store for matching
    concepts and return the counterparts in the target language."""
    if target_language not in ("en", "ru"):
        return []
    cap = _clamp(limit)
    out: list[dict[str, Any]] = []
    seen_concept_ids: set[str] = set()
    for e in entries[:cap]:
        word = (e.get("word") or e.get("phrase") or "").strip()
        if not word:
            continue
        # Source candidates are in source_language; look up concepts where
        # an entry_link's source_word matches this word (more reliable than
        # canonical-label match, since the row's exact `word` is what we
        # know). Fall back to canonical-label lookup if no entry-link hit.
        source_lang = "en" if target_language == "ru" else "ru"
        concepts = bls.find_concepts_by_entry_word(
            word, language=source_lang, limit=cap, db_path=link_db_path)
        if not concepts:
            concepts = bls.find_concepts_by_label(
                word, language=source_lang, limit=cap, db_path=link_db_path)
        for c in concepts:
            cid = c["concept_id"]
            if cid in seen_concept_ids:
                continue
            seen_concept_ids.add(cid)
            links = bls.get_links_for_concept(cid, limit=cap,
                                               db_path=link_db_path)
            counterparts = [l for l in links
                            if l["language"] == target_language]
            for cp in counterparts:
                cov_list = c.get("coverage_categories") or []
                reg_list = c.get("register_tags") or []
                saf_list = c.get("safety_tags") or []
                out.append({
                    "concept_id": cid,
                    "source_word": word,
                    "target_word": cp.get("source_word"),
                    "target_phrase": cp.get("source_phrase"),
                    "target_lemma": cp.get("lemma"),
                    "target_pos": cp.get("part_of_speech"),
                    "target_language": target_language,
                    "confidence": cp.get("confidence"),
                    "link_method": cp.get("link_method"),
                    "coverage_categories": cov_list,
                    "register_tags": reg_list,
                    "safety_tags": saf_list,
                })
                if len(out) >= cap:
                    return out
    return out[:cap]


def _row_safety_set(e: dict[str, Any]) -> set[str]:
    tags = e.get("safety_tags") or e.get("safety_tags_json") or []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            tags = []
    return set(tags or [])


def _row_register_set(e: dict[str, Any]) -> set[str]:
    tags = e.get("register_tags") or e.get("register_tags_json") or []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            tags = []
    return set(tags or [])


def filter_bilingual_safety(entries: list[dict[str, Any]],
                            mode: str = "teacher",
                            is_user_prompted: bool = False
                            ) -> dict[str, Any]:
    """Per-row safety classification. We compute this directly from each
    entry's safety_tags/register_tags so the same policy applies whether
    the row was returned as `target_word` (bridge) or `word` (raw lookup)."""
    safe: list[dict[str, Any]] = []
    blocked_count = 0
    suggestion_recognized_count = 0
    vulgar_in_teacher_mode_count = 0
    for e in entries:
        st = _row_safety_set(e)
        rg = _row_register_set(e)
        if "do_not_use_unprompted" in st and not is_user_prompted:
            blocked_count += 1
            continue
        if ({"vulgar", "offensive"} & (st | rg)) and mode == "teacher" \
                and not is_user_prompted:
            vulgar_in_teacher_mode_count += 1
            continue
        e2 = dict(e)
        if "recognition_only" in st and not is_user_prompted:
            e2["_suggestion_blocked"] = True
            suggestion_recognized_count += 1
        safe.append(e2)
    return {"ok": True, "mode": mode, "is_user_prompted": is_user_prompted,
            "safe_entries": safe,
            "blocked_count": blocked_count,
            "suggestion_recognized_count": suggestion_recognized_count,
            "vulgar_in_teacher_mode_count": vulgar_in_teacher_mode_count}


def format_bilingual_context(entries: list[dict[str, Any]],
                             limit: int = _DEFAULT_LIMIT
                             ) -> dict[str, Any]:
    cap = _clamp(limit)
    bilingual: list[dict[str, Any]] = []
    for e in entries[:cap]:
        bilingual.append({
            "source_word": e.get("source_word"),
            "target_word": e.get("target_word"),
            "target_phrase": e.get("target_phrase"),
            "target_language": e.get("target_language"),
            "concept_id": e.get("concept_id"),
            "confidence": e.get("confidence"),
            "coverage_categories": e.get("coverage_categories") or [],
            "register_tags": e.get("register_tags") or [],
            "safety_tags": e.get("safety_tags") or [],
            "suggestion_blocked": bool(e.get("_suggestion_blocked")),
        })
    return {"ok": True, "count": len(bilingual),
            "entries": bilingual}


def explain_bilingual_gap(query: str,
                          source_language: Optional[str] = None,
                          target_language: Optional[str] = None,
                          link_db_path: Optional[str | Path] = None
                          ) -> dict[str, Any]:
    en = retrieve_english_candidates(query, limit=5)
    ru = retrieve_russian_candidates(query, limit=5)
    concepts_en = bls.find_concepts_by_label(query, language="en",
                                              limit=10,
                                              db_path=link_db_path)
    concepts_ru = bls.find_concepts_by_label(query, language="ru",
                                              limit=10,
                                              db_path=link_db_path)
    note = "no_bilingual_link_for_query"
    if concepts_en or concepts_ru:
        note = "concept_present_but_no_counterpart"
    return {"ok": True, "query": query,
            "source_language": source_language,
            "target_language": target_language,
            "en_candidates_local": len(en),
            "ru_candidates_local": len(ru),
            "concepts_matched_en": len(concepts_en),
            "concepts_matched_ru": len(concepts_ru),
            "note": note}


def get_bilingual_context(query: str,
                         source_language: Optional[str] = None,
                         target_language: Optional[str] = None,
                         mode: str = "teacher",
                         limit: int = _DEFAULT_LIMIT,
                         is_user_prompted: bool = False,
                         link_db_path: Optional[str | Path] = None
                         ) -> dict[str, Any]:
    """Main entry point. Returns a bounded bilingual context dict."""
    _ensure_flags()
    cap = _clamp(limit)
    # Pick source by Cyrillic heuristic if not declared.
    if source_language is None:
        has_cyr = any("Ѐ" <= c <= "ӿ" for c in (query or ""))
        source_language = "ru" if has_cyr else "en"
    if target_language is None:
        target_language = "ru" if source_language == "en" else "en"

    if source_language == "en":
        source_candidates = retrieve_english_candidates(query, limit=cap)
    else:
        source_candidates = retrieve_russian_candidates(query, limit=cap)
    counterparts = retrieve_linked_counterparts(source_candidates,
                                                 target_language,
                                                 limit=cap,
                                                 link_db_path=link_db_path)
    safety = filter_bilingual_safety(counterparts, mode=mode,
                                      is_user_prompted=is_user_prompted)
    formatted = format_bilingual_context(safety["safe_entries"], limit=cap)
    if formatted["count"] == 0:
        gap = explain_bilingual_gap(query, source_language, target_language,
                                     link_db_path=link_db_path)
    else:
        gap = None
    return {"ok": True,
            "query": query,
            "source_language": source_language,
            "target_language": target_language,
            "mode": mode, "is_user_prompted": is_user_prompted,
            "limit": cap,
            "source_count": len(source_candidates),
            "counterparts_before_safety": len(counterparts),
            "safety_summary": {k: safety[k] for k in
                              ("blocked_count",
                               "suggestion_recognized_count",
                               "vulgar_in_teacher_mode_count")},
            "context": formatted,
            "gap_explanation": gap}


def write_bilingual_retrieval_report(report: dict[str, Any],
                                     output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_bilingual_context",
    "retrieve_english_candidates",
    "retrieve_russian_candidates",
    "retrieve_linked_counterparts",
    "filter_bilingual_safety",
    "format_bilingual_context",
    "explain_bilingual_gap",
    "write_bilingual_retrieval_report",
]
