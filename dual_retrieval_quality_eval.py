"""Phase 17 - Dual Retrieval Quality Evaluator.

Measures whether the English / Russian sovereign vocabulary stacks retrieve
useful, safe, bounded entries for representative queries.

Read-only. Does NOT mutate either lexicon store. Does NOT integrate with the
Luna main runtime. No daemon, no scheduler.

Safety check semantics:
  * 'recognition_only' terms may be recognized/explained but must not be
    suggested for Luna's own wording (decision_context=='suggestion').
  * 'do_not_use_unprompted' terms are blocked unless ``is_user_prompted=True``.
  * vulgar / offensive must never surface for default (teacher) mode without
    user prompting.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Optional


_DEFAULT_LIMIT = 25


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def build_eval_queries() -> dict[str, list[dict[str, Any]]]:
    """Return canonical evaluation queries grouped by language."""
    en_queries: list[dict[str, Any]] = [
        {"id": "en_professions_jobs", "category": "professions_jobs",
         "needle": "engineer", "expected": ["professions_jobs"]},
        {"id": "en_trades_construction", "category": "trades_construction",
         "needle": "concrete", "expected": ["trades_construction"]},
        {"id": "en_poetry_literary", "category": "poetry_literary",
         "needle": "verse", "expected": ["poetry_literary"]},
        {"id": "en_philosophy_abstract", "category": "philosophy_abstract",
         "needle": "essence", "expected": ["philosophy_abstract"]},
        {"id": "en_business_finance", "category": "business_finance",
         "needle": "ledger", "expected": ["business_finance"]},
        {"id": "en_law_government", "category": "law_government",
         "needle": "statute", "expected": ["law_government"]},
        {"id": "en_science_math", "category": "science_math",
         "needle": "vector", "expected": ["science_math"]},
        {"id": "en_coding_technology", "category": "coding_technology",
         "needle": "function", "expected": ["coding_technology"]},
        {"id": "en_slang_gated", "category": "slang_street_talk",
         "needle": "homie", "expected": ["slang_street_talk"],
         "is_gated": True, "is_user_prompted": False},
        {"id": "en_voice_personality", "category": "voice_personality",
         "needle": "gentle", "expected": ["voice_personality"]},
        {"id": "en_idioms_phrases", "category": "idioms_phrases",
         "needle": "break", "expected": ["idioms_phrases"]},
        {"id": "en_core_vocabulary", "category": "core_vocabulary",
         "needle": "water", "expected": ["core_vocabulary"]},
    ]
    ru_queries: list[dict[str, Any]] = [
        {"id": "ru_professions_jobs", "category": "professions_jobs",
         "needle": "инженер", "expected": ["professions_jobs"]},
        {"id": "ru_trades_construction", "category": "trades_construction",
         "needle": "бетон", "expected": ["trades_construction"]},
        {"id": "ru_poetry_literary", "category": "poetry_literary",
         "needle": "стих", "expected": ["poetry_literary"]},
        {"id": "ru_philosophy_abstract", "category": "philosophy_abstract",
         "needle": "сущность", "expected": ["philosophy_abstract"]},
        {"id": "ru_business_finance", "category": "business_finance",
         "needle": "бюджет", "expected": ["business_finance"]},
        {"id": "ru_law_government", "category": "law_government",
         "needle": "закон", "expected": ["law_government"]},
        {"id": "ru_science_math", "category": "science_math",
         "needle": "число", "expected": ["science_math"]},
        {"id": "ru_coding_technology", "category": "coding_technology",
         "needle": "функция", "expected": ["coding_technology"]},
        {"id": "ru_slang_gated", "category": "slang_street_talk",
         "needle": "чувак", "expected": ["slang_street_talk"],
         "is_gated": True, "is_user_prompted": False},
        {"id": "ru_voice_personality", "category": "voice_personality",
         "needle": "тёплый", "expected": ["voice_personality"]},
        {"id": "ru_idioms_phrases", "category": "idioms_phrases",
         "needle": "душа", "expected": ["idioms_phrases"]},
        {"id": "ru_core_vocabulary", "category": "core_vocabulary",
         "needle": "вода", "expected": ["core_vocabulary"]},
    ]
    return {"en": en_queries, "ru": ru_queries}


def check_result_bounds(results: list[dict[str, Any]],
                        limit: int) -> dict[str, Any]:
    cap = max(1, int(limit))
    n = len(results)
    return {"ok": n <= cap, "limit": cap, "returned": n}


def _row_safety(row: dict[str, Any]) -> set[str]:
    s = row.get("safety_tags") or row.get("safety_tags_json") or []
    if isinstance(s, str):
        try:
            s = json.loads(s)
        except Exception:
            s = []
    return set(s or [])


def _row_register(row: dict[str, Any]) -> set[str]:
    r = row.get("register_tags") or row.get("register_tags_json") or []
    if isinstance(r, str):
        try:
            r = json.loads(r)
        except Exception:
            r = []
    return set(r or [])


def _row_coverage(row: dict[str, Any]) -> set[str]:
    c = row.get("coverage_categories") or row.get("coverage_categories_json") or []
    if isinstance(c, str):
        try:
            c = json.loads(c)
        except Exception:
            c = []
    return set(c or [])


def check_safety_policy_on_results(results: list[dict[str, Any]],
                                   mode: str = "teacher",
                                   is_user_prompted: bool = False
                                   ) -> dict[str, Any]:
    """For each row, decide whether it would be ALLOWED as 'suggestion' under
    the given mode. Returns counts."""
    suggestion_violations: list[str] = []
    do_not_use_violations: list[str] = []
    vulgar_seen: list[str] = []
    for r in results:
        s = _row_safety(r)
        if "do_not_use_unprompted" in s and not is_user_prompted:
            do_not_use_violations.append(str(r.get("word") or r.get("phrase") or ""))
        if "recognition_only" in s and not is_user_prompted:
            suggestion_violations.append(str(r.get("word") or r.get("phrase") or ""))
        if ({"vulgar", "offensive"} & s) and mode == "teacher" and not is_user_prompted:
            vulgar_seen.append(str(r.get("word") or r.get("phrase") or ""))
    return {"ok": not (do_not_use_violations or vulgar_seen),
            "mode": mode, "is_user_prompted": is_user_prompted,
            "do_not_use_violations": do_not_use_violations[:20],
            "do_not_use_violation_count": len(do_not_use_violations),
            "suggestion_only_recognized": suggestion_violations[:20],
            "suggestion_only_recognized_count": len(suggestion_violations),
            "vulgar_in_teacher_mode": vulgar_seen[:20],
            "vulgar_in_teacher_mode_count": len(vulgar_seen)}


def check_register_fit(results: list[dict[str, Any]],
                       mode: str) -> dict[str, Any]:
    """Soft fit check: in teacher mode, rows registered as 'street' or
    'vulgar' without recognition_only are mismatches."""
    mismatches: list[str] = []
    for r in results:
        regs = _row_register(r)
        saf = _row_safety(r)
        if mode == "teacher" and ("street" in regs or "vulgar" in regs):
            if "recognition_only" not in saf:
                mismatches.append(str(r.get("word") or r.get("phrase") or ""))
    return {"ok": not mismatches, "mode": mode,
            "mismatches": mismatches[:20],
            "mismatch_count": len(mismatches)}


def check_category_coverage(results: list[dict[str, Any]],
                            expected_categories: Iterable[str]
                            ) -> dict[str, Any]:
    expected = set(expected_categories or [])
    hits = 0
    for r in results:
        if _row_coverage(r) & expected:
            hits += 1
    n = len(results)
    return {"ok": hits > 0 if expected else True,
            "expected": sorted(expected),
            "hit_rows": hits, "total_rows": n,
            "coverage_ratio": round(hits / n, 3) if n else 0.0}


def score_retrieval_result(query: dict[str, Any],
                           results: list[dict[str, Any]],
                           expected_categories: Optional[Iterable[str]] = None,
                           expected_language: Optional[str] = None
                           ) -> dict[str, Any]:
    cov = check_category_coverage(results,
                                  expected_categories or query.get("expected") or [])
    saf = check_safety_policy_on_results(
        results, mode="teacher",
        is_user_prompted=bool(query.get("is_user_prompted")))
    fit = check_register_fit(results, mode="teacher")
    n = len(results)
    lang_hits = 0
    if expected_language:
        for r in results:
            if (r.get("language") or "").lower() == expected_language:
                lang_hits += 1
    score = 0.0
    if n > 0:
        score = cov["coverage_ratio"] * 0.6
        if saf["ok"]:
            score += 0.25
        if fit["ok"]:
            score += 0.15
    return {
        "query_id": query.get("id"),
        "needle": query.get("needle"),
        "n_results": n,
        "coverage": cov,
        "safety": saf,
        "register_fit": fit,
        "language_hits": lang_hits,
        "score": round(score, 3),
    }


def _en_search(needle: str, limit: int, db_path: Optional[str | Path] = None
               ) -> list[dict[str, Any]]:
    import cognitive_lexicon_store as enlex
    return enlex.search_contains(needle, limit=limit, db_path=db_path)


def _ru_search(needle: str, limit: int, db_path: Optional[str | Path] = None
               ) -> list[dict[str, Any]]:
    import russian_lexicon_store as rulex
    return rulex.search_contains(needle, limit=limit, db_path=db_path)


def run_english_retrieval_eval(queries: Optional[list[dict[str, Any]]] = None,
                               limit: int = _DEFAULT_LIMIT,
                               db_path: Optional[str | Path] = None
                               ) -> dict[str, Any]:
    _ensure_flags()
    qs = queries or build_eval_queries()["en"]
    cap = max(1, min(int(limit), 100))
    per_query: list[dict[str, Any]] = []
    for q in qs:
        results = _en_search(q["needle"], cap, db_path=db_path)
        bounds = check_result_bounds(results, cap)
        scored = score_retrieval_result(q, results,
                                        expected_categories=q.get("expected"),
                                        expected_language="en")
        per_query.append({"query": q, "bounds": bounds, **scored})
    avg = (sum(x["score"] for x in per_query) / max(1, len(per_query)))
    bounds_ok = all(x["bounds"]["ok"] for x in per_query)
    safety_ok = all(x["safety"]["ok"] for x in per_query)
    return {"ok": True, "language": "en", "limit": cap,
            "n_queries": len(per_query), "average_score": round(avg, 3),
            "bounds_ok": bounds_ok, "safety_ok": safety_ok,
            "per_query": per_query, "generated_at": time.time()}


def run_russian_retrieval_eval(queries: Optional[list[dict[str, Any]]] = None,
                               limit: int = _DEFAULT_LIMIT,
                               db_path: Optional[str | Path] = None
                               ) -> dict[str, Any]:
    _ensure_flags()
    qs = queries or build_eval_queries()["ru"]
    cap = max(1, min(int(limit), 100))
    per_query: list[dict[str, Any]] = []
    for q in qs:
        results = _ru_search(q["needle"], cap, db_path=db_path)
        bounds = check_result_bounds(results, cap)
        scored = score_retrieval_result(q, results,
                                        expected_categories=q.get("expected"),
                                        expected_language="ru")
        per_query.append({"query": q, "bounds": bounds, **scored})
    avg = (sum(x["score"] for x in per_query) / max(1, len(per_query)))
    bounds_ok = all(x["bounds"]["ok"] for x in per_query)
    safety_ok = all(x["safety"]["ok"] for x in per_query)
    return {"ok": True, "language": "ru", "limit": cap,
            "n_queries": len(per_query), "average_score": round(avg, 3),
            "bounds_ok": bounds_ok, "safety_ok": safety_ok,
            "per_query": per_query, "generated_at": time.time()}


def write_retrieval_eval_report(report: dict[str, Any],
                                output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "build_eval_queries",
    "run_english_retrieval_eval",
    "run_russian_retrieval_eval",
    "score_retrieval_result",
    "check_result_bounds",
    "check_safety_policy_on_results",
    "check_register_fit",
    "check_category_coverage",
    "write_retrieval_eval_report",
]
