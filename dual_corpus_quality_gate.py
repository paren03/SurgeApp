"""Phase 16 - Dual Corpus Quality Gate.

Read-only sampling and scoring of a candidate corpus source BEFORE chunked
import is allowed to write to the lexicon stores.

The quality gate NEVER writes to ``cognitive_lexicon_store`` or
``russian_lexicon_store``. It only:
  * Streams a small sample (head + middle + tail by default).
  * Scores each row for completeness, language match, and safety labeling.
  * Estimates an accepted-vs-rejected ratio on the sample.
  * Generates a quality report (dict) that the chunked importer must consult.

A real large import is gated on ``should_allow_import(report, min_quality_score)``
returning True and operator passing ``dry_run=False`` explicitly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional

from coverage_taxonomy import (
    COVERAGE_CATEGORIES,
    REGISTER_TAGS,
    SAFETY_TAGS,
)


SAMPLE_HARD_MAX = 500


_SAFETY_SET = set(SAFETY_TAGS)
_REGISTER_SET = set(REGISTER_TAGS)
_COVERAGE_SET = set(COVERAGE_CATEGORIES)


_SENSITIVE_FREE_TEXT_MARKERS = (
    "how to make",
    "step by step",
    "step-by-step",
    "instructions to",
    "how to build",
    "how to synthesize",
    "exploit",
    "bypass",
    "kill yourself",
    "kill themselves",
    "self-harm",
    "harm yourself",
)


def _stream_sample_offsets(path: Path, total_lines_estimate: int,
                           sample_size: int, strategy: str) -> list[int]:
    """Compute target line indices for sampling.

    Strategies: 'head', 'middle', 'tail', 'head_middle_tail', 'uniform'.
    """
    n = max(1, min(int(sample_size), SAMPLE_HARD_MAX))
    if total_lines_estimate <= 0:
        return list(range(n))
    if strategy == "head":
        return list(range(min(n, total_lines_estimate)))
    if strategy == "tail":
        start = max(0, total_lines_estimate - n)
        return list(range(start, total_lines_estimate))
    if strategy == "middle":
        mid = total_lines_estimate // 2
        start = max(0, mid - n // 2)
        return list(range(start, min(total_lines_estimate, start + n)))
    if strategy == "uniform":
        if n >= total_lines_estimate:
            return list(range(total_lines_estimate))
        step = max(1, total_lines_estimate // n)
        return [min(total_lines_estimate - 1, i * step) for i in range(n)]
    # default: head_middle_tail
    third = max(1, n // 3)
    rem = n - 3 * third
    head_idx = list(range(min(third, total_lines_estimate)))
    mid_start = max(0, total_lines_estimate // 2 - third // 2)
    mid_idx = list(range(mid_start,
                         min(total_lines_estimate, mid_start + third)))
    tail_start = max(0, total_lines_estimate - third - rem)
    tail_idx = list(range(tail_start, total_lines_estimate))
    out = sorted(set(head_idx + mid_idx + tail_idx))
    return out[:n]


def _parse_row(line: str, fmt: str) -> Optional[dict[str, Any]]:
    s = line.strip()
    if not s:
        return None
    if fmt == "jsonl":
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else {"_value": obj}
        except Exception:
            return None
    if fmt == "csv":
        cells = [c.strip() for c in s.split(",")]
        return {"_csv_cells": cells, "word": cells[0] if cells else ""}
    return {"word": s}


def _bounded_total(path: Path, max_scan: int = 50000) -> int:
    n = 0
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for _ in fh:
                n += 1
                if n >= max_scan:
                    break
    except Exception:
        return n
    return n


def sample_corpus(
    path: str | Path,
    expected_format: str,
    sample_size: int = 100,
    strategy: str = "head_middle_tail",
) -> dict[str, Any]:
    """Stream a bounded sample of rows from a corpus file."""
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "path": str(p),
                "rows": [], "total_scanned": 0}
    if expected_format not in ("jsonl", "txt", "csv"):
        return {"ok": False, "error": f"invalid_format: {expected_format!r}",
                "rows": [], "total_scanned": 0}

    cap = max(1, min(int(sample_size), SAMPLE_HARD_MAX))
    total_est = _bounded_total(p)
    targets = set(_stream_sample_offsets(p, total_est, cap, strategy))

    rows: list[dict[str, Any]] = []
    seen = 0
    try:
        with p.open("r", encoding="utf-8", errors="replace") as fh:
            for idx, line in enumerate(fh):
                if idx in targets:
                    obj = _parse_row(line, expected_format)
                    if obj is not None:
                        obj.setdefault("_source_line", idx)
                        rows.append(obj)
                seen += 1
                if len(rows) >= cap:
                    break
                if seen >= total_est and not targets - set(range(seen)):
                    break
    except Exception as e:
        return {"ok": False, "error": f"sample_failed: {e}",
                "path": str(p), "rows": rows, "total_scanned": seen}

    return {"ok": True, "path": str(p), "expected_format": expected_format,
            "strategy": strategy, "sample_size_requested": cap,
            "sample_size_returned": len(rows),
            "total_scanned": seen, "total_estimate": total_est, "rows": rows}


def detect_metadata_completeness(row: dict[str, Any]) -> dict[str, Any]:
    """Score whether row has the expected metadata fields."""
    has_word = bool((row.get("word") or row.get("phrase") or "").strip())
    has_def = bool((row.get("definition") or "").strip())
    has_cov = isinstance(row.get("coverage_categories"), list) and row["coverage_categories"]
    has_reg = isinstance(row.get("register_tags"), list) and row["register_tags"]
    score = 0
    score += 0.40 if has_word else 0.0
    score += 0.20 if has_def else 0.0
    score += 0.20 if has_cov else 0.0
    score += 0.20 if has_reg else 0.0
    return {"has_word_or_phrase": has_word, "has_definition": has_def,
            "has_coverage": has_cov, "has_register": has_reg,
            "completeness_score": round(score, 3)}


def detect_language_mismatch(row: dict[str, Any], language: str) -> dict[str, Any]:
    """Detect declared-vs-content language mismatch (best-effort)."""
    declared = (row.get("language") or "").strip().lower()
    word = (row.get("word") or row.get("phrase") or "").strip()
    if not word:
        return {"mismatch": False, "declared": declared,
                "detected": None, "reason": "empty"}
    has_cyrillic = any("Ѐ" <= ch <= "ӿ" for ch in word)
    detected = "ru" if has_cyrillic else "en"
    if declared and declared != language:
        return {"mismatch": True, "declared": declared,
                "detected": detected, "reason": "declared_does_not_match_target"}
    if language == "ru" and not has_cyrillic:
        return {"mismatch": True, "declared": declared,
                "detected": detected,
                "reason": "ru_target_but_no_cyrillic"}
    if language == "en" and has_cyrillic:
        return {"mismatch": True, "declared": declared,
                "detected": detected,
                "reason": "en_target_but_cyrillic_detected"}
    return {"mismatch": False, "declared": declared,
            "detected": detected, "reason": "ok"}


def detect_unsafe_unlabeled(row: dict[str, Any]) -> dict[str, Any]:
    """Flag rows that look unsafe but carry no safety tags.

    Operational unsafe instructions are flagged hard. Sensitive but vocabulary-
    only rows trigger a softer 'needs_recognition_only' flag.
    """
    text_blobs: list[str] = []
    for k in ("definition", "phrase", "examples", "notes", "explanation"):
        v = row.get(k)
        if isinstance(v, str):
            text_blobs.append(v)
        elif isinstance(v, list):
            text_blobs.extend(str(x) for x in v)
    big = " ".join(text_blobs).lower()
    safety = set(row.get("safety_tags") or [])
    register = set(row.get("register_tags") or [])

    operational_unsafe = any(m in big for m in _SENSITIVE_FREE_TEXT_MARKERS)
    sensitive_word = bool(safety & {"vulgar", "offensive"})
    sensitive_word |= bool(register & {"vulgar", "offensive"})
    needs_recognition_only = sensitive_word and ("recognition_only" not in safety)
    return {"operational_unsafe": operational_unsafe,
            "sensitive_word": sensitive_word,
            "needs_recognition_only": needs_recognition_only,
            "safety_tags": sorted(safety),
            "register_tags": sorted(register)}


def score_row_quality(row: dict[str, Any], language: str) -> dict[str, Any]:
    comp = detect_metadata_completeness(row)
    lang = detect_language_mismatch(row, language)
    safe = detect_unsafe_unlabeled(row)
    score = comp["completeness_score"]
    if lang["mismatch"]:
        score *= 0.5
    if safe["operational_unsafe"]:
        score = 0.0
    if safe["needs_recognition_only"]:
        score *= 0.7
    return {"score": round(score, 3), "completeness": comp,
            "language": lang, "safety": safe}


def detect_duplicate_sample_rows(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    seen: dict[str, int] = {}
    dupes: list[str] = []
    for r in rows:
        key = (r.get("word") or r.get("phrase") or "").strip().lower()
        if not key:
            continue
        if key in seen:
            dupes.append(key)
        seen[key] = seen.get(key, 0) + 1
    return {"unique_keys": len(seen),
            "duplicate_keys": len(dupes),
            "duplicate_samples": dupes[:20]}


def estimate_acceptance_rate(scored: list[dict[str, Any]],
                             accept_threshold: float = 0.4) -> dict[str, Any]:
    if not scored:
        return {"acceptance_rate": 0.0, "n": 0, "n_accept": 0,
                "n_reject": 0, "accept_threshold": accept_threshold}
    n_accept = sum(1 for s in scored if s["score"] >= accept_threshold)
    n = len(scored)
    return {"acceptance_rate": round(n_accept / n, 3), "n": n,
            "n_accept": n_accept, "n_reject": n - n_accept,
            "accept_threshold": accept_threshold}


def generate_quality_gate_report(
    path: str | Path,
    expected_format: str,
    language: str,
    sample_size: int = 100,
    strategy: str = "head_middle_tail",
) -> dict[str, Any]:
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    s = sample_corpus(path, expected_format, sample_size, strategy)
    if not s.get("ok"):
        return {"ok": False, "error": s.get("error", "sample_failed"),
                "sample": s}
    rows: list[dict[str, Any]] = s["rows"]
    scored = [score_row_quality(r, language) for r in rows]
    dupes = detect_duplicate_sample_rows(rows)
    accept = estimate_acceptance_rate(scored)
    operational_unsafe_n = sum(1 for sc in scored
                               if sc["safety"]["operational_unsafe"])
    needs_reconly_n = sum(1 for sc in scored
                          if sc["safety"]["needs_recognition_only"])
    language_mismatch_n = sum(1 for sc in scored
                              if sc["language"]["mismatch"])
    quality_score = (
        accept["acceptance_rate"] * 0.7
        + min(1.0, (1.0 - dupes["duplicate_keys"] / max(1, dupes["unique_keys"]
                                                       + dupes["duplicate_keys"]))) * 0.15
        + (0.0 if operational_unsafe_n > 0 else 0.15)
    )
    return {"ok": True, "path": str(path), "language": language,
            "expected_format": expected_format,
            "sample_summary": {"requested": s["sample_size_requested"],
                               "returned": s["sample_size_returned"],
                               "total_scanned": s["total_scanned"],
                               "strategy": s["strategy"]},
            "completeness_avg": round(
                sum(sc["completeness"]["completeness_score"] for sc in scored)
                / max(1, len(scored)), 3),
            "duplicates": dupes,
            "acceptance": accept,
            "language_mismatch_count": language_mismatch_n,
            "operational_unsafe_count": operational_unsafe_n,
            "needs_recognition_only_count": needs_reconly_n,
            "quality_score": round(quality_score, 3),
            "rows_scored": len(scored)}


def should_allow_import(quality_report: dict[str, Any],
                        min_quality_score: float = 0.75) -> dict[str, Any]:
    """Hard gate. Returns ok=False if anything looks unsafe or below threshold."""
    if not quality_report or not quality_report.get("ok"):
        return {"ok": False, "reason": "no_quality_report_or_invalid"}
    if quality_report.get("operational_unsafe_count", 0) > 0:
        return {"ok": False, "reason": "operational_unsafe_content_detected",
                "operational_unsafe_count": quality_report["operational_unsafe_count"]}
    q = float(quality_report.get("quality_score", 0.0))
    if q < float(min_quality_score):
        return {"ok": False, "reason": "quality_score_below_threshold",
                "quality_score": q,
                "min_quality_score": float(min_quality_score)}
    return {"ok": True, "quality_score": q,
            "min_quality_score": float(min_quality_score)}


__all__ = [
    "SAMPLE_HARD_MAX",
    "sample_corpus",
    "score_row_quality",
    "detect_metadata_completeness",
    "detect_language_mismatch",
    "detect_unsafe_unlabeled",
    "detect_duplicate_sample_rows",
    "estimate_acceptance_rate",
    "generate_quality_gate_report",
    "should_allow_import",
]
