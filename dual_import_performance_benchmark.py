"""Phase 19 - Dual Import Performance Benchmark.

Measures bounded performance of:
    * streaming read of a local JSONL file
    * dry-run import via the Phase 16 chunked importer
    * index build via Phase 19 dual_retrieval_index_builder
    * indexed query
    * retrieval evaluation

No daemons, no threads, no internet, no full-file load.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import dual_corpus_chunked_importer as imp
import dual_retrieval_index_builder as idx
import dual_retrieval_quality_eval as rqe


HARD_ROW_CAP = 200_000
DEFAULT_FIXTURE_ROWS = 100_000


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def _make_en_row(i: int) -> dict[str, Any]:
    cats = ("core_vocabulary", "professions_jobs", "science_math",
            "coding_technology", "idioms_phrases")
    return {"word": f"phase19_en_bm_{i:07d}", "language": "en",
            "definition": "synthetic benchmark row",
            "examples": ["benchmark sentence"],
            "tags": ["synthetic", "phase19_bm"],
            "coverage_categories": [cats[i % len(cats)]],
            "register_tags": ["standard"],
            "safety_tags": [],
            "frequency_score": round(0.1 + (i % 50) / 100.0, 3),
            "word_level": "common"}


def _make_ru_row(i: int) -> dict[str, Any]:
    cats = ("core_vocabulary", "professions_jobs", "science_math",
            "coding_technology", "idioms_phrases")
    return {"word": f"фаза19_ру_bm_{i:07d}",
            "lemma": f"фаза19_ру_bm_{i:07d}",
            "part_of_speech": "noun", "language": "ru",
            "definition": "синтетическая строка бенчмарка",
            "examples": ["пример предложения"],
            "tags": ["synthetic", "phase19_bm"],
            "coverage_categories": [cats[i % len(cats)]],
            "register_tags": ["standard"],
            "safety_tags": [],
            "frequency_score": round(0.1 + (i % 50) / 100.0, 3),
            "word_level": "common"}


def create_synthetic_scale_fixture(language: str,
                                   output_path: str | Path,
                                   rows: int = DEFAULT_FIXTURE_ROWS
                                   ) -> dict[str, Any]:
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    n = max(100, min(int(rows), HARD_ROW_CAP))
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    with p.open("w", encoding="utf-8") as fh:
        if language == "en":
            for i in range(n):
                fh.write(json.dumps(_make_en_row(i), ensure_ascii=False) + "\n")
        else:
            for i in range(n):
                fh.write(json.dumps(_make_ru_row(i), ensure_ascii=False) + "\n")
    elapsed = time.perf_counter() - start
    size = p.stat().st_size
    return {"ok": True, "language": language, "path": str(p),
            "rows_written": n, "bytes": int(size),
            "elapsed_seconds": round(elapsed, 4),
            "rows_per_second": round(n / max(1e-6, elapsed), 1)}


def benchmark_streaming_read(path: str | Path,
                             max_rows: int = DEFAULT_FIXTURE_ROWS
                             ) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "path": str(p)}
    cap = max(1, min(int(max_rows), HARD_ROW_CAP))
    n = 0
    start = time.perf_counter()
    with p.open("rb") as fh:
        for _line in fh:
            n += 1
            if n >= cap:
                break
    elapsed = time.perf_counter() - start
    return {"ok": True, "path": str(p), "rows_read": n,
            "elapsed_seconds": round(elapsed, 4),
            "rows_per_second": round(n / max(1e-6, elapsed), 1)}


def benchmark_dry_run_import(path: str | Path, language: str,
                             source_type: str = "word_list",
                             max_entries: int = DEFAULT_FIXTURE_ROWS,
                             reports_dir: Optional[str | Path] = None,
                             rejections_dir: Optional[str | Path] = None,
                             checkpoint_db_path: Optional[str | Path] = None
                             ) -> dict[str, Any]:
    _ensure_flags()
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    cap = max(1, min(int(max_entries), HARD_ROW_CAP))
    start = time.perf_counter()
    res = imp.import_file(
        path=path, language=language,
        source_type=source_type, expected_format="jsonl",
        batch_size=1000, max_entries=cap, dry_run=True,
        skip_quality_gate=True,
        reports_dir=reports_dir, rejections_dir=rejections_dir,
        checkpoint_db_path=checkpoint_db_path)
    elapsed = time.perf_counter() - start
    return {"ok": bool(res.get("ok")), "language": language,
            "accepted": int(res.get("accepted") or 0),
            "rejected": int(res.get("rejected") or 0),
            "duplicates": int(res.get("duplicates") or 0),
            "batches": int(res.get("batches") or 0),
            "elapsed_seconds": round(elapsed, 4),
            "rows_per_second": round(
                int(res.get("accepted") or 0) / max(1e-6, elapsed), 1),
            "underlying_result": res}


def benchmark_index_build(language: str, limit: int = DEFAULT_FIXTURE_ROWS,
                          en_db_path: Optional[str | Path] = None,
                          ru_db_path: Optional[str | Path] = None
                          ) -> dict[str, Any]:
    _ensure_flags()
    cap = max(1, min(int(limit), HARD_ROW_CAP))
    start = time.perf_counter()
    if language == "en":
        normal = idx.ensure_english_indexes(en_db_path)
        fts = idx.build_english_fts_index(rebuild=True, limit=cap,
                                          db_path=en_db_path)
    else:
        normal = idx.ensure_russian_indexes(ru_db_path)
        fts = idx.build_russian_fts_index(rebuild=True, limit=cap,
                                          db_path=ru_db_path)
    elapsed = time.perf_counter() - start
    return {"ok": True, "language": language,
            "normal_indexes": normal,
            "fts_result": fts,
            "elapsed_seconds": round(elapsed, 4)}


def benchmark_index_query(language: str, queries: list[str],
                          limit: int = 25,
                          en_db_path: Optional[str | Path] = None,
                          ru_db_path: Optional[str | Path] = None
                          ) -> dict[str, Any]:
    _ensure_flags()
    n = max(1, min(int(limit), 100))
    qs = list(queries or [])
    per_q: list[dict[str, Any]] = []
    start = time.perf_counter()
    for q in qs:
        s = time.perf_counter()
        if language == "en":
            rows = idx.query_english_index(q, limit=n, db_path=en_db_path)
        else:
            rows = idx.query_russian_index(q, limit=n, db_path=ru_db_path)
        e = time.perf_counter() - s
        per_q.append({"query": q, "returned": len(rows),
                      "limit": n,
                      "bounds_ok": len(rows) <= n,
                      "elapsed_seconds": round(e, 4)})
    elapsed = time.perf_counter() - start
    return {"ok": True, "language": language, "n_queries": len(qs),
            "per_query": per_q,
            "total_elapsed_seconds": round(elapsed, 4),
            "queries_per_second": round(len(qs) / max(1e-6, elapsed), 1)}


def benchmark_retrieval_eval(limit: int = 25,
                             en_db_path: Optional[str | Path] = None,
                             ru_db_path: Optional[str | Path] = None
                             ) -> dict[str, Any]:
    _ensure_flags()
    n = max(1, min(int(limit), 100))
    start = time.perf_counter()
    en = rqe.run_english_retrieval_eval(limit=n, db_path=en_db_path)
    ru = rqe.run_russian_retrieval_eval(limit=n, db_path=ru_db_path)
    elapsed = time.perf_counter() - start
    return {"ok": True, "limit": n,
            "en_average_score": en.get("average_score"),
            "ru_average_score": ru.get("average_score"),
            "en_bounds_ok": en.get("bounds_ok"),
            "ru_bounds_ok": ru.get("bounds_ok"),
            "en_safety_ok": en.get("safety_ok"),
            "ru_safety_ok": ru.get("safety_ok"),
            "elapsed_seconds": round(elapsed, 4)}


def write_benchmark_report(report: dict[str, Any],
                           output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "HARD_ROW_CAP",
    "DEFAULT_FIXTURE_ROWS",
    "create_synthetic_scale_fixture",
    "benchmark_streaming_read",
    "benchmark_dry_run_import",
    "benchmark_index_build",
    "benchmark_index_query",
    "benchmark_retrieval_eval",
    "write_benchmark_report",
]
