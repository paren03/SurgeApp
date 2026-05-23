"""Phase 20 - Dual Retrieval SLA Evaluator.

Measures retrieval latency against documented SLA targets. Bounded,
read-only. Returns pass/warn/fail verdicts per metric. No daemon.

Default targets (local hardware, warm DB):
    simple lookup p95             <= 150 ms
    category lookup p95           <= 250 ms
    register lookup p95           <= 250 ms
    safety-filtered lookup p95    <= 300 ms
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import dual_retrieval_index_builder as idx


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def define_retrieval_sla() -> dict[str, Any]:
    return {
        "simple_lookup_p95_ms": 150.0,
        "category_lookup_p95_ms": 250.0,
        "register_lookup_p95_ms": 250.0,
        "safety_filter_lookup_p95_ms": 300.0,
        "limit_max": 25,
    }


def _p_value(samples: list[float], p: float) -> float:
    if not samples:
        return 0.0
    s = sorted(samples)
    k = max(0, min(len(s) - 1,
                    int(round((p / 100.0) * (len(s) - 1)))))
    return round(s[k], 3)


def _verdict(observed_ms: float, target_ms: float) -> str:
    if observed_ms <= target_ms:
        return "pass"
    if observed_ms <= target_ms * 2.0:
        return "warn"
    return "fail"


def _bench_query(fn, args_list, limit: int) -> dict[str, Any]:
    times_ms: list[float] = []
    n_results: list[int] = []
    bounds_ok = True
    for args in args_list:
        s = time.perf_counter()
        try:
            rows = fn(*args, limit=limit)
        except Exception:
            rows = []
        elapsed_ms = (time.perf_counter() - s) * 1000.0
        times_ms.append(elapsed_ms)
        n_results.append(len(rows))
        if len(rows) > limit:
            bounds_ok = False
    return {"n_calls": len(args_list),
            "limit": limit, "bounds_ok": bounds_ok,
            "results_max": max(n_results) if n_results else 0,
            "avg_ms": round(sum(times_ms) / max(1, len(times_ms)), 3),
            "p50_ms": _p_value(times_ms, 50),
            "p95_ms": _p_value(times_ms, 95),
            "p99_ms": _p_value(times_ms, 99)}


def benchmark_query_latency(language: str,
                            queries: list[str],
                            limit: int = 25,
                            en_db_path: Optional[str | Path] = None,
                            ru_db_path: Optional[str | Path] = None
                            ) -> dict[str, Any]:
    _ensure_flags()
    n = max(1, min(int(limit), 25))
    fn = (lambda q, limit: idx.query_english_index(q, limit=limit,
                                                   db_path=en_db_path)) \
        if language == "en" else \
        (lambda q, limit: idx.query_russian_index(q, limit=limit,
                                                   db_path=ru_db_path))
    res = _bench_query(fn, [(q,) for q in queries], n)
    res["language"] = language
    return res


def benchmark_category_lookup_latency(language: str,
                                      categories: list[str],
                                      limit: int = 25,
                                      en_db_path: Optional[str | Path] = None,
                                      ru_db_path: Optional[str | Path] = None
                                      ) -> dict[str, Any]:
    _ensure_flags()
    n = max(1, min(int(limit), 25))
    db_path = en_db_path if language == "en" else ru_db_path
    fn = lambda c, limit: idx.query_by_category(language, c, limit=limit,
                                                 db_path=db_path)
    res = _bench_query(fn, [(c,) for c in categories], n)
    res["language"] = language
    return res


def benchmark_register_lookup_latency(language: str,
                                      register_tags: list[str],
                                      limit: int = 25,
                                      en_db_path: Optional[str | Path] = None,
                                      ru_db_path: Optional[str | Path] = None
                                      ) -> dict[str, Any]:
    _ensure_flags()
    n = max(1, min(int(limit), 25))
    db_path = en_db_path if language == "en" else ru_db_path
    fn = lambda t, limit: idx.query_by_register(language, t, limit=limit,
                                                 db_path=db_path)
    res = _bench_query(fn, [(t,) for t in register_tags], n)
    res["language"] = language
    return res


def benchmark_safety_filter_latency(language: str,
                                    safety_tags: list[str],
                                    limit: int = 25,
                                    en_db_path: Optional[str | Path] = None,
                                    ru_db_path: Optional[str | Path] = None
                                    ) -> dict[str, Any]:
    _ensure_flags()
    n = max(1, min(int(limit), 25))
    db_path = en_db_path if language == "en" else ru_db_path
    fn = lambda s, limit: idx.query_by_safety(language, s, limit=limit,
                                               db_path=db_path)
    res = _bench_query(fn, [(s,) for s in safety_tags], n)
    res["language"] = language
    return res


def evaluate_sla_results(results: dict[str, Any]) -> dict[str, Any]:
    sla = define_retrieval_sla()
    out: dict[str, Any] = {"sla": sla, "verdicts": {}}
    for key, target_key in (("simple_lookup", "simple_lookup_p95_ms"),
                            ("category_lookup", "category_lookup_p95_ms"),
                            ("register_lookup", "register_lookup_p95_ms"),
                            ("safety_filter_lookup",
                             "safety_filter_lookup_p95_ms")):
        section = results.get(key) or {}
        # section may be {"en": {...}, "ru": {...}}
        per_lang: dict[str, Any] = {}
        for lang in ("en", "ru"):
            data = section.get(lang) if isinstance(section, dict) else None
            if not data:
                per_lang[lang] = {"verdict": "skipped"}
                continue
            p95 = float(data.get("p95_ms", 0.0))
            per_lang[lang] = {"p95_ms": p95,
                              "verdict": _verdict(p95, sla[target_key]),
                              "bounds_ok": bool(data.get("bounds_ok"))}
        out["verdicts"][key] = per_lang
    worst = "pass"
    for _, by_lang in out["verdicts"].items():
        for _, v in by_lang.items():
            if v.get("verdict") == "fail":
                worst = "fail"
            elif v.get("verdict") == "warn" and worst == "pass":
                worst = "warn"
    out["overall_verdict"] = worst
    return out


def write_sla_report(report: dict[str, Any],
                     output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "define_retrieval_sla",
    "benchmark_query_latency",
    "benchmark_category_lookup_latency",
    "benchmark_register_lookup_latency",
    "benchmark_safety_filter_latency",
    "evaluate_sla_results",
    "write_sla_report",
]
