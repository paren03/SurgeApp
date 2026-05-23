"""Phase 19 - Scale Runner.

Coordinates Phase 19 workflow:
  pre-flight -> folder setup -> synthetic fixture generation ->
  quality gates -> scale-plan build -> dry-run -> index build ->
  dedupe reports -> benchmarks -> retrieval eval -> coverage report.

Real scale imports are OFF by default. Operator must pass
``allow_real_import=True`` AND each plan must clear quality gate + dry-run +
hard caps to allow a real write.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import dual_corpus_quality_gate as qg
import dual_corpus_chunked_importer as imp
import dual_corpus_pilot_import_planner as pip_
import dual_retrieval_quality_eval as rqe
import dual_coverage_reporter as cr
import dual_scale_import_planner as sip
import dual_retrieval_index_builder as idx
import dual_dedupe_collision_reporter as dedup
import dual_import_performance_benchmark as bench


PHASE19_SCALE_DIR = Path("corpus_sources/scale_plans/phase19")
PHASE19_BENCH_DIR = Path("corpus_sources/benchmarks/phase19")
PHASE19_DEDUPE_DIR = Path("corpus_sources/dedupe_reports/phase19")
PHASE19_INDEX_DIR = Path("corpus_sources/indexes/phase19")
PHASE19_EVAL_DIR = Path("corpus_sources/evaluations/phase19")
PHASE19_COVERAGE_DIR = Path("corpus_sources/coverage_reports/phase19")
PHASE19_SYNTH_DIR = Path("corpus_sources/quality_samples/phase19_synthetic")
PHASE19_REPORTS_DIR = Path("corpus_sources/reports/phase19")


def verify_phase19_preflight() -> dict[str, Any]:
    return sip.verify_phase18_preflight()


def setup_phase19_folders() -> dict[str, Any]:
    out: dict[str, str] = {}
    for d in (PHASE19_SCALE_DIR, PHASE19_BENCH_DIR, PHASE19_DEDUPE_DIR,
              PHASE19_INDEX_DIR, PHASE19_EVAL_DIR, PHASE19_COVERAGE_DIR,
              PHASE19_SYNTH_DIR, PHASE19_REPORTS_DIR):
        d.mkdir(parents=True, exist_ok=True)
        out[d.name] = str(d)
    return {"ok": True, "folders": out}


def create_phase19_synthetic_fixtures(rows_per_language: int = 100_000
                                      ) -> dict[str, Any]:
    PHASE19_SYNTH_DIR.mkdir(parents=True, exist_ok=True)
    en_path = PHASE19_SYNTH_DIR / "english_scale_fixture.jsonl"
    ru_path = PHASE19_SYNTH_DIR / "russian_scale_fixture.jsonl"
    en = bench.create_synthetic_scale_fixture("en", en_path,
                                              rows=rows_per_language)
    ru = bench.create_synthetic_scale_fixture("ru", ru_path,
                                              rows=rows_per_language)
    return {"ok": en.get("ok") and ru.get("ok"),
            "en": en, "ru": ru}


def run_phase19_quality_gates(fixtures: dict[str, Any]
                              ) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for lang in ("en", "ru"):
        f = fixtures.get(lang) or {}
        path = f.get("path")
        if not path:
            out[lang] = {"ok": False, "error": "no_path"}
            continue
        rep = qg.generate_quality_gate_report(path, "jsonl", lang,
                                              sample_size=100)
        gate = qg.should_allow_import(rep, min_quality_score=0.75)
        out[lang] = {"path": path,
                     "quality_score": rep.get("quality_score"),
                     "gate_open": gate.get("ok"),
                     "quality_report": rep}
    return out


def build_phase19_scale_plans(fixtures: dict[str, Any]
                              ) -> dict[str, Any]:
    plans: dict[str, Any] = {}
    for lang in ("en", "ru"):
        f = fixtures.get(lang) or {}
        path = f.get("path")
        if not path:
            plans[lang] = {"ok": False, "error": "no_path"}
            continue
        rows = int(f.get("rows_written") or 0)
        capacity = sip.estimate_candidate_capacity({"path": path})
        srcs = [{"path": path, "language": lang, "source_type": "word_list",
                 "expected_format": "jsonl", "adapter_type": "luna_jsonl",
                 "row_estimate": int(capacity.get("row_estimate") or rows),
                 "capacity_within_phase19_cap":
                     int(capacity.get("capacity_within_phase19_cap") or
                         min(rows, sip.HARD_PER_SOURCE_CAP))}]
        bp = sip.build_scale_plan(srcs, language=lang,
                                  target_total=min(rows, sip.HARD_TARGET_PER_LANG),
                                  per_source_cap=sip.HARD_PER_SOURCE_CAP,
                                  batch_size=sip.HARD_BATCH_SIZE,
                                  notes="phase19_synthetic")
        if bp.get("ok"):
            plan_path = PHASE19_SCALE_DIR / f"{bp['plan']['scale_plan_id']}.plan.json"
            sip.write_scale_plan(bp["plan"], plan_path)
            plans[lang] = {"ok": True, "plan": bp["plan"],
                           "plan_path": str(plan_path),
                           "summary": sip.summarize_scale_plan(bp["plan"])}
        else:
            plans[lang] = {"ok": False, "error": bp.get("error")}
    return plans


def run_phase19_dry_runs(plans: dict[str, Any],
                         checkpoint_db_path: Optional[str | Path] = None,
                         max_per_lang: int = 5000
                         ) -> dict[str, Any]:
    """Stream-bounded dry-run import per plan. Defaults to 5000 rows for
    speed; callers can raise it. Always dry_run=True regardless."""
    out: dict[str, Any] = {}
    for lang, payload in plans.items():
        if not payload.get("ok"):
            out[lang] = {"ok": False, "error": payload.get("error")}
            continue
        plan = payload["plan"]
        src = (plan.get("source_records") or [{}])[0]
        path = src.get("path")
        if not path:
            out[lang] = {"ok": False, "error": "no_source_path"}
            continue
        res = imp.import_file(
            path=path, language=lang, source_type="word_list",
            expected_format="jsonl",
            batch_size=plan.get("batch_size", 1000),
            max_entries=min(int(max_per_lang),
                            int(plan.get("per_source_cap",
                                          sip.HARD_PER_SOURCE_CAP))),
            dry_run=True, skip_quality_gate=True,
            checkpoint_db_path=checkpoint_db_path,
            reports_dir=PHASE19_REPORTS_DIR,
            rejections_dir=PHASE19_REPORTS_DIR)
        out[lang] = {"ok": bool(res.get("ok")),
                     "accepted": res.get("accepted"),
                     "rejected": res.get("rejected"),
                     "duplicates": res.get("duplicates"),
                     "report_path": res.get("report_path")}
    return out


def run_phase19_index_builds(en_db_path: Optional[str | Path] = None,
                             ru_db_path: Optional[str | Path] = None,
                             limit: Optional[int] = None,
                             ) -> dict[str, Any]:
    return idx.rebuild_all_indexes(limit=limit,
                                   en_db_path=en_db_path,
                                   ru_db_path=ru_db_path)


def run_phase19_dedupe_reports(en_db_path: Optional[str | Path] = None,
                               ru_db_path: Optional[str | Path] = None,
                               limit: int = 1000
                               ) -> dict[str, Any]:
    payload = {"generated_at": time.time(), "limit": int(limit), "en": {}, "ru": {}}
    for lang, db in (("en", en_db_path), ("ru", ru_db_path)):
        dup = dedup.find_exact_duplicates(lang, limit=limit, db_path=db)
        coll = dedup.find_pack_collisions(lang, limit=limit, db_path=db)
        cross = dedup.find_cross_category_reuse(lang, limit=limit, db_path=db)
        miss_pid = dedup.find_missing_pack_ids(lang, limit=limit, db_path=db)
        miss_safe = dedup.find_missing_safety_tags(lang, limit=limit, db_path=db)
        miss_reg = dedup.find_missing_register_tags(lang, limit=limit, db_path=db)
        payload[lang] = {
            "exact_duplicates_count": len(dup),
            "pack_collisions_count": len(coll),
            "cross_category_reuse_count": len(cross),
            "missing_pack_ids_count": len(miss_pid),
            "missing_safety_tags_count": len(miss_safe),
            "missing_register_tags_count": len(miss_reg),
            "samples": {
                "exact_duplicates": dup[:20],
                "pack_collisions": coll[:20],
                "cross_category_reuse": cross[:20],
                "missing_pack_ids": miss_pid[:10],
                "missing_safety_tags": miss_safe[:10],
                "missing_register_tags": miss_reg[:10],
            },
        }
    out_path = PHASE19_DEDUPE_DIR / "phase19_dedupe_report.json"
    dedup.write_dedupe_report(payload, out_path)
    payload["report_path"] = str(out_path)
    return payload


def run_phase19_benchmarks(fixtures: dict[str, Any],
                           en_db_path: Optional[str | Path] = None,
                           ru_db_path: Optional[str | Path] = None,
                           ) -> dict[str, Any]:
    out: dict[str, Any] = {"per_fixture": {}}
    for lang in ("en", "ru"):
        f = fixtures.get(lang) or {}
        path = f.get("path")
        if not path:
            continue
        out["per_fixture"][lang] = {
            "streaming_read": bench.benchmark_streaming_read(
                path, max_rows=int(f.get("rows_written") or 100_000)),
            "dry_run_import": bench.benchmark_dry_run_import(
                path, lang, source_type="word_list",
                max_entries=10_000,
                reports_dir=PHASE19_REPORTS_DIR,
                rejections_dir=PHASE19_REPORTS_DIR),
        }
    out["index_build"] = {
        "en": bench.benchmark_index_build("en", limit=100_000,
                                          en_db_path=en_db_path,
                                          ru_db_path=ru_db_path),
        "ru": bench.benchmark_index_build("ru", limit=100_000,
                                          en_db_path=en_db_path,
                                          ru_db_path=ru_db_path),
    }
    queries_en = ["engineer", "ledger", "verse", "vector", "function"]
    queries_ru = ["инженер", "бюджет", "стих", "число", "функция"]
    out["index_query"] = {
        "en": bench.benchmark_index_query("en", queries_en, limit=25,
                                          en_db_path=en_db_path,
                                          ru_db_path=ru_db_path),
        "ru": bench.benchmark_index_query("ru", queries_ru, limit=25,
                                          en_db_path=en_db_path,
                                          ru_db_path=ru_db_path),
    }
    out["retrieval_eval"] = bench.benchmark_retrieval_eval(
        limit=15, en_db_path=en_db_path, ru_db_path=ru_db_path)
    out_path = PHASE19_BENCH_DIR / "phase19_benchmark_report.json"
    bench.write_benchmark_report(out, out_path)
    out["report_path"] = str(out_path)
    return out


def run_phase19_retrieval_evals(en_db_path: Optional[str | Path] = None,
                                ru_db_path: Optional[str | Path] = None,
                                limit: int = 15
                                ) -> dict[str, Any]:
    PHASE19_EVAL_DIR.mkdir(parents=True, exist_ok=True)
    en = rqe.run_english_retrieval_eval(limit=limit, db_path=en_db_path)
    ru = rqe.run_russian_retrieval_eval(limit=limit, db_path=ru_db_path)
    rqe.write_retrieval_eval_report(en, PHASE19_EVAL_DIR / "phase19_retrieval_en.json")
    rqe.write_retrieval_eval_report(ru, PHASE19_EVAL_DIR / "phase19_retrieval_ru.json")
    return {"ok": True, "en": en, "ru": ru}


def run_phase19_coverage_reports(en_db_path: Optional[str | Path] = None,
                                 ru_db_path: Optional[str | Path] = None
                                 ) -> dict[str, Any]:
    PHASE19_COVERAGE_DIR.mkdir(parents=True, exist_ok=True)
    return cr.write_coverage_report(
        PHASE19_COVERAGE_DIR / "phase19_coverage_report.json",
        en_db_path=en_db_path, ru_db_path=ru_db_path)


def run_phase19_real_scale_imports(
    plans: Optional[dict[str, Any]] = None,
    *,
    allow_real_import: bool = False,
    max_total_per_language: int = 100_000,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
    registry_db_path: Optional[str | Path] = None,
    checkpoint_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if not allow_real_import:
        return {"ok": True, "allow_real_import": False,
                "reason": "real_scale_import_disabled_by_default"}
    if int(max_total_per_language) > sip.HARD_TARGET_PER_LANG:
        return {"ok": False,
                "error": f"max_total_per_language_exceeds_{sip.HARD_TARGET_PER_LANG}"}
    if not plans:
        return {"ok": False, "error": "no_plans_supplied"}

    results: dict[str, Any] = {}
    for lang, payload in plans.items():
        if not payload.get("ok"):
            results[lang] = {"ok": False, "error": payload.get("error",
                                                                "plan_invalid")}
            continue
        plan = payload["plan"]
        if plan.get("allow_full_source"):
            results[lang] = {"ok": False,
                             "error": "allow_full_source_forbidden_in_phase19"}
            continue
        src = (plan.get("source_records") or [{}])[0]
        path = src.get("path")
        if not path or not Path(path).exists():
            results[lang] = {"ok": False, "error": "source_path_missing"}
            continue
        # Build a corpus row + pilot plan + dry-run via the Phase 17 planner.
        bp = pip_.build_pilot_plan(source_path=path, language=lang,
                                   source_type="word_list",
                                   adapter_type="luna_jsonl",
                                   target_entries=min(int(max_total_per_language),
                                                      sip.HARD_PER_SOURCE_CAP))
        if not bp.get("ok"):
            results[lang] = {"ok": False,
                             "error": f"pilot_plan_failed: {bp.get('error')}"}
            continue
        pp = bp["plan"]
        gate = pip_.require_quality_gate_pass(pp.get("quality_report") or {})
        if not gate.get("ok"):
            results[lang] = {"ok": False, "error": "quality_gate_closed",
                             "gate_decision": gate}
            continue
        # Dry-run first
        dr = pip_.run_pilot_dry_run(pp,
                                    registry_db_path=registry_db_path,
                                    checkpoint_db_path=checkpoint_db_path,
                                    en_db_path=en_db_path,
                                    ru_db_path=ru_db_path)
        if not dr.get("ok"):
            results[lang] = {"ok": False, "error": "dry_run_failed",
                             "dry_run": dr}
            continue
        rr = pip_.run_pilot_import(pp, dry_run=False,
                                   registry_db_path=registry_db_path,
                                   checkpoint_db_path=checkpoint_db_path,
                                   en_db_path=en_db_path,
                                   ru_db_path=ru_db_path)
        results[lang] = {"ok": bool(rr.get("ok")),
                         "real_accepted": int(rr.get("accepted") or 0),
                         "rollback_key": pp.get("rollback_key"),
                         "plan_id": pp.get("plan_id"),
                         "underlying_result": rr}
    return {"ok": True, "allow_real_import": True,
            "results": results}


def write_phase19_report(report: dict[str, Any],
                         output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "PHASE19_SCALE_DIR", "PHASE19_BENCH_DIR", "PHASE19_DEDUPE_DIR",
    "PHASE19_INDEX_DIR", "PHASE19_EVAL_DIR", "PHASE19_COVERAGE_DIR",
    "PHASE19_SYNTH_DIR", "PHASE19_REPORTS_DIR",
    "verify_phase19_preflight",
    "setup_phase19_folders",
    "create_phase19_synthetic_fixtures",
    "run_phase19_quality_gates",
    "build_phase19_scale_plans",
    "run_phase19_dry_runs",
    "run_phase19_index_builds",
    "run_phase19_dedupe_reports",
    "run_phase19_benchmarks",
    "run_phase19_retrieval_evals",
    "run_phase19_coverage_reports",
    "run_phase19_real_scale_imports",
    "write_phase19_report",
]
