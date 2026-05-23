"""Phase 20 - Million Readiness Runner.

Coordinates the Phase 20 workflow:
  pre-flight -> folders -> backup -> stage plan -> dry-run rehearsal ->
  synthetic million rehearsal -> post-stage audit -> retrieval SLA ->
  index consistency -> safety regression -> optional real staged import.

Real staged import is OFF by default. Multiple hard gates required even with
``allow_real_import=True``.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import dual_million_stage_planner as msp
import dual_vocab_backup_restore as bk
import dual_import_batch_ledger as led
import dual_post_stage_quality_audit as pq
import dual_retrieval_sla_eval as sla
import dual_index_consistency_checker as ic
import dual_safety_regression_auditor as sra
import dual_import_performance_benchmark as bench
import dual_corpus_chunked_importer as imp
import dual_corpus_quality_gate as qg


PHASE20_BASE = Path("corpus_sources/phase20")
PHASE20_STAGE_DIR = PHASE20_BASE / "stage_plans"
PHASE20_BACKUP_REPORTS_DIR = PHASE20_BASE / "backup_reports"
PHASE20_LEDGER_REPORTS_DIR = PHASE20_BASE / "ledger_reports"
PHASE20_QUALITY_AUDITS_DIR = PHASE20_BASE / "quality_audits"
PHASE20_SLA_REPORTS_DIR = PHASE20_BASE / "sla_reports"
PHASE20_INDEX_REPORTS_DIR = PHASE20_BASE / "index_reports"
PHASE20_SAFETY_REPORTS_DIR = PHASE20_BASE / "safety_reports"
PHASE20_SYNTH_DIR = PHASE20_BASE / "synthetic_million"
PHASE20_FINAL_DIR = PHASE20_BASE / "final_readiness"


def verify_phase20_preflight() -> dict[str, Any]:
    return msp.verify_phase19_preflight()


def setup_phase20_folders() -> dict[str, Any]:
    out: dict[str, str] = {}
    for d in (PHASE20_BASE, PHASE20_STAGE_DIR, PHASE20_BACKUP_REPORTS_DIR,
              PHASE20_LEDGER_REPORTS_DIR, PHASE20_QUALITY_AUDITS_DIR,
              PHASE20_SLA_REPORTS_DIR, PHASE20_INDEX_REPORTS_DIR,
              PHASE20_SAFETY_REPORTS_DIR, PHASE20_SYNTH_DIR,
              PHASE20_FINAL_DIR, bk.BACKUP_BASE):
        d.mkdir(parents=True, exist_ok=True)
        out[d.name] = str(d)
    return {"ok": True, "folders": out}


def create_phase20_backup_snapshot(label: str = "phase20"
                                   ) -> dict[str, Any]:
    snap = bk.create_backup_snapshot(label=label, include_manifests=True)
    if snap.get("ok"):
        path = PHASE20_BACKUP_REPORTS_DIR / f"{snap['snapshot_id']}.report.json"
        bk.write_backup_report(snap, path)
        snap["report_path"] = str(path)
    return snap


def build_phase20_stage_plans(
    language: Optional[str] = None,
    target_total: int = msp.DEFAULT_TARGET_TOTAL,
    stage_size: int = msp.HARD_STAGE_SIZE,
    per_source_cap: int = msp.HARD_PER_SOURCE_CAP,
) -> dict[str, Any]:
    langs = ("en", "ru") if language is None else (language,)
    out: dict[str, Any] = {}
    for lang in langs:
        sources = msp.discover_eligible_sources(language=lang, limit=200)
        # If incoming is empty, build an EMPTY plan (still valid, 0 stages).
        bp = msp.build_million_stage_plan(
            language=lang, source_records=sources,
            target_total=target_total,
            stage_size=stage_size,
            per_source_cap=per_source_cap,
            notes="phase20")
        if bp.get("ok"):
            plan = bp["plan"]
            path = PHASE20_STAGE_DIR / f"{plan['million_plan_id']}.plan.json"
            msp.write_stage_plan(plan, path)
            out[lang] = {"ok": True, "plan": plan, "plan_path": str(path),
                         "summary": msp.summarize_stage_plan(plan)}
        else:
            out[lang] = {"ok": False, "error": bp.get("error")}
    return out


def run_phase20_dry_run_rehearsal(
    fixtures: dict[str, Any],
    *,
    max_per_lang: int = 5000,
    checkpoint_db_path: Optional[str | Path] = None,
    ledger_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    """Dry-run import of the synthetic-million fixtures, capped low for
    speed. Records a ledger row for each language."""
    out: dict[str, Any] = {}
    for lang in ("en", "ru"):
        f = fixtures.get(lang) or {}
        path = f.get("path")
        if not path:
            out[lang] = {"ok": False, "error": "no_path"}
            continue
        ledger_row = led.create_batch_record(
            language=lang, stage_id="phase20_dryrun",
            source_path=path, dry_run=True,
            db_path=ledger_db_path,
            notes="phase20_dry_run_rehearsal")
        bid = ledger_row.get("batch_id")
        res = imp.import_file(
            path=path, language=lang, source_type="word_list",
            expected_format="jsonl",
            batch_size=1000, max_entries=int(max_per_lang),
            dry_run=True, skip_quality_gate=True,
            checkpoint_db_path=checkpoint_db_path,
            reports_dir=PHASE20_QUALITY_AUDITS_DIR,
            rejections_dir=PHASE20_QUALITY_AUDITS_DIR)
        if bid:
            led.update_batch_status(
                bid, "completed",
                accepted_count=int(res.get("accepted") or 0),
                rejected_count=int(res.get("rejected") or 0),
                duplicate_count=int(res.get("duplicates") or 0),
                completed=True, db_path=ledger_db_path)
        out[lang] = {"ok": bool(res.get("ok")),
                     "accepted": res.get("accepted"),
                     "rejected": res.get("rejected"),
                     "duplicates": res.get("duplicates"),
                     "report_path": res.get("report_path"),
                     "batch_id": bid}
    return out


def run_phase20_synthetic_million_rehearsal(rows_per_language: int = 1_000_000
                                            ) -> dict[str, Any]:
    """Generate (or reuse) million-row synthetic fixtures and stream-validate
    them via the quality gate. Caps at 1M; can be lowered for speed.
    """
    PHASE20_SYNTH_DIR.mkdir(parents=True, exist_ok=True)
    en_path = PHASE20_SYNTH_DIR / "english_million_fixture.jsonl"
    ru_path = PHASE20_SYNTH_DIR / "russian_million_fixture.jsonl"
    en = bench.create_synthetic_scale_fixture("en", en_path,
                                              rows=rows_per_language)
    ru = bench.create_synthetic_scale_fixture("ru", ru_path,
                                              rows=rows_per_language)
    # Stream-only quality validation: sample 100 rows out of however many.
    qg_en = qg.generate_quality_gate_report(en_path, "jsonl", "en",
                                            sample_size=100)
    qg_ru = qg.generate_quality_gate_report(ru_path, "jsonl", "ru",
                                            sample_size=100)
    # Bounded streaming read benchmark
    rs_en = bench.benchmark_streaming_read(en_path,
                                           max_rows=rows_per_language)
    rs_ru = bench.benchmark_streaming_read(ru_path,
                                           max_rows=rows_per_language)
    return {"ok": en.get("ok") and ru.get("ok"),
            "rows_per_language": int(rows_per_language),
            "fixtures": {"en": en, "ru": ru},
            "quality_gates": {"en": qg_en, "ru": qg_ru},
            "streaming_read": {"en": rs_en, "ru": rs_ru}}


def run_phase20_post_stage_quality_audits(language: str = "en",
                                          db_path: Optional[str | Path] = None,
                                          ) -> dict[str, Any]:
    rows = pq.sample_recent_import_rows(language, limit=500, db_path=db_path)
    res = {
        "metadata_completeness": pq.audit_metadata_completeness(rows),
        "language_consistency": pq.audit_language_consistency(rows, language),
        "safety_consistency": pq.audit_safety_tag_consistency(rows),
        "register_consistency": pq.audit_register_tag_consistency(rows),
        "coverage_consistency": pq.audit_coverage_category_consistency(rows),
        "duplicate_rate": pq.audit_duplicate_rate(language, db_path=db_path),
        "rejected_rows": pq.audit_rejected_rows(language, limit=200),
    }
    score = pq.compute_stage_quality_score(res)
    res["quality_score"] = score
    out_path = PHASE20_QUALITY_AUDITS_DIR / f"phase20_quality_audit_{language}.json"
    pq.write_post_stage_quality_audit(res, out_path)
    res["report_path"] = str(out_path)
    return res


def run_phase20_retrieval_sla_eval(en_db_path: Optional[str | Path] = None,
                                   ru_db_path: Optional[str | Path] = None,
                                   ) -> dict[str, Any]:
    en_queries = ["engineer", "ledger", "verse", "vector", "function",
                  "essence", "statute"]
    ru_queries = ["инженер", "бюджет", "стих", "число", "функция",
                  "сущность", "закон"]
    categories = ["core_vocabulary", "professions_jobs", "science_math",
                  "coding_technology", "law_government"]
    register_tags = ["standard", "professional", "technical"]
    safety_tags = ["recognition_only", "do_not_use_unprompted",
                   "vulgar", "offensive"]
    simple = {
        "en": sla.benchmark_query_latency("en", en_queries, limit=25,
                                          en_db_path=en_db_path),
        "ru": sla.benchmark_query_latency("ru", ru_queries, limit=25,
                                          ru_db_path=ru_db_path),
    }
    cat = {
        "en": sla.benchmark_category_lookup_latency("en", categories,
                                                    limit=25,
                                                    en_db_path=en_db_path),
        "ru": sla.benchmark_category_lookup_latency("ru", categories,
                                                    limit=25,
                                                    ru_db_path=ru_db_path),
    }
    reg = {
        "en": sla.benchmark_register_lookup_latency("en", register_tags,
                                                    limit=25,
                                                    en_db_path=en_db_path),
        "ru": sla.benchmark_register_lookup_latency("ru", register_tags,
                                                    limit=25,
                                                    ru_db_path=ru_db_path),
    }
    saf = {
        "en": sla.benchmark_safety_filter_latency("en", safety_tags,
                                                   limit=25,
                                                   en_db_path=en_db_path),
        "ru": sla.benchmark_safety_filter_latency("ru", safety_tags,
                                                   limit=25,
                                                   ru_db_path=ru_db_path),
    }
    bundle = {"simple_lookup": simple,
              "category_lookup": cat,
              "register_lookup": reg,
              "safety_filter_lookup": saf}
    verdict = sla.evaluate_sla_results(bundle)
    bundle["verdict"] = verdict
    path = PHASE20_SLA_REPORTS_DIR / "phase20_retrieval_sla.json"
    sla.write_sla_report(bundle, path)
    bundle["report_path"] = str(path)
    return bundle


def run_phase20_index_consistency_checks(en_db_path: Optional[str | Path] = None,
                                         ru_db_path: Optional[str | Path] = None,
                                         ) -> dict[str, Any]:
    payload = {
        "generated_at": time.time(),
        "en_index": ic.check_english_index_consistency(db_path=en_db_path),
        "ru_index": ic.check_russian_index_consistency(db_path=ru_db_path),
        "fts_counts": ic.check_fts_row_counts(en_db_path=en_db_path,
                                              ru_db_path=ru_db_path),
        "pack_id_en": ic.check_pack_id_index_coverage("en",
                                                      db_path=en_db_path),
        "pack_id_ru": ic.check_pack_id_index_coverage("ru",
                                                      db_path=ru_db_path),
        "category_en": ic.check_category_index_coverage("en",
                                                         db_path=en_db_path),
        "category_ru": ic.check_category_index_coverage("ru",
                                                         db_path=ru_db_path),
        "safety_filter_en": ic.check_safety_filter_index_behavior(
            "en", db_path=en_db_path),
        "safety_filter_ru": ic.check_safety_filter_index_behavior(
            "ru", db_path=ru_db_path),
        "bounds_en": ic.check_index_query_bounds("en", db_path=en_db_path),
        "bounds_ru": ic.check_index_query_bounds("ru", db_path=ru_db_path),
    }
    path = PHASE20_INDEX_REPORTS_DIR / "phase20_index_consistency.json"
    ic.write_index_consistency_report(payload, path)
    payload["report_path"] = str(path)
    return payload


def run_phase20_safety_regression_audit(en_db_path: Optional[str | Path] = None,
                                        ru_db_path: Optional[str | Path] = None,
                                        ) -> dict[str, Any]:
    payload = {
        "generated_at": time.time(),
        "english_policy": sra.audit_english_safety_policy(),
        "russian_policy": sra.audit_russian_safety_policy(),
        "indexed_safety_en": sra.audit_indexed_retrieval_safety(
            "en", en_db_path=en_db_path),
        "indexed_safety_ru": sra.audit_indexed_retrieval_safety(
            "ru", ru_db_path=ru_db_path),
        "runtime_context_en": sra.audit_runtime_context_safety(
            "en", db_path=en_db_path),
        "runtime_context_ru": sra.audit_runtime_context_safety(
            "ru", db_path=ru_db_path),
        "prompted_vs_unprompted_en":
            sra.audit_prompted_vs_unprompted_behavior("en"),
        "prompted_vs_unprompted_ru":
            sra.audit_prompted_vs_unprompted_behavior("ru"),
    }
    path = PHASE20_SAFETY_REPORTS_DIR / "phase20_safety_regression.json"
    sra.write_safety_regression_report(payload, path)
    payload["report_path"] = str(path)
    return payload


def run_phase20_real_stage_imports(
    plans: Optional[dict[str, Any]] = None,
    *,
    allow_real_import: bool = False,
    max_stage_size: int = msp.HARD_STAGE_SIZE,
    backup_snapshot_id: Optional[str] = None,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
    registry_db_path: Optional[str | Path] = None,
    checkpoint_db_path: Optional[str | Path] = None,
    ledger_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if not allow_real_import:
        return {"ok": True, "allow_real_import": False,
                "reason": "real_stage_import_disabled_by_default"}
    if int(max_stage_size) > msp.HARD_STAGE_SIZE:
        return {"ok": False,
                "error": f"max_stage_size_exceeds_{msp.HARD_STAGE_SIZE}"}
    if not backup_snapshot_id:
        return {"ok": False, "error": "backup_snapshot_required"}
    ver = bk.verify_backup_snapshot(backup_snapshot_id)
    if not ver.get("ok"):
        return {"ok": False, "error": "backup_snapshot_unverified",
                "details": ver}
    if not plans:
        return {"ok": False, "error": "no_plans_supplied"}
    return {"ok": True, "allow_real_import": True,
            "note": ("real_import_path_documented_but_no_stage_was_run_in_this_call;"
                     " operator must drive per-stage execution with the pilot"
                     " planner and ledger explicitly"),
            "backup_snapshot_id": backup_snapshot_id}


def write_phase20_report(report: dict[str, Any],
                         output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "PHASE20_BASE", "PHASE20_STAGE_DIR", "PHASE20_BACKUP_REPORTS_DIR",
    "PHASE20_LEDGER_REPORTS_DIR", "PHASE20_QUALITY_AUDITS_DIR",
    "PHASE20_SLA_REPORTS_DIR", "PHASE20_INDEX_REPORTS_DIR",
    "PHASE20_SAFETY_REPORTS_DIR", "PHASE20_SYNTH_DIR",
    "PHASE20_FINAL_DIR",
    "verify_phase20_preflight",
    "setup_phase20_folders",
    "create_phase20_backup_snapshot",
    "build_phase20_stage_plans",
    "run_phase20_dry_run_rehearsal",
    "run_phase20_synthetic_million_rehearsal",
    "run_phase20_post_stage_quality_audits",
    "run_phase20_retrieval_sla_eval",
    "run_phase20_index_consistency_checks",
    "run_phase20_safety_regression_audit",
    "run_phase20_real_stage_imports",
    "write_phase20_report",
]
