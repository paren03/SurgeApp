"""Phase 21 - Operator-Staged First Real Corpus Import Runner.

Coordinates the first real import: discover -> validate -> register ->
quality gate -> stage plan -> backup -> dry-run -> all gates -> guarded
real import -> indexes -> post audits -> SLA -> safety regression ->
rollback drill.

Real import is OFF by default and refused unless 6 hard gates pass.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import dual_corpus_source_adapters as adp
import dual_corpus_registry as reg
import dual_corpus_quality_gate as qg
import dual_corpus_chunked_importer as imp
import dual_corpus_pilot_import_planner as pip_
import dual_retrieval_quality_eval as rqe
import dual_retrieval_index_builder as idx
import dual_coverage_reporter as cr
import dual_million_stage_planner as msp
import dual_vocab_backup_restore as bk
import dual_import_batch_ledger as led
import dual_post_stage_quality_audit as pq
import dual_retrieval_sla_eval as sla
import dual_index_consistency_checker as ic
import dual_safety_regression_auditor as sra


PHASE21_REQUIRED_PRIOR = (
    "PHASE20_MILLION_READINESS_GATE_REPORT.md",
    "test_phase20_million_readiness_gate.py",
    "dual_vocab_backup_restore.py",
    "dual_import_batch_ledger.py",
    "dual_million_stage_planner.py",
    "dual_post_stage_quality_audit.py",
    "dual_retrieval_sla_eval.py",
    "dual_index_consistency_checker.py",
    "dual_safety_regression_auditor.py",
    "phase20_million_readiness_runner.py",
    "PHASE19_100K_SCALE_INDEX_AND_DEDUPE_REPORT.md",
    "dual_scale_import_planner.py",
    "dual_retrieval_index_builder.py",
    "dual_dedupe_collision_reporter.py",
    "dual_import_performance_benchmark.py",
    "phase19_scale_runner.py",
    "PHASE18_PILOT_IMPORT_AND_RETRIEVAL_HARDENING_REPORT.md",
    "phase18_pilot_import_runner.py",
    "PHASE17_SOURCE_ADAPTERS_AND_RETRIEVAL_EVAL_REPORT.md",
    "dual_corpus_source_adapters.py",
    "dual_corpus_pilot_import_planner.py",
    "dual_retrieval_quality_eval.py",
    "dual_coverage_reporter.py",
    "PHASE16_MILLION_SCALE_READINESS_REPORT.md",
    "dual_corpus_registry.py",
    "dual_corpus_chunked_importer.py",
    "dual_corpus_quality_gate.py",
    "dual_corpus_checkpoint.py",
)


HARD_MAX_TOTAL_PER_LANG = 10_000
HARD_MAX_PER_SOURCE = 10_000
HARD_BATCH_SIZE = 2_500
MIN_QUALITY_SCORE = 0.75


PHASE21_BASE = Path("corpus_sources/phase21")
PHASE21_STAGE_DIR = PHASE21_BASE / "stage_plans"
PHASE21_QUALITY_DIR = PHASE21_BASE / "quality_reports"
PHASE21_DRY_RUN_DIR = PHASE21_BASE / "dry_runs"
PHASE21_IMPORT_DIR = PHASE21_BASE / "import_reports"
PHASE21_LEDGER_DIR = PHASE21_BASE / "ledger_reports"
PHASE21_QUALITY_AUDIT_DIR = PHASE21_BASE / "quality_audits"
PHASE21_SLA_DIR = PHASE21_BASE / "sla_reports"
PHASE21_INDEX_DIR = PHASE21_BASE / "index_reports"
PHASE21_SAFETY_DIR = PHASE21_BASE / "safety_reports"
PHASE21_ROLLBACK_DIR = PHASE21_BASE / "rollback_drills"
PHASE21_FINAL_DIR = PHASE21_BASE / "final_decision"


_ALL_FOLDERS = (PHASE21_BASE, PHASE21_STAGE_DIR, PHASE21_QUALITY_DIR,
                PHASE21_DRY_RUN_DIR, PHASE21_IMPORT_DIR, PHASE21_LEDGER_DIR,
                PHASE21_QUALITY_AUDIT_DIR, PHASE21_SLA_DIR,
                PHASE21_INDEX_DIR, PHASE21_SAFETY_DIR,
                PHASE21_ROLLBACK_DIR, PHASE21_FINAL_DIR)


def verify_phase21_preflight() -> dict[str, Any]:
    missing = [f for f in PHASE21_REQUIRED_PRIOR if not Path(f).exists()]
    return {"ok": not missing,
            "missing_files": missing,
            "checked": list(PHASE21_REQUIRED_PRIOR)}


def setup_phase21_folders() -> dict[str, Any]:
    out: dict[str, str] = {}
    for d in _ALL_FOLDERS:
        d.mkdir(parents=True, exist_ok=True)
        out[d.name] = str(d)
    return {"ok": True, "folders": out}


# -------------------- Discovery --------------------

def discover_operator_staged_sources(language: Optional[str] = None,
                                     limit: int = 50) -> list[dict[str, Any]]:
    cap = max(1, min(int(limit), 200))
    langs = ("en", "ru") if language is None else (language,)
    out: list[dict[str, Any]] = []
    for lang in langs:
        sub = "english" if lang == "en" else "russian"
        base = Path("corpus_sources") / sub / "incoming"
        if not base.exists():
            continue
        for p in sorted(base.iterdir()):
            if not p.is_file():
                continue
            if len(out) >= cap:
                return out
            try:
                size = p.stat().st_size
            except Exception:
                size = 0
            out.append({"path": str(p), "language": lang,
                        "file_name": p.name, "size_bytes": size,
                        "suffix": p.suffix.lower()})
    return out


def validate_operator_sources(sources: list[dict[str, Any]]
                              ) -> dict[str, Any]:
    en = [s for s in sources if s.get("language") == "en"]
    ru = [s for s in sources if s.get("language") == "ru"]
    bilingual_ok = bool(en) and bool(ru)
    return {"ok": bilingual_ok,
            "en_count": len(en), "ru_count": len(ru),
            "missing_english": not en,
            "missing_russian": not ru,
            "bilingual_ready": bilingual_ok}


def register_phase21_sources(
    sources: list[dict[str, Any]],
    registry_db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    reg.init_registry(registry_db_path)
    out: list[dict[str, Any]] = []
    for s in sources:
        det = adp.detect_adapter_type(s["path"])
        if not det.get("ok"):
            out.append({**s, "register_error": det.get("error",
                                                       "adapter_not_recognized")})
            continue
        adapter = det["adapter_type"]
        if adapter.endswith("_jsonl"):
            fmt = "jsonl"
        elif adapter.endswith("_csv"):
            fmt = "csv"
        else:
            fmt = "txt"
        source_type = {
            "luna_jsonl": "word_list",
            "wiktextract_jsonl": "word_list",
            "simple_word_list_txt": "word_list",
            "frequency_word_list_txt": "word_list",
            "phrase_list_txt": "phrase_list",
            "idiom_list_txt": "idiom_list",
            "slang_list_txt": "slang_list",
            "profession_job_csv": "profession_job_list",
            "domain_terms_csv": "domain_terms",
            "bilingual_glossary_csv": "domain_terms",
            "russian_morphology_csv": "word_list",
            "mixed_jsonl": "mixed_jsonl",
        }.get(adapter, "word_list")
        r = reg.register_corpus_source(
            language=s["language"], source_type=source_type,
            expected_format=fmt, source_path=s["path"],
            notes="phase21_real_import",
            db_path=registry_db_path)
        if r.get("ok"):
            out.append({**s, "adapter_type": adapter,
                        "expected_format": fmt,
                        "source_type": source_type,
                        "corpus_id": r["corpus_id"],
                        "source_sha256": r["source_sha256"],
                        "row_estimate": r["row_estimate"]})
        else:
            out.append({**s, "register_error": r.get("error")})
    return out


# -------------------- Quality gate --------------------

def run_phase21_quality_gates(source_records: list[dict[str, Any]]
                              ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in source_records:
        if "register_error" in s:
            out.append({**s, "quality_ok": False,
                        "quality_error": s["register_error"]})
            continue
        rep = qg.generate_quality_gate_report(
            s["path"], s["expected_format"], s["language"],
            sample_size=100)
        gate = qg.should_allow_import(rep,
                                      min_quality_score=MIN_QUALITY_SCORE)
        out.append({**s, "quality_report": rep,
                    "quality_ok": gate["ok"],
                    "quality_gate_decision": gate})
        try:
            qpath = (PHASE21_QUALITY_DIR
                     / f"{Path(s['path']).stem}.quality.json")
            qpath.write_text(json.dumps({"source": s, "report": rep,
                                         "gate": gate},
                                        ensure_ascii=False, indent=2,
                                        default=str),
                             encoding="utf-8")
        except Exception:
            pass
    return out


# -------------------- Stage plan --------------------

def build_phase21_stage_plan(
    source_records: list[dict[str, Any]],
    max_total_per_language: int = HARD_MAX_TOTAL_PER_LANG,
    max_per_source: int = HARD_MAX_PER_SOURCE,
    batch_size: int = HARD_BATCH_SIZE,
) -> dict[str, Any]:
    mtl = max(1, min(int(max_total_per_language), HARD_MAX_TOTAL_PER_LANG))
    mps = max(1, min(int(max_per_source), HARD_MAX_PER_SOURCE))
    bs = max(1, min(int(batch_size), HARD_BATCH_SIZE))
    eligible = [s for s in source_records if s.get("quality_ok")]
    plans: dict[str, Any] = {}
    for lang in ("en", "ru"):
        srcs = [s for s in eligible if s.get("language") == lang]
        if not srcs:
            plans[lang] = {"ok": False, "error": "no_eligible_sources"}
            continue
        used = 0
        per_source: list[dict[str, Any]] = []
        for s in srcs:
            remain = mtl - used
            if remain <= 0:
                break
            take = min(mps, remain,
                       int(s.get("row_estimate") or mps))
            per_source.append({**s, "stage_take": take})
            used += take
        plan_id = f"p21_{lang}_{int(time.time())}_{uuid.uuid4().hex[:10]}"
        plan = {
            "plan_id": plan_id, "language": lang,
            "max_total_per_language": mtl,
            "max_per_source": mps,
            "batch_size": bs,
            "per_source_breakdown": per_source,
            "stage_total": used,
            "allow_full_source": False,
            "quality_gate_required": True,
            "dry_run_required": True,
            "backup_required": True,
            "checkpoint_required": True,
            "manifest_required": True,
            "rollback_required": True,
            "created_at": time.time(),
        }
        path = PHASE21_STAGE_DIR / f"{plan_id}.plan.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(plan, ensure_ascii=False, indent=2,
                                   default=str), encoding="utf-8")
        plans[lang] = {"ok": True, "plan": plan, "plan_path": str(path)}
    return plans


# -------------------- Backup --------------------

def create_phase21_backup_snapshot(label: str = "phase21"
                                   ) -> dict[str, Any]:
    snap = bk.create_backup_snapshot(label=label, include_manifests=True)
    if snap.get("ok"):
        ver = bk.verify_backup_snapshot(snap["snapshot_id"])
        snap["verified"] = ver.get("ok") is True
        snap["verify_details"] = ver
    return snap


# -------------------- Dry run --------------------

def run_phase21_dry_runs(
    stage_plan: dict[str, Any],
    checkpoint_db_path: Optional[str | Path] = None,
    ledger_db_path: Optional[str | Path] = None,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for lang in ("en", "ru"):
        payload = stage_plan.get(lang) or {}
        if not payload.get("ok"):
            out[lang] = {"ok": False, "error": payload.get("error",
                                                            "no_plan")}
            continue
        plan = payload["plan"]
        per_source_results: list[dict[str, Any]] = []
        for src in plan["per_source_breakdown"]:
            ledger_row = led.create_batch_record(
                language=lang, stage_id=plan["plan_id"],
                corpus_id=str(src.get("corpus_id") or ""),
                source_path=src["path"],
                source_sha256=str(src.get("source_sha256") or ""),
                dry_run=True, db_path=ledger_db_path,
                notes="phase21_dry_run")
            bid = ledger_row.get("batch_id")
            res = imp.import_file(
                path=src["path"], language=lang,
                source_type=src.get("source_type", "word_list"),
                expected_format=src.get("expected_format", "jsonl"),
                batch_size=plan["batch_size"],
                max_entries=int(src.get("stage_take",
                                        plan["max_per_source"])),
                dry_run=True, skip_quality_gate=True,
                checkpoint_db_path=checkpoint_db_path,
                en_db_path=en_db_path, ru_db_path=ru_db_path,
                reports_dir=PHASE21_DRY_RUN_DIR,
                rejections_dir=PHASE21_DRY_RUN_DIR)
            if bid:
                led.update_batch_status(
                    bid, "completed",
                    accepted_count=int(res.get("accepted") or 0),
                    rejected_count=int(res.get("rejected") or 0),
                    duplicate_count=int(res.get("duplicates") or 0),
                    completed=True, db_path=ledger_db_path)
            per_source_results.append({"source_path": src["path"],
                                       "batch_id": bid,
                                       "ok": bool(res.get("ok")),
                                       "accepted": res.get("accepted"),
                                       "rejected": res.get("rejected"),
                                       "duplicates": res.get("duplicates"),
                                       "report_path": res.get("report_path")})
        out[lang] = {"ok": all(r["ok"] for r in per_source_results) if per_source_results else False,
                     "per_source": per_source_results}
    return out


# -------------------- Gate verifier --------------------

def verify_phase21_import_gates(
    stage_plan: dict[str, Any],
    quality_reports: list[dict[str, Any]],
    dry_run_reports: dict[str, Any],
    backup_snapshot_id: Optional[str],
) -> dict[str, Any]:
    reasons: list[str] = []
    # Quality must pass for at least one source per language with quality_ok
    en_q = any(s.get("quality_ok") and s.get("language") == "en"
               for s in quality_reports)
    ru_q = any(s.get("quality_ok") and s.get("language") == "ru"
               for s in quality_reports)
    if not en_q:
        reasons.append("english_quality_gate_failed_or_missing")
    if not ru_q:
        reasons.append("russian_quality_gate_failed_or_missing")
    # Dry run must succeed
    if not (dry_run_reports.get("en") or {}).get("ok"):
        reasons.append("english_dry_run_failed")
    if not (dry_run_reports.get("ru") or {}).get("ok"):
        reasons.append("russian_dry_run_failed")
    # Backup snapshot must exist + verify
    if not backup_snapshot_id:
        reasons.append("backup_snapshot_required")
    else:
        ver = bk.verify_backup_snapshot(backup_snapshot_id)
        if not ver.get("ok"):
            reasons.append("backup_snapshot_unverified")
    # Caps
    for lang in ("en", "ru"):
        payload = (stage_plan or {}).get(lang) or {}
        if not payload.get("ok"):
            reasons.append(f"{lang}_stage_plan_missing")
            continue
        plan = payload["plan"]
        if int(plan.get("max_total_per_language", 0)) > HARD_MAX_TOTAL_PER_LANG:
            reasons.append(f"{lang}_max_total_per_language_exceeds_10000")
        if int(plan.get("max_per_source", 0)) > HARD_MAX_PER_SOURCE:
            reasons.append(f"{lang}_max_per_source_exceeds_10000")
        if int(plan.get("batch_size", 0)) > HARD_BATCH_SIZE:
            reasons.append(f"{lang}_batch_size_exceeds_2500")
        if plan.get("allow_full_source"):
            reasons.append(f"{lang}_allow_full_source_forbidden")
    return {"ok": not reasons, "reasons": reasons}


# -------------------- Real import --------------------

def run_phase21_real_import(
    stage_plan: dict[str, Any],
    *,
    allow_real_import: bool = False,
    quality_reports: Optional[list[dict[str, Any]]] = None,
    dry_run_reports: Optional[dict[str, Any]] = None,
    backup_snapshot_id: Optional[str] = None,
    registry_db_path: Optional[str | Path] = None,
    checkpoint_db_path: Optional[str | Path] = None,
    ledger_db_path: Optional[str | Path] = None,
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    if not allow_real_import:
        return {"ok": True, "allow_real_import": False,
                "reason": "real_import_disabled_by_default"}
    gate = verify_phase21_import_gates(
        stage_plan=stage_plan,
        quality_reports=quality_reports or [],
        dry_run_reports=dry_run_reports or {},
        backup_snapshot_id=backup_snapshot_id)
    if not gate["ok"]:
        return {"ok": False, "error": "gates_failed",
                "gates": gate}
    out: dict[str, Any] = {}
    for lang in ("en", "ru"):
        plan = (stage_plan.get(lang) or {}).get("plan") or {}
        results: list[dict[str, Any]] = []
        used = 0
        for src in plan.get("per_source_breakdown") or []:
            remain = int(plan["max_total_per_language"]) - used
            if remain <= 0:
                results.append({"source_path": src["path"],
                                "skipped": "lang_cap_reached"})
                continue
            take = min(int(src.get("stage_take", plan["max_per_source"])),
                       remain)
            rollback_key = f"phase21_{lang}_{plan['plan_id']}_{src.get('corpus_id', '')}"
            ledger_row = led.create_batch_record(
                language=lang, stage_id=plan["plan_id"],
                corpus_id=str(src.get("corpus_id") or ""),
                source_path=src["path"],
                source_sha256=str(src.get("source_sha256") or ""),
                dry_run=False,
                rollback_key=rollback_key,
                backup_snapshot_id=str(backup_snapshot_id or ""),
                db_path=ledger_db_path,
                notes="phase21_real_import")
            bid = ledger_row.get("batch_id")
            res = imp.import_file(
                path=src["path"], language=lang,
                source_type=src.get("source_type", "word_list"),
                expected_format=src.get("expected_format", "jsonl"),
                batch_size=plan["batch_size"],
                max_entries=take,
                dry_run=False, skip_quality_gate=True,
                checkpoint_db_path=checkpoint_db_path,
                en_db_path=en_db_path, ru_db_path=ru_db_path,
                reports_dir=PHASE21_IMPORT_DIR,
                rejections_dir=PHASE21_IMPORT_DIR)
            accepted = int(res.get("accepted") or 0)
            if bid:
                led.update_batch_status(
                    bid,
                    "completed" if res.get("ok") else "failed",
                    accepted_count=accepted,
                    rejected_count=int(res.get("rejected") or 0),
                    duplicate_count=int(res.get("duplicates") or 0),
                    completed=True, db_path=ledger_db_path)
            used += accepted
            results.append({"source_path": src["path"],
                            "batch_id": bid,
                            "rollback_key": rollback_key,
                            "ok": bool(res.get("ok")),
                            "accepted": accepted,
                            "rejected": res.get("rejected"),
                            "duplicates": res.get("duplicates"),
                            "report_path": res.get("report_path"),
                            "manifest_path": res.get("manifest_path",
                                                     "")})
        out[lang] = {"ok": all(r.get("ok") for r in results) if results else False,
                     "used_total": used, "per_source": results}
    return {"ok": True, "allow_real_import": True,
            "gates": gate, "results": out,
            "backup_snapshot_id": backup_snapshot_id}


# -------------------- Post import --------------------

def rebuild_phase21_indexes(en_db_path: Optional[str | Path] = None,
                            ru_db_path: Optional[str | Path] = None
                            ) -> dict[str, Any]:
    return idx.rebuild_all_indexes(en_db_path=en_db_path,
                                   ru_db_path=ru_db_path)


def run_phase21_post_import_quality_audits(
    en_db_path: Optional[str | Path] = None,
    ru_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for lang, db in (("en", en_db_path), ("ru", ru_db_path)):
        rows = pq.sample_recent_import_rows(lang, limit=500, db_path=db)
        comp = pq.audit_metadata_completeness(rows)
        lc = pq.audit_language_consistency(rows, lang)
        sc = pq.audit_safety_tag_consistency(rows)
        rc = pq.audit_register_tag_consistency(rows)
        cc = pq.audit_coverage_category_consistency(rows)
        dr = pq.audit_duplicate_rate(lang, db_path=db)
        rj = pq.audit_rejected_rows(lang, limit=200)
        bundle = {"metadata_completeness": comp,
                  "language_consistency": lc,
                  "safety_consistency": sc,
                  "register_consistency": rc,
                  "coverage_consistency": cc,
                  "duplicate_rate": dr,
                  "rejected_rows": rj}
        score = pq.compute_stage_quality_score(bundle)
        bundle["quality_score"] = score
        path = PHASE21_QUALITY_AUDIT_DIR / f"phase21_quality_{lang}.json"
        pq.write_post_stage_quality_audit(bundle, path)
        out[lang] = {**bundle, "report_path": str(path)}
    return out


def run_phase21_retrieval_sla(en_db_path: Optional[str | Path] = None,
                              ru_db_path: Optional[str | Path] = None
                              ) -> dict[str, Any]:
    en_q = ["engineer", "ledger", "vector", "verse", "essence"]
    ru_q = ["инженер", "бюджет", "число", "стих", "сущность"]
    cats = ["core_vocabulary", "professions_jobs", "science_math"]
    regs = ["standard", "professional"]
    safe = ["recognition_only", "do_not_use_unprompted"]
    bundle = {
        "simple_lookup": {
            "en": sla.benchmark_query_latency("en", en_q, limit=25,
                                              en_db_path=en_db_path),
            "ru": sla.benchmark_query_latency("ru", ru_q, limit=25,
                                              ru_db_path=ru_db_path)},
        "category_lookup": {
            "en": sla.benchmark_category_lookup_latency("en", cats,
                                                        limit=25,
                                                        en_db_path=en_db_path),
            "ru": sla.benchmark_category_lookup_latency("ru", cats,
                                                        limit=25,
                                                        ru_db_path=ru_db_path)},
        "register_lookup": {
            "en": sla.benchmark_register_lookup_latency("en", regs,
                                                        limit=25,
                                                        en_db_path=en_db_path),
            "ru": sla.benchmark_register_lookup_latency("ru", regs,
                                                        limit=25,
                                                        ru_db_path=ru_db_path)},
        "safety_filter_lookup": {
            "en": sla.benchmark_safety_filter_latency("en", safe, limit=25,
                                                      en_db_path=en_db_path),
            "ru": sla.benchmark_safety_filter_latency("ru", safe, limit=25,
                                                      ru_db_path=ru_db_path)},
    }
    bundle["verdict"] = sla.evaluate_sla_results(bundle)
    path = PHASE21_SLA_DIR / "phase21_retrieval_sla.json"
    sla.write_sla_report(bundle, path)
    bundle["report_path"] = str(path)
    return bundle


def run_phase21_index_consistency(en_db_path: Optional[str | Path] = None,
                                  ru_db_path: Optional[str | Path] = None
                                  ) -> dict[str, Any]:
    payload = {
        "generated_at": time.time(),
        "en_index": ic.check_english_index_consistency(db_path=en_db_path),
        "ru_index": ic.check_russian_index_consistency(db_path=ru_db_path),
        "fts_counts": ic.check_fts_row_counts(en_db_path=en_db_path,
                                              ru_db_path=ru_db_path),
        "safety_filter_en": ic.check_safety_filter_index_behavior(
            "en", db_path=en_db_path),
        "safety_filter_ru": ic.check_safety_filter_index_behavior(
            "ru", db_path=ru_db_path),
        "bounds_en": ic.check_index_query_bounds("en", db_path=en_db_path),
        "bounds_ru": ic.check_index_query_bounds("ru", db_path=ru_db_path),
    }
    path = PHASE21_INDEX_DIR / "phase21_index_consistency.json"
    ic.write_index_consistency_report(payload, path)
    payload["report_path"] = str(path)
    return payload


def run_phase21_safety_regression(en_db_path: Optional[str | Path] = None,
                                  ru_db_path: Optional[str | Path] = None
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
    path = PHASE21_SAFETY_DIR / "phase21_safety_regression.json"
    sra.write_safety_regression_report(payload, path)
    payload["report_path"] = str(path)
    return payload


def run_phase21_rollback_drill(
    backup_snapshot_id: Optional[str] = None,
    rollback_keys: Optional[list[str]] = None,
    dry_run: bool = True,
    ledger_db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    """Drill: identify rows by rollback_key + report what a restore would
    do. Dry-run by default; no DB mutated unless dry_run=False AND caller
    has the chain authorization handled separately."""
    actions: list[dict[str, Any]] = []
    for rk in rollback_keys or []:
        batches = led.get_batches_by_rollback_key(rk, db_path=ledger_db_path)
        for b in batches:
            actions.append({"action": "delete_rows_by_pack_id",
                            "pack_id": b.get("pack_id"),
                            "language": b.get("language"),
                            "rollback_key": rk,
                            "accepted_count": b.get("accepted_count")})
    restore_intent = None
    if backup_snapshot_id:
        rr = bk.restore_backup_snapshot(backup_snapshot_id, dry_run=True)
        restore_intent = rr
    payload = {"dry_run": True if dry_run else False,
               "rollback_keys": list(rollback_keys or []),
               "batch_actions_intended": actions,
               "restore_intent_dry": restore_intent,
               "generated_at": time.time()}
    path = PHASE21_ROLLBACK_DIR / "phase21_rollback_drill.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2,
                               default=str), encoding="utf-8")
    payload["report_path"] = str(path)
    return payload


# -------------------- Reports --------------------

def write_phase21_report(report: dict[str, Any],
                         output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_staging_required_report(missing: dict[str, Any],
                                  output_path: str | Path) -> str:
    """Markdown-format clean staging-required report."""
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = [
        "# Phase 21 - Operator Staging Required",
        "",
        "Status: **WAITING_FOR_OPERATOR**.",
        "",
        "Phase 21 infrastructure is ready (runner, harness, folders, "
        "report writers). No real local corpus files are present, so "
        "**no real import occurred and production DBs were not modified**.",
        "",
        "## What is missing",
        f"- English files in `corpus_sources/english/incoming/`: "
        f"{'PRESENT' if not missing.get('missing_english') else 'MISSING'}",
        f"- Russian files in `corpus_sources/russian/incoming/`: "
        f"{'PRESENT' if not missing.get('missing_russian') else 'MISSING'}",
        "",
        "## Where to place files",
        "- English: `D:\\SurgeApp\\corpus_sources\\english\\incoming\\`",
        "- Russian: `D:\\SurgeApp\\corpus_sources\\russian\\incoming\\`",
        "",
        "## Supported file types",
        "- `.jsonl` (Luna-canonical or wiktextract-style)",
        "- `.txt`  (one word per line; or `word freq` for frequency lists)",
        "- `.csv`  (domain terms / profession_job / bilingual_glossary / russian_morphology)",
        "",
        "## Required metadata in JSONL rows",
        "- `word` (required)",
        "- `language` (`en` or `ru`)",
        "- `definition` (recommended)",
        "- `coverage_categories` (recommended, list)",
        "- `register_tags` (recommended, list)",
        "- `safety_tags` (optional, list - empty if benign)",
        "",
        "## Example file names",
        "- `english_general_5k.jsonl`",
        "- `russian_general_5k.jsonl`",
        "- `english_idioms_500.txt`",
        "- `russian_morphology_1k.csv`",
        "",
        "## Re-run after staging",
        "```",
        "python -c \"import phase21_operator_stage_runner as p; "
        "print(p.discover_operator_staged_sources())\"",
        "python test_phase21_operator_staged_first_import.py",
        "```",
        "",
        "## Production DB confirmation",
        "Production lexicon counts are unchanged. Run the runner's "
        "preflight + discovery functions to verify before any real "
        "ingestion attempt.",
    ]
    p.write_text("\n".join(body), encoding="utf-8")
    return str(p)


__all__ = [
    "HARD_MAX_TOTAL_PER_LANG",
    "HARD_MAX_PER_SOURCE",
    "HARD_BATCH_SIZE",
    "MIN_QUALITY_SCORE",
    "PHASE21_BASE", "PHASE21_STAGE_DIR", "PHASE21_QUALITY_DIR",
    "PHASE21_DRY_RUN_DIR", "PHASE21_IMPORT_DIR", "PHASE21_LEDGER_DIR",
    "PHASE21_QUALITY_AUDIT_DIR", "PHASE21_SLA_DIR", "PHASE21_INDEX_DIR",
    "PHASE21_SAFETY_DIR", "PHASE21_ROLLBACK_DIR", "PHASE21_FINAL_DIR",
    "PHASE21_REQUIRED_PRIOR",
    "verify_phase21_preflight",
    "setup_phase21_folders",
    "discover_operator_staged_sources",
    "validate_operator_sources",
    "register_phase21_sources",
    "run_phase21_quality_gates",
    "build_phase21_stage_plan",
    "create_phase21_backup_snapshot",
    "run_phase21_dry_runs",
    "verify_phase21_import_gates",
    "run_phase21_real_import",
    "rebuild_phase21_indexes",
    "run_phase21_post_import_quality_audits",
    "run_phase21_retrieval_sla",
    "run_phase21_index_consistency",
    "run_phase21_safety_regression",
    "run_phase21_rollback_drill",
    "write_phase21_report",
    "write_staging_required_report",
]
