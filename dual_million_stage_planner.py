"""Phase 20 - Million-Scale Stage Planner.

Builds staged plans toward 1,000,000 entries per language. Splits operator-
staged source records into per-stage batches honoring the Phase 20 hard caps:

    stage_size       <= 100,000
    per_source_cap   <= 50,000
    allow_full_source must be False
    quality_gate_required = True
    dry_run_required = True
    backup_required = True
    safety_audit_required = True
    retrieval_sla_required = True

No daemon. No scheduler. No internet. Bounded scans only.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


PHASE20_REQUIRED_PRIOR = (
    "PHASE19_100K_SCALE_INDEX_AND_DEDUPE_REPORT.md",
    "test_phase19_100k_scale_index_and_dedupe.py",
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


HARD_STAGE_SIZE = 100_000
HARD_PER_SOURCE_CAP = 50_000
DEFAULT_TARGET_TOTAL = 1_000_000


def verify_phase19_preflight() -> dict[str, Any]:
    missing = [f for f in PHASE20_REQUIRED_PRIOR if not Path(f).exists()]
    return {"ok": not missing,
            "missing_files": missing,
            "checked": list(PHASE20_REQUIRED_PRIOR)}


def discover_eligible_sources(language: Optional[str] = None,
                              limit: int = 200) -> list[dict[str, Any]]:
    cap = max(1, min(int(limit), 1000))
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


def split_sources_into_stages(
    source_records: list[dict[str, Any]],
    stage_size: int = HARD_STAGE_SIZE,
    per_source_cap: int = HARD_PER_SOURCE_CAP,
) -> list[dict[str, Any]]:
    ss = max(1, min(int(stage_size), HARD_STAGE_SIZE))
    psc = max(1, min(int(per_source_cap), HARD_PER_SOURCE_CAP))
    stages: list[dict[str, Any]] = []
    cur: list[dict[str, Any]] = []
    cur_total = 0
    for s in source_records or []:
        if not isinstance(s, dict):
            continue
        cap_here = min(int(s.get("row_estimate", psc) or psc), psc)
        if cur_total + cap_here > ss and cur:
            stages.append({"sources": cur, "stage_capacity": cur_total})
            cur = []
            cur_total = 0
        # If a single source exceeds the stage_size, clamp it.
        if cap_here > ss:
            cap_here = ss
        cur.append({**s, "stage_take": cap_here})
        cur_total += cap_here
    if cur:
        stages.append({"sources": cur, "stage_capacity": cur_total})
    return stages


def enforce_stage_caps(plan: dict[str, Any]) -> dict[str, Any]:
    if plan.get("allow_full_source"):
        return {"ok": False,
                "reason": "allow_full_source_forbidden_in_phase20",
                "plan_id": plan.get("million_plan_id")}
    issues: list[str] = []
    if int(plan.get("stage_size", 0)) > HARD_STAGE_SIZE:
        plan["stage_size"] = HARD_STAGE_SIZE
        issues.append("stage_size_clamped_to_100000")
    if int(plan.get("per_source_cap", 0)) > HARD_PER_SOURCE_CAP:
        plan["per_source_cap"] = HARD_PER_SOURCE_CAP
        issues.append("per_source_cap_clamped_to_50000")
    plan["allow_full_source"] = False
    for flag in ("quality_gate_required", "dry_run_required",
                 "backup_required", "checkpoint_required",
                 "manifest_required", "safety_audit_required",
                 "retrieval_sla_required", "rollback_required"):
        plan[flag] = True
    return {"ok": True, "clamped": issues, "plan": plan}


def estimate_total_import_capacity(plan: dict[str, Any]) -> dict[str, Any]:
    stages = plan.get("stages") or []
    cap = 0
    n_sources = 0
    for st in stages:
        cap += int(st.get("stage_capacity", 0))
        n_sources += len(st.get("sources") or [])
    return {"ok": True, "total_estimated_capacity": cap,
            "n_stages": len(stages), "n_sources": n_sources}


def build_million_stage_plan(
    language: str,
    source_records: Optional[list[dict[str, Any]]] = None,
    target_total: int = DEFAULT_TARGET_TOTAL,
    stage_size: int = HARD_STAGE_SIZE,
    per_source_cap: int = HARD_PER_SOURCE_CAP,
    allow_full_source: bool = False,
    notes: str = "",
) -> dict[str, Any]:
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    if allow_full_source:
        return {"ok": False,
                "error": "allow_full_source_forbidden_in_phase20"}
    srcs = list(source_records or [])
    stages = split_sources_into_stages(srcs, stage_size, per_source_cap)
    plan = {
        "million_plan_id": f"mil_{language}_{int(time.time())}_{uuid.uuid4().hex[:10]}",
        "language": language,
        "target_total": int(target_total),
        "stage_size": min(int(stage_size), HARD_STAGE_SIZE),
        "per_source_cap": min(int(per_source_cap), HARD_PER_SOURCE_CAP),
        "stages": stages,
        "quality_gate_required": True,
        "dry_run_required": True,
        "backup_required": True,
        "checkpoint_required": True,
        "manifest_required": True,
        "safety_audit_required": True,
        "retrieval_sla_required": True,
        "rollback_required": True,
        "allow_full_source": False,
        "created_at": time.time(),
        "notes": str(notes),
    }
    cap = estimate_total_import_capacity(plan)
    plan["total_estimated_capacity"] = cap["total_estimated_capacity"]
    enf = enforce_stage_caps(plan)
    if not enf.get("ok"):
        return {"ok": False, "error": enf.get("reason")}
    return {"ok": True, "plan": enf["plan"], "estimate": cap,
            "clamped": enf.get("clamped", [])}


def write_stage_plan(plan: dict[str, Any],
                     output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(plan, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_stage_plan(path: str | Path) -> Optional[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def summarize_stage_plan(plan: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan, dict):
        return {"ok": False, "error": "not_a_plan"}
    cap = estimate_total_import_capacity(plan)
    return {"ok": True,
            "million_plan_id": plan.get("million_plan_id"),
            "language": plan.get("language"),
            "target_total": plan.get("target_total"),
            "stage_size": plan.get("stage_size"),
            "per_source_cap": plan.get("per_source_cap"),
            "n_stages": cap["n_stages"],
            "n_sources": cap["n_sources"],
            "total_estimated_capacity": cap["total_estimated_capacity"],
            "phase20_caps_enforced": {
                "hard_stage_size": HARD_STAGE_SIZE,
                "hard_per_source_cap": HARD_PER_SOURCE_CAP,
                "allow_full_source": plan.get("allow_full_source", False),
            }}


__all__ = [
    "HARD_STAGE_SIZE",
    "HARD_PER_SOURCE_CAP",
    "DEFAULT_TARGET_TOTAL",
    "PHASE20_REQUIRED_PRIOR",
    "verify_phase19_preflight",
    "discover_eligible_sources",
    "split_sources_into_stages",
    "enforce_stage_caps",
    "estimate_total_import_capacity",
    "build_million_stage_plan",
    "write_stage_plan",
    "read_stage_plan",
    "summarize_stage_plan",
]
