"""Phase 19 - Controlled 100K-Scale Import Planner.

Plans a per-language 100K-capped import campaign across one or more local
corpus sources. Refuses any plan that would request ``allow_full_source=True``
or exceed the Phase 19 caps.

Caps (hard):
    target_total            <= 100,000 per language
    per_source_cap          <= 25,000
    batch_size              <= 5,000
    allow_full_source       must be False

This module reads files only via Phase 16/17 streaming helpers. No daemon,
no scheduler, no auto-runner, no network.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import dual_corpus_registry as reg


PHASE19_REQUIRED_PRIOR = (
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

HARD_TARGET_PER_LANG = 100_000
HARD_PER_SOURCE_CAP = 25_000
HARD_BATCH_SIZE = 5_000


def verify_phase18_preflight() -> dict[str, Any]:
    """Return {ok, missing_files, checked} - read-only."""
    missing = [f for f in PHASE19_REQUIRED_PRIOR if not Path(f).exists()]
    return {"ok": not missing,
            "missing_files": missing,
            "checked": list(PHASE19_REQUIRED_PRIOR)}


def discover_scale_candidates(language: Optional[str] = None,
                              limit: int = 100) -> list[dict[str, Any]]:
    """Walk corpus_sources/<lang>/incoming/ and return up to ``limit`` files.

    Does not open or hash any file - just reports name/size/path.
    """
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


def estimate_candidate_capacity(source_record: dict[str, Any]) -> dict[str, Any]:
    """Estimate how many rows this source can contribute, bounded scan."""
    path = source_record.get("path")
    if not path:
        return {"ok": False, "error": "missing_path"}
    p = Path(path)
    if not p.exists():
        return {"ok": False, "error": "file_not_found", "path": str(p)}
    est = reg.estimate_rows_streaming(p, max_scan_rows=20_000)
    sha = reg.compute_source_sha256(p)
    return {"ok": True, "path": str(p), "row_estimate": int(est),
            "source_sha256": sha,
            "capacity_within_phase19_cap": min(int(est), HARD_PER_SOURCE_CAP)}


def choose_scale_batches(total_target: int, per_source_cap: int,
                          batch_size_cap: int = HARD_BATCH_SIZE
                          ) -> dict[str, int]:
    tt = max(1, min(int(total_target), HARD_TARGET_PER_LANG))
    psc = max(1, min(int(per_source_cap), HARD_PER_SOURCE_CAP))
    bs = max(1, min(int(batch_size_cap), HARD_BATCH_SIZE))
    sources_needed = (tt + psc - 1) // psc
    return {"total_target": tt, "per_source_cap": psc,
            "batch_size": bs, "sources_needed_min": int(sources_needed)}


def enforce_phase19_caps(plan: dict[str, Any]) -> dict[str, Any]:
    """Validate and clamp a plan; refuse if allow_full_source set."""
    issues: list[str] = []
    if plan.get("allow_full_source"):
        return {"ok": False,
                "reason": "allow_full_source_forbidden_in_phase19",
                "plan_id": plan.get("scale_plan_id")}
    if int(plan.get("target_total", 0)) > HARD_TARGET_PER_LANG:
        issues.append("target_total_exceeds_100000")
        plan["target_total"] = HARD_TARGET_PER_LANG
    if int(plan.get("per_source_cap", 0)) > HARD_PER_SOURCE_CAP:
        issues.append("per_source_cap_exceeds_25000")
        plan["per_source_cap"] = HARD_PER_SOURCE_CAP
    if int(plan.get("batch_size", 0)) > HARD_BATCH_SIZE:
        issues.append("batch_size_exceeds_5000")
        plan["batch_size"] = HARD_BATCH_SIZE
    plan["allow_full_source"] = False
    plan["quality_gate_required"] = True
    plan["dry_run_required"] = True
    plan["checkpoint_required"] = True
    plan["manifest_required"] = True
    plan["rollback_required"] = True
    return {"ok": True, "clamped": issues, "plan": plan}


def build_scale_plan(source_records: list[dict[str, Any]], language: str,
                     target_total: int = HARD_TARGET_PER_LANG,
                     per_source_cap: int = HARD_PER_SOURCE_CAP,
                     batch_size: int = HARD_BATCH_SIZE,
                     allow_full_source: bool = False,
                     notes: str = "",
                     ) -> dict[str, Any]:
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}
    if allow_full_source:
        return {"ok": False,
                "error": "allow_full_source_forbidden_in_phase19"}
    sizing = choose_scale_batches(target_total, per_source_cap, batch_size)
    plan = {
        "scale_plan_id": f"scale_{language}_{int(time.time())}_{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "language": language,
        "target_total": sizing["total_target"],
        "per_source_cap": sizing["per_source_cap"],
        "batch_size": sizing["batch_size"],
        "source_records": list(source_records or []),
        "quality_gate_required": True,
        "dry_run_required": True,
        "checkpoint_required": True,
        "manifest_required": True,
        "rollback_required": True,
        "allow_full_source": False,
        "notes": str(notes),
    }
    enf = enforce_phase19_caps(plan)
    if not enf.get("ok"):
        return {"ok": False, "error": enf.get("reason", "cap_enforcement_failed")}
    return {"ok": True, "plan": enf["plan"], "sizing": sizing,
            "clamped": enf.get("clamped", [])}


def write_scale_plan(plan: dict[str, Any], output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(plan, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_scale_plan(path: str | Path) -> Optional[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def summarize_scale_plan(plan: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan, dict):
        return {"ok": False, "error": "not_a_plan"}
    src = plan.get("source_records") or []
    total_estimated = 0
    for s in src:
        if isinstance(s, dict):
            total_estimated += int(s.get("row_estimate", 0)
                                   or s.get("capacity_within_phase19_cap", 0))
    return {
        "ok": True,
        "scale_plan_id": plan.get("scale_plan_id"),
        "language": plan.get("language"),
        "target_total": plan.get("target_total"),
        "per_source_cap": plan.get("per_source_cap"),
        "batch_size": plan.get("batch_size"),
        "n_sources": len(src),
        "total_row_estimate_in_sources": total_estimated,
        "phase19_caps_enforced": {
            "target_total_max": HARD_TARGET_PER_LANG,
            "per_source_cap_max": HARD_PER_SOURCE_CAP,
            "batch_size_max": HARD_BATCH_SIZE,
            "allow_full_source": plan.get("allow_full_source", False),
        },
    }


__all__ = [
    "HARD_TARGET_PER_LANG",
    "HARD_PER_SOURCE_CAP",
    "HARD_BATCH_SIZE",
    "PHASE19_REQUIRED_PRIOR",
    "verify_phase18_preflight",
    "discover_scale_candidates",
    "estimate_candidate_capacity",
    "choose_scale_batches",
    "enforce_phase19_caps",
    "build_scale_plan",
    "write_scale_plan",
    "read_scale_plan",
    "summarize_scale_plan",
]
