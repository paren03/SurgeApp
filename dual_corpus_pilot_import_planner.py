"""Phase 17 - Dual Corpus Pilot Import Planner.

Plans small, safe pilot imports BEFORE a real large corpus ingest is allowed.
Every pilot is bounded, quality-gated, dry-run-by-default, and rollback-
identifiable. No daemon, no scheduler, no auto-runner.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import dual_corpus_registry as reg
import dual_corpus_quality_gate as qg
import dual_corpus_chunked_importer as imp
import dual_corpus_source_adapters as adp


DEFAULT_TARGET_ENTRIES = 1000
HARD_PILOT_MAX = 5000

PILOT_DIR = Path("corpus_sources") / "pilot_imports"


def _now() -> float:
    return time.time()


def _new_plan_id() -> str:
    return f"pilot_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def discover_incoming_sources(language: Optional[str] = None,
                              limit: int = 100) -> list[dict[str, Any]]:
    """Walk corpus_sources/<lang>/incoming/ and report candidate files.

    Bounded by ``limit``. No file is opened or hashed here - just listed.
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


def choose_safe_max_entries(estimated_rows: int, source_type: str,
                            quality_score: float) -> int:
    """Pick a conservative cap for a pilot."""
    est = max(0, int(estimated_rows))
    base = min(DEFAULT_TARGET_ENTRIES, est) if est > 0 else DEFAULT_TARGET_ENTRIES
    if quality_score < 0.6:
        base = min(base, 200)
    elif quality_score < 0.75:
        base = min(base, 500)
    if source_type in ("slang_list", "street_talk_list"):
        base = min(base, 300)
    return max(1, min(base, HARD_PILOT_MAX))


def choose_batch_size(file_size_bytes: int, estimated_rows: int) -> int:
    """Pick a batch size that matches file scale."""
    sz = max(0, int(file_size_bytes))
    est = max(0, int(estimated_rows))
    if est <= 200:
        return min(50, max(10, est // 4 or 10))
    if sz < 1_000_000 or est < 5_000:
        return 200
    if sz < 50_000_000 or est < 50_000:
        return 500
    return 1000


def require_quality_gate_pass(quality_report: dict[str, Any]) -> dict[str, Any]:
    """Hard gate - planner refuses to schedule a real run unless gate is open."""
    gate = qg.should_allow_import(quality_report, min_quality_score=0.75)
    return gate


def build_pilot_plan(source_path: str | Path, language: str,
                     source_type: str,
                     adapter_type: Optional[str] = None,
                     target_entries: int = DEFAULT_TARGET_ENTRIES,
                     registry_db_path: Optional[str | Path] = None,
                     ) -> dict[str, Any]:
    p = Path(source_path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "error": "file_not_found", "source_path": str(p)}
    if language not in ("en", "ru"):
        return {"ok": False, "error": f"invalid_language: {language!r}"}

    det = adp.detect_adapter_type(p, declared_type=adapter_type)
    if not det.get("ok"):
        return {"ok": False, "error": det.get("error", "adapter_detection_failed"),
                "details": det}
    adapter = det["adapter_type"]

    # Heuristic format mapping for the chunked importer.
    if adapter.endswith("_jsonl"):
        expected_format = "jsonl"
    elif adapter.endswith("_csv"):
        expected_format = "csv"
    else:
        expected_format = "txt"

    estimated_rows = reg.estimate_rows_streaming(p, max_scan_rows=10000)
    try:
        size = p.stat().st_size
    except Exception:
        size = 0

    qreport = qg.generate_quality_gate_report(
        p, expected_format, language,
        sample_size=min(100, max(20, estimated_rows or 50)))
    qscore = float(qreport.get("quality_score", 0.0)) if qreport.get("ok") else 0.0

    safe_cap = choose_safe_max_entries(estimated_rows, source_type, qscore)
    safe_cap = min(safe_cap, max(1, int(target_entries)))
    batch = choose_batch_size(size, estimated_rows)

    pid_seed = f"pilot_{language}_{source_type}_{int(_now())}"
    plan = {
        "plan_id": _new_plan_id(),
        "source_path": str(p),
        "language": language,
        "source_type": source_type,
        "adapter_type": adapter,
        "expected_format": expected_format,
        "estimated_rows": int(estimated_rows),
        "file_size_bytes": int(size),
        "quality_score": qscore,
        "quality_report": qreport,
        "target_entries": int(target_entries),
        "safe_max_entries": int(safe_cap),
        "batch_size": int(batch),
        "dry_run_default": True,
        "required_quality_gate": True,
        "expected_manifest_path": str(p) + ".pilot_manifest.json",
        "rollback_key": {"pack_id_prefix": pid_seed,
                         "language": language,
                         "source_type": source_type,
                         "source_path": str(p)},
        "created_at": _now(),
        "notes": f"adapter={adapter} qgate_threshold=0.75",
    }
    return {"ok": True, "plan": plan}


def write_pilot_plan(plan: dict[str, Any],
                     output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(plan, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_pilot_result(result: dict[str, Any],
                       output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(result)
    body["result_generated_at"] = _now()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def _run(plan: dict[str, Any], *, dry_run: bool,
         registry_db_path: Optional[str | Path],
         checkpoint_db_path: Optional[str | Path],
         en_db_path: Optional[str | Path],
         ru_db_path: Optional[str | Path]) -> dict[str, Any]:
    if not plan or not isinstance(plan, dict):
        return {"ok": False, "error": "invalid_plan"}
    gate = require_quality_gate_pass(plan.get("quality_report") or {})
    if not gate["ok"]:
        return {"ok": False, "error": "quality_gate_blocked",
                "gate_decision": gate, "plan_id": plan.get("plan_id")}

    reg.init_registry(registry_db_path)
    r = reg.register_corpus_source(
        language=plan["language"], source_type=plan["source_type"],
        expected_format=plan["expected_format"], source_path=plan["source_path"],
        notes=f"pilot_plan_id={plan.get('plan_id')}",
        db_path=registry_db_path)
    if not r.get("ok"):
        return {"ok": False, "error": "registration_failed", "details": r,
                "plan_id": plan.get("plan_id")}
    corpus_id = r["corpus_id"]
    res = imp.import_corpus(
        corpus_id=corpus_id,
        batch_size=int(plan["batch_size"]),
        max_entries=int(plan["safe_max_entries"]),
        dry_run=dry_run,
        registry_db_path=registry_db_path,
        checkpoint_db_path=checkpoint_db_path,
        en_db_path=en_db_path, ru_db_path=ru_db_path,
        skip_quality_gate=True)
    res["plan_id"] = plan.get("plan_id")
    res["corpus_id"] = corpus_id
    res["rollback_key"] = plan.get("rollback_key")
    return res


def run_pilot_dry_run(plan: dict[str, Any],
                      registry_db_path: Optional[str | Path] = None,
                      checkpoint_db_path: Optional[str | Path] = None,
                      en_db_path: Optional[str | Path] = None,
                      ru_db_path: Optional[str | Path] = None) -> dict[str, Any]:
    return _run(plan, dry_run=True,
                registry_db_path=registry_db_path,
                checkpoint_db_path=checkpoint_db_path,
                en_db_path=en_db_path, ru_db_path=ru_db_path)


def run_pilot_import(plan: dict[str, Any], dry_run: bool = True,
                     registry_db_path: Optional[str | Path] = None,
                     checkpoint_db_path: Optional[str | Path] = None,
                     en_db_path: Optional[str | Path] = None,
                     ru_db_path: Optional[str | Path] = None
                     ) -> dict[str, Any]:
    return _run(plan, dry_run=dry_run,
                registry_db_path=registry_db_path,
                checkpoint_db_path=checkpoint_db_path,
                en_db_path=en_db_path, ru_db_path=ru_db_path)


__all__ = [
    "DEFAULT_TARGET_ENTRIES",
    "HARD_PILOT_MAX",
    "PILOT_DIR",
    "discover_incoming_sources",
    "build_pilot_plan",
    "choose_safe_max_entries",
    "choose_batch_size",
    "require_quality_gate_pass",
    "run_pilot_dry_run",
    "run_pilot_import",
    "write_pilot_plan",
    "write_pilot_result",
]
