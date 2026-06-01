"""Phase 42 - Drift-Stability Matrix.

Detects drift across multiple traces. Bounded, read-only over
production DBs and Phase 21 incoming folders.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase42.drift_stability_matrix.v1"


_REQUIRED_MATRIX_FIELDS = (
    "matrix_id", "created_at", "phase",
    "trace_count",
    "adapter_selection_drift",
    "baseline_drift",
    "boundary_drift",
    "phase21_status_drift",
    "fail_count", "warn_count", "pass_count",
    "ok",
)


_EXPECTED_BASELINES = {
    "english_words": 2814,
    "russian_words": 2518,
    "russian_phrases": 35,
    "bilingual_concepts": 26,
    "bilingual_entry_links": 52,
    "live_pack_manifests": 90,
}


def _root() -> Path:
    return Path(__file__).resolve().parent


def detect_phase42_adapter_selection_drift(
    matrix_input: Any,
) -> dict[str, Any]:
    trace_results = matrix_input.get("trace_results") \
        if isinstance(matrix_input, dict) else None
    if not isinstance(trace_results, list):
        return {"category": "adapter_selection_drift",
                "drifted": True,
                "severity": "fail",
                "reason": "trace_results_not_list"}
    failures: list[dict[str, Any]] = []
    for r in trace_results:
        if not isinstance(r, dict):
            continue
        if str(r.get("status") or "") != "ok":
            continue
        if r.get("adapter_matches_expected") is False:
            failures.append({
                "scenario_id": r.get("scenario_id"),
                "expected": r.get("expected_adapter_family"),
                "observed":
                    r.get("selected_adapter_name"),
            })
    return {
        "category": "adapter_selection_drift",
        "drifted": bool(failures),
        "severity": "fail" if failures else "pass",
        "failures": failures,
    }


def detect_phase42_baseline_drift(
    matrix_input: Any,
    root: Optional[Path] = None,
) -> dict[str, Any]:
    r = root or _root()
    observed: dict[str, int] = {}
    en_db = r / "lexicon" / "luna_vocabulary.sqlite"
    ru_db = r / "russian_stack" / "russian_lexicon.sqlite"
    link_db = (r / "bilingual_stack"
                  / "bilingual_links.sqlite")
    try:
        if en_db.exists():
            c = sqlite3.connect(str(en_db))
            observed["english_words"] = c.execute(
                "SELECT COUNT(*) FROM words"
                ).fetchone()[0]
            c.close()
        if ru_db.exists():
            c = sqlite3.connect(str(ru_db))
            observed["russian_words"] = c.execute(
                "SELECT COUNT(*) FROM words"
                ).fetchone()[0]
            observed["russian_phrases"] = c.execute(
                "SELECT COUNT(*) FROM phrases"
                ).fetchone()[0]
            c.close()
        if link_db.exists():
            c = sqlite3.connect(str(link_db))
            observed["bilingual_concepts"] = c.execute(
                "SELECT COUNT(*) FROM concepts"
                ).fetchone()[0]
            observed["bilingual_entry_links"] = c.execute(
                "SELECT COUNT(*) FROM entry_links"
                ).fetchone()[0]
            c.close()
    except Exception as e:  # noqa: BLE001
        return {
            "category": "baseline_drift",
            "drifted": True,
            "severity": "fail",
            "reason": f"db_read_failed:{e}",
            "observed": observed,
        }
    import glob
    live = [p for p in glob.glob(
        str(r / "**" / "*pack_manifest*.json"),
        recursive=True) if "backups" not in p]
    observed["live_pack_manifests"] = len(live)
    drifts: list[str] = []
    for k, v in _EXPECTED_BASELINES.items():
        if k in observed and observed[k] != v:
            drifts.append(f"{k}:{observed[k]}!={v}")
    return {
        "category": "baseline_drift",
        "drifted": bool(drifts),
        "severity": "fail" if drifts else "pass",
        "observed": observed,
        "expected": dict(_EXPECTED_BASELINES),
        "drifts": drifts,
    }


def detect_phase42_boundary_drift(
    matrix_input: Any,
) -> dict[str, Any]:
    trace_results = matrix_input.get("trace_results") \
        if isinstance(matrix_input, dict) else None
    if not isinstance(trace_results, list):
        return {"category": "boundary_drift",
                "drifted": True,
                "severity": "fail",
                "reason": "trace_results_not_list"}
    failures: list[dict[str, Any]] = []
    for r in trace_results:
        if not isinstance(r, dict):
            continue
        meta = r.get("selected_result_metadata") or {}
        for k in ("produced_audio", "invoked_tts",
                  "used_subprocess", "used_network",
                  "wrote_files"):
            if meta.get(k) is True:
                failures.append({
                    "scenario_id": r.get("scenario_id"),
                    "boundary": k})
    return {
        "category": "boundary_drift",
        "drifted": bool(failures),
        "severity": "fail" if failures else "pass",
        "failures": failures,
    }


def detect_phase42_phase21_status_drift(
    matrix_input: Any,
    root: Optional[Path] = None,
) -> dict[str, Any]:
    r = root or _root()
    en_inc = r / "corpus_sources" / "english" / "incoming"
    ru_inc = r / "corpus_sources" / "russian" / "incoming"
    en_files: list[str] = []
    ru_files: list[str] = []
    try:
        if en_inc.exists() and en_inc.is_dir():
            for p in en_inc.iterdir():
                if p.is_file():
                    en_files.append(p.name)
                if len(en_files) > 100:
                    break
        if ru_inc.exists() and ru_inc.is_dir():
            for p in ru_inc.iterdir():
                if p.is_file():
                    ru_files.append(p.name)
                if len(ru_files) > 100:
                    break
    except Exception as e:  # noqa: BLE001
        return {
            "category": "phase21_status_drift",
            "drifted": True,
            "severity": "fail",
            "reason": f"phase21_scan_failed:{e}",
        }
    staged = bool(en_files or ru_files)
    return {
        "category": "phase21_status_drift",
        "drifted": staged,
        "severity": "warn" if staged else "pass",
        "phase21_status_text":
            ("STAGED_AWAITING_OPERATOR"
             if staged else "BLOCKED"),
        "english_incoming_filenames": en_files,
        "russian_incoming_filenames": ru_files,
    }


def create_phase42_drift_stability_matrix(
    trace_results: list[Any],
    coherence_audit: Optional[dict[str, Any]] = None,
    replay_matrix: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    inp = {"trace_results": trace_results}
    asd = detect_phase42_adapter_selection_drift(inp)
    bld = detect_phase42_baseline_drift(inp)
    bdd = detect_phase42_boundary_drift(inp)
    p21 = detect_phase42_phase21_status_drift(inp)
    checks = [asd, bld, bdd, p21]
    fail = sum(1 for c in checks
                if c.get("severity") == "fail")
    warn = sum(1 for c in checks
                if c.get("severity") == "warn")
    passc = sum(1 for c in checks
                 if c.get("severity") == "pass")
    return {
        "matrix_id": f"p42drift_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "trace_count": len(trace_results or []),
        "coherence_audit_ok":
            bool((coherence_audit or {}).get("ok"))
            if coherence_audit is not None else None,
        "replay_matrix_compatibility":
            (replay_matrix or {}).get(
                "compatibility_status")
            if replay_matrix is not None else None,
        "adapter_selection_drift": asd,
        "baseline_drift": bld,
        "boundary_drift": bdd,
        "phase21_status_drift": p21,
        "fail_count": fail,
        "warn_count": warn,
        "pass_count": passc,
        "ok": fail == 0,
        "summary": (
            f"phase42 drift: fail={fail} warn={warn} "
            f"pass={passc}"),
    }


def validate_phase42_drift_stability_matrix(
    matrix: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(matrix, dict):
        return {"ok": False,
                "reasons": ["matrix_not_dict"]}
    for f in _REQUIRED_MATRIX_FIELDS:
        if f not in matrix:
            reasons.append(f"missing_field:{f}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase42_drift_stability(
    matrix: Any,
) -> dict[str, Any]:
    if not isinstance(matrix, dict):
        return {"ok": False, "summary": "no_matrix"}
    return {
        "ok": bool(matrix.get("ok")),
        "summary": matrix.get("summary"),
        "matrix_id": matrix.get("matrix_id"),
        "phase": _PHASE,
    }


def write_phase42_drift_stability_matrix(
    matrix: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(matrix)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase42_drift_stability_matrix",
    "validate_phase42_drift_stability_matrix",
    "detect_phase42_adapter_selection_drift",
    "detect_phase42_baseline_drift",
    "detect_phase42_boundary_drift",
    "detect_phase42_phase21_status_drift",
    "summarize_phase42_drift_stability",
    "write_phase42_drift_stability_matrix",
]
