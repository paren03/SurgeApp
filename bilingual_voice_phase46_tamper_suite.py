"""Phase 46 - Timeline Tamper Suite (12 cases)."""

from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase46_long_horizon_verifier as lhv


_PHASE = "phase46.tamper_suite.v1"


_TAMPER_CASES = (
    "break_monotonic_order",
    "duplicate_archive_id",
    "modified_captured_sha",
    "missing_chain_link",
    "tamper_timeline_root_hash",
    "phase21_status_unexpected_at_archive",
    "phase21_status_unexpected_at_timeline",
    "boundary_violation_per_archive_audio",
    "boundary_violation_timeline_adapter",
    "injected_secret_field_in_timeline",
    "injected_command_field_in_timeline",
    "injected_runtime_db_capture_path",
)


def create_phase46_tamper_cases(
    timeline: Any,
) -> list[dict[str, Any]]:
    if not isinstance(timeline, dict):
        return []
    return [{"case": c, "expected_detection": True}
             for c in _TAMPER_CASES]


def apply_phase46_tamper_case(
    timeline: Any,
    tamper_case: str,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reason": "timeline_not_dict"}
    t = copy.deepcopy(timeline)
    case = str(tamper_case or "")
    oa = t.get("ordered_archives") or []
    if case == "break_monotonic_order":
        if len(oa) >= 2:
            # Force second archive's timestamp earlier
            oa[1]["archive_created_at"] = float(
                oa[0].get("archive_created_at") or 0) \
                - 10.0
    elif case == "duplicate_archive_id":
        if len(oa) >= 2 and oa[0].get("archive_id"):
            oa[1]["archive_id"] = oa[0].get(
                "archive_id")
    elif case == "modified_captured_sha":
        if oa:
            oa[0]["captured_sha256"] = "0" * 64
    elif case == "missing_chain_link":
        links = t.get("chain_links") or []
        if links:
            t["chain_links"] = links[:-1]
    elif case == "tamper_timeline_root_hash":
        t["timeline_root_hash"] = "0" * 64
    elif case == "phase21_status_unexpected_at_archive":
        if oa:
            oa[0]["phase21_status_text"] = "UNBLOCKED"
    elif case == "phase21_status_unexpected_at_timeline":
        t["phase21_status_text"] = "UNBLOCKED"
    elif case == "boundary_violation_per_archive_audio":
        if oa:
            bs = dict(oa[0].get(
                "boundary_summary") or {})
            bs["no_adapter_invocation_in_archive"] = \
                False
            oa[0]["boundary_summary"] = bs
    elif case == "boundary_violation_timeline_adapter":
        bs = dict(t.get("boundary_summary") or {})
        bs["no_adapter_invocation_in_timeline"] = False
        t["boundary_summary"] = bs
    elif case == "injected_secret_field_in_timeline":
        t["signing_key_material"] = "leak"
    elif case == "injected_command_field_in_timeline":
        t["command"] = "rm -rf /"
    elif case == "injected_runtime_db_capture_path":
        if oa:
            oa[0]["captured_path"] = \
                "lexicon/luna_vocabulary.sqlite"
    else:
        return {"ok": False, "tamper_case": case,
                "reason": "unknown_case",
                "phase": _PHASE}
    t["ordered_archives"] = oa
    return {"ok": True, "timeline": t,
            "tamper_case": case, "phase": _PHASE}


def _detect_via_verifier(
    timeline: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> bool:
    res = lhv.verify_phase46_long_horizon_timeline(
        timeline, manifest=manifest)
    return not bool(res.get("ok"))


def run_phase46_tamper_suite(
    timeline: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"],
                "phase": _PHASE}
    results: list[dict[str, Any]] = []
    detected = 0
    undetected = 0
    for case in _TAMPER_CASES:
        applied = apply_phase46_tamper_case(
            timeline, case)
        if not applied.get("ok"):
            results.append({"case": case,
                             "applied": False,
                             "detected": False,
                             "reason":
                                 applied.get("reason")})
            undetected += 1
            continue
        bad = applied.get("timeline") or {}
        caught = _detect_via_verifier(
            bad, manifest=manifest)
        if caught:
            detected += 1
        else:
            undetected += 1
        results.append({
            "case": case,
            "applied": True,
            "detected": caught,
        })
    return {
        "suite_id": f"p46tamper_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "case_count": len(_TAMPER_CASES),
        "detected_count": detected,
        "undetected_count": undetected,
        "results": results,
        "ok": undetected == 0
              and detected == len(_TAMPER_CASES),
        "summary": (
            f"phase46 tamper suite: cases="
            f"{len(_TAMPER_CASES)} detected="
            f"{detected} undetected={undetected}"),
    }


def validate_phase46_tamper_suite_result(
    result: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(result, dict):
        return {"ok": False,
                "reasons": ["result_not_dict"]}
    for f in ("suite_id", "created_at", "phase",
              "case_count", "detected_count",
              "undetected_count", "results", "ok"):
        if f not in result:
            reasons.append(f"missing_field:{f}")
    if result.get("case_count") != len(_TAMPER_CASES):
        reasons.append("case_count_mismatch")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase46_tamper_suite(
    result: Any,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "summary": "no_result"}
    return {
        "ok": bool(result.get("ok")),
        "summary": result.get("summary"),
        "suite_id": result.get("suite_id"),
        "phase": _PHASE,
    }


def write_phase46_tamper_suite_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase46_tamper_cases",
    "apply_phase46_tamper_case",
    "run_phase46_tamper_suite",
    "validate_phase46_tamper_suite_result",
    "summarize_phase46_tamper_suite",
    "write_phase46_tamper_suite_report",
]
