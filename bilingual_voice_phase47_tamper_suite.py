"""Phase 47 - Federation Tamper Suite (13 cases)."""

from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_phase47_federation_verifier as fv


_PHASE = "phase47.tamper_suite.v1"


_TAMPER_CASES = (
    "duplicate_checkout_id",
    "timeline_root_hash_mutation",
    "federation_root_hash_mutation",
    "imported_package_hash_mutation",
    "missing_checkout_node",
    "adapter_count_mutation",
    "phase21_status_unexpected",
    "injected_audio_flag",
    "injected_secret_field",
    "injected_command_field",
    "injected_runtime_db_reference",
    "production_db_read_claim",
    "adapter_invocation_claim",
)


def create_phase47_tamper_cases(
    imported_timelines: Any,
    graph: Any,
    manifest: Any,
) -> list[dict[str, Any]]:
    if (not isinstance(imported_timelines, list)
            or not isinstance(graph, dict)
            or not isinstance(manifest, dict)):
        return []
    return [{"case": c, "expected_detection": True}
             for c in _TAMPER_CASES]


def apply_phase47_tamper_case(
    imported_timelines: Any,
    graph: Any,
    manifest: Any,
    tamper_case: str,
) -> dict[str, Any]:
    if (not isinstance(imported_timelines, list)
            or not isinstance(graph, dict)
            or not isinstance(manifest, dict)):
        return {"ok": False,
                "reason": "non_dict_or_non_list"}
    imp = copy.deepcopy(imported_timelines)
    g = copy.deepcopy(graph)
    m = copy.deepcopy(manifest)
    case = str(tamper_case or "")
    if case == "duplicate_checkout_id":
        nodes = g.get("checkout_nodes") or []
        if len(nodes) >= 2:
            nodes[1]["checkout_id"] = \
                nodes[0].get("checkout_id")
    elif case == "timeline_root_hash_mutation":
        nodes = g.get("checkout_nodes") or []
        if nodes:
            nodes[0]["timeline_root_hash"] = "0" * 64
    elif case == "federation_root_hash_mutation":
        g["federation_root_hash"] = "0" * 64
    elif case == "imported_package_hash_mutation":
        if imp and isinstance(imp[0], dict):
            pkg = imp[0].get("package") or {}
            pkg["package_hash"] = "0" * 64
    elif case == "missing_checkout_node":
        nodes = g.get("checkout_nodes") or []
        if nodes:
            g["checkout_nodes"] = nodes[1:]
            g["checkout_count"] = max(
                0, int(g.get("checkout_count") or 0)
                - 1)
    elif case == "adapter_count_mutation":
        if imp and isinstance(imp[0], dict):
            pkg = imp[0].get("package") or {}
            pkg["adapter_allowlist_count"] = 4
    elif case == "phase21_status_unexpected":
        if imp and isinstance(imp[0], dict):
            pkg = imp[0].get("package") or {}
            pkg["phase21_status_text"] = "UNBLOCKED"
    elif case == "injected_audio_flag":
        bs = dict(g.get("boundary_summary") or {})
        bs["no_audio"] = False
        g["boundary_summary"] = bs
    elif case == "injected_secret_field":
        g["signing_key_material"] = "leak"
    elif case == "injected_command_field":
        g["command"] = "rm -rf /"
    elif case == "injected_runtime_db_reference":
        if imp and isinstance(imp[0], dict):
            imp[0]["imported_path"] = \
                "lexicon/luna_vocabulary.sqlite"
    elif case == "production_db_read_claim":
        bs = dict(g.get("boundary_summary") or {})
        bs["no_production_db_read_in_federation"] = \
            False
        g["boundary_summary"] = bs
    elif case == "adapter_invocation_claim":
        bs = dict(g.get("boundary_summary") or {})
        bs["no_adapter_invocation_in_federation"] = \
            False
        g["boundary_summary"] = bs
    else:
        return {"ok": False, "tamper_case": case,
                "reason": "unknown_case",
                "phase": _PHASE}
    return {"ok": True,
            "imported_timelines": imp,
            "graph": g,
            "manifest": m,
            "tamper_case": case,
            "phase": _PHASE}


def _detect(imported_timelines, graph, manifest) -> bool:
    res = fv.verify_phase47_federation(
        imported_timelines=imported_timelines,
        graph=graph,
        manifest=manifest)
    return not bool(res.get("ok"))


def run_phase47_tamper_suite(
    imported_timelines: Any,
    graph: Any,
    manifest: Any,
) -> dict[str, Any]:
    if (not isinstance(imported_timelines, list)
            or not isinstance(graph, dict)
            or not isinstance(manifest, dict)):
        return {"ok": False,
                "reasons": ["non_compatible_input"],
                "phase": _PHASE}
    results: list[dict[str, Any]] = []
    detected = 0
    undetected = 0
    for case in _TAMPER_CASES:
        applied = apply_phase47_tamper_case(
            imported_timelines, graph, manifest, case)
        if not applied.get("ok"):
            results.append({"case": case,
                             "applied": False,
                             "detected": False,
                             "reason":
                                 applied.get("reason")})
            undetected += 1
            continue
        caught = _detect(
            applied.get("imported_timelines") or [],
            applied.get("graph") or {},
            applied.get("manifest") or {})
        if caught:
            detected += 1
        else:
            undetected += 1
        results.append({"case": case,
                         "applied": True,
                         "detected": caught})
    return {
        "suite_id": f"p47tamper_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "case_count": len(_TAMPER_CASES),
        "detected_count": detected,
        "undetected_count": undetected,
        "results": results,
        "ok": (undetected == 0
                and detected == len(_TAMPER_CASES)),
        "summary": (
            f"phase47 tamper suite: cases="
            f"{len(_TAMPER_CASES)} detected="
            f"{detected} undetected={undetected}"),
    }


def validate_phase47_tamper_suite_result(
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


def summarize_phase47_tamper_suite(
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


def write_phase47_tamper_suite_report(
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
    "create_phase47_tamper_cases",
    "apply_phase47_tamper_case",
    "run_phase47_tamper_suite",
    "validate_phase47_tamper_suite_result",
    "summarize_phase47_tamper_suite",
    "write_phase47_tamper_suite_report",
]
