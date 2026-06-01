"""Phase 48 - Capsule Tamper Suite (13 cases)."""

from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase48_fresh_checkout_verifier \
    as fcv


_PHASE = "phase48.capsule_tamper_suite.v1"


_TAMPER_CASES = (
    "missing_federation_graph",
    "missing_federation_manifest",
    "missing_tamper_suite_result",
    "modified_federation_root_hash",
    "modified_capsule_artifact_hash",
    "adapter_count_mutation",
    "phase21_status_unexpected",
    "injected_audio_flag",
    "injected_secret_field",
    "injected_command_field",
    "injected_runtime_db_reference",
    "production_db_read_claim",
    "adapter_invocation_claim",
)


def create_phase48_tamper_cases(
    capsule: Any,
) -> list[dict[str, Any]]:
    if not isinstance(capsule, dict):
        return []
    return [{"case": c, "expected_detection": True}
             for c in _TAMPER_CASES]


def _drop_entry(c: dict[str, Any], key: str) -> None:
    c["artifacts"] = [
        e for e in c.get("artifacts") or []
        if isinstance(e, dict)
        and e.get("artifact_key") != key]
    c["artifact_count"] = len(c["artifacts"])
    h = dict(c.get("artifact_hashes") or {})
    h.pop(key, None)
    c["artifact_hashes"] = h


def apply_phase48_tamper_case(
    capsule: Any,
    tamper_case: str,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"ok": False,
                "reason": "capsule_not_dict"}
    c = copy.deepcopy(capsule)
    case = str(tamper_case or "")
    if case == "missing_federation_graph":
        _drop_entry(c, "phase47_federation_graph")
    elif case == "missing_federation_manifest":
        _drop_entry(c, "phase47_federation_manifest")
    elif case == "missing_tamper_suite_result":
        _drop_entry(c, "phase47_tamper_suite_result")
    elif case == "modified_federation_root_hash":
        # Tamper the federation_graph's inline content
        for e in c.get("artifacts") or []:
            if e.get("artifact_key") == \
                    "phase47_federation_graph":
                ic = e.get("inline_content")
                if isinstance(ic, dict):
                    ic["federation_root_hash"] = (
                        "0" * 64)
    elif case == "modified_capsule_artifact_hash":
        # Mutate the sha256 on the first artifact (so
        # capsule_root_hash drifts on re-derive)
        ents = c.get("artifacts") or []
        if ents:
            ents[0]["sha256"] = "0" * 64
    elif case == "adapter_count_mutation":
        c["adapter_allowlist_count"] = 4
    elif case == "phase21_status_unexpected":
        c["phase21_status_text"] = "UNBLOCKED"
    elif case == "injected_audio_flag":
        bs = dict(c.get("boundary_summary") or {})
        bs["no_audio"] = False
        c["boundary_summary"] = bs
    elif case == "injected_secret_field":
        # Inject into the federation_graph's inline
        # content (the verifier scans inline)
        for e in c.get("artifacts") or []:
            ic = e.get("inline_content")
            if isinstance(ic, dict):
                ic["signing_key_material"] = "leak"
                break
    elif case == "injected_command_field":
        for e in c.get("artifacts") or []:
            ic = e.get("inline_content")
            if isinstance(ic, dict):
                ic["command"] = "rm -rf /"
                break
    elif case == "injected_runtime_db_reference":
        ents = list(c.get("artifacts") or [])
        ents.append({
            "artifact_key": "leak_db",
            "source_phase": "leak",
            "relative_path":
                "lexicon/luna_vocabulary.sqlite",
            "absolute_path":
                "lexicon/luna_vocabulary.sqlite",
            "artifact_type": "other",
            "size_bytes": 100,
            "sha256": "1" * 64,
        })
        c["artifacts"] = ents
        h = dict(c.get("artifact_hashes") or {})
        h["leak_db"] = "1" * 64
        c["artifact_hashes"] = h
    elif case == "production_db_read_claim":
        bs = dict(c.get("boundary_summary") or {})
        bs["no_production_db_read_in_capsule"] = False
        c["boundary_summary"] = bs
    elif case == "adapter_invocation_claim":
        bs = dict(c.get("boundary_summary") or {})
        bs["no_adapter_invocation_in_capsule"] = False
        c["boundary_summary"] = bs
    else:
        return {"ok": False, "tamper_case": case,
                "reason": "unknown_case",
                "phase": _PHASE}
    return {"ok": True, "capsule": c,
            "tamper_case": case,
            "phase": _PHASE}


def _detect(
    capsule: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> bool:
    res = fcv.verify_phase48_capsule_fresh_checkout(
        capsule, manifest=manifest)
    return not bool(res.get("ok"))


def run_phase48_tamper_suite(
    capsule: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"ok": False,
                "reasons": ["capsule_not_dict"],
                "phase": _PHASE}
    results: list[dict[str, Any]] = []
    detected = 0
    undetected = 0
    for case in _TAMPER_CASES:
        applied = apply_phase48_tamper_case(
            capsule, case)
        if not applied.get("ok"):
            results.append({"case": case,
                             "applied": False,
                             "detected": False,
                             "reason":
                                 applied.get("reason")})
            undetected += 1
            continue
        bad = applied.get("capsule") or {}
        caught = _detect(bad, manifest=manifest)
        if caught:
            detected += 1
        else:
            undetected += 1
        results.append({"case": case,
                         "applied": True,
                         "detected": caught})
    return {
        "suite_id": f"p48tamper_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "case_count": len(_TAMPER_CASES),
        "detected_count": detected,
        "undetected_count": undetected,
        "results": results,
        "ok": (undetected == 0
                and detected == len(_TAMPER_CASES)),
        "summary": (
            f"phase48 tamper suite: cases="
            f"{len(_TAMPER_CASES)} detected="
            f"{detected} undetected={undetected}"),
    }


def validate_phase48_tamper_suite_result(
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


def summarize_phase48_tamper_suite(
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


def write_phase48_tamper_suite_report(
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
    "create_phase48_tamper_cases",
    "apply_phase48_tamper_case",
    "run_phase48_tamper_suite",
    "validate_phase48_tamper_suite_result",
    "summarize_phase48_tamper_suite",
    "write_phase48_tamper_suite_report",
]
