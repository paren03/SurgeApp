"""Phase 42 - Multi-Trace Coherence Audit Contract."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase42.audit_contract.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "audit_id", "scenario_count",
    "required_scenarios",
    "required_adapter_coverage",
    "forbidden_actions",
    "rehearsal_dry_run_only",
    "new_adapter_invocation_forbidden",
)


_REQUIRED_SCENARIOS = (
    "simple_english",
    "russian_first",
    "mixed_code_switch",
    "high_prosody",
    "safety_redaction",
    "memory_continuity",
    "approve_false_refusal",
    "kill_switch_refusal",
)


_REQUIRED_ADAPTER_COVERAGE = (
    "dummy_metadata_adapter",
    "bilingual_segment_metadata_adapter",
    "prosody_density_metadata_adapter",
    "safety_redaction_trace_metadata_adapter",
    "memory_continuity_audit_metadata_adapter",
)


_FORBIDDEN_ACTIONS = (
    "new_adapter_invocation",
    "generate_audio", "invoke_tts", "run_subprocess",
    "call_powershell", "call_sapi", "call_piper",
    "write_audio_file", "clone_voice", "network_call",
    "open_socket", "multiprocessing",
    "main_runtime_integration",
    "production_db_modification",
    "corpus_import",
    "production_signing_secret_storage",
    "git_commit_of_signing_secret",
    "program_s_modification",
    "tier_" + _PROBE_ATT + "_modification",
    "worker_or_" + _LUNA_MODS + "_modification",
    "raw_transcript_exposure",
    "sensitive_fact_exposure",
)


def get_phase42_audit_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "required_scenarios": list(_REQUIRED_SCENARIOS),
        "required_adapter_coverage":
            list(_REQUIRED_ADAPTER_COVERAGE),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "new_adapter_invocation_forbidden": True,
        "scenario_count_min": 1,
        "scenario_count_max": 12,
        "production_db_must_remain_unchanged": True,
        "phase21_must_remain_blocked_unless_staged": True,
    }


def get_phase42_required_scenarios() -> list[str]:
    return list(_REQUIRED_SCENARIOS)


def get_phase42_required_adapter_coverage() -> list[str]:
    return list(_REQUIRED_ADAPTER_COVERAGE)


def get_phase42_forbidden_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_phase42_audit_contract(
    audit_id: str,
    scenario_count: int = 8,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    sc = max(1, min(int(scenario_count), 12))
    return {
        "contract_id": f"p42contract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "audit_id": str(audit_id or ""),
        "scenario_count": sc,
        "metadata": dict(metadata or {}),
        "required_scenarios": list(_REQUIRED_SCENARIOS),
        "required_adapter_coverage":
            list(_REQUIRED_ADAPTER_COVERAGE),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "new_adapter_invocation_forbidden": True,
        "production_db_must_remain_unchanged": True,
        "phase21_must_remain_blocked_unless_staged": True,
        "notes": [
            "Bounded multi-trace coherence audit.",
            "Read-only over production DBs.",
            "No new adapters; the five Phase 41 adapters "
            "are the only callable surface.",
        ],
    }


def validate_phase42_audit_contract(
    contract: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(contract, dict):
        return {"ok": False,
                "reasons": ["contract_not_dict"]}
    for f in _REQUIRED_CONTRACT_FIELDS:
        if f not in contract:
            reasons.append(f"missing_field:{f}")
    if contract.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    if contract.get(
            "new_adapter_invocation_forbidden") is not True:
        reasons.append(
            "new_adapter_invocation_must_be_forbidden")
    sc = contract.get("scenario_count")
    if not (isinstance(sc, int) and 1 <= sc <= 12):
        reasons.append("scenario_count_out_of_range")
    scen = contract.get("required_scenarios") or []
    for must in _REQUIRED_SCENARIOS:
        if must not in scen:
            reasons.append(f"missing_scenario:{must}")
    cov = contract.get("required_adapter_coverage") or []
    for must in _REQUIRED_ADAPTER_COVERAGE:
        if must not in cov:
            reasons.append(f"missing_adapter_coverage:{must}")
    if len(cov) != 5:
        reasons.append(f"adapter_coverage_not_5:{len(cov)}")
    forb = contract.get("forbidden_actions") or []
    for must in ("new_adapter_invocation",
                  "generate_audio", "invoke_tts",
                  "run_subprocess", "network_call",
                  "multiprocessing", "corpus_import",
                  "production_db_modification"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase42_audit_contract_report(
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
    "get_phase42_audit_contract_schema",
    "create_phase42_audit_contract",
    "validate_phase42_audit_contract",
    "get_phase42_required_scenarios",
    "get_phase42_required_adapter_coverage",
    "get_phase42_forbidden_actions",
    "write_phase42_audit_contract_report",
]
