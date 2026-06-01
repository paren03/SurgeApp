"""Phase 46 - Cross-Archive Long-Horizon Timeline Contract."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase46.timeline_contract.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "timeline_id", "source_phase",
    "min_archive_count",
    "max_archive_count",
    "required_per_archive_fields",
    "required_chain_invariants",
    "forbidden_actions",
    "rehearsal_dry_run_only",
    "adapter_invocation_forbidden",
    "production_db_read_forbidden",
    "monotonic_ordering_required",
    "tamper_detection_required",
)


_REQUIRED_PER_ARCHIVE_FIELDS = (
    "archive_id", "created_at", "phase",
    "source_phases", "phase_counts",
    "artifact_count", "artifact_hashes",
    "phase21_status_text",
    "boundary_summary",
)


_REQUIRED_CHAIN_INVARIANTS = (
    "monotonic_created_at",
    "deterministic_root_hash",
    "all_archives_phase21_blocked_or_staged",
    "all_archives_boundary_intact",
    "no_runtime_db_reference_in_timeline",
    "no_adapter_invocation_in_timeline",
)


_FORBIDDEN_ACTIONS = (
    "adapter_invocation_in_timeline",
    "adapter_reinvocation_in_verifier",
    "new_adapter_invocation",
    "production_db_read_in_verifier",
    "production_db_modification",
    "generate_audio", "invoke_tts", "run_subprocess",
    "call_powershell", "call_sapi", "call_piper",
    "write_audio_file", "clone_voice", "network_call",
    "open_socket", "multiprocessing",
    "main_runtime_integration",
    "corpus_import",
    "production_signing_secret_storage",
    "git_commit_of_signing_secret",
    "program_s_modification",
    "tier_" + _PROBE_ATT + "_modification",
    "worker_or_" + _LUNA_MODS + "_modification",
    "raw_transcript_exposure",
    "sensitive_fact_exposure",
    "broken_monotonic_order",
    "tampered_archive_hash",
    "duplicate_archive_id",
)


def get_phase46_timeline_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "required_per_archive_fields":
            list(_REQUIRED_PER_ARCHIVE_FIELDS),
        "required_chain_invariants":
            list(_REQUIRED_CHAIN_INVARIANTS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "monotonic_ordering_required": True,
        "tamper_detection_required": True,
        "min_archive_count": 2,
        "max_archive_count": 32,
    }


def get_phase46_required_per_archive_fields(
) -> list[str]:
    return list(_REQUIRED_PER_ARCHIVE_FIELDS)


def get_phase46_required_chain_invariants() -> list[str]:
    return list(_REQUIRED_CHAIN_INVARIANTS)


def get_phase46_forbidden_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_phase46_timeline_contract(
    timeline_id: str,
    source_phase: str = "phase45",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "contract_id": f"p46contract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "timeline_id": str(timeline_id or ""),
        "source_phase": str(source_phase or "phase45"),
        "metadata": dict(metadata or {}),
        "min_archive_count": 2,
        "max_archive_count": 32,
        "required_per_archive_fields":
            list(_REQUIRED_PER_ARCHIVE_FIELDS),
        "required_chain_invariants":
            list(_REQUIRED_CHAIN_INVARIANTS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "monotonic_ordering_required": True,
        "tamper_detection_required": True,
        "notes": [
            "Timeline ledger aggregates multiple Phase "
            "45 archives across sessions.",
            "Archives are ordered by monotonic "
            "created_at; duplicate ids rejected.",
            "Verifier reads only the captured archive "
            "JSON; never production DBs; never invokes "
            "adapters.",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase46_timeline_contract(
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
            "adapter_invocation_forbidden") is not True:
        reasons.append(
            "adapter_invocation_must_be_forbidden")
    if contract.get(
            "production_db_read_forbidden") is not True:
        reasons.append(
            "production_db_read_must_be_forbidden")
    if contract.get(
            "monotonic_ordering_required") is not True:
        reasons.append(
            "monotonic_ordering_must_be_required")
    if contract.get(
            "tamper_detection_required") is not True:
        reasons.append(
            "tamper_detection_must_be_required")
    mn = contract.get("min_archive_count")
    mx = contract.get("max_archive_count")
    if not (isinstance(mn, int) and mn >= 2):
        reasons.append("min_archive_count_must_be_ge_2")
    if not (isinstance(mx, int) and mx >= mn):
        reasons.append("max_archive_count_must_be_ge_min")
    fields = contract.get(
        "required_per_archive_fields") or []
    for must in ("archive_id", "created_at",
                  "source_phases", "phase_counts",
                  "artifact_count", "artifact_hashes",
                  "phase21_status_text",
                  "boundary_summary"):
        if must not in fields:
            reasons.append(f"missing_per_archive:{must}")
    inv = contract.get("required_chain_invariants") or []
    for must in _REQUIRED_CHAIN_INVARIANTS:
        if must not in inv:
            reasons.append(f"missing_invariant:{must}")
    forb = contract.get("forbidden_actions") or []
    for must in ("adapter_invocation_in_timeline",
                  "production_db_read_in_verifier",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "broken_monotonic_order",
                  "tampered_archive_hash",
                  "duplicate_archive_id"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase46_timeline_contract_report(
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
    "get_phase46_timeline_contract_schema",
    "create_phase46_timeline_contract",
    "validate_phase46_timeline_contract",
    "get_phase46_required_per_archive_fields",
    "get_phase46_required_chain_invariants",
    "get_phase46_forbidden_actions",
    "write_phase46_timeline_contract_report",
]
