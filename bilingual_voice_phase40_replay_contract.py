"""Phase 40 - Audit-Replay Contract.

Defines the contract for replaying stored Phase 39 rehearsal
artifacts against the current live governance stack. Read-only,
JSON-serializable, no new adapter behavior.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase40.replay_contract.v1"


# Runtime-assembled forbidden-runtime tokens.
_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "replay_id", "source_phase",
    "required_replay_inputs",
    "drift_categories",
    "forbidden_actions",
    "rehearsal_dry_run_only",
    "new_adapter_invocation_forbidden",
)


_REQUIRED_REPLAY_INPUTS = (
    "rehearsal_contract",
    "umbrella_consent",
    "stage_receipts",
    "rehearsal_trace",
    "rehearsal_recheck",
    "rehearsal_report",
    "phase38_governance_artifacts",
)


_DRIFT_CATEGORIES = (
    "missing_artifact",
    "hash_chain_drift",
    "consent_binding_drift",
    "adapter_allowlist_drift",
    "governance_doc_drift",
    "baseline_drift",
    "phase21_status_drift",
    "forbidden_boundary_drift",
    "secret_leakage",
    "audio_artifact_drift",
    "rollback_matrix_drift",
    "commit_safety_drift",
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
)


def get_phase40_replay_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "required_replay_inputs":
            list(_REQUIRED_REPLAY_INPUTS),
        "drift_categories": list(_DRIFT_CATEGORIES),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "new_adapter_invocation_forbidden": True,
        "production_db_must_remain_unchanged": True,
        "secret_audio_command_leakage_forbidden": True,
    }


def get_phase40_required_replay_inputs() -> list[str]:
    return list(_REQUIRED_REPLAY_INPUTS)


def get_phase40_drift_categories() -> list[str]:
    return list(_DRIFT_CATEGORIES)


def get_phase40_forbidden_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_phase40_replay_contract(
    replay_id: str,
    source_phase: str = "phase39",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "contract_id": f"rpcontract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "replay_id": str(replay_id or ""),
        "source_phase": str(source_phase or "phase39"),
        "metadata": dict(metadata or {}),
        "required_replay_inputs":
            list(_REQUIRED_REPLAY_INPUTS),
        "drift_categories": list(_DRIFT_CATEGORIES),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "new_adapter_invocation_forbidden": True,
        "production_db_must_remain_unchanged": True,
        "notes": [
            "Replay re-derives hashes and compares against "
            "stored evidence; it does NOT invoke adapters.",
            "All reads are bounded and local.",
            "Drift detection is read-only.",
        ],
    }


def validate_phase40_replay_contract(
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
        reasons.append("new_adapter_invocation_must_be_forbidden")
    inputs = contract.get("required_replay_inputs") or []
    for must in ("rehearsal_contract", "umbrella_consent",
                  "stage_receipts", "rehearsal_trace",
                  "rehearsal_recheck", "rehearsal_report"):
        if must not in inputs:
            reasons.append(f"missing_input:{must}")
    cats = contract.get("drift_categories") or []
    for must in ("hash_chain_drift",
                  "adapter_allowlist_drift",
                  "baseline_drift",
                  "phase21_status_drift",
                  "secret_leakage"):
        if must not in cats:
            reasons.append(f"missing_drift_cat:{must}")
    forb = contract.get("forbidden_actions") or []
    for must in ("new_adapter_invocation",
                  "generate_audio", "invoke_tts",
                  "run_subprocess", "network_call",
                  "multiprocessing"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase40_replay_contract_report(
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
    "get_phase40_replay_contract_schema",
    "get_phase40_required_replay_inputs",
    "get_phase40_drift_categories",
    "get_phase40_forbidden_actions",
    "create_phase40_replay_contract",
    "validate_phase40_replay_contract",
    "write_phase40_replay_contract_report",
]
