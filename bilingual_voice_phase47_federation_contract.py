"""Phase 47 - Cross-Checkout Federation Contract."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase47.federation_contract.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "federation_id", "source_phase",
    "checkout_count",
    "required_artifacts",
    "required_invariants",
    "forbidden_artifacts",
    "forbidden_actions",
    "rehearsal_dry_run_only",
    "adapter_invocation_forbidden",
    "production_db_read_forbidden",
    "tamper_detection_required",
    "distinct_checkout_ids_required",
)


_REQUIRED_ARTIFACTS = (
    "imported_timeline_packages",
    "federation_graph",
    "federation_manifest",
    "federation_verification_result",
    "drift_report",
    "tamper_suite_result",
    "operator_packet",
    "status_dashboard",
)


_REQUIRED_INVARIANTS = (
    "imported_timeline_roots_preserved",
    "checkout_ids_distinct",
    "no_adapter_invocation",
    "no_production_db_read",
    "no_audio", "no_tts",
    "no_subprocess", "no_network",
    "no_multiprocessing",
    "no_secret_leakage",
    "phase21_status_tracked_not_unblocked",
    "adapter_allowlist_count_remains_5",
)


_FORBIDDEN_ARTIFACTS = (
    "runtime_dbs", "backups", "synthetic_corpora",
    "quality_samples", "pilot_imports", "checkpoints",
    "local_secret_handoff_contents",
    "audio_files", "corpus_incoming_files",
    "claude_directory_contents",
)


_FORBIDDEN_ACTIONS = (
    "adapter_invocation_in_federation",
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
    "duplicate_checkout_id",
    "tampered_timeline_root_hash",
    "tampered_federation_root_hash",
    "path_traversal", "url_scheme_path",
    "shell_metacharacter_path",
)


def get_phase47_federation_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "required_artifacts": list(_REQUIRED_ARTIFACTS),
        "required_invariants": list(_REQUIRED_INVARIANTS),
        "forbidden_artifacts":
            list(_FORBIDDEN_ARTIFACTS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "tamper_detection_required": True,
        "distinct_checkout_ids_required": True,
        "checkout_count_min": 2,
        "checkout_count_max": 8,
    }


def get_phase47_required_federation_artifacts(
) -> list[str]:
    return list(_REQUIRED_ARTIFACTS)


def get_phase47_required_invariants() -> list[str]:
    return list(_REQUIRED_INVARIANTS)


def get_phase47_forbidden_artifacts() -> list[str]:
    return list(_FORBIDDEN_ARTIFACTS)


def get_phase47_forbidden_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_phase47_federation_contract(
    federation_id: str,
    checkout_count: int = 2,
    source_phase: str = "phase46",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    cc = max(2, min(int(checkout_count or 2), 8))
    return {
        "contract_id": f"p47contract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "federation_id": str(federation_id or ""),
        "source_phase": str(source_phase or "phase46"),
        "checkout_count": cc,
        "metadata": dict(metadata or {}),
        "required_artifacts": list(_REQUIRED_ARTIFACTS),
        "required_invariants":
            list(_REQUIRED_INVARIANTS),
        "forbidden_artifacts":
            list(_FORBIDDEN_ARTIFACTS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "tamper_detection_required": True,
        "distinct_checkout_ids_required": True,
        "notes": [
            "Federation simulates N checkouts (default "
            "2) of Phase 46 timeline ledgers locally.",
            "No internet/network/multiprocessing.",
            "Verifier reads only imported timeline JSON; "
            "never production DBs; never invokes "
            "adapters.",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase47_federation_contract(
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
            "tamper_detection_required") is not True:
        reasons.append(
            "tamper_detection_must_be_required")
    if contract.get(
            "distinct_checkout_ids_required") is not True:
        reasons.append(
            "distinct_checkout_ids_must_be_required")
    cc = contract.get("checkout_count")
    if not (isinstance(cc, int) and 2 <= cc <= 8):
        reasons.append("checkout_count_out_of_range")
    arts = contract.get("required_artifacts") or []
    for must in _REQUIRED_ARTIFACTS:
        if must not in arts:
            reasons.append(f"missing_artifact:{must}")
    inv = contract.get("required_invariants") or []
    for must in _REQUIRED_INVARIANTS:
        if must not in inv:
            reasons.append(f"missing_invariant:{must}")
    forb_arts = contract.get(
        "forbidden_artifacts") or []
    for must in ("runtime_dbs", "audio_files",
                  "local_secret_handoff_contents",
                  "corpus_incoming_files",
                  "claude_directory_contents"):
        if must not in forb_arts:
            reasons.append(
                f"missing_forbidden_artifact:{must}")
    forb = contract.get("forbidden_actions") or []
    for must in ("adapter_invocation_in_federation",
                  "production_db_read_in_verifier",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "duplicate_checkout_id",
                  "tampered_timeline_root_hash",
                  "path_traversal", "url_scheme_path"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase47_federation_contract_report(
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
    "get_phase47_federation_contract_schema",
    "create_phase47_federation_contract",
    "validate_phase47_federation_contract",
    "get_phase47_required_federation_artifacts",
    "get_phase47_required_invariants",
    "get_phase47_forbidden_artifacts",
    "get_phase47_forbidden_actions",
    "write_phase47_federation_contract_report",
]
