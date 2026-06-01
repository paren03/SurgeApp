"""Phase 45 - Multi-Bundle Archive + Chain-of-Trust Contract."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase45.archive_contract.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "archive_id", "source_phases",
    "required_archive_artifacts",
    "required_chain_links",
    "forbidden_archive_artifacts",
    "forbidden_actions",
    "rehearsal_dry_run_only",
    "adapter_invocation_forbidden",
    "production_db_read_forbidden",
    "chain_verification_required",
    "tamper_detection_required",
)


_REQUIRED_SOURCE_PHASES = ("phase42", "phase43", "phase44")


_REQUIRED_ARCHIVE_ARTIFACTS = (
    # Phase 42
    "phase42_audit_contract",
    "phase42_trace_batch",
    "phase42_coherence_audit",
    "phase42_replay_matrix",
    "phase42_drift_stability_matrix",
    "phase42_operator_packet",
    "phase42_operator_markdown",
    # Phase 43
    "phase43_portable_bundle",
    "phase43_bundle_manifest",
    "phase43_fresh_checkout_result",
    "phase43_portability_audit",
    "phase43_operator_packet",
    "phase43_status_dashboard_json",
    "phase43_status_dashboard_md",
    # Phase 44
    "phase44_imported_bundle",
    "phase44_import_manifest",
    "phase44_fresh_import_result",
    "phase44_tamper_suite",
    "phase44_roundtrip_receipt",
    "phase44_operator_packet",
    "phase44_status_dashboard_json",
    "phase44_status_dashboard_md",
    # Reports
    "phase42_report", "phase43_report", "phase44_report",
)


_REQUIRED_CHAIN_LINKS = (
    "phase42_to_phase43_bundle",
    "phase43_to_phase44_import",
    "phase44_import_to_roundtrip_receipt",
    "phase44_tamper_suite_to_operator_packet",
    "phase44_operator_packet_to_dashboard",
)


_FORBIDDEN_ARCHIVE_ARTIFACTS = (
    "runtime_dbs", "backups", "synthetic_corpora",
    "quality_samples", "pilot_imports", "checkpoints",
    "local_secret_handoff_contents",
    "audio_files", "corpus_incoming_files",
    "claude_directory_contents",
)


_FORBIDDEN_ACTIONS = (
    "adapter_invocation_in_archive",
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
    "broken_chain_order",
    "tampered_artifact_hash",
)


def get_phase45_archive_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "required_source_phases":
            list(_REQUIRED_SOURCE_PHASES),
        "required_archive_artifacts":
            list(_REQUIRED_ARCHIVE_ARTIFACTS),
        "required_chain_links":
            list(_REQUIRED_CHAIN_LINKS),
        "forbidden_archive_artifacts":
            list(_FORBIDDEN_ARCHIVE_ARTIFACTS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "chain_verification_required": True,
        "tamper_detection_required": True,
    }


def get_phase45_required_archive_artifacts() -> list[str]:
    return list(_REQUIRED_ARCHIVE_ARTIFACTS)


def get_phase45_required_chain_links() -> list[str]:
    return list(_REQUIRED_CHAIN_LINKS)


def get_phase45_forbidden_archive_artifacts() -> list[str]:
    return list(_FORBIDDEN_ARCHIVE_ARTIFACTS)


def get_phase45_forbidden_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_phase45_archive_contract(
    archive_id: str,
    source_phases: Optional[list[str]] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    sp = list(source_phases) if source_phases else \
        list(_REQUIRED_SOURCE_PHASES)
    return {
        "contract_id": f"p45contract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "archive_id": str(archive_id or ""),
        "source_phases": sp,
        "metadata": dict(metadata or {}),
        "required_archive_artifacts":
            list(_REQUIRED_ARCHIVE_ARTIFACTS),
        "required_chain_links":
            list(_REQUIRED_CHAIN_LINKS),
        "forbidden_archive_artifacts":
            list(_FORBIDDEN_ARCHIVE_ARTIFACTS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "chain_verification_required": True,
        "tamper_detection_required": True,
        "notes": [
            "Archive aggregates Phase 42/43/44 portable "
            "JSON / Markdown / report artifacts.",
            "Chain-of-trust ledger verifies Phase 42 -> "
            "43 -> 44 ordering.",
            "Verifier reads only the archive; never "
            "production DBs.",
            "Phase 21 status carried, never unblocked.",
        ],
    }


def validate_phase45_archive_contract(
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
            "chain_verification_required") is not True:
        reasons.append(
            "chain_verification_must_be_required")
    if contract.get(
            "tamper_detection_required") is not True:
        reasons.append(
            "tamper_detection_must_be_required")
    sp = contract.get("source_phases") or []
    for must in _REQUIRED_SOURCE_PHASES:
        if must not in sp:
            reasons.append(f"missing_source_phase:{must}")
    arts = contract.get("required_archive_artifacts") or []
    for must in ("phase42_replay_matrix",
                  "phase42_operator_packet",
                  "phase43_portable_bundle",
                  "phase43_bundle_manifest",
                  "phase44_roundtrip_receipt",
                  "phase44_operator_packet"):
        if must not in arts:
            reasons.append(f"missing_artifact:{must}")
    links = contract.get("required_chain_links") or []
    for must in _REQUIRED_CHAIN_LINKS:
        if must not in links:
            reasons.append(f"missing_chain_link:{must}")
    forb_arts = contract.get(
        "forbidden_archive_artifacts") or []
    for must in ("runtime_dbs", "audio_files",
                  "local_secret_handoff_contents",
                  "claude_directory_contents"):
        if must not in forb_arts:
            reasons.append(
                f"missing_forbidden_artifact:{must}")
    forb = contract.get("forbidden_actions") or []
    for must in ("adapter_invocation_in_archive",
                  "production_db_read_in_verifier",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "broken_chain_order",
                  "tampered_artifact_hash"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase45_archive_contract_report(
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
    "get_phase45_archive_contract_schema",
    "create_phase45_archive_contract",
    "validate_phase45_archive_contract",
    "get_phase45_required_archive_artifacts",
    "get_phase45_required_chain_links",
    "get_phase45_forbidden_archive_artifacts",
    "get_phase45_forbidden_actions",
    "write_phase45_archive_contract_report",
]
