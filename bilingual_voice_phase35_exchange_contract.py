"""Phase 35 - Local Witness Exchange Contract.

Defines the protocol contract for a local file-based handoff of a
Phase 34 witness package from an exporter to a witness verifier.
No network, no subprocess, no multiprocessing.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase35.exchange_contract.v1"


_SUPPORTED_ROLES = ("exporter", "witness")

_SUPPORTED_ARTIFACT_TYPES = (
    "witness_package",
    "public_key_descriptor",
    "integrity_manifest",
    "operator_guide",
    "verification_request",
    "verification_result",
    "witness_receipt",
)

_REQUIRED_ARTIFACTS = (
    "witness_package",
    "public_key_descriptor",
    "integrity_manifest",
    "operator_guide",
    "verification_request",
    "verification_result",
    "witness_receipt",
)


_FORBIDDEN_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
    "operator_id", "private_key", "secret",
    "signing_key_material", "material_hex",
    "socket", "url", "remote_host", "remote_port",
    "http_url", "https_url",
)


_VERIFICATION_ORDER = (
    "load_exchange_contract",
    "load_exporter_packet",
    "build_witness_input",
    "verify_artifact_hashes",
    "verify_witness_package",
    "verify_public_key_descriptor",
    "verify_integrity_manifest",
    "create_witness_output",
    "create_handshake_record",
)


_FAILURE_STATES = (
    "contract_invalid",
    "exporter_packet_invalid",
    "witness_input_invalid",
    "artifact_hash_mismatch",
    "witness_package_invalid",
    "public_key_descriptor_invalid",
    "integrity_manifest_invalid",
    "boundary_violation",
    "secret_leakage",
    "audio_field_present",
    "command_field_present",
    "network_field_present",
    "remote_path_rejected",
    "command_path_rejected",
    "size_limit_exceeded",
    "replay_detected",
    "exchange_id_mismatch",
    "unknown_failure",
)


_REQUIRED_CONTRACT_FIELDS = (
    "exchange_id", "created_at", "protocol_version",
    "exporter_id", "witness_id", "roles", "artifact_types",
    "required_artifacts", "forbidden_fields",
    "bounded_read_policy", "no_network_policy",
    "no_subprocess_policy", "no_audio_policy",
    "no_secret_policy", "replay_protection_policy",
    "verification_order", "failure_states", "metadata",
)


def _new_id() -> str:
    return f"xch_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def get_supported_exchange_roles() -> list[str]:
    return list(_SUPPORTED_ROLES)


def get_supported_exchange_artifact_types() -> list[str]:
    return list(_SUPPORTED_ARTIFACT_TYPES)


def get_forbidden_exchange_fields() -> list[str]:
    return list(_FORBIDDEN_FIELDS)


def get_phase35_exchange_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "protocol_version": _PHASE,
        "supported_roles": list(_SUPPORTED_ROLES),
        "supported_artifact_types": list(_SUPPORTED_ARTIFACT_TYPES),
        "required_artifacts": list(_REQUIRED_ARTIFACTS),
        "required_contract_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "forbidden_fields": list(_FORBIDDEN_FIELDS),
        "verification_order": list(_VERIFICATION_ORDER),
        "failure_states": list(_FAILURE_STATES),
        "notes": [
            "Phase 35 is local file-based exchange only.",
            "No subprocess, no network, no multiprocessing.",
            "Bounded reads enforced via max_artifact_bytes.",
            "No signing secrets ever exit the exporter process.",
        ],
    }


def create_exchange_contract(
    exchange_id: Optional[str] = None,
    exporter_id: str = "local_exporter",
    witness_id: str = "local_witness",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    eid = str(exchange_id or _new_id())
    return {
        "exchange_id": eid,
        "created_at": time.time(),
        "protocol_version": _PHASE,
        "exporter_id": str(exporter_id or "local_exporter"),
        "witness_id": str(witness_id or "local_witness"),
        "roles": list(_SUPPORTED_ROLES),
        "artifact_types": list(_SUPPORTED_ARTIFACT_TYPES),
        "required_artifacts": list(_REQUIRED_ARTIFACTS),
        "forbidden_fields": list(_FORBIDDEN_FIELDS),
        "bounded_read_policy": {
            "default_max_artifact_bytes": 5_000_000,
            "max_artifacts_per_packet": 16,
            "max_path_length_chars": 1024,
        },
        "no_network_policy": {
            "internet_disabled": True,
            "sockets_disabled": True,
            "url_paths_rejected": True,
        },
        "no_subprocess_policy": {
            "subprocess_disabled": True,
            "shell_disabled": True,
            "multiprocessing_disabled": True,
        },
        "no_audio_policy": {
            "audio_fields_rejected": True,
            "audio_files_excluded": True,
        },
        "no_secret_policy": {
            "secret_fields_rejected": True,
            "raw_key_material_rejected": True,
        },
        "replay_protection_policy": {
            "exchange_id_required": True,
            "artifact_hash_summary_required": True,
            "duplicate_exchange_id_flagged": True,
        },
        "verification_order": list(_VERIFICATION_ORDER),
        "failure_states": list(_FAILURE_STATES),
        "metadata": dict(metadata or {}),
        "phase": _PHASE,
    }


def validate_exchange_contract(contract: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(contract, dict):
        return {"ok": False, "reasons": ["contract_not_dict"]}
    for f in _REQUIRED_CONTRACT_FIELDS:
        if f not in contract:
            reasons.append(f"missing_field:{f}")
    if str(contract.get("protocol_version") or "") != _PHASE:
        reasons.append("wrong_protocol_version")
    roles = contract.get("roles") or []
    if not isinstance(roles, list) or set(roles) != set(
            _SUPPORTED_ROLES):
        reasons.append("roles_mismatch")
    fb = contract.get("forbidden_fields") or []
    if not isinstance(fb, list) or not set(_FORBIDDEN_FIELDS).issubset(
            set(fb)):
        reasons.append("forbidden_fields_incomplete")
    vo = contract.get("verification_order") or []
    if vo != list(_VERIFICATION_ORDER):
        reasons.append("verification_order_mismatch")
    # No forbidden fields appearing inside the contract itself
    for k in contract.keys():
        if str(k).lower() in _FORBIDDEN_FIELDS:
            reasons.append(f"forbidden_field_in_contract:{k}")
    try:
        json.dumps(contract, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def write_exchange_contract_report(
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
    "get_phase35_exchange_contract_schema",
    "create_exchange_contract",
    "validate_exchange_contract",
    "get_supported_exchange_roles",
    "get_supported_exchange_artifact_types",
    "get_forbidden_exchange_fields",
    "write_exchange_contract_report",
]
