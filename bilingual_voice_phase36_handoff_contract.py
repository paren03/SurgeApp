"""Phase 36 - Out-of-Band Key Handoff Contract.

Defines the test-only key handoff protocol contract. No network,
no subprocess, no production key handling.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase36.handoff_contract.v1"


_SUPPORTED_HANDOFF_ARTIFACTS = (
    "sealed_envelope",
    "public_descriptor",
    "consent_marker_hash",
    "operator_guide",
    "verification_input",
    "verification_output",
    "leakage_scan_report",
)


_FORBIDDEN_HANDOFF_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
    "operator_id",
    # Public-artifact-only forbidden tokens. Note: secret-bearing
    # envelopes legitimately CARRY `sealed_payload`, but contract
    # validation is applied to the contract itself (not the
    # envelope), so `sealed_payload` is listed here for the
    # public-artifact policy.
    "private_key", "secret", "material_hex",
    "signing_key_material", "raw_key", "hmac_key",
    "sealed_payload",
    "socket", "url", "remote_host", "remote_port",
    "http_url", "https_url",
)


_VERIFICATION_ORDER = (
    "load_handoff_contract",
    "load_sealed_envelope",
    "check_consent_marker",
    "unseal_envelope_in_memory",
    "verify_signed_evidence_with_envelope_key",
    "wipe_unsealed_key_from_memory",
    "emit_verification_result",
)


_FAILURE_STATES = (
    "contract_invalid",
    "envelope_invalid",
    "consent_marker_missing",
    "consent_marker_mismatch",
    "envelope_not_test_only",
    "production_label_rejected",
    "secret_leaked_in_public_artifact",
    "secret_artifact_in_unsafe_path",
    "tampered_envelope",
    "tampered_evidence",
    "unknown_failure",
)


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "protocol_version",
    "exporter_id", "witness_id", "artifact_types",
    "consent_required", "consent_marker_required",
    "secret_bearing_artifact_policy",
    "public_artifact_policy",
    "gitignore_policy",
    "no_network_policy", "no_subprocess_policy",
    "no_audio_policy", "no_production_key_policy",
    "verification_order", "failure_states", "metadata",
)


def _new_id() -> str:
    return f"hcontract_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def get_supported_handoff_artifacts() -> list[str]:
    return list(_SUPPORTED_HANDOFF_ARTIFACTS)


def get_forbidden_handoff_fields() -> list[str]:
    return list(_FORBIDDEN_HANDOFF_FIELDS)


def get_secret_bearing_artifact_rules() -> dict[str, Any]:
    return {
        "allowed_locations": ["local_secret_handoff"],
        "forbidden_locations": [
            "reports", "public_descriptors",
            "witness_packages", "exporter_packets",
            "witness_inputs", "witness_outputs",
            "handshake_records", "integrity_manifests",
        ],
        "must_be_gitignored": True,
        "must_carry_consent_marker_hash": True,
        "must_be_test_only": True,
        "must_not_contain_production_label": True,
        "writes_require_explicit_allow_flag": True,
        "phase": _PHASE,
    }


def get_phase36_handoff_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "protocol_version": _PHASE,
        "supported_handoff_artifacts":
            list(_SUPPORTED_HANDOFF_ARTIFACTS),
        "required_contract_fields":
            list(_REQUIRED_CONTRACT_FIELDS),
        "forbidden_handoff_fields_in_public_artifacts":
            list(_FORBIDDEN_HANDOFF_FIELDS),
        "secret_bearing_artifact_rules":
            get_secret_bearing_artifact_rules(),
        "verification_order": list(_VERIFICATION_ORDER),
        "failure_states": list(_FAILURE_STATES),
        "notes": [
            "Phase 36 is test-only key handoff. Not production "
            "secret management.",
            "Secret-bearing envelopes live ONLY in "
            "local_secret_handoff (gitignored).",
            "Public reports, witness packages, public descriptors, "
            "manifests, exporter packets, and witness inputs/"
            "outputs MUST contain no secret material.",
            "No network. No subprocess. No multiprocessing. No "
            "OS keychain. No cloud KMS.",
        ],
    }


def create_handoff_contract(
    contract_id: Optional[str] = None,
    exporter_id: str = "local_exporter",
    witness_id: str = "local_witness",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    cid = str(contract_id or _new_id())
    return {
        "contract_id": cid,
        "created_at": time.time(),
        "protocol_version": _PHASE,
        "exporter_id": str(exporter_id or "local_exporter"),
        "witness_id": str(witness_id or "local_witness"),
        "artifact_types": list(_SUPPORTED_HANDOFF_ARTIFACTS),
        "consent_required": True,
        "consent_marker_required": True,
        "secret_bearing_artifact_policy":
            get_secret_bearing_artifact_rules(),
        "public_artifact_policy": {
            "forbidden_fields":
                list(_FORBIDDEN_HANDOFF_FIELDS),
            "secret_leakage_check_required": True,
            "fingerprint_only_in_public_descriptor": True,
        },
        "gitignore_policy": {
            "local_secret_handoff_folder":
                "bilingual_stack/voice_adapter_phase36/"
                "local_secret_handoff",
            "must_be_gitignored": True,
            "auto_gitignore_file_present": True,
            "cleanup_instructions": [
                "Rotate envelopes after each verification run.",
                "Delete sealed envelopes after use.",
                "Confirm `git status` shows no envelope files "
                "staged.",
            ],
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
        "no_production_key_policy": {
            "production_labels_rejected":
                ["prod", "production", "live", "real",
                 "kms", "cloud", "external"],
            "test_only_required": True,
            "os_keychain_disabled": True,
            "cloud_kms_disabled": True,
        },
        "verification_order": list(_VERIFICATION_ORDER),
        "failure_states": list(_FAILURE_STATES),
        "metadata": dict(metadata or {}),
        "phase": _PHASE,
    }


def validate_handoff_contract(contract: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(contract, dict):
        return {"ok": False, "reasons": ["contract_not_dict"]}
    for f in _REQUIRED_CONTRACT_FIELDS:
        if f not in contract:
            reasons.append(f"missing_field:{f}")
    if str(contract.get("protocol_version") or "") != _PHASE:
        reasons.append("wrong_protocol_version")
    if contract.get("consent_required") is not True:
        reasons.append("consent_required_must_be_true")
    if contract.get("consent_marker_required") is not True:
        reasons.append("consent_marker_required_must_be_true")
    if (contract.get("no_production_key_policy") or {}).get(
            "test_only_required") is not True:
        reasons.append("test_only_required_not_set")
    if (contract.get("gitignore_policy") or {}).get(
            "must_be_gitignored") is not True:
        reasons.append("gitignore_policy_missing")
    # Public-artifact forbidden fields must include all
    fb = ((contract.get("public_artifact_policy") or {})
          .get("forbidden_fields") or [])
    if not set(_FORBIDDEN_HANDOFF_FIELDS).issubset(set(fb)):
        reasons.append("public_artifact_forbidden_fields_incomplete")
    # Verification order
    vo = contract.get("verification_order") or []
    if vo != list(_VERIFICATION_ORDER):
        reasons.append("verification_order_mismatch")
    # No forbidden field present at the contract top level
    for k in contract.keys():
        if str(k).lower() in _FORBIDDEN_HANDOFF_FIELDS:
            reasons.append(f"forbidden_field_in_contract:{k}")
    try:
        json.dumps(contract, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase36_handoff_contract_report(
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
    "get_phase36_handoff_contract_schema",
    "create_handoff_contract",
    "validate_handoff_contract",
    "get_supported_handoff_artifacts",
    "get_forbidden_handoff_fields",
    "get_secret_bearing_artifact_rules",
    "write_phase36_handoff_contract_report",
]
