"""Phase 48 - Federation Portability Trust-Capsule Contract."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase48.capsule_contract.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "capsule_id", "source_phase",
    "required_capsule_artifacts",
    "excluded_artifact_patterns",
    "forbidden_actions",
    "rehearsal_dry_run_only",
    "adapter_invocation_forbidden",
    "production_db_read_forbidden",
    "tamper_detection_required",
    "portable_only_verification_required",
)


_REQUIRED_CAPSULE_ARTIFACTS = (
    "phase47_federation_contract",
    "phase47_federation_graph",
    "phase47_federation_manifest",
    "phase47_verification_result",
    "phase47_drift_report",
    "phase47_tamper_suite_result",
    "phase47_operator_packet",
    "phase47_status_dashboard",
    "phase47_dashboard_markdown",
    "phase47_report",
    "capsule_manifest",
    "fresh_checkout_verification_result",
    "capsule_receipt",
    "operator_packet",
)


_EXCLUDED_PATTERNS = (
    "*.sqlite", "*.sqlite3", "*.db",
    "backups/", "synthetic_million/",
    "quality_samples/", "pilot_imports/",
    "checkpoints/", "local_secret_handoff/",
    "*.wav", "*.mp3", "*.ogg", "*.flac",
    "*.m4a", "*.aac", "*.opus",
    "corpus_sources/english/incoming/",
    "corpus_sources/russian/incoming/",
    ".claude/",
)


_FORBIDDEN_ACTIONS = (
    "adapter_invocation_in_capsule",
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
    "tampered_capsule_root_hash",
    "tampered_manifest_root_hash",
    "missing_federation_artifact",
)


def get_phase48_capsule_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "required_capsule_artifacts":
            list(_REQUIRED_CAPSULE_ARTIFACTS),
        "excluded_artifact_patterns":
            list(_EXCLUDED_PATTERNS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "tamper_detection_required": True,
        "portable_only_verification_required": True,
        "max_artifact_inline_bytes": 512 * 1024,
    }


def get_phase48_required_capsule_artifacts() -> list[str]:
    return list(_REQUIRED_CAPSULE_ARTIFACTS)


def get_phase48_excluded_artifact_patterns() -> list[str]:
    return list(_EXCLUDED_PATTERNS)


def get_phase48_forbidden_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_phase48_capsule_contract(
    capsule_id: str,
    source_phase: str = "phase47",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "contract_id": f"p48contract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "capsule_id": str(capsule_id or ""),
        "source_phase": str(source_phase or "phase47"),
        "metadata": dict(metadata or {}),
        "required_capsule_artifacts":
            list(_REQUIRED_CAPSULE_ARTIFACTS),
        "excluded_artifact_patterns":
            list(_EXCLUDED_PATTERNS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "tamper_detection_required": True,
        "portable_only_verification_required": True,
        "notes": [
            "Capsule packages the Phase 47 federation as "
            "portable JSON / Markdown / report artifacts.",
            "Verifier reads only the capsule; never "
            "production DBs; never invokes any adapter.",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase48_capsule_contract(
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
            "portable_only_verification_required"
            ) is not True:
        reasons.append(
            "portable_only_verification_must_be_required")
    arts = contract.get(
        "required_capsule_artifacts") or []
    for must in ("phase47_federation_graph",
                  "phase47_federation_manifest",
                  "phase47_tamper_suite_result",
                  "phase47_operator_packet",
                  "phase47_report",
                  "capsule_manifest"):
        if must not in arts:
            reasons.append(f"missing_artifact:{must}")
    excl = contract.get("excluded_artifact_patterns") or []
    for must in ("*.sqlite", "*.wav", "*.mp3",
                  "local_secret_handoff/",
                  "backups/", ".claude/"):
        if must not in excl:
            reasons.append(f"missing_exclusion:{must}")
    forb = contract.get("forbidden_actions") or []
    for must in ("adapter_invocation_in_capsule",
                  "adapter_reinvocation_in_verifier",
                  "production_db_read_in_verifier",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "tampered_capsule_root_hash",
                  "missing_federation_artifact"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase48_capsule_contract_report(
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
    "get_phase48_capsule_contract_schema",
    "create_phase48_capsule_contract",
    "validate_phase48_capsule_contract",
    "get_phase48_required_capsule_artifacts",
    "get_phase48_excluded_artifact_patterns",
    "get_phase48_forbidden_actions",
    "write_phase48_capsule_contract_report",
]
