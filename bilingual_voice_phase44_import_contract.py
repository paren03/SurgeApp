"""Phase 44 - Cross-Machine Import Simulation Contract."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase44.import_contract.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "import_id", "source_phase",
    "required_import_artifacts",
    "forbidden_import_artifacts",
    "forbidden_actions",
    "rehearsal_dry_run_only",
    "adapter_invocation_forbidden",
    "production_db_read_forbidden",
)


_REQUIRED_IMPORT_ARTIFACTS = (
    "portable_bundle",
    "bundle_manifest",
    "source_operator_packet",
    "source_status_dashboard",
    "source_phase43_report",
    "import_manifest",
    "fresh_checkout_verification_result",
    "roundtrip_receipt",
    "operator_packet",
)


_FORBIDDEN_IMPORT_ARTIFACTS = (
    "runtime_dbs",
    "backups",
    "synthetic_corpora",
    "quality_samples",
    "pilot_imports",
    "checkpoints",
    "local_secret_handoff_contents",
    "audio_files",
    "corpus_incoming_files",
    "claude_directory_contents",
)


_FORBIDDEN_ACTIONS = (
    "adapter_invocation_on_import",
    "adapter_reinvocation_on_fresh_checkout",
    "new_adapter_invocation",
    "production_db_read_in_fresh_checkout",
    "generate_audio", "invoke_tts", "run_subprocess",
    "call_powershell", "call_sapi", "call_piper",
    "write_audio_file", "clone_voice", "network_call",
    "open_socket", "multiprocessing",
    "main_runtime_integration",
    "corpus_import",
    "production_db_modification",
    "production_signing_secret_storage",
    "git_commit_of_signing_secret",
    "program_s_modification",
    "tier_" + _PROBE_ATT + "_modification",
    "worker_or_" + _LUNA_MODS + "_modification",
    "raw_transcript_exposure",
    "sensitive_fact_exposure",
    "path_traversal",
    "url_scheme_path",
    "shell_metacharacter_path",
)


def get_phase44_import_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "required_import_artifacts":
            list(_REQUIRED_IMPORT_ARTIFACTS),
        "forbidden_import_artifacts":
            list(_FORBIDDEN_IMPORT_ARTIFACTS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "tamper_detection_required": True,
        "phase21_must_remain_blocked_unless_staged": True,
    }


def get_phase44_required_import_artifacts() -> list[str]:
    return list(_REQUIRED_IMPORT_ARTIFACTS)


def get_phase44_forbidden_import_artifacts() -> list[str]:
    return list(_FORBIDDEN_IMPORT_ARTIFACTS)


def get_phase44_forbidden_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_phase44_import_contract(
    import_id: str,
    source_phase: str = "phase43",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "contract_id": f"p44contract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "import_id": str(import_id or ""),
        "source_phase": str(source_phase or "phase43"),
        "metadata": dict(metadata or {}),
        "required_import_artifacts":
            list(_REQUIRED_IMPORT_ARTIFACTS),
        "forbidden_import_artifacts":
            list(_FORBIDDEN_IMPORT_ARTIFACTS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "adapter_invocation_forbidden": True,
        "production_db_read_forbidden": True,
        "tamper_detection_required": True,
        "notes": [
            "Cross-machine import is a LOCAL simulation: "
            "files copied via Python read/write only.",
            "Fresh-import verifier reads only the "
            "imported bundle.",
            "Phase 21 status carried, never unblocked.",
        ],
    }


def validate_phase44_import_contract(
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
    arts = contract.get("required_import_artifacts") or []
    for must in ("portable_bundle", "bundle_manifest",
                  "import_manifest",
                  "fresh_checkout_verification_result",
                  "roundtrip_receipt",
                  "operator_packet"):
        if must not in arts:
            reasons.append(f"missing_artifact:{must}")
    forb_arts = contract.get(
        "forbidden_import_artifacts") or []
    for must in ("runtime_dbs", "audio_files",
                  "local_secret_handoff_contents",
                  "corpus_incoming_files",
                  "claude_directory_contents"):
        if must not in forb_arts:
            reasons.append(
                f"missing_forbidden_artifact:{must}")
    forb = contract.get("forbidden_actions") or []
    for must in ("adapter_invocation_on_import",
                  "production_db_read_in_fresh_checkout",
                  "generate_audio", "run_subprocess",
                  "network_call", "multiprocessing",
                  "path_traversal",
                  "url_scheme_path",
                  "shell_metacharacter_path"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase44_import_contract_report(
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
    "get_phase44_import_contract_schema",
    "create_phase44_import_contract",
    "validate_phase44_import_contract",
    "get_phase44_required_import_artifacts",
    "get_phase44_forbidden_import_artifacts",
    "get_phase44_forbidden_actions",
    "write_phase44_import_contract_report",
]
