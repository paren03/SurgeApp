"""Phase 43 - Portability Contract.

Defines the portable witness bundle contract for Phase 42 audit
outputs. Read-only, JSON-serializable, no adapter re-invocation
on fresh checkout.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase43.portability_contract.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"


_REQUIRED_CONTRACT_FIELDS = (
    "contract_id", "created_at", "phase",
    "bundle_id", "source_phase",
    "required_bundle_artifacts",
    "excluded_artifact_patterns",
    "forbidden_actions",
    "rehearsal_dry_run_only",
    "fresh_checkout_no_adapter_reinvocation",
    "production_db_must_remain_unchanged",
)


_REQUIRED_BUNDLE_ARTIFACTS = (
    "phase42_audit_contract",
    "phase42_trace_batch",
    "phase42_coherence_audit",
    "phase42_replay_matrix",
    "phase42_drift_stability_matrix",
    "phase42_operator_packet",
    "phase42_operator_markdown",
    "phase42_report",
    "integrity_manifest",
    "portability_summary",
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
    "new_adapter_invocation",
    "adapter_reinvocation_on_fresh_checkout",
    "generate_audio", "invoke_tts", "run_subprocess",
    "call_powershell", "call_sapi", "call_piper",
    "write_audio_file", "clone_voice", "network_call",
    "open_socket", "multiprocessing",
    "main_runtime_integration",
    "production_db_modification",
    "production_db_read_in_fresh_checkout",
    "corpus_import",
    "production_signing_secret_storage",
    "git_commit_of_signing_secret",
    "program_s_modification",
    "tier_" + _PROBE_ATT + "_modification",
    "worker_or_" + _LUNA_MODS + "_modification",
    "raw_transcript_exposure",
    "sensitive_fact_exposure",
)


def get_phase43_portability_contract_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_CONTRACT_FIELDS),
        "required_bundle_artifacts":
            list(_REQUIRED_BUNDLE_ARTIFACTS),
        "excluded_artifact_patterns":
            list(_EXCLUDED_PATTERNS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "fresh_checkout_no_adapter_reinvocation": True,
        "production_db_must_remain_unchanged": True,
        "production_db_must_not_be_read_in_fresh_checkout":
            True,
        "max_artifact_inline_bytes": 512 * 1024,
    }


def get_phase43_required_bundle_artifacts() -> list[str]:
    return list(_REQUIRED_BUNDLE_ARTIFACTS)


def get_phase43_excluded_artifact_patterns() -> list[str]:
    return list(_EXCLUDED_PATTERNS)


def get_phase43_forbidden_actions() -> list[str]:
    return list(_FORBIDDEN_ACTIONS)


def create_phase43_portability_contract(
    bundle_id: str,
    source_phase: str = "phase42",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "contract_id": f"p43contract_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "bundle_id": str(bundle_id or ""),
        "source_phase": str(source_phase or "phase42"),
        "metadata": dict(metadata or {}),
        "required_bundle_artifacts":
            list(_REQUIRED_BUNDLE_ARTIFACTS),
        "excluded_artifact_patterns":
            list(_EXCLUDED_PATTERNS),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "rehearsal_dry_run_only": True,
        "fresh_checkout_no_adapter_reinvocation": True,
        "production_db_must_remain_unchanged": True,
        "notes": [
            "Bundle is portable JSON/Markdown only.",
            "Fresh checkout MUST NOT read production DBs.",
            "Fresh checkout MUST NOT invoke any adapter.",
            "Excluded artifacts cover runtime DBs, "
            "secrets, audio, corpus incoming, and "
            ".claude/.",
        ],
    }


def validate_phase43_portability_contract(
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
            "fresh_checkout_no_adapter_reinvocation"
            ) is not True:
        reasons.append(
            "fresh_checkout_no_reinvoke_must_be_true")
    arts = contract.get("required_bundle_artifacts") or []
    for must in ("phase42_operator_packet",
                  "phase42_replay_matrix",
                  "phase42_coherence_audit",
                  "integrity_manifest"):
        if must not in arts:
            reasons.append(f"missing_artifact:{must}")
    excl = contract.get("excluded_artifact_patterns") or []
    for must in ("*.sqlite", "*.wav", "*.mp3",
                  "local_secret_handoff/",
                  "backups/", ".claude/"):
        if must not in excl:
            reasons.append(f"missing_exclusion:{must}")
    forb = contract.get("forbidden_actions") or []
    for must in ("new_adapter_invocation",
                  "adapter_reinvocation_on_fresh_checkout",
                  "generate_audio", "run_subprocess",
                  "network_call",
                  "production_db_read_in_fresh_checkout"):
        if must not in forb:
            reasons.append(f"missing_forbidden:{must}")
    return {"ok": not reasons, "reasons": reasons}


def write_phase43_portability_contract_report(
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
    "get_phase43_portability_contract_schema",
    "create_phase43_portability_contract",
    "validate_phase43_portability_contract",
    "get_phase43_required_bundle_artifacts",
    "get_phase43_excluded_artifact_patterns",
    "get_phase43_forbidden_actions",
    "write_phase43_portability_contract_report",
]
