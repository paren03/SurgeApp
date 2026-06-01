"""Phase 38 - Rollback Matrix.

Per-phase guidance for what to delete to roll back Phase 27-37
cleanly. Guidance only — no destructive commands are auto-executed.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase38.rollback_matrix.v1"


def create_phase_file_groups() -> dict[int, dict[str, Any]]:
    return {
        27: {
            "title": "Voice-Render Adapter Skeleton",
            "files_created": [
                "bilingual_voice_adapter_contract.py",
                "bilingual_voice_adapter_policy.py",
                "bilingual_voice_adapter_registry.py",
                "bilingual_piper_adapter_contract.py",
                "bilingual_sapi_adapter_contract.py",
                "bilingual_voice_dry_run_pipeline.py",
                "bilingual_voice_adapter_validation.py",
                "test_phase27_voice_render_adapter_skeleton.py",
                "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folder tree. Prior phases (Phase 25/26) "
                "remain green without Phase 27.",
            "side_effects": "none",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "none",
        },
        28: {
            "title": "Operator-Gated Voice Adapter (dry-run)",
            "files_created": [
                "bilingual_voice_operator_consent.py",
                "bilingual_voice_adapter_audit_log.py",
                "bilingual_voice_call_envelope.py",
                "bilingual_voice_execution_boundary.py",
                "bilingual_voice_capability_negotiator.py",
                "bilingual_voice_adapter_errors.py",
                "bilingual_voice_adapter_phase28_runtime.py",
                "test_phase28_operator_gated_voice_adapter.py",
                "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase28/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folders.",
            "side_effects": "none",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "none",
        },
        29: {
            "title": "Per-Invocation Consent + Tamper-Evident "
                      "Audit Chain (Phase B)",
            "files_created": [
                "bilingual_voice_invocation_consent.py",
                "bilingual_voice_audit_chain.py",
                "bilingual_voice_calltime_boundary.py",
                "bilingual_voice_operator_review_packet.py",
                "bilingual_voice_dry_run_queue.py",
                "bilingual_voice_refusal_analytics.py",
                "bilingual_voice_adapter_phase29_runtime.py",
                "test_phase29_operator_gated_runtime_adapter_b.py",
                "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase29/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folders.",
            "side_effects": "none",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "none",
        },
        30: {
            "title": "First Callable Boundary (dummy adapter)",
            "files_created": [
                "bilingual_voice_callable_adapter_interface.py",
                "bilingual_voice_dummy_metadata_adapter.py",
                "bilingual_voice_emergency_kill_switch.py",
                "bilingual_voice_pre_call_validator.py",
                "bilingual_voice_post_call_validator.py",
                "bilingual_voice_invocation_receipt.py",
                "bilingual_voice_adapter_phase30_runtime.py",
                "test_phase30_callable_adapter_boundary.py",
                "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase30/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folders. Phase 31+ become unusable but the "
                "earlier phases stay green.",
            "side_effects": "Phase 31/33/37 lose their dummy "
                              "adapter dependency.",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "Phase 31/33/37 depend on Phase 30 "
                        "interface.",
        },
        31: {
            "title": "Two Metadata-Only Adapters (Phase D)",
            "files_created": [
                "bilingual_voice_phase31_adapter_interface.py",
                "bilingual_segment_metadata_adapter.py",
                "bilingual_voice_phase31_selection_policy.py",
                "bilingual_voice_phase31_adapter_comparison.py",
                "bilingual_voice_phase31_selection_receipt.py",
                "bilingual_voice_phase31_post_call_equivalence.py",
                "bilingual_voice_adapter_phase31_runtime.py",
                "test_phase31_multi_adapter_boundary.py",
                "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase31/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folders.",
            "side_effects": "Phase 33/37 lose bilingual_segment "
                              "adapter.",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "Phase 33/37 depend on Phase 31 "
                        "bilingual_segment adapter.",
        },
        32: {
            "title": "Audit Signing + Evidence Bundle",
            "files_created": [
                "bilingual_voice_audit_signing_policy.py",
                "bilingual_voice_audit_chain_signer.py",
                "bilingual_voice_receipt_verifier.py",
                "bilingual_voice_report_integrity_manifest.py",
                "bilingual_voice_evidence_bundle.py",
                "bilingual_voice_governance_verifier.py",
                "bilingual_voice_verification_cli.py",
                "test_phase32_audit_signing_and_verification.py",
                "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase32/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folders.",
            "side_effects": "Phase 33-37 lose signing.",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "Phase 33-37 depend on Phase 32 signing.",
        },
        33: {
            "title": "Three Metadata-Only Adapters + Signed "
                      "Evidence Default",
            "files_created": [
                "bilingual_voice_phase33_adapter_interface.py",
                "bilingual_prosody_density_metadata_adapter.py",
                "bilingual_voice_phase33_selection_policy.py",
                "bilingual_voice_phase33_signed_evidence.py",
                "bilingual_voice_phase33_governance_recheck.py",
                "bilingual_voice_phase33_result_verifier.py",
                "bilingual_voice_adapter_phase33_runtime.py",
                "test_phase33_three_adapter_signed_governance.py",
                "PHASE33_THREE_ADAPTER_SIGNED_GOVERNANCE_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase33/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folders.",
            "side_effects": "Phase 37 loses prosody_density "
                              "adapter.",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "Phase 37 depends on Phase 33 prosody "
                        "adapter.",
        },
        34: {
            "title": "External Witness Verification",
            "files_created": [
                "bilingual_voice_phase34_witness_package.py",
                "bilingual_voice_phase34_offline_verifier.py",
                "bilingual_voice_phase34_key_descriptor_export.py",
                "bilingual_voice_phase34_operator_guide.py",
                "bilingual_voice_phase34_witness_receipt.py",
                "bilingual_voice_phase34_witness_governance.py",
                "bilingual_voice_phase34_export_runtime.py",
                "test_phase34_external_witness_verification.py",
                "PHASE34_EXTERNAL_WITNESS_VERIFICATION_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase34/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folders.",
            "side_effects": "Phase 35/36/37 lose witness export.",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "Phase 35/36/37 depend on Phase 34.",
        },
        35: {
            "title": "Local File-Based Witness Exchange Protocol",
            "files_created": [
                "bilingual_voice_phase35_exchange_contract.py",
                "bilingual_voice_phase35_exporter_packet.py",
                "bilingual_voice_phase35_witness_input.py",
                "bilingual_voice_phase35_witness_verifier.py",
                "bilingual_voice_phase35_handshake_record.py",
                "bilingual_voice_phase35_operator_exchange_guide.py",
                "bilingual_voice_phase35_exchange_runtime.py",
                "test_phase35_witness_exchange_protocol.py",
                "PHASE35_WITNESS_EXCHANGE_PROTOCOL_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase35/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the empty "
                "sub-folders.",
            "side_effects": "Phase 37 pipeline loses exchange "
                              "summary.",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "Phase 37 depends on Phase 35 exchange.",
        },
        36: {
            "title": "Out-of-Band Test-Key Handoff Envelope",
            "files_created": [
                "bilingual_voice_phase36_handoff_contract.py",
                "bilingual_voice_phase36_key_handoff_envelope.py",
                "bilingual_voice_phase36_secret_boundary.py",
                "bilingual_voice_phase36_public_descriptor_bridge.py",
                "bilingual_voice_phase36_handoff_verifier.py",
                "bilingual_voice_phase36_operator_guide.py",
                "bilingual_voice_phase36_handoff_runtime.py",
                "test_phase36_key_handoff_envelope.py",
                "PHASE36_KEY_HANDOFF_ENVELOPE_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase36/",
                "bilingual_stack/voice_adapter_phase36/"
                "local_secret_handoff/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the 12 sub-"
                "folders (including the gitignored "
                "local_secret_handoff sub-folder). Confirm "
                "`git status` shows no envelope files staged.",
            "side_effects": "Phase 37 pipeline loses optional "
                              "handoff path.",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "Phase 37 references Phase 36 modules; "
                        "remove Phase 37 first if rolling back "
                        "both.",
        },
        37: {
            "title": "Fourth Metadata-Only Callable + Signed "
                      "Witness Pipeline",
            "files_created": [
                "bilingual_voice_phase37_adapter_interface.py",
                "bilingual_safety_redaction_trace_adapter.py",
                "bilingual_voice_phase37_selection_policy.py",
                "bilingual_voice_phase37_signed_witness_pipeline.py",
                "bilingual_voice_phase37_governance_recheck.py",
                "bilingual_voice_phase37_result_verifier.py",
                "bilingual_voice_adapter_phase37_runtime.py",
                "test_phase37_safety_trace_adapter_governance.py",
                "PHASE37_SAFETY_TRACE_ADAPTER_GOVERNANCE_REPORT.md",
            ],
            "folders_created": [
                "bilingual_stack/voice_adapter_phase37/",
            ],
            "files_modified": [],
            "rollback_guidance":
                "Delete the 9 listed files and the 11 sub-"
                "folders. Phase 30-36 remain green without "
                "Phase 37.",
            "side_effects": "none",
            "db_impact": "zero",
            "prior_phases_green_without_it": True,
            "caution": "none",
        },
    }


def create_rollback_matrix() -> dict[str, Any]:
    return {
        "matrix_id": f"rbmtx_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "title": "Phase 27-37 Rollback Matrix",
        "general_guidance": (
            "Each phase rolls back by deleting the file group "
            "and the empty sub-folder set. No prior phase "
            "depends on any later phase. No destructive "
            "commands are auto-executed by this module."),
        "phases": create_phase_file_groups(),
        "auto_destructive_commands_executed": False,
        "manual_command_examples": [
            "# Inspect first; do not run blindly.",
            "# git status --short",
            "# rm -i <file>",
            "# rmdir -p <empty_subfolder>",
        ],
    }


def validate_rollback_matrix(matrix: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(matrix, dict):
        return {"ok": False, "reasons": ["matrix_not_dict"]}
    for f in ("matrix_id", "created_at", "phase",
              "general_guidance", "phases",
              "auto_destructive_commands_executed"):
        if f not in matrix:
            reasons.append(f"missing_field:{f}")
    if matrix.get("auto_destructive_commands_executed") is not False:
        reasons.append("auto_destructive_must_be_false")
    phases = matrix.get("phases") or {}
    if not isinstance(phases, dict):
        reasons.append("phases_not_dict")
    else:
        for p in range(27, 38):
            if p not in phases and str(p) not in phases:
                reasons.append(f"missing_phase:{p}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_rollback_steps(matrix: Any) -> dict[str, Any]:
    if not isinstance(matrix, dict):
        return {"ok": False, "summary": "no_matrix"}
    phases = matrix.get("phases") or {}
    total_files = 0
    for p, entry in phases.items():
        if isinstance(entry, dict):
            total_files += len(entry.get("files_created") or [])
    return {
        "ok": True,
        "summary": (
            f"phase38 rollback matrix: phases={len(phases)} "
            f"files_documented={total_files} "
            f"auto_destructive=False"),
        "matrix_id": matrix.get("matrix_id"),
        "phase": _PHASE,
    }


def write_rollback_matrix(
    matrix: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(matrix)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_rollback_matrix",
    "validate_rollback_matrix",
    "summarize_rollback_steps",
    "create_phase_file_groups",
    "write_rollback_matrix",
]
