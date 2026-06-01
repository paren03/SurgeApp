"""Phase 34 - Operator Verification Guide.

Generates operator-readable offline verification instructions for the
witness package + public key descriptor.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase34.operator_guide.v1"


def create_boundary_explanation() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "what_the_boundary_means": [
            "No audio bytes are generated anywhere in Phase 27-34.",
            "No TTS engine is invoked. No Piper. No SAPI.",
            "No subprocess is spawned. No PowerShell. No external "
            "renderer.",
            "No network call is made by the adapter governance "
            "stack.",
            "No voice cloning. No voice samples shipped.",
            "Phase 30 still allows only one callable: "
            "dummy_metadata_adapter.",
            "Phase 31 still allows exactly two metadata-only "
            "callables.",
            "Phase 33 still allows exactly three metadata-only "
            "callables.",
            "Phase 34 emits witness packages without secret material.",
        ],
        "what_signed_evidence_proves": [
            "The audit chain was hash-linked and not tampered with.",
            "The HMAC-SHA256 signature over the chain matches the "
            "public/test key descriptor's fingerprint.",
            "The selected adapter was one of the allowed "
            "metadata-only adapters.",
            "All execution-shape flags in the result are False.",
        ],
        "what_signed_evidence_does_not_prove": [
            "It does NOT prove a real voice was synthesized.",
            "It does NOT prove anyone listened to audio.",
            "It does NOT prove Luna's main runtime ran.",
            "It does NOT prove Phase 21 real corpus import "
            "happened (it remains operator-staged and blocked).",
        ],
    }


def create_failure_interpretation_guide() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "failure_modes": [
            {"code": "signed_evidence_failed",
              "meaning": "Audit-chain signature or hash chain is "
              "broken or wrong key was used."},
            {"code": "integrity_manifest_failed",
              "meaning": "One or more reports have changed since "
              "manifest creation."},
            {"code": "boundary_summary_failed",
              "meaning": "A boundary flag indicates execution or "
              "the witness package leaked a forbidden field."},
            {"code": "governance_summary_failed",
              "meaning": "Phase 30/31/33 boundary or required "
              "signed evidence check failed."},
            {"code": "no_secret_leakage_failed",
              "meaning": "Private key, secret, material_hex, or "
              "signing_key_material appeared in the package."},
            {"code": "public_key_descriptor_failed",
              "meaning": "The supplied key descriptor is not a "
              "valid public/test descriptor."},
        ],
        "if_failed": [
            "Stop. Do not bind any audio runtime.",
            "Re-export the witness package from the source process.",
            "Verify the public key descriptor matches the signing "
            "fingerprint.",
            "Re-run Phase 33 invocation and re-export.",
        ],
    }


def create_step_by_step_verification_instructions(
    package_path: str = "",
    key_descriptor_path: str = "",
) -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "steps": [
            {"step": 1,
              "title": "Load the public key descriptor",
              "detail": (
                  f"Read JSON from {key_descriptor_path or '<key>'}; "
                  "confirm test_only=True, algorithm=HMAC-SHA256, "
                  "label does not contain prod/production/live/real/"
                  "kms/cloud/external."),
              },
            {"step": 2,
              "title": "Load the witness package",
              "detail": (
                  f"Read JSON from {package_path or '<package>'}; "
                  "confirm package_id, signed_evidence_payload, "
                  "boundary_summary."),
              },
            {"step": 3,
              "title": "Verify the report integrity manifest",
              "detail": (
                  "For each entry in report_integrity_manifest, "
                  "re-hash the file with SHA-256 (streaming) and "
                  "compare to the recorded sha256."),
              },
            {"step": 4,
              "title": "Verify the signed audit chain",
              "detail": (
                  "Use the public key descriptor with the Phase 32 "
                  "audit-chain signer's verify function to "
                  "re-verify the signed_audit_chain inside "
                  "signed_evidence_payload."),
              },
            {"step": 5,
              "title": "Verify boundary + governance summaries",
              "detail": (
                  "Confirm execution_blocked=True, dry_run=True, "
                  "phase30_strict=True, phase31_two_adapter_"
                  "boundary=True, phase33_three_adapter_"
                  "boundary=True, signed_evidence_required=True."),
              },
            {"step": 6,
              "title": "Verify no secret / audio / command field",
              "detail": (
                  "Walk the entire package tree and assert no key "
                  "named private_key, secret, material_hex, "
                  "signing_key_material, audio_bytes, audio_path, "
                  "command, shell, powershell_command, etc."),
              },
            {"step": 7,
              "title": "Produce a witness receipt",
              "detail": (
                  "Capture the structured pass/fail/warn result "
                  "into a witness_receipt and store it locally."),
              },
        ],
    }


def create_operator_verification_guide() -> dict[str, Any]:
    return {
        "guide_id": f"opguide_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "title": (
            "Phase 34 Operator Verification Guide: "
            "Verifying Luna voice adapter governance evidence"),
        "intent": [
            "Let an operator confirm Phase 33 evidence is valid, "
            "unchanged, and within the no-audio / no-subprocess "
            "boundary.",
            "All checks are local. No internet. No subprocess.",
        ],
        "boundary_explanation": create_boundary_explanation(),
        "step_by_step":
            create_step_by_step_verification_instructions(),
        "failure_interpretation":
            create_failure_interpretation_guide(),
        "caveats": [
            "This procedure is not real voice execution.",
            "Phase 21 real corpus import is a separate workflow "
            "and remains operator-staged.",
            "Signing keys are test-only stdlib HMAC-SHA256 keys; "
            "no production secret is involved.",
        ],
        "next_safe_actions": [
            "Review the demo witness exports in the demos folder.",
            "If signature verification passes, the operator may "
            "proceed to consider further phases that BIND a real "
            "runtime adapter, but those phases are NOT part of "
            "Phase 34.",
        ],
    }


def write_operator_verification_guide(
    guide: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(guide)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_operator_verification_guide",
    "create_step_by_step_verification_instructions",
    "create_boundary_explanation",
    "create_failure_interpretation_guide",
    "write_operator_verification_guide",
]
