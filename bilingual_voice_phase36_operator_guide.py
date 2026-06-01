"""Phase 36 - Operator Handoff Guide.

Generates an operator-readable guide for safe local test-key
handoff. No network, no subprocess, no production secret handling.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase36.operator_handoff_guide.v1"


def create_handoff_creation_steps() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "role": "creator",
        "steps": [
            {"step": 1, "title": "Generate a fresh test key",
              "detail": (
                  "Use Phase 32 create_test_signing_key with a "
                  "label that does NOT contain prod / production "
                  "/ live / real / kms / cloud / external.")},
            {"step": 2, "title": "Choose a consent marker",
              "detail": (
                  "Pick a fresh consent marker string for this "
                  "handoff. It will be stored only as a SHA-256 "
                  "hash inside the envelope.")},
            {"step": 3, "title": "Create the handoff envelope",
              "detail": (
                  "Call create_key_handoff_envelope(key, "
                  "consent_marker). It returns a sealed envelope "
                  "with sealed_payload and public_fingerprint.")},
            {"step": 4, "title": "Write the envelope safely",
              "detail": (
                  "Call write_key_handoff_envelope(env, path, "
                  "allow_secret_write=True). The path MUST be "
                  "inside bilingual_stack/voice_adapter_phase36/"
                  "local_secret_handoff/. Writes outside that "
                  "folder are refused.")},
            {"step": 5, "title": "Confirm gitignore",
              "detail": (
                  "Inspect bilingual_stack/voice_adapter_phase36/"
                  "local_secret_handoff/.gitignore — it should "
                  "exclude everything except itself. Run "
                  "`git status` to confirm no envelope file is "
                  "staged.")},
            {"step": 6, "title": "Hand the envelope to the verifier",
              "detail": (
                  "An operator may copy the envelope file by hand "
                  "to a verifier workspace. Luna does not "
                  "transmit it."),
              },
        ],
    }


def create_handoff_verification_steps() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "role": "verifier",
        "steps": [
            {"step": 1, "title": "Receive the envelope file",
              "detail": (
                  "Place it under local_secret_handoff in your own "
                  "Phase 36 workspace.")},
            {"step": 2, "title": "Hold the consent marker",
              "detail": (
                  "The consent marker must come out-of-band from "
                  "the operator. Never write it into a report.")},
            {"step": 3, "title": "Unseal in memory",
              "detail": (
                  "Call unseal_key_handoff_envelope(env, "
                  "consent_marker). The unsealed key descriptor "
                  "stays in memory only.")},
            {"step": 4, "title": "Verify Phase 32/33 evidence",
              "detail": (
                  "Use verify_with_handoff_envelope to verify a "
                  "signed evidence chain, or "
                  "verify_witness_package_with_handoff for a "
                  "Phase 34 witness package, or "
                  "verify_exchange_packet_with_handoff for a "
                  "Phase 35 exporter packet.")},
            {"step": 5, "title": "Discard the unsealed key",
              "detail": (
                  "Discard the in-memory key descriptor after "
                  "verification. Do NOT write it to disk."),
              },
        ],
    }


def create_cleanup_rotation_steps() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "role": "cleanup",
        "steps": [
            {"step": 1,
              "title": "Delete the sealed envelope file",
              "detail": "Remove the envelope from "
                        "local_secret_handoff."},
            {"step": 2,
              "title": "Rotate the test key",
              "detail": "Generate a fresh test key for the next "
                        "handoff. Do not reuse the prior key."},
            {"step": 3,
              "title": "Pick a fresh consent marker",
              "detail": "Never reuse a consent marker across "
                        "handoffs."},
            {"step": 4,
              "title": "Audit the public artifacts",
              "detail": "Run validate_no_secret_leakage_in_directory "
                        "over reports / public_descriptors / "
                        "witness_packages folders."},
        ],
    }


def create_gitignore_safety_steps() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "role": "gitignore_safety",
        "steps": [
            {"step": 1, "title": "Verify .gitignore presence",
              "detail": (
                  "bilingual_stack/voice_adapter_phase36/"
                  "local_secret_handoff/.gitignore must exist and "
                  "exclude everything except itself.")},
            {"step": 2, "title": "Confirm git status",
              "detail": (
                  "Run `git status --short` and confirm no "
                  "envelope file appears as staged or modified.")},
            {"step": 3, "title": "Refuse to commit envelopes",
              "detail": (
                  "If an envelope ever appears in `git add` "
                  "output, unstage immediately and reseat the "
                  ".gitignore."),
              },
        ],
    }


def create_failure_interpretation_steps() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "failure_modes": [
            {"code": "consent_marker_missing",
              "meaning": "Verifier did not supply a marker.",
              "action": "Obtain the marker out-of-band."},
            {"code": "consent_marker_mismatch",
              "meaning": "Marker hash does not match the envelope.",
              "action": "Reject; do not retry without operator "
                        "confirmation."},
            {"code": "envelope_not_test_only",
              "meaning": "Envelope claims production scope.",
              "action": "Reject; Phase 36 is test-only."},
            {"code": "forbidden_label",
              "meaning": "Envelope key_label contains a "
                        "production-shape token.",
              "action": "Reject; choose a test-only label."},
            {"code": "tampered_envelope",
              "meaning": "Envelope sealed_payload has been "
                        "modified.",
              "action": "Reject; request a fresh envelope."},
            {"code": "secret_leaked_in_public_artifact",
              "meaning": "A report / public descriptor / witness "
                        "package contains a secret field.",
              "action": "Stop; rotate keys; investigate source."},
            {"code": "secret_artifact_in_unsafe_path",
              "meaning": "Sealed envelope was written outside "
                        "local_secret_handoff.",
              "action": "Move or delete; confirm gitignore."},
        ],
    }


def create_phase36_operator_handoff_guide() -> dict[str, Any]:
    return {
        "guide_id": f"p36opguide_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "title": ("Phase 36 Operator Handoff Guide: "
                  "Local test-only key handoff for offline "
                  "verification"),
        "intent": [
            "Allow an operator to ship a local sealed envelope "
            "carrying a test HMAC-SHA256 verification key, with "
            "explicit consent marker, to a separate local "
            "verification workspace.",
            "Phase 36 is TEST-ONLY and does NOT manage production "
            "secrets.",
        ],
        "boundary_explanation": {
            "phase": _PHASE,
            "statements": [
                "This is test-only key handoff. NOT production "
                "secret management.",
                "Sealed envelopes live only in local_secret_handoff.",
                "Reports / public_descriptors / witness_packages / "
                "manifests / exporter_packets / witness_inputs/"
                "outputs MUST contain no secret material.",
                "No network. No subprocess. No multiprocessing.",
                "No OS keychain integration. No cloud KMS.",
                "Production / live / real / kms / cloud / external "
                "labels are rejected at envelope creation.",
                "Phase 21 real corpus import is a separate "
                "workflow.",
                "No audio. No TTS. No voice cloning.",
            ],
        },
        "creator_role": create_handoff_creation_steps(),
        "verifier_role": create_handoff_verification_steps(),
        "cleanup_rotation": create_cleanup_rotation_steps(),
        "gitignore_safety": create_gitignore_safety_steps(),
        "failure_interpretation":
            create_failure_interpretation_steps(),
        "caveats": [
            "This is NOT real voice execution.",
            "Phase 21 real corpus import is separate.",
            "Witness packages still ship NO secrets; only the "
            "Phase 36 envelope carries sealed material, and only "
            "under local_secret_handoff.",
            "Sealing is DETERMINISTIC LOCAL TEST WRAPPING; do "
            "not treat it as production encryption.",
        ],
    }


def write_phase36_operator_handoff_guide(
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
    "create_phase36_operator_handoff_guide",
    "create_handoff_creation_steps",
    "create_handoff_verification_steps",
    "create_cleanup_rotation_steps",
    "create_gitignore_safety_steps",
    "create_failure_interpretation_steps",
    "write_phase36_operator_handoff_guide",
]
