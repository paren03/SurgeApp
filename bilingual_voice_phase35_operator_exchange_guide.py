"""Phase 35 - Operator Exchange Guide.

Generates operator instructions for local file-based witness exchange.
No network, no subprocess, no real second process spawned.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase35.operator_exchange_guide.v1"


def create_security_boundary_explanation() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "boundary_statements": [
            "Phase 35 is local file-based exchange only.",
            "No network transfer happens.",
            "No subprocess is spawned by Luna.",
            "No multiprocessing worker is spawned by Luna.",
            "No real second process is required.",
            "An operator may manually copy the exporter packet "
            "files to a different folder or machine, but Luna's "
            "code does not transfer them.",
            "Phase 34 still ships no signing secrets.",
            "Phase 30/31/33 adapter boundaries still hold.",
            "Phase 21 real corpus import remains an entirely "
            "separate workflow.",
        ],
    }


def create_exporter_steps() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "role": "exporter",
        "steps": [
            {"step": 1, "title": "Run Phase 34 export",
              "detail": (
                  "Use create_phase34_witness_export to produce a "
                  "witness package + public key descriptor + "
                  "integrity manifest, all on local disk.")},
            {"step": 2, "title": "Build exchange contract",
              "detail": (
                  "Use create_exchange_contract to produce a "
                  "JSON-serializable contract listing required "
                  "artifacts and forbidden fields.")},
            {"step": 3, "title": "Build exporter packet",
              "detail": (
                  "Use create_exporter_packet with the local "
                  "paths to the package + key + manifest + "
                  "optional operator guide. SHA-256 hashes are "
                  "computed by streaming the files.")},
            {"step": 4, "title": "Write exporter packet",
              "detail": (
                  "Write the packet JSON to a local folder; this "
                  "is what the operator hands to the witness "
                  "verifier process (or to themselves).")},
            {"step": 5, "title": "No further action by exporter",
              "detail": (
                  "Exporter does not run subprocess, network, or "
                  "any audio code. The exporter packet is "
                  "complete.")},
        ],
    }


def create_witness_steps() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "role": "witness",
        "steps": [
            {"step": 1, "title": "Load exporter packet",
              "detail": "Read the JSON file with bounded size."},
            {"step": 2, "title": "Build witness input",
              "detail": (
                  "Use create_witness_input to produce a bounded "
                  "verification input. Remote/URL/command-like "
                  "paths are rejected here.")},
            {"step": 3, "title": "Verify input",
              "detail": (
                  "Run verify_witness_input. This re-hashes each "
                  "artifact, validates the witness package via "
                  "Phase 34 offline verifier, validates the "
                  "public key descriptor, and re-verifies the "
                  "integrity manifest.")},
            {"step": 4, "title": "Inspect witness output",
              "detail": (
                  "Witness output contains structured "
                  "pass/fail/warn for every check.")},
            {"step": 5, "title": "Record handshake",
              "detail": (
                  "Use create_handshake_record. It captures "
                  "exchange_id, artifact hash summary, and "
                  "replay-protection flags.")},
        ],
    }


def create_failure_handling_steps() -> dict[str, Any]:
    return {
        "phase": _PHASE,
        "failure_modes": [
            {"code": "artifact_hash_mismatch",
              "meaning": "A file was changed after export.",
              "action": "Re-export and re-verify; do NOT bind any "
              "audio runtime."},
            {"code": "remote_or_url_scheme",
              "meaning": "A path looked remote or scheme-prefixed.",
              "action": "Replace with a local path; Phase 35 is "
              "local-only."},
            {"code": "shell_metacharacter",
              "meaning": "A path contains shell-special characters.",
              "action": "Rename the file so the path is plain."},
            {"code": "size_limit_exceeded",
              "meaning": "Artifact exceeds the bounded-read limit.",
              "action": "Investigate why the artifact grew; do not "
              "raise the cap blindly."},
            {"code": "boundary_violation",
              "meaning": "Witness package or exporter packet "
              "indicates audio/subprocess/network/secret.",
              "action": "Stop; investigate the source process."},
            {"code": "replay_exchange_id",
              "meaning": "The exchange_id has been used before.",
              "action": "Issue a fresh exchange_id; do not reuse "
              "evidence."},
        ],
    }


def create_phase35_operator_exchange_guide() -> dict[str, Any]:
    return {
        "guide_id": f"p35opguide_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "title": ("Phase 35 Operator Exchange Guide: "
                  "Local file-based witness verification"),
        "intent": [
            "Let an operator hand a Phase 34 witness package to "
            "a separate verification process (or to themselves) "
            "without ever using network, subprocess, "
            "multiprocessing, audio, or TTS.",
        ],
        "security_boundary_explanation":
            create_security_boundary_explanation(),
        "exporter_role": create_exporter_steps(),
        "witness_role": create_witness_steps(),
        "failure_handling": create_failure_handling_steps(),
        "caveats": [
            "This is NOT real voice execution.",
            "Phase 21 real corpus import is a separate workflow.",
            "Witness packages carry no signing secret; HMAC "
            "verification requires the operator to hold the key "
            "out-of-band.",
        ],
    }


def write_phase35_operator_exchange_guide(
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
    "create_phase35_operator_exchange_guide",
    "create_exporter_steps",
    "create_witness_steps",
    "create_failure_handling_steps",
    "create_security_boundary_explanation",
    "write_phase35_operator_exchange_guide",
]
