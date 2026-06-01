"""Phase 48 - Operator Packet."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase48.operator_packet.v1"


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "phase",
    "phase48_status",
    "source_phase",
    "capsule_id",
    "artifact_count",
    "capsule_root_hash",
    "fresh_checkout_verification_status",
    "tamper_suite_summary",
    "receipt_summary",
    "no_runtime_state_dependency_status",
    "excluded_artifacts_summary",
    "phase21_import_status",
    "adapter_allowlist_status",
    "rollback_readiness",
    "next_recommended_phase",
    "rehearsal_dry_run_only",
)


_BANNED_PACKET_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def create_phase48_operator_packet(
    contract: dict[str, Any],
    capsule: dict[str, Any],
    manifest: dict[str, Any],
    fresh_result: dict[str, Any],
    tamper_result: dict[str, Any],
    receipt: dict[str, Any],
) -> dict[str, Any]:
    c = capsule or {}
    fr = fresh_result or {}
    tr = tamper_result or {}
    r = receipt or {}
    nrs = fr.get("no_runtime_state_check") or {}
    status = "ok"
    if not fr.get("ok") or not tr.get("ok"):
        status = "drift_detected"
    return {
        "packet_id": f"p48pkt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "phase48_status": status,
        "source_phase":
            (contract or {}).get(
                "source_phase", "phase47"),
        "contract_id":
            (contract or {}).get("contract_id"),
        "capsule_id": c.get("capsule_id"),
        "manifest_id":
            (manifest or {}).get("manifest_id"),
        "receipt_id": r.get("receipt_id"),
        "artifact_count":
            int(c.get("artifact_count") or 0),
        "capsule_root_hash":
            c.get("capsule_root_hash"),
        "fresh_checkout_verification_status":
            "ok" if fr.get("ok") else "drift_detected",
        "verification_breakdown": {
            "presence_ok":
                bool((fr.get("presence_check")
                       or {}).get("ok")),
            "hash_ok":
                bool((fr.get("hash_check")
                       or {}).get("ok")),
            "federation_ok":
                bool((fr.get("federation_check")
                       or {}).get("ok")),
            "boundary_ok":
                bool((fr.get("boundary_check")
                       or {}).get("ok")),
            "phase21_ok":
                bool((fr.get("phase21_check")
                       or {}).get("ok")),
            "no_runtime_state_ok": bool(nrs.get("ok")),
        },
        "tamper_suite_summary": {
            "ok": bool(tr.get("ok")),
            "case_count": tr.get("case_count"),
            "detected_count":
                tr.get("detected_count"),
            "undetected_count":
                tr.get("undetected_count"),
        },
        "receipt_summary": {
            "receipt_id": r.get("receipt_id"),
            "fresh_checkout_verification_status":
                r.get(
                    "fresh_checkout_verification_status"),
            "snapshot_status":
                r.get("snapshot_status", status),
        },
        "no_runtime_state_dependency_status":
            "ok" if nrs.get("ok") else "drift",
        "excluded_artifacts_summary": {
            "missing":
                c.get("missing_artifacts") or [],
            "excluded_keys":
                c.get("excluded_artifact_keys") or [],
        },
        "phase21_import_status": {
            "status_text":
                c.get("phase21_status_text",
                       "BLOCKED"),
            "note":
                ("Phase 48 NEVER imports corpus files; "
                 "status is reported only."),
        },
        "adapter_allowlist_status": {
            "expected_count": 5,
            "observed_count":
                int(c.get(
                    "adapter_allowlist_count") or 0),
        },
        "production_baseline_expected":
            dict(c.get(
                "production_baseline_expected") or {}),
        "boundary_summary":
            dict(c.get("boundary_summary") or {}),
        "rollback_readiness":
            "Delete the 11 Phase 48 files (9 modules + "
            "harness + report) and the 13 sub-folders "
            "under bilingual_stack/voice_adapter_phase48/. "
            "Phase 27-47 remain green.",
        "next_recommended_phase":
            "Phase 49 federation portability replay "
            "verification OR Phase 41a continuity-ledger.",
        "rehearsal_dry_run_only": True,
        "notes": [
            "Packet carries no operator_id, no signing "
            "material, no raw transcript, no audio, no "
            "command fields.",
            "Phase 21 import remains BLOCKED unless "
            "operator explicitly stages corpus files "
            "AND runs Phase 21 separately.",
        ],
    }


def validate_phase48_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(packet, dict):
        return {"ok": False,
                "reasons": ["packet_not_dict"]}
    for f in _REQUIRED_PACKET_FIELDS:
        if f not in packet:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_PACKET_FIELDS:
        if k in packet and packet.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    if packet.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase48_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": str(packet.get("phase48_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase48 packet: status="
            f"{packet.get('phase48_status')} "
            f"fresh="
            f"{packet.get('fresh_checkout_verification_status')} "
            f"tamper_ok="
            f"{(packet.get('tamper_suite_summary') or {}).get('ok')} "
            f"phase21="
            f"{(packet.get('phase21_import_status') or {}).get('status_text')}"),
        "packet_id": packet.get("packet_id"),
        "phase": _PHASE,
    }


def create_phase48_operator_packet_markdown(
    packet: Any,
) -> str:
    if not isinstance(packet, dict):
        return ""
    p21 = packet.get("phase21_import_status") or {}
    vb = packet.get("verification_breakdown") or {}
    ts = packet.get("tamper_suite_summary") or {}
    allow = (packet.get("adapter_allowlist_status")
              or {})
    lines: list[str] = []
    lines.append("# Phase 48 - Federation Portability "
                  "Snapshot - Operator Packet\n")
    lines.append(f"_Generated at "
                  f"{int(packet.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 48 status:** "
                  f"{packet.get('phase48_status')}\n")
    lines.append(f"- **Source phase:** "
                  f"{packet.get('source_phase')}\n")
    lines.append(f"- **Capsule id:** "
                  f"{packet.get('capsule_id')}\n")
    lines.append(f"- **Artifact count:** "
                  f"{packet.get('artifact_count')}\n")
    lines.append(f"- **Capsule root hash:** "
                  f"{(packet.get('capsule_root_hash') or '')[:32]}\n")
    lines.append(f"- **Fresh-checkout verification:** "
                  f"{packet.get('fresh_checkout_verification_status')}\n")
    lines.append(f"  - presence: {vb.get('presence_ok')} "
                  f"hash: {vb.get('hash_ok')} "
                  f"federation: {vb.get('federation_ok')} "
                  f"boundary: {vb.get('boundary_ok')} "
                  f"phase21: {vb.get('phase21_ok')} "
                  f"no_runtime_state: "
                  f"{vb.get('no_runtime_state_ok')}\n")
    lines.append(f"- **Tamper suite:** ok={ts.get('ok')} "
                  f"detected={ts.get('detected_count')}/"
                  f"{ts.get('case_count')}\n")
    lines.append(f"- **No-runtime-state dependency:** "
                  f"{packet.get('no_runtime_state_dependency_status')}\n")
    lines.append(f"- **Phase 21 import status:** "
                  f"{p21.get('status_text')}\n")
    lines.append(f"- **Adapter allowlist:** "
                  f"{allow.get('observed_count')}/"
                  f"{allow.get('expected_count')}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{packet.get('next_recommended_phase')}\n")
    lines.append("")
    lines.append("**Phase 48 packages the Phase 47 "
                  "federation as a portable trust "
                  "capsule. Verifier reads only the "
                  "capsule; never production DBs; never "
                  "invokes any adapter.**\n")
    return "".join(lines)


def write_phase48_operator_packet(
    packet: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(packet)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase48_operator_packet_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase48_operator_packet",
    "validate_phase48_operator_packet",
    "summarize_phase48_operator_packet",
    "create_phase48_operator_packet_markdown",
    "write_phase48_operator_packet",
    "write_phase48_operator_packet_markdown",
]
