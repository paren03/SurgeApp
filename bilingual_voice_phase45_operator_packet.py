"""Phase 45 - Operator Packet (chain-of-trust report)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase45.operator_packet.v1"


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "phase",
    "phase45_status", "source_phases",
    "artifact_count",
    "chain_of_trust_status",
    "manifest_verification_status",
    "archive_verification_status",
    "tamper_suite_summary",
    "no_runtime_state_dependency_status",
    "excluded_artifacts_summary",
    "phase21_import_status",
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


def create_phase45_operator_packet(
    contract: dict[str, Any],
    archive: dict[str, Any],
    manifest: dict[str, Any],
    ledger: dict[str, Any],
    verification_result: dict[str, Any],
    tamper_result: dict[str, Any],
) -> dict[str, Any]:
    a = archive or {}
    v = verification_result or {}
    t = tamper_result or {}
    nrs = v.get("no_runtime_state_check") or {}
    chain = v.get("chain_integrity_check") or {}
    status = "ok"
    if not v.get("ok") or not t.get("ok"):
        status = "drift_detected"
    return {
        "packet_id": f"p45pkt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "phase45_status": status,
        "source_phases": list(a.get("source_phases")
                               or []),
        "contract_id":
            (contract or {}).get("contract_id"),
        "archive_id": a.get("archive_id"),
        "manifest_id":
            (manifest or {}).get("manifest_id"),
        "ledger_id": (ledger or {}).get("ledger_id"),
        "artifact_count":
            int(a.get("artifact_count") or 0),
        "phase_counts":
            dict(a.get("phase_counts") or {}),
        "chain_of_trust_status":
            "ok" if chain.get("ok") else "drift_detected",
        "manifest_verification_status":
            "ok" if (v.get("hash_check") or {}).get(
                "ok") else "mismatch",
        "archive_verification_status":
            "ok" if v.get("ok") else "drift_detected",
        "verification_breakdown": {
            "presence_ok":
                bool((v.get("presence_check") or {})
                     .get("ok")),
            "hash_ok":
                bool((v.get("hash_check") or {})
                     .get("ok")),
            "chain_ok":
                bool(chain.get("ok")),
            "boundary_ok":
                bool((v.get("boundary_check") or {})
                     .get("ok")),
            "phase21_ok":
                bool((v.get("phase21_check") or {})
                     .get("ok")),
            "no_runtime_state_ok": bool(nrs.get("ok")),
        },
        "tamper_suite_summary": {
            "ok": bool(t.get("ok")),
            "case_count": t.get("case_count"),
            "detected_count": t.get("detected_count"),
            "undetected_count":
                t.get("undetected_count"),
        },
        "no_runtime_state_dependency_status":
            "ok" if nrs.get("ok") else "drift",
        "excluded_artifacts_summary": {
            "missing":
                a.get("missing_artifacts") or [],
            "excluded_keys":
                a.get("excluded_artifact_keys") or [],
        },
        "phase21_import_status": {
            "status_text":
                a.get("phase21_status_text",
                       "BLOCKED"),
            "note":
                ("Phase 45 NEVER imports corpus files; "
                 "status is reported only."),
        },
        "production_baseline_expected":
            dict(a.get("production_baseline_expected")
                 or {}),
        "boundary_summary":
            dict(a.get("boundary_summary") or {}),
        "rollback_readiness":
            "Delete the 10 Phase 45 files (9 modules + "
            "harness + report) and the 12 sub-folders "
            "under bilingual_stack/voice_adapter_phase45/. "
            "Phase 27-44 remain green.",
        "next_recommended_phase":
            "Phase 46 cross-archive long-horizon timeline "
            "OR Phase 41a continuity-ledger.",
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


def validate_phase45_operator_packet(
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


def summarize_phase45_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": str(packet.get("phase45_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase45 packet: status="
            f"{packet.get('phase45_status')} "
            f"chain="
            f"{packet.get('chain_of_trust_status')} "
            f"tamper_ok="
            f"{(packet.get('tamper_suite_summary') or {}).get('ok')} "
            f"phase21="
            f"{(packet.get('phase21_import_status') or {}).get('status_text')}"),
        "packet_id": packet.get("packet_id"),
        "phase": _PHASE,
    }


def create_phase45_operator_packet_markdown(
    packet: Any,
) -> str:
    if not isinstance(packet, dict):
        return ""
    p21 = packet.get("phase21_import_status") or {}
    vb = packet.get("verification_breakdown") or {}
    ts = packet.get("tamper_suite_summary") or {}
    lines: list[str] = []
    lines.append("# Phase 45 - Multi-Bundle Chain-of-"
                  "Trust - Operator Packet\n")
    lines.append(f"_Generated at "
                  f"{int(packet.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 45 status:** "
                  f"{packet.get('phase45_status')}\n")
    lines.append(f"- **Source phases:** "
                  f"{packet.get('source_phases')}\n")
    lines.append(f"- **Artifact count:** "
                  f"{packet.get('artifact_count')}\n")
    lines.append(f"- **Phase counts:** "
                  f"{packet.get('phase_counts')}\n")
    lines.append(f"- **Chain-of-trust status:** "
                  f"{packet.get('chain_of_trust_status')}"
                  f"\n")
    lines.append(f"- **Manifest verification:** "
                  f"{packet.get('manifest_verification_status')}"
                  f"\n")
    lines.append(f"- **Archive verification:** "
                  f"{packet.get('archive_verification_status')}"
                  f"\n")
    lines.append(f"  - presence: {vb.get('presence_ok')} "
                  f"hash: {vb.get('hash_ok')} "
                  f"chain: {vb.get('chain_ok')} "
                  f"boundary: {vb.get('boundary_ok')} "
                  f"phase21: {vb.get('phase21_ok')} "
                  f"no_runtime_state: "
                  f"{vb.get('no_runtime_state_ok')}\n")
    lines.append(f"- **Tamper suite:** "
                  f"ok={ts.get('ok')} "
                  f"detected={ts.get('detected_count')}/"
                  f"{ts.get('case_count')}\n")
    lines.append(f"- **No-runtime-state dependency:** "
                  f"{packet.get('no_runtime_state_dependency_status')}"
                  f"\n")
    lines.append(f"- **Phase 21 import status:** "
                  f"{p21.get('status_text')}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{packet.get('next_recommended_phase')}"
                  f"\n")
    lines.append("")
    lines.append("**Phase 45 aggregates Phase 42/43/44 "
                  "portable evidence into one chain-of-"
                  "trust archive. Verifier reads only the "
                  "archive; never production DBs; never "
                  "invokes any adapter.**\n")
    return "".join(lines)


def write_phase45_operator_packet(
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


def write_phase45_operator_packet_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase45_operator_packet",
    "validate_phase45_operator_packet",
    "summarize_phase45_operator_packet",
    "create_phase45_operator_packet_markdown",
    "write_phase45_operator_packet",
    "write_phase45_operator_packet_markdown",
]
