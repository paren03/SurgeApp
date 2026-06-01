"""Phase 46 - Operator Packet."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase46.operator_packet.v1"


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "phase",
    "phase46_status",
    "source_phase",
    "archive_count",
    "monotonic_ordering_status",
    "chain_integrity_status",
    "manifest_verification_status",
    "long_horizon_verification_status",
    "tamper_suite_summary",
    "no_runtime_state_dependency_status",
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


def create_phase46_operator_packet(
    contract: dict[str, Any],
    timeline: dict[str, Any],
    manifest: dict[str, Any],
    verification_result: dict[str, Any],
    tamper_result: dict[str, Any],
) -> dict[str, Any]:
    t = timeline or {}
    v = verification_result or {}
    ts = tamper_result or {}
    status = "ok"
    if not v.get("ok") or not ts.get("ok"):
        status = "drift_detected"
    return {
        "packet_id": f"p46pkt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "phase46_status": status,
        "source_phase":
            (contract or {}).get("source_phase",
                                  "phase45"),
        "contract_id":
            (contract or {}).get("contract_id"),
        "timeline_id": t.get("timeline_id"),
        "manifest_id":
            (manifest or {}).get("manifest_id"),
        "archive_count":
            int(t.get("archive_count") or 0),
        "monotonic_ordering_status":
            "ok"
            if (v.get("monotonic_check") or {}).get(
                "ok") else "drift",
        "chain_integrity_status":
            "ok"
            if (v.get("chain_check") or {}).get("ok")
            else "drift",
        "manifest_verification_status":
            "ok"
            if (v.get("manifest_check") or {}).get(
                "ok") else "drift",
        "long_horizon_verification_status":
            "ok" if v.get("ok") else "drift_detected",
        "verification_breakdown": {
            "monotonic_ok":
                bool((v.get("monotonic_check")
                       or {}).get("ok")),
            "unique_ids_ok":
                bool((v.get("unique_ids_check")
                       or {}).get("ok")),
            "chain_ok":
                bool((v.get("chain_check")
                       or {}).get("ok")),
            "boundary_ok":
                bool((v.get("boundary_check")
                       or {}).get("ok")),
            "phase21_ok":
                bool((v.get("phase21_check")
                       or {}).get("ok")),
            "no_runtime_state_ok":
                bool((v.get("no_runtime_state_check")
                       or {}).get("ok")),
            "root_hash_ok":
                bool((v.get("root_hash_check")
                       or {}).get("ok")),
            "manifest_ok":
                bool((v.get("manifest_check")
                       or {}).get("ok")),
        },
        "tamper_suite_summary": {
            "ok": bool(ts.get("ok")),
            "case_count": ts.get("case_count"),
            "detected_count": ts.get("detected_count"),
            "undetected_count":
                ts.get("undetected_count"),
        },
        "no_runtime_state_dependency_status":
            "ok"
            if (v.get("no_runtime_state_check")
                or {}).get("ok") else "drift",
        "phase21_import_status": {
            "status_text":
                t.get("phase21_status_text",
                       "BLOCKED"),
            "note":
                ("Phase 46 NEVER imports corpus files; "
                 "status is reported only."),
        },
        "boundary_summary":
            dict(t.get("boundary_summary") or {}),
        "rollback_readiness":
            "Delete the 10 Phase 46 files (9 modules + "
            "harness + report) and the 11 sub-folders "
            "under bilingual_stack/voice_adapter_phase46/. "
            "Phase 27-45 remain green.",
        "next_recommended_phase":
            "Phase 47 cross-checkout federated timeline "
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


def validate_phase46_operator_packet(
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


def summarize_phase46_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": str(packet.get("phase46_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase46 packet: status="
            f"{packet.get('phase46_status')} "
            f"archives={packet.get('archive_count')} "
            f"chain="
            f"{packet.get('chain_integrity_status')} "
            f"tamper_ok="
            f"{(packet.get('tamper_suite_summary') or {}).get('ok')} "
            f"phase21="
            f"{(packet.get('phase21_import_status') or {}).get('status_text')}"),
        "packet_id": packet.get("packet_id"),
        "phase": _PHASE,
    }


def create_phase46_operator_packet_markdown(
    packet: Any,
) -> str:
    if not isinstance(packet, dict):
        return ""
    p21 = packet.get("phase21_import_status") or {}
    vb = packet.get("verification_breakdown") or {}
    ts = packet.get("tamper_suite_summary") or {}
    lines: list[str] = []
    lines.append("# Phase 46 - Cross-Archive Long-"
                  "Horizon Timeline - Operator Packet\n")
    lines.append(f"_Generated at "
                  f"{int(packet.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 46 status:** "
                  f"{packet.get('phase46_status')}\n")
    lines.append(f"- **Source phase:** "
                  f"{packet.get('source_phase')}\n")
    lines.append(f"- **Archive count:** "
                  f"{packet.get('archive_count')}\n")
    lines.append(f"- **Monotonic ordering:** "
                  f"{packet.get('monotonic_ordering_status')}"
                  f"\n")
    lines.append(f"- **Chain integrity:** "
                  f"{packet.get('chain_integrity_status')}"
                  f"\n")
    lines.append(f"- **Manifest verification:** "
                  f"{packet.get('manifest_verification_status')}"
                  f"\n")
    lines.append(f"- **Long-horizon verification:** "
                  f"{packet.get('long_horizon_verification_status')}"
                  f"\n")
    lines.append(f"  - monotonic: {vb.get('monotonic_ok')} "
                  f"unique_ids: {vb.get('unique_ids_ok')} "
                  f"chain: {vb.get('chain_ok')} "
                  f"boundary: {vb.get('boundary_ok')} "
                  f"phase21: {vb.get('phase21_ok')} "
                  f"no_runtime_state: "
                  f"{vb.get('no_runtime_state_ok')} "
                  f"root_hash: {vb.get('root_hash_ok')} "
                  f"manifest: {vb.get('manifest_ok')}\n")
    lines.append(f"- **Tamper suite:** ok={ts.get('ok')} "
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
    lines.append("**Phase 46 aggregates Phase 45 "
                  "archives across sessions into a "
                  "monotonic timeline ledger. Verifier "
                  "reads only the captured archive JSON; "
                  "never production DBs; never invokes "
                  "any adapter.**\n")
    return "".join(lines)


def write_phase46_operator_packet(
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


def write_phase46_operator_packet_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase46_operator_packet",
    "validate_phase46_operator_packet",
    "summarize_phase46_operator_packet",
    "create_phase46_operator_packet_markdown",
    "write_phase46_operator_packet",
    "write_phase46_operator_packet_markdown",
]
