"""Phase 40 - Operator-Readable Audit-Replay Packet."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase40.operator_packet.v1"


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "phase",
    "replay_status",
    "source_phase39_summary",
    "trace_replay_summary",
    "drift_summary",
    "baseline_summary",
    "phase21_status",
    "boundary_preservation_summary",
    "rollback_readiness",
    "next_recommended_phase",
    "rehearsal_dry_run_only",
)


_BANNED_PACKET_FIELDS = (
    "operator_id",
    "signing_key_material", "private_key",
    "material_hex", "sealed_payload",
    "produced_audio", "invoked_tts",
    "used_subprocess", "used_network",
    "wrote_files",
    "command", "command_line",
    "spoken_render_payload",
)


def _safe_dict(d: Any) -> dict[str, Any]:
    if not isinstance(d, dict):
        return {}
    out = dict(d)
    for k in _BANNED_PACKET_FIELDS:
        out.pop(k, None)
    return out


def create_phase40_operator_packet(
    verification_result: Any,
) -> dict[str, Any]:
    vr = verification_result \
        if isinstance(verification_result, dict) else {}
    trace = vr.get("trace_replay") or {}
    drift = vr.get("drift") or {}
    art_sum = vr.get("artifacts_summary") or {}
    status = str(vr.get("status") or "drift_detected")
    return {
        "packet_id": f"oppkt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "replay_status": status,
        "source_phase39_summary": {
            "loaded_artifact_count": len(
                (art_sum.get("sizes_bytes") or {})),
            "sizes_bytes":
                art_sum.get("sizes_bytes") or {},
        },
        "trace_replay_summary": {
            "ok": bool(trace.get("ok")),
            "rederived_root_hash":
                trace.get("rederived_root_hash"),
            "stored_root_hash":
                trace.get("stored_root_hash"),
            "rederived_receipt_count":
                trace.get("rederived_receipt_count"),
            "stored_receipt_count":
                trace.get("stored_receipt_count"),
            "chain_matches_loaded_receipts":
                (trace.get("tampering_check") or {})
                .get("chain_matches_loaded_receipts"),
            "chain_matches_inline_receipts":
                (trace.get("tampering_check") or {})
                .get("chain_matches_inline_receipts"),
        },
        "drift_summary": {
            "ok": bool(drift.get("ok")),
            "fail_count": drift.get("fail_count"),
            "warn_count": drift.get("warn_count"),
            "pass_count": drift.get("pass_count"),
            "categories":
                [r.get("category") for r in
                 (drift.get("results") or [])
                 if isinstance(r, dict)],
        },
        "baseline_summary": {
            "observed": vr.get("baseline_observed") or {},
            "drifts": vr.get("baseline_drifts") or [],
            "expected": {
                "english_words": 2814,
                "russian_words": 2518,
                "russian_phrases": 35,
                "bilingual_concepts": 26,
                "bilingual_entry_links": 52,
                "live_pack_manifests": 90,
            },
        },
        "phase21_status": {
            "status_text":
                vr.get("phase21_status_text", "BLOCKED"),
            "drifted": bool(vr.get("phase21_drifted")),
            "note": ("Phase 21 incoming files are NEVER "
                      "imported by Phase 40."),
        },
        "boundary_preservation_summary": {
            "no_audio": True,
            "no_tts": True,
            "no_subprocess": True,
            "no_network": True,
            "no_multiprocessing": True,
            "no_main_runtime_integration": True,
            "no_production_db_modification":
                not bool(vr.get("baseline_drifts")),
        },
        "rollback_readiness":
            "Delete the 7 Phase 40 modules + harness + "
            "report + the 9 sub-folders under "
            "bilingual_stack/governance_phase40/. Phase "
            "27-39 remain green.",
        "next_recommended_phase":
            "Phase G fifth metadata-only adapter (e.g. "
            "memory-continuity audit adapter) OR Phase 41 "
            "cross-machine witness portability harness.",
        "rehearsal_dry_run_only": True,
        "notes": [
            "Packet carries no operator_id, no signing "
            "material, no audio, no command fields.",
            "Phase 21 real import remains BLOCKED unless "
            "operator explicitly stages corpus files AND "
            "runs Phase 21 separately.",
        ],
    }


def validate_phase40_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(packet, dict):
        return {"ok": False, "reasons": ["packet_not_dict"]}
    for f in _REQUIRED_PACKET_FIELDS:
        if f not in packet:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_PACKET_FIELDS:
        if k in packet and packet.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field_present:{k}")
    if packet.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase40_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": str(packet.get("replay_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase40 packet: status="
            f"{packet.get('replay_status')} "
            f"fail="
            f"{(packet.get('drift_summary') or {}).get('fail_count')} "
            f"phase21="
            f"{(packet.get('phase21_status') or {}).get('status_text')}"),
        "packet_id": packet.get("packet_id"),
        "phase": _PHASE,
    }


def write_phase40_operator_packet(
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


def write_phase40_operator_packet_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase40_operator_packet(
        report, output_path)


__all__ = [
    "create_phase40_operator_packet",
    "validate_phase40_operator_packet",
    "summarize_phase40_operator_packet",
    "write_phase40_operator_packet",
    "write_phase40_operator_packet_report",
]
