"""Phase 42 - Operator Packet (final readable output)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase42.operator_packet.v1"


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "phase",
    "audit_status",
    "scenario_coverage",
    "adapter_coverage",
    "status_summary",
    "evidence_summary",
    "replay_matrix_summary",
    "drift_stability_summary",
    "memory_privacy_summary",
    "production_baseline_summary",
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


_PROD_INVARIANTS = {
    "english_words": 2814,
    "russian_words": 2518,
    "russian_phrases": 35,
    "bilingual_concepts": 26,
    "bilingual_entry_links": 52,
    "live_pack_manifests": 90,
}


def create_phase42_operator_packet(
    contract: dict[str, Any],
    scenarios: list[dict[str, Any]],
    trace_results: list[dict[str, Any]],
    coherence_audit: dict[str, Any],
    replay_matrix: dict[str, Any],
    drift_matrix: dict[str, Any],
) -> dict[str, Any]:
    ca = coherence_audit or {}
    rm = replay_matrix or {}
    dm = drift_matrix or {}
    status = "ok"
    if (not ca.get("ok")) or (not dm.get("ok")):
        status = "drift_detected"
    elif rm.get("compatibility_status") != "ok":
        status = "drift_detected"
    elif int(dm.get("warn_count") or 0) > 0:
        status = "ok_with_warnings"
    scenario_ids = [s.get("scenario_id") for s in
                     (scenarios or [])
                     if isinstance(s, dict)]
    ok_count = sum(1 for r in trace_results
                    if isinstance(r, dict)
                    and r.get("status") == "ok")
    refused_count = sum(1 for r in trace_results
                         if isinstance(r, dict)
                         and "refused" in str(r.get("status")
                                                 or ""))
    ks_count = sum(1 for r in trace_results
                    if isinstance(r, dict)
                    and r.get("status")
                    == "kill_switch_blocked")
    adapter_dist: dict[str, int] = {}
    for r in trace_results or []:
        if not isinstance(r, dict):
            continue
        name = r.get("selected_adapter_name") or "none"
        adapter_dist[name] = adapter_dist.get(name, 0) + 1
    ev = ca.get("evidence_coherence") or {}
    proj = ca.get("replay_projection_coherence") or {}
    mp = ca.get("memory_privacy") or {}
    bp = ca.get("boundary_preservation") or {}
    p21d = dm.get("phase21_status_drift") or {}
    bld = dm.get("baseline_drift") or {}
    return {
        "packet_id": f"p42pkt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "audit_status": status,
        "scenario_coverage": {
            "scenario_count": len(scenarios or []),
            "scenario_ids": scenario_ids,
            "required_count_observed":
                len([s for s in (scenarios or [])
                     if isinstance(s, dict)
                     and s.get("scenario_id")]),
        },
        "adapter_coverage": {
            "required_count": 5,
            "covered_in_success":
                (ca.get("adapter_coverage") or {})
                .get("covered_in_success", []),
            "missing_in_success":
                (ca.get("adapter_coverage") or {})
                .get("missing_in_success", []),
            "adapter_distribution": adapter_dist,
        },
        "status_summary": {
            "trace_count": len(trace_results or []),
            "ok_count": ok_count,
            "refused_count": refused_count,
            "kill_switch_blocked_count": ks_count,
        },
        "evidence_summary": {
            "signed_evidence_missing":
                ev.get("missing_signed_evidence", []),
            "witness_export_missing":
                ev.get("missing_witness_export", []),
            "exchange_missing":
                ev.get("missing_exchange", []),
        },
        "replay_matrix_summary": {
            "projection_count":
                rm.get("projection_count"),
            "compatibility_status":
                rm.get("compatibility_status"),
            "matrix_id": rm.get("matrix_id"),
        },
        "drift_stability_summary": {
            "fail_count": dm.get("fail_count"),
            "warn_count": dm.get("warn_count"),
            "pass_count": dm.get("pass_count"),
            "matrix_id": dm.get("matrix_id"),
        },
        "memory_privacy_summary": {
            "ok": bool(mp.get("ok")),
            "failures": mp.get("failures", []),
        },
        "production_baseline_summary": {
            "expected": dict(_PROD_INVARIANTS),
            "observed": bld.get("observed") or {},
            "drifts": bld.get("drifts") or [],
        },
        "phase21_import_status": {
            "status_text": p21d.get(
                "phase21_status_text", "BLOCKED"),
            "drifted": bool(p21d.get("drifted")),
            "note": ("Phase 42 NEVER imports corpus "
                     "files; status is reported only."),
        },
        "boundary_preservation_summary": {
            "ok": bool(bp.get("ok")),
            "failures": bp.get("failures", []),
        },
        "rollback_readiness":
            "Delete the 9 Phase 42 files (8 modules + "
            "harness + report) and the 12 sub-folders "
            "under bilingual_stack/voice_adapter_phase42/. "
            "Phase 27-41 remain green.",
        "next_recommended_phase":
            "Phase 43 multi-machine continuity portability "
            "harness OR Phase 41a continuity-ledger.",
        "rehearsal_dry_run_only": True,
        "notes": [
            "Packet carries no operator_id, no signing "
            "material, no raw transcript, no audio, no "
            "command fields.",
            "Phase 21 import remains BLOCKED unless "
            "operator explicitly stages corpus files AND "
            "runs Phase 21 separately.",
        ],
    }


def validate_phase42_operator_packet(
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


def summarize_phase42_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": str(packet.get("audit_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase42 packet: status="
            f"{packet.get('audit_status')} "
            f"traces="
            f"{(packet.get('status_summary') or {}).get('trace_count')} "
            f"adapter_covered="
            f"{len((packet.get('adapter_coverage') or {}).get('covered_in_success') or [])} "
            f"phase21="
            f"{(packet.get('phase21_import_status') or {}).get('status_text')}"),
        "packet_id": packet.get("packet_id"),
        "phase": _PHASE,
    }


def create_phase42_operator_packet_markdown(
    packet: Any,
) -> str:
    if not isinstance(packet, dict):
        return ""
    ac = packet.get("adapter_coverage") or {}
    ss = packet.get("status_summary") or {}
    pb = packet.get("production_baseline_summary") or {}
    p21 = packet.get("phase21_import_status") or {}
    rm = packet.get("replay_matrix_summary") or {}
    dm = packet.get("drift_stability_summary") or {}
    lines: list[str] = []
    lines.append("# Phase 42 - Multi-Trace Coherence "
                  "Audit - Operator Packet\n")
    lines.append(f"_Generated at "
                  f"{int(packet.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Audit status:** "
                  f"{packet.get('audit_status')}\n")
    lines.append(f"- **Trace count:** "
                  f"{ss.get('trace_count')}\n")
    lines.append(f"- **ok / refused / kill-switch:** "
                  f"{ss.get('ok_count')} / "
                  f"{ss.get('refused_count')} / "
                  f"{ss.get('kill_switch_blocked_count')}\n")
    lines.append(f"- **Adapters covered in success:** "
                  f"{ac.get('covered_in_success')}\n")
    lines.append(f"- **Replay matrix compatibility:** "
                  f"{rm.get('compatibility_status')}\n")
    lines.append(f"- **Drift count:** fail="
                  f"{dm.get('fail_count')} warn="
                  f"{dm.get('warn_count')} pass="
                  f"{dm.get('pass_count')}\n")
    lines.append(f"- **Production baselines:** "
                  f"{pb.get('observed') or pb.get('expected')}"
                  f"\n")
    lines.append(f"- **Phase 21 import status:** "
                  f"{p21.get('status_text')}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{packet.get('next_recommended_phase')}"
                  f"\n")
    lines.append("")
    lines.append("**Phase 42 generates no audio, no TTS, "
                  "no subprocess, no network, no "
                  "multiprocessing. Phase 21 remains "
                  "BLOCKED unless explicitly staged.**\n")
    return "".join(lines)


def write_phase42_operator_packet(
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


def write_phase42_operator_packet_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase42_operator_packet",
    "validate_phase42_operator_packet",
    "summarize_phase42_operator_packet",
    "create_phase42_operator_packet_markdown",
    "write_phase42_operator_packet",
    "write_phase42_operator_packet_markdown",
]
