"""Phase 47 - Operator Packet."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase47.operator_packet.v1"


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "phase",
    "phase47_status",
    "source_phase",
    "checkout_count",
    "checkout_ids",
    "federation_root_hash",
    "imported_timeline_verification_status",
    "graph_verification_status",
    "manifest_verification_status",
    "drift_summary",
    "tamper_suite_summary",
    "no_runtime_state_dependency_status",
    "phase21_status_history",
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


def create_phase47_operator_packet(
    contract: dict[str, Any],
    imported_timelines: list[Any],
    graph: dict[str, Any],
    manifest: dict[str, Any],
    verification_result: dict[str, Any],
    drift_result: dict[str, Any],
    tamper_result: dict[str, Any],
) -> dict[str, Any]:
    v = verification_result or {}
    d = drift_result or {}
    t = tamper_result or {}
    status = "ok"
    if not v.get("ok") or not t.get("ok") \
            or not d.get("ok"):
        status = "drift_detected"
    elif int(d.get("warn_count") or 0) > 0:
        status = "ok_with_warnings"
    cids = list((manifest or {}).get(
        "checkout_ids") or [])
    p21_hist = dict((manifest or {}).get(
        "phase21_status_history") or {})
    return {
        "packet_id": f"p47pkt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "phase47_status": status,
        "source_phase":
            (contract or {}).get(
                "source_phase", "phase46"),
        "contract_id":
            (contract or {}).get("contract_id"),
        "federation_id":
            (graph or {}).get("graph_id"),
        "manifest_id":
            (manifest or {}).get("manifest_id"),
        "checkout_count":
            int((graph or {}).get(
                "checkout_count") or 0),
        "checkout_ids": cids,
        "federation_root_hash":
            (graph or {}).get("federation_root_hash"),
        "imported_timeline_verification_status":
            "ok"
            if (v.get("imported_check") or {}).get(
                "ok") else "drift",
        "graph_verification_status":
            "ok"
            if (v.get("graph_check") or {}).get("ok")
            else "drift",
        "manifest_verification_status":
            "ok"
            if (v.get("manifest_check") or {}).get(
                "ok") else "drift",
        "verification_breakdown": {
            "imported_ok":
                bool((v.get("imported_check")
                       or {}).get("ok")),
            "graph_ok":
                bool((v.get("graph_check")
                       or {}).get("ok")),
            "manifest_ok":
                bool((v.get("manifest_check")
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
        },
        "drift_summary": {
            "ok": bool(d.get("ok")),
            "fail_count": d.get("fail_count"),
            "warn_count": d.get("warn_count"),
            "pass_count": d.get("pass_count"),
        },
        "tamper_suite_summary": {
            "ok": bool(t.get("ok")),
            "case_count": t.get("case_count"),
            "detected_count": t.get("detected_count"),
            "undetected_count":
                t.get("undetected_count"),
        },
        "no_runtime_state_dependency_status":
            "ok"
            if (v.get("no_runtime_state_check")
                or {}).get("ok") else "drift",
        "phase21_status_history": p21_hist,
        "phase21_import_status": {
            "status_text": "BLOCKED",
            "note":
                ("Phase 47 NEVER imports corpus "
                 "files; status is reported only."),
        },
        "boundary_summary":
            dict((graph or {}).get(
                "boundary_summary") or {}),
        "rollback_readiness":
            "Delete the 11 Phase 47 files (10 modules + "
            "harness + report) and the 13 sub-folders "
            "under bilingual_stack/voice_adapter_phase47/. "
            "Phase 27-46 remain green.",
        "next_recommended_phase":
            "Phase 48 federation portability snapshot OR "
            "Phase 41a continuity-ledger.",
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


def validate_phase47_operator_packet(
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


def summarize_phase47_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": str(packet.get("phase47_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase47 packet: status="
            f"{packet.get('phase47_status')} "
            f"checkouts={packet.get('checkout_count')} "
            f"fed_root="
            f"{(packet.get('federation_root_hash') or '')[:16]} "
            f"tamper_ok="
            f"{(packet.get('tamper_suite_summary') or {}).get('ok')}"),
        "packet_id": packet.get("packet_id"),
        "phase": _PHASE,
    }


def create_phase47_operator_packet_markdown(
    packet: Any,
) -> str:
    if not isinstance(packet, dict):
        return ""
    vb = packet.get("verification_breakdown") or {}
    ds = packet.get("drift_summary") or {}
    ts = packet.get("tamper_suite_summary") or {}
    lines: list[str] = []
    lines.append("# Phase 47 - Cross-Checkout Federated "
                  "Timeline - Operator Packet\n")
    lines.append(f"_Generated at "
                  f"{int(packet.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 47 status:** "
                  f"{packet.get('phase47_status')}\n")
    lines.append(f"- **Source phase:** "
                  f"{packet.get('source_phase')}\n")
    lines.append(f"- **Checkout count:** "
                  f"{packet.get('checkout_count')}\n")
    lines.append(f"- **Federation root hash:** "
                  f"{(packet.get('federation_root_hash') or '')[:32]}\n")
    lines.append(f"- **Imported timeline verification:** "
                  f"{packet.get('imported_timeline_verification_status')}\n")
    lines.append(f"- **Graph verification:** "
                  f"{packet.get('graph_verification_status')}\n")
    lines.append(f"- **Manifest verification:** "
                  f"{packet.get('manifest_verification_status')}\n")
    lines.append(f"  - imported: {vb.get('imported_ok')} "
                  f"graph: {vb.get('graph_ok')} "
                  f"manifest: {vb.get('manifest_ok')} "
                  f"boundary: {vb.get('boundary_ok')} "
                  f"phase21: {vb.get('phase21_ok')} "
                  f"no_runtime_state: "
                  f"{vb.get('no_runtime_state_ok')}\n")
    lines.append(f"- **Drift summary:** fail="
                  f"{ds.get('fail_count')} warn="
                  f"{ds.get('warn_count')} pass="
                  f"{ds.get('pass_count')}\n")
    lines.append(f"- **Tamper suite:** ok={ts.get('ok')} "
                  f"detected={ts.get('detected_count')}/"
                  f"{ts.get('case_count')}\n")
    lines.append(f"- **No-runtime-state dependency:** "
                  f"{packet.get('no_runtime_state_dependency_status')}\n")
    p21 = packet.get("phase21_import_status") or {}
    lines.append(f"- **Phase 21 import status:** "
                  f"{p21.get('status_text')}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{packet.get('next_recommended_phase')}\n")
    lines.append("")
    lines.append("**Phase 47 federates Phase 46 "
                  "timelines across simulated checkouts. "
                  "Verifier reads only imported timeline "
                  "JSON; never production DBs; never "
                  "invokes any adapter.**\n")
    return "".join(lines)


def write_phase47_operator_packet(
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


def write_phase47_operator_packet_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase47_operator_packet",
    "validate_phase47_operator_packet",
    "summarize_phase47_operator_packet",
    "create_phase47_operator_packet_markdown",
    "write_phase47_operator_packet",
    "write_phase47_operator_packet_markdown",
]
