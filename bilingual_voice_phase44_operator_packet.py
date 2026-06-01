"""Phase 44 - Operator Packet."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase44.operator_packet.v1"


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "phase",
    "phase44_status",
    "import_simulation_status",
    "artifact_import_summary",
    "fresh_verification_status",
    "tamper_suite_summary",
    "roundtrip_receipt_summary",
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


def create_phase44_operator_packet(
    contract: dict[str, Any],
    imported_bundle: dict[str, Any],
    import_manifest: dict[str, Any],
    fresh_result: dict[str, Any],
    tamper_result: dict[str, Any],
    roundtrip_receipt: dict[str, Any],
) -> dict[str, Any]:
    ib = imported_bundle or {}
    fr = fresh_result or {}
    tr = tamper_result or {}
    rc = roundtrip_receipt or {}
    nrs = fr.get("no_runtime_state_check") or {}
    status = "ok"
    if not fr.get("ok") or not tr.get("ok"):
        status = "drift_detected"
    elif str(rc.get("import_status") or "") not in (
            "ok", "ok_with_warnings"):
        status = "drift_detected"
    return {
        "packet_id": f"p44pkt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "phase44_status": status,
        "source_phase":
            (contract or {}).get("source_phase",
                                  "phase43"),
        "contract_id":
            (contract or {}).get("contract_id"),
        "import_id":
            (import_manifest or {}).get("import_id"),
        "import_simulation_status":
            "ok" if ib.get("ok") else "drift_detected",
        "artifact_import_summary": {
            "imported_count":
                int(ib.get("imported_count") or 0),
            "missing": ib.get("missing") or [],
            "rejected": ib.get("rejected") or [],
            "workspace_dir":
                ib.get("workspace_artifacts_dir"),
        },
        "fresh_verification_status":
            "ok" if fr.get("ok") else "drift_detected",
        "fresh_check_breakdown": {
            "presence_ok":
                bool((fr.get("presence_check")
                       or {}).get("ok")),
            "hash_ok":
                bool((fr.get("hash_check")
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
            "detected_count": tr.get("detected_count"),
            "undetected_count":
                tr.get("undetected_count"),
        },
        "roundtrip_receipt_summary": {
            "receipt_id": rc.get("receipt_id"),
            "import_status": rc.get("import_status"),
            "fresh_verification_status":
                rc.get("fresh_verification_status"),
        },
        "no_runtime_state_dependency_status":
            "ok" if nrs.get("ok") else "drift",
        "excluded_artifacts_summary": {
            "missing": ib.get("missing") or [],
            "rejected": ib.get("rejected") or [],
        },
        "phase21_import_status": {
            "status_text":
                ib.get("phase21_status_text",
                         "BLOCKED"),
            "note": ("Phase 44 NEVER imports corpus "
                     "files."),
        },
        "rollback_readiness":
            "Delete the 10 Phase 44 files (9 modules + "
            "harness + report) and the 12 sub-folders "
            "under bilingual_stack/voice_adapter_phase44/. "
            "Phase 27-43 remain green.",
        "next_recommended_phase":
            rc.get("next_recommended_phase",
                    "Phase 45 multi-bundle archive + "
                    "chain-of-trust verification"),
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


def validate_phase44_operator_packet(
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


def summarize_phase44_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": str(packet.get("phase44_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase44 packet: status="
            f"{packet.get('phase44_status')} "
            f"fresh="
            f"{packet.get('fresh_verification_status')} "
            f"tamper_ok="
            f"{(packet.get('tamper_suite_summary') or {}).get('ok')} "
            f"phase21="
            f"{(packet.get('phase21_import_status') or {}).get('status_text')}"),
        "packet_id": packet.get("packet_id"),
        "phase": _PHASE,
    }


def create_phase44_operator_packet_markdown(
    packet: Any,
) -> str:
    if not isinstance(packet, dict):
        return ""
    p21 = packet.get("phase21_import_status") or {}
    fb = packet.get("fresh_check_breakdown") or {}
    ts = packet.get("tamper_suite_summary") or {}
    ais = packet.get("artifact_import_summary") or {}
    lines: list[str] = []
    lines.append("# Phase 44 - Cross-Machine Import "
                  "Simulation - Operator Packet\n")
    lines.append(f"_Generated at "
                  f"{int(packet.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Phase 44 status:** "
                  f"{packet.get('phase44_status')}\n")
    lines.append(f"- **Source phase:** "
                  f"{packet.get('source_phase')}\n")
    lines.append(f"- **Import simulation:** "
                  f"{packet.get('import_simulation_status')}"
                  f"\n")
    lines.append(f"- **Imported artifact count:** "
                  f"{ais.get('imported_count')}\n")
    lines.append(f"- **Fresh verification:** "
                  f"{packet.get('fresh_verification_status')}"
                  f"\n")
    lines.append(f"  - presence: {fb.get('presence_ok')} "
                  f"hash: {fb.get('hash_ok')} "
                  f"boundary: {fb.get('boundary_ok')} "
                  f"phase21: {fb.get('phase21_ok')} "
                  f"no_runtime_state: "
                  f"{fb.get('no_runtime_state_ok')}\n")
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
    lines.append("**Phase 44 simulates cross-machine "
                  "import via local Python file copies "
                  "only. No subprocess, no network, no "
                  "multiprocessing. Fresh-import verifier "
                  "reads only the imported bundle.**\n")
    return "".join(lines)


def write_phase44_operator_packet(
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


def write_phase44_operator_packet_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase44_operator_packet",
    "validate_phase44_operator_packet",
    "summarize_phase44_operator_packet",
    "create_phase44_operator_packet_markdown",
    "write_phase44_operator_packet",
    "write_phase44_operator_packet_markdown",
]
