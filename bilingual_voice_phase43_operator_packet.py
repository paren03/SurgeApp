"""Phase 43 - Operator Packet."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase43.operator_packet.v1"


_REQUIRED_PACKET_FIELDS = (
    "packet_id", "created_at", "phase",
    "portability_status",
    "artifact_count",
    "hash_verification_status",
    "fresh_checkout_verification_status",
    "excluded_artifacts_summary",
    "no_secret_status",
    "no_audio_status",
    "no_runtime_db_status",
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


def _check_severity(audit_checks: list[Any],
                     category: str) -> dict[str, Any]:
    for c in audit_checks or []:
        if isinstance(c, dict) \
                and c.get("category") == category:
            return c
    return {}


def create_phase43_operator_packet(
    contract: dict[str, Any],
    bundle: dict[str, Any],
    manifest: dict[str, Any],
    fresh_result: dict[str, Any],
    portability_audit: dict[str, Any],
) -> dict[str, Any]:
    fr = fresh_result or {}
    pa = portability_audit or {}
    pa_checks = pa.get("checks") or []
    runtime_db = _check_severity(
        pa_checks, "no_runtime_db_artifacts")
    audio = _check_severity(pa_checks,
                             "no_audio_artifacts")
    secrets = _check_severity(pa_checks,
                                "no_secret_leakage")
    excluded = _check_severity(pa_checks,
                                 "excluded_artifacts")
    p21 = _check_severity(pa_checks, "phase21_metadata")
    status = "ok"
    if not fr.get("ok") or not pa.get("ok"):
        status = "drift_detected"
    elif int(pa.get("warn_count") or 0) > 0:
        status = "ok_with_warnings"
    return {
        "packet_id": f"p43pkt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "portability_status": status,
        "source_phase":
            (bundle or {}).get("source_phase",
                                "phase42"),
        "bundle_id": (bundle or {}).get("bundle_id"),
        "manifest_id":
            (manifest or {}).get("manifest_id"),
        "contract_id":
            (contract or {}).get("contract_id"),
        "artifact_count":
            int((bundle or {}).get("artifact_count")
                 or 0),
        "hash_verification_status":
            "ok" if (fr.get("hash_check") or {}).get(
                "ok") else "mismatch",
        "fresh_checkout_verification_status":
            "ok" if fr.get("ok") else "drift_detected",
        "presence_check_ok":
            bool((fr.get("presence_check") or {})
                 .get("ok")),
        "phase42_claims_check_ok":
            bool((fr.get("phase42_claims_check")
                   or {}).get("ok")),
        "boundary_claims_check_ok":
            bool((fr.get("boundary_claims_check")
                   or {}).get("ok")),
        "phase21_claim_check_ok":
            bool((fr.get("phase21_claim_check")
                   or {}).get("ok")),
        "excluded_artifacts_summary": {
            "ok": bool(excluded.get("ok")),
            "hits": excluded.get("hits", []),
        },
        "no_secret_status": {
            "ok": bool(secrets.get("ok")),
            "hits": secrets.get("hits", []),
        },
        "no_audio_status": {
            "ok": bool(audio.get("ok")),
            "hits": audio.get("hits", []),
        },
        "no_runtime_db_status": {
            "ok": bool(runtime_db.get("ok")),
            "hits": runtime_db.get("hits", []),
        },
        "phase21_import_status": {
            "status_text":
                p21.get("phase21_status_text",
                          "BLOCKED"),
            "drifted":
                str(p21.get("severity") or "") == "warn",
            "note":
                ("Phase 43 NEVER imports corpus files; "
                 "status is reported only."),
        },
        "production_baseline_expected":
            dict((bundle or {}).get(
                "production_baseline_expected") or {}),
        "boundary_summary":
            dict((bundle or {}).get(
                "boundary_summary") or {}),
        "rollback_readiness":
            "Delete the 9 Phase 43 files (8 modules + "
            "harness + report) and the 12 sub-folders "
            "under bilingual_stack/voice_adapter_phase43/. "
            "Phase 27-42 remain green.",
        "next_recommended_phase":
            "Phase 44 cross-machine bundle import + "
            "fresh-checkout regression OR Phase 41a "
            "continuity-ledger.",
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


def validate_phase43_operator_packet(
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


def summarize_phase43_operator_packet(
    packet: Any,
) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": str(packet.get("portability_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase43 packet: status="
            f"{packet.get('portability_status')} "
            f"hash="
            f"{packet.get('hash_verification_status')} "
            f"fresh="
            f"{packet.get('fresh_checkout_verification_status')} "
            f"phase21="
            f"{(packet.get('phase21_import_status') or {}).get('status_text')}"),
        "packet_id": packet.get("packet_id"),
        "phase": _PHASE,
    }


def create_phase43_operator_packet_markdown(
    packet: Any,
) -> str:
    if not isinstance(packet, dict):
        return ""
    p21 = packet.get("phase21_import_status") or {}
    lines: list[str] = []
    lines.append("# Phase 43 - Cross-Machine "
                  "Portability - Operator Packet\n")
    lines.append(f"_Generated at "
                  f"{int(packet.get('created_at') or time.time())}._\n")
    lines.append("")
    lines.append(f"- **Portability status:** "
                  f"{packet.get('portability_status')}\n")
    lines.append(f"- **Source phase:** "
                  f"{packet.get('source_phase')}\n")
    lines.append(f"- **Artifact count:** "
                  f"{packet.get('artifact_count')}\n")
    lines.append(f"- **Hash verification:** "
                  f"{packet.get('hash_verification_status')}"
                  f"\n")
    lines.append(f"- **Fresh-checkout verification:** "
                  f"{packet.get('fresh_checkout_verification_status')}"
                  f"\n")
    lines.append(f"- **No-runtime-DB:** "
                  f"{(packet.get('no_runtime_db_status') or {}).get('ok')}"
                  f"\n")
    lines.append(f"- **No-audio:** "
                  f"{(packet.get('no_audio_status') or {}).get('ok')}"
                  f"\n")
    lines.append(f"- **No-secret-leakage:** "
                  f"{(packet.get('no_secret_status') or {}).get('ok')}"
                  f"\n")
    lines.append(f"- **Phase 21 import status:** "
                  f"{p21.get('status_text')}\n")
    lines.append(f"- **Next recommended phase:** "
                  f"{packet.get('next_recommended_phase')}\n")
    lines.append("")
    lines.append("**Phase 43 makes a portable bundle. "
                  "Fresh-checkout verifier reads only "
                  "the bundle; never the production DBs; "
                  "never invokes an adapter.**\n")
    return "".join(lines)


def write_phase43_operator_packet(
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


def write_phase43_operator_packet_markdown(
    markdown: str,
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(markdown or "", encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase43_operator_packet",
    "validate_phase43_operator_packet",
    "summarize_phase43_operator_packet",
    "create_phase43_operator_packet_markdown",
    "write_phase43_operator_packet",
    "write_phase43_operator_packet_markdown",
]
