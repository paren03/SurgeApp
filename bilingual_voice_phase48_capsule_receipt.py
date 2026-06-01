"""Phase 48 - Capsule Receipt."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase48.capsule_receipt.v1"


_REQUIRED_RECEIPT_FIELDS = (
    "receipt_id", "created_at", "phase",
    "source_phase", "capsule_id",
    "capsule_root_hash", "manifest_root_hash",
    "artifact_count",
    "fresh_checkout_verification_status",
    "tamper_suite_status",
    "no_runtime_state_dependency",
    "no_adapter_invocation",
    "no_audio", "no_tts",
    "no_subprocess", "no_network",
    "no_multiprocessing",
    "phase21_status",
    "adapter_allowlist_count",
    "rollback_readiness",
    "next_recommended_phase",
    "rehearsal_dry_run_only",
)


_BANNED_RECEIPT_FIELDS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def create_phase48_capsule_receipt(
    contract: dict[str, Any],
    capsule: dict[str, Any],
    manifest: dict[str, Any],
    fresh_result: dict[str, Any],
    tamper_result: dict[str, Any],
) -> dict[str, Any]:
    c = capsule or {}
    m = manifest or {}
    fr = fresh_result or {}
    tr = tamper_result or {}
    status = "ok"
    if not fr.get("ok") or not tr.get("ok"):
        status = "drift_detected"
    nrs = fr.get("no_runtime_state_check") or {}
    bs = c.get("boundary_summary") or {}
    return {
        "receipt_id": f"p48rcpt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "source_phase":
            (contract or {}).get("source_phase",
                                  "phase47"),
        "capsule_id": c.get("capsule_id"),
        "capsule_root_hash":
            c.get("capsule_root_hash"),
        "manifest_root_hash":
            m.get("manifest_root_hash"),
        "artifact_count":
            int(c.get("artifact_count") or 0),
        "fresh_checkout_verification_status":
            "ok" if fr.get("ok") else "drift_detected",
        "tamper_suite_status": {
            "ok": bool(tr.get("ok")),
            "case_count": tr.get("case_count"),
            "detected_count":
                tr.get("detected_count"),
            "undetected_count":
                tr.get("undetected_count"),
        },
        "no_runtime_state_dependency":
            bool(nrs.get("ok")),
        "no_adapter_invocation":
            bool(bs.get(
                "no_adapter_invocation_in_capsule")),
        "no_audio": bool(bs.get("no_audio")),
        "no_tts": bool(bs.get("no_tts")),
        "no_subprocess":
            bool(bs.get("no_subprocess")),
        "no_network": bool(bs.get("no_network")),
        "no_multiprocessing":
            bool(bs.get("no_multiprocessing")),
        "phase21_status":
            c.get("phase21_status_text", "BLOCKED"),
        "adapter_allowlist_count":
            int(c.get(
                "adapter_allowlist_count") or 0),
        "snapshot_status": status,
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
            "Receipt is summary-only; no raw artifact "
            "content.",
            "Phase 21 import remains BLOCKED unless "
            "operator runs Phase 21 separately.",
        ],
    }


def validate_phase48_capsule_receipt(
    receipt: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(receipt, dict):
        return {"ok": False,
                "reasons": ["receipt_not_dict"]}
    for f in _REQUIRED_RECEIPT_FIELDS:
        if f not in receipt:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_RECEIPT_FIELDS:
        if k in receipt and receipt.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    if receipt.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    if int(receipt.get(
            "adapter_allowlist_count") or 0) != 5:
        reasons.append("adapter_count_not_5")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase48_capsule_receipt(
    receipt: Any,
) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        return {"ok": False, "summary": "no_receipt"}
    return {
        "ok": str(receipt.get(
            "fresh_checkout_verification_status") or "")
            == "ok"
            and bool((receipt.get(
                "tamper_suite_status") or {}).get(
                "ok")),
        "summary": (
            f"phase48 receipt: fresh="
            f"{receipt.get('fresh_checkout_verification_status')} "
            f"tamper_ok="
            f"{(receipt.get('tamper_suite_status') or {}).get('ok')} "
            f"phase21="
            f"{receipt.get('phase21_status')} "
            f"adapter_count="
            f"{receipt.get('adapter_allowlist_count')}"),
        "receipt_id": receipt.get("receipt_id"),
        "phase": _PHASE,
    }


def write_phase48_capsule_receipt(
    receipt: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(receipt)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase48_capsule_receipt_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase48_capsule_receipt(report,
                                            output_path)


__all__ = [
    "create_phase48_capsule_receipt",
    "validate_phase48_capsule_receipt",
    "summarize_phase48_capsule_receipt",
    "write_phase48_capsule_receipt",
    "write_phase48_capsule_receipt_report",
]
