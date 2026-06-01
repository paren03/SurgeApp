"""Phase 44 - Roundtrip Receipt."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase44.roundtrip_receipt.v1"


_REQUIRED_RECEIPT_FIELDS = (
    "receipt_id", "created_at", "phase",
    "source_phase", "import_status",
    "source_bundle_hash_summary",
    "import_manifest_hash_summary",
    "fresh_verification_status",
    "tamper_suite_status",
    "artifact_count",
    "excluded_artifacts_summary",
    "no_runtime_db_dependency",
    "no_adapter_reinvocation",
    "no_audio", "no_tts",
    "no_subprocess", "no_network",
    "no_multiprocessing",
    "phase21_status",
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


def create_phase44_roundtrip_receipt(
    contract: dict[str, Any],
    imported_bundle: dict[str, Any],
    import_manifest: dict[str, Any],
    fresh_result: dict[str, Any],
    tamper_result: dict[str, Any],
) -> dict[str, Any]:
    ib = imported_bundle or {}
    im = import_manifest or {}
    fr = fresh_result or {}
    tr = tamper_result or {}
    status = "ok"
    if not fr.get("ok") or not tr.get("ok"):
        status = "drift_detected"
    nrs = fr.get("no_runtime_state_check") or {}
    bs = ib.get("boundary_summary") or {}
    return {
        "receipt_id": f"p44rcpt_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "source_phase":
            (contract or {}).get("source_phase",
                                  "phase43"),
        "import_status": status,
        "source_bundle_hash_summary": {
            "source_manifest_root_hash":
                im.get("source_manifest_root_hash"),
            "source_bundle_id":
                im.get("source_bundle_id"),
        },
        "import_manifest_hash_summary": {
            "import_manifest_root_hash":
                im.get("import_manifest_root_hash"),
            "imported_artifact_count":
                im.get("imported_artifact_count"),
        },
        "fresh_verification_status":
            "ok" if fr.get("ok") else "drift_detected",
        "tamper_suite_status": {
            "ok": bool(tr.get("ok")),
            "case_count": tr.get("case_count"),
            "detected_count": tr.get("detected_count"),
            "undetected_count":
                tr.get("undetected_count"),
        },
        "artifact_count":
            int(ib.get("imported_count") or 0),
        "excluded_artifacts_summary": {
            "missing": ib.get("missing") or [],
            "rejected": ib.get("rejected") or [],
        },
        "no_runtime_db_dependency":
            bool(nrs.get("ok")),
        "no_adapter_reinvocation":
            bool(bs.get(
                "no_adapter_invocation_on_import")),
        "no_audio": bool(bs.get("no_audio")),
        "no_tts": bool(bs.get("no_tts")),
        "no_subprocess": bool(bs.get("no_subprocess")),
        "no_network": bool(bs.get("no_network")),
        "no_multiprocessing":
            bool(bs.get("no_multiprocessing")),
        "phase21_status":
            ib.get("phase21_status_text", "BLOCKED"),
        "rollback_readiness":
            "Delete the 10 Phase 44 files (9 modules + "
            "harness + report) and the 12 sub-folders "
            "under bilingual_stack/voice_adapter_phase44/. "
            "Phase 27-43 remain green.",
        "next_recommended_phase":
            "Phase 45 multi-bundle archive + chain-of-"
            "trust verification OR Phase 41a continuity-"
            "ledger.",
        "rehearsal_dry_run_only": True,
        "notes": [
            "Receipt is content-summary only; no raw "
            "artifact bodies.",
            "Phase 21 import remains BLOCKED unless "
            "operator runs Phase 21 separately.",
        ],
    }


def validate_phase44_roundtrip_receipt(
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
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase44_roundtrip_receipt(
    receipt: Any,
) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        return {"ok": False, "summary": "no_receipt"}
    return {
        "ok": str(receipt.get("import_status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase44 roundtrip: status="
            f"{receipt.get('import_status')} "
            f"fresh="
            f"{receipt.get('fresh_verification_status')} "
            f"tamper_ok="
            f"{(receipt.get('tamper_suite_status') or {}).get('ok')} "
            f"phase21="
            f"{receipt.get('phase21_status')}"),
        "receipt_id": receipt.get("receipt_id"),
        "phase": _PHASE,
    }


def write_phase44_roundtrip_receipt(
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


def write_phase44_roundtrip_receipt_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase44_roundtrip_receipt(report,
                                             output_path)


__all__ = [
    "create_phase44_roundtrip_receipt",
    "validate_phase44_roundtrip_receipt",
    "summarize_phase44_roundtrip_receipt",
    "write_phase44_roundtrip_receipt",
    "write_phase44_roundtrip_receipt_report",
]
