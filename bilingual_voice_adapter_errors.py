"""Phase 28 - Voice Adapter Error / Refusal Taxonomy."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_ERROR_CODES = (
    "PHASE28_EXECUTION_BLOCKED",
    "CONSENT_MISSING",
    "CONSENT_INVALID",
    "UNSAFE_PAYLOAD",
    "UNSUPPORTED_LANGUAGE_MODE",
    "UNSUPPORTED_CODE_SWITCHING",
    "UNSUPPORTED_PROSODY",
    "UNSUPPORTED_PRONUNCIATION_HINTS",
    "ADAPTER_DRY_RUN_REQUIRED",
    "AUDIO_FIELD_FORBIDDEN",
    "SUBPROCESS_FIELD_FORBIDDEN",
    "NETWORK_FIELD_FORBIDDEN",
    "VOICE_CLONE_FIELD_FORBIDDEN",
    "PAYLOAD_INVALID",
    "CAPABILITY_MISMATCH",
    "UNKNOWN_ADAPTER",
)

_SEVERITIES = ("info", "warn", "error", "blocking")

_BLOCKING = {
    "PHASE28_EXECUTION_BLOCKED",
    "CONSENT_MISSING",
    "CONSENT_INVALID",
    "UNSAFE_PAYLOAD",
    "AUDIO_FIELD_FORBIDDEN",
    "SUBPROCESS_FIELD_FORBIDDEN",
    "NETWORK_FIELD_FORBIDDEN",
    "VOICE_CLONE_FIELD_FORBIDDEN",
    "ADAPTER_DRY_RUN_REQUIRED",
}


def get_voice_adapter_error_codes() -> list[str]:
    return list(_ERROR_CODES)


def create_adapter_error(
    code: str,
    message: str,
    severity: str = "error",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    c = str(code or "").upper().strip()
    if c not in _ERROR_CODES:
        c = "PAYLOAD_INVALID"
    sev = str(severity or "error").lower()
    if sev not in _SEVERITIES:
        sev = "error"
    if c in _BLOCKING:
        sev = "blocking"
    return {
        "error_id": f"err_{int(time.time())}_{uuid.uuid4().hex[:10]}",
        "code": c,
        "message": str(message or "")[:512],
        "severity": sev,
        "metadata": dict(metadata or {}),
        "phase": "phase28",
        "created_at": time.time(),
    }


def validate_adapter_error(error: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(error, dict):
        return {"ok": False, "reasons": ["error_not_dict"]}
    for f in ("error_id", "code", "message", "severity",
              "metadata", "phase", "created_at"):
        if f not in error:
            reasons.append(f"missing_field:{f}")
    if error.get("code") not in _ERROR_CODES:
        reasons.append("unsupported_code")
    if error.get("severity") not in _SEVERITIES:
        reasons.append("unsupported_severity")
    if not isinstance(error.get("metadata"), dict):
        reasons.append("metadata_not_dict")
    return {"ok": not reasons, "reasons": reasons}


def is_blocking_error(error: Any) -> bool:
    if not isinstance(error, dict):
        return True
    c = str(error.get("code") or "").upper()
    s = str(error.get("severity") or "").lower()
    return c in _BLOCKING or s == "blocking"


def summarize_adapter_errors(
    errors: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(errors, list):
        errors = []
    by_code: dict[str, int] = {}
    by_sev: dict[str, int] = {}
    blocking = 0
    for e in errors:
        if not isinstance(e, dict):
            continue
        c = str(e.get("code") or "")
        s = str(e.get("severity") or "")
        by_code[c] = by_code.get(c, 0) + 1
        by_sev[s] = by_sev.get(s, 0) + 1
        if is_blocking_error(e):
            blocking += 1
    return {
        "count": len(errors),
        "by_code": by_code,
        "by_severity": by_sev,
        "blocking_count": blocking,
        "phase": "phase28",
    }


def write_adapter_error_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "get_voice_adapter_error_codes",
    "create_adapter_error",
    "validate_adapter_error",
    "is_blocking_error",
    "summarize_adapter_errors",
    "write_adapter_error_report",
]
