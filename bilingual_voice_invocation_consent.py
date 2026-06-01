"""Phase 29 - Per-Invocation Consent Tokens.

Bind a single dry-run invocation to a hashed (envelope_id, job_id) pair
with an expiry. approve=True still cannot enable execution in Phase 29.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase29.invocation_consent.v1"

SUPPORTED_SCOPES = ("dry_run_prepare",)
REJECTED_SCOPES = (
    "execute_audio", "run_tts", "run_subprocess",
    "write_audio", "speak_now",
)


def get_invocation_consent_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "supported_scopes": list(SUPPORTED_SCOPES),
        "rejected_scopes": list(REJECTED_SCOPES),
        "required_token_fields": [
            "token_id", "created_at", "envelope_id", "job_id",
            "binding_hash", "operator_id", "approved", "scope",
            "expires_at", "revoked", "revoked_reason", "phase",
        ],
        "default_expiry_seconds": 300,
        "dry_run_only_in_phase29": True,
        "notes": [
            "approve=True does NOT enable audio/TTS/subprocess.",
            "Missing operator_id rejects any approval.",
            "Token must bind to envelope_id and job_id via hash.",
        ],
    }


def _new_id() -> str:
    return f"itok_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _binding_hash(envelope_id: str, job_id: str) -> str:
    h = hashlib.sha256()
    h.update(str(envelope_id or "").encode("utf-8"))
    h.update(b"|")
    h.update(str(job_id or "").encode("utf-8"))
    return h.hexdigest()


def create_invocation_consent_token(
    envelope: dict[str, Any],
    operator_id: str = "",
    approved: bool = False,
    scope: str = "dry_run_prepare",
    expires_in_seconds: int = 300,
) -> dict[str, Any]:
    env = envelope if isinstance(envelope, dict) else {}
    envelope_id = str(env.get("envelope_id") or "")
    job_id = str((env.get("render_job") or {}).get("job_id") or "")
    now = time.time()
    exp = float(max(1, int(expires_in_seconds or 1)))
    return {
        "token_id": _new_id(),
        "created_at": now,
        "envelope_id": envelope_id,
        "job_id": job_id,
        "binding_hash": _binding_hash(envelope_id, job_id),
        "operator_id": str(operator_id or ""),
        "approved": bool(approved),
        "scope": str(scope or "dry_run_prepare"),
        "expires_at": now + exp,
        "revoked": False,
        "revoked_reason": "",
        "phase": _PHASE,
        "dry_run_only": True,
        "notes": "phase29 invocation consent token; dry_run_only=True",
    }


def is_invocation_token_expired(
    token: Any,
    now: Optional[float] = None,
) -> bool:
    if not isinstance(token, dict):
        return True
    ts = now if isinstance(now, (int, float)) else time.time()
    exp = token.get("expires_at")
    if not isinstance(exp, (int, float)):
        return True
    return ts >= float(exp)


def revoke_invocation_consent_token(
    token: dict[str, Any],
    reason: str = "",
) -> dict[str, Any]:
    if not isinstance(token, dict):
        return create_invocation_consent_token({})
    out = dict(token)
    out["revoked"] = True
    out["revoked_reason"] = str(reason or "")
    return out


def validate_invocation_consent_token(token: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(token, dict):
        return {"ok": False, "reasons": ["token_not_dict"]}
    for f in ("token_id", "created_at", "envelope_id", "job_id",
              "binding_hash", "operator_id", "approved", "scope",
              "expires_at", "revoked", "revoked_reason", "phase"):
        if f not in token:
            reasons.append(f"missing_field:{f}")
    scope = str(token.get("scope") or "").lower()
    if scope in REJECTED_SCOPES:
        reasons.append(f"rejected_scope:{scope}")
    elif scope and scope not in SUPPORTED_SCOPES:
        reasons.append(f"unsupported_scope:{scope}")
    if token.get("approved") and not token.get("operator_id"):
        reasons.append("operator_id_required_when_approved")
    if is_invocation_token_expired(token):
        reasons.append("expired")
    if token.get("revoked"):
        reasons.append("revoked")
    # Hash binding sanity
    expected = _binding_hash(token.get("envelope_id") or "",
                             token.get("job_id") or "")
    if token.get("binding_hash") != expected:
        reasons.append("binding_hash_mismatch")
    return {"ok": not reasons, "reasons": reasons}


def require_valid_invocation_consent(
    token: Any,
    envelope: Any,
) -> dict[str, Any]:
    val = validate_invocation_consent_token(token)
    reasons: list[str] = list(val.get("reasons", []))
    env_id = ""
    job_id = ""
    if isinstance(envelope, dict):
        env_id = str(envelope.get("envelope_id") or "")
        job_id = str((envelope.get("render_job") or {})
                     .get("job_id") or "")
    if not isinstance(token, dict):
        return {"ok": False, "reasons": ["token_not_dict"],
                "execution_blocked": True}
    if env_id and token.get("envelope_id") != env_id:
        reasons.append("envelope_id_mismatch")
    if job_id and token.get("job_id") != job_id:
        reasons.append("job_id_mismatch")
    # Always assert dry-run-only in Phase 29
    return {
        "ok": not reasons,
        "reasons": reasons,
        "execution_blocked": True,
        "dry_run_only": True,
        "phase": _PHASE,
    }


def explain_invocation_consent_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "summary": "no_result_dict"}
    return {
        "ok": bool(result.get("ok")),
        "summary": ("phase29 invocation consent: "
                    f"ok={bool(result.get('ok'))} "
                    f"reasons={result.get('reasons') or []}"),
        "execution_blocked": True,
        "dry_run_only": True,
        "phase": _PHASE,
        "advice": (
            "Phase 29 refuses audio/TTS/subprocess/PowerShell/SAPI/Piper "
            "regardless of approval. A future phase may add runtime "
            "execution behind per-invocation consent + chain audit + "
            "boundary recheck."),
    }


def write_invocation_consent_report(
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
    "SUPPORTED_SCOPES",
    "REJECTED_SCOPES",
    "get_invocation_consent_schema",
    "create_invocation_consent_token",
    "validate_invocation_consent_token",
    "is_invocation_token_expired",
    "revoke_invocation_consent_token",
    "require_valid_invocation_consent",
    "explain_invocation_consent_result",
    "write_invocation_consent_report",
]
