"""Phase 29 - Operator Review Packet.

Operator-readable, sanitized JSON-serializable summary of a would-be
voice adapter dry-run call. No audio, no commands, no transcripts.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase29.review_packet.v1"


_FORBIDDEN_FIELDS = (
    "audio_bytes", "audio_url", "audio_path",
    "wav_path", "wav_bytes", "mp3_path", "mp3_bytes",
    "voice_clone_ref", "speaker_embedding", "tts_model_path",
    "output_audio_file", "command", "shell", "subprocess",
    "powershell", "executable", "run_command",
    "transcript", "full_transcript", "user_text_raw",
    "assistant_text_raw",
)


_REQUIRED_FIELDS = (
    "packet_id", "created_at", "envelope_id", "job_id",
    "adapter_name", "language_mode", "dry_run", "execution_blocked",
    "consent_summary", "boundary_summary", "safety_summary",
    "capability_summary", "audit_chain_summary",
    "operator_next_actions", "forbidden_actions", "notes",
)


_FORBIDDEN_ACTIONS_DEFAULT = (
    "generate_audio", "invoke_tts", "run_subprocess",
    "call_powershell", "call_sapi", "call_piper",
    "write_audio_file", "clone_voice", "network_call",
)


def _new_id() -> str:
    return f"rev_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _safe_summary(d: Any) -> dict[str, Any]:
    if not isinstance(d, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in d.items():
        if str(k).lower() in _FORBIDDEN_FIELDS:
            continue
        if isinstance(v, dict):
            out[k] = _safe_summary(v)
        elif isinstance(v, (list, tuple)):
            out[k] = [
                (_safe_summary(x) if isinstance(x, dict) else x)
                for x in v
                if not (isinstance(x, str) and any(
                    f in x.lower() for f in _FORBIDDEN_FIELDS))
            ][:50]
        else:
            out[k] = v
    return out


def create_operator_review_packet(
    envelope: dict[str, Any],
    invocation_token: Optional[dict[str, Any]] = None,
    boundary_result: Optional[dict[str, Any]] = None,
    audit_chain: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    env = envelope if isinstance(envelope, dict) else {}
    rj = env.get("render_job") or {}
    desc = (env.get("adapter_choice") or {}).get("chosen") or {}
    spoken = rj.get("spoken_render_payload") or {}
    consent = env.get("consent_decision") or {}
    capability = (env.get("boundary_checks") or {}).get(
        "capability_negotiation") or {}
    safety = rj.get("safety_summary") or spoken.get(
        "safety_summary") or {}
    chain = list(audit_chain or [])
    chain_summary = {
        "length": len(chain),
        "first_event_type": (chain[0].get("event_type")
                             if chain else None),
        "last_event_type": (chain[-1].get("event_type")
                            if chain else None),
    }
    boundary_summary = {
        "execution_blocked": True,
        "ok": (boundary_result or {}).get("ok", False),
        "reasons": (boundary_result or {}).get("reasons", []),
    }
    consent_summary = {
        "approved": bool(consent.get("approved")),
        "operator_id": consent.get("operator_id") or "",
        "scope": consent.get("requested_action")
            or consent.get("scope") or "dry_run_prepare",
        "dry_run_only": True,
    }
    if invocation_token and isinstance(invocation_token, dict):
        consent_summary["invocation_token_id"] = invocation_token.get(
            "token_id")
        consent_summary["expires_at"] = invocation_token.get("expires_at")
        consent_summary["revoked"] = bool(invocation_token.get("revoked"))
    return {
        "packet_id": _new_id(),
        "created_at": time.time(),
        "envelope_id": env.get("envelope_id") or "",
        "job_id": rj.get("job_id") or "",
        "adapter_name": desc.get("adapter_name") or "",
        "language_mode": spoken.get("language_mode") or "",
        "dry_run": True,
        "execution_blocked": True,
        "consent_summary": _safe_summary(consent_summary),
        "boundary_summary": _safe_summary(boundary_summary),
        "safety_summary": _safe_summary(safety),
        "capability_summary": _safe_summary({
            "ok": capability.get("ok"),
            "rejected": capability.get("rejected"),
            "unsupported_features":
                capability.get("unsupported_features", []),
            "downgrade_plan_notes":
                (capability.get("downgrade_plan") or {}).get("notes", []),
        }),
        "audit_chain_summary": chain_summary,
        "operator_next_actions": ["review", "approve_dry_run_only",
                                  "refuse"],
        "forbidden_actions": list(_FORBIDDEN_ACTIONS_DEFAULT),
        "notes": ("phase29 operator review packet; execution blocked; "
                  "no audio; no subprocess"),
        "phase": _PHASE,
    }


def validate_operator_review_packet(packet: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(packet, dict):
        return {"ok": False, "reasons": ["packet_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in packet:
            reasons.append(f"missing_field:{f}")
    if packet.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    if packet.get("execution_blocked") is not True:
        reasons.append("execution_blocked_must_be_true")
    forbidden_missing = set(_FORBIDDEN_ACTIONS_DEFAULT) - set(
        packet.get("forbidden_actions") or [])
    if forbidden_missing:
        reasons.append("missing_forbidden_actions:" +
                       ",".join(sorted(forbidden_missing)))
    # Forbidden key scan (whole packet)
    forb = _scan_forbidden_keys(packet)
    if forb:
        reasons.append("forbidden_field_present:" +
                       ",".join(sorted(set(forb))))
    try:
        json.dumps(packet, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def _scan_forbidden_keys(obj: Any) -> list[str]:
    hits: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                # Skip negation/forbidden-list-style keys
                if ks in ("forbidden_actions",
                          "forbidden_runtime_actions",
                          "operator_next_actions",
                          "permitted_actions",
                          "next_allowed_actions",
                          "supports_languages",
                          "output_placeholders"):
                    continue
                parts = ks.split("_")
                if parts and parts[0] in ("no", "supports", "max",
                                           "accepted", "is", "has",
                                           "forbidden"):
                    _walk(v)
                    continue
                for tok in _FORBIDDEN_FIELDS:
                    if tok == ks or (("_" in tok) and (
                            tok in ks) and not ks.startswith("no_")):
                        if tok not in hits:
                            hits.append(tok)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)

    _walk(obj)
    return hits


def summarize_packet_for_operator(packet: Any) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {"ok": False, "summary": "no_packet"}
    return {
        "ok": True,
        "summary": (
            f"phase29 review packet: adapter="
            f"{packet.get('adapter_name') or 'unknown'} "
            f"lang_mode={packet.get('language_mode') or 'unknown'} "
            f"dry_run=True execution_blocked=True"),
        "packet_id": packet.get("packet_id"),
        "execution_blocked": True,
        "dry_run": True,
        "phase": _PHASE,
    }


def redact_packet_sensitive_fields(packet: Any) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {}
    return _safe_summary(packet)


def write_operator_review_packet(
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


def write_operator_review_packet_report(
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
    "create_operator_review_packet",
    "validate_operator_review_packet",
    "summarize_packet_for_operator",
    "redact_packet_sensitive_fields",
    "write_operator_review_packet",
    "write_operator_review_packet_report",
]
