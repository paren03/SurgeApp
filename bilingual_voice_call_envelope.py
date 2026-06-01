"""Phase 28 - Voice Adapter Call Envelope.

Defines a future adapter-call envelope without executing it. Always
dry_run=True; always execution_blocked=True; no audio bytes or paths
ever embedded.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase28.envelope.v1"


_PERMITTED_ACTIONS_DEFAULT = (
    "validate", "plan", "simulate_acceptance", "write_report",
)

_FORBIDDEN_ACTIONS_DEFAULT = (
    "generate_audio", "invoke_tts", "run_subprocess",
    "call_powershell", "call_sapi", "call_piper",
    "write_audio_file", "clone_voice", "network_call",
)


_REQUIRED_FIELDS = (
    "envelope_id", "created_at", "status", "dry_run",
    "render_job", "consent_decision", "adapter_choice",
    "boundary_checks", "execution_blocked", "permitted_actions",
    "forbidden_actions", "audit_summary", "output_placeholders",
    "notes",
)


_FORBIDDEN_KEY_TOKENS = (
    "audio_bytes", "audio_url", "audio_path",
    "wav_path", "wav_bytes", "mp3_path", "mp3_bytes",
    "voice_clone_ref", "speaker_embedding", "tts_model_path",
    "output_audio_file",
)


_NEGATION_PREFIXES = ("no", "supports", "max", "accepted",
                      "is", "has", "forbidden")


def _new_id() -> str:
    return f"venv_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _key_matches(ks: str, tok: str) -> bool:
    if ks == tok:
        return True
    parts = ks.split("_")
    if tok in parts:
        if parts[0] in _NEGATION_PREFIXES:
            return False
        return True
    if "_" in tok and tok in ks:
        return True
    return False


_SKIP_KEYS_FOR_SCAN = (
    "output_placeholders",
    "forbidden_actions",
    "forbidden_runtime_actions",
    "output_policy",
)


def _scan_keys(obj: Any) -> list[str]:
    hits: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                if ks in _SKIP_KEYS_FOR_SCAN:
                    continue
                for tok in _FORBIDDEN_KEY_TOKENS:
                    if _key_matches(ks, tok) and tok not in hits:
                        hits.append(tok)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)

    _walk(obj)
    return hits


def get_call_envelope_schema() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "required_fields": list(_REQUIRED_FIELDS),
        "permitted_actions_default": list(_PERMITTED_ACTIONS_DEFAULT),
        "forbidden_actions_default": list(_FORBIDDEN_ACTIONS_DEFAULT),
        "forbidden_key_tokens": list(_FORBIDDEN_KEY_TOKENS),
        "dry_run_only": True,
        "execution_blocked_default": True,
        "notes": [
            "Phase 28 envelope. Dry-run only. Execution blocked.",
            "No audio bytes, no audio paths, no engine calls.",
        ],
    }


def create_call_envelope(
    render_job: dict[str, Any],
    consent_decision: dict[str, Any],
    adapter_choice: dict[str, Any],
    audit_events: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    audit = list(audit_events or [])
    return {
        "envelope_id": _new_id(),
        "created_at": time.time(),
        "status": "dry_run_ready",
        "dry_run": True,
        "render_job": dict(render_job or {}),
        "consent_decision": dict(consent_decision or {}),
        "adapter_choice": dict(adapter_choice or {}),
        "boundary_checks": {},
        "execution_blocked": True,
        "permitted_actions": list(_PERMITTED_ACTIONS_DEFAULT),
        "forbidden_actions": list(_FORBIDDEN_ACTIONS_DEFAULT),
        "audit_summary": {
            "count": len(audit),
            "phase": "phase28",
        },
        "output_placeholders": {
            "audio_bytes_present": False,
            "audio_file_path": None,
            "renderer_response_present": False,
        },
        "notes": "phase28 dry-run envelope; execution blocked",
        "phase": _PHASE,
    }


def validate_call_envelope(envelope: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(envelope, dict):
        return {"ok": False, "reasons": ["envelope_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in envelope:
            reasons.append(f"missing_field:{f}")
    if envelope.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    if envelope.get("execution_blocked") is not True:
        reasons.append("execution_blocked_must_be_true")
    forbidden = set(_FORBIDDEN_ACTIONS_DEFAULT) - \
        set(envelope.get("forbidden_actions") or [])
    if forbidden:
        reasons.append("missing_forbidden_actions:" +
                       ",".join(sorted(forbidden)))
    hits = _scan_keys(envelope)
    if hits:
        reasons.append("forbidden_key_tokens:" +
                       ",".join(sorted(set(hits))))
    try:
        json.dumps(envelope, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def normalize_call_envelope(envelope: Any) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        return create_call_envelope({}, {}, {})
    out = dict(envelope)
    out["dry_run"] = True
    out["execution_blocked"] = True
    out.setdefault("envelope_id", _new_id())
    out.setdefault("created_at", time.time())
    out.setdefault("status", "dry_run_ready")
    out.setdefault("permitted_actions", list(_PERMITTED_ACTIONS_DEFAULT))
    forbidden = list(set(out.get("forbidden_actions") or []) |
                     set(_FORBIDDEN_ACTIONS_DEFAULT))
    out["forbidden_actions"] = forbidden
    out.setdefault("audit_summary", {"count": 0, "phase": "phase28"})
    out.setdefault("output_placeholders", {
        "audio_bytes_present": False,
        "audio_file_path": None,
        "renderer_response_present": False,
    })
    out.setdefault("boundary_checks", {})
    out.setdefault("notes", "phase28 dry-run envelope; execution blocked")
    out.setdefault("phase", _PHASE)
    return out


def mark_envelope_refused(
    envelope: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    out = normalize_call_envelope(envelope)
    out["status"] = "refused"
    notes = str(out.get("notes") or "")
    out["notes"] = (notes + " | refused:" + str(reason or ""))[:1024]
    return out


def mark_envelope_dry_run_ready(
    envelope: dict[str, Any],
) -> dict[str, Any]:
    out = normalize_call_envelope(envelope)
    out["status"] = "dry_run_ready"
    return out


def write_call_envelope_report(
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
    "get_call_envelope_schema",
    "create_call_envelope",
    "validate_call_envelope",
    "normalize_call_envelope",
    "mark_envelope_refused",
    "mark_envelope_dry_run_ready",
    "write_call_envelope_report",
]
