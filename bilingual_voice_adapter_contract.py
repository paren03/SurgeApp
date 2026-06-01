"""Phase 27 — Voice-Render Adapter Contract.

Canonical future voice-render adapter contract. Dry-run only. Defines
descriptor + render-job shape. No engine import, no subprocess, no
audio. Renderer paths in Phase 27 are dry-run exclusively.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


SUPPORTED_ADAPTER_TYPES = (
    "dry_run_renderer",
    "piper_shaped",
    "sapi_shaped",
    "kokoro_shaped",
    "local_renderer_shaped",
    "remote_renderer_placeholder",
    "unknown_future_renderer",
)


REQUIRED_DESCRIPTOR_FIELDS = (
    "adapter_name",
    "adapter_type",
    "dry_run",
    "supports_languages",
    "supports_code_switching",
    "supports_segments",
    "supports_prosody",
    "supports_pronunciation_hints",
    "supports_emotion",
    "supports_streaming",
    "max_text_chars",
    "max_segments",
    "accepted_payload_version",
    "forbidden_runtime_actions",
    "notes",
)


REQUIRED_JOB_FIELDS = (
    "job_id",
    "created_at",
    "adapter_descriptor",
    "spoken_render_payload",
    "voice_memory_summary",
    "render_preferences",
    "compatibility_result",
    "dry_run",
    "output_policy",
    "safety_summary",
    "status",
    "notes",
)


# Field/key fragments that may never appear in any descriptor or render job.
FORBIDDEN_FIELD_TOKENS = (
    "audio_bytes", "audio_url", "audio_path",
    "wav_path", "wav_bytes", "mp3_path", "mp3_bytes",
    "voice_clone_ref", "speaker_embedding", "tts_model_path",
    "command", "shell", "subprocess", "powershell",
    "executable", "run_command", "output_audio_file",
)


FORBIDDEN_RUNTIME_ACTIONS_DEFAULT = (
    "audio_generation",
    "tts_invocation",
    "voice_cloning",
    "subprocess_execution",
    "powershell_invocation",
    "sapi_speak",
    "network_call",
    "audio_file_write",
)


HARD_TEXT_CHAR_CAP = 8000
HARD_SEGMENT_CAP = 200


def get_voice_adapter_schema() -> dict[str, Any]:
    return {
        "version": "phase27.v1",
        "supported_adapter_types": list(SUPPORTED_ADAPTER_TYPES),
        "required_descriptor_fields": list(REQUIRED_DESCRIPTOR_FIELDS),
        "required_job_fields": list(REQUIRED_JOB_FIELDS),
        "forbidden_field_tokens": list(FORBIDDEN_FIELD_TOKENS),
        "forbidden_runtime_actions_default":
            list(FORBIDDEN_RUNTIME_ACTIONS_DEFAULT),
        "hard_text_char_cap": HARD_TEXT_CHAR_CAP,
        "hard_segment_cap": HARD_SEGMENT_CAP,
        "dry_run_only": True,
        "notes": [
            "Phase 27 adapter contract. Dry-run only.",
            "No audio bytes, no audio files, no subprocess.",
            "Renderer paths produce plans only; no engine is invoked.",
        ],
    }


def get_supported_adapter_types() -> list[str]:
    return list(SUPPORTED_ADAPTER_TYPES)


def get_required_adapter_fields() -> list[str]:
    return list(REQUIRED_DESCRIPTOR_FIELDS)


def _new_job_id() -> str:
    return f"vrjob_{int(time.time())}_{uuid.uuid4().hex[:10]}"


_NEGATION_PREFIXES = ("no", "supports", "max", "accepted", "is", "has",
                      "forbidden")


def _key_matches_token(ks: str, tok: str) -> bool:
    """Word-aware match: token must equal the key OR appear as an
    underscore-delimited word, but not when the key starts with a
    negation prefix (no_*, supports_*, etc.) — those are documentation
    flags asserting the absence of the forbidden behavior."""
    if ks == tok:
        return True
    parts = ks.split("_")
    if tok not in parts:
        # Allow substring match for compound audio-style tokens that
        # carry their own underscore (audio_bytes, wav_path, etc.).
        if "_" in tok and tok in ks:
            return True
        return False
    if parts[0] in _NEGATION_PREFIXES:
        return False
    return True


def _scan_dict_for_forbidden(obj: Any) -> list[str]:
    hits: list[str] = []

    def _walk(o: Any) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                for tok in FORBIDDEN_FIELD_TOKENS:
                    if _key_matches_token(ks, tok) and tok not in hits:
                        hits.append(tok)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)

    _walk(obj)
    return hits


def create_voice_adapter_descriptor(
    adapter_name: str,
    adapter_type: str,
    capabilities: Optional[dict[str, Any]] = None,
    constraints: Optional[dict[str, Any]] = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    caps = dict(capabilities or {})
    cons = dict(constraints or {})
    descriptor: dict[str, Any] = {
        "adapter_name": str(adapter_name or "").strip()
            or f"adapter_{uuid.uuid4().hex[:6]}",
        "adapter_type": adapter_type if adapter_type in SUPPORTED_ADAPTER_TYPES
            else "unknown_future_renderer",
        "dry_run": bool(dry_run),
        "supports_languages": list(caps.get(
            "supports_languages", ["en", "ru", "mixed"])),
        "supports_code_switching": bool(caps.get(
            "supports_code_switching", False)),
        "supports_segments": bool(caps.get("supports_segments", True)),
        "supports_prosody": bool(caps.get("supports_prosody", False)),
        "supports_pronunciation_hints": bool(caps.get(
            "supports_pronunciation_hints", False)),
        "supports_emotion": bool(caps.get("supports_emotion", False)),
        "supports_streaming": bool(caps.get("supports_streaming", False)),
        "max_text_chars": int(cons.get("max_text_chars", HARD_TEXT_CHAR_CAP)),
        "max_segments": int(cons.get("max_segments", HARD_SEGMENT_CAP)),
        "accepted_payload_version": str(cons.get(
            "accepted_payload_version", "phase25.v1")),
        "forbidden_runtime_actions": list(cons.get(
            "forbidden_runtime_actions",
            list(FORBIDDEN_RUNTIME_ACTIONS_DEFAULT))),
        "notes": str(cons.get("notes", "phase27 dry-run adapter descriptor")),
    }
    return descriptor


def validate_voice_adapter_descriptor(descriptor: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(descriptor, dict):
        return {"ok": False, "reasons": ["descriptor_not_dict"]}
    for f in REQUIRED_DESCRIPTOR_FIELDS:
        if f not in descriptor:
            reasons.append(f"missing_field:{f}")
    if descriptor.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    if descriptor.get("adapter_type") not in SUPPORTED_ADAPTER_TYPES:
        reasons.append("unsupported_adapter_type")
    if descriptor.get("max_text_chars", 0) <= 0:
        reasons.append("invalid_max_text_chars")
    if descriptor.get("max_segments", 0) <= 0:
        reasons.append("invalid_max_segments")
    if not isinstance(descriptor.get("supports_languages"), list) or \
            not descriptor.get("supports_languages"):
        reasons.append("invalid_supports_languages")
    if not isinstance(descriptor.get("forbidden_runtime_actions"), list):
        reasons.append("invalid_forbidden_runtime_actions")
    # Scan descriptor but strip `forbidden_runtime_actions` (it
    # intentionally enumerates banned actions as STRING VALUES; the new
    # word-aware key scanner only looks at keys so this is safe, but we
    # strip defensively).
    desc_for_scan = {k: v for k, v in descriptor.items()
                     if k != "forbidden_runtime_actions"}
    hits = _scan_dict_for_forbidden(desc_for_scan)
    if hits:
        reasons.append("forbidden_field_tokens:" + ",".join(hits))
    return {"ok": not reasons, "reasons": reasons}


def create_render_job(
    payload: dict[str, Any],
    adapter_descriptor: dict[str, Any],
    voice_memory_state: Optional[dict[str, Any]] = None,
    render_preferences: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    vms_summary: dict[str, Any] = {}
    if isinstance(voice_memory_state, dict):
        vms_summary = {
            "session_id": voice_memory_state.get("session_id"),
            "preferred_language_mode":
                voice_memory_state.get("preferred_language_mode"),
            "preferred_spoken_mode":
                voice_memory_state.get("preferred_spoken_mode"),
            "preferred_formality":
                voice_memory_state.get("preferred_formality"),
            "preferred_code_switch_density":
                voice_memory_state.get("preferred_code_switch_density"),
        }
    safety = {}
    if isinstance(payload, dict):
        safety = dict(payload.get("safety_summary") or {})
    job: dict[str, Any] = {
        "job_id": _new_job_id(),
        "created_at": time.time(),
        "adapter_descriptor": dict(adapter_descriptor or {}),
        "spoken_render_payload": dict(payload or {}),
        "voice_memory_summary": vms_summary,
        "render_preferences": dict(render_preferences or {}),
        "compatibility_result": {},
        "dry_run": True,
        "output_policy": {
            "no_audio": True,
            "no_subprocess": True,
            "no_network": True,
            "no_voice_clone": True,
            "no_audio_file_write": True,
            "plan_only": True,
        },
        "safety_summary": safety,
        "status": "planned_dry_run",
        "notes": "phase27 dry-run render job",
    }
    return job


def validate_render_job(job: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(job, dict):
        return {"ok": False, "reasons": ["job_not_dict"]}
    for f in REQUIRED_JOB_FIELDS:
        if f not in job:
            reasons.append(f"missing_field:{f}")
    if job.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    desc_val = validate_voice_adapter_descriptor(
        job.get("adapter_descriptor"))
    if not desc_val["ok"]:
        reasons.append("invalid_adapter_descriptor")
    payload = job.get("spoken_render_payload")
    if not isinstance(payload, dict):
        reasons.append("payload_not_dict")
    op = job.get("output_policy") or {}
    for required_flag in ("no_audio", "no_subprocess", "no_network",
                          "no_voice_clone", "no_audio_file_write",
                          "plan_only"):
        if not op.get(required_flag):
            reasons.append(f"output_policy_missing:{required_flag}")
    # Forbidden-key scan over the whole job. The word-aware matcher
    # already ignores "no_*" / "supports_*" / "max_*" / "forbidden_*"
    # prefixes, so output_policy / forbidden_runtime_actions are safe.
    hits = _scan_dict_for_forbidden({
        k: v for k, v in job.items() if k != "adapter_descriptor"
    })
    hits += _scan_dict_for_forbidden({
        k: v for k, v in (job.get("adapter_descriptor") or {}).items()
        if k != "forbidden_runtime_actions"
    })
    if hits:
        reasons.append("forbidden_field_tokens:" + ",".join(sorted(set(hits))))
    try:
        json.dumps(job, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def normalize_render_job(job: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(job, dict):
        return create_render_job({}, create_voice_adapter_descriptor(
            "fallback_dry_run", "dry_run_renderer"))
    out = dict(job)
    out["dry_run"] = True
    op = dict(out.get("output_policy") or {})
    for k in ("no_audio", "no_subprocess", "no_network", "no_voice_clone",
              "no_audio_file_write", "plan_only"):
        op[k] = True
    out["output_policy"] = op
    if not out.get("job_id"):
        out["job_id"] = _new_job_id()
    if not out.get("created_at"):
        out["created_at"] = time.time()
    out.setdefault("notes", "phase27 dry-run render job")
    out.setdefault("status", "planned_dry_run")
    out.setdefault("compatibility_result", {})
    out.setdefault("render_preferences", {})
    out.setdefault("voice_memory_summary", {})
    return out


def write_voice_adapter_contract_report(
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
    "SUPPORTED_ADAPTER_TYPES",
    "REQUIRED_DESCRIPTOR_FIELDS",
    "REQUIRED_JOB_FIELDS",
    "FORBIDDEN_FIELD_TOKENS",
    "FORBIDDEN_RUNTIME_ACTIONS_DEFAULT",
    "HARD_TEXT_CHAR_CAP",
    "HARD_SEGMENT_CAP",
    "get_voice_adapter_schema",
    "get_supported_adapter_types",
    "get_required_adapter_fields",
    "create_voice_adapter_descriptor",
    "validate_voice_adapter_descriptor",
    "create_render_job",
    "validate_render_job",
    "normalize_render_job",
    "write_voice_adapter_contract_report",
]
