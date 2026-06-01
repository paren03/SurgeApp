"""Phase 27 — Voice-Render Adapter Validation.

Fail-closed validation that adapter plans cannot cross the
no-audio / no-subprocess / no-network boundary.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


FORBIDDEN_KEY_TOKENS = (
    "audio_bytes", "audio_url", "audio_path",
    "wav_path", "wav_bytes", "mp3_path", "mp3_bytes",
    "voice_clone_ref", "speaker_embedding", "tts_model_path",
    "output_audio_file",
)


FORBIDDEN_EXECUTION_TOKENS = (
    "command", "shell", "subprocess", "powershell",
    "executable", "run_command",
)


_NEGATION_PREFIXES = ("no", "supports", "max", "accepted", "is", "has",
                      "forbidden")


def _key_matches_token(ks: str, tok: str) -> bool:
    if ks == tok:
        return True
    parts = ks.split("_")
    if tok not in parts:
        if "_" in tok and tok in ks:
            return True
        return False
    if parts[0] in _NEGATION_PREFIXES:
        return False
    return True


def _scan_keys(obj: Any, tokens: tuple) -> list[str]:
    hits: list[str] = []

    def _walk(o: Any) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                for tok in tokens:
                    if _key_matches_token(ks, tok) and tok not in hits:
                        hits.append(tok)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)

    _walk(obj)
    return hits


def _excluded_paths_from_descriptor(obj: Any) -> Any:
    """Return obj with `forbidden_runtime_actions` lists/values stripped so
    the scan does not flag the policy enumeration itself."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if str(k).lower() == "forbidden_runtime_actions":
                continue
            out[k] = _excluded_paths_from_descriptor(v)
        return out
    if isinstance(obj, list):
        return [_excluded_paths_from_descriptor(v) for v in obj]
    return obj


def scan_adapter_descriptor_for_forbidden_fields(
    descriptor: Any,
) -> dict[str, Any]:
    cleaned = _excluded_paths_from_descriptor(descriptor)
    audio_hits = _scan_keys(cleaned, FORBIDDEN_KEY_TOKENS)
    exec_hits = _scan_keys(cleaned, FORBIDDEN_EXECUTION_TOKENS)
    return {
        "ok": not audio_hits and not exec_hits,
        "audio_hits": audio_hits,
        "execution_hits": exec_hits,
    }


def scan_render_job_for_forbidden_fields(job: Any) -> dict[str, Any]:
    if not isinstance(job, dict):
        return {"ok": False, "audio_hits": [],
                "execution_hits": [], "reasons": ["job_not_dict"]}
    desc = job.get("adapter_descriptor") or {}
    rest = {k: v for k, v in job.items() if k != "adapter_descriptor"}
    desc_scan = scan_adapter_descriptor_for_forbidden_fields(desc)
    rest_audio = _scan_keys(rest, FORBIDDEN_KEY_TOKENS)
    rest_exec = _scan_keys(rest, FORBIDDEN_EXECUTION_TOKENS)
    audio = sorted(set(desc_scan["audio_hits"] + rest_audio))
    execu = sorted(set(desc_scan["execution_hits"] + rest_exec))
    return {
        "ok": not audio and not execu,
        "audio_hits": audio,
        "execution_hits": execu,
        "reasons": [],
    }


def validate_no_audio_payload(job: Any) -> dict[str, Any]:
    scan = scan_render_job_for_forbidden_fields(job)
    return {
        "ok": not scan["audio_hits"],
        "audio_hits": scan["audio_hits"],
    }


def validate_no_runtime_execution_fields(job: Any) -> dict[str, Any]:
    scan = scan_render_job_for_forbidden_fields(job)
    return {
        "ok": not scan["execution_hits"],
        "execution_hits": scan["execution_hits"],
    }


def validate_dry_run_only(job: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(job, dict):
        return {"ok": False, "reasons": ["job_not_dict"]}
    if job.get("dry_run") is not True:
        reasons.append("job_dry_run_not_true")
    desc = job.get("adapter_descriptor") or {}
    if desc.get("dry_run") is not True:
        reasons.append("descriptor_dry_run_not_true")
    op = job.get("output_policy") or {}
    for flag in ("no_audio", "no_subprocess", "no_network",
                 "no_voice_clone", "no_audio_file_write", "plan_only"):
        if not op.get(flag):
            reasons.append(f"output_policy_flag_missing:{flag}")
    return {"ok": not reasons, "reasons": reasons}


def validate_adapter_boundary(job: Any) -> dict[str, Any]:
    audio = validate_no_audio_payload(job)
    exec_ = validate_no_runtime_execution_fields(job)
    dry = validate_dry_run_only(job)
    reasons: list[str] = []
    if not audio["ok"]:
        reasons.append("audio_boundary_crossed:"
                       + ",".join(audio["audio_hits"]))
    if not exec_["ok"]:
        reasons.append("execution_boundary_crossed:"
                       + ",".join(exec_["execution_hits"]))
    if not dry["ok"]:
        reasons.append("dry_run_boundary_crossed:"
                       + ",".join(dry["reasons"]))
    return {
        "ok": not reasons,
        "reasons": reasons,
        "audio_check": audio,
        "execution_check": exec_,
        "dry_run_check": dry,
    }


def write_adapter_validation_report(
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
    "FORBIDDEN_KEY_TOKENS",
    "FORBIDDEN_EXECUTION_TOKENS",
    "scan_adapter_descriptor_for_forbidden_fields",
    "scan_render_job_for_forbidden_fields",
    "validate_no_audio_payload",
    "validate_no_runtime_execution_fields",
    "validate_dry_run_only",
    "validate_adapter_boundary",
    "write_adapter_validation_report",
]
