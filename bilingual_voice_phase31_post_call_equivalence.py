"""Phase 31 - Post-Call Equivalence Validator.

Validates that both metadata-only adapters preserve the same hard
safety boundary. Fails closed.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase31.post_call_equivalence.v1"


_FORBIDDEN_RESULT_KEYS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command",
)


def verify_metadata_only_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    reasons: list[str] = []
    if result.get("dry_run") is not True:
        reasons.append("dry_run_must_be_true")
    if result.get("test_only") is not True:
        reasons.append("test_only_must_be_true")
    for k in _FORBIDDEN_RESULT_KEYS:
        if k in result:
            reasons.append(f"forbidden_field:{k}")
    return {"ok": not reasons, "reasons": reasons}


def verify_no_side_effect_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    reasons: list[str] = []
    flags = (("produced_audio", "audio_produced"),
             ("invoked_tts", "tts_invoked"),
             ("used_subprocess", "subprocess_used"),
             ("used_network", "network_used"),
             ("wrote_files", "files_written"))
    for k, label in flags:
        if result.get(k) is True:
            reasons.append(f"{label}_true")
    return {"ok": not reasons, "reasons": reasons}


def verify_result_matches_request_phase31(
    result: Any,
    request: Any,
) -> dict[str, Any]:
    if not isinstance(result, dict) or not isinstance(request, dict):
        return {"ok": False, "reasons": ["bad_inputs"]}
    reasons: list[str] = []
    req_lang = str(request.get("language_mode") or "")
    res_lang = str(result.get("received_language_mode") or "")
    if req_lang and res_lang and req_lang != res_lang:
        reasons.append(f"language_mismatch:{req_lang}!={res_lang}")
    req_seg = int(request.get("segment_count") or 0)
    res_seg = int(result.get("received_segment_count") or 0)
    if req_seg != res_seg:
        reasons.append(f"segment_count_mismatch:{req_seg}!={res_seg}")
    return {"ok": not reasons, "reasons": reasons}


def validate_phase31_result_boundary(
    result: Any,
    request: Optional[Any] = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    sub = {
        "metadata_only": verify_metadata_only_result(result),
        "no_side_effects": verify_no_side_effect_result(result),
    }
    if request is not None:
        sub["matches_request"] = \
            verify_result_matches_request_phase31(result, request)
    for name, r in sub.items():
        if not r["ok"]:
            reasons.append(f"{name}_failed:" +
                           ",".join(r.get("reasons", [])))
    return {
        "ok": not reasons,
        "reasons": reasons,
        "execution_blocked": True,
        "sub_results": sub,
        "phase": _PHASE,
    }


def compare_phase30_phase31_boundaries(
    phase30_result: Any,
    phase31_result: Any,
) -> dict[str, Any]:
    p30_check = validate_phase31_result_boundary(phase30_result)
    p31_check = validate_phase31_result_boundary(phase31_result)
    equal = p30_check["ok"] and p31_check["ok"]
    return {
        "ok": equal,
        "phase30_boundary_ok": p30_check["ok"],
        "phase31_boundary_ok": p31_check["ok"],
        "phase30_reasons": p30_check.get("reasons", []),
        "phase31_reasons": p31_check.get("reasons", []),
        "execution_blocked": True,
        "phase": _PHASE,
    }


def write_phase31_post_call_equivalence_report(
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
    "validate_phase31_result_boundary",
    "compare_phase30_phase31_boundaries",
    "verify_metadata_only_result",
    "verify_no_side_effect_result",
    "verify_result_matches_request_phase31",
    "write_phase31_post_call_equivalence_report",
]
