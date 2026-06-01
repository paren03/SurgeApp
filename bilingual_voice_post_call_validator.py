"""Phase 30 - Post-Call Validator.

Re-validates the dummy adapter's returned metadata after the call.
Fails closed if any execution-shape flag is True or if the result does
not match request metadata.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase30.post_call.v1"


_FORBIDDEN_RESULT_KEYS = (
    "audio_bytes", "audio_url", "audio_path",
    "wav_path", "wav_bytes", "mp3_path", "mp3_bytes",
    "voice_clone_ref", "speaker_embedding", "tts_model_path",
    "output_audio_file", "command", "shell", "subprocess",
    "powershell", "executable", "run_command",
)


def verify_no_audio_generated(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    if result.get("produced_audio") is True:
        return {"ok": False, "reasons": ["produced_audio_true"]}
    # Forbidden audio keys
    forb = [k for k in _FORBIDDEN_RESULT_KEYS if k in result
            and k in ("audio_bytes", "audio_url", "audio_path",
                      "wav_path", "wav_bytes", "mp3_path", "mp3_bytes",
                      "voice_clone_ref", "speaker_embedding",
                      "tts_model_path", "output_audio_file")]
    if forb:
        return {"ok": False, "reasons": ["forbidden_audio_keys:" +
                ",".join(forb)]}
    return {"ok": True, "reasons": []}


def verify_no_tts_invoked(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    if result.get("invoked_tts") is True:
        return {"ok": False, "reasons": ["invoked_tts_true"]}
    return {"ok": True, "reasons": []}


def verify_no_subprocess_used(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    if result.get("used_subprocess") is True:
        return {"ok": False, "reasons": ["used_subprocess_true"]}
    forb = [k for k in _FORBIDDEN_RESULT_KEYS if k in result and
            k in ("command", "shell", "subprocess", "powershell",
                  "executable", "run_command")]
    if forb:
        return {"ok": False, "reasons": ["forbidden_command_keys:" +
                ",".join(forb)]}
    return {"ok": True, "reasons": []}


def verify_no_network_used(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    if result.get("used_network") is True:
        return {"ok": False, "reasons": ["used_network_true"]}
    return {"ok": True, "reasons": []}


def verify_no_files_written(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": False, "reasons": ["result_not_dict"]}
    if result.get("wrote_files") is True:
        return {"ok": False, "reasons": ["wrote_files_true"]}
    return {"ok": True, "reasons": []}


def verify_result_matches_request(
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


def validate_post_call_result(
    result: Any,
    request: Optional[Any] = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    sub = {
        "no_audio": verify_no_audio_generated(result),
        "no_tts": verify_no_tts_invoked(result),
        "no_subprocess": verify_no_subprocess_used(result),
        "no_network": verify_no_network_used(result),
        "no_files": verify_no_files_written(result),
    }
    if request is not None:
        sub["matches_request"] = verify_result_matches_request(
            result, request)
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


def write_post_call_validation_report(
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
    "validate_post_call_result",
    "verify_no_audio_generated",
    "verify_no_tts_invoked",
    "verify_no_subprocess_used",
    "verify_no_network_used",
    "verify_no_files_written",
    "verify_result_matches_request",
    "write_post_call_validation_report",
]
