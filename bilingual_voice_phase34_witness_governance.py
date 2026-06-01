"""Phase 34 - Witness Governance.

Boundary checks on a witness package: Phase 30 / 31 / 33 allowlists,
adapter legality, no runtime execution / secret / audio fields.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import bilingual_voice_callable_adapter_interface as p30i
import bilingual_voice_phase31_adapter_interface as p31i
import bilingual_voice_phase33_adapter_interface as p33i


_PHASE = "phase34.witness_governance.v1"


_ALLOWED_METADATA_ONLY = (
    "dummy_metadata_adapter",
    "bilingual_segment_metadata_adapter",
    "prosody_density_metadata_adapter",
)


_AUDIO_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes",
    "voice_clone_ref", "speaker_embedding",
    "tts_model_path", "output_audio_file",
)

_EXEC_FIELDS = (
    "command", "shell", "powershell_command",
    "executable", "run_command",
)

_SECRET_FIELDS = (
    "private_key", "secret", "material_hex",
    "signing_key_material",
)


def _walk_keys(obj: Any) -> list[str]:
    keys: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                keys.append(str(k).lower())
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)
    _walk(obj)
    return keys


def _walk_string_values(obj: Any) -> list[str]:
    vals: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for v in o.values():
                _walk(v)
        elif isinstance(o, list):
            for v in o:
                _walk(v)
        elif isinstance(o, str):
            vals.append(o)
    _walk(obj)
    return vals


def verify_phase34_phase30_strictness() -> dict[str, Any]:
    allowed = list(p30i.ALLOWED_ADAPTER_TYPES)
    return {
        "ok": allowed == ["dummy_metadata_adapter"],
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase34_phase31_boundary() -> dict[str, Any]:
    allowed = list(p31i.ALLOWED_ADAPTER_TYPES)
    expected = ["dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter"]
    return {
        "ok": allowed == expected,
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


def verify_phase34_phase33_boundary() -> dict[str, Any]:
    allowed = list(p33i.ALLOWED_ADAPTER_TYPES)
    expected = ["dummy_metadata_adapter",
                "bilingual_segment_metadata_adapter",
                "prosody_density_metadata_adapter"]
    return {
        "ok": allowed == expected,
        "allowed_adapter_types": allowed,
        "phase": _PHASE,
    }


_REAL_ADAPTER_TOKENS = (
    "real_piper", "sapi_real", "kokoro_real", "piper_real",
    "real_tts", "audio_renderer", "subprocess_renderer",
    "powershell_renderer", "network_renderer",
)


def verify_phase34_package_adapter_legality(
    package: Any,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    keys = _walk_keys(package)
    bad_adapter_keys = []
    for k in ("adapter_name", "selected_adapter_name",
              "adapter_type"):
        if k in keys:
            pass
    # Walk string values for real adapter tokens — bounded scan
    values = _walk_string_values(package)[:2000]
    bad: list[str] = []
    for v in values:
        vs = str(v).lower()
        for tok in _REAL_ADAPTER_TOKENS:
            if vs == tok and tok not in bad:
                bad.append(tok)
    return {
        "ok": not bad and not bad_adapter_keys,
        "bad_adapter_tokens": bad,
        "phase": _PHASE,
    }


def verify_phase34_package_no_runtime_execution(
    package: Any,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    keys = _walk_keys(package)
    hits = [k for k in _EXEC_FIELDS if k in keys]
    return {"ok": not hits,
            "exec_fields_present": hits, "phase": _PHASE}


def verify_phase34_package_no_secret_material(
    package: Any,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    keys = _walk_keys(package)
    hits = [k for k in _SECRET_FIELDS if k in keys]
    return {"ok": not hits,
            "secret_fields_present": hits, "phase": _PHASE}


def verify_phase34_package_no_audio_material(
    package: Any,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    keys = _walk_keys(package)
    hits = [k for k in _AUDIO_FIELDS if k in keys]
    return {"ok": not hits,
            "audio_fields_present": hits, "phase": _PHASE}


def write_phase34_witness_governance_report(
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
    "verify_phase34_phase30_strictness",
    "verify_phase34_phase31_boundary",
    "verify_phase34_phase33_boundary",
    "verify_phase34_package_adapter_legality",
    "verify_phase34_package_no_runtime_execution",
    "verify_phase34_package_no_secret_material",
    "verify_phase34_package_no_audio_material",
    "write_phase34_witness_governance_report",
]
