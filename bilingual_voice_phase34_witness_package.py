"""Phase 34 - Witness Package.

Local witness package for external/offline verification of Phase 33
signed governance evidence. Never carries secret material.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase34.witness_package.v1"


_REQUIRED_FIELDS = (
    "package_id", "created_at", "phase",
    "signed_evidence_summary", "signed_evidence_payload",
    "key_descriptor_public", "report_integrity_manifest",
    "governance_summary", "boundary_summary",
    "verification_instructions", "forbidden_runtime_actions",
    "production_impact_summary", "metadata",
)


_FORBIDDEN_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
    "operator_id", "private_key", "secret",
    "signing_key_material", "material_hex",
)


def _new_id() -> str:
    return f"wit_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _scan_forbidden(obj: Any) -> list[str]:
    hits: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                if ks in _FORBIDDEN_FIELDS and ks not in hits:
                    hits.append(ks)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)
    _walk(obj)
    return hits


def strip_witness_package_secrets(package: Any) -> dict[str, Any]:
    """Recursive copy with any forbidden key removed."""
    if not isinstance(package, dict):
        return {}

    def _strip(o: Any) -> Any:
        if isinstance(o, dict):
            return {k: _strip(v) for k, v in o.items()
                    if str(k).lower() not in _FORBIDDEN_FIELDS}
        if isinstance(o, list):
            return [_strip(v) for v in o]
        return o

    return _strip(package)


def create_witness_package(
    package_id: str,
    signed_evidence: Optional[dict[str, Any]] = None,
    integrity_manifest: Optional[dict[str, Any]] = None,
    governance_report: Optional[dict[str, Any]] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    se = signed_evidence if isinstance(signed_evidence,
                                         dict) else {}
    im = integrity_manifest if isinstance(integrity_manifest,
                                            dict) else {}
    gv = governance_report if isinstance(governance_report,
                                           dict) else {}
    sm = se.get("signing_metadata") or {}
    summary = {
        "evidence_id": se.get("evidence_id"),
        "phase": se.get("phase"),
        "chain_length":
            (se.get("audit_chain_summary") or {}).get("length", 0),
        "signed_at": sm.get("signed_at"),
        "algorithm": sm.get("algorithm"),
        "key_id": sm.get("key_id"),
        "test_only": sm.get("test_only"),
    }
    return {
        "package_id": str(package_id or _new_id()),
        "created_at": time.time(),
        "phase": _PHASE,
        "signed_evidence_summary": summary,
        "signed_evidence_payload": se,
        "key_descriptor_public": {},  # populated by export runtime
        "report_integrity_manifest": im,
        "governance_summary": gv,
        "boundary_summary": {
            "execution_blocked": True,
            "dry_run": True,
            "test_only": True,
            "phase30_strict": True,
            "phase31_two_adapter_boundary": True,
            "phase33_three_adapter_boundary": True,
            "signed_evidence_required": True,
        },
        "verification_instructions": [
            "Open the public key descriptor and confirm test_only=True.",
            "Re-hash files in report_integrity_manifest and compare.",
            "Re-verify signed_evidence_payload via Phase 32 chain "
            "signer using the public key descriptor.",
            "Confirm boundary_summary flags all indicate "
            "execution_blocked and dry_run.",
            "Confirm no secret/audio/command fields appear anywhere "
            "in the package.",
        ],
        "forbidden_runtime_actions": [
            "generate_audio", "invoke_tts", "run_subprocess",
            "call_powershell", "call_sapi", "call_piper",
            "write_audio_file", "clone_voice", "network_call",
        ],
        "production_impact_summary": {
            "expected_en_words": 2814,
            "expected_ru_words": 2518,
            "expected_ru_phrases": 35,
            "expected_concepts": 26,
            "expected_links": 52,
            "expected_manifests": 90,
        },
        "metadata": dict(metadata or {}),
    }


def validate_witness_package(package: Any) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    for f in _REQUIRED_FIELDS:
        if f not in package:
            reasons.append(f"missing_field:{f}")
    hits = _scan_forbidden(package)
    if hits:
        reasons.append("forbidden_field:" +
                       ",".join(sorted(set(hits))))
    bsum = package.get("boundary_summary") or {}
    if bsum.get("execution_blocked") is not True:
        reasons.append("execution_not_blocked")
    if bsum.get("dry_run") is not True:
        reasons.append("dry_run_not_true")
    try:
        json.dumps(package, default=str)
    except Exception as e:  # noqa: BLE001
        reasons.append(f"not_json_serializable:{type(e).__name__}")
    return {"ok": not reasons, "reasons": reasons}


def summarize_witness_package(package: Any) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "summary": "no_package"}
    sa = package.get("signed_evidence_summary") or {}
    return {
        "ok": True,
        "summary": (
            f"phase34 witness: package_id={package.get('package_id')} "
            f"chain_length={sa.get('chain_length', 0)} "
            f"execution_blocked="
            f"{(package.get('boundary_summary') or {}).get('execution_blocked')}"),
        "package_id": package.get("package_id"),
        "phase": _PHASE,
    }


def write_witness_package(
    package: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(package)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_witness_package(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        body = json.loads(p.read_text(encoding="utf-8"))
        return body if isinstance(body, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def write_witness_package_report(
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
    "create_witness_package",
    "validate_witness_package",
    "summarize_witness_package",
    "write_witness_package",
    "read_witness_package",
    "strip_witness_package_secrets",
    "write_witness_package_report",
]
