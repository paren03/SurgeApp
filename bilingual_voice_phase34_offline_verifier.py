"""Phase 34 - Offline Witness Verifier.

Verifies a witness package using only local data and a public/test
key descriptor. No subprocess, no network.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_audit_chain_signer as acs
import bilingual_voice_phase34_witness_package as wp
import bilingual_voice_phase34_key_descriptor_export as kde
import bilingual_voice_report_integrity_manifest as rim


_PHASE = "phase34.offline_verifier.v1"


_FORBIDDEN_FIELDS = (
    "audio_bytes", "audio_url", "audio_path", "wav_path",
    "wav_bytes", "mp3_path", "mp3_bytes", "voice_clone_ref",
    "speaker_embedding", "tts_model_path", "output_audio_file",
    "command", "shell", "powershell_command",
    "executable", "run_command", "transcript",
    "full_transcript", "user_text_raw", "assistant_text_raw",
    "operator_id",
)


_SECRET_FIELDS = (
    "private_key", "secret", "material_hex",
    "signing_key_material",
)


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


def _scan_secrets(obj: Any) -> list[str]:
    hits: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                if ks in _SECRET_FIELDS and ks not in hits:
                    hits.append(ks)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)
    _walk(obj)
    return hits


def verify_package_signed_evidence(
    package: Any,
    key_descriptor: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    se = package.get("signed_evidence_payload") or {}
    chain = se.get("signed_audit_chain") or []
    if not isinstance(chain, list) or not chain:
        return {"ok": False,
                "reasons": ["signed_audit_chain_missing_or_empty"]}
    if key_descriptor is None:
        return {"ok": False,
                "reasons": ["key_descriptor_required_for_verify"]}
    res = acs.verify_signed_audit_chain(chain, key_descriptor)
    return {
        "ok": bool(res.get("ok")),
        "reasons": res.get("reasons", []),
        "chain_length": res.get("length", len(chain)),
        "phase": _PHASE,
    }


def verify_package_integrity_manifest(
    package: Any,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    manifest = package.get("report_integrity_manifest") or {}
    if not isinstance(manifest, dict) or not manifest:
        return {"ok": True, "reasons": [],
                "note": "no_manifest_present"}
    val = rim.validate_report_integrity_manifest(manifest)
    if not val["ok"]:
        return {"ok": False, "reasons": val["reasons"],
                "phase": _PHASE}
    vres = rim.verify_report_integrity_manifest(manifest)
    return {
        "ok": vres["ok"],
        "reasons": vres.get("reasons", []),
        "missing": vres.get("missing", []),
        "phase": _PHASE,
    }


def verify_package_boundary_summary(
    package: Any,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    reasons: list[str] = []
    bsum = package.get("boundary_summary") or {}
    if bsum.get("execution_blocked") is not True:
        reasons.append("execution_not_blocked")
    if bsum.get("dry_run") is not True:
        reasons.append("dry_run_not_true")
    if bsum.get("phase30_strict") is not True:
        reasons.append("phase30_not_strict")
    if bsum.get("phase31_two_adapter_boundary") is not True:
        reasons.append("phase31_boundary_not_two")
    if bsum.get("phase33_three_adapter_boundary") is not True:
        reasons.append("phase33_boundary_not_three")
    if bsum.get("signed_evidence_required") is not True:
        reasons.append("signed_evidence_not_required")
    forb = _scan_forbidden(package)
    if forb:
        reasons.append("forbidden_field:" +
                       ",".join(sorted(set(forb))))
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def verify_package_governance_summary(
    package: Any,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    gv = package.get("governance_summary") or {}
    if not isinstance(gv, dict):
        return {"ok": False, "reasons": ["governance_not_dict"]}
    # Accept either an empty governance summary or a dict with ok flags
    flagged = []
    for key in ("phase30_strict", "phase31_boundary",
                 "phase33_boundary",
                 "allowed_adapters_only", "metadata_only",
                 "signed_evidence_required"):
        sub = gv.get(key)
        if isinstance(sub, dict) and sub.get("ok") is False:
            flagged.append(key)
    return {
        "ok": not flagged,
        "reasons": ([f"governance_failed:{k}" for k in flagged]),
        "phase": _PHASE,
    }


def verify_package_no_secret_leakage(
    package: Any,
) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {"ok": False, "reasons": ["package_not_dict"]}
    hits = _scan_secrets(package)
    pub = package.get("key_descriptor_public") or {}
    if isinstance(pub, dict):
        for k in _SECRET_FIELDS:
            if k in pub and k not in hits:
                hits.append(k)
    return {
        "ok": not hits,
        "reasons": ([f"secret_leak:{h}" for h in hits]),
        "phase": _PHASE,
    }


def create_offline_verification_result(
    package: Any,
    checks: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(checks, dict):
        checks = {}
    pid = (package or {}).get("package_id") \
        if isinstance(package, dict) else ""
    passed = [k for k, v in checks.items()
              if isinstance(v, dict) and v.get("ok")]
    failed = [k for k, v in checks.items()
              if isinstance(v, dict) and v.get("ok") is False]
    return {
        "package_id": pid,
        "created_at": time.time(),
        "status": ("pass" if not failed else "fail"),
        "checks": checks,
        "checks_passed": passed,
        "checks_failed": failed,
        "checks_warned": [],
        "phase": _PHASE,
    }


def verify_witness_package(
    package: Any,
    key_descriptor: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    pkg_val = wp.validate_witness_package(package)
    checks = {
        "package_validation": {
            "ok": pkg_val["ok"], "reasons": pkg_val["reasons"]},
        "signed_evidence": verify_package_signed_evidence(
            package, key_descriptor),
        "integrity_manifest":
            verify_package_integrity_manifest(package),
        "boundary_summary":
            verify_package_boundary_summary(package),
        "governance_summary":
            verify_package_governance_summary(package),
        "no_secret_leakage":
            verify_package_no_secret_leakage(package),
    }
    if isinstance(key_descriptor, dict):
        # If the supplied key has raw HMAC material it is a private
        # signing key the verifier uses for chain math; the public-
        # descriptor sub-check does not apply. Validate the public
        # descriptor only when no material_hex is present.
        if not key_descriptor.get("material_hex"):
            key_val = kde.validate_public_key_descriptor(
                key_descriptor)
            checks["public_key_descriptor"] = {
                "ok": key_val["ok"],
                "reasons": key_val["reasons"]}
    return create_offline_verification_result(package, checks)


def write_offline_verification_report(
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
    "verify_witness_package",
    "verify_package_signed_evidence",
    "verify_package_integrity_manifest",
    "verify_package_boundary_summary",
    "verify_package_governance_summary",
    "verify_package_no_secret_leakage",
    "create_offline_verification_result",
    "write_offline_verification_report",
]
