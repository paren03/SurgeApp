"""Phase 32 - Local Verification CLI.

Callable CLI-style verification entry points. No subprocess, no shell,
no network, no sys.exit during tests.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_audit_signing_policy as asp
import bilingual_voice_audit_chain_signer as acs
import bilingual_voice_evidence_bundle as veb
import bilingual_voice_report_integrity_manifest as rim
import bilingual_voice_governance_verifier as gv


_PHASE = "phase32.cli.v1"


_KEY_REGISTRY: dict[str, dict[str, Any]] = {}


def _resolve_key(key_label: str) -> dict[str, Any]:
    if key_label in _KEY_REGISTRY:
        return _KEY_REGISTRY[key_label]
    key = asp.create_test_signing_key(label=key_label)
    _KEY_REGISTRY[key_label] = key
    return key


def verify_phase32_evidence_bundle(
    path: str,
    key_label: str = "phase32_test_key",
) -> dict[str, Any]:
    bundle = veb.read_evidence_bundle(path)
    if not bundle:
        return {"ok": False, "reason": "bundle_not_found",
                "path": str(path), "phase": _PHASE}
    key = _resolve_key(key_label)
    return {
        "ok": True,
        "path": str(path),
        "verification": veb.verify_evidence_bundle(bundle, key),
        "summary": veb.summarize_evidence_bundle(bundle),
        "phase": _PHASE,
    }


def verify_phase32_signed_chain(
    path: str,
    key_label: str = "phase32_test_key",
) -> dict[str, Any]:
    chain = acs.read_signed_audit_chain(path)
    if not chain:
        return {"ok": False, "reason": "chain_not_found",
                "path": str(path), "phase": _PHASE}
    key = _resolve_key(key_label)
    return {
        "ok": True,
        "path": str(path),
        "verification": acs.verify_signed_audit_chain(chain, key),
        "phase": _PHASE,
    }


def verify_phase32_integrity_manifest(
    path: str,
) -> dict[str, Any]:
    manifest = rim.read_report_integrity_manifest(path)
    if not manifest:
        return {"ok": False, "reason": "manifest_not_found",
                "path": str(path), "phase": _PHASE}
    return {
        "ok": True,
        "path": str(path),
        "validation": rim.validate_report_integrity_manifest(manifest),
        "verification": rim.verify_report_integrity_manifest(manifest),
        "phase": _PHASE,
    }


def verify_phase32_governance(
    paths: Optional[list[str]] = None,
) -> dict[str, Any]:
    paths = list(paths or [])
    return {
        "phase30_strict": gv.verify_phase30_strictness(),
        "phase31_two_adapter":
            gv.verify_phase31_two_adapter_boundary(),
        "no_audio_in_artifacts":
            gv.verify_no_audio_boundary_in_artifacts(paths),
        "no_execution_in_artifacts":
            gv.verify_no_execution_boundary_in_artifacts(paths),
        "phase": _PHASE,
    }


def run_phase32_local_verification_suite(
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "started_at": time.time(),
        "phase": _PHASE,
    }
    # Governance only — bundle / chain / manifest paths may not exist
    # in the test environment.
    out["governance"] = verify_phase32_governance(paths=[])
    out["completed_at"] = time.time()
    if output_dir:
        p = Path(output_dir)
        p.mkdir(parents=True, exist_ok=True)
        fpath = p / f"phase32_local_suite_{int(time.time())}.json"
        fpath.write_text(json.dumps(out, ensure_ascii=False, indent=2,
                                     default=str), encoding="utf-8")
        out["written_to"] = str(fpath)
    return out


def main(argv: Optional[list[str]] = None) -> dict[str, Any]:
    """Local callable CLI. Returns structured dict; does not sys.exit
    during tests. No subprocess. No shell."""
    args = list(argv or [])
    if not args:
        return {"ok": False, "reason": "no_command",
                "supported": ["verify-bundle", "verify-chain",
                               "verify-manifest", "verify-governance",
                               "run-suite"],
                "phase": _PHASE}
    cmd = str(args[0]).lower()
    if cmd == "verify-bundle":
        if len(args) < 2:
            return {"ok": False, "reason": "missing_path",
                    "phase": _PHASE}
        return verify_phase32_evidence_bundle(args[1])
    if cmd == "verify-chain":
        if len(args) < 2:
            return {"ok": False, "reason": "missing_path",
                    "phase": _PHASE}
        return verify_phase32_signed_chain(args[1])
    if cmd == "verify-manifest":
        if len(args) < 2:
            return {"ok": False, "reason": "missing_path",
                    "phase": _PHASE}
        return verify_phase32_integrity_manifest(args[1])
    if cmd == "verify-governance":
        return verify_phase32_governance(paths=args[1:])
    if cmd == "run-suite":
        out_dir = args[1] if len(args) >= 2 else None
        return run_phase32_local_verification_suite(out_dir)
    return {"ok": False, "reason": f"unknown_command:{cmd}",
            "phase": _PHASE}


__all__ = [
    "verify_phase32_evidence_bundle",
    "verify_phase32_signed_chain",
    "verify_phase32_integrity_manifest",
    "verify_phase32_governance",
    "run_phase32_local_verification_suite",
    "main",
]
