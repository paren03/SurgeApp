"""Phase 34 - Witness Export Runtime (standalone).

Creates a witness package + public key descriptor + integrity manifest
+ offline verification + receipt + operator guide, all in-memory or
local-disk only. No subprocess, no network.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_adapter_phase33_runtime as p33
import bilingual_voice_audit_signing_policy as asp
import bilingual_voice_phase34_witness_package as wp
import bilingual_voice_phase34_key_descriptor_export as kde
import bilingual_voice_phase34_offline_verifier as ov
import bilingual_voice_phase34_witness_receipt as wr
import bilingual_voice_phase34_witness_governance as wg
import bilingual_voice_phase34_operator_guide as og
import bilingual_voice_report_integrity_manifest as rim


_PHASE = "phase34.export_runtime.v1"


def _new_id() -> str:
    return f"p34_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _build_default_key() -> dict[str, Any]:
    return asp.create_test_signing_key("phase34_witness_test_key")


def _integrity_manifest_for_reports() -> dict[str, Any]:
    root = Path(__file__).resolve().parent
    candidates = [
        str(root / f) for f in (
            "PHASE27_VOICE_RENDER_ADAPTER_SKELETON_REPORT.md",
            "PHASE28_OPERATOR_GATED_VOICE_ADAPTER_REPORT.md",
            "PHASE29_OPERATOR_GATED_RUNTIME_ADAPTER_B_REPORT.md",
            "PHASE30_CALLABLE_ADAPTER_BOUNDARY_REPORT.md",
            "PHASE31_MULTI_ADAPTER_BOUNDARY_REPORT.md",
            "PHASE32_AUDIT_SIGNING_AND_VERIFICATION_REPORT.md",
            "PHASE33_THREE_ADAPTER_SIGNED_GOVERNANCE_REPORT.md",
        )
    ]
    return rim.create_report_integrity_manifest(
        candidates, manifest_id="phase34_witness")


def _governance_summary() -> dict[str, Any]:
    return {
        "phase30_strict": wg.verify_phase34_phase30_strictness(),
        "phase31_boundary": wg.verify_phase34_phase31_boundary(),
        "phase33_boundary": wg.verify_phase34_phase33_boundary(),
    }


def create_phase34_witness_export(
    user_text: str,
    draft_response_text: str = "",
    operator_id: str = "operator_local",
    approve: bool = True,
    preferred_adapter: Optional[str] = None,
    sign_evidence: bool = True,
    limit: int = 25,
) -> dict[str, Any]:
    # Run Phase 33 with a fixed signing key by injecting a key via
    # the signed evidence module's default; we re-sign with the
    # SAME explicit key here so the public descriptor we export
    # matches what the witness package carries.
    key = _build_default_key()
    p33_out = p33.prepare_phase33_three_adapter_invocation(
        user_text=user_text,
        draft_response_text=draft_response_text,
        operator_id=operator_id,
        approve=bool(approve),
        preferred_adapter=preferred_adapter,
        sign_evidence=bool(sign_evidence),
        limit=limit,
    )
    # Re-sign the Phase 33 evidence with our explicit key so the
    # exported public descriptor matches.
    import bilingual_voice_phase33_signed_evidence as p33sev
    if sign_evidence and p33_out.get("status") == "ok":
        provisional = {
            "audit_chain": p33_out.get("audit_chain") or [],
            "invocation_receipt": p33_out.get("invocation_receipt")
                or {},
            "selection_receipt": p33_out.get("selection_receipt")
                or {},
            "selected_adapter_result":
                p33_out.get("selected_adapter_result") or {},
            "status": "ok",
        }
        p33_out["signed_evidence"] = \
            p33sev.create_phase33_signed_evidence(provisional, key)
    integrity = _integrity_manifest_for_reports()
    package = wp.create_witness_package(
        package_id=f"witpkg_{int(time.time())}_"
                    f"{uuid.uuid4().hex[:10]}",
        signed_evidence=p33_out.get("signed_evidence") or {},
        integrity_manifest=integrity,
        governance_report=_governance_summary())
    public_key = kde.create_public_test_key_descriptor(key)
    package["key_descriptor_public"] = public_key
    pkg_val = wp.validate_witness_package(package)
    # HMAC verification needs the actual key material, so we pass the
    # PRIVATE descriptor here. The PUBLIC descriptor is what the
    # witness package ships to external operators for identity
    # comparison; HMAC operators must already hold the same key.
    verification = ov.verify_witness_package(package, key)
    receipt = wr.create_witness_verification_receipt(
        verification, package.get("package_id"),
        verifier_id="local_phase34_export_runtime")
    guide = og.create_operator_verification_guide()
    status = ("ok" if (pkg_val["ok"] and verification.get(
        "status") == "pass" and bool(p33_out.get("signed_evidence")))
              else "refused")
    return {
        "phase34_id": _new_id(),
        "phase33_output": p33_out,
        "witness_package": package,
        "public_key_descriptor": public_key,
        "offline_verification_result": verification,
        "witness_receipt": receipt,
        "operator_guide": guide,
        "integrity_manifest": integrity,
        "status": status,
        "safety_summary": {
            "audio_generated": False, "tts_invoked": False,
            "subprocess_used": False, "network_used": False,
            "files_written_outside_reports": False,
            "signing_secret_exported": False,
        },
        "isolation_summary": {
            "program_s_touched": False,
            "gated_internals_touched": False,
            "core_runtime_modules_touched": False,
            "daemon_spawned": False,
        },
        "gap_notes": [],
        "phase": _PHASE,
    }


def verify_phase34_witness_export(
    package: Any,
    key_descriptor: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return ov.verify_witness_package(package, key_descriptor)


def create_phase34_operator_bundle(
    output_dir: Optional[str] = None,
    include_demo: bool = True,
) -> dict[str, Any]:
    base = Path(output_dir or
                  (Path(__file__).resolve().parent /
                   "bilingual_stack" / "voice_adapter_phase34" /
                   "operator_guides"))
    base.mkdir(parents=True, exist_ok=True)
    guide = og.create_operator_verification_guide()
    guide_path = base / f"operator_guide_{int(time.time())}.json"
    og.write_operator_verification_guide(guide, str(guide_path))
    out: dict[str, Any] = {
        "guide_path": str(guide_path),
        "phase": _PHASE,
    }
    if include_demo:
        export = create_phase34_witness_export(
            "operator bundle demo",
            draft_response_text="ok",
            operator_id="operator_local",
            approve=True, sign_evidence=True)
        pkg_path = base / f"witness_package_{int(time.time())}.json"
        wp.write_witness_package(export["witness_package"],
                                 str(pkg_path))
        key_path = base / f"public_key_{int(time.time())}.json"
        kde.write_public_key_descriptor(
            export["public_key_descriptor"], str(key_path))
        receipt_path = base / f"witness_receipt_{int(time.time())}.json"
        wr.write_witness_verification_receipt(
            export["witness_receipt"], str(receipt_path))
        out.update({
            "witness_package_path": str(pkg_path),
            "public_key_path": str(key_path),
            "witness_receipt_path": str(receipt_path),
            "demo_status": export["status"],
        })
    return out


def demo_phase34_witness_exports(limit: int = 6) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 6))
    scenarios = [
        ("Hello Luna", None),
        ("Привет Луна", None),
        ("Mix russian and english", None),
        ("English with prosody",
         "prosody_density_metadata_adapter"),
        ("Bilingual segments",
         "bilingual_segment_metadata_adapter"),
        ("Simple Russian", None),
    ][:cap]
    out: list[dict[str, Any]] = []
    for ut, pref in scenarios:
        r = create_phase34_witness_export(
            user_text=ut, operator_id="operator_local",
            approve=True, preferred_adapter=pref,
            sign_evidence=True)
        out.append({
            "user_text": ut,
            "status": r["status"],
            "package_id": (r.get("witness_package") or {}).get(
                "package_id"),
            "verification_status":
                (r.get("offline_verification_result") or {}).get(
                    "status"),
            "boundary_preserved":
                (r.get("witness_receipt") or {}).get(
                    "boundary_preserved"),
            "secrets_absent":
                (r.get("witness_receipt") or {}).get(
                    "secrets_absent"),
        })
    return {"demo": out, "count": len(out), "phase": _PHASE}


def write_phase34_export_runtime_report(
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
    "create_phase34_witness_export",
    "verify_phase34_witness_export",
    "create_phase34_operator_bundle",
    "demo_phase34_witness_exports",
    "write_phase34_export_runtime_report",
]
