"""Phase 36 - Handoff Runtime (standalone).

Creates a handoff contract + sealed envelope + public descriptor +
secret-boundary scan + handoff verification + operator guide. Writes
secret-bearing artifacts ONLY under local_secret_handoff and ONLY
when allow_secret_write=True.
"""

from __future__ import annotations

import json
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_audit_signing_policy as asp
import bilingual_voice_phase36_handoff_contract as hc
import bilingual_voice_phase36_key_handoff_envelope as henv
import bilingual_voice_phase36_secret_boundary as sb
import bilingual_voice_phase36_public_descriptor_bridge as pdb
import bilingual_voice_phase36_handoff_verifier as hv
import bilingual_voice_phase36_operator_guide as oag
import bilingual_voice_phase34_export_runtime as p34


_PHASE = "phase36.handoff_runtime.v1"


_LOCAL_SECRET_FOLDER = (
    "bilingual_stack/voice_adapter_phase36/local_secret_handoff")


def _new_id() -> str:
    return f"p36_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _envelope_summary_only(env: dict[str, Any]) -> dict[str, Any]:
    """Build a report-safe summary that EXCLUDES sealed_payload and
    any other secret field. Suitable for `reports/` folder."""
    keep = ("envelope_id", "created_at", "envelope_label",
             "test_only", "algorithm", "key_id", "key_label",
             "consent_marker_hash", "public_fingerprint",
             "allowed_use", "forbidden_use",
             "expiration_hint", "cleanup_instructions",
             "secret_material_present", "phase", "sealing_note")
    return {k: env.get(k) for k in keep if k in env}


def create_phase36_key_handoff(
    consent_marker: str = "",
    output_dir: Optional[str] = None,
    allow_secret_write: bool = False,
) -> dict[str, Any]:
    if not consent_marker:
        return {
            "phase36_id": _new_id(),
            "status": "refused",
            "reason": "consent_marker_required",
            "phase": _PHASE,
        }
    contract = hc.create_handoff_contract()
    key = asp.create_test_signing_key("phase36_test_handoff_key")
    envelope = henv.create_key_handoff_envelope(
        key, consent_marker=consent_marker)
    if "ok" in envelope and envelope.get("ok") is False:
        return {
            "phase36_id": _new_id(),
            "status": "refused",
            "reason": "envelope_creation_failed",
            "envelope_reasons": envelope.get("reasons", []),
            "phase": _PHASE,
        }
    pub = pdb.create_public_descriptor_from_handoff(envelope)
    boundary = {
        "envelope_summary_safe":
            sb.validate_no_secret_leakage_in_public_artifact(
                _envelope_summary_only(envelope)),
        "public_descriptor_safe":
            sb.validate_no_secret_leakage_in_public_artifact(pub),
    }
    written_paths: dict[str, str] = {}
    if output_dir:
        base = Path(output_dir)
        base.mkdir(parents=True, exist_ok=True)
        # Contract + public descriptor + summary + guide go under
        # the operator-facing folders (no secret).
        contract_path = base / "handoff_contract.json"
        hc.write_phase36_handoff_contract_report(
            contract, str(contract_path))
        written_paths["handoff_contract"] = str(contract_path)
        pub_path = base / "public_descriptor.json"
        pdb.write_handoff_public_descriptor(pub, str(pub_path))
        written_paths["public_descriptor"] = str(pub_path)
        summary_path = base / "envelope_summary.json"
        # The summary excludes sealed_payload — safe to write
        # anywhere except local_secret_handoff.
        summary = _envelope_summary_only(envelope)
        henv.write_key_handoff_envelope_report(
            summary, str(summary_path))
        written_paths["envelope_summary"] = str(summary_path)
        guide = oag.create_phase36_operator_handoff_guide()
        guide_path = base / "operator_handoff_guide.json"
        oag.write_phase36_operator_handoff_guide(
            guide, str(guide_path))
        written_paths["operator_guide"] = str(guide_path)
        if allow_secret_write:
            # Secret-bearing write is only permitted under
            # local_secret_handoff.
            secret_dir = Path(_LOCAL_SECRET_FOLDER)
            secret_dir.mkdir(parents=True, exist_ok=True)
            secret_path = (
                secret_dir / f"handoff_envelope_{int(time.time())}_"
                f"{uuid.uuid4().hex[:6]}.json")
            try:
                henv.write_key_handoff_envelope(
                    envelope, str(secret_path),
                    allow_secret_write=True)
                written_paths["sealed_envelope"] = str(secret_path)
            except ValueError as e:
                written_paths["sealed_envelope_error"] = str(e)
    # Verification result (passes when consent_marker matches)
    # Use a tiny synthetic signed evidence to demonstrate the
    # unseal+verify path without needing any external file.
    import bilingual_voice_audit_chain as vac
    import bilingual_voice_phase33_signed_evidence as p33sev
    chain = vac.append_chain_event(
        [], vac.create_audit_chain_event("preflight", "ok", "x"))
    inv = {"receipt_id": "recv_x",
            "adapter_name": "dummy_metadata_adapter",
            "adapter_type": "dummy_metadata_adapter",
            "operator_id_hash": "a" * 16, "dry_run": True,
            "test_only": True,
            "execution_boundary_preserved": True,
            "audio_generated": False, "tts_invoked": False,
            "subprocess_used": False, "network_used": False,
            "files_written": False, "request_id": "x",
            "result_id": "y", "pre_call_status": "ok",
            "post_call_status": "ok",
            "audit_chain_hash": "abc", "notes": "x",
            "created_at": 1.0, "phase": "p"}
    sev = p33sev.create_phase33_signed_evidence({
        "audit_chain": chain,
        "invocation_receipt": inv,
        "selection_receipt": {},
        "selected_adapter_result": {
            "result_id": "y",
            "adapter_name": "dummy_metadata_adapter",
            "produced_audio": False, "invoked_tts": False,
            "used_subprocess": False, "used_network": False,
            "wrote_files": False},
        "status": "ok"}, key)
    verification = hv.verify_with_handoff_envelope(
        sev, envelope, consent_marker)
    return {
        "phase36_id": _new_id(),
        "handoff_contract": contract,
        "sealed_envelope_summary":
            henv.summarize_key_handoff_envelope(envelope),
        "public_descriptor": pub,
        "secret_boundary_result": boundary,
        "handoff_verification_result": verification,
        "operator_guide":
            oag.create_phase36_operator_handoff_guide(),
        "written_paths": written_paths,
        "safety_summary": {
            "audio_generated": False, "tts_invoked": False,
            "subprocess_used": False, "network_used": False,
            "multiprocessing_used": False,
            "signing_secret_committed": False,
            "signing_secret_in_reports": False,
        },
        "isolation_summary": {
            "program_s_touched": False,
            "gated_internals_touched": False,
            "core_runtime_modules_touched": False,
            "daemon_spawned": False,
        },
        "status": ("ok" if verification.get(
            "status") == "pass" else "refused"),
        "gap_notes": [],
        "phase": _PHASE,
    }


def verify_phase36_with_handoff(
    consent_marker: str = "",
    output_dir: Optional[str] = None,
    include_demo: bool = True,
    limit: int = 6,
) -> dict[str, Any]:
    # Build a Phase 34 export and verify it through Phase 36 with a
    # fresh sealed envelope.
    if not consent_marker:
        return {
            "phase36_id": _new_id(),
            "status": "refused",
            "reason": "consent_marker_required",
            "phase": _PHASE,
        }
    key = asp.create_test_signing_key("phase36_verify_test_key")
    envelope = henv.create_key_handoff_envelope(
        key, consent_marker=consent_marker)
    # Phase 34 export with our key
    export = p34.create_phase34_witness_export(
        "hello luna", operator_id="operator_local",
        approve=True, sign_evidence=True)
    pkg = export.get("witness_package") or {}
    # Re-sign the signed_evidence under OUR key so the envelope
    # actually matches what the witness package carries.
    import bilingual_voice_phase33_signed_evidence as p33sev
    re_sev = p33sev.create_phase33_signed_evidence({
        "audit_chain":
            (export.get("phase33_output") or {}).get(
                "audit_chain") or [],
        "invocation_receipt":
            (export.get("phase33_output") or {}).get(
                "invocation_receipt") or {},
        "selection_receipt":
            (export.get("phase33_output") or {}).get(
                "selection_receipt") or {},
        "selected_adapter_result":
            (export.get("phase33_output") or {}).get(
                "selected_adapter_result") or {},
        "status": "ok"}, key)
    pkg2 = dict(pkg)
    pkg2["signed_evidence_payload"] = re_sev
    pkg_check = hv.verify_witness_package_with_handoff(
        pkg2, envelope, consent_marker)
    res: dict[str, Any] = {
        "phase36_id": _new_id(),
        "envelope_summary":
            henv.summarize_key_handoff_envelope(envelope),
        "witness_package_check": pkg_check,
        "status": "ok" if pkg_check.get("status") == "pass"
            else "refused",
        "phase": _PHASE,
    }
    if include_demo:
        res["demo"] = create_phase36_handoff_demo(limit)
    if output_dir:
        base = Path(output_dir)
        base.mkdir(parents=True, exist_ok=True)
        rep_path = base / f"phase36_verify_{int(time.time())}.json"
        # Use the secret-stripped verification result for write
        hv.write_handoff_verification_report(pkg_check,
                                              str(rep_path))
        res["written_path"] = str(rep_path)
    return res


def create_phase36_handoff_demo(limit: int = 6) -> dict[str, Any]:
    cap = max(1, min(int(limit or 1), 6))
    scenarios = [
        ("operator_consent_alpha",),
        ("operator_consent_beta",),
        ("operator_consent_gamma",),
        ("operator_consent_delta",),
        ("operator_consent_epsilon",),
        ("operator_consent_zeta",),
    ][:cap]
    out: list[dict[str, Any]] = []
    for (consent,) in scenarios:
        with tempfile.TemporaryDirectory(
                prefix="phase36_demo_") as td:
            r = create_phase36_key_handoff(
                consent_marker=consent,
                output_dir=td, allow_secret_write=False)
        out.append({
            "consent_marker": "*" * 8,  # never echo consent
            "status": r.get("status"),
            "envelope_summary_present":
                isinstance(r.get("sealed_envelope_summary"), dict),
            "public_descriptor_safe":
                ((r.get("secret_boundary_result") or {})
                  .get("public_descriptor_safe") or {})
                .get("ok"),
            "verification_status":
                (r.get("handoff_verification_result") or {}).get(
                    "status"),
        })
    return {"demo": out, "count": len(out), "phase": _PHASE}


def validate_phase36_handoff_output(output: Any) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "reasons": ["output_not_dict"]}
    reasons: list[str] = []
    for f in ("phase36_id", "handoff_contract",
              "sealed_envelope_summary", "public_descriptor",
              "secret_boundary_result",
              "handoff_verification_result", "operator_guide",
              "status"):
        if f not in output:
            reasons.append(f"missing_field:{f}")
    # Output must never carry sealed_payload directly
    hits = sb.scan_object_for_secret_fields(output)
    # Allow consent_marker_hash but not raw consent_marker or sealed_payload
    if "sealed_payload" in hits:
        reasons.append("sealed_payload_leaked_in_output")
    if output.get("status") == "ok":
        ver = output.get("handoff_verification_result") or {}
        if ver.get("status") != "pass":
            reasons.append("status_ok_but_verification_not_pass")
    return {"ok": not reasons, "reasons": reasons, "phase": _PHASE}


def write_phase36_handoff_runtime_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    if not isinstance(report, dict):
        raise ValueError("report_not_dict")
    hits = sb.scan_object_for_secret_fields(report)
    if "sealed_payload" in hits:
        raise ValueError(
            "refuse_to_write_report_with_sealed_payload")
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_phase36_key_handoff",
    "verify_phase36_with_handoff",
    "create_phase36_handoff_demo",
    "validate_phase36_handoff_output",
    "write_phase36_handoff_runtime_report",
]
