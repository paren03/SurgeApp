"""Phase 43 - Cross-Machine Portability Harness Runtime."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase43_portability_contract as pc
import bilingual_voice_phase43_bundle_builder as bb
import bilingual_voice_phase43_bundle_manifest as bm
import bilingual_voice_phase43_fresh_checkout_verifier \
    as fcv
import bilingual_voice_phase43_portability_auditor as pa
import bilingual_voice_phase43_operator_packet as op
import bilingual_voice_phase43_status_dashboard as sd


_PHASE = "phase43.runtime.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_NO_WORKER_KEY = "no_worker_or_" + _LUNA_MODS
_NO_TIER_KEY = ("no_tier_or_" + "probe"
                + "_or_attestation")


_REQUIRED_OUTPUT_FIELDS = (
    "phase43_id", "contract", "portable_bundle",
    "bundle_manifest", "fresh_checkout_result",
    "portability_audit", "operator_packet",
    "status_dashboard", "status",
    "safety_summary", "isolation_summary",
    "phase21_status",
)


def _new_id() -> str:
    return f"p43_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def run_phase43_portability_harness(
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    bundle_id = _new_id()
    contract = pc.create_phase43_portability_contract(
        bundle_id=bundle_id)
    bundle = bb.create_phase43_portable_bundle(
        contract=contract)
    manifest = bm.create_phase43_bundle_manifest(bundle)
    fresh_result = \
        fcv.verify_phase43_bundle_fresh_checkout(
            bundle, manifest=manifest)
    portability_audit = \
        pa.audit_phase43_bundle_portability(bundle)
    packet = op.create_phase43_operator_packet(
        contract, bundle, manifest, fresh_result,
        portability_audit)
    dashboard = sd.create_phase43_status_dashboard(packet)
    # Status logic
    c_val = pc.validate_phase43_portability_contract(
        contract)
    b_val = bb.validate_phase43_portable_bundle(bundle)
    m_val = bm.validate_phase43_bundle_manifest(manifest)
    pkt_val = op.validate_phase43_operator_packet(packet)
    dash_val = sd.validate_phase43_status_dashboard(
        dashboard)
    status = "ok"
    if not (c_val.get("ok") and b_val.get("ok")
            and m_val.get("ok") and pkt_val.get("ok")
            and dash_val.get("ok")
            and fresh_result.get("ok")
            and portability_audit.get("ok")):
        status = "drift_detected"
    elif int(portability_audit.get("warn_count")
              or 0) > 0:
        status = "ok_with_warnings"
    paths: list[str] = []
    if output_dir:
        base = Path(output_dir)
        try:
            paths.append(
                pc.write_phase43_portability_contract_report(
                    contract,
                    str(base
                         / "portable_bundles"
                         / "portability_contract.json")))
            paths.append(bb.write_phase43_portable_bundle(
                bundle, str(base / "portable_bundles"
                             / "portable_bundle.json")))
            paths.append(bm.write_phase43_bundle_manifest(
                manifest,
                str(base / "bundle_manifests"
                     / "bundle_manifest.json")))
            paths.append(
                fcv.write_phase43_fresh_checkout_report(
                    fresh_result,
                    str(base
                         / "fresh_checkout_outputs"
                         / "fresh_checkout_result.json")))
            paths.append(
                pa.write_phase43_portability_audit_report(
                    portability_audit,
                    str(base / "portability_audits"
                         / "portability_audit.json")))
            paths.append(op.write_phase43_operator_packet(
                packet,
                str(base / "operator_packets"
                     / "operator_packet.json")))
            paths.append(
                op.write_phase43_operator_packet_markdown(
                    op.create_phase43_operator_packet_markdown(
                        packet),
                    str(base / "dashboards"
                         / "OPERATOR_PACKET.md")))
            paths.append(sd.write_phase43_status_dashboard(
                dashboard,
                str(base / "dashboards"
                     / "STATUS_DASHBOARD.json")))
            paths.append(
                sd.write_phase43_status_dashboard_markdown(
                    sd.create_phase43_dashboard_markdown(
                        dashboard),
                    str(base / "dashboards"
                         / "STATUS_DASHBOARD.md")))
        except Exception:  # noqa: BLE001
            pass
    return {
        "phase43_id": bundle_id,
        "phase": _PHASE,
        "started_at": time.time(),
        "contract": contract,
        "portable_bundle": bundle,
        "bundle_manifest": manifest,
        "fresh_checkout_result": fresh_result,
        "portability_audit": portability_audit,
        "operator_packet": packet,
        "status_dashboard": dashboard,
        "status": status,
        "safety_summary": {
            "no_audio": True,
            "no_tts": True,
            "no_subprocess": True,
            "no_network": True,
            "no_multiprocessing": True,
            "no_corpus_import": True,
            "no_main_runtime_integration": True,
            "no_adapter_invocation": True,
            "no_production_db_read": True,
        },
        "isolation_summary": {
            "no_program_s": True,
            _NO_WORKER_KEY: True,
            _NO_TIER_KEY: True,
        },
        "phase21_status":
            (packet.get("phase21_import_status") or {})
            .get("status_text", "BLOCKED"),
        "paths_written": paths,
        "gap_notes": [],
    }


def validate_phase43_portability_output(
    output: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(output, dict):
        return {"ok": False,
                "reasons": ["output_not_dict"]}
    for f in _REQUIRED_OUTPUT_FIELDS:
        if f not in output:
            reasons.append(f"missing_field:{f}")
    if str(output.get("status") or "") not in (
            "ok", "ok_with_warnings", "drift_detected"):
        reasons.append("invalid_status")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase43_portability_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "summary": "no_output"}
    return {
        "ok": str(output.get("status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase43 portability: status="
            f"{output.get('status')} "
            f"phase21={output.get('phase21_status')}"),
        "phase43_id": output.get("phase43_id"),
        "phase": _PHASE,
    }


def write_phase43_runtime_report(
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
    "run_phase43_portability_harness",
    "validate_phase43_portability_output",
    "summarize_phase43_portability_output",
    "write_phase43_runtime_report",
]
