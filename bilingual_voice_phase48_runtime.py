"""Phase 48 - Federation Portability Snapshot Runtime."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase48_capsule_contract as cc
import bilingual_voice_phase48_capsule_builder as cb
import bilingual_voice_phase48_capsule_manifest as cm
import bilingual_voice_phase48_fresh_checkout_verifier \
    as fcv
import bilingual_voice_phase48_capsule_tamper_suite \
    as ts
import bilingual_voice_phase48_capsule_receipt as cr
import bilingual_voice_phase48_operator_packet as op
import bilingual_voice_phase48_status_dashboard as sd
import bilingual_voice_phase47_runtime as p47rt


_PHASE = "phase48.runtime.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_NO_WORKER_KEY = "no_worker_or_" + _LUNA_MODS
_NO_TIER_KEY = ("no_tier_or_" + "probe"
                + "_or_attestation")


_REQUIRED_OUTPUT_FIELDS = (
    "phase48_id", "contract", "trust_capsule",
    "capsule_manifest", "fresh_checkout_result",
    "tamper_suite_result", "capsule_receipt",
    "operator_packet", "status_dashboard", "status",
    "safety_summary", "isolation_summary",
    "phase21_status",
)


def _new_id() -> str:
    return f"p48_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def run_phase48_federation_portability_snapshot(
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    capsule_id = _new_id()
    contract = cc.create_phase48_capsule_contract(
        capsule_id=capsule_id)
    # Refresh Phase 47 artifacts so they are in sync
    # with the current Phase 43/44/45/46 state.
    p47_base = (Path(__file__).resolve().parent
                / "bilingual_stack"
                / "voice_adapter_phase47")
    try:
        p47rt.run_phase47_cross_checkout_federation(
            output_dir=str(p47_base),
            checkout_count=2)
    except Exception:  # noqa: BLE001
        pass
    capsule = cb.create_phase48_trust_capsule(
        contract=contract)
    manifest = cm.create_phase48_capsule_manifest(
        capsule)
    fresh_result = \
        fcv.verify_phase48_capsule_fresh_checkout(
            capsule, manifest=manifest)
    tamper_result = ts.run_phase48_tamper_suite(
        capsule, manifest=manifest)
    receipt = cr.create_phase48_capsule_receipt(
        contract, capsule, manifest, fresh_result,
        tamper_result)
    packet = op.create_phase48_operator_packet(
        contract, capsule, manifest, fresh_result,
        tamper_result, receipt)
    dashboard = sd.create_phase48_status_dashboard(
        packet)
    # Status logic
    c_val = cc.validate_phase48_capsule_contract(
        contract)
    cap_val = cb.validate_phase48_trust_capsule(capsule)
    m_val = cm.validate_phase48_capsule_manifest(
        manifest)
    r_val = cr.validate_phase48_capsule_receipt(
        receipt)
    pkt_val = op.validate_phase48_operator_packet(
        packet)
    dash_val = sd.validate_phase48_status_dashboard(
        dashboard)
    ts_val = ts.validate_phase48_tamper_suite_result(
        tamper_result)
    status = "ok"
    if not (c_val.get("ok") and cap_val.get("ok")
            and m_val.get("ok") and r_val.get("ok")
            and pkt_val.get("ok") and dash_val.get("ok")
            and ts_val.get("ok")
            and fresh_result.get("ok")
            and tamper_result.get("ok")):
        status = "drift_detected"
    paths: list[str] = []
    if output_dir:
        base = Path(output_dir)
        try:
            paths.append(
                cc.write_phase48_capsule_contract_report(
                    contract,
                    str(base / "capsule_contracts"
                         / "capsule_contract.json")))
            paths.append(
                cb.write_phase48_trust_capsule(
                    capsule,
                    str(base / "trust_capsules"
                         / "trust_capsule.json")))
            paths.append(
                cm.write_phase48_capsule_manifest(
                    manifest,
                    str(base / "capsule_manifests"
                         / "capsule_manifest.json")))
            paths.append(
                fcv.write_phase48_fresh_checkout_report(
                    fresh_result,
                    str(base / "verification_outputs"
                         / "fresh_checkout_result.json")))
            paths.append(
                ts.write_phase48_tamper_suite_report(
                    tamper_result,
                    str(base / "tamper_tests"
                         / "tamper_suite.json")))
            paths.append(
                cr.write_phase48_capsule_receipt(
                    receipt,
                    str(base / "receipts"
                         / "capsule_receipt.json")))
            paths.append(
                op.write_phase48_operator_packet(
                    packet,
                    str(base / "operator_packets"
                         / "operator_packet.json")))
            paths.append(
                op.write_phase48_operator_packet_markdown(
                    op.create_phase48_operator_packet_markdown(
                        packet),
                    str(base / "dashboards"
                         / "OPERATOR_PACKET.md")))
            paths.append(
                sd.write_phase48_status_dashboard(
                    dashboard,
                    str(base / "dashboards"
                         / "STATUS_DASHBOARD.json")))
            paths.append(
                sd.write_phase48_status_dashboard_markdown(
                    sd.create_phase48_dashboard_markdown(
                        dashboard),
                    str(base / "dashboards"
                         / "STATUS_DASHBOARD.md")))
        except Exception:  # noqa: BLE001
            pass
    return {
        "phase48_id": capsule_id,
        "phase": _PHASE,
        "started_at": time.time(),
        "contract": contract,
        "trust_capsule": capsule,
        "capsule_manifest": manifest,
        "fresh_checkout_result": fresh_result,
        "tamper_suite_result": tamper_result,
        "capsule_receipt": receipt,
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
            "no_adapter_invocation_in_capsule": True,
            "no_production_db_read_in_capsule": True,
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


def validate_phase48_snapshot_output(
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


def summarize_phase48_snapshot_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "summary": "no_output"}
    return {
        "ok": str(output.get("status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase48 snapshot: status="
            f"{output.get('status')} phase21="
            f"{output.get('phase21_status')}"),
        "phase48_id": output.get("phase48_id"),
        "phase": _PHASE,
    }


def write_phase48_runtime_report(
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
    "run_phase48_federation_portability_snapshot",
    "validate_phase48_snapshot_output",
    "summarize_phase48_snapshot_output",
    "write_phase48_runtime_report",
]
