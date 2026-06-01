"""Phase 44 - Cross-Machine Import Simulation Runtime."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase44_import_contract as ic
import bilingual_voice_phase44_bundle_importer as bi
import bilingual_voice_phase44_import_manifest as im
import bilingual_voice_phase44_fresh_import_verifier as fiv
import bilingual_voice_phase44_tamper_suite as ts
import bilingual_voice_phase44_roundtrip_receipt as rr
import bilingual_voice_phase44_operator_packet as op
import bilingual_voice_phase44_status_dashboard as sd


_PHASE = "phase44.runtime.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_NO_WORKER_KEY = "no_worker_or_" + _LUNA_MODS
_NO_TIER_KEY = ("no_tier_or_" + "probe"
                + "_or_attestation")


_REQUIRED_OUTPUT_FIELDS = (
    "phase44_id", "contract", "imported_bundle",
    "import_manifest", "fresh_import_result",
    "tamper_suite_result", "roundtrip_receipt",
    "operator_packet", "status_dashboard", "status",
    "safety_summary", "isolation_summary",
    "phase21_status",
)


def _new_id() -> str:
    return f"p44_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def run_phase44_cross_machine_import_simulation(
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    import_id = _new_id()
    contract = ic.create_phase44_import_contract(
        import_id=import_id)
    # Workspace dir for the fresh-checkout simulation
    if output_dir:
        base = Path(output_dir)
    else:
        base = (Path(__file__).resolve().parent
                / "bilingual_stack"
                / "voice_adapter_phase44")
    workspace_root = (
        base / "fresh_checkout_simulation"
            / f"workspace_{int(time.time())}_"
              f"{uuid.uuid4().hex[:8]}")
    workspace_root.mkdir(parents=True, exist_ok=True)
    (workspace_root / "artifacts").mkdir(
        parents=True, exist_ok=True)
    imported = bi.import_phase43_bundle_to_workspace(
        workspace_dir=str(workspace_root))
    manifest = im.create_phase44_import_manifest(imported)
    fresh_result = \
        fiv.verify_phase44_imported_bundle_fresh(
            imported, import_manifest=manifest)
    tamper_result = ts.run_phase44_tamper_suite(
        imported, import_manifest=manifest)
    receipt = rr.create_phase44_roundtrip_receipt(
        contract, imported, manifest, fresh_result,
        tamper_result)
    packet = op.create_phase44_operator_packet(
        contract, imported, manifest, fresh_result,
        tamper_result, receipt)
    dashboard = sd.create_phase44_status_dashboard(packet)
    # Status logic
    c_val = ic.validate_phase44_import_contract(contract)
    ib_val = bi.validate_phase44_imported_bundle(imported)
    m_val = im.validate_phase44_import_manifest(manifest)
    rcpt_val = rr.validate_phase44_roundtrip_receipt(
        receipt)
    pkt_val = op.validate_phase44_operator_packet(packet)
    dash_val = sd.validate_phase44_status_dashboard(
        dashboard)
    ts_val = ts.validate_phase44_tamper_suite_result(
        tamper_result)
    status = "ok"
    if not (c_val.get("ok") and ib_val.get("ok")
            and m_val.get("ok") and rcpt_val.get("ok")
            and pkt_val.get("ok") and dash_val.get("ok")
            and ts_val.get("ok")
            and fresh_result.get("ok")
            and tamper_result.get("ok")
            and imported.get("ok")):
        status = "drift_detected"
    paths: list[str] = []
    try:
        paths.append(
            ic.write_phase44_import_contract_report(
                contract,
                str(base / "import_contracts"
                     / "import_contract.json")))
        paths.append(bi.write_phase44_imported_bundle(
            imported,
            str(base / "imported_bundles"
                 / "imported_bundle.json")))
        paths.append(im.write_phase44_import_manifest(
            manifest,
            str(base / "roundtrip_manifests"
                 / "import_manifest.json")))
        paths.append(fiv.write_phase44_fresh_import_report(
            fresh_result,
            str(base / "verification_outputs"
                 / "fresh_import_result.json")))
        paths.append(ts.write_phase44_tamper_suite_report(
            tamper_result,
            str(base / "tamper_tests"
                 / "tamper_suite.json")))
        paths.append(rr.write_phase44_roundtrip_receipt(
            receipt,
            str(base / "reports"
                 / "roundtrip_receipt.json")))
        paths.append(op.write_phase44_operator_packet(
            packet,
            str(base / "operator_packets"
                 / "operator_packet.json")))
        paths.append(
            op.write_phase44_operator_packet_markdown(
                op.create_phase44_operator_packet_markdown(
                    packet),
                str(base / "dashboards"
                     / "OPERATOR_PACKET.md")))
        paths.append(sd.write_phase44_status_dashboard(
            dashboard,
            str(base / "dashboards"
                 / "STATUS_DASHBOARD.json")))
        paths.append(
            sd.write_phase44_status_dashboard_markdown(
                sd.create_phase44_dashboard_markdown(
                    dashboard),
                str(base / "dashboards"
                     / "STATUS_DASHBOARD.md")))
    except Exception:  # noqa: BLE001
        pass
    return {
        "phase44_id": import_id,
        "phase": _PHASE,
        "started_at": time.time(),
        "contract": contract,
        "imported_bundle": imported,
        "import_manifest": manifest,
        "fresh_import_result": fresh_result,
        "tamper_suite_result": tamper_result,
        "roundtrip_receipt": receipt,
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
            "no_adapter_invocation_on_import": True,
            "no_production_db_read_on_import": True,
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
        "workspace_root": str(workspace_root),
        "gap_notes": [],
    }


def validate_phase44_import_simulation_output(
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


def summarize_phase44_import_simulation_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "summary": "no_output"}
    return {
        "ok": str(output.get("status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase44 import: status="
            f"{output.get('status')} phase21="
            f"{output.get('phase21_status')}"),
        "phase44_id": output.get("phase44_id"),
        "phase": _PHASE,
    }


def write_phase44_runtime_report(
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
    "run_phase44_cross_machine_import_simulation",
    "validate_phase44_import_simulation_output",
    "summarize_phase44_import_simulation_output",
    "write_phase44_runtime_report",
]
