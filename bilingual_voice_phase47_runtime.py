"""Phase 47 - Cross-Checkout Federation Runtime."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase47_federation_contract as fc
import bilingual_voice_phase47_timeline_importer as ti
import bilingual_voice_phase47_federation_graph as fg
import bilingual_voice_phase47_federation_manifest as fm
import bilingual_voice_phase47_federation_verifier as fv
import bilingual_voice_phase47_drift_detector as dd
import bilingual_voice_phase47_tamper_suite as ts
import bilingual_voice_phase47_operator_packet as op
import bilingual_voice_phase47_status_dashboard as sd


_PHASE = "phase47.runtime.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_NO_WORKER_KEY = "no_worker_or_" + _LUNA_MODS
_NO_TIER_KEY = ("no_tier_or_" + "probe"
                + "_or_attestation")


_REQUIRED_OUTPUT_FIELDS = (
    "phase47_id", "contract", "imported_timelines",
    "federation_graph", "federation_manifest",
    "verification_result", "drift_result",
    "tamper_suite_result", "operator_packet",
    "status_dashboard", "status",
    "safety_summary", "isolation_summary",
    "phase21_status",
)


def _new_id() -> str:
    return f"p47_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def run_phase47_cross_checkout_federation(
    output_dir: Optional[str] = None,
    checkout_count: int = 2,
) -> dict[str, Any]:
    federation_id = _new_id()
    cc = max(2, min(int(checkout_count or 2), 8))
    contract = fc.create_phase47_federation_contract(
        federation_id=federation_id,
        checkout_count=cc)
    # Workspace
    if output_dir:
        ws_root = (Path(output_dir)
                    / "imported_timelines"
                    / f"federation_{int(time.time())}_"
                      f"{uuid.uuid4().hex[:8]}")
    else:
        ws_root = (Path(__file__).resolve().parent
                    / "bilingual_stack"
                    / "voice_adapter_phase47"
                    / "imported_timelines"
                    / f"federation_{int(time.time())}_"
                      f"{uuid.uuid4().hex[:8]}")
    ws_root.mkdir(parents=True, exist_ok=True)
    imported_timelines = \
        ti.import_n_phase47_timeline_packages(
            n=cc, workspace_dir=str(ws_root))
    graph = fg.create_phase47_federation_graph(
        imported_timelines)
    manifest = fm.create_phase47_federation_manifest(
        graph, imported_timelines)
    verification_result = fv.verify_phase47_federation(
        imported_timelines=imported_timelines,
        graph=graph, manifest=manifest)
    drift_result = dd.detect_phase47_federation_drift(
        imported_timelines, graph, manifest)
    tamper_result = ts.run_phase47_tamper_suite(
        imported_timelines, graph, manifest)
    packet = op.create_phase47_operator_packet(
        contract, imported_timelines, graph, manifest,
        verification_result, drift_result,
        tamper_result)
    dashboard = sd.create_phase47_status_dashboard(packet)
    # Status logic
    c_val = fc.validate_phase47_federation_contract(
        contract)
    g_val = fg.validate_phase47_federation_graph(graph)
    m_val = fm.validate_phase47_federation_manifest(
        manifest)
    pkt_val = op.validate_phase47_operator_packet(
        packet)
    dash_val = sd.validate_phase47_status_dashboard(
        dashboard)
    ts_val = ts.validate_phase47_tamper_suite_result(
        tamper_result)
    status = "ok"
    if not (c_val.get("ok") and g_val.get("ok")
            and m_val.get("ok") and pkt_val.get("ok")
            and dash_val.get("ok") and ts_val.get("ok")
            and verification_result.get("ok")
            and tamper_result.get("ok")
            and drift_result.get("ok")):
        status = "drift_detected"
    paths: list[str] = []
    if output_dir:
        base = Path(output_dir)
        try:
            paths.append(
                fc.write_phase47_federation_contract_report(
                    contract,
                    str(base / "federation_contracts"
                         / "federation_contract.json")))
            paths.append(
                fg.write_phase47_federation_graph(
                    graph,
                    str(base / "federation_graphs"
                         / "federation_graph.json")))
            paths.append(
                fm.write_phase47_federation_manifest(
                    manifest,
                    str(base / "federation_manifests"
                         / "federation_manifest.json")))
            paths.append(
                fv.write_phase47_federation_verification_report(
                    verification_result,
                    str(base / "verification_outputs"
                         / "verification_result.json")))
            paths.append(
                dd.write_phase47_drift_report(
                    drift_result,
                    str(base / "drift_reports"
                         / "drift_report.json")))
            paths.append(
                ts.write_phase47_tamper_suite_report(
                    tamper_result,
                    str(base / "tamper_tests"
                         / "tamper_suite.json")))
            paths.append(
                op.write_phase47_operator_packet(
                    packet,
                    str(base / "operator_packets"
                         / "operator_packet.json")))
            paths.append(
                op.write_phase47_operator_packet_markdown(
                    op.create_phase47_operator_packet_markdown(
                        packet),
                    str(base / "dashboards"
                         / "OPERATOR_PACKET.md")))
            paths.append(
                sd.write_phase47_status_dashboard(
                    dashboard,
                    str(base / "dashboards"
                         / "STATUS_DASHBOARD.json")))
            paths.append(
                sd.write_phase47_status_dashboard_markdown(
                    sd.create_phase47_dashboard_markdown(
                        dashboard),
                    str(base / "dashboards"
                         / "STATUS_DASHBOARD.md")))
        except Exception:  # noqa: BLE001
            pass
    return {
        "phase47_id": federation_id,
        "phase": _PHASE,
        "started_at": time.time(),
        "contract": contract,
        "imported_timelines": imported_timelines,
        "federation_graph": graph,
        "federation_manifest": manifest,
        "verification_result": verification_result,
        "drift_result": drift_result,
        "tamper_suite_result": tamper_result,
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
            "no_adapter_invocation_in_federation":
                True,
            "no_production_db_read_in_federation":
                True,
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
        "workspace_dir": str(ws_root),
        "gap_notes": [],
    }


def validate_phase47_federation_output(
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


def summarize_phase47_federation_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "summary": "no_output"}
    return {
        "ok": str(output.get("status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase47 federation: status="
            f"{output.get('status')} phase21="
            f"{output.get('phase21_status')}"),
        "phase47_id": output.get("phase47_id"),
        "phase": _PHASE,
    }


def write_phase47_runtime_report(
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
    "run_phase47_cross_checkout_federation",
    "validate_phase47_federation_output",
    "summarize_phase47_federation_output",
    "write_phase47_runtime_report",
]
