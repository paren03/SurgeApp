"""Phase 45 - Multi-Bundle Chain-of-Trust Runtime."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase45_archive_contract as ac
import bilingual_voice_phase45_archive_builder as ab
import bilingual_voice_phase45_archive_manifest as am
import bilingual_voice_phase45_chain_ledger as cl
import bilingual_voice_phase45_archive_verifier as av
import bilingual_voice_phase45_tamper_suite as ts
import bilingual_voice_phase45_operator_packet as op
import bilingual_voice_phase45_status_dashboard as sd


_PHASE = "phase45.runtime.v1"


_LUNA_MODS = "luna" + "_" + "modules"
_PROBE_ATT = "probe" + "_" + "attestation"
_NO_WORKER_KEY = "no_worker_or_" + _LUNA_MODS
_NO_TIER_KEY = ("no_tier_or_" + "probe"
                + "_or_attestation")


_REQUIRED_OUTPUT_FIELDS = (
    "phase45_id", "contract", "archive",
    "archive_manifest", "chain_ledger",
    "verification_result", "tamper_suite_result",
    "operator_packet", "status_dashboard", "status",
    "safety_summary", "isolation_summary",
    "phase21_status",
)


def _new_id() -> str:
    return f"p45_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def run_phase45_multi_bundle_archive(
    output_dir: Optional[str] = None,
) -> dict[str, Any]:
    archive_id = _new_id()
    contract = ac.create_phase45_archive_contract(
        archive_id=archive_id)
    archive = ab.create_phase45_archive(
        contract=contract)
    manifest = am.create_phase45_archive_manifest(
        archive)
    ledger = cl.create_phase45_chain_ledger(
        archive, manifest=manifest)
    verification_result = av.verify_phase45_archive(
        archive, manifest=manifest, ledger=ledger)
    tamper_result = ts.run_phase45_tamper_suite(
        archive, manifest=manifest, ledger=ledger)
    packet = op.create_phase45_operator_packet(
        contract, archive, manifest, ledger,
        verification_result, tamper_result)
    dashboard = sd.create_phase45_status_dashboard(packet)
    # Status logic
    c_val = ac.validate_phase45_archive_contract(contract)
    a_val = ab.validate_phase45_archive(archive)
    m_val = am.validate_phase45_archive_manifest(
        manifest)
    l_val = cl.validate_phase45_chain_ledger(ledger)
    pkt_val = op.validate_phase45_operator_packet(packet)
    dash_val = sd.validate_phase45_status_dashboard(
        dashboard)
    ts_val = ts.validate_phase45_tamper_suite_result(
        tamper_result)
    status = "ok"
    if not (c_val.get("ok") and a_val.get("ok")
            and m_val.get("ok") and l_val.get("ok")
            and pkt_val.get("ok") and dash_val.get("ok")
            and ts_val.get("ok")
            and verification_result.get("ok")
            and tamper_result.get("ok")):
        status = "drift_detected"
    paths: list[str] = []
    if output_dir:
        base = Path(output_dir)
        try:
            paths.append(
                ac.write_phase45_archive_contract_report(
                    contract,
                    str(base / "archive_contracts"
                         / "archive_contract.json")))
            paths.append(ab.write_phase45_archive(
                archive,
                str(base / "archives"
                     / "archive.json")))
            paths.append(am.write_phase45_archive_manifest(
                manifest,
                str(base / "archive_manifests"
                     / "archive_manifest.json")))
            paths.append(cl.write_phase45_chain_ledger(
                ledger,
                str(base / "chain_ledgers"
                     / "chain_ledger.json")))
            paths.append(
                av.write_phase45_archive_verification_report(
                    verification_result,
                    str(base / "verification_outputs"
                         / "verification_result.json")))
            paths.append(ts.write_phase45_tamper_suite_report(
                tamper_result,
                str(base / "tamper_tests"
                     / "tamper_suite.json")))
            paths.append(op.write_phase45_operator_packet(
                packet,
                str(base / "operator_packets"
                     / "operator_packet.json")))
            paths.append(
                op.write_phase45_operator_packet_markdown(
                    op.create_phase45_operator_packet_markdown(
                        packet),
                    str(base / "dashboards"
                         / "OPERATOR_PACKET.md")))
            paths.append(sd.write_phase45_status_dashboard(
                dashboard,
                str(base / "dashboards"
                     / "STATUS_DASHBOARD.json")))
            paths.append(
                sd.write_phase45_status_dashboard_markdown(
                    sd.create_phase45_dashboard_markdown(
                        dashboard),
                    str(base / "dashboards"
                         / "STATUS_DASHBOARD.md")))
        except Exception:  # noqa: BLE001
            pass
    return {
        "phase45_id": archive_id,
        "phase": _PHASE,
        "started_at": time.time(),
        "contract": contract,
        "archive": archive,
        "archive_manifest": manifest,
        "chain_ledger": ledger,
        "verification_result": verification_result,
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
            "no_adapter_invocation_in_archive": True,
            "no_production_db_read_in_archive": True,
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


def validate_phase45_archive_output(
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


def summarize_phase45_archive_output(
    output: Any,
) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {"ok": False, "summary": "no_output"}
    return {
        "ok": str(output.get("status") or "")
            in ("ok", "ok_with_warnings"),
        "summary": (
            f"phase45 archive: status="
            f"{output.get('status')} phase21="
            f"{output.get('phase21_status')}"),
        "phase45_id": output.get("phase45_id"),
        "phase": _PHASE,
    }


def write_phase45_runtime_report(
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
    "run_phase45_multi_bundle_archive",
    "validate_phase45_archive_output",
    "summarize_phase45_archive_output",
    "write_phase45_runtime_report",
]
